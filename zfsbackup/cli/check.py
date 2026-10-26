"""`zfsbackup-config check` -- is this host's backup healthy? (plan item C13, B-6b)

One read-only pass that answers "are snapshots, sends and received copies as
fresh as the configuration says they should be, and is the daemon up on the
stored configuration?" for a human, a script, node_exporter or a Nagios-style
monitor.

What it does, in order:

1. Reads the stored configuration (`read_snapshot`: one read transaction, no
   writer lock) and the daemon's state (`status.collect`: the stored key and the
   pid file; nothing is signalled).
2. Lazily imports `zfsbackup.health_collect` (which imports `libzfseasy`, so a
   host without ZFS can still run every other command: the `rename.py`
   precedent) and `zfsbackup.health` (stdlib only). `health_collect.collect`
   makes the read-only `zfs list` / `zfs get` calls; `health.evaluate` grades them.
3. Adds this command's own entries as `health.CheckEntry`s, so every serialiser
   handles them: the daemon (not running = CRIT; running but not on the stored
   generation = WARN; draining for an upgrade = WARN `daemon/upgrade`), the client certificate (expired = CRIT, expiring
   within 30 days or unusable = WARN) and, once the daemon's workers have
   written their status files (`<db>.status/<worker>.json`, S-7), the outcome of
   their last attempts (below).
4. Prints one of: text (default; one line per entry, then an overall line),
   `--json`, `--prometheus` or `--nagios`; `--output FILE` writes the text,
   JSON or Prometheus form to FILE atomically instead of stdout (node_exporter's
   textfile collector). `--nagios` stands alone (it is one line on stdout).

Worker outcomes (S-7b). One entry per `(kind, dataset[, destination])` record in
the status files, source `worker_snapshot`, `worker_prune` or `worker_send`,
subject the dataset, peer the destination. The grading mirrors the lag grading:
a failed attempt counts like one missed interval, so `consecutive_failures`
below `--warn-factor` (default 2) is OK (one failure in a row is noted in the
reason but not a verdict: the next cycle may fix it), from `--warn-factor` WARN,
from `--crit-factor` (default 4) CRIT. A last attempt that succeeded is OK
whatever happened before. NO DATA IS NEVER A FAILURE: a missing status
directory (an older daemon, or nothing written yet) or a worker with no file
adds no entry at all. A file the reader refuses (a symlink, writable by
group/others, an untrusted owner, too big, not JSON, a newer schema) is one
WARN entry (source `worker_status`) naming it; the other files are still read.
A file that cannot be taken as current is STALE and reported as one WARN
`worker_status` entry per worker ("no recent data"), its outcomes not graded:
no daemon is running (a stopped daemon, or a worker a live-to-dry-run reload
replaced, leaves its last file behind for ever), the writing pid is gone, or the
file was not updated for `max(4 x check_interval, 24 h)` (a backstop: a worker may go hours
without writing during one long transfer). Staleness is never OK
and never CRIT on its own (a stopped daemon is already the daemon entry's CRIT).
In the JSON each such entry also carries `kind`, `consecutive_failures`,
`last_attempt` and `last_success`; the Prometheus form adds
`zfsbackup_worker_consecutive_failures`; `--nagios` adds a perfdata item per
record (failures with the two thresholds).

Exit codes:

- Without `--output`: 0 healthy, 1 unhealthy (WARN or CRIT), 78 the config
  database is unusable, and the CLI's usual codes for anything else (2 usage,
  75 busy).
- With `--output FILE`: 0 once the file has been written, WHATEVER the verdict
  (the verdict is in the file; the timer that runs this must not look failed
  because a backup is late). Any other code means the check could not run or
  the file could not be written -- an unwritable `--output`, an unusable
  database, an unreadable lock file, an unexpected exception -- and then the
  previous file is left untouched, its `zfsbackup_check_timestamp_seconds`
  showing how old it is.
- With `--nagios`, the plugin API's: 0 OK, 1 WARN, 2 CRIT, 3 UNKNOWN. A pool
  that cannot be listed is a CRIT entry (2), not UNKNOWN; UNKNOWN (3) is for
  a check that could not run at all -- usage errors, an unusable database, an
  exception while collecting -- since that says nothing about the backups.
"""

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, List, Optional, Tuple

import click

from zfsbackup import health as _health  # stdlib only
from zfsbackup import identity
from zfsbackup.cli import status as status_view
from zfsbackup.cli import tlsfiles
from zfsbackup.cli.errors import CliError
from zfsbackup.cli.write import read_snapshot

__all__ = [
    "register",
    "gather",
    "client_certificate_entry",
    "daemon_entries",
    "worker_entries",
    "OutcomeEntry",
    "CERT_WARN_DAYS",
]

#: A client certificate that expires within this many days is a WARN.
CERT_WARN_DAYS = 30

EXIT_NAGIOS_UNKNOWN = 3

#: Source names of the entries this module adds.
SOURCE_DAEMON = "daemon"
SOURCE_TLS = "tls"

#: Prefix of the source of a worker-outcome entry (`worker_send`, ...), and
#: the source of an entry about a status file the reader refused.
SOURCE_WORKER = "worker"
SOURCE_WORKER_STATUS = "worker_status"

_NAGIOS_NAME = "ZFSBACKUP"


@dataclass(frozen=True)
class OutcomeEntry(_health.CheckEntry):
    """A worker-outcome `CheckEntry` with the numbers behind it (the JSON,
    Prometheus and Nagios forms use them; `health`'s own serialisers know only
    the base fields)."""

    kind: str = ""
    consecutive_failures: int = 0
    last_attempt: Optional[float] = None
    last_success: Optional[float] = None
    warn_failures: float = 0.0
    crit_failures: float = 0.0


# --------------------------------------------------------------------------
# this command's own entries
# --------------------------------------------------------------------------


def daemon_entries(
    health: Any, status: status_view.Status, now: Optional[float] = None
) -> List[Any]:
    """The daemon's presence, whether it runs the stored configuration and,
    only while it drains for an in-place upgrade (S-8), a `daemon/upgrade`
    WARN (never CRIT: snapshots and pruning continue and systemd restarts the
    daemon; absent otherwise)."""
    CheckEntry = health.CheckEntry
    info = status.daemon
    if info is None:
        return [CheckEntry(SOURCE_DAEMON, "process", health.STATE_CRIT, "no daemon is running")]
    if status.verdict == status_view.SUPERVISOR_GONE:
        return [CheckEntry(SOURCE_DAEMON, "process", health.STATE_CRIT, status.message)]
    pid = f"pid {info.pid}" if info.pid is not None else "pid unknown"
    entries = [CheckEntry(SOURCE_DAEMON, "process", health.STATE_OK, f"running ({pid})")]
    if status.verdict == status_view.IN_SYNC:
        state, reason = health.STATE_OK, f"applied the stored generation {status.stored.generation}"
    elif status.verdict == status_view.APPLYING:
        state, reason = health.STATE_OK, status.message
    else:  # behind, deferred, rejected, unreadable: not on the stored configuration
        state, reason = health.STATE_WARN, status.message
    entries.append(CheckEntry(SOURCE_DAEMON, "config_applied", state, reason))
    if info.draining is not None:
        moment = time.time() if now is None else now
        since = datetime.fromtimestamp(info.draining, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        why = f": {info.draining_reason}" if info.draining_reason else ""
        entries.append(CheckEntry(
            SOURCE_DAEMON, "upgrade", health.STATE_WARN,
            f"draining for an upgrade since {since} "
            f"({health.format_duration(max(moment - info.draining, 0.0))} ago){why}",
        ))
    return entries


def client_certificate_entry(
    health: Any, config: Any, resolved: Any, now: datetime
) -> Optional[Any]:
    """The client certificate's verdict, or `None` when no destination is
    configured (the certificate is then not needed; no data is no entry).
    Reads the certificate only, never a key."""
    CheckEntry = health.CheckEntry
    if not getattr(config, "destinations", None):
        return None
    files = tlsfiles.client_identity_files(config, resolved)
    summary = tlsfiles.describe_certificate(files.cert)
    subject = "client_certificate"
    if not summary.exists:
        return CheckEntry(
            SOURCE_TLS, subject, health.STATE_WARN,
            f"{files.cert} does not exist: this host is not enrolled with its destinations",
        )
    if summary.error is not None or summary.not_after is None:
        return CheckEntry(
            SOURCE_TLS, subject, health.STATE_WARN,
            f"{files.cert} cannot be read: {summary.error or 'no expiry'}",
        )
    if summary.not_after <= now:
        return CheckEntry(
            SOURCE_TLS, subject, health.STATE_CRIT,
            f"the client certificate expired on {summary.not_after:%Y-%m-%d}; run `identity rotate`",
        )
    left = summary.not_after - now
    if left <= timedelta(days=CERT_WARN_DAYS):
        days = max(left.days, 0)
        return CheckEntry(
            SOURCE_TLS, subject, health.STATE_WARN,
            f"the client certificate expires in {days} day(s) ({summary.not_after:%Y-%m-%d}); "
            "run `identity rotate`",
        )
    # Not expired and not near it: is it still one the server would accept?
    try:
        identity.check_client_cert(files.cert.read_bytes()[: identity.MAX_CLIENT_CERT_PEM + 1], now)
    except (ValueError, OSError) as exc:
        return CheckEntry(
            SOURCE_TLS, subject, health.STATE_WARN,
            f"the client certificate would not be accepted: {exc}",
        )
    return CheckEntry(
        SOURCE_TLS, subject, health.STATE_OK,
        f"the client certificate is valid until {summary.not_after:%Y-%m-%d}",
    )


def _ago(health: Any, now: float, ts: Optional[float]) -> str:
    if ts is None:
        return "never"
    return f"{health.format_duration(max(now - ts, 0.0))} ago"


def _outcome_entry(health: Any, rec: Any, now: float, warn: float, crit: float) -> OutcomeEntry:
    failures = max(rec.consecutive_failures, 1 if rec.result == "error" else 0)
    if failures >= 1 and failures >= crit:
        state = health.STATE_CRIT
    elif failures >= 1 and failures >= warn:
        state = health.STATE_WARN
    else:
        state = health.STATE_OK
    error = f": {rec.last_error}" if rec.last_error else ""
    if failures == 0:
        if rec.result is None and rec.last_attempt is None:
            reason = "no attempt recorded yet"
        else:
            reason = f"last attempt succeeded {_ago(health, now, rec.last_attempt)}"
            if rec.detail:
                reason += f" ({rec.detail})"
    elif state == health.STATE_OK:
        reason = (
            f"last attempt failed {_ago(health, now, rec.last_error_at or rec.last_attempt)} "
            f"(1 in a row; WARN from {warn:g}){error}"
        )
    else:
        reason = (
            f"{failures} attempts in a row failed (WARN from {warn:g}, CRIT from {crit:g}); "
            f"last failure {_ago(health, now, rec.last_error_at or rec.last_attempt)}{error}; "
            f"last success {_ago(health, now, rec.last_success)}"
        )
    return OutcomeEntry(
        f"{SOURCE_WORKER}_{rec.kind}", rec.dataset, state, reason, rec.destination,
        kind=rec.kind, consecutive_failures=failures,
        last_attempt=rec.last_attempt, last_success=rec.last_success,
        warn_failures=warn, crit_failures=crit,
    )


_FILE_RE = re.compile(r"(\S+\.json)")


def _problem_subject(problem: str, seen: set) -> str:
    match = _FILE_RE.search(problem)
    subject = match.group(1).rsplit("/", 1)[-1] if match else "status directory"
    unique, n = subject, 1
    while unique in seen:  # one Prometheus series per entry
        n += 1
        unique = f"{subject}#{n}"
    seen.add(unique)
    return unique


def worker_entries(
    health: Any, workers: status_view.Workers, now: float, warn_factor: float, crit_factor: float
) -> List[Any]:
    """The entries for the daemon's worker status files (see the module
    docstring): one per outcome record, one WARN per refused file; nothing at
    all when there is no data."""
    entries: List[Any] = []
    stale = workers.stale or {}
    for st in workers.statuses:
        if st.worker in stale:
            # Not current (a stopped daemon's, or a replaced worker's, leftover
            # file): never graded as if fresh -- no OK, and no CRIT for age alone.
            failing = sum(
                1 for r in st.records if max(r.consecutive_failures, 1 if r.result == "error" else 0)
            )
            entries.append(health.CheckEntry(
                SOURCE_WORKER_STATUS, st.worker, health.STATE_WARN,
                f"no recent data from {st.worker}: {stale[st.worker]}; its last outcomes "
                f"({len(st.records)} record(s), {failing} failing) are not trusted as current",
            ))
            continue
        for rec in st.records:
            if rec.kind not in health.STATUS_KINDS:
                continue  # a kind a later daemon records; this reader does not grade it
            entries.append(_outcome_entry(health, rec, now, warn_factor, crit_factor))
    seen: set = set()
    for problem in workers.problems:
        entries.append(health.CheckEntry(
            SOURCE_WORKER_STATUS, _problem_subject(problem, seen), health.STATE_WARN,
            f"worker status not read: {_one_line(problem)}",
        ))
    return entries


def gather(
    resolved: Any, now: datetime, warn_factor: float, crit_factor: float
) -> Tuple[Any, List[Any]]:
    """Everything `check` reports: -> `(health module, entries)`. Read-only."""
    from zfsbackup import health

    # Validate the factors before any ZFS or database work.
    try:
        health.evaluate([], now, warn_factor, crit_factor)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from None

    snapshot = read_snapshot(resolved)  # the schema is checked inside; errors -> 78
    config = snapshot.config
    try:
        status = status_view.collect(resolved)
    except CliError as exc:  # the pid file cannot be read at all
        status = None
        daemon = [health.CheckEntry(
            SOURCE_DAEMON, "process", health.STATE_WARN, f"cannot tell whether a daemon runs: {exc}"
        )]
    else:
        if status.schema_error is not None:
            raise status.schema_error
        daemon = daemon_entries(health, status, now.timestamp())

    from zfsbackup import health_collect

    observations = health_collect.collect(config)
    entries = list(health.evaluate(observations, now, warn_factor, crit_factor))
    entries.extend(daemon)
    cert = client_certificate_entry(health, config, resolved, now)
    if cert is not None:
        entries.append(cert)
    daemon_running = None if status is None else status.daemon is not None
    interval = getattr(config, "check_interval", None)
    workers = status_view.collect_workers(
        resolved, daemon_running=daemon_running, now=now.timestamp(),
        interval=interval.total_seconds() if interval is not None else None,
    )
    entries.extend(worker_entries(health, workers, now.timestamp(), warn_factor, crit_factor))
    return health, entries


# --------------------------------------------------------------------------
# rendering
# --------------------------------------------------------------------------


def _one_line(text: str) -> str:
    return " ".join(str(text).split())


def _label(entry: Any) -> str:
    peer = f" ({entry.peer})" if entry.peer else ""
    return _one_line(f"{entry.source}: {entry.subject}{peer}")


def render_text(health: Any, entries: List[Any]) -> str:
    lines = [f"{e.state:<4} {_label(e)}: {_one_line(e.reason)}" for e in entries]
    counts = {s: sum(1 for e in entries if e.state == s) for s in ("OK", "WARN", "CRIT")}
    lines.append(
        f"overall: {health.overall_state(entries)} "
        f"({len(entries)} checks: {counts['OK']} ok, {counts['WARN']} warn, {counts['CRIT']} crit)"
    )
    return "\n".join(lines) + "\n"


def _perf_label(entry: Any) -> str:
    peer = f"@{entry.peer}" if entry.peer else ""
    # Nagios perfdata labels are single-quoted; `'` is doubled, `=` is not allowed.
    return _one_line(f"{entry.source}:{entry.subject}{peer}").replace("'", "''").replace("=", ":")


def _num(value: Optional[float]) -> str:
    return "" if value is None else f"{value:.0f}"


def render_nagios(health: Any, entries: List[Any]) -> str:
    """One line: `ZFSBACKUP <STATE> - <summary> | <perfdata>`. The summary names
    the entries that are not OK (or counts them if all are); the perfdata is the
    age of each lag entry with its warn/crit thresholds."""
    overall = health.overall_state(entries)
    bad = [e for e in entries if e.state != health.STATE_OK]
    if bad:
        shown = "; ".join(f"{e.state} {_label(e)}: {_one_line(e.reason)}" for e in bad[:3])
        if len(bad) > 3:
            shown += f"; and {len(bad) - 3} more"
    else:
        shown = f"{len(entries)} checks OK"
    perf = [
        f"'{_perf_label(e)}'={_num(e.age_seconds)}s;{_num(e.warn_seconds)};{_num(e.crit_seconds)};0"
        for e in entries
        if getattr(e, "age_seconds", None) is not None
    ]
    perf.extend(
        f"'{_perf_label(e)}'={e.consecutive_failures};{e.warn_failures:g};{e.crit_failures:g};0"
        for e in entries
        if isinstance(e, OutcomeEntry)
    )
    line = f"{_NAGIOS_NAME} {overall} - {shown}"
    return line + (" | " + " ".join(perf) if perf else "") + "\n"


def _iso(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def render_json(health: Any, entries: List[Any], now: float, version: Optional[str]) -> str:
    """`health.to_json`, with the worker-outcome numbers added to their
    entries (additive keys; `"schema"` stays 1)."""
    document = json.loads(health.to_json(entries, now, version))
    for entry, data in zip(entries, document["entries"]):
        if isinstance(entry, OutcomeEntry):
            data["kind"] = entry.kind
            data["consecutive_failures"] = entry.consecutive_failures
            data["last_attempt"] = _iso(entry.last_attempt)
            data["last_success"] = _iso(entry.last_success)
    return json.dumps(document, indent=2, ensure_ascii=True)


_OVERALL_HELP = "# HELP zfsbackup_check_overall_state "


def render_prometheus(health: Any, entries: List[Any], now: float, version: Optional[str]) -> str:
    """`health.to_prometheus`, plus `zfsbackup_worker_consecutive_failures`
    (0 = the last attempt succeeded) with labels `kind`, `dataset` and, for a
    send, `destination`, placed before the overall-state block."""
    text = health.to_prometheus(entries, now, version)
    outcomes = [e for e in entries if isinstance(e, OutcomeEntry)]
    if not outcomes:
        return text
    lines = [
        "# HELP zfsbackup_worker_consecutive_failures Attempts in a row that failed (0 = the last "
        "one succeeded).",
        "# TYPE zfsbackup_worker_consecutive_failures gauge",
    ]
    for e in outcomes:
        labels = [("kind", e.kind), ("dataset", e.subject)]
        if e.peer is not None:
            labels.append(("destination", e.peer))
        inner = ",".join(f'{k}="{health.escape_label_value(v)}"' for k, v in labels)
        lines.append(f"zfsbackup_worker_consecutive_failures{{{inner}}} {e.consecutive_failures}")
    block = "\n".join(lines) + "\n"
    at = text.find(_OVERALL_HELP)
    return text + block if at < 0 else text[:at] + block + text[at:]


# --------------------------------------------------------------------------
# the command
# --------------------------------------------------------------------------


def _nagios_unknown(reason: str) -> str:
    return f"{_NAGIOS_NAME} UNKNOWN - {_one_line(reason)}\n"


class _CheckCommand(click.Command):
    """`check --nagios` reports usage errors as UNKNOWN (exit 3) in plugin
    form, not as Click's exit 2. Only a parse error needs this hook; errors the
    command finds itself are handled in its body."""

    def parse_args(self, ctx: click.Context, args: List[str]) -> List[str]:
        given = list(args)  # the parser consumes `args` as it goes
        try:
            return super().parse_args(ctx, args)
        except click.UsageError as exc:
            if "--nagios" not in given:
                raise
            click.echo(_nagios_unknown(f"usage: {exc.format_message()}"), nl=False)
            ctx.exit(EXIT_NAGIOS_UNKNOWN)


def register(
    cli: click.Group,
    common: Callable[[Callable], Callable],
    get_state: Callable[[Optional[str], bool], Any],
) -> None:
    """Add `check` to `cli` (`common` and `get_state` are `main.py`'s)."""

    @cli.command(
        "check",
        cls=_CheckCommand,
        help=(
            "Is this host's backup healthy? Grades, per enabled dataset, the newest "
            "snapshot, the snapshot last sent to each destination and (on a server) "
            "each received client dataset against how often they should be taken: "
            "WARN from --warn-factor times that interval, CRIT from --crit-factor "
            "times it. Also checks that a daemon is running on the stored "
            "configuration and that the client certificate is not about to expire "
            "(30 days). Once the daemon's workers have written their status files "
            "(<db>.status/), also grades each worker's last attempts: failures in a row "
            "count like missed intervals (WARN from --warn-factor, CRIT from "
            "--crit-factor); no status files is no data, never a failure. "
            "Read-only: it runs only `zfs list`/`zfs get`, takes no lock "
            "and never signals the daemon.\n\n"
            "Output: text by default; --json; --prometheus (text exposition format); "
            "--nagios (one plugin line). --output FILE writes that output to FILE "
            "atomically instead of stdout (for node_exporter's textfile collector).\n\n"
            "Exit 0 healthy, 1 any WARN or CRIT, 78 the config database is unusable. "
            "With --output FILE: 0 whenever the file was written, whatever the verdict "
            "(it is in the file); any other code means the check could not run or the "
            "write failed. With --nagios: 0 OK, 1 WARN, 2 CRIT, 3 UNKNOWN (usage errors "
            "and anything that stops the check from running are UNKNOWN)."
        ),
    )
    @click.option("--json", "as_json", is_flag=True, default=False, help="Machine-readable JSON (schema 1).")
    @click.option("--prometheus", "as_prometheus", is_flag=True, default=False,
                  help="Prometheus text exposition format.")
    @click.option("--nagios", "as_nagios", is_flag=True, default=False,
                  help="One Nagios-plugin line with perfdata; exit 0/1/2/3 (OK/WARN/CRIT/UNKNOWN).")
    @click.option("--output", "-o", "output", default=None, metavar="FILE",
                  help="Write the output to FILE (atomically) instead of stdout.")
    @click.option("--warn-factor", type=float, default=2.0, show_default=True, metavar="N",
                  help="WARN when older than N times the expected interval.")
    @click.option("--crit-factor", type=float, default=4.0, show_default=True, metavar="N",
                  help="CRIT when older than N times the expected interval.")
    @common
    def check_cmd(
        as_json: bool, as_prometheus: bool, as_nagios: bool, output: Optional[str],
        warn_factor: float, crit_factor: float, config_path: Optional[str], verbose: bool,
    ) -> None:
        state = get_state(config_path, verbose)
        state.quiet()
        ctx = click.get_current_context()
        try:
            code = _run(state, as_json, as_prometheus, as_nagios, output, warn_factor, crit_factor)
        except Exception as exc:  # noqa: BLE001 - nagios mode never fails otherwise than UNKNOWN
            if not as_nagios:
                raise
            from zfsbackup.cli.errors import format_error

            click.echo(_nagios_unknown(format_error(exc, state.resolved)[0]), nl=False)
            ctx.exit(EXIT_NAGIOS_UNKNOWN)
        ctx.exit(code)


def _run(
    state: Any, as_json: bool, as_prometheus: bool, as_nagios: bool, output: Optional[str],
    warn_factor: float, crit_factor: float,
) -> int:
    if as_json and as_prometheus:
        raise click.UsageError("--json and --prometheus are exclusive")
    if as_nagios and (as_json or as_prometheus or output):
        raise click.UsageError("--nagios is a one-line format of its own: it cannot be combined with "
                               "--json, --prometheus or --output")
    resolved = state.resolve()
    now_dt = datetime.now(timezone.utc)
    health, entries = gather(resolved, now_dt, warn_factor, crit_factor)
    from zfsbackup.version import installed_version

    overall = health.overall_state(entries)
    now = now_dt.timestamp()
    if as_nagios:
        click.echo(render_nagios(health, entries), nl=False)
        return health.STATE_VALUES[overall]  # 0 OK, 1 WARN, 2 CRIT
    if as_json:
        text = render_json(health, entries, now, installed_version()) + "\n"
    elif as_prometheus:
        text = render_prometheus(health, entries, now, installed_version())
    else:
        text = render_text(health, entries)
    if output is not None and output != "-":
        try:
            health.write_text_atomic(output, text)
        except OSError as exc:
            raise CliError(f"cannot write {output}: {exc.strerror or exc}") from exc
        return 0  # the verdict is in the file; the exit code says the check ran
    click.echo(text, nl=False)
    return 0 if overall == health.STATE_OK else 1

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
whatever happened before, unless the record carries a `warning` (a standing note
about that attempt, e.g. a recursive snapshot that left backup copies below it
out): then the entry is WARN, with the warning in the reason (never CRIT; the
next attempt replaces the warning, so a clean one clears it; a failing record
keeps its failure grading). NO DATA IS NEVER A FAILURE: a missing status
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
`last_attempt`, `last_success` and `warning` (null when none); the Prometheus form adds
`zfsbackup_worker_consecutive_failures`; `--nagios` adds a perfdata item per
record (failures with the two thresholds; the whole `--nagios` output is capped at
`--nagios-max-bytes`, default 4000, see `render_nagios`).

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
    warning: Optional[str] = None


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
    """The client certificate's verdict, or `None` when no https destination is
    configured (the certificate is then not needed; no data is no entry --
    a local `zfs:` destination has no server to present it to). Reads the
    certificate only, never a key."""
    CheckEntry = health.CheckEntry
    destinations = getattr(config, "destinations", None) or {}
    if not any(
        not getattr(d, "is_local", False)
        for d in (destinations.values() if hasattr(destinations, "values") else destinations)
    ):
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
    warning = getattr(rec, "warning", None) or None
    error = f": {rec.last_error}" if rec.last_error else ""
    if failures == 0:
        if rec.result is None and rec.last_attempt is None:
            reason = "no attempt recorded yet"
        else:
            reason = f"last attempt succeeded {_ago(health, now, rec.last_attempt)}"
            if rec.detail:
                reason += f" ({rec.detail})"
            if warning:
                # The attempt succeeded but said something is not right (e.g. a
                # recursive snapshot that left backup copies out): WARN, never CRIT,
                # and cleared by the next attempt. A failing record keeps its grading.
                state = health.STATE_WARN
                reason += f", but: {_one_line(warning)}"
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
        warning=warning if failures == 0 else None,
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


#: `--time-limit` default: seconds for the whole ZFS collection. The systemd unit
#: `packaging/systemd/zfsbackup-check.service` sets `TimeoutStartSec=150`, i.e.
#: 2 x this + 30 s, so it assumes this default.
DEFAULT_TIME_LIMIT = 60.0
#: `--per-call-timeout` default: seconds one `zfs` command may run (the same
#: value as `health_collect.DEFAULT_PER_CALL_SECONDS`).
DEFAULT_PER_CALL_TIMEOUT = 10.0


def _seconds_option(name: str) -> Callable[[click.Context, click.Parameter, Any], Optional[float]]:
    """A click callback for a seconds option: a positive finite number, or `0`
    or `none` (any case) for no limit (-> `None`). Anything else is a usage
    error (so `--nagios` reports it as UNKNOWN)."""

    def callback(ctx: click.Context, param: click.Parameter, value: Any) -> Optional[float]:
        text = str(value).strip().lower()
        if text in ("none", "off", "unlimited"):
            return None
        try:
            number = float(text)
        except ValueError:
            raise click.BadParameter(
                f"{value!r} is not a number of seconds (use 0 or none for no limit)") from None
        if number != number or number in (float("inf"), float("-inf")) or number < 0:
            raise click.BadParameter(
                f"{value!r} must be a positive number of seconds (use 0 or none for no limit)")
        return None if number == 0 else number

    callback.__name__ = f"_check_{name}"
    return callback


def gather(
    resolved: Any, now: datetime, warn_factor: float, crit_factor: float,
    time_limit: Optional[float] = DEFAULT_TIME_LIMIT,
    per_call: Optional[float] = DEFAULT_PER_CALL_TIMEOUT,
) -> Tuple[Any, List[Any]]:
    """Everything `check` reports: -> `(health module, entries)`. Read-only.
    `time_limit` bounds the whole ZFS collection and `per_call` each `zfs`
    command (seconds; `None` = unbounded); what a passed limit left unchecked is
    one WARN entry."""
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

    observations = health_collect.collect(config, deadline=time_limit, per_call=per_call)
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


#: Default for `--nagios-max-bytes`: the whole `--nagios` output (status line,
#: perfdata and the newline) stays within this many bytes. What is known:
#: Nagios Core 4 reads about 8 KiB of plugin output, and a configuration with
#: recursive datasets easily has hundreds of entries (~47 bytes of perfdata
#: each). What is assumed (from memory, not checked against each release): NRPE
#: v2 carries only about 1 KiB of output per packet, NRPE 3 and later much more,
#: and Icinga 2 has no small limit. 4000 is therefore a middle value that fits
#: Nagios Core 4 and NRPE 3+ with a margin; a site behind NRPE v2 should pass
#: `--nagios-max-bytes 1000` or so. The perfdata is capped, never the verdict.
NAGIOS_OUTPUT_LIMIT = 4000

#: The smallest accepted `--nagios-max-bytes`: room for a short status line and
#: the `omitted=N` item. Below it the status line is what gets clipped.
NAGIOS_MIN_OUTPUT_LIMIT = 64


def _nagios_perfdata(entries: List[Any], severity: Callable[[Any], int]) -> List[Tuple[int, str]]:
    """One `(severity, perfdata item)` per lag entry and per worker-outcome
    record, in the order of `entries` (the order consumers that map perfdata by
    position rely on; `render_nagios` sorts by severity only to choose which
    items to keep)."""
    items: List[Tuple[int, str]] = []
    for e in entries:
        if getattr(e, "age_seconds", None) is not None:
            items.append((
                severity(e),
                f"'{_perf_label(e)}'={_num(e.age_seconds)}s;{_num(e.warn_seconds)};{_num(e.crit_seconds)};0",
            ))
        if isinstance(e, OutcomeEntry):
            items.append((
                severity(e),
                f"'{_perf_label(e)}'={e.consecutive_failures};{e.warn_failures:g};{e.crit_failures:g};0",
            ))
    return items


def _fit_bytes(text: str, limit: int) -> str:
    """`text` cut at a character boundary to at most `limit` UTF-8 bytes (with
    an ellipsis when something was cut)."""
    if len(text.encode("utf-8")) <= limit:
        return text
    marker = "..."
    room = max(limit - len(marker), 0)
    return text.encode("utf-8")[:room].decode("utf-8", "ignore") + marker


def render_nagios(health: Any, entries: List[Any], max_bytes: int = NAGIOS_OUTPUT_LIMIT) -> str:
    """One line: `ZFSBACKUP <STATE> - <summary> | <perfdata>`. The summary names
    the first three entries that are not OK, then `and N more` (or counts the
    entries if all are OK); the perfdata is the age of each lag entry with its
    warn/crit thresholds, and the failure count of each worker-outcome record.

    The whole output is capped at `max_bytes` (default `NAGIOS_OUTPUT_LIMIT`,
    4000; `--nagios-max-bytes`), newline included, because a longer perfdata
    would be cut mid-item by the transport and mis-parsed. When everything fits,
    the perfdata is in the entries' own order, exactly as without a cap. When it
    does not, the most severe items (CRIT, then WARN, then OK; in the entries'
    order within a state) are chosen whole until the next one would not fit, and
    those are emitted in the entries' original order, followed by one last item
    `omitted=N`, N being the number of items left out. (Severity decides only
    which items are kept: consumers that map perfdata by position must see a
    stable order.) The status line and the exit code (from the overall state)
    are never affected; the status line is clipped only if it alone could not
    leave room for that item. `--json` and `--prometheus` have no size limit and
    list every entry."""
    overall = health.overall_state(entries)
    bad = [e for e in entries if e.state != health.STATE_OK]
    if bad:
        shown = "; ".join(f"{e.state} {_label(e)}: {_one_line(e.reason)}" for e in bad[:3])
        if len(bad) > 3:
            shown += f"; and {len(bad) - 3} more"
    else:
        shown = f"{len(entries)} checks OK"
    perf = _nagios_perfdata(
        entries, lambda e: health.STATE_VALUES.get(e.state, 2))
    line = f"{_NAGIOS_NAME} {overall} - {shown}"
    budget = max_bytes - 1                               # the trailing newline
    if not perf:
        return _fit_bytes(line, budget) + "\n"

    def size(text: str) -> int:
        return len(text.encode("utf-8"))

    separator = " | "
    if size(line) + size(separator) + sum(size(text) + 1 for _, text in perf) - 1 <= budget:
        return line + separator + " ".join(text for _, text in perf) + "\n"
    # Not everything fits: always leave room for the `omitted=N` item.
    reserve = size(f" omitted={len(perf)}")
    line = _fit_bytes(line, max(budget - size(separator) - reserve + 1, 0))
    used = size(line) + size(separator) + reserve - 1    # line, " | " and `omitted=N`
    kept: List[int] = []
    # Most severe first; `sorted` is stable, so the entries' order breaks ties.
    for index in sorted(range(len(perf)), key=lambda i: -perf[i][0]):
        cost = size(perf[index][1]) + 1                  # the item and the space after it
        if used + cost > budget:
            break
        kept.append(index)
        used += cost
    items = [perf[i][1] for i in sorted(kept)]           # back in the entries' own order
    items.append(f"omitted={len(perf) - len(kept)}")
    return line + separator + " ".join(items) + "\n"


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
            data["warning"] = entry.warning
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


def _check_nagios_max_bytes(ctx: click.Context, param: click.Parameter, value: int) -> int:
    if value < NAGIOS_MIN_OUTPUT_LIMIT:
        raise click.BadParameter(
            f"{value} is too small: it must be at least {NAGIOS_MIN_OUTPUT_LIMIT} bytes "
            "(room for the status line and omitted=N)")
    return value


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
            "--crit-factor); a last attempt that succeeded but recorded a warning (for "
            "example a recursive snapshot that left backup copies out) is WARN until the "
            "next attempt is clean; no status files is no data, never a failure. "
            "Read-only: it runs only `zfs list`/`zfs get`, takes no lock "
            "and never signals the daemon.\n\n"
            "Output: text by default; --json; --prometheus (text exposition format); "
            "--nagios (one plugin line, at most --nagios-max-bytes bytes in all, 4000 by "
            "default: when the perfdata does not fit, the most severe items are kept, in their "
            "usual order, and the last item is omitted=N; the verdict and the exit code are "
            "unaffected; --json and --prometheus list every entry). --output FILE writes that "
            "output to FILE atomically instead of stdout (for node_exporter's textfile "
            "collector).\n\n"
            "Time bound: the ZFS reads stop after --time-limit seconds (default 60) in all, "
            "and any one zfs command after --per-call-timeout seconds (default 10); what was "
            "not checked in time is reported as one WARN entry '(time limit)', never as OK. "
            "0 or none turns a limit off. A caller's own timeout must exceed --time-limit "
            "plus a few seconds: NRPE's -t (command_timeout) must be larger than it, or NRPE "
            "kills the check and reports a timeout instead. The systemd unit "
            "zfsbackup-check.service sets TimeoutStartSec=150, which assumes the default 60 s "
            "limit: if you raise --time-limit, raise it too (2 x the limit + 30 s).\n\n"
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
                  help="One Nagios-plugin line with perfdata; exit 0/1/2/3 (OK/WARN/CRIT/UNKNOWN). "
                       "The whole output is capped (see --nagios-max-bytes): when the perfdata "
                       "does not fit, failing entries are kept first, then OK ones, and the last "
                       "item is omitted=N; the kept items stay in their usual order.")
    @click.option("--nagios-max-bytes", "nagios_max_bytes", type=int, default=NAGIOS_OUTPUT_LIMIT,
                  show_default=True, metavar="N", callback=_check_nagios_max_bytes,
                  help="With --nagios: cap the whole output (status line, perfdata, newline) at N "
                       f"bytes; at least {NAGIOS_MIN_OUTPUT_LIMIT}. Nagios Core 4 reads about 8 KiB; "
                       "NRPE v2 carries only about 1 KiB of output per packet (use 1000 there), "
                       "NRPE 3+ much more, Icinga 2 has no small limit. The status line and the "
                       "exit code are never cut for the perfdata.")
    @click.option("--time-limit", "time_limit", default=str(int(DEFAULT_TIME_LIMIT)),
                  show_default=True, metavar="SECONDS",
                  callback=_seconds_option("time_limit"),
                  help="Stop reading ZFS after SECONDS in all; whatever was not checked in time "
                       "is one WARN entry '(time limit)'. 0 or none: no limit. A caller's timeout "
                       "(NRPE's -t, systemd's TimeoutStartSec) must exceed this: the shipped "
                       "zfsbackup-check.service assumes the default of 60.")
    @click.option("--per-call-timeout", "per_call_timeout", default=str(int(DEFAULT_PER_CALL_TIMEOUT)),
                  show_default=True, metavar="SECONDS",
                  callback=_seconds_option("per_call_timeout"),
                  help="Give up on one zfs command after SECONDS (a hung pool then costs this, "
                       "not the whole --time-limit). 0 or none: no per-command limit.")
    @click.option("--output", "-o", "output", default=None, metavar="FILE",
                  help="Write the output to FILE (atomically) instead of stdout.")
    @click.option("--warn-factor", type=float, default=2.0, show_default=True, metavar="N",
                  help="WARN when older than N times the expected interval.")
    @click.option("--crit-factor", type=float, default=4.0, show_default=True, metavar="N",
                  help="CRIT when older than N times the expected interval.")
    @common
    def check_cmd(
        as_json: bool, as_prometheus: bool, as_nagios: bool, output: Optional[str],
        warn_factor: float, crit_factor: float, nagios_max_bytes: int,
        time_limit: Optional[float], per_call_timeout: Optional[float],
        config_path: Optional[str], verbose: bool,
    ) -> None:
        state = get_state(config_path, verbose)
        state.quiet()
        ctx = click.get_current_context()
        try:
            if not as_nagios and (
                ctx.get_parameter_source("nagios_max_bytes") is not click.core.ParameterSource.DEFAULT
            ):
                raise click.UsageError("--nagios-max-bytes only applies with --nagios")
            code = _run(state, as_json, as_prometheus, as_nagios, output, warn_factor, crit_factor,
                        nagios_max_bytes, time_limit, per_call_timeout)
        except Exception as exc:  # noqa: BLE001 - nagios mode never fails otherwise than UNKNOWN
            if not as_nagios:
                raise
            from zfsbackup.cli.errors import format_error

            click.echo(_nagios_unknown(format_error(exc, state.resolved)[0]), nl=False)
            ctx.exit(EXIT_NAGIOS_UNKNOWN)
        ctx.exit(code)


def _run(
    state: Any, as_json: bool, as_prometheus: bool, as_nagios: bool, output: Optional[str],
    warn_factor: float, crit_factor: float, nagios_max_bytes: int = NAGIOS_OUTPUT_LIMIT,
    time_limit: Optional[float] = DEFAULT_TIME_LIMIT,
    per_call_timeout: Optional[float] = DEFAULT_PER_CALL_TIMEOUT,
) -> int:
    if as_json and as_prometheus:
        raise click.UsageError("--json and --prometheus are exclusive")
    if as_nagios and (as_json or as_prometheus or output):
        raise click.UsageError("--nagios is a one-line format of its own: it cannot be combined with "
                               "--json, --prometheus or --output")
    resolved = state.resolve()
    now_dt = datetime.now(timezone.utc)
    health, entries = gather(resolved, now_dt, warn_factor, crit_factor, time_limit, per_call_timeout)
    from zfsbackup.version import installed_version

    overall = health.overall_state(entries)
    now = now_dt.timestamp()
    if as_nagios:
        click.echo(render_nagios(health, entries, nagios_max_bytes), nl=False)
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

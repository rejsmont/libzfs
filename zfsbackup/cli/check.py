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
   generation = WARN) and the client certificate (expired = CRIT, expiring
   within 30 days or unusable = WARN).
4. Prints one of: text (default; one line per entry, then an overall line),
   `--json`, `--prometheus` or `--nagios`; `--output FILE` writes the text,
   JSON or Prometheus form to FILE atomically instead of stdout (node_exporter's
   textfile collector). `--nagios` stands alone (it is one line on stdout).

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

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, List, Optional, Tuple

import click

from zfsbackup import identity
from zfsbackup.cli import status as status_view
from zfsbackup.cli import tlsfiles
from zfsbackup.cli.errors import CliError
from zfsbackup.cli.write import read_snapshot

__all__ = ["register", "gather", "client_certificate_entry", "daemon_entries", "CERT_WARN_DAYS"]

#: A client certificate that expires within this many days is a WARN.
CERT_WARN_DAYS = 30

EXIT_NAGIOS_UNKNOWN = 3

#: Source names of the entries this module adds.
SOURCE_DAEMON = "daemon"
SOURCE_TLS = "tls"

_NAGIOS_NAME = "ZFSBACKUP"


# --------------------------------------------------------------------------
# this command's own entries
# --------------------------------------------------------------------------


def daemon_entries(health: Any, status: status_view.Status) -> List[Any]:
    """The daemon's presence, and whether it runs the stored configuration."""
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
        daemon = daemon_entries(health, status)

    from zfsbackup import health_collect

    observations = health_collect.collect(config)
    entries = list(health.evaluate(observations, now, warn_factor, crit_factor))
    entries.extend(daemon)
    cert = client_certificate_entry(health, config, resolved, now)
    if cert is not None:
        entries.append(cert)
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
    line = f"{_NAGIOS_NAME} {overall} - {shown}"
    return line + (" | " + " ".join(perf) if perf else "") + "\n"


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
            "(30 days). Read-only: it runs only `zfs list`/`zfs get`, takes no lock "
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
        text = health.to_json(entries, now, installed_version()) + "\n"
    elif as_prometheus:
        text = health.to_prometheus(entries, now, installed_version())
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

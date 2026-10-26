"""`zfsbackup-config status` -- is the running daemon on the stored configuration?
(plan item 19.2)

Read-only and signal-free: it reads the stored `(store_id, generation)` key
(`read_store_state`, one cheap snapshot, then a dispose) and the daemon's pid
file (`daemonlock.probe`), and compares them. It never calls `request_reload`
and sends no signal of any kind.

Verdicts (``verdict`` in the JSON, the last line in text):

=================  =====================================================
``in-sync``        the daemon's applied key is the stored key
``applying``       the daemon accepted the stored key and is switching
                   its workers over (in-flight transfers finish first)
``rejected``       the daemon refused the stored key and keeps running an
                   older one (its log says why)
``deferred``       the daemon refused the stored key for a reason that can
                   pass later with nothing written (today: the API address
                   cannot be bound) and re-checks it at every poll; NOT
                   final, unlike ``rejected``
``behind``         the daemon is on another key; it picks the stored one
                   up at its next check (a daemon that predates live
                   reload must be restarted instead)
``supervisor-gone``  the lock is held by a process the supervisor forked,
                   not by the supervisor itself
``no-daemon``      nobody holds the daemon lock
``unreadable``     the pid file is being rewritten (two reads kept
                   disagreeing): not called in sync
=================  =====================================================

JSON shape (stable; keys may be added, none renamed or removed)::

    {
      "database": str,
      "stored":   {"store_id": int, "generation": int},
      "daemon":   null | {
        "pid": int | null,
        "reload_capable": bool,         # false = predates live reload
        "supervisor_alive": bool | null,
        "generation": int | null,       # the pid file's line 2
        "applied":  {"store_id": int, "generation": int} | null,
        "target":   {...} | null,       # only while a reload is being applied
        "rejected": {...} | null,
        "deferred": {...} | null,       # retryable refusal; re-checked each poll
        "deferred_reason": str | null,  # one line, as the daemon published it
        "draining": int,                # ONLY while the daemon drains for an upgrade:
        "draining_reason": str | null   #   unix time it began, and why (S-8)
      },
      "schema_error": str | null,       # the schema problem behind exit 78
      "verdict": str,                   # one of the above
      "message": str                    # the text form's verdict line
    }

Worker outcomes (S-7b). When the daemon's workers have written their status
files (`<db>.status/<worker>.json`, read through the stdlib `zfsbackup.health`
reader), the text form gets a ``workers:`` row per worker before the final
``status:`` line, and ``--json`` gets one more key, ``"workers"``::

    "workers": {
      "directory": str,
      "problems": [str],                # refused or unreadable files, one line each
      "files": [{
        "worker": str, "pid": int | null,
        "started_at": str | null, "updated_at": str | null,   # UTC, ...Z
        "stale": bool,                  # not current: see below
        "stale_reason": str | null,
        "records": int,                 # outcomes the worker tracks
        "failing": int,                 # records whose last attempt(s) failed
        "worst": null | {"kind", "dataset", "destination", "consecutive_failures",
                         "last_error", "last_error_at"}
      }]
    }

A file is **stale** when it cannot be taken as the workers' current state: no
daemon is running (a stopped daemon's last files stay on disk for ever, as
do those of a worker a live-to-dry-run reload replaced), the writing process
is gone, or it was not updated for longer than `max(4 x check_interval, 24 h)`
(`check` knows the interval; `status` reads no configuration and uses 24 h). The age
is only a backstop: the two liveness criteria carry the signal, and a worker may go
hours without writing during one long transfer. A
stale file is shown as such and its outcomes are never reported as current.

The key and the rows are absent when there is no status directory at all (an
older daemon, or nothing written yet): no data is not a failure. Reading them
never changes the verdict or an exit code.

Exit codes of the command: 0; 1 when the daemon's lock file cannot be read at
all (`probe` raised `OSError`); 78 when the database is unusable, including a
schema that is out of date or does not match (the daemon state is printed
first, then the error). With `--wait SECONDS`: 0 once the daemon is in sync
(or when no daemon is running -- there is nothing to wait for), 1 if the
daemon rejected the stored key or can never catch up (a daemon that predates
live reload and is behind, or a supervisor that is gone), 75 if the wait ran
out (including while the daemon is still `deferred`: that is never exit 1).
"""

import json
import os
import stat as stat_module
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from zfsbackup import daemonlock
from zfsbackup.cli.errors import CliError, ConfigUnusable
from zfsbackup.config.store import (
    ResolvedConfigPath,
    SchemaError,
    StoreState,
    check_config_schema,
    dispose_all,
    read_store_state,
)

__all__ = [
    "IN_SYNC",
    "APPLYING",
    "REJECTED",
    "DEFERRED",
    "BEHIND",
    "SUPERVISOR_GONE",
    "NO_DAEMON",
    "UNREADABLE",
    "Status",
    "WAIT_POLL_SECONDS",
    "Workers",
    "collect",
    "collect_workers",
    "stale_after",
    "stale_reason",
    "trusted_status_uids",
    "render_json",
    "render_text",
    "wait_exit_code",
    "wait_for",
]

IN_SYNC = "in-sync"
APPLYING = "applying"
REJECTED = "rejected"
DEFERRED = "deferred"
BEHIND = "behind"
SUPERVISOR_GONE = "supervisor-gone"
NO_DAEMON = "no-daemon"
UNREADABLE = "unreadable"

#: How often `--wait` re-reads the stored key and the pid file.
WAIT_POLL_SECONDS = 0.2
#: A torn pid-file read is retried this many times (one `collect`) ...
STABLE_TRIES = 3
#: ... this long apart.
STABLE_RETRY_SECONDS = 0.05
_SEE_LOG = "if it does not, see the daemon log"

Key = Tuple[int, int]


@dataclass(frozen=True)
class Status:
    database: str
    stored: StoreState
    daemon: Optional[daemonlock.DaemonInfo]
    verdict: str
    message: str
    #: The `SchemaError` that makes the database unusable (exit 78), or None.
    schema_error: Optional[BaseException] = None

    @property
    def will_not_catch_up(self) -> bool:
        """Waiting cannot help: the supervisor is gone, or the daemon predates
        live reload and is behind (it must be restarted)."""
        if self.verdict == SUPERVISOR_GONE:
            return True
        return (
            self.verdict == BEHIND
            and self.daemon is not None
            and not self.daemon.reload_capable
        )


def _key_text(key: Key) -> str:
    return f"store {key[0]}, generation {key[1]}"


def _reaches(found: Optional[Key], key: Key) -> bool:
    """Is `found` the same store as `key` at the same or a later generation?"""
    return found is not None and found[0] == key[0] and found[1] >= key[1]


def _verdict(stored: StoreState, info: Optional[daemonlock.DaemonInfo]) -> Tuple[str, str]:
    key: Key = (stored.store_id, stored.generation)
    if info is None:
        return NO_DAEMON, "no running daemon"
    if not info.stable:
        return UNREADABLE, "pid file unreadable right now (being rewritten)"
    if info.supervisor_state == "gone":
        return SUPERVISOR_GONE, (
            f"daemon supervisor pid {info.pid} is gone, but a process it forked "
            "still holds the daemon lock"
        )
    if not info.reload_capable:
        # A legacy daemon records only its generation (line 2), never a store.
        if info.generation == stored.generation:
            return IN_SYNC, "in sync"
        on = f"generation {info.generation}" if info.generation is not None else "an unknown generation"
        return BEHIND, (
            f"daemon on {on}, stored {stored.generation} "
            "(it predates live reload; restart it to apply this)"
        )
    # Same rules as the CLI's acknowledgement wait: within the SAME store id,
    # a generation at or past the stored one counts; across store ids nothing
    # is ordered.
    if _reaches(info.applied, key):
        return IN_SYNC, "in sync"
    if _reaches(info.target, key):
        return APPLYING, f"applying generation {stored.generation}"
    if info.rejected == key:
        return REJECTED, f"daemon rejected generation {stored.generation}"
    if info.draining is not None:
        # S-8: the daemon reloads nothing while it drains for an upgrade and
        # exits 76; the restarted daemon loads the stored key. Not a re-check.
        msg = (
            f"daemon is draining for an upgrade and does not reload: generation "
            f"{stored.generation} will be applied after the restart, not before"
        )
        return (DEFERRED if info.deferred == key else BEHIND), msg
    if info.deferred == key:
        # Retryable: not final, and never the rejection verdict.
        return DEFERRED, (
            f"daemon has not applied generation {stored.generation} yet: "
            f"{info.deferred_reason or 'see the daemon log'}; it re-checks every poll "
            f"({daemonlock.RELOAD_POLL_SECONDS} s)"
        )
    applied = info.applied
    if applied is not None and applied[0] != key[0]:
        on = _key_text(applied)
        replaced = "the database file was replaced; " if applied[0] != 0 else ""
        return BEHIND, (
            f"daemon on {on}, stored {_key_text(key)} "
            f"({replaced}it will pick it up; {_SEE_LOG})"
        )
    running = applied[1] if applied is not None else info.generation
    on = f"generation {running}" if running is not None else "an unknown generation"
    return BEHIND, (
        f"daemon on {on}, stored {stored.generation} (it will pick it up; {_SEE_LOG})"
    )


def collect(
    resolved: ResolvedConfigPath, *, sleep: Optional[Callable[[float], None]] = None
) -> Status:
    """One reading of the stored key and the daemon's pid file. Leaves no
    database handle open. Never signals.

    The schema is checked too (`read_store_state` does not): a `SchemaError` is
    NOT raised here but returned in `Status.schema_error`, so the caller can
    print the daemon state first and then fail with exit 78. A torn pid-file
    read is retried (`STABLE_TRIES` times); if it stays torn the verdict is
    `unreadable`.

    Raises the store's errors for an unusable database (`errors.py` maps them
    to 78) and `CliError` if the pid file cannot be read at all."""
    schema_error: Optional[BaseException] = None
    try:
        try:
            check_config_schema(resolved)
        except SchemaError as exc:
            schema_error = exc
    finally:
        dispose_all()
    try:
        try:
            stored = read_store_state(resolved)
        except ValueError as exc:
            # An initialised-looking file with no settings row.
            raise ConfigUnusable(f"the stored configuration cannot be read: {exc}") from exc
        except Exception:
            if schema_error is not None:
                raise schema_error from None  # the real cause
            raise
    finally:
        dispose_all()
    info = None
    for attempt in range(STABLE_TRIES):
        try:
            info = daemonlock.probe(resolved)
        except OSError as exc:  # DaemonLockError is an OSError
            raise CliError(
                f"could not check whether a daemon is running for "
                f"{resolved.resolved_path}: {exc.strerror or exc}"
            ) from exc
        if info is None or info.stable:
            break
        if attempt + 1 < STABLE_TRIES:
            (sleep or time.sleep)(STABLE_RETRY_SECONDS)
    verdict, message = _verdict(stored, info)
    return Status(str(resolved.resolved_path), stored, info, verdict, message, schema_error)


# --------------------------------------------------------------------------
# worker status files (S-7b)
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Workers:
    """The worker status files, as read: `present` is False when there is no
    `<db>.status/` directory ("no data"); `statuses` are the parsed files
    (`health.WorkerStatus`); `problems` are refused or unreadable files."""

    directory: str
    present: bool
    statuses: tuple = ()
    problems: tuple = ()
    #: worker name -> why its file is not current (see `stale_reason`).
    stale: Optional[Dict[str, str]] = None

    @property
    def has_data(self) -> bool:
        return self.present or bool(self.problems)


def trusted_status_uids(resolved: ResolvedConfigPath) -> Set[int]:
    """The uids whose ownership of `<db>.status/` and its files is accepted.

    The reader's own default is root, this process's euid and the DB file's
    owner. The daemon can run as another user than the DB's owner (the
    documented layout is a DB owned by root or an operator, group `zfsbackup`,
    and the daemon as a service user), and its status files are then owned by
    that user. So this adds ONE more: the owner of the daemon's pid file
    (`<db>.daemon.pid`), which the daemon itself creates -- it is the same owner
    `daemonlock.request_reload` accepts as "the target's uid". The pid file is
    taken only if it is a regular file (never a symlink); it stays after the
    daemon exits, so a stopped daemon's last status stays readable. Nothing the
    status files say can then do more than an owner of the DB directory could
    already do, and the reader still refuses group- or world-writable files and
    directories and symlinks."""
    uids: Set[int] = {0, os.geteuid()}
    try:
        uids.add(os.stat(resolved.resolved_path).st_uid)
    except OSError:
        pass
    try:
        st = os.lstat(daemonlock.lock_path(resolved))
    except OSError:
        pass
    else:
        if stat_module.S_ISREG(st.st_mode):
            uids.add(st.st_uid)
    return uids


#: A status file older than this is stale whatever the configuration says.
STALE_FLOOR_SECONDS = 24 * 3600.0
#: ... and, when the check interval is known, older than this many of them.
STALE_INTERVALS = 4


def stale_after(interval: Optional[float]) -> float:
    """Seconds without an update after which a status file is stale: `max(4 x
    check_interval, 24 h)`. Only a backstop: a worker writes between datasets and
    once per cycle, so one long transfer can leave the file untouched for hours;
    "no daemon" and "writer pid gone" are the real signals."""
    return max(STALE_INTERVALS * interval, STALE_FLOOR_SECONDS) if interval else STALE_FLOOR_SECONDS


def stale_reason(
    st: Any, now: float, *, daemon_running: Optional[bool] = None,
    interval: Optional[float] = None,
) -> Optional[str]:
    """Why `st` (a `health.WorkerStatus`) cannot be taken as current, or
    `None` when it can. A file is left behind by a stopped daemon and by a
    worker that a reload replaced (for example live -> `--dry-run`, which
    writes nothing), and is then never updated again; its last outcomes would
    otherwise read as fresh. `daemon_running=None` = not known."""
    from zfsbackup import health

    if daemon_running is False:
        return "no daemon is running, so this is the last state of a stopped one"
    if st.pid is not None and daemonlock.pid_is_alive(st.pid) is False:
        return f"its writer (pid {st.pid}) is gone"
    if st.updated_at is None:
        return "it records no update time"
    age = now - st.updated_at
    bound = stale_after(interval)
    if age > bound:
        return (
            f"not updated for {health.format_duration(age)} "
            f"(more than {health.format_duration(bound)})"
        )
    return None


def collect_workers(
    resolved: ResolvedConfigPath, *, daemon_running: Optional[bool] = None,
    interval: Optional[float] = None, now: Optional[float] = None,
) -> Workers:
    """Read every worker status file for `resolved`. Read-only and tolerant: a
    missing directory is `present=False`, a refused file is a `problems` line.
    Never raises for what it finds. Files that are not current are named in
    `Workers.stale` (`stale_reason`)."""
    from zfsbackup import health

    directory = health.status_dir(resolved.resolved_path)
    try:
        result = health.read_status_dir(directory, trusted_uids=trusted_status_uids(resolved))
    except Exception as exc:  # noqa: BLE001 - the reader is documented not to raise
        return Workers(str(directory), True, (), (f"{directory}: cannot be read: {exc}",))
    moment = time.time() if now is None else now
    stale = {}
    for st in result.statuses:
        reason = stale_reason(st, moment, daemon_running=daemon_running, interval=interval)
        if reason is not None:
            stale[st.worker] = reason
    return Workers(
        str(directory), result.present, tuple(result.statuses), tuple(result.problems), stale
    )


def _iso(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _failures(record: Any) -> int:
    """Attempts in a row that failed (an `error` result counts at least 1)."""
    return max(record.consecutive_failures, 1 if record.result == "error" else 0)


def worst_record(status: Any) -> Optional[Any]:
    """The record with the most consecutive failures (ties: the latest error),
    or `None` when none is failing."""
    failing = [r for r in status.records if _failures(r) >= 1]
    if not failing:
        return None
    return max(failing, key=lambda r: (_failures(r), r.last_error_at or 0.0))


def _subject(record: Any) -> str:
    dest = f" -> {record.destination}" if record.destination else ""
    return f"{record.kind} {record.dataset}{dest}"


def _worker_json(status: Any, stale: Optional[str] = None) -> Dict[str, Any]:
    worst = worst_record(status)
    return {
        "worker": status.worker,
        "stale": stale is not None,
        "stale_reason": stale,
        "pid": status.pid,
        "started_at": _iso(status.started_at),
        "updated_at": _iso(status.updated_at),
        "records": len(status.records),
        "failing": sum(1 for r in status.records if _failures(r) >= 1),
        "worst": None if worst is None else {
            "kind": worst.kind,
            "dataset": worst.dataset,
            "destination": worst.destination,
            "consecutive_failures": _failures(worst),
            "last_error": worst.last_error,
            "last_error_at": _iso(worst.last_error_at),
        },
    }


def workers_json(workers: Workers) -> Dict[str, Any]:
    return {
        "directory": workers.directory,
        "problems": list(workers.problems),
        "files": [_worker_json(s, (workers.stale or {}).get(s.worker)) for s in workers.statuses],
    }


def _ago(ts: Optional[float], now: float) -> str:
    if ts is None:
        return "never"
    seconds = max(int(now - ts), 0)
    if seconds < 90:
        return f"{seconds}s ago"
    if seconds < 5400:
        return f"{seconds // 60}m ago"
    if seconds < 172800:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"


def _worker_lines(workers: Workers, now: float) -> List[Tuple[str, str]]:
    """`(row name, value)` pairs for the text form; `("", ...)` continues the
    previous row."""
    rows: List[Tuple[str, str]] = []
    for st in workers.statuses:
        failing = sum(1 for r in st.records if _failures(r) >= 1)
        updated = "-" if st.updated_at is None else f"{_iso(st.updated_at)} ({_ago(st.updated_at, now)})"
        count = f"{len(st.records)} record{'s' if len(st.records) != 1 else ''}"
        stale = (workers.stale or {}).get(st.worker)
        rows.append(("workers" if not rows else "", (
            f"{st.worker}: " + ("STALE (no recent data) " if stale else "")
            + f"updated {updated}, {count}, "
            + (f"{failing} failing" if failing else "none failing")
        )))
        if stale:
            rows.append(("", f"  stale: {stale}"))
        worst = worst_record(st)
        if worst is not None:
            error = f": {worst.last_error}" if worst.last_error else ""
            label = "last known worst" if stale else "worst"
            rows.append(("", (
                f"  {label}: {_subject(worst)}, {_failures(worst)} failure(s) in a row{error}"
            )))
    for problem in workers.problems:
        rows.append(("workers" if not rows else "", f"problem: {' '.join(problem.split())}"))
    return rows


def _pair_json(pair: Optional[Key]) -> Optional[Dict[str, int]]:
    return None if pair is None else {"store_id": pair[0], "generation": pair[1]}


def render_json(status: Status, workers: Optional[Workers] = None) -> str:
    info = status.daemon
    daemon: Optional[Dict[str, Any]] = None
    if info is not None:
        daemon = {
            "pid": info.pid,
            "reload_capable": info.reload_capable,
            "supervisor_alive": {"alive": True, "gone": False}.get(info.supervisor_state),
            "generation": info.generation,
            "applied": _pair_json(info.applied),
            "target": _pair_json(info.target),
            "rejected": _pair_json(info.rejected),
            "deferred": _pair_json(info.deferred),
            "deferred_reason": info.deferred_reason,
        }
        if info.draining is not None:  # additive, and only while it is true
            daemon["draining"] = info.draining
            daemon["draining_reason"] = info.draining_reason
    document: Dict[str, Any] = {
        "database": status.database,
        "stored": {
            "store_id": status.stored.store_id,
            "generation": status.stored.generation,
        },
        "daemon": daemon,
        "schema_error": None if status.schema_error is None else str(status.schema_error),
        "verdict": status.verdict,
        "message": status.message,
    }
    if workers is not None and workers.has_data:
        document["workers"] = workers_json(workers)
    return json.dumps(document, indent=2) + "\n"


def render_text(status: Status, workers: Optional[Workers] = None, now: Optional[float] = None) -> str:
    info = status.daemon
    rows = [
        ("database", status.database),
        ("stored", _key_text((status.stored.store_id, status.stored.generation))),
    ]
    if info is None:
        rows.append(("daemon", "not running"))
    else:
        pid = f"pid {info.pid}" if info.pid is not None else "pid unknown"
        rows.append(("daemon", f"{pid} (reload-capable: {'yes' if info.reload_capable else 'no'})"))
        if info.reload_capable:
            for name, pair in (("applied", info.applied), ("target", info.target), ("rejected", info.rejected)):
                rows.append((name, "-" if pair is None else _key_text(pair)))
            if info.deferred is not None:
                rows.append(("deferred", _key_text(info.deferred)))
                if info.deferred_reason:
                    rows.append(("reason", info.deferred_reason))
        else:
            gen = "unknown" if info.generation is None else f"generation {info.generation}"
            rows.append(("applied", gen))
    if info is not None and info.draining is not None:
        since = _iso(info.draining)
        reason = f": {info.draining_reason}" if info.draining_reason else ""
        rows.append((
            "draining",
            f"since {since} ({_ago(info.draining, time.time() if now is None else now)}){reason}",
        ))
    if status.schema_error is not None:
        rows.append(("schema", str(status.schema_error).strip().splitlines()[0]))
    if workers is not None and workers.has_data:
        rows.extend(_worker_lines(workers, time.time() if now is None else now))
    rows.append(("status", status.message))
    return "".join(
        (f"{name + ':':<10}" if name else " " * 10) + f"{value}\n" for name, value in rows
    )


def wait_for(
    resolved: ResolvedConfigPath,
    seconds: float,
    *,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> Status:
    """Re-read until the daemon is in sync, has rejected the stored key, no
    daemon is running, or `seconds` pass; -> the last `Status`. A `deferred`
    daemon is waited on like `applying` (the next poll may resolve it). The stored key is
    re-read every time (a write may land meanwhile). Reads only; no lock is held
    between readings."""
    deadline = clock() + seconds
    while True:
        status = collect(resolved)
        if (
            status.schema_error is not None
            or status.verdict in (IN_SYNC, REJECTED, NO_DAEMON)
            or status.will_not_catch_up
        ):
            return status
        remaining = deadline - clock()
        if remaining <= 0:
            return status
        sleep(min(WAIT_POLL_SECONDS, remaining))


def wait_exit_code(status: Status) -> int:
    """Exit code of `status --wait`: 0 in sync or no daemon, 1 rejected or
    never going to catch up (`will_not_catch_up`), 75 anything else (the wait
    ran out, including a daemon still `deferred`). A schema error is the caller's 78."""
    if status.verdict in (IN_SYNC, NO_DAEMON):
        return 0
    if status.verdict == REJECTED or status.will_not_catch_up:
        return 1
    return 75

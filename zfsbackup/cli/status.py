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
        "rejected": {...} | null
      },
      "schema_error": str | null,       # the schema problem behind exit 78
      "verdict": str,                   # one of the above
      "message": str                    # the text form's verdict line
    }

Exit codes of the command: 0; 1 when the daemon's lock file cannot be read at
all (`probe` raised `OSError`); 78 when the database is unusable, including a
schema that is out of date or does not match (the daemon state is printed
first, then the error). With `--wait SECONDS`: 0 once the daemon is in sync
(or when no daemon is running -- there is nothing to wait for), 1 if the
daemon rejected the stored key or can never catch up (a daemon that predates
live reload and is behind, or a supervisor that is gone), 75 if the wait ran
out.
"""

import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

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
    "BEHIND",
    "SUPERVISOR_GONE",
    "NO_DAEMON",
    "UNREADABLE",
    "Status",
    "WAIT_POLL_SECONDS",
    "collect",
    "render_json",
    "render_text",
    "wait_exit_code",
    "wait_for",
]

IN_SYNC = "in-sync"
APPLYING = "applying"
REJECTED = "rejected"
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


def _pair_json(pair: Optional[Key]) -> Optional[Dict[str, int]]:
    return None if pair is None else {"store_id": pair[0], "generation": pair[1]}


def render_json(status: Status) -> str:
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
        }
    return json.dumps(
        {
            "database": status.database,
            "stored": {
                "store_id": status.stored.store_id,
                "generation": status.stored.generation,
            },
            "daemon": daemon,
            "schema_error": None if status.schema_error is None else str(status.schema_error),
            "verdict": status.verdict,
            "message": status.message,
        },
        indent=2,
    ) + "\n"


def render_text(status: Status) -> str:
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
        else:
            gen = "unknown" if info.generation is None else f"generation {info.generation}"
            rows.append(("applied", gen))
    if status.schema_error is not None:
        rows.append(("schema", str(status.schema_error).strip().splitlines()[0]))
    rows.append(("status", status.message))
    return "".join(f"{name + ':':<10}{value}\n" for name, value in rows)


def wait_for(
    resolved: ResolvedConfigPath,
    seconds: float,
    *,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> Status:
    """Re-read until the daemon is in sync, has rejected the stored key, no
    daemon is running, or `seconds` pass; -> the last `Status`. The stored key is
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
    ran out). A schema error is the caller's 78."""
    if status.verdict in (IN_SYNC, NO_DAEMON):
        return 0
    if status.verdict == REJECTED or status.will_not_catch_up:
        return 1
    return 75

"""Daemon presence lock: "is a zfsbackup daemon running against this database?"

One file, `<db path>.daemon.pid`, next to the config database, and one kernel
`flock(2)` on it. **"Running" means "the exclusive lock is held" -- never "the
pid in the file exists".** A pid is only ever *reported*, for the operator; it
is never used to decide anything, because pids are reused and the file survives
a `kill -9`. A stale file (daemon gone, file left behind) is therefore
harmless: nobody holds the lock, so `probe()` says "not running" and the next
`acquire()` simply takes the lock and rewrites the content.

Two consumers:

* the daemon's supervisor calls `acquire()` once, BEFORE it reads the config
  and so before the first fork. Taking it first closes a startup window: were
  the config read first, a `zfsbackup-config` write plus `probe()` landing
  between the read and the lock would report "no running daemon" while a
  daemon started on the old generation. A second daemon on the same database
  fails with `DaemonAlreadyRunning` (naming the pid) and exits 1, so two
  supervisors can never double-snapshot/prune one database. The lock is held for the life of
  the process; `release()` removes the file on a *clean* exit only. After a
  crash or `kill -9` the kernel drops the lock and the (stale) file stays.
* the `zfsbackup-config` CLI calls `probe()` after a write, to tell the operator
  whether a daemon is running and on which generation, and `request_reload()`
  to ask it to apply the write now. **The CLI signals, after verifying**: a
  `SIGHUP` goes out only to a process proven to be the lock holder (see
  `request_reload`), never to a process group, never anything but `SIGHUP`.
  The file is `0660` in a `2770` directory, so any group operator can write it;
  its content is untrusted, and a pid named in it is never signalled unless the
  kernel's lock table says that pid holds the exclusive lock on this very
  inode. Where that cannot be checked (macOS), nothing is signalled and the
  daemon's generation poll (`RELOAD_POLL_SECONDS`) picks the change up.

File format (ASCII; "pid file v2")::

    <pid>\\n
    <generation>\\n        # empty line when the generation is not known
    start=<token>\\n      # procident.start_token of the supervisor; may be empty
    applied=<store>:<gen>\\n
    target=<store>:<gen>\\n    # only while a reload is being applied
    rejected=<store>:<gen>\\n    # final: this key will not be retried
    deferred=<store>:<gen>\\n    # refused for now, re-checked every poll
    deferred_reason=<text>\\n    # one line, printable ASCII, at most 200 chars

`rejected=` is final (the poll never retries that key; only a new write or a
SIGHUP does). `deferred=` is the other kind of refusal: the stored
configuration is fine but cannot be applied *yet* (its API address is busy),
and the daemon re-checks it at every poll, applying it as soon as it passes.
The two are never published for the same key. `deferred` is cleared when the
key is accepted (it becomes `target=`/`applied=`), when a newer key supersedes
it, or when it turns into a `rejected=`. Older readers ignore both new keys.

Lines 1-2 are the original format and are unchanged, so a reader that parses
only two lines keeps working. From line 3 on, `key=value` lines; unknown keys
and malformed values are ignored. `start=` is always written by a v2 daemon and
its *presence* means "supports live reload" (a legacy daemon has no such line;
`DaemonInfo.reload_capable`). An empty `start=` means the platform has no start
token, so the daemon cannot be signalled safely.

`pid` is the supervisor's. `generation` is the config generation the daemon
loaded (`DaemonLock.set_generation` / `set_state` rewrite it in place once the
config has been read, so it is empty for the instant between `acquire()` and the
load, and a reader must treat an empty generation as "not known yet", not as
"old"). Readers tolerate an empty or truncated file: the file is written with
`pwrite` + `ftruncate` (never truncate-then-write), so a concurrent `probe()`
never sees an empty file once the first write has happened, but it can still see
one in the instant between `acquire()` taking the lock and writing --
`DaemonInfo.pid` is then `None` while the daemon is still correctly reported as
running. Because the rewrite is not atomic across the `pwrite`/`ftruncate`
pair, readers that act on the content (`request_reload`) use `_read_stable`:
two consecutive byte-identical reads.

Permissions mirror the store's (`zfsbackup.config.store.paths`): the file is
created `0660` in the setgid `2770` config directory, so group operators can
open it read-only -- which is all `probe()` needs, because `flock` takes no
particular open mode. The mode is fixed with `fchmod` only when this process
owns the file (euid 0 or the file's uid) and the mode actually differs; a file
we do not own is never chmod'd. When root creates the pidfile next to the
*default* database, it is `fchown`ed to the `zfsbackup` group, like the
database itself; elsewhere the setgid directory supplies the group.

Start methods, and what the lock does and does not cover. CPython's default
start method is `spawn` on macOS (3.8+), `fork` on Linux up to 3.13 and
`forkserver` on Linux from 3.14; the daemon uses the default, so all three
occur in practice.

* `fork`: workers inherit the descriptor, and a `flock` belongs to the open
  file *description*, which fork shares -- so the lock stays held until the
  **last** holder exits. Workers must not unlink the file (`release()` is a
  no-op in any process but the one that acquired).
* `spawn` / `forkserver`: workers do **not** inherit the descriptor (it is
  `O_CLOEXEC`, and `multiprocessing` passes only explicit fds; the forkserver
  process does not get it either), so the lock belongs to the supervisor alone.

Orphans. A `kill -9`'d supervisor used to leave its workers running forever:
under `fork` holding the lock on a frozen config, under `spawn`/`forkserver`
holding nothing, so `probe()` said "not running" and a second daemon could
start beside them. Workers now notice the death themselves
(`zfsbackup.workers._SupervisorWatch`) and exit at their next safe point --
within about one `stopflag.STOP_POLL` slice (0.5s) when idle, or after the
dataset they are in the middle of (a `zfs send` is never torn). So an orphan
is a short transient, not a standing state, and the lock covers it as follows:

* `fork`: during that window the orphans still hold the lock, so `probe()`
  still reports a daemon (with a dead supervisor pid, see
  `stale_supervisor_message`) and a second daemon is refused until the last
  orphan has exited -- exactly the guarantee wanted, no overlap possible.
* `spawn` / `forkserver`: the orphans never held it, so for that same window
  `probe()` says "not running" and a new daemon *can* start beside a worker
  that is finishing its last dataset. This is a bounded overlap of one
  in-flight dataset operation, not the unbounded double-daemon of before; the
  lock cannot close it without the workers holding the descriptor (which
  would need `pass_fds`, i.e. leaking it into every `spawn`ed worker's
  lifetime), and the orphan exits as soon as it reaches a safe point.

`<db>.workers.lock` (never deleted, `0660`, like `.writer.lock`) is the
*workers'* presence lock: each worker takes `LOCK_SH` on its own descriptor
(`hold_workers_lock`), and `workers_present()` asks "is anyone holding it?" with a
momentary `LOCK_EX|LOCK_NB`. Unlike the daemon lock it is meant to be held by the
workers themselves, so that a supervisor that died leaves a visible trace of
its surviving workers under **every** start method:

* `fork`: the descriptor is opened by the worker after the fork, `O_CLOEXEC`,
  so no supervisor descriptor is inherited and a worker's lock dies with the
  worker.
* `spawn` / `forkserver`: the same -- the worker opens its own descriptor by
  path in its own process; nothing is passed through `pass_fds`.

A worker's exit on supervisor death is logged at ERROR, once ("supervisor N
is gone; exiting"). The descriptor is `O_CLOEXEC` in every case so the `zfs`
children the workers `exec` never inherit it -- a long-lived `zfs send` must
not keep a dead daemon's lock alive.

**Not supported on NFS** (or other network filesystems): `flock` semantics
there are emulated or absent, and the SQLite WAL config database is itself
unsupported on NFS. Keep the config directory on a local filesystem.
"""

import errno
import fcntl
import logging
import os
import random
import signal
import stat as stat_module
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple, Optional, Tuple

from zfsbackup import procident

from zfsbackup.config.store.paths import (
    CONFIG_DB_MODE,
    CONFIG_OWNER,
    is_default_config_path,
)

logger = logging.getLogger(__name__)

#: Appended to the database path (`config.db` -> `config.db.daemon.pid`).
LOCK_SUFFIX = ".daemon.pid"

#: Bounded retry for the unlink race in `acquire` (see there).
_ACQUIRE_ATTEMPTS = 5
#: How long `acquire` keeps retrying a contended `flock` before concluding a
#: daemon really holds it. `probe()` (run by the CLI after every write) takes
#: `LOCK_SH` for microseconds, and a `LOCK_EX|LOCK_NB` that lands in that
#: instant fails exactly as it would against a daemon. A real daemon holds
#: `LOCK_EX` for life, so waiting this long costs a refused second daemon one
#: second and saves a starting daemon from a spurious refusal naming a stale pid.
_CONTENTION_WINDOW = 1.0
_CONTENTION_POLL = 0.025

#: Appended to the database path for the workers' presence lock
#: (`config.db` -> `config.db.workers.lock`).
WORKERS_LOCK_SUFFIX = ".workers.lock"

#: How often the supervisor polls the store's `(store_id, generation)`. Shared
#: so the CLI's wording ("applied within N seconds") cannot drift from it.
RELOAD_POLL_SECONDS = 10

#: `_read_stable`: re-reads before giving up on a torn read.
_STABLE_READS = 3

_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
#: A FIFO planted in the (group-writable) config directory must not make an
#: `open(O_RDONLY)` block forever. `flock` and `pread` on a regular file are
#: unaffected by it, and every opener checks `S_ISREG` afterwards.
_O_NONBLOCK = getattr(os, "O_NONBLOCK", 0)
_RDONLY_FLAGS = os.O_RDONLY | _O_CLOEXEC | _O_NOFOLLOW | _O_NONBLOCK


class DaemonLockError(OSError):
    """The lock file could not be opened/created/written (permissions, a
    non-regular file, a symlink, a read-only filesystem...). Not the same as
    "another daemon holds it" -- that is `DaemonAlreadyRunning`."""


class DaemonAlreadyRunning(Exception):
    """Another process holds the daemon lock for this database.

    `pid`/`generation` come from the lock file and are informational only;
    either may be `None` (empty file in the instant after the other daemon
    took the lock, or generation not yet known).
    """

    def __init__(
        self,
        path: Path,
        pid: Optional[int],
        generation: Optional[int] = None,
        start: Optional[str] = None,
    ):
        self.path = path
        self.pid = pid
        self.generation = generation
        #: The holder's recorded start token (`procident.identity` input), or
        #: `None` for a legacy daemon / an unreadable file.
        self.start = start
        if self.pid_alive is False:
            super().__init__(stale_supervisor_message(path, pid))
            return
        who = f"pid {pid}" if pid is not None else "an unknown pid"
        super().__init__(
            f"another zfsbackup daemon ({who}) is already running against this "
            f"configuration database (lock held on {path})"
        )

    @property
    def pid_alive(self) -> Optional[bool]:
        """See `pid_is_alive`. Evaluated at call time, not at construction."""
        return pid_is_alive(self.pid)


@dataclass(frozen=True)
class DaemonInfo:
    """What `probe()` found: a daemon holds the lock. `pid` is informational
    (see module docstring); `generation` is the config generation it loaded,
    or `None` when it has not recorded one."""

    pid: Optional[int]
    generation: Optional[int]
    # The v2 fields. `compare=False`: two infos about the same daemon are equal
    # when pid and generation are, so code (and tests) written against the
    # two-line format keep working unchanged.
    #: Supervisor start token; `None` = the key is absent (a legacy daemon).
    start: Optional[str] = field(default=None, compare=False)
    #: `(store_id, generation)` pairs, `None` when absent/malformed.
    applied: Optional[Tuple[int, int]] = field(default=None, compare=False)
    target: Optional[Tuple[int, int]] = field(default=None, compare=False)
    rejected: Optional[Tuple[int, int]] = field(default=None, compare=False)
    #: A refusal that is re-checked at every poll (not final, unlike
    #: `rejected`), and the daemon's one-line reason (`None` when absent).
    deferred: Optional[Tuple[int, int]] = field(default=None, compare=False)
    deferred_reason: Optional[str] = field(default=None, compare=False)
    #: `False` when two consecutive reads never agreed (a torn read).
    stable: bool = field(default=True, compare=False)

    @property
    def pid_alive(self) -> Optional[bool]:
        """See `pid_is_alive`. `False` means the supervisor is gone but
        something it forked still holds the lock (see `stale_supervisor_message`)."""
        return pid_is_alive(self.pid)

    @property
    def reload_capable(self) -> bool:
        """Does the daemon advertise live reload (a `start=` line)? A legacy
        daemon does not, and must be restarted to pick up a change."""
        return self.start is not None

    @property
    def supervisor_state(self) -> str:
        """`'alive'`, `'gone'` or `'unknown'`: see `supervisor_state()`.
        Evaluated at call time."""
        return supervisor_state(self)


def pid_is_alive(pid: Optional[int]) -> Optional[bool]:
    """Does process `pid` exist? `None` when that cannot be told.

    Informational only (module docstring): used to pick the wording of a
    report, never to decide whether a daemon is running. `kill(pid, 0)`
    raising `PermissionError` means the process exists (it is someone
    else's), so that is `True`. A pid <= 0 is never probed (`kill(0, 0)`
    addresses the caller's whole process group).
    """
    if pid is None or pid <= 0:
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        alive = True
    except OSError:
        return None
    else:
        alive = True
    # `kill(pid, 0)` succeeds on a zombie: an exited, unreaped process is not
    # alive (it runs no code and handles no signal).
    if alive and procident.is_zombie(pid) is True:
        return False
    return alive


def supervisor_state(info) -> str:
    """Is the supervisor recorded in a pid file still running? `'alive'`,
    `'gone'` or `'unknown'`, for reporting (`zfsbackup-config status`).

    `info` is anything with `pid` and `start` (a `DaemonInfo`). With a
    recorded start token the answer is `procident.identity(pid, start)`:
    `'match'` is alive, `'gone'` (no such process, or a zombie) and
    `'mismatch'` (the pid was reused) are gone. Without a token (a legacy
    daemon: `start` is `None`; or `""`, a platform with no tokens), or when
    identity cannot be read (`'unknown'`, e.g. `hidepid`), it falls back to
    `pid_is_alive`, which still treats a zombie as gone but cannot detect a
    reused pid. Like `pid_is_alive`, informational only: "a daemon is
    running" means "the lock is held", never this. Never raises.
    """
    try:
        pid = getattr(info, "pid", None)
        if pid is None or isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
            return "unknown"
        start = getattr(info, "start", None)
        if start:
            ident = procident.identity(pid, start)
            if ident == "match":
                return "alive"
            if ident in ("gone", "mismatch"):
                return "gone"
        alive = pid_is_alive(pid)
    except Exception:
        return "unknown"
    if alive is True:
        return "alive"
    if alive is False:
        return "gone"
    return "unknown"


def stale_supervisor_message(path, pid: int) -> str:
    """Wording for "the lock is held but the supervisor pid is gone": under
    `fork` the workers inherit the lock descriptor and briefly outlive a
    `kill -9`'d supervisor (until their next safe point, see the module
    docstring). Shared by the daemon and the CLI."""
    return (
        f"supervisor pid {pid} is gone, but processes it forked still hold the "
        f"daemon lock {path}; find them with `fuser {path}` or `lsof {path}`"
    )


def lock_path(resolved) -> Path:
    """`<db path>.daemon.pid` for a `ResolvedConfigPath` (or a plain path)."""
    db = getattr(resolved, "resolved_path", resolved)
    return Path(str(db) + LOCK_SUFFIX)


#: Sentinel for `set_state`: "leave this key as it is".
_KEEP = object()

#: A start token is short; refuse to carry arbitrary junk around.
_MAX_START = 128


def _format(
    pid: int,
    generation: Optional[int],
    start: Optional[str] = None,
    applied: Optional[Tuple[int, int]] = None,
    target: Optional[Tuple[int, int]] = None,
    rejected: Optional[Tuple[int, int]] = None,
    deferred: Optional[Tuple[int, int]] = None,
    deferred_reason: Optional[str] = None,
) -> bytes:
    """Pid file content. `start=None` writes the legacy two-line form; any
    string (including "") writes the v2 form."""
    text = f"{pid}\n{'' if generation is None else generation}\n"
    if start is not None:
        text += f"start={start}\n"
        for key, pair in (("applied", applied), ("target", target), ("rejected", rejected)):
            if pair is not None:
                text += f"{key}={pair[0]}:{pair[1]}\n"
        if deferred is not None:
            text += f"deferred={deferred[0]}:{deferred[1]}\n"
            reason = _clean_reason(deferred_reason)
            if reason:
                text += f"deferred_reason={reason}\n"
    return text.encode("ascii", errors="replace")


#: Longest `deferred_reason=` written or accepted.
_MAX_REASON = 200


def _clean_reason(text: Optional[str]) -> str:
    """One printable-ASCII line of at most `_MAX_REASON` characters: the
    file is group-writable and read by the CLI, so nothing else goes in or
    comes out."""
    if not text:
        return ""
    out = "".join(c if (c.isascii() and c.isprintable()) else " " for c in text)
    out = " ".join(out.split())
    return out[:_MAX_REASON]


class _Parsed(NamedTuple):
    pid: Optional[int]
    generation: Optional[int]
    start: Optional[str]
    applied: Optional[Tuple[int, int]]
    target: Optional[Tuple[int, int]]
    rejected: Optional[Tuple[int, int]]
    deferred: Optional[Tuple[int, int]] = None
    deferred_reason: Optional[str] = None


def _pair(value: str) -> Optional[Tuple[int, int]]:
    store, sep, gen = value.strip().partition(":")
    if not sep:
        return None
    try:
        a, b = int(store), int(gen)
    except ValueError:
        return None
    return (a, b) if a >= 0 and b >= 0 else None


def _parse(raw: bytes) -> _Parsed:
    """Parse the pid file. Lines 1-2 are pid and generation; from line 3 on,
    `key=value` lines. Anything unparseable is `None` rather than an error
    (see module docstring); unknown keys are ignored; the first occurrence of
    a key wins. `start` is `None` only when the key is absent."""
    lines = raw.decode("ascii", errors="replace").split("\n")

    def _num(i: int) -> Optional[int]:
        try:
            return int(lines[i].strip()) if i < len(lines) else None
        except ValueError:
            return None

    kv = {}
    for line in lines[2:]:
        key, sep, value = line.partition("=")
        key = key.strip()
        if sep and key and key not in kv:
            kv[key] = value.strip()
    start = kv.get("start")
    if start is not None and (len(start) > _MAX_START or any(c.isspace() or not c.isprintable() for c in start)):
        start = ""  # present but unusable -> reported as "identity not recorded"
    return _Parsed(
        _num(0),
        _num(1),
        start,
        _pair(kv["applied"]) if "applied" in kv else None,
        _pair(kv["target"]) if "target" in kv else None,
        _pair(kv["rejected"]) if "rejected" in kv else None,
        _pair(kv["deferred"]) if "deferred" in kv else None,
        (_clean_reason(kv["deferred_reason"]) or None) if "deferred_reason" in kv else None,
    )


def _read_all(fd: int) -> bytes:
    chunks = []
    off = 0
    while True:
        chunk = os.pread(fd, 4096, off)
        if not chunk:
            break
        chunks.append(chunk)
        off += len(chunk)
        if off > 65536:  # never trust the file to be small
            break
    return b"".join(chunks)


def _read_stable(fd: int) -> Tuple[bytes, bool]:
    """-> (content, stable). Re-read up to `_STABLE_READS` times until two
    consecutive reads are byte-identical. The writer overwrites then truncates,
    so a single read can straddle an update and mix two versions; `stable` is
    `False` when no two consecutive reads ever agreed (the last read is
    returned anyway, for display only -- never act on an unstable read)."""
    prev = _read_all(fd)
    for _ in range(_STABLE_READS):
        cur = _read_all(fd)
        if cur == prev:
            return cur, True
        prev = cur
    return prev, False


def _info(raw: bytes, stable: bool = True) -> DaemonInfo:
    p = _parse(raw)
    return DaemonInfo(
        pid=p.pid,
        generation=p.generation,
        start=p.start,
        applied=p.applied,
        target=p.target,
        rejected=p.rejected,
        deferred=p.deferred,
        deferred_reason=p.deferred_reason,
        stable=stable,
    )


class DaemonLock:
    """A held daemon lock. Create with `acquire()`; do not construct directly.

    Also a context manager: `release()` runs on a clean exit from the block
    only (an exception propagating out still releases the descriptor but
    leaves the file, like a crash would -- the kernel drops the lock either
    way, and a stale file is harmless).
    """

    def __init__(self, path: Path, fd: int):
        self.path = path
        self._fd: Optional[int] = fd
        #: Only the acquiring process may unlink/close-with-unlink. A forked
        #: worker inherits this object (and the fd); it must never delete the
        #: file out from under the supervisor.
        self._owner_pid = os.getpid()
        self.pid = self._owner_pid
        self.generation: Optional[int] = None
        #: Always written by this (v2) daemon; "" when the platform has no
        #: start token, which tells readers not to signal us.
        self.start: str = procident.start_token(self._owner_pid) or ""
        self.applied: Optional[Tuple[int, int]] = None
        self.target: Optional[Tuple[int, int]] = None
        self.rejected: Optional[Tuple[int, int]] = None
        self.deferred: Optional[Tuple[int, int]] = None
        self.deferred_reason: Optional[str] = None

    @property
    def held(self) -> bool:
        return self._fd is not None and os.getpid() == self._owner_pid

    def _write(self) -> None:
        fd = self._fd
        if fd is None or os.getpid() != self._owner_pid:
            return
        data = _format(
            self.pid, self.generation, self.start, self.applied, self.target,
            self.rejected, self.deferred, self.deferred_reason,
        )
        # Overwrite first, trim second: never an empty window for a reader.
        # The overwrite is padded with newlines up to the old length, so a
        # writer preempted between the two calls leaves new content followed
        # by blank lines (which `_parse` ignores), never new content followed
        # by the old tail -- which two consecutive reads would agree on.
        try:
            old_len = os.fstat(fd).st_size
        except OSError:
            old_len = 0
        os.pwrite(fd, data + b"\n" * max(0, old_len - len(data)), 0)
        os.ftruncate(fd, len(data))
        os.fsync(fd)

    def set_generation(self, generation: Optional[int]) -> None:
        """Record the config generation this daemon loaded (item 15.2(a))."""
        self.generation = generation
        try:
            self._write()
        except OSError as exc:
            # Informational data; never take the daemon down for it.
            logger.warning("could not record generation in %s: %s", self.path, exc)

    def set_state(
        self, *, applied=_KEEP, target=_KEEP, rejected=_KEEP,
        deferred=_KEEP, deferred_reason=_KEEP,
    ) -> None:
        """Record reload state. Each value is `Optional[Tuple[store_id, generation]]`
        (`None` clears the key); omitted keys keep their value. Setting
        `applied` also rewrites line 2 with its generation. `deferred` (a
        refusal that is re-checked at every poll) takes an optional one-line
        `deferred_reason`; clearing `deferred` clears its reason too. One
        write; failures are logged at WARNING and never raised."""
        if applied is not _KEEP:
            self.applied = applied
            self.generation = None if applied is None else applied[1]
        if target is not _KEEP:
            self.target = target
        if rejected is not _KEEP:
            self.rejected = rejected
        if deferred is not _KEEP:
            self.deferred = deferred
            if deferred is None:
                self.deferred_reason = None
        if deferred_reason is not _KEEP:
            self.deferred_reason = (
                _clean_reason(deferred_reason) or None if self.deferred is not None else None
            )
        try:
            self._write()
        except OSError as exc:
            logger.warning("could not record reload state in %s: %s", self.path, exc)

    def release(self) -> None:
        """Remove the file, then drop the lock. Call on a clean exit only.

        Unlink happens while the lock is still held, so a concurrent
        `acquire()` can never lock a file we are about to delete and a
        successor can never lock a half-deleted one (`acquire` re-checks the
        inode after locking for the other half of that race). No-op in any
        process but the one that acquired (forked workers). Idempotent.
        """
        fd = self._fd
        if fd is None:
            return
        if os.getpid() != self._owner_pid:
            return
        self._fd = None
        try:
            try:
                st_fd = os.fstat(fd)
                st_path = os.lstat(self.path)
                if (st_fd.st_dev, st_fd.st_ino) == (st_path.st_dev, st_path.st_ino):
                    os.unlink(self.path)
            except FileNotFoundError:
                pass
            except OSError as exc:
                logger.warning("could not remove %s: %s", self.path, exc)
        finally:
            try:
                os.close(fd)  # drops the flock (if no fork child shares it)
            except OSError:
                pass

    def __enter__(self) -> "DaemonLock":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.release()
        else:
            fd, self._fd = self._fd, None
            if fd is not None and os.getpid() == self._owner_pid:
                try:
                    os.close(fd)
                except OSError:
                    pass


def _service_gid() -> Optional[int]:
    try:
        import grp

        return grp.getgrnam(CONFIG_OWNER.split(":", 1)[1]).gr_gid
    except (ImportError, KeyError):
        return None


def _open_for_lock(path: Path):
    """Open (creating if needed) the lock file; -> (fd, created).

    `O_EXCL` first so we know whether *we* made it (only then may root
    chgrp it); fall back to opening the existing file. `O_NOFOLLOW`: a
    privileged daemon must not write through a symlink planted next to the
    database. No `O_TRUNC` -- truncating before the lock is held would wipe
    the live daemon's pid out from under a refused second daemon.
    """
    base = os.O_RDWR | _O_CLOEXEC | _O_NOFOLLOW | _O_NONBLOCK
    try:
        try:
            return os.open(str(path), base | os.O_CREAT | os.O_EXCL, CONFIG_DB_MODE), True
        except FileExistsError:
            return os.open(str(path), base), False
    except FileNotFoundError:
        # Lost a race with the previous daemon's release(); caller retries.
        raise
    except OSError as exc:
        raise DaemonLockError(
            exc.errno, f"cannot open daemon lock file {path}: {exc.strerror}", str(path)
        ) from exc


def _fix_permissions(fd: int, path: Path, db_path: Path, created: bool) -> None:
    """Chmod/chgrp only what we own; never fatal (the lock works regardless)."""
    try:
        st = os.fstat(fd)
        euid = os.geteuid()
        if created and euid == 0 and is_default_config_path(db_path):
            gid = _service_gid()
            if gid is None:
                logger.warning(
                    "created %s as root but the %r group does not exist; "
                    "non-root operators will not be able to probe it. "
                    "chgrp it once the group exists.",
                    path, CONFIG_OWNER.split(":", 1)[1],
                )
            else:
                os.fchown(fd, -1, gid)
        owned = euid == 0 or st.st_uid == euid
        if owned and stat_module.S_IMODE(os.fstat(fd).st_mode) != CONFIG_DB_MODE:
            os.fchmod(fd, CONFIG_DB_MODE)
    except OSError as exc:
        logger.warning("could not fix permissions on %s: %s", path, exc)


def acquire(resolved, *, generation: Optional[int] = None) -> DaemonLock:
    """Take the exclusive daemon lock for `resolved` (a `ResolvedConfigPath`).

    Raises `DaemonAlreadyRunning` if another process holds it, and
    `DaemonLockError` if the file cannot be opened. On success the returned
    `DaemonLock` owns an `O_CLOEXEC` descriptor that must stay open for the
    life of the process (do not let the object be garbage-collected).

    A contended lock is retried for up to `_CONTENTION_WINDOW` (1s) before
    `DaemonAlreadyRunning` is raised, so a CLI `probe()` that holds `LOCK_SH`
    for a few microseconds as the daemon starts cannot make it refuse to
    start. A missing directory raises `DaemonLockError` (ENOENT) at once.
    """
    db_path = Path(getattr(resolved, "resolved_path", resolved))
    path = lock_path(resolved)
    for _ in range(_ACQUIRE_ATTEMPTS):
        try:
            fd, created = _open_for_lock(path)
        except FileNotFoundError:
            # ENOENT on the open is either "the previous daemon unlinked the
            # file under us" (retry) or "the directory is not there" (never
            # going to work; say so at once rather than after five tries).
            if not path.parent.is_dir():
                raise DaemonLockError(
                    errno.ENOENT,
                    f"cannot create daemon lock file {path}: the directory "
                    f"{path.parent} does not exist",
                    str(path),
                ) from None
            continue
        try:
            deadline = time.monotonic() + _CONTENTION_WINDOW
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError as exc:
                    if exc.errno not in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES):
                        raise DaemonLockError(
                            exc.errno, f"cannot lock {path}: {exc.strerror}", str(path)
                        ) from exc
                    if time.monotonic() < deadline:
                        time.sleep(_CONTENTION_POLL)
                        continue
                    held = _info(*_read_stable(fd))
                    raise DaemonAlreadyRunning(path, held.pid, held.generation, held.start) from None

            # The previous holder unlinks the file before closing it. If we
            # opened it in that window we now hold a lock on an unlinked
            # inode while a third process may lock a fresh file at `path`.
            # Only a lock on the inode `path` currently names counts.
            try:
                st_path = os.stat(path)
            except FileNotFoundError:
                os.close(fd)
                fd = -1
                continue
            st_fd = os.fstat(fd)
            if (st_fd.st_dev, st_fd.st_ino) != (st_path.st_dev, st_path.st_ino):
                os.close(fd)
                fd = -1
                continue
            if not stat_module.S_ISREG(st_fd.st_mode):
                raise DaemonLockError(
                    errno.EINVAL, f"daemon lock file {path} is not a regular file", str(path)
                )

            _fix_permissions(fd, path, db_path, created)
            lock = DaemonLock(path, fd)
            lock.generation = generation
            fd = -1  # ownership transferred
            try:
                lock._write()
            except OSError as exc:
                # Holding the lock is what matters; the content is advisory.
                logger.warning("could not write pid to %s: %s", path, exc)
            return lock
        finally:
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
    raise DaemonLockError(
        errno.EAGAIN, f"could not stabilise daemon lock file {path} (it kept being replaced)", str(path)
    )


def probe(resolved) -> Optional[DaemonInfo]:
    """Is a daemon holding the lock for `resolved`? -> `DaemonInfo` or `None`.

    Opens the file read-only on a fresh descriptor (so it never shares an
    open file description -- and hence a lock -- with anything) and tries
    `LOCK_SH|LOCK_NB`: success means no exclusive holder, i.e. a stale file
    (released immediately, returns `None`); `EWOULDBLOCK` means a daemon
    holds `LOCK_EX`. A missing file is `None`. Takes no exclusive lock and
    writes nothing, so non-root group operators can call it, and it works
    while a daemon runs.

    Raises `DaemonLockError` only if the file exists but cannot be opened or
    locked for a reason other than contention (typically EACCES): the caller
    cannot tell, which is not the same as "not running".
    """
    path = lock_path(resolved)
    try:
        fd = os.open(str(path), _RDONLY_FLAGS)
    except (FileNotFoundError, NotADirectoryError):
        return None
    except OSError as exc:
        raise DaemonLockError(
            exc.errno, f"cannot open daemon lock file {path}: {exc.strerror}", str(path)
        ) from exc
    try:
        if not stat_module.S_ISREG(os.fstat(fd).st_mode):
            raise DaemonLockError(
                errno.EINVAL, f"daemon lock file {path} is not a regular file", str(path)
            )
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES):
                raise DaemonLockError(
                    exc.errno, f"cannot lock {path}: {exc.strerror}", str(path)
                ) from exc
            return _info(*_read_stable(fd))
        # Got the shared lock: nobody holds LOCK_EX. Stale file.
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        return None
    finally:
        os.close(fd)


# ---------------------------------------------------------------------------
# Verified reload request (the CLI's only way to signal the daemon)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReloadRequest:
    """Result of `request_reload`. `outcome` is one of:

    * `no-daemon`        -- nobody holds the daemon lock (or no pid file);
    * `already-applied`  -- the daemon already applied `skip_if_applied`;
    * `signalled`        -- a SIGHUP was delivered to the verified supervisor;
    * `not-permitted`    -- verified, but the kernel refused the signal (EPERM);
    * `legacy`           -- a daemon that predates live reload (restart it);
    * `supervisor-gone`  -- the recorded pid is not the lock holder (dead,
      reused, a fork orphan's leftover, or a forged file);
    * `unverified`       -- could not be proven safe to signal; the daemon's
      generation poll will pick the change up;
    * `error`            -- the pid file could not be opened/locked.

    Only `signalled` means a signal was sent. `pid` is the recorded pid (for
    reporting), `info` what was read, `detail` a human-readable reason.

    `reason_code` is a stable, machine-readable refinement of `outcome` for
    callers that pick their own wording (`""` when there is nothing to add):

    * `unverified`: `torn-read`, `no-identity` (empty `start=`), `refused-pid`,
      `pid-file-owner`, `not-linux` (no lock-table query on this platform),
      `locks-unreadable` (`/proc/locks`), `mount-device` (the pid file's mount
      device could not be established), `lock-holders-unknown`,
      `proc-hidden` (`hidepid`), `identity-unknown`;
    * `supervisor-gone`: `no-such-process`, `not-lock-holder`, `zombie`,
      `reused`, `file-not-open` (the pid does not have the pid file open: a
      recycled pid the lock table still names), `exited`;
    * `unverified` also: `fd-table-unreadable` (the target's `/proc/<pid>/fd`
      cannot be read and a signal-0 probe was not refused);
    * `not-permitted`: `other-user` (the fd table cannot be read and a
      signal-0 probe is refused with EPERM: another user's process; nothing
      is signalled).
    """

    outcome: str
    pid: Optional[int] = None
    info: Optional[DaemonInfo] = None
    detail: str = ""
    reason_code: str = ""


def _signal0_not_permitted(pid: int, pfd: int) -> bool:
    """Would the kernel refuse to signal `pid`? Probed with signal 0, which
    checks existence and permission and delivers nothing -- through the
    pinned pidfd where there is one (no pid race), else `os.kill(pid, 0)`.
    `True` only for `PermissionError`; anything else (success, gone, other
    errors) is `False`. Never raises, never sends a real signal."""
    try:
        if pfd >= 0:
            signal.pidfd_send_signal(pfd, 0)
        else:
            os.kill(pid, 0)
    except PermissionError:
        return True
    except Exception:
        return False
    return False


#: `procident.flock_holders_unavailable_reason` -> (`reason_code`, detail).
_UNAVAILABLE_DETAILS = {
    procident.UNAVAILABLE_NOT_LINUX: ("not-linux", "signals are verified only on Linux"),
    procident.UNAVAILABLE_LOCKS_UNREADABLE: ("locks-unreadable", "/proc/locks is not readable"),
    procident.UNAVAILABLE_MOUNT_DEVICE: (
        "mount-device", "cannot resolve the pid file's mount device",
    ),
    None: ("lock-holders-unknown", "the daemon lock's holders could not be determined"),
}


def request_reload(resolved, *, skip_if_applied: Optional[Tuple[int, int]] = None) -> ReloadRequest:
    """Ask the running daemon to reload its configuration, after proving the
    target is the daemon. Never raises.

    The pid file is `0660` and group-writable, so its pid is attacker-
    influenced; a SIGHUP's default action kills its target. The pid is
    therefore signalled only if, in order:

    1. the daemon lock is held (a `LOCK_SH|LOCK_NB` probe fails);
    2. the file reads back byte-identically twice (not torn);
    3. it carries a non-empty `start=` token (a v2 daemon on a platform with
       tokens);
    4. it has not already applied `skip_if_applied` (no signal needed);
    5. the pid is neither <= 1 nor this process;
    6. (Linux >= 5.3) a pidfd for the pid is opened *first*, so the pid cannot
       be recycled between the checks and the signal;
    7. the pid file's owner is root, the CLI's own euid, or the target
       process's own uid (`procident.process_uid`; if that cannot be read, it
       does not match) -- otherwise `unverified`, "pid file owned by uid N" --
       and `/proc/locks` lists that pid as holding an exclusive `FLOCK` on
       this very file, matched on device AND inode with the device taken from
       the file's mount (`procident.flock_write_holders(fd)`; an inode number
       alone collides across filesystems). The owner check is defence in
       depth: it stops a group member's own file from naming someone else's
       process. Each accepted owner is safe because: a root-owned file can
       only be rewritten in place by a group member, and the flock proof
       still binds the target to a holder of that exact file; the CLI's own
       files are the caller's own; and a file owned by the target's uid is
       one the target (the daemon, running as its service user) created -- a
       forger could only create such a file naming their own processes. The
       owner is checked after the pidfd is open, so it is compared with the
       pinned process. Where the lock table (or the device) is unavailable --
       macOS, `/proc/locks` unreadable, or the mount device unresolved --
       nothing is signalled (`unverified`, each with its own `detail` and
       `reason_code`; see `ReloadRequest`);
    8. `procident.identity(pid, start)` is `'match'`. This, not the lock
       table, is what rejects the fork orphans of a dead supervisor and a
       recycled pid: `/proc/locks` keeps listing the *dead locker's* pid for
       as long as a forked worker still holds the open file description, so
       step 7 alone would accept it. A zombie supervisor (exited, not yet
       reaped -- `pidfd_open` and the signal would both still succeed) is
       `'gone'` there and is reported as `supervisor-gone` (`zombie`). Any
       other `'gone'` after a successful `pidfd_open` is `unverified`, not
       `supervisor-gone`: the process existed a moment ago, so the kernel hid
       its `/proc` entry (`hidepid`).
    8b. the pid has this very file open: some `/proc/<pid>/fd/<n>` is the
       same `(st_dev, st_ino)` as our descriptor, with an exclusive `FLOCK`
       in its `fdinfo` where the kernel lists locks
       (`procident.process_has_file_open`). The lock table keeps naming a
       dead locker's pid while a forked worker holds the lock, so after pid
       reuse step 7 alone would accept an unrelated process; a recycled pid
       cannot have the file open. An unreadable fd table (`/proc/<pid>/fd`
       needs root or the same user) is probed with signal 0 (via the pidfd
       where available): EPERM is `not-permitted`, `other-user` -- the
       SIGHUP would be refused anyway -- and anything else is `unverified`,
       `fd-table-unreadable`. Nothing but signal 0 is sent here;
    9. only then `SIGHUP` -- exclusively that signal, to exactly that pid,
       through the pidfd where available (otherwise `os.kill`, with a
       check-to-kill window of microseconds), never to a process group.
    """
    path = lock_path(resolved)
    fd = -1
    pfd = -1
    try:
        # 1
        try:
            fd = os.open(str(path), _RDONLY_FLAGS)
        except (FileNotFoundError, NotADirectoryError):
            return ReloadRequest("no-daemon", detail=f"no daemon lock file {path}")
        except OSError as exc:
            return ReloadRequest("error", detail=f"cannot open daemon lock file {path}: {exc.strerror}")
        st = os.fstat(fd)
        if not stat_module.S_ISREG(st.st_mode):
            return ReloadRequest("error", detail=f"daemon lock file {path} is not a regular file")
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES):
                return ReloadRequest("error", detail=f"cannot lock {path}: {exc.strerror}")
        else:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
            return ReloadRequest("no-daemon", detail="no daemon holds the lock")

        # 2, 3
        raw, stable = _read_stable(fd)
        info = _info(raw, stable)
        pid = info.pid
        if not stable or pid is None:
            return ReloadRequest(
                "unverified", pid, info, "the pid file could not be read consistently",
                "torn-read",
            )
        if info.start is None:
            return ReloadRequest(
                "legacy", pid, info, "the daemon predates live reload; restart it to apply the change"
            )
        if info.start == "":
            return ReloadRequest(
                "unverified", pid, info, "the daemon did not record its identity on this platform",
                "no-identity",
            )
        # 4 -- only for a supervisor that is not provably dead: a pid file
        # left by a dead (or zombie, or pid-reused) supervisor still records
        # its last `applied=`, and "already applied" would hide that nothing
        # is running it. Such a pid falls through to the checks below.
        if skip_if_applied is not None and info.applied == tuple(skip_if_applied):
            if procident.identity(pid, info.start) not in ("gone", "mismatch"):
                return ReloadRequest(
                    "already-applied", pid, info, "the daemon already applied this change"
                )
        # 5
        if pid <= 1 or pid == os.getpid():
            return ReloadRequest(
                "unverified", pid, info, f"refusing to signal pid {pid}", "refused-pid"
            )

        # 6
        if hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"):
            try:
                pfd = os.pidfd_open(pid)
            except ProcessLookupError:
                return ReloadRequest(
                    "supervisor-gone", pid, info, f"process {pid} no longer exists",
                    "no-such-process",
                )
            except OSError:
                pfd = -1  # old kernel / EPERM-like: fall back to os.kill

        # 7
        if st.st_uid not in (0, os.geteuid()):
            # The third acceptable owner: the target process itself (a daemon
            # running as the service user writes its own pid file).
            if st.st_uid != procident.process_uid(pid):
                return ReloadRequest(
                    "unverified", pid, info, f"pid file owned by uid {st.st_uid}",
                    "pid-file-owner",
                )
        holders = procident.flock_write_holders(fd)
        if holders is None:
            why = procident.flock_holders_unavailable_reason(fd)
            code, detail = _UNAVAILABLE_DETAILS.get(why, _UNAVAILABLE_DETAILS[None])
            return ReloadRequest("unverified", pid, info, detail, code)
        if pid not in holders:
            return ReloadRequest(
                "supervisor-gone", pid, info, f"pid {pid} does not hold the daemon lock",
                "not-lock-holder",
            )

        # 8
        ident = procident.identity(pid, info.start)
        if ident == "gone" and procident.is_zombie(pid) is True:
            # Exited but not reaped: `pidfd_open` and `pidfd_send_signal`
            # both succeed on a zombie, and `/proc/locks` keeps the dead
            # locker's pid while a forked worker holds the lock -- but nothing
            # would ever handle the SIGHUP.
            return ReloadRequest(
                "supervisor-gone", pid, info,
                f"pid {pid} has exited (a zombie, not yet reaped)", "zombie",
            )
        if ident == "gone" and pfd >= 0:
            # The pidfd was opened, so the process existed a moment ago and is
            # not a zombie (above): "gone" here is a /proc entry the kernel
            # hides from us (hidepid). Cannot verify, so do not signal.
            return ReloadRequest(
                "unverified", pid, info, f"cannot verify the identity of pid {pid} (/proc hidden?)",
                "proc-hidden",
            )
        if ident in ("gone", "mismatch"):
            return ReloadRequest(
                "supervisor-gone", pid, info, f"pid {pid} is not the recorded supervisor ({ident})",
                "no-such-process" if ident == "gone" else "reused",
            )
        if ident != "match":
            return ReloadRequest(
                "unverified", pid, info, f"cannot verify the identity of pid {pid}",
                "identity-unknown",
            )
        # 8b -- the target must have THIS file open (`/proc/<pid>/fd`). The
        # lock table names the pid that took the lock and keeps naming it
        # while a forked worker holds the open file description; once that
        # pid is reused, step 7 would accept an unrelated process, and a
        # group operator can write its pid and real start token into the
        # file. A recycled pid cannot have the file open. Checked after the
        # pidfd is open, i.e. against the pinned process; after the identity
        # step so a zombie or a reused pid keeps its specific reason.
        has_open = procident.process_has_file_open(pid, st.st_dev, st.st_ino)
        if has_open is None and _signal0_not_permitted(pid, pfd):
            # Another user's process: `/proc/<pid>/fd` needs root or the same
            # user, and the SIGHUP itself would be refused too. Report it as
            # the kernel would, so the operator wording ("runs as another
            # user") is kept. Probed with signal 0 only -- no signal is sent.
            return ReloadRequest(
                "not-permitted", pid, info,
                f"pid {pid} runs as another user; not permitted to signal it",
                "other-user",
            )
        if has_open is None:
            return ReloadRequest(
                "unverified", pid, info,
                f"cannot read the open files of pid {pid} to confirm it holds the daemon lock",
                "fd-table-unreadable",
            )
        if not has_open:
            return ReloadRequest(
                "supervisor-gone", pid, info,
                f"pid {pid} does not have the daemon lock file open",
                "file-not-open",
            )

        # 9
        try:
            if pfd >= 0:
                signal.pidfd_send_signal(pfd, signal.SIGHUP)
            else:
                os.kill(pid, signal.SIGHUP)
        except PermissionError as exc:
            return ReloadRequest("not-permitted", pid, info, f"not permitted to signal pid {pid}: {exc.strerror}")
        except ProcessLookupError:
            return ReloadRequest("supervisor-gone", pid, info, f"process {pid} exited", "exited")
        except OSError as exc:
            return ReloadRequest("error", pid, info, f"could not signal pid {pid}: {exc.strerror}")
        return ReloadRequest("signalled", pid, info, f"SIGHUP sent to pid {pid}")
    except Exception as exc:  # the contract: never raises
        logger.debug("request_reload failed", exc_info=True)
        return ReloadRequest("error", detail=f"{type(exc).__name__}: {exc}")
    finally:
        for f in (pfd, fd):
            if f >= 0:
                try:
                    os.close(f)
                except OSError:
                    pass


# ---------------------------------------------------------------------------
# The workers' presence lock (`<db>.workers.lock`)
# ---------------------------------------------------------------------------


def workers_lock_path(resolved) -> Path:
    """`<db path>.workers.lock` for a `ResolvedConfigPath` (or a plain path)."""
    db = getattr(resolved, "resolved_path", resolved)
    return Path(str(db) + WORKERS_LOCK_SUFFIX)


def ensure_workers_lock(resolved) -> Path:
    """Create the workers lock file if absent (`0660`, same ownership rules as
    the pid file) and return its path. The file is never deleted. Raises
    `DaemonLockError` if it cannot be opened/created."""
    db_path = Path(getattr(resolved, "resolved_path", resolved))
    path = workers_lock_path(resolved)
    base = _O_CLOEXEC | _O_NOFOLLOW | _O_NONBLOCK
    created = False
    try:
        try:
            fd = os.open(str(path), os.O_RDWR | os.O_CREAT | os.O_EXCL | base, CONFIG_DB_MODE)
            created = True
        except FileExistsError:
            fd = os.open(str(path), os.O_RDONLY | base)
    except OSError as exc:
        raise DaemonLockError(
            exc.errno, f"cannot create workers lock file {path}: {exc.strerror}", str(path)
        ) from exc
    try:
        if not stat_module.S_ISREG(os.fstat(fd).st_mode):
            raise DaemonLockError(errno.EINVAL, f"workers lock file {path} is not a regular file", str(path))
        _fix_permissions(fd, path, db_path, created)
    finally:
        os.close(fd)
    return path


def hold_workers_lock(path) -> Optional[int]:
    """Take a shared lock on the workers lock file; -> the descriptor to keep
    open for the life of the process, or `None` (with a WARNING) if it cannot
    be taken. The descriptor is `O_CLOEXEC`, so `zfs` children never inherit
    it; open it in the worker itself, never in the supervisor before a fork.

    A `LOCK_EX|LOCK_NB` from `workers_present` held for a few microseconds
    refuses a `LOCK_SH|LOCK_NB`, so contention is retried for 1s.
    """
    path = Path(path)
    try:
        fd = os.open(str(path), _RDONLY_FLAGS)
    except OSError as exc:
        logger.warning("cannot open workers lock %s: %s", path, exc.strerror)
        return None
    if not stat_module.S_ISREG(os.fstat(fd).st_mode):
        logger.warning("workers lock %s is not a regular file", path)
        os.close(fd)
        return None
    deadline = time.monotonic() + _CONTENTION_WINDOW
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
            return fd
        except OSError as exc:
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES) and time.monotonic() < deadline:
                time.sleep(_CONTENTION_POLL)
                continue
            logger.warning("cannot lock workers lock %s: %s", path, exc.strerror)
            try:
                os.close(fd)
            except OSError:
                pass
            return None


#: `workers_present` tries the exclusive lock this many times, `_PROBE_RETRY_DELAY`
#: seconds apart (jittered), before it reports a holder: about 100 ms in all.
_PROBE_ATTEMPTS = 5
_PROBE_RETRY_DELAY = 0.025


def workers_present(resolved) -> bool:
    """Does any process hold the workers lock? A momentary `LOCK_EX|LOCK_NB` on
    a fresh read-only descriptor: contention that lasts through a few short
    retries (about 100 ms) means a holder, because another probe's own
    momentary lock clears at once. Never blocks longer than that. The lock is always
    released and the descriptor closed before returning. A missing file is
    `False`. Raises `DaemonLockError` if the file exists but cannot be opened or
    locked for any reason other than contention."""
    path = workers_lock_path(resolved)
    try:
        fd = os.open(str(path), _RDONLY_FLAGS)
    except (FileNotFoundError, NotADirectoryError):
        return False
    except OSError as exc:
        raise DaemonLockError(
            exc.errno, f"cannot open workers lock file {path}: {exc.strerror}", str(path)
        ) from exc
    try:
        if not stat_module.S_ISREG(os.fstat(fd).st_mode):
            raise DaemonLockError(
                errno.EINVAL, f"workers lock file {path} is not a regular file", str(path)
            )
        for attempt in range(_PROBE_ATTEMPTS):
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno not in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES):
                    raise DaemonLockError(
                        exc.errno, f"cannot lock {path}: {exc.strerror}", str(path)
                    ) from exc
                if attempt + 1 < _PROBE_ATTEMPTS:
                    # Two probes (`migrate` and a starting daemon) each hold
                    # `LOCK_EX` for an instant and would each take the other
                    # for a worker. A real worker holds `LOCK_SH` for its whole
                    # life, so it is still there after a short, jittered wait.
                    time.sleep(_PROBE_RETRY_DELAY * random.uniform(0.5, 1.5))
                continue
            return False
        return True
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(fd)

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
  that a running daemon is on an older generation and will not apply the change
  until it is restarted. It never signals the daemon.

File format (two lines, ASCII)::

    <pid>\\n
    <generation>\\n        # empty line when the generation is not known

`pid` is the supervisor's. `generation` is the config generation the daemon
loaded (`DaemonLock.set_generation` rewrites it in place once the config has
been read, so it is empty for the instant between `acquire()` and the load, and
a reader must treat an empty generation as "not known yet", not as "old").
Readers parse only the first two lines and tolerate an empty or
truncated file: the file is written with `pwrite` + `ftruncate` (never
truncate-then-write), so a concurrent `probe()` never sees an empty file once
the first write has happened, but it can still see one in the instant between
`acquire()` taking the lock and writing -- `DaemonInfo.pid` is then `None`
while the daemon is still correctly reported as running.

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
import stat as stat_module
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

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

_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)


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

    def __init__(self, path: Path, pid: Optional[int], generation: Optional[int] = None):
        self.path = path
        self.pid = pid
        self.generation = generation
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

    @property
    def pid_alive(self) -> Optional[bool]:
        """See `pid_is_alive`. `False` means the supervisor is gone but
        something it forked still holds the lock (see `stale_supervisor_message`)."""
        return pid_is_alive(self.pid)


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
        return True
    except OSError:
        return None
    return True


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


def _format(pid: int, generation: Optional[int]) -> bytes:
    return f"{pid}\n{'' if generation is None else generation}\n".encode("ascii")


def _parse(raw: bytes):
    """-> (pid | None, generation | None) from the first two lines; anything
    unparseable is `None` rather than an error (see module docstring)."""
    lines = raw.decode("ascii", errors="replace").split("\n")

    def _num(i: int) -> Optional[int]:
        try:
            return int(lines[i].strip()) if i < len(lines) else None
        except ValueError:
            return None

    return _num(0), _num(1)


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

    @property
    def held(self) -> bool:
        return self._fd is not None and os.getpid() == self._owner_pid

    def _write(self) -> None:
        fd = self._fd
        if fd is None or os.getpid() != self._owner_pid:
            return
        data = _format(self.pid, self.generation)
        # Overwrite first, trim second: never an empty window for a reader.
        os.pwrite(fd, data, 0)
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
    base = os.O_RDWR | _O_CLOEXEC | _O_NOFOLLOW
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
                    pid, gen = _parse(_read_all(fd))
                    raise DaemonAlreadyRunning(path, pid, gen) from None

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
        fd = os.open(str(path), os.O_RDONLY | _O_CLOEXEC | _O_NOFOLLOW)
    except (FileNotFoundError, NotADirectoryError):
        return None
    except OSError as exc:
        raise DaemonLockError(
            exc.errno, f"cannot open daemon lock file {path}: {exc.strerror}", str(path)
        ) from exc
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES):
                raise DaemonLockError(
                    exc.errno, f"cannot lock {path}: {exc.strerror}", str(path)
                ) from exc
            pid, gen = _parse(_read_all(fd))
            return DaemonInfo(pid=pid, generation=gen)
        # Got the shared lock: nobody holds LOCK_EX. Stale file.
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        return None
    finally:
        os.close(fd)

"""Never close a raw fd on a config database while this process may hold a
SQLite lock on it.

POSIX `fcntl` locks belong to the (process, inode) pair, not to a file
descriptor, and `close()` of ANY descriptor on the inode releases EVERY such
lock the process holds on it. In WAL mode an open SQLite connection keeps a
SHARED lock on the database file for its whole life; that lock is how another
process's closing connection learns it is NOT the last one. The preflight
(`paths.check_config_db`) used to `open()`/`close()` config.db to read its
header -- after a pooled `read_config` (inside `write_config`), between the
importer's phases, and on every trust call while other threads may hold trust
connections. Measured on SQLite 3.45.1 (Ubuntu 24.04, the CI runners): the
close dropped the pooled connection's SHARED lock (`/proc/locks`), another
process's close then believed itself the last connection, checkpointed and
DELETED `-wal` and unlinked `-shm`, and this process's still-open connections
went on through the unlinked wal-index; their close-time checkpoint later
copied stale frames into config.db -> "database disk image is malformed"
(`integrity_check` fails on 3.45 and 3.53 alike). SQLite's own VFS avoids
exactly this by deferring the close of an fd while any connection in the
process holds locks on the inode (`unixInodeInfo.pUnused`); a raw `open()`
bypasses that bookkeeping, so this module does the same for it.

The test: the process's own fd table
------------------------------------
A raw fd on a database (`read_db_header`, `close_db_fd`) is closed only if NO
other descriptor of this process refers to the same `(st_dev, st_ino)` --
listed from `/proc/self/fd` (Linux) or `/dev/fd` (macOS, BSD) and `fstat`ed.
Otherwise it is PARKED and closed by a later sweep that finds the inode free.
This holds by construction: a POSIX lock only lives while the process has an
fd on the file (closing the fd releases it), and SQLite keeps a connection's
fd open for as long as the connection -- or its zombie -- can hold a lock (it
even parks its OWN fds in `pUnused` while a sibling connection holds locks).
"Another fd on this inode exists" is therefore implied by "SQLite may hold a
lock here", whoever opened that connection and in whatever state it is.

It replaced a count of open `TrackedConnection`s, which could not see a
zombie: `sqlite3_close_v2` with a live statement (on Python >= 3.11
`Connection.close()` finalises only cached statements, so a partly consumed
cursor keeps its own) returns while the zombie still holds its fd and SHARED
lock, and the count had already dropped (measured on SQLite 3.45: the next
preflight released the lock). Unregistering from a weakref callback on the
Connection does NOT fix that: `cursor_clear` releases the cursor's connection
BEFORE its statement, so the connection is finalised -- and the callback runs
-- while the zombie still holds both (measured on 3.11, 3.12 and 3.13).
Keeping one fd per inode open forever would hold, but costs a descriptor for
every database the process ever preflighted (the test suite opens thousands).

- **Parked fds are reused and swept.** `read_db_header` reads through a
  parked fd on the same inode instead of opening another, so a process keeps
  at most one parked fd per database inode. Every `close_db_fd` /
  `read_db_header` and every `TrackedConnection.close()` sweeps: parked fds
  whose inode no other descriptor refers to are closed. A connection that is
  garbage-collected without `close()`, or a zombie that finalises later,
  leaves its inode's parked fd for the next sweep.
- **Connects are serialised with the decision (`_LOCK`, re-entrant).** The
  fd-table scan and the `os.close` it licenses run under `_LOCK`, and so does
  every `connect_tracked`, so a connection cannot open its fd after the scan
  and take its first lock before the close (an API request thread opening a
  trust connection while another runs the preflight). `_CONNECTS` counts
  connects; a close whose scan predates a connect (a finaliser that connected
  re-entrantly, on this thread, while `_LOCK` was held) parks instead.
- **Unknown means park.** An fd whose inode cannot be `fstat`ed, or an fd
  table that cannot be listed -- or whose listing does not show a
  descriptor this module knows is open, i.e. is incomplete (FreeBSD's
  `/dev/fd` without fdescfs) -- keeps the fd parked: a parked fd costs a
  descriptor, a wrong close corrupts the database.

Scope: every connection the store makes goes through `connect_tracked` (every
file engine `db.make_engine` builds, and the importer's raw probe). A plain
`sqlite3.connect()` elsewhere in the process is SEEN by the scan (its fd is in
the table) but not serialised with it -- do not add one; and code that
`open()`s and `close()`s the database file itself bypasses all of this.

**Why this is its own module, importing nothing from the store** (the same
rule as `_alembic_quiet.py`). Every tracked connection references its class,
whose methods' `__globals__` is THIS module's dict, and the at-fork hook below
pins that dict for the life of the interpreter. Defined in `db.py`, the dict
would reach `_ENGINES` and every pooled connection, which must stay free to be
closed at interpreter shutdown (a process that exits without `dispose_all()`
must still leave no `-wal`/`-shm`). Keep this module's globals free of
anything that can reach an engine, a session or a connection.
"""

from __future__ import annotations

import errno
import os
import sqlite3
import threading
from pathlib import Path
from typing import Callable, Dict, FrozenSet, List, Optional, Set, Tuple, Union

_DbKey = Tuple[int, int]

_LOCK = threading.RLock()
# Raw fds on databases, by inode, kept open because another descriptor of
# this process referred to the same inode when they were released.
_PARKED: Dict[Optional[_DbKey], List[int]] = {}
# Bumped (under `_LOCK`) before every `connect_tracked`; see the module
# docstring.
_CONNECTS = 0

# Where this process's open descriptors are listed: Linux first (`/dev/fd`
# is a symlink to it there), then macOS/BSD. A listing is only trusted if it
# shows a descriptor known to be open (`_other_fd_keys`): FreeBSD's devfs
# `/dev/fd` WITHOUT fdescfs mounted (the default) lists only 0, 1 and 2, and
# an incomplete listing taken as complete is exactly the lock drop this
# module exists to prevent.
_FD_DIRS = ("/proc/self/fd", "/dev/fd")


def _db_key(st: os.stat_result) -> _DbKey:
    return (st.st_dev, st.st_ino)


def _other_fd_keys(exclude: FrozenSet[int]) -> Optional[Set[_DbKey]]:
    """The inode of every descriptor this process has open, except `exclude`;
    `None` if the fd table cannot be read (the caller then parks).

    `exclude` is the caller's own descriptors -- the raw fd being decided
    and/or the parked ones -- all open under `_LOCK`, so the highest of them
    is a WITNESS: a listing that does not show it is incomplete (FreeBSD's
    devfs `/dev/fd` without fdescfs lists only 0-2) and is not used; the next
    directory is tried, and with none left the answer is `None`. A witness
    of 2 or below proves nothing (that same incomplete listing shows 0-2;
    `os.open` returns one when stdin/stdout/stderr is closed), so then the
    answer is `None` too -- park, the rare conservative case.

    Caller holds `_LOCK`, so no tracked connection opens an fd while this
    runs. An fd closed since the listing (`EBADF` -- the listing's own
    directory fd among them) is skipped: whatever it was, it holds no lock
    any more."""
    witness = max(exclude, default=-1)
    if witness <= 2:
        return None
    for fd_dir in _FD_DIRS:
        try:
            names = os.listdir(fd_dir)
        except OSError:
            continue
        if str(witness) in names:
            break
    else:
        return None
    keys: Set[_DbKey] = set()
    for name in names:
        try:
            fd = int(name)
        except ValueError:
            continue
        if fd in exclude:
            continue
        try:
            st = os.fstat(fd)
        except OSError as exc:
            if exc.errno == errno.EBADF:
                continue
            return None
        keys.add(_db_key(st))
    return keys


def _parked_fds() -> FrozenSet[int]:
    return frozenset(fd for fds in _PARKED.values() for fd in fds)


def _sweep(others: Optional[Set[_DbKey]], seq: int) -> None:
    """Close every parked fd whose inode is not in `others` (the scan taken
    when `_CONNECTS` was `seq`). Caller holds `_LOCK`.

    Re-entrancy (a finaliser that runs on this thread mid-loop and closes a
    connection or parks an fd): `pop(key, ())` tolerates a key a nested
    sweep already emptied, and a connect since the scan (`_CONNECTS` moved)
    stops the loop, because the scan no longer covers that connection's fd.
    An fd parked by a nested call after the scan was parked because of a
    descriptor the scan saw or a connect it did not -- the first keeps its
    key out of the loop, the second stops it."""
    if others is None:
        return
    for key in [k for k in _PARKED if k is not None and k not in others]:
        if _CONNECTS != seq:
            return
        for fd in _PARKED.pop(key, ()):
            try:
                os.close(fd)
            except OSError:  # pragma: no cover - EBADF/EIO on close
                pass


def _sweep_now() -> None:
    """Scan, then sweep. Caller holds `_LOCK`."""
    if _PARKED:
        seq = _CONNECTS
        _sweep(_other_fd_keys(_parked_fds()), seq)


class TrackedConnection(sqlite3.Connection):
    """A `sqlite3.Connection` whose `close()` sweeps parked raw fds.

    The real close runs first, outside `_LOCK` (it may checkpoint); the
    sweep after it re-scans the fd table, so a connection that `close()`
    left a zombie (live statements) still has its fd in that table and keeps
    its inode's parked fd open. The sweep runs even if the close raises --
    it is a scan, never a guess.

    **Deliberately no `__del__`.** A connection garbage-collected without
    `close()` (a pool dropped without `dispose()`) is closed by pysqlite's
    deallocator and leaves any parked fd on its inode for the next sweep. A
    `__del__` here was measured to keep the pooled connections of a process
    that exits without `dispose_all()` from ever being closed at interpreter
    shutdown (pysqlite's statement cache makes every connection part of a
    reference cycle), leaving `-wal`/`-shm` behind
    (`test_a_process_that_exits_without_dispose_all_leaves_no_sidecars`).

    Nothing here is per-process: a connection inherited across `fork` and
    closed in the child sweeps the CHILD's fd table, which is the right one
    (POSIX locks are per process; the child holds none through it).
    """

    def close(self) -> None:
        try:
            super().close()
        finally:
            with _LOCK:
                _sweep_now()


def connect_tracked(
    *args,
    _connect: Optional[Callable[..., sqlite3.Connection]] = None,
    **kwargs,
) -> sqlite3.Connection:
    """`sqlite3.connect(*args, **kwargs)` (or `_connect(...)`, e.g. a
    dialect's `connect`) as a `TrackedConnection`, opened under `_LOCK`.

    `sqlite3.connect` opens the database fd but takes no file lock (the
    first one is taken by the first statement, after this returns), so
    under `_LOCK` a concurrent raw close either completes before this
    connection's fd exists or sees that fd in its scan."""
    global _CONNECTS
    kwargs["factory"] = TrackedConnection
    with _LOCK:
        _CONNECTS += 1
        return (_connect or sqlite3.connect)(*args, **kwargs)


def _pread_all(fd: int, size: int) -> bytes:
    chunks = []
    offset = 0
    while offset < size:
        chunk = os.pread(fd, size - offset, offset)
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
    return b"".join(chunks)


def close_db_fd(fd: int) -> None:
    """Close a raw fd on a database file -- or, if any other descriptor of
    this process refers to the same inode (a SQLite connection that may hold
    a lock on it), park it for a later sweep. Use instead of `os.close` for
    ANY fd on a config database. Also sweeps other parked fds."""
    with _LOCK:
        try:
            key: Optional[_DbKey] = _db_key(os.fstat(fd))
        except OSError:
            key = None
        seq = _CONNECTS
        others = _other_fd_keys(_parked_fds() | {fd})
        if key is None or others is None or key in others or _CONNECTS != seq:
            _PARKED.setdefault(key, []).append(fd)
        else:
            os.close(fd)
        _sweep(others, seq)


def read_db_header(path: Union[str, Path], size: int = 100) -> bytes:
    """Up to `size` bytes from the start of `path`, without ever closing an
    fd on a database this process may hold a SQLite lock on.

    Reuses a parked fd on the same inode when there is one (no open at all;
    then sweeps); otherwise opens, reads, and hands the fd to `close_db_fd`.
    Raises the
    `OSError` of the `stat`/`open`/`read` (`FileNotFoundError`,
    `PermissionError`, ...) as a plain `open(path, "rb")` would."""
    with _LOCK:
        parked = _PARKED.get(_db_key(os.stat(path)))
        if parked:
            try:
                return _pread_all(parked[0], size)
            finally:
                # Releases it (and any other stale one) if whatever kept it
                # parked -- a GC'd connection, a zombie -- has gone since.
                _sweep_now()
        # O_NONBLOCK: no effect on a regular file, but a FIFO swapped in
        # after the caller's S_ISREG check must not hang every thread that
        # needs `_LOCK`.
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0))
        try:
            return _pread_all(fd, size)
        finally:
            close_db_fd(fd)


def _reset_after_fork() -> None:
    """A forked child starts with a fresh lock (a fork taken while another
    thread held `_LOCK` would deadlock the child) and closes the inherited
    parked fds: it holds no POSIX lock at all yet (locks are per process,
    never inherited), so closing them releases nothing -- and they would
    otherwise leak in the child."""
    global _LOCK
    _LOCK = threading.RLock()
    for fds in _PARKED.values():
        for fd in fds:
            try:
                os.close(fd)
            except OSError:  # pragma: no cover
                pass
    _PARKED.clear()


if hasattr(os, "register_at_fork"):  # POSIX only
    os.register_at_fork(after_in_child=_reset_after_fork)

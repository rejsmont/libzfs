"""`zfsbackup/config/store/_liveconn.py` -- never close a raw fd on a config
database while this process may hold a SQLite lock on it.

The bug (measured on SQLite 3.45.1, Ubuntu 24.04): `check_config_db` used to
`open()`/`close()` config.db to read its header. POSIX locks belong to the
(process, inode) pair, so that close dropped the SHARED lock of every live WAL
connection this process had on the file; another process's close then believed
itself the last connection, deleted `-wal`/`-shm`, and this process's
still-open connections later checkpointed stale frames into the database ->
"database disk image is malformed". The fix parks the raw fd instead of closing
it whenever another descriptor of this process refers to the same inode.

Everything here is file-backed under `tmp_path` and goes through the store's own
factories (`store.db`), never a bare engine. No test uses `mock_subprocess`.

Areas (class -> what it pins):

- `TestPreflightKeepsTheLock` -- the `/proc/locks` assertion and the end to end
  corruption repro (Linux only; the repro cannot corrupt on SQLite >= 3.53
  even unfixed, the lock assertion always holds).
- `TestZombieConnection` -- a closed connection with a live, partly consumed
  cursor still holds its fd and lock; the raw fd is parked, then swept.
- `TestCursorOrderTrap` -- documents why a weakref callback on the Connection
  cannot be the unregister hook (it fires while the zombie's fd is open).
- `TestFailClosed` -- unreadable fd directories, `fstat` errors, a FreeBSD
  style incomplete `/dev/fd`, and a witness fd <= 2 all park.
- `TestReentrancy` -- nested `TrackedConnection.close()` / a connect during a
  sweep (no `KeyError`, no double close, a connect after the scan prevents
  the close).
- `TestBoundedParking` -- one parked fd per inode however many preflights,
  released after the last close, no fd leak.
- `TestForeignConnection` -- a plain `sqlite3.connect` outside the store blocks
  the raw close too.
- `TestFork` -- the at-fork reset: parked state, lock, parent's lock intact.
- `TestEveryStoreConnectionIsTracked` -- writer `do_connect`, reader `creator`,
  the importer's probe, and a wrap-`sqlite3.connect` guard over store operations.
- `TestExitWithoutDisposeAll` -- a parked fd must not keep pooled connections
  from being closed at interpreter exit (the "no `__del__`" rule).

Mutation counts (scratch copy of the tree, one revert at a time; tests in this
file that then fail) are listed in the hand-off report for this file.
"""

import errno
import gc
import json
import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import threading
import traceback
import weakref
from pathlib import Path

import pytest
from _hermetic import hermetic_env

import zfsbackup
from zfsbackup.config.store import _liveconn
from zfsbackup.config.store import importer as importer_module
from zfsbackup.config.store.access import read_config, read_store_state, write_config
from zfsbackup.config.store.db import (
    dispose_all,
    ephemeral_engine,
    get_engine,
    make_engine,
    url_for_path,
)
from zfsbackup.config.store.importer import _probe_config_empty, import_yaml
from zfsbackup.config.store.migrate import upgrade_schema
from zfsbackup.config.store.paths import (
    check_config_db,
    open_config_connection,
    open_config_session,
    resolve_config_path,
)

ROOT = Path(__file__).resolve().parent.parent
ZFSBACKUP_DIR = Path(zfsbackup.__file__).resolve().parent

HAS_PROC_LOCKS = os.path.exists("/proc/locks")
needs_proc_locks = pytest.mark.skipif(
    not HAS_PROC_LOCKS, reason="needs /proc/locks (Linux) to observe POSIX locks"
)
needs_fork = pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")

FD_DIR = "/proc/self/fd" if os.path.isdir("/proc/self/fd") else "/dev/fd"

YAML = "snapshot_prefix: other\ndatasets:\n  - name: tank/x\n    frequency: 1h\n"


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_liveconn_state():
    """No engine, no connection and no parked fd leaks from one test into the
    next (the module state is process-global)."""
    dispose_all()
    yield
    dispose_all()
    gc.collect()
    with _liveconn._LOCK:
        for fds in list(_liveconn._PARKED.values()):
            for fd in fds:
                try:
                    os.close(fd)
                except OSError:
                    pass
        _liveconn._PARKED.clear()


def make_wal_db(path: Path, rows: int = 300) -> Path:
    """A WAL-mode SQLite file with a table `t` and no sidecars left behind."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, blob BLOB)")
    conn.executemany("INSERT INTO t (blob) VALUES (?)", [(os.urandom(700),)] * rows)
    conn.commit()
    conn.close()
    assert not sidecars(path), "setup connection left sidecars"
    return path


def sidecars(db: Path) -> list:
    return [s for s in ("-wal", "-shm") if os.path.exists(str(db) + s)]


def db_key(path) -> tuple:
    st = os.stat(path)
    return (st.st_dev, st.st_ino)


def fd_is_open(fd: int) -> bool:
    try:
        os.fstat(fd)
    except OSError as exc:
        if exc.errno == errno.EBADF:
            return False
        raise
    return True


def fds_on(path) -> int:
    """How many descriptors of THIS process refer to `path`'s inode (SQLite's
    own, a zombie's, a parked raw fd)."""
    key = db_key(path)
    n = 0
    for name in os.listdir(FD_DIR):
        try:
            if _liveconn._db_key(os.fstat(int(name))) == key:
                n += 1
        except (OSError, ValueError):
            continue
    return n


def fd_count() -> int:
    return len(os.listdir(FD_DIR))


def parked_fds() -> list:
    return [fd for fds in _liveconn._PARKED.values() for fd in fds]


def posix_locks(path) -> list:
    """The kinds ("READ"/"WRITE") of the POSIX locks THIS process holds on
    `path`'s inode, from /proc/locks."""
    ino = os.stat(path).st_ino
    pid = str(os.getpid())
    kinds = []
    for line in Path("/proc/locks").read_text().splitlines():
        f = line.split()
        if len(f) >= 6 and f[1] == "POSIX" and f[4] == pid and f[5].endswith(f":{ino}"):
            kinds.append(f[3])
    return kinds


def hold_lock(db: Path) -> sqlite3.Connection:
    """A tracked connection that has read (so, in WAL mode, holds its SHARED
    lock for as long as it is open)."""
    conn = _liveconn.connect_tracked(str(db))
    conn.execute("SELECT count(*) FROM t").fetchall()
    return conn


def run_other_process(db: Path, code: str) -> None:
    """Run `code` against `db` in another process (`c` is its connection),
    then close it -- this process's connections see it as a peer."""
    subprocess.run(
        [
            sys.executable, "-c",
            "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]);" + code + "; c.close()",
            str(db),
        ],
        check=True, capture_output=True, timeout=60, env=hermetic_env(),
    )


def integrity_check(db: Path) -> str:
    out = subprocess.run(
        [
            sys.executable, "-c",
            "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]);"
            "print(c.execute('PRAGMA integrity_check').fetchall()[:2])",
            str(db),
        ],
        capture_output=True, text=True, timeout=60, env=hermetic_env(),
    )
    return (out.stdout + out.stderr).strip()


def run_in_child(fn, timeout: int = 30):
    """Fork, run `fn()` in the child (its JSON-able result comes back through a
    pipe), wait for it and return `(exit_code, result)`. A hung child is killed
    by SIGALRM (`exit_code` is then negative); nothing is ever left behind."""
    rfd, wfd = os.pipe()
    pid = os.fork()
    if pid == 0:  # pragma: no cover - runs in the child
        code = 1
        try:
            os.close(rfd)
            signal.alarm(timeout)
            os.write(wfd, json.dumps(fn()).encode())
            code = 0
        except BaseException:
            traceback.print_exc()
        finally:
            os._exit(code)
    os.close(wfd)
    try:
        chunks = []
        while True:
            chunk = os.read(rfd, 65536)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        os.close(rfd)
        _, status = os.waitpid(pid, 0)
    data = b"".join(chunks)
    return os.waitstatus_to_exitcode(status), (json.loads(data) if data else None)


# --------------------------------------------------------------------------
# 1. The lock survives the preflight
# --------------------------------------------------------------------------


@pytest.mark.skipif(
    not sys.platform.startswith("linux") or not HAS_PROC_LOCKS,
    reason="Linux only: observes POSIX locks through /proc/locks",
)
class TestPreflightKeepsTheLock:
    def _pooled_read(self, resolved, db):
        with open_config_connection(resolved, readonly=True) as conn:
            conn.exec_driver_sql("SELECT count(*) FROM t").scalar()
        if "READ" not in posix_locks(db):  # precondition, before the preflight
            pytest.skip("this kernel does not list the process's own POSIX locks")

    def test_a_pooled_connections_shared_lock_survives_check_config_db(self, tmp_path):
        db = make_wal_db(tmp_path / "config.db")
        resolved = resolve_config_path(db)
        self._pooled_read(resolved, db)
        before = posix_locks(db)

        check_config_db(resolved)

        assert "READ" in posix_locks(db)
        assert posix_locks(db) == before

    def test_the_lock_survives_repeated_preflights_and_both_open_helpers(self, tmp_path):
        db = make_wal_db(tmp_path / "config.db")
        resolved = resolve_config_path(db)
        self._pooled_read(resolved, db)
        for _ in range(5):
            check_config_db(resolved)
            with open_config_connection(resolved, readonly=True) as conn:
                conn.exec_driver_sql("SELECT 1").scalar()
            with open_config_session(resolved, readonly=True):
                pass
            assert "READ" in posix_locks(db)

    def test_an_ephemeral_connection_held_open_keeps_its_lock_through_a_preflight(
        self, tmp_path
    ):
        # The trust path: one thread holds an unpooled connection while another
        # runs the preflight.
        db = make_wal_db(tmp_path / "config.db")
        resolved = resolve_config_path(db)
        with open_config_connection(resolved, readonly=True, ephemeral=True) as conn:
            conn.exec_driver_sql("SELECT count(*) FROM t").scalar()
            if "READ" not in posix_locks(db):
                pytest.skip("this kernel does not list the process's own POSIX locks")
            check_config_db(resolved)
            assert "READ" in posix_locks(db)

    def test_a_peer_close_after_a_preflight_does_not_delete_the_sidecars(self, tmp_path):
        db = make_wal_db(tmp_path / "config.db")
        resolved = resolve_config_path(db)
        self._pooled_read(resolved, db)
        check_config_db(resolved)

        run_other_process(db, "c.execute('SELECT count(*) FROM t').fetchone()")

        # This process is still connected, so the peer was not the last one.
        assert sidecars(db) == ["-wal", "-shm"]

    def test_pooled_read_preflight_peer_read_pooled_write_peer_write_leaves_a_sound_database(
        self, tmp_path
    ):
        """The measured corruption sequence. Corrupts on SQLite 3.45 without
        the fix (not on >= 3.53, where only the lock assertions bite)."""
        db = make_wal_db(tmp_path / "config.db")
        resolved = resolve_config_path(db)

        # 1. pooled read   2. the preflight
        self._pooled_read(resolved, db)
        check_config_db(resolved)
        assert "READ" in posix_locks(db)

        # 3. another process reads and closes: not the last connection
        run_other_process(db, "c.execute('SELECT count(*) FROM t').fetchone()")
        assert sidecars(db) == ["-wal", "-shm"]

        # 4. pooled write (preflights again)
        with open_config_connection(resolved, readonly=False) as conn:
            conn.exec_driver_sql("INSERT INTO t (blob) VALUES (randomblob(700))")
            conn.commit()

        # 5. another process writes and closes
        run_other_process(
            db,
            "c.execute('DELETE FROM t WHERE id % 7 = 0');"
            "c.executemany('INSERT INTO t (blob) VALUES (randomblob(900))', [()]*50);"
            "c.commit()",
        )
        assert sidecars(db) == ["-wal", "-shm"]

        # 6. this process closes everything: the last close removes the sidecars
        dispose_all()
        assert sidecars(db) == []

        # 7. nothing malformed
        assert integrity_check(db) == "[('ok',)]"


# --------------------------------------------------------------------------
# 2. Zombie connection
# --------------------------------------------------------------------------


class TestZombieConnection:
    def _zombie(self, db: Path):
        """A closed connection that is still alive inside SQLite: its cursor
        is partly consumed, so the statement (and the fd and lock) outlive
        `close()`. Returns `(cursor, key)`; skips where `close()` finalises
        everything."""
        conn = _liveconn.connect_tracked(str(db))
        cur = conn.execute("SELECT blob FROM t")
        assert cur.fetchone() is not None
        conn.close()
        if fds_on(db) == 0:
            pytest.skip("this Python/SQLite finalises every statement on close()")
        return cur, db_key(db)

    def test_the_raw_header_fd_is_parked_not_closed_while_a_zombie_holds_the_inode(
        self, tmp_path
    ):
        db = make_wal_db(tmp_path / "config.db")
        cur, key = self._zombie(db)
        before = fds_on(db)

        header = _liveconn.read_db_header(db)

        assert header.startswith(b"SQLite format 3")
        assert len(_liveconn._PARKED[key]) == 1
        parked = _liveconn._PARKED[key][0]
        assert fd_is_open(parked)
        assert fds_on(db) == before + 1
        del cur

    @needs_proc_locks
    def test_the_zombies_lock_is_still_listed_after_the_preflight(self, tmp_path):
        db = make_wal_db(tmp_path / "config.db")
        cur, _ = self._zombie(db)
        if "READ" not in posix_locks(db):
            pytest.skip("the zombie holds no lock to observe on this SQLite")

        check_config_db(resolve_config_path(db))

        assert "READ" in posix_locks(db)
        del cur

    def test_the_parked_fd_is_swept_once_the_cursor_is_gone_and_another_call_happens(
        self, tmp_path
    ):
        db = make_wal_db(tmp_path / "config.db")
        cur, key = self._zombie(db)
        _liveconn.read_db_header(db)
        parked = _liveconn._PARKED[key][0]

        # Still referenced: another call changes nothing.
        _liveconn.read_db_header(db)
        assert _liveconn._PARKED[key] == [parked] and fd_is_open(parked)

        del cur
        gc.collect()
        assert fds_on(db) == 1  # only the parked fd is left on the inode

        _liveconn.read_db_header(db)  # reuses the parked fd, then sweeps it

        assert key not in _liveconn._PARKED
        assert not fd_is_open(parked)
        assert fds_on(db) == 0

    def test_another_connections_close_sweeps_the_released_parked_fd(self, tmp_path):
        db = make_wal_db(tmp_path / "config.db")
        other = make_wal_db(tmp_path / "other.db")
        cur, key = self._zombie(db)
        _liveconn.read_db_header(db)
        parked = _liveconn._PARKED[key][0]
        del cur
        gc.collect()

        spare = _liveconn.connect_tracked(str(other))
        spare.close()

        assert key not in _liveconn._PARKED
        assert not fd_is_open(parked)


# --------------------------------------------------------------------------
# 3. The cursor-order trap
# --------------------------------------------------------------------------


class TestCursorOrderTrap:
    """`cursor_clear` releases the cursor's reference to the Connection BEFORE
    finalising its statement, so a weakref callback on a closed (zombie)
    Connection runs while the zombie still holds its fd and lock. Unregistering
    a connection from such a callback therefore frees the inode too early --
    which is why `_liveconn` scans the fd table instead. If this test starts
    failing the premise changed; skipped where this Python orders it the other
    way."""

    def _observe(self, db: Path, in_cycle: bool) -> dict:
        z = _liveconn.connect_tracked(str(db), isolation_level=None)
        cur = z.execute("SELECT blob FROM t")
        cur.fetchone()
        z.close()
        seen = {}

        def callback(_ref):
            seen["fds"] = fds_on(db)
            if HAS_PROC_LOCKS:
                seen["locks"] = len(posix_locks(db))

        ref = weakref.ref(z, callback)
        if in_cycle:
            holder = [cur, z]
            holder.append(holder)  # only the cycle collector can free these
            del holder
        del z, cur
        gc.collect()
        assert ref() is None
        assert "fds" in seen, "the weakref callback never ran"
        return seen

    @pytest.mark.parametrize("in_cycle", [False, True], ids=["refcount", "gc-cycle"])
    def test_a_weakref_callback_on_a_zombie_connection_fires_while_its_fd_is_open(
        self, tmp_path, in_cycle
    ):
        db = make_wal_db(tmp_path / "config.db")
        seen = self._observe(db, in_cycle)
        if seen["fds"] == 0:
            pytest.skip("this Python finalises the statement before the callback")

        assert seen["fds"] >= 1
        if HAS_PROC_LOCKS and seen["locks"] == 0:
            pytest.skip("the zombie holds no lock to observe on this SQLite")
        # ...and only afterwards is the inode free:
        assert fds_on(db) == 0


# --------------------------------------------------------------------------
# 4. Fail-closed paths
# --------------------------------------------------------------------------


class TestFailClosed:
    def _raw_fd(self, db: Path) -> int:
        fd = os.open(str(db), os.O_RDONLY)
        assert fd > 2
        return fd

    def _assert_parked(self, fd: int, db: Path):
        try:
            assert fd_is_open(fd)
            assert _liveconn._PARKED[db_key(db)] == [fd]
        except BaseException:
            if fd_is_open(fd):
                os.close(fd)
            raise

    def test_unreadable_fd_directories_park_the_fd(self, tmp_path, monkeypatch):
        db = make_wal_db(tmp_path / "config.db")
        monkeypatch.setattr(
            _liveconn, "_FD_DIRS", (str(tmp_path / "no" / "a"), str(tmp_path / "no" / "b"))
        )
        fd = self._raw_fd(db)

        _liveconn.close_db_fd(fd)

        self._assert_parked(fd, db)

    def test_read_db_header_parks_too_when_the_fd_table_is_unreadable(
        self, tmp_path, monkeypatch
    ):
        db = make_wal_db(tmp_path / "config.db")
        monkeypatch.setattr(_liveconn, "_FD_DIRS", (str(tmp_path / "no"),))

        assert _liveconn.read_db_header(db).startswith(b"SQLite format 3")

        assert len(_liveconn._PARKED[db_key(db)]) == 1
        assert fd_is_open(_liveconn._PARKED[db_key(db)][0])

    def test_a_parked_fd_is_released_once_the_fd_table_is_readable_again(
        self, tmp_path, monkeypatch
    ):
        db = make_wal_db(tmp_path / "config.db")
        with monkeypatch.context() as m:
            m.setattr(_liveconn, "_FD_DIRS", (str(tmp_path / "no"),))
            fd = self._raw_fd(db)
            _liveconn.close_db_fd(fd)
            self._assert_parked(fd, db)

        _liveconn.read_db_header(db)  # reuse + sweep with a working fd table

        assert db_key(db) not in _liveconn._PARKED
        assert not fd_is_open(fd)

    def test_an_fstat_error_other_than_ebadf_parks_the_fd(self, tmp_path, monkeypatch):
        db = make_wal_db(tmp_path / "config.db")
        fd = self._raw_fd(db)
        real_fstat = os.fstat

        def fstat(arg):
            if arg == fd:
                return real_fstat(arg)
            raise OSError(errno.EIO, "injected")

        monkeypatch.setattr(os, "fstat", fstat)
        try:
            _liveconn.close_db_fd(fd)
        finally:
            monkeypatch.undo()

        self._assert_parked(fd, db)

    def test_ebadf_from_fstat_means_closed_since_the_listing_and_is_ignored(
        self, tmp_path, monkeypatch
    ):
        db = make_wal_db(tmp_path / "config.db")
        fd = self._raw_fd(db)
        real_fstat = os.fstat

        def fstat(arg):
            if arg == fd:
                return real_fstat(arg)
            raise OSError(errno.EBADF, "closed behind our back")

        monkeypatch.setattr(os, "fstat", fstat)
        try:
            _liveconn.close_db_fd(fd)
        finally:
            monkeypatch.undo()

        assert not fd_is_open(fd)
        assert _liveconn._PARKED == {}

    def test_an_fd_that_cannot_be_statted_is_parked_under_the_unknown_key(self):
        # An fd number that is not open: `fstat` fails, so the inode is unknown.
        fd = os.open(os.devnull, os.O_RDONLY)
        os.close(fd)

        _liveconn.close_db_fd(fd)

        assert _liveconn._PARKED[None] == [fd]
        del _liveconn._PARKED[None]  # nothing real to close

    def test_freebsd_style_listing_of_only_0_1_2_parks(self, tmp_path, monkeypatch):
        db = make_wal_db(tmp_path / "config.db")
        fake = tmp_path / "fakefd"
        fake.mkdir()
        for name in "012":
            (fake / name).touch()
        monkeypatch.setattr(_liveconn, "_FD_DIRS", (str(fake),))
        fd = self._raw_fd(db)

        assert _liveconn._other_fd_keys(frozenset({fd})) is None
        _liveconn.close_db_fd(fd)

        self._assert_parked(fd, db)

    def test_an_incomplete_listing_followed_by_the_real_directory_falls_through_and_closes(
        self, tmp_path, monkeypatch
    ):
        db = make_wal_db(tmp_path / "config.db")
        fake = tmp_path / "fakefd"
        fake.mkdir()
        for name in "012":
            (fake / name).touch()
        monkeypatch.setattr(
            _liveconn, "_FD_DIRS", (str(fake), str(tmp_path / "no")) + _liveconn._FD_DIRS
        )
        fd = self._raw_fd(db)

        _liveconn.close_db_fd(fd)

        assert not fd_is_open(fd)
        assert _liveconn._PARKED == {}

    @pytest.mark.parametrize("witness", [-1, 0, 1, 2])
    def test_a_witness_of_two_or_below_proves_nothing(self, witness):
        exclude = frozenset() if witness < 0 else frozenset({witness})
        assert _liveconn._other_fd_keys(exclude) is None
        assert _liveconn._other_fd_keys(frozenset({0, 1, 2})) is None

    def test_a_witness_above_two_that_the_listing_shows_is_trusted(self, tmp_path):
        db = make_wal_db(tmp_path / "config.db")
        fd = self._raw_fd(db)
        try:
            keys = _liveconn._other_fd_keys(frozenset({fd}))
        finally:
            os.close(fd)
        assert keys is not None
        assert db_key(db) not in keys

    def test_an_fd_zero_is_parked_end_to_end(self, tmp_path):
        # `os.open` returns 0 when stdin is closed; a witness of 0 is shown by
        # even the incomplete FreeBSD listing, so it must not count.
        db = make_wal_db(tmp_path / "config.db")
        script = textwrap.dedent('''
            import os, sys
            from zfsbackup.config.store import _liveconn
            os.close(0)
            fd = os.open(sys.argv[1], os.O_RDONLY)
            assert fd == 0, fd
            _liveconn.close_db_fd(fd)
            os.fstat(0)  # still open
            assert _liveconn._PARKED and list(_liveconn._PARKED.values())[0] == [0]
            print("parked")
        ''')
        result = subprocess.run(
            [sys.executable, "-c", script, str(db)], cwd=ROOT, text=True,
            capture_output=True, timeout=60, env=hermetic_env(PYTHONPATH=str(ROOT)),
            stdin=subprocess.DEVNULL,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "parked"


# --------------------------------------------------------------------------
# 5. Re-entrancy
# --------------------------------------------------------------------------


class TestReentrancy:
    def _free_parked(self, *dbs: Path) -> list:
        """One parked fd per db, each on an inode no other descriptor refers
        to any more. Returns the parked fds in `_PARKED` order."""
        keepers = []
        for db in dbs:
            keeper = sqlite3.connect(str(db))
            keeper.execute("SELECT count(*) FROM t").fetchall()
            keepers.append(keeper)
            _liveconn.read_db_header(db)
        for keeper in keepers:
            keeper.close()  # a plain Connection: sweeps nothing
        fds = parked_fds()
        assert len(fds) == len(dbs) and all(fd_is_open(fd) for fd in fds)
        return fds

    def test_a_nested_tracked_close_during_a_sweep_raises_nothing_and_closes_each_fd_once(
        self, tmp_path, monkeypatch
    ):
        db_a = make_wal_db(tmp_path / "a.db")
        db_b = make_wal_db(tmp_path / "b.db")
        db_c = make_wal_db(tmp_path / "c.db")
        fd_a, fd_b = self._free_parked(db_a, db_b)
        spare = _liveconn.connect_tracked(str(db_c))
        closed = []
        real_close = os.close
        nested = []

        def spy(fd):
            real_close(fd)
            if fd in (fd_a, fd_b):
                closed.append(fd)
            if fd == fd_a and not nested:
                nested.append(True)
                spare.close()  # TrackedConnection.close() -> a nested sweep

        monkeypatch.setattr(os, "close", spy)
        try:
            _liveconn.read_db_header(db_a)  # reuse A, then sweep: A and B are free
        finally:
            monkeypatch.undo()

        assert nested
        assert sorted(closed) == sorted([fd_a, fd_b])  # each exactly once
        assert _liveconn._PARKED == {}

    def test_a_connect_during_a_sweep_stops_it_and_keeps_that_inodes_fd_parked(
        self, tmp_path, monkeypatch
    ):
        db_a = make_wal_db(tmp_path / "a.db")
        db_b = make_wal_db(tmp_path / "b.db")
        fd_a, fd_b = self._free_parked(db_a, db_b)
        real_close = os.close
        late = []

        def spy(fd):
            real_close(fd)
            if fd == fd_a and not late:  # a finaliser connecting re-entrantly
                late.append(_liveconn.connect_tracked(str(db_b)))

        monkeypatch.setattr(os, "close", spy)
        try:
            _liveconn.read_db_header(db_a)
        finally:
            monkeypatch.undo()

        assert late
        assert fd_is_open(fd_b)
        assert _liveconn._PARKED[db_key(db_b)] == [fd_b]
        late[0].close()  # the last user of the inode goes: now it is swept
        assert not fd_is_open(fd_b)
        assert _liveconn._PARKED == {}

    def test_a_connect_between_the_scan_and_the_close_parks_the_fd(
        self, tmp_path, monkeypatch
    ):
        db = make_wal_db(tmp_path / "config.db")
        real_scan = _liveconn._other_fd_keys
        late = []

        def scan_then_connect(exclude):
            result = real_scan(exclude)
            late.append(_liveconn.connect_tracked(str(db)))  # after the scan
            return result

        fd = os.open(str(db), os.O_RDONLY)
        monkeypatch.setattr(_liveconn, "_other_fd_keys", scan_then_connect)
        try:
            _liveconn.close_db_fd(fd)
        finally:
            monkeypatch.undo()

        assert fd_is_open(fd), "closed an fd whose inode a new connection had just opened"
        assert _liveconn._PARKED[db_key(db)] == [fd]
        late[0].close()
        assert not fd_is_open(fd)

    def test_a_connect_between_the_scan_and_the_sweep_keeps_the_parked_fd(
        self, tmp_path, monkeypatch
    ):
        db = make_wal_db(tmp_path / "config.db")
        (fd,) = self._free_parked(db)
        real_scan = _liveconn._other_fd_keys
        late = []

        def scan_then_connect(exclude):
            result = real_scan(exclude)
            late.append(_liveconn.connect_tracked(str(db)))
            return result

        monkeypatch.setattr(_liveconn, "_other_fd_keys", scan_then_connect)
        try:
            _liveconn.read_db_header(db)
        finally:
            monkeypatch.undo()

        assert late
        assert fd_is_open(fd)
        assert _liveconn._PARKED[db_key(db)] == [fd]
        late[0].close()
        assert not fd_is_open(fd)

    def test_connect_tracked_counts_connects_and_the_lock_is_reentrant(self, tmp_path):
        db = make_wal_db(tmp_path / "config.db")
        before = _liveconn._CONNECTS
        with _liveconn._LOCK:  # a finaliser connecting while the scan holds it
            conn = _liveconn.connect_tracked(str(db))
        assert _liveconn._CONNECTS == before + 1
        assert isinstance(conn, _liveconn.TrackedConnection)
        conn.close()


# --------------------------------------------------------------------------
# 6. Bounded parking
# --------------------------------------------------------------------------


class TestBoundedParking:
    def test_many_preflights_leave_one_parked_fd_per_inode_and_it_is_released_on_close(
        self, tmp_path
    ):
        db = make_wal_db(tmp_path / "config.db")
        resolved = resolve_config_path(db)
        baseline = fd_count()
        keeper = hold_lock(db)
        with_keeper = fd_count()
        assert with_keeper > baseline

        for _ in range(60):
            check_config_db(resolved)

        assert parked_fds() and len(_liveconn._PARKED[db_key(db)]) == 1
        assert len(parked_fds()) == 1
        assert fd_count() == with_keeper + 1  # exactly the one parked fd, no leak

        keeper.close()

        assert _liveconn._PARKED == {}
        assert fd_count() == baseline

    def test_one_parked_fd_per_inode_across_databases(self, tmp_path):
        db_a = make_wal_db(tmp_path / "a.db")
        db_b = make_wal_db(tmp_path / "b.db")
        res_a, res_b = resolve_config_path(db_a), resolve_config_path(db_b)
        baseline = fd_count()
        keep_a, keep_b = hold_lock(db_a), hold_lock(db_b)
        with_keepers = fd_count()

        for _ in range(20):
            check_config_db(res_a)
            check_config_db(res_b)

        assert sorted(len(v) for v in _liveconn._PARKED.values()) == [1, 1]
        assert fd_count() == with_keepers + 2

        keep_a.close()
        assert list(_liveconn._PARKED) == [db_key(db_b)]
        keep_b.close()
        assert _liveconn._PARKED == {}
        assert fd_count() == baseline

    def test_no_connection_means_no_parked_fd_at_all(self, tmp_path):
        db = make_wal_db(tmp_path / "config.db")
        resolved = resolve_config_path(db)
        baseline = fd_count()
        for _ in range(10):
            check_config_db(resolved)
        assert _liveconn._PARKED == {}
        assert fd_count() == baseline

    def test_closing_the_last_connection_releases_the_parked_fd_without_another_call(
        self, tmp_path
    ):
        db = make_wal_db(tmp_path / "config.db")
        keeper = hold_lock(db)
        _liveconn.read_db_header(db)
        (fd,) = parked_fds()

        keeper.close()

        assert not fd_is_open(fd)
        assert _liveconn._PARKED == {}

    def test_a_pooled_engine_disposed_releases_the_parked_fd(self, tmp_path):
        db = make_wal_db(tmp_path / "config.db")
        resolved = resolve_config_path(db)
        with open_config_connection(resolved, readonly=True) as conn:
            conn.exec_driver_sql("SELECT count(*) FROM t").scalar()
        check_config_db(resolved)
        (fd,) = parked_fds()

        dispose_all()  # closes the pooled TrackedConnection

        assert not fd_is_open(fd)
        assert _liveconn._PARKED == {}


# --------------------------------------------------------------------------
# 7. A connection the store did not make
# --------------------------------------------------------------------------


class TestForeignConnection:
    def test_a_plain_sqlite3_connect_outside_the_store_blocks_the_raw_close(self, tmp_path):
        db = make_wal_db(tmp_path / "config.db")
        foreign = sqlite3.connect(str(db))
        foreign.execute("SELECT count(*) FROM t").fetchall()
        fd = os.open(str(db), os.O_RDONLY)

        _liveconn.close_db_fd(fd)

        assert fd_is_open(fd)
        assert _liveconn._PARKED[db_key(db)] == [fd]
        foreign.close()  # an untracked close sweeps nothing...
        assert fd_is_open(fd)
        _liveconn.read_db_header(db)  # ...the next store call does
        assert not fd_is_open(fd)
        assert _liveconn._PARKED == {}

    def test_the_preflight_parks_while_a_foreign_connection_is_open(self, tmp_path):
        db = make_wal_db(tmp_path / "config.db")
        foreign = sqlite3.connect(str(db))
        foreign.execute("SELECT count(*) FROM t").fetchall()
        try:
            check_config_db(resolve_config_path(db))
            assert len(_liveconn._PARKED[db_key(db)]) == 1
        finally:
            foreign.close()

    @needs_proc_locks
    def test_the_foreign_connections_lock_survives_the_preflight(self, tmp_path):
        db = make_wal_db(tmp_path / "config.db")
        foreign = sqlite3.connect(str(db))
        foreign.execute("SELECT count(*) FROM t").fetchall()
        try:
            if "READ" not in posix_locks(db):
                pytest.skip("this kernel does not list the process's own POSIX locks")
            check_config_db(resolve_config_path(db))
            assert "READ" in posix_locks(db)
        finally:
            foreign.close()


# --------------------------------------------------------------------------
# 8. Fork
# --------------------------------------------------------------------------


@needs_fork
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
class TestFork:
    def test_a_child_that_closes_an_inherited_connection_leaves_the_parents_lock_intact(
        self, tmp_path
    ):
        db = make_wal_db(tmp_path / "config.db")
        resolved = resolve_config_path(db)
        parent_conn = hold_lock(db)
        inherited = hold_lock(db)
        if HAS_PROC_LOCKS and "READ" not in posix_locks(db):
            pytest.skip("this kernel does not list the process's own POSIX locks")

        def child():
            inherited.close()  # TrackedConnection.close() sweeps the CHILD's table
            own = _liveconn.connect_tracked(str(db))
            own.execute("SELECT count(*) FROM t").fetchall()
            check_config_db(resolved)
            with open_config_connection(resolved, readonly=True) as conn:
                conn.exec_driver_sql("SELECT count(*) FROM t").scalar()
            check_config_db(resolved)
            own.close()
            dispose_all()
            return {"parked": sum(len(v) for v in _liveconn._PARKED.values())}

        code, result = run_in_child(child)

        assert code == 0
        if HAS_PROC_LOCKS:
            assert "READ" in posix_locks(db)
        assert sidecars(db) == ["-wal", "-shm"]
        assert parent_conn.execute("SELECT count(*) FROM t").fetchone()[0] == 300
        parent_conn.close()
        assert integrity_check(db) == "[('ok',)]"

    def test_the_childs_parked_state_is_reset_and_its_lock_replaced(self, tmp_path):
        db = make_wal_db(tmp_path / "config.db")
        keeper = hold_lock(db)
        _liveconn.read_db_header(db)
        (parked,) = parked_fds()
        parent_lock = _liveconn._LOCK

        def child():
            return {
                "parked": {str(k): v for k, v in _liveconn._PARKED.items()},
                "inherited_fd_open": fd_is_open(parked),
                "lock_replaced": _liveconn._LOCK is not parent_lock,
            }

        code, result = run_in_child(child)

        assert code == 0
        assert result == {"parked": {}, "inherited_fd_open": False, "lock_replaced": True}
        # The parent is untouched.
        assert _liveconn._LOCK is parent_lock
        assert _liveconn._PARKED[db_key(db)] == [parked] and fd_is_open(parked)
        keeper.close()
        assert not fd_is_open(parked)

    def test_a_fork_taken_while_another_thread_holds_the_lock_does_not_deadlock_the_child(
        self, tmp_path
    ):
        db = make_wal_db(tmp_path / "config.db")
        held, release = threading.Event(), threading.Event()

        def holder():
            with _liveconn._LOCK:
                held.set()
                release.wait(30)

        thread = threading.Thread(target=holder)
        thread.start()
        assert held.wait(10)
        try:
            code, result = run_in_child(
                lambda: {"header": len(_liveconn.read_db_header(db))}, timeout=5
            )
        finally:
            release.set()
            thread.join()

        assert code == 0, f"child exit status {code} (a negative value is the SIGALRM kill)"
        assert result == {"header": 100}

    def test_a_child_with_no_inherited_state_still_parks_and_sweeps(self, tmp_path):
        db = make_wal_db(tmp_path / "config.db")

        def child():
            keeper = hold_lock(db)
            _liveconn.read_db_header(db)
            parked = sum(len(v) for v in _liveconn._PARKED.values())
            keeper.close()
            return {"parked": parked, "after": sum(len(v) for v in _liveconn._PARKED.values())}

        code, result = run_in_child(child)

        assert code == 0
        assert result == {"parked": 1, "after": 0}


# --------------------------------------------------------------------------
# 9. Every store connection is tracked
# --------------------------------------------------------------------------


class TestEveryStoreConnectionIsTracked:
    @pytest.mark.parametrize("readonly", [False, True], ids=["writer", "reader"])
    @pytest.mark.parametrize("how", ["cached", "ephemeral", "uncached"])
    @pytest.mark.parametrize("name", ["plain", "percent"])
    def test_the_dbapi_connection_is_a_tracked_connection(
        self, tmp_path, name, how, readonly
    ):
        directory = "100%done" if name == "percent" else "store"
        db = make_wal_db(tmp_path / directory / "config.db")
        url = url_for_path(db)

        def check(engine):
            with engine.connect() as conn:
                raw = conn.connection.dbapi_connection
                assert isinstance(raw, _liveconn.TrackedConnection)
                assert conn.exec_driver_sql("SELECT count(*) FROM t").scalar() == 300

        if how == "cached":
            check(get_engine(url, readonly=readonly))
        elif how == "ephemeral":
            with ephemeral_engine(url, readonly=readonly) as engine:
                check(engine)
        else:
            engine = make_engine(url, readonly=readonly)
            try:
                check(engine)
            finally:
                engine.dispose()

    def test_a_memory_engine_is_left_alone(self):
        engine = make_engine("sqlite:///:memory:")
        try:
            with engine.connect() as conn:
                assert conn.exec_driver_sql("SELECT 1").scalar() == 1
        finally:
            engine.dispose()

    def test_the_importers_probe_connection_is_a_tracked_connection(
        self, tmp_path, monkeypatch
    ):
        db = make_wal_db(tmp_path / "config.db")
        made = []
        real = importer_module.connect_tracked

        def spy(*args, **kwargs):
            conn = real(*args, **kwargs)
            made.append(conn)
            return conn

        monkeypatch.setattr(importer_module, "connect_tracked", spy)

        assert _probe_config_empty(db) is True  # no global_settings table

        assert len(made) == 1
        assert isinstance(made[0], _liveconn.TrackedConnection)

    def test_no_zfsbackup_frame_makes_an_untracked_file_connect(
        self, tmp_path, monkeypatch
    ):
        """Wraps `sqlite3.connect` (and `sqlite3.dbapi2.connect`) over a tour of the store (import, read,
        write, poll, upgrade, ephemeral, the probe) and flags any file
        connect issued from a `zfsbackup/` frame without the tracked factory."""
        real_connect = sqlite3.connect
        untracked, tracked = [], []

        def wrapper(*args, **kwargs):
            target = args[0] if args else kwargs.get("database")
            if str(target) not in (":memory:", "") and "mode=memory" not in str(target):
                if kwargs.get("factory") is _liveconn.TrackedConnection:
                    tracked.append(str(target))
                else:
                    frames = [
                        f for f in traceback.extract_stack()[:-1]
                        if Path(f.filename).resolve().is_relative_to(ZFSBACKUP_DIR)
                    ]
                    if frames:
                        untracked.append(
                            (str(target), [f"{Path(f.filename).name}:{f.lineno}" for f in frames])
                        )
            return real_connect(*args, **kwargs)

        # SQLAlchemy's pysqlite dialect imports `sqlite3.dbapi2`, whose `connect`
        # is a separate module attribute: patch both or the writers go unseen.
        monkeypatch.setattr(sqlite3, "connect", wrapper)
        monkeypatch.setattr(sqlite3.dbapi2, "connect", wrapper)

        yaml_path = tmp_path / "in.yaml"
        yaml_path.write_text(YAML)
        db = tmp_path / "store" / "config.db"
        resolved = resolve_config_path(db)
        import_yaml(yaml_path, resolved)
        yaml_path.write_text(YAML.replace("1h", "2h"))
        import_yaml(yaml_path, resolved)  # the re-import path
        snapshot = read_config(resolved)
        read_store_state(resolved)
        write_config(
            resolved, snapshot.config, expected_generation=snapshot.generation
        )
        upgrade_schema(resolved)
        with open_config_session(resolved, readonly=True, ephemeral=True):
            pass
        with open_config_connection(resolved, readonly=False, ephemeral=True):
            pass
        dispose_all()
        _probe_config_empty(db)

        assert untracked == []
        assert any("?mode=rw" in t for t in tracked)  # readers
        assert len(tracked) >= 8, tracked  # not vacuous: readers, writers, ephemeral, probe


# --------------------------------------------------------------------------
# 10. Exit without dispose_all
# --------------------------------------------------------------------------


class TestExitWithoutDisposeAll:
    """No `__del__` on `TrackedConnection`: it keeps pooled connections from
    being closed at interpreter shutdown. A process that holds a pooled
    connection, parks an fd and exits without `dispose_all()` must leave no
    `-wal`/`-shm` (see also `test_zfsbackup_store_upgrade.py::
    TestTheQuietFilterPinsNoEngine`)."""

    SCRIPT = textwrap.dedent('''
        import sys
        from zfsbackup.config.store import _liveconn
        from zfsbackup.config.store.paths import (
            check_config_db, open_config_connection, resolve_config_path)
        resolved = resolve_config_path(sys.argv[1])
        with open_config_connection(resolved, readonly=True) as conn:
            conn.exec_driver_sql("SELECT count(*) FROM t").scalar()
        check_config_db(resolved)  # parks a raw fd while the pooled connection is open
        assert any(_liveconn._PARKED.values()), "nothing was parked"
        # Falls off the end: no dispose_all(), no engine.dispose(), no os._exit().
        print("done")
    ''')

    def test_a_process_with_a_parked_fd_that_exits_without_dispose_all_leaves_no_sidecars(
        self, tmp_path
    ):
        db = make_wal_db(tmp_path / "config.db")

        result = subprocess.run(
            [sys.executable, "-c", self.SCRIPT, str(db)], cwd=ROOT, text=True,
            capture_output=True, timeout=120, env=hermetic_env(PYTHONPATH=str(ROOT)),
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "done"
        assert sidecars(db) == [], "pooled connections were not closed at exit"
        assert integrity_check(db) == "[('ok',)]"

    def test_tracked_connection_defines_no_del(self):
        assert "__del__" not in vars(_liveconn.TrackedConnection)

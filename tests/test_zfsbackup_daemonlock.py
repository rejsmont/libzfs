"""The daemon presence lock (item 15.2(c)): `zfsbackup/daemonlock.py`.

"Running" means "the exclusive `flock` is held" -- never "the pid in the file
exists". Almost everything here is in-process and hermetic: `flock` locks
belong to an *open file description*, so a second `acquire()`/`probe()` in the
same process (each opens its own descriptor) contends with the first exactly as
another process would. The handful of tests that need a genuinely different
pid, a `kill -9`, or `fork` semantics use real subprocesses
(`daemon_lock_holder` in `conftest.py`) with hard timeouts and unconditional
teardown kills.

**Path-only:** none of this needs a SQLite database; the lock file lives next
to a database *path* (`<db>.daemon.pid`), which need not exist.

Mutation counts (scratch copy outside the repo, one change at a time) are
recorded in the item-15.2(c) hand-off report.
"""

import errno
import fcntl
import logging
import os
import signal
import stat
import time

import pytest

from zfsbackup import daemonlock
from zfsbackup.config.store import resolve_config_path
from zfsbackup.daemonlock import (
    DaemonAlreadyRunning,
    DaemonInfo,
    DaemonLockError,
    acquire,
    lock_path,
    probe,
)

HAS_FORK = hasattr(os, "fork")
IS_ROOT = hasattr(os, "geteuid") and os.geteuid() == 0


@pytest.fixture
def db(tmp_path):
    """A config *database path* (the file itself is never created)."""
    return tmp_path / "config.db"


@pytest.fixture
def lockfile(db):
    return lock_path(db)


@pytest.fixture
def held():
    """Collects locks a test wants closed at teardown even if it fails."""
    locks = []
    yield locks
    for lock in locks:
        try:
            lock.release()
        except Exception:
            pass


def read(path):
    return path.read_bytes()


def fd_count():
    return len(os.listdir("/dev/fd"))


def wait_until(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def hold_raw(path, content: bytes, mode=0o660):
    """Take a bare `LOCK_EX` on `path` with arbitrary content (no daemonlock)."""
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, mode)
    os.write(fd, content)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return fd


# ---------------------------------------------------------------------------
# lock_path
# ---------------------------------------------------------------------------


class TestLockPath:
    def test_is_the_database_path_plus_the_suffix(self, db):
        assert lock_path(db) == db.parent / "config.db.daemon.pid"

    def test_accepts_a_plain_string(self, db):
        assert lock_path(str(db)) == db.parent / "config.db.daemon.pid"

    def test_accepts_a_resolved_config_path(self, db):
        resolved = resolve_config_path(str(db))
        assert lock_path(resolved) == lock_path(resolved.resolved_path)
        assert lock_path(resolved).name == "config.db.daemon.pid"

    def test_a_dotted_database_name_is_not_mangled(self, tmp_path):
        assert lock_path(tmp_path / "my.config.v2.db").name == "my.config.v2.db.daemon.pid"

    def test_acquire_and_probe_accept_a_resolved_config_path(self, db, held):
        resolved = resolve_config_path(str(db))
        lock = acquire(resolved)
        held.append(lock)
        assert lock.path == lock_path(resolved)
        assert probe(resolved) == DaemonInfo(os.getpid(), None)


# ---------------------------------------------------------------------------
# probe: nothing there / stale / held
# ---------------------------------------------------------------------------


class TestProbe:
    def test_no_file_means_not_running(self, db):
        assert probe(db) is None

    def test_probe_creates_nothing(self, db, lockfile):
        probe(db)
        assert not lockfile.exists()

    def test_missing_directory_means_not_running(self, tmp_path):
        assert probe(tmp_path / "nodir" / "config.db") is None

    def test_a_path_component_that_is_a_file_means_not_running(self, tmp_path):
        (tmp_path / "afile").write_text("x")
        assert probe(tmp_path / "afile" / "config.db") is None

    def test_a_stale_file_means_not_running(self, db, lockfile):
        lockfile.write_text("99999\n12\n")
        assert probe(db) is None

    def test_probing_a_stale_file_leaves_it_untouched(self, db, lockfile):
        lockfile.write_text("99999\n12\n")
        before = lockfile.stat()
        probe(db)
        after = lockfile.stat()
        assert read(lockfile) == b"99999\n12\n"
        assert (before.st_mtime_ns, before.st_ino) == (after.st_mtime_ns, after.st_ino)

    def test_probe_does_not_leave_a_shared_lock_behind(self, db, lockfile):
        """A stale file probed must not stay locked, or the next daemon
        could not start."""
        lockfile.write_text("99999\n\n")
        assert probe(db) is None
        lock = acquire(db)
        lock.release()

    def test_a_held_lock_is_reported_with_pid_and_generation(self, db, held):
        held.append(acquire(db, generation=7))
        assert probe(db) == DaemonInfo(pid=os.getpid(), generation=7)

    def test_a_held_lock_with_no_generation_reports_none(self, db, held):
        held.append(acquire(db))
        info = probe(db)
        assert info.pid == os.getpid()
        assert info.generation is None

    def test_probe_does_not_modify_a_held_lock_file(self, db, lockfile, held):
        held.append(acquire(db, generation=3))
        before = lockfile.stat()
        content = read(lockfile)
        probe(db)
        probe(db)
        after = lockfile.stat()
        assert read(lockfile) == content
        assert before.st_mtime_ns == after.st_mtime_ns

    def test_probe_is_repeatable_and_does_not_disturb_the_holder(self, db, held):
        lock = acquire(db, generation=1)
        held.append(lock)
        for _ in range(3):
            assert probe(db) is not None
        assert lock.held

    def test_probe_sees_the_lock_go_away_on_release(self, db, held):
        lock = acquire(db)
        held.append(lock)
        assert probe(db) is not None
        lock.release()
        assert probe(db) is None

    def test_probe_returns_a_daemoninfo(self, db, held):
        held.append(acquire(db))
        assert isinstance(probe(db), DaemonInfo)

    def test_probe_opens_read_only_close_on_exec_and_nofollow(self, db, held, monkeypatch):
        held.append(acquire(db))
        seen = []
        real_open = os.open

        def spy(path, flags, *a, **kw):
            seen.append(flags)
            return real_open(path, flags, *a, **kw)

        monkeypatch.setattr(daemonlock.os, "open", spy)
        probe(db)
        (flags,) = seen
        assert flags & os.O_ACCMODE == os.O_RDONLY
        assert flags & os.O_CLOEXEC
        assert flags & os.O_NOFOLLOW
        assert not flags & (os.O_CREAT | os.O_TRUNC | os.O_WRONLY | os.O_RDWR)

    def test_probe_closes_its_descriptor(self, db, held):
        held.append(acquire(db))
        before = fd_count()
        for _ in range(5):
            probe(db)
        assert fd_count() == before
        stale = lock_path(db.parent / "other.db")
        stale.write_text("1\n\n")
        for _ in range(5):
            probe(db.parent / "other.db")
        assert fd_count() == before


# ---------------------------------------------------------------------------
# probe: the parser tolerates garbage
# ---------------------------------------------------------------------------


class TestParser:
    @pytest.mark.parametrize(
        "content, expected",
        [
            (b"", DaemonInfo(None, None)),
            (b"\n", DaemonInfo(None, None)),
            (b"123\n", DaemonInfo(123, None)),
            (b"123", DaemonInfo(123, None)),
            (b"123\n\n", DaemonInfo(123, None)),
            (b"123\n45\n", DaemonInfo(123, 45)),
            (b"123\n45", DaemonInfo(123, 45)),
            (b"  123  \n  45  \n", DaemonInfo(123, 45)),
            (b"123\n45\njunk\nmore\n", DaemonInfo(123, 45)),
            (b"\n45\n", DaemonInfo(None, 45)),
            (b"abc\n45\n", DaemonInfo(None, 45)),
            (b"123\nxyz\n", DaemonInfo(123, None)),
            (b"garbage\nmore garbage\n", DaemonInfo(None, None)),
            (b"12.5\n1e3\n", DaemonInfo(None, None)),
            (b"\xff\xfe\x00\x01\n\xff\n", DaemonInfo(None, None)),
            (b"\x00" * 100, DaemonInfo(None, None)),
        ],
    )
    def test_probe_tolerates_any_content(self, db, lockfile, content, expected):
        fd = hold_raw(lockfile, content)
        try:
            assert probe(db) == expected
        finally:
            os.close(fd)

    def test_a_huge_file_is_bounded_not_slurped_forever(self, db, lockfile):
        fd = hold_raw(lockfile, b"7\n9\n" + b"x" * (1 << 20))
        try:
            start = time.monotonic()
            assert probe(db) == DaemonInfo(7, 9)
            assert time.monotonic() - start < 5
        finally:
            os.close(fd)

    def test_acquire_refused_on_a_garbage_file_names_an_unknown_pid(self, db, lockfile):
        fd = hold_raw(lockfile, b"\xff\xfe")
        try:
            with pytest.raises(DaemonAlreadyRunning) as exc:
                acquire(db)
            assert exc.value.pid is None
            assert "unknown pid" in str(exc.value)
        finally:
            os.close(fd)


# ---------------------------------------------------------------------------
# acquire: the exclusive lock
# ---------------------------------------------------------------------------


class TestAcquire:
    def test_creates_the_file_with_pid_and_empty_generation(self, db, lockfile, held):
        held.append(acquire(db))
        assert read(lockfile) == f"{os.getpid()}\n\n".encode()

    def test_generation_argument_is_written(self, db, lockfile, held):
        held.append(acquire(db, generation=41))
        assert read(lockfile) == f"{os.getpid()}\n41\n".encode()

    def test_returns_a_held_lock(self, db, lockfile, held):
        lock = acquire(db)
        held.append(lock)
        assert lock.held
        assert lock.path == lockfile
        assert lock.pid == os.getpid()

    def test_a_second_acquire_is_refused_naming_the_holder(self, db, lockfile, held):
        held.append(acquire(db, generation=5))
        with pytest.raises(DaemonAlreadyRunning) as exc:
            acquire(db)
        err = exc.value
        assert err.pid == os.getpid()
        assert err.generation == 5
        assert err.path == lockfile
        assert f"pid {os.getpid()}" in str(err)
        assert str(lockfile) in str(err)

    def test_refusal_is_not_an_oserror(self, db, held):
        """`DaemonLockError` (can't open) and `DaemonAlreadyRunning` (held)
        are different conditions and `_run` handles them differently."""
        held.append(acquire(db))
        with pytest.raises(DaemonAlreadyRunning) as exc:
            acquire(db)
        assert not isinstance(exc.value, OSError)
        assert not issubclass(DaemonAlreadyRunning, DaemonLockError)
        assert issubclass(DaemonLockError, OSError)

    def test_a_refused_acquire_does_not_wipe_the_holders_content(self, db, lockfile, held):
        """No `O_TRUNC` before the lock is held: a refused second daemon must
        not blank the live daemon's pid out of the file."""
        held.append(acquire(db, generation=9))
        before = read(lockfile)
        for _ in range(3):
            with pytest.raises(DaemonAlreadyRunning):
                acquire(db)
        assert read(lockfile) == before
        assert probe(db) == DaemonInfo(os.getpid(), 9)

    def test_a_refused_acquire_does_not_drop_the_holders_lock(self, db, held):
        lock = acquire(db)
        held.append(lock)
        with pytest.raises(DaemonAlreadyRunning):
            acquire(db)
        assert lock.held
        assert probe(db) is not None
        with pytest.raises(DaemonAlreadyRunning):
            acquire(db)

    def test_a_refused_acquire_leaks_no_descriptor(self, db, held):
        held.append(acquire(db))
        before = fd_count()
        for _ in range(5):
            with pytest.raises(DaemonAlreadyRunning):
                acquire(db)
        assert fd_count() == before

    def test_a_successful_acquire_holds_exactly_one_descriptor(self, db, held):
        before = fd_count()
        held.append(acquire(db))
        assert fd_count() == before + 1

    def test_stale_file_is_taken_over_and_overwritten(self, db, lockfile, held):
        lockfile.write_text("99999\n12\n")
        held.append(acquire(db, generation=30))
        assert read(lockfile) == f"{os.getpid()}\n30\n".encode()

    def test_overwriting_a_longer_stale_file_leaves_no_tail(self, db, lockfile, held):
        """`pwrite` then `ftruncate`: the old, longer content must not survive."""
        lockfile.write_bytes(b"123456789\n987654321\n" + b"junk" * 200)
        held.append(acquire(db))
        assert read(lockfile) == f"{os.getpid()}\n\n".encode()

    def test_garbage_stale_file_is_taken_over(self, db, lockfile, held):
        lockfile.write_bytes(b"\xff\xfe\x00garbage")
        held.append(acquire(db))
        assert probe(db).pid == os.getpid()

    def test_empty_stale_file_is_taken_over(self, db, lockfile, held):
        lockfile.write_bytes(b"")
        held.append(acquire(db))
        assert read(lockfile) == f"{os.getpid()}\n\n".encode()

    def test_reacquire_after_release_works(self, db, lockfile):
        acquire(db).release()
        assert not lockfile.exists()
        lock = acquire(db)
        try:
            assert lockfile.exists()
        finally:
            lock.release()

    def test_descriptor_is_close_on_exec(self, db, held):
        lock = acquire(db)
        held.append(lock)
        assert fcntl.fcntl(lock._fd, fcntl.F_GETFD) & fcntl.FD_CLOEXEC

    def test_open_flags_and_mode(self, db, held, monkeypatch):
        seen = []
        real_open = os.open

        def spy(path, flags, *a, **kw):
            seen.append((flags, a, kw))
            return real_open(path, flags, *a, **kw)

        monkeypatch.setattr(daemonlock.os, "open", spy)
        held.append(acquire(db))
        flags, args, kwargs = seen[0]
        assert flags & os.O_ACCMODE == os.O_RDWR
        assert flags & os.O_CREAT and flags & os.O_EXCL
        assert flags & os.O_CLOEXEC
        assert flags & os.O_NOFOLLOW
        assert not flags & os.O_TRUNC
        mode = args[0] if args else kwargs.get("mode")
        assert mode == 0o660

    def test_an_existing_file_is_opened_without_o_excl_or_trunc(
        self, db, lockfile, held, monkeypatch
    ):
        lockfile.write_text("1\n\n")
        seen = []
        real_open = os.open

        def spy(path, flags, *a, **kw):
            seen.append(flags)
            return real_open(path, flags, *a, **kw)

        monkeypatch.setattr(daemonlock.os, "open", spy)
        held.append(acquire(db))
        assert seen[0] & os.O_EXCL  # tried to create first...
        assert not seen[-1] & (os.O_EXCL | os.O_CREAT | os.O_TRUNC)  # ...then plain open
        assert seen[-1] & os.O_NOFOLLOW and seen[-1] & os.O_CLOEXEC

    def test_missing_directory_is_a_lock_error_not_a_crash(self, tmp_path):
        with pytest.raises(DaemonLockError):
            acquire(tmp_path / "nodir" / "config.db")

    def test_a_directory_at_the_lock_path_is_a_lock_error(self, db, lockfile):
        lockfile.mkdir()
        with pytest.raises(DaemonLockError) as exc:
            acquire(db)
        assert str(lockfile) in str(exc.value)

    @pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="needs mkfifo")
    def test_a_fifo_at_the_lock_path_is_refused_as_not_regular(self, db, lockfile):
        os.mkfifo(lockfile)

        def _timeout(signum, frame):
            raise TimeoutError("opening a fifo blocked")

        old = signal.signal(signal.SIGALRM, _timeout)
        signal.alarm(10)
        try:
            with pytest.raises(DaemonLockError):
                acquire(db)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old)

    def test_a_non_regular_file_is_refused_and_its_descriptor_closed(self, db, lockfile, monkeypatch):
        """Seam test: on some platforms a FIFO fails earlier (in `flock`), so
        the `S_ISREG` guard is only reachable for a lockable non-regular file
        such as a character device. Force `S_ISREG` false to reach it."""
        lockfile.write_text("1\n\n")
        monkeypatch.setattr(daemonlock.stat_module, "S_ISREG", lambda mode: False)
        before = fd_count()
        with pytest.raises(DaemonLockError) as exc:
            acquire(db)
        assert exc.value.errno == errno.EINVAL
        assert "not a regular file" in str(exc.value)
        assert fd_count() == before

    def test_lock_error_leaks_no_descriptor(self, db, lockfile):
        lockfile.mkdir()
        before = fd_count()
        with pytest.raises(DaemonLockError):
            acquire(db)
        assert fd_count() == before

    def test_flock_failure_other_than_contention_is_a_lock_error(self, db, lockfile, monkeypatch):
        def boom(fd, op):
            raise OSError(errno.ENOLCK, "no locks available")

        monkeypatch.setattr(daemonlock.fcntl, "flock", boom)
        before = fd_count()
        with pytest.raises(DaemonLockError) as exc:
            acquire(db)
        assert exc.value.errno == errno.ENOLCK
        assert fd_count() == before

    def test_probe_flock_failure_other_than_contention_is_a_lock_error(
        self, db, lockfile, monkeypatch
    ):
        lockfile.write_text("1\n\n")

        def boom(fd, op):
            raise OSError(errno.ENOLCK, "no locks available")

        monkeypatch.setattr(daemonlock.fcntl, "flock", boom)
        before = fd_count()
        with pytest.raises(DaemonLockError):
            probe(db)
        assert fd_count() == before

    def test_an_unwritable_pid_is_a_warning_not_a_failure(self, db, held, monkeypatch, caplog):
        """Holding the lock is what matters; the content is advisory."""

        def boom(fd, data, off):
            raise OSError(errno.ENOSPC, "no space")

        monkeypatch.setattr(daemonlock.os, "pwrite", boom)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemonlock"):
            lock = acquire(db)
        held.append(lock)
        assert lock.held
        assert "could not write pid" in caplog.text
        assert probe(db) is not None  # still reported as running (pid None)


# ---------------------------------------------------------------------------
# acquire against a real second process
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestAcquireAgainstAnotherProcess:
    def test_held_by_another_process_probe_names_its_pid(self, db, daemon_lock_holder):
        holder = daemon_lock_holder(db, generation=11)
        assert holder.pid != os.getpid()
        assert probe(db) == DaemonInfo(pid=holder.pid, generation=11)

    def test_held_by_another_process_acquire_names_its_pid(self, db, daemon_lock_holder):
        holder = daemon_lock_holder(db, generation=11)
        with pytest.raises(DaemonAlreadyRunning) as exc:
            acquire(db)
        assert exc.value.pid == holder.pid
        assert exc.value.generation == 11
        assert f"pid {holder.pid}" in str(exc.value)
        # And it did not blank the holder's record.
        assert read(lock_path(db)) == f"{holder.pid}\n11\n".encode()

    def test_kill_9_leaves_a_stale_file_that_probe_ignores(self, db, lockfile, daemon_lock_holder):
        holder = daemon_lock_holder(db, generation=2)
        holder.kill9()
        assert lockfile.exists(), "a kill -9'd daemon leaves its file behind"
        assert read(lockfile) == f"{holder.pid}\n2\n".encode()
        assert wait_until(lambda: probe(db) is None)

    def test_kill_9_stale_file_is_taken_over_by_the_next_daemon(
        self, db, lockfile, daemon_lock_holder, held
    ):
        holder = daemon_lock_holder(db, generation=2)
        holder.kill9()
        assert wait_until(lambda: probe(db) is None)
        held.append(acquire(db, generation=3))
        assert read(lockfile) == f"{os.getpid()}\n3\n".encode()

    def test_a_clean_exit_of_the_holder_drops_the_lock(self, db, daemon_lock_holder):
        holder = daemon_lock_holder(db)
        assert probe(db) is not None
        holder.proc.terminate()
        holder.proc.wait(timeout=10)
        assert wait_until(lambda: probe(db) is None)

    @pytest.mark.skipif(not HAS_FORK, reason="needs fork")
    def test_an_orphaned_forked_worker_keeps_the_lock_after_kill_9(
        self, db, lockfile, daemon_lock_holder
    ):
        """Under `fork` the workers share the open file description, so the
        lock survives the supervisor until the *last* holder exits: an
        orphaned worker is still snapshotting, and a second daemon must not
        start beside it."""
        holder = daemon_lock_holder(db, generation=4, fork=True)
        assert holder.orphan_pid
        holder.kill9()
        info = probe(db)
        assert info is not None, "orphan inherited the descriptor; lock must stay held"
        assert info.pid == holder.pid  # the dead supervisor's pid: informational only
        with pytest.raises(DaemonAlreadyRunning):
            acquire(db)
        holder.kill_orphan()
        assert wait_until(lambda: probe(db) is None)
        assert lockfile.exists()  # stale, harmless

    @pytest.mark.skipif(not HAS_FORK, reason="needs fork")
    def test_exec_does_not_inherit_the_descriptor(self, db, held):
        """`zfs send` children must never keep a dead daemon's lock alive:
        `os.fork` + `os.execv` (no `close_fds` help from `subprocess`) must
        see `O_CLOEXEC` close the descriptor."""
        import sys

        lock = acquire(db)
        held.append(lock)
        r, w = os.pipe()
        pid = os.fork()
        if pid == 0:  # pragma: no cover - child
            try:
                os.close(r)
                os.dup2(w, 1)
                code = (
                    "import os, sys\n"
                    "try:\n"
                    f"    os.fstat({lock._fd}); print('inherited')\n"
                    "except OSError:\n"
                    "    print('closed')\n"
                )
                os.execv(sys.executable, [sys.executable, "-I", "-c", code])
            finally:
                os._exit(127)
        os.close(w)
        deadline = time.monotonic() + 30
        data = b""
        while time.monotonic() < deadline:
            chunk = os.read(r, 100)
            if not chunk:
                break
            data += chunk
        os.close(r)
        os.waitpid(pid, 0)
        assert data.strip() == b"closed"


# ---------------------------------------------------------------------------
# set_generation
# ---------------------------------------------------------------------------


class TestSetGeneration:
    def test_updates_the_file(self, db, lockfile, held):
        lock = acquire(db)
        held.append(lock)
        lock.set_generation(8)
        assert read(lockfile) == f"{os.getpid()}\n8\n".encode()
        assert lock.generation == 8

    def test_probe_sees_the_new_generation(self, db, held):
        lock = acquire(db)
        held.append(lock)
        lock.set_generation(8)
        assert probe(db) == DaemonInfo(os.getpid(), 8)
        lock.set_generation(9)
        assert probe(db).generation == 9

    def test_a_shorter_value_truncates_the_old_tail(self, db, lockfile, held):
        lock = acquire(db, generation=123456789)
        held.append(lock)
        lock.set_generation(5)
        assert read(lockfile) == f"{os.getpid()}\n5\n".encode()

    def test_none_clears_the_generation(self, db, lockfile, held):
        lock = acquire(db, generation=5)
        held.append(lock)
        lock.set_generation(None)
        assert read(lockfile) == f"{os.getpid()}\n\n".encode()
        assert probe(db).generation is None

    def test_zero_is_a_real_generation_not_unset(self, db, held):
        lock = acquire(db)
        held.append(lock)
        lock.set_generation(0)
        assert probe(db).generation == 0

    def test_keeps_the_same_inode_and_the_lock(self, db, lockfile, held):
        lock = acquire(db)
        held.append(lock)
        ino = lockfile.stat().st_ino
        lock.set_generation(3)
        assert lockfile.stat().st_ino == ino
        with pytest.raises(DaemonAlreadyRunning):
            acquire(db)

    def test_never_leaves_an_empty_window(self, db, lockfile, held, monkeypatch):
        """Overwrite first, trim second: a reader between the two syscalls
        sees a full (if over-long) record, never an empty file."""
        lock = acquire(db, generation=99999)
        held.append(lock)
        seen = []
        real_ftruncate = os.ftruncate

        def spy(fd, n):
            seen.append(read(lockfile))
            return real_ftruncate(fd, n)

        monkeypatch.setattr(daemonlock.os, "ftruncate", spy)
        lock.set_generation(1)
        assert seen and all(content for content in seen)
        assert seen[0].startswith(f"{os.getpid()}\n1\n".encode())

    def test_a_write_failure_is_logged_not_raised(self, db, held, monkeypatch, caplog):
        lock = acquire(db)
        held.append(lock)

        def boom(fd, data, off):
            raise OSError(errno.EIO, "io error")

        monkeypatch.setattr(daemonlock.os, "pwrite", boom)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemonlock"):
            lock.set_generation(4)  # must not raise
        assert "could not record generation" in caplog.text
        assert lock.generation == 4

    def test_after_release_it_is_a_harmless_noop(self, db, lockfile):
        lock = acquire(db)
        lock.release()
        lock.set_generation(5)
        assert not lockfile.exists()


# ---------------------------------------------------------------------------
# O_NOFOLLOW
# ---------------------------------------------------------------------------


class TestSymlinkRefused:
    def test_a_symlink_at_the_lock_path_is_refused(self, db, lockfile, tmp_path):
        target = tmp_path / "victim"
        target.write_text("precious")
        lockfile.symlink_to(target)
        with pytest.raises(DaemonLockError):
            acquire(db)
        assert target.read_text() == "precious", "must never write through the link"

    def test_a_dangling_symlink_is_refused_and_not_created_through(self, db, lockfile, tmp_path):
        target = tmp_path / "does-not-exist"
        lockfile.symlink_to(target)
        with pytest.raises(DaemonLockError):
            acquire(db)
        assert not target.exists(), "a dangling link must not be created through"
        assert lockfile.is_symlink()

    def test_probe_refuses_a_symlink(self, db, lockfile, tmp_path):
        target = tmp_path / "victim"
        target.write_text("1\n\n")
        lockfile.symlink_to(target)
        with pytest.raises(DaemonLockError):
            probe(db)

    def test_the_refused_symlink_is_not_unlinked(self, db, lockfile, tmp_path):
        target = tmp_path / "victim"
        target.write_text("x")
        lockfile.symlink_to(target)
        with pytest.raises(DaemonLockError):
            acquire(db)
        assert lockfile.is_symlink() and target.exists()


# ---------------------------------------------------------------------------
# the unlink race
# ---------------------------------------------------------------------------


class TestUnlinkRace:
    """The previous holder unlinks the file *before* closing it. A process that
    opened it just before that holds a lock on an unlinked inode, while a third
    process may lock a fresh file at the same path. Only a lock on the inode the
    path currently names counts."""

    def test_replaced_after_lock_is_retried_on_the_new_file(self, db, lockfile, held, monkeypatch):
        lockfile.write_text("1\n\n")
        real_stat = os.stat
        calls = []

        def racing_stat(path, *a, **kw):
            if str(path) == str(lockfile) and not calls:
                calls.append(1)
                # A successor released (unlink) and a third process re-created it.
                os.unlink(lockfile)
                lockfile.write_text("2\n\n")
            elif str(path) == str(lockfile):
                calls.append(1)
            return real_stat(path, *a, **kw)

        monkeypatch.setattr(daemonlock.os, "stat", racing_stat)
        before = fd_count()
        lock = acquire(db, generation=6)
        held.append(lock)
        assert len(calls) == 2, "first attempt rejected, second accepted"
        st_fd = os.fstat(lock._fd)
        st_path = real_stat(lockfile)
        assert (st_fd.st_dev, st_fd.st_ino) == (st_path.st_dev, st_path.st_ino)
        assert read(lockfile) == f"{os.getpid()}\n6\n".encode()
        assert fd_count() == before + 1, "the abandoned descriptor must be closed"
        assert lock.held

    def test_unlinked_after_lock_is_retried(self, db, lockfile, held, monkeypatch):
        lockfile.write_text("1\n\n")
        real_stat = os.stat
        calls = []

        def racing_stat(path, *a, **kw):
            if str(path) == str(lockfile):
                calls.append(1)
                if len(calls) == 1:
                    os.unlink(lockfile)
            return real_stat(path, *a, **kw)

        monkeypatch.setattr(daemonlock.os, "stat", racing_stat)
        before = fd_count()
        lock = acquire(db)
        held.append(lock)
        assert len(calls) == 2
        assert lockfile.exists()
        assert fd_count() == before + 1
        assert probe(db).pid == os.getpid()

    def test_the_retry_is_bounded(self, db, lockfile, monkeypatch):
        lockfile.write_text("1\n\n")
        real_stat = os.stat
        calls = []

        def always_replaced(path, *a, **kw):
            if str(path) == str(lockfile):
                calls.append(1)
                os.unlink(lockfile)
                lockfile.write_text("x\n\n")
            return real_stat(path, *a, **kw)

        monkeypatch.setattr(daemonlock.os, "stat", always_replaced)
        before = fd_count()
        with pytest.raises(DaemonLockError) as exc:
            acquire(db)
        assert len(calls) == daemonlock._ACQUIRE_ATTEMPTS == 5
        assert exc.value.errno == errno.EAGAIN
        assert fd_count() == before, "every abandoned descriptor closed"

    def test_open_losing_the_race_with_release_is_retried(self, db, held, monkeypatch):
        real = daemonlock._open_for_lock
        calls = []

        def flaky(path):
            calls.append(1)
            if len(calls) <= 2:
                raise FileNotFoundError(errno.ENOENT, "gone", str(path))
            return real(path)

        monkeypatch.setattr(daemonlock, "_open_for_lock", flaky)
        held.append(acquire(db))
        assert len(calls) == 3

    def test_open_that_keeps_losing_the_race_gives_up(self, db, monkeypatch):
        calls = []

        def always_gone(path):
            calls.append(1)
            raise FileNotFoundError(errno.ENOENT, "gone", str(path))

        monkeypatch.setattr(daemonlock, "_open_for_lock", always_gone)
        with pytest.raises(DaemonLockError):
            acquire(db)
        assert len(calls) == 5


# ---------------------------------------------------------------------------
# permissions
# ---------------------------------------------------------------------------


def mode_of(path):
    return stat.S_IMODE(path.stat().st_mode)


class TestPermissions:
    def test_created_0660_regardless_of_umask(self, db, lockfile, held):
        old = os.umask(0o077)
        try:
            held.append(acquire(db))
        finally:
            os.umask(old)
        assert mode_of(lockfile) == 0o660

    def test_created_0660_under_a_normal_umask(self, db, lockfile, held):
        old = os.umask(0o022)
        try:
            held.append(acquire(db))
        finally:
            os.umask(old)
        assert mode_of(lockfile) == 0o660

    def test_an_owned_file_with_the_wrong_mode_is_fixed(self, db, lockfile, held):
        lockfile.write_text("1\n\n")
        lockfile.chmod(0o600)
        held.append(acquire(db))
        assert mode_of(lockfile) == 0o660

    def test_an_owned_file_with_the_right_mode_is_not_chmodded(self, db, lockfile, held, monkeypatch):
        lockfile.write_text("1\n\n")
        lockfile.chmod(0o660)
        calls = []
        monkeypatch.setattr(daemonlock.os, "fchmod", lambda fd, m: calls.append(m))
        held.append(acquire(db))
        assert calls == []

    def test_a_file_we_do_not_own_is_never_chmodded(self, db, lockfile, held, monkeypatch):
        lockfile.write_text("1\n\n")
        lockfile.chmod(0o600)
        monkeypatch.setattr(daemonlock.os, "geteuid", lambda: os.getuid() + 4242)
        calls = []
        monkeypatch.setattr(daemonlock.os, "fchmod", lambda fd, m: calls.append(m))
        held.append(acquire(db))
        assert calls == []
        assert mode_of(lockfile) == 0o600

    def test_root_chmods_a_file_it_does_not_own(self, db, lockfile, held, monkeypatch):
        lockfile.write_text("1\n\n")
        lockfile.chmod(0o600)
        monkeypatch.setattr(daemonlock.os, "geteuid", lambda: 0)
        monkeypatch.setattr(daemonlock, "is_default_config_path", lambda p: False)
        calls = []
        real = os.fchmod
        monkeypatch.setattr(daemonlock.os, "fchmod", lambda fd, m: (calls.append(m), real(fd, m)))
        held.append(acquire(db))
        assert calls == [0o660]

    def test_a_chmod_failure_is_a_warning_not_a_failure(self, db, lockfile, held, monkeypatch, caplog):
        lockfile.write_text("1\n\n")
        lockfile.chmod(0o600)

        def boom(fd, mode):
            raise PermissionError(errno.EPERM, "nope")

        monkeypatch.setattr(daemonlock.os, "fchmod", boom)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemonlock"):
            lock = acquire(db)
        held.append(lock)
        assert lock.held
        assert "could not fix permissions" in caplog.text

    @pytest.mark.skipif(IS_ROOT, reason="root reads any file")
    def test_probe_works_on_a_read_only_file(self, db, lockfile, held):
        """`flock` needs no particular open mode, so a group operator with
        read-only access can still probe."""
        lock = acquire(db, generation=2)
        held.append(lock)
        lockfile.chmod(0o440)
        assert probe(db) == DaemonInfo(os.getpid(), 2)

    @pytest.mark.skipif(IS_ROOT, reason="root reads any file")
    def test_probe_of_a_stale_read_only_file_is_none(self, db, lockfile):
        lockfile.write_text("1\n\n")
        lockfile.chmod(0o440)
        assert probe(db) is None

    @pytest.mark.skipif(IS_ROOT, reason="root bypasses file permissions")
    def test_probe_of_an_unreadable_file_is_a_lock_error(self, db, lockfile):
        lockfile.write_text("1\n\n")
        lockfile.chmod(0o000)
        try:
            with pytest.raises(DaemonLockError) as exc:
                probe(db)
            assert exc.value.errno == errno.EACCES
        finally:
            lockfile.chmod(0o600)

    @pytest.mark.skipif(IS_ROOT, reason="root bypasses file permissions")
    def test_acquire_of_an_unwritable_file_is_a_lock_error(self, db, lockfile):
        lockfile.write_text("1\n\n")
        lockfile.chmod(0o440)
        before = fd_count()
        try:
            with pytest.raises(DaemonLockError) as exc:
                acquire(db)
            assert exc.value.errno == errno.EACCES
            assert str(lockfile) in str(exc.value)
        finally:
            lockfile.chmod(0o600)
        assert fd_count() == before

    @pytest.mark.skipif(IS_ROOT, reason="root bypasses file permissions")
    def test_acquire_in_an_unwritable_directory_is_a_lock_error(self, tmp_path):
        d = tmp_path / "ro"
        d.mkdir()
        d.chmod(0o500)
        try:
            with pytest.raises(DaemonLockError) as exc:
                acquire(d / "config.db")
            assert exc.value.errno == errno.EACCES
        finally:
            d.chmod(0o700)


class TestRootGroupHandling:
    """Root creating the pid file next to the *default* database gives it the
    `zfsbackup` group, like the database itself."""

    @pytest.fixture
    def as_root(self, monkeypatch):
        monkeypatch.setattr(daemonlock.os, "geteuid", lambda: 0)
        chowns = []
        monkeypatch.setattr(daemonlock.os, "fchown", lambda fd, u, g: chowns.append((u, g)))
        return chowns

    def test_root_creating_next_to_the_default_db_chgrps(self, db, held, monkeypatch, as_root):
        monkeypatch.setattr(daemonlock, "is_default_config_path", lambda p: True)
        monkeypatch.setattr(daemonlock, "_service_gid", lambda: 4242)
        held.append(acquire(db))
        assert as_root == [(-1, 4242)]

    def test_root_with_a_missing_group_warns_and_does_not_chown(
        self, db, held, monkeypatch, as_root, caplog
    ):
        monkeypatch.setattr(daemonlock, "is_default_config_path", lambda p: True)
        monkeypatch.setattr(daemonlock, "_service_gid", lambda: None)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemonlock"):
            lock = acquire(db)
        held.append(lock)
        assert as_root == []
        assert "group does not exist" in caplog.text
        assert lock.held

    def test_root_next_to_a_non_default_db_does_not_chgrp(self, db, held, monkeypatch, as_root):
        monkeypatch.setattr(daemonlock, "is_default_config_path", lambda p: False)
        monkeypatch.setattr(daemonlock, "_service_gid", lambda: 4242)
        held.append(acquire(db))
        assert as_root == []

    def test_root_does_not_chgrp_a_file_it_did_not_create(
        self, db, lockfile, held, monkeypatch, as_root
    ):
        lockfile.write_text("1\n\n")
        monkeypatch.setattr(daemonlock, "is_default_config_path", lambda p: True)
        monkeypatch.setattr(daemonlock, "_service_gid", lambda: 4242)
        held.append(acquire(db))
        assert as_root == []

    def test_non_root_never_chgrps(self, db, held, monkeypatch):
        monkeypatch.setattr(daemonlock.os, "geteuid", lambda: os.getuid() + 4242)
        monkeypatch.setattr(daemonlock, "is_default_config_path", lambda p: True)
        monkeypatch.setattr(daemonlock, "_service_gid", lambda: 4242)
        chowns = []
        monkeypatch.setattr(daemonlock.os, "fchown", lambda fd, u, g: chowns.append(g))
        held.append(acquire(db))
        assert chowns == []

    def test_a_chown_failure_is_a_warning_not_a_failure(self, db, held, monkeypatch, caplog):
        monkeypatch.setattr(daemonlock.os, "geteuid", lambda: 0)
        monkeypatch.setattr(daemonlock, "is_default_config_path", lambda p: True)
        monkeypatch.setattr(daemonlock, "_service_gid", lambda: 4242)

        def boom(fd, u, g):
            raise PermissionError(errno.EPERM, "nope")

        monkeypatch.setattr(daemonlock.os, "fchown", boom)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemonlock"):
            lock = acquire(db)
        held.append(lock)
        assert lock.held

    def test_service_gid_is_none_when_the_group_is_absent(self, monkeypatch):
        import grp

        def missing(name):
            raise KeyError(name)

        monkeypatch.setattr(grp, "getgrnam", missing)
        assert daemonlock._service_gid() is None

    def test_service_gid_looks_up_the_zfsbackup_group(self, monkeypatch):
        import grp
        from types import SimpleNamespace

        seen = []

        def lookup(name):
            seen.append(name)
            return SimpleNamespace(gr_gid=777)

        monkeypatch.setattr(grp, "getgrnam", lookup)
        assert daemonlock._service_gid() == 777
        assert seen == ["zfsbackup"]


# ---------------------------------------------------------------------------
# release
# ---------------------------------------------------------------------------


class TestRelease:
    def test_removes_the_file_and_drops_the_lock(self, db, lockfile):
        lock = acquire(db)
        lock.release()
        assert not lockfile.exists()
        assert not lock.held
        assert probe(db) is None

    def test_closes_the_descriptor(self, db):
        before = fd_count()
        lock = acquire(db)
        assert fd_count() == before + 1
        lock.release()
        assert fd_count() == before

    def test_is_idempotent(self, db, lockfile):
        lock = acquire(db)
        lock.release()
        lock.release()
        lock.release()
        assert not lockfile.exists()

    def test_a_second_release_does_not_touch_a_successors_file(self, db, lockfile, held):
        first = acquire(db)
        first.release()
        second = acquire(db, generation=2)
        held.append(second)
        first.release()  # stale handle: must be a no-op
        assert lockfile.exists()
        assert probe(db) == DaemonInfo(os.getpid(), 2)

    def test_does_not_unlink_a_file_that_replaced_ours(self, db, lockfile):
        """Inode check: if the path no longer names our file, leave it."""
        lock = acquire(db)
        os.unlink(lockfile)
        lockfile.write_text("someone else\n")
        lock.release()
        assert read(lockfile) == b"someone else\n"
        assert not lock.held

    def test_tolerates_the_file_having_been_removed(self, db, lockfile):
        lock = acquire(db)
        os.unlink(lockfile)
        lock.release()  # must not raise
        assert not lock.held

    def test_an_unlink_failure_is_a_warning_and_still_closes(self, db, caplog, monkeypatch):
        lock = acquire(db)
        fd = lock._fd

        def boom(p):
            raise PermissionError(errno.EACCES, "no")

        monkeypatch.setattr(daemonlock.os, "unlink", boom)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemonlock"):
            lock.release()
        assert "could not remove" in caplog.text
        with pytest.raises(OSError):
            os.fstat(fd)  # descriptor was closed regardless
        assert probe(db) is None
        # clean up the leftover file
        monkeypatch.undo()
        os.unlink(lock.path)

    def test_unlinks_while_the_lock_is_still_held(self, db, lockfile, monkeypatch):
        """Unlink first, close second: a successor can never lock a file that
        is about to be deleted."""
        lock = acquire(db)
        seen = []
        real_unlink = os.unlink

        def spy(p):
            seen.append(probe(db))  # lock must still be visible here
            return real_unlink(p)

        monkeypatch.setattr(daemonlock.os, "unlink", spy)
        lock.release()
        assert seen and seen[0] is not None

    @pytest.mark.skipif(not HAS_FORK, reason="needs fork")
    def test_a_forked_child_cannot_release_the_parents_lock(self, db, lockfile, held):
        lock = acquire(db, generation=3)
        held.append(lock)
        r, w = os.pipe()
        pid = os.fork()
        if pid == 0:  # pragma: no cover - child
            code = 1
            try:
                was_held = lock.held
                lock.release()
                lock.release()
                lock.set_generation(999)  # must not rewrite the parent's record
                os.write(w, b"held=%d" % was_held)
                code = 0
            finally:
                os._exit(code)
        os.close(w)
        out = b""
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            chunk = os.read(r, 100)
            if not chunk:
                break
            out += chunk
        os.close(r)
        _, status = os.waitpid(pid, 0)
        assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
        assert out == b"held=0", "the lock must not claim to be held in a child"
        assert lockfile.exists()
        assert read(lockfile) == f"{os.getpid()}\n3\n".encode()
        assert lock.held
        assert probe(db) is not None
        # ...and the parent still can.
        lock.release()
        assert not lockfile.exists()

    def test_held_is_false_for_a_different_pid(self, db, held, monkeypatch):
        lock = acquire(db)
        held.append(lock)
        assert lock.held
        real_pid = os.getpid()
        monkeypatch.setattr(daemonlock.os, "getpid", lambda: real_pid + 1)
        assert not lock.held


# ---------------------------------------------------------------------------
# context manager
# ---------------------------------------------------------------------------


class TestContextManager:
    def test_enter_returns_the_lock(self, db):
        with acquire(db) as lock:
            assert lock.held

    def test_a_clean_exit_releases_and_removes_the_file(self, db, lockfile):
        with acquire(db):
            assert lockfile.exists()
        assert not lockfile.exists()
        assert probe(db) is None

    def test_an_exception_leaves_the_file_like_a_crash_would(self, db, lockfile):
        with pytest.raises(RuntimeError):
            with acquire(db, generation=3):
                raise RuntimeError("boom")
        assert lockfile.exists()
        assert read(lockfile) == f"{os.getpid()}\n3\n".encode()

    def test_an_exception_still_drops_the_lock(self, db):
        with pytest.raises(RuntimeError):
            with acquire(db):
                raise RuntimeError("boom")
        assert probe(db) is None, "stale file: the lock itself must be gone"
        acquire(db).release()

    def test_an_exception_closes_the_descriptor(self, db, lockfile):
        before = fd_count()
        with pytest.raises(RuntimeError):
            with acquire(db):
                raise RuntimeError("boom")
        assert fd_count() == before

    def test_an_exception_does_not_swallow_it(self, db):
        with pytest.raises(KeyError):
            with acquire(db):
                raise KeyError("k")

    def test_a_stale_file_left_by_an_exception_is_taken_over(self, db, lockfile):
        with pytest.raises(RuntimeError):
            with acquire(db):
                raise RuntimeError("boom")
        with acquire(db, generation=1):
            assert read(lockfile) == f"{os.getpid()}\n1\n".encode()
        assert not lockfile.exists()

    def test_release_inside_the_block_then_clean_exit_is_fine(self, db, lockfile):
        with acquire(db) as lock:
            lock.release()
        assert not lockfile.exists()


# ---------------------------------------------------------------------------
# the error types
# ---------------------------------------------------------------------------


class TestDaemonAlreadyRunning:
    def test_message_names_the_pid(self, tmp_path):
        err = DaemonAlreadyRunning(tmp_path / "x.pid", 4321, 7)
        assert "pid 4321" in str(err)
        assert err.pid == 4321 and err.generation == 7 and err.path == tmp_path / "x.pid"

    def test_unknown_pid_message(self, tmp_path):
        err = DaemonAlreadyRunning(tmp_path / "x.pid", None)
        assert "unknown pid" in str(err)
        assert err.generation is None

    def test_daemoninfo_is_immutable(self):
        info = DaemonInfo(1, 2)
        with pytest.raises(Exception):
            info.pid = 5

    def test_a_live_pid_gets_the_ordinary_message(self, tmp_path):
        path = tmp_path / "x.pid"
        err = DaemonAlreadyRunning(path, os.getpid(), 7)
        assert err.pid_alive is True
        assert str(err) == (
            f"another zfsbackup daemon (pid {os.getpid()}) is already running against "
            f"this configuration database (lock held on {path})"
        )
        assert "gone" not in str(err)

    def test_a_dead_pid_gets_the_stale_supervisor_message(self, tmp_path, monkeypatch):
        path = tmp_path / "x.pid"
        monkeypatch.setattr(daemonlock.os, "kill", _raise(ProcessLookupError()))
        err = DaemonAlreadyRunning(path, 4321, 7)
        assert err.pid_alive is False
        assert str(err) == (
            f"supervisor pid 4321 is gone, but processes it forked still hold the "
            f"daemon lock {path}; find them with `fuser {path}` or `lsof {path}`"
        )
        assert str(err) == daemonlock.stale_supervisor_message(path, 4321)
        assert "already running" not in str(err)

    def test_an_unknowable_pid_keeps_the_ordinary_message(self, tmp_path, monkeypatch):
        monkeypatch.setattr(daemonlock.os, "kill", _raise(OSError(errno.EINVAL, "x")))
        assert "is already running" in str(DaemonAlreadyRunning(tmp_path / "x.pid", 4321))
        assert "is already running" in str(DaemonAlreadyRunning(tmp_path / "x.pid", None))
        assert DaemonAlreadyRunning(tmp_path / "x.pid", None).pid_alive is None

    def test_pid_alive_is_evaluated_when_asked_not_when_constructed(
        self, tmp_path, monkeypatch
    ):
        err = DaemonAlreadyRunning(tmp_path / "x.pid", 4321)
        monkeypatch.setattr(daemonlock.os, "kill", _raise(ProcessLookupError()))
        assert err.pid_alive is False
        monkeypatch.setattr(daemonlock.os, "kill", lambda *a: None)
        assert err.pid_alive is True

    def test_daemoninfo_pid_alive_follows_pid_is_alive(self, monkeypatch):
        monkeypatch.setattr(daemonlock.os, "kill", _raise(ProcessLookupError()))
        assert DaemonInfo(pid=4321, generation=None).pid_alive is False
        monkeypatch.setattr(daemonlock.os, "kill", lambda *a: None)
        assert DaemonInfo(pid=4321, generation=None).pid_alive is True
        assert DaemonInfo(pid=None, generation=3).pid_alive is None

    def test_daemoninfo_has_no_pid_alive_field(self):
        import dataclasses

        assert [f.name for f in dataclasses.fields(DaemonInfo)] == ["pid", "generation"]
        assert DaemonInfo(1, 2) == DaemonInfo(pid=1, generation=2)


def _raise(exc):
    def fn(*args, **kwargs):
        raise exc

    return fn


class TestPidIsAlive:
    """`pid_is_alive(pid) -> Optional[bool]`: informational only. Never probes
    `kill(0, ...)` (the caller's whole process group) or a negative pid."""

    def test_this_process_is_alive(self):
        assert daemonlock.pid_is_alive(os.getpid()) is True

    def test_a_reaped_child_is_not(self):
        import subprocess

        proc = subprocess.Popen(["true"])
        proc.wait()
        assert daemonlock.pid_is_alive(proc.pid) is False

    @pytest.mark.parametrize("pid", [None, 0, -1, -4321])
    def test_a_non_pid_is_unknown_and_never_probed(self, monkeypatch, pid):
        monkeypatch.setattr(
            daemonlock.os, "kill",
            lambda *a: pytest.fail(f"kill{a} must never be called for pid {pid!r}"),
        )
        assert daemonlock.pid_is_alive(pid) is None

    @pytest.mark.parametrize(
        "effect, expected",
        [
            (None, True),
            (ProcessLookupError(errno.ESRCH, "no such process"), False),
            (PermissionError(errno.EPERM, "not permitted"), True),
            (OSError(errno.EINVAL, "invalid"), None),
            (OSError(errno.EIO, "io"), None),
        ],
        ids=["ok", "ESRCH", "EPERM", "EINVAL", "EIO"],
    )
    def test_the_kill_zero_matrix(self, monkeypatch, effect, expected):
        calls = []

        def fake_kill(pid, sig):
            calls.append((pid, sig))
            if effect is not None:
                raise effect

        monkeypatch.setattr(daemonlock.os, "kill", fake_kill)
        assert daemonlock.pid_is_alive(4321) is expected
        assert calls == [(4321, 0)]  # signal 0: existence only, nothing is delivered

    def test_the_stale_supervisor_message_text(self, tmp_path):
        path = tmp_path / "config.db.daemon.pid"
        assert daemonlock.stale_supervisor_message(path, 77) == (
            f"supervisor pid 77 is gone, but processes it forked still hold the daemon "
            f"lock {path}; find them with `fuser {path}` or `lsof {path}`"
        )

    @pytest.mark.skipif(not HAS_FORK, reason="needs fork")
    def test_an_orphaned_worker_makes_acquire_report_the_dead_supervisor(
        self, db, daemon_lock_holder
    ):
        """End to end: kill -9 the supervisor, keep its forked worker alive.
        The lock is still held, the recorded pid is dead, and the refusal says
        so instead of claiming a daemon is running."""
        holder = daemon_lock_holder(db, generation=4, fork=True)
        holder.kill9()
        info = probe(db)
        assert info is not None and info.pid == holder.pid
        assert wait_until(lambda: info.pid_alive is False)
        with pytest.raises(DaemonAlreadyRunning) as exc:
            acquire(db)
        assert exc.value.pid_alive is False
        assert f"supervisor pid {holder.pid} is gone" in str(exc.value)
        assert f"fuser {lock_path(db)}" in str(exc.value)

    def test_a_live_holder_makes_acquire_report_a_running_daemon(
        self, db, daemon_lock_holder
    ):
        holder = daemon_lock_holder(db, generation=4)
        with pytest.raises(DaemonAlreadyRunning) as exc:
            acquire(db)
        assert exc.value.pid_alive is True
        assert f"another zfsbackup daemon (pid {holder.pid}) is already running" in str(exc.value)
        assert probe(db).pid_alive is True


# ---------------------------------------------------------------------------
# Contention window: `acquire` retries a contended LOCK_EX|LOCK_NB for 1 s
# (25 ms apart) before concluding a daemon really holds it, because the CLI's
# `probe()` holds LOCK_SH for microseconds and would otherwise make a starting
# daemon refuse itself, naming a stale pid. Plus the missing-directory message.
#
# Mutation counts (scratch copy outside the repo, one change at a time, tests
# failing in this file): no retry at all (`if False:` in place of the deadline
# check) 4; `_CONTENTION_WINDOW = 0.1` 5; the missing-directory check removed
# 3 (five attempts and the generic message instead).
# ---------------------------------------------------------------------------


class _SleepSpy:
    """Counts the real `time.sleep` calls `acquire` makes in its retry loop."""

    def __init__(self, monkeypatch):
        self.calls = []
        real = time.sleep

        def spy(seconds):
            self.calls.append(seconds)
            real(seconds)

        monkeypatch.setattr(daemonlock.time, "sleep", spy)


def _transient_holder(path, mode, hold_for):
    """A thread that takes `flock(mode)` on its OWN descriptor for `hold_for`
    seconds. Different open file descriptions contend even within one
    process, exactly as a CLI `probe()` would. Returns (thread, ready, stop);
    `stop` is None for a fixed-duration hold."""
    import threading

    ready = threading.Event()
    stop = threading.Event()

    def run():
        fd = os.open(str(path), os.O_RDONLY)
        try:
            fcntl.flock(fd, mode)
            ready.set()
            stop.wait(hold_for)
        finally:
            os.close(fd)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    assert ready.wait(10), "holder thread never got the lock"
    return thread, stop


class TestContentionWindow:
    def test_production_constants_are_one_second_and_25ms(self, _short_daemonlock_contention_window):
        """The suite shrinks the window for speed (conftest); this pins what
        production runs with."""
        assert _short_daemonlock_contention_window == 1.0
        assert daemonlock._CONTENTION_POLL == 0.025

    def test_a_transient_shared_holder_released_mid_window_does_not_refuse_the_daemon(
        self, db, lockfile, held, real_contention_window, monkeypatch
    ):
        """The CLI's `probe()` (LOCK_SH, microseconds) landing exactly as the
        daemon starts must not be read as "another daemon". Here the holder
        lets go after ~0.3 s: `acquire` waits it out and succeeds."""
        lockfile.write_text("")  # contention needs the file to exist
        spy = _SleepSpy(monkeypatch)
        thread, stop = _transient_holder(lockfile, fcntl.LOCK_SH, 0.3)

        started = time.monotonic()
        lock = acquire(db, generation=4)
        elapsed = time.monotonic() - started
        held.append(lock)
        thread.join(5)

        assert lock.held
        assert 0.25 <= elapsed < 1.0, elapsed  # waited for the holder, not the whole window
        assert len(spy.calls) >= 5
        assert all(c == daemonlock._CONTENTION_POLL for c in spy.calls)
        assert read(lockfile) == f"{os.getpid()}\n4\n".encode()
        assert probe(db) == DaemonInfo(os.getpid(), 4)

    def test_a_transient_exclusive_holder_released_mid_window_also_succeeds(
        self, db, lockfile, held, real_contention_window
    ):
        lockfile.write_text("")
        thread, stop = _transient_holder(lockfile, fcntl.LOCK_EX, 0.3)
        started = time.monotonic()
        held.append(acquire(db))
        assert 0.25 <= time.monotonic() - started < 1.0
        thread.join(5)

    @pytest.mark.parametrize("mode", [fcntl.LOCK_EX, fcntl.LOCK_SH], ids=["exclusive", "shared"])
    def test_a_persistent_holder_is_refused_after_about_one_second(
        self, db, lockfile, held, real_contention_window, monkeypatch, mode
    ):
        lockfile.write_text("4242\n8\n")
        spy = _SleepSpy(monkeypatch)
        thread, stop = _transient_holder(lockfile, mode, 30)
        try:
            started = time.monotonic()
            with pytest.raises(DaemonAlreadyRunning) as exc:
                acquire(db)
            elapsed = time.monotonic() - started
        finally:
            stop.set()
            thread.join(5)

        assert 0.95 <= elapsed < 3.0, elapsed
        assert exc.value.pid == 4242 and exc.value.generation == 8
        # ~1.0 / 0.025 polls; allow scheduler slack either way.
        assert 20 <= len(spy.calls) <= 41, len(spy.calls)

    def test_an_uncontended_acquire_never_sleeps(self, db, held, monkeypatch):
        spy = _SleepSpy(monkeypatch)
        held.append(acquire(db))
        assert spy.calls == []

    def test_only_contention_is_retried(self, db, lockfile, monkeypatch):
        """Any other `flock` failure is a lock error at once -- not a second
        of pointless polling."""
        spy = _SleepSpy(monkeypatch)

        def boom(fd, op):
            raise OSError(errno.EIO, "I/O error")

        monkeypatch.setattr(daemonlock.fcntl, "flock", boom)
        started = time.monotonic()
        with pytest.raises(DaemonLockError) as exc:
            acquire(db)
        assert time.monotonic() - started < 0.5
        assert exc.value.errno == errno.EIO
        assert spy.calls == []

    def test_a_refused_retry_loop_leaks_no_descriptor_and_keeps_the_holders_file(
        self, db, lockfile, held
    ):
        held.append(acquire(db, generation=6))
        before_fds, before = fd_count(), read(lockfile)
        with pytest.raises(DaemonAlreadyRunning):
            acquire(db)
        assert fd_count() == before_fds
        assert read(lockfile) == before


class TestMissingDirectoryMessage:
    def test_missing_parent_directory_is_named_in_an_enoent_lock_error(self, tmp_path):
        target = tmp_path / "nodir" / "config.db"
        with pytest.raises(DaemonLockError) as exc:
            acquire(target)
        err = exc.value
        assert err.errno == errno.ENOENT
        assert "the directory" in str(err)
        assert str(tmp_path / "nodir") in str(err)
        assert "does not exist" in str(err)
        assert str(lock_path(target)) in str(err)
        assert not (tmp_path / "nodir").exists()  # nothing was created

    def test_it_fails_on_the_first_attempt_not_after_five(self, tmp_path, monkeypatch):
        """ENOENT with an existing directory means "lost a race with the
        previous daemon's release" and is retried (`_ACQUIRE_ATTEMPTS`); with
        a missing directory it can never work, so say so at once."""
        calls = []
        real = daemonlock._open_for_lock

        def counting(path):
            calls.append(path)
            return real(path)

        monkeypatch.setattr(daemonlock, "_open_for_lock", counting)
        started = time.monotonic()
        with pytest.raises(DaemonLockError):
            acquire(tmp_path / "nodir" / "config.db")
        assert len(calls) == 1
        assert time.monotonic() - started < 0.5

    def test_a_deleted_directory_is_reported_the_same_way(self, tmp_path):
        gone = tmp_path / "was-here"
        gone.mkdir()
        gone.rmdir()
        with pytest.raises(DaemonLockError, match="does not exist"):
            acquire(gone / "config.db")

    def test_enoent_with_an_existing_directory_is_still_retried(self, db, held, monkeypatch):
        """The other half of the split: the message is for a missing
        directory only; a vanishing *file* keeps the bounded retry."""
        real = daemonlock._open_for_lock
        calls = []

        def flaky(path):
            calls.append(path)
            if len(calls) < 3:
                raise FileNotFoundError(errno.ENOENT, "gone", str(path))
            return real(path)

        monkeypatch.setattr(daemonlock, "_open_for_lock", flaky)
        held.append(acquire(db))
        assert len(calls) == 3


# ---------------------------------------------------------------------------
# The real `python -m zfsbackup.daemon` entry point, end to end.
#
# Shape follows tests/test_zfsbackup_stop_signals.py: the daemon's output goes
# to a file (never a PIPE: an orphaned worker holding the write end would hang
# `communicate()`), it runs in its own session so teardown can `killpg` the
# whole tree, and every wait has a hard timeout.
# ---------------------------------------------------------------------------

import shutil
import socket
import subprocess
import sys
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
STARTUP_TIMEOUT = 60.0
SHUTDOWN_TIMEOUT = 30.0


def _killpg_quiet(pid, sig=signal.SIGKILL):
    """Signal the whole process group of a daemon started with
    `start_new_session=True`. Its pgid IS its pid, so no `getpgid()` lookup: that
    raises `ProcessLookupError` once the leader has been reaped (`wait()`), which
    is exactly when the workers still need killing -- they are orphans in a group
    whose leader is gone, and `killpg` on the recorded pgid still reaches them."""
    try:
        os.killpg(pid, sig)
    except (ProcessLookupError, PermissionError):
        pass


@pytest.fixture
def real_daemon_env(tmp_path, monkeypatch):
    """An imported config database (one nonexistent dataset, free API port) and
    a `zfs` that always succeeds, so a `--dry-run` daemon starts all workers
    without any pool."""
    from zfsbackup.config.store import import_yaml

    from _hermetic import hermetic_env

    # a stub, never `which("zfs")`: a --dry-run daemon still runs `zfs set`
    env = hermetic_env()
    monkeypatch.setenv("ZFS_CMD", env["ZFS_CMD"])
    monkeypatch.setenv("ZPOOL_CMD", env["ZPOOL_CMD"])

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        "datasets:\n"
        f"  - name: zfsbackup_locktest_{uuid.uuid4().hex[:8]}/nonexistent\n"
        "api_host: 127.0.0.1\n"
        f"api_port: {port}\n"
    )
    resolved = resolve_config_path(str(tmp_path / "config.db"))
    import_yaml(cfg, resolved)
    from zfsbackup.config.store import dispose_all

    dispose_all()
    return resolved


@pytest.fixture
def spawn_daemon(tmp_path, real_daemon_env):
    """Factory for real daemon processes against `real_daemon_env`, each in its
    own session and each SIGKILLed (whole group) at teardown."""
    started = []

    def start(*extra, name=None):
        log_path = tmp_path / (name or f"daemon{len(started)}.log")
        from _hermetic import hermetic_env

        env = hermetic_env(PYTHONUNBUFFERED="1")
        log_fh = open(log_path, "wb")
        proc = subprocess.Popen(
            [sys.executable, "-m", "zfsbackup.daemon", "-c", str(real_daemon_env.path),
             "--dry-run", *extra],
            stdout=log_fh, stderr=subprocess.STDOUT, cwd=str(REPO_ROOT), env=env,
            start_new_session=True,
        )
        started.append((proc, log_fh))
        return proc, log_path

    yield start
    for proc, log_fh in started:
        log_fh.close()
        _killpg_quiet(proc.pid)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass


def _log(path):
    try:
        return path.read_text(errors="replace")
    except FileNotFoundError:
        return ""


@pytest.mark.integration
@pytest.mark.slow
class TestRealDaemonProcess:
    def _wait_running(self, resolved, proc, log_path):
        """Lock taken AND the supervisor past `BackupDaemon.__init__` (which
        installs the SIGTERM/SIGINT handlers): the lock appears slightly
        earlier, and a signal in that gap kills the process by default
        disposition -- a test race, not a daemon defect."""

        def up():
            if proc.poll() is not None:
                pytest.fail(f"daemon exited early ({proc.returncode}):\n{_log(log_path)}")
            info = probe(resolved)
            return (
                info is not None
                and info.pid == proc.pid
                and "worker (pid=" in _log(log_path)
            )

        assert wait_until(up, STARTUP_TIMEOUT), f"daemon never came up:\n{_log(log_path)}"

    def test_second_daemon_is_refused_and_sigterm_removes_the_file(
        self, real_daemon_env, spawn_daemon
    ):
        resolved = real_daemon_env
        first, first_log = spawn_daemon()
        self._wait_running(resolved, first, first_log)
        record = lock_path(resolved).read_bytes()
        assert record.startswith(f"{first.pid}\n".encode())

        # A second daemon on the same database stops with exit 1, naming the first.
        second, second_log = spawn_daemon()
        assert second.wait(timeout=SHUTDOWN_TIMEOUT) == 1, _log(second_log)
        assert f"pid {first.pid}" in _log(second_log)
        assert "Refusing to start a second daemon" in _log(second_log)

        # ...without disturbing the first.
        assert first.poll() is None
        assert lock_path(resolved).read_bytes() == record
        assert probe(resolved).pid == first.pid

        # `--test-config` works beside a running daemon and does not touch the lock.
        checker, checker_log = spawn_daemon("--test-config")
        assert checker.wait(timeout=SHUTDOWN_TIMEOUT) == 0, _log(checker_log)
        assert "Configuration is valid!" in _log(checker_log)
        assert lock_path(resolved).read_bytes() == record

        # A clean stop removes the file and frees the lock.
        first.send_signal(signal.SIGTERM)
        assert first.wait(timeout=SHUTDOWN_TIMEOUT) == 0, _log(first_log)
        assert "Daemon stopped" in _log(first_log)
        assert not lock_path(resolved).exists()
        assert probe(resolved) is None

    def test_kill_9_leaves_a_stale_file_and_the_next_daemon_takes_over(
        self, real_daemon_env, spawn_daemon
    ):
        resolved = real_daemon_env
        first, first_log = spawn_daemon()
        self._wait_running(resolved, first, first_log)

        # kill -9 the supervisor, then its workers (whose descriptor handling
        # depends on the start method; orphan-under-fork is covered in
        # TestAcquireAgainstAnotherProcess).
        first.send_signal(signal.SIGKILL)
        first.wait(timeout=10)
        _killpg_quiet(first.pid)

        assert lock_path(resolved).exists(), "kill -9 leaves the file behind"
        assert wait_until(lambda: probe(resolved) is None, 15), "...but not the lock"

        second, second_log = spawn_daemon()
        self._wait_running(resolved, second, second_log)
        assert probe(resolved).pid == second.pid

        second.send_signal(signal.SIGTERM)
        assert second.wait(timeout=SHUTDOWN_TIMEOUT) == 0, _log(second_log)
        assert not lock_path(resolved).exists()

    def test_sigint_also_removes_the_file(self, real_daemon_env, spawn_daemon):
        first, first_log = spawn_daemon()
        self._wait_running(real_daemon_env, first, first_log)
        first.send_signal(signal.SIGINT)
        assert first.wait(timeout=SHUTDOWN_TIMEOUT) == 0, _log(first_log)
        assert not lock_path(real_daemon_env).exists()

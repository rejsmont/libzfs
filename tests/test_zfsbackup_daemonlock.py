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

import contextlib
import errno
import fcntl
import logging
import os
import signal
import stat
import threading
import time

import pytest

from zfsbackup import daemonlock, procident
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


def assert_v2(raw: bytes, pid, generation):
    """`raw` is pid-file v2: the unchanged `pid\\ngen\\n` prefix, then a `start=`
    line (always written by a v2 daemon), and parses back to the same values."""
    prefix = f"{pid}\n{'' if generation is None else generation}\n".encode()
    assert raw.startswith(prefix), raw
    assert raw[len(prefix):].startswith(b"start="), raw
    parsed = daemonlock._parse(raw)
    assert (parsed.pid, parsed.generation) == (pid, generation)
    assert parsed.start is not None


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
        assert_v2(read(lockfile), os.getpid(), None)

    def test_generation_argument_is_written(self, db, lockfile, held):
        held.append(acquire(db, generation=41))
        assert_v2(read(lockfile), os.getpid(), 41)

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
        assert_v2(read(lockfile), os.getpid(), 30)

    def test_overwriting_a_longer_stale_file_leaves_no_tail(self, db, lockfile, held):
        """`pwrite` then `ftruncate`: the old, longer content must not survive."""
        lockfile.write_bytes(b"123456789\n987654321\n" + b"junk" * 200)
        held.append(acquire(db))
        assert_v2(read(lockfile), os.getpid(), None)

    def test_garbage_stale_file_is_taken_over(self, db, lockfile, held):
        lockfile.write_bytes(b"\xff\xfe\x00garbage")
        held.append(acquire(db))
        assert probe(db).pid == os.getpid()

    def test_empty_stale_file_is_taken_over(self, db, lockfile, held):
        lockfile.write_bytes(b"")
        held.append(acquire(db))
        assert_v2(read(lockfile), os.getpid(), None)

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
        assert_v2(read(lock_path(db)), holder.pid, 11)

    def test_kill_9_leaves_a_stale_file_that_probe_ignores(self, db, lockfile, daemon_lock_holder):
        holder = daemon_lock_holder(db, generation=2)
        holder.kill9()
        assert lockfile.exists(), "a kill -9'd daemon leaves its file behind"
        assert_v2(read(lockfile), holder.pid, 2)
        assert wait_until(lambda: probe(db) is None)

    def test_kill_9_stale_file_is_taken_over_by_the_next_daemon(
        self, db, lockfile, daemon_lock_holder, held
    ):
        holder = daemon_lock_holder(db, generation=2)
        holder.kill9()
        assert wait_until(lambda: probe(db) is None)
        held.append(acquire(db, generation=3))
        assert_v2(read(lockfile), os.getpid(), 3)

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
        assert_v2(read(lockfile), os.getpid(), 8)
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
        assert_v2(read(lockfile), os.getpid(), 5)

    def test_none_clears_the_generation(self, db, lockfile, held):
        lock = acquire(db, generation=5)
        held.append(lock)
        lock.set_generation(None)
        assert_v2(read(lockfile), os.getpid(), None)
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
        assert_v2(read(lockfile), os.getpid(), 6)
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
        assert_v2(read(lockfile), os.getpid(), 3)
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
        assert_v2(read(lockfile), os.getpid(), 3)

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
            assert_v2(read(lockfile), os.getpid(), 1)
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

        assert [f.name for f in dataclasses.fields(DaemonInfo)] == [
            "pid", "generation", "start", "applied", "target", "rejected", "stable",
        ]
        assert "pid_alive" not in {f.name for f in dataclasses.fields(DaemonInfo)}
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
        # On Linux the zombie check (`procident.is_zombie`) falls back to its own kill(pid, 0)
        # for a pid with no /proc entry; this test is about pid_is_alive's probe alone.
        monkeypatch.setattr(daemonlock.procident, "is_zombie", lambda pid: None)
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

    @pytest.fixture
    def virtual_time(self, monkeypatch, real_contention_window):
        """A clock local to `daemonlock` (its `time` name only -- the real `time`
        module is untouched): `sleep` advances it instantly and runs `on_sleep(n)`
        after the n-th sleep. Nothing here depends on how long a real
        `time.sleep(0.025)` takes (~125 ms on a loaded VM)."""
        import types

        class Clock:
            now = 1000.0
            sleeps = []
            on_sleep = None

        def sleep(seconds):
            Clock.sleeps.append(seconds)
            Clock.now += seconds
            if Clock.on_sleep:
                Clock.on_sleep(len(Clock.sleeps))

        Clock.sleeps = []
        monkeypatch.setattr(daemonlock, "time", types.SimpleNamespace(monotonic=lambda: Clock.now, sleep=sleep))
        return Clock

    @staticmethod
    def _polls_in_a_window(window, poll):
        """How many sleeps the loop makes against a never-released holder, by the
        same float arithmetic."""
        now, deadline, n = 1000.0, 1000.0 + window, 0
        while now < deadline:
            now += poll
            n += 1
        return n

    @pytest.mark.parametrize("mode", [fcntl.LOCK_SH, fcntl.LOCK_EX], ids=["shared", "exclusive"])
    @pytest.mark.parametrize("release_after", [1, 5, 12])
    def test_a_transient_holder_released_mid_window_does_not_refuse_the_daemon(
        self, db, lockfile, held, virtual_time, mode, release_after
    ):
        """The CLI's `probe()` (LOCK_SH, microseconds) landing exactly as the
        daemon starts must not be read as "another daemon": `acquire` waits the
        holder out, one poll at a time, and succeeds."""
        lockfile.write_text("")  # contention needs the file to exist
        fd = os.open(str(lockfile), os.O_RDONLY)
        fcntl.flock(fd, mode)
        closed = []

        def on_sleep(n):
            if n == release_after:
                os.close(fd)
                closed.append(n)

        virtual_time.on_sleep = on_sleep
        lock = acquire(db, generation=4)
        held.append(lock)

        assert lock.held and closed == [release_after]
        assert virtual_time.sleeps == [daemonlock._CONTENTION_POLL] * release_after      # exact
        assert virtual_time.now == pytest.approx(1000.0 + release_after * daemonlock._CONTENTION_POLL)
        assert_v2(read(lockfile), os.getpid(), 4)
        assert probe(db) == DaemonInfo(os.getpid(), 4)

    @pytest.mark.parametrize("mode", [fcntl.LOCK_EX, fcntl.LOCK_SH], ids=["exclusive", "shared"])
    def test_a_persistent_holder_is_refused_after_one_window_of_polls(
        self, db, lockfile, held, virtual_time, mode
    ):
        lockfile.write_text("4242\n8\n")
        fd = os.open(str(lockfile), os.O_RDONLY)
        fcntl.flock(fd, mode)
        try:
            with pytest.raises(DaemonAlreadyRunning) as exc:
                acquire(db)
        finally:
            os.close(fd)

        assert exc.value.pid == 4242 and exc.value.generation == 8
        expected = self._polls_in_a_window(1.0, daemonlock._CONTENTION_POLL)
        assert len(virtual_time.sleeps) == expected
        assert 39 <= expected <= 41
        assert set(virtual_time.sleeps) == {daemonlock._CONTENTION_POLL}
        assert virtual_time.now - 1000.0 >= 1.0                       # the whole window was spent

    def test_the_window_is_measured_on_the_modules_clock(self, db, lockfile, virtual_time, monkeypatch):
        """A shortened window shortens the poll count proportionally."""
        monkeypatch.setattr(daemonlock, "_CONTENTION_WINDOW", 0.2)
        lockfile.write_text("4242\n8\n")
        fd = os.open(str(lockfile), os.O_RDONLY)
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            with pytest.raises(DaemonAlreadyRunning):
                acquire(db)
        finally:
            os.close(fd)
        assert len(virtual_time.sleeps) == self._polls_in_a_window(0.2, daemonlock._CONTENTION_POLL)

    def test_a_real_transient_holder_still_works_against_the_real_clock(
        self, db, lockfile, held, real_contention_window, monkeypatch
    ):
        """One smoke test on the real clock and real `flock`, with no timing assumption: the
        holder lets go the moment `acquire` first sleeps (a spy on the real `time.sleep`), so the
        release can neither come late (a loaded VM) nor early (before `acquire` first contends).
        `acquire` must then succeed on its next attempt, after exactly one real poll."""
        lockfile.write_text("")  # contention needs the file to exist
        fd = os.open(str(lockfile), os.O_RDONLY)
        fcntl.flock(fd, fcntl.LOCK_EX)
        sleeps = []
        real_sleep = time.sleep

        def release_on_first_sleep(seconds):
            sleeps.append(seconds)
            if len(sleeps) == 1:
                os.close(fd)
            real_sleep(seconds)

        monkeypatch.setattr(daemonlock.time, "sleep", release_on_first_sleep)
        try:
            held.append(acquire(db))
        finally:
            if len(sleeps) == 0:  # acquire never contended: do not leak the descriptor
                os.close(fd)
        assert held[-1].held
        assert sleeps == [daemonlock._CONTENTION_POLL]

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


# ===========================================================================
# Phase 3, item 17.1: pid file v2, the verified reload request, the workers
# lock.  (Plan: docs/config_db_cli_plan.md, "Phase 3 -- Live config reload".)
#
# Platform notes. `request_reload` signals only a process the kernel's lock
# table proves holds the lock (`procident.flock_write_holders`, Linux only).
# The matrix below therefore runs the REAL proof on Linux, and on macOS patches
# `procident.flock_write_holders` to what Linux would answer -- everything else
# (a real helper process holding a real `LOCK_EX`, a real SIGHUP, a real start
# token) is genuine on both. Every process signalled is a helper this module
# started; each is SIGKILLed and reaped at teardown.
# ===========================================================================

IS_LINUX = sys.platform.startswith("linux")

#: Teardown must kill helpers even while a test has `os.kill` patched.
_REAL_KILL = os.kill


def _hard_kill(proc):
    try:
        _REAL_KILL(proc.pid, signal.SIGKILL)
    except OSError:
        pass
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:  # pragma: no cover
        pass


def _real_fd_table():
    """Can this host read its own `/proc/<pid>/fd` (Linux)? Then step 8b's
    recycled-pid proof (`procident.process_has_file_open`) is real in the
    matrix; elsewhere it is patched to what Linux would answer."""
    return IS_LINUX and procident.process_has_file_open(os.getpid(), 0, 0) is not None


@pytest.fixture(autouse=True)
def _recycled_pid_proof(monkeypatch):
    """Step 8b answers `None` off Linux (-> 'unverified'). The `request_reload`
    matrix is about the OTHER steps, so where the real proof is unavailable it
    answers True ("the target has the file open"); individual tests override
    it with their own `monkeypatch.setattr`."""
    if not _real_fd_table():
        monkeypatch.setattr(procident, "process_has_file_open", lambda pid, st_dev, st_ino: True)


def _real_lock_table():
    """Does this host answer `flock_write_holders` (Linux with /proc/locks)?"""
    return IS_LINUX and procident.flock_write_holders(0) is not None


def _pidfd_works():
    if not (hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal")):
        return False
    try:
        os.close(os.pidfd_open(os.getpid()))
    except OSError:
        return False
    return True


# ---------------------------------------------------------------------------
# Format and parse
# ---------------------------------------------------------------------------


class TestPidFileV2Format:
    def test_two_line_prefix_is_unchanged_when_start_is_present(self):
        raw = daemonlock._format(123, 45, "linux:99")
        assert raw.split(b"\n")[:2] == [b"123", b"45"]
        assert raw == b"123\n45\nstart=linux:99\n"

    def test_start_none_writes_the_legacy_two_line_form(self):
        assert daemonlock._format(123, 45) == b"123\n45\n"
        assert daemonlock._format(123, None) == b"123\n\n"
        # reload state is meaningless without a start line: never written
        assert daemonlock._format(123, 45, None, (1, 2), (1, 3), (1, 4)) == b"123\n45\n"

    def test_all_keys(self):
        raw = daemonlock._format(7, 5, "tok", (1, 5), (1, 6), (2, 9))
        assert raw == b"7\n5\nstart=tok\napplied=1:5\ntarget=1:6\nrejected=2:9\n"

    def test_unset_keys_are_not_written(self):
        assert daemonlock._format(7, 5, "tok", (1, 5)) == b"7\n5\nstart=tok\napplied=1:5\n"

    @pytest.mark.parametrize(
        "args",
        [
            (123, 45, "linux:99", (1, 2), (3, 4), (5, 6)),
            (123, None, "linux:99", None, None, None),
            (123, 0, "darwin:1700000000.000042", (0, 0), None, (4, 7)),
            (123, 45, "", None, None, None),  # empty start= is present, not absent
            (123, None, "", None, (1, 1), None),
        ],
    )
    def test_round_trip(self, args):
        pid, gen, start, applied, target, rejected = args
        parsed = daemonlock._parse(daemonlock._format(*args))
        assert tuple(parsed) == (pid, gen, start, applied, target, rejected)

    def test_an_empty_start_value_is_present_and_empty(self):
        parsed = daemonlock._parse(b"5\n3\nstart=\n")
        assert parsed.start == ""
        assert parsed.start is not None

    def test_a_legacy_two_line_file_has_no_start(self):
        parsed = daemonlock._parse(b"5\n3\n")
        assert (parsed.pid, parsed.generation) == (5, 3)
        assert parsed.start is None
        assert parsed.applied is parsed.target is parsed.rejected is None

    def test_reload_capable_follows_the_presence_of_start(self):
        assert daemonlock._info(b"5\n3\n").reload_capable is False
        assert daemonlock._info(b"5\n3\nstart=\n").reload_capable is True
        assert daemonlock._info(b"5\n3\nstart=tok\n").reload_capable is True
        assert DaemonInfo(5, 3).reload_capable is False
        assert DaemonInfo(5, 3, start="").reload_capable is True

    def test_unknown_keys_are_ignored(self):
        parsed = daemonlock._parse(b"5\n3\nstart=tok\nfuture=whatever\nrejected=1:2\nx=y=z\n")
        assert (parsed.start, parsed.rejected) == ("tok", (1, 2))

    @pytest.mark.parametrize(
        "tail",
        [
            b"junk\n",
            b"\n\n\n",
            b"=\n",
            b"=value\n",
            b"   \n",
            b"\xff\xfe\x00\n",
            b"start\n",  # no '='
        ],
    )
    def test_junk_lines_are_ignored_without_hiding_real_ones(self, tail):
        parsed = daemonlock._parse(b"5\n3\n" + tail + b"start=tok\napplied=1:2\n")
        assert (parsed.start, parsed.applied) == ("tok", (1, 2))

    @pytest.mark.parametrize(
        "value",
        ["abc", "1", "1:", ":1", "a:b", "1:2:3", "-1:2", "1:-2", "1.5:2", "", " ", "1 : 2x"],
    )
    def test_malformed_pairs_are_none(self, value):
        parsed = daemonlock._parse(f"5\n3\nstart=tok\napplied={value}\ntarget={value}\nrejected={value}\n".encode())
        assert parsed.applied is parsed.target is parsed.rejected is None

    def test_pairs_tolerate_surrounding_whitespace(self):
        assert daemonlock._parse(b"5\n3\nstart=t\napplied= 1:2 \n").applied == (1, 2)

    def test_the_first_duplicate_wins(self):
        parsed = daemonlock._parse(b"5\n3\nstart=first\nstart=second\napplied=1:1\napplied=9:9\n")
        assert parsed.start == "first"
        assert parsed.applied == (1, 1)

    def test_the_first_duplicate_wins_even_when_malformed(self):
        parsed = daemonlock._parse(b"5\n3\nstart=tok\napplied=oops\napplied=1:1\n")
        assert parsed.applied is None

    def test_keys_are_read_from_line_three_on_only(self):
        parsed = daemonlock._parse(b"start=evil\napplied=1:1\nstart=late\n")
        assert parsed.pid is None and parsed.generation is None
        assert parsed.start == "late"  # line 3; lines 1-2 are never key=value
        assert parsed.applied is None

    @pytest.mark.parametrize(
        "value",
        ["has space", "tab\there", "x" * 129, "x" * 4096, "bell\x07", "nul\x00x", "a\x7fb"],
    )
    def test_an_unusable_start_value_becomes_empty(self, value):
        parsed = daemonlock._parse(f"5\n3\nstart={value}\n".encode("utf-8"))
        assert parsed.start == ""

    def test_non_ascii_in_start_cannot_match_a_real_token(self):
        """Decoded with replacement characters: never raises, and the result
        differs from anything `procident` could have produced."""
        start = daemonlock._parse("5\n3\nstart=uni\u00e9\n".encode("utf-8")).start
        assert start is not None and start != "uni\u00e9"
        assert procident.identity(os.getpid(), start) in ("mismatch", "unknown")

    def test_non_ascii_in_start_cannot_match_a_real_token(self):
        """Decoded with replacement characters: never raises, and the result
        differs from anything `procident` could have produced."""
        start = daemonlock._parse(b"5\n3\nstart=uni\xc3\xa9\n").start
        assert start is not None and start != "uni\u00e9"
        assert procident.identity(os.getpid(), start) in ("mismatch", "unknown")

    def test_a_start_of_exactly_128_characters_is_kept(self):
        assert daemonlock._parse(f"5\n3\nstart={'x' * 128}\n".encode()).start == "x" * 128

    def test_surrounding_whitespace_on_start_is_stripped(self):
        assert daemonlock._parse(b"5\n3\nstart=  tok  \n").start == "tok"

    def test_a_bad_start_makes_the_daemon_unsignallable_not_legacy(self):
        info = daemonlock._info(b"5\n3\nstart=has space\n")
        assert info.start == ""
        assert info.reload_capable is True

    def test_probe_reports_the_v2_fields(self, db, lockfile):
        fd = hold_raw(lockfile, b"5\n3\nstart=tok\napplied=1:3\ntarget=1:4\nrejected=1:2\n")
        try:
            info = probe(db)
        finally:
            os.close(fd)
        assert (info.pid, info.generation, info.start) == (5, 3, "tok")
        assert (info.applied, info.target, info.rejected) == ((1, 3), (1, 4), (1, 2))
        assert info.stable is True
        assert info.reload_capable is True

    def test_probe_reports_a_legacy_daemon_as_not_reload_capable(self, db, lockfile):
        fd = hold_raw(lockfile, b"5\n3\n")
        try:
            info = probe(db)
        finally:
            os.close(fd)
        assert info == DaemonInfo(5, 3)
        assert info.reload_capable is False
        assert info.start is None


class TestDaemonLockRecordsV2:
    def test_the_lock_records_its_own_start_token(self, db, held):
        lock = acquire(db)
        held.append(lock)
        assert lock.start == (procident.start_token(os.getpid()) or "")
        info = probe(db)
        assert info.start == lock.start
        assert info.reload_capable is True

    @pytest.mark.skipif(not (IS_LINUX or sys.platform == "darwin"), reason="start tokens exist only on Linux/macOS")
    def test_the_recorded_token_identifies_this_process(self, db, held):
        lock = acquire(db)
        held.append(lock)
        assert lock.start
        assert procident.identity(os.getpid(), probe(db).start) == "match"

    def test_a_platform_without_tokens_writes_an_empty_start_line(self, db, lockfile, held, monkeypatch):
        monkeypatch.setattr(procident, "start_token", lambda pid: None)
        lock = acquire(db, generation=2)
        held.append(lock)
        assert lock.start == ""
        assert read(lockfile) == f"{os.getpid()}\n2\nstart=\n".encode()
        assert probe(db).reload_capable is True

    def test_a_new_lock_has_no_reload_state(self, db, held):
        lock = acquire(db)
        held.append(lock)
        assert (lock.applied, lock.target, lock.rejected) == (None, None, None)
        info = probe(db)
        assert (info.applied, info.target, info.rejected) == (None, None, None)

    def test_set_generation_keeps_the_start_line(self, db, lockfile, held):
        lock = acquire(db)
        held.append(lock)
        lock.set_generation(12)
        assert daemonlock._parse(read(lockfile)).start == lock.start
        assert daemonlock._parse(read(lockfile)).generation == 12


class TestSetState:
    def test_applied_rewrites_the_generation_line(self, db, lockfile, held):
        lock = acquire(db, generation=1)
        held.append(lock)
        lock.set_state(applied=(7, 42))
        parsed = daemonlock._parse(read(lockfile))
        assert parsed.generation == 42
        assert parsed.applied == (7, 42)
        assert lock.generation == 42
        assert read(lockfile).split(b"\n")[:2] == [str(os.getpid()).encode(), b"42"]

    def test_omitted_keys_keep_their_value(self, db, lockfile, held):
        lock = acquire(db)
        held.append(lock)
        lock.set_state(applied=(1, 5), target=(1, 6), rejected=(1, 4))
        lock.set_state(target=(1, 7))
        parsed = daemonlock._parse(read(lockfile))
        assert (parsed.applied, parsed.target, parsed.rejected) == ((1, 5), (1, 7), (1, 4))
        assert parsed.generation == 5

    def test_none_clears_a_key(self, db, lockfile, held):
        lock = acquire(db)
        held.append(lock)
        lock.set_state(applied=(1, 5), target=(1, 6), rejected=(1, 4))
        lock.set_state(target=None)
        parsed = daemonlock._parse(read(lockfile))
        assert parsed.target is None
        assert (parsed.applied, parsed.rejected) == ((1, 5), (1, 4))
        assert b"target" not in read(lockfile)

    def test_clearing_applied_clears_the_generation_too(self, db, lockfile, held):
        lock = acquire(db, generation=9)
        held.append(lock)
        lock.set_state(applied=(1, 9))
        lock.set_state(applied=None)
        parsed = daemonlock._parse(read(lockfile))
        assert parsed.applied is None
        assert parsed.generation is None
        assert lock.generation is None

    def test_shrinking_leaves_no_tail(self, db, lockfile, held):
        lock = acquire(db)
        held.append(lock)
        lock.set_state(applied=(1, 5), target=(1, 6), rejected=(1, 4))
        long = read(lockfile)
        lock.set_state(target=None, rejected=None)
        short = read(lockfile)
        assert len(short) < len(long)
        assert short == daemonlock._format(
            os.getpid(), 5, lock.start, (1, 5), None, None
        )

    def test_with_no_arguments_it_rewrites_the_same_content(self, db, lockfile, held):
        lock = acquire(db, generation=3)
        held.append(lock)
        before = read(lockfile)
        lock.set_state()
        assert read(lockfile) == before

    def test_the_start_line_survives_every_rewrite(self, db, lockfile, held):
        lock = acquire(db)
        held.append(lock)
        for kwargs in ({"applied": (1, 1)}, {"target": (1, 2)}, {"rejected": (1, 3)}, {"target": None}):
            lock.set_state(**kwargs)
            assert daemonlock._parse(read(lockfile)).start == lock.start

    def test_probe_sees_the_state(self, db, held):
        lock = acquire(db)
        held.append(lock)
        lock.set_state(applied=(2, 8), target=(2, 9))
        info = probe(db)
        assert (info.generation, info.applied, info.target, info.rejected) == (8, (2, 8), (2, 9), None)

    def test_a_write_failure_only_logs_a_warning(self, db, held, monkeypatch, caplog):
        lock = acquire(db)
        held.append(lock)

        def full(fd, data, off):
            raise OSError(errno.ENOSPC, "No space left on device")

        monkeypatch.setattr(daemonlock.os, "pwrite", full)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemonlock"):
            lock.set_state(applied=(1, 5), target=(1, 6))  # must not raise
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "reload state" in warnings[0].getMessage()
        assert not [r for r in caplog.records if r.levelno > logging.WARNING]
        # the in-memory state still moved, so the next successful write is right
        assert (lock.applied, lock.target, lock.generation) == ((1, 5), (1, 6), 5)

    def test_a_failed_write_is_retried_by_the_next_one(self, db, lockfile, held, monkeypatch):
        lock = acquire(db)
        held.append(lock)
        real = os.pwrite
        monkeypatch.setattr(daemonlock.os, "pwrite", lambda *a: (_ for _ in ()).throw(OSError(errno.EIO, "io")))
        lock.set_state(applied=(1, 5))
        monkeypatch.setattr(daemonlock.os, "pwrite", real)
        lock.set_state(rejected=(1, 4))
        parsed = daemonlock._parse(read(lockfile))
        assert (parsed.applied, parsed.rejected) == ((1, 5), (1, 4))

    def test_an_fsync_failure_is_also_only_a_warning(self, db, held, monkeypatch, caplog):
        lock = acquire(db)
        held.append(lock)
        monkeypatch.setattr(daemonlock.os, "fsync", lambda fd: (_ for _ in ()).throw(OSError(errno.EIO, "io")))
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemonlock"):
            lock.set_state(applied=(1, 5))
        assert any(r.levelno == logging.WARNING for r in caplog.records)

    def test_a_released_lock_ignores_set_state(self, db, lockfile, held):
        lock = acquire(db)
        lock.release()
        lock.set_state(applied=(1, 5))  # no descriptor: nothing to write, nothing raised
        assert not lockfile.exists()


class TestDaemonInfoV2:
    def test_equality_ignores_the_v2_fields(self):
        assert DaemonInfo(1, 2, start="a", applied=(1, 2), target=(1, 3), rejected=(1, 4), stable=False) == DaemonInfo(1, 2)
        assert DaemonInfo(1, 2, start="a") == DaemonInfo(1, 2, start="b")

    def test_equality_still_distinguishes_pid_and_generation(self):
        assert DaemonInfo(1, 2, start="a") != DaemonInfo(1, 3, start="a")
        assert DaemonInfo(1, 2, start="a") != DaemonInfo(2, 2, start="a")

    def test_hashing_follows_equality(self):
        assert hash(DaemonInfo(1, 2, start="a")) == hash(DaemonInfo(1, 2, start="b"))

    def test_defaults(self):
        info = DaemonInfo(1, 2)
        assert (info.start, info.applied, info.target, info.rejected, info.stable) == (None, None, None, None, True)

    def test_is_frozen(self):
        import dataclasses

        with pytest.raises(dataclasses.FrozenInstanceError):
            DaemonInfo(1, 2).start = "x"


class TestDaemonAlreadyRunningStart:
    def test_start_defaults_to_none(self, db):
        assert DaemonAlreadyRunning(lock_path(db), 1234).start is None

    def test_start_is_carried_and_independent_of_the_message(self, db):
        exc = DaemonAlreadyRunning(lock_path(db), os.getpid(), 3, "linux:1")
        assert exc.start == "linux:1"
        assert str(os.getpid()) in str(exc)
        assert "linux:1" not in str(exc)

    def test_acquire_reports_the_holders_start(self, db, lockfile):
        fd = hold_raw(lockfile, b"4242\n9\nstart=linux:777\n")
        try:
            with pytest.raises(DaemonAlreadyRunning) as exc:
                acquire(db)
        finally:
            os.close(fd)
        assert (exc.value.pid, exc.value.generation, exc.value.start) == (4242, 9, "linux:777")

    def test_acquire_against_a_legacy_holder_has_no_start(self, db, lockfile):
        fd = hold_raw(lockfile, b"4242\n9\n")
        try:
            with pytest.raises(DaemonAlreadyRunning) as exc:
                acquire(db)
        finally:
            os.close(fd)
        assert exc.value.start is None
        assert exc.value.generation == 9

    def test_acquire_against_a_real_v2_holder(self, db, daemon_lock_holder):
        holder = daemon_lock_holder(db, generation=5)
        with pytest.raises(DaemonAlreadyRunning) as exc:
            acquire(db)
        assert exc.value.pid == holder.pid
        assert exc.value.start == probe(db).start
        assert exc.value.start is not None


# ---------------------------------------------------------------------------
# _read_stable: a torn read is never acted on
# ---------------------------------------------------------------------------


def _fake_pread(monkeypatch, torn):
    """Make the first `len(torn)` whole-file reads return `torn[i]` (a complete
    read each, ended by an empty chunk), then read for real."""
    real = os.pread
    state = {"reads": 0}

    def pread(fd, n, off):
        if off == 0:
            state["reads"] += 1
        index = state["reads"] - 1
        if index < len(torn):
            return torn[index] if off == 0 else b""
        return real(fd, n, off)

    monkeypatch.setattr(daemonlock.os, "pread", pread)
    return state


class TestReadStable:
    @pytest.fixture
    def fd(self, tmp_path):
        path = tmp_path / "f"
        path.write_bytes(b"hello\n")
        fd = os.open(str(path), os.O_RDONLY)
        yield fd
        os.close(fd)

    def test_a_stable_file_is_returned_stable(self, fd):
        assert daemonlock._read_stable(fd) == (b"hello\n", True)

    def test_an_empty_file_is_stable(self, tmp_path):
        path = tmp_path / "empty"
        path.write_bytes(b"")
        fd = os.open(str(path), os.O_RDONLY)
        try:
            assert daemonlock._read_stable(fd) == (b"", True)
        finally:
            os.close(fd)

    def test_a_file_larger_than_the_read_chunk_is_read_whole(self, tmp_path):
        path = tmp_path / "big"
        path.write_bytes(b"x" * 9000)
        fd = os.open(str(path), os.O_RDONLY)
        try:
            assert daemonlock._read_stable(fd) == (b"x" * 9000, True)
        finally:
            os.close(fd)

    def test_one_or_two_torn_reads_are_retried(self, fd, monkeypatch):
        for torn in ([b"A\n"], [b"A\n", b"B\n"]):
            _fake_pread(monkeypatch, torn)
            assert daemonlock._read_stable(fd) == (b"hello\n", True)

    def test_a_read_that_never_settles_is_unstable(self, fd, monkeypatch):
        _fake_pread(monkeypatch, [b"A\n", b"B\n", b"A\n", b"B\n", b"A\n", b"B\n"])
        content, stable = daemonlock._read_stable(fd)
        assert stable is False
        assert content in (b"A\n", b"B\n")  # returned for display only

    def test_it_gives_up_after_a_bounded_number_of_reads(self, fd, monkeypatch):
        state = _fake_pread(monkeypatch, [bytes([65 + i % 2]) for i in range(1000)])
        daemonlock._read_stable(fd)
        assert state["reads"] == daemonlock._STABLE_READS + 1

    def test_probe_marks_a_torn_read_unstable(self, db, held, monkeypatch):
        lock = acquire(db)
        held.append(lock)
        _fake_pread(monkeypatch, [b"1\n2\nstart=a\n", b"1\n3\nstart=b\n"] * 3)
        assert probe(db).stable is False

    def test_probe_of_a_stable_file_is_stable(self, db, held):
        held.append(acquire(db))
        assert probe(db).stable is True


# ---------------------------------------------------------------------------
# request_reload: helpers
# ---------------------------------------------------------------------------

_SUPERVISOR_HELPER = r'''
import fcntl, os, signal, sys, time
from zfsbackup import daemonlock, procident

db, marker, spec, gen = sys.argv[1:5]


def note(tag):
    def handler(signum, frame):
        with open(marker, "a") as fh:
            fh.write(tag + "\n")
    return handler


signal.signal(signal.SIGHUP, note("HUP"))
signal.signal(signal.SIGUSR1, note("SYNC"))

if spec == "real":
    lock = daemonlock.acquire(db, generation=int(gen) if gen else None)
else:
    content = spec.replace("{pid}", str(os.getpid())).replace("{start}", procident.start_token(os.getpid()) or "")
    fd = os.open(str(daemonlock.lock_path(db)), os.O_RDWR | os.O_CREAT, 0o660)
    os.write(fd, content.encode())
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
print("ready", os.getpid(), flush=True)
for _ in range(600):  # bounded: never outlives a forgotten teardown
    time.sleep(0.2)
'''

_WORKERS_HELPER = r'''
import sys, time
from zfsbackup import daemonlock

fd = daemonlock.hold_workers_lock(sys.argv[1])
print("ready" if fd is not None else "failed", flush=True)
for _ in range(600):
    time.sleep(0.2)
'''


class _Helper:
    """A real process holding the daemon lock (or a forged copy of its file)."""

    def __init__(self, proc, pid, marker):
        self.proc = proc
        self.pid = pid
        self.marker = marker

    def lines(self):
        try:
            return Path(self.marker).read_text().split()
        except FileNotFoundError:
            return []

    def wait_for(self, tag, timeout=10):
        return wait_until(lambda: tag in self.lines(), timeout)

    def sync(self, timeout=10):
        """Barrier: SIGUSR1 sorts after SIGHUP, so once it has been handled any
        SIGHUP sent before this call has been too."""
        _REAL_KILL(self.proc.pid, signal.SIGUSR1)
        assert self.wait_for("SYNC", timeout), "helper never handled SIGUSR1"
        return self.lines()

    def got_hup(self):
        return "HUP" in self.sync()


@pytest.fixture
def start_helper(tmp_path):
    """`start_helper(db, spec="real", gen=3)` -> `_Helper`. `spec` is either
    "real" (a genuine `acquire`) or the literal pid-file content to forge, with
    `{pid}`/`{start}` standing for the helper's own pid/token."""
    from _hermetic import hermetic_env

    procs = []

    def start(db, spec="real", gen=3):
        marker = str(tmp_path / f"marker{len(procs)}")
        proc = subprocess.Popen(
            [sys.executable, "-c", _SUPERVISOR_HELPER, str(db), marker, spec, "" if gen is None else str(gen)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            env=hermetic_env(),
            cwd=str(REPO_ROOT),
        )
        procs.append(proc)
        line = proc.stdout.readline().split()
        assert line and line[0] == "ready", f"helper failed to start: {line!r}"
        return _Helper(proc, int(line[1]), marker)

    yield start
    for proc in procs:
        _hard_kill(proc)
        proc.stdout.close()


@pytest.fixture
def plain_process():
    """`plain_process()` -> a started, signal-free `python` sleeper (a decoy or
    a pid to let die). Killed and reaped at teardown."""
    from _hermetic import hermetic_env

    procs = []

    def start():
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time\nfor _ in range(600): time.sleep(0.2)"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=hermetic_env(),
            cwd=str(REPO_ROOT),
        )
        procs.append(proc)
        return proc

    yield start
    for proc in procs:
        _hard_kill(proc)


@pytest.fixture
def proof(monkeypatch):
    """The lock-holder proof. Real on Linux; on other hosts patched to answer
    what Linux would (`proof(pid, ...)` names the holders). Also records the
    inode `request_reload` asked about, in `proof.inodes`."""
    inodes = []
    use_real = _real_lock_table()

    def install(*pids):
        if use_real:
            return

        def fake(fd):
            inodes.append(os.fstat(fd).st_ino)
            return set(pids)

        monkeypatch.setattr(procident, "flock_write_holders", fake)

    install.inodes = inodes
    install.real = use_real
    return install


class SignalSpy:
    """Records every way a signal could leave this process. With
    `passthrough=False` nothing is delivered (use that whenever the target is
    not a helper we started)."""

    def __init__(self, monkeypatch, passthrough=True):
        self.calls = []
        self.passthrough = passthrough
        spy = self

        real_kill, real_killpg = os.kill, os.killpg
        real_raise = signal.raise_signal
        real_pidfd_send = getattr(signal, "pidfd_send_signal", None)

        def kill(pid, sig):
            spy.calls.append(("kill", pid, sig))
            if spy.passthrough:
                return real_kill(pid, sig)

        def killpg(pgid, sig):
            spy.calls.append(("killpg", pgid, sig))
            if spy.passthrough:
                return real_killpg(pgid, sig)

        def raise_signal(sig):
            spy.calls.append(("raise_signal", os.getpid(), sig))

        monkeypatch.setattr(os, "kill", kill)
        monkeypatch.setattr(os, "killpg", killpg)
        monkeypatch.setattr(signal, "raise_signal", raise_signal)
        if real_pidfd_send is not None:

            def pidfd_send_signal(pidfd, sig, *a, **kw):
                spy.calls.append(("pidfd_send_signal", pidfd, sig))
                if spy.passthrough:
                    return real_pidfd_send(pidfd, sig, *a, **kw)

            monkeypatch.setattr(signal, "pidfd_send_signal", pidfd_send_signal)

    def signals(self):
        return [sig for _, _, sig in self.calls]


@pytest.fixture
def spy(monkeypatch):
    return SignalSpy(monkeypatch)


@pytest.fixture
def no_signal(monkeypatch):
    """A spy that delivers nothing, and no pidfd route: only `os.kill` exists."""
    monkeypatch.delattr(os, "pidfd_open", raising=False)
    monkeypatch.delattr(signal, "pidfd_send_signal", raising=False)
    return SignalSpy(monkeypatch, passthrough=False)


@pytest.fixture
def kill_path(monkeypatch):
    """Force the `os.kill` route (no pidfd), delivering for real."""
    monkeypatch.delattr(os, "pidfd_open", raising=False)
    monkeypatch.delattr(signal, "pidfd_send_signal", raising=False)


def _as_reload(result):
    assert isinstance(result, daemonlock.ReloadRequest)
    return result


# ---------------------------------------------------------------------------
# request_reload: the outcome matrix (real helper processes)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestRequestReloadOutcomes:
    def test_a_genuine_supervisor_is_signalled(self, db, start_helper, proof, spy):
        helper = start_helper(db, gen=3)
        proof(helper.pid)
        result = _as_reload(daemonlock.request_reload(db))
        assert result.outcome == "signalled"
        assert result.pid == helper.pid
        assert isinstance(result.info, DaemonInfo)
        assert (result.info.pid, result.info.generation) == (helper.pid, 3)
        assert str(helper.pid) in result.detail
        assert helper.wait_for("HUP"), "the supervisor never received SIGHUP"

    def test_it_accepts_a_resolved_config_path(self, db, start_helper, proof, spy):
        helper = start_helper(db)
        proof(helper.pid)
        result = daemonlock.request_reload(resolve_config_path(str(db)))
        assert result.outcome == "signalled"
        assert helper.wait_for("HUP")

    def test_signalling_twice_delivers_twice(self, db, start_helper, proof, spy):
        helper = start_helper(db)
        proof(helper.pid)
        assert daemonlock.request_reload(db).outcome == "signalled"
        assert daemonlock.request_reload(db).outcome == "signalled"
        assert helper.wait_for("HUP")

    def test_a_wrong_start_token_is_supervisor_gone_and_nothing_is_sent(self, db, start_helper, proof, spy):
        helper = start_helper(db, spec="{pid}\n3\nstart=bogus:1\n")
        proof(helper.pid)  # the holder proof passes; only the identity check can stop it
        result = daemonlock.request_reload(db)
        assert result.outcome == "supervisor-gone"
        assert result.pid == helper.pid
        assert spy.calls == []
        assert not helper.got_hup()

    @pytest.mark.skipif(not (IS_LINUX or sys.platform == "darwin"), reason="needs start tokens")
    def test_a_decoy_with_its_own_real_token_that_does_not_hold_the_lock_is_not_signalled(
        self, db, start_helper, plain_process, proof, spy
    ):
        decoy = plain_process()
        token = procident.start_token(decoy.pid)
        assert token
        # The lock is held by `holder`; the (group-writable) file names the decoy
        # and the decoy's genuine token. Identity would match; only the proof stops it.
        holder = start_helper(db, spec=f"{decoy.pid}\n3\nstart={token}\n")
        proof(holder.pid)
        result = daemonlock.request_reload(db)
        assert result.outcome == "supervisor-gone"
        assert result.pid == decoy.pid
        assert spy.calls == []
        assert decoy.poll() is None, "the decoy must still be alive"
        assert not holder.got_hup()

    def test_a_pid_that_is_gone_is_supervisor_gone(self, db, start_helper, plain_process, proof, spy):
        victim = plain_process()
        token = procident.start_token(victim.pid) or "linux:1"
        dead = victim.pid
        victim.kill()
        victim.wait(timeout=10)
        spy.calls.clear()  # the victim's own kill is not request_reload's
        holder = start_helper(db, spec=f"{dead}\n3\nstart={token}\n")
        proof(holder.pid)
        result = daemonlock.request_reload(db)
        assert result.outcome == "supervisor-gone"
        assert result.pid == dead
        assert spy.calls == []
        assert not holder.got_hup()

    def test_a_legacy_daemon_is_legacy_and_nothing_is_sent(self, db, start_helper, proof, spy):
        helper = start_helper(db, spec="{pid}\n3\n")
        proof(helper.pid)
        result = daemonlock.request_reload(db)
        assert result.outcome == "legacy"
        assert result.pid == helper.pid
        assert result.info.reload_capable is False
        assert "restart" in result.detail
        assert spy.calls == []
        assert not helper.got_hup()

    def test_an_empty_start_is_unverified(self, db, start_helper, proof, spy):
        helper = start_helper(db, spec="{pid}\n3\nstart=\n")
        proof(helper.pid)
        result = daemonlock.request_reload(db)
        assert result.outcome == "unverified"
        assert spy.calls == []
        assert not helper.got_hup()

    def test_a_malformed_start_is_unverified_not_legacy(self, db, start_helper, proof, spy):
        helper = start_helper(db, spec="{pid}\n3\nstart=has space\n")
        proof(helper.pid)
        assert daemonlock.request_reload(db).outcome == "unverified"
        assert not helper.got_hup()

    def test_a_lock_table_that_cannot_be_read_is_unverified(self, db, start_helper, monkeypatch, spy):
        helper = start_helper(db)
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: None)
        # The detail names the reason; on Linux the real reason would be "unknown", so pin the
        # platform-dependent one this test asserts on.
        monkeypatch.setattr(
            procident, "flock_holders_unavailable_reason", lambda fd: procident.UNAVAILABLE_NOT_LINUX
        )
        result = daemonlock.request_reload(db)
        assert result.outcome == "unverified"
        assert "Linux" in result.detail
        assert spy.calls == []
        assert not helper.got_hup()

    def test_an_empty_lock_table_means_the_pid_is_not_the_holder(self, db, start_helper, monkeypatch, spy):
        helper = start_helper(db)
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: set())
        assert daemonlock.request_reload(db).outcome == "supervisor-gone"
        assert not helper.got_hup()

    def test_the_proof_is_handed_the_open_pid_file_descriptor(self, db, lockfile, start_helper, proof, monkeypatch, spy):
        """It takes an FD (so the device can be read from the mount), and the fd
        is the pid file's own."""
        helper = start_helper(db)
        proof(helper.pid)  # (on Linux: the real table)
        inner = procident.flock_write_holders
        asked = []

        def recording(fd):
            assert isinstance(fd, int) and not isinstance(fd, bool)
            st = os.fstat(fd)
            asked.append((st.st_dev, st.st_ino, stat.S_ISREG(st.st_mode)))
            return inner(fd)

        monkeypatch.setattr(procident, "flock_write_holders", recording)
        assert daemonlock.request_reload(db).outcome == "signalled"
        real = os.stat(lockfile)
        assert asked == [(real.st_dev, real.st_ino, True)]

    def test_identity_unknown_is_unverified_and_nothing_is_sent(self, db, start_helper, proof, monkeypatch, spy):
        helper = start_helper(db)
        proof(helper.pid)
        monkeypatch.setattr(procident, "identity", lambda pid, token: "unknown")
        result = daemonlock.request_reload(db)
        assert result.outcome == "unverified"
        assert spy.calls == []
        assert not helper.got_hup()

    @pytest.mark.parametrize("ident", ["gone", "mismatch"])
    def test_identity_gone_or_mismatch_is_supervisor_gone(self, db, start_helper, proof, monkeypatch, spy, kill_path, ident):
        """No pidfd: 'gone' is trusted (`hidepid` is only ruled out by a pidfd)."""
        helper = start_helper(db)
        proof(helper.pid)
        monkeypatch.setattr(procident, "identity", lambda pid, token: ident)
        assert daemonlock.request_reload(db).outcome == "supervisor-gone"
        assert spy.calls == []
        assert not helper.got_hup()

    def test_skip_if_applied_equal_sends_nothing(self, db, lockfile, start_helper, proof, spy):
        helper = start_helper(db, spec="{pid}\n3\nstart={start}\napplied=1:3\n")
        proof(helper.pid)
        result = daemonlock.request_reload(db, skip_if_applied=(1, 3))
        assert result.outcome == "already-applied"
        assert result.info.applied == (1, 3)
        assert spy.calls == []
        assert not helper.got_hup()

    def test_skip_if_applied_accepts_a_list(self, db, start_helper, proof, spy):
        helper = start_helper(db, spec="{pid}\n3\nstart={start}\napplied=1:3\n")
        proof(helper.pid)
        assert daemonlock.request_reload(db, skip_if_applied=[1, 3]).outcome == "already-applied"

    @pytest.mark.parametrize("skip", [(1, 4), (2, 3), (0, 0)])
    def test_skip_if_applied_that_differs_still_signals(self, db, start_helper, proof, spy, skip):
        helper = start_helper(db, spec="{pid}\n3\nstart={start}\napplied=1:3\n")
        proof(helper.pid)
        assert daemonlock.request_reload(db, skip_if_applied=skip).outcome == "signalled"
        assert helper.wait_for("HUP")

    def test_skip_if_applied_with_nothing_applied_still_signals(self, db, start_helper, proof, spy):
        helper = start_helper(db)  # real daemon, no applied= yet
        proof(helper.pid)
        assert daemonlock.request_reload(db, skip_if_applied=(1, 3)).outcome == "signalled"

    def test_skip_if_applied_never_beats_a_legacy_verdict(self, db, start_helper, proof, spy):
        helper = start_helper(db, spec="{pid}\n3\n")
        proof(helper.pid)
        assert daemonlock.request_reload(db, skip_if_applied=(1, 3)).outcome == "legacy"

    def test_a_real_daemons_set_state_is_honoured_by_skip_if_applied(self, db, start_helper, proof, spy):
        helper = start_helper(db)
        proof(helper.pid)
        # the helper is a separate process; mutate its file the way set_state would
        raw = daemonlock._format(helper.pid, 8, probe(db).start, (1, 8))
        fd = os.open(str(lock_path(db)), os.O_RDWR)
        try:
            os.pwrite(fd, raw, 0)
            os.ftruncate(fd, len(raw))
        finally:
            os.close(fd)
        assert daemonlock.request_reload(db, skip_if_applied=(1, 8)).outcome == "already-applied"

    def test_no_lock_held_is_no_daemon(self, db, lockfile, spy):
        lockfile.write_bytes(b"4242\n3\nstart=linux:1\n")  # a stale file
        result = daemonlock.request_reload(db)
        assert result.outcome == "no-daemon"
        assert result.pid is None
        assert spy.calls == []

    def test_no_lock_held_leaves_the_file_and_no_lock_behind(self, db, lockfile, held):
        lockfile.write_bytes(b"4242\n3\nstart=linux:1\n")
        before = read(lockfile)
        daemonlock.request_reload(db)
        assert read(lockfile) == before
        held.append(acquire(db))  # the probe's shared lock was released

    def test_a_missing_file_is_no_daemon(self, db, spy):
        assert daemonlock.request_reload(db).outcome == "no-daemon"
        assert spy.calls == []

    def test_a_missing_directory_is_no_daemon(self, tmp_path, spy):
        assert daemonlock.request_reload(tmp_path / "nodir" / "config.db").outcome == "no-daemon"

    def test_a_path_component_that_is_a_file_is_no_daemon(self, tmp_path):
        (tmp_path / "afile").write_text("x")
        assert daemonlock.request_reload(tmp_path / "afile" / "config.db").outcome == "no-daemon"

    def test_request_reload_creates_nothing(self, db, lockfile):
        daemonlock.request_reload(db)
        assert not lockfile.exists()

    def test_eacces_on_open_is_error(self, db, lockfile, monkeypatch, spy):
        lockfile.write_bytes(b"1\n\n")
        real_open = os.open

        def deny(path, flags, *a, **kw):
            if str(path).endswith(".daemon.pid"):
                raise PermissionError(errno.EACCES, "Permission denied")
            return real_open(path, flags, *a, **kw)

        monkeypatch.setattr(daemonlock.os, "open", deny)
        result = _as_reload(daemonlock.request_reload(db))
        assert result.outcome == "error"
        assert "Permission denied" in result.detail
        assert spy.calls == []

    @pytest.mark.skipif(IS_ROOT, reason="root opens any file")
    def test_an_unreadable_real_file_is_error(self, db, lockfile):
        lockfile.write_bytes(b"1\n\n")
        lockfile.chmod(0)
        try:
            assert daemonlock.request_reload(db).outcome == "error"
        finally:
            lockfile.chmod(0o600)

    def test_a_symlinked_pid_file_is_error_and_never_followed(self, db, lockfile, tmp_path, spy):
        target = tmp_path / "elsewhere"
        target.write_bytes(b"1\n\nstart=x\n")
        os.symlink(target, lockfile)
        assert daemonlock.request_reload(db).outcome == "error"
        assert spy.calls == []

    def test_a_flock_failure_other_than_contention_is_error(self, db, lockfile, monkeypatch, spy):
        lockfile.write_bytes(b"1\n\n")

        def eio(fd, op):
            raise OSError(errno.EIO, "Input/output error")

        monkeypatch.setattr(daemonlock.fcntl, "flock", eio)
        result = daemonlock.request_reload(db)
        assert result.outcome == "error"
        assert spy.calls == []

    def test_an_unexpected_exception_is_still_a_reload_request(self, db, lockfile, monkeypatch):
        lockfile.write_bytes(b"1\n\n")

        def boom(path, flags, *a, **kw):
            raise RuntimeError("kaboom")

        monkeypatch.setattr(daemonlock.os, "open", boom)
        result = _as_reload(daemonlock.request_reload(db))
        assert result.outcome == "error"
        assert "kaboom" in result.detail

    def test_an_exception_in_the_proof_is_error_not_a_raise(self, db, start_helper, monkeypatch, spy):
        helper = start_helper(db)

        def boom(fd):
            raise RuntimeError("proof exploded")

        monkeypatch.setattr(procident, "flock_write_holders", boom)
        result = _as_reload(daemonlock.request_reload(db))
        assert result.outcome == "error"
        assert spy.calls == []
        assert not helper.got_hup()

    def test_every_documented_outcome_is_in_the_reload_request_docstring(self):
        doc = daemonlock.ReloadRequest.__doc__
        for outcome in (
            "no-daemon", "already-applied", "signalled", "not-permitted",
            "legacy", "supervisor-gone", "unverified", "error",
        ):
            assert f"`{outcome}`" in doc


@pytest.mark.integration
@pytest.mark.skipif(not _real_lock_table(), reason="the real lock proof needs Linux /proc/locks")
class TestRequestReloadRealLockProof:
    """[L] No patching of the proof: the kernel's lock table decides."""

    def test_a_genuine_supervisor_is_proven_and_signalled(self, db, start_helper, spy):
        helper = start_helper(db)
        fd = os.open(lock_path(db), os.O_RDONLY)
        try:
            holders = procident.flock_write_holders(fd)   # takes the open pid-file fd (device + inode)
        finally:
            os.close(fd)
        assert holders is not None and helper.pid in holders
        assert daemonlock.request_reload(db).outcome == "signalled"
        assert helper.wait_for("HUP")

    def test_a_forged_file_naming_another_process_is_refused_by_the_proof(self, db, start_helper, plain_process, spy):
        decoy = plain_process()
        token = procident.start_token(decoy.pid)
        holder = start_helper(db, spec=f"{decoy.pid}\n3\nstart={token}\n")
        result = daemonlock.request_reload(db)
        assert result.outcome == "supervisor-gone"
        assert "does not hold the daemon lock" in result.detail
        assert decoy.poll() is None
        assert not holder.got_hup()


# ---------------------------------------------------------------------------
# request_reload: the pid guard, the signal contract, and the descriptor budget
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestRequestReloadNeverSignalsTheWrongThing:
    @pytest.fixture
    def force_everything_else_to_pass(self, monkeypatch):
        """Make the holder proof and the identity check say yes to ANY pid, so
        the pid guard is the only thing standing between the file and a signal."""
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: set(range(-5, 2**22)) | {os.getpid()})
        monkeypatch.setattr(procident, "identity", lambda pid, token: "match")

    @pytest.mark.parametrize("pid", [0, 1, -1, -2, -4242])
    def test_pids_zero_one_and_negative_are_never_signalled(
        self, db, lockfile, force_everything_else_to_pass, no_signal, pid
    ):
        fd = hold_raw(lockfile, f"{pid}\n3\nstart=tok\n".encode())
        try:
            result = daemonlock.request_reload(db)
        finally:
            os.close(fd)
        assert result.outcome == "unverified"
        assert f"refusing to signal pid {pid}" in result.detail
        assert no_signal.calls == []

    def test_our_own_pid_is_never_signalled(self, db, lockfile, force_everything_else_to_pass, no_signal):
        fd = hold_raw(lockfile, f"{os.getpid()}\n3\nstart=tok\n".encode())
        try:
            result = daemonlock.request_reload(db)
        finally:
            os.close(fd)
        assert result.outcome == "unverified"
        assert f"refusing to signal pid {os.getpid()}" in result.detail
        assert no_signal.calls == []

    def test_our_own_lock_is_not_signalled_even_when_it_is_the_holder(self, db, held, force_everything_else_to_pass, no_signal):
        held.append(acquire(db))
        result = daemonlock.request_reload(db)
        assert result.outcome == "unverified"
        assert no_signal.calls == []

    def test_a_pid_beyond_the_pid_range_is_an_error_not_a_raise(self, db, lockfile, force_everything_else_to_pass, monkeypatch):
        """`os.kill` would raise OverflowError; the contract is that nothing is."""
        monkeypatch.delattr(os, "pidfd_open", raising=False)
        fd = hold_raw(lockfile, f"{2**70}\n3\nstart=tok\n".encode())
        try:
            result = _as_reload(daemonlock.request_reload(db))
        finally:
            os.close(fd)
        assert result.outcome in ("error", "supervisor-gone", "unverified")

    def test_the_only_signal_sent_is_sighup_to_exactly_the_supervisor(self, db, start_helper, proof, kill_path, monkeypatch):
        helper = start_helper(db)
        proof(helper.pid)
        spy = SignalSpy(monkeypatch)
        assert daemonlock.request_reload(db).outcome == "signalled"
        calls = list(spy.calls)
        assert calls == [("kill", helper.pid, signal.SIGHUP)]
        assert helper.wait_for("HUP")

    def test_never_a_process_group_nor_any_other_signal(self, db, start_helper, proof, monkeypatch):
        helper = start_helper(db)
        proof(helper.pid)
        spy = SignalSpy(monkeypatch)
        for _ in range(3):
            daemonlock.request_reload(db)
        assert spy.calls, "nothing was signalled at all"
        assert {kind for kind, _, _ in spy.calls} <= {"kill", "pidfd_send_signal"}
        assert not [c for c in spy.calls if c[0] == "killpg"]
        assert set(spy.signals()) == {signal.SIGHUP}
        assert all(pid > 1 for kind, pid, _ in spy.calls if kind == "kill")
        assert all(pid == helper.pid for kind, pid, _ in spy.calls if kind == "kill")

    def test_the_signal_is_not_sent_to_a_negative_pid(self, db, start_helper, proof, monkeypatch):
        """`os.kill(-pid)` would be a process group; the pid is passed as read."""
        helper = start_helper(db)
        proof(helper.pid)
        spy = SignalSpy(monkeypatch)
        daemonlock.request_reload(db)
        assert all(pid >= 0 for _, pid, _ in spy.calls)

    def test_no_signal_is_sent_when_the_lock_is_not_held(self, db, lockfile, monkeypatch):
        lockfile.write_bytes(f"{os.getpid()}\n3\nstart=tok\n".encode())
        spy = SignalSpy(monkeypatch, passthrough=False)
        daemonlock.request_reload(db)
        assert spy.calls == []

    def test_no_descriptor_is_leaked_in_any_outcome(self, db, lockfile, start_helper, plain_process, proof):
        helper = start_helper(db)
        proof(helper.pid)
        before = fd_count()
        for _ in range(5):
            assert daemonlock.request_reload(db).outcome == "signalled"
            assert daemonlock.request_reload(db, skip_if_applied=(9, 9)).outcome == "signalled"
        assert fd_count() == before
        # a missing file, a stale file, an unreadable one
        other = db.parent / "other.db"
        for _ in range(5):
            daemonlock.request_reload(other)
        lock_path(other).write_bytes(b"1\n\n")
        for _ in range(5):
            assert daemonlock.request_reload(other).outcome == "no-daemon"
        assert fd_count() == before
        helper.sync()

    def test_no_descriptor_is_leaked_on_the_refusal_paths(self, db, start_helper, proof, monkeypatch):
        helper = start_helper(db, spec="{pid}\n3\nstart=bogus:1\n")
        proof(helper.pid)
        before = fd_count()
        for _ in range(5):
            assert daemonlock.request_reload(db).outcome == "supervisor-gone"
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: None)
        for _ in range(5):
            assert daemonlock.request_reload(db).outcome == "unverified"
        assert fd_count() == before

    def test_no_descriptor_is_leaked_when_it_raises_internally(self, db, start_helper, monkeypatch):
        start_helper(db)
        before = fd_count()
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: 1 / 0)
        for _ in range(5):
            assert daemonlock.request_reload(db).outcome == "error"
        assert fd_count() == before

    def test_the_pid_file_is_opened_read_only_nofollow_and_cloexec(self, db, start_helper, proof, monkeypatch):
        helper = start_helper(db)
        proof(helper.pid)
        seen = []
        real_open = os.open

        def spying(path, flags, *a, **kw):
            seen.append(flags)
            return real_open(path, flags, *a, **kw)

        monkeypatch.setattr(daemonlock.os, "open", spying)
        daemonlock.request_reload(db)
        (flags,) = seen
        assert flags & os.O_ACCMODE == os.O_RDONLY
        assert flags & os.O_NOFOLLOW and flags & os.O_CLOEXEC
        assert not flags & (os.O_CREAT | os.O_TRUNC)

    def test_it_writes_nothing_to_the_pid_file(self, db, lockfile, start_helper, proof):
        helper = start_helper(db)
        proof(helper.pid)
        before = (read(lockfile), lockfile.stat().st_mtime_ns)
        daemonlock.request_reload(db)
        assert (read(lockfile), lockfile.stat().st_mtime_ns) == before


@pytest.mark.integration
class TestRequestReloadTornReads:
    def test_a_torn_read_is_unverified_and_nothing_is_sent(self, db, start_helper, proof, monkeypatch):
        helper = start_helper(db)
        proof(helper.pid)
        good = read(lock_path(db))
        spy = SignalSpy(monkeypatch)
        # Two versions that each name the REAL, signallable helper: acting on
        # either read would deliver a SIGHUP.
        _fake_pread(monkeypatch, [good, good + b"applied=1:1\n"] * 4)
        result = daemonlock.request_reload(db)
        assert result.outcome == "unverified"
        assert result.info.stable is False
        assert "consistently" in result.detail
        assert spy.calls == []
        monkeypatch.undo()
        assert not helper.got_hup()

    @pytest.mark.parametrize("torn", [1, 2])
    def test_a_transient_tear_is_retried_and_then_acted_on(self, db, start_helper, proof, monkeypatch, kill_path, torn):
        helper = start_helper(db)
        proof(helper.pid)
        good = read(lock_path(db))
        _fake_pread(monkeypatch, [b"99999999\n1\nstart=torn\n", good + b"applied=1:1\n"][:torn])
        result = daemonlock.request_reload(db)
        assert result.outcome == "signalled"
        assert result.info.stable is True

    def test_a_torn_read_that_would_otherwise_say_legacy_is_still_unverified(self, db, start_helper, monkeypatch):
        helper = start_helper(db)
        _fake_pread(monkeypatch, [b"5\n3\n", b"5\n4\n"] * 4)
        assert daemonlock.request_reload(db).outcome == "unverified"

    def test_an_empty_file_with_the_lock_held_is_unverified(self, db, lockfile):
        fd = hold_raw(lockfile, b"")
        try:
            result = daemonlock.request_reload(db)
        finally:
            os.close(fd)
        assert result.outcome == "unverified"
        assert result.pid is None

    def test_garbage_with_the_lock_held_is_unverified(self, db, lockfile):
        fd = hold_raw(lockfile, b"garbage\nmore\n")
        try:
            assert daemonlock.request_reload(db).outcome == "unverified"
        finally:
            os.close(fd)


# ---------------------------------------------------------------------------
# request_reload: signal delivery routes (os.kill vs pidfd)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestRequestReloadKillRoute:
    """`os.pidfd_open` deleted: the portable path (macOS, old kernels)."""

    def test_os_kill_delivers_sighup(self, db, start_helper, proof, kill_path):
        helper = start_helper(db)
        proof(helper.pid)
        assert not hasattr(os, "pidfd_open")
        assert daemonlock.request_reload(db).outcome == "signalled"
        assert helper.wait_for("HUP")

    @pytest.mark.parametrize(
        "exc, outcome",
        [
            (PermissionError(errno.EPERM, "Operation not permitted"), "not-permitted"),
            (ProcessLookupError(errno.ESRCH, "No such process"), "supervisor-gone"),
            (OSError(errno.EINVAL, "Invalid argument"), "error"),
        ],
    )
    def test_kill_failures_map_to_outcomes(self, db, start_helper, proof, no_signal, monkeypatch, exc, outcome):
        helper = start_helper(db)
        proof(helper.pid)

        def refuse(pid, sig):
            no_signal.calls.append(("kill", pid, sig))
            raise exc

        monkeypatch.setattr(os, "kill", refuse)
        result = _as_reload(daemonlock.request_reload(db))
        assert result.outcome == outcome
        assert result.pid == helper.pid
        assert no_signal.calls == [("kill", helper.pid, signal.SIGHUP)]

    def test_not_permitted_names_the_pid(self, db, start_helper, proof, no_signal, monkeypatch):
        helper = start_helper(db)
        proof(helper.pid)
        monkeypatch.setattr(os, "kill", lambda pid, sig: (_ for _ in ()).throw(PermissionError(errno.EPERM, "Operation not permitted")))
        assert str(helper.pid) in daemonlock.request_reload(db).detail


class _FakePidfd:
    """`os.pidfd_open` / `signal.pidfd_send_signal` stand-ins that work on every
    host: the 'pidfd' is a real descriptor (/dev/null) so leaks are observable,
    and sending through it delivers to the helper it was opened for."""

    def __init__(self, monkeypatch, order):
        self.opened = []
        self.sent = []
        self.open_error = None
        self.send_error = None
        fake = self

        def pidfd_open(pid, flags=0):
            order.append("pidfd_open")
            if fake.open_error is not None:
                raise fake.open_error
            fd = os.open(os.devnull, os.O_RDONLY | os.O_CLOEXEC)
            fake.opened.append((fd, pid))
            return fd

        def pidfd_send_signal(pidfd, sig, *a, **kw):
            fake.sent.append((pidfd, sig))
            if fake.send_error is not None:
                raise fake.send_error
            (pid,) = [p for fd, p in fake.opened if fd == pidfd]
            fake.real_kill(pid, sig)

        self.real_kill = os.kill
        monkeypatch.setattr(os, "pidfd_open", pidfd_open, raising=False)
        monkeypatch.setattr(signal, "pidfd_send_signal", pidfd_send_signal, raising=False)

    def fd_is_closed(self, fd):
        try:
            os.fstat(fd)
        except OSError:
            return True
        return False


@pytest.mark.integration
class TestRequestReloadPidfdRoute:
    @pytest.fixture
    def order(self):
        return []

    @pytest.fixture
    def pidfd(self, monkeypatch, order):
        return _FakePidfd(monkeypatch, order)

    @pytest.fixture
    def ordered_proof(self, monkeypatch, order):
        def install(*pids):
            def fake(fd):
                order.append("holders")
                return set(pids)

            monkeypatch.setattr(procident, "flock_write_holders", fake)

        return install

    def test_pidfd_is_opened_before_the_holder_proof_and_used_to_signal(self, db, start_helper, pidfd, ordered_proof, order, monkeypatch):
        helper = start_helper(db)
        ordered_proof(helper.pid)
        kill_spy = SignalSpy(monkeypatch)
        # SignalSpy replaced pidfd_send_signal with a wrapper around our fake
        assert daemonlock.request_reload(db).outcome == "signalled"
        assert order == ["pidfd_open", "holders"]
        assert [c[0] for c in kill_spy.calls] == ["pidfd_send_signal"], "must signal through the pidfd, not os.kill"
        assert helper.wait_for("HUP")
        (fd, pid) = pidfd.opened[0]
        assert pid == helper.pid
        assert pidfd.fd_is_closed(fd), "the pidfd must be closed again"

    def test_a_vanished_pid_at_pidfd_open_is_supervisor_gone_without_consulting_the_proof(
        self, db, start_helper, pidfd, ordered_proof, order, monkeypatch
    ):
        helper = start_helper(db)
        ordered_proof(helper.pid)
        pidfd.open_error = ProcessLookupError(errno.ESRCH, "No such process")
        spy = SignalSpy(monkeypatch, passthrough=False)
        result = daemonlock.request_reload(db)
        assert result.outcome == "supervisor-gone"
        assert order == ["pidfd_open"]
        assert spy.calls == []

    @pytest.mark.parametrize("err", [errno.EPERM, errno.ENOSYS, errno.EMFILE, errno.EINVAL])
    def test_any_other_pidfd_open_failure_falls_back_to_os_kill(self, db, start_helper, pidfd, ordered_proof, monkeypatch, err):
        helper = start_helper(db)
        ordered_proof(helper.pid)
        pidfd.open_error = OSError(err, os.strerror(err))
        spy = SignalSpy(monkeypatch)
        result = daemonlock.request_reload(db)
        assert result.outcome == "signalled"
        assert spy.calls == [("kill", helper.pid, signal.SIGHUP)]
        assert pidfd.sent == []
        assert helper.wait_for("HUP")

    @pytest.mark.parametrize(
        "exc, outcome",
        [
            (PermissionError(errno.EPERM, "Operation not permitted"), "not-permitted"),
            (ProcessLookupError(errno.ESRCH, "No such process"), "supervisor-gone"),
            (OSError(errno.EINVAL, "Invalid argument"), "error"),
        ],
    )
    def test_pidfd_send_failures_map_to_outcomes_and_close_the_pidfd(self, db, start_helper, pidfd, ordered_proof, exc, outcome):
        helper = start_helper(db)
        ordered_proof(helper.pid)
        pidfd.send_error = exc
        before = fd_count()
        result = daemonlock.request_reload(db)
        assert result.outcome == outcome
        assert fd_count() == before
        assert all(pidfd.fd_is_closed(fd) for fd, _ in pidfd.opened)
        assert not helper.got_hup()

    @pytest.mark.parametrize("spec", ["{pid}\n3\nstart=bogus:1\n", "{pid}\n3\n", "{pid}\n3\nstart=\n"])
    def test_the_pidfd_is_closed_on_every_refusal(self, db, start_helper, pidfd, ordered_proof, spec):
        helper = start_helper(db, spec=spec)
        ordered_proof(helper.pid)
        before = fd_count()
        daemonlock.request_reload(db)
        assert fd_count() == before
        assert all(pidfd.fd_is_closed(fd) for fd, _ in pidfd.opened)

    def test_no_pidfd_is_opened_for_a_refused_pid(self, db, lockfile, pidfd, monkeypatch, order):
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: {1})
        monkeypatch.setattr(procident, "identity", lambda pid, token: "match")
        spy = SignalSpy(monkeypatch, passthrough=False)
        fd = hold_raw(lockfile, b"1\n3\nstart=tok\n")
        try:
            assert daemonlock.request_reload(db).outcome == "unverified"
        finally:
            os.close(fd)
        assert order == []
        assert spy.calls == []

    def test_the_pidfd_route_needs_both_halves(self, db, start_helper, proof, monkeypatch):
        helper = start_helper(db)
        proof(helper.pid)
        monkeypatch.setattr(os, "pidfd_open", lambda *a, **kw: pytest.fail("pidfd_open used"), raising=False)
        monkeypatch.delattr(signal, "pidfd_send_signal", raising=False)
        assert daemonlock.request_reload(db).outcome == "signalled"  # via os.kill
        assert helper.wait_for("HUP")


@pytest.mark.integration
@pytest.mark.skipif(not (_pidfd_works() and _real_lock_table()), reason="needs Linux with pidfd_open and /proc/locks")
class TestRequestReloadRealPidfd:
    """[L] The kernel's own pidfd and lock table, nothing patched."""

    def test_a_real_supervisor_is_signalled_through_a_pidfd_opened_before_the_proof(self, db, start_helper, monkeypatch):
        helper = start_helper(db)
        order = []
        real_open, real_holders = os.pidfd_open, procident.flock_write_holders

        def pidfd_open(pid, flags=0):
            order.append("pidfd_open")
            return real_open(pid, flags)

        def holders(fd):
            order.append("holders")
            return real_holders(fd)

        monkeypatch.setattr(os, "pidfd_open", pidfd_open)
        monkeypatch.setattr(procident, "flock_write_holders", holders)
        spy = SignalSpy(monkeypatch)
        before = fd_count()
        assert daemonlock.request_reload(db).outcome == "signalled"
        assert order == ["pidfd_open", "holders"]
        assert [c[0] for c in spy.calls] == ["pidfd_send_signal"]
        assert fd_count() == before
        assert helper.wait_for("HUP")

    def test_a_dead_pid_is_gone_at_pidfd_open(self, db, start_helper, plain_process):
        victim = plain_process()
        token = procident.start_token(victim.pid)
        dead = victim.pid
        victim.kill()
        victim.wait(timeout=10)
        holder = start_helper(db, spec=f"{dead}\n3\nstart={token}\n")
        result = daemonlock.request_reload(db)
        assert result.outcome == "supervisor-gone"
        assert "no longer exists" in result.detail
        assert not holder.got_hup()


# ---------------------------------------------------------------------------
# The workers lock
# ---------------------------------------------------------------------------


class TestWorkersLockPaths:
    def test_constants(self):
        assert daemonlock.WORKERS_LOCK_SUFFIX == ".workers.lock"
        assert daemonlock.RELOAD_POLL_SECONDS == 10

    def test_path_is_the_database_path_plus_the_suffix(self, db):
        assert daemonlock.workers_lock_path(db) == db.parent / "config.db.workers.lock"
        assert daemonlock.workers_lock_path(str(db)) == db.parent / "config.db.workers.lock"

    def test_accepts_a_resolved_config_path(self, db):
        resolved = resolve_config_path(str(db))
        assert daemonlock.workers_lock_path(resolved) == daemonlock.workers_lock_path(db)

    def test_it_is_not_the_daemon_lock_file(self, db):
        assert daemonlock.workers_lock_path(db) != lock_path(db)


class TestEnsureWorkersLock:
    def test_creates_the_file_with_mode_0660(self, db):
        path = daemonlock.ensure_workers_lock(db)
        assert path == daemonlock.workers_lock_path(db)
        assert path.is_file()
        assert mode_of(path) == 0o660
        assert path.read_bytes() == b""

    def test_the_mode_is_0660_whatever_the_umask(self, db):
        old = os.umask(0o077)
        try:
            path = daemonlock.ensure_workers_lock(db)
        finally:
            os.umask(old)
        assert mode_of(path) == 0o660

    def test_is_idempotent(self, db):
        first = daemonlock.ensure_workers_lock(db)
        ino = first.stat().st_ino
        second = daemonlock.ensure_workers_lock(db)
        assert second == first
        assert second.stat().st_ino == ino
        assert mode_of(second) == 0o660

    def test_an_existing_file_with_a_narrower_mode_is_fixed_when_we_own_it(self, db):
        path = daemonlock.workers_lock_path(db)
        path.write_bytes(b"")
        path.chmod(0o600)
        daemonlock.ensure_workers_lock(db)
        assert mode_of(path) == 0o660

    def test_existing_content_is_left_alone(self, db):
        path = daemonlock.workers_lock_path(db)
        path.write_bytes(b"keep me")
        daemonlock.ensure_workers_lock(db)
        assert path.read_bytes() == b"keep me"

    def test_accepts_a_resolved_config_path(self, db):
        path = daemonlock.ensure_workers_lock(resolve_config_path(str(db)))
        assert path.name == "config.db.workers.lock"

    def test_does_not_create_the_database_or_the_daemon_lock(self, db, lockfile):
        daemonlock.ensure_workers_lock(db)
        assert not db.exists()
        assert not lockfile.exists()

    def test_the_file_is_not_locked_afterwards_and_no_descriptor_leaks(self, db):
        before = fd_count()
        daemonlock.ensure_workers_lock(db)
        daemonlock.ensure_workers_lock(db)
        assert fd_count() == before
        assert daemonlock.workers_present(db) is False

    def test_a_symlink_is_refused_and_never_followed(self, db, tmp_path):
        target = tmp_path / "victim"
        target.write_bytes(b"precious")
        target.chmod(0o600)
        os.symlink(target, daemonlock.workers_lock_path(db))
        before = fd_count()
        with pytest.raises(DaemonLockError):
            daemonlock.ensure_workers_lock(db)
        assert target.read_bytes() == b"precious"
        assert mode_of(target) == 0o600, "must not chmod through a symlink"
        assert fd_count() == before

    def test_a_dangling_symlink_is_refused_and_nothing_is_created(self, db, tmp_path):
        target = tmp_path / "not-there"
        os.symlink(target, daemonlock.workers_lock_path(db))
        with pytest.raises(DaemonLockError):
            daemonlock.ensure_workers_lock(db)
        assert not target.exists()

    def test_a_non_regular_file_is_refused(self, db):
        daemonlock.workers_lock_path(db).mkdir()
        before = fd_count()
        with pytest.raises(DaemonLockError) as exc:
            daemonlock.ensure_workers_lock(db)
        assert exc.value.errno == errno.EINVAL
        assert "not a regular file" in str(exc.value)
        assert fd_count() == before

    def test_a_missing_directory_is_a_daemonlockerror(self, tmp_path):
        with pytest.raises(DaemonLockError):
            daemonlock.ensure_workers_lock(tmp_path / "nodir" / "config.db")

    @pytest.mark.skipif(IS_ROOT, reason="root creates files anywhere")
    def test_a_read_only_directory_is_a_daemonlockerror(self, tmp_path):
        ro = tmp_path / "ro"
        ro.mkdir()
        ro.chmod(0o500)
        try:
            with pytest.raises(DaemonLockError):
                daemonlock.ensure_workers_lock(ro / "config.db")
        finally:
            ro.chmod(0o700)

    def test_the_descriptor_is_opened_cloexec_and_nofollow(self, db, monkeypatch):
        flags_seen = []
        real_open = os.open

        def spying(path, flags, *a, **kw):
            flags_seen.append(flags)
            return real_open(path, flags, *a, **kw)

        monkeypatch.setattr(daemonlock.os, "open", spying)
        daemonlock.ensure_workers_lock(db)
        assert flags_seen
        for flags in flags_seen:
            assert flags & os.O_CLOEXEC and flags & os.O_NOFOLLOW


class TestHoldWorkersLock:
    def test_returns_a_descriptor_that_holds_a_shared_lock(self, db):
        path = daemonlock.ensure_workers_lock(db)
        fd = daemonlock.hold_workers_lock(path)
        try:
            assert isinstance(fd, int) and fd >= 0
            assert daemonlock.workers_present(db) is True
        finally:
            os.close(fd)
        assert daemonlock.workers_present(db) is False

    def test_the_descriptor_is_close_on_exec(self, db):
        fd = daemonlock.hold_workers_lock(daemonlock.ensure_workers_lock(db))
        try:
            assert fcntl.fcntl(fd, fcntl.F_GETFD) & fcntl.FD_CLOEXEC
        finally:
            os.close(fd)

    @pytest.mark.skipif(not HAS_FORK, reason="needs fork")
    def test_an_exec_child_does_not_inherit_the_lock(self, db):
        fd = daemonlock.hold_workers_lock(daemonlock.ensure_workers_lock(db))
        r, w = os.pipe()
        pid = os.fork()
        if pid == 0:  # pragma: no cover - child
            try:
                os.close(r)
                os.dup2(w, 1)
                code = (
                    "import os\n"
                    "try:\n"
                    f"    os.fstat({fd}); print('inherited')\n"
                    "except OSError:\n"
                    "    print('closed')\n"
                )
                os.execv(sys.executable, [sys.executable, "-I", "-c", code])
            finally:
                os._exit(127)
        os.close(w)
        data = b""
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            chunk = os.read(r, 100)
            if not chunk:
                break
            data += chunk
        os.close(r)
        os.waitpid(pid, 0)
        os.close(fd)
        assert data.strip() == b"closed"

    def test_the_descriptor_is_read_only(self, db):
        fd = daemonlock.hold_workers_lock(daemonlock.ensure_workers_lock(db))
        try:
            assert fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE == os.O_RDONLY
        finally:
            os.close(fd)

    def test_two_shared_holders_coexist(self, db):
        path = daemonlock.ensure_workers_lock(db)
        a = daemonlock.hold_workers_lock(path)
        b = daemonlock.hold_workers_lock(path)
        try:
            assert a is not None and b is not None and a != b
            assert daemonlock.workers_present(db) is True
            os.close(a)
            assert daemonlock.workers_present(db) is True, "the second holder still holds it"
        finally:
            os.close(b)
        assert daemonlock.workers_present(db) is False

    def test_it_accepts_a_string_path(self, db):
        path = daemonlock.ensure_workers_lock(db)
        fd = daemonlock.hold_workers_lock(str(path))
        assert fd is not None
        os.close(fd)

    def test_a_missing_file_is_none_with_a_warning(self, db, caplog):
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemonlock"):
            assert daemonlock.hold_workers_lock(daemonlock.workers_lock_path(db)) is None
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "cannot open workers lock" in warnings[0].getMessage()

    def test_a_missing_file_is_not_created(self, db):
        daemonlock.hold_workers_lock(daemonlock.workers_lock_path(db))
        assert not daemonlock.workers_lock_path(db).exists()

    def test_a_symlink_is_none_with_a_warning(self, db, tmp_path, caplog):
        target = tmp_path / "t"
        target.write_bytes(b"")
        os.symlink(target, daemonlock.workers_lock_path(db))
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemonlock"):
            assert daemonlock.hold_workers_lock(daemonlock.workers_lock_path(db)) is None
        assert any(r.levelno == logging.WARNING for r in caplog.records)

    def test_a_transient_exclusive_holder_is_retried(self, db, real_contention_window, monkeypatch):
        """The holder (a momentary `workers_present` LOCK_EX) lets go the moment
        `hold_workers_lock` first sleeps -- a spy on the real `time.sleep` -- so the
        release can be neither late nor early, and no wall-clock duration is asserted."""
        path = daemonlock.ensure_workers_lock(db)
        holder = os.open(str(path), os.O_RDONLY)
        fcntl.flock(holder, fcntl.LOCK_EX)
        sleeps = []
        real_sleep = time.sleep

        def release_on_first_sleep(seconds):
            sleeps.append(seconds)
            if len(sleeps) == 1:
                os.close(holder)
            real_sleep(seconds)

        monkeypatch.setattr(daemonlock.time, "sleep", release_on_first_sleep)
        fd = None
        try:
            fd = daemonlock.hold_workers_lock(path)
            assert fd is not None, "a momentary LOCK_EX (workers_present) must not lose the worker its lock"
            assert sleeps == [daemonlock._CONTENTION_POLL]  # one real poll, then the lock was free
            assert fcntl.fcntl(fd, fcntl.F_GETFD) & fcntl.FD_CLOEXEC
        finally:
            if fd is not None:
                os.close(fd)
            if not sleeps:  # never contended: do not leak the holder descriptor
                os.close(holder)

    def test_a_persistent_exclusive_holder_gives_up_with_a_warning_and_no_leak(self, db, monkeypatch, caplog):
        monkeypatch.setattr(daemonlock, "_CONTENTION_WINDOW", 0.15)
        path = daemonlock.ensure_workers_lock(db)
        thread, stop = _transient_holder(path, fcntl.LOCK_EX, 10)
        before = fd_count()
        try:
            with caplog.at_level(logging.WARNING, logger="zfsbackup.daemonlock"):
                started = time.monotonic()
                assert daemonlock.hold_workers_lock(path) is None
                elapsed = time.monotonic() - started
            assert elapsed >= 0.1
            assert any("cannot lock workers lock" in r.getMessage() for r in caplog.records)
            assert fd_count() == before, "the failed attempt must close its descriptor"
        finally:
            stop.set()
            thread.join(5)

    def test_a_flock_error_other_than_contention_is_none_without_retry(self, db, monkeypatch, caplog):
        path = daemonlock.ensure_workers_lock(db)
        calls = []

        def eio(fd, op):
            calls.append(op)
            raise OSError(errno.EIO, "Input/output error")

        monkeypatch.setattr(daemonlock.fcntl, "flock", eio)
        before = fd_count()
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemonlock"):
            assert daemonlock.hold_workers_lock(path) is None
        assert len(calls) == 1
        assert fd_count() == before

    def test_holding_does_not_modify_or_delete_the_file(self, db):
        path = daemonlock.ensure_workers_lock(db)
        before = (path.stat().st_ino, path.stat().st_mtime_ns, mode_of(path))
        fd = daemonlock.hold_workers_lock(path)
        os.close(fd)
        assert path.exists()
        assert (path.stat().st_ino, path.stat().st_mtime_ns, mode_of(path)) == before

    def test_repeated_hold_and_close_leaks_nothing(self, db):
        path = daemonlock.ensure_workers_lock(db)
        before = fd_count()
        for _ in range(20):
            os.close(daemonlock.hold_workers_lock(path))
        assert fd_count() == before


class TestWorkersPresent:
    def test_a_missing_file_is_false(self, db):
        assert daemonlock.workers_present(db) is False

    def test_a_missing_directory_is_false(self, tmp_path):
        assert daemonlock.workers_present(tmp_path / "nodir" / "config.db") is False

    def test_a_path_component_that_is_a_file_is_false(self, tmp_path):
        (tmp_path / "afile").write_text("x")
        assert daemonlock.workers_present(tmp_path / "afile" / "config.db") is False

    def test_an_unlocked_file_is_false_and_stays_present(self, db):
        path = daemonlock.ensure_workers_lock(db)
        assert daemonlock.workers_present(db) is False
        assert path.exists()

    def test_it_creates_nothing(self, db):
        daemonlock.workers_present(db)
        assert not daemonlock.workers_lock_path(db).exists()

    def test_true_while_held_false_after_release(self, db):
        path = daemonlock.ensure_workers_lock(db)
        fd = daemonlock.hold_workers_lock(path)
        assert daemonlock.workers_present(db) is True
        assert daemonlock.workers_present(db) is True
        os.close(fd)
        assert daemonlock.workers_present(db) is False

    def test_an_exclusive_holder_counts_too(self, db):
        path = daemonlock.ensure_workers_lock(db)
        thread, stop = _transient_holder(path, fcntl.LOCK_EX, 10)
        try:
            assert daemonlock.workers_present(db) is True
        finally:
            stop.set()
            thread.join(5)

    def test_it_always_unlocks_and_closes(self, db):
        path = daemonlock.ensure_workers_lock(db)
        before = fd_count()
        for _ in range(10):
            assert daemonlock.workers_present(db) is False
        assert fd_count() == before
        # its momentary LOCK_EX is gone: a worker can take LOCK_SH at once
        fd = daemonlock.hold_workers_lock(path)
        assert fd is not None
        os.close(fd)

    def test_the_momentary_lock_does_not_disturb_a_holder(self, db):
        path = daemonlock.ensure_workers_lock(db)
        fd = daemonlock.hold_workers_lock(path)
        try:
            for _ in range(5):
                assert daemonlock.workers_present(db) is True
            assert fcntl.fcntl(fd, fcntl.F_GETFD) >= 0
            other = daemonlock.hold_workers_lock(path)
            assert other is not None
            os.close(other)
        finally:
            os.close(fd)

    def test_holders_in_a_real_process_are_seen_and_survive_sigkill(self, db):
        from _hermetic import hermetic_env

        path = daemonlock.ensure_workers_lock(db)
        proc = subprocess.Popen(
            [sys.executable, "-c", _WORKERS_HELPER, str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            env=hermetic_env(),
            cwd=str(REPO_ROOT),
        )
        try:
            assert proc.stdout.readline().strip() == "ready"
            assert daemonlock.workers_present(db) is True
            proc.send_signal(signal.SIGKILL)
            proc.wait(timeout=10)
            assert wait_until(lambda: daemonlock.workers_present(db) is False)
            assert path.exists(), "the workers lock file is never deleted"
        finally:
            proc.kill()
            proc.wait(timeout=10)
            proc.stdout.close()

    def test_a_process_that_exits_cleanly_drops_it(self, db):
        from _hermetic import hermetic_env

        path = daemonlock.ensure_workers_lock(db)
        code = (
            "import sys\nfrom zfsbackup import daemonlock\n"
            "fd = daemonlock.hold_workers_lock(sys.argv[1])\nprint('held' if fd is not None else 'failed')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code, str(path)],
            capture_output=True, text=True, timeout=60, env=hermetic_env(), cwd=str(REPO_ROOT),
        )
        assert result.stdout.strip() == "held"
        assert daemonlock.workers_present(db) is False

    def test_an_error_other_than_contention_raises_daemonlockerror(self, db, monkeypatch):
        daemonlock.ensure_workers_lock(db)
        real_flock = fcntl.flock

        def eio(fd, op):
            if op & fcntl.LOCK_EX:
                raise OSError(errno.EIO, "Input/output error")
            return real_flock(fd, op)

        monkeypatch.setattr(daemonlock.fcntl, "flock", eio)
        before = fd_count()
        with pytest.raises(DaemonLockError) as exc:
            daemonlock.workers_present(db)
        assert exc.value.errno == errno.EIO
        assert fd_count() == before

    def test_an_unopenable_file_raises_daemonlockerror(self, db, monkeypatch):
        daemonlock.ensure_workers_lock(db)
        real_open = os.open

        def deny(path, flags, *a, **kw):
            if str(path).endswith(".workers.lock"):
                raise PermissionError(errno.EACCES, "Permission denied")
            return real_open(path, flags, *a, **kw)

        monkeypatch.setattr(daemonlock.os, "open", deny)
        with pytest.raises(DaemonLockError) as exc:
            daemonlock.workers_present(db)
        assert exc.value.errno == errno.EACCES

    def test_a_symlink_raises_daemonlockerror(self, db, tmp_path):
        target = tmp_path / "t"
        target.write_bytes(b"")
        os.symlink(target, daemonlock.workers_lock_path(db))
        with pytest.raises(DaemonLockError):
            daemonlock.workers_present(db)

    def test_accepts_a_resolved_config_path(self, db):
        daemonlock.ensure_workers_lock(db)
        assert daemonlock.workers_present(resolve_config_path(str(db))) is False

    def test_it_is_independent_of_the_daemon_lock(self, db, held):
        held.append(acquire(db))
        daemonlock.ensure_workers_lock(db)
        assert daemonlock.workers_present(db) is False
        assert probe(db) is not None


# ===========================================================================
# G2 review fixes: device + inode proof, owner check, hidepid, padded writes,
# O_NONBLOCK.
# ===========================================================================


def _table_proof(monkeypatch, tmp_path, pid, *, lock_device="08:01", mount_device=(8, 1)):
    """A `flock_write_holders` that answers from a fixture lock table naming
    `pid` as the holder of THIS file's inode on `lock_device` (hex, as
    /proc/locks prints it) while the mount says `mount_device`. Runs the real
    parser on any host."""
    locks = tmp_path / "locks"

    def holders(fd):
        ino = os.fstat(fd).st_ino
        locks.write_text(f"1: FLOCK  ADVISORY  WRITE {pid} {lock_device}:{ino} 0 EOF\n")
        with monkeypatch.context() as m:
            m.setattr(procident, "_PROC_ROOT", str(tmp_path))
            return procident._linux_flock_write_holders(mount_device, ino)

    monkeypatch.setattr(procident, "flock_write_holders", holders)


@pytest.mark.integration
class TestRequestReloadHolderDevice:
    def test_the_same_device_and_inode_proves_the_holder(self, db, start_helper, monkeypatch, tmp_path, spy):
        helper = start_helper(db)
        _table_proof(monkeypatch, tmp_path, helper.pid)
        assert daemonlock.request_reload(db).outcome == "signalled"
        assert helper.wait_for("HUP")

    @pytest.mark.parametrize("lock_device", ["fe:09", "08:02", "09:01", "00:00"])
    def test_the_same_inode_on_another_device_is_not_the_holder_and_nothing_is_sent(
        self, db, start_helper, monkeypatch, tmp_path, spy, lock_device
    ):
        helper = start_helper(db)
        _table_proof(monkeypatch, tmp_path, helper.pid, lock_device=lock_device)
        result = daemonlock.request_reload(db)
        assert result.outcome in ("supervisor-gone", "unverified")
        assert result.outcome == "supervisor-gone"
        assert spy.calls == []
        assert not helper.got_hup()

    def test_a_device_that_cannot_be_established_is_unverified(self, db, start_helper, monkeypatch, spy):
        helper = start_helper(db)
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: None)
        assert daemonlock.request_reload(db).outcome == "unverified"
        assert spy.calls == []
        assert not helper.got_hup()


def _foreign_euid(monkeypatch):
    """Make the pid file (owned by us) look like it belongs to a third party."""
    monkeypatch.setattr(daemonlock.os, "geteuid", lambda: os.getuid() + 1)


class _OwnerProxy:
    def __init__(self, real, uid):
        self._real, self.st_uid = real, uid

    def __getattr__(self, name):
        return getattr(self._real, name)


@pytest.mark.integration
@pytest.mark.skipif(IS_ROOT, reason="root-owned files pass the owner check by definition")
class TestRequestReloadOwnerCheck:
    @pytest.fixture
    def uid_spy(self, monkeypatch):
        calls = []
        answer = {"uid": None}

        def process_uid(pid):
            calls.append(pid)
            return answer["uid"]

        monkeypatch.setattr(procident, "process_uid", process_uid)
        process_uid.calls, process_uid.answer = calls, answer
        return process_uid

    def test_a_third_party_owner_is_unverified_and_nothing_is_sent(self, db, start_helper, proof, monkeypatch, uid_spy, spy):
        helper = start_helper(db)
        proof(helper.pid)
        _foreign_euid(monkeypatch)
        uid_spy.answer["uid"] = os.getuid() + 5          # the target is someone else too
        result = daemonlock.request_reload(db)
        assert result.outcome == "unverified"
        assert f"pid file owned by uid {os.getuid()}" in result.detail
        assert spy.calls == []
        assert not helper.got_hup()

    def test_process_uid_is_asked_about_the_target_pid(self, db, start_helper, proof, monkeypatch, uid_spy, spy):
        helper = start_helper(db)
        proof(helper.pid)
        _foreign_euid(monkeypatch)
        daemonlock.request_reload(db)
        assert uid_spy.calls == [helper.pid]

    def test_a_file_owned_by_the_targets_uid_passes(self, db, start_helper, proof, monkeypatch, uid_spy, spy):
        helper = start_helper(db)
        proof(helper.pid)
        _foreign_euid(monkeypatch)
        uid_spy.answer["uid"] = os.getuid()               # the daemon wrote its own pid file
        assert daemonlock.request_reload(db).outcome == "signalled"
        assert helper.wait_for("HUP")

    def test_an_unreadable_target_uid_with_a_foreign_owner_is_unverified(self, db, start_helper, proof, monkeypatch, uid_spy, spy):
        helper = start_helper(db)
        proof(helper.pid)
        _foreign_euid(monkeypatch)
        uid_spy.answer["uid"] = None
        result = daemonlock.request_reload(db)
        assert result.outcome == "unverified" and "owned by uid" in result.detail
        assert not helper.got_hup()

    def test_an_euid_owner_passes_even_when_the_target_uid_is_unreadable(self, db, start_helper, proof, uid_spy, spy):
        helper = start_helper(db)
        proof(helper.pid)
        uid_spy.answer["uid"] = None
        assert daemonlock.request_reload(db).outcome == "signalled"
        assert uid_spy.calls == [], "no need to consult the target when we own the file"
        assert helper.wait_for("HUP")

    def test_a_root_owner_passes_even_when_the_target_uid_is_unreadable(self, db, start_helper, proof, monkeypatch, uid_spy, spy):
        helper = start_helper(db)
        proof(helper.pid)
        _foreign_euid(monkeypatch)
        real_fstat = os.fstat
        monkeypatch.setattr(daemonlock.os, "fstat", lambda fd: _OwnerProxy(real_fstat(fd), 0))
        uid_spy.answer["uid"] = None
        assert daemonlock.request_reload(db).outcome == "signalled"
        assert uid_spy.calls == []
        assert helper.wait_for("HUP")

    def test_the_owner_check_is_before_the_holder_proof(self, db, start_helper, monkeypatch, uid_spy, spy):
        helper = start_helper(db)
        asked = []
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: asked.append(1) or {helper.pid})
        _foreign_euid(monkeypatch)
        daemonlock.request_reload(db)
        assert asked == [], "the proof must not even be consulted for a third-party file"

    def test_a_pidfd_is_opened_before_the_owner_check(self, db, start_helper, monkeypatch, uid_spy, spy):
        order = []
        _FakePidfd(monkeypatch, order)
        helper = start_helper(db)
        monkeypatch.setattr(uid_spy, "calls", order, raising=False)
        monkeypatch.setattr(procident, "process_uid", lambda pid: order.append("process_uid") or None)
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: order.append("holders") or {helper.pid})
        _foreign_euid(monkeypatch)
        result = daemonlock.request_reload(db)
        assert result.outcome == "unverified"
        assert order == ["pidfd_open", "process_uid"]

    def test_the_whole_order_with_a_passing_owner(self, db, start_helper, monkeypatch, spy):
        order = []
        _FakePidfd(monkeypatch, order)
        helper = start_helper(db)
        monkeypatch.setattr(procident, "process_uid", lambda pid: order.append("process_uid") or os.getuid())
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: order.append("holders") or {helper.pid})
        _foreign_euid(monkeypatch)
        assert daemonlock.request_reload(db).outcome == "signalled"
        assert order == ["pidfd_open", "process_uid", "holders"]


@pytest.mark.integration
class TestRequestReloadHidepid:
    @pytest.fixture
    def order(self):
        return []

    def _setup(self, db, start_helper, monkeypatch, ident):
        helper = start_helper(db)
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: {helper.pid})
        monkeypatch.setattr(procident, "identity", lambda pid, token: ident)
        return helper

    def test_gone_after_a_successful_pidfd_open_is_unverified_and_nothing_is_sent(
        self, db, start_helper, monkeypatch, order
    ):
        pidfd = _FakePidfd(monkeypatch, order)
        helper = self._setup(db, start_helper, monkeypatch, "gone")
        spy = SignalSpy(monkeypatch, passthrough=False)
        result = daemonlock.request_reload(db)
        assert result.outcome == "unverified"
        assert "/proc hidden" in result.detail
        assert spy.calls == [] and pidfd.sent == []
        assert order == ["pidfd_open"]
        assert all(pidfd.fd_is_closed(fd) for fd, _ in pidfd.opened)

    def test_gone_without_a_pidfd_is_supervisor_gone(self, db, start_helper, monkeypatch, kill_path):
        helper = self._setup(db, start_helper, monkeypatch, "gone")
        spy = SignalSpy(monkeypatch, passthrough=False)
        assert daemonlock.request_reload(db).outcome == "supervisor-gone"
        assert spy.calls == []

    def test_gone_after_a_failed_pidfd_open_falls_back_to_supervisor_gone(self, db, start_helper, monkeypatch, order):
        pidfd = _FakePidfd(monkeypatch, order)
        pidfd.open_error = OSError(errno.EPERM, "Operation not permitted")
        helper = self._setup(db, start_helper, monkeypatch, "gone")
        spy = SignalSpy(monkeypatch, passthrough=False)
        assert daemonlock.request_reload(db).outcome == "supervisor-gone"
        assert spy.calls == []

    def test_mismatch_is_supervisor_gone_with_a_pidfd(self, db, start_helper, monkeypatch, order):
        pidfd = _FakePidfd(monkeypatch, order)
        helper = self._setup(db, start_helper, monkeypatch, "mismatch")
        spy = SignalSpy(monkeypatch, passthrough=False)
        result = daemonlock.request_reload(db)
        assert result.outcome == "supervisor-gone" and "mismatch" in result.detail
        assert spy.calls == [] and pidfd.sent == []

    def test_mismatch_is_supervisor_gone_without_a_pidfd(self, db, start_helper, monkeypatch, kill_path):
        helper = self._setup(db, start_helper, monkeypatch, "mismatch")
        spy = SignalSpy(monkeypatch, passthrough=False)
        assert daemonlock.request_reload(db).outcome == "supervisor-gone"
        assert spy.calls == []


# ---------------------------------------------------------------------------
# The pid file is written padded, then truncated
# ---------------------------------------------------------------------------


class TestPaddedWrite:
    def _long(self, lock):
        lock.set_state(applied=(1, 5), target=(1, 6), rejected=(1, 4))
        return read(lock.path)

    def test_a_shorter_rewrite_without_ftruncate_leaves_only_blank_lines_after_the_new_content(
        self, db, lockfile, held, monkeypatch
    ):
        lock = acquire(db)
        held.append(lock)
        old = self._long(lock)
        monkeypatch.setattr(daemonlock.os, "ftruncate", lambda fd, n: None)
        lock.set_state(target=None, rejected=None)
        raw = read(lockfile)
        new = daemonlock._format(os.getpid(), 5, lock.start, (1, 5), None, None)
        assert len(new) < len(old)
        assert raw.startswith(new)
        assert raw[len(new):].strip(b"\n") == b"" and len(raw) == len(old)
        assert b"target" not in raw and b"rejected" not in raw, "the old tail leaked"

    def test_probe_parses_the_padded_file_as_the_new_state_and_stable(self, db, held, monkeypatch):
        lock = acquire(db)
        held.append(lock)
        self._long(lock)
        monkeypatch.setattr(daemonlock.os, "ftruncate", lambda fd, n: None)
        lock.set_state(target=None, rejected=None)
        info = probe(db)
        assert info.stable is True
        assert (info.applied, info.target, info.rejected) == ((1, 5), None, None)
        assert info.generation == 5 and info.start == lock.start

    def test_a_failing_ftruncate_is_only_a_warning_and_the_file_is_still_right(self, db, lockfile, held, monkeypatch, caplog):
        lock = acquire(db)
        held.append(lock)
        self._long(lock)

        def boom(fd, n):
            raise OSError(errno.EIO, "io")

        monkeypatch.setattr(daemonlock.os, "ftruncate", boom)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemonlock"):
            lock.set_state(target=None, rejected=None)
        assert any(r.levelno == logging.WARNING for r in caplog.records)
        info = probe(db)
        assert (info.applied, info.target, info.rejected) == ((1, 5), None, None)
        assert b"target" not in read(lockfile)

    def test_a_reader_between_pwrite_and_ftruncate_never_sees_the_old_tail(self, db, lockfile, held, monkeypatch):
        lock = acquire(db)
        held.append(lock)
        self._long(lock)
        seen = []
        real = os.ftruncate

        def midway(fd, n):
            rfd = os.open(str(lockfile), os.O_RDONLY)
            try:
                a = daemonlock._read_all(rfd)
                b = daemonlock._read_all(rfd)
            finally:
                os.close(rfd)
            seen.append((a, b))
            real(fd, n)

        monkeypatch.setattr(daemonlock.os, "ftruncate", midway)
        lock.set_state(target=None, rejected=None)
        (a, b), = seen
        assert a == b, "the two consecutive reads agreed -- the stable-read check would accept them"
        parsed = daemonlock._parse(a)
        assert (parsed.applied, parsed.target, parsed.rejected) == ((1, 5), None, None)
        assert b"target" not in a

    @pytest.mark.parametrize("grow", [False, True])
    def test_the_pwrite_length_is_the_larger_of_old_and_new(self, db, held, monkeypatch, grow):
        lock = acquire(db)
        held.append(lock)
        if not grow:
            self._long(lock)
        lengths = []
        real = os.pwrite

        def spying(fd, data, off):
            lengths.append((len(data), off))
            return real(fd, data, off)

        monkeypatch.setattr(daemonlock.os, "pwrite", spying)
        old_len = os.fstat(lock._fd).st_size
        if grow:
            lock.set_state(applied=(1, 5), target=(1, 6), rejected=(1, 4))
        else:
            lock.set_state(target=None, rejected=None)
        new_len = len(daemonlock._format(lock.pid, lock.generation, lock.start, lock.applied, lock.target, lock.rejected))
        assert lengths == [(max(old_len, new_len), 0)]

    def test_the_first_write_is_not_padded(self, db, lockfile, monkeypatch):
        lengths = []
        real = os.pwrite
        monkeypatch.setattr(daemonlock.os, "pwrite", lambda fd, data, off: lengths.append(len(data)) or real(fd, data, off))
        lock = acquire(db, generation=3)
        try:
            assert lengths == [len(read(lockfile))]
        finally:
            lock.release()

    def test_after_a_real_ftruncate_the_file_is_exactly_the_new_bytes(self, db, lockfile, held):
        lock = acquire(db)
        held.append(lock)
        self._long(lock)
        lock.set_state(target=None, rejected=None)
        assert read(lockfile) == daemonlock._format(os.getpid(), 5, lock.start, (1, 5), None, None)

    def test_a_longer_rewrite_is_exact(self, db, lockfile, held):
        lock = acquire(db)
        held.append(lock)
        lock.set_state(applied=(1, 5), target=(1, 6), rejected=(1, 4))
        assert read(lockfile) == daemonlock._format(os.getpid(), 5, lock.start, (1, 5), (1, 6), (1, 4))

    def test_a_failing_fstat_means_no_padding_not_a_failed_write(self, db, lockfile, held, monkeypatch):
        lock = acquire(db)
        held.append(lock)
        monkeypatch.setattr(daemonlock.os, "fstat", lambda fd: (_ for _ in ()).throw(OSError(errno.EIO, "io")))
        lock.set_state(applied=(1, 9))
        monkeypatch.undo()
        assert daemonlock._parse(read(lockfile)).applied == (1, 9)


# ---------------------------------------------------------------------------
# O_NONBLOCK: a FIFO planted at a lock path cannot hang anything
# ---------------------------------------------------------------------------


class _Hang(BaseException):
    """Not an `Exception` (let alone the `OSError` a `TimeoutError` is): the code
    under test has `except OSError` / `except Exception` arms that would swallow
    the alarm and carry on into the next blocking call."""


@contextlib.contextmanager
def _guard(seconds=5):
    """A hang becomes a failure: SIGALRM raises `_Hang` in the main thread."""
    def fire(signum, frame):
        raise _Hang("blocked: a FIFO open hung")

    old = signal.signal(signal.SIGALRM, fire)
    signal.alarm(seconds)
    started = time.monotonic()
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)
    assert time.monotonic() - started < 1.5, "a FIFO took too long to refuse"


fifo = pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="needs mkfifo")


@fifo
class TestFifoAtThePidFile:
    def test_probe_refuses(self, db, lockfile):
        os.mkfifo(lockfile)
        with _guard(), pytest.raises(DaemonLockError) as exc:
            probe(db)
        assert "not a regular file" in str(exc.value)

    def test_request_reload_is_error(self, db, lockfile, spy):
        os.mkfifo(lockfile)
        with _guard():
            result = daemonlock.request_reload(db)
        assert result.outcome == "error" and "not a regular file" in result.detail
        assert spy.calls == []

    def test_acquire_refuses_without_hanging(self, db, lockfile):
        os.mkfifo(lockfile)
        with _guard(), pytest.raises(DaemonLockError):
            acquire(db)


@fifo
class TestFifoAtTheWorkersLock:
    def test_workers_present_refuses(self, db):
        os.mkfifo(daemonlock.workers_lock_path(db))
        with _guard(), pytest.raises(DaemonLockError):
            daemonlock.workers_present(db)

    def test_hold_workers_lock_is_none_with_a_warning(self, db, caplog):
        path = daemonlock.workers_lock_path(db)
        os.mkfifo(path)
        with _guard(), caplog.at_level(logging.WARNING, logger="zfsbackup.daemonlock"):
            assert daemonlock.hold_workers_lock(path) is None
        assert any(r.levelno == logging.WARNING and "not a regular file" in r.getMessage() for r in caplog.records)

    def test_ensure_workers_lock_refuses(self, db):
        os.mkfifo(daemonlock.workers_lock_path(db))
        with _guard(), pytest.raises(DaemonLockError):
            daemonlock.ensure_workers_lock(db)

    def test_a_refused_hold_leaks_no_descriptor(self, db):
        path = daemonlock.workers_lock_path(db)
        os.mkfifo(path)
        before = fd_count()
        with _guard():
            daemonlock.hold_workers_lock(path)
            with pytest.raises(DaemonLockError):
                daemonlock.workers_present(db)
        assert fd_count() == before


class TestOpenFlags:
    WANT = os.O_NONBLOCK | os.O_CLOEXEC | os.O_NOFOLLOW

    @pytest.fixture
    def flags(self, monkeypatch):
        seen = []
        real = os.open

        def spying(path, fl, *a, **kw):
            if ".daemon.pid" in str(path) or ".workers.lock" in str(path):
                seen.append(fl)
            return real(path, fl, *a, **kw)

        monkeypatch.setattr(daemonlock.os, "open", spying)
        return seen

    def check(self, flags):
        assert flags, "no open was observed"
        for fl in flags:
            assert fl & self.WANT == self.WANT, f"flags {fl:#x} lack O_NONBLOCK|O_CLOEXEC|O_NOFOLLOW"

    def test_probe(self, db, held, flags):
        held.append(acquire(db))
        flags.clear()
        probe(db)
        self.check(flags)

    def test_acquire(self, db, held, flags):
        held.append(acquire(db))
        self.check(flags)

    def test_request_reload(self, db, held, flags):
        held.append(acquire(db))
        flags.clear()
        daemonlock.request_reload(db)
        self.check(flags)

    def test_ensure_workers_lock_create_and_reopen(self, db, flags):
        daemonlock.ensure_workers_lock(db)
        daemonlock.ensure_workers_lock(db)
        assert len(flags) == 3          # create; the failed O_EXCL retry; the reopen
        self.check(flags)

    def test_hold_workers_lock(self, db, flags):
        path = daemonlock.ensure_workers_lock(db)
        flags.clear()
        os.close(daemonlock.hold_workers_lock(path))
        self.check(flags)

    def test_workers_present(self, db, flags):
        daemonlock.ensure_workers_lock(db)
        flags.clear()
        daemonlock.workers_present(db)
        self.check(flags)

    def test_regular_files_still_lock_normally_with_nonblock(self, db, held):
        lock = acquire(db)
        held.append(lock)
        assert probe(db) is not None
        path = daemonlock.ensure_workers_lock(db)
        fd = daemonlock.hold_workers_lock(path)
        assert daemonlock.workers_present(db) is True
        os.close(fd)
        assert daemonlock.workers_present(db) is False


# ===========================================================================
# Zombies, supervisor_state, and the machine-readable reason_code
# ===========================================================================

import dataclasses  # noqa: E402
import types  # noqa: E402

HAS_TOKENS = IS_LINUX or sys.platform == "darwin"


@pytest.fixture
def zombie():
    """`zombie()` -> `(popen, token)`: an exited child nobody has reaped (and the
    start token it had while alive). Reaped at teardown."""
    from _hermetic import hermetic_env

    made = []

    def make():
        proc = subprocess.Popen(
            [sys.executable, "-c", "import sys; sys.stdin.read()"],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=hermetic_env(), cwd=str(REPO_ROOT),
        )
        made.append(proc)
        token = None
        deadline = time.monotonic() + 30
        while token is None and time.monotonic() < deadline:
            token = procident.start_token(proc.pid)
            time.sleep(0.02)
        proc.stdin.close()
        assert wait_until(lambda: procident.is_zombie(proc.pid) is True, 30), "child never became a zombie"
        return proc, token

    yield make
    for proc in made:
        try:
            _REAL_KILL(proc.pid, signal.SIGKILL)
        except OSError:
            pass
        proc.wait(timeout=10)


@pytest.mark.skipif(not HAS_TOKENS, reason="zombie detection needs /proc or sysctl")
class TestZombieIsNotAlive:
    def test_pid_is_alive_is_false_for_an_unreaped_child(self, zombie):
        proc, _ = zombie()
        assert os.kill(proc.pid, 0) is None          # the probe alone would say "alive"
        assert daemonlock.pid_is_alive(proc.pid) is False

    def test_it_is_true_for_a_live_process_and_false_once_reaped(self, zombie):
        assert daemonlock.pid_is_alive(os.getpid()) is True
        proc, _ = zombie()
        proc.wait(timeout=10)
        assert daemonlock.pid_is_alive(proc.pid) is False

    def test_a_permission_error_still_means_alive_unless_a_zombie(self, monkeypatch):
        monkeypatch.setattr(daemonlock.os, "kill", _raise(PermissionError()))
        monkeypatch.setattr(procident, "is_zombie", lambda pid: False)
        assert daemonlock.pid_is_alive(4321) is True
        monkeypatch.setattr(procident, "is_zombie", lambda pid: True)
        assert daemonlock.pid_is_alive(4321) is False

    def test_an_undeterminable_zombie_state_does_not_change_the_answer(self, monkeypatch):
        monkeypatch.setattr(daemonlock.os, "kill", lambda pid, sig: None)
        monkeypatch.setattr(procident, "is_zombie", lambda pid: None)
        assert daemonlock.pid_is_alive(4321) is True

    def test_a_missing_process_is_not_asked_about_zombies(self, monkeypatch):
        monkeypatch.setattr(daemonlock.os, "kill", _raise(ProcessLookupError()))
        monkeypatch.setattr(procident, "is_zombie", lambda pid: pytest.fail("consulted"))
        assert daemonlock.pid_is_alive(4321) is False

    def test_the_stale_supervisor_wording_follows(self, db, zombie):
        proc, _ = zombie()
        exc = DaemonAlreadyRunning(lock_path(db), proc.pid, 1)
        assert exc.pid_alive is False
        assert "is gone" in str(exc)


class TestSupervisorState:
    def info(self, pid, start):
        return types.SimpleNamespace(pid=pid, start=start)

    @pytest.fixture
    def world(self, monkeypatch):
        """Scriptable `identity` and `pid_is_alive`."""
        state = {"identity": "unknown", "alive": None, "identity_calls": []}

        def identity(pid, token):
            state["identity_calls"].append((pid, token))
            if isinstance(state["identity"], Exception):
                raise state["identity"]
            return state["identity"]

        monkeypatch.setattr(procident, "identity", identity)
        monkeypatch.setattr(daemonlock, "pid_is_alive", lambda pid: state["alive"])
        return state

    def test_a_matching_token_is_alive(self, world):
        world["identity"] = "match"
        world["alive"] = False                      # never consulted
        assert daemonlock.supervisor_state(self.info(4242, "tok")) == "alive"
        assert world["identity_calls"] == [(4242, "tok")]

    def test_a_mismatched_token_on_a_live_pid_is_gone(self, world):
        world["identity"], world["alive"] = "mismatch", True
        assert daemonlock.supervisor_state(self.info(4242, "tok")) == "gone"

    def test_a_zombie_is_gone(self, world):
        world["identity"], world["alive"] = "gone", True
        assert daemonlock.supervisor_state(self.info(4242, "tok")) == "gone"

    def test_no_token_on_a_live_pid_is_alive(self, world):
        world["alive"] = True
        assert daemonlock.supervisor_state(self.info(4242, None)) == "alive"
        assert world["identity_calls"] == [], "no token: identity is not asked"

    def test_no_token_on_a_dead_pid_is_gone(self, world):
        world["alive"] = False
        assert daemonlock.supervisor_state(self.info(4242, None)) == "gone"

    @pytest.mark.parametrize("pid", [None, 0, -1, -4242, True, False, "42", 4.0])
    def test_an_unusable_pid_is_unknown_and_nothing_is_probed(self, world, pid):
        world["alive"] = True
        assert daemonlock.supervisor_state(self.info(pid, "tok")) == "unknown"
        assert world["identity_calls"] == []

    @pytest.mark.parametrize(
        "alive, expected", [(True, "alive"), (False, "gone"), (None, "unknown")]
    )
    def test_identity_unknown_falls_back_to_pid_is_alive(self, world, alive, expected):
        world["identity"], world["alive"] = "unknown", alive
        assert daemonlock.supervisor_state(self.info(4242, "tok")) == expected

    @pytest.mark.parametrize("alive, expected", [(True, "alive"), (False, "gone"), (None, "unknown")])
    def test_an_empty_token_is_treated_like_no_token(self, world, alive, expected):
        world["alive"] = alive
        assert daemonlock.supervisor_state(self.info(4242, "")) == expected
        assert world["identity_calls"] == []

    def test_an_exception_is_unknown(self, world):
        world["identity"] = RuntimeError("boom")
        assert daemonlock.supervisor_state(self.info(4242, "tok")) == "unknown"

    def test_an_object_without_the_attributes_is_unknown(self, world):
        assert daemonlock.supervisor_state(object()) == "unknown"

    def test_daemoninfo_delegates(self, monkeypatch):
        seen = []
        monkeypatch.setattr(daemonlock, "supervisor_state", lambda info: seen.append(info) or "sentinel")
        info = DaemonInfo(1234, 5, start="tok")
        assert info.supervisor_state == "sentinel"
        assert seen == [info]

    @pytest.mark.skipif(not HAS_TOKENS, reason="needs start tokens")
    def test_for_real_this_process_is_alive_a_wrong_token_is_gone(self):
        token = procident.start_token(os.getpid())
        assert DaemonInfo(os.getpid(), 1, start=token).supervisor_state == "alive"
        assert DaemonInfo(os.getpid(), 1, start="bogus:1").supervisor_state == "gone"
        assert DaemonInfo(os.getpid(), 1).supervisor_state == "alive"            # legacy: pid_is_alive

    @pytest.mark.skipif(not HAS_TOKENS, reason="zombie detection needs /proc or sysctl")
    def test_for_real_a_zombie_is_gone_with_or_without_a_token(self, zombie):
        proc, token = zombie()
        assert DaemonInfo(proc.pid, 1, start=token).supervisor_state == "gone"
        assert DaemonInfo(proc.pid, 1).supervisor_state == "gone"
        assert DaemonInfo(proc.pid, 1, start="").supervisor_state == "gone"

    def test_it_is_evaluated_at_call_time(self, world):
        info = DaemonInfo(4242, 1, start="tok")
        world["identity"] = "match"
        assert info.supervisor_state == "alive"
        world["identity"] = "gone"
        assert info.supervisor_state == "gone"


class TestReloadRequestReasonCode:
    def test_it_defaults_to_empty_and_comes_last(self):
        names = [f.name for f in dataclasses.fields(daemonlock.ReloadRequest)]
        assert names == ["outcome", "pid", "info", "detail", "reason_code"]
        assert daemonlock.ReloadRequest("x").reason_code == ""
        assert daemonlock.ReloadRequest("x", 1, None, "d").reason_code == ""

    def test_positional_construction_without_it_still_works(self):
        r = daemonlock.ReloadRequest("unverified", 5, None, "why")
        assert (r.outcome, r.pid, r.detail, r.reason_code) == ("unverified", 5, "why", "")

    def test_it_is_frozen(self):
        with pytest.raises(dataclasses.FrozenInstanceError):
            daemonlock.ReloadRequest("x").reason_code = "y"

    def test_every_documented_code_is_in_the_docstring(self):
        doc = daemonlock.ReloadRequest.__doc__
        for code in (
            "torn-read", "no-identity", "refused-pid", "pid-file-owner", "not-linux", "locks-unreadable",
            "mount-device", "lock-holders-unknown", "proc-hidden", "identity-unknown",
            "no-such-process", "not-lock-holder", "zombie", "reused", "exited",
        ):
            assert f"`{code}`" in doc, code


@pytest.mark.integration
class TestRequestReloadReasonCodes:
    """Every outcome carries a stable `reason_code`."""

    def _check(self, result, outcome, code, detail=None):
        assert result.outcome == outcome
        assert result.reason_code == code, result
        if detail:
            assert detail in result.detail, result.detail

    # -- no code -------------------------------------------------------------

    def test_outcomes_with_nothing_to_add_have_an_empty_code(self, db, lockfile, start_helper, proof, spy):
        self._check(daemonlock.request_reload(db), "no-daemon", "")
        helper = start_helper(db, spec="{pid}\n3\nstart={start}\napplied=1:3\n")
        proof(helper.pid)
        self._check(daemonlock.request_reload(db, skip_if_applied=(1, 3)), "already-applied", "")
        self._check(daemonlock.request_reload(db, skip_if_applied=(1, 4)), "signalled", "")

    def test_legacy_has_an_empty_code(self, db, start_helper):
        start_helper(db, spec="{pid}\n3\n")
        self._check(daemonlock.request_reload(db), "legacy", "")

    def test_error_has_an_empty_code(self, db, lockfile):
        os.symlink(db.parent / "elsewhere", lockfile)
        self._check(daemonlock.request_reload(db), "error", "")

    def test_not_permitted_has_an_empty_code(self, db, start_helper, proof, no_signal, monkeypatch):
        helper = start_helper(db)
        proof(helper.pid)
        monkeypatch.setattr(os, "kill", _raise(PermissionError(errno.EPERM, "Operation not permitted")))
        self._check(daemonlock.request_reload(db), "not-permitted", "")

    # -- unverified ----------------------------------------------------------

    def test_torn_read(self, db, start_helper, proof, monkeypatch):
        helper = start_helper(db)
        proof(helper.pid)
        good = read(lock_path(db))
        _fake_pread(monkeypatch, [good, good + b"applied=1:1\n"] * 4)
        self._check(daemonlock.request_reload(db), "unverified", "torn-read")

    def test_no_identity(self, db, start_helper):
        start_helper(db, spec="{pid}\n3\nstart=\n")
        self._check(daemonlock.request_reload(db), "unverified", "no-identity")

    @pytest.mark.parametrize("pid", [0, 1, -3])
    def test_refused_pid(self, db, lockfile, pid):
        fd = hold_raw(lockfile, f"{pid}\n3\nstart=tok\n".encode())
        try:
            self._check(daemonlock.request_reload(db), "unverified", "refused-pid", f"refusing to signal pid {pid}")
        finally:
            os.close(fd)

    def test_our_own_pid_is_a_refused_pid(self, db, held):
        held.append(acquire(db))
        self._check(daemonlock.request_reload(db), "unverified", "refused-pid")

    @pytest.mark.skipif(IS_ROOT, reason="root-owned files pass the owner check")
    def test_pid_file_owner(self, db, start_helper, proof, monkeypatch):
        helper = start_helper(db)
        proof(helper.pid)
        monkeypatch.setattr(daemonlock.os, "geteuid", lambda: os.getuid() + 1)
        monkeypatch.setattr(procident, "process_uid", lambda pid: None)
        self._check(daemonlock.request_reload(db), "unverified", "pid-file-owner", "pid file owned by uid")

    @pytest.mark.parametrize(
        "why, code, detail",
        [
            (procident.UNAVAILABLE_NOT_LINUX, "not-linux", "signals are verified only on Linux"),
            (procident.UNAVAILABLE_LOCKS_UNREADABLE, "locks-unreadable", "/proc/locks is not readable"),
            (procident.UNAVAILABLE_MOUNT_DEVICE, "mount-device", "cannot resolve the pid file's mount device"),
            (procident.UNAVAILABLE_UNKNOWN, "lock-holders-unknown", "the daemon lock's holders could not be determined"),
            ("something-new", "lock-holders-unknown", "the daemon lock's holders could not be determined"),
            (None, "lock-holders-unknown", "the daemon lock's holders could not be determined"),
        ],
    )
    def test_each_unavailable_reason_has_its_own_detail_and_code(self, db, start_helper, monkeypatch, spy, why, code, detail):
        helper = start_helper(db)
        asked = []
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: None)
        monkeypatch.setattr(procident, "flock_holders_unavailable_reason", lambda fd: asked.append(fd) or why)
        result = daemonlock.request_reload(db)
        self._check(result, "unverified", code, detail)
        assert len(asked) == 1 and isinstance(asked[0], int)
        assert spy.calls == []
        assert not helper.got_hup()

    @pytest.mark.skipif(IS_LINUX, reason="Linux can read its lock table")
    def test_on_this_platform_the_real_reason_is_not_linux(self, db, start_helper, spy):
        helper = start_helper(db)
        self._check(daemonlock.request_reload(db), "unverified", "not-linux", "signals are verified only on Linux")
        assert not helper.got_hup()

    def test_the_reason_is_not_asked_when_the_proof_answers(self, db, start_helper, proof, monkeypatch, spy):
        helper = start_helper(db)
        proof(helper.pid)
        monkeypatch.setattr(procident, "flock_holders_unavailable_reason", lambda fd: pytest.fail("asked"))
        assert daemonlock.request_reload(db).outcome == "signalled"

    def test_proc_hidden(self, db, start_helper, monkeypatch):
        pidfd = _FakePidfd(monkeypatch, [])
        helper = start_helper(db)
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: {helper.pid})
        monkeypatch.setattr(procident, "identity", lambda pid, tok: "gone")
        monkeypatch.setattr(procident, "is_zombie", lambda pid: False)
        self._check(daemonlock.request_reload(db), "unverified", "proc-hidden", "/proc hidden")

    def test_identity_unknown(self, db, start_helper, proof, monkeypatch, kill_path):
        helper = start_helper(db)
        proof(helper.pid)
        monkeypatch.setattr(procident, "identity", lambda pid, tok: "unknown")
        self._check(daemonlock.request_reload(db), "unverified", "identity-unknown")

    # -- supervisor-gone -----------------------------------------------------

    def test_no_such_process_at_pidfd_open(self, db, start_helper, monkeypatch):
        pidfd = _FakePidfd(monkeypatch, [])
        pidfd.open_error = ProcessLookupError(errno.ESRCH, "No such process")
        start_helper(db)
        self._check(daemonlock.request_reload(db), "supervisor-gone", "no-such-process")

    def test_no_such_process_from_identity_without_a_pidfd(self, db, start_helper, proof, monkeypatch, kill_path):
        helper = start_helper(db)
        proof(helper.pid)
        monkeypatch.setattr(procident, "identity", lambda pid, tok: "gone")
        monkeypatch.setattr(procident, "is_zombie", lambda pid: None)
        self._check(daemonlock.request_reload(db), "supervisor-gone", "no-such-process")

    def test_not_lock_holder(self, db, start_helper, monkeypatch, kill_path):
        start_helper(db)
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: set())
        self._check(daemonlock.request_reload(db), "supervisor-gone", "not-lock-holder")

    def test_reused(self, db, start_helper, proof, monkeypatch, kill_path):
        helper = start_helper(db, spec="{pid}\n3\nstart=bogus:1\n")
        proof(helper.pid)
        monkeypatch.setattr(procident, "identity", lambda pid, tok: "mismatch")
        self._check(daemonlock.request_reload(db), "supervisor-gone", "reused", "mismatch")

    def test_exited(self, db, start_helper, proof, monkeypatch, no_signal):
        helper = start_helper(db)
        proof(helper.pid)
        monkeypatch.setattr(os, "kill", _raise(ProcessLookupError(errno.ESRCH, "x")))
        self._check(daemonlock.request_reload(db), "supervisor-gone", "exited")

    @pytest.mark.skipif(not HAS_TOKENS, reason="zombie detection needs /proc or sysctl")
    def test_a_zombie_still_listed_as_a_holder_is_supervisor_gone_and_nothing_is_signalled(
        self, db, start_helper, zombie, monkeypatch, spy
    ):
        proc, token = zombie()
        holder = start_helper(db, spec=f"{proc.pid}\n3\nstart={token}\n")
        # the lock table still lists the dead locker (a forked worker holds the file)
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: {proc.pid, holder.pid})
        spy.calls.clear()
        result = daemonlock.request_reload(db)
        self._check(result, "supervisor-gone", "zombie", "has exited (a zombie, not yet reaped)")
        assert result.pid == proc.pid
        assert spy.calls == []
        assert not holder.got_hup()

    @pytest.mark.skipif(not HAS_TOKENS, reason="zombie detection needs /proc or sysctl")
    def test_the_zombie_verdict_beats_proc_hidden_even_with_a_pidfd(self, db, start_helper, zombie, monkeypatch):
        order = []
        pidfd = _FakePidfd(monkeypatch, order)
        proc, token = zombie()
        start_helper(db, spec=f"{proc.pid}\n3\nstart={token}\n")
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: {proc.pid})
        spy = SignalSpy(monkeypatch, passthrough=False)
        pidfd.opened.clear()
        result = daemonlock.request_reload(db)
        self._check(result, "supervisor-gone", "zombie")
        assert spy.calls == [] and pidfd.sent == []
        assert order == ["pidfd_open"]                     # the pidfd opened fine on a zombie
        assert all(pidfd.fd_is_closed(fd) for fd, _ in pidfd.opened)

    # -- every code the matrix can produce is a documented one ----------------

    def test_a_signalled_request_has_an_empty_code(self, db, start_helper, proof, spy):
        helper = start_helper(db)
        proof(helper.pid)
        self._check(daemonlock.request_reload(db), "signalled", "")


# ===========================================================================
# Step 8b: the recycled-pid proof (the target must have the pid file open)
# ===========================================================================


def _signal_zero_only(calls):
    return all(sig == 0 for _, _, sig in calls)


@pytest.mark.integration
class TestRequestReloadRecycledPidProof:
    @pytest.fixture
    def rig(self, db, start_helper, proof, monkeypatch):
        """A real helper holding the lock, every other step satisfied, and the
        step-8b proof recorded and scripted: `rig.answer` is what
        `process_has_file_open` says (True/False/None)."""
        helper = start_helper(db)
        proof(helper.pid)
        calls = []

        class Rig:
            pass

        rig = Rig()
        rig.helper, rig.calls, rig.answer = helper, calls, True

        def has_open(pid, st_dev, st_ino):
            calls.append((pid, st_dev, st_ino))
            return rig.answer

        monkeypatch.setattr(procident, "process_has_file_open", has_open)
        return rig

    # -- the three outcomes ---------------------------------------------------

    def test_true_signals(self, rig, db, kill_path, spy):
        assert daemonlock.request_reload(db).outcome == "signalled"
        assert rig.helper.wait_for("HUP")

    def test_false_is_supervisor_gone_file_not_open_and_nothing_is_signalled(self, rig, db, kill_path, spy):
        rig.answer = False
        result = daemonlock.request_reload(db)
        assert (result.outcome, result.reason_code) == ("supervisor-gone", "file-not-open")
        assert f"pid {rig.helper.pid} does not have the daemon lock file open" in result.detail
        assert spy.calls == [], "not even a signal-0 probe for a definite answer"
        assert not rig.helper.got_hup()

    def test_none_is_unverified_fd_table_unreadable_and_only_signal_zero_is_sent(self, rig, db, kill_path, spy):
        rig.answer = None
        result = daemonlock.request_reload(db)
        assert (result.outcome, result.reason_code) == ("unverified", "fd-table-unreadable")
        assert "cannot read the open files of pid" in result.detail
        assert spy.calls == [("kill", rig.helper.pid, 0)]
        assert not rig.helper.got_hup()
        assert rig.helper.proc.poll() is None, "the target must stay alive"

    def test_none_with_eperm_on_signal_zero_is_not_permitted_other_user(self, rig, db, kill_path, monkeypatch):
        rig.answer = None
        sent = []

        def kill(pid, sig):
            sent.append((pid, sig))
            raise PermissionError(errno.EPERM, "Operation not permitted")

        monkeypatch.setattr(os, "kill", kill)
        result = daemonlock.request_reload(db)
        assert (result.outcome, result.reason_code) == ("not-permitted", "other-user")
        assert result.detail == f"pid {rig.helper.pid} runs as another user; not permitted to signal it"
        assert sent == [(rig.helper.pid, 0)]

    @pytest.mark.parametrize(
        "exc",
        [
            ProcessLookupError(errno.ESRCH, "No such process"),
            OSError(errno.EINVAL, "Invalid argument"),
            OSError(errno.EIO, "io"),
        ],
    )
    def test_none_with_any_other_signal_zero_failure_is_unverified(self, rig, db, kill_path, monkeypatch, exc):
        rig.answer = None
        monkeypatch.setattr(os, "kill", _raise(exc))
        result = daemonlock.request_reload(db)
        assert (result.outcome, result.reason_code) == ("unverified", "fd-table-unreadable")

    def test_none_with_a_successful_signal_zero_is_unverified(self, rig, db, kill_path, monkeypatch):
        rig.answer = None
        monkeypatch.setattr(os, "kill", lambda pid, sig: None)
        assert daemonlock.request_reload(db).reason_code == "fd-table-unreadable"

    # -- the signal-0 route ----------------------------------------------------

    def test_with_a_pidfd_signal_zero_goes_through_the_pidfd_and_never_os_kill(self, db, start_helper, monkeypatch):
        order = []
        pidfd = _FakePidfd(monkeypatch, order)
        helper = start_helper(db)
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: {helper.pid})
        monkeypatch.setattr(procident, "process_has_file_open", lambda *a: None)
        spy = SignalSpy(monkeypatch, passthrough=False)
        result = daemonlock.request_reload(db)
        assert (result.outcome, result.reason_code) == ("unverified", "fd-table-unreadable")
        (fd, pid), = pidfd.opened
        assert pid == helper.pid
        assert spy.calls == [("pidfd_send_signal", fd, 0)]
        assert not [c for c in spy.calls if c[0] == "kill"], "os.kill must not be used when a pidfd exists"
        assert all(pidfd.fd_is_closed(f) for f, _ in pidfd.opened)

    def test_with_a_pidfd_eperm_on_signal_zero_is_other_user(self, db, start_helper, monkeypatch):
        order = []
        pidfd = _FakePidfd(monkeypatch, order)
        pidfd.send_error = PermissionError(errno.EPERM, "Operation not permitted")
        helper = start_helper(db)
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: {helper.pid})
        monkeypatch.setattr(procident, "process_has_file_open", lambda *a: None)
        spy = SignalSpy(monkeypatch)            # passes through to the fake, which raises EPERM
        result = daemonlock.request_reload(db)
        assert (result.outcome, result.reason_code) == ("not-permitted", "other-user")
        assert [c[0] for c in spy.calls] == ["pidfd_send_signal"] and spy.signals() == [0]
        assert pidfd.sent and all(sig == 0 for _, sig in pidfd.sent)

    def test_without_a_pidfd_it_is_os_kill_pid_zero(self, rig, db, kill_path, spy):
        rig.answer = None
        daemonlock.request_reload(db)
        assert spy.calls == [("kill", rig.helper.pid, 0)]

    def test_the_real_signal_is_never_sent_when_the_proof_does_not_pass(self, rig, db, kill_path, spy):
        for answer in (None, False):
            rig.answer = answer
            daemonlock.request_reload(db)
        assert _signal_zero_only(spy.calls)
        assert not rig.helper.got_hup()

    # -- what it is asked, and when --------------------------------------------

    def test_it_is_called_with_the_recorded_pid_and_the_pid_files_device_and_inode(self, rig, db, lockfile, kill_path, spy):
        daemonlock.request_reload(db)
        st = os.stat(lockfile)
        assert rig.calls == [(rig.helper.pid, st.st_dev, st.st_ino)]

    def test_it_runs_after_pidfd_open_the_holder_proof_and_the_identity_check(self, db, start_helper, monkeypatch):
        order = []
        _FakePidfd(monkeypatch, order)
        helper = start_helper(db)
        real_identity = procident.identity
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: order.append("holders") or {helper.pid})
        monkeypatch.setattr(procident, "identity", lambda pid, tok: order.append("identity") or real_identity(pid, tok))
        monkeypatch.setattr(procident, "process_has_file_open", lambda *a: order.append("has_open") or True)
        SignalSpy(monkeypatch)
        assert daemonlock.request_reload(db).outcome in ("signalled", "unverified")
        if "has_open" in order:                       # (identity may be unknown on a host without tokens)
            assert order == ["pidfd_open", "holders", "identity", "has_open"]
        else:
            assert order[:3] == ["pidfd_open", "holders", "identity"]

    def test_an_identity_that_is_not_a_match_never_reaches_it(self, rig, db, kill_path, monkeypatch, spy):
        for ident in ("gone", "mismatch", "unknown"):
            monkeypatch.setattr(procident, "identity", lambda pid, tok, _i=ident: _i)
            daemonlock.request_reload(db)
        assert rig.calls == []

    def test_a_failed_holder_proof_never_reaches_it(self, rig, db, kill_path, monkeypatch, spy):
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: set())
        assert daemonlock.request_reload(db).reason_code == "not-lock-holder"
        assert rig.calls == []

    # -- the attack it exists for ------------------------------------------------

    def test_a_forged_file_naming_an_unrelated_live_process_leaves_it_alone(
        self, db, start_helper, plain_process, monkeypatch, kill_path
    ):
        """The lock table (patched here to keep naming the dead locker's recycled
        pid) 'proves' the decoy; the decoy's real token matches too. Only the open-file
        check stops it -- and the decoy must stay alive."""
        decoy = plain_process()
        token = procident.start_token(decoy.pid)
        if not token:
            pytest.skip("no start tokens on this platform")
        holder = start_helper(db, spec=f"{decoy.pid}\n3\nstart={token}\n")
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: {decoy.pid})
        monkeypatch.setattr(procident, "process_has_file_open", lambda pid, dev, ino: False)
        spy = SignalSpy(monkeypatch, passthrough=False)
        result = daemonlock.request_reload(db)
        assert (result.outcome, result.reason_code) == ("supervisor-gone", "file-not-open")
        assert result.pid == decoy.pid
        assert spy.calls == []
        assert decoy.poll() is None, "the unrelated process must not have been signalled"
        assert not holder.got_hup()

    @pytest.mark.skipif(not _real_fd_table(), reason="the real fd table needs Linux /proc")
    def test_the_same_attack_against_the_real_fd_table(self, db, start_helper, plain_process, monkeypatch, kill_path):
        monkeypatch.undo()
        decoy = plain_process()
        token = procident.start_token(decoy.pid)
        holder = start_helper(db, spec=f"{decoy.pid}\n3\nstart={token}\n")
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: {decoy.pid})
        result = daemonlock.request_reload(db)
        assert (result.outcome, result.reason_code) == ("supervisor-gone", "file-not-open")
        assert decoy.poll() is None
        assert not holder.got_hup()

    @pytest.mark.skipif(not _real_fd_table(), reason="the real fd table needs Linux /proc")
    def test_a_genuine_daemon_passes_the_real_fd_table(self, db, start_helper, kill_path, spy):
        helper = start_helper(db)
        assert daemonlock.request_reload(db).outcome == "signalled"
        assert helper.wait_for("HUP")

    # -- zombies keep their verdict ------------------------------------------------

    @pytest.mark.skipif(not HAS_TOKENS, reason="zombie detection needs /proc or sysctl")
    def test_a_zombie_still_gives_zombie_and_the_fd_table_is_not_consulted(
        self, db, start_helper, zombie, monkeypatch, spy
    ):
        proc, token = zombie()
        start_helper(db, spec=f"{proc.pid}\n3\nstart={token}\n")
        monkeypatch.setattr(procident, "flock_write_holders", lambda fd: {proc.pid})
        monkeypatch.setattr(procident, "process_has_file_open", lambda *a: pytest.fail("consulted"))
        spy.calls.clear()
        result = daemonlock.request_reload(db)
        assert (result.outcome, result.reason_code) == ("supervisor-gone", "zombie")
        assert spy.calls == []

    def test_the_new_codes_are_documented(self):
        doc = daemonlock.ReloadRequest.__doc__
        for code in ("file-not-open", "fd-table-unreadable", "other-user"):
            assert f"`{code}`" in doc, code


# ---------------------------------------------------------------------------
# skip_if_applied: "already applied" is only believed of a supervisor not provably dead
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestSkipIfAppliedAndDeadSupervisors:
    SPEC = "{pid}\n3\nstart={start}\napplied=1:3\n"

    @pytest.mark.parametrize("ident", ["gone", "mismatch"])
    def test_an_equal_key_with_a_dead_or_reused_supervisor_is_not_already_applied(
        self, db, start_helper, monkeypatch, ident
    ):
        helper = start_helper(db, spec=self.SPEC)
        monkeypatch.setattr(procident, "identity", lambda pid, tok: ident)
        spy = SignalSpy(monkeypatch, passthrough=False)
        result = daemonlock.request_reload(db, skip_if_applied=(1, 3))
        assert result.outcome != "already-applied"
        assert result.outcome in ("unverified", "supervisor-gone")      # falls through (macOS: unverified/not-linux)
        assert [c for c in spy.calls if c[2] != 0] == []
        assert not helper.got_hup()

    @pytest.mark.skipif(IS_LINUX, reason="off Linux the fall-through ends at the lock-table step")
    @pytest.mark.parametrize("ident", ["gone", "mismatch"])
    def test_off_linux_the_fall_through_is_unverified_not_linux(self, db, start_helper, monkeypatch, ident):
        start_helper(db, spec=self.SPEC)
        monkeypatch.setattr(procident, "identity", lambda pid, tok: ident)
        result = daemonlock.request_reload(db, skip_if_applied=(1, 3))
        assert (result.outcome, result.reason_code) == ("unverified", "not-linux")

    @pytest.mark.parametrize("ident", ["match", "unknown"])
    def test_an_equal_key_with_a_live_or_unreadable_supervisor_is_already_applied_and_nothing_is_sent(
        self, db, start_helper, monkeypatch, ident
    ):
        helper = start_helper(db, spec=self.SPEC)
        monkeypatch.setattr(procident, "identity", lambda pid, tok: ident)
        spy = SignalSpy(monkeypatch, passthrough=False)
        result = daemonlock.request_reload(db, skip_if_applied=(1, 3))
        assert result.outcome == "already-applied"
        assert spy.calls == []
        assert not helper.got_hup()

    def test_a_dead_supervisors_pid_file_is_not_reported_as_applied_for_real(self, db, start_helper, plain_process):
        victim = plain_process()
        token = procident.start_token(victim.pid)
        if not token:
            pytest.skip("no start tokens on this platform")
        dead = victim.pid
        _hard_kill(victim)
        start_helper(db, spec=f"{dead}\n3\nstart={token}\napplied=1:3\n")
        result = daemonlock.request_reload(db, skip_if_applied=(1, 3))
        assert result.outcome != "already-applied"

    def test_a_real_live_supervisor_with_an_equal_key_is_already_applied(self, db, start_helper, spy):
        start_helper(db, spec=self.SPEC)
        assert daemonlock.request_reload(db, skip_if_applied=(1, 3)).outcome == "already-applied"
        assert spy.calls == []

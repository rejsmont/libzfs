"""`BackupDaemon._stop_forkserver` -- the supervisor stops the multiprocessing forkserver itself.

Under Python 3.14's default ``forkserver`` start method the forkserver exits only on EOF of its
"alive" pipe, whose write end the supervisor held until process exit; systemd (``KillMode=mixed``)
therefore SIGKILLed it on every stop. ``_stop_forkserver`` runs in ``_run``'s ``finally`` right
after ``_shutdown_workers()`` and before "Daemon stopped": under ``forkserver`` only, it takes the
forkserver's lock without blocking, forgets pid / alive fd / address (/ authkey), closes the alive
fd, polls ``waitpid(WNOHANG)`` for at most ``FORKSERVER_STOP_TIMEOUT``, warns once if the forkserver
did not exit (a worker that outlived shutdown still holds the pipe), unlinks a non-abstract socket
address only when it reaped the forkserver itself (never on ECHILD or a timeout), and swallows every
exception.

- ``TestStopForkserverMocked``   a fake forkserver object: state, fd, reaping, timeout, lock, address,
                                 every private-attribute failure (nothing real is started or signalled)
- ``TestRunOrdering``            ``run()``: after ``_shutdown_workers``, before "Daemon stopped", also
                                 when the poll loop (or startup) raises
- ``TestRealForkserver``         a REAL forkserver in a driver subprocess (the process-global forkserver
                                 of pytest's own interpreter is never touched): reaped and gone, already
                                 dead, a live holder of the alive fd (one WARNING, bounded), no-op under
                                 fork / spawn
- ``TestRealDaemon``             a real ``zfsbackup.daemon`` under ``forkserver``: after SIGTERM the
                                 supervisor exits 0 and its forkserver child was reaped before it did

Mutation checks (one revert at a time against a scratch copy of the tree, made before the
unlink-only-when-reaped rule; tests in this file that then fail): the ``_stop_forkserver()`` call removed from ``run()``: 5 (three ordering tests, the
once-after-the-flag test, the real daemon); the alive fd not closed: 7; no ``waitpid`` loop (return
after the close): 11; an unbounded ``waitpid(pid, 0)``: 2 (the holder test, by its timeout, and the
mocked timeout one); the lock taken blocking: 1; the ``_tracks_orphans`` guard removed: 5.
"""

import json
import logging
import multiprocessing
import os
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from _hermetic import hermetic_env
from test_zfsbackup_reload import RealDaemon, _killpg_quiet, _pid_alive

import zfsbackup.daemon as daemon_module
from zfsbackup.config import BackupConfig, DatasetConfig
from zfsbackup.daemon import BackupDaemon

REPO = Path(__file__).resolve().parent.parent

HAVE_FORKSERVER = "forkserver" in multiprocessing.get_all_start_methods()
needs_forkserver = pytest.mark.skipif(
    not HAVE_FORKSERVER, reason="needs the forkserver start method on this platform"
)

# ---------------------------------------------------------------------------
# Mocked: a fake forkserver object
# ---------------------------------------------------------------------------

FAKE_PID = 4_000_002        # never a real pid on a sane system; waitpid is always mocked with it


class FakeForkserver:
    """The attributes `_stop_forkserver` touches, on `multiprocessing.forkserver._forkserver`."""

    def __init__(self, pid=FAKE_PID, alive_fd=None, address=None, *, authkey="key", lock=None):
        self._lock = lock if lock is not None else threading.Lock()
        self._forkserver_pid = pid
        self._forkserver_alive_fd = alive_fd
        self._forkserver_address = address
        if authkey is not None:
            self._forkserver_authkey = authkey      # 3.14+; absent on older Pythons


@pytest.fixture(autouse=True)
def _real_stop(real_stop_forkserver):
    """conftest turns `_stop_forkserver` into a no-op for in-process runs; these tests are
    about the real one (against fakes, or in subprocesses)."""


@pytest.fixture
def daemon(mocker):
    saved = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    mocker.patch("zfsbackup.daemon.dispose_all")
    d = BackupDaemon(BackupConfig(datasets=[DatasetConfig(name="pool/data")]), None)
    yield d
    for s, h in saved.items():
        signal.signal(s, h)


_DEFAULT = object()


@pytest.fixture
def armed(monkeypatch):
    """`armed(fake)`: the daemon sees `forkserver` as the start method and `fake` as the
    process-global forkserver. Opened fds are closed afterwards."""
    from multiprocessing import forkserver

    fds = []

    def arm(fake=_DEFAULT, method="forkserver", *, with_fd=True, **kw):
        if fake is _DEFAULT:
            if with_fd:
                r, w = os.pipe()
                fds.extend([r, w])
                kw.setdefault("alive_fd", w)
            fake = FakeForkserver(**kw)
        monkeypatch.setattr(forkserver, "_forkserver", fake)
        monkeypatch.setattr(
            daemon_module.multiprocessing, "get_start_method",
            lambda allow_none=False: method,
        )
        return fake

    yield arm
    for fd in fds:
        try:
            os.close(fd)
        except OSError:
            pass


def fd_open(fd):
    try:
        os.fstat(fd)
    except OSError:
        return False
    return True


def warnings_of(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]


def mock_waitpid(mocker, *answers):
    """`os.waitpid` as the daemon sees it: each call answers the next item (an exception
    instance is raised); the last answer repeats."""
    seq = list(answers)

    def waitpid(pid, flags):
        item = seq.pop(0) if len(seq) > 1 else seq[0]
        if isinstance(item, BaseException):
            raise item
        return item

    return mocker.patch.object(daemon_module.os, "waitpid", side_effect=waitpid)


@pytest.fixture
def short_timeout(monkeypatch):
    monkeypatch.setattr(daemon_module, "FORKSERVER_STOP_TIMEOUT", 0.2)
    return 0.2


def test_the_stop_wait_is_three_seconds():
    assert daemon_module.FORKSERVER_STOP_TIMEOUT == 3.0


@pytest.mark.unit
class TestStopForkserverMocked:
    def test_a_stopped_forkserver_is_forgotten_closed_and_reaped(
        self, daemon, armed, mocker, caplog
    ):
        fake = armed()
        fd = fake._forkserver_alive_fd
        wait = mock_waitpid(mocker, (FAKE_PID, 0))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.daemon"):
            daemon._stop_forkserver()
        wait.assert_called_once_with(FAKE_PID, os.WNOHANG)
        assert fake._forkserver_pid is None
        assert fake._forkserver_alive_fd is None
        assert fake._forkserver_address is None
        assert fake._forkserver_authkey is None
        assert not fd_open(fd)
        assert warnings_of(caplog) == []
        assert fake._lock.acquire(blocking=False), "the lock must be released"
        assert any(f"forkserver (pid={FAKE_PID}) stopped in" in r.getMessage()
                   for r in caplog.records if r.levelno == logging.DEBUG)

    def test_the_fd_is_closed_before_the_first_waitpid(self, daemon, armed, mocker):
        fake = armed()
        fd = fake._forkserver_alive_fd
        seen = []
        mocker.patch.object(
            daemon_module.os, "waitpid",
            side_effect=lambda pid, flags: (seen.append(fd_open(fd)), (pid, 0))[1],
        )
        daemon._stop_forkserver()
        assert seen == [False]

    def test_it_keeps_polling_until_the_forkserver_is_reaped(self, daemon, armed, mocker, caplog):
        armed()
        wait = mock_waitpid(mocker, (0, 0), (0, 0), (FAKE_PID, 0))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.daemon"):
            daemon._stop_forkserver()
        assert wait.call_count == 3
        assert warnings_of(caplog) == []

    def test_a_forkserver_that_does_not_exit_gives_exactly_one_warning(
        self, daemon, armed, mocker, caplog, short_timeout
    ):
        fake = armed()
        wait = mock_waitpid(mocker, (0, 0))
        started = time.monotonic()
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.daemon"):
            daemon._stop_forkserver()                      # returns, does not raise
        elapsed = time.monotonic() - started
        (warning,) = warnings_of(caplog)
        assert f"pid={FAKE_PID}" in warning and "0.2s" in warning
        assert elapsed >= short_timeout
        assert elapsed < short_timeout + 5
        assert wait.call_count > 2                         # it polled, it did not give up at once
        assert fake._forkserver_pid is None                # forgotten even though it did not exit

    @pytest.mark.parametrize("error", [ChildProcessError(10, "no child"), ProcessLookupError()])
    def test_already_reaped_or_not_our_child_is_quiet(self, daemon, armed, mocker, caplog, error):
        fake = armed()
        mock_waitpid(mocker, error)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.daemon"):
            daemon._stop_forkserver()
        if isinstance(error, ChildProcessError):
            assert warnings_of(caplog) == []
            assert fake._forkserver_pid is None
        assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []

    def test_a_pid_that_is_not_our_child_is_quiet_for_real(self, daemon, armed, caplog):
        """No mock: our parent is a process whose child we are not, so the kernel itself says
        ECHILD. (Never signalled, only waited on.)"""
        parent = os.getppid()
        if parent <= 1:
            pytest.skip("no usable parent process")
        fake = armed(pid=parent)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.daemon"):
            daemon._stop_forkserver()
        assert warnings_of(caplog) == []
        assert fake._forkserver_pid is None
        assert _pid_alive(parent)

    @pytest.mark.parametrize("method", ["fork", "spawn"])
    def test_a_noop_under_fork_and_spawn(self, daemon, armed, mocker, method):
        fake = armed(method=method)
        fd = fake._forkserver_alive_fd
        wait = mock_waitpid(mocker, (FAKE_PID, 0))
        daemon._stop_forkserver()
        wait.assert_not_called()
        assert fake._forkserver_pid == FAKE_PID
        assert fake._forkserver_alive_fd == fd and fd_open(fd)
        assert fake._forkserver_authkey == "key"

    def test_an_unreadable_start_method_is_a_noop(self, daemon, armed, mocker, monkeypatch):
        fake = armed()
        monkeypatch.setattr(
            daemon_module.multiprocessing, "get_start_method",
            MagicMock(side_effect=RuntimeError("context broken")),
        )
        wait = mock_waitpid(mocker, (FAKE_PID, 0))
        daemon._stop_forkserver()
        wait.assert_not_called()
        assert fake._forkserver_pid == FAKE_PID

    def test_a_busy_lock_skips_without_blocking(self, daemon, armed, mocker):
        lock = threading.Lock()
        held, release = threading.Event(), threading.Event()

        def holder():
            with lock:
                held.set()
                release.wait(30)

        t = threading.Thread(target=holder, daemon=True)
        t.start()
        assert held.wait(5)
        try:
            fake = armed(lock=lock)
            fd = fake._forkserver_alive_fd
            wait = mock_waitpid(mocker, (FAKE_PID, 0))
            started = time.monotonic()
            daemon._stop_forkserver()
            assert time.monotonic() - started < 2.0
            wait.assert_not_called()
            assert fake._forkserver_pid == FAKE_PID and fd_open(fd)
            assert fake._forkserver_alive_fd == fd
        finally:
            release.set()
            t.join(5)
        assert not t.is_alive()

    @pytest.mark.parametrize("pid", [None, 0, 1, "12", 1.5])
    def test_no_usable_pid_touches_nothing_and_releases_the_lock(
        self, daemon, armed, mocker, pid
    ):
        fake = armed(pid=pid)
        fd = fake._forkserver_alive_fd
        wait = mock_waitpid(mocker, (FAKE_PID, 0))
        daemon._stop_forkserver()
        wait.assert_not_called()
        assert fake._forkserver_pid == pid and fd_open(fd)
        assert fake._lock.acquire(blocking=False)

    def test_no_alive_fd_is_fine(self, daemon, armed, mocker, caplog):
        fake = armed(with_fd=False, alive_fd=None)
        mock_waitpid(mocker, (FAKE_PID, 0))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.daemon"):
            daemon._stop_forkserver()
        assert warnings_of(caplog) == []
        assert fake._forkserver_pid is None

    def test_an_alive_fd_that_is_already_closed_does_not_stop_the_reap(
        self, daemon, armed, mocker, caplog
    ):
        fake = armed()
        os.close(fake._forkserver_alive_fd)                # EBADF when the daemon closes it
        wait = mock_waitpid(mocker, (FAKE_PID, 0))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.daemon"):
            daemon._stop_forkserver()
        wait.assert_called_once()
        assert warnings_of(caplog) == []

    def test_no_authkey_attribute_is_not_invented(self, daemon, armed, mocker):
        fake = armed(authkey=None)
        mock_waitpid(mocker, (FAKE_PID, 0))
        daemon._stop_forkserver()
        assert not hasattr(fake, "_forkserver_authkey")
        assert fake._forkserver_pid is None

    # -- the socket address -------------------------------------------------

    def test_a_filesystem_socket_address_is_unlinked(self, daemon, armed, mocker, tmp_path):
        sock = tmp_path / "fs.sock"
        sock.write_text("")
        fake = armed(address=str(sock))
        mock_waitpid(mocker, (FAKE_PID, 0))
        daemon._stop_forkserver()
        assert not sock.exists()
        assert fake._forkserver_address is None

    def test_the_socket_stays_when_the_forkserver_did_not_exit(
        self, daemon, armed, mocker, tmp_path, short_timeout, caplog
    ):
        """Only a forkserver this call reaped had its socket unlinked."""
        sock = tmp_path / "fs.sock"
        sock.write_text("")
        armed(address=str(sock))
        mock_waitpid(mocker, (0, 0))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.daemon"):
            daemon._stop_forkserver()
        assert sock.exists()
        (warning,) = warnings_of(caplog)
        assert f"pid={FAKE_PID}" in warning and "alive pipe" in warning

    def test_the_socket_stays_when_the_pid_is_not_our_child(
        self, daemon, armed, mocker, tmp_path, caplog
    ):
        """ECHILD: state inherited through a raw `os.fork()` from a process whose forkserver is
        still serving -- its socket is not ours to remove. Quiet, and the state is forgotten."""
        sock = tmp_path / "fs.sock"
        sock.write_text("")
        fake = armed(address=str(sock))
        mock_waitpid(mocker, ChildProcessError(10, "no child"))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.daemon"):
            daemon._stop_forkserver()
        assert sock.exists()
        assert warnings_of(caplog) == []
        assert fake._forkserver_pid is None and fake._forkserver_address is None

    def test_the_socket_stays_for_a_real_pid_that_is_not_our_child(self, daemon, armed, tmp_path):
        parent = os.getppid()
        if parent <= 1:
            pytest.skip("no usable parent process")
        sock = tmp_path / "fs.sock"
        sock.write_text("")
        armed(pid=parent, address=str(sock))
        daemon._stop_forkserver()
        assert sock.exists()

    def test_an_abstract_address_is_never_unlinked(self, daemon, armed, mocker):
        armed(address="\0zfsb-abstract")
        mock_waitpid(mocker, (FAKE_PID, 0))
        unlink = mocker.patch.object(daemon_module.os, "unlink")
        daemon._stop_forkserver()
        unlink.assert_not_called()

    def test_a_missing_socket_file_is_swallowed(self, daemon, armed, mocker, tmp_path, caplog):
        armed(address=str(tmp_path / "gone.sock"))
        mock_waitpid(mocker, (FAKE_PID, 0))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.daemon"):
            daemon._stop_forkserver()
        assert warnings_of(caplog) == []

    def test_no_address_unlinks_nothing(self, daemon, armed, mocker):
        armed(address=None)
        mock_waitpid(mocker, (FAKE_PID, 0))
        unlink = mocker.patch.object(daemon_module.os, "unlink")
        daemon._stop_forkserver()
        unlink.assert_not_called()

    # -- private-attribute failures ---------------------------------------------

    @pytest.mark.parametrize(
        "replacement",
        [None, object(), SimpleNamespace(), SimpleNamespace(_lock=None),
         SimpleNamespace(_lock=threading.Lock())],
        ids=["none", "bare-object", "empty-namespace", "lock-is-none", "no-pid-attribute"],
    )
    def test_a_replaced_or_odd_forkserver_object_is_swallowed(
        self, daemon, armed, mocker, replacement
    ):
        armed(replacement)
        wait = mock_waitpid(mocker, (FAKE_PID, 0))
        daemon._stop_forkserver()                          # must not raise
        wait.assert_not_called()

    def test_the_forkserver_module_missing_its_object_is_swallowed(
        self, daemon, armed, monkeypatch
    ):
        from multiprocessing import forkserver

        armed()
        monkeypatch.delattr(forkserver, "_forkserver")
        daemon._stop_forkserver()

    def test_an_attribute_that_cannot_be_cleared_still_releases_the_lock(
        self, daemon, armed, mocker
    ):
        class Frozen(FakeForkserver):
            @property
            def _forkserver_address(self):
                return self.__dict__.get("addr")

            @_forkserver_address.setter
            def _forkserver_address(self, value):
                if self.__dict__.get("armed"):
                    raise AttributeError("read-only")
                self.__dict__["addr"] = value

        frozen = Frozen(alive_fd=None)
        frozen.__dict__["armed"] = True
        fake = armed(frozen)
        mock_waitpid(mocker, (FAKE_PID, 0))
        daemon._stop_forkserver()                          # must not raise
        assert fake._lock.acquire(blocking=False), "the lock must be released after a failure"

    @pytest.mark.parametrize("error", [PermissionError(1, "no"), OSError(4, "EINTR"), KeyError("x")])
    def test_any_waitpid_failure_is_swallowed(self, daemon, armed, mocker, error):
        fake = armed()
        mock_waitpid(mocker, error)
        daemon._stop_forkserver()
        assert fake._forkserver_pid is None
        assert fake._lock.acquire(blocking=False)


# ---------------------------------------------------------------------------
# Ordering in run()
# ---------------------------------------------------------------------------


@pytest.fixture
def run_daemon(sqlite_config_db, sample_backup_config, mocker):
    d = BackupDaemon(sample_backup_config, sqlite_config_db)
    mocker.patch("zfsbackup.daemon.dispose_all")
    mocker.patch("zfsbackup.backup_manager.zfs.exists", return_value=True)
    mocker.patch("zfsbackup.backup_manager.zfs.list", return_value=[])
    mocker.patch("zfsbackup.backup_manager.zfs.set")
    return d


class LogOrder(logging.Handler):
    """Appends `log:<message>` to the shared order list for the lines the ordering is about."""

    def __init__(self, order):
        super().__init__(logging.INFO)
        self.order = order

    def emit(self, record):
        if record.getMessage() in ("Daemon stopped", "Shutting down workers..."):
            self.order.append(f"log:{record.getMessage()}")


@pytest.fixture
def order(run_daemon, mocker):
    steps = []
    mocker.patch.object(run_daemon, "_shutdown_workers",
                        side_effect=lambda *a, **k: steps.append("shutdown_workers"))
    mocker.patch.object(run_daemon, "_stop_forkserver",
                        side_effect=lambda *a, **k: steps.append("stop_forkserver"))
    handler = LogOrder(steps)
    logger = logging.getLogger("zfsbackup.daemon")
    old_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    yield steps
    logger.removeHandler(handler)
    logger.setLevel(old_level)


@pytest.mark.unit
class TestRunOrdering:
    def test_after_the_workers_and_before_daemon_stopped(self, run_daemon, mocker, order):
        mocker.patch.object(run_daemon, "_start_workers")
        mocker.patch.object(run_daemon, "_supervisor_sleep",
                            side_effect=lambda s: run_daemon._stop_event.set())
        assert run_daemon.run() == 0
        assert order == [
            "log:Shutting down workers...", "shutdown_workers", "stop_forkserver",
            "log:Daemon stopped",
        ]

    def test_a_poll_loop_exception_still_stops_the_forkserver(self, run_daemon, mocker, order):
        mocker.patch.object(run_daemon, "_start_workers")
        mocker.patch.object(run_daemon, "_supervisor_sleep", side_effect=RuntimeError("boom"))
        with pytest.raises(RuntimeError, match="boom"):
            run_daemon.run()
        assert order == ["log:Shutting down workers...", "shutdown_workers", "stop_forkserver"]

    def test_a_startup_exception_still_stops_the_forkserver(self, run_daemon, mocker, order):
        mocker.patch.object(run_daemon, "_start_workers", side_effect=OSError("EAGAIN"))
        with pytest.raises(OSError):
            run_daemon.run()
        assert order[-2:] == ["shutdown_workers", "stop_forkserver"]

    def test_it_runs_once_and_after_the_stop_flag_is_raised(self, run_daemon, mocker):
        seen = []
        mocker.patch.object(run_daemon, "_shutdown_workers")
        mocker.patch.object(run_daemon, "_stop_forkserver",
                            side_effect=lambda: seen.append(run_daemon._stop_event.is_set()))
        mocker.patch.object(run_daemon, "_start_workers")
        mocker.patch.object(run_daemon, "_supervisor_sleep", side_effect=RuntimeError("boom"))
        with pytest.raises(RuntimeError):
            run_daemon.run()
        assert seen == [True]

    def test_the_exit_code_is_not_changed_by_it(self, run_daemon, mocker):
        mocker.patch.object(run_daemon, "_shutdown_workers")
        mocker.patch.object(run_daemon, "_stop_forkserver")
        mocker.patch.object(run_daemon, "_start_workers")
        mocker.patch.object(run_daemon, "_supervisor_sleep",
                            side_effect=lambda s: run_daemon._stop_event.set())
        run_daemon._crash_looped.add("snapshot")
        assert run_daemon.run() == 1


# ---------------------------------------------------------------------------
# Real forkserver, in a driver subprocess
# ---------------------------------------------------------------------------

DRIVER = r'''
import json, logging, multiprocessing, os, signal, sys, time

spec = json.loads(sys.argv[1])
sys.path.insert(0, spec["repo"])


def fd_open(fd):
    try:
        os.fstat(fd)
    except OSError:
        return False
    return True


def main():
    out = {"error": None}
    try:
        run(out)
    except BaseException as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        with open(spec["out"], "w") as f:
            json.dump(out, f)


def run(out):
    import zfsbackup.daemon as dm
    from zfsbackup.config import BackupConfig, DatasetConfig

    method = spec["method"]
    scenario = spec["scenario"]
    multiprocessing.set_start_method(method, force=True)
    records = []

    class H(logging.Handler):
        def emit(self, record):
            records.append([record.levelname, record.getMessage()])

    lg = logging.getLogger("zfsbackup.daemon")
    lg.setLevel(logging.DEBUG)
    lg.addHandler(H())
    dm.FORKSERVER_STOP_TIMEOUT = spec["timeout"]

    d = dm.BackupDaemon(BackupConfig(datasets=[DatasetConfig(name="pool/data")]), None)
    out["imported_before"] = "multiprocessing.forkserver" in sys.modules
    holder = None
    fs = fs_pid = alive_fd = address = None

    if method == "forkserver":
        from multiprocessing import forkserver
        fs = forkserver._forkserver
        seconds = 120 if scenario == "holder" else 0
        holder = multiprocessing.Process(target=time.sleep, args=(seconds,))
        holder.start()
        if scenario != "holder":
            holder.join(20)
            holder = None
        fs_pid, alive_fd, address = fs._forkserver_pid, fs._forkserver_alive_fd, fs._forkserver_address
        out["fs_pid"], out["address"] = fs_pid, address
        out["pid_before"] = fs_pid
        if scenario in ("dead_unreaped", "dead_reaped"):
            os.kill(fs_pid, signal.SIGKILL)
            time.sleep(0.3)
            if scenario == "dead_reaped":
                os.waitpid(fs_pid, 0)

    t0 = time.monotonic()
    d._stop_forkserver()
    out["elapsed"] = time.monotonic() - t0
    out["alive_fd_open"] = fd_open(alive_fd) if alive_fd is not None else None
    out["imported_after"] = "multiprocessing.forkserver" in sys.modules
    if fs is not None:
        out["attrs"] = {
            "pid": fs._forkserver_pid, "alive_fd": fs._forkserver_alive_fd,
            "address": fs._forkserver_address,
            "authkey": getattr(fs, "_forkserver_authkey", None),
        }
    elif out["imported_after"]:
        from multiprocessing import forkserver
        out["attrs"] = {"pid": forkserver._forkserver._forkserver_pid}
    if fs_pid is not None:
        try:
            os.kill(fs_pid, 0)
            out["pid_exists"] = True
        except ProcessLookupError:
            out["pid_exists"] = False
        if isinstance(address, str) and not address.startswith("\0"):
            out["address_exists"] = os.path.exists(address)
    out["records"] = records

    if holder is not None:                                  # clean up the live holder
        holder.terminate()
        holder.join(10)
        if holder.is_alive():
            holder.kill()
            holder.join(10)
        end = time.time() + 15
        out["forkserver_left_after_holder"] = False
        while time.time() < end:
            try:
                done, _ = os.waitpid(fs_pid, os.WNOHANG)
            except ChildProcessError:
                out["forkserver_left_after_holder"] = True
                break
            if done:
                out["forkserver_left_after_holder"] = True
                break
            time.sleep(0.05)
        if not out["forkserver_left_after_holder"]:
            os.kill(fs_pid, signal.SIGKILL)
            os.waitpid(fs_pid, 0)


if __name__ == "__main__":
    main()
'''


@pytest.fixture
def run_driver(tmp_path):
    started = []

    def run(method, scenario="clean", *, timeout=0.5, limit=90):
        driver = tmp_path / "fs_stop_driver.py"
        driver.write_text(DRIVER)
        out_path = tmp_path / "out.json"
        out_path.unlink(missing_ok=True)
        spec = {"method": method, "scenario": scenario, "timeout": timeout,
                "repo": str(REPO), "out": str(out_path)}
        proc = subprocess.Popen(
            [sys.executable, str(driver), json.dumps(spec)],
            cwd=str(tmp_path), env=hermetic_env(), start_new_session=True,
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        started.append(proc)
        try:
            _, stderr = proc.communicate(timeout=limit)
        except subprocess.TimeoutExpired:
            _killpg_quiet(proc.pid)
            _, stderr = proc.communicate()
            pytest.fail(f"driver timed out after {limit}s:\n{stderr[-3000:]}")
        finally:
            _killpg_quiet(proc.pid)             # whatever the driver left (it leads a session)
        assert out_path.exists(), f"driver wrote no result (rc={proc.returncode}):\n{stderr[-3000:]}"
        result = json.loads(out_path.read_text())
        assert result["error"] is None, f"{result['error']}\n{stderr[-3000:]}"
        result["stderr"] = stderr
        return result

    yield run
    for proc in started:
        _killpg_quiet(proc.pid)


def driver_warnings(result):
    return [m for lv, m in result["records"] if lv in ("WARNING", "ERROR", "CRITICAL")]


@pytest.mark.integration
@pytest.mark.slow
@needs_forkserver
class TestRealForkserver:
    def assert_forgotten(self, result):
        attrs = result["attrs"]
        assert attrs["pid"] is None
        assert attrs["alive_fd"] is None
        assert attrs["address"] is None
        assert attrs["authkey"] is None
        assert result["alive_fd_open"] is False

    def test_a_running_forkserver_is_stopped_reaped_and_forgotten(self, run_driver):
        result = run_driver("forkserver", "clean", timeout=10.0)
        assert result["fs_pid"] > 1
        self.assert_forgotten(result)
        assert result["pid_exists"] is False, "the forkserver must be reaped, not a zombie"
        assert driver_warnings(result) == []
        assert any(f"forkserver (pid={result['fs_pid']}) stopped in" in m
                   for lv, m in result["records"] if lv == "DEBUG")
        if "address_exists" in result:
            assert result["address_exists"] is False, "the socket file must be unlinked"

    @pytest.mark.parametrize("scenario", ["dead_unreaped", "dead_reaped"])
    def test_a_forkserver_that_already_died_is_quiet(self, run_driver, scenario):
        result = run_driver("forkserver", scenario, timeout=10.0)
        self.assert_forgotten(result)
        assert result["pid_exists"] is False
        assert driver_warnings(result) == []
        if "address_exists" in result:
            # Unlinked only when this call reaped it; already reaped elsewhere = not ours to remove.
            assert result["address_exists"] is (scenario == "dead_reaped")

    def test_a_live_holder_of_the_alive_fd_gives_one_bounded_warning(self, run_driver):
        result = run_driver("forkserver", "holder", timeout=0.5)
        pid = result["fs_pid"]
        warnings = driver_warnings(result)
        assert len(warnings) == 1, warnings
        assert f"pid={pid}" in warnings[0] and "did not exit within 0.5s" in warnings[0]
        assert 0.5 <= result["elapsed"] < 20
        self.assert_forgotten(result)
        assert result["pid_exists"] is True, "the forkserver is still there: the holder keeps it"
        if "address_exists" in result:
            assert result["address_exists"] is True, "a forkserver still serving keeps its socket"
        # Once the holder is gone the forkserver does exit by itself.
        assert result["forkserver_left_after_holder"] is True

    @pytest.mark.parametrize("method", ["fork", "spawn"])
    def test_a_noop_under_fork_and_spawn_that_starts_no_forkserver(self, run_driver, method):
        if method not in multiprocessing.get_all_start_methods():
            pytest.skip(f"no {method} start method here")
        result = run_driver(method, "clean", timeout=10.0)
        assert driver_warnings(result) == []
        assert result["elapsed"] < 5
        # Importing the forkserver module as a side effect would be harmless but pointless.
        assert result["imported_after"] == result["imported_before"]
        if "attrs" in result:
            assert result["attrs"]["pid"] is None


# ---------------------------------------------------------------------------
# A real daemon
# ---------------------------------------------------------------------------

_WRAPPER = (
    "import multiprocessing, sys\n"
    "multiprocessing.set_start_method('forkserver')\n"
    "from zfsbackup import daemon\n"
    "sys.argv = ['zfsbackup.daemon'] + sys.argv[1:]\n"
    "sys.exit(daemon.main())\n"
)


class ForkserverDaemon(RealDaemon):
    """A real daemon whose multiprocessing start method is `forkserver`."""

    def start(self, *flags):
        self._log_fh = open(self.log_path, "wb")
        self.proc = subprocess.Popen(
            [sys.executable, "-c", _WRAPPER, "-c", str(self.db), *flags],
            stdout=self._log_fh, stderr=subprocess.STDOUT, cwd=REPO, env=self.env,
            start_new_session=True,
        )

    def forkserver_pids(self):
        """Children of the supervisor that are the multiprocessing forkserver."""
        out = subprocess.run(
            ["ps", "-ax", "-o", "pid=,ppid=,command="], capture_output=True, text=True, timeout=30
        ).stdout
        pids = []
        for line in out.splitlines():
            parts = line.split(None, 2)
            if (len(parts) == 3 and parts[1] == str(self.supervisor)
                    and "multiprocessing.forkserver" in parts[2]):
                pids.append(int(parts[0]))
        return pids


@pytest.fixture
def forkserver_daemon(tmp_path, monkeypatch):
    made = []

    def make():
        d = ForkserverDaemon(tmp_path, monkeypatch)
        made.append(d)
        d.import_config()
        return d

    yield make
    for d in made:
        d.stop()


@pytest.mark.integration
@pytest.mark.slow
@needs_forkserver
class TestRealDaemon:
    def test_a_stop_reaps_the_forkserver_before_the_supervisor_exits(self, forkserver_daemon):
        d = forkserver_daemon()
        d.start("--dry-run", "-v")
        d.wait_ready()
        deadline = time.monotonic() + 20
        pids = d.forkserver_pids()
        while not pids and time.monotonic() < deadline:
            time.sleep(0.1)
            pids = d.forkserver_pids()
        assert len(pids) == 1, f"expected the supervisor's forkserver child:\n{d.log()}"
        (fs_pid,) = pids

        os.kill(d.supervisor, signal.SIGTERM)
        assert d.proc.wait(timeout=90) == 0, d.log()
        # Reaped by the supervisor before it exited: not left to exit on its own afterwards (as
        # systemd's cgroup kill would find it), and not a zombie either (nobody else reaped it).
        assert not _pid_alive(fs_pid), "the forkserver outlived the supervisor"

        log = d.log()
        stopped = re.search(rf"forkserver \(pid={fs_pid}\) stopped in", log)
        assert stopped, log
        assert "did not exit within" not in log
        assert "Daemon stopped" in log
        assert stopped.start() < log.index("Daemon stopped")
        assert log.index("Shutting down workers") < stopped.start()

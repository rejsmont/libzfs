"""Phase 2 close-out, daemon side:

- ``TestStartupHup``              the flag-only SIGHUP handler for the window before
                                  `BackupDaemon` exists (and what `_run` does with it);
- ``TestStartupHupInARealDaemon`` a real SIGHUP landing in that window;
- ``TestSupervisorWatch``         the rules by which a worker decides its supervisor is
                                  gone (patched `getppid` / `kill`);
- ``TestWorkerWait``              `_wait`: slices, 0 / negative / NaN, early exit;
- ``TestWorkersExitWithTheSupervisor``  the loops end when the supervisor is gone;
- ``TestApiWorkerFailures``       a bind failure and a dead server thread exit 1;
- ``TestBindApiServer``           the real bind seam (port 0 on loopback);
- ``TestRealKillNine``            a REAL `kill -9` of the supervisor under fork / spawn /
                                  forkserver: the workers leave, `probe()` says "not running",
                                  a new daemon starts.

Everything that starts a process puts it in its own session and kills the whole group by the
pgid recorded at spawn (pgid == pid), independently of reaping the leader.
"""

import errno
import logging
import math
import multiprocessing
import os
import signal
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from _hermetic import hermetic_env
from test_zfsbackup_daemon import _args, _stub_daemon, no_hard_exit  # noqa: F401
from test_zfsbackup_daemonlock import real_daemon_env, spawn_daemon  # noqa: F401  (fixtures)
from zfsbackup import daemon as daemon_module
from zfsbackup import daemonlock, workers
from zfsbackup.config import BackupConfig, DatasetConfig
from zfsbackup.stopflag import STOP_POLL
from zfsbackup.workers import ApiWorker, ExitReason, PruningWorker, SnapshotWorker

REPO = Path(__file__).resolve().parent.parent
SUPERVISOR = 4242       # a pid that is not ours: "the recorded supervisor"


# ---------------------------------------------------------------------------
# _StartupHup
# ---------------------------------------------------------------------------


@pytest.fixture
def hup_sentinel():
    """A recognisable SIGHUP handler to find (or not find) again."""
    def sentinel(signum, frame):
        pass

    previous = signal.signal(signal.SIGHUP, sentinel)
    yield sentinel
    signal.signal(signal.SIGHUP, previous)


class TestStartupHup:
    def test_install_makes_sighup_a_flag_not_a_death(self, hup_sentinel):
        early = daemon_module._StartupHup()
        early.install()
        try:
            assert signal.getsignal(signal.SIGHUP) == early._handler
            assert early.pending is False
            os.kill(os.getpid(), signal.SIGHUP)       # default action would terminate us
            assert early.pending is True
        finally:
            early.restore()

    def test_restore_puts_the_previous_handler_back(self, hup_sentinel):
        early = daemon_module._StartupHup()
        early.install()
        early.restore()
        assert signal.getsignal(signal.SIGHUP) is hup_sentinel

    def test_restore_is_idempotent(self, hup_sentinel):
        early = daemon_module._StartupHup()
        early.install()
        early.restore()
        early.restore()
        assert signal.getsignal(signal.SIGHUP) is hup_sentinel

    def test_restore_without_install_does_nothing(self, hup_sentinel):
        daemon_module._StartupHup().restore()
        assert signal.getsignal(signal.SIGHUP) is hup_sentinel

    def test_restore_after_hand_over_does_nothing(self, hup_sentinel):
        early = daemon_module._StartupHup()
        early.install()
        owner = lambda signum, frame: None  # noqa: E731 - what BackupDaemon.__init__ would install
        signal.signal(signal.SIGHUP, owner)
        early.handed_over = True
        early.restore()
        assert signal.getsignal(signal.SIGHUP) is owner

    def test_install_outside_the_main_thread_is_a_quiet_no_op(self, hup_sentinel):
        box = {}

        def body():
            early = daemon_module._StartupHup()
            early.install()
            early.restore()
            box["installed"] = early._installed

        thread = threading.Thread(target=body)
        thread.start()
        thread.join(10)
        assert box == {"installed": False}
        assert signal.getsignal(signal.SIGHUP) is hup_sentinel


class TestStartupHupInRun:
    def sighup_during_lock(self, monkeypatch):
        """SIGHUP arrives right after the presence lock is taken: the window."""
        real = daemonlock.acquire

        def acquiring(*args, **kwargs):
            lock = real(*args, **kwargs)
            os.kill(os.getpid(), signal.SIGHUP)
            return lock

        monkeypatch.setattr(daemon_module.daemonlock, "acquire", acquiring)

    def test_a_sighup_before_the_daemon_exists_is_carried_into_it(
        self, sqlite_config_db, mocker, monkeypatch, no_hard_exit, hup_sentinel
    ):
        self.sighup_during_lock(monkeypatch)
        factory, stub = _stub_daemon(mocker)
        stub._hup_requested = False
        assert daemon_module._run(_args(sqlite_config_db.path)) == 0
        assert stub._hup_requested is True

    def test_without_a_sighup_nothing_is_carried(
        self, sqlite_config_db, mocker, no_hard_exit, hup_sentinel
    ):
        factory, stub = _stub_daemon(mocker)
        stub._hup_requested = False
        daemon_module._run(_args(sqlite_config_db.path))
        assert stub._hup_requested is False

    def test_a_request_the_daemon_already_holds_is_kept(
        self, sqlite_config_db, mocker, no_hard_exit, hup_sentinel
    ):
        factory, stub = _stub_daemon(mocker)
        stub._hup_requested = True
        daemon_module._run(_args(sqlite_config_db.path))
        assert stub._hup_requested is True

    def test_after_hand_over_the_daemons_own_handler_is_left_alone(
        self, sqlite_config_db, mocker, no_hard_exit, hup_sentinel
    ):
        owner = lambda signum, frame: None  # noqa: E731

        def build(config, resolved, *args, **kwargs):
            signal.signal(signal.SIGHUP, owner)        # what BackupDaemon.__init__ does
            stub = MagicMock()
            stub.run.return_value = 0
            stub.abandoned_workers = []
            stub._hup_requested = False
            return stub

        mocker.patch("zfsbackup.daemon.BackupDaemon", side_effect=build)
        daemon_module._run(_args(sqlite_config_db.path))
        assert signal.getsignal(signal.SIGHUP) is owner

    def test_the_startup_handler_is_in_place_while_the_config_is_read(
        self, sqlite_config_db, mocker, no_hard_exit, hup_sentinel
    ):
        seen = []
        real = daemon_module.load_runtime_config

        def spying(resolved):
            seen.append(signal.getsignal(signal.SIGHUP))
            return real(resolved)

        mocker.patch("zfsbackup.daemon.load_runtime_config", side_effect=spying)
        _stub_daemon(mocker)
        daemon_module._run(_args(sqlite_config_db.path))
        assert len(seen) == 1 and seen[0] is not hup_sentinel
        assert getattr(seen[0], "__name__", "") == "_handler"

    def test_the_handler_is_installed_before_the_lock_is_taken(
        self, sqlite_config_db, mocker, monkeypatch, no_hard_exit, hup_sentinel
    ):
        seen = []
        real = daemonlock.acquire

        def acquiring(*args, **kwargs):
            seen.append(getattr(signal.getsignal(signal.SIGHUP), "__name__", ""))
            return real(*args, **kwargs)

        monkeypatch.setattr(daemon_module.daemonlock, "acquire", acquiring)
        _stub_daemon(mocker)
        daemon_module._run(_args(sqlite_config_db.path))
        assert seen == ["_handler"]

    def test_a_refused_start_restores_the_previous_handler(
        self, sqlite_config_db, mocker, daemon_lock_holder, no_hard_exit, hup_sentinel
    ):
        daemon_lock_holder(sqlite_config_db.path, generation=1)
        _stub_daemon(mocker)
        assert daemon_module._run(_args(sqlite_config_db.path)) == 1
        assert signal.getsignal(signal.SIGHUP) is hup_sentinel

    def test_a_failed_config_load_restores_the_previous_handler(
        self, sqlite_config_db, mocker, no_hard_exit, hup_sentinel
    ):
        mocker.patch("zfsbackup.daemon.load_runtime_config", side_effect=ValueError("bad"))
        _stub_daemon(mocker)
        assert daemon_module._run(_args(sqlite_config_db.path)) == 1
        assert signal.getsignal(signal.SIGHUP) is hup_sentinel

    def test_test_config_never_installs_it(
        self, sqlite_config_db, mocker, no_hard_exit, hup_sentinel
    ):
        seen = []
        real = daemon_module.load_runtime_config

        def spying(resolved):
            seen.append(signal.getsignal(signal.SIGHUP))
            return real(resolved)

        mocker.patch("zfsbackup.daemon.load_runtime_config", side_effect=spying)
        assert daemon_module._run(_args(sqlite_config_db.path, test_config=True)) == 0
        assert seen == [hup_sentinel]
        assert signal.getsignal(signal.SIGHUP) is hup_sentinel

    def test_a_daemon_that_crashed_in_run_still_hands_over(
        self, sqlite_config_db, mocker, no_hard_exit, hup_sentinel
    ):
        _stub_daemon(mocker, run_effect=RuntimeError("boom"))
        assert daemon_module._run(_args(sqlite_config_db.path)) == 1


DRIVER = (
    "import sys, time\n"
    "from zfsbackup import daemon\n"
    "real = daemon.load_runtime_config\n"
    "delay = float(sys.argv[1])\n"
    "def slow(resolved):\n"
    "    time.sleep(delay)   # widens the window between the lock and BackupDaemon\n"
    "    return real(resolved)\n"
    "daemon.load_runtime_config = slow\n"
    "sys.argv = ['zfsbackup.daemon'] + sys.argv[2:]\n"
    "sys.exit(daemon.main())\n"
)


def wait_until(predicate, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def log_text(path):
    return Path(path).read_text(errors="replace") if Path(path).exists() else ""


@pytest.fixture
def spawned(tmp_path):
    """Factory for daemon processes in their own session; the group is killed by
    the pgid recorded at spawn (pgid == pid), whether or not the leader was reaped."""
    started = []

    def start(argv, log_name="daemon.log", env=None):
        log_path = tmp_path / f"{len(started)}-{log_name}"
        handle = open(log_path, "wb")
        process = subprocess.Popen(
            argv, stdout=handle, stderr=subprocess.STDOUT, cwd=str(REPO),
            env=env or hermetic_env(PYTHONUNBUFFERED="1", PYTHONPATH=str(REPO)),
            start_new_session=True,
        )
        started.append((process, handle))
        return process, log_path

    yield start
    for process, handle in started:
        handle.close()
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass


@pytest.mark.integration
@pytest.mark.slow
class TestStartupHupInARealDaemon:
    def test_a_sighup_in_the_startup_window_does_not_kill_the_daemon(
        self, real_daemon_env, spawned
    ):
        resolved = real_daemon_env
        process, log = spawned([sys.executable, "-c", DRIVER, "3", "-c", str(resolved.path), "--dry-run"])
        # the lock is visible (so `zfsbackup-config` would signal us) but BackupDaemon is not built yet
        assert wait_until(lambda: (daemonlock.probe(resolved) or None) is not None, 60), log_text(log)
        assert process.poll() is None
        os.kill(process.pid, signal.SIGHUP)
        time.sleep(0.3)
        assert process.poll() is None, f"the daemon died of the SIGHUP:\n{log_text(log)}"
        assert wait_until(lambda: "worker (pid=" in log_text(log), 60), log_text(log)
        # Carried into the daemon and handled by its first tick -- a real reload now:
        # the store has not changed, so "configuration unchanged" (or "accepted",
        # had a write landed meanwhile) -- and before the first worker is forked.
        text = log_text(log)
        assert "live reload not implemented" not in text
        outcome = [m for m in ("SIGHUP: configuration unchanged", "accepted") if m in text]
        assert outcome, text
        assert text.index(outcome[0]) < text.index("Started "), text
        os.kill(process.pid, signal.SIGTERM)
        assert process.wait(timeout=60) == 0, log_text(log)


# ---------------------------------------------------------------------------
# _SupervisorWatch
# ---------------------------------------------------------------------------


def make_config():
    return BackupConfig(datasets=[DatasetConfig(name="pool/data")])


@contextmanager
def watchdog(stop, seconds=20.0):
    """If the code under test fails to leave its loop (the bug these tests exist
    to catch), raise its stop flag after `seconds` so the test FAILS on its
    assertions instead of hanging the run."""
    timer = threading.Timer(seconds, stop.set)
    timer.daemon = True
    timer.start()
    try:
        yield
    finally:
        timer.cancel()


class Parent:
    """A patchable `os.getppid` / `kill(pid, 0)` pair."""

    def __init__(self, monkeypatch, parent=SUPERVISOR, supervisor_alive=True):
        self.parent = parent
        self.supervisor_alive = supervisor_alive
        monkeypatch.setattr(workers.os, "getppid", lambda: self.parent)
        monkeypatch.setattr(workers, "_pid_exists", lambda pid: self.supervisor_alive)


def armed(monkeypatch, cls=SnapshotWorker, *, parent=SUPERVISOR, forkserver=False, alive=True):
    """A worker armed as if it were a child of `SUPERVISOR`."""
    world = Parent(monkeypatch, parent=parent, supervisor_alive=alive)
    worker = cls(make_config(), multiprocessing.Event())
    worker.supervisor_pid = SUPERVISOR              # recorded by the constructor, in the supervisor
    worker.supervisor_start = None             # no token: identity falls back to _pid_exists
    monkeypatch.setattr(
        workers.multiprocessing, "get_start_method",
        lambda allow_none=False: "forkserver" if forkserver else "spawn",
    )
    worker._arm_supervisor_watch()
    return worker, world


class TestSupervisorWatch:
    def test_the_constructor_records_the_constructing_process(self):
        for cls in (SnapshotWorker, PruningWorker, ApiWorker):
            worker = cls(make_config(), multiprocessing.Event())
            assert worker.supervisor_pid == os.getpid()
            assert worker._parent_at_start is None

    def test_inline_in_the_constructing_process_it_is_never_armed(self, monkeypatch):
        world = Parent(monkeypatch, parent=1, supervisor_alive=False)
        worker = SnapshotWorker(make_config(), multiprocessing.Event())
        worker._arm_supervisor_watch()
        assert worker._parent_at_start is None
        assert worker._supervisor_gone() is False        # whatever getppid / kill say

    def test_an_intact_supervisor_is_not_gone(self, monkeypatch):
        worker, world = armed(monkeypatch)
        assert worker._supervisor_gone() is False

    def test_a_changed_parent_is_gone(self, monkeypatch, caplog):
        worker, world = armed(monkeypatch)
        world.parent = 1                                   # reparented to init
        with caplog.at_level(logging.ERROR):
            assert worker._supervisor_gone() is True
        assert "supervisor 4242 is gone (parent changed from 4242 to 1); exiting" in caplog.text

    def test_a_reparent_to_a_subreaper_counts_too(self, monkeypatch):
        worker, world = armed(monkeypatch)
        world.parent = 777
        assert worker._supervisor_gone() is True

    def test_dead_before_the_first_instruction(self, monkeypatch, caplog):
        # the supervisor died, and the worker was reparented, before it ran:
        # getppid() never CHANGES, so the start parent is compared with the supervisor
        worker, world = armed(monkeypatch, parent=1)
        with caplog.at_level(logging.ERROR):
            assert worker._supervisor_gone() is True
        assert "parent at start was 1, not the supervisor" in caplog.text

    def test_under_forkserver_a_different_start_parent_is_normal(self, monkeypatch):
        worker, world = armed(monkeypatch, parent=777, forkserver=True)
        assert worker._direct_child is False
        assert worker._supervisor_gone() is False         # the forkserver is the parent, not the supervisor

    def test_under_forkserver_a_vanished_supervisor_is_noticed_by_kill_zero(self, monkeypatch, caplog):
        worker, world = armed(monkeypatch, parent=777, forkserver=True)
        world.supervisor_alive = False
        with caplog.at_level(logging.ERROR):
            assert worker._supervisor_gone() is True
        assert "no such process" in caplog.text

    def test_under_forkserver_the_forkserver_exiting_trips_the_reparent_rule(self, monkeypatch):
        worker, world = armed(monkeypatch, parent=777, forkserver=True)
        world.parent = 1
        assert worker._supervisor_gone() is True

    def test_under_fork_and_spawn_the_parent_is_compared_with_the_supervisor(self, monkeypatch):
        worker, world = armed(monkeypatch, parent=SUPERVISOR, forkserver=False)
        assert worker._direct_child is True
        assert worker._supervisor_gone() is False

    def test_a_vanished_supervisor_with_an_unchanged_parent_is_gone(self, monkeypatch, caplog):
        worker, world = armed(monkeypatch)
        world.supervisor_alive = False
        with caplog.at_level(logging.ERROR):
            assert worker._supervisor_gone() is True
        assert "no such process" in caplog.text

    def test_it_is_logged_once(self, monkeypatch, caplog):
        worker, world = armed(monkeypatch)
        world.parent = 1
        with caplog.at_level(logging.ERROR):
            for _ in range(5):
                assert worker._supervisor_gone() is True
        assert caplog.text.count("is gone") == 1

    def test_it_is_logged_at_error(self, monkeypatch, caplog):
        worker, world = armed(monkeypatch)
        world.parent = 1
        with caplog.at_level(logging.DEBUG):
            worker._supervisor_gone()
        records = [r for r in caplog.records if "is gone" in r.getMessage()]
        assert len(records) == 1 and records[0].levelno == logging.ERROR

    def test_should_stop_is_the_flag_or_the_supervisor(self, monkeypatch):
        worker, world = armed(monkeypatch)
        assert worker._should_stop() is False
        world.parent = 1
        assert worker._should_stop() is True
        worker2, world2 = armed(monkeypatch)
        world2.parent = SUPERVISOR
        worker2.stop_event.set()
        assert worker2._should_stop() is True

    def test_a_raised_flag_does_not_consult_the_supervisor(self, monkeypatch):
        worker, world = armed(monkeypatch)
        worker.stop_event.set()
        monkeypatch.setattr(workers.os, "getppid", lambda: pytest.fail("not needed"))
        assert worker._should_stop() is True

    @pytest.mark.parametrize(
        "error, expected",
        [
            (ProcessLookupError(errno.ESRCH, "x"), False),
            (PermissionError(errno.EPERM, "x"), True),      # exists, merely not ours
            (OSError(errno.EIO, "x"), True),                # cannot tell: never declare it gone
        ],
    )
    def test_pid_exists(self, monkeypatch, error, expected):
        def kill(pid, sig):
            raise error

        monkeypatch.setattr(os, "kill", kill)
        assert workers._pid_exists(SUPERVISOR) is expected

    def test_pid_exists_is_true_when_kill_succeeds_and_sends_signal_zero(self, monkeypatch):
        sent = []
        monkeypatch.setattr(os, "kill", lambda pid, sig: sent.append((pid, sig)))
        assert workers._pid_exists(SUPERVISOR) is True
        assert sent == [(SUPERVISOR, 0)]

    def test_api_worker_has_the_same_watch(self, monkeypatch):
        worker, world = armed(monkeypatch, cls=ApiWorker)
        assert worker._supervisor_gone() is False
        world.parent = 1
        assert worker._supervisor_gone() is True


# ---------------------------------------------------------------------------
# _wait
# ---------------------------------------------------------------------------


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def monotonic(self):
        return self.now


@pytest.fixture
def sliced(monkeypatch):
    """A worker with a MagicMock stop flag whose `wait(timeout=t)` advances a fake clock by t."""
    clock = FakeClock()
    monkeypatch.setattr(workers.time, "monotonic", clock.monotonic)
    stop = MagicMock()
    stop.is_set.return_value = False
    def wait(timeout):
        if stop.wait.call_count > 200:
            raise AssertionError("runaway _wait loop")
        clock.now += timeout

    stop.wait.side_effect = wait
    worker = SnapshotWorker(make_config(), stop)
    return worker, stop, clock


class TestWorkerWait:
    def test_it_waits_in_stop_poll_slices(self, sliced):
        worker, stop, clock = sliced
        assert worker._wait(1.75) is None
        assert [c.kwargs["timeout"] for c in stop.wait.call_args_list] == [
            STOP_POLL, STOP_POLL, STOP_POLL, pytest.approx(0.25),
        ]

    def test_a_short_timeout_is_one_short_slice(self, sliced):
        worker, stop, clock = sliced
        assert worker._wait(0.1) is None
        assert [c.kwargs["timeout"] for c in stop.wait.call_args_list] == [pytest.approx(0.1)]

    @pytest.mark.parametrize("timeout", [0, 0.0, -1, -5.5, float("nan")])
    def test_zero_negative_and_nan_return_the_state_without_sleeping(self, sliced, timeout):
        worker, stop, clock = sliced
        assert worker._wait(timeout) is None
        stop.wait.assert_not_called()
        stop.is_set.return_value = True
        assert worker._wait(timeout) is ExitReason.STOP
        stop.wait.assert_not_called()

    def test_infinity_is_not_special_cased_away(self, sliced):
        worker, stop, clock = sliced
        stop.is_set.side_effect = [False, False, True]      # raised on the third check
        assert worker._wait(math.inf) is ExitReason.STOP
        assert stop.wait.call_count == 2

    def test_a_raised_flag_ends_it_at_the_next_slice(self, sliced):
        worker, stop, clock = sliced
        stop.is_set.side_effect = [False, False, True]
        assert worker._wait(10) is ExitReason.STOP
        assert stop.wait.call_count == 2                    # not 20 slices

    def test_a_raised_flag_returns_immediately_without_waiting(self, sliced):
        worker, stop, clock = sliced
        stop.is_set.return_value = True
        assert worker._wait(10) is ExitReason.STOP
        stop.wait.assert_not_called()

    def test_a_dead_supervisor_is_noticed_within_one_slice(self, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(workers.time, "monotonic", clock.monotonic)
        stop = MagicMock()
        stop.is_set.return_value = False
        world = Parent(monkeypatch)
        slices = []

        def wait(timeout):
            slices.append(timeout)
            assert len(slices) < 50, "runaway _wait loop"
            clock.now += timeout
            if len(slices) == 3:
                world.parent = 1                          # the supervisor dies during slice 3

        stop.wait.side_effect = wait
        worker = SnapshotWorker(make_config(), stop)
        worker.supervisor_pid = SUPERVISOR
        worker.supervisor_start = None             # no token: identity falls back to _pid_exists
        monkeypatch.setattr(workers.multiprocessing, "get_start_method", lambda allow_none=False: "spawn")
        worker._arm_supervisor_watch()
        assert worker._wait(3600) is ExitReason.SUPERVISOR_GONE                 # an hour-long interval
        assert len(slices) == 3 and all(t == STOP_POLL for t in slices)

    def test_the_real_flag_is_polled_in_slices_end_to_end(self):
        stop = multiprocessing.Event()
        worker = SnapshotWorker(make_config(), stop)
        timer = threading.Timer(0.3, stop.set)
        timer.start()
        started = time.monotonic()
        try:
            assert worker._wait(10) is ExitReason.STOP
        finally:
            timer.cancel()
        assert time.monotonic() - started < 5


# ---------------------------------------------------------------------------
# the loops end when the supervisor is gone
# ---------------------------------------------------------------------------


class TestWorkersExitWithTheSupervisor:
    def test_the_base_worker_loop_ends_instead_of_sleeping_the_interval(self, monkeypatch, mocker, caplog):
        mocker.patch("zfsbackup.backup_manager.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.backup_manager.zfs.list", return_value=[])
        world = Parent(monkeypatch)
        stop = multiprocessing.Event()
        worker = SnapshotWorker(make_config(), stop)
        worker.supervisor_pid = SUPERVISOR
        worker.supervisor_start = None             # no token: identity falls back to _pid_exists
        mocker.patch.object(worker, "_get_interval", return_value=30)   # would sleep 30 s if ignored
        monkeypatch.setattr(workers.multiprocessing, "get_start_method", lambda allow_none=False: "spawn")

        def process(manager, dsi):
            world.parent = 1                              # the supervisor dies mid-cycle

        mocker.patch.object(worker, "_process_dataset", side_effect=process)
        started = time.monotonic()
        with caplog.at_level(logging.INFO), watchdog(stop):
            worker.run()
        assert time.monotonic() - started < 10
        assert "supervisor 4242 is gone" in caplog.text
        assert "snapshot-worker stopped" in caplog.text

    def test_the_dataset_in_flight_is_finished_before_leaving(self, monkeypatch, mocker):
        mocker.patch("zfsbackup.backup_manager.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.backup_manager.zfs.list", return_value=[])
        world = Parent(monkeypatch)
        config = BackupConfig(datasets=[DatasetConfig(name="pool/a"), DatasetConfig(name="pool/b")])
        worker = SnapshotWorker(config, multiprocessing.Event())
        worker.supervisor_pid = SUPERVISOR
        worker.supervisor_start = None             # no token: identity falls back to _pid_exists
        monkeypatch.setattr(workers.multiprocessing, "get_start_method", lambda allow_none=False: "spawn")
        done = []

        def process(manager, dsi):
            done.append(dsi.name)
            world.parent = 1

        mocker.patch.object(worker, "_process_dataset", side_effect=process)
        mocker.patch.object(worker, "_get_interval", return_value=30)
        with watchdog(worker.stop_event):
            worker.run()
        assert done == ["pool/a"]                        # a started dataset completes; the next is not begun

    def test_a_gone_supervisor_before_the_first_cycle_runs_no_cycle(self, monkeypatch, mocker):
        mocker.patch("zfsbackup.backup_manager.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.backup_manager.zfs.list", return_value=[])
        Parent(monkeypatch, parent=1)                     # reparented before the first instruction
        worker = SnapshotWorker(make_config(), multiprocessing.Event())
        worker.supervisor_pid = SUPERVISOR
        worker.supervisor_start = None             # no token: identity falls back to _pid_exists
        monkeypatch.setattr(workers.multiprocessing, "get_start_method", lambda allow_none=False: "spawn")
        process = mocker.patch.object(worker, "_process_dataset")
        with watchdog(worker.stop_event, 5):
            worker.run()
        process.assert_not_called()

    def test_inline_runs_are_unaffected(self, mocker):
        mocker.patch("zfsbackup.backup_manager.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.backup_manager.zfs.list", return_value=[])
        stop = multiprocessing.Event()
        worker = SnapshotWorker(make_config(), stop)
        calls = []

        def process(manager, dsi):
            calls.append(1)
            stop.set()

        mocker.patch.object(worker, "_process_dataset", side_effect=process)
        worker.run()
        assert calls == [1]


# ---------------------------------------------------------------------------
# ApiWorker failures
# ---------------------------------------------------------------------------


class TestApiWorkerFailures:
    @pytest.fixture
    def api(self, mocker):
        mocker.patch("zfsbackup.api.create_app", return_value=MagicMock())
        stop = multiprocessing.Event()
        return ApiWorker(make_config(), stop), stop

    def test_a_bind_failure_exits_one_and_never_says_listening(self, api, mocker, caplog):
        worker, stop = api
        mocker.patch("zfsbackup.workers._bind_api_server", side_effect=OSError(errno.EADDRINUSE, "Address already in use"))
        thread = mocker.patch("zfsbackup.workers.threading.Thread")
        with caplog.at_level(logging.INFO), pytest.raises(SystemExit) as info:
            worker.run()
        assert info.value.code == 1
        assert "API worker cannot listen on 127.0.0.1:8080" in caplog.text
        assert "Address already in use" in caplog.text
        assert "API listening" not in caplog.text
        thread.assert_not_called()                          # no thread for a server that does not exist

    @pytest.mark.parametrize("error", [PermissionError(errno.EACCES, "denied"), OSError(errno.EADDRNOTAVAIL, "bad host")])
    def test_any_oserror_is_a_bind_failure(self, api, mocker, error):
        worker, stop = api
        mocker.patch("zfsbackup.workers._bind_api_server", side_effect=error)
        with pytest.raises(SystemExit) as info:
            worker.run()
        assert info.value.code == 1

    def test_the_server_is_bound_with_the_configured_address(self, api, mocker):
        worker, stop = api
        server = MagicMock()
        bind = mocker.patch("zfsbackup.workers._bind_api_server", return_value=server)
        mocker.patch("zfsbackup.workers.threading.Thread", return_value=MagicMock())
        stop.set()
        worker.run()
        app, host, port = bind.call_args.args
        assert (host, port) == (worker.config.api_host, worker.config.api_port)

    def test_a_dead_server_thread_exits_one(self, api, mocker, caplog):
        worker, stop = api
        server = MagicMock()
        server.serve_forever.side_effect = RuntimeError("serve failed")
        mocker.patch("zfsbackup.workers._bind_api_server", return_value=server)
        started = time.monotonic()
        with caplog.at_level(logging.INFO), pytest.raises(SystemExit) as info:
            worker.run()
        assert info.value.code == 1
        assert time.monotonic() - started < 10            # noticed within a slice or two
        assert "API server thread died" in caplog.text
        assert "API server thread exited unexpectedly" in caplog.text

    def test_the_bind_happens_before_the_thread_and_the_log_line(self, api, mocker, caplog):
        worker, stop = api
        order = []
        mocker.patch("zfsbackup.workers._bind_api_server", side_effect=lambda *a: (order.append("bind"), MagicMock())[1])
        thread = MagicMock()
        thread.start.side_effect = lambda: order.append("thread")
        mocker.patch("zfsbackup.workers.threading.Thread", return_value=thread)
        stop.set()
        with caplog.at_level(logging.INFO):
            worker.run()
        assert order == ["bind", "thread"]
        assert caplog.text.index("API listening") > 0

    def test_a_stop_ends_a_healthy_worker_cleanly(self, api, mocker, caplog):
        worker, stop = api
        server = MagicMock()
        server.serve_forever.side_effect = lambda: stop.wait(30)       # "serves" until stopped
        mocker.patch("zfsbackup.workers._bind_api_server", return_value=server)
        timer = threading.Timer(0.5, stop.set)
        timer.start()
        try:
            with caplog.at_level(logging.INFO):
                worker.run()                                  # returns, no SystemExit
        finally:
            timer.cancel()
        assert "API listening" in caplog.text and "API worker stopped" in caplog.text

    def test_a_gone_supervisor_ends_it(self, monkeypatch, mocker, caplog):
        mocker.patch("zfsbackup.api.create_app", return_value=MagicMock())
        world = Parent(monkeypatch)
        worker = ApiWorker(make_config(), multiprocessing.Event())
        worker.supervisor_pid = SUPERVISOR
        worker.supervisor_start = None             # no token: identity falls back to _pid_exists
        monkeypatch.setattr(workers.multiprocessing, "get_start_method", lambda allow_none=False: "spawn")
        server = MagicMock()
        server.serve_forever.side_effect = lambda: time.sleep(30)
        mocker.patch("zfsbackup.workers._bind_api_server", return_value=server)
        timer = threading.Timer(0.3, lambda: setattr(world, "parent", 1))
        timer.start()
        try:
            with caplog.at_level(logging.INFO), watchdog(worker.stop_event):
                worker.run()
        finally:
            timer.cancel()
        assert "supervisor 4242 is gone" in caplog.text and "API worker stopped" in caplog.text


class TestBindApiServer:
    def test_it_binds_in_the_callers_thread(self):
        server = workers._bind_api_server(MagicMock(), "127.0.0.1", 0)
        try:
            assert server.server_port > 0                 # already listening: nothing deferred to a thread
        finally:
            server.server_close()

    def test_an_occupied_port_fails_to_bind(self, capsys):
        first = workers._bind_api_server(MagicMock(), "127.0.0.1", 0)
        try:
            # werkzeug turns EADDRINUSE into print + sys.exit(1) rather than an OSError
            with pytest.raises((OSError, SystemExit)) as info:
                workers._bind_api_server(MagicMock(), "127.0.0.1", first.server_port)
            if info.type is SystemExit:
                assert info.value.code == 1
        finally:
            first.server_close()

    def test_an_unusable_address_fails_to_bind(self):
        with pytest.raises((OSError, SystemExit)) as info:
            workers._bind_api_server(MagicMock(), "203.0.113.77", 0)    # TEST-NET-3: not ours
        if info.type is SystemExit:
            assert info.value.code == 1

# ---------------------------------------------------------------------------
# A REAL kill -9 of the supervisor
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.slow
class TestRealKillNine:
    GRACE = 30.0

    def start_daemon(self, spawned, resolved, method=None):
        script = (
            "import multiprocessing, sys\n"
            "if sys.argv[1] != 'default':\n"
            "    multiprocessing.set_start_method(sys.argv[1])\n"
            "from zfsbackup import daemon\n"
            "sys.argv = ['zfsbackup.daemon'] + sys.argv[2:]\n"
            "sys.exit(daemon.main())\n"
        )
        return spawned([
            sys.executable, "-c", script, method or "default", "-c", str(resolved.path), "--dry-run",
        ])

    def worker_pids(self, log):
        import re

        return sorted({int(m) for m in re.findall(r"worker \(pid=(\d+)\)", log_text(log))})

    def alive(self, pid):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        try:        # a zombie answers kill(0); it is not running
            state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout
            return bool(state.strip()) and not state.strip().startswith("Z")
        except OSError:
            return True

    @pytest.mark.parametrize("method", ["fork", "spawn", "forkserver"])
    def test_the_workers_leave_with_the_supervisor_and_a_new_daemon_starts(
        self, real_daemon_env, spawned, method
    ):
        if method not in multiprocessing.get_all_start_methods():
            pytest.skip(f"no {method} start method here")
        resolved = real_daemon_env
        first, log = self.start_daemon(spawned, resolved, method)
        assert wait_until(
            lambda: len(self.worker_pids(log)) >= 3
            and ("api-worker started" in log_text(log) or "API listening" in log_text(log)),
            90,
        ), log_text(log)
        assert wait_until(lambda: (daemonlock.probe(resolved) or None) is not None, 30)
        pids = self.worker_pids(log)
        assert len(pids) >= 3 and all(self.alive(p) for p in pids)

        first.send_signal(signal.SIGKILL)                  # nobody is left to raise the stop flag
        first.wait(timeout=10)

        assert wait_until(lambda: not any(self.alive(p) for p in pids), self.GRACE), (
            f"workers still running: {[p for p in pids if self.alive(p)]}\n{log_text(log)}"
        )
        text = log_text(log)
        assert "is gone" in text and "exiting" in text    # logged by the workers
        # the lock is free (under fork the orphans held it until they left)
        assert wait_until(lambda: daemonlock.probe(resolved) is None, 15)

        second, second_log = self.start_daemon(spawned, resolved, method)
        assert wait_until(
            lambda: (lambda info: info is not None and info.pid == second.pid)(daemonlock.probe(resolved)), 90
        ), log_text(second_log)
        assert wait_until(lambda: "worker (pid=" in log_text(second_log), 60)
        second.send_signal(signal.SIGTERM)
        assert second.wait(timeout=60) == 0, log_text(second_log)
        assert daemonlock.probe(resolved) is None


class TestApiWorkerWithARealOccupiedPort:
    """The worker's observable contract when it cannot listen: it exits with
    status 1 (so the supervisor's backoff applies) and never claims to listen."""

    def test_it_exits_one_and_never_says_listening(self, mocker, caplog, capsys):
        mocker.patch("zfsbackup.api.create_app", return_value=MagicMock())
        blocker = workers._bind_api_server(MagicMock(), "127.0.0.1", 0)
        try:
            config = BackupConfig(
                datasets=[DatasetConfig(name="pool/data")], api_host="127.0.0.1",
                api_port=blocker.server_port,
            )
            worker = ApiWorker(config, multiprocessing.Event())
            with caplog.at_level(logging.INFO), pytest.raises(SystemExit) as info:
                worker.run()
            assert info.value.code == 1
            assert "API listening" not in caplog.text
        finally:
            blocker.server_close()


# ---------------------------------------------------------------------------
# SIGHUP before the daemon can catch it (final review, item A)
# ---------------------------------------------------------------------------


class TestEarlySighupDisposition:
    @pytest.fixture
    def keep_sighup(self):
        previous = signal.getsignal(signal.SIGHUP)
        yield
        signal.signal(signal.SIGHUP, previous)

    def test_importing_the_daemon_module_leaves_sighup_alone(self):
        """The `SIG_IGN` is for `__main__` only: an importer (the test suite,
        the CLI) keeps its disposition -- and an ignore would be inherited by
        every process it execs."""
        script = (
            "import signal\n"
            "signal.signal(signal.SIGHUP, signal.SIG_DFL)\n"
            "import zfsbackup.daemon\n"
            "print('DFL' if signal.getsignal(signal.SIGHUP) == signal.SIG_DFL else 'CHANGED')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=120,
            cwd=str(REPO), env=hermetic_env(PYTHONPATH=str(REPO)),
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "DFL"

    def test_importing_it_does_not_replace_a_custom_handler_either(self):
        script = (
            "import signal\n"
            "def mine(s, f): pass\n"
            "signal.signal(signal.SIGHUP, mine)\n"
            "import zfsbackup.daemon\n"
            "print('MINE' if signal.getsignal(signal.SIGHUP) is mine else 'CHANGED')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=120,
            cwd=str(REPO), env=hermetic_env(PYTHONPATH=str(REPO)),
        )
        assert result.stdout.strip() == "MINE", result.stderr

    @pytest.mark.parametrize("previous", [signal.SIG_DFL, signal.SIG_IGN])
    def test_a_refusal_restores_the_previous_disposition(
        self, sqlite_config_db, mocker, daemon_lock_holder, keep_sighup, no_hard_exit, previous
    ):
        daemon_lock_holder(sqlite_config_db.path, generation=3)
        _stub_daemon(mocker)
        signal.signal(signal.SIGHUP, previous)
        assert daemon_module._run(_args(sqlite_config_db.path)) == 1
        assert signal.getsignal(signal.SIGHUP) == previous

    @pytest.mark.parametrize("previous", [signal.SIG_DFL, signal.SIG_IGN])
    def test_test_config_leaves_the_disposition_alone(self, sqlite_config_db, keep_sighup, previous):
        signal.signal(signal.SIGHUP, previous)
        assert daemon_module._run(_args(sqlite_config_db.path, test_config=True)) == 0
        assert signal.getsignal(signal.SIGHUP) == previous

    @pytest.mark.parametrize("outcome", ["returns", "raises"])
    def test_process_main_leaves_sighup_ignored_whatever_main_does(self, keep_sighup, mocker, outcome):
        signal.signal(signal.SIGHUP, signal.SIG_DFL)
        if outcome == "returns":
            mocker.patch.object(daemon_module, "main", return_value=7)
            assert daemon_module._process_main() == 7
        else:
            mocker.patch.object(daemon_module, "main", side_effect=RuntimeError("boom"))
            with pytest.raises(RuntimeError):
                daemon_module._process_main()
        assert signal.getsignal(signal.SIGHUP) == signal.SIG_IGN

    def test_process_main_is_what_the_module_runs(self):
        source = Path(daemon_module.__file__).read_text()
        assert "sys.exit(_process_main())" in source
        assert source.index("signal.signal(signal.SIGHUP, signal.SIG_IGN)") < source.index("import argparse")


@pytest.mark.integration
@pytest.mark.slow
class TestEarlySighupInARealDaemon:
    """`zfsbackup-config`'s auto-reload SIGHUP (or `systemctl reload` straight
    after `start`) lands while the interpreter is still importing SQLAlchemy and
    friends. A few tens of milliseconds are accepted residue (interpreter start-up
    before the first line, which Python cannot close), so the earliest delay
    tested is 50 ms -- never 0."""

    @pytest.mark.parametrize("delay", [0.05, 0.1, 0.2])
    def test_the_daemon_survives_and_a_later_sigterm_exits_zero(self, real_daemon_env, spawn_daemon, delay):
        proc, log = spawn_daemon()
        time.sleep(delay)
        os.kill(proc.pid, signal.SIGHUP)
        # alive well past the import window, and still alive when the workers start
        assert wait_until(lambda: "Started snapshot worker" in log_text(log), 90), log_text(log)
        assert proc.poll() is None, f"the daemon died of an early SIGHUP:\n{log_text(log)}"
        os.kill(proc.pid, signal.SIGTERM)
        assert proc.wait(timeout=60) == 0, log_text(log)

    def test_a_hup_burst_during_startup_is_survived(self, real_daemon_env, spawn_daemon):
        proc, log = spawn_daemon()
        for _ in range(5):
            time.sleep(0.08)
            os.kill(proc.pid, signal.SIGHUP)
        assert wait_until(lambda: "Started snapshot worker" in log_text(log), 90), log_text(log)
        assert proc.poll() is None
        os.kill(proc.pid, signal.SIGTERM)
        assert proc.wait(timeout=60) == 0, log_text(log)

    def test_the_hup_is_handled_as_a_reload_not_ignored_forever(self, real_daemon_env, spawn_daemon):
        proc, log = spawn_daemon()
        assert wait_until(lambda: "Started snapshot worker" in log_text(log), 90), log_text(log)
        os.kill(proc.pid, signal.SIGHUP)
        assert wait_until(lambda: "SIGHUP: configuration unchanged" in log_text(log), 60), log_text(log)
        assert proc.poll() is None
        os.kill(proc.pid, signal.SIGTERM)
        assert proc.wait(timeout=60) == 0

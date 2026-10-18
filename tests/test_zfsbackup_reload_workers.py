"""Phase 3, item 18.2: workers leave their loop for a reload, and hold the
workers lock.

`workers.ExitReason` (STOP > SUPERVISOR_GONE > RELOAD), `_exit_reason`, `_wait`,
the keyword-only constructor trio (`reload_epoch`, `born_epoch`, `workers_lock`),
orphan detection by process identity, and `<db>.workers.lock`.

* Unit tests drive a worker inline with patched `getppid` / `procident.identity`.
* `TestRealWorkerProcesses` starts REAL worker processes under fork, spawn and
  forkserver (`tests/_reload_workers.py`); each blocks inside `_process_dataset`
  on a file gate, so "a reload never interrupts the dataset in hand" is observed,
  not assumed. Every process is killed at teardown.
"""

import fcntl
import inspect
import json
import logging
import math
import multiprocessing
import os
import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import _reload_workers
from _reload_workers import FdProbeWorker, GatedSnapshotWorker, descriptors_on
from test_zfsbackup_daemon_closeout import (  # noqa: F401
    SUPERVISOR,
    Parent,
    armed,
    make_config,
    watchdog,
)
from zfsbackup import daemonlock, procident, workers
from zfsbackup.backup_manager import DatasetManager
from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Destination,
    RemoteDatasetConfig,
)
from zfsbackup.stopflag import STOP_POLL, ReloadEpoch, StopFlag
from zfsbackup.workers import (
    WORKER_EXIT_RELOAD,
    ApiWorker,
    BaseWorker,
    ExitReason,
    PruningWorker,
    RemoteBackupWorker,
    SnapshotWorker,
)

START_METHODS = [
    m for m in ("fork", "spawn", "forkserver") if m in multiprocessing.get_all_start_methods()
]

ALL_FIVE = [SnapshotWorker, PruningWorker, RemoteBackupWorker, ApiWorker, "base"]


class _Concrete(BaseWorker):
    """Minimal BaseWorker subclass (the base is abstract)."""

    def __init__(self, *args, **kwargs):
        super().__init__("concrete-worker", *args, **kwargs)

    def _get_interval(self, config):
        return 1.0

    def _process_dataset(self, manager, dsi):
        pass


def _build(cls, *args, **kwargs):
    if cls == "base":
        return _Concrete(*args, **kwargs)
    return cls(*args, **kwargs)


def wait_until(predicate, timeout=20.0, step=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(step)
    return predicate()


# ---------------------------------------------------------------------------
# Constructors
# ---------------------------------------------------------------------------


class TestConstructorTrio:
    @pytest.mark.parametrize("cls", ALL_FIVE)
    def test_the_old_positional_constructors_still_work(self, cls):
        stop = StopFlag()
        worker = _build(cls, make_config(), stop, True)
        assert worker.stop_event is stop and worker.verbose is True
        assert worker.reload_epoch is None
        assert worker.born_epoch == 0
        assert worker.workers_lock is None

    @pytest.mark.parametrize("cls", ALL_FIVE)
    def test_the_trio_is_stored_on_every_class(self, cls, tmp_path):
        epoch = ReloadEpoch()
        epoch.set(4)
        lock = tmp_path / "x.workers.lock"
        worker = _build(
            cls, make_config(), StopFlag(), False,
            reload_epoch=epoch, born_epoch=4, workers_lock=lock,
        )
        assert worker.reload_epoch is epoch
        assert worker.born_epoch == 4
        assert worker.workers_lock == lock

    @pytest.mark.parametrize("cls", ALL_FIVE)
    def test_the_trio_is_keyword_only(self, cls):
        with pytest.raises(TypeError):
            _build(cls, make_config(), StopFlag(), False, ReloadEpoch())

    @pytest.mark.parametrize("cls", ALL_FIVE)
    def test_the_constructor_records_the_supervisors_start_token(self, cls):
        worker = _build(cls, make_config(), StopFlag())
        assert worker.supervisor_pid == os.getpid()
        assert worker.supervisor_start == procident.start_token(os.getpid())

    def test_a_worker_built_without_init_has_inert_class_defaults(self):
        """`_SupervisorWatch` defaults: a test double that bypasses `__init__`
        behaves exactly as before reload existed."""
        worker = SnapshotWorker.__new__(SnapshotWorker)
        assert worker.reload_epoch is None and worker.born_epoch == 0
        assert worker.workers_lock is None and worker.supervisor_start is None


# ---------------------------------------------------------------------------
# _exit_reason / _wait
# ---------------------------------------------------------------------------


@pytest.fixture
def parts():
    """(stop flag, epoch) for a worker born under epoch 0."""
    return StopFlag(), ReloadEpoch()


def _worker(monkeypatch, parts, *, gone=False, born=0, epoch=True):
    stop, rel = parts
    world = Parent(monkeypatch, supervisor_alive=True)
    worker = SnapshotWorker(
        make_config(), stop, reload_epoch=rel if epoch else None, born_epoch=born
    )
    worker.supervisor_pid = SUPERVISOR
    worker.supervisor_start = None
    monkeypatch.setattr(workers.multiprocessing, "get_start_method", lambda allow_none=False: "spawn")
    worker._arm_supervisor_watch()
    if gone:
        world.parent = 1
    return worker, world


class TestExitReason:
    def test_nothing_to_do_is_none(self, monkeypatch, parts):
        worker, _ = _worker(monkeypatch, parts)
        assert worker._exit_reason() is None
        assert worker._should_stop() is False

    def test_each_reason_alone(self, monkeypatch, parts):
        stop, rel = parts
        worker, world = _worker(monkeypatch, parts)
        rel.set(1)
        assert worker._exit_reason() is ExitReason.RELOAD
        rel.set(0)
        world.parent = 1
        assert worker._exit_reason() is ExitReason.SUPERVISOR_GONE
        world.parent = SUPERVISOR
        stop.set()
        assert worker._exit_reason() is ExitReason.STOP

    def test_priority_is_stop_then_supervisor_gone_then_reload(self, monkeypatch, parts):
        stop, rel = parts
        worker, world = _worker(monkeypatch, parts)
        rel.set(1)
        world.parent = 1
        assert worker._exit_reason() is ExitReason.SUPERVISOR_GONE   # beats RELOAD
        stop.set()
        assert worker._exit_reason() is ExitReason.STOP              # beats both

    def test_stop_beats_reload_without_a_gone_supervisor(self, monkeypatch, parts):
        stop, rel = parts
        worker, _ = _worker(monkeypatch, parts)
        rel.set(9)
        stop.set()
        assert worker._exit_reason() is ExitReason.STOP

    def test_the_declaration_order_is_the_priority(self):
        assert list(ExitReason) == [ExitReason.STOP, ExitReason.SUPERVISOR_GONE, ExitReason.RELOAD]

    def test_without_an_epoch_reload_is_never_reported(self, monkeypatch, parts):
        worker, _ = _worker(monkeypatch, parts, epoch=False, born=5)
        assert worker.reload_epoch is None
        assert worker._exit_reason() is None

    def test_reload_means_the_epoch_differs_not_that_it_is_larger(self, monkeypatch, parts):
        _, rel = parts
        worker, _ = _worker(monkeypatch, parts, born=5)
        rel.set(4)
        assert worker._exit_reason() is ExitReason.RELOAD
        rel.set(5)
        assert worker._exit_reason() is None

    def test_the_epoch_it_was_born_under_is_not_a_reload(self, monkeypatch, parts):
        _, rel = parts
        rel.set(7)
        worker, _ = _worker(monkeypatch, parts, born=7)
        assert worker._exit_reason() is None

    def test_a_raised_stop_does_not_consult_the_epoch(self, monkeypatch, parts):
        stop, _ = parts
        worker, _ = _worker(monkeypatch, parts)
        stop.set()
        worker.reload_epoch = MagicMock(get=MagicMock(side_effect=AssertionError("consulted")))
        assert worker._exit_reason() is ExitReason.STOP


@pytest.fixture
def sliced_pair(monkeypatch):
    """A worker whose `stop_event.wait` is a mock and whose clock is fake."""

    class Clock:
        now = 1000.0

    stop = MagicMock()
    stop.is_set.return_value = False
    stop.wait.side_effect = lambda timeout: setattr(Clock, "now", Clock.now + timeout)
    monkeypatch.setattr(workers.time, "monotonic", lambda: Clock.now)
    rel = ReloadEpoch()
    worker = SnapshotWorker(make_config(), stop, reload_epoch=rel, born_epoch=0)
    return worker, stop, rel


class TestWait:
    def test_returns_none_on_timeout(self, sliced_pair):
        worker, stop, _ = sliced_pair
        assert worker._wait(1.2) is None
        assert stop.wait.call_count == 3

    def test_returns_each_reason(self, monkeypatch, parts):
        stop, rel = parts
        worker, world = _worker(monkeypatch, parts)
        stop.set()
        assert worker._wait(30) is ExitReason.STOP
        stop.clear()
        world.parent = 1
        assert worker._wait(30) is ExitReason.SUPERVISOR_GONE
        world.parent = SUPERVISOR
        rel.set(1)
        assert worker._wait(30) is ExitReason.RELOAD

    @pytest.mark.parametrize("timeout", [0, 0.0, -1, -7.5, float("nan")])
    def test_zero_negative_and_nan_return_the_current_reason_without_sleeping(self, sliced_pair, timeout):
        worker, stop, rel = sliced_pair
        assert worker._wait(timeout) is None
        rel.set(2)
        assert worker._wait(timeout) is ExitReason.RELOAD
        stop.is_set.return_value = True
        assert worker._wait(timeout) is ExitReason.STOP
        stop.wait.assert_not_called()

    def test_a_reload_ends_the_wait_at_the_next_slice(self, sliced_pair):
        worker, stop, rel = sliced_pair
        calls = []

        def wait(timeout):
            calls.append(timeout)
            if len(calls) == 2:
                rel.set(1)

        stop.wait.side_effect = wait
        assert worker._wait(3600) is ExitReason.RELOAD
        assert len(calls) == 2 and all(t == STOP_POLL for t in calls)

    def test_an_epoch_bump_is_noticed_within_one_stop_poll_for_real(self, parts):
        stop, rel = parts
        worker = SnapshotWorker(make_config(), stop, reload_epoch=rel, born_epoch=0)
        timer = threading.Timer(0.2, rel.set, args=(1,))
        timer.start()
        started = time.monotonic()
        try:
            with watchdog(stop, 15):
                reason = worker._wait(30)
        finally:
            timer.cancel()
        elapsed = time.monotonic() - started
        assert reason is ExitReason.RELOAD
        assert elapsed < 0.2 + STOP_POLL + 1.0, f"took {elapsed:.2f}s"

    def test_a_wait_with_no_epoch_runs_its_full_timeout(self, parts):
        stop, _ = parts
        worker = SnapshotWorker(make_config(), stop)
        started = time.monotonic()
        assert worker._wait(0.3) is None
        assert time.monotonic() - started >= 0.25

    def test_wait_returns_an_exitreason_or_none_never_a_bool(self, sliced_pair):
        worker, stop, _ = sliced_pair
        result = worker._wait(0.1)
        assert result is None
        assert not isinstance(result, bool)


# ---------------------------------------------------------------------------
# Orphan detection by identity
# ---------------------------------------------------------------------------


class TestOrphanIdentity:
    @pytest.fixture
    def watched(self, monkeypatch):
        worker, world = armed(monkeypatch)
        worker.supervisor_start = "tok:1"
        self.seen = []

        def make(answer):
            def identity(pid, token):
                self.seen.append((pid, token))
                return answer

            monkeypatch.setattr(workers.procident, "identity", identity)

        return worker, world, make

    def test_mismatch_is_gone_with_its_own_wording_logged_once(self, watched, caplog):
        worker, world, answer = watched
        answer("mismatch")
        world.supervisor_alive = True     # the pid IS alive -- it is somebody else
        with caplog.at_level(logging.DEBUG):
            for _ in range(4):
                assert worker._supervisor_gone() is True
        assert f"pid {SUPERVISOR} now belongs to another process" in caplog.text
        errors = [r for r in caplog.records if r.levelno == logging.ERROR and "is gone" in r.getMessage()]
        assert len(errors) == 1
        assert self.seen[0] == (SUPERVISOR, "tok:1")

    def test_gone_is_no_such_process(self, watched, caplog):
        worker, world, answer = watched
        answer("gone")
        with caplog.at_level(logging.ERROR):
            assert worker._supervisor_gone() is True
        assert "supervisor 4242 is gone (no such process); exiting" in caplog.text

    def test_a_gone_zombie_says_it_has_exited_but_is_unreaped(self, watched, monkeypatch, caplog):
        worker, world, answer = watched
        answer("gone")
        asked = []
        monkeypatch.setattr(workers.procident, "is_zombie", lambda pid: asked.append(pid) or True)
        with caplog.at_level(logging.ERROR):
            for _ in range(3):
                assert worker._supervisor_gone() is True
        assert f"supervisor {SUPERVISOR} is gone (it has exited (a zombie, not yet reaped)); exiting" in caplog.text
        assert "no such process" not in caplog.text
        assert asked[0] == SUPERVISOR
        assert caplog.text.count("is gone") == 1                     # still logged once

    @pytest.mark.parametrize("zombie", [False, None])
    def test_gone_and_not_a_zombie_stays_no_such_process(self, watched, monkeypatch, caplog, zombie):
        worker, world, answer = watched
        answer("gone")
        monkeypatch.setattr(workers.procident, "is_zombie", lambda pid: zombie)
        with caplog.at_level(logging.ERROR):
            assert worker._supervisor_gone() is True
        assert "(no such process)" in caplog.text and "zombie" not in caplog.text

    def test_zombie_status_is_not_consulted_unless_identity_says_gone(self, watched, monkeypatch):
        worker, world, answer = watched
        monkeypatch.setattr(workers.procident, "is_zombie", lambda pid: pytest.fail("consulted"))
        for ident in ("match", "mismatch", "unknown"):
            answer(ident)
            worker._supervisor_gone_logged = False
            worker._supervisor_gone()

    @pytest.mark.skipif(
        not (sys.platform.startswith("linux") or sys.platform == "darwin"), reason="needs start tokens"
    )
    def test_a_real_unreaped_supervisor_is_gone_with_the_zombie_wording(self, monkeypatch, caplog):
        import subprocess

        from _hermetic import hermetic_env

        proc = subprocess.Popen(
            [sys.executable, "-c", "import sys; sys.stdin.read()"],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=hermetic_env(),
        )
        try:
            token = None
            deadline = time.monotonic() + 30
            while token is None and time.monotonic() < deadline:
                token = procident.start_token(proc.pid)
                time.sleep(0.02)
            worker, world = armed(monkeypatch)
            worker.supervisor_pid, worker.supervisor_start = proc.pid, token
            monkeypatch.setattr(workers.os, "getppid", lambda: proc.pid)
            worker._parent_at_start = proc.pid
            monkeypatch.setattr(workers, "_pid_exists", lambda pid: True)       # kill(pid, 0) would say alive
            assert worker._supervisor_gone() is False
            proc.stdin.close()
            assert wait_until(lambda: procident.is_zombie(proc.pid) is True, 30)
            with caplog.at_level(logging.ERROR):
                assert worker._supervisor_gone() is True
            assert "it has exited (a zombie, not yet reaped)" in caplog.text
        finally:
            proc.kill()
            proc.wait(timeout=10)

    def test_unknown_falls_back_to_plain_existence(self, watched, caplog):
        worker, world, answer = watched
        answer("unknown")
        world.supervisor_alive = True
        assert worker._supervisor_gone() is False
        world.supervisor_alive = False
        with caplog.at_level(logging.ERROR):
            assert worker._supervisor_gone() is True
        assert "no such process" in caplog.text

    def test_match_is_not_gone_even_if_pid_probing_would_say_so(self, watched):
        worker, world, answer = watched
        answer("match")
        world.supervisor_alive = False
        assert worker._supervisor_gone() is False

    def test_the_recorded_token_is_what_identity_is_asked_about(self, monkeypatch):
        worker, world = armed(monkeypatch)
        worker.supervisor_start = "darwin:123.000456"
        asked = []
        monkeypatch.setattr(workers.procident, "identity", lambda pid, tok: asked.append((pid, tok)) or "match")
        worker._supervisor_gone()
        assert asked == [(SUPERVISOR, "darwin:123.000456")]

    def test_identity_is_not_consulted_when_the_parent_already_says_gone(self, watched):
        worker, world, answer = watched
        world.parent = 1
        monkeypatch_calls = []
        answer("match")
        assert worker._supervisor_gone() is True
        assert self.seen == []

    @pytest.mark.skipif(
        not (sys.platform.startswith("linux") or sys.platform == "darwin"),
        reason="needs start tokens",
    )
    def test_a_reused_pid_is_detected_with_a_real_token(self, monkeypatch, caplog):
        """The recorded token is ours, but the 'supervisor' pid is recorded with
        a token from another moment in time: same pid, different process."""
        worker, world = armed(monkeypatch)
        worker.supervisor_pid = os.getpid()
        monkeypatch.setattr(workers, "_pid_exists", lambda pid: True)
        worker.supervisor_start = procident.start_token(os.getpid())
        monkeypatch.setattr(workers.os, "getppid", lambda: os.getpid())
        worker._parent_at_start = os.getpid()
        assert worker._supervisor_gone() is False
        worker.supervisor_start = "linux:1" if sys.platform.startswith("linux") else "darwin:1.000000"
        with caplog.at_level(logging.ERROR):
            assert worker._supervisor_gone() is True
        assert "now belongs to another process" in caplog.text


# ---------------------------------------------------------------------------
# What run() does with each reason (inline)
# ---------------------------------------------------------------------------


@pytest.fixture
def inline(mocker, monkeypatch):
    """A SnapshotWorker whose `run()` can be called in this process: one dataset,
    no ZFS."""
    mocker.patch("zfsbackup.backup_manager.zfs.exists", return_value=True)
    mocker.patch("zfsbackup.backup_manager.zfs.list", return_value=[])
    stop, rel = StopFlag(), ReloadEpoch()
    config = BackupConfig(datasets=[DatasetConfig(name="pool/a"), DatasetConfig(name="pool/b")])
    worker = SnapshotWorker(config, stop, reload_epoch=rel, born_epoch=0)
    mocker.patch.object(worker, "_get_interval", return_value=30)
    yield worker, stop, rel
    fd = worker._workers_lock_fd if hasattr(worker, "_workers_lock_fd") else None
    if fd is not None:
        os.close(fd)


class TestRunLeaving:
    def test_reload_logs_info_and_exits_3_after_the_dataset_in_hand(self, inline, mocker, caplog):
        worker, stop, rel = inline
        done = []

        def process(manager, dsi):
            done.append(dsi.name)
            rel.set(1)

        mocker.patch.object(worker, "_process_dataset", side_effect=process)
        with caplog.at_level(logging.INFO), watchdog(stop):
            with pytest.raises(SystemExit) as info:
                worker.run()
        assert info.value.code == WORKER_EXIT_RELOAD == 3
        assert done == ["pool/a"]            # the second dataset was never begun
        infos = [r for r in caplog.records if r.levelno == logging.INFO and "exiting to switch configuration" in r.getMessage()]
        assert len(infos) == 1 and "snapshot-worker finished its current work" in infos[0].getMessage()
        assert "snapshot-worker stopped" not in caplog.text

    def test_reload_before_the_first_dataset_runs_no_dataset(self, inline, mocker):
        worker, stop, rel = inline
        rel.set(1)
        process = mocker.patch.object(worker, "_process_dataset")
        with watchdog(stop), pytest.raises(SystemExit) as info:
            worker.run()
        assert info.value.code == 3
        process.assert_not_called()

    def test_reload_during_the_idle_wait_exits_3_promptly(self, inline, mocker):
        worker, stop, rel = inline
        mocker.patch.object(worker, "_process_dataset")
        timer = threading.Timer(0.3, rel.set, args=(1,))
        timer.start()
        started = time.monotonic()
        try:
            with watchdog(stop, 20), pytest.raises(SystemExit) as info:
                worker.run()
        finally:
            timer.cancel()
        assert info.value.code == 3
        assert time.monotonic() - started < 10     # not the 30 s interval

    def test_stop_exits_normally_and_logs_stopped(self, inline, mocker, caplog):
        worker, stop, rel = inline
        mocker.patch.object(worker, "_process_dataset", side_effect=lambda m, d: stop.set())
        with caplog.at_level(logging.INFO), watchdog(stop):
            assert worker.run() is None
        assert "snapshot-worker stopped" in caplog.text
        assert "exiting to switch configuration" not in caplog.text

    def test_stop_and_reload_together_is_a_stop(self, inline, mocker, caplog):
        worker, stop, rel = inline

        def process(manager, dsi):
            rel.set(1)
            stop.set()

        mocker.patch.object(worker, "_process_dataset", side_effect=process)
        with caplog.at_level(logging.INFO), watchdog(stop):
            assert worker.run() is None          # no SystemExit
        assert "snapshot-worker stopped" in caplog.text

    def test_a_gone_supervisor_logs_error_then_stopped_and_exits_normally(self, inline, mocker, monkeypatch, caplog):
        worker, stop, rel = inline
        world = Parent(monkeypatch)
        worker.supervisor_pid = SUPERVISOR
        worker.supervisor_start = None
        monkeypatch.setattr(workers.multiprocessing, "get_start_method", lambda allow_none=False: "spawn")

        def process(manager, dsi):
            world.parent = 1
            rel.set(1)                           # a reload is pending too: the gone supervisor wins

        mocker.patch.object(worker, "_process_dataset", side_effect=process)
        with caplog.at_level(logging.INFO), watchdog(stop):
            assert worker.run() is None
        order = [r.getMessage() for r in caplog.records if "supervisor 4242 is gone" in r.getMessage() or "snapshot-worker stopped" in r.getMessage()]
        assert len(order) == 2
        assert "is gone" in order[0] and "stopped" in order[1]
        errors = [r for r in caplog.records if "is gone" in r.getMessage()]
        assert errors[0].levelno == logging.ERROR

    def test_an_exception_in_a_dataset_does_not_hide_a_pending_reload(self, inline, mocker):
        worker, stop, rel = inline

        def process(manager, dsi):
            rel.set(1)
            raise RuntimeError("zfs said no")

        mocker.patch.object(worker, "_process_dataset", side_effect=process)
        with watchdog(stop), pytest.raises(SystemExit) as info:
            worker.run()
        assert info.value.code == 3


class TestRemoteWorkerBetweenDestinations:
    @pytest.fixture
    def remote(self, mocker):
        stop, rel = StopFlag(), ReloadEpoch()
        config = BackupConfig(
            datasets=[
                DatasetConfig(
                    name="pool/data",
                    remote=[RemoteDatasetConfig(destination="one"), RemoteDatasetConfig(destination="two")],
                )
            ],
            destinations={"one": Destination(url="http://one"), "two": Destination(url="http://two")},
        )
        worker = RemoteBackupWorker(config, stop, reload_epoch=rel, born_epoch=0)
        rbm = MagicMock()
        worker._remote_manager = rbm
        manager = DatasetManager(config)
        mocker.patch.object(DatasetManager, "get_anchor", return_value=None)
        return worker, rbm, manager, manager.datasets[0], stop, rel

    @staticmethod
    def destinations(rbm):
        return [c.args[1].destination for c in rbm.backup_dataset.call_args_list]

    def test_both_destinations_run_when_nothing_changes(self, remote):
        worker, rbm, manager, dsi, _, _ = remote
        worker._process_dataset(manager, dsi)
        assert self.destinations(rbm) == ["one", "two"]

    def test_a_reload_during_the_first_destination_skips_the_second(self, remote):
        worker, rbm, manager, dsi, _, rel = remote
        rbm.backup_dataset.side_effect = lambda *a: rel.set(1)
        worker._process_dataset(manager, dsi)
        assert self.destinations(rbm) == ["one"]

    def test_there_is_no_check_before_the_first_destination(self, remote):
        worker, rbm, manager, dsi, _, rel = remote
        rel.set(1)                    # the reason to leave is already known
        worker._process_dataset(manager, dsi)
        assert self.destinations(rbm) == ["one"]

    def test_a_stop_also_skips_the_second_destination(self, remote):
        worker, rbm, manager, dsi, stop, _ = remote
        rbm.backup_dataset.side_effect = lambda *a: stop.set()
        worker._process_dataset(manager, dsi)
        assert self.destinations(rbm) == ["one"]

    def test_the_run_exits_3_without_starting_the_second_destination(self, remote, mocker):
        worker, rbm, manager, dsi, stop, rel = remote
        rbm.backup_dataset.side_effect = lambda *a: rel.set(1)
        mocker.patch("zfsbackup.remote.RemoteBackupManager", return_value=rbm)
        mocker.patch.object(worker, "_get_interval", return_value=30)
        with watchdog(stop), pytest.raises(SystemExit) as info:
            worker.run()
        assert info.value.code == 3
        assert self.destinations(rbm) == ["one"]

    def test_a_single_destination_dataset_is_unaffected(self, remote):
        worker, rbm, manager, dsi, _, rel = remote
        dsi.config.remote = dsi.config.remote[:1]
        rel.set(1)
        worker._process_dataset(manager, dsi)
        assert self.destinations(rbm) == ["one"]


# ---------------------------------------------------------------------------
# The workers lock, inline
# ---------------------------------------------------------------------------


class TestHoldWorkersLockInWorker:
    def test_no_path_means_no_lock_and_no_warning(self, caplog):
        worker = SnapshotWorker(make_config(), StopFlag())
        with caplog.at_level(logging.WARNING):
            worker._hold_workers_lock()
        assert worker._workers_lock_fd is None
        assert caplog.records == []

    def test_a_held_lock_is_visible_and_cloexec(self, tmp_path):
        db = tmp_path / "config.db"
        path = daemonlock.ensure_workers_lock(db)
        worker = SnapshotWorker(make_config(), StopFlag(), workers_lock=path)
        worker._hold_workers_lock()
        try:
            fd = worker._workers_lock_fd
            assert isinstance(fd, int)
            assert fcntl.fcntl(fd, fcntl.F_GETFD) & fcntl.FD_CLOEXEC
            assert daemonlock.workers_present(db) is True
        finally:
            os.close(worker._workers_lock_fd)
        assert daemonlock.workers_present(db) is False

    def test_a_missing_lock_file_warns_and_the_worker_continues(self, tmp_path, caplog, mocker):
        mocker.patch("zfsbackup.backup_manager.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.backup_manager.zfs.list", return_value=[])
        stop = StopFlag()
        worker = SnapshotWorker(make_config(), stop, workers_lock=tmp_path / "gone.workers.lock")
        process = mocker.patch.object(worker, "_process_dataset", side_effect=lambda m, d: stop.set())
        with caplog.at_level(logging.INFO), watchdog(stop):
            worker.run()
        assert worker._workers_lock_fd is None
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "workers lock" in r.getMessage()]
        assert warnings, caplog.text
        process.assert_called_once()
        assert "snapshot-worker stopped" in caplog.text

    def test_hold_workers_lock_raising_warns_and_the_worker_continues(self, tmp_path, caplog, mocker):
        mocker.patch("zfsbackup.backup_manager.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.backup_manager.zfs.list", return_value=[])
        mocker.patch("zfsbackup.daemonlock.hold_workers_lock", side_effect=OSError("boom"))
        stop = StopFlag()
        worker = SnapshotWorker(make_config(), stop, workers_lock=tmp_path / "x.workers.lock")
        process = mocker.patch.object(worker, "_process_dataset", side_effect=lambda m, d: stop.set())
        with caplog.at_level(logging.INFO), watchdog(stop):
            worker.run()
        assert worker._workers_lock_fd is None
        assert any(
            r.levelno == logging.WARNING and "cannot hold workers lock" in r.getMessage() and "boom" in r.getMessage()
            for r in caplog.records
        )
        process.assert_called_once()

    def test_the_api_worker_takes_the_lock_too(self, tmp_path, mocker):
        db = tmp_path / "config.db"
        path = daemonlock.ensure_workers_lock(db)
        mocker.patch("zfsbackup.api.create_app", return_value=MagicMock())
        server = MagicMock()
        server.serve_forever.side_effect = lambda: time.sleep(30)
        mocker.patch("zfsbackup.workers._bind_api_server", return_value=server)
        stop = StopFlag()
        worker = ApiWorker(make_config(), stop, workers_lock=path)
        seen = {}
        real_start = threading.Thread.start

        def start(thread):
            seen["present"] = daemonlock.workers_present(db)
            stop.set()
            return real_start(thread)

        mocker.patch.object(threading.Thread, "start", start)
        try:
            worker.run()
        finally:
            if worker._workers_lock_fd is not None:
                os.close(worker._workers_lock_fd)
        assert seen["present"] is True


# ---------------------------------------------------------------------------
# Real worker processes under each start method
# ---------------------------------------------------------------------------


def _events(rec):
    try:
        return (Path(rec) / "events").read_text().split()
    except FileNotFoundError:
        return []


@pytest.fixture
def gated(tmp_path):
    """`gated(method)` -> `(worker, rec_dir, epoch, stop, db)`: a started
    `GatedSnapshotWorker` blocked inside its first dataset."""
    started = []

    def start(method):
        db = tmp_path / f"config-{method}.db"
        lock = daemonlock.ensure_workers_lock(db)
        rec = tmp_path / f"rec-{method}"
        rec.mkdir()
        stop, epoch = StopFlag(), ReloadEpoch()
        worker = GatedSnapshotWorker(
            make_config(), stop, reload_epoch=epoch, born_epoch=0, workers_lock=str(lock)
        )
        worker.start_method = method
        worker.record_dir = str(rec)
        worker.release_path = str(rec / "release")
        started.append((worker, rec))
        worker.start()
        assert wait_until(lambda: "start:pool/a" in _events(rec), 60), f"{method} worker never began its dataset"
        return worker, rec, epoch, stop, db

    yield start
    for worker, rec in started:
        (rec / "release").touch()
        worker.join(15)
        if worker.is_alive():
            worker.kill()
            worker.join(10)


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
@pytest.mark.parametrize("method", START_METHODS)
class TestRealWorkerProcesses:
    def test_a_reload_lets_the_dataset_finish_starts_no_other_and_exits_3(self, gated, method):
        worker, rec, epoch, stop, db = gated(method)
        epoch.set(1)
        time.sleep(STOP_POLL * 3)               # several polls: it is NOT interrupted
        assert worker.is_alive() and worker.exitcode is None
        assert _events(rec) == ["start:pool/a"]
        (rec / "release").touch()
        worker.join(30)
        assert worker.exitcode == WORKER_EXIT_RELOAD
        assert _events(rec) == ["start:pool/a", "done:pool/a"]

    def test_a_stop_finishes_the_dataset_and_exits_0(self, gated, method):
        worker, rec, epoch, stop, db = gated(method)
        stop.set()
        time.sleep(STOP_POLL * 3)
        assert worker.is_alive()
        (rec / "release").touch()
        worker.join(30)
        assert worker.exitcode == 0
        assert _events(rec) == ["start:pool/a", "done:pool/a"]

    def test_stop_wins_over_reload(self, gated, method):
        worker, rec, epoch, stop, db = gated(method)
        epoch.set(1)
        stop.set()
        (rec / "release").touch()
        worker.join(30)
        assert worker.exitcode == 0, "stop and reload together must be a plain stop"
        assert _events(rec) == ["start:pool/a", "done:pool/a"]

    def test_the_workers_lock_is_held_while_running_and_released_on_exit(self, gated, method):
        worker, rec, epoch, stop, db = gated(method)
        assert daemonlock.workers_present(db) is True
        info = json.loads((rec / "lock.json").read_text())
        assert isinstance(info["fd"], int) and info["cloexec"] is True
        epoch.set(1)
        (rec / "release").touch()
        worker.join(30)
        assert worker.exitcode == 3
        assert wait_until(lambda: daemonlock.workers_present(db) is False)

    def test_the_lock_dies_with_a_killed_worker(self, gated, method):
        worker, rec, epoch, stop, db = gated(method)
        assert daemonlock.workers_present(db) is True
        worker.kill()
        worker.join(15)
        assert wait_until(lambda: daemonlock.workers_present(db) is False)

    def test_the_supervisor_holds_no_workers_lock_descriptor_across_the_fork(self, tmp_path, method):
        db = tmp_path / "config.db"
        lock = daemonlock.ensure_workers_lock(db)
        rec = tmp_path / "probe"
        rec.mkdir()
        worker = FdProbeWorker(make_config(), StopFlag(), workers_lock=str(lock))
        worker.start_method = method
        worker.record_dir = str(rec)
        worker.lock_file = str(lock)
        assert descriptors_on(lock) == 0
        worker.start()
        try:
            worker.join(60)
            assert worker.exitcode == 0
            assert descriptors_on(lock) == 0, "the supervisor must never open the workers lock"
        finally:
            if worker.is_alive():
                worker.kill()
                worker.join(10)
        (record,) = [json.loads(p.read_text()) for p in rec.glob("probe.*.json")]
        assert record["before"] == 0, "nothing inherited from the supervisor"
        assert record["after"] == 1, "the worker took exactly its own descriptor"

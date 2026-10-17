"""Unit tests for BackupDaemon."""

import multiprocessing
import signal
import sqlite3
import time
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

from zfsbackup.config import BackupConfig, DatasetConfig, RemoteDatasetConfig, RemoteServerConfig, Destination
from zfsbackup.config.store import (
    CONFIG_PATH_ENV,
    EXPECTED_HEAD,
    ConfigSnapshot,
    dispose_all,
    import_yaml,
    resolve_config_path,
)
from zfsbackup.daemon import (
    BackupDaemon,
    WORKER_MAX_CONSECUTIVE_FAILURES,
    WORKER_RESTART_DELAY,
    main,
)
from zfsbackup.workers import SnapshotWorker, PruningWorker, ApiWorker, RemoteBackupWorker


def _make_config(**kwargs) -> BackupConfig:
    defaults = dict(datasets=[DatasetConfig(name='pool/data')])
    defaults.update(kwargs)
    return BackupConfig(**defaults)


def _fake_clock(daemon, mocker, start=1000.0):
    """Drive the supervisor's restart deadlines through the `_monotonic`
    seam. Returns a one-element list; assign `clock[0]` to advance time."""
    clock = [start]
    mocker.patch.object(daemon, '_monotonic', side_effect=lambda: clock[0])
    return clock


class TestBackupDaemonInit:
    def test_instantiation(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        assert daemon.config is sample_backup_config
        # Renamed from `config_path` (item 8, decision D-3): the type
        # changed from a bare `Path` to `ResolvedConfigPath`, and keeping
        # the old attribute name is exactly how a bare `Path` -- which
        # lacks the `.url()`/`.resolved_path` every real open needs --
        # would sneak back in unnoticed.
        assert daemon.resolved_config == sqlite_config_db
        assert daemon.verbose is False

    def test_verbose_flag(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db, verbose=True)
        assert daemon.verbose is True

    def test_stop_event_initially_clear(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        assert not daemon._stop_event.is_set()


class TestSignalHandler:
    def test_signal_sets_stop_event(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        daemon._signal_handler(signal.SIGINT, None)
        assert daemon._stop_event.is_set()

    def test_sigterm_sets_stop_event(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        daemon._signal_handler(signal.SIGTERM, None)
        assert daemon._stop_event.is_set()

    def test_records_signum_for_report_stop_signal(self, sqlite_config_db, sample_backup_config):
        """`_signal_handler` records which signal fired so `_report_stop_signal`
        can name it later, from a context where logging is legal (see both
        functions' docstrings). `None` means "raised by `_check_workers`,
        not a signal" -- `_report_stop_signal` relies on that distinction to
        stay silent for a configuration fault.
        """
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        assert daemon._stop_signum is None
        daemon._signal_handler(signal.SIGTERM, None)
        assert daemon._stop_signum == signal.SIGTERM

    def test_handler_does_no_logging(self, sqlite_config_db, sample_backup_config, caplog):
        """A signal handler must not call anything that takes a lock --
        `logger.info()` takes `logging._lock` and a `Handler.lock`, then
        performs a blocking write a stalled journald or a full pipe buffer
        can hang forever on, which would mean `set()` (and thus the flag
        every worker polls) is never reached. See `_signal_handler`'s own
        docstring for the measured before/after. `_report_stop_signal` --
        called from `run()`'s poll loop, never from the handler itself --
        is where the operator-facing line is actually emitted.
        """
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        with caplog.at_level('DEBUG'):
            daemon._signal_handler(signal.SIGTERM, None)
        assert caplog.text == ''
        assert caplog.records == []


class TestReportStopSignal:
    """`_report_stop_signal` is the operator-facing line `_signal_handler`
    is deliberately forbidden from logging itself (see both docstrings).
    Called from `run()`'s poll loop instead, where taking a lock and
    blocking on a journald write is merely slow, not signal-handler-fatal.
    """

    def test_logs_sigterm_line(self, sqlite_config_db, sample_backup_config, caplog):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        daemon._stop_signum = signal.SIGTERM
        with caplog.at_level('INFO'):
            daemon._report_stop_signal()
        assert 'Received SIGTERM, shutting down gracefully' in caplog.text

    def test_logs_sigint_line(self, sqlite_config_db, sample_backup_config, caplog):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        daemon._stop_signum = signal.SIGINT
        with caplog.at_level('INFO'):
            daemon._report_stop_signal()
        assert 'Received SIGINT, shutting down gracefully' in caplog.text

    def test_idempotent_logs_only_once(self, sqlite_config_db, sample_backup_config, caplog):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        daemon._stop_signum = signal.SIGTERM
        with caplog.at_level('INFO'):
            daemon._report_stop_signal()
            daemon._report_stop_signal()
            daemon._report_stop_signal()
        assert caplog.text.count('Received SIGTERM') == 1

    def test_silent_when_flag_raised_by_check_workers_not_a_signal(
        self, sqlite_config_db, sample_backup_config, caplog
    ):
        """`_stop_signum` stays `None` when `_check_workers` raises the
        flag itself (a crash-looped worker) -- that path logs its own
        explanation, and `_report_stop_signal` must add nothing.
        """
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        assert daemon._stop_signum is None
        with caplog.at_level('INFO'):
            daemon._report_stop_signal()
        assert caplog.text == ''


class TestSupervisorSleep:
    """`_supervisor_sleep` is the seam that bounds SIGTERM/SIGINT-to-
    `_shutdown_workers` latency to `SUPERVISOR_SLEEP_SLICE` instead of the
    full `SUPERVISOR_POLL` -- a bare `time.sleep(SUPERVISOR_POLL)` is not
    interruptible mid-sleep (PEP 475: retried with the remaining time once
    a signal handler returns), so the flag being set promptly buys nothing
    without this. Exercised directly, not just through `run()`'s counting
    tests above, so a regression that reintroduces the un-sliced sleep (or
    breaks the early return) is caught even if the `run()`-level tests'
    iteration counting is ever changed.
    """

    def test_returns_promptly_once_per_iteration(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        """One call to `_supervisor_sleep` must not busy-loop internally in
        a way a caller can observe as more than one real sleep -- assert it
        actually calls `time.sleep` in slices no larger than
        `SUPERVISOR_SLEEP_SLICE`, and stops as soon as the flag is set from
        outside (mirroring a signal handler firing mid-sleep).
        """
        from zfsbackup.daemon import SUPERVISOR_SLEEP_SLICE

        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        calls = []

        def fake_sleep(seconds):
            calls.append(seconds)
            assert seconds <= SUPERVISOR_SLEEP_SLICE
            if len(calls) == 2:
                daemon._stop_event.set()

        mocker.patch('zfsbackup.daemon.time.sleep', side_effect=fake_sleep)

        daemon._supervisor_sleep(5)

        # Stopped after the slice that set the flag, not after sleeping out
        # the full 5s requested -- this is the entire point of slicing.
        assert len(calls) == 2

    def test_returns_immediately_if_flag_already_set(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        daemon._stop_event.set()
        sleep_mock = mocker.patch('zfsbackup.daemon.time.sleep')

        daemon._supervisor_sleep(5)

        sleep_mock.assert_not_called()

    def test_respects_deadline_when_flag_never_set(
        self, sqlite_config_db, sample_backup_config
    ):
        """No mocking: a real, short sleep budget must actually elapse
        (within a generous margin) and return, rather than the flag check
        alone governing how long the call takes.
        """
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        start = time.monotonic()
        daemon._supervisor_sleep(0.3)
        elapsed = time.monotonic() - start
        assert 0.3 <= elapsed < 5
        assert not daemon._stop_event.is_set()


class TestActiveWorkerNames:
    def test_base_workers_always_included(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        names = daemon._active_worker_names()
        assert 'snapshot' in names
        assert 'pruning' in names
        assert 'api' in names

    def test_remote_worker_excluded_when_no_remote(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        names = daemon._active_worker_names()
        assert 'remote' not in names

    def test_remote_worker_included_when_datasets_have_remote(self, sqlite_config_db):
        config = _make_config(datasets=[
            DatasetConfig(
                name='pool/data',
                remote=[RemoteDatasetConfig(destination='offsite')],
            ),
        ])
        daemon = BackupDaemon(config, sqlite_config_db)
        names = daemon._active_worker_names()
        assert 'remote' in names


class TestNewWorker:
    def test_new_worker_snapshot(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        worker = daemon._new_worker('snapshot')
        assert isinstance(worker, SnapshotWorker)

    def test_new_worker_pruning(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        worker = daemon._new_worker('pruning')
        assert isinstance(worker, PruningWorker)

    def test_new_worker_api(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        worker = daemon._new_worker('api')
        assert isinstance(worker, ApiWorker)

    def test_new_worker_remote(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        worker = daemon._new_worker('remote')
        assert isinstance(worker, RemoteBackupWorker)


class TestStartWorkers:
    def test_starts_all_active_workers(self, sqlite_config_db, sample_backup_config, mocker):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        mock_worker = MagicMock()
        mock_worker.pid = 1234
        mocker.patch.object(daemon, '_new_worker', return_value=mock_worker)
        mocker.patch('zfsbackup.daemon.dispose_all')
        daemon._start_workers()
        assert mock_worker.start.call_count == len(daemon._active_worker_names())
        assert len(daemon._workers) == len(daemon._active_worker_names())


class TestCheckWorkers:
    def test_restarts_dead_worker(self, sqlite_config_db, sample_backup_config, mocker):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        dead_worker = MagicMock()
        dead_worker.is_alive.return_value = False
        dead_worker.pid = 100
        dead_worker.exitcode = 1
        daemon._workers = {'snapshot': dead_worker}

        new_worker = MagicMock()
        new_worker.pid = 200
        mocker.patch.object(daemon, '_new_worker', return_value=new_worker)
        mocker.patch('zfsbackup.daemon.dispose_all')
        clock = _fake_clock(daemon, mocker)

        # First pass only schedules the restart (a backoff deadline, no
        # sleep): nothing is forked yet.
        daemon._check_workers()
        new_worker.start.assert_not_called()
        assert daemon._workers['snapshot'] is dead_worker
        assert 'snapshot' in daemon._restart_at

        # Past the deadline the next poll forks the replacement.
        clock[0] += WORKER_RESTART_DELAY
        daemon._check_workers()
        new_worker.start.assert_called_once()
        assert daemon._workers['snapshot'] is new_worker

    def test_alive_workers_not_restarted(self, sqlite_config_db, sample_backup_config, mocker):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        alive_worker = MagicMock()
        alive_worker.is_alive.return_value = True
        daemon._workers = {'snapshot': alive_worker}
        mocker.patch.object(daemon, '_new_worker')
        daemon._check_workers()
        daemon._new_worker.assert_not_called()


class TestCrashLoopGuard:
    """The supervisor's only fatal worker classification is the crash loop
    (A-5): `WORKER_MAX_CONSECUTIVE_FAILURES` consecutive deaths of one worker
    stop the WHOLE daemon, and `run()` returns 1.

    Item 15.2(b) deleted the other one. A worker used to exit `EX_CONFIG`
    when its own config load hit a permanent fault, and `_check_workers`
    treated that exit as fatal on first sight; workers now take the
    supervisor's detached config and never open the database, so that fault
    can only be met -- and is refused -- in `_run`, before any worker exists
    (`TestRunRefusesBadConfigBeforeAnyWorker`). What is left here is pinned
    through `run()` rather than `_check_workers` alone, because the
    interesting property is the daemon's exit code and that nothing is
    forked once a worker has given up.

    Mutation-checked (scratch copy): deleting the `if not self._crash_looped:`
    guard in front of the respawn loop fails
    `test_mixed_pass_respawns_nothing_once_one_worker_gives_up` (1 of 4 --
    only a restart already DUE in the pass in which another worker gives up
    can reach the guard, which is why that test pre-arms one); `run()`
    returning a constant 0 fails
    `test_mixed_pass_respawns_nothing_once_one_worker_gives_up` and
    `test_run_returns_one_when_a_worker_gives_up` (2 of 4) plus 1 in the
    resilience file.
    """

    @staticmethod
    def _dead(exitcode=1, pid=100):
        w = MagicMock()
        w.is_alive.return_value = False
        w.pid = pid
        w.exitcode = exitcode
        return w

    def _arm_run(self, daemon, mocker, workers):
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        mocker.patch('zfsbackup.backup_manager.zfs.set')
        mocker.patch('zfsbackup.daemon.dispose_all')
        # A broken guard must loop in pure Python, never fork real children.
        mocker.patch.object(daemon, '_new_worker', return_value=MagicMock(pid=999))
        mocker.patch.object(
            daemon, '_start_workers',
            side_effect=lambda: daemon._workers.update(workers),
        )
        _fake_clock(daemon, mocker)
        sleeps = [0]

        def bounded_sleep(_):
            sleeps[0] += 1
            if sleeps[0] > 20:  # safety valve: a clean failure, not a hang
                daemon._stop_event.set()

        mocker.patch.object(daemon, '_supervisor_sleep', side_effect=bounded_sleep)
        return sleeps

    def test_a_worker_on_its_last_failure_is_dropped_and_never_restarted(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        daemon._failures['snapshot'] = WORKER_MAX_CONSECUTIVE_FAILURES - 1
        daemon._workers = {'snapshot': self._dead()}
        mocker.patch.object(daemon, '_new_worker')
        mocker.patch('zfsbackup.daemon.dispose_all')

        daemon._check_workers()

        daemon._new_worker.assert_not_called()
        assert 'snapshot' not in daemon._workers
        assert daemon._crash_looped == {'snapshot'}
        assert daemon._stop_event.is_set()

    def test_mixed_pass_respawns_nothing_once_one_worker_gives_up(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        """One worker gives up in the same pass in which another's backoff
        deadline has come due. The due restart is NOT carried out (a
        replacement forked into a daemon that is about to stop would only be
        stopped again) and the whole daemon stops with exit code 1."""
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        daemon._failures['snapshot'] = WORKER_MAX_CONSECUTIVE_FAILURES - 1
        waiting = self._dead(pid=200)
        sleeps = self._arm_run(
            daemon, mocker, {'snapshot': self._dead(pid=100), 'pruning': waiting}
        )
        daemon._failures['pruning'] = 1
        daemon._restart_at['pruning'] = 900.0  # due: the fake clock reads 1000

        result = daemon.run()

        assert sleeps[0] <= 20, "supervisor loop never stopped on its own"
        daemon._new_worker.assert_not_called()
        assert result == 1
        assert daemon._crash_looped == {'snapshot'}
        assert 'snapshot' not in daemon._workers
        assert daemon._workers.get('pruning') is waiting  # left as it was
        assert daemon._stop_event.is_set()

    def test_run_returns_one_when_a_worker_gives_up(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        daemon._failures = {n: WORKER_MAX_CONSECUTIVE_FAILURES - 1 for n in daemon._active_worker_names()}
        dead = {n: self._dead() for n in daemon._active_worker_names()}
        sleeps = self._arm_run(daemon, mocker, dead)

        assert daemon.run() == 1

        assert sleeps[0] <= 20
        assert daemon._workers == {}
        assert daemon._crash_looped == set(dead)
        daemon._new_worker.assert_not_called()

    def test_run_returns_zero_on_a_clean_stop(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        alive = MagicMock()
        alive.is_alive.return_value = False  # joined at shutdown
        sleeps = self._arm_run(daemon, mocker, {'snapshot': alive})
        mocker.patch.object(daemon, '_check_workers')
        mocker.patch.object(
            daemon, '_supervisor_sleep',
            side_effect=lambda _: daemon._signal_handler(signal.SIGTERM, None),
        )

        assert daemon.run() == 0
        assert daemon._crash_looped == set()


class TestSpawnOrdering:
    """8g: `_spawn` is the single fork point, and `dispose_all()` must run
    BEFORE the worker's own `start()` -- both at initial startup
    (`_start_workers`) and at every crash re-fork (`_check_workers`),
    which forks arbitrarily late in the daemon's life, after `main()` has
    left a live pooled connection to the config database. Asserting mere
    call counts (as the pre-item-8 `test_starts_all_active_workers` did)
    cannot catch a reordering; only the sequence itself can.

    Mutation-checked: reordering `_spawn` to call `worker.start()` before
    `dispose_all()` fails both tests below (the recorded order becomes
    `['new_worker:...', 'start', 'dispose_all']`), and deleting the
    `dispose_all()` call entirely fails both as well (missing from the
    recorded order).
    """

    def _record_calls(self, daemon, mocker):
        call_order = []
        mocker.patch(
            'zfsbackup.daemon.dispose_all',
            side_effect=lambda: call_order.append('dispose_all'),
        )

        def fake_new_worker(name):
            call_order.append(f'new_worker:{name}')
            worker = MagicMock()
            worker.pid = 4242
            worker.start.side_effect = lambda: call_order.append('start')
            return worker

        mocker.patch.object(daemon, '_new_worker', side_effect=fake_new_worker)
        return call_order

    def test_start_workers_disposes_before_starting(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        call_order = self._record_calls(daemon, mocker)

        daemon._spawn('pruning')

        assert call_order == ['dispose_all', 'new_worker:pruning', 'start']

    def test_check_workers_re_fork_disposes_before_starting(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        dead_worker = MagicMock()
        dead_worker.is_alive.return_value = False
        dead_worker.pid = 100
        dead_worker.exitcode = 1
        daemon._workers = {'pruning': dead_worker}
        clock = _fake_clock(daemon, mocker)

        call_order = self._record_calls(daemon, mocker)

        daemon._check_workers()
        assert call_order == []  # backoff pending: nothing forked yet
        clock[0] += WORKER_RESTART_DELAY
        daemon._check_workers()

        assert call_order == ['dispose_all', 'new_worker:pruning', 'start']


class TestShutdownWorkers:
    def test_joins_all_workers(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        worker1 = MagicMock()
        worker1.is_alive.return_value = False
        worker2 = MagicMock()
        worker2.is_alive.return_value = False
        daemon._workers = {'snapshot': worker1, 'pruning': worker2}
        daemon._shutdown_workers()
        worker1.join.assert_called()
        worker2.join.assert_called()

    def test_terminates_workers_that_dont_exit(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        stubborn = MagicMock()
        stubborn.is_alive.return_value = True
        daemon._workers = {'snapshot': stubborn}
        daemon._shutdown_workers(timeout=1)
        stubborn.terminate.assert_called_once()

    def test_escalates_to_sigkill_when_terminate_does_not_work(
        self, sqlite_config_db, sample_backup_config
    ):
        """Coordinator-reported defect 4: `terminate()` sends SIGTERM,
        which a child with a SIGTERM *handler* installed does not have to
        obey (measured: `alive after terminate+join(5): True exitcode
        None`, before `workers._init_child_process` reset the child's
        disposition). `_shutdown_workers` now escalates to `.kill()`
        (SIGKILL) rather than falling through silently -- `is_alive()` is
        checked 3 times in sequence (after the initial `join`, after
        `terminate()+join(5)`, after `kill()+join(5)`); this worker
        reports alive for the first two and dead for the third, so
        `.kill()` must have been the thing that actually stopped it.
        """
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        worker = MagicMock()
        worker.pid = 4242
        worker.is_alive.side_effect = [True, True, False]
        daemon._workers = {'snapshot': worker}

        daemon._shutdown_workers(timeout=1)

        worker.terminate.assert_called_once()
        worker.kill.assert_called_once()

    def test_logs_error_when_worker_survives_sigkill(
        self, sqlite_config_db, sample_backup_config, caplog
    ):
        """The pathological case `_shutdown_workers`'s own docstring
        names explicitly (an uninterruptible sleep, e.g. blocked in a
        `zfs` ioctl): even SIGKILL does not stop it. Must not raise, must
        not loop -- one more ERROR line naming the worker, then move on
        (an earlier version of this exact case fell through silently,
        leaving the interpreter's own exit handler to `join()` with NO
        timeout, hanging `systemctl stop` until TimeoutStopSec).
        """
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        worker = MagicMock()
        worker.pid = 4242
        worker.is_alive.side_effect = [True, True, True]
        daemon._workers = {'snapshot': worker}

        with caplog.at_level('ERROR'):
            daemon._shutdown_workers(timeout=1)

        worker.kill.assert_called_once()
        assert 'survived SIGKILL' in caplog.text
        assert '4242' in caplog.text

    def test_shutdown_budget_is_a_single_shared_deadline_not_per_worker(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        """`timeout` is a budget shared across ALL workers, computed ONCE,
        not re-granted in full to each one in turn -- the old per-worker
        `timeout=30` meant four workers stuck in a long `zfs send` (the
        case the budget exists for) could cost 4 x (30 + 5 + 5) = 160s
        *after* the poll loop exited, against systemd's default
        `TimeoutStopSec=90s`. See `_shutdown_workers`'s own docstring.

        Modeled with a mocked `time.monotonic` whose `join()` side effect
        advances the clock by exactly the `timeout` it was given -- the
        worst case, a worker that stays busy for its entire granted
        allowance (e.g. a genuinely stuck `zfs send`). Three workers, a
        12s total budget: the first is offered the whole 12s and consumes
        it; every worker after that must be offered exactly `0.0` (clamped
        by `max(0.0, ...)`, never negative) because the shared deadline has
        already passed -- not a fresh budget each time. The recorded
        offered timeouts are non-increasing and sum to no more than the
        12s budget, which is the property the docstring promises; a
        reversion to per-worker timeouts would instead offer `12.0` to
        every worker regardless of how much time the earlier ones took.
        """
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)

        workers = {}
        for name in ('snapshot', 'pruning', 'api'):
            w = MagicMock()
            w.is_alive.return_value = False
            workers[name] = w
        daemon._workers = workers

        clock = [0.0]

        def fake_monotonic():
            return clock[0]

        def advance_and_join(timeout=None):
            clock[0] += timeout if timeout is not None else 0.0
            return None

        for w in workers.values():
            w.join.side_effect = advance_and_join

        mocker.patch('zfsbackup.daemon.time.monotonic', side_effect=fake_monotonic)

        daemon._shutdown_workers(timeout=12)

        recorded = [w.join.call_args.kwargs['timeout'] for w in workers.values()]
        assert recorded == [12.0, 0.0, 0.0]
        assert recorded == sorted(recorded, reverse=True)
        assert sum(recorded) <= 12


class TestDaemonRun:
    def test_run_does_not_start_workers_when_stop_flag_pre_set(
        self, sqlite_config_db, sample_backup_config, mocker, caplog
    ):
        """A SIGTERM landing during `verify_datasets()`/
        `sync_all_config_properties()` -- both of which run before the
        `_start_workers()` guard -- must not fork any worker at all: each
        one would do `dispose_all()`, a schema check and a full config
        load only to die at the loop guard's first evaluation a moment
        later. With the flag pre-set (mirroring that startup SIGTERM),
        `_start_workers` must never be called, `_shutdown_workers` still
        must be (an empty `_workers` makes it a no-op, but the shutdown
        tail is the single exit either way), `run()` must return 0 -- no
        worker was ever forked -- and the log must say so.

        This replaces the pre-item-8x version of this test, which asserted
        the opposite (`_start_workers.assert_called_once()`) -- that was
        "forking into a shutdown", the exact hazard `run()`'s guard (and
        `_check_workers`' matching one on the restart path) now exists to
        prevent. See `run()`'s own docstring and the branch comment above
        the guard.
        """
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)

        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        mocker.patch('zfsbackup.backup_manager.zfs.set')

        mocker.patch.object(daemon, '_start_workers')
        mocker.patch.object(daemon, '_check_workers')
        mocker.patch.object(daemon, '_shutdown_workers')

        daemon._stop_event.set()
        with caplog.at_level('INFO'):
            result = daemon.run()

        daemon._start_workers.assert_not_called()
        daemon._shutdown_workers.assert_called_once()
        daemon._check_workers.assert_not_called()
        assert result == 0
        assert 'Shutdown requested during startup' in caplog.text

    def test_run_starts_workers_when_stop_flag_set_from_inside_loop(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        """The sibling of the pre-set case above: with the flag raised
        from INSIDE the poll loop (the ordinary running case -- a signal
        arriving once the daemon is up, or `_check_workers` raising it
        itself), `_start_workers` MUST be called exactly once. Without
        this, an inverted guard (e.g. `if not self._stop_event.is_set():`
        turned into `if self._stop_event.is_set():`, or the guard's branch
        arms swapped) would pass
        `test_run_does_not_start_workers_when_stop_flag_pre_set` above
        vacuously -- nothing would ever call `_start_workers` -- and the
        daemon would never actually start a worker in normal operation.
        """
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)

        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        mocker.patch('zfsbackup.backup_manager.zfs.set')

        mocker.patch.object(daemon, '_start_workers')
        mocker.patch.object(daemon, '_check_workers')
        mocker.patch.object(daemon, '_shutdown_workers')

        def stop_after_start():
            daemon._stop_event.set()

        daemon._start_workers.side_effect = stop_after_start

        result = daemon.run()

        daemon._start_workers.assert_called_once()
        daemon._shutdown_workers.assert_called_once()
        assert result == 0

    def test_run_supervisor_loop_checks_workers(self, sqlite_config_db, sample_backup_config, mocker):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)

        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        mocker.patch('zfsbackup.backup_manager.zfs.set')

        mocker.patch.object(daemon, '_start_workers')
        mocker.patch.object(daemon, '_check_workers')
        mocker.patch.object(daemon, '_shutdown_workers')

        # Patches `_supervisor_sleep` -- the named, once-per-iteration seam
        # -- rather than the raw `time.sleep` it now calls twenty times per
        # iteration internally (0.25s slices of `SUPERVISOR_POLL=5`).
        # `sleep_calls[0] >= 2` must fire on the second *iteration*, not the
        # second slice of the first.
        sleep_calls = [0]

        def sleep_and_maybe_stop(_):
            sleep_calls[0] += 1
            if sleep_calls[0] >= 2:
                daemon._stop_event.set()

        mocker.patch.object(daemon, '_supervisor_sleep', side_effect=sleep_and_maybe_stop)
        daemon.run()

        daemon._check_workers.assert_called_once()
        daemon._shutdown_workers.assert_called_once()


class TestMain:
    @pytest.fixture(autouse=True)
    def _preflight_and_lock(self, mocker, lock_spy):
        """`_run` now stats the database (`check_config_db`) and takes the
        presence lock BEFORE `load_runtime_config`; these tests mock the
        load, so there is no database behind the path. Stub only the
        stat-only preflight; the lock is real (beside a tmp path) and
        `lock_spy` closes whatever a crashed run leaves open."""
        mocker.patch('zfsbackup.daemon.check_config_db')

    def _patch_config(self, mocker, **kwargs):
        defaults = dict(
            datasets=[DatasetConfig(name='pool/data')],
            destinations={},
            remote_backup=None,
            dry_run=False,
        )
        defaults.update(kwargs)
        mock_config = MagicMock(spec=BackupConfig)
        for k, v in defaults.items():
            setattr(mock_config, k, v)
        mocker.patch(
            'zfsbackup.daemon.load_runtime_config',
            return_value=ConfigSnapshot(mock_config, 7, 'deadbeef'),
        )
        return mock_config

    def test_test_config_returns_zero(self, tmp_path, mocker):
        self._patch_config(mocker)
        with patch('sys.argv', ['daemon', '--test-config', '-c', str(tmp_path / 'cfg.db')]):
            result = main()
        assert result == 0

    def test_test_config_with_destinations(self, tmp_path, mocker):
        self._patch_config(
            mocker,
            destinations={'offsite': Destination(url='http://backup.example.com')},
            remote_backup=MagicMock(target_dataset='pool/backups'),
        )
        with patch('sys.argv', ['daemon', '--test-config', '-c', str(tmp_path / 'cfg.db')]):
            result = main()
        assert result == 0

    def test_config_load_failure_returns_one(self, tmp_path, mocker):
        mocker.patch('zfsbackup.daemon.load_runtime_config', side_effect=Exception('bad config'))
        with patch('sys.argv', ['daemon', '-c', str(tmp_path / 'cfg.db')]):
            result = main()
        assert result == 1

    def test_daemon_failure_returns_one(self, tmp_path, mocker):
        self._patch_config(mocker)
        mock_daemon = MagicMock()
        mock_daemon.run.side_effect = Exception('crash')
        mocker.patch('zfsbackup.daemon.BackupDaemon', return_value=mock_daemon)
        with patch('sys.argv', ['daemon', '-c', str(tmp_path / 'cfg.db')]):
            result = main()
        assert result == 1

    def test_normal_run_returns_zero(self, tmp_path, mocker):
        self._patch_config(mocker)
        mock_daemon = MagicMock()
        # `main()` now returns `daemon.run()`'s own value (item 8i: `run()`
        # returns 0/1 rather than always 0, so a stale-schema crash loop is
        # visible in a service manager's exit code) -- an unconfigured
        # `MagicMock` return isn't `== 0`, so this must be explicit.
        mock_daemon.run.return_value = 0
        mocker.patch('zfsbackup.daemon.BackupDaemon', return_value=mock_daemon)
        with patch('sys.argv', ['daemon', '-c', str(tmp_path / 'cfg.db')]):
            result = main()
        assert result == 0
        mock_daemon.run.assert_called_once()

    def test_dry_run_flag_sets_config(self, tmp_path, mocker):
        mock_config = self._patch_config(mocker)
        mock_daemon = MagicMock()
        mock_daemon.run.return_value = 0
        mocker.patch('zfsbackup.daemon.BackupDaemon', return_value=mock_daemon)
        with patch('sys.argv', ['daemon', '-d', '-c', str(tmp_path / 'cfg.db')]):
            main()
        assert mock_config.dry_run is True

    def test_verbose_flag_sets_log_level(self, tmp_path, mocker):
        self._patch_config(mocker)
        mock_daemon = MagicMock()
        mock_daemon.run.return_value = 0
        mocker.patch('zfsbackup.daemon.BackupDaemon', return_value=mock_daemon)
        with patch('sys.argv', ['daemon', '-v', '-c', str(tmp_path / 'cfg.db')]):
            result = main()
        assert result == 0


class TestMainConfigSourceResolution:
    """New coverage (item 8, sub-item 8k): the contract item 6a names as
    the single most likely thing item 8 breaks -- `-c`'s `default=None`
    and the deliberate absence of `type=Path` (`str(Path("")) == "."`,
    which would turn `-c ""` into "the current directory" and silently
    defeat `resolve_config_path`'s documented empty-string fallthrough).
    These exercise `main()` end-to-end, with NO mocking of
    `load_runtime_config`/`check_config_schema`, against a real imported
    database -- so a regression that reintroduces `type=Path` or a
    non-`None` default fails here even though every mocked `TestMain`
    test above would keep passing (they never touch `ZFSBACKUP_CONFIG`
    or an empty `-c` at all).
    """

    def test_env_var_reachable_when_dash_c_absent(self, sqlite_config_db, monkeypatch):
        monkeypatch.setenv(CONFIG_PATH_ENV, str(sqlite_config_db.path))
        with patch('sys.argv', ['daemon', '--test-config']):
            result = main()
        assert result == 0

    def test_dash_c_empty_string_falls_through_to_env_var(self, sqlite_config_db, monkeypatch):
        monkeypatch.setenv(CONFIG_PATH_ENV, str(sqlite_config_db.path))
        with patch('sys.argv', ['daemon', '--test-config', '-c', '']):
            result = main()
        assert result == 0

    def test_dash_c_yaml_target_exits_one_with_import_hint(self, config_yaml_path, caplog):
        with patch('sys.argv', ['daemon', '-c', str(config_yaml_path)]):
            with caplog.at_level('ERROR'):
                result = main()
        assert result == 1
        assert 'zfsbackup-config import' in caplog.text
        # No traceback: the message is operator-grade and logged verbatim,
        # not wrapped by the generic `except Exception` arm.
        assert 'Traceback' not in caplog.text

    def test_test_config_valid_db_creates_no_new_files(self, tmp_path, config_yaml_path, caplog):
        """`check_config_schema` opens a connection through the store's
        pid-keyed, pooled engine cache, which -- as long as ANY process
        holds it -- keeps `-wal`/`-shm` sidecars present on disk (measured;
        see `db.py`'s "WAL sidecars" section and
        `tests/test_zfsbackup_store.py`'s `TestDbWalSidecars`). They are
        checkpointed away once the pool's connection is genuinely closed.

        **Deliberately in-process, with an explicit `dispose_all()` after
        `main()` returns, not a subprocess relying on interpreter-exit GC
        to do that closing implicitly.** An earlier version of this test
        ran the daemon as a real `python -m zfsbackup.daemon` subprocess
        and asserted the sidecars were gone the instant it exited -- true,
        measured, on this repo's default Python 3.14/darwin. It is NOT
        portable: the identical subprocess, under `/tmp/v311/bin/python`
        (3.11), left `-wal`/`-shm` on disk with no sign of going away
        even after a 2s poll -- an environment-dependent detail of
        *when* CPython's interpreter-shutdown sequence finalizes a
        SQLAlchemy pool's DBAPI connection, not a contract `--test-config`
        (or anything in `daemon.py`) actually makes or controls. The
        property this test can honestly pin is narrower and still
        meaningful: `--test-config` itself performs no migration, no
        schema write, and leaves no file behind that a normal, explicit
        `dispose_all()` -- the same cleanup `_spawn` already performs at
        every real fork point -- does not immediately clean up.
        """
        db_dir = tmp_path / 'dbonly'
        db_dir.mkdir()
        db_path = db_dir / 'config.db'
        resolved = resolve_config_path(str(db_path))
        import_yaml(config_yaml_path, resolved)
        dispose_all()  # simulates the importer process having exited

        # The importer leaves `<db>.writer.lock` behind by design (item
        # 15.4: never deleted); the daemon must add nothing to it.
        before = sorted(p.name for p in db_dir.iterdir())
        assert before == ['config.db', 'config.db.writer.lock']

        with patch('sys.argv', ['daemon', '--test-config', '-c', str(db_path)]):
            with caplog.at_level('INFO'):
                result = main()

        assert result == 0
        assert 'Schema revision:' in caplog.text

        dispose_all()
        after = sorted(p.name for p in db_dir.iterdir())
        assert after == before

    def test_test_config_out_of_date_schema_exits_one(self, tmp_path, caplog):
        """A valid, but never-migrated, SQLite file -- the same state
        `tests/test_zfsbackup_store.py`'s
        `test_empty_never_migrated_database_raises_schema_out_of_date`
        pins directly against `check_schema`. Built with raw `sqlite3`
        (not `import_yaml`, which would migrate it to head) specifically
        so it has a real SQLite header without an `alembic_version` table
        or any application tables -- `check_schema`'s `SchemaOutOfDate`
        case for "no recorded Alembic revision at all".
        """
        db_path = tmp_path / 'config.db'
        conn = sqlite3.connect(str(db_path))
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('CREATE TABLE _placeholder(x)')
        conn.execute('DROP TABLE _placeholder')
        conn.commit()
        conn.close()

        with patch('sys.argv', ['daemon', '--test-config', '-c', str(db_path)]):
            with caplog.at_level('ERROR'):
                result = main()

        assert result == 1
        # Item 10.4: the store's SchemaOutOfDate text is neutral (here the
        # "no recorded revision" variant, which points at `import`, because
        # `migrate` never creates a database) and the daemon appends its own
        # sentence after it.
        assert "has no recorded schema revision (it holds no configuration tables)" in caplog.text
        assert "`zfsbackup-config import`" in caplog.text
        assert (
            "The daemon only checks the schema and never migrates it; run the "
            "command above, then start the daemon again." in caplog.text
        )

    def test_test_config_foreign_database_is_logged_verbatim_and_left_alone(
        self, tmp_path, caplog
    ):
        """Item 10.4: an SQLite file with someone else's tables is
        `NotAConfigDatabase`; the daemon logs the store's text with NO
        appended "never migrates" sentence (it is not an out-of-date
        schema) and does not modify the file (the reader engine no longer
        switches the journal mode)."""
        db_path = tmp_path / 'other.db'
        conn = sqlite3.connect(str(db_path))
        conn.execute('CREATE TABLE orders(id INTEGER PRIMARY KEY, x TEXT)')
        conn.commit()
        conn.close()
        before = db_path.read_bytes()

        with patch('sys.argv', ['daemon', '--test-config', '-c', str(db_path)]):
            with caplog.at_level('ERROR'):
                result = main()

        assert result == 1
        assert "not a zfsbackup config database" in caplog.text
        assert "tables: orders" in caplog.text
        assert "The daemon only checks the schema" not in caplog.text
        assert db_path.read_bytes() == before
        assert before[18:20] == b"\x01\x01"


# ---------------------------------------------------------------------------
# Item 15.2(c): the daemon presence lock, as `daemon._run` uses it.
#
# `BackupDaemon` is replaced with a stub throughout: these test *when* the lock
# is taken and released and what a refusal looks like, not the supervisor. The
# config is loaded for real from an imported SQLite database, so "after the
# config has loaded" is an observable ordering rather than an assumption.
# ---------------------------------------------------------------------------

import argparse as _argparse
import os as _os

#: Any non-zero code that `run()` can return would do (it returns 1 when a
#: worker crash-looped); spelled out so the release tests do not depend on it.
_RUN_FAILED = 1

from zfsbackup import daemonlock as _daemonlock
import zfsbackup.daemon as _daemon_module


@pytest.fixture
def no_hard_exit(mocker):
    """`main()` calls `os._exit` when workers were abandoned; never in-process."""
    return mocker.patch('zfsbackup.daemon._hard_exit')


def _stub_daemon(mocker, run=0, abandoned=(), run_effect=None):
    stub = MagicMock()
    stub.run.return_value = run
    if run_effect is not None:
        stub.run.side_effect = run_effect
    stub.abandoned_workers = list(abandoned)
    factory = mocker.patch('zfsbackup.daemon.BackupDaemon', return_value=stub)
    return factory, stub


def _args(db, **kw):
    ns = dict(config=str(db), dry_run=False, test_config=False, verbose=False)
    ns.update(kw)
    return _argparse.Namespace(**ns)


def _lock_file(resolved):
    return _daemonlock.lock_path(resolved)


class TestDaemonLockInRun:
    def test_daemon_lock_is_held_while_running_and_removed_after_a_clean_return(
        self, sqlite_config_db, mocker, lock_spy, no_hard_exit
    ):
        seen = {}

        def during_run():
            seen['file'] = _lock_file(sqlite_config_db).read_bytes()
            seen['probe'] = _daemonlock.probe(sqlite_config_db)
            return 0

        _stub_daemon(mocker, run_effect=during_run)
        assert _daemon_module._run(_args(sqlite_config_db.path)) == 0

        assert seen['probe'] is not None and seen['probe'].pid == _os.getpid()
        assert seen['file'].startswith(f"{_os.getpid()}\n".encode())
        assert not _lock_file(sqlite_config_db).exists()
        assert _daemonlock.probe(sqlite_config_db) is None
        assert len(lock_spy) == 1 and not lock_spy[0].held

    def test_daemon_lock_file_sits_next_to_the_resolved_database(
        self, sqlite_config_db, mocker, lock_spy
    ):
        _stub_daemon(mocker)
        _daemon_module._run(_args(sqlite_config_db.path))
        assert lock_spy[0].path == Path(str(sqlite_config_db.resolved_path) + '.daemon.pid')

    def test_daemon_lock_is_held_when_the_config_loads_and_before_the_daemon_is_built(
        self, sqlite_config_db, mocker, lock_spy
    ):
        """Lock BEFORE read (item 15.5): a `zfsbackup-config` write plus its
        `probe()` landing between the read and the lock would report "no
        running daemon" for a daemon that then runs the old generation."""
        events = []
        from zfsbackup.runtime_config import load_runtime_config as real_load

        def load(resolved, *args, **kwargs):
            events.append(('load', _daemonlock.probe(resolved)))
            return real_load(resolved, *args, **kwargs)

        mocker.patch('zfsbackup.daemon.load_runtime_config', side_effect=load)
        stub = MagicMock(abandoned_workers=[])
        stub.run.return_value = 0

        def build(config, resolved, *args, **kwargs):
            events.append(('build', _daemonlock.probe(resolved)))
            return stub

        mocker.patch('zfsbackup.daemon.BackupDaemon', side_effect=build)
        _daemon_module._run(_args(sqlite_config_db.path))

        assert [e[0] for e in events] == ['load', 'build']
        assert events[0][1] is not None, 'the lock must be held before the config is read'
        assert events[0][1].pid == _os.getpid()
        assert events[0][1].generation is None, 'generation is unknown until the load returns'
        assert events[1][1] is not None, 'the lock must be held before the daemon is built'
        assert events[1][1].generation == read_config(sqlite_config_db).generation

    def test_daemon_lock_second_daemon_is_refused_with_exit_one_naming_the_holder(
        self, sqlite_config_db, mocker, daemon_lock_holder, caplog, no_hard_exit
    ):
        holder = daemon_lock_holder(sqlite_config_db.path, generation=3)
        factory, stub = _stub_daemon(mocker)
        with caplog.at_level('ERROR'):
            result = _daemon_module._run(_args(sqlite_config_db.path))

        assert result == 1
        assert f"pid {holder.pid}" in caplog.text
        assert 'Refusing to start a second daemon' in caplog.text
        factory.assert_not_called()
        stub.run.assert_not_called()

    def test_daemon_lock_refused_daemon_leaves_the_holders_file_and_lock_alone(
        self, sqlite_config_db, mocker, daemon_lock_holder
    ):
        holder = daemon_lock_holder(sqlite_config_db.path, generation=3)
        _stub_daemon(mocker)
        _daemon_module._run(_args(sqlite_config_db.path))

        assert _lock_file(sqlite_config_db).read_bytes() == f"{holder.pid}\n3\n".encode()
        info = _daemonlock.probe(sqlite_config_db)
        assert info is not None and info.pid == holder.pid

    def test_daemon_lock_second_daemon_through_main_returns_one(
        self, sqlite_config_db, mocker, daemon_lock_holder, caplog
    ):
        holder = daemon_lock_holder(sqlite_config_db.path)
        factory, _ = _stub_daemon(mocker)
        with patch('sys.argv', ['daemon', '-c', str(sqlite_config_db.path)]):
            with caplog.at_level('ERROR'):
                assert main() == 1
        assert f"pid {holder.pid}" in caplog.text
        factory.assert_not_called()

    def test_daemon_lock_second_daemon_in_the_same_process_is_also_refused(
        self, sqlite_config_db, mocker, lock_spy, caplog
    ):
        """`flock` is per open file description, so even a second `_run` in
        this very process cannot slip past the first."""
        outer = _daemonlock.acquire(sqlite_config_db)
        try:
            factory, _ = _stub_daemon(mocker)
            with caplog.at_level('ERROR'):
                assert _daemon_module._run(_args(sqlite_config_db.path)) == 1
            assert f"pid {_os.getpid()}" in caplog.text
            factory.assert_not_called()
            assert outer.held and _lock_file(sqlite_config_db).exists()
        finally:
            outer.release()

    def test_daemon_lock_unopenable_lock_file_exits_one_and_says_so(
        self, sqlite_config_db, mocker, caplog
    ):
        _lock_file(sqlite_config_db).mkdir()
        factory, _ = _stub_daemon(mocker)
        with caplog.at_level('ERROR'):
            result = _daemon_module._run(_args(sqlite_config_db.path))
        assert result == 1
        assert 'Cannot take the daemon lock' in caplog.text
        assert 'Refusing to start a second daemon' not in caplog.text
        factory.assert_not_called()

    def test_daemon_lock_symlink_at_the_lock_path_exits_one(
        self, sqlite_config_db, mocker, tmp_path, caplog
    ):
        victim = tmp_path / 'victim'
        victim.write_text('precious')
        _lock_file(sqlite_config_db).symlink_to(victim)
        factory, _ = _stub_daemon(mocker)
        with caplog.at_level('ERROR'):
            assert _daemon_module._run(_args(sqlite_config_db.path)) == 1
        assert victim.read_text() == 'precious'
        factory.assert_not_called()


class TestDaemonLockTestConfig:
    def test_daemon_lock_test_config_neither_creates_nor_takes_it(
        self, sqlite_config_db, lock_spy
    ):
        with patch('sys.argv', ['daemon', '--test-config', '-c', str(sqlite_config_db.path)]):
            assert main() == 0
        assert lock_spy == []
        assert not _lock_file(sqlite_config_db).exists()

    def test_daemon_lock_test_config_works_while_another_daemon_holds_it(
        self, sqlite_config_db, daemon_lock_holder, lock_spy, caplog
    ):
        holder = daemon_lock_holder(sqlite_config_db.path, generation=5)
        before = _lock_file(sqlite_config_db).read_bytes()
        with patch('sys.argv', ['daemon', '--test-config', '-c', str(sqlite_config_db.path)]):
            with caplog.at_level('INFO'):
                assert main() == 0
        assert 'Configuration is valid!' in caplog.text
        assert 'already running' not in caplog.text
        assert lock_spy == []
        assert _lock_file(sqlite_config_db).read_bytes() == before
        assert _daemonlock.probe(sqlite_config_db).pid == holder.pid

    def test_daemon_lock_test_config_with_an_unopenable_lock_path_still_passes(
        self, sqlite_config_db
    ):
        _lock_file(sqlite_config_db).mkdir()
        with patch('sys.argv', ['daemon', '--test-config', '-c', str(sqlite_config_db.path)]):
            assert main() == 0


class TestDaemonLockOnConfigFailure:
    """Item 15.5: the preflight (`check_config_db`) runs first and creates
    nothing; a refusal there never takes the lock. A refusal AFTER the lock
    was taken (the config would not load) takes it, then `_release_refused`
    removes the pid file and drops the lock, so a refused start leaves
    nothing that `probe()` could mistake for a daemon."""

    def test_lock_is_taken_then_released_when_the_config_will_not_load(
        self, sqlite_config_db, mocker, lock_spy
    ):
        seen = []

        def boom(resolved, *a, **k):
            seen.append(_daemonlock.probe(resolved))
            raise RuntimeError('bad')

        mocker.patch('zfsbackup.daemon.load_runtime_config', side_effect=boom)
        assert _daemon_module._run(_args(sqlite_config_db.path)) == 1
        assert seen and seen[0] is not None, 'the lock was held while the load failed'
        assert len(lock_spy) == 1 and not lock_spy[0].held
        assert not _lock_file(sqlite_config_db).exists()
        assert _daemonlock.probe(sqlite_config_db) is None

    def test_a_failed_real_load_leaves_no_pid_file_and_no_lock(
        self, sqlite_config_db, lock_spy, mocker
    ):
        """No mocking of the load: a database the schema check refuses."""
        import sqlite3

        con = sqlite3.connect(str(sqlite_config_db.resolved_path))
        con.execute('DELETE FROM alembic_version')
        con.commit()
        con.close()
        assert _daemon_module._run(_args(sqlite_config_db.path)) == 1
        assert len(lock_spy) == 1 and not lock_spy[0].held
        assert not _lock_file(sqlite_config_db).exists()
        assert _daemonlock.probe(sqlite_config_db) is None

    def test_a_refusal_after_the_lock_is_released_so_a_retry_can_start(
        self, sqlite_config_db, mocker, lock_spy
    ):
        mocker.patch('zfsbackup.daemon.load_runtime_config', side_effect=KeyError('x'))
        assert _daemon_module._run(_args(sqlite_config_db.path)) == 1
        mocker.stopall()
        again = _daemonlock.acquire(sqlite_config_db)
        try:
            assert again.held
        finally:
            again.release()

    def test_config_path_error_after_the_lock_releases_it(
        self, sqlite_config_db, mocker, lock_spy
    ):
        mocker.patch(
            'zfsbackup.daemon.load_runtime_config',
            side_effect=ConfigPathError('gone between preflight and load'),
        )
        assert _daemon_module._run(_args(sqlite_config_db.path)) == 1
        assert not lock_spy[0].held and not _lock_file(sqlite_config_db).exists()

    def test_the_preflight_runs_before_the_lock(self, sqlite_config_db, mocker, lock_spy):
        order = []
        real = _daemon_module.check_config_db
        mocker.patch(
            'zfsbackup.daemon.check_config_db',
            side_effect=lambda r: (order.append(('check', _lock_file(r).exists())), real(r))[1],
        )
        _stub_daemon(mocker)
        _daemon_module._run(_args(sqlite_config_db.path))
        assert order == [('check', False)]

    def test_a_missing_database_takes_no_lock_and_creates_no_lock_file(self, tmp_path, lock_spy):
        db = tmp_path / 'missing.db'
        assert _daemon_module._run(_args(db)) == 1
        assert lock_spy == []
        assert not Path(str(db) + '.daemon.pid').exists()

    def test_a_missing_directory_creates_nothing(self, tmp_path, lock_spy):
        db = tmp_path / 'nodir' / 'missing.db'
        assert _daemon_module._run(_args(db)) == 1
        assert lock_spy == []
        assert list(tmp_path.iterdir()) == []

    def test_a_yaml_target_takes_no_lock(self, config_yaml_path, lock_spy):
        assert _daemon_module._run(_args(config_yaml_path)) == 1
        assert lock_spy == []
        assert not Path(str(config_yaml_path) + '.daemon.pid').exists()

    def test_a_missing_database_beside_a_held_lock_does_not_touch_the_holders_file(
        self, sqlite_config_db, daemon_lock_holder, lock_spy
    ):
        """The preflight refuses before `acquire` could even report the
        holder; the holder's file is untouched."""
        holder = daemon_lock_holder(sqlite_config_db.path, generation=4)
        sqlite_config_db.resolved_path.unlink()
        before = _lock_file(sqlite_config_db).read_bytes()
        assert _daemon_module._run(_args(sqlite_config_db.path)) == 1
        assert lock_spy == []
        assert _lock_file(sqlite_config_db).read_bytes() == before
        assert _daemonlock.probe(sqlite_config_db).pid == holder.pid


class TestStartupWindow:
    """Mutation counts (15.5, scratch copy; tests failing across
    test_zfsbackup_daemon.py / _daemon_resilience.py / _daemonlock.py): the lock
    taken after the load instead of before it 13; `_release_refused` a no-op 11;
    the `check_config_db` preflight removed 5; the preflight/lock also run for
    `--test-config` 1 / 8; `set_generation` dropped 4.

    The window item 15.5 closed: the lock is held, the config is not yet
    read. A `zfsbackup-config` write landing here must find a daemon (not
    "no running daemon detected") and must be told that its generation is
    unknown, because it is the generation the daemon is ABOUT to load."""

    @staticmethod
    def _blocked_load(mocker):
        import threading

        from zfsbackup.runtime_config import load_runtime_config as real_load

        entered, release = threading.Event(), threading.Event()

        def blocked(resolved, *args, **kwargs):
            entered.set()
            assert release.wait(30), 'the test never released the load'
            return real_load(resolved, *args, **kwargs)

        mocker.patch('zfsbackup.daemon.load_runtime_config', side_effect=blocked)
        return entered, release

    def _run_in_thread(self, db):
        import threading

        box = {}

        def target():
            try:
                box['code'] = _daemon_module._run(_args(db))
            except BaseException as exc:  # pragma: no cover - reported below
                box['error'] = exc

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        return thread, box

    def test_probe_during_the_load_finds_a_daemon_with_no_generation(
        self, sqlite_config_db, mocker, lock_spy
    ):
        entered, release = self._blocked_load(mocker)
        _stub_daemon(mocker)
        thread, box = self._run_in_thread(sqlite_config_db.path)
        try:
            assert entered.wait(30)
            info = _daemonlock.probe(sqlite_config_db)
            assert info == _daemonlock.DaemonInfo(pid=_os.getpid(), generation=None)
            assert _lock_file(sqlite_config_db).read_bytes() == f"{_os.getpid()}\n\n".encode()
        finally:
            release.set()
            thread.join(30)
        assert box == {'code': 0}
        assert _daemonlock.probe(sqlite_config_db) is None

    def test_the_generation_is_recorded_once_the_load_returns(
        self, sqlite_config_db, mocker, lock_spy
    ):
        seen = {}

        def during_run():
            seen['info'] = _daemonlock.probe(sqlite_config_db)
            return 0

        _stub_daemon(mocker, run_effect=during_run)
        assert _daemon_module._run(_args(sqlite_config_db.path)) == 0
        assert seen['info'].generation == read_config(sqlite_config_db).generation

    def test_a_cli_import_during_the_load_reports_an_unknown_generation(
        self, sqlite_config_db, mocker, lock_spy, config_yaml_path
    ):
        from click.testing import CliRunner

        from zfsbackup.cli.main import cli

        entered, release = self._blocked_load(mocker)
        _stub_daemon(mocker)
        thread, box = self._run_in_thread(sqlite_config_db.path)
        try:
            assert entered.wait(30)
            result = CliRunner().invoke(
                cli,
                ['import', str(config_yaml_path), '-c', str(sqlite_config_db.path)],
                prog_name='zfsbackup-config',
            )
        finally:
            release.set()
            thread.join(30)
        assert result.exit_code == 0, result.output
        last = result.stdout.splitlines()[-1]
        assert last.startswith('config updated (generation ')
        assert f'running daemon pid {_os.getpid()} is on an unknown generation' in last
        assert 'will not apply this until restarted' in last
        assert 'no running daemon detected' not in result.stdout
        assert box == {'code': 0}


class TestTestConfigSkipsPreflightAndLock:
    def test_test_config_runs_no_preflight_and_takes_no_lock(
        self, sqlite_config_db, mocker, lock_spy
    ):
        pre = mocker.patch('zfsbackup.daemon.check_config_db')
        assert _daemon_module._run(_args(sqlite_config_db.path, test_config=True)) == 0
        pre.assert_not_called()
        assert lock_spy == []
        assert not _lock_file(sqlite_config_db).exists()

    def test_test_config_runs_beside_a_held_lock_and_leaves_it_alone(
        self, sqlite_config_db, daemon_lock_holder, lock_spy
    ):
        holder = daemon_lock_holder(sqlite_config_db.path, generation=9)
        before = _lock_file(sqlite_config_db).read_bytes()
        assert _daemon_module._run(_args(sqlite_config_db.path, test_config=True)) == 0
        assert lock_spy == []
        assert _lock_file(sqlite_config_db).read_bytes() == before
        assert _daemonlock.probe(sqlite_config_db) == _daemonlock.DaemonInfo(holder.pid, 9)

    def test_test_config_on_a_missing_database_is_still_refused_and_creates_nothing(
        self, tmp_path, lock_spy
    ):
        db = tmp_path / 'missing.db'
        assert _daemon_module._run(_args(db, test_config=True)) == 1
        assert lock_spy == []
        assert list(tmp_path.iterdir()) == []


class TestDaemonLockRelease:
    def test_daemon_lock_released_on_a_clean_nonzero_return(
        self, sqlite_config_db, mocker, lock_spy
    ):
        """`run()` returning a failure code (a crash-looped worker) is a
        normal return: the supervisor has shut its workers down and the file
        goes."""
        _stub_daemon(mocker, run=_RUN_FAILED)
        assert _daemon_module._run(_args(sqlite_config_db.path)) == _RUN_FAILED
        assert not _lock_file(sqlite_config_db).exists()
        assert not lock_spy[0].held

    def test_daemon_lock_released_through_main(self, sqlite_config_db, mocker, lock_spy):
        _stub_daemon(mocker)
        with patch('sys.argv', ['daemon', '-c', str(sqlite_config_db.path)]):
            assert main() == 0
        assert not _lock_file(sqlite_config_db).exists()
        assert not lock_spy[0].held

    def test_daemon_lock_kept_when_workers_were_abandoned(
        self, sqlite_config_db, mocker, lock_spy
    ):
        """A worker that survived SIGKILL still holds the (fork-inherited)
        lock, correctly: it is still running. Do not delete the file out from
        under it."""
        _stub_daemon(mocker, run=0, abandoned=['snapshot'])
        collected = []
        assert _daemon_module._run(_args(sqlite_config_db.path), collected) == 0
        assert collected == ['snapshot']
        assert _lock_file(sqlite_config_db).exists()
        assert lock_spy[0].held, 'must not have been released'

    def test_daemon_lock_kept_when_workers_were_abandoned_through_main(
        self, sqlite_config_db, mocker, lock_spy, no_hard_exit
    ):
        _stub_daemon(mocker, run=0, abandoned=['pruning'])
        with patch('sys.argv', ['daemon', '-c', str(sqlite_config_db.path)]):
            main()
        no_hard_exit.assert_called_once()
        assert _lock_file(sqlite_config_db).exists()
        assert lock_spy[0].held

    def test_daemon_lock_kept_when_run_raises(self, sqlite_config_db, mocker, lock_spy):
        _stub_daemon(mocker, run_effect=RuntimeError('poll loop died'))
        assert _daemon_module._run(_args(sqlite_config_db.path)) == 1
        assert _lock_file(sqlite_config_db).exists()
        assert lock_spy[0].held

    def test_daemon_lock_kept_when_the_daemon_cannot_be_built(
        self, sqlite_config_db, mocker, lock_spy
    ):
        mocker.patch('zfsbackup.daemon.BackupDaemon', side_effect=OSError('EAGAIN'))
        assert _daemon_module._run(_args(sqlite_config_db.path)) == 1
        assert _lock_file(sqlite_config_db).exists()
        assert lock_spy[0].held

    def test_daemon_lock_kept_when_run_raises_and_workers_were_abandoned(
        self, sqlite_config_db, mocker, lock_spy
    ):
        _stub_daemon(mocker, run_effect=RuntimeError('x'), abandoned=['api'])
        collected = []
        assert _daemon_module._run(_args(sqlite_config_db.path), collected) == 1
        assert collected == ['api']
        assert lock_spy[0].held

    def test_daemon_lock_stale_file_after_a_crash_does_not_block_a_restart(
        self, sqlite_config_db, mocker, lock_spy
    ):
        """Crash leaves the file; the kernel drops the lock when the process
        exits (simulated by closing the descriptor); the next daemon starts."""
        _stub_daemon(mocker, run_effect=RuntimeError('crash'))
        assert _daemon_module._run(_args(sqlite_config_db.path)) == 1
        crashed = lock_spy[0]
        stale = _lock_file(sqlite_config_db).read_bytes()
        assert stale.startswith(f"{_os.getpid()}\n".encode())
        _os.close(crashed._fd)
        crashed._fd = None
        assert _daemonlock.probe(sqlite_config_db) is None

        _stub_daemon(mocker, run=0)
        assert _daemon_module._run(_args(sqlite_config_db.path)) == 0
        assert not _lock_file(sqlite_config_db).exists()

    def test_daemon_lock_survives_a_run_that_leaves_it_held_until_the_process_exits(
        self, sqlite_config_db, mocker, lock_spy
    ):
        """While the (crashed-run) process lives, a second daemon is refused."""
        _stub_daemon(mocker, run_effect=RuntimeError('crash'))
        _daemon_module._run(_args(sqlite_config_db.path))
        factory, _ = _stub_daemon(mocker)
        assert _daemon_module._run(_args(sqlite_config_db.path)) == 1
        factory.assert_not_called()


# ---------------------------------------------------------------------------
# Item 15.2(a)/(b): one read of the config database, one config object for
# every worker. The supervisor loads the configuration once, logs the
# generation and schema revision it read, applies --dry-run in memory, and
# hands the same detached BackupConfig to every worker at startup and at every
# crash restart. Workers never open the database.
# ---------------------------------------------------------------------------

import logging as _logging
from sqlalchemy.exc import DatabaseError, OperationalError

import multiprocessing.process as _mp_process

from _config_handoff import (
    DbTouched,
    RecordingSnapshotWorker,
    config_signature,
    literals_in,
    read_records,
)
from zfsbackup.config.store import (
    ConfigPathError,
    NotAConfigDatabase,
    SchemaOutOfDate,
    db as _store_db,
    read_config,
)
from zfsbackup.runtime_config import load_runtime_config

_START_METHODS = [
    pytest.param(
        m,
        marks=pytest.mark.skipif(
            m not in multiprocessing.get_all_start_methods(),
            reason=f"{m!r} start method unavailable on this platform",
        ),
    )
    for m in ("fork", "spawn", "forkserver")
]

_POPEN_MODULE = {
    'fork': 'popen_fork',
    'spawn': 'popen_spawn_posix',
    'forkserver': 'popen_forkserver',
}

_OTHER_YAML = (
    "snapshot_prefix: other\n"
    "datasets:\n"
    "  - name: pool/replaced\n"
    "    frequency: 7m\n"
)


def _bump_generation(resolved, yaml_path, times=2):
    for _ in range(times):
        import_yaml(yaml_path, resolved)
    dispose_all()
    return read_config(resolved).generation


@pytest.mark.unit
class TestRunRefusesBadConfigBeforeAnyWorker:
    """A configuration the supervisor cannot read is refused in `_run`, with
    exit 1, after the presence lock is taken and released again, and before any worker exists.
    That is now the ONLY place such a fault can surface -- workers are handed
    an already-loaded config and have no config-error exit of their own.

    Mutation-checked (scratch copy): deleting `_run`'s generic `except
    Exception` arm (so only `ConfigPathError`/`SchemaError` are caught) fails
    17 tests: 13 here (the four exception kinds outside those two, times the
    three tests that use `failing_load`, plus the plain-DatabaseError test)
    and 4 elsewhere."""

    @staticmethod
    def _operational():
        return OperationalError('SELECT 1', {}, sqlite3.OperationalError('database is locked'))

    @staticmethod
    def _database_error():
        return DatabaseError('SELECT 1', {}, sqlite3.DatabaseError('file is not a database'))

    @pytest.fixture(params=[
        ('config_path', lambda: ConfigPathError('no such database')),
        ('schema_stale', lambda: SchemaOutOfDate('stale schema')),
        ('foreign_db', lambda: NotAConfigDatabase('not a zfsbackup config database')),
        ('operational', lambda: TestRunRefusesBadConfigBeforeAnyWorker._operational()),
        ('damaged', lambda: TestRunRefusesBadConfigBeforeAnyWorker._database_error()),
        ('no_datasets', lambda: ValueError('No datasets configured')),
        ('mapper_bug', lambda: KeyError('surprise')),
    ], ids=lambda p: p[0])
    def failing_load(self, request, mocker):
        kind, make = request.param
        mocker.patch('zfsbackup.daemon.load_runtime_config', side_effect=make())
        return kind

    def test_exit_one_and_no_daemon_no_lock_no_worker(
        self, sqlite_config_db, failing_load, mocker, lock_spy
    ):
        factory = mocker.patch('zfsbackup.daemon.BackupDaemon')
        starts = mocker.patch.object(_mp_process.BaseProcess, 'start')
        for cls in ('SnapshotWorker', 'PruningWorker', 'ApiWorker', 'RemoteBackupWorker'):
            mocker.patch(f'zfsbackup.daemon.{cls}')

        assert _daemon_module._run(_args(sqlite_config_db.path)) == 1

        factory.assert_not_called()
        starts.assert_not_called()
        # The lock WAS taken (it precedes the read, item 15.5) and was then
        # released by `_release_refused`: nothing is left behind.
        assert len(lock_spy) == 1 and not lock_spy[0].held
        assert not _lock_file(sqlite_config_db).exists()
        assert _daemonlock.probe(sqlite_config_db) is None
        for cls in ('SnapshotWorker', 'PruningWorker', 'ApiWorker', 'RemoteBackupWorker'):
            getattr(_daemon_module, cls).assert_not_called()

    def test_through_main_returns_one(self, sqlite_config_db, failing_load, mocker):
        factory = mocker.patch('zfsbackup.daemon.BackupDaemon')
        with patch('sys.argv', ['daemon', '-c', str(sqlite_config_db.path)]):
            assert main() == 1
        factory.assert_not_called()

    def test_traceback_only_for_the_undiagnosed(
        self, sqlite_config_db, failing_load, mocker, caplog
    ):
        mocker.patch('zfsbackup.config.store.messages.diagnose_open_failure', return_value=None)
        with caplog.at_level(_logging.ERROR):
            _daemon_module._run(_args(sqlite_config_db.path))
        (record,) = [r for r in caplog.records if r.name == 'zfsbackup.daemon']
        has_traceback = bool(record.exc_info)
        # ConfigPathError/SchemaError are operator-grade verbatim; an
        # OperationalError and a plain DatabaseError are diagnosed by
        # `config_error_message`; a bare ValueError/KeyError is not, and
        # needs its traceback to be diagnosable at all.
        assert has_traceback == (failing_load in ('no_datasets', 'mapper_bug'))

    def test_a_plain_database_error_logs_a_damaged_database_line_without_a_traceback(
        self, sqlite_config_db, mocker, caplog
    ):
        mocker.patch('zfsbackup.daemon.load_runtime_config', side_effect=self._database_error())
        with caplog.at_level(_logging.ERROR):
            assert _daemon_module._run(_args(sqlite_config_db.path)) == 1
        (record,) = [r for r in caplog.records if r.name == 'zfsbackup.daemon']
        assert not record.exc_info
        assert 'damaged or not an SQLite database' in record.getMessage()
        assert 'file is not a database' in record.getMessage()
        assert 'Traceback' not in caplog.text


@pytest.mark.unit
class TestRunReadsOnceAndReportsTheGeneration:
    """`_run` reads the database once, says which generation and schema
    revision it read, and carries nothing from the read into the forks."""

    @pytest.fixture
    def bumped(self, sqlite_config_db, config_yaml_path):
        """`sqlite_config_db` after two more imports, so its generation is
        neither 0 nor 1 and a hard-coded value cannot satisfy a test."""
        generation = _bump_generation(sqlite_config_db, config_yaml_path)
        assert generation >= 2
        return sqlite_config_db, generation

    def test_logs_generation_and_schema_revision_once(self, bumped, mocker, caplog):
        resolved, generation = bumped
        _stub_daemon(mocker)
        with caplog.at_level(_logging.INFO, logger='zfsbackup.daemon'):
            _daemon_module._run(_args(resolved.path))

        lines = [r.getMessage() for r in caplog.records if 'Configuration generation' in r.getMessage()]
        assert lines == [f'Configuration generation {generation}, schema revision {EXPECTED_HEAD}']

    def test_the_log_line_comes_after_the_source_and_before_the_daemon_is_built(
        self, bumped, mocker, caplog
    ):
        resolved, _ = bumped
        order = []
        stub = MagicMock(abandoned_workers=[])
        stub.run.return_value = 0

        def build(*a, **k):
            order.append('build')
            return stub

        mocker.patch('zfsbackup.daemon.BackupDaemon', side_effect=build)
        with caplog.at_level(_logging.INFO, logger='zfsbackup.daemon'):
            _daemon_module._run(_args(resolved.path))
        text = [r.getMessage() for r in caplog.records]
        i_source = next(i for i, m in enumerate(text) if m.startswith('Loading configuration from'))
        i_gen = next(i for i, m in enumerate(text) if m.startswith('Configuration generation'))
        assert i_source < i_gen and order == ['build']

    def test_the_logged_values_are_the_snapshots_not_a_second_read(self, sqlite_config_db, mocker, caplog):
        """The generation in the log line is whatever `load_runtime_config`
        returned -- the supervisor does not consult the database again."""
        config = BackupConfig(datasets=[DatasetConfig(name='pool/data')])
        mocker.patch(
            'zfsbackup.daemon.load_runtime_config',
            return_value=ConfigSnapshot(config, 9876, 'cafef00d'),
        )
        factory, _ = _stub_daemon(mocker)
        with caplog.at_level(_logging.INFO, logger='zfsbackup.daemon'):
            _daemon_module._run(_args(sqlite_config_db.path))
        assert 'Configuration generation 9876, schema revision cafef00d' in caplog.text
        assert factory.call_args.args[0] is config

    def test_the_lock_file_records_the_generation_the_workers_will_run(
        self, bumped, mocker, lock_spy
    ):
        resolved, generation = bumped
        seen = {}

        def during_run():
            seen['file'] = _lock_file(resolved).read_bytes()
            seen['probe'] = _daemonlock.probe(resolved)
            return 0

        _stub_daemon(mocker, run_effect=during_run)
        _daemon_module._run(_args(resolved.path))

        assert seen['file'] == f"{_os.getpid()}\n{generation}\n".encode()
        assert seen['probe'] == _daemonlock.DaemonInfo(_os.getpid(), generation)

    def test_the_generation_is_recorded_after_the_lock_is_taken(self, bumped, mocker, lock_spy):
        """`acquire(resolved)` is called without a generation and
        `set_generation` fills it in; both happen before the daemon is built."""
        resolved, generation = bumped
        events = []
        real_set = _daemonlock.DaemonLock.set_generation

        def set_generation(self, value):
            events.append(('set_generation', value, _lock_file(resolved).exists()))
            return real_set(self, value)

        mocker.patch.object(_daemonlock.DaemonLock, 'set_generation', set_generation)
        stub = MagicMock(abandoned_workers=[])
        stub.run.return_value = 0
        mocker.patch(
            'zfsbackup.daemon.BackupDaemon',
            side_effect=lambda *a, **k: events.append(('build',)) or stub,
        )
        _daemon_module._run(_args(resolved.path))
        assert events == [('set_generation', generation, True), ('build',)]

    def test_pooled_reader_is_disposed_straight_after_the_read(self, bumped, mocker):
        """The read leaves the reader engine's pooled connection open; it is
        closed before the long dataset verification and well before the
        first fork. Observed from the moment the daemon is built: no engine
        is cached in this process."""
        resolved, _ = bumped
        events = []
        real_dispose = _daemon_module.dispose_all
        real_load = _daemon_module.load_runtime_config

        def load(r):
            snap = real_load(r)
            events.append(('load', len(_store_db._ENGINES)))
            return snap

        def dispose():
            events.append(('dispose_all',))
            real_dispose()

        mocker.patch('zfsbackup.daemon.load_runtime_config', side_effect=load)
        mocker.patch('zfsbackup.daemon.dispose_all', side_effect=dispose)
        stub = MagicMock(abandoned_workers=[])
        stub.run.return_value = 0

        def build(*a, **k):
            events.append(('build', len(_store_db._ENGINES)))
            return stub

        mocker.patch('zfsbackup.daemon.BackupDaemon', side_effect=build)
        _daemon_module._run(_args(resolved.path))

        assert events[0] == ('load', 1)  # the read really did leave an engine behind
        assert events[1] == ('dispose_all',)
        assert events[2] == ('build', 0)

    def test_dry_run_flag_is_applied_to_the_config_the_daemon_receives(
        self, bumped, mocker
    ):
        resolved, generation = bumped
        factory, _ = _stub_daemon(mocker)
        _daemon_module._run(_args(resolved.path, dry_run=True))
        assert factory.call_args.args[0].dry_run is True
        # `--dry-run` is a property of the run, never written back.
        dispose_all()
        after = read_config(resolved)
        assert after.config.dry_run is False and after.generation == generation

    def test_without_the_flag_the_stored_dry_run_value_is_kept(self, bumped, mocker):
        resolved, _ = bumped
        factory, _ = _stub_daemon(mocker)
        _daemon_module._run(_args(resolved.path, dry_run=False))
        assert factory.call_args.args[0].dry_run is False

    def test_the_flag_only_adds_dry_run_and_never_clears_a_stored_one(self, tmp_path, mocker):
        src = tmp_path / 'dry.yaml'
        src.write_text('dry_run: true\ndatasets:\n  - name: pool/data\n')
        resolved = resolve_config_path(str(tmp_path / 'dry.db'))
        import_yaml(src, resolved)
        factory, _ = _stub_daemon(mocker)
        _daemon_module._run(_args(resolved.path, dry_run=False))
        assert factory.call_args.args[0].dry_run is True

    def test_dry_run_is_applied_before_the_config_reaches_the_daemon(self, bumped, mocker):
        """Not set after construction: `BackupDaemon` is built from a config
        that already has it, because workers are created from `daemon.config`."""
        resolved, _ = bumped
        seen = []

        def build(config, *a, **k):
            seen.append(config.dry_run)
            stub = MagicMock(abandoned_workers=[])
            stub.run.return_value = 0
            return stub

        mocker.patch('zfsbackup.daemon.BackupDaemon', side_effect=build)
        _daemon_module._run(_args(resolved.path, dry_run=True))
        assert seen == [True]

    def test_the_daemon_is_given_verbose_and_the_resolved_database(self, bumped, mocker):
        resolved, _ = bumped
        factory, _ = _stub_daemon(mocker)
        _daemon_module._run(_args(resolved.path, verbose=True))
        (config, passed_resolved), kwargs = factory.call_args
        assert passed_resolved.resolved_path == resolved.resolved_path
        assert kwargs == {'verbose': True}


@pytest.mark.unit
class TestTestConfigReportsTheSchemaRevision:
    def test_prints_the_revision_the_check_inside_the_snapshot_returned(
        self, sqlite_config_db, caplog
    ):
        with patch('sys.argv', ['daemon', '--test-config', '-c', str(sqlite_config_db.path)]):
            with caplog.at_level(_logging.INFO, logger='zfsbackup.daemon'):
                assert main() == 0
        assert f'Schema revision: {EXPECTED_HEAD}' in caplog.text
        assert 'Configuration is valid!' in caplog.text

    def test_prints_whatever_the_snapshot_says_not_a_constant(self, sqlite_config_db, mocker, caplog):
        config = BackupConfig(datasets=[DatasetConfig(name='pool/data')])
        mocker.patch(
            'zfsbackup.daemon.load_runtime_config',
            return_value=ConfigSnapshot(config, 3, 'abc123'),
        )
        with patch('sys.argv', ['daemon', '--test-config', '-c', str(sqlite_config_db.path)]):
            with caplog.at_level(_logging.INFO, logger='zfsbackup.daemon'):
                assert main() == 0
        assert 'Schema revision: abc123' in caplog.text
        assert 'Configuration generation 3, schema revision abc123' in caplog.text

    @staticmethod
    def _dump(resolved):
        """The database's logical content (a WAL checkpoint at close may
        legitimately touch header bytes, so compare rows, not file bytes)."""
        conn = sqlite3.connect(f'file:{resolved.resolved_path}?mode=ro', uri=True)
        try:
            return list(conn.iterdump())
        finally:
            conn.close()

    def test_modifies_nothing_and_takes_no_lock(self, sqlite_config_db, lock_spy):
        dispose_all()
        before = self._dump(sqlite_config_db)
        files = sorted(p.name for p in sqlite_config_db.resolved_path.parent.iterdir())
        with patch('sys.argv', ['daemon', '--test-config', '-c', str(sqlite_config_db.path)]):
            assert main() == 0
        dispose_all()
        assert lock_spy == []
        assert self._dump(sqlite_config_db) == before
        assert sorted(p.name for p in sqlite_config_db.resolved_path.parent.iterdir()) == files

    def test_applies_dry_run_before_reporting(self, sqlite_config_db, mocker):
        seen = []
        mocker.patch.object(
            _daemon_module.logger, 'info',
            side_effect=lambda m, *a, **k: seen.append(m),
        )
        with patch('sys.argv', ['daemon', '--test-config', '-d', '-c', str(sqlite_config_db.path)]):
            assert main() == 0
        assert any(str(m).startswith('Schema revision:') for m in seen)


@pytest.mark.unit
class TestWorkerConfigHandoff:
    """Every worker, at startup and at every crash restart, is constructed
    with the supervisor's own `BackupConfig` object, its stop flag and the
    verbose flag -- `cls(self.config, self._stop_event, self.verbose)` -- and
    nothing in the daemon reaches back to the database afterwards.

    Mutation-checked (scratch copy; failing tests in this file): `_new_worker`
    passing `self.resolved_config` (the old first argument) 15; re-reading the
    config with `load_runtime_config(self.resolved_config).config` 17;
    deleting `_spawn`'s `dispose_all()` 5 (3 here, 2 in `TestSpawnOrdering`);
    the worker deep-copying its config 15 (here and in the worker files)."""

    NAMES = ['snapshot', 'pruning', 'api', 'remote']

    @pytest.fixture
    def supervisor(self, literal_config_db):
        snapshot = load_runtime_config(literal_config_db)
        dispose_all()
        snapshot.config.dry_run = True
        daemon = BackupDaemon(snapshot.config, literal_config_db, verbose=True)
        return daemon, snapshot

    @pytest.mark.parametrize('name', NAMES)
    def test_new_worker_is_handed_the_supervisors_config_stop_flag_and_verbose(
        self, supervisor, name
    ):
        daemon, snapshot = supervisor
        worker = daemon._new_worker(name)
        assert worker.config is snapshot.config
        assert worker.stop_event is daemon._stop_event
        assert worker.verbose is True

    def test_all_four_workers_share_one_config_object(self, supervisor):
        daemon, _ = supervisor
        assert daemon._active_worker_names() == self.NAMES
        workers = [daemon._new_worker(n) for n in self.NAMES]
        assert len({id(w.config) for w in workers}) == 1

    @pytest.mark.parametrize('name, cls', [
        ('snapshot', 'SnapshotWorker'), ('pruning', 'PruningWorker'),
        ('api', 'ApiWorker'), ('remote', 'RemoteBackupWorker'),
    ])
    def test_the_constructor_call_is_exactly_config_stop_verbose(
        self, supervisor, mocker, name, cls
    ):
        daemon, snapshot = supervisor
        factory = mocker.patch(f'zfsbackup.daemon.{cls}')
        daemon._new_worker(name)
        factory.assert_called_once_with(snapshot.config, daemon._stop_event, True)

    def test_a_restart_is_constructed_from_the_same_config_object(self, supervisor, mocker):
        daemon, snapshot = supervisor
        mocker.patch.object(_mp_process.BaseProcess, 'start')
        mocker.patch('zfsbackup.daemon.dispose_all')
        clock = _fake_clock(daemon, mocker)
        configs = []
        real = _daemon_module.SnapshotWorker

        def spy(config, stop, verbose):
            configs.append(config)
            return real(config, stop, verbose)

        mocker.patch('zfsbackup.daemon.SnapshotWorker', side_effect=spy)
        daemon._spawn('snapshot')
        for _ in range(3):  # three consecutive crashes, three restarts
            worker = daemon._workers['snapshot']
            mocker.patch.object(worker, 'is_alive', return_value=False)
            daemon._check_workers()
            clock[0] += 3600
            daemon._check_workers()

        assert len(configs) == 4
        assert all(c is snapshot.config for c in configs)
        assert daemon._workers['snapshot'].config is snapshot.config

    def test_a_respawned_worker_keeps_the_supervisors_generation_after_a_reimport(
        self, supervisor, literal_config_db, tmp_path, mocker
    ):
        """The database changes under a running daemon (a CLI write
        or a re-import bumps the generation); a crashed worker is restarted
        on the configuration the daemon started with, NOT on the new one, and
        without reading the database at all.

        Mutation-checked: making `_new_worker` re-read via
        `load_runtime_config(self.resolved_config).config` fails this test
        (the respawned worker would carry `pool/replaced`)."""
        daemon, snapshot = supervisor
        before = config_signature(snapshot.config)
        started_generation = snapshot.generation

        other = tmp_path / 'other.yaml'
        other.write_text(_OTHER_YAML)
        new_generation = _bump_generation(literal_config_db, other, times=1)
        assert new_generation > started_generation
        assert [d.name for d in read_config(literal_config_db).config.datasets] == ['pool/replaced']
        dispose_all()

        # Armed only now: the DB has changed, and from here on nothing in the
        # supervisor may open it.
        from_db = mocker.patch('sqlite3.connect', side_effect=DbTouched('sqlite3.connect'))
        mocker.patch('zfsbackup.runtime_config.read_config', side_effect=DbTouched('read_config'))
        mocker.patch('zfsbackup.daemon.load_runtime_config', side_effect=DbTouched('load'))
        mocker.patch.object(_mp_process.BaseProcess, 'start')
        recorded = []
        mocker.patch(
            'zfsbackup.daemon.dispose_all',
            side_effect=lambda: recorded.append('dispose_all'),
        )
        clock = _fake_clock(daemon, mocker)

        daemon._spawn('snapshot')
        first = daemon._workers['snapshot']
        mocker.patch.object(first, 'is_alive', return_value=False)
        daemon._check_workers()
        clock[0] += WORKER_RESTART_DELAY
        daemon._check_workers()

        second = daemon._workers['snapshot']
        assert second is not first
        assert second.config is snapshot.config
        assert config_signature(second.config) == before
        assert [d.name for d in second.config.datasets] == ['pool/data', 'pool/other']
        assert from_db.call_count == 0
        assert recorded == ['dispose_all', 'dispose_all']  # once per fork, startup and restart

    def test_the_supervisor_never_opens_the_database_while_starting_and_restarting_workers(
        self, supervisor, forbid_db, mocker
    ):
        daemon, _ = supervisor
        mocker.patch('zfsbackup.daemon.load_runtime_config', side_effect=DbTouched('daemon.load'))
        order = []
        mocker.patch('zfsbackup.daemon.dispose_all', side_effect=lambda: order.append('dispose'))
        mocker.patch.object(
            _mp_process.BaseProcess, 'start',
            side_effect=lambda *a, **k: order.append('fork'),
        )
        clock = _fake_clock(daemon, mocker)

        daemon._start_workers()
        for w in daemon._workers.values():
            mocker.patch.object(w, 'is_alive', return_value=False)
        daemon._check_workers()
        clock[0] += 3600
        daemon._check_workers()

        assert forbid_db == []
        # dispose_all() immediately before EVERY fork: 4 starts, 4 restarts.
        assert order == ['dispose', 'fork'] * 8

    def test_dispose_all_runs_before_each_fork_even_when_nothing_was_read(
        self, supervisor, mocker
    ):
        daemon, _ = supervisor
        order = []
        mocker.patch('zfsbackup.daemon.dispose_all', side_effect=lambda: order.append('dispose'))
        mocker.patch.object(
            _mp_process.BaseProcess, 'start',
            side_effect=lambda *a, **k: order.append('fork'),
        )
        daemon._spawn('pruning')
        daemon._spawn('api')
        assert order == ['dispose', 'fork', 'dispose', 'fork']


def _bind_recording_worker(method, record_dir):
    """A `RecordingSnapshotWorker` factory with the `SnapshotWorker`
    constructor signature `BackupDaemon._new_worker` calls."""
    def factory(config, stop_event, verbose):
        worker = RecordingSnapshotWorker(config, stop_event, verbose)
        worker.start_method = method
        worker.record_dir = str(record_dir)
        return worker
    return factory


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestWorkerConfigHandoffToRealChildren:
    """The handoff through a real OS process under each start method.

    `Duration.__eq__` is a plain `timedelta` comparison, so `config ==
    config` would pass for a config whose `60m` came back as `1h`; the
    comparison here is `config_signature`, which records every `Duration`'s
    literal. Under `spawn` and `forkserver` the config is pickled
    (`Duration.__reduce__` carries the literal); under `fork` it is
    inherited. `dry_run` is applied by the supervisor before the handoff.

    Mutation-checked (scratch copy): removing `Duration.__reduce__` fails 3
    tests here -- the spawn and forkserver parametrizations of the first test
    and the spawn parametrization of the respawn test, with the literals
    collapsed to canonical form -- and 6 in `test_zfsbackup_workers.py`; the
    fork parametrizations are unaffected, which is the point of running all
    three."""

    @pytest.mark.parametrize('method', _START_METHODS)
    def test_child_receives_a_config_identical_in_every_literal_and_dry_run(
        self, literal_config_db, tmp_path, mocker, method
    ):
        snapshot = load_runtime_config(literal_config_db)
        dispose_all()
        snapshot.config.dry_run = True
        daemon = BackupDaemon(snapshot.config, literal_config_db)
        records = tmp_path / 'rec'
        records.mkdir()
        mocker.patch('zfsbackup.daemon.SnapshotWorker', side_effect=_bind_recording_worker(method, records))

        worker = daemon._spawn('snapshot')
        worker.join(120)
        assert worker.exitcode == 0, f'{method} child failed'
        # The override really selected the start method under test.
        assert type(worker._popen).__module__.endswith(_POPEN_MODULE[method])

        (record,) = read_records(records)
        expected = config_signature(snapshot.config)
        assert record['pid'] != _os.getpid()
        assert record['signature'] == expected
        assert record['type'] == 'BackupConfig'
        assert record['signature']['dry_run'] is True
        # Non-vacuous: the compared signature really carries the operator's
        # non-canonical literals, not their canonical forms.
        assert {'600s', '90m', '60m', '4w', '1M', '120m', '52w'} <= set(literals_in(expected))
        assert '1h' not in literals_in(expected)  # 60m must not have become 1h

    @pytest.mark.parametrize('method', [m for m in _START_METHODS if m.values[0] != 'forkserver'])
    def test_a_respawned_child_gets_the_same_config_after_the_database_changed(
        self, literal_config_db, tmp_path, mocker, method
    ):
        snapshot = load_runtime_config(literal_config_db)
        dispose_all()
        snapshot.config.dry_run = True
        daemon = BackupDaemon(snapshot.config, literal_config_db)
        records = tmp_path / 'rec'
        records.mkdir()
        mocker.patch('zfsbackup.daemon.SnapshotWorker', side_effect=_bind_recording_worker(method, records))
        clock = _fake_clock(daemon, mocker)

        first = daemon._spawn('snapshot')
        first.join(120)
        assert first.exitcode == 0
        assert type(first._popen).__module__.endswith(_POPEN_MODULE[method])

        other = tmp_path / 'other.yaml'
        other.write_text(_OTHER_YAML)
        assert _bump_generation(literal_config_db, other, times=1) > snapshot.generation

        daemon._check_workers()  # the child exited: backoff scheduled
        clock[0] += WORKER_RESTART_DELAY
        daemon._check_workers()  # ...and respawned
        second = daemon._workers['snapshot']
        assert second is not first
        second.join(120)
        assert second.exitcode == 0

        records_seen = read_records(records)
        assert len(records_seen) == 2
        expected = config_signature(snapshot.config)
        assert [r['signature'] for r in records_seen] == [expected, expected]
        assert records_seen[0]['pid'] != records_seen[1]['pid']
        names = [d['name'] for d in records_seen[1]['signature']['datasets']]
        assert names == ['pool/data', 'pool/other']  # not 'pool/replaced'

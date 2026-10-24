"""Unit tests for zfsbackup worker classes."""

import copy
import inspect
import logging
import multiprocessing
import pickle
import signal
import pytest
from datetime import timedelta
from unittest.mock import MagicMock

import zfsbackup.workers as workers_module
from _config_handoff import config_signature, literals_in
from zfsbackup.config import BackupConfig, DatasetConfig, Duration, RemoteDatasetConfig
from zfsbackup.config.store import dispose_all
from zfsbackup.runtime_config import load_runtime_config
from zfsbackup.backup_manager import DatasetInfo, DatasetManager
from zfsbackup.stopflag import ReloadEpoch, StopFlag
from zfsbackup.workers import (
    WORKER_EXIT_RELOAD, BaseWorker, SnapshotWorker, PruningWorker, RemoteBackupWorker, ApiWorker,
)

#: The union `workers.StopEvent` actually accepts (see `workers.py`'s own
#: module docstring): a plain `multiprocessing.Event` -- what ~35 existing
#: call sites in this file still construct -- and `StopFlag`, the item-8x
#: replacement every real `BackupDaemon` actually hands its workers today.
#: Before this, no FAST (mocked) test ever drove a worker with a real
#: `StopFlag` at all; only the skippable, `real_zfs`-adjacent
#: `test_zfsbackup_stop_signals.py` did. Parametrizing over both here means
#: a change to `BaseWorker.run`/`ApiWorker.run` that happens to work for
#: `Event`'s specific implementation but not `StopFlag`'s (or vice versa)
#: fails in the always-run mocked suite, not only in a slow, occasionally
#: skipped integration module.
STOP_EVENT_FACTORIES = [multiprocessing.Event, StopFlag]
STOP_EVENT_IDS = ['mp_event', 'stopflag']


class TestSnapshotWorker:
    def test_name_and_daemon(self, worker_config):
        stop_event = multiprocessing.Event()
        worker = SnapshotWorker(worker_config, stop_event)
        assert worker.name == 'snapshot-worker'
        assert worker.daemon is True

    def test_get_interval_returns_check_interval(self, sample_backup_config):
        worker = SnapshotWorker.__new__(SnapshotWorker)
        interval = worker._get_interval(sample_backup_config)
        assert interval == sample_backup_config.check_interval.total_seconds()

    def test_process_dataset_creates_snapshot_when_needed(self, mocker, sample_backup_config):
        manager = MagicMock()
        manager.needs_snapshot.return_value = True
        dsi = MagicMock()
        worker = SnapshotWorker.__new__(SnapshotWorker)
        worker.name = 'snapshot-worker'
        worker._process_dataset(manager, dsi)
        manager.create_snapshot.assert_called_once_with(dsi)

    def test_process_dataset_skips_when_not_needed(self, mocker, sample_backup_config):
        manager = MagicMock()
        manager.needs_snapshot.return_value = False
        dsi = MagicMock()
        worker = SnapshotWorker.__new__(SnapshotWorker)
        worker.name = 'snapshot-worker'
        worker._process_dataset(manager, dsi)
        manager.create_snapshot.assert_not_called()

    def test_run_uses_handed_config_and_processes(self, mocker, worker_config):
        stop_event = multiprocessing.Event()
        stop_event.set()
        worker = SnapshotWorker(worker_config, stop_event)
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        worker.run()


def _three_datasets(dry_run=False) -> BackupConfig:
    return BackupConfig(
        datasets=[DatasetConfig(name=f'pool/{n}') for n in 'abc'], dry_run=dry_run,
    )


class TestSnapshotWorkerBeforeLoop:
    """B1: the dataset existence check and the `org.zfsbackup:config` sync are
    the snapshot worker's first act -- the supervisor makes no `zfs` call.
    Driven inline with the real `DatasetManager` over a mocked `zfs`."""

    @pytest.fixture(autouse=True)
    def _grab_mocker(self, mocker):
        self._mocker = mocker

    @pytest.fixture
    def zfs_mock(self, mocker):
        m = mocker.MagicMock()
        m.exists.return_value = True
        m.list.return_value = []
        mocker.patch('zfsbackup.backup_manager.zfs', m)
        return m

    def _worker(self, config=None, *, epoch=None, born=0):
        stop = StopFlag()
        worker = SnapshotWorker(
            config or _three_datasets(), stop, reload_epoch=epoch, born_epoch=born,
        )
        # Bounds every run to one cycle, so a regression fails an assertion
        # instead of sleeping a whole check interval.
        self._mocker.patch.object(
            worker, '_wait', return_value=workers_module.ExitReason.STOP
        )
        return worker, stop

    def _record(self, mocker, worker, order):
        verify = mocker.patch.object(
            DatasetManager, 'verify_datasets', autospec=True,
            side_effect=lambda self, should_stop=None: order.append('verify'),
        )
        sync = mocker.patch.object(
            DatasetManager, 'sync_all_config_properties', autospec=True,
            side_effect=lambda self, should_stop=None: order.append('sync'),
        )
        return verify, sync

    def test_it_verifies_then_syncs_once_before_the_first_cycle(self, mocker, zfs_mock):
        worker, stop = self._worker()
        order = []
        verify, sync = self._record(mocker, worker, order)

        def process(manager, dsi):
            order.append(f'process:{dsi.name}')
            stop.set()

        mocker.patch.object(worker, '_process_dataset', side_effect=process)
        worker.run()
        assert order == ['verify', 'sync', 'process:pool/a']
        verify.assert_called_once()
        sync.assert_called_once()
        # Both are given the worker's own safe-point check, not the stop flag
        # alone: a reload ends the pre-loop work too.
        for call in (verify, sync):
            assert call.call_args.kwargs['should_stop'] == worker._should_stop

    def test_it_runs_once_per_incarnation_not_once_per_cycle(self, mocker, zfs_mock):
        worker, stop = self._worker()
        order = []
        verify, sync = self._record(mocker, worker, order)
        cycles = []

        def process(manager, dsi):
            cycles.append(dsi.name)
            if len(cycles) == 9:        # three full cycles over three datasets
                stop.set()

        mocker.patch.object(worker, '_process_dataset', side_effect=process)
        mocker.patch.object(worker, '_wait', return_value=None)
        worker.run()
        assert len(cycles) == 9
        assert verify.call_count == 1 and sync.call_count == 1

    def test_only_the_snapshot_worker_does_it(self, mocker, zfs_mock):
        assert PruningWorker._before_loop is BaseWorker._before_loop
        assert RemoteBackupWorker._before_loop is not SnapshotWorker._before_loop
        pruning = PruningWorker(_three_datasets(), StopFlag())
        pruning._before_loop(pruning.config, DatasetManager(pruning.config))
        zfs_mock.exists.assert_not_called()
        zfs_mock.set.assert_not_called()

    def test_a_stop_already_set_checks_and_syncs_nothing(self, mocker, zfs_mock):
        worker, stop = self._worker()
        stop.set()
        sync_one = mocker.patch.object(DatasetManager, 'sync_config_property')
        mocker.patch.object(worker, '_process_dataset')
        worker.run()
        zfs_mock.exists.assert_not_called()
        zfs_mock.list.assert_not_called()
        zfs_mock.set.assert_not_called()
        sync_one.assert_not_called()
        worker._process_dataset.assert_not_called()

    def test_a_reload_already_signalled_checks_and_syncs_nothing_and_exits_for_it(
        self, mocker, zfs_mock
    ):
        epoch = ReloadEpoch()
        worker, stop = self._worker(epoch=epoch, born=0)
        epoch.set(1)                    # the epoch moved before the worker got going
        sync_one = mocker.patch.object(DatasetManager, 'sync_config_property')
        mocker.patch.object(worker, '_process_dataset')
        with pytest.raises(SystemExit) as exc:
            worker.run()
        assert exc.value.code == WORKER_EXIT_RELOAD == 3
        zfs_mock.exists.assert_not_called()
        zfs_mock.set.assert_not_called()
        sync_one.assert_not_called()
        worker._process_dataset.assert_not_called()

    def test_a_stop_after_the_first_dataset_breaks_the_sync(self, mocker, zfs_mock):
        worker, stop = self._worker()
        calls = []

        def sync_one(self_, dsi):
            calls.append(dsi.name)
            stop.set()

        mocker.patch.object(DatasetManager, 'sync_config_property', autospec=True,
                            side_effect=sync_one)
        mocker.patch.object(worker, '_process_dataset')
        worker.run()
        assert calls == ['pool/a']                      # b and c never synced
        assert zfs_mock.exists.call_count == 3          # the verify pass completed
        worker._process_dataset.assert_not_called()     # and no cycle followed

    def test_a_stop_during_the_verify_pass_skips_the_sync_altogether(self, mocker, zfs_mock):
        worker, stop = self._worker()

        def exists(ds):
            stop.set()
            return True

        zfs_mock.exists.side_effect = exists
        sync_one = mocker.patch.object(DatasetManager, 'sync_config_property')
        mocker.patch.object(worker, '_process_dataset')
        worker.run()
        assert zfs_mock.exists.call_count == 1          # broke after the first dataset
        sync_one.assert_not_called()

    def test_a_reload_during_the_sync_breaks_it_and_the_worker_exits_three(
        self, mocker, zfs_mock
    ):
        epoch = ReloadEpoch()
        worker, stop = self._worker(epoch=epoch, born=0)
        calls = []

        def sync_one(self_, dsi):
            calls.append(dsi.name)
            epoch.set(1)                                # the supervisor accepts a reload

        mocker.patch.object(DatasetManager, 'sync_config_property', autospec=True,
                            side_effect=sync_one)
        mocker.patch.object(worker, '_process_dataset')
        with pytest.raises(SystemExit) as exc:
            worker.run()
        assert exc.value.code == WORKER_EXIT_RELOAD
        assert calls == ['pool/a']
        worker._process_dataset.assert_not_called()

    def test_an_exception_in_the_verify_is_logged_and_the_worker_still_cycles(
        self, mocker, zfs_mock, caplog
    ):
        worker, stop = self._worker()
        zfs_mock.exists.side_effect = OSError('pool hung up')
        cycles = []
        mocker.patch.object(
            worker, '_process_dataset',
            side_effect=lambda m, d: (cycles.append(d.name), stop.set()),
        )
        with caplog.at_level(logging.ERROR, logger='zfsbackup.workers'):
            worker.run()
        records = [r for r in caplog.records if 'could not check or sync' in r.getMessage()]
        assert len(records) == 1 and 'pool hung up' in records[0].getMessage()
        assert records[0].exc_info is not None
        assert cycles == ['pool/a']

    def test_an_exception_in_the_sync_is_logged_and_the_worker_still_cycles(
        self, mocker, zfs_mock, caplog
    ):
        worker, stop = self._worker()
        mocker.patch.object(
            DatasetManager, 'sync_all_config_properties',
            side_effect=RuntimeError('zfs exploded'),
        )
        cycles = []
        mocker.patch.object(
            worker, '_process_dataset',
            side_effect=lambda m, d: (cycles.append(d.name), stop.set()),
        )
        with caplog.at_level(logging.ERROR, logger='zfsbackup.workers'):
            worker.run()
        records = [r for r in caplog.records if 'could not check or sync' in r.getMessage()]
        assert len(records) == 1 and 'zfs exploded' in records[0].getMessage()
        assert cycles == ['pool/a']

    def test_a_failing_zfs_set_is_a_warning_inside_the_sync_not_an_error(
        self, mocker, zfs_mock, caplog
    ):
        """`sync_config_property` already swallows a `zfs set` failure, so the
        sync runs through every dataset."""
        worker, stop = self._worker()
        zfs_mock.set.side_effect = OSError('read-only pool')
        mocker.patch.object(
            worker, '_process_dataset', side_effect=lambda m, d: stop.set(),
        )
        with caplog.at_level(logging.WARNING):
            worker.run()
        assert zfs_mock.set.call_count == 3
        assert not [r for r in caplog.records if 'could not check or sync' in r.getMessage()]

    def test_a_dry_run_writes_nothing_and_says_what_it_would_do(
        self, mocker, zfs_mock, caplog
    ):
        worker, stop = self._worker(_three_datasets(dry_run=True))
        mocker.patch.object(
            worker, '_process_dataset', side_effect=lambda m, d: stop.set(),
        )
        with caplog.at_level(logging.INFO):
            worker.run()
        zfs_mock.set.assert_not_called()
        assert zfs_mock.exists.call_count == 3          # existence checks are reads
        dry = [r.getMessage() for r in caplog.records if '[DRY RUN] Would set' in r.getMessage()]
        assert len(dry) == 3

    def test_without_dry_run_each_dataset_gets_its_config_property(self, mocker, zfs_mock):
        """The positive control for the dry-run test above."""
        worker, stop = self._worker()
        mocker.patch.object(
            worker, '_process_dataset', side_effect=lambda m, d: stop.set(),
        )
        worker.run()
        assert [c.args[0].name for c in zfs_mock.set.call_args_list] == [
            'pool/a', 'pool/b', 'pool/c',
        ]

    def test_a_missing_dataset_is_logged_and_does_not_stop_the_worker(
        self, mocker, zfs_mock, caplog
    ):
        worker, stop = self._worker()
        zfs_mock.exists.side_effect = lambda ds: ds.name != 'pool/b'
        mocker.patch.object(
            worker, '_process_dataset', side_effect=lambda m, d: stop.set(),
        )
        with caplog.at_level(logging.ERROR):
            worker.run()
        assert [r.getMessage() for r in caplog.records
                if 'does not exist' in r.getMessage()] == ['Dataset pool/b does not exist']


class TestPruningWorker:
    def test_name_and_daemon(self, worker_config):
        stop_event = multiprocessing.Event()
        worker = PruningWorker(worker_config, stop_event)
        assert worker.name == 'pruning-worker'
        assert worker.daemon is True

    def test_get_interval_returns_prune_interval(self, sample_backup_config):
        # sample_backup_config sets neither check_interval nor
        # prune_interval, so prune_interval is None (unset) and the
        # effective value derives from check_interval's default (5m/300s),
        # not the old hard-coded prune_interval default (1h/3600s).
        worker = PruningWorker.__new__(PruningWorker)
        interval = worker._get_interval(sample_backup_config)
        assert sample_backup_config.prune_interval is None
        assert interval == sample_backup_config.effective_prune_interval.total_seconds()
        assert interval == 300

    def test_process_dataset_calls_prune(self, mocker):
        manager = MagicMock()
        dsi = MagicMock()
        worker = PruningWorker.__new__(PruningWorker)
        worker._process_dataset(manager, dsi)
        manager.prune_snapshots.assert_called_once_with(dsi)

    def test_get_datasets_includes_received(self, worker_config):
        manager = MagicMock()
        local = [MagicMock()]
        received = [MagicMock()]
        manager.datasets = local
        manager.received_datasets.return_value = received
        worker = PruningWorker(worker_config, multiprocessing.Event())     # nothing pending
        result = worker._get_datasets(manager)
        assert local[0] in result
        assert received[0] in result
        assert result == local + received

    def test_the_enumeration_is_given_the_workers_exit_check(self, worker_config):
        """The listing is a safe point: it checks the worker's own "any reason to leave"."""
        manager = MagicMock()
        manager.datasets = []
        manager.received_datasets.return_value = []
        worker = PruningWorker(worker_config, multiprocessing.Event())
        worker._get_datasets(manager)
        manager.received_datasets.assert_called_once_with(should_stop=worker._should_stop)

    def test_the_check_passed_follows_the_stop_flag(self, worker_config):
        stop = multiprocessing.Event()
        manager = MagicMock()
        manager.datasets = []
        seen = []
        manager.received_datasets.side_effect = lambda should_stop=None: (
            seen.append(should_stop()), stop.set(), seen.append(should_stop()), [])[-1]
        PruningWorker(worker_config, stop)._get_datasets(manager)
        assert seen == [False, True]

    def test_a_pending_stop_gives_no_datasets_even_if_the_listing_returned_some(self, worker_config):
        """A listing cut short is partial: the worker must not prune from it (`run()` leaves at
        its next check; the reason is sticky)."""
        stop = multiprocessing.Event()
        stop.set()
        manager = MagicMock()
        manager.datasets = [MagicMock()]
        manager.received_datasets.return_value = [MagicMock()]
        assert PruningWorker(worker_config, stop)._get_datasets(manager) == []

    def test_a_pending_reload_gives_no_datasets(self, worker_config):
        epoch = ReloadEpoch()
        epoch.set(1)
        manager = MagicMock()
        manager.datasets = [MagicMock()]
        manager.received_datasets.return_value = [MagicMock()]
        worker = PruningWorker(worker_config, multiprocessing.Event(), reload_epoch=epoch, born_epoch=0)
        assert worker._get_datasets(manager) == []

    def test_a_reason_arriving_during_the_listing_gives_no_datasets(self, worker_config):
        stop = multiprocessing.Event()
        manager = MagicMock()
        manager.datasets = [MagicMock()]
        partial = [MagicMock()]
        manager.received_datasets.side_effect = lambda should_stop=None: (stop.set(), partial)[1]
        assert PruningWorker(worker_config, stop)._get_datasets(manager) == []

    def test_a_reload_epoch_that_matches_is_no_reason(self, worker_config):
        epoch = ReloadEpoch()
        epoch.set(2)
        manager = MagicMock()
        manager.datasets = []
        manager.received_datasets.return_value = [MagicMock()]
        worker = PruningWorker(worker_config, multiprocessing.Event(), reload_epoch=epoch, born_epoch=2)
        assert len(worker._get_datasets(manager)) == 1

    def test_run_stops_when_event_set(self, worker_config, mocker):
        stop_event = multiprocessing.Event()
        stop_event.set()
        worker = PruningWorker(worker_config, stop_event)
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        worker.run()


class TestRemoteBackupWorker:
    def test_name_and_daemon(self, worker_config):
        stop_event = multiprocessing.Event()
        worker = RemoteBackupWorker(worker_config, stop_event)
        assert worker.name == 'remote-backup-worker'
        assert worker.daemon is True

    def test_get_interval_uses_min_remote_frequency(self):
        config = BackupConfig(
            datasets=[
                DatasetConfig(
                    name='pool/data',
                    frequency=timedelta(hours=1),
                    remote=[
                        RemoteDatasetConfig(destination='a', frequency=timedelta(minutes=30)),
                        RemoteDatasetConfig(destination='b', frequency=timedelta(hours=2)),
                    ],
                ),
            ],
        )
        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        interval = worker._get_interval(config)
        assert interval == timedelta(minutes=30).total_seconds()

    def test_get_interval_falls_back_to_check_interval_when_no_remote(self, sample_backup_config):
        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        interval = worker._get_interval(sample_backup_config)
        assert interval == sample_backup_config.check_interval.total_seconds()

    def test_get_interval_uses_dataset_frequency_when_remote_has_none(self):
        config = BackupConfig(
            datasets=[
                DatasetConfig(
                    name='pool/data',
                    frequency=timedelta(hours=2),
                    remote=[RemoteDatasetConfig(destination='a', frequency=None)],
                ),
            ],
        )
        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        interval = worker._get_interval(config)
        assert interval == timedelta(hours=2).total_seconds()

    def test_process_dataset_skips_when_no_remote_config(self, mocker):
        manager = MagicMock()
        dsi = MagicMock()
        dsi.config.remote = []
        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        worker.name = 'remote-backup-worker'
        worker._remote_manager = MagicMock()
        worker._process_dataset(manager, dsi)
        worker._remote_manager.backup_dataset.assert_not_called()

    def test_process_dataset_skips_when_anchor_fresh(self, mocker):
        manager = MagicMock()
        remote_cfg = MagicMock()
        remote_cfg.destination = 'offsite'
        remote_cfg.frequency = timedelta(hours=1)
        dsi = MagicMock()
        dsi.config.remote = [remote_cfg]
        dsi.config.frequency = timedelta(hours=1)

        anchor_snap = MagicMock()
        anchor_snap.name = 'autosnap_20240101120000'
        anchor_snap.age = timedelta(minutes=30)
        manager.get_anchor.return_value = 'autosnap_20240101120000'
        manager.list_snapshots.return_value = [anchor_snap]
        manager.clock_behind.return_value = False       # the anchor is not future-dated

        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        worker.name = 'remote-backup-worker'
        worker._remote_manager = MagicMock()
        worker._process_dataset(manager, dsi)
        worker._remote_manager.backup_dataset.assert_not_called()

    def test_process_dataset_runs_backup_when_no_anchor(self, mocker):
        manager = MagicMock()
        remote_cfg = MagicMock()
        remote_cfg.destination = 'offsite'
        remote_cfg.frequency = timedelta(hours=1)
        dsi = MagicMock()
        dsi.config.remote = [remote_cfg]
        dsi.config.frequency = timedelta(hours=1)
        manager.get_anchor.return_value = None

        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        worker.name = 'remote-backup-worker'
        worker._remote_manager = MagicMock()
        worker._process_dataset(manager, dsi)
        worker._remote_manager.backup_dataset.assert_called_once()
        args, kwargs = worker._remote_manager.backup_dataset.call_args
        assert args == (dsi, remote_cfg)
        assert set(kwargs) == {'should_stop'} and callable(kwargs['should_stop'])


class TestRemoteWorkerPassesShouldStop:
    """C3: `backup_dataset(dsi, remote_cfg, should_stop=...)` -- the callable is
    "an exit reason is pending" (stop, supervisor gone, or reload), evaluated
    when the remote manager calls it, not when the worker builds it."""

    def _worker(self, epoch=None, born=0):
        stop = StopFlag()
        worker = RemoteBackupWorker(
            BackupConfig(datasets=[DatasetConfig(name='pool/data')]), stop,
            reload_epoch=epoch, born_epoch=born,
        )
        worker._remote_manager = MagicMock()
        return worker, stop

    def _run(self, worker):
        remote_cfg = MagicMock()
        remote_cfg.destination = 'offsite'
        remote_cfg.frequency = timedelta(hours=1)
        dsi = MagicMock()
        dsi.config.remote = [remote_cfg]
        dsi.config.frequency = timedelta(hours=1)
        manager = MagicMock()
        manager.get_anchor.return_value = None
        worker._process_dataset(manager, dsi)
        (args, kwargs), = worker._remote_manager.backup_dataset.call_args_list
        return args, kwargs['should_stop']

    def test_it_is_false_while_nothing_is_pending(self):
        worker, _ = self._worker()
        args, should_stop = self._run(worker)
        assert should_stop() is False

    def test_it_turns_true_when_the_stop_flag_is_raised_after_the_call_was_made(self):
        worker, stop = self._worker()
        _, should_stop = self._run(worker)
        assert should_stop() is False
        stop.set()
        assert should_stop() is True

    def test_it_turns_true_when_a_reload_epoch_moves(self):
        epoch = ReloadEpoch()
        worker, _ = self._worker(epoch=epoch, born=0)
        _, should_stop = self._run(worker)
        assert should_stop() is False
        epoch.set(1)
        assert should_stop() is True

    def test_it_follows_the_exit_reason_exactly(self, mocker):
        worker, _ = self._worker()
        _, should_stop = self._run(worker)
        reason = mocker.patch.object(worker, '_exit_reason', return_value=None)
        assert should_stop() is False
        reason.return_value = workers_module.ExitReason.SUPERVISOR_GONE
        assert should_stop() is True
        assert reason.call_count == 2

    def test_a_stop_already_pending_is_true_at_once(self):
        worker, stop = self._worker()
        stop.set()
        remote_cfg = MagicMock()
        remote_cfg.destination = 'offsite'
        remote_cfg.frequency = timedelta(hours=1)
        dsi = MagicMock()
        dsi.config.remote = [remote_cfg]
        manager = MagicMock()
        manager.get_anchor.return_value = None
        worker._process_dataset(manager, dsi)
        _, kwargs = worker._remote_manager.backup_dataset.call_args
        assert kwargs['should_stop']() is True


class TestApiWorker:
    def test_name_and_daemon(self, worker_config):
        stop_event = multiprocessing.Event()
        worker = ApiWorker(worker_config, stop_event)
        assert worker.name == 'api-worker'
        assert worker.daemon is True


class TestBaseWorkerRun:
    def test_run_uses_the_handed_config_with_dry_run_applied(self, worker_config, mocker):
        """`run()` builds its `DatasetManager` from `self.config` -- the very
        object it was constructed with, `dry_run` already applied by the
        supervisor -- and does not substitute another."""
        stop_event = multiprocessing.Event()
        stop_event.set()
        worker = SnapshotWorker(worker_config, stop_event)
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        seen = []
        real_init = DatasetManager.__init__

        def spy(self_, config, *a, **k):
            seen.append(config)
            return real_init(self_, config, *a, **k)

        mocker.patch.object(DatasetManager, '__init__', spy)
        worker.run()
        assert len(seen) == 1 and seen[0] is worker_config
        assert seen[0].dry_run is True


class TestBaseWorkerLoop:
    """Cover the main dataset-processing loop (lines 74-82) and _get_datasets (line 51)."""

    def test_dataset_exception_logged_and_loop_exits_on_stop(self, mocker):
        stop_event = multiprocessing.Event()
        two_dataset_config = BackupConfig(datasets=[
            DatasetConfig(name='pool/a'),
            DatasetConfig(name='pool/b'),
        ])
        worker = SnapshotWorker(two_dataset_config, stop_event)
        mocker.patch.object(SnapshotWorker, '_get_interval', return_value=0)

        call_count = [0]

        def process_side_effect(manager, dsi):
            call_count[0] += 1
            stop_event.set()      # trigger break on next dataset and exit the while loop
            raise RuntimeError("injected error")

        mocker.patch.object(SnapshotWorker, '_process_dataset', side_effect=process_side_effect)
        worker.run()
        # Only the first dataset is processed; second is skipped by the stop_event break
        assert call_count[0] == 1


class TestRemoteBackupWorkerBeforeLoop:
    def test_before_loop_creates_remote_manager(self, tmp_path):
        from zfsbackup.remote import RemoteBackupManager
        from zfsbackup.config import Destination
        config = BackupConfig(
            datasets=[DatasetConfig(
                name='pool/data',
                remote=[RemoteDatasetConfig(destination='offsite')],
            )],
            destinations={'offsite': Destination(url='https://backup.example.com')},
        )
        manager = MagicMock()
        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        worker._remote_manager = None
        worker.client_id = 'host-1'                      # from the Handoff
        worker.tls_dir = tmp_path / 'db.tls'
        worker._before_loop(config, manager)
        assert isinstance(worker._remote_manager, RemoteBackupManager)
        assert worker._remote_manager.client_id == 'host-1'
        assert worker._remote_manager.tls_dir == tmp_path / 'db.tls'


class TestApiWorkerThread:
    @pytest.mark.parametrize(
        'stop_event_factory', STOP_EVENT_FACTORIES, ids=STOP_EVENT_IDS
    )
    def test_run_starts_thread_and_stops_on_event(
        self, worker_config, sqlite_config_db, mocker, stop_event_factory
    ):
        stop_event = stop_event_factory()
        stop_event.set()  # stop_event.wait() returns immediately
        # dry_run: no server identity on disk -> a throwaway one in a temp dir
        worker = ApiWorker(worker_config, stop_event, config_db=sqlite_config_db)

        mock_app = MagicMock()
        mocker.patch('zfsbackup.api.create_app', return_value=mock_app)
        mock_thread = MagicMock()
        thread_cls = mocker.patch('zfsbackup.workers.threading.Thread', return_value=mock_thread)
        mocker.patch('zfsbackup.workers._bind_api_server', return_value=MagicMock())  # no real socket

        worker.run()
        # The server thread is started (the worker may run helper threads too,
        # e.g. the trust poll -- so look for the server's, not "exactly one").
        targets = [c.kwargs.get('target') for c in thread_cls.call_args_list]
        assert workers_module._serve in targets
        mock_thread.start.assert_called()


class TestChildSignalDispositionWiring:
    """Closes a gap the coordinator's re-review found in
    `test_zfsbackup_signals.py`: that file's fork tests call
    `_init_child_process` directly, which pins the function's OWN
    behaviour but nothing about whether `BaseWorker.run`/`ApiWorker.run`
    actually call it -- deleting either call site
    (`workers.py:159`/`workers.py:289`) left the entire signals test file
    green, since neither of those tests ever goes through `run()` at all.

    Deliberately in-process, no fork needed: `run()` resets THIS
    process's OWN SIGINT/SIGTERM disposition the moment it is called
    synchronously (it is the first statement of `run()`, before any
    config load), which is exactly what `tests/conftest.py`'s autouse
    `_restore_signal_handlers` fixture exists to make safe to assert on
    and clean up afterward -- without it, a non-default handler installed
    here would leak into every later test in the session.

    Mutation-checked: deleting the `_init_child_process(self.verbose)`
    call from `BaseWorker.run` fails
    `test_base_worker_run_resets_signal_disposition_first` (1/2 tests in
    this class); deleting the `_init_child_process(self.verbose)` call
    from `ApiWorker.run` fails
    `test_api_worker_run_resets_signal_disposition_first` (the other
    1/2) -- each call site is pinned by exactly the one test that goes
    through it, which is the point: this is wiring coverage, not a
    retest of `_init_child_process` itself (see
    `test_zfsbackup_signals.py` for that).
    """

    def _install_non_default_handlers(self):
        def _handler(signum, frame):  # pragma: no cover - never actually invoked
            pass
        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

    def test_base_worker_run_resets_signal_disposition_first(
        self, worker_config, mocker
    ):
        self._install_non_default_handlers()
        stop_event = multiprocessing.Event()
        stop_event.set()  # zero loop iterations -- run() still must reset first
        worker = SnapshotWorker(worker_config, stop_event)
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])

        worker.run()

        assert signal.getsignal(signal.SIGINT) is signal.SIG_DFL
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL

    @pytest.mark.parametrize(
        'stop_event_factory', STOP_EVENT_FACTORIES, ids=STOP_EVENT_IDS
    )
    def test_api_worker_run_resets_signal_disposition_first(
        self, worker_config, sqlite_config_db, mocker, stop_event_factory
    ):
        self._install_non_default_handlers()
        stop_event = stop_event_factory()
        stop_event.set()  # stop_event.wait() returns immediately
        worker = ApiWorker(worker_config, stop_event, config_db=sqlite_config_db)

        mock_app = MagicMock()
        mocker.patch('zfsbackup.api.create_app', return_value=mock_app)
        mock_thread = MagicMock()
        mocker.patch('zfsbackup.workers.threading.Thread', return_value=mock_thread)
        mocker.patch('zfsbackup.workers._bind_api_server', return_value=MagicMock())  # no real socket

        worker.run()

        assert signal.getsignal(signal.SIGINT) is signal.SIG_DFL
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL


# ---------------------------------------------------------------------------
# Item 15.2(b): workers are handed the supervisor's detached BackupConfig and
# never open the config database.
# ---------------------------------------------------------------------------

WORKER_CLASSES = [
    pytest.param(SnapshotWorker, 'snapshot-worker', id='snapshot'),
    pytest.param(PruningWorker, 'pruning-worker', id='pruning'),
    pytest.param(RemoteBackupWorker, 'remote-backup-worker', id='remote'),
    pytest.param(ApiWorker, 'api-worker', id='api'),
]


#: The keyword-only parameters a class takes after the shared trio (M9).
EXTRA_KEYWORDS = {
    ApiWorker: ['config_db'],
    RemoteBackupWorker: ['tls_dir', 'client_id'],
}


class TestWorkerConstructors:
    """`(config, stop_event, log_level=logging.INFO)` for every concrete
    worker; `BaseWorker(worker_name, config, stop_event, log_level=logging.INFO)`.
    The fourth parameter is the numeric logging level (C3), not a `-v` flag. There is no
    path and no `dry_run` parameter any more: `--dry-run` is applied to the
    config by the supervisor before the handoff.

    Mutation-checked (scratch copy): re-adding a trailing `dry_run` parameter
    to `SnapshotWorker.__init__` fails 2 tests
    (`test_the_signature_is_exactly_config_stop_event_log_level[snapshot]` and
    `test_the_old_dry_run_and_path_parameters_are_gone[snapshot]`)."""

    @pytest.mark.parametrize('cls, name', WORKER_CLASSES)
    def test_positional_config_stop_event_log_level(self, cls, name, worker_config):
        stop = multiprocessing.Event()
        worker = cls(worker_config, stop, logging.DEBUG)
        assert worker.config is worker_config
        assert worker.stop_event is stop
        assert worker.log_level == logging.DEBUG
        assert not hasattr(worker, 'verbose')
        assert worker.name == name and worker.daemon is True

    @pytest.mark.parametrize('cls, name', WORKER_CLASSES)
    def test_log_level_defaults_to_info(self, cls, name, worker_config):
        assert cls(worker_config, multiprocessing.Event()).log_level == logging.INFO

    @pytest.mark.parametrize('cls, name', WORKER_CLASSES)
    def test_the_signature_is_exactly_config_stop_event_log_level(self, cls, name):
        sig = inspect.signature(cls.__init__)
        extra = EXTRA_KEYWORDS.get(cls, [])
        assert list(sig.parameters) == [
            'self', 'config', 'stop_event', 'log_level',
            'reload_epoch', 'born_epoch', 'workers_lock', *extra,
        ]
        # The reload trio is keyword-only with inert defaults, so every old
        # positional call keeps working; so are the class-specific extras (M9:
        # the API worker's `config_db`, the remote worker's `tls_dir` and
        # `client_id`), all defaulting to `None`.
        for trio, default in (('reload_epoch', None), ('born_epoch', 0), ('workers_lock', None)):
            assert sig.parameters[trio].kind is inspect.Parameter.KEYWORD_ONLY
            assert sig.parameters[trio].default == default
        for name_ in extra:
            assert sig.parameters[name_].kind is inspect.Parameter.KEYWORD_ONLY
            assert sig.parameters[name_].default is None

    def test_base_worker_signature_takes_the_name_first(self):
        sig = inspect.signature(BaseWorker.__init__)
        assert list(sig.parameters) == [
            'self', 'worker_name', 'config', 'stop_event', 'log_level',
            'reload_epoch', 'born_epoch', 'workers_lock',
        ]
        assert [n for n, p in sig.parameters.items()
                if p.kind is inspect.Parameter.KEYWORD_ONLY] == ['reload_epoch', 'born_epoch', 'workers_lock']

    @pytest.mark.parametrize('cls, name', WORKER_CLASSES)
    def test_the_old_dry_run_and_path_parameters_are_gone(self, cls, name, worker_config):
        stop = multiprocessing.Event()
        with pytest.raises(TypeError):
            cls(worker_config, stop, dry_run=True)
        with pytest.raises(TypeError):
            cls(worker_config, stop, resolved_config=object())
        with pytest.raises(TypeError):
            cls(worker_config, stop, False, True)  # the old 4th positional (dry_run)

    @pytest.mark.parametrize('cls, name', WORKER_CLASSES)
    def test_the_config_is_not_copied(self, cls, name, worker_config):
        """Identity matters: all workers and every restart share one object."""
        a = cls(worker_config, multiprocessing.Event())
        b = cls(worker_config, multiprocessing.Event())
        assert a.config is b.config is worker_config

    def test_load_config_and_the_config_error_exit_are_gone(self):
        assert not hasattr(workers_module, '_load_config')
        assert not hasattr(workers_module, 'load_runtime_config')

    def test_workers_module_imports_nothing_from_the_config_store(self):
        """The strongest cheap guard that no worker can reach the database:
        nothing in the module's namespace comes from the store or the
        daemon's reader."""
        offenders = [
            name for name, obj in vars(workers_module).items()
            if getattr(obj, '__module__', '').startswith(
                ('zfsbackup.config.store', 'zfsbackup.runtime_config')
            ) or (inspect.ismodule(obj) and obj.__name__.startswith(
                ('zfsbackup.config.store', 'zfsbackup.runtime_config')
            ))
        ]
        assert offenders == []


class TestM9WorkerArguments:
    """The keyword-only arguments only some workers take (TLS/mTLS plan M9):
    stored as given, `None` otherwise, and never taken by the other classes."""

    def test_the_remote_worker_keeps_the_tls_directory_and_the_client_id(self, worker_config, tmp_path):
        worker = RemoteBackupWorker(
            worker_config, multiprocessing.Event(), tls_dir=tmp_path / 'db.tls', client_id='host-1'
        )
        assert worker.tls_dir == tmp_path / 'db.tls' and worker.client_id == 'host-1'

    def test_they_default_to_none(self, worker_config):
        worker = RemoteBackupWorker(worker_config, multiprocessing.Event())
        assert worker.tls_dir is None and worker.client_id is None
        assert ApiWorker(worker_config, multiprocessing.Event()).config_db is None

    def test_the_api_worker_keeps_the_config_database(self, worker_config, sqlite_config_db):
        worker = ApiWorker(worker_config, multiprocessing.Event(), config_db=sqlite_config_db)
        assert worker.config_db is sqlite_config_db

    @pytest.mark.parametrize('cls', [SnapshotWorker, PruningWorker])
    @pytest.mark.parametrize('kwarg', ['config_db', 'tls_dir', 'client_id'])
    def test_the_local_workers_take_none_of_them(self, cls, kwarg, worker_config):
        with pytest.raises(TypeError):
            cls(worker_config, multiprocessing.Event(), **{kwarg: 'x'})

    def test_the_other_classes_do_not_take_each_others(self, worker_config):
        with pytest.raises(TypeError):
            ApiWorker(worker_config, multiprocessing.Event(), client_id='x')
        with pytest.raises(TypeError):
            RemoteBackupWorker(worker_config, multiprocessing.Event(), config_db=object())


class TestConfigSurvivesPickling:
    """Under `spawn`/`forkserver` the config crosses the process boundary by
    pickling. `Duration.__eq__` ignores literals, so equality alone proves
    nothing: the signature comparison records each literal.

    Mutation-checked (scratch copy): removing `Duration.__reduce__` fails 6 of
    the 8 tests here -- `test_pickled_signature_is_identical` at all four
    protocols, `test_every_duration_literal_survives` and
    `test_deepcopy_agrees_with_pickle`."""

    @pytest.fixture
    def config(self, literal_config_db):
        snapshot = load_runtime_config(literal_config_db)
        dispose_all()
        snapshot.config.dry_run = True
        return snapshot.config

    @pytest.mark.parametrize('protocol', range(2, pickle.HIGHEST_PROTOCOL + 1))
    def test_pickled_signature_is_identical(self, config, protocol):
        clone = pickle.loads(pickle.dumps(config, protocol))
        assert clone is not config
        assert clone == config
        assert config_signature(clone) == config_signature(config)
        assert clone.dry_run is True

    def test_every_duration_literal_survives(self, config):
        clone = pickle.loads(pickle.dumps(config))
        expected = ['600s', '90m', '60m', '4w', '1M', '120m', '52w']
        assert set(expected) <= set(literals_in(config_signature(config)))
        assert set(expected) <= set(literals_in(config_signature(clone)))
        ds = clone.datasets[0]
        assert ds.frequency.literal == '60m'
        assert [r.age.literal for r in ds.retention_rules] == ['1d', '1w']
        assert [r.keep_for.literal for r in ds.retention_rules] == ['4w', '1M']
        (remote,) = ds.remote
        assert remote.frequency.literal == '120m'
        assert [(r.age.literal, r.keep_for.literal) for r in remote.retention_rules] == [('1d', '52w')]
        assert clone.check_interval.literal == '600s'
        assert clone.prune_interval.literal == '90m'

    def test_deepcopy_agrees_with_pickle(self, config):
        assert config_signature(copy.deepcopy(config)) == config_signature(config)

    def test_equality_alone_cannot_see_a_lost_literal(self):
        """Why the signature exists: these compare equal, and are not the same."""
        a, b = Duration('60m'), Duration('1h')
        assert a == b
        assert config_signature(a) != config_signature(b)

    def test_a_pickled_worker_config_drives_a_worker_identically(self, config):
        clone = pickle.loads(pickle.dumps(config))
        for cls in (SnapshotWorker, PruningWorker):
            original = cls(config, multiprocessing.Event())
            copied = cls(clone, multiprocessing.Event())
            assert original._get_interval(original.config) == copied._get_interval(copied.config)


class TestWorkersNeverOpenTheConfigDatabase:
    """Item 15.2(b): the supervisor read the database once; a worker's `run()`
    must work with the database gone and every way of opening it armed to
    explode. Run for real (stop flag already set, so zero loop iterations but
    the whole setup path: signal reset, `DatasetManager`, interval, and
    `_before_loop`).

    Mutation-checked (scratch copy): making `BaseWorker.run` open any SQLite
    connection fails 8 tests: 7 here (the three-class x two-stop-flag run test
    and the loop test) and `TestForkedWorkerRunsOnTheInheritedConfig`."""

    @pytest.fixture
    def armed(self, literal_config_db, tmp_path, request, mocker):
        """-> the config. Read first, then the database is deleted and every
        way of opening it is armed (`forbid_db`, resolved here so it cannot
        be armed before the read). The one door left is the API worker's
        trust view (`zfsbackup.config.store.trust`, never the configuration):
        `store.trust`'s two reads are answered by an empty in-memory trust, so
        a worker that opened the configuration -- or anything else -- would
        still trip `forbid_db`."""
        from _tls_support import StubTrust
        from zfsbackup.config.store import trust as store_trust

        stub = StubTrust()
        mocker.patch.object(store_trust, 'read_trust_key', stub.read_trust_key)
        mocker.patch.object(store_trust, 'read_trust', stub.read_trust)
        snapshot = load_runtime_config(literal_config_db)
        dispose_all()
        config = snapshot.config
        config.dry_run = True
        for path in tmp_path.glob('literal.db*'):
            path.unlink()
        assert not list(tmp_path.glob('literal.db*'))
        request.getfixturevalue('forbid_db')
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        mocker.patch('zfsbackup.backup_manager.zfs.set')
        return config

    @pytest.mark.parametrize('stop_factory', STOP_EVENT_FACTORIES, ids=STOP_EVENT_IDS)
    @pytest.mark.parametrize('cls', [SnapshotWorker, PruningWorker, RemoteBackupWorker])
    def test_run_completes_without_touching_the_database(
        self, cls, stop_factory, armed, forbid_db
    ):
        stop = stop_factory()
        stop.set()
        cls(armed, stop).run()
        assert forbid_db == []

    @pytest.mark.parametrize('stop_factory', STOP_EVENT_FACTORIES, ids=STOP_EVENT_IDS)
    def test_api_worker_run_completes_without_touching_the_database(
        self, stop_factory, armed, forbid_db, literal_config_db, mocker
    ):
        stop = stop_factory()
        stop.set()
        app = MagicMock()
        mocker.patch('zfsbackup.api.create_app', return_value=app)
        mocker.patch('zfsbackup.workers.threading.Thread', return_value=MagicMock())
        mocker.patch('zfsbackup.workers._bind_api_server', return_value=MagicMock())  # no real socket
        # `armed.dry_run`: the missing server identity is a throwaway one.
        ApiWorker(armed, stop, config_db=literal_config_db).run()
        assert forbid_db == []

    def test_the_api_app_is_built_from_the_handed_config(
        self, armed, literal_config_db, mocker
    ):
        from zfsbackup.api import TrustGateway

        stop = multiprocessing.Event()
        stop.set()
        factory = mocker.patch('zfsbackup.api.create_app', return_value=MagicMock())
        mocker.patch('zfsbackup.workers.threading.Thread', return_value=MagicMock())
        mocker.patch('zfsbackup.workers._bind_api_server', return_value=MagicMock())  # no real socket
        ApiWorker(armed, stop, config_db=literal_config_db).run()
        factory.assert_called_once()
        args = factory.call_args.args
        assert args[0] is armed                              # the handed config, not a re-read one
        assert isinstance(args[1], TrustGateway)             # trust
        assert args[1].server_fingerprint == args[2]         # and the identity it presents

    def test_the_worker_loop_runs_on_the_handed_config_not_a_reloaded_one(self, armed, mocker):
        """A cycle processes the datasets of the config it was handed; there
        is nowhere to get another one from."""
        stop = multiprocessing.Event()
        seen = []

        def process(self_, manager, dsi):
            seen.append(dsi.name)
            stop.set()

        mocker.patch.object(SnapshotWorker, '_process_dataset', process)
        SnapshotWorker(armed, stop).run()
        assert seen == ['pool/data']  # first of the config's datasets, then stopped

    def test_interval_comes_from_the_handed_config(self, armed):
        assert SnapshotWorker(armed, multiprocessing.Event())._get_interval(armed) == 600.0
        assert PruningWorker(armed, multiprocessing.Event())._get_interval(armed) == 5400.0
        # min of the remote frequencies: the `offsite` override of 120m
        assert RemoteBackupWorker(armed, multiprocessing.Event())._get_interval(armed) == 7200.0


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(),
    reason="'fork' multiprocessing start method unavailable on this platform",
)
@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestForkedWorkerRunsOnTheInheritedConfig:
    """A real forked child runs the real `SnapshotWorker.run` on the config it
    inherited, with the database deleted and `sqlite3.connect` armed to raise
    in the parent (a fork child inherits the patched module, so a child that
    opened the database would die with `DbTouched` and a non-zero exit)."""

    def test_child_exits_cleanly_without_opening_the_database(
        self, literal_config_db, tmp_path, mocker
    ):
        from _config_handoff import ForkedSnapshotWorker

        snapshot = load_runtime_config(literal_config_db)
        dispose_all()
        for path in tmp_path.glob('literal.db*'):
            path.unlink()
        forbid = mocker.patch('sqlite3.connect', side_effect=RuntimeError('db opened'))
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        mocker.patch('zfsbackup.backup_manager.zfs.set')
        stop = StopFlag()
        stop.set()  # zero loop iterations; the whole setup path still runs
        worker = ForkedSnapshotWorker(snapshot.config, stop)
        worker.start_method = 'fork'

        worker.start()
        worker.join(60)

        assert worker.exitcode == 0
        forbid.assert_not_called()


# ---------------------------------------------------------------------------
# S1-c / B11: a future-dated anchor makes the remote destination due.
# ---------------------------------------------------------------------------

from datetime import timezone  # noqa: E402

from _snapshot_support import frozen_clock, listed_info  # noqa: E402,F401  (fixture)

NTP_MESSAGE = 'system clock is behind the newest snapshot; check NTP'


class TestFutureDatedAnchor:
    """The remote worker skips a destination whose anchor is younger than the frequency. An anchor
    dated beyond the clock tolerance is not 'young': the clock is wrong, and the destination is
    due (`DatasetManager.clock_behind` logs the ERROR once per destination)."""

    def build(self, mocker, frozen_clock, anchors, *, check_interval=timedelta(minutes=5),
              destinations=('a', 'b')):
        config = BackupConfig(
            datasets=[DatasetConfig(
                name='pool/data', frequency=timedelta(hours=1),
                remote=[RemoteDatasetConfig(destination=d) for d in destinations],
            )],
            check_interval=check_interval,
        )
        manager = DatasetManager(config)
        dsi = manager.datasets[0]
        # one snapshot per distinct anchor offset (minutes from the frozen "now"), fixed at build time
        snaps = {m: listed_info(frozen_clock.stamp(timedelta(minutes=m)), txg=i + 1)
                 for i, m in enumerate(sorted(set(anchors.values())))}
        anchor_name = {dest: snaps[m].name for dest, m in anchors.items()}
        mocker.patch.object(manager, 'list_snapshots', side_effect=lambda d: list(snaps.values()))
        mocker.patch.object(manager, 'get_anchor', side_effect=lambda d, dest: anchor_name.get(dest))
        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        worker.name = 'remote-backup-worker'
        worker._exit_reason = lambda: None
        worker._remote_manager = MagicMock()
        return worker, manager, dsi

    def cycle(self, worker, manager, dsi):
        worker._remote_manager.backup_dataset.reset_mock()
        worker._process_dataset(manager, dsi)
        return [c.args[1].destination for c in worker._remote_manager.backup_dataset.call_args_list]

    def test_a_fresh_past_anchor_is_not_due(self, mocker, frozen_clock, caplog):
        worker, manager, dsi = self.build(mocker, frozen_clock, {'a': -20, 'b': -20})
        with caplog.at_level(logging.DEBUG):
            assert self.cycle(worker, manager, dsi) == []
        assert not [r for r in caplog.records if NTP_MESSAGE in r.getMessage()]

    def test_an_old_anchor_is_due(self, mocker, frozen_clock):
        worker, manager, dsi = self.build(mocker, frozen_clock, {'a': -90, 'b': -90})
        assert self.cycle(worker, manager, dsi) == ['a', 'b']

    def test_a_future_dated_anchor_is_due_with_one_error_per_destination(
        self, mocker, frozen_clock, caplog
    ):
        worker, manager, dsi = self.build(mocker, frozen_clock, {'a': 600, 'b': 600})
        with caplog.at_level(logging.DEBUG):
            for _ in range(3):
                assert self.cycle(worker, manager, dsi) == ['a', 'b']
        errors = [r for r in caplog.records if NTP_MESSAGE in r.getMessage() and r.levelname == 'ERROR']
        assert len(errors) == 2                          # once for each destination, not per cycle
        text = ' '.join(r.getMessage() for r in errors)
        assert 'anchor for a' in text and 'anchor for b' in text

    def test_each_destination_is_judged_by_its_own_anchor(self, mocker, frozen_clock, caplog):
        worker, manager, dsi = self.build(mocker, frozen_clock, {'a': 600, 'b': -20})
        with caplog.at_level(logging.DEBUG):
            assert self.cycle(worker, manager, dsi) == ['a']
        errors = [r for r in caplog.records if NTP_MESSAGE in r.getMessage()]
        assert len(errors) == 1 and 'anchor for a' in errors[0].getMessage()

    def test_an_anchor_within_the_tolerance_is_just_young(self, mocker, frozen_clock, caplog):
        worker, manager, dsi = self.build(mocker, frozen_clock, {'a': 4, 'b': 4})
        with caplog.at_level(logging.DEBUG):
            assert self.cycle(worker, manager, dsi) == []
        assert not [r for r in caplog.records if NTP_MESSAGE in r.getMessage()]

    def test_just_past_the_tolerance_it_is_due(self, mocker, frozen_clock):
        worker, manager, dsi = self.build(mocker, frozen_clock, {'a': 6, 'b': 6})
        assert self.cycle(worker, manager, dsi) == ['a', 'b']

    def test_a_long_check_interval_widens_the_tolerance(self, mocker, frozen_clock):
        worker, manager, dsi = self.build(
            mocker, frozen_clock, {'a': 20, 'b': 20}, check_interval=timedelta(minutes=30))
        assert self.cycle(worker, manager, dsi) == []

    def test_the_error_comes_again_for_a_later_episode(self, mocker, frozen_clock, caplog):
        worker, manager, dsi = self.build(mocker, frozen_clock, {'a': 600}, destinations=('a',))
        with caplog.at_level(logging.ERROR):
            self.cycle(worker, manager, dsi)                       # episode 1
            frozen_clock.advance(timedelta(hours=11))               # the anchor is now past: normal
            self.cycle(worker, manager, dsi)
            frozen_clock.advance(-timedelta(hours=11))              # the clock goes back again
            self.cycle(worker, manager, dsi)                       # episode 2
        assert len([r for r in caplog.records if NTP_MESSAGE in r.getMessage()]) == 2

    def test_no_anchor_is_due_and_not_an_error(self, mocker, frozen_clock, caplog):
        worker, manager, dsi = self.build(mocker, frozen_clock, {'a': 0, 'b': 0})
        manager.get_anchor.side_effect = lambda d, dest: None
        with caplog.at_level(logging.ERROR):
            assert self.cycle(worker, manager, dsi) == ['a', 'b']
        assert not caplog.records


# ---------------------------------------------------------------------------
# Dataset order: the remote worker sends ancestors before descendants
# ---------------------------------------------------------------------------

def _named(*names):
    import types
    return [types.SimpleNamespace(name=n) for n in names]


def _remote_worker():
    worker = RemoteBackupWorker(BackupConfig(datasets=[], destinations={}), StopFlag(), client_id="c")
    worker._remote_manager = MagicMock()
    return worker


class TestRemoteWorkerDatasetOrder:
    """A child sent before its parent would leave the parent's server dataset as an unowned
    placeholder that a raw (or encrypted-target) full stream can never replace: the remote worker
    visits datasets by path depth, shallowest first, and keeps config order within a depth."""

    @staticmethod
    def order(*names):
        manager = MagicMock(datasets=_named(*names))
        return [d.name for d in _remote_worker()._get_datasets(manager)]

    def test_a_child_listed_before_its_parent_is_processed_after_it(self):
        assert self.order('tank/data/child', 'tank/data') == ['tank/data', 'tank/data/child']

    def test_equal_depth_keeps_config_order(self):
        assert self.order('tank/b', 'tank/a', 'tank/c') == ['tank/b', 'tank/a', 'tank/c']
        assert self.order('tank/c', 'tank/a', 'tank/b') == ['tank/c', 'tank/a', 'tank/b']

    def test_mixed_depths(self):
        assert self.order('tank/x/y', 'tank/b', 'tank/x', 'tank/a/c', 'tank/a') == [
            'tank/b', 'tank/x', 'tank/a', 'tank/x/y', 'tank/a/c']

    def test_nested_grandparents(self):
        assert self.order('tank/p/c/g', 'tank/p/c', 'tank/p') == ['tank/p', 'tank/p/c', 'tank/p/c/g']
        assert self.order('tank/p/c/g', 'tank/q', 'tank/p', 'tank/p/c') == [
            'tank/q', 'tank/p', 'tank/p/c', 'tank/p/c/g']

    def test_a_pool_root_comes_first(self):
        assert self.order('tank/a', 'tank') == ['tank', 'tank/a']

    def test_the_config_list_itself_is_not_reordered(self):
        names = ['tank/p/c', 'tank/p']
        manager = MagicMock(datasets=_named(*names))
        before = list(manager.datasets)
        _remote_worker()._get_datasets(manager)
        assert manager.datasets == before

    def test_an_empty_list_is_empty(self):
        assert self.order() == []

    def test_begin_cycle_still_runs_first(self):
        worker = _remote_worker()
        worker._get_datasets(MagicMock(datasets=_named('tank/a/b', 'tank/a')))
        worker._remote_manager.begin_cycle.assert_called_once_with()


class TestOtherWorkersKeepConfigOrder:
    NAMES = ('tank/p/c', 'tank/p', 'tank/q/r', 'tank/q')

    def test_the_snapshot_worker(self, worker_config):
        manager = MagicMock(datasets=_named(*self.NAMES))
        result = SnapshotWorker(worker_config, multiprocessing.Event())._get_datasets(manager)
        assert [d.name for d in result] == list(self.NAMES)

    def test_the_pruning_worker(self, worker_config):
        received = _named('tank/backups/c1/z/y', 'tank/backups/c1/z')
        manager = MagicMock(datasets=_named(*self.NAMES))
        manager.received_datasets.return_value = received
        result = PruningWorker(worker_config, multiprocessing.Event())._get_datasets(manager)
        assert [d.name for d in result] == list(self.NAMES) + [d.name for d in received]

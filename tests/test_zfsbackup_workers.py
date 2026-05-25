"""Unit tests for zfsbackup worker classes."""

import multiprocessing
import pytest
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

from zfsbackup.config import BackupConfig, DatasetConfig, RemoteDatasetConfig
from zfsbackup.backup_manager import DatasetInfo, DatasetManager
from zfsbackup.workers import (
    SnapshotWorker, PruningWorker, RemoteBackupWorker, ApiWorker,
)


class TestSnapshotWorker:
    def test_name_and_daemon(self, config_yaml_path):
        stop_event = multiprocessing.Event()
        worker = SnapshotWorker(config_yaml_path, stop_event, dry_run=True)
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

    def test_run_loads_config_and_processes(self, mocker, config_yaml_path):
        stop_event = multiprocessing.Event()
        stop_event.set()
        worker = SnapshotWorker(config_yaml_path, stop_event, dry_run=True)
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        worker.run()


class TestPruningWorker:
    def test_name_and_daemon(self, config_yaml_path):
        stop_event = multiprocessing.Event()
        worker = PruningWorker(config_yaml_path, stop_event, dry_run=True)
        assert worker.name == 'pruning-worker'
        assert worker.daemon is True

    def test_get_interval_returns_prune_interval(self, sample_backup_config):
        worker = PruningWorker.__new__(PruningWorker)
        interval = worker._get_interval(sample_backup_config)
        assert interval == sample_backup_config.prune_interval.total_seconds()

    def test_process_dataset_calls_prune(self, mocker):
        manager = MagicMock()
        dsi = MagicMock()
        worker = PruningWorker.__new__(PruningWorker)
        worker._process_dataset(manager, dsi)
        manager.prune_snapshots.assert_called_once_with(dsi)

    def test_get_datasets_includes_received(self, mocker):
        manager = MagicMock()
        local = [MagicMock()]
        received = [MagicMock()]
        manager.datasets = local
        manager.received_datasets.return_value = received
        worker = PruningWorker.__new__(PruningWorker)
        result = worker._get_datasets(manager)
        assert local[0] in result
        assert received[0] in result

    def test_run_stops_when_event_set(self, config_yaml_path, mocker):
        stop_event = multiprocessing.Event()
        stop_event.set()
        worker = PruningWorker(config_yaml_path, stop_event, dry_run=True)
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        worker.run()


class TestRemoteBackupWorker:
    def test_name_and_daemon(self, config_yaml_path):
        stop_event = multiprocessing.Event()
        worker = RemoteBackupWorker(config_yaml_path, stop_event, dry_run=True)
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
        worker._remote_manager.backup_dataset.assert_called_once_with(dsi, remote_cfg)


class TestApiWorker:
    def test_name_and_daemon(self, config_yaml_path):
        stop_event = multiprocessing.Event()
        worker = ApiWorker(config_yaml_path, stop_event, dry_run=True)
        assert worker.name == 'api-worker'
        assert worker.daemon is True

    def test_run_bad_config_exits_cleanly(self, tmp_path, mocker):
        bad_config = tmp_path / 'bad.yaml'
        bad_config.write_text('')
        stop_event = multiprocessing.Event()
        worker = ApiWorker(bad_config, stop_event, dry_run=True)
        worker.run()


class TestBaseWorkerRun:
    def test_run_exits_on_bad_config(self, tmp_path, mocker):
        bad_config = tmp_path / 'bad.yaml'
        bad_config.write_text('')
        stop_event = multiprocessing.Event()
        worker = SnapshotWorker(bad_config, stop_event, dry_run=True)
        worker.run()

    def test_run_sets_dry_run_from_flag(self, config_yaml_path, mocker):
        stop_event = multiprocessing.Event()
        stop_event.set()
        worker = SnapshotWorker(config_yaml_path, stop_event, dry_run=True)
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        worker.run()


class TestBaseWorkerLoop:
    """Cover the main dataset-processing loop (lines 74-82) and _get_datasets (line 51)."""

    def test_dataset_exception_logged_and_loop_exits_on_stop(self, config_yaml_path, mocker):
        stop_event = multiprocessing.Event()
        worker = SnapshotWorker(config_yaml_path, stop_event, dry_run=True)

        two_dataset_config = BackupConfig(datasets=[
            DatasetConfig(name='pool/a'),
            DatasetConfig(name='pool/b'),
        ])
        mocker.patch('zfsbackup.workers.BackupConfig.from_file', return_value=two_dataset_config)
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
            destinations={'offsite': Destination(url='http://backup.example.com')},
            client_id_file=tmp_path / 'client_id',
        )
        manager = MagicMock()
        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        worker._remote_manager = None
        worker._before_loop(config, manager)
        assert isinstance(worker._remote_manager, RemoteBackupManager)


class TestApiWorkerThread:
    def test_run_starts_thread_and_stops_on_event(self, config_yaml_path, mocker):
        stop_event = multiprocessing.Event()
        stop_event.set()  # stop_event.wait() returns immediately
        worker = ApiWorker(config_yaml_path, stop_event, dry_run=True)  # dry_run covers line 198

        mock_app = MagicMock()
        mocker.patch('zfsbackup.api.create_app', return_value=mock_app)
        mock_thread = MagicMock()
        mocker.patch('zfsbackup.workers.threading.Thread', return_value=mock_thread)

        worker.run()
        mock_thread.start.assert_called_once()

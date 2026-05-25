"""Unit tests for BackupDaemon."""

import multiprocessing
import signal
import sys
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

from zfsbackup.config import BackupConfig, DatasetConfig, RemoteDatasetConfig, RemoteServerConfig, Destination
from zfsbackup.daemon import BackupDaemon, main
from zfsbackup.workers import SnapshotWorker, PruningWorker, ApiWorker, RemoteBackupWorker


def _make_config(**kwargs) -> BackupConfig:
    defaults = dict(datasets=[DatasetConfig(name='pool/data')])
    defaults.update(kwargs)
    return BackupConfig(**defaults)


class TestBackupDaemonInit:
    def test_instantiation(self, config_yaml_path, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)
        assert daemon.config is sample_backup_config
        assert daemon.config_path == config_yaml_path
        assert daemon.verbose is False

    def test_verbose_flag(self, config_yaml_path, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path, verbose=True)
        assert daemon.verbose is True

    def test_stop_event_initially_clear(self, config_yaml_path, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)
        assert not daemon._stop_event.is_set()


class TestSignalHandler:
    def test_signal_sets_stop_event(self, config_yaml_path, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)
        daemon._signal_handler(signal.SIGINT, None)
        assert daemon._stop_event.is_set()

    def test_sigterm_sets_stop_event(self, config_yaml_path, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)
        daemon._signal_handler(signal.SIGTERM, None)
        assert daemon._stop_event.is_set()


class TestActiveWorkerNames:
    def test_base_workers_always_included(self, config_yaml_path, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)
        names = daemon._active_worker_names()
        assert 'snapshot' in names
        assert 'pruning' in names
        assert 'api' in names

    def test_remote_worker_excluded_when_no_remote(self, config_yaml_path, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)
        names = daemon._active_worker_names()
        assert 'remote' not in names

    def test_remote_worker_included_when_datasets_have_remote(self, config_yaml_path):
        config = _make_config(datasets=[
            DatasetConfig(
                name='pool/data',
                remote=[RemoteDatasetConfig(destination='offsite')],
            ),
        ])
        daemon = BackupDaemon(config, config_yaml_path)
        names = daemon._active_worker_names()
        assert 'remote' in names


class TestNewWorker:
    def test_new_worker_snapshot(self, config_yaml_path, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)
        worker = daemon._new_worker('snapshot')
        assert isinstance(worker, SnapshotWorker)

    def test_new_worker_pruning(self, config_yaml_path, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)
        worker = daemon._new_worker('pruning')
        assert isinstance(worker, PruningWorker)

    def test_new_worker_api(self, config_yaml_path, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)
        worker = daemon._new_worker('api')
        assert isinstance(worker, ApiWorker)

    def test_new_worker_remote(self, config_yaml_path, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)
        worker = daemon._new_worker('remote')
        assert isinstance(worker, RemoteBackupWorker)


class TestStartWorkers:
    def test_starts_all_active_workers(self, config_yaml_path, sample_backup_config, mocker):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)
        mock_worker = MagicMock()
        mock_worker.pid = 1234
        mocker.patch.object(daemon, '_new_worker', return_value=mock_worker)
        daemon._start_workers()
        assert mock_worker.start.call_count == len(daemon._active_worker_names())
        assert len(daemon._workers) == len(daemon._active_worker_names())


class TestCheckWorkers:
    def test_restarts_dead_worker(self, config_yaml_path, sample_backup_config, mocker):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)
        dead_worker = MagicMock()
        dead_worker.is_alive.return_value = False
        dead_worker.pid = 100
        dead_worker.exitcode = 1
        daemon._workers = {'snapshot': dead_worker}

        new_worker = MagicMock()
        new_worker.pid = 200
        mocker.patch.object(daemon, '_new_worker', return_value=new_worker)
        mocker.patch('zfsbackup.daemon.time.sleep')

        daemon._check_workers()
        new_worker.start.assert_called_once()
        assert daemon._workers['snapshot'] is new_worker

    def test_alive_workers_not_restarted(self, config_yaml_path, sample_backup_config, mocker):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)
        alive_worker = MagicMock()
        alive_worker.is_alive.return_value = True
        daemon._workers = {'snapshot': alive_worker}
        mocker.patch.object(daemon, '_new_worker')
        daemon._check_workers()
        daemon._new_worker.assert_not_called()


class TestShutdownWorkers:
    def test_joins_all_workers(self, config_yaml_path, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)
        worker1 = MagicMock()
        worker1.is_alive.return_value = False
        worker2 = MagicMock()
        worker2.is_alive.return_value = False
        daemon._workers = {'snapshot': worker1, 'pruning': worker2}
        daemon._shutdown_workers()
        worker1.join.assert_called()
        worker2.join.assert_called()

    def test_terminates_workers_that_dont_exit(self, config_yaml_path, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)
        stubborn = MagicMock()
        stubborn.is_alive.return_value = True
        daemon._workers = {'snapshot': stubborn}
        daemon._shutdown_workers(timeout=1)
        stubborn.terminate.assert_called_once()


class TestDaemonRun:
    def test_run_starts_and_shuts_down(self, config_yaml_path, sample_backup_config, mocker):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)

        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        mocker.patch('zfsbackup.backup_manager.zfs.set')

        mocker.patch.object(daemon, '_start_workers')
        mocker.patch.object(daemon, '_check_workers')
        mocker.patch.object(daemon, '_shutdown_workers')

        daemon._stop_event.set()
        daemon.run()

        daemon._start_workers.assert_called_once()
        daemon._shutdown_workers.assert_called_once()

    def test_run_supervisor_loop_checks_workers(self, config_yaml_path, sample_backup_config, mocker):
        daemon = BackupDaemon(sample_backup_config, config_yaml_path)

        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        mocker.patch('zfsbackup.backup_manager.zfs.set')

        mocker.patch.object(daemon, '_start_workers')
        mocker.patch.object(daemon, '_check_workers')
        mocker.patch.object(daemon, '_shutdown_workers')

        sleep_calls = [0]

        def sleep_and_maybe_stop(_):
            sleep_calls[0] += 1
            if sleep_calls[0] >= 2:
                daemon._stop_event.set()

        mocker.patch('zfsbackup.daemon.time.sleep', side_effect=sleep_and_maybe_stop)
        daemon.run()

        daemon._check_workers.assert_called_once()
        daemon._shutdown_workers.assert_called_once()


class TestMain:
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
        mocker.patch('zfsbackup.daemon.BackupConfig.from_file', return_value=mock_config)
        return mock_config

    def test_test_config_returns_zero(self, tmp_path, mocker):
        self._patch_config(mocker)
        with patch('sys.argv', ['daemon', '--test-config', '-c', str(tmp_path / 'cfg.yaml')]):
            result = main()
        assert result == 0

    def test_test_config_with_destinations(self, tmp_path, mocker):
        self._patch_config(
            mocker,
            destinations={'offsite': Destination(url='http://backup.example.com')},
            remote_backup=MagicMock(target_dataset='pool/backups'),
        )
        with patch('sys.argv', ['daemon', '--test-config', '-c', str(tmp_path / 'cfg.yaml')]):
            result = main()
        assert result == 0

    def test_config_load_failure_returns_one(self, tmp_path, mocker):
        mocker.patch('zfsbackup.daemon.BackupConfig.from_file', side_effect=Exception('bad config'))
        with patch('sys.argv', ['daemon', '-c', str(tmp_path / 'cfg.yaml')]):
            result = main()
        assert result == 1

    def test_daemon_failure_returns_one(self, tmp_path, mocker):
        self._patch_config(mocker)
        mock_daemon = MagicMock()
        mock_daemon.run.side_effect = Exception('crash')
        mocker.patch('zfsbackup.daemon.BackupDaemon', return_value=mock_daemon)
        with patch('sys.argv', ['daemon', '-c', str(tmp_path / 'cfg.yaml')]):
            result = main()
        assert result == 1

    def test_normal_run_returns_zero(self, tmp_path, mocker):
        self._patch_config(mocker)
        mock_daemon = MagicMock()
        mocker.patch('zfsbackup.daemon.BackupDaemon', return_value=mock_daemon)
        with patch('sys.argv', ['daemon', '-c', str(tmp_path / 'cfg.yaml')]):
            result = main()
        assert result == 0
        mock_daemon.run.assert_called_once()

    def test_dry_run_flag_sets_config(self, tmp_path, mocker):
        mock_config = self._patch_config(mocker)
        mock_daemon = MagicMock()
        mocker.patch('zfsbackup.daemon.BackupDaemon', return_value=mock_daemon)
        with patch('sys.argv', ['daemon', '-d', '-c', str(tmp_path / 'cfg.yaml')]):
            main()
        assert mock_config.dry_run is True

    def test_verbose_flag_sets_log_level(self, tmp_path, mocker):
        self._patch_config(mocker)
        mock_daemon = MagicMock()
        mocker.patch('zfsbackup.daemon.BackupDaemon', return_value=mock_daemon)
        with patch('sys.argv', ['daemon', '-v', '-c', str(tmp_path / 'cfg.yaml')]):
            result = main()
        assert result == 0

"""Unit tests for RemoteBackupManager and ClientIdentity."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from zfsbackup.config import BackupConfig, DatasetConfig, RemoteDatasetConfig, Destination
from zfsbackup.backup_manager import DatasetInfo, DatasetManager
from zfsbackup.remote import ClientIdentity, RemoteBackupManager


def _make_config(**kwargs) -> BackupConfig:
    defaults = dict(
        datasets=[DatasetConfig(
            name='pool/data',
            remote=[RemoteDatasetConfig(destination='offsite')],
        )],
        destinations={'offsite': Destination(url='http://backup.example.com')},
        client_id_file=Path('/tmp/test_client_id'),
    )
    defaults.update(kwargs)
    return BackupConfig(**defaults)


class TestClientIdentity:
    def test_generates_new_id_when_no_file(self, tmp_path):
        id_file = tmp_path / 'client_id'
        identity = ClientIdentity(id_file)
        cid = identity.client_id
        assert '-' in cid
        assert len(cid) > 5
        assert id_file.exists()

    def test_loads_existing_id(self, tmp_path):
        id_file = tmp_path / 'client_id'
        id_file.write_text('myhost-abc12345')
        identity = ClientIdentity(id_file)
        assert identity.client_id == 'myhost-abc12345'

    def test_generates_and_saves_id(self, tmp_path):
        id_file = tmp_path / 'client_id'
        identity = ClientIdentity(id_file)
        cid = identity.client_id
        assert id_file.read_text().strip() == cid

    def test_id_format_contains_hostname_and_uuid(self, tmp_path):
        id_file = tmp_path / 'client_id'
        identity = ClientIdentity(id_file)
        cid = identity.client_id
        parts = cid.rsplit('-', 1)
        assert len(parts) == 2
        assert len(parts[1]) == 8

    def test_caches_client_id(self, tmp_path):
        id_file = tmp_path / 'client_id'
        identity = ClientIdentity(id_file)
        cid1 = identity.client_id
        cid2 = identity.client_id
        assert cid1 == cid2

    def test_empty_file_generates_new_id(self, tmp_path):
        id_file = tmp_path / 'client_id'
        id_file.write_text('')
        identity = ClientIdentity(id_file)
        cid = identity.client_id
        assert len(cid) > 0

    def test_sanitizes_hostname(self, tmp_path, mocker):
        mocker.patch('socket.gethostname', return_value='my host!.local')
        id_file = tmp_path / 'client_id'
        identity = ClientIdentity(id_file)
        cid = identity.client_id
        assert '!' not in cid
        assert ' ' not in cid


class TestRemoteBackupManagerRegister:
    def test_register_success(self, tmp_path, requests_mock):
        config = _make_config(client_id_file=tmp_path / 'client_id')
        manager = DatasetManager(config)
        rbm = RemoteBackupManager(config, manager)
        requests_mock.post(
            'http://backup.example.com/backup/register',
            json={'target_dataset': 'pool/backups/client1'},
        )
        result = rbm._register('http://backup.example.com', 'client1')
        assert result is True

    def test_register_failure(self, tmp_path, requests_mock):
        config = _make_config(client_id_file=tmp_path / 'client_id')
        manager = DatasetManager(config)
        rbm = RemoteBackupManager(config, manager)
        requests_mock.post(
            'http://backup.example.com/backup/register',
            status_code=500,
        )
        result = rbm._register('http://backup.example.com', 'client1')
        assert result is False

    def test_register_connection_error(self, tmp_path, requests_mock):
        import requests
        config = _make_config(client_id_file=tmp_path / 'client_id')
        manager = DatasetManager(config)
        rbm = RemoteBackupManager(config, manager)
        requests_mock.post(
            'http://backup.example.com/backup/register',
            exc=requests.ConnectionError,
        )
        result = rbm._register('http://backup.example.com', 'client1')
        assert result is False


class TestRemoteBackupManagerNegotiate:
    def _make_rbm(self, tmp_path):
        config = _make_config(client_id_file=tmp_path / 'client_id')
        manager = DatasetManager(config)
        return RemoteBackupManager(config, manager), manager.datasets[0]

    def test_negotiate_returns_common_snapshot(self, tmp_path, requests_mock):
        rbm, dsi = self._make_rbm(tmp_path)
        requests_mock.post(
            f'http://backup.example.com/backup/client1/{dsi.name}/negotiate',
            json={'common_snapshot': 'autosnap_20240101120000', 'server_dataset': 'pool/b/c1'},
        )
        snaps = [MagicMock(name='autosnap_20240101120000')]
        snaps[0].name = 'autosnap_20240101120000'
        result = rbm._negotiate('http://backup.example.com', 'client1', dsi, snaps)
        assert result == 'autosnap_20240101120000'

    def test_negotiate_returns_none_when_no_common(self, tmp_path, requests_mock):
        rbm, dsi = self._make_rbm(tmp_path)
        requests_mock.post(
            f'http://backup.example.com/backup/client1/{dsi.name}/negotiate',
            json={'common_snapshot': None, 'server_dataset': 'pool/b/c1'},
        )
        result = rbm._negotiate('http://backup.example.com', 'client1', dsi, [])
        assert result is None

    def test_negotiate_returns_none_on_error(self, tmp_path, requests_mock):
        import requests
        rbm, dsi = self._make_rbm(tmp_path)
        requests_mock.post(
            f'http://backup.example.com/backup/client1/{dsi.name}/negotiate',
            exc=requests.ConnectionError,
        )
        result = rbm._negotiate('http://backup.example.com', 'client1', dsi, [])
        assert result is None


class TestGetResumeStream:
    def _make_rbm(self, tmp_path):
        config = _make_config(client_id_file=tmp_path / 'client_id')
        manager = DatasetManager(config)
        return RemoteBackupManager(config, manager), manager.datasets[0]

    def test_returns_fresh_send_when_no_resume_token(self, tmp_path, requests_mock, mocker):
        rbm, dsi = self._make_rbm(tmp_path)
        requests_mock.get(
            f'http://backup.example.com/backup/client1/{dsi.name}/resume_token',
            json={'resume_token': None},
        )
        mock_stream = MagicMock()
        mocker.patch('zfsbackup.remote.zfs.send.snapshot', return_value=mock_stream)
        latest = MagicMock()
        latest.snapshot = MagicMock()
        stream = rbm._get_resume_stream('http://backup.example.com', 'client1', dsi, latest, None)
        assert stream is mock_stream

    def test_returns_resume_stream_when_token_exists(self, tmp_path, requests_mock, mocker):
        rbm, dsi = self._make_rbm(tmp_path)
        requests_mock.get(
            f'http://backup.example.com/backup/client1/{dsi.name}/resume_token',
            json={'resume_token': 'abc123resumetoken'},
        )
        mock_stream = MagicMock()
        mocker.patch('zfsbackup.remote.zfs.send.resume', return_value=mock_stream)
        latest = MagicMock()
        stream = rbm._get_resume_stream('http://backup.example.com', 'client1', dsi, latest, None)
        assert stream is mock_stream

    def test_returns_incremental_send_when_common_snap(self, tmp_path, requests_mock, mocker):
        rbm, dsi = self._make_rbm(tmp_path)
        requests_mock.get(
            f'http://backup.example.com/backup/client1/{dsi.name}/resume_token',
            json={'resume_token': None},
        )
        mock_stream = MagicMock()
        mocker.patch('zfsbackup.remote.zfs.send.snapshot', return_value=mock_stream)
        latest = MagicMock()
        latest.snapshot = MagicMock()
        common = MagicMock()
        common.snapshot = MagicMock()
        common.name = 'autosnap_20240101120000'
        stream = rbm._get_resume_stream('http://backup.example.com', 'client1', dsi, latest, common)
        assert stream is mock_stream

    def test_returns_none_on_send_exception(self, tmp_path, requests_mock, mocker):
        rbm, dsi = self._make_rbm(tmp_path)
        requests_mock.get(
            f'http://backup.example.com/backup/client1/{dsi.name}/resume_token',
            json={'resume_token': None},
        )
        mocker.patch('zfsbackup.remote.zfs.send.snapshot', side_effect=Exception('err'))
        latest = MagicMock()
        latest.snapshot = MagicMock()
        stream = rbm._get_resume_stream('http://backup.example.com', 'client1', dsi, latest, None)
        assert stream is None


class TestBackupDataset:
    def test_returns_false_when_destination_unknown(self, tmp_path):
        config = _make_config(
            client_id_file=tmp_path / 'client_id',
            destinations={},
        )
        manager = DatasetManager(config)
        rbm = RemoteBackupManager(config, manager)
        dsi = manager.datasets[0]
        remote_cfg = dsi.config.remote[0]
        result = rbm.backup_dataset(dsi, remote_cfg)
        assert result is False

    def test_returns_true_when_no_snapshots(self, tmp_path, requests_mock, mocker):
        config = _make_config(client_id_file=tmp_path / 'client_id')
        manager = DatasetManager(config)
        rbm = RemoteBackupManager(config, manager)
        dsi = manager.datasets[0]
        remote_cfg = dsi.config.remote[0]

        requests_mock.post('http://backup.example.com/backup/register', json={})
        mocker.patch.object(manager, 'list_snapshots', return_value=[])

        result = rbm.backup_dataset(dsi, remote_cfg)
        assert result is True

    def test_returns_false_when_register_fails(self, tmp_path, requests_mock, mocker):
        config = _make_config(client_id_file=tmp_path / 'client_id')
        manager = DatasetManager(config)
        rbm = RemoteBackupManager(config, manager)
        dsi = manager.datasets[0]
        remote_cfg = dsi.config.remote[0]

        requests_mock.post('http://backup.example.com/backup/register', status_code=500)

        result = rbm.backup_dataset(dsi, remote_cfg)
        assert result is False

    def test_dry_run_returns_true_without_transfer(self, tmp_path, requests_mock, mocker):
        config = _make_config(
            client_id_file=tmp_path / 'client_id',
            dry_run=True,
        )
        manager = DatasetManager(config)
        rbm = RemoteBackupManager(config, manager)
        dsi = manager.datasets[0]
        remote_cfg = dsi.config.remote[0]

        requests_mock.post('http://backup.example.com/backup/register', json={})
        requests_mock.post(
            f'http://backup.example.com/backup/{rbm.identity.client_id}/{dsi.name}/negotiate',
            json={'common_snapshot': None, 'server_dataset': 'pool/b'},
        )
        requests_mock.get(
            f'http://backup.example.com/backup/{rbm.identity.client_id}/{dsi.name}/resume_token',
            json={'resume_token': None},
        )

        mock_stream = MagicMock()
        mocker.patch('zfsbackup.remote.zfs.send.snapshot', return_value=mock_stream)

        from libzfseasy.types import Filesystem, Snapshot as ZFSSnapshot
        snap_mock = MagicMock(spec=ZFSSnapshot)
        snap_mock.short = 'autosnap_20240101120000'
        snap_info = MagicMock()
        snap_info.name = 'autosnap_20240101120000'
        snap_info.snapshot = snap_mock

        mocker.patch.object(manager, 'list_snapshots', return_value=[snap_info])

        result = rbm.backup_dataset(dsi, remote_cfg)
        assert result is True

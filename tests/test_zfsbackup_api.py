"""Unit tests for the Flask API (zfsbackup.api)."""

import json
import pytest
from unittest.mock import MagicMock, PropertyMock, patch
from datetime import datetime, timedelta, timezone

from libzfseasy.types import Dataset, Filesystem, Snapshot as ZFSSnapshot
from zfsbackup.api import create_app
from zfsbackup.backup_manager import DatasetInfo, DatasetManager, SnapshotInfo
from zfsbackup.config import (
    BackupConfig, DatasetConfig, RetentionRule, RemoteServerConfig,
)


def _make_config(**kwargs) -> BackupConfig:
    defaults = dict(
        datasets=[DatasetConfig(
            name='pool/data',
            frequency=timedelta(hours=1),
            retention_rules=[RetentionRule(timedelta(days=1), timedelta(days=30))],
        )],
        snapshot_prefix='autosnap',
    )
    defaults.update(kwargs)
    return BackupConfig(**defaults)


def _make_dsi(name='pool/data') -> DatasetInfo:
    ds = Dataset(name)
    cfg = DatasetConfig(name=name, frequency=timedelta(hours=1))
    dsi = DatasetInfo(ds, cfg)
    return dsi


@pytest.fixture
def client(mocker):
    config = _make_config()
    dsi = _make_dsi()
    mocker.patch.object(DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[dsi])
    app = create_app(config)
    app.testing = True
    return app.test_client()


@pytest.fixture
def client_with_remote(mocker):
    config = _make_config(
        remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
    )
    dsi = _make_dsi()
    mocker.patch.object(DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[dsi])
    app = create_app(config)
    app.testing = True
    return app.test_client()


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        resp = client.get('/health')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['status'] == 'ok'


class TestConfigEndpoint:
    def test_config_returns_fields(self, client):
        resp = client.get('/config')
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'snapshot_prefix' in data
        assert 'check_interval_seconds' in data
        assert 'prune_interval_seconds' in data
        assert 'api_host' in data
        assert 'api_port' in data
        assert 'dry_run' in data

    def test_config_values_match(self, mocker):
        config = _make_config(snapshot_prefix='testsnap', dry_run=True)
        dsi = _make_dsi()
        mocker.patch.object(DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[dsi])
        app = create_app(config)
        app.testing = True
        resp = app.test_client().get('/config')
        data = resp.get_json()
        assert data['snapshot_prefix'] == 'testsnap'
        assert data['dry_run'] is True


class TestDatasetsEndpoint:
    def test_datasets_returns_list(self, client):
        resp = client.get('/datasets')
        assert resp.status_code == 200
        data = resp.get_json()
        assert isinstance(data, list)
        assert len(data) == 1

    def test_dataset_entry_has_required_fields(self, client):
        resp = client.get('/datasets')
        entry = resp.get_json()[0]
        assert 'name' in entry
        assert 'frequency_seconds' in entry
        assert 'recursive' in entry
        assert 'retention_rules' in entry

    def test_dataset_name_matches(self, client):
        resp = client.get('/datasets')
        assert resp.get_json()[0]['name'] == 'pool/data'


class TestSnapshotsEndpoint:
    def test_snapshots_returns_empty_list(self, mocker):
        config = _make_config()
        dsi = _make_dsi()
        mocker.patch.object(DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[dsi])
        mocker.patch.object(DatasetManager, 'list_snapshots', return_value=[])
        app = create_app(config)
        app.testing = True
        resp = app.test_client().get('/datasets/pool/data/snapshots')
        assert resp.status_code == 200
        assert resp.get_json() == []

    def test_snapshots_returns_404_for_unknown_dataset(self, client):
        resp = client.get('/datasets/pool/unknown/snapshots')
        assert resp.status_code == 404

    def test_snapshots_returns_snapshot_info(self, mocker):
        config = _make_config()
        dsi = _make_dsi()
        mocker.patch.object(DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[dsi])

        fs = Filesystem('pool/data')
        snap = ZFSSnapshot(fs, 'autosnap_20240101120000')
        snap_info = SnapshotInfo(snap, 'autosnap')

        mocker.patch.object(DatasetManager, 'list_snapshots', return_value=[snap_info])
        app = create_app(config)
        app.testing = True
        resp = app.test_client().get('/datasets/pool/data/snapshots')
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data) == 1
        assert data[0]['name'] == 'autosnap_20240101120000'
        assert 'timestamp' in data[0]
        assert 'age_seconds' in data[0]


class TestBackupRegister:
    def test_register_disabled_when_no_remote_config(self, client):
        resp = client.post('/backup/register', json={'client_id': 'testclient'})
        assert resp.status_code == 503

    def test_register_returns_400_without_client_id(self, client_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=False)
        resp = client_with_remote.post('/backup/register', json={})
        assert resp.status_code == 400
        assert 'client_id' in resp.get_json()['error']

    def test_register_creates_filesystem_when_not_exists(self, client_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=False)
        mock_create = mocker.patch('zfsbackup.api.zfs.create')
        resp = client_with_remote.post('/backup/register', json={'client_id': 'testclient'})
        assert resp.status_code == 200
        mock_create.assert_called_once()

    def test_register_skips_create_when_exists(self, client_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mock_create = mocker.patch('zfsbackup.api.zfs.create')
        resp = client_with_remote.post('/backup/register', json={'client_id': 'testclient'})
        assert resp.status_code == 200
        mock_create.assert_not_called()

    def test_register_dry_run_returns_dry_run_flag(self, mocker):
        config = _make_config(
            dry_run=True,
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        )
        dsi = _make_dsi()
        mocker.patch.object(DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[dsi])
        mocker.patch('zfsbackup.api.zfs.exists', return_value=False)
        app = create_app(config)
        app.testing = True
        resp = app.test_client().post('/backup/register', json={'client_id': 'testclient'})
        assert resp.status_code == 200
        assert resp.get_json().get('dry_run') is True

    def test_register_returns_target_dataset(self, client_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        resp = client_with_remote.post('/backup/register', json={'client_id': 'testclient'})
        data = resp.get_json()
        assert 'target_dataset' in data
        assert 'testclient' in data['target_dataset']


class TestBackupNegotiate:
    def test_negotiate_disabled_when_no_remote(self, client, mocker):
        resp = client.post('/backup/client1/pool/data/negotiate', json={})
        assert resp.status_code == 503

    def test_negotiate_finds_common_snapshot(self, client_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mock_snap = MagicMock()
        mock_snap.short = 'autosnap_20240101120000'
        mocker.patch('zfsbackup.api.zfs.list', return_value=[mock_snap])

        resp = client_with_remote.post(
            '/backup/client1/pool/data/negotiate',
            json={'snapshots': ['autosnap_20240101120000', 'autosnap_20240102120000']},
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'server_dataset' in data
        assert data['common_snapshot'] == 'autosnap_20240101120000'

    def test_negotiate_returns_none_common_when_no_overlap(self, client_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mock_snap = MagicMock()
        mock_snap.short = 'autosnap_99990101120000'
        mocker.patch('zfsbackup.api.zfs.list', return_value=[mock_snap])

        resp = client_with_remote.post(
            '/backup/client1/pool/data/negotiate',
            json={'snapshots': ['autosnap_20240101120000']},
        )
        assert resp.status_code == 200
        assert resp.get_json()['common_snapshot'] is None

    def test_negotiate_creates_server_dataset_when_config_provided(self, client_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=False)
        mock_create = mocker.patch('zfsbackup.api.zfs.create')
        mock_set = mocker.patch('zfsbackup.api.zfs.set')
        mocker.patch('zfsbackup.api.zfs.list', return_value=[])
        from zfsbackup.config import DatasetConfig
        encoded = DatasetConfig(name='pool/data').to_property()

        resp = client_with_remote.post(
            '/backup/client1/pool/data/negotiate',
            json={'snapshots': [], 'config': encoded},
        )
        assert resp.status_code == 200
        mock_create.assert_called_once()
        mock_set.assert_called_once()


class TestResumeToken:
    def test_returns_503_when_disabled(self, client):
        resp = client.get('/backup/client1/pool/data/resume_token')
        assert resp.status_code == 503

    def test_returns_none_token_when_dataset_not_exists(self, client_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=False)
        resp = client_with_remote.get('/backup/client1/pool/data/resume_token')
        assert resp.status_code == 200
        assert resp.get_json()['resume_token'] is None

    def test_returns_token_when_present(self, client_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mock_prop = MagicMock()
        mock_prop.__str__ = lambda self: 'abc123resumetoken'
        mock_ds = MagicMock()
        mock_ds.__getitem__ = MagicMock(return_value=mock_prop)
        mocker.patch('zfsbackup.api.zfs.list', return_value=[mock_ds])
        resp = client_with_remote.get('/backup/client1/pool/data/resume_token')
        assert resp.status_code == 200
        assert resp.get_json()['resume_token'] == 'abc123resumetoken'

    def test_returns_none_when_token_is_none_string(self, client_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mock_prop = MagicMock()
        mock_prop.__str__ = lambda self: 'none'
        mock_ds = MagicMock()
        mock_ds.__getitem__ = MagicMock(return_value=mock_prop)
        mocker.patch('zfsbackup.api.zfs.list', return_value=[mock_ds])
        resp = client_with_remote.get('/backup/client1/pool/data/resume_token')
        assert resp.status_code == 200
        assert resp.get_json()['resume_token'] is None

"""Unit tests for the Flask API (zfsbackup.api)."""

import json
import logging
import threading
import pytest
from unittest.mock import MagicMock, PropertyMock, patch
from datetime import datetime, timedelta, timezone
from simple_websocket import ConnectionClosed

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

    def test_prune_interval_seconds_is_a_number_when_unset(self, mocker):
        # config/model.py item 3c.3: BackupConfig.prune_interval defaults to None
        # ("derive from check_interval"). The /config endpoint uses
        # effective_prune_interval, so it must still emit a JSON number
        # here, not null/None -- a client parsing this response should
        # never have to special-case a missing prune interval.
        config = _make_config()
        assert config.prune_interval is None
        dsi = _make_dsi()
        mocker.patch.object(
            DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[dsi]
        )
        app = create_app(config)
        app.testing = True
        resp = app.test_client().get('/config')
        data = resp.get_json()
        assert data['prune_interval_seconds'] is not None
        assert isinstance(data['prune_interval_seconds'], (int, float))
        assert data['prune_interval_seconds'] == config.check_interval.total_seconds()

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

    def test_negotiate_returns_500_on_snapshot_list_error(self, client_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.api.zfs.list', side_effect=Exception('zfs error'))
        resp = client_with_remote.post(
            '/backup/client1/pool/data/negotiate',
            json={'snapshots': ['autosnap_20240101120000']},
        )
        assert resp.status_code == 500
        assert 'error' in resp.get_json()

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

    def test_returns_500_on_zfs_list_error(self, client_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.api.zfs.list', side_effect=Exception('zfs error'))
        resp = client_with_remote.get('/backup/client1/pool/data/resume_token')
        assert resp.status_code == 500
        assert 'error' in resp.get_json()


class TestBackupStream:
    """Tests for the backup_stream WebSocket endpoint.

    The handler is a closure inside create_app; we access it via
    app.view_functions['backup_stream'].__wrapped__ which is the original
    function accepting (ws, client_id, dataset).
    """

    def _get_handler(self, config, mocker):
        mocker.patch.object(DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[])
        app = create_app(config)
        return app.view_functions['backup_stream'].__wrapped__, app

    def test_stream_disabled_sends_error(self, mocker):
        config = _make_config()  # no remote_backup
        handler, app = self._get_handler(config, mocker)
        ws = MagicMock()
        with app.test_request_context(query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')
        sent = json.loads(ws.send.call_args[0][0])
        assert 'error' in sent

    def test_stream_disabled_ws_send_exception_suppressed(self, mocker):
        config = _make_config()  # no remote_backup
        handler, app = self._get_handler(config, mocker)
        ws = MagicMock()
        ws.send.side_effect = Exception('ws already closed')
        with app.test_request_context(query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')  # must not raise

    def test_stream_creates_filesystem_when_not_exists(self, mocker):
        config = _make_config(
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        )
        handler, app = self._get_handler(config, mocker)
        mocker.patch('zfsbackup.api.zfs.exists', return_value=False)
        mock_create = mocker.patch('zfsbackup.api.zfs.create')
        mocker.patch('zfsbackup.api.zfs.set')

        mock_writer = MagicMock()
        mocker.patch('zfsbackup.api.zfs.receive.filesystem', return_value=mock_writer)
        ws = MagicMock()
        ws.receive.side_effect = [json.dumps({'done': True})]

        with app.test_request_context(query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')
        mock_create.assert_called_once()

    def test_stream_receive_startup_failure_sends_error_and_releases_the_gate(self, mocker):
        """`zfs.receive.filesystem` raises when `zfs receive` cannot start (it
        never returns None)."""
        config = _make_config(
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        )
        handler, app = self._get_handler(config, mocker)
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mocker.patch(
            'zfsbackup.api.zfs.receive.filesystem',
            side_effect=RuntimeError('cannot receive: dataset is busy'),
        )
        ws = MagicMock()
        with app.test_request_context(query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')
        ws.send.assert_called_once()
        assert json.loads(ws.send.call_args[0][0]) == {
            'error': 'Failed to start zfs receive: cannot receive: dataset is busy'
        }
        ws.receive.assert_not_called()
        assert app.extensions['zfsbackup.stream_gate'].active == 0

    def test_stream_dry_run_sends_ok(self, mocker):
        config = _make_config(
            dry_run=True,
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        )
        handler, app = self._get_handler(config, mocker)
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        ws = MagicMock()
        with app.test_request_context(query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')
        sent = json.loads(ws.send.call_args[0][0])
        assert sent.get('status') == 'ok'
        assert sent.get('dry_run') is True

    def test_stream_success_writes_chunks_and_closes(self, mocker):
        config = _make_config(
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        )
        handler, app = self._get_handler(config, mocker)
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)

        mock_writer = MagicMock()
        mocker.patch('zfsbackup.api.zfs.receive.filesystem', return_value=mock_writer)

        ws = MagicMock()
        ws.receive.side_effect = [b'chunk1', b'chunk2', json.dumps({'done': True})]

        with app.test_request_context(query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')

        mock_writer.write.assert_any_call(b'chunk1')
        mock_writer.write.assert_any_call(b'chunk2')
        mock_writer.close.assert_called_once()
        sent = json.loads(ws.send.call_args[0][0])
        assert sent.get('status') == 'ok'

    def test_stream_connection_closed_closes_writer(self, mocker):
        config = _make_config(
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        )
        handler, app = self._get_handler(config, mocker)
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)

        mock_writer = MagicMock()
        mocker.patch('zfsbackup.api.zfs.receive.filesystem', return_value=mock_writer)

        ws = MagicMock()
        ws.receive.side_effect = ConnectionClosed(None, None)

        with app.test_request_context(query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')

        mock_writer.close.assert_called_once()

    def test_stream_write_exception_sends_error(self, mocker):
        config = _make_config(
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        )
        handler, app = self._get_handler(config, mocker)
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)

        mock_writer = MagicMock()
        mock_writer.write.side_effect = IOError('disk full')
        mocker.patch('zfsbackup.api.zfs.receive.filesystem', return_value=mock_writer)

        ws = MagicMock()
        ws.receive.return_value = b'chunk'

        with app.test_request_context(query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')

        mock_writer.close.assert_called_once()
        error_sent = json.loads(ws.send.call_args[0][0])
        assert 'error' in error_sent


class TestBackupStreamWriteFailure:
    """A write into `zfs receive` fails: the pipe error is the symptom, the
    diagnostic is whatever `writer.close()` raises (it reaps the process)."""

    def _setup(self, mocker, writer):
        config = _make_config(
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        )
        mocker.patch.object(DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[])
        app = create_app(config)
        handler = app.view_functions['backup_stream'].__wrapped__
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.api.zfs.receive.filesystem', return_value=writer)
        return handler, app

    def _run(self, handler, app, ws):
        with app.test_request_context(query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')

    def test_the_receive_diagnostic_replaces_the_pipe_error_and_both_are_logged(
        self, mocker, caplog
    ):
        writer = MagicMock()
        writer.write.side_effect = BrokenPipeError(32, 'Broken pipe')
        writer.close.side_effect = RuntimeError('cannot receive: checksum mismatch')
        handler, app = self._setup(mocker, writer)
        ws = MagicMock()
        ws.receive.return_value = b'chunk'
        with caplog.at_level(logging.ERROR, logger='zfsbackup.api'):
            self._run(handler, app, ws)

        assert json.loads(ws.send.call_args[0][0]) == {
            'error': 'cannot receive: checksum mismatch'
        }
        ws.send.assert_called_once()
        writer.write.assert_called_once_with(b'chunk')
        writer.close.assert_called_once()
        errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(errors) == 1
        assert 'cannot receive: checksum mismatch' in errors[0]
        assert 'Broken pipe' in errors[0]
        assert 'client1' in errors[0]            # the server dataset is named

    def test_a_pipe_error_with_a_clean_close_is_reported_as_the_pipe_error(self, mocker, caplog):
        writer = MagicMock()
        writer.write.side_effect = OSError(32, 'Broken pipe')
        handler, app = self._setup(mocker, writer)       # close() raises nothing
        ws = MagicMock()
        ws.receive.return_value = b'chunk'
        with caplog.at_level(logging.ERROR, logger='zfsbackup.api'):
            self._run(handler, app, ws)

        assert json.loads(ws.send.call_args[0][0]) == {'error': '[Errno 32] Broken pipe'}
        ws.send.assert_called_once()
        writer.close.assert_called_once()
        errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(errors) == 1 and 'Broken pipe' in errors[0]

    def test_a_failure_after_some_good_chunks_still_closes_exactly_once(self, mocker):
        writer = MagicMock()
        writer.write.side_effect = [None, None, BrokenPipeError(32, 'Broken pipe')]
        writer.close.side_effect = RuntimeError('cannot receive: out of space')
        handler, app = self._setup(mocker, writer)
        ws = MagicMock()
        ws.receive.side_effect = [b'a', b'b', b'c', b'never read']
        self._run(handler, app, ws)
        assert writer.write.call_count == 3
        writer.close.assert_called_once()
        assert json.loads(ws.send.call_args[0][0]) == {'error': 'cannot receive: out of space'}
        assert ws.receive.call_count == 3                # nothing read after the failure

    def test_a_non_oserror_write_failure_closes_once_via_the_general_path(self, mocker):
        writer = MagicMock()
        writer.write.side_effect = ValueError('bad chunk')
        handler, app = self._setup(mocker, writer)
        ws = MagicMock()
        ws.receive.return_value = b'chunk'
        self._run(handler, app, ws)
        writer.close.assert_called_once()
        assert json.loads(ws.send.call_args[0][0]) == {'error': 'bad chunk'}

    def test_the_success_path_closes_then_says_ok_unchanged(self, mocker):
        order = []
        writer = MagicMock()
        writer.write.side_effect = lambda d: order.append(('write', d))
        writer.close.side_effect = lambda: order.append(('close',))
        handler, app = self._setup(mocker, writer)
        ws = MagicMock()
        ws.send.side_effect = lambda m: order.append(('send', json.loads(m)))
        ws.receive.side_effect = [b'x', json.dumps({'done': True})]
        self._run(handler, app, ws)
        assert order == [('write', b'x'), ('close',), ('send', {'status': 'ok'})]

    def test_a_failing_close_after_done_reports_the_error_not_ok(self, mocker):
        writer = MagicMock()
        writer.close.side_effect = RuntimeError('cannot receive: bad stream')
        handler, app = self._setup(mocker, writer)
        ws = MagicMock()
        ws.receive.side_effect = [b'x', json.dumps({'done': True})]
        self._run(handler, app, ws)
        assert json.loads(ws.send.call_args[0][0]) == {'error': 'cannot receive: bad stream'}
        ws.send.assert_called_once()
        writer.close.assert_called_once()                # reaped before the close: no second close

    @pytest.mark.parametrize(
        'close_error',
        [RuntimeError('cannot receive: checksum mismatch'), None],
        ids=['close-raises', 'close-clean'],
    )
    def test_the_stream_gate_is_balanced_on_both_write_failure_paths(self, mocker, close_error):
        writer = MagicMock()
        writer.write.side_effect = BrokenPipeError(32, 'Broken pipe')
        writer.close.side_effect = close_error
        handler, app = self._setup(mocker, writer)
        gate = app.extensions['zfsbackup.stream_gate']
        during = []
        ws = MagicMock()

        def receive(timeout=None):
            during.append(gate.active)
            return b'chunk'

        ws.receive.side_effect = receive
        assert gate.active == 0
        self._run(handler, app, ws)
        assert during == [1]                 # registered while the stream ran...
        assert gate.active == 0              # ...and released afterwards

    def test_the_gate_is_balanced_after_several_failing_streams(self, mocker):
        writer = MagicMock()
        writer.write.side_effect = BrokenPipeError(32, 'Broken pipe')
        writer.close.side_effect = RuntimeError('cannot receive: x')
        handler, app = self._setup(mocker, writer)
        gate = app.extensions['zfsbackup.stream_gate']
        for _ in range(5):
            ws = MagicMock()
            ws.receive.return_value = b'chunk'
            self._run(handler, app, ws)
        assert gate.active == 0
        assert gate.begin() is True           # not wedged
        gate.end()

    def test_the_gate_is_released_even_when_the_handler_itself_raises(self, mocker, source_props):
        """A refusal that cannot even be sent (the socket is gone) escapes
        `_receive_stream_locked`; the dataset lock and the gate's `finally`
        must still release."""
        source_props.return_value = {'org.zfsbackup:source_dataset': 'tank/data'}
        handler, app = self._setup(mocker, MagicMock())
        gate = app.extensions['zfsbackup.stream_gate']
        ws = MagicMock()
        ws.receive.side_effect = ConnectionClosed(None, None)    # a regression must fail, not spin
        ws.send.side_effect = RuntimeError('socket gone')
        with pytest.raises(RuntimeError, match='socket gone'):
            self._run(handler, app, ws)
        assert gate.active == 0
        # ...and the per-dataset lock: the next stream for it is not 'busy'.
        source_props.return_value = {}
        ws2 = MagicMock()
        ws2.receive.side_effect = [json.dumps({'done': True})]
        self._run(handler, app, ws2)
        assert json.loads(ws2.send.call_args[0][0]) == {'status': 'ok'}

    def test_a_client_that_vanishes_before_the_ok_does_not_cause_a_second_close(self, mocker):
        """The stream was written and `close()` succeeded; only the final
        `{'status': 'ok'}` hits a closed socket."""
        writer = MagicMock()
        handler, app = self._setup(mocker, writer)
        gate = app.extensions['zfsbackup.stream_gate']
        ws = MagicMock()
        ws.receive.side_effect = [b'x', json.dumps({'done': True})]
        ws.send.side_effect = ConnectionClosed(None, None)
        self._run(handler, app, ws)                      # must not raise
        writer.write.assert_called_once_with(b'x')
        writer.close.assert_called_once()
        ws.send.assert_called_once()
        assert json.loads(ws.send.call_args[0][0]) == {'status': 'ok'}
        assert gate.active == 0


# ---------------------------------------------------------------------------
# Protocol: ?mode=fresh|resume, partial-receive discard, source-dataset check
# ---------------------------------------------------------------------------

SERVER_DS = 'pool/backups/client1/data'     # target / client id / the client dataset minus its pool
TOKEN = (
    '1-a292306e5-b8-789c636064000310a500c4ec50360710e72765a52697303030419460caa7a515a7968'
    '064cae0f26c48f2499525a9c520f1d00aacfa4bf2d34b33531818f238834cbe7b33276a20c97382e5f31'
    '2735319188a4a0af48b53f352f4131d8a0da16e0100ed3915db'
)
MODE_HINT = 'use ?mode=fresh or ?mode=resume'


@pytest.fixture(autouse=True)
def source_props(mocker):
    """No recorded `org.zfsbackup:source_dataset` unless a test says so, and
    never a real `zfs get`. Tests that need one set `.return_value`."""
    return mocker.patch('zfsbackup.api.read_zfsbackup_properties', return_value={})


def _remote_config(**kwargs):
    return _make_config(
        remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True), **kwargs
    )


def _streams_in_progress(app):
    """The live `streams_in_progress` set of `create_app`'s closure."""
    import inspect

    handler = app.view_functions['backup_stream'].__wrapped__
    receive = inspect.getclosurevars(handler).nonlocals['_receive_stream']
    claim = inspect.getclosurevars(receive).nonlocals['_claim_dataset']
    return inspect.getclosurevars(claim).nonlocals['streams_in_progress']


class Stream:
    """The `stream` handler of one app, driven directly with a mock websocket."""

    def __init__(self, mocker, **config_kwargs):
        mocker.patch.object(DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[])
        self.app = create_app(_remote_config(**config_kwargs))
        self.handler = self.app.view_functions['backup_stream'].__wrapped__
        self.gate = self.app.extensions['zfsbackup.stream_gate']
        self.ws = MagicMock()
        self.ws.receive.side_effect = [b'chunk', json.dumps({'done': True})]
        self.order = []
        self.exists = mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        self.list = mocker.patch('zfsbackup.api.zfs.list', return_value=[])
        self.create = mocker.patch('zfsbackup.api.zfs.create')
        self.set = mocker.patch('zfsbackup.api.zfs.set')
        self.abort = mocker.patch('zfsbackup.api.zfs.receive.abort')
        self.writer = MagicMock()
        self.receive = mocker.patch('zfsbackup.api.zfs.receive.filesystem', return_value=self.writer)
        self.create.side_effect = lambda *a, **k: self.order.append('create')
        self.set.side_effect = lambda *a, **k: self.order.append('set')
        self.abort.side_effect = lambda *a, **k: self.order.append('abort')
        self.receive.side_effect = lambda *a, **k: (self.order.append('receive'), self.writer)[1]

    @property
    def claimed(self):
        """A copy of the set of server datasets with a stream in progress
        (the app's `streams_in_progress`, found through the handler's closures)."""
        return set(_streams_in_progress(self.app))

    def token(self, value):
        """The server dataset reports `value` as its receive_resume_token."""
        row = MagicMock()
        row.__getitem__ = MagicMock(return_value=value)
        self.list.return_value = [row]

    def run(self, mode='fresh', *, ws=None, client='client1', dataset='pool/data', **query):
        qs = dict(query)
        if mode is not None:
            qs['mode'] = mode
        with self.app.test_request_context(query_string=qs):
            self.handler(ws if ws is not None else self.ws, client, dataset)

    @property
    def sent(self):
        return [json.loads(c[0][0]) for c in self.ws.send.call_args_list]

    def untouched(self):
        for m in (self.exists, self.list, self.create, self.set, self.abort, self.receive):
            m.assert_not_called()
        self.ws.receive.assert_not_called()


@pytest.fixture
def stream(mocker):
    return Stream(mocker)


class TestStreamModeIsRequired:
    def test_a_missing_mode_is_refused_with_the_exact_message_and_nothing_runs(self, stream):
        stream.run(mode=None)
        assert stream.sent == [{'error': f'stream mode required: {MODE_HINT}'}]
        stream.untouched()
        assert stream.gate.active == 0

    @pytest.mark.parametrize('mode', ['x', 'FRESH', 'Resume', '', 'fresh,resume', 'fresh '])
    def test_an_unknown_mode_is_refused_with_the_exact_message_and_nothing_runs(self, stream, mode):
        stream.run(mode=mode)
        assert stream.sent == [{'error': f'unknown stream mode {mode!r}: {MODE_HINT}'}]
        stream.untouched()
        assert stream.gate.active == 0

    def test_the_mode_is_checked_before_the_gate(self, stream):
        """A draining worker answers the protocol error, not 'reloading': the
        client must fix its request, not retry it."""
        stream.gate.start_draining()
        stream.run(mode=None)
        assert stream.sent == [{'error': f'stream mode required: {MODE_HINT}'}]
        stream.run(mode='bogus')
        assert stream.sent[-1] == {'error': f"unknown stream mode 'bogus': {MODE_HINT}"}
        stream.untouched()
        assert stream.gate.active == 0

    def test_a_valid_mode_is_still_refused_while_draining(self, stream):
        stream.gate.start_draining()
        stream.run(mode='fresh')
        assert stream.sent == [{'error': 'server is reloading; retry later', 'retry': True}]
        stream.untouched()

    def test_a_refusal_survives_a_dead_socket(self, stream):
        stream.ws.send.side_effect = Exception('closed')
        stream.run(mode=None)                   # must not raise
        stream.run(mode='x')
        stream.untouched()

    def test_a_disabled_server_still_says_disabled_first(self, mocker):
        mocker.patch.object(DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[])
        app = create_app(_make_config())
        ws = MagicMock()
        with app.test_request_context(query_string={'mode': 'bogus'}):
            app.view_functions['backup_stream'].__wrapped__(ws, 'c', 'pool/data')
        assert json.loads(ws.send.call_args[0][0]) == {'error': 'remote_backup disabled'}

    @pytest.mark.parametrize('mode', ['fresh', 'resume'])
    def test_both_valid_modes_receive_the_stream(self, stream, mode):
        stream.run(mode=mode)
        assert stream.sent == [{'status': 'ok'}]
        stream.writer.write.assert_called_once_with(b'chunk')
        stream.writer.close.assert_called_once()
        stream.receive.assert_called_once()
        assert stream.gate.active == 0

    def test_from_is_optional_and_only_logged_at_debug(self, stream, caplog):
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.api'):
            stream.run(mode='resume', **{'from': 'autosnap_20240101120000'})
        assert stream.sent == [{'status': 'ok'}]
        records = [r for r in caplog.records if 'autosnap_20240101120000' in r.getMessage()]
        assert records and all(r.levelno == logging.DEBUG for r in records)
        assert 'mode=resume' in records[0].getMessage()

    def test_without_from_it_still_works(self, stream, caplog):
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.api'):
            stream.run(mode='resume')
        assert stream.sent == [{'status': 'ok'}]
        assert not [r for r in caplog.records if r.levelno >= logging.INFO]


class TestFreshStreamDiscardsPartialState:
    def test_fresh_with_a_token_warns_aborts_then_creates_and_receives_in_that_order(
        self, stream, caplog
    ):
        stream.token(TOKEN)
        stream.exists.side_effect = [True, True, False]      # source check, token lookup, after the abort
        with caplog.at_level(logging.INFO, logger='zfsbackup.api'):
            stream.run('fresh')
        assert stream.order == ['abort', 'create', 'set', 'receive']
        stream.abort.assert_called_once()
        assert str(stream.abort.call_args[0][0]) == SERVER_DS
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings == [
            f'discarding the partial receive state of {SERVER_DS}: the client is sending a fresh stream'
        ]
        assert stream.sent == [{'status': 'ok'}]
        assert stream.gate.active == 0

    def test_fresh_with_a_token_and_a_surviving_dataset_does_not_recreate_it(self, stream):
        stream.token(TOKEN)
        stream.run('fresh')                                  # an incremental's dataset survives the abort
        assert stream.order == ['abort', 'receive']
        stream.create.assert_not_called()

    def test_fresh_without_a_token_neither_warns_nor_aborts(self, stream, caplog):
        stream.token(None)
        with caplog.at_level(logging.INFO, logger='zfsbackup.api'):
            stream.run('fresh')
        stream.abort.assert_not_called()
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert stream.order == ['receive']

    def test_fresh_with_no_dataset_creates_it_and_never_looks_for_a_token(self, stream):
        stream.exists.return_value = False
        stream.run('fresh')
        stream.list.assert_not_called()
        stream.abort.assert_not_called()
        assert stream.order == ['create', 'set', 'receive']

    def test_the_token_none_string_means_no_state(self, stream):
        stream.token('none')
        stream.run('fresh')
        stream.abort.assert_not_called()

    def test_fresh_looks_the_token_up_by_name_alone(self, stream):
        stream.token(None)
        stream.run('fresh')
        kwargs = stream.list.call_args.kwargs
        assert str(kwargs['roots']) == SERVER_DS
        assert kwargs['properties'] == ['receive_resume_token']
        assert 'types' not in kwargs                  # a filesystem OR a volume

    def test_resume_never_aborts_and_never_looks_for_a_token(self, stream):
        stream.token(TOKEN)
        stream.run('resume')
        stream.abort.assert_not_called()
        stream.list.assert_not_called()
        assert stream.order == ['receive']
        assert stream.sent == [{'status': 'ok'}]

    @pytest.mark.parametrize('text', [
        "cannot receive: 'pool/backups/client1/data' does not have any resumable receive state to abort",
        "cannot open 'pool/backups/client1/data': dataset does not exist",
        "CANNOT OPEN 'x': Dataset Does Not Exist",
    ])
    def test_an_abort_that_finds_nothing_to_abort_is_a_clean_success(self, stream, text, caplog):
        stream.token(TOKEN)
        stream.abort.side_effect = Exception(text)
        stream.exists.side_effect = [True, True, False]
        with caplog.at_level(logging.INFO, logger='zfsbackup.api'):
            stream.run('fresh')
        assert stream.sent == [{'status': 'ok'}]
        stream.create.assert_called_once()                    # the abort destroyed the dataset
        stream.receive.assert_called_once()
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_any_other_abort_error_refuses_the_stream(self, stream, caplog):
        stream.token(TOKEN)
        stream.abort.side_effect = Exception('cannot abort: pool I/O is currently suspended')
        with caplog.at_level(logging.INFO, logger='zfsbackup.api'):
            stream.run('fresh')
        assert stream.sent == [{
            'error': f'Failed to discard the partial receive state of {SERVER_DS}: '
                     'cannot abort: pool I/O is currently suspended'
        }]
        stream.create.assert_not_called()
        stream.receive.assert_not_called()
        stream.ws.receive.assert_not_called()
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1 and 'pool I/O is currently suspended' in errors[0]
        assert stream.gate.active == 0

    def test_a_token_lookup_failure_refuses_the_stream_and_aborts_nothing(self, stream):
        stream.list.side_effect = RuntimeError('boom')
        stream.run('fresh')
        assert stream.sent == [{
            'error': f'Failed to check {SERVER_DS} for partial receive state: boom'
        }]
        stream.abort.assert_not_called()
        stream.create.assert_not_called()
        stream.receive.assert_not_called()
        assert stream.gate.active == 0

    def test_an_exists_failure_during_the_token_lookup_is_reported_the_same_way(self, stream):
        stream.exists.side_effect = [True, RuntimeError('exists failed')]
        stream.run('fresh')
        assert stream.sent == [{
            'error': f'Failed to check {SERVER_DS} for partial receive state: exists failed'
        }]
        stream.receive.assert_not_called()

    def test_a_volume_is_aborted_like_a_filesystem(self, stream):
        stream.token(TOKEN)
        stream.run('fresh', dataset='pool/vol')
        assert str(stream.abort.call_args[0][0]) == 'pool/backups/client1/vol'


class TestDryRunAndOrder:
    def test_a_dry_run_replies_ok_before_any_abort_create_or_receive(self, mocker):
        s = Stream(mocker, dry_run=True)
        s.token(TOKEN)
        s.run('fresh')
        assert s.sent == [{'status': 'ok', 'dry_run': True}]
        s.list.assert_not_called()
        for m in (s.abort, s.create, s.set, s.receive):
            m.assert_not_called()
        s.ws.receive.assert_not_called()
        assert s.gate.active == 0

    def test_a_dry_run_still_runs_the_source_check_first(self, mocker, source_props):
        s = Stream(mocker, dry_run=True)
        source_props.return_value = {'org.zfsbackup:source_dataset': 'tank/other'}
        s.run('fresh')
        assert s.sent == [{
            'error': f"server dataset {SERVER_DS} already holds backups of 'tank/other'; "
                     "refusing to mix in 'pool/data'"
        }]

    def test_the_order_is_source_check_then_token_lookup_then_abort_then_create_then_receive(
        self, stream, source_props
    ):
        events = []
        source_props.side_effect = lambda ds: events.append('source') or {}
        stream.token(TOKEN)
        stream.list.side_effect = lambda **k: events.append('token') or [
            MagicMock(__getitem__=MagicMock(return_value=TOKEN))
        ]
        stream.abort.side_effect = lambda *a: events.append('abort')
        stream.create.side_effect = lambda *a, **k: events.append('create')
        stream.set.side_effect = lambda *a, **k: events.append('set')
        stream.receive.side_effect = lambda *a, **k: (events.append('receive'), stream.writer)[1]
        stream.exists.side_effect = [True, True, False]
        stream.run('fresh')
        assert events == ['source', 'token', 'abort', 'create', 'set', 'receive']


class TestSourceDatasetCheck:
    MISMATCH = (
        f"server dataset {SERVER_DS} already holds backups of 'tank/data'; "
        "refusing to mix in 'pool/data'"
    )

    def test_negotiate_refuses_a_different_recorded_source_with_409_and_writes_nothing(
        self, client_with_remote, mocker, source_props
    ):
        source_props.return_value = {'org.zfsbackup:source_dataset': 'tank/data'}
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mock_set = mocker.patch('zfsbackup.api.zfs.set')
        mock_create = mocker.patch('zfsbackup.api.zfs.create')
        mock_list = mocker.patch('zfsbackup.api.zfs.list', return_value=[])
        encoded = DatasetConfig(name='pool/data').to_property()
        resp = client_with_remote.post(
            '/backup/client1/pool/data/negotiate', json={'snapshots': ['a'], 'config': encoded}
        )
        assert resp.status_code == 409
        assert resp.get_json() == {'error': self.MISMATCH}
        mock_set.assert_not_called()
        mock_create.assert_not_called()
        mock_list.assert_not_called()

    def test_negotiate_allows_a_dataset_with_no_recorded_source_and_records_it(
        self, client_with_remote, mocker
    ):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mock_set = mocker.patch('zfsbackup.api.zfs.set')
        mocker.patch('zfsbackup.api.zfs.list', return_value=[])
        encoded = DatasetConfig(name='pool/data').to_property()
        resp = client_with_remote.post(
            '/backup/client1/pool/data/negotiate', json={'snapshots': [], 'config': encoded}
        )
        assert resp.status_code == 200
        mock_set.assert_called_once()
        assert mock_set.call_args[0][1]['org.zfsbackup:source_dataset'] == 'pool/data'

    def test_negotiate_allows_the_same_recorded_source(
        self, client_with_remote, mocker, source_props
    ):
        source_props.return_value = {'org.zfsbackup:source_dataset': 'pool/data'}
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.api.zfs.set')
        mocker.patch('zfsbackup.api.zfs.list', return_value=[])
        resp = client_with_remote.post('/backup/client1/pool/data/negotiate', json={'snapshots': []})
        assert resp.status_code == 200

    def test_negotiate_on_a_missing_dataset_reads_no_properties(
        self, client_with_remote, mocker, source_props
    ):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=False)
        mocker.patch('zfsbackup.api.zfs.create')
        mocker.patch('zfsbackup.api.zfs.set')
        mocker.patch('zfsbackup.api.zfs.list', return_value=[])
        encoded = DatasetConfig(name='pool/data').to_property()
        resp = client_with_remote.post(
            '/backup/client1/pool/data/negotiate', json={'snapshots': [], 'config': encoded}
        )
        assert resp.status_code == 200
        source_props.assert_not_called()

    def test_negotiate_property_read_failure_is_a_500_and_writes_nothing(
        self, client_with_remote, mocker, source_props
    ):
        source_props.side_effect = RuntimeError('zfs get failed')
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mock_set = mocker.patch('zfsbackup.api.zfs.set')
        encoded = DatasetConfig(name='pool/data').to_property()
        resp = client_with_remote.post(
            '/backup/client1/pool/data/negotiate', json={'snapshots': [], 'config': encoded}
        )
        assert resp.status_code == 500
        assert resp.get_json() == {'error': f'Failed to check {SERVER_DS}: zfs get failed'}
        mock_set.assert_not_called()

    def test_negotiate_releases_the_gate_on_409_and_500(
        self, client_with_remote, mocker, source_props
    ):
        gate = client_with_remote.application.extensions['zfsbackup.stream_gate']
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        source_props.return_value = {'org.zfsbackup:source_dataset': 'tank/data'}
        assert client_with_remote.post('/backup/c/pool/data/negotiate', json={}).status_code == 409
        assert gate.active == 0
        source_props.side_effect = RuntimeError('x')
        assert client_with_remote.post('/backup/c/pool/data/negotiate', json={}).status_code == 500
        assert gate.active == 0

    def test_the_server_dataset_drops_the_pool_so_two_pools_collide(self, stream, source_props):
        """The reason for the check: tank/data and pool/data both map to
        `<target>/<client>/data`."""
        source_props.return_value = {'org.zfsbackup:source_dataset': 'tank/data'}
        stream.run('fresh')
        assert stream.sent == [{'error': self.MISMATCH}]

    @pytest.mark.parametrize('mode', ['fresh', 'resume'])
    def test_stream_refuses_a_mismatch_before_anything_else(self, stream, source_props, mode):
        source_props.return_value = {'org.zfsbackup:source_dataset': 'tank/data'}
        stream.token(TOKEN)
        stream.run(mode)
        assert stream.sent == [{'error': self.MISMATCH}]
        stream.list.assert_not_called()
        for m in (stream.abort, stream.create, stream.receive):
            m.assert_not_called()
        stream.ws.receive.assert_not_called()
        assert stream.gate.active == 0

    def test_stream_allows_no_recorded_source_and_a_matching_one(self, stream, source_props):
        stream.run('resume')
        assert stream.sent == [{'status': 'ok'}]
        stream.ws.receive.side_effect = [b'chunk', json.dumps({'done': True})]
        source_props.return_value = {'org.zfsbackup:source_dataset': 'pool/data'}
        stream.run('resume')
        assert stream.sent[-1] == {'status': 'ok'}

    def test_stream_property_read_failure_is_an_error_and_nothing_runs(self, stream, source_props):
        source_props.side_effect = RuntimeError('zfs get failed')
        stream.run('fresh')
        assert stream.sent == [{'error': f'Failed to check {SERVER_DS}: zfs get failed'}]
        stream.list.assert_not_called()
        for m in (stream.abort, stream.create, stream.receive):
            m.assert_not_called()
        assert stream.gate.active == 0

    def test_stream_on_a_missing_dataset_reads_no_properties(self, stream, source_props):
        stream.exists.return_value = False
        stream.run('resume')
        source_props.assert_not_called()
        assert stream.sent == [{'status': 'ok'}]

    def test_the_property_is_read_for_the_server_dataset(self, stream, source_props):
        stream.run('resume')
        assert str(source_props.call_args[0][0]) == SERVER_DS


# ---------------------------------------------------------------------------
# /resume_token through the REAL ListCommand (only Popen is mocked)
# ---------------------------------------------------------------------------


class TestResumeTokenThroughTheRealList:
    URL = '/backup/client1/pool/data/resume_token'

    def get(self, client, mocker, mock_subprocess, *rows):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mock_subprocess.setup(stdout=list(rows))
        return client.get(self.URL)

    def argv(self, mock_subprocess):
        return mock_subprocess.call_args_list[0][0][0]

    def test_a_filesystem_with_a_token(self, client_with_remote, mocker, mock_subprocess):
        resp = self.get(client_with_remote, mocker, mock_subprocess, f'{SERVER_DS}\tfilesystem\t{TOKEN}\n')
        assert resp.status_code == 200
        assert resp.get_json() == {'resume_token': TOKEN}
        argv = self.argv(mock_subprocess)
        assert argv[1:3] == ['list', '-H']
        assert argv[argv.index('-o') + 1] == 'name,type,receive_resume_token'
        assert argv[-1] == SERVER_DS
        assert '-t' not in argv                  # no type filter: volumes are found by name too

    def test_a_dash_row_is_no_token(self, client_with_remote, mocker, mock_subprocess):
        resp = self.get(client_with_remote, mocker, mock_subprocess, f'{SERVER_DS}\tfilesystem\t-\n')
        assert resp.status_code == 200
        assert resp.get_json() == {'resume_token': None}

    def test_a_volume_with_a_token(self, client_with_remote, mocker, mock_subprocess):
        resp = self.get(client_with_remote, mocker, mock_subprocess, f'{SERVER_DS}\tvolume\t{TOKEN}\n')
        assert resp.status_code == 200
        assert resp.get_json() == {'resume_token': TOKEN}

    def test_a_volume_dash_row_is_no_token(self, client_with_remote, mocker, mock_subprocess):
        resp = self.get(client_with_remote, mocker, mock_subprocess, f'{SERVER_DS}\tvolume\t-\n')
        assert resp.get_json() == {'resume_token': None}

    def test_no_rows_is_no_token(self, client_with_remote, mocker, mock_subprocess):
        resp = self.get(client_with_remote, mocker, mock_subprocess)
        assert resp.status_code == 200 and resp.get_json() == {'resume_token': None}

    def test_a_failing_list_is_a_500(self, client_with_remote, mocker, mock_subprocess):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mock_subprocess.setup(stdout=[], stderr='cannot open: permission denied', returncode=1)
        resp = client_with_remote.get(self.URL)
        assert resp.status_code == 500 and 'error' in resp.get_json()


class TestFreshWithTokenThroughRealCommands:
    """Abort and receive run as real argv (only Popen is mocked): `receive -A`
    precedes `receive -F -s -u`."""

    def setup_popen(self, mock_subprocess, token_row, abort=('', '', 0)):
        mock_subprocess.setup_multi(
            (token_row,),               # zfs list -o name,type,receive_resume_token
            abort,                      # zfs receive -A
            ('',),                      # zfs receive (the stream)
        )

    def test_the_abort_argv_runs_before_the_receive_argv(self, mocker, mock_subprocess):
        mocker.patch.object(DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[])
        mocker.patch('zfsbackup.api.read_zfsbackup_properties', return_value={})
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.api.zfs.create')
        app = create_app(_remote_config())
        handler = app.view_functions['backup_stream'].__wrapped__
        self.setup_popen(mock_subprocess, [f'{SERVER_DS}\tfilesystem\t{TOKEN}\n'])
        ws = MagicMock()
        ws.receive.side_effect = [b'chunk', json.dumps({'done': True})]
        with app.test_request_context(query_string={'mode': 'fresh'}):
            handler(ws, 'client1', 'pool/data')
        argvs = [c[0][0][1:] for c in mock_subprocess.call_args_list]
        assert len(argvs) == 3, argvs
        assert argvs[0][0] == 'list'
        assert argvs[1] == ['receive', '-A', SERVER_DS]
        assert argvs[2][0] == 'receive' and '-A' not in argvs[2]
        assert '-s' in argvs[2] and argvs[2][-1] == SERVER_DS
        assert json.loads(ws.send.call_args[0][0]) == {'status': 'ok'}

    def test_resume_runs_no_list_and_no_abort(self, mocker, mock_subprocess):
        mocker.patch.object(DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[])
        mocker.patch('zfsbackup.api.read_zfsbackup_properties', return_value={})
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        app = create_app(_remote_config())
        handler = app.view_functions['backup_stream'].__wrapped__
        mock_subprocess.setup_multi(('',))
        ws = MagicMock()
        ws.receive.side_effect = [json.dumps({'done': True})]
        with app.test_request_context(query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')
        argvs = [c[0][0][1:] for c in mock_subprocess.call_args_list]
        assert len(argvs) == 1 and argvs[0][0] == 'receive' and '-A' not in argvs[0]

    @pytest.mark.parametrize('stderr, rc, clean', [
        (f"cannot receive: '{SERVER_DS}' does not have any resumable receive state to abort", 1, True),
        (f"cannot open '{SERVER_DS}': dataset does not exist", 2, True),
        ('cannot receive: permission denied', 1, False),
    ], ids=['no-state', 'no-dataset', 'other'])
    def test_real_abort_failures_are_classified_by_their_text(
        self, mocker, mock_subprocess, stderr, rc, clean
    ):
        mocker.patch.object(DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[])
        mocker.patch('zfsbackup.api.read_zfsbackup_properties', return_value={})
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.api.zfs.create')
        app = create_app(_remote_config())
        handler = app.view_functions['backup_stream'].__wrapped__
        self.setup_popen(
            mock_subprocess, [f'{SERVER_DS}\tfilesystem\t{TOKEN}\n'], abort=('', stderr, rc)
        )
        ws = MagicMock()
        ws.receive.side_effect = [json.dumps({'done': True})]
        with app.test_request_context(query_string={'mode': 'fresh'}):
            handler(ws, 'client1', 'pool/data')
        sent = json.loads(ws.send.call_args[0][0])
        if clean:
            assert sent == {'status': 'ok'}
            assert len(mock_subprocess.call_args_list) == 3
        else:
            assert sent['error'].startswith(f'Failed to discard the partial receive state of {SERVER_DS}: ')
            assert 'permission denied' in sent['error']
            assert len(mock_subprocess.call_args_list) == 2      # no receive was started


def _ws():
    """A mock websocket that, if a regression lets a stream read from it,
    ends the stream instead of spinning on endless MagicMock chunks."""
    ws = MagicMock()
    ws.receive.side_effect = ConnectionClosed(None, None)
    return ws


def _sent_by(ws):
    return [json.loads(c[0][0]) for c in ws.send.call_args_list]


class HeldStream:
    """A stream parked inside `ws.receive` (so it holds its dataset lock and the
    gate) until `release()`; run from a thread."""

    def __init__(self, stream, **run_kwargs):
        self.stream = stream
        self.entered = threading.Event()
        self._release = threading.Event()
        self.ws = MagicMock()
        self.errors = []
        calls = []

        def receive(timeout=None):
            calls.append(1)
            if len(calls) == 1:
                self.entered.set()
                assert self._release.wait(20), 'the held stream was never released'
                return b'chunk'
            return json.dumps({'done': True})

        self.ws.receive.side_effect = receive

        def target():
            try:
                stream.run('resume', ws=self.ws, **run_kwargs)
            except BaseException as e:      # noqa: BLE001 - handed to the test thread
                self.errors.append(e)

        self.thread = threading.Thread(target=target, daemon=True)

    def __enter__(self):
        self.thread.start()
        assert self.entered.wait(20), 'the held stream never reached ws.receive'
        return self

    def release(self):
        self._release.set()
        self.thread.join(20)
        assert not self.thread.is_alive()

    def __exit__(self, *exc):
        self.release()


class TestPerDatasetStreamLock:
    BUSY = {'error': 'busy', 'retry': True}

    def test_the_busy_reply_constant(self):
        from zfsbackup.api import BUSY_ERROR
        assert BUSY_ERROR == self.BUSY

    def test_a_second_stream_for_the_same_dataset_gets_busy_and_the_first_proceeds(self, stream):
        with HeldStream(stream) as held:
            assert stream.gate.active == 1
            calls_before = stream.exists.call_count
            other = _ws()
            other.receive.side_effect = [json.dumps({'done': True})]
            stream.run('resume', ws=other)
            assert _sent_by(other) == [self.BUSY]
            other.receive.assert_not_called()
            assert stream.exists.call_count == calls_before       # refused before any zfs call
            assert stream.gate.active == 1                        # the loser released its own registration
            held.release()
        assert _sent_by(held.ws) == [{'status': 'ok'}]
        assert held.errors == []
        stream.writer.close.assert_called_once()                  # one receive, not two
        assert stream.receive.call_count == 1
        assert stream.gate.active == 0

    def test_a_busy_refusal_holds_for_as_long_as_the_first_runs(self, stream):
        with HeldStream(stream) as held:
            for _ in range(3):
                ws = _ws()
                stream.run('fresh', ws=ws)
                assert _sent_by(ws) == [self.BUSY]
            held.release()

    def test_the_lock_is_free_again_once_the_first_stream_finishes(self, stream):
        with HeldStream(stream) as held:
            held.release()
        ws = _ws()
        ws.receive.side_effect = [json.dumps({'done': True})]
        stream.run('resume', ws=ws)
        assert _sent_by(ws) == [{'status': 'ok'}]

    def test_busy_is_answered_before_the_source_check(self, stream, source_props):
        with HeldStream(stream) as held:
            source_props.return_value = {'org.zfsbackup:source_dataset': 'tank/data'}   # would be refused
            ws = _ws()
            stream.run('fresh', ws=ws)
            assert _sent_by(ws) == [self.BUSY]
            held.release()

    def test_a_busy_refusal_survives_a_dead_socket_and_keeps_the_first_lock(self, stream):
        with HeldStream(stream) as held:
            dead = _ws()
            dead.send.side_effect = Exception('closed')
            stream.run('resume', ws=dead)                   # must not raise
            ws = _ws()
            stream.run('resume', ws=ws)
            assert _sent_by(ws) == [self.BUSY]               # the loser did not steal or free the lock
            held.release()
        assert held.errors == []

    def test_different_datasets_do_not_block_each_other(self, stream):
        with HeldStream(stream) as held:
            for client, dataset in (('client2', 'pool/data'), ('client1', 'pool/other')):
                ws = _ws()
                ws.receive.side_effect = [json.dumps({'done': True})]
                stream.run('resume', ws=ws, client=client, dataset=dataset)
                assert _sent_by(ws) == [{'status': 'ok'}], (client, dataset)
            held.release()

    def test_two_pools_mapping_to_one_server_dataset_do_share_the_lock(self, stream):
        """`_server_dataset` drops the pool: tank/data and pool/data are the
        same server dataset, so they exclude each other."""
        with HeldStream(stream) as held:
            ws = _ws()
            stream.run('resume', ws=ws, dataset='tank/data')
            assert _sent_by(ws) == [self.BUSY]
            held.release()

    def test_the_locks_are_per_app(self, mocker):
        a, b = Stream(mocker), Stream(mocker)
        with HeldStream(a) as held:
            ws = _ws()
            ws.receive.side_effect = [json.dumps({'done': True})]
            b.run('resume', ws=ws)
            assert _sent_by(ws) == [{'status': 'ok'}]
            held.release()

    def test_many_racing_requests_one_wins_the_rest_are_busy(self, stream):
        """Eight requests released together onto one dataset: exactly one is
        served, the other seven get the busy reply; no deadlock."""
        n = 8
        barrier = threading.Barrier(n, timeout=20)
        results = []
        lock = threading.Lock()
        gate_open = threading.Event()

        def make_ws():
            ws = _ws()
            calls = []

            def receive(timeout=None):
                calls.append(1)
                if len(calls) == 1:
                    assert gate_open.wait(20)
                    return b'chunk'
                return json.dumps({'done': True})

            ws.receive.side_effect = receive
            return ws

        def worker(ws):
            barrier.wait()
            stream.run('resume', ws=ws)
            with lock:
                results.append(_sent_by(ws))

        wss = [make_ws() for _ in range(n)]
        threads = [threading.Thread(target=worker, args=(w,), daemon=True) for w in wss]
        for t in threads:
            t.start()
        # Wait until the n-1 losers have answered, then let the winner finish.
        deadline = threading.Event()
        for _ in range(400):
            with lock:
                if len(results) == n - 1:
                    break
            deadline.wait(0.05)
        gate_open.set()
        for t in threads:
            t.join(20)
            assert not t.is_alive()
        assert sorted(map(json.dumps, results)) == sorted(
            [json.dumps([self.BUSY])] * (n - 1) + [json.dumps([{'status': 'ok'}])]
        )
        assert stream.receive.call_count == 1
        assert stream.gate.active == 0


class TestDatasetLockIsReleased:
    """After every way a stream can end, the next stream for the dataset is
    served (not 'busy')."""

    FIRST_REPLY = {
        'mismatch': 'refusing to mix in', 'source-read-fails': 'Failed to check',
        'token-lookup-fails': 'for partial receive state', 'abort-fails': 'Failed to discard',
        'create-fails': 'Failed to create', 'set-fails': 'Failed to create',
        'receive-start-fails': 'Failed to start zfs receive', 'write-fails': 'Broken pipe',
        'client-vanishes': None, 'idle-timeout': 'no data from the client',
    }

    @staticmethod
    def _arrangements():
        # name -> (apply(stream, source_props) -> revert callable, expect first reply key)
        def mismatch(s, props):
            props.return_value = {'org.zfsbackup:source_dataset': 'tank/data'}
            return lambda: props.configure_mock(return_value={})

        def source_read_fails(s, props):
            props.side_effect = RuntimeError('x')
            return lambda: props.configure_mock(side_effect=None, return_value={})

        def token_fails(s, props):
            s.list.side_effect = RuntimeError('x')
            return lambda: setattr(s.list, 'side_effect', None)

        def abort_fails(s, props):
            s.token('1-t')
            s.abort.side_effect = Exception('pool I/O is currently suspended')

            def revert():
                s.abort.side_effect = None
                s.token(None)
            return revert

        def create_fails(s, props):
            s.exists.return_value = False
            s.create.side_effect = RuntimeError('x')

            def revert():
                s.exists.return_value = True
                s.create.side_effect = None
            return revert

        def set_fails(s, props):
            s.exists.return_value = False
            s.set.side_effect = RuntimeError('x')

            def revert():
                s.exists.return_value = True
                s.set.side_effect = None
            return revert

        def receive_fails(s, props):
            s.receive.side_effect = Exception('x')
            return lambda: setattr(s.receive, 'side_effect', lambda *a, **k: s.writer)

        def write_fails(s, props):
            s.writer.write.side_effect = BrokenPipeError(32, 'Broken pipe')
            return lambda: setattr(s.writer.write, 'side_effect', None)

        def client_vanishes(s, props):
            s.ws.receive.side_effect = ConnectionClosed(None, None)
            return lambda: None

        def timeout(s, props):
            s.ws.receive.side_effect = [None]
            return lambda: None

        return {
            'mismatch': mismatch, 'source-read-fails': source_read_fails,
            'token-lookup-fails': token_fails, 'abort-fails': abort_fails,
            'create-fails': create_fails, 'set-fails': set_fails,
            'receive-start-fails': receive_fails, 'write-fails': write_fails,
            'client-vanishes': client_vanishes, 'idle-timeout': timeout,
        }

    @pytest.mark.parametrize('name', list(_arrangements.__func__().keys()))
    def test_the_next_stream_is_served_after(self, stream, source_props, name):
        revert = self._arrangements()[name](stream, source_props)
        stream.run('fresh')
        assert stream.claimed == set(), name              # nothing left claimed
        expected = self.FIRST_REPLY[name]
        if expected is None:
            assert stream.sent == []                     # the client was gone: nobody to tell
        else:
            assert len(stream.sent) == 1 and expected in stream.sent[0]['error'], stream.sent
        revert()
        ws = _ws()
        ws.receive.side_effect = [json.dumps({'done': True})]
        stream.writer = MagicMock()
        stream.run('fresh', ws=ws)
        assert _sent_by(ws) == [{'status': 'ok'}], name
        assert stream.gate.active == 0

    def test_the_next_stream_is_served_after_success(self, stream):
        stream.run('resume')
        ws = _ws()
        ws.receive.side_effect = [json.dumps({'done': True})]
        stream.run('resume', ws=ws)
        assert _sent_by(ws) == [{'status': 'ok'}]

    def test_the_next_stream_is_served_after_a_dry_run(self, mocker):
        s = Stream(mocker, dry_run=True)
        s.run('fresh')
        ws = _ws()
        s.run('fresh', ws=ws)
        assert _sent_by(ws) == [{'status': 'ok', 'dry_run': True}]

    def test_the_next_stream_is_served_after_an_exception(self, stream, source_props):
        source_props.return_value = {'org.zfsbackup:source_dataset': 'tank/data'}
        stream.ws.send.side_effect = RuntimeError('socket gone')
        with pytest.raises(RuntimeError, match='socket gone'):
            stream.run('fresh')
        source_props.return_value = {}
        ws = _ws()
        ws.receive.side_effect = [json.dumps({'done': True})]
        stream.run('fresh', ws=ws)
        assert _sent_by(ws) == [{'status': 'ok'}]

    def test_a_mode_refusal_never_takes_the_lock(self, stream):
        with HeldStream(stream) as held:
            stream.run(None)
            assert stream.sent == [{'error': f'stream mode required: {MODE_HINT}'}]   # not 'busy'
            held.release()


class TestCreateRestampsOwnership:
    def test_a_created_dataset_gets_client_id_and_source_dataset_after_create_before_receive(
        self, stream
    ):
        from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_SOURCE_DATASET

        stream.exists.return_value = False
        stream.run('resume')
        stream.create.assert_called_once()
        assert str(stream.create.call_args[0][0]) == SERVER_DS
        stream.set.assert_called_once()
        assert str(stream.set.call_args[0][0]) == SERVER_DS
        assert stream.set.call_args[0][1] == {
            PROP_CLIENT_ID: 'client1', PROP_SOURCE_DATASET: 'pool/data',
        }
        assert stream.order == ['create', 'set', 'receive']

    def test_a_dataset_the_abort_destroyed_is_recreated_and_restamped(self, stream):
        stream.token(TOKEN)
        stream.exists.side_effect = [True, True, False]
        stream.run('fresh')
        assert stream.order == ['abort', 'create', 'set', 'receive']

    def test_an_existing_dataset_is_not_restamped(self, stream):
        stream.run('resume')
        stream.create.assert_not_called()
        stream.set.assert_not_called()

    def test_the_restamp_uses_the_requesting_clients_names(self, stream):
        stream.exists.return_value = False
        stream.run('resume', client='other', dataset='tank/vol')
        assert stream.set.call_args[0][1]['org.zfsbackup:client_id'] == 'other'
        assert stream.set.call_args[0][1]['org.zfsbackup:source_dataset'] == 'tank/vol'

    def test_a_create_failure_replies_an_error_and_receives_nothing(self, stream):
        stream.exists.return_value = False
        stream.create.side_effect = RuntimeError('pool is full')
        stream.run('resume')
        assert stream.sent == [{'error': f'Failed to create {SERVER_DS}: pool is full'}]
        stream.set.assert_not_called()
        stream.receive.assert_not_called()
        stream.ws.receive.assert_not_called()
        assert stream.gate.active == 0

    def test_a_set_failure_replies_an_error_and_receives_nothing(self, stream):
        stream.exists.return_value = False
        stream.set.side_effect = RuntimeError('permission denied')
        stream.run('resume')
        assert stream.sent == [{'error': f'Failed to create {SERVER_DS}: permission denied'}]
        stream.create.assert_called_once()
        stream.receive.assert_not_called()
        stream.ws.receive.assert_not_called()
        assert stream.gate.active == 0


class TestStreamsInProgressSet:
    """The claim set (`streams_in_progress`) never keeps a dataset longer than
    its stream, and never grows beyond the concurrent streams."""

    def test_it_exists_and_starts_empty(self, stream):
        assert stream.claimed == set()

    def test_it_is_empty_after_success_and_during_it_holds_exactly_the_dataset(self, stream):
        seen = []
        stream.ws.receive.side_effect = lambda timeout=None: (
            seen.append(stream.claimed), json.dumps({'done': True}))[1]
        stream.run('resume')
        assert seen == [{SERVER_DS}]
        assert stream.claimed == set()

    @pytest.mark.parametrize('mode', ['fresh', 'resume'])
    def test_it_is_empty_after_a_dry_run(self, mocker, mode):
        s = Stream(mocker, dry_run=True)
        s.run(mode)
        assert s.claimed == set()

    def test_it_is_empty_after_an_exception(self, stream, source_props):
        source_props.return_value = {'org.zfsbackup:source_dataset': 'tank/data'}
        stream.ws.send.side_effect = RuntimeError('socket gone')
        with pytest.raises(RuntimeError):
            stream.run('fresh')
        assert stream.claimed == set()

    def test_a_busy_reply_neither_adds_nor_removes_a_claim(self, stream):
        with HeldStream(stream) as held:
            assert stream.claimed == {SERVER_DS}
            for _ in range(3):
                ws = _ws()
                stream.run('fresh', ws=ws)
                assert _sent_by(ws) == [{'error': 'busy', 'retry': True}]
                assert stream.claimed == {SERVER_DS}       # the loser did not release the winner's claim
            held.release()
        assert stream.claimed == set()

    def test_the_same_server_dataset_by_another_name_is_busy_too(self, stream):
        with HeldStream(stream) as held:
            ws = _ws()
            stream.run('resume', ws=ws, client='client1', dataset='tank/data')   # same server dataset
            assert _sent_by(ws) == [{'error': 'busy', 'retry': True}]
            held.release()

    @pytest.mark.parametrize('arrange', [
        'mode-missing', 'mode-unknown', 'draining', 'invalid-name', 'mismatch',
    ])
    def test_it_is_empty_after_each_early_refusal(self, stream, source_props, arrange):
        kwargs = {}
        if arrange == 'mode-missing':
            mode = None
        elif arrange == 'mode-unknown':
            mode = 'x'
        elif arrange == 'draining':
            stream.gate.start_draining()
            mode = 'fresh'
        elif arrange == 'invalid-name':
            mode = 'fresh'
            kwargs['dataset'] = 'pool//x'
        else:
            source_props.return_value = {'org.zfsbackup:source_dataset': 'tank/data'}
            mode = 'fresh'
        stream.run(mode, **kwargs)
        assert len(stream.sent) == 1 and 'error' in stream.sent[0]
        assert stream.claimed == set()
        assert stream.gate.active == 0

    def test_an_invalid_name_is_refused_with_no_claim_and_no_zfs(self, stream):
        bad = 'pool/backups/client1//x'
        stream.run('fresh', dataset='pool//x')
        assert stream.sent == [{
            'error': f'Invalid dataset name {bad!r}: {bad} is not a valid ZFS dataset name'
        }]
        stream.untouched()                               # no exists/list/create/set/abort/receive
        stream.ws.receive.assert_not_called()
        assert stream.claimed == set()
        assert stream.gate.active == 0

    def test_an_invalid_name_is_refused_the_same_way_every_time_never_busy(self, stream):
        """A leaked claim would turn the second identical request into 'busy'."""
        for _ in range(3):
            ws = _ws()
            stream.run('resume', ws=ws, dataset='pool//x')
            assert 'Invalid dataset name' in _sent_by(ws)[0]['error']
            assert stream.claimed == set()

    def test_an_invalid_name_refusal_survives_a_dead_socket(self, stream):
        stream.ws.send.side_effect = Exception('closed')
        stream.run('fresh', dataset='pool//x')           # must not raise
        stream.untouched()
        assert stream.claimed == set()

    def test_an_invalid_name_does_not_disturb_a_running_stream(self, stream):
        with HeldStream(stream) as held:
            ws = _ws()
            stream.run('resume', ws=ws, dataset='pool//x')
            assert 'Invalid dataset name' in _sent_by(ws)[0]['error']
            assert stream.claimed == {SERVER_DS}
            held.release()
        assert stream.claimed == set()

    def test_a_hundred_sequential_distinct_datasets_leave_it_empty(self, stream):
        for i in range(100):
            ws = _ws()
            ws.receive.side_effect = [json.dumps({'done': True})]
            stream.run('resume', ws=ws, dataset=f'pool/ds{i}')
            assert _sent_by(ws) == [{'status': 'ok'}]
            assert len(stream.claimed) == 0, i
        assert stream.claimed == set()

    def test_it_never_exceeds_the_concurrent_streams(self, stream):
        held = []
        try:
            for i in range(4):
                h = HeldStream(stream, dataset=f'pool/c{i}')
                h.__enter__()
                held.append(h)
                assert len(stream.claimed) == i + 1
            assert stream.claimed == {f'pool/backups/client1/c{i}' for i in range(4)}
            for n, h in enumerate(held):
                h.release()
                assert len(stream.claimed) == 3 - n
        finally:
            for h in held:
                h._release.set()
                h.thread.join(20)
        assert stream.claimed == set()

    def test_a_set_per_app(self, mocker):
        a, b = Stream(mocker), Stream(mocker)
        with HeldStream(a):
            assert a.claimed == {SERVER_DS} and b.claimed == set()

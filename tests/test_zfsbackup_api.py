"""Unit tests for the Flask API (zfsbackup.api)."""

import json
import logging
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
        with app.test_request_context():
            handler(ws, 'client1', 'pool/data')
        sent = json.loads(ws.send.call_args[0][0])
        assert 'error' in sent

    def test_stream_disabled_ws_send_exception_suppressed(self, mocker):
        config = _make_config()  # no remote_backup
        handler, app = self._get_handler(config, mocker)
        ws = MagicMock()
        ws.send.side_effect = Exception('ws already closed')
        with app.test_request_context():
            handler(ws, 'client1', 'pool/data')  # must not raise

    def test_stream_creates_filesystem_when_not_exists(self, mocker):
        config = _make_config(
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        )
        handler, app = self._get_handler(config, mocker)
        mocker.patch('zfsbackup.api.zfs.exists', return_value=False)
        mock_create = mocker.patch('zfsbackup.api.zfs.create')

        mock_writer = MagicMock()
        mocker.patch('zfsbackup.api.zfs.receive.filesystem', return_value=mock_writer)
        ws = MagicMock()
        ws.receive.side_effect = [json.dumps({'done': True})]

        with app.test_request_context():
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
        with app.test_request_context():
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
        with app.test_request_context():
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

        with app.test_request_context():
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

        with app.test_request_context():
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

        with app.test_request_context():
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
        with app.test_request_context():
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

    def test_the_gate_is_released_even_when_the_handler_itself_raises(self, mocker):
        """`zfs.exists` failing escapes `_receive_stream`; the gate's `finally`
        must still release the registration."""
        handler, app = self._setup(mocker, MagicMock())
        mocker.patch('zfsbackup.api.zfs.exists', side_effect=RuntimeError('pool unavailable'))
        gate = app.extensions['zfsbackup.stream_gate']
        with pytest.raises(RuntimeError, match='pool unavailable'):
            self._run(handler, app, MagicMock())
        assert gate.active == 0

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

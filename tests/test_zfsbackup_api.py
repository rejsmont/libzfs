"""Unit tests for the Flask API (zfsbackup.api).

Every endpoint but `/health` and `/enroll` needs a pinned client certificate (plan M10), so
these tests drive the app through `_api_support.Authed`: a `FakeGateway` that pins one
certificate under the client id `client1` (`CLIENT_ID`), presented in `SSL_CLIENT_CERT` as the
TLS layer would. The server dataset of a request is `<target>/<BOUND id>/...`. Authentication,
authorization, `/enroll`, `/rotate` and `/whoami` are in `test_zfsbackup_api_auth.py`.
"""

import contextlib
import json
import logging
import threading
import pytest
from unittest.mock import MagicMock, PropertyMock, patch
from datetime import datetime, timedelta, timezone
from simple_websocket import ConnectionClosed

from _api_support import Authed, CLIENT_ID
from _snapshot_support import listed_snapshot
from libzfseasy.types import Dataset, Filesystem, Snapshot as ZFSSnapshot
from zfsbackup.backup_manager import (
    DatasetInfo, DatasetManager, SnapshotInfo, PROP_CLIENT_ID, PROP_CONFIG,
    PROP_SOURCE_DATASET,
)
from zfsbackup.api import canonical_dataset_config
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


#: The websocket route of `client1`'s `pool/data`.
STREAM_PATH = f'/backup/{CLIENT_ID}/pool/data/stream'

#: A negotiate `config` the server accepts (plan B-1a: it is required and decoded), and a body.
GOOD_CONFIG = DatasetConfig(name='pool/data').to_property()


def _body(snapshots=(), **extra):
    """A valid negotiate body for `pool/data`."""
    return {'snapshots': list(snapshots), 'config': GOOD_CONFIG, **extra}


def _make_dsi(name='pool/data') -> DatasetInfo:
    ds = Dataset(name)
    cfg = DatasetConfig(name=name, frequency=timedelta(hours=1))
    dsi = DatasetInfo(ds, cfg)
    return dsi


@pytest.fixture
def authed(mocker):
    mocker.patch.object(
        DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[_make_dsi()]
    )
    return Authed(_make_config())


@pytest.fixture
def client(authed):
    """A test client presenting the certificate pinned under `client1`."""
    return authed.client()


@pytest.fixture
def authed_with_remote(mocker):
    config = _make_config(
        remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
    )
    mocker.patch.object(
        DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[_make_dsi()]
    )
    return Authed(config)


@pytest.fixture
def client_with_remote(authed_with_remote):
    return authed_with_remote.client()


def _authed_client(mocker, config, **kwargs):
    """An authenticated test client for `config` (the pinned id is `client1`)."""
    mocker.patch.object(
        DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[_make_dsi()]
    )
    return Authed(config, **kwargs).client()


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        resp = client.get('/health')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['status'] == 'ok'


class TestInfoEndpointsAreGone:
    """`/config`, `/datasets` and `/datasets/<name>/snapshots` were removed (plan S-4): they told
    every enrolled client about the server's own configuration, datasets and snapshots, and
    nothing in the client uses them."""

    PATHS = [
        '/config',
        '/datasets',
        '/datasets/pool/data/snapshots',
        '/datasets/pool/unknown/snapshots',
    ]

    @pytest.mark.parametrize('path', PATHS)
    @pytest.mark.parametrize('method', ['get', 'post'])
    def test_an_authenticated_caller_gets_404(self, client, path, method):
        resp = getattr(client, method)(path)
        assert resp.status_code == 404

    @pytest.mark.parametrize('path', PATHS)
    def test_the_404_discloses_nothing_about_the_server(self, client, path):
        body = client.get(path).get_data(as_text=True)
        for needle in ('snapshot_prefix', 'check_interval', 'api_port', 'dry_run',
                       'retention', 'frequency', 'autosnap', 'pool/data'):
            assert needle not in body.replace(path, '')

    @pytest.mark.parametrize('path', PATHS)
    def test_an_unauthenticated_caller_is_not_told_the_route_is_missing(self, authed, path):
        # routing errors are answered only to authenticated clients: 401, not 404
        assert authed.client(None).get(path).status_code == 401

    def test_the_routes_and_views_are_not_registered(self, authed):
        app = authed.app
        for view in ('get_config', 'get_datasets', 'get_snapshots'):
            assert view not in app.view_functions
        rules = {r.rule for r in app.url_map.iter_rules()}
        assert not {r for r in rules if r.startswith(('/config', '/datasets'))}

    def test_the_server_keeps_no_dataset_manager(self):
        # the removed routes were the only users of the `DatasetManager` the app built
        import zfsbackup.api as api
        assert not hasattr(api, 'DatasetManager')


class TestBackupRegisterIsGone:
    """`/backup/register` was removed (plan M10): `/whoami` says who the server takes the
    caller for, and the first `negotiate` creates the server dataset."""

    @pytest.mark.parametrize('method', ['post', 'get'])
    def test_an_authenticated_caller_gets_404(self, client_with_remote, mocker, method):
        mock_exists = mocker.patch('zfsbackup.api.zfs.exists')
        mock_create = mocker.patch('zfsbackup.api.zfs.create')
        resp = getattr(client_with_remote, method)('/backup/register', json={'client_id': 'testclient'})
        assert resp.status_code == 404
        mock_exists.assert_not_called()
        mock_create.assert_not_called()

    def test_the_route_and_the_view_are_not_registered(self, authed_with_remote):
        app = authed_with_remote.app
        assert 'backup_register' not in app.view_functions
        assert '/backup/register' not in {r.rule for r in app.url_map.iter_rules()}

    def test_whoami_replaced_it(self, client_with_remote):
        resp = client_with_remote.get('/whoami')
        assert resp.status_code == 200
        assert resp.get_json()['client_id'] == CLIENT_ID


class TestBackupNegotiate:
    def test_negotiate_disabled_when_no_remote(self, client, mocker):
        resp = client.post('/backup/client1/pool/data/negotiate', json={})
        assert resp.status_code == 503

    def test_negotiate_finds_common_snapshot(self, client_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.api.zfs.set')
        mock_snap = MagicMock()
        mock_snap.short = 'autosnap_20240101120000'
        mocker.patch('zfsbackup.api.zfs.list', return_value=[mock_snap])

        resp = client_with_remote.post(
            '/backup/client1/pool/data/negotiate',
            json=_body(['autosnap_20240101120000', 'autosnap_20240102120000']),
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'server_dataset' in data
        assert data['common_snapshot'] == 'autosnap_20240101120000'

    def test_negotiate_returns_none_common_when_no_overlap(self, client_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.api.zfs.set')
        mock_snap = MagicMock()
        mock_snap.short = 'autosnap_99990101120000'
        mocker.patch('zfsbackup.api.zfs.list', return_value=[mock_snap])

        resp = client_with_remote.post(
            '/backup/client1/pool/data/negotiate',
            json=_body(['autosnap_20240101120000']),
        )
        assert resp.status_code == 200
        assert resp.get_json()['common_snapshot'] is None

    def test_negotiate_returns_500_on_snapshot_list_error(self, client_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.api.zfs.set')
        mocker.patch('zfsbackup.api.zfs.list', side_effect=Exception('zfs error'))
        resp = client_with_remote.post(
            '/backup/client1/pool/data/negotiate',
            json=_body(['autosnap_20240101120000']),
        )
        assert resp.status_code == 500
        assert 'error' in resp.get_json()

    def test_negotiate_creates_only_the_parent_of_a_missing_dataset(self, client_with_remote, mocker):
        """R0: the first full stream creates the leaf (zfs refuses a raw full onto a pre-created
        one), so negotiate makes the parents with `-p` and sets nothing."""
        mocker.patch('zfsbackup.api.zfs.exists', return_value=False)
        mock_create = mocker.patch('zfsbackup.api.zfs.create')
        mock_set = mocker.patch('zfsbackup.api.zfs.set')
        mocker.patch('zfsbackup.api.zfs.list', return_value=[])
        encoded = DatasetConfig(name='pool/data').to_property()

        resp = client_with_remote.post(
            '/backup/client1/pool/data/negotiate',
            json={'snapshots': [], 'config': encoded},
        )
        assert resp.status_code == 200
        mock_create.assert_called_once()
        assert str(mock_create.call_args[0][0]) == 'pool/backups/client1'      # the parent, not the leaf
        assert mock_create.call_args.kwargs == {'parents': True}
        mock_set.assert_not_called()

    def test_negotiate_sets_the_properties_of_an_existing_dataset(self, client_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mock_create = mocker.patch('zfsbackup.api.zfs.create')
        mock_set = mocker.patch('zfsbackup.api.zfs.set')
        mocker.patch('zfsbackup.api.zfs.list', return_value=[])
        encoded = DatasetConfig(name='pool/data').to_property()

        resp = client_with_remote.post(
            '/backup/client1/pool/data/negotiate',
            json={'snapshots': [], 'config': encoded},
        )
        assert resp.status_code == 200
        mock_create.assert_not_called()
        mock_set.assert_called_once()
        assert str(mock_set.call_args[0][0]) == SERVER_DS


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
        authed = Authed(config)
        return authed.app.view_functions['backup_stream'].__wrapped__, authed

    def test_stream_disabled_sends_error(self, mocker):
        config = _make_config()  # no remote_backup
        handler, authed = self._get_handler(config, mocker)
        ws = MagicMock()
        with authed.context(STREAM_PATH, query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')
        sent = json.loads(ws.send.call_args[0][0])
        assert 'error' in sent

    def test_stream_disabled_ws_send_exception_suppressed(self, mocker):
        config = _make_config()  # no remote_backup
        handler, authed = self._get_handler(config, mocker)
        ws = MagicMock()
        ws.send.side_effect = Exception('ws already closed')
        with authed.context(STREAM_PATH, query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')  # must not raise

    def test_stream_never_recreates_a_filesystem_that_vanished_before_the_receive(self, mocker):
        """The dataset existed for the ownership and configuration reads and is gone at the last
        check: nothing is created or stamped (R0: the receive creates it), and without `-F`."""
        config = _make_config(
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        )
        handler, authed = self._get_handler(config, mocker)
        mocker.patch('zfsbackup.api.zfs.exists', side_effect=[True, True, False])
        mock_create = mocker.patch('zfsbackup.api.zfs.create')
        mock_set = mocker.patch('zfsbackup.api.zfs.set')

        mock_writer = MagicMock()
        mock_receive = mocker.patch('zfsbackup.api.zfs.receive.filesystem', return_value=mock_writer)
        ws = MagicMock()
        ws.receive.side_effect = [json.dumps({'done': True})]

        with authed.context(STREAM_PATH, query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')
        mock_create.assert_not_called()
        mock_set.assert_not_called()
        mock_receive.assert_called_once()
        assert mock_receive.call_args.kwargs['force'] is False
        assert json.loads(ws.send.call_args[0][0]) == {'status': 'ok'}

    def test_stream_receive_startup_failure_sends_error_and_releases_the_gate(self, mocker):
        """`zfs.receive.filesystem` raises when `zfs receive` cannot start (it
        never returns None)."""
        config = _make_config(
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        )
        handler, authed = self._get_handler(config, mocker)
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mocker.patch(
            'zfsbackup.api.zfs.receive.filesystem',
            side_effect=RuntimeError('cannot receive: dataset is busy'),
        )
        ws = MagicMock()
        with authed.context(STREAM_PATH, query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')
        ws.send.assert_called_once()
        assert json.loads(ws.send.call_args[0][0]) == {
            'error': 'Failed to start zfs receive: cannot receive: dataset is busy'
        }
        ws.receive.assert_not_called()
        assert authed.gate.active == 0

    def test_stream_dry_run_sends_ok(self, mocker):
        config = _make_config(
            dry_run=True,
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        )
        handler, authed = self._get_handler(config, mocker)
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        ws = MagicMock()
        with authed.context(STREAM_PATH, query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')
        sent = json.loads(ws.send.call_args[0][0])
        assert sent.get('status') == 'ok'
        assert sent.get('dry_run') is True

    def test_stream_success_writes_chunks_and_closes(self, mocker):
        config = _make_config(
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        )
        handler, authed = self._get_handler(config, mocker)
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)

        mock_writer = MagicMock()
        mocker.patch('zfsbackup.api.zfs.receive.filesystem', return_value=mock_writer)

        ws = MagicMock()
        ws.receive.side_effect = [b'chunk1', b'chunk2', json.dumps({'done': True})]

        with authed.context(STREAM_PATH, query_string={'mode': 'resume'}):
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
        handler, authed = self._get_handler(config, mocker)
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)

        mock_writer = MagicMock()
        mocker.patch('zfsbackup.api.zfs.receive.filesystem', return_value=mock_writer)

        ws = MagicMock()
        ws.receive.side_effect = ConnectionClosed(None, None)

        with authed.context(STREAM_PATH, query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')

        mock_writer.close.assert_called_once()

    def test_stream_write_exception_sends_error(self, mocker):
        config = _make_config(
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        )
        handler, authed = self._get_handler(config, mocker)
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)

        mock_writer = MagicMock()
        mock_writer.write.side_effect = IOError('disk full')
        mocker.patch('zfsbackup.api.zfs.receive.filesystem', return_value=mock_writer)

        ws = MagicMock()
        ws.receive.return_value = b'chunk'

        with authed.context(STREAM_PATH, query_string={'mode': 'resume'}):
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
        authed = Authed(config)
        handler = authed.app.view_functions['backup_stream'].__wrapped__
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.api.zfs.receive.filesystem', return_value=writer)
        self.set = mocker.patch('zfsbackup.api.zfs.set')       # the stream handler never calls it
        return handler, authed

    def _run(self, handler, authed, ws):
        with authed.context(STREAM_PATH, query_string={'mode': 'resume'}):
            handler(ws, 'client1', 'pool/data')

    def test_the_receive_diagnostic_replaces_the_pipe_error_and_both_are_logged(
        self, mocker, caplog
    ):
        writer = MagicMock()
        writer.write.side_effect = BrokenPipeError(32, 'Broken pipe')
        writer.close.side_effect = RuntimeError('cannot receive: checksum mismatch')
        handler, authed = self._setup(mocker, writer)
        ws = MagicMock()
        ws.receive.return_value = b'chunk'
        with caplog.at_level(logging.ERROR, logger='zfsbackup.api'):
            self._run(handler, authed, ws)

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
        handler, authed = self._setup(mocker, writer)       # close() raises nothing
        ws = MagicMock()
        ws.receive.return_value = b'chunk'
        with caplog.at_level(logging.ERROR, logger='zfsbackup.api'):
            self._run(handler, authed, ws)

        assert json.loads(ws.send.call_args[0][0]) == {'error': '[Errno 32] Broken pipe'}
        ws.send.assert_called_once()
        writer.close.assert_called_once()
        errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(errors) == 1 and 'Broken pipe' in errors[0]

    def test_a_failure_after_some_good_chunks_still_closes_exactly_once(self, mocker):
        writer = MagicMock()
        writer.write.side_effect = [None, None, BrokenPipeError(32, 'Broken pipe')]
        writer.close.side_effect = RuntimeError('cannot receive: out of space')
        handler, authed = self._setup(mocker, writer)
        ws = MagicMock()
        ws.receive.side_effect = [b'a', b'b', b'c', b'never read']
        self._run(handler, authed, ws)
        assert writer.write.call_count == 3
        writer.close.assert_called_once()
        assert json.loads(ws.send.call_args[0][0]) == {'error': 'cannot receive: out of space'}
        assert ws.receive.call_count == 3                # nothing read after the failure

    def test_a_non_oserror_write_failure_closes_once_via_the_general_path(self, mocker):
        writer = MagicMock()
        writer.write.side_effect = ValueError('bad chunk')
        handler, authed = self._setup(mocker, writer)
        ws = MagicMock()
        ws.receive.return_value = b'chunk'
        self._run(handler, authed, ws)
        writer.close.assert_called_once()
        assert json.loads(ws.send.call_args[0][0]) == {'error': 'bad chunk'}

    def test_the_success_path_closes_then_says_ok_unchanged(self, mocker):
        order = []
        writer = MagicMock()
        writer.write.side_effect = lambda d: order.append(('write', d))
        writer.close.side_effect = lambda: order.append(('close',))
        handler, authed = self._setup(mocker, writer)
        ws = MagicMock()
        ws.send.side_effect = lambda m: order.append(('send', json.loads(m)))
        ws.receive.side_effect = [b'x', json.dumps({'done': True})]
        self._run(handler, authed, ws)
        assert order == [('write', b'x'), ('close',), ('send', {'status': 'ok'})]
        self.set.assert_not_called()                     # no post-receive re-stamp: `-o` did it

    def test_a_failing_close_after_done_reports_the_error_not_ok(self, mocker):
        writer = MagicMock()
        writer.close.side_effect = RuntimeError('cannot receive: bad stream')
        handler, authed = self._setup(mocker, writer)
        ws = MagicMock()
        ws.receive.side_effect = [b'x', json.dumps({'done': True})]
        self._run(handler, authed, ws)
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
        handler, authed = self._setup(mocker, writer)
        gate = authed.gate
        during = []
        ws = MagicMock()

        def receive(timeout=None):
            during.append(gate.active)
            return b'chunk'

        ws.receive.side_effect = receive
        assert gate.active == 0
        self._run(handler, authed, ws)
        assert during == [1]                 # registered while the stream ran...
        assert gate.active == 0              # ...and released afterwards

    def test_the_gate_is_balanced_after_several_failing_streams(self, mocker):
        writer = MagicMock()
        writer.write.side_effect = BrokenPipeError(32, 'Broken pipe')
        writer.close.side_effect = RuntimeError('cannot receive: x')
        handler, authed = self._setup(mocker, writer)
        gate = authed.gate
        for _ in range(5):
            ws = MagicMock()
            ws.receive.return_value = b'chunk'
            self._run(handler, authed, ws)
        assert gate.active == 0
        assert gate.begin() is True           # not wedged
        gate.end()

    def test_the_gate_is_released_even_when_the_handler_itself_raises(self, mocker, source_props):
        """A refusal that cannot even be sent (the socket is gone) escapes
        `_receive_stream_locked`; the dataset lock and the gate's `finally`
        must still release."""
        source_props.return_value = {'org.zfsbackup:source_dataset': 'tank/data'}
        handler, authed = self._setup(mocker, MagicMock())
        gate = authed.gate
        ws = MagicMock()
        ws.receive.side_effect = ConnectionClosed(None, None)    # a regression must fail, not spin
        ws.send.side_effect = RuntimeError('socket gone')
        with pytest.raises(RuntimeError, match='socket gone'):
            self._run(handler, authed, ws)
        assert gate.active == 0
        # ...and the per-dataset lock: the next stream for it is not 'busy'.
        source_props.return_value = {}
        ws2 = MagicMock()
        ws2.receive.side_effect = [json.dumps({'done': True})]
        self._run(handler, authed, ws2)
        assert json.loads(ws2.send.call_args[0][0]) == {'status': 'ok'}

    def test_a_client_that_vanishes_before_the_ok_does_not_cause_a_second_close(self, mocker):
        """The stream was written and `close()` succeeded; only the final
        `{'status': 'ok'}` hits a closed socket."""
        writer = MagicMock()
        handler, authed = self._setup(mocker, writer)
        gate = authed.gate
        ws = MagicMock()
        ws.receive.side_effect = [b'x', json.dumps({'done': True})]
        ws.send.side_effect = ConnectionClosed(None, None)
        self._run(handler, authed, ws)                      # must not raise
        writer.write.assert_called_once_with(b'x')
        writer.close.assert_called_once()
        ws.send.assert_called_once()
        assert json.loads(ws.send.call_args[0][0]) == {'status': 'ok'}
        assert gate.active == 0


# ---------------------------------------------------------------------------
# Protocol: ?mode=fresh|resume, partial-receive discard, source-dataset check
# ---------------------------------------------------------------------------

SERVER_DS = 'pool/backups/client1/data'     # target / client id / the client dataset minus its pool
NOT_NEGOTIATED = {
    'error': (
        f'no configuration is recorded for {SERVER_DS}; negotiate again before streaming '
        '(a restart of the server forgets the configuration of a dataset that has not been '
        'received yet)'
    ),
    'retry': True,
}
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


@pytest.fixture(autouse=True)
def stored_config(mocker):
    """What negotiate stored as the dataset's local `org.zfsbackup:config`
    (plan B-1b): a stream reads it with `zfs.get.value` and refuses without
    it. `Stream` re-patches it to record ordering; tests that need another
    value (or none) set `.return_value`."""
    return mocker.patch(
        'zfsbackup.api.zfs.get.value', return_value=canonical_dataset_config(GOOD_CONFIG)
    )


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
    """The `stream` handler of one app, driven directly with a mock websocket.

    `run(client=X)` is a request authenticated as the certificate pinned under id X (the
    handler takes the id from the binding, never from its argument)."""

    def __init__(self, mocker, **config_kwargs):
        mocker.patch.object(DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[])
        self.authed = Authed(_remote_config(**config_kwargs))
        self.app = self.authed.app
        self.handler = self.app.view_functions['backup_stream'].__wrapped__
        self.gate = self.app.extensions['zfsbackup.stream_gate']
        self.ws = MagicMock()
        self.ws.receive.side_effect = [b'chunk', json.dumps({'done': True})]
        self.order = []
        self.exists = mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        # `zfs.list` serves two callers: the receive-flag decision lists the server dataset's
        # snapshots (`types='snapshot'`, rows from `self.snapshots`), and the fresh-mode token
        # lookup lists the dataset itself (`self.token_rows`). Tests that make ONE of them fail
        # use `fail_token_lookup` / `fail_snapshot_listing`, so the other keeps working.
        self.snapshots = []
        self.token_rows = []
        self.list = mocker.patch('zfsbackup.api.zfs.list', side_effect=self._list)
        self.create = mocker.patch('zfsbackup.api.zfs.create')
        self.set = mocker.patch('zfsbackup.api.zfs.set')
        self.abort = mocker.patch('zfsbackup.api.zfs.receive.abort')
        # What negotiate stored on the server dataset (plan B-1b): the stream
        # reads it (local source) and puts it in effect with `-o`.
        self.stored = canonical_dataset_config(GOOD_CONFIG)
        self.get_value = mocker.patch('zfsbackup.api.zfs.get.value', return_value=self.stored)
        self.writer = MagicMock()
        self.receive = mocker.patch('zfsbackup.api.zfs.receive.filesystem', return_value=self.writer)
        self.create.side_effect = lambda *a, **k: self.order.append('create')
        self.set.side_effect = lambda *a, **k: self.order.append('set')
        self.abort.side_effect = lambda *a, **k: self.order.append('abort')
        self.get_value.side_effect = lambda *a, **k: (self.order.append('get'), self.stored)[1]
        self.receive.side_effect = lambda *a, **k: (self.order.append('receive'), self.writer)[1]

    def _list(self, *args, **kwargs):
        return list(self.snapshots if kwargs.get('types') == 'snapshot' else self.token_rows)

    def server_has(self, *snaps):
        """The server dataset holds these snapshots, oldest first: a name (txg and guid derived
        from it, as `_snapshot_support` does) or a `(name, txg, guid)` tuple."""
        self.snapshots = [
            listed_snapshot(s, SERVER_DS) if isinstance(s, str)
            else listed_snapshot(s[0], SERVER_DS, txg=s[1], guid=s[2])
            for s in snaps
        ]

    def fail_token_lookup(self, exc):
        """Only the listing of the dataset itself (the resume-token lookup) raises `exc`."""
        def fail(*args, **kwargs):
            if kwargs.get('types') == 'snapshot':
                return self._list(*args, **kwargs)
            raise exc
        self.list.side_effect = fail

    def fail_snapshot_listing(self, exc):
        """Only the listing of the dataset's snapshots (the `-F` decision) raises `exc`."""
        def fail(*args, **kwargs):
            if kwargs.get('types') == 'snapshot':
                raise exc
            return self._list(*args, **kwargs)
        self.list.side_effect = fail

    def restore_listing(self):
        self.list.side_effect = self._list

    @property
    def claimed(self):
        """A copy of the set of server datasets with a stream in progress
        (the app's `streams_in_progress`, found through the handler's closures)."""
        return set(_streams_in_progress(self.app))

    def token(self, value):
        """The server dataset reports `value` as its receive_resume_token."""
        row = MagicMock()
        row.__getitem__ = MagicMock(return_value=value)
        self.token_rows = [row]

    def run(self, mode='fresh', *, ws=None, client='client1', dataset='pool/data', **query):
        qs = dict(query)
        if mode is not None:
            qs['mode'] = mode
        who = self.authed.cert_for(client)
        with self.authed.context(f'/backup/{client}/{dataset}/stream', who=who, query_string=qs):
            self.handler(ws if ws is not None else self.ws, client, dataset)

    @property
    def sent(self):
        return [json.loads(c[0][0]) for c in self.ws.send.call_args_list]

    def untouched(self):
        for m in (self.exists, self.list, self.create, self.set, self.abort, self.receive,
                  self.get_value):
            m.assert_not_called()
        self.ws.receive.assert_not_called()


@pytest.fixture
def stream(mocker):
    return Stream(mocker)


def assert_only_the_snapshot_listing(stream):
    """`zfs.list` ran exactly once, for the snapshots (the `-F` decision), never for a token."""
    (call,) = stream.list.call_args_list
    assert call.kwargs['types'] == 'snapshot'
    assert call.kwargs['properties'] == ['createtxg', 'guid']
    assert str(call.kwargs['roots']) == SERVER_DS


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
        authed = Authed(_make_config(), client_id='c')
        ws = MagicMock()
        with authed.context('/backup/c/pool/data/stream', query_string={'mode': 'bogus'}):
            authed.app.view_functions['backup_stream'].__wrapped__(ws, 'c', 'pool/data')
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
    def test_fresh_with_a_token_warns_aborts_then_receives_in_that_order_and_recreates_nothing(
        self, stream, caplog
    ):
        stream.token(TOKEN)
        # ownership check, config read, token lookup, after the abort (the new dataset is gone)
        stream.exists.side_effect = [True, True, True, False]
        with caplog.at_level(logging.INFO, logger='zfsbackup.api'):
            stream.run('fresh')
        # the config is read BEFORE the abort (which can destroy the dataset); the receive
        # CREATES it again, so nothing is created or stamped in between (R0)
        assert stream.order == ['get', 'abort', 'receive']
        stream.create.assert_not_called()
        stream.set.assert_not_called()
        assert stream.receive.call_args.kwargs['force'] is False
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
        assert stream.order == ['get', 'abort', 'receive']
        stream.create.assert_not_called()

    def test_fresh_without_a_token_neither_warns_nor_aborts(self, stream, caplog):
        stream.token(None)
        with caplog.at_level(logging.INFO, logger='zfsbackup.api'):
            stream.run('fresh')
        stream.abort.assert_not_called()
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert stream.order == ['get', 'receive']

    def test_fresh_with_no_dataset_and_no_pending_config_is_refused_and_never_looks_for_a_token(
        self, stream
    ):
        stream.exists.return_value = False
        stream.run('fresh')
        assert stream.sent == [NOT_NEGOTIATED]
        stream.list.assert_not_called()
        for m in (stream.abort, stream.create, stream.set, stream.receive):
            m.assert_not_called()

    def test_the_token_none_string_means_no_state(self, stream):
        stream.token('none')
        stream.run('fresh')
        stream.abort.assert_not_called()

    def test_fresh_looks_the_token_up_by_name_alone(self, stream):
        stream.token(None)
        stream.run('fresh')
        (kwargs,) = [c.kwargs for c in stream.list.call_args_list if c.kwargs.get('types') != 'snapshot']
        assert str(kwargs['roots']) == SERVER_DS
        assert kwargs['properties'] == ['receive_resume_token']
        assert 'types' not in kwargs                  # a filesystem OR a volume

    def test_resume_never_aborts_and_never_looks_for_a_token(self, stream):
        stream.token(TOKEN)
        stream.run('resume')
        stream.abort.assert_not_called()
        assert_only_the_snapshot_listing(stream)       # the -F decision; no token lookup
        assert stream.order == ['get', 'receive']
        assert stream.sent == [{'status': 'ok'}]

    @pytest.mark.parametrize('text', [
        "cannot receive: 'pool/backups/client1/data' does not have any resumable receive state to abort",
        "cannot open 'pool/backups/client1/data': dataset does not exist",
        "CANNOT OPEN 'x': Dataset Does Not Exist",
    ])
    def test_an_abort_that_finds_nothing_to_abort_is_a_clean_success(self, stream, text, caplog):
        stream.token(TOKEN)
        stream.abort.side_effect = Exception(text)
        stream.exists.side_effect = [True, True, True, False]
        with caplog.at_level(logging.INFO, logger='zfsbackup.api'):
            stream.run('fresh')
        assert stream.sent == [{'status': 'ok'}]
        stream.create.assert_not_called()                     # the abort destroyed the dataset; the receive makes it
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
        stream.fail_token_lookup(RuntimeError('boom'))
        stream.run('fresh')
        assert stream.sent == [{
            'error': f'Failed to check {SERVER_DS} for partial receive state: boom'
        }]
        stream.abort.assert_not_called()
        stream.create.assert_not_called()
        stream.receive.assert_not_called()
        assert stream.gate.active == 0

    def test_an_exists_failure_during_the_token_lookup_is_reported_the_same_way(self, stream):
        stream.exists.side_effect = [True, True, RuntimeError('exists failed')]
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

    def test_the_order_is_source_check_config_read_snapshots_token_lookup_abort_receive(
        self, stream, source_props
    ):
        events = []
        source_props.side_effect = lambda ds: events.append('source') or {}
        stream.get_value.side_effect = lambda *a, **k: (events.append('config'), stream.stored)[1]
        stream.token(TOKEN)
        stream.list.side_effect = lambda **k: (
            events.append('snapshots') or []
            if k.get('types') == 'snapshot'
            else events.append('token') or [MagicMock(__getitem__=MagicMock(return_value=TOKEN))]
        )
        stream.abort.side_effect = lambda *a: events.append('abort')
        stream.create.side_effect = lambda *a, **k: events.append('create')
        stream.set.side_effect = lambda *a, **k: events.append('set')
        stream.receive.side_effect = lambda *a, **k: (events.append('receive'), stream.writer)[1]
        stream.exists.side_effect = [True, True, True, False]
        stream.run('fresh')
        assert events == ['source', 'config', 'snapshots', 'token', 'abort', 'receive']


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
        resp = client_with_remote.post('/backup/client1/pool/data/negotiate', json=_body())
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
        assert client_with_remote.post('/backup/client1/pool/data/negotiate', json=_body()).status_code == 409
        assert gate.active == 0
        source_props.side_effect = RuntimeError('x')
        assert client_with_remote.post('/backup/client1/pool/data/negotiate', json=_body()).status_code == 500
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
        assert stream.sent == [NOT_NEGOTIATED]            # and nothing else runs

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

    #: `zfs list -t snapshot -o name,type,createtxg,guid` of a server dataset with one snapshot
    SNAPSHOT_ROWS = [f'{SERVER_DS}@autosnap_20260101000000\tsnapshot\t100\t5000\n']

    def setup_popen(self, mock_subprocess, token_row, abort=('', '', 0)):
        mock_subprocess.setup_multi(
            (self.SNAPSHOT_ROWS,),      # zfs list -t snapshot  (the -F decision)
            (token_row,),               # zfs list -o name,type,receive_resume_token
            abort,                      # zfs receive -A
            ('',),                      # zfs receive (the stream); no `zfs set` after it
        )

    def test_the_abort_argv_runs_before_the_receive_argv(self, mocker, mock_subprocess):
        mocker.patch.object(DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[])
        mocker.patch('zfsbackup.api.read_zfsbackup_properties', return_value={})
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.api.zfs.create')
        authed = Authed(_remote_config())
        handler = authed.app.view_functions['backup_stream'].__wrapped__
        self.setup_popen(mock_subprocess, [f'{SERVER_DS}\tfilesystem\t{TOKEN}\n'])
        ws = MagicMock()
        ws.receive.side_effect = [b'chunk', json.dumps({'done': True})]
        with authed.context(STREAM_PATH, query_string={'mode': 'fresh', 'from_guid': '5000'}):
            handler(ws, 'client1', 'pool/data')
        argvs = [c[0][0][1:] for c in mock_subprocess.call_args_list]
        assert len(argvs) == 4, argvs
        assert argvs[0][0] == 'list' and 'snapshot' in argvs[0]        # the -F decision, before the abort
        assert argvs[1][0] == 'list' and 'receive_resume_token' in ','.join(argvs[1])
        assert argvs[2] == ['receive', '-A', SERVER_DS]
        assert argvs[3][0] == 'receive' and '-A' not in argvs[3]
        assert '-s' in argvs[3] and argvs[3][-1] == SERVER_DS
        assert argvs[3].count('-o') == 3 and '-x' not in argvs[3]      # the owned values, in the receive
        assert json.loads(ws.send.call_args[0][0]) == {'status': 'ok'}

    def test_resume_runs_only_the_snapshot_listing_and_no_abort(self, mocker, mock_subprocess):
        mocker.patch.object(DatasetManager, 'datasets', new_callable=PropertyMock, return_value=[])
        mocker.patch('zfsbackup.api.read_zfsbackup_properties', return_value={})
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        authed = Authed(_remote_config())
        handler = authed.app.view_functions['backup_stream'].__wrapped__
        mock_subprocess.setup_multi((self.SNAPSHOT_ROWS,), ('',))     # list -t snapshot, receive
        ws = MagicMock()
        ws.receive.side_effect = [json.dumps({'done': True})]
        with authed.context(STREAM_PATH, query_string={'mode': 'resume', 'from_guid': '5000'}):
            handler(ws, 'client1', 'pool/data')
        argvs = [c[0][0][1:] for c in mock_subprocess.call_args_list]
        # the one listing is the -F decision; there is no token lookup and no abort
        assert [a[0] for a in argvs] == ['list', 'receive'] and '-A' not in argvs[1]
        assert 'snapshot' in argvs[0] and 'receive_resume_token' not in ','.join(argvs[0])

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
        authed = Authed(_remote_config())
        handler = authed.app.view_functions['backup_stream'].__wrapped__
        self.setup_popen(
            mock_subprocess, [f'{SERVER_DS}\tfilesystem\t{TOKEN}\n'], abort=('', stderr, rc)
        )
        ws = MagicMock()
        ws.receive.side_effect = [json.dumps({'done': True})]
        with authed.context(STREAM_PATH, query_string={'mode': 'fresh', 'from_guid': '5000'}):
            handler(ws, 'client1', 'pool/data')
        sent = json.loads(ws.send.call_args[0][0])
        if clean:
            assert sent == {'status': 'ok'}
            assert len(mock_subprocess.call_args_list) == 4
        else:
            assert sent['error'].startswith(f'Failed to discard the partial receive state of {SERVER_DS}: ')
            assert 'permission denied' in sent['error']
            assert len(mock_subprocess.call_args_list) == 3      # no receive was started


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
        'not-negotiated': 'negotiate again', 'placeholder-overwrite': 'placeholder',
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
            s.fail_token_lookup(RuntimeError('x'))
            return s.restore_listing

        def abort_fails(s, props):
            s.token('1-t')
            s.abort.side_effect = Exception('pool I/O is currently suspended')

            def revert():
                s.abort.side_effect = None
                s.token(None)
            return revert

        def not_negotiated(s, props):
            # the dataset does not exist and nothing is pending: refused after the claim
            s.exists.return_value = False
            return lambda: setattr(s.exists, 'return_value', True)

        def placeholder_overwrite(s, props):
            # zfs refuses to overwrite an existing dataset with an encrypted stream
            s.writer.close.side_effect = RuntimeError(
                'cannot receive new filesystem stream: zfs receive -F cannot be used to destroy '
                'an encrypted filesystem or overwrite an unencrypted one with an encrypted one'
            )
            return lambda: setattr(s.writer.close, 'side_effect', None)

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
            'not-negotiated': not_negotiated, 'placeholder-overwrite': placeholder_overwrite,
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


class TestTheReceiveCreatesTheNewDataset:
    """R0: the stream handler never creates or stamps a dataset. A dataset that does not exist (the
    first stream of a client dataset, or one the fresh-mode `receive -A` destroyed whole, Z3) is
    created by the receive itself, with the `-o` owned values read BEFORE the abort. More cases
    (the pending map) are in `test_zfsbackup_api_receive.py::TestPendingConfigStream`."""

    def owned(self, stream, client='client1', dataset='pool/data'):
        return {
            PROP_CONFIG: stream.stored,
            PROP_CLIENT_ID: client,
            PROP_SOURCE_DATASET: dataset,
        }

    def test_a_dataset_the_abort_destroyed_is_received_without_recreating_it(self, stream):
        stream.token(TOKEN)
        stream.exists.side_effect = [True, True, True, False]
        stream.run('fresh')
        assert stream.order == ['get', 'abort', 'receive']
        stream.create.assert_not_called()
        stream.set.assert_not_called()
        (call,) = stream.receive.call_args_list
        assert str(call.args[0]) == SERVER_DS
        assert call.kwargs['props'] == self.owned(stream)
        assert call.kwargs['force'] is False
        assert stream.sent == [{'status': 'ok'}]

    def test_a_vanished_dataset_is_received_with_the_owned_values_and_no_force(self, stream):
        stream.exists.side_effect = [True, True, False]
        stream.run('resume')
        assert stream.order == ['get', 'receive']
        assert stream.receive.call_args.kwargs['props'] == self.owned(stream)
        assert stream.receive.call_args.kwargs['force'] is False

    def test_the_received_config_is_the_one_read_before_the_abort(self, stream):
        """Not whatever a later read would return: the dataset is gone by then."""
        stream.token(TOKEN)
        stream.exists.side_effect = [True, True, True, False]
        stream.get_value.side_effect = [stream.stored, None, None]
        stream.run('fresh')
        assert stream.get_value.call_count == 1
        assert stream.receive.call_args.kwargs['props'][PROP_CONFIG] == stream.stored

    def test_an_existing_dataset_is_never_stamped_and_keeps_force(self, stream):
        """Only the receive's own `-o` writes to it; nothing is written before or after."""
        stream.run('resume')
        stream.create.assert_not_called()
        stream.set.assert_not_called()
        assert stream.order == ['get', 'receive']
        assert stream.receive.call_args.kwargs['force'] is True

    def test_the_owned_values_use_the_requesting_clients_names(self, stream):
        stream.exists.side_effect = [True, True, False]
        stream.run('resume', client='other', dataset='tank/vol')
        stream.set.assert_not_called()
        owned = stream.receive.call_args.kwargs['props']
        assert owned[PROP_CLIENT_ID] == 'other'
        assert owned[PROP_SOURCE_DATASET] == 'tank/vol'

    def test_no_zfs_create_or_set_exists_on_any_stream_path(self, stream):
        """A create or set failure cannot happen any more: the handler has no such call."""
        stream.create.side_effect = RuntimeError('pool is full')
        stream.set.side_effect = RuntimeError('permission denied')
        for mode in ('fresh', 'resume'):
            stream.exists.side_effect = [True, True, True, False]
            stream.token(TOKEN)
            stream.ws.receive.side_effect = [json.dumps({'done': True})]
            stream.run(mode)
        assert stream.sent == [{'status': 'ok'}, {'status': 'ok'}]
        stream.create.assert_not_called()
        stream.set.assert_not_called()


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
            'error': f'invalid dataset name {bad!r}: {bad} is not a valid ZFS dataset name'
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
            assert 'invalid dataset name' in _sent_by(ws)[0]['error']
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
            assert 'invalid dataset name' in _sent_by(ws)[0]['error']
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


# ---------------------------------------------------------------------------
# The bound id owns the server namespace (plan M10, D5)
# ---------------------------------------------------------------------------


class TestServerPathsUseTheBoundId:
    def test_negotiate_answers_with_a_server_dataset_under_the_bound_id(
        self, client_with_remote, mocker
    ):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=False)
        mocker.patch('zfsbackup.api.zfs.create')
        mocker.patch('zfsbackup.api.zfs.set')
        mocker.patch('zfsbackup.api.zfs.list', return_value=[])
        resp = client_with_remote.post('/backup/client1/pool/data/negotiate', json=_body())
        assert resp.status_code == 200
        assert resp.get_json()['server_dataset'] == SERVER_DS

    def test_negotiate_stamps_the_bound_id_and_the_source_dataset(
        self, client_with_remote, mocker
    ):
        """On an existing dataset; a missing one keeps them in memory (see
        `TestPendingConfigs` in test_zfsbackup_api_negotiate.py)."""
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mock_create = mocker.patch('zfsbackup.api.zfs.create')
        mock_set = mocker.patch('zfsbackup.api.zfs.set')
        mocker.patch('zfsbackup.api.zfs.list', return_value=[])
        encoded = DatasetConfig(name='pool/data').to_property()
        resp = client_with_remote.post(
            '/backup/client1/pool/data/negotiate', json={'snapshots': [], 'config': encoded}
        )
        assert resp.status_code == 200
        mock_create.assert_not_called()
        assert str(mock_set.call_args[0][0]) == SERVER_DS
        props = mock_set.call_args[0][1]
        assert props[PROP_CLIENT_ID] == 'client1'
        assert props[PROP_SOURCE_DATASET] == 'pool/data'
        assert props['org.zfsbackup:config'] == encoded

    def test_each_certificate_gets_its_own_namespace(self, authed_with_remote, mocker):
        """Two clients negotiating the same client dataset name get disjoint server datasets."""
        mocker.patch('zfsbackup.api.zfs.exists', return_value=False)
        mocker.patch('zfsbackup.api.zfs.create')
        mocker.patch('zfsbackup.api.zfs.set')
        mocker.patch('zfsbackup.api.zfs.list', return_value=[])
        other = authed_with_remote.cert_for('client2')
        a = authed_with_remote.client().post('/backup/client1/pool/data/negotiate', json=_body())
        b = authed_with_remote.client(other).post('/backup/client2/pool/data/negotiate', json=_body())
        assert a.get_json()['server_dataset'] == 'pool/backups/client1/data'
        assert b.get_json()['server_dataset'] == 'pool/backups/client2/data'

    def test_the_stream_handler_ignores_the_client_id_it_is_called_with(self, stream):
        """The view's `client_id` argument is the URL's; the binding (`g.client_id`) is what
        names the server dataset, the claim and the owned properties."""
        stream.exists.side_effect = [True, True, False]
        with stream.authed.context(STREAM_PATH, query_string={'mode': 'resume'}):
            stream.handler(stream.ws, 'someone-else', 'pool/data')
        assert str(stream.receive.call_args[0][0]) == SERVER_DS
        assert stream.receive.call_args.kwargs['props'][PROP_CLIENT_ID] == 'client1'
        stream.set.assert_not_called()

    def test_resume_token_looks_under_the_bound_id(self, authed_with_remote, mocker):
        exists = mocker.patch('zfsbackup.api.zfs.exists', return_value=False)
        resp = authed_with_remote.client().get('/backup/client1/pool/data/resume_token')
        assert resp.status_code == 200
        assert str(exists.call_args[0][0]) == SERVER_DS


class TestBadDatasetNamesAre400:
    """A client dataset that makes `<target>/<id>/<rest>` an invalid filesystem name is a 400
    (or a websocket error), never a 500, and no `zfs` command sees it."""

    BAD = 'pool/a%25b'          # the URL decodes to `pool/a%b`

    @pytest.fixture
    def zfs(self, mocker):
        return {n: mocker.patch(f'zfsbackup.api.zfs.{n}') for n in ('exists', 'list', 'create', 'set')}

    def test_negotiate(self, client_with_remote, zfs):
        resp = client_with_remote.post(f'/backup/client1/{self.BAD}/negotiate', json={'snapshots': []})
        assert resp.status_code == 400
        assert 'invalid dataset name' in resp.get_json()['error']
        for name, mock in zfs.items():
            assert not mock.called, name

    def test_resume_token(self, client_with_remote, zfs):
        resp = client_with_remote.get(f'/backup/client1/{self.BAD}/resume_token')
        assert resp.status_code == 400
        assert 'invalid dataset name' in resp.get_json()['error']
        for name, mock in zfs.items():
            assert not mock.called, name

    def test_the_gate_is_balanced(self, authed_with_remote, zfs):
        client = authed_with_remote.client()
        client.post(f'/backup/client1/{self.BAD}/negotiate', json={})
        client.get(f'/backup/client1/{self.BAD}/resume_token')
        assert authed_with_remote.gate.active == 0

    def test_stream_sends_an_error_and_takes_no_claim(self, stream):
        stream.run('fresh', dataset='pool/a%b')
        assert len(stream.sent) == 1 and 'invalid dataset name' in stream.sent[0]['error']
        stream.untouched()
        assert stream.claimed == set()


class TestOwnershipOrderInResumeToken:
    """`resume_token` reads the dataset's properties before it lists it (the ownership check
    comes first, so another client's token is never read)."""

    URL = '/backup/client1/pool/data/resume_token'

    def test_a_conflict_is_409_and_nothing_is_listed(
        self, client_with_remote, mocker, source_props
    ):
        source_props.return_value = {PROP_CLIENT_ID: 'someone-else'}
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        lister = mocker.patch('zfsbackup.api.zfs.list')
        resp = client_with_remote.get(self.URL)
        assert resp.status_code == 409
        assert 'someone-else' not in resp.get_data(as_text=True)
        lister.assert_not_called()

    def test_a_source_dataset_mismatch_is_409_too(self, client_with_remote, mocker, source_props):
        source_props.return_value = {PROP_SOURCE_DATASET: 'tank/data'}
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        lister = mocker.patch('zfsbackup.api.zfs.list')
        resp = client_with_remote.get(self.URL)
        assert resp.status_code == 409
        assert 'tank/data' in resp.get_json()['error']
        lister.assert_not_called()

    def test_a_property_read_failure_is_500_and_nothing_is_listed(
        self, client_with_remote, mocker, source_props
    ):
        source_props.side_effect = RuntimeError('zfs get failed')
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        lister = mocker.patch('zfsbackup.api.zfs.list')
        resp = client_with_remote.get(self.URL)
        assert resp.status_code == 500
        assert resp.get_json() == {'error': 'zfs get failed'}
        lister.assert_not_called()

    def test_properties_are_read_before_the_list(self, client_with_remote, mocker, source_props):
        events = []
        source_props.side_effect = lambda ds: events.append('properties') or {}
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.api.zfs.list', side_effect=lambda **k: events.append('list') or [])
        assert client_with_remote.get(self.URL).status_code == 200
        assert events == ['properties', 'list']

    def test_the_gate_is_balanced_after_a_conflict(
        self, authed_with_remote, mocker, source_props
    ):
        source_props.return_value = {PROP_CLIENT_ID: 'someone-else'}
        mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        authed_with_remote.client().get(self.URL)
        assert authed_with_remote.gate.active == 0


class TestRequestSizeLimit:
    def test_the_limit_is_one_mebibyte(self, authed_with_remote):
        assert authed_with_remote.app.config['MAX_CONTENT_LENGTH'] == 1024 * 1024

    def test_an_oversized_negotiate_body_is_a_json_413(self, authed_with_remote, mocker):
        zfs_exists = mocker.patch('zfsbackup.api.zfs.exists')
        resp = authed_with_remote.client().post(
            '/backup/client1/pool/data/negotiate',
            data=b'{"config": "' + b'x' * (1024 * 1024) + b'"}',
            content_type='application/json',
        )
        assert resp.status_code == 413
        assert resp.is_json and 'too large' in resp.get_json()['error']
        zfs_exists.assert_not_called()
        assert authed_with_remote.gate.active == 0

    def test_a_body_under_the_limit_is_served(self, authed_with_remote, mocker):
        mocker.patch('zfsbackup.api.zfs.exists', return_value=False)
        mocker.patch('zfsbackup.api.zfs.create')
        mocker.patch('zfsbackup.api.zfs.set')
        mocker.patch('zfsbackup.api.zfs.list', return_value=[])
        resp = authed_with_remote.client().post(
            '/backup/client1/pool/data/negotiate',
            data=json.dumps(_body(pad='x' * (512 * 1024))).encode(),
            content_type='application/json',
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# R1: the per-dataset claim is released BEFORE the final reply is sent
# ---------------------------------------------------------------------------


def _release_cell(app):
    """The closure cell of `create_app` that holds `_release_dataset`, as `_receive_stream`
    sees it (its `release_claim` closure calls it)."""
    import inspect

    handler = app.view_functions['backup_stream'].__wrapped__
    receive = inspect.getclosurevars(handler).nonlocals['_receive_stream']
    return receive.__closure__[receive.__code__.co_freevars.index('_release_dataset')]


@contextlib.contextmanager
def _spy_releases(s):
    """Every real release of a claim of `s` (a `Stream`), as `(server dataset, sends on
    s.ws so far)`; the real release still happens. `s.order` also gets a 'release' entry."""
    cell = _release_cell(s.app)
    real = cell.cell_contents
    log = []

    def spy(name):
        log.append((name, len(s.ws.send.call_args_list)))
        s.order.append('release')
        real(name)

    cell.cell_contents = spy
    try:
        yield log
    finally:
        cell.cell_contents = real


@pytest.fixture
def releases(stream):
    with _spy_releases(stream) as log:
        yield log


class TestClaimReleasedBeforeTheReply:
    """`_receive_stream` hands `_receive_stream_locked` a once-only `release_claim`. The rule:
    the claim is released before ANY reply is sent (the `ok` after `writer.close()`, an error
    once the receive is reaped, an early refusal, the dry-run `ok`), and in any case by the
    `finally` -- but never twice, because after the first call the name may belong to
    another stream."""

    def test_the_claim_is_already_gone_when_ok_is_sent(self, stream, releases):
        seen = []

        def send(message):
            stream.order.append('send')
            seen.append((json.loads(message), stream.claimed))

        stream.ws.send.side_effect = send
        stream.writer.close.side_effect = lambda *a: stream.order.append('close')
        stream.run('resume')
        assert seen == [({'status': 'ok'}, set())]
        assert stream.order[-3:] == ['close', 'release', 'send']      # reaped, released, answered
        assert releases == [(SERVER_DS, 0)]                           # once, before any reply
        assert stream.claimed == set()
        assert stream.gate.active == 0

    @pytest.mark.parametrize('how', ['done', 'write-error'])
    def test_the_claim_is_held_for_as_long_as_zfs_receive_is_being_reaped(self, stream, releases, how):
        """Early does not mean before `writer.close()`: until the receive process has been
        waited for it still works on the dataset, and a second stream would race it."""
        during_close = []
        stream.writer.close.side_effect = lambda *a: during_close.append(stream.claimed)
        if how == 'write-error':
            stream.writer.write.side_effect = BrokenPipeError(32, 'Broken pipe')
            stream.ws.receive.side_effect = [b'chunk']
        stream.run('resume')
        assert during_close == [{SERVER_DS}]
        assert releases == [(SERVER_DS, 0)]
        assert stream.claimed == set()

    def test_a_second_stream_started_from_inside_the_ok_send_is_served_not_busy(self, stream, releases):
        inner = _ws()
        inner.receive.side_effect = [b'next', json.dumps({'done': True})]
        started = []

        def send(message):
            if not started:
                started.append(True)
                stream.run('resume', ws=inner)      # the client's next stream, on the ack

        stream.ws.send.side_effect = send
        stream.run('resume')
        assert started == [True]
        assert _sent_by(inner) == [{'status': 'ok'}]               # served, not busy
        assert stream.receive.call_count == 2
        assert stream.writer.close.call_count == 2
        assert stream.claimed == set()
        assert len(releases) == 2                                  # one per stream, not per call site

    def test_a_dry_run_releases_once_before_its_ok(self, mocker):
        s = Stream(mocker, dry_run=True)
        with _spy_releases(s) as log:
            s.run('fresh')
        assert log == [(SERVER_DS, 0)]
        assert s.sent == [{'status': 'ok', 'dry_run': True}]
        assert s.claimed == set()
        assert s.gate.active == 0

    # -- every failure path: exactly one release, an empty set afterwards ------------

    @staticmethod
    def _close_raises(s, props):
        s.writer.close.side_effect = RuntimeError('cannot receive: x')

    @staticmethod
    def _write_error_then_close_raises(s, props):
        s.writer.write.side_effect = BrokenPipeError(32, 'Broken pipe')
        s.writer.close.side_effect = RuntimeError('cannot receive: y')

    @staticmethod
    def _write_error_close_ok(s, props):
        s.writer.write.side_effect = BrokenPipeError(32, 'Broken pipe')

    @staticmethod
    def _idle_timeout(s, props):
        s.ws.receive.side_effect = [None]

    @staticmethod
    def _connection_closed(s, props):
        s.ws.receive.side_effect = [b'chunk', ConnectionClosed(None, None)]

    @staticmethod
    def _receive_raises(s, props):
        s.receive.side_effect = RuntimeError('boom')

    @staticmethod
    def _ownership(s, props):
        props.return_value = {'org.zfsbackup:source_dataset': 'tank/data'}

    @staticmethod
    def _ownership_check_raises(s, props):
        props.side_effect = RuntimeError('zfs exploded')

    @staticmethod
    def _config_read_fails(s, props):
        s.get_value.side_effect = RuntimeError('cannot get')

    @staticmethod
    def _no_config_recorded(s, props):
        s.stored = None

    @staticmethod
    def _token_lookup_fails(s, props):
        s.fail_token_lookup(RuntimeError('cannot list'))

    @staticmethod
    def _abort_fails(s, props):
        s.token('1-token')
        s.abort.side_effect = RuntimeError('cannot receive: permission denied')

    @staticmethod
    def _not_negotiated_missing(s, props):
        s.exists.return_value = False        # no dataset and nothing pending

    @staticmethod
    def _placeholder_overwrite(s, props):
        s.writer.close.side_effect = RuntimeError(
            'cannot receive new filesystem stream: zfs receive -F cannot be used to destroy an '
            'encrypted filesystem or overwrite an unencrypted one with an encrypted one'
        )

    @staticmethod
    def _ack_send_raises(s, props):
        s.ws.send.side_effect = RuntimeError('socket gone')

    @staticmethod
    def _ack_send_closed(s, props):
        s.ws.send.side_effect = ConnectionClosed(None, None)

    # id -> (arrange, replies, `close` calls). Every claim is released with NO reply sent yet.
    CASES = {
        'close-raises': ('_close_raises', ['cannot receive: x'], 1),
        'write-error-then-close-raises': ('_write_error_then_close_raises',
                                          ['cannot receive: y'], 1),
        'write-error-close-ok': ('_write_error_close_ok', ['Broken pipe'], 1),
        'idle-timeout': ('_idle_timeout', ['no data from the client'], 1),
        'connection-closed': ('_connection_closed', [], 1),
        'receive-filesystem-raises': ('_receive_raises',
                                      ['Failed to start zfs receive: boom'], 0),
        'ownership-refusal': ('_ownership', ['refusing to mix in'], 0),
        'ownership-check-raises': ('_ownership_check_raises', ['Failed to check'], 0),
        'config-read-fails': ('_config_read_fails',
                              ['Failed to read the configuration'], 0),
        'no-config-recorded': ('_no_config_recorded', ['no configuration is recorded'], 0),
        'token-lookup-fails': ('_token_lookup_fails', ['for partial receive state'], 0),
        'abort-fails': ('_abort_fails', ['Failed to discard'], 0),
        'not-negotiated-missing': ('_not_negotiated_missing', ['negotiate again'], 0),
        'placeholder-overwrite': ('_placeholder_overwrite', ['placeholder'], 1),
        'ack-send-raises': ('_ack_send_raises', None, 1),
        'ack-send-connection-closed': ('_ack_send_closed', None, 1),
    }

    @pytest.mark.parametrize('case', list(CASES))
    def test_every_failure_path_releases_once_before_any_reply_and_leaves_the_set_empty(
        self, stream, releases, source_props, case
    ):
        arrange, replies, closes = self.CASES[case]
        getattr(self, arrange)(stream, source_props)
        stream.run('fresh')                                        # must not raise
        assert releases == [(SERVER_DS, 0)]                        # once, before the first send
        assert stream.claimed == set()
        assert stream.gate.active == 0
        assert stream.writer.close.call_count == closes
        if replies is not None:
            sent = stream.sent
            if replies:
                assert len(sent) == 1 and replies[0] in sent[0]['error']
            else:
                assert sent == []
        # ...and the name is free for the next stream
        stream.ws.send.side_effect = None
        stream.ws.receive.side_effect = [json.dumps({'done': True})]
        for mock in (stream.receive, stream.list, stream.abort, stream.create, stream.exists,
                     stream.get_value, stream.writer.close, stream.writer.write, source_props):
            mock.side_effect = None
        stream.exists.return_value = True
        stream.get_value.return_value = stream.stored = canonical_dataset_config(GOOD_CONFIG)
        stream.list.return_value = []
        source_props.return_value = {}
        stream.run('resume')
        assert stream.sent[-1] == {'status': 'ok'}

    def test_an_unsendable_ownership_refusal_releases_once_before_the_send(self, stream, releases, source_props):
        source_props.return_value = {'org.zfsbackup:source_dataset': 'tank/data'}
        stream.ws.send.side_effect = RuntimeError('socket gone')
        with pytest.raises(RuntimeError, match='socket gone'):
            stream.run('resume')
        assert releases == [(SERVER_DS, 0)]       # released before the (failing) send
        assert stream.claimed == set()

    # -- exactly once: the finally must not drop another stream's claim ---------------

    @pytest.mark.parametrize('how', ['ok', 'close-raises', 'ack-send-raises', 'config-read-fails'])
    def test_a_stream_that_claimed_the_name_after_the_early_release_keeps_its_claim(
        self, stream, how
    ):
        """Between the first handler's early release and its `finally`, a second stream (on
        another thread) claims the same name. The `finally` must not release it again."""
        holder = {}

        def take_over():
            holder['held'] = HeldStream(stream)
            holder['held'].__enter__()          # parked in ws.receive, holding the claim
            assert stream.claimed == {SERVER_DS}

        if how == 'config-read-fails':
            # an early refusal: released before the reply, the finally must not release again
            stream.get_value.side_effect = [RuntimeError('cannot get'), stream.stored]
            stream.ws.send.side_effect = lambda m: take_over()
        elif how == 'close-raises':
            stream.writer.close.side_effect = [RuntimeError('cannot receive: x'), None]
            stream.ws.send.side_effect = lambda m: take_over()      # the error reply
        elif how == 'ack-send-raises':
            def send(message):
                if 'held' not in holder:
                    take_over()
                    raise RuntimeError('socket gone')
            stream.ws.send.side_effect = send
        else:
            stream.ws.send.side_effect = lambda m: take_over()      # the ok
        try:
            stream.run('resume')                                   # the first handler returns
            held = holder['held']
            assert stream.claimed == {SERVER_DS}                   # THEIR claim survived our finally
            third = _ws()
            stream.run('resume', ws=third)
            assert _sent_by(third) == [{'error': 'busy', 'retry': True}]
            assert stream.claimed == {SERVER_DS}
        finally:
            holder['held'].release()
        assert held.errors == []
        assert _sent_by(held.ws) == [{'status': 'ok'}]
        assert stream.claimed == set()
        assert stream.gate.active == 0

    # -- the reload drain still waits for the ack -------------------------------------

    def test_the_gate_still_counts_the_stream_while_the_ok_is_sent(self, stream, releases):
        active = []
        stream.ws.send.side_effect = lambda m: active.append((stream.gate.active, stream.claimed))
        stream.run('resume')
        assert active == [(1, set())]       # claim free, but the drain still sees the stream
        assert stream.gate.active == 0

"""Unit tests for RemoteBackupManager and ClientIdentity."""

import logging
import re
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from zfsbackup.config import (
    BackupConfig, DatasetConfig, RemoteDatasetConfig, Destination, RetentionRule, Duration,
)
from zfsbackup.backup_manager import DatasetInfo, DatasetManager
from zfsbackup.remote import ClientIdentity, NegotiationFailed, RemoteBackupManager


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


def _finite_stream():
    """A `zfs send` stand-in that ends at once (never an endless MagicMock read)."""
    stream = MagicMock()
    stream.read.return_value = b''
    return stream


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
        result = rbm._negotiate('http://backup.example.com', 'client1', dsi, snaps, 'offsite')
        assert result == 'autosnap_20240101120000'

    def test_negotiate_returns_none_when_no_common(self, tmp_path, requests_mock):
        rbm, dsi = self._make_rbm(tmp_path)
        requests_mock.post(
            f'http://backup.example.com/backup/client1/{dsi.name}/negotiate',
            json={'common_snapshot': None, 'server_dataset': 'pool/b/c1'},
        )
        result = rbm._negotiate('http://backup.example.com', 'client1', dsi, [], 'offsite')
        assert result is None

    def test_negotiate_returns_none_on_error(self, tmp_path, requests_mock):
        import requests
        rbm, dsi = self._make_rbm(tmp_path)
        requests_mock.post(
            f'http://backup.example.com/backup/client1/{dsi.name}/negotiate',
            exc=requests.ConnectionError,
        )
        # A failed negotiation is NOT "no common snapshot": it raises, so the
        # caller can never fall through to a needless full send.
        with pytest.raises(NegotiationFailed) as exc:
            rbm._negotiate('http://backup.example.com', 'client1', dsi, [], 'offsite')
        assert exc.value.reloading is False

    def test_negotiate_passes_destination_through_to_property(self, tmp_path, requests_mock):
        """The whole point of the 5th `destination` argument: `_negotiate` must
        call `to_property(destination)` with the destination it is negotiating
        for, not the dataset-level rules unconditionally (item 3b)."""
        config = _make_config(
            client_id_file=tmp_path / 'client_id',
            datasets=[DatasetConfig(
                name='pool/data',
                retention_rules=[RetentionRule(Duration('1h'), Duration('1d'))],
                remote=[RemoteDatasetConfig(
                    destination='offsite',
                    retention_rules=[RetentionRule(Duration('1d'), Duration('5y'))],
                )],
            )],
        )
        manager = DatasetManager(config)
        rbm = RemoteBackupManager(config, manager)
        dsi = manager.datasets[0]

        requests_mock.post(
            f'http://backup.example.com/backup/client1/{dsi.name}/negotiate',
            json={'common_snapshot': None, 'server_dataset': 'pool/b/c1'},
        )
        rbm._negotiate('http://backup.example.com', 'client1', dsi, [], 'offsite')

        sent_config = requests_mock.last_request.json()['config']
        # The destination declared its own retention, so the negotiated config
        # must carry THAT ruleset -- not the dataset-level one.
        assert sent_config == dsi.config.to_property('offsite')
        assert sent_config != dsi.config.to_property()


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

    def test_falls_back_to_fresh_send_on_resume_token_error(self, tmp_path, requests_mock, mocker):
        import requests as _requests
        rbm, dsi = self._make_rbm(tmp_path)
        requests_mock.get(
            f'http://backup.example.com/backup/client1/{dsi.name}/resume_token',
            exc=_requests.ConnectionError,
        )
        mock_stream = MagicMock()
        mocker.patch('zfsbackup.remote.zfs.send.snapshot', return_value=mock_stream)
        latest = MagicMock()
        latest.snapshot = MagicMock()
        stream = rbm._get_resume_stream('http://backup.example.com', 'client1', dsi, latest, None)
        assert stream is mock_stream


class TestTransfer:
    def _make_rbm(self, tmp_path):
        config = _make_config(client_id_file=tmp_path / 'client_id')
        manager = DatasetManager(config)
        return RemoteBackupManager(config, manager), manager.datasets[0]

    def _make_snap(self, name='autosnap_20240101120000'):
        snap = MagicMock()
        snap.name = name
        snap.snapshot = MagicMock()
        return snap

    def test_returns_false_when_stream_is_none(self, tmp_path, mocker):
        rbm, dsi = self._make_rbm(tmp_path)
        mocker.patch.object(rbm, '_get_resume_stream', return_value=None)
        latest = self._make_snap()
        result = rbm._transfer('http://backup.example.com', 'client1', dsi, latest, None, 'offsite')
        assert result is False

    def test_dry_run_returns_true_without_websocket(self, tmp_path, mocker):
        config = _make_config(client_id_file=tmp_path / 'client_id', dry_run=True)
        manager = DatasetManager(config)
        rbm = RemoteBackupManager(config, manager)
        dsi = manager.datasets[0]

        mock_stream = MagicMock()
        mocker.patch.object(rbm, '_get_resume_stream', return_value=mock_stream)
        mock_ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')

        latest = self._make_snap()
        result = rbm._transfer('http://backup.example.com', 'client1', dsi, latest, None, 'offsite')
        assert result is True
        mock_ws_cls.assert_not_called()

    def test_connection_error_returns_false(self, tmp_path, mocker):
        rbm, dsi = self._make_rbm(tmp_path)
        mock_stream = MagicMock()
        mocker.patch.object(rbm, '_get_resume_stream', return_value=mock_stream)

        mock_ws = MagicMock()
        mock_ws.connect.side_effect = Exception('connection refused')
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=mock_ws)

        latest = self._make_snap()
        result = rbm._transfer('http://backup.example.com', 'client1', dsi, latest, None, 'offsite')
        assert result is False
        mock_ws.close.assert_called()

    def test_server_error_response_returns_false(self, tmp_path, mocker):
        import json as _json
        rbm, dsi = self._make_rbm(tmp_path)

        mock_stream = MagicMock()
        mock_stream.read.side_effect = [b'data', b'']
        mocker.patch.object(rbm, '_get_resume_stream', return_value=mock_stream)

        mock_ws = MagicMock()
        mock_ws.recv.return_value = _json.dumps({'status': 'error', 'error': 'disk full'})
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=mock_ws)

        latest = self._make_snap()
        result = rbm._transfer('http://backup.example.com', 'client1', dsi, latest, None, 'offsite')
        assert result is False

    def test_successful_transfer_sets_anchor(self, tmp_path, mocker):
        import json as _json
        rbm, dsi = self._make_rbm(tmp_path)

        mock_stream = MagicMock()
        mock_stream.read.side_effect = [b'chunk1', b'chunk2', b'']
        mocker.patch.object(rbm, '_get_resume_stream', return_value=mock_stream)

        mock_ws = MagicMock()
        mock_ws.recv.return_value = _json.dumps({'status': 'ok'})
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=mock_ws)

        mock_set_anchor = mocker.patch.object(rbm.local_manager, 'set_anchor')

        latest = self._make_snap('autosnap_20240202120000')
        result = rbm._transfer('http://backup.example.com', 'client1', dsi, latest, None, 'offsite')
        assert result is True
        mock_set_anchor.assert_called_once_with(dsi, 'offsite', 'autosnap_20240202120000')

    def test_successful_transfer_logs_superseded_anchor(self, tmp_path, mocker):
        import json as _json
        rbm, dsi = self._make_rbm(tmp_path)

        mock_stream = MagicMock()
        mock_stream.read.side_effect = [b'']
        mocker.patch.object(rbm, '_get_resume_stream', return_value=mock_stream)

        mock_ws = MagicMock()
        mock_ws.recv.return_value = _json.dumps({'status': 'ok'})
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=mock_ws)

        mocker.patch.object(rbm.local_manager, 'get_anchor', return_value='autosnap_OLD')
        mocker.patch.object(rbm.local_manager, 'set_anchor')

        latest = self._make_snap('autosnap_20240202120000')
        result = rbm._transfer('http://backup.example.com', 'client1', dsi, latest, None, 'offsite')
        assert result is True

    def test_incremental_transfer_uses_ws_url(self, tmp_path, mocker):
        import json as _json
        rbm, dsi = self._make_rbm(tmp_path)

        mock_stream = MagicMock()
        mock_stream.read.side_effect = [b'', ]
        mocker.patch.object(rbm, '_get_resume_stream', return_value=mock_stream)

        mock_ws = MagicMock()
        mock_ws.recv.return_value = _json.dumps({'status': 'ok'})
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=mock_ws)
        mocker.patch.object(rbm.local_manager, 'set_anchor')

        latest = self._make_snap('autosnap_20240202120000')
        common = self._make_snap('autosnap_20240101120000')

        result = rbm._transfer('http://backup.example.com', 'client1', dsi, latest, common, 'offsite')
        assert result is True
        connect_url = mock_ws.connect.call_args[0][0]
        assert 'from=autosnap_20240101120000' in connect_url


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

    def test_resolves_common_snap_from_negotiate_result(self, tmp_path, requests_mock, mocker):
        config = _make_config(client_id_file=tmp_path / 'client_id')
        manager = DatasetManager(config)
        rbm = RemoteBackupManager(config, manager)
        dsi = manager.datasets[0]
        remote_cfg = dsi.config.remote[0]

        requests_mock.post('http://backup.example.com/backup/register', json={})

        client_id = rbm.identity.client_id
        old_snap_name = 'autosnap_20240101120000'
        new_snap_name = 'autosnap_20240202120000'

        from libzfseasy.types import Filesystem, Snapshot as ZFSSnapshot
        fs = Filesystem('pool/data')
        old_snap = ZFSSnapshot(fs, old_snap_name)
        new_snap = ZFSSnapshot(fs, new_snap_name)

        from zfsbackup.backup_manager import SnapshotInfo
        old_info = SnapshotInfo(old_snap, 'autosnap')
        new_info = SnapshotInfo(new_snap, 'autosnap')

        mocker.patch.object(manager, 'list_snapshots', return_value=[new_info, old_info])

        requests_mock.post(
            f'http://backup.example.com/backup/{client_id}/{dsi.name}/negotiate',
            json={'common_snapshot': old_snap_name, 'server_dataset': 'pool/backups/c/pool/data'},
        )

        # Mock _transfer to avoid WebSocket and stream setup; we're testing the
        # common_snap resolution logic in backup_dataset (line 85).
        captured = {}

        def capture_transfer(base_url, cid, d, latest, common_snap, destination):
            captured['common_snap'] = common_snap
            return True

        mocker.patch.object(rbm, '_transfer', side_effect=capture_transfer)

        result = rbm.backup_dataset(dsi, remote_cfg)
        assert result is True
        assert captured.get('common_snap') is old_info

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


class TestBackupDatasetNegotiateWiring:
    """`backup_dataset` passes `remote_cfg.destination` into `_negotiate` --
    previously untested: every negotiate test above calls `_negotiate`
    directly with a literal `'offsite'`, and `TestBackupDataset` mocks around
    the real negotiate call, so the one line of production wiring
    (`backup_dataset` -> `_negotiate(..., remote_cfg.destination)`) had no
    coverage. A wrong value here (e.g. `dsi.name` instead of
    `remote_cfg.destination`) would send the wrong destination's rules."""

    def test_backup_dataset_calls_negotiate_with_remote_cfg_destination(
        self, tmp_path, requests_mock, mocker
    ):
        # Deliberately use a destination name distinct from dsi.name (and
        # from other declared destinations) so a swap -- e.g. passing
        # dsi.name, or the first destination in the mapping -- is caught.
        config = _make_config(
            client_id_file=tmp_path / 'client_id',
            datasets=[DatasetConfig(
                name='pool/data',
                remote=[RemoteDatasetConfig(destination='dc2')],
            )],
            destinations={
                'offsite': Destination(url='http://backup.example.com'),
                'dc2': Destination(url='http://dc2.example.com'),
            },
        )
        manager = DatasetManager(config)
        rbm = RemoteBackupManager(config, manager)
        dsi = manager.datasets[0]
        remote_cfg = dsi.config.remote[0]

        requests_mock.post('http://dc2.example.com/backup/register', json={})
        snap = MagicMock()
        snap.name = 'autosnap_20240101120000'
        mocker.patch.object(manager, 'list_snapshots', return_value=[snap])
        mock_negotiate = mocker.patch.object(rbm, '_negotiate', return_value=None)
        mocker.patch.object(rbm, '_transfer', return_value=True)

        result = rbm.backup_dataset(dsi, remote_cfg)
        assert result is True
        mock_negotiate.assert_called_once()
        # Positional signature: (base_url, client_id, dsi, snapshots, destination)
        assert mock_negotiate.call_args.args[-1] == 'dc2'


class TestClientSideRetentionCollisionWarning:
    """Client-side collision warning: previously a colliding *per-destination*
    ruleset warned nowhere on the client -- the only warning appeared in the
    receiving admin's journal, naming a server-side path the client operator
    does not administer, with canonicalized literals. `RemoteBackupManager.
    backup_dataset` now warns locally too, once per `(dataset, destination)`,
    naming the operator's own literals."""

    def _make_rbm(self, tmp_path, retention_rules=None, destination='offsite'):
        config = _make_config(
            client_id_file=tmp_path / 'client_id',
            datasets=[DatasetConfig(
                name='pool/data',
                remote=[RemoteDatasetConfig(
                    destination=destination,
                    retention_rules=retention_rules or [],
                )],
            )],
        )
        manager = DatasetManager(config)
        return RemoteBackupManager(config, manager), manager.datasets[0]

    def _colliding_rules(self):
        # 1h:7d and 1d:1w collide on keep_for -- both are 604800s.
        return [
            RetentionRule(Duration('1h'), Duration('7d')),
            RetentionRule(Duration('1d'), Duration('1w')),
        ]

    def test_warns_once_for_colliding_destination_ruleset(self, tmp_path, requests_mock, caplog):
        rbm, dsi = self._make_rbm(tmp_path, retention_rules=self._colliding_rules())
        requests_mock.post('http://backup.example.com/backup/register', json={})
        with patch.object(rbm.local_manager, 'list_snapshots', return_value=[]):
            with caplog.at_level(logging.WARNING):
                rbm.backup_dataset(dsi, dsi.config.remote[0])
        warnings = [r for r in caplog.records if r.levelname == 'WARNING']
        assert len(warnings) == 1
        assert warnings[0].message.startswith('Dataset pool/data -> offsite:')

    def test_warning_names_operator_literal_not_canonicalized(self, tmp_path, requests_mock, caplog):
        # The message must contain "7d" -- the text the operator wrote --
        # not the numerically-equal-but-different canonicalized "1w". That
        # distinction is the entire point of warning client-side: the
        # server-side message names its own (canonicalized) literals, which
        # this client operator never sees and does not administer.
        rbm, dsi = self._make_rbm(tmp_path, retention_rules=self._colliding_rules())
        requests_mock.post('http://backup.example.com/backup/register', json={})
        with patch.object(rbm.local_manager, 'list_snapshots', return_value=[]):
            with caplog.at_level(logging.WARNING):
                rbm.backup_dataset(dsi, dsi.config.remote[0])
        warnings = [r for r in caplog.records if r.levelname == 'WARNING']
        assert len(warnings) == 1
        assert '7d' in warnings[0].message

    def test_does_not_rewarn_same_ruleset(self, tmp_path, requests_mock, caplog):
        rbm, dsi = self._make_rbm(tmp_path, retention_rules=self._colliding_rules())
        requests_mock.post('http://backup.example.com/backup/register', json={})
        with patch.object(rbm.local_manager, 'list_snapshots', return_value=[]):
            with caplog.at_level(logging.WARNING):
                rbm.backup_dataset(dsi, dsi.config.remote[0])
                rbm.backup_dataset(dsi, dsi.config.remote[0])
                rbm.backup_dataset(dsi, dsi.config.remote[0])
        warnings = [r for r in caplog.records if r.levelname == 'WARNING']
        assert len(warnings) == 1

    def test_warns_separately_for_two_different_destinations(self, tmp_path, requests_mock, caplog):
        rules = self._colliding_rules()
        config = _make_config(
            client_id_file=tmp_path / 'client_id',
            datasets=[DatasetConfig(
                name='pool/data',
                remote=[
                    RemoteDatasetConfig(destination='offsite', retention_rules=rules),
                    RemoteDatasetConfig(destination='dc2', retention_rules=rules),
                ],
            )],
            destinations={
                'offsite': Destination(url='http://backup.example.com'),
                'dc2': Destination(url='http://dc2.example.com'),
            },
        )
        manager = DatasetManager(config)
        rbm = RemoteBackupManager(config, manager)
        dsi = manager.datasets[0]

        requests_mock.post('http://backup.example.com/backup/register', json={})
        requests_mock.post('http://dc2.example.com/backup/register', json={})
        with patch.object(manager, 'list_snapshots', return_value=[]):
            with caplog.at_level(logging.WARNING):
                rbm.backup_dataset(dsi, dsi.config.remote[0])
                rbm.backup_dataset(dsi, dsi.config.remote[1])
        warnings = [r for r in caplog.records if r.levelname == 'WARNING']
        assert len(warnings) == 2
        assert any(w.message.startswith('Dataset pool/data -> offsite:') for w in warnings)
        assert any(w.message.startswith('Dataset pool/data -> dc2:') for w in warnings)

    def test_no_collision_produces_no_warning(self, tmp_path, requests_mock, caplog):
        rules = [RetentionRule(Duration('1h'), Duration('1d'))]
        rbm, dsi = self._make_rbm(tmp_path, retention_rules=rules)
        requests_mock.post('http://backup.example.com/backup/register', json={})
        with patch.object(rbm.local_manager, 'list_snapshots', return_value=[]):
            with caplog.at_level(logging.WARNING):
                rbm.backup_dataset(dsi, dsi.config.remote[0])
        assert [r for r in caplog.records if r.levelname == 'WARNING'] == []


class TestNegotiationFailedAndReload:
    """The server's retryable 503 ("reloading") and every other failure are
    told apart; neither is ever read as "no common snapshot"."""

    URL = 'http://backup.example.com'
    RELOADING = {'error': 'server is reloading; retry later', 'retry': True}

    def _make(self, tmp_path):
        config = _make_config(client_id_file=tmp_path / 'client_id')
        manager = DatasetManager(config)
        rbm = RemoteBackupManager(config, manager)
        dsi = manager.datasets[0]
        return rbm, manager, dsi, dsi.config.remote[0]

    def _post(self, requests_mock, dsi, **kw):
        # any client id: `backup_dataset` uses the manager's own identity
        requests_mock.post(re.compile(rf'{re.escape(self.URL)}/backup/[^/]+/{re.escape(dsi.name)}/negotiate$'), **kw)

    # -- _negotiate ---------------------------------------------------------

    def test_a_retryable_503_is_reloading(self, tmp_path, requests_mock):
        rbm, _, dsi, _ = self._make(tmp_path)
        self._post(requests_mock, dsi, status_code=503, json=self.RELOADING)
        with pytest.raises(NegotiationFailed) as exc:
            rbm._negotiate(self.URL, 'client1', dsi, [], 'offsite')
        assert exc.value.reloading is True

    def test_a_reloading_503_is_not_logged_as_an_error(self, tmp_path, requests_mock, caplog):
        rbm, _, dsi, _ = self._make(tmp_path)
        self._post(requests_mock, dsi, status_code=503, json=self.RELOADING)
        with caplog.at_level(logging.DEBUG), pytest.raises(NegotiationFailed):
            rbm._negotiate(self.URL, 'client1', dsi, [], 'offsite')
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    @pytest.mark.parametrize(
        'kwargs',
        [
            {'status_code': 500, 'json': {'error': 'boom'}},
            {'status_code': 503, 'json': {'error': 'remote_backup disabled'}},          # no retry flag
            {'status_code': 503, 'json': {'error': 'x', 'retry': False}},
            {'status_code': 503, 'text': 'Service Unavailable'},                         # not JSON
            {'status_code': 404, 'json': {}},
            {'status_code': 200, 'text': 'not json'},
        ],
    )
    def test_any_other_failure_is_not_reloading(self, tmp_path, requests_mock, caplog, kwargs):
        rbm, _, dsi, _ = self._make(tmp_path)
        self._post(requests_mock, dsi, **kwargs)
        with caplog.at_level(logging.ERROR), pytest.raises(NegotiationFailed) as exc:
            rbm._negotiate(self.URL, 'client1', dsi, [], 'offsite')
        assert exc.value.reloading is False
        assert 'Negotiation failed' in caplog.text

    @pytest.mark.parametrize('exc_type', ['ConnectionError', 'Timeout'])
    def test_a_connection_error_is_not_reloading(self, tmp_path, requests_mock, exc_type):
        import requests
        rbm, _, dsi, _ = self._make(tmp_path)
        self._post(requests_mock, dsi, exc=getattr(requests, exc_type))
        with pytest.raises(NegotiationFailed) as exc:
            rbm._negotiate(self.URL, 'client1', dsi, [], 'offsite')
        assert exc.value.reloading is False

    def test_a_null_common_snapshot_is_none_not_a_failure(self, tmp_path, requests_mock):
        rbm, _, dsi, _ = self._make(tmp_path)
        self._post(requests_mock, dsi, json={'common_snapshot': None, 'server_dataset': 'x'})
        assert rbm._negotiate(self.URL, 'client1', dsi, [], 'offsite') is None

    def test_a_common_snapshot_is_returned(self, tmp_path, requests_mock):
        rbm, _, dsi, _ = self._make(tmp_path)
        self._post(requests_mock, dsi, json={'common_snapshot': 'autosnap_1'})
        assert rbm._negotiate(self.URL, 'client1', dsi, [], 'offsite') == 'autosnap_1'

    # -- backup_dataset -----------------------------------------------------

    @pytest.fixture
    def wired(self, tmp_path, requests_mock, mocker):
        rbm, manager, dsi, remote_cfg = self._make(tmp_path)
        requests_mock.post(f'{self.URL}/backup/register', json={})
        snap = MagicMock()
        snap.name = 'autosnap_20240101120000'
        mocker.patch.object(manager, 'list_snapshots', return_value=[snap])
        sends = {
            'transfer': mocker.spy(rbm, '_transfer'),
            'resume': mocker.spy(rbm, '_get_resume_stream'),
            # A bare MagicMock stream's `.read()` is always truthy: a URL
            # mismatch that fell through to a real send would loop forever.
            'send_snapshot': mocker.patch('zfsbackup.remote.zfs.send.snapshot', return_value=_finite_stream()),
            'send_resume': mocker.patch('zfsbackup.remote.zfs.send.resume', return_value=_finite_stream()),
        }
        return rbm, dsi, remote_cfg, sends

    def assert_no_send(self, sends):
        for name, mock in sends.items():
            assert not mock.called, f'{name} was called after a failed negotiation'

    def test_reloading_logs_info_returns_false_and_never_sends(self, wired, requests_mock, caplog):
        rbm, dsi, remote_cfg, sends = wired
        self._post(requests_mock, dsi, status_code=503, json=self.RELOADING)
        with caplog.at_level(logging.INFO):
            assert rbm.backup_dataset(dsi, remote_cfg) is False
        infos = [r for r in caplog.records if r.levelno == logging.INFO
                 and r.getMessage() == f'server {self.URL} is reloading; skipping {dsi.name} this cycle']
        assert len(infos) == 1
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
        self.assert_no_send(sends)

    def test_another_failure_logs_an_error_returns_false_and_never_sends(self, wired, requests_mock, caplog):
        rbm, dsi, remote_cfg, sends = wired
        self._post(requests_mock, dsi, status_code=500, json={'error': 'boom'})
        with caplog.at_level(logging.INFO):
            assert rbm.backup_dataset(dsi, remote_cfg) is False
        assert [r for r in caplog.records if r.levelno == logging.ERROR and 'Negotiation failed' in r.getMessage()]
        assert 'is reloading' not in caplog.text
        self.assert_no_send(sends)

    def test_a_connection_error_never_sends(self, wired, requests_mock):
        import requests
        rbm, dsi, remote_cfg, sends = wired
        self._post(requests_mock, dsi, exc=requests.ConnectionError)
        assert rbm.backup_dataset(dsi, remote_cfg) is False
        self.assert_no_send(sends)

    def test_a_null_common_snapshot_still_does_the_full_initial_send(self, wired, requests_mock, mocker):
        rbm, dsi, remote_cfg, sends = wired
        self._post(requests_mock, dsi, json={'common_snapshot': None})
        requests_mock.get(re.compile(r'.*/resume_token$'), json={'resume_token': None})
        ws = MagicMock()
        ws.recv.return_value = '{"status": "ok"}'
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=ws)
        stream = MagicMock()
        stream.read.side_effect = [b'x', b'']
        sends['send_snapshot'].return_value = stream
        mocker.patch.object(rbm.local_manager, 'set_anchor')
        assert rbm.backup_dataset(dsi, remote_cfg) is True
        sends['transfer'].assert_called_once()
        assert sends['transfer'].call_args.args[4] is None            # no common snapshot: full send
        sends['send_snapshot'].assert_called_once()
        assert 'since' not in sends['send_snapshot'].call_args.kwargs

    def test_a_reloading_register_is_false_with_an_info_log_and_goes_no_further(self, wired, requests_mock, caplog):
        rbm, dsi, remote_cfg, sends = wired
        requests_mock.post(f'{self.URL}/backup/register', status_code=503, json=self.RELOADING)
        negotiate = requests_mock.post(re.compile(r'.*/negotiate$'), json={})
        with caplog.at_level(logging.INFO):
            assert rbm.backup_dataset(dsi, remote_cfg) is False
        assert 'is reloading; registration skipped this cycle' in caplog.text
        assert 'Registration failed' not in caplog.text
        assert negotiate.call_count == 0
        self.assert_no_send(sends)

    def test_a_non_retry_register_503_is_a_registration_failure(self, wired, requests_mock, caplog):
        rbm, dsi, remote_cfg, sends = wired
        requests_mock.post(f'{self.URL}/backup/register', status_code=503, json={'error': 'remote_backup disabled'})
        with caplog.at_level(logging.INFO):
            assert rbm.backup_dataset(dsi, remote_cfg) is False
        assert 'Registration failed' in caplog.text

    # -- resume_token -------------------------------------------------------

    def test_a_reloading_resume_token_skips_the_dataset_without_a_fresh_send(self, tmp_path, requests_mock, mocker, caplog):
        rbm, manager, dsi, _ = self._make(tmp_path)
        send_snapshot = mocker.patch('zfsbackup.remote.zfs.send.snapshot')
        send_resume = mocker.patch('zfsbackup.remote.zfs.send.resume')
        requests_mock.get(
            f'{self.URL}/backup/client1/{dsi.name}/resume_token', status_code=503, json=self.RELOADING
        )
        latest = MagicMock()
        latest.name = 'autosnap_1'
        with caplog.at_level(logging.INFO):
            assert rbm._get_resume_stream(self.URL, 'client1', dsi, latest, None) is None
        assert f'server {self.URL} is reloading; skipping {dsi.name} this cycle' in caplog.text
        send_snapshot.assert_not_called()
        send_resume.assert_not_called()

    def test_a_reloading_resume_token_makes_transfer_false(self, tmp_path, requests_mock, mocker):
        rbm, manager, dsi, _ = self._make(tmp_path)
        send_snapshot = mocker.patch('zfsbackup.remote.zfs.send.snapshot', return_value=_finite_stream())
        ws_cls = mocker.patch('zfsbackup.remote.websocket.WebSocket')
        requests_mock.get(
            f'{self.URL}/backup/client1/{dsi.name}/resume_token', status_code=503, json=self.RELOADING
        )
        latest = MagicMock()
        latest.name = 'autosnap_1'
        assert rbm._transfer(self.URL, 'client1', dsi, latest, None, 'offsite') is False
        send_snapshot.assert_not_called()
        ws_cls.assert_not_called()

    def test_an_ordinary_resume_token_failure_still_falls_back_to_a_fresh_send(self, tmp_path, requests_mock, mocker):
        rbm, manager, dsi, _ = self._make(tmp_path)
        send_snapshot = mocker.patch('zfsbackup.remote.zfs.send.snapshot', return_value='STREAM')
        requests_mock.get(f'{self.URL}/backup/client1/{dsi.name}/resume_token', status_code=500)
        latest = MagicMock()
        latest.name = 'autosnap_1'
        assert rbm._get_resume_stream(self.URL, 'client1', dsi, latest, None) == 'STREAM'
        send_snapshot.assert_called_once()


class TestUrlPathEncoding:
    """Item 3a: client ids and dataset names are percent-encoded in URL paths
    (`/` and `:` stay literal in a path; `?from=` encodes everything).

    `requests` itself would turn a raw space into `%20`, so these tests capture
    the URL handed to `requests`/`websocket` instead of going through a real
    request, where an unencoded `%`/`#`/`?` would otherwise pass unnoticed."""

    BASE = 'http://backup.example.com'

    def _rbm(self, tmp_path, name='pool/my data'):
        config = _make_config(
            datasets=[DatasetConfig(name=name, remote=[RemoteDatasetConfig(destination='offsite')])],
            client_id_file=tmp_path / 'client_id',
        )
        manager = DatasetManager(config)
        return RemoteBackupManager(config, manager), manager.datasets[0]

    @pytest.mark.parametrize('name, client, quoted', [
        ('pool/my data', 'client 1', 'client%201/pool/my%20data'),
        ('pool/data', 'client1', 'client1/pool/data'),
        ('pool/a:b', 'host-abc12345', 'host-abc12345/pool/a:b'),
        ('pool/data', 'c#d?e%f', 'c%23d%3Fe%25f/pool/data'),
        ('pool/dot.ted-and_under', 'my.host-1', 'my.host-1/pool/dot.ted-and_under'),
        ('pool/data', 'hôst-1', 'h%C3%B4st-1/pool/data'),
    ])
    def test_negotiate_url(self, tmp_path, mocker, name, client, quoted):
        rbm, dsi = self._rbm(tmp_path, name)
        post = mocker.patch('zfsbackup.remote.requests.post')
        post.return_value.json.return_value = {'common_snapshot': None}
        post.return_value.status_code = 200
        rbm._negotiate(self.BASE, client, dsi, [], 'offsite')
        assert post.call_args.args[0] == f'{self.BASE}/backup/{quoted}/negotiate'

    @pytest.mark.parametrize('name, client, quoted', [
        ('pool/my data', 'client 1', 'client%201/pool/my%20data'),
        ('pool/data', 'client1', 'client1/pool/data'),
        ('pool/a:b', 'c', 'c/pool/a:b'),
    ])
    def test_resume_token_url(self, tmp_path, mocker, name, client, quoted):
        rbm, dsi = self._rbm(tmp_path, name)
        get = mocker.patch('zfsbackup.remote.requests.get')
        get.return_value.ok = True
        get.return_value.status_code = 200
        get.return_value.json.return_value = {'resume_token': None}
        mocker.patch('zfsbackup.remote.zfs.send.snapshot')
        rbm._get_resume_stream(self.BASE, client, dsi, MagicMock(), None)
        assert get.call_args.args[0] == f'{self.BASE}/backup/{quoted}/resume_token'

    def _stream_url(self, tmp_path, mocker, name, client, common, base=None):
        rbm, dsi = self._rbm(tmp_path, name)
        mocker.patch.object(rbm, '_get_resume_stream', return_value=MagicMock(read=MagicMock(return_value=b'')))
        mocker.patch.object(rbm.local_manager, 'get_anchor', return_value=None)
        mocker.patch.object(rbm.local_manager, 'set_anchor')
        ws = MagicMock()
        ws.recv.return_value = '{"status": "ok"}'
        mocker.patch('zfsbackup.remote.websocket.WebSocket', return_value=ws)
        common_snap = None
        if common is not None:
            common_snap = MagicMock()
            common_snap.name = common
        assert rbm._transfer(base or self.BASE, client, dsi, MagicMock(), common_snap, 'offsite') is True
        return ws.connect.call_args.args[0]

    def test_stream_url_full_send(self, tmp_path, mocker):
        url = self._stream_url(tmp_path, mocker, 'pool/my data', 'client 1', None)
        assert url == 'ws://backup.example.com/backup/client%201/pool/my%20data/stream'
        assert '?' not in url

    def test_stream_url_incremental_encodes_the_snapshot_name_fully(self, tmp_path, mocker):
        url = self._stream_url(tmp_path, mocker, 'pool/my data', 'client 1', 'snap:one two/x')
        assert url == (
            'ws://backup.example.com/backup/client%201/pool/my%20data/stream'
            '?from=snap%3Aone%20two%2Fx'
        )

    def test_a_plain_snapshot_name_is_unchanged(self, tmp_path, mocker):
        url = self._stream_url(tmp_path, mocker, 'pool/data', 'client1', 'autosnap_20240101120000')
        assert url == (
            'ws://backup.example.com/backup/client1/pool/data/stream'
            '?from=autosnap_20240101120000'
        )

    def test_https_becomes_wss_and_the_path_is_still_quoted(self, tmp_path, mocker):
        url = self._stream_url(tmp_path, mocker, 'pool/my data', 'c', None, base='https://backup.example.com')
        assert url == 'wss://backup.example.com/backup/c/pool/my%20data/stream'

    def test_the_query_value_round_trips(self, tmp_path, mocker):
        from urllib.parse import parse_qs, urlsplit
        url = self._stream_url(tmp_path, mocker, 'pool/data', 'c', 'a b:c&d=e#f')
        assert parse_qs(urlsplit(url).query) == {'from': ['a b:c&d=e#f']}

    def test_path_helper_keeps_slash_and_colon_only(self):
        from zfsbackup.remote import _path
        assert _path('pool/a:b c') == 'pool/a:b%20c'
        assert _path('a#b?c%d&e') == 'a%23b%3Fc%25d%26e'
        assert _path('plain-name_1.x') == 'plain-name_1.x'


class TestUrlRoundTripThroughTheServer:
    """The quoted negotiate URL the client builds is decoded by the API's
    routes back to the original client id and dataset name."""

    def test_the_client_quoted_negotiate_url_reaches_the_server_decoded(self, tmp_path, mocker):
        from urllib.parse import urlsplit
        from zfsbackup.api import create_app
        from zfsbackup.config import RemoteServerConfig

        server_config = _make_config(
            datasets=[DatasetConfig(name='pool/srv')],
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
            client_id_file=tmp_path / 'server_client_id',
        )
        app = create_app(server_config)
        app.testing = True
        web = app.test_client()
        exists = mocker.patch('zfsbackup.api.zfs.exists', return_value=True)
        zset = mocker.patch('zfsbackup.api.zfs.set')
        mocker.patch('zfsbackup.api.zfs.list', return_value=[])

        config = _make_config(
            datasets=[DatasetConfig(name='pool/my data', remote=[RemoteDatasetConfig(destination='offsite')])],
            client_id_file=tmp_path / 'client_id',
        )
        manager = DatasetManager(config)
        rbm = RemoteBackupManager(config, manager)
        dsi = manager.datasets[0]

        def deliver(url, json=None, timeout=None):
            path = urlsplit(url).path
            assert path == '/backup/client%201/pool/my%20data/negotiate'
            resp = web.post(path, json=json)       # the server decodes the percent-escapes
            fake = MagicMock()
            fake.status_code = resp.status_code
            fake.ok = resp.status_code < 400
            fake.json.return_value = resp.get_json()
            return fake

        mocker.patch('zfsbackup.remote.requests.post', side_effect=deliver)
        snaps = [MagicMock()]
        snaps[0].name = 'autosnap_20240101120000'
        assert rbm._negotiate('http://backup.example.com', 'client 1', dsi, snaps, 'offsite') is None

        server_fs = exists.call_args.args[0]
        assert server_fs.name == 'pool/backups/client 1/my data'
        props = zset.call_args.args[1]
        assert props['org.zfsbackup:client_id'] == 'client 1'
        assert props['org.zfsbackup:source_dataset'] == 'pool/my data'

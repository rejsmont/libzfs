"""Unit tests for RemoteBackupManager and ClientIdentity."""

import logging
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from zfsbackup.config import (
    BackupConfig, DatasetConfig, RemoteDatasetConfig, Destination, RetentionRule, Duration,
)
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
        result = rbm._negotiate('http://backup.example.com', 'client1', dsi, [], 'offsite')
        assert result is None

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

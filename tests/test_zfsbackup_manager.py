"""Unit tests for DatasetManager."""

import logging
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from libzfseasy.types import Dataset, Filesystem, Snapshot as ZFSSnapshot
from zfsbackup.backup_manager import DatasetManager, DatasetInfo, SnapshotInfo
from zfsbackup.config import (
    BackupConfig, DatasetConfig, RetentionRule, RemoteServerConfig, RemoteDatasetConfig,
)


def _make_config(**kwargs) -> BackupConfig:
    defaults = dict(
        datasets=[DatasetConfig(name='pool/data')],
        snapshot_prefix='autosnap',
    )
    defaults.update(kwargs)
    return BackupConfig(**defaults)


def _make_snap_info(prefix: str, days_old: int) -> SnapshotInfo:
    dt = datetime.now(timezone.utc) - timedelta(days=days_old)
    name = f"{prefix}_{dt.strftime('%Y%m%d%H%M%S')}"
    snap = ZFSSnapshot(Filesystem('pool/data'), name)
    return SnapshotInfo(snap, prefix)


class TestDatasetManagerDatasets:
    def test_datasets_cached(self):
        manager = DatasetManager(_make_config())
        assert manager.datasets is manager.datasets

    def test_datasets_creates_dataset_infos(self):
        config = _make_config(datasets=[
            DatasetConfig(name='pool/a'),
            DatasetConfig(name='pool/b'),
        ])
        manager = DatasetManager(config)
        assert len(manager.datasets) == 2
        assert manager.datasets[0].name == 'pool/a'
        assert manager.datasets[1].name == 'pool/b'

    def test_disabled_datasets_excluded(self):
        config = _make_config(datasets=[
            DatasetConfig(name='pool/a', enabled=True),
            DatasetConfig(name='pool/b', enabled=False),
        ])
        manager = DatasetManager(config)
        assert len(manager.datasets) == 1
        assert manager.datasets[0].name == 'pool/a'

    def test_prefix_from_config(self):
        manager = DatasetManager(_make_config(snapshot_prefix='mysnap'))
        assert manager.prefix == 'mysnap'


class TestVerifyDatasets:
    def test_no_error_when_dataset_exists(self, mocker):
        manager = DatasetManager(_make_config())
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        manager.verify_datasets()

    def test_logs_error_when_dataset_missing(self, mocker, caplog):
        manager = DatasetManager(_make_config())
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=False)
        with caplog.at_level(logging.ERROR):
            manager.verify_datasets()
        assert 'does not exist' in caplog.text


class TestNeedsSnapshot:
    def test_needs_snapshot_when_no_snapshots(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        mocker.patch.object(manager, 'list_snapshots', return_value=[])
        assert manager.needs_snapshot(dsi) is True

    def test_needs_snapshot_when_latest_too_old(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        dsi.config.frequency = timedelta(hours=1)
        snap = MagicMock()
        snap.age = timedelta(hours=2)
        mocker.patch.object(manager, 'list_snapshots', return_value=[snap])
        assert manager.needs_snapshot(dsi) is True

    def test_no_snapshot_needed_when_recent(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        dsi.config.frequency = timedelta(hours=1)
        snap = MagicMock()
        snap.age = timedelta(minutes=30)
        mocker.patch.object(manager, 'list_snapshots', return_value=[snap])
        assert manager.needs_snapshot(dsi) is False


class TestCreateSnapshot:
    def test_creates_snapshot_and_returns_info(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        mock_snap = ZFSSnapshot(Filesystem('pool/data'), 'autosnap_20240101120000')
        mocker.patch('zfsbackup.backup_manager.zfs.snapshot', return_value=mock_snap)
        result = manager.create_snapshot(dsi)
        assert result is not None
        assert result.name.startswith('autosnap_')

    def test_dry_run_skips_zfs_call(self, mocker):
        manager = DatasetManager(_make_config(dry_run=True))
        dsi = manager.datasets[0]
        mock_snap = mocker.patch('zfsbackup.backup_manager.zfs.snapshot')
        result = manager.create_snapshot(dsi)
        assert result is None
        mock_snap.assert_not_called()

    def test_handles_list_result_from_recursive(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        snap1 = ZFSSnapshot(Filesystem('pool/data'), 'autosnap_20240101120000')
        snap2 = ZFSSnapshot(Filesystem('pool/data/sub'), 'autosnap_20240101120000')
        mocker.patch('zfsbackup.backup_manager.zfs.snapshot', return_value=[snap1, snap2])
        result = manager.create_snapshot(dsi)
        assert result is not None

    def test_exception_returns_none(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        mocker.patch('zfsbackup.backup_manager.zfs.snapshot', side_effect=Exception('zfs error'))
        result = manager.create_snapshot(dsi)
        assert result is None

    def test_snapshot_name_uses_prefix(self, mocker):
        manager = DatasetManager(_make_config(snapshot_prefix='mysnap'))
        dsi = manager.datasets[0]
        captured = {}

        def capture_snap(ds, name, **kwargs):
            captured['name'] = name
            return ZFSSnapshot(Filesystem('pool/data'), name)

        mocker.patch('zfsbackup.backup_manager.zfs.snapshot', side_effect=capture_snap)
        manager.create_snapshot(dsi)
        assert captured['name'].startswith('mysnap_')


class TestListSnapshots:
    def test_returns_only_managed_snapshots(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        fs = Filesystem('pool/data')
        managed = ZFSSnapshot(fs, 'autosnap_20240101120000')
        unmanaged = ZFSSnapshot(fs, 'manual_20240101120000')
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[managed, unmanaged])
        result = manager.list_snapshots(dsi)
        assert len(result) == 1
        assert result[0].name == 'autosnap_20240101120000'

    def test_sorted_newest_first(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        fs = Filesystem('pool/data')
        older = ZFSSnapshot(fs, 'autosnap_20240101120000')
        newer = ZFSSnapshot(fs, 'autosnap_20240102120000')
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[older, newer])
        result = manager.list_snapshots(dsi)
        assert result[0].name == 'autosnap_20240102120000'

    def test_exception_returns_empty_list(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        mocker.patch('zfsbackup.backup_manager.zfs.list', side_effect=Exception('err'))
        assert manager.list_snapshots(dsi) == []

    def test_updates_dsi_snapshots(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        fs = Filesystem('pool/data')
        snap = ZFSSnapshot(fs, 'autosnap_20240101120000')
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[snap])
        manager.list_snapshots(dsi)
        assert len(dsi.snapshots) == 1


class TestNeedsPrunning:
    def test_empty_snapshots_returns_empty(self):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        dsi.snapshots = []
        assert manager.needs_prunning(dsi) == []

    def test_no_retention_rules_returns_empty(self):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        dsi.config.retention_rules = []
        dsi.snapshots = [_make_snap_info('autosnap', 1)]
        assert manager.needs_prunning(dsi) == []

    def test_latest_snapshot_always_kept(self):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        dsi.config.retention_rules = [
            RetentionRule(timedelta(hours=1), timedelta(days=1)),
        ]
        snap = _make_snap_info('autosnap', 0)
        dsi.snapshots = [snap]
        to_prune = manager.needs_prunning(dsi)
        assert snap not in to_prune

    def test_anchored_snapshots_excluded_from_pruning(self):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        dsi.config.retention_rules = [
            RetentionRule(timedelta(hours=1), timedelta(hours=2)),
        ]
        snaps = [_make_snap_info('autosnap', 5 + i) for i in range(3)]
        dsi.snapshots = sorted(snaps, key=lambda s: s.timestamp, reverse=True)
        anchors = {snaps[1].name}
        to_prune = manager.needs_prunning(dsi, anchors=anchors)
        prune_names = [s.name for s in to_prune]
        assert snaps[1].name not in prune_names

    def test_snaps_with_none_timestamp_not_added_to_keep_slots(self):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        dsi.config.retention_rules = [
            RetentionRule(timedelta(hours=1), timedelta(days=1)),
        ]
        good = _make_snap_info('autosnap', 0)
        bad_snap = ZFSSnapshot(Filesystem('pool/data'), 'autosnap_badformat')
        bad = SnapshotInfo(bad_snap, 'autosnap')
        # The 'good' snap is newest and always kept; 'bad' has None timestamp
        # and won't match any retention slot, so it can be pruned
        dsi.snapshots = [good, bad]
        to_prune = manager.needs_prunning(dsi)
        assert good not in to_prune


class TestPropertyHelpers:
    def test_get_prop_returns_value(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        mock_prop = MagicMock()
        mock_prop.__str__ = lambda self: 'testvalue'
        mock_ds = MagicMock()
        mock_ds.__getitem__ = MagicMock(return_value=mock_prop)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[mock_ds])
        result = manager._get_prop(dsi.dataset, 'org.test:prop')
        assert result == 'testvalue'

    def test_get_prop_returns_none_on_exception(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        mocker.patch('zfsbackup.backup_manager.zfs.list', side_effect=Exception('err'))
        assert manager._get_prop(dsi.dataset, 'org.test:prop') is None

    def test_get_prop_returns_none_when_prop_is_none(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        mock_ds = MagicMock()
        mock_ds.__getitem__ = MagicMock(return_value=None)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[mock_ds])
        assert manager._get_prop(dsi.dataset, 'org.test:prop') is None

    def test_set_anchor_calls_zfs_set(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        mock_set = mocker.patch('zfsbackup.backup_manager.zfs.set')
        manager.set_anchor(dsi, 'offsite', 'autosnap_20240101120000')
        mock_set.assert_called_once()

    def test_clear_anchor_calls_zfs_inherit(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        mock_inherit = mocker.patch('zfsbackup.backup_manager.zfs.inherit')
        manager.clear_anchor(dsi, 'offsite')
        mock_inherit.assert_called_once()

    def test_get_anchors_empty_when_no_remote(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        dsi.config.remote = []
        assert manager.get_anchors(dsi) == set()

    def test_get_anchors_returns_set_of_names(self, mocker):
        config = _make_config(datasets=[
            DatasetConfig(
                name='pool/data',
                remote=[RemoteDatasetConfig(destination='offsite')],
            ),
        ])
        manager = DatasetManager(config)
        dsi = manager.datasets[0]
        mocker.patch.object(manager, 'get_anchor', return_value='autosnap_20240101120000')
        anchors = manager.get_anchors(dsi)
        assert 'autosnap_20240101120000' in anchors


class TestSyncConfigProperty:
    def test_syncs_when_different(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        mocker.patch.object(manager, '_get_prop', return_value='old_value')
        mock_set = mocker.patch('zfsbackup.backup_manager.zfs.set')
        manager.sync_config_property(dsi)
        mock_set.assert_called_once()

    def test_skips_when_same(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        encoded = dsi.config.to_property()
        mocker.patch.object(manager, '_get_prop', return_value=encoded)
        mock_set = mocker.patch('zfsbackup.backup_manager.zfs.set')
        manager.sync_config_property(dsi)
        mock_set.assert_not_called()


class TestReceivedDatasets:
    def test_no_remote_backup_config_returns_empty(self):
        manager = DatasetManager(_make_config())
        assert manager.received_datasets() == []

    def test_disabled_remote_backup_returns_empty(self):
        config = _make_config()
        config.remote_backup = RemoteServerConfig(target_dataset='pool/backups', enabled=False)
        manager = DatasetManager(config)
        assert manager.received_datasets() == []

    def test_exception_returns_empty(self, mocker):
        config = _make_config()
        config.remote_backup = RemoteServerConfig(target_dataset='pool/backups', enabled=True)
        manager = DatasetManager(config)
        mocker.patch('zfsbackup.backup_manager.zfs.list', side_effect=Exception('err'))
        assert manager.received_datasets() == []


class TestPruneSnapshots:
    def test_no_snapshots_does_nothing(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        mocker.patch.object(manager, 'list_snapshots', return_value=[])
        mock_destroy = mocker.patch('zfsbackup.backup_manager.zfs.destroy')
        manager.prune_snapshots(dsi)
        mock_destroy.assert_not_called()

    def test_dry_run_skips_destroy(self, mocker):
        manager = DatasetManager(_make_config(dry_run=True))
        dsi = manager.datasets[0]
        snaps = [_make_snap_info('autosnap', 5 + i) for i in range(3)]
        dsi.config.retention_rules = [
            RetentionRule(timedelta(hours=1), timedelta(hours=2)),
        ]
        mocker.patch.object(manager, 'list_snapshots', return_value=snaps)
        mocker.patch.object(manager, 'get_anchors', return_value=set())
        mocker.patch.object(manager, 'needs_prunning', return_value=snaps[1:])
        mock_destroy = mocker.patch('zfsbackup.backup_manager.zfs.destroy')
        manager.prune_snapshots(dsi)
        mock_destroy.assert_not_called()

    def test_destroy_called_for_batch(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        snaps = [_make_snap_info('autosnap', 5 + i) for i in range(3)]
        mocker.patch.object(manager, 'list_snapshots', return_value=snaps)
        mocker.patch.object(manager, 'get_anchors', return_value=set())
        mocker.patch.object(manager, 'needs_prunning', return_value=snaps[1:])
        mock_destroy = mocker.patch('zfsbackup.backup_manager.zfs.destroy')
        manager.prune_snapshots(dsi)
        mock_destroy.assert_called_once()

    def test_destroy_exception_does_not_propagate(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        snaps = [_make_snap_info('autosnap', 5 + i) for i in range(3)]
        mocker.patch.object(manager, 'list_snapshots', return_value=snaps)
        mocker.patch.object(manager, 'get_anchors', return_value=set())
        mocker.patch.object(manager, 'needs_prunning', return_value=snaps[1:])
        mocker.patch('zfsbackup.backup_manager.zfs.destroy', side_effect=Exception('destroy failed'))
        manager.prune_snapshots(dsi)  # must not raise


class TestDatasetReport:
    def test_smoke(self):
        config = _make_config(datasets=[
            DatasetConfig(
                name='pool/data',
                remote=[RemoteDatasetConfig(destination='offsite')],
            ),
        ])
        manager = DatasetManager(config)
        manager.dataset_report()  # must not raise

    def test_no_remote(self):
        manager = DatasetManager(_make_config())
        manager.dataset_report()  # must not raise


class TestSyncConfigPropertyException:
    def test_zfs_set_exception_does_not_propagate(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        mocker.patch.object(manager, '_get_prop', return_value='old_value')
        mocker.patch('zfsbackup.backup_manager.zfs.set', side_effect=Exception('set failed'))
        manager.sync_config_property(dsi)  # must not raise

    def test_sync_all_iterates_all_datasets(self, mocker):
        config = _make_config(datasets=[
            DatasetConfig(name='pool/a'),
            DatasetConfig(name='pool/b'),
        ])
        manager = DatasetManager(config)
        calls = []
        mocker.patch.object(manager, 'sync_config_property', side_effect=lambda dsi: calls.append(dsi.name))
        manager.sync_all_config_properties()
        assert calls == ['pool/a', 'pool/b']


class TestAnchorExceptions:
    def test_set_anchor_exception_does_not_propagate(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        mocker.patch('zfsbackup.backup_manager.zfs.set', side_effect=Exception('set failed'))
        manager.set_anchor(dsi, 'offsite', 'autosnap_20240101120000')  # must not raise

    def test_clear_anchor_exception_does_not_propagate(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        mocker.patch('zfsbackup.backup_manager.zfs.inherit', side_effect=Exception('inherit failed'))
        manager.clear_anchor(dsi, 'offsite')  # must not raise


class TestReceivedDatasetsSuccess:
    def _make_remote_config(self):
        config = _make_config()
        config.remote_backup = RemoteServerConfig(target_dataset='pool/backups', enabled=True)
        return config

    def test_returns_dataset_info_for_valid_props(self, mocker):
        from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_CONFIG
        from zfsbackup.config import DatasetConfig
        config_encoded = DatasetConfig(name='pool/data').to_property()

        mock_ds = MagicMock()
        mock_ds.__getitem__ = MagicMock(side_effect=lambda k: {
            PROP_CLIENT_ID: 'client1',
            PROP_CONFIG: config_encoded,
        }.get(k))
        mock_ds.name = 'pool/backups/client1'

        manager = DatasetManager(self._make_remote_config())
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[mock_ds])
        result = manager.received_datasets()
        assert len(result) == 1

    def test_skips_ds_with_missing_client_id(self, mocker):
        from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_CONFIG
        from zfsbackup.config import DatasetConfig
        config_encoded = DatasetConfig(name='pool/data').to_property()

        mock_ds = MagicMock()
        mock_ds.__getitem__ = MagicMock(side_effect=lambda k: {
            PROP_CLIENT_ID: None,
            PROP_CONFIG: config_encoded,
        }.get(k))

        manager = DatasetManager(self._make_remote_config())
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[mock_ds])
        result = manager.received_datasets()
        assert result == []

    def test_skips_ds_with_invalid_config_prop(self, mocker):
        from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_CONFIG

        mock_ds = MagicMock()
        mock_ds.__getitem__ = MagicMock(side_effect=lambda k: {
            PROP_CLIENT_ID: 'client1',
            PROP_CONFIG: 'not-valid-base64!!!',
        }.get(k))
        mock_ds.name = 'pool/backups/client1'

        manager = DatasetManager(self._make_remote_config())
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[mock_ds])
        result = manager.received_datasets()
        assert result == []

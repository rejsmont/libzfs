"""Unit tests for DatasetManager."""

import logging
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

from libzfseasy.types import Dataset, Filesystem, Snapshot as ZFSSnapshot
from zfsbackup.backup_manager import (
    DatasetManager, DatasetInfo, SnapshotInfo, _collapse_retention_rules,
)
from zfsbackup.config import (
    BackupConfig, DatasetConfig, RetentionRule, RemoteServerConfig, RemoteDatasetConfig, Duration,
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

    def test_get_anchors_empty_when_no_remote(self, mock_subprocess):
        # No remote destinations and no anchor property on the dataset: nothing
        # to protect. (The properties ARE read: orphan anchors count.)
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        dsi.config.remote = []
        mock_subprocess.setup(stdout=_zget('pool/data', []))
        assert manager.get_anchors(dsi) == set()
        assert _subcommands(mock_subprocess) == ['get']

    def test_get_anchors_returns_set_of_names(self, mock_subprocess, mocker):
        config = _make_config(datasets=[
            DatasetConfig(
                name='pool/data',
                remote=[RemoteDatasetConfig(destination='offsite')],
            ),
        ])
        manager = DatasetManager(config)
        dsi = manager.datasets[0]
        mock_subprocess.setup(stdout=_zget('pool/data', [
            (PROP_ANCHOR_PREFIX + 'offsite', 'autosnap_20240101120000', 'local'),
        ]))
        mocker.patch.object(manager, 'get_anchor', return_value='autosnap_20240101120000')
        anchors = manager.get_anchors(dsi)
        assert anchors == {'autosnap_20240101120000'}


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


class TestCollapseRetentionRules:
    """Item 2b: `_collapse_retention_rules` resolves `keep_for`/`age` collisions
    toward retaining more data, instead of the old `{keep_for: age}` dict
    comprehension that silently discarded the finer-grained rule.
    """

    def test_realistic_trigger_keeps_hourly_not_daily(self):
        # "1h": "7d" and "1d": "1w" both expire at 604800s -- mixed notation,
        # not a contrived same-literal case. The old dict-comprehension
        # collapse verifiably produced {604800.0: 86400.0} (daily wins,
        # because rules are sorted by age ascending and dict-last-write-wins).
        # The fix must keep the finer, hourly interval instead.
        rules = [
            RetentionRule(Duration('1h'), Duration('7d')),
            RetentionRule(Duration('1d'), Duration('1w')),
        ]
        plan, warnings = _collapse_retention_rules(rules)
        assert plan == {604800.0: 3600.0}
        assert len(warnings) == 1

    def test_same_keep_for_different_age_keeps_min_age(self):
        rules = [
            RetentionRule(Duration('6h'), Duration('30d')),
            RetentionRule(Duration('1h'), Duration('30d')),
        ]
        plan, warnings = _collapse_retention_rules(rules)
        assert plan == {2592000.0: 3600.0}
        assert len(warnings) == 1
        assert '1h' in warnings[0]

    def test_same_age_different_keep_for_keeps_max_keep_for(self):
        rules = [
            RetentionRule(Duration('1d'), Duration('1w')),
            RetentionRule(Duration('24h'), Duration('1M')),
        ]
        plan, warnings = _collapse_retention_rules(rules)
        assert plan == {2592000.0: 86400.0}
        assert len(warnings) == 1
        assert '1M' in warnings[0]

    def test_exact_duplicates_are_silent(self):
        # Same age AND same keep_for is a true duplicate, not a conflict.
        rules = [
            RetentionRule(Duration('1d'), Duration('30d')),
            RetentionRule(Duration('1d'), Duration('30d')),
        ]
        plan, warnings = _collapse_retention_rules(rules)
        assert plan == {2592000.0: 86400.0}
        assert warnings == []

    def test_non_commuting_order_keeps_two_rules(self):
        # {(1d,1w), (1d,1M), (1h,1M)}: collapsing `age` first collapses to a
        # single rule {(1h,1M)}; collapsing `keep_for` first (the required
        # order) keeps two: {(1d,1w), (1h,1M)}. Pin the two-rule outcome so a
        # future refactor that swaps the pass order fails loudly.
        rules = [
            RetentionRule(Duration('1d'), Duration('1w')),
            RetentionRule(Duration('1d'), Duration('1M')),
            RetentionRule(Duration('1h'), Duration('1M')),
        ]
        plan, warnings = _collapse_retention_rules(rules)
        assert plan == {604800.0: 86400.0, 2592000.0: 3600.0}
        assert len(plan) == 2

    def test_non_colliding_configs_produce_naive_plan(self):
        # With no collisions, the resolved plan must be byte-identical to the
        # pre-fix, unresolved {keep_for: age} dict comprehension.
        rules = [
            RetentionRule(Duration('1h'), Duration('1d')),
            RetentionRule(Duration('1d'), Duration('1w')),
            RetentionRule(Duration('1w'), Duration('1M')),
        ]
        plan, warnings = _collapse_retention_rules(rules)
        naive = {r.keep_for.total_seconds(): r.age.total_seconds() for r in rules}
        assert plan == naive
        assert warnings == []

    def test_example_config_retention_ladders_unaffected(self):
        """The shipped config.example.yaml retention ladders have no
        collisions; the resolved plan for each dataset must match the naive
        pre-fix plan exactly (the fix must not alter normal configs)."""
        example = Path(__file__).resolve().parent.parent / 'zfsbackup' / 'config.example.yaml'
        config = BackupConfig.from_file(example)
        assert config.datasets  # sanity: the file actually has datasets
        for ds in config.datasets:
            plan, warnings = _collapse_retention_rules(ds.retention_rules)
            naive = {
                r.keep_for.total_seconds(): r.age.total_seconds() for r in ds.retention_rules
            }
            assert plan == naive, f"dataset {ds.name} plan changed"
            assert warnings == [], f"dataset {ds.name} unexpectedly collided"


class TestRetentionPlanWarnings:
    """Item 2b: the collision warning fires once per dataset, not once per
    pruning cycle -- `needs_prunning` runs every cycle (`workers.py:122`)."""

    def test_warning_fires_once_per_dataset_across_pruning_cycles(self, caplog):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        dsi.config.retention_rules = [
            RetentionRule(Duration('1h'), Duration('7d')),
            RetentionRule(Duration('1d'), Duration('1w')),
        ]
        dsi.snapshots = [_make_snap_info('autosnap', 0)]
        with caplog.at_level(logging.WARNING):
            for _ in range(3):
                manager.needs_prunning(dsi)
        warnings = [r for r in caplog.records if r.levelname == 'WARNING']
        assert len(warnings) == 1
        assert 'pool/data' in warnings[0].message

    def test_no_warning_for_non_colliding_rules(self, caplog):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        dsi.config.retention_rules = [
            RetentionRule(Duration('1h'), Duration('1d')),
        ]
        dsi.snapshots = [_make_snap_info('autosnap', 0)]
        with caplog.at_level(logging.WARNING):
            manager.needs_prunning(dsi)
        assert [r for r in caplog.records if r.levelname == 'WARNING'] == []

    def test_load_never_hard_fails_and_still_prunes(self, tmp_path):
        # A colliding config must load (never raise) and the daemon must
        # still be able to compute a pruning plan from it.
        cfg_file = tmp_path / 'colliding.yaml'
        cfg_file.write_text(
            "datasets:\n"
            "  - name: pool/data\n"
            "    retention:\n"
            "      1h: 7d\n"
            "      1d: 1w\n"
        )
        config = BackupConfig.from_file(cfg_file)  # must not raise
        manager = DatasetManager(config)
        dsi = manager.datasets[0]
        dsi.snapshots = [_make_snap_info('autosnap', 0)]
        to_prune = manager.needs_prunning(dsi)  # must not raise either
        assert isinstance(to_prune, list)


class TestPerDestinationRetentionScope:
    """Item 3b: destination-level retention overrides live independently of
    the dataset-level rule set that governs local pruning."""

    def test_local_pruning_uses_dataset_level_rules_only(self):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        dsi.config.retention_rules = [
            RetentionRule(Duration('1h'), Duration('1d')),
        ]
        dsi.config.remote = [
            RemoteDatasetConfig(
                destination='offsite',
                retention_rules=[RetentionRule(Duration('1d'), Duration('5y'))],
            ),
        ]
        plan = manager._retention_plan(dsi)
        # Only the dataset-level rule governs local pruning; the
        # destination's override never reaches it.
        assert plan == {86400.0: 3600.0}

    def test_destination_keep_for_collision_resolves_independently(self):
        # The destination's own rule set collides on keep_for; the
        # dataset-level set is collision-free. Item 2b's resolution applies
        # within each (dataset, destination) scope independently.
        #
        # Rewired through the production path (`DatasetConfig.
        # effective_retention_rules`, the method `RemoteBackupManager.
        # backup_dataset` -> `_warn_retention_collisions` actually calls) --
        # not hand-built lists fed straight to `_collapse_retention_rules` --
        # so this fails loudly if per-destination resolution is ever removed
        # (e.g. `effective_retention_rules` collapsed back to always
        # returning the dataset-level set).
        config = DatasetConfig(
            name='pool/data',
            retention_rules=[RetentionRule(Duration('1h'), Duration('1d'))],
            remote=[RemoteDatasetConfig(
                destination='offsite',
                retention_rules=[
                    RetentionRule(Duration('1h'), Duration('7d')),
                    RetentionRule(Duration('1d'), Duration('1w')),
                ],
            )],
        )

        dataset_plan, dataset_warnings = _collapse_retention_rules(
            config.effective_retention_rules()
        )
        dest_plan, dest_warnings = _collapse_retention_rules(
            config.effective_retention_rules('offsite')
        )

        assert dataset_plan == {86400.0: 3600.0}
        assert dataset_warnings == []
        assert dest_plan == {604800.0: 3600.0}
        assert len(dest_warnings) == 1


class TestContentKeyedRetentionWarning:
    """`DatasetManager._retention_warned` is keyed on `(dataset name,
    frozenset of (age, keep_for) pairs)`, not name alone. `received_datasets()`
    re-parses `PROP_CONFIG` every pruning cycle (`PruningWorker._get_datasets`
    -> `received_datasets()`), so a client pushing a *different* colliding
    ruleset under the same dataset name must still warn -- keying on name
    alone silently swallowed that for the life of the process."""

    def _dsi(self, name, rules):
        config = DatasetConfig(name=name, retention_rules=rules)
        return DatasetInfo(Filesystem(name), config)

    def test_same_name_same_ruleset_warns_once_across_calls(self, caplog):
        manager = DatasetManager(_make_config())
        rules = [
            RetentionRule(Duration('1h'), Duration('7d')),
            RetentionRule(Duration('1d'), Duration('1w')),
        ]
        with caplog.at_level(logging.WARNING):
            for _ in range(3):
                manager._retention_plan(self._dsi('pool/data', rules))
        warnings = [r for r in caplog.records if r.levelname == 'WARNING']
        assert len(warnings) == 1

    def test_same_name_different_colliding_ruleset_warns_again(self, caplog):
        manager = DatasetManager(_make_config())
        rules_a = [
            RetentionRule(Duration('1h'), Duration('7d')),
            RetentionRule(Duration('1d'), Duration('1w')),
        ]
        rules_b = [
            RetentionRule(Duration('6h'), Duration('30d')),
            RetentionRule(Duration('1h'), Duration('30d')),
        ]
        with caplog.at_level(logging.WARNING):
            manager._retention_plan(self._dsi('pool/data', rules_a))
            manager._retention_plan(self._dsi('pool/data', rules_b))
            manager._retention_plan(self._dsi('pool/data', rules_a))  # seen before -> no 3rd warning
        warnings = [r for r in caplog.records if r.levelname == 'WARNING']
        assert len(warnings) == 2

    def test_two_different_datasets_each_get_own_warning(self, caplog):
        manager = DatasetManager(_make_config())
        rules = [
            RetentionRule(Duration('1h'), Duration('7d')),
            RetentionRule(Duration('1d'), Duration('1w')),
        ]
        with caplog.at_level(logging.WARNING):
            manager._retention_plan(self._dsi('pool/a', rules))
            manager._retention_plan(self._dsi('pool/b', rules))
        warnings = [r for r in caplog.records if r.levelname == 'WARNING']
        assert len(warnings) == 2
        assert any('pool/a' in w.message for w in warnings)
        assert any('pool/b' in w.message for w in warnings)


class TestDatasetReportCollisionWarnings:
    """`DatasetManager.dataset_report()` surfaces retention-rule collisions
    at boot -- the first signal for a colliding config should be in the
    supervisor's log, not a worker's first prune/backup cycle -- for both
    the dataset-level rules and each declared destination's effective
    rules."""

    def test_dataset_level_collision_warns(self, caplog):
        config = _make_config(datasets=[DatasetConfig(
            name='pool/data',
            retention_rules=[
                RetentionRule(Duration('1h'), Duration('7d')),
                RetentionRule(Duration('1d'), Duration('1w')),
            ],
        )])
        manager = DatasetManager(config)
        with caplog.at_level(logging.WARNING):
            manager.dataset_report()
        warnings = [r for r in caplog.records if r.levelname == 'WARNING']
        assert len(warnings) == 1
        assert warnings[0].message.startswith('Dataset pool/data:')

    def test_destination_effective_collision_warns(self, caplog):
        config = _make_config(datasets=[DatasetConfig(
            name='pool/data',
            retention_rules=[RetentionRule(Duration('1h'), Duration('1d'))],  # no collision
            remote=[RemoteDatasetConfig(
                destination='offsite',
                retention_rules=[
                    RetentionRule(Duration('1h'), Duration('7d')),
                    RetentionRule(Duration('1d'), Duration('1w')),
                ],
            )],
        )])
        manager = DatasetManager(config)
        with caplog.at_level(logging.WARNING):
            manager.dataset_report()
        warnings = [r for r in caplog.records if r.levelname == 'WARNING']
        assert len(warnings) == 1
        assert warnings[0].message.startswith('Dataset pool/data -> offsite:')

    def test_no_collision_produces_no_warnings(self, caplog):
        manager = DatasetManager(_make_config())
        with caplog.at_level(logging.WARNING):
            manager.dataset_report()
        assert [r for r in caplog.records if r.levelname == 'WARNING'] == []

    def test_dataset_and_destination_collisions_both_warn_independently(self, caplog):
        config = _make_config(datasets=[DatasetConfig(
            name='pool/data',
            retention_rules=[
                RetentionRule(Duration('1h'), Duration('7d')),
                RetentionRule(Duration('1d'), Duration('1w')),
            ],
            remote=[RemoteDatasetConfig(
                destination='offsite',
                retention_rules=[
                    RetentionRule(Duration('6h'), Duration('30d')),
                    RetentionRule(Duration('1h'), Duration('30d')),
                ],
            )],
        )])
        manager = DatasetManager(config)
        with caplog.at_level(logging.WARNING):
            manager.dataset_report()
        warnings = [r for r in caplog.records if r.levelname == 'WARNING']
        assert len(warnings) == 2
        assert any(w.message.startswith('Dataset pool/data:') for w in warnings)
        assert any(w.message.startswith('Dataset pool/data -> offsite:') for w in warnings)


# ---------------------------------------------------------------------------
# Item 14.1: module-level property helpers (no BackupConfig, no DatasetManager)
#
# Driven through the real `libzfseasy` command classes with `Popen` mocked, so
# the stdout fed in is exactly the `zfs get -H -o name,property,value,received,
# source` shape the parser reads, and the command lines actually issued can be
# asserted (e.g. "no `zfs set` under dry-run").
# ---------------------------------------------------------------------------

from zfsbackup.backup_manager import (  # noqa: E402
    PROP_ANCHOR_PREFIX, PROP_CONFIG, PROP_PREFIX,
    clear_zfsbackup_properties, copy_anchor_property, read_zfsbackup_properties,
)


def _zget(name, props, ptype='filesystem'):
    """`zfs get -H -o name,property,value,received,source all <name>` lines.

    `props` is a list of `(property, value, source)`; a `received` source also
    fills the `received` column, like the real command.
    """
    lines = [f'{name}\tname\t{name}\t-\t-\n', f'{name}\ttype\t{ptype}\t-\t-\n']
    for prop, value, source in props:
        received = value if source == 'received' else '-'
        lines.append(f'{name}\t{prop}\t{value}\t{received}\t{source}\n')
    return lines


def _fail_get(mock_subprocess, stderr):
    """A `zfs get` that exits 1. `poll()` must keep answering after the first
    call because `Command._exec_out` polls again in its `finally` when the
    generator is abandoned by the exception; the stock fixture's one-shot
    `side_effect` would raise `StopIteration` there and mask the real error."""
    proc = mock_subprocess.setup(stdout=[], stderr=stderr, returncode=1)
    proc.poll.side_effect = None
    proc.poll.return_value = 1
    return proc


def _cmds(mock_subprocess):
    return [c.args[0] for c in mock_subprocess.call_args_list]


def _subcommands(mock_subprocess):
    return [cmd[1] for cmd in _cmds(mock_subprocess)]


A = PROP_ANCHOR_PREFIX


class TestReadZfsbackupProperties:
    def test_returns_only_org_zfsbackup_properties(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/data', [
            ('compression', 'lz4', 'local'),
            (PROP_CONFIG, '{"x":1}', 'local'),
            ('org.other:thing', 'nope', 'local'),
            (A + 'offsite', 'autosnap_1', 'local'),
            ('mountpoint', '/mnt', 'local'),
        ]))
        result = read_zfsbackup_properties(Filesystem('pool/data'))
        assert result == {PROP_CONFIG: '{"x":1}', A + 'offsite': 'autosnap_1'}

    def test_local_and_received_sources_are_kept(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/data', [
            (PROP_CONFIG, 'c', 'local'),
            (A + 'dest', 'snap', 'received'),
        ]))
        assert read_zfsbackup_properties(Filesystem('pool/data')) == {
            PROP_CONFIG: 'c', A + 'dest': 'snap',
        }

    @pytest.mark.parametrize('source', [
        'inherited from pool', 'default', 'temporary', '-',
    ])
    def test_other_sources_are_ignored(self, mock_subprocess, source):
        """An inherited value belongs to an ancestor; acting on it would be
        acting on the wrong dataset."""
        mock_subprocess.setup(stdout=_zget('pool/data', [
            (PROP_CONFIG, 'ancestors', source),
            (A + 'dest', 'ancestors', source),
        ]))
        assert read_zfsbackup_properties(Filesystem('pool/data')) == {}

    def test_a_local_property_is_kept_beside_an_inherited_one(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/data', [
            (PROP_CONFIG, 'inherited-config', 'inherited from pool'),
            (A + 'dest', 'mine', 'local'),
        ]))
        assert read_zfsbackup_properties(Filesystem('pool/data')) == {A + 'dest': 'mine'}

    def test_values_are_returned_as_strings_verbatim(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/data', [
            (A + 'a', 'autosnap_20240101120000', 'local'),
            (A + 'b', 'has spaces: and colons', 'local'),
            (A + 'c', '12345', 'local'),
        ]))
        result = read_zfsbackup_properties(Filesystem('pool/data'))
        assert result[A + 'a'] == 'autosnap_20240101120000'
        assert result[A + 'b'] == 'has spaces: and colons'
        assert result[A + 'c'] == '12345' and isinstance(result[A + 'c'], str)

    def test_no_properties_gives_an_empty_dict(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/data', []))
        assert read_zfsbackup_properties(Filesystem('pool/data')) == {}

    def test_empty_output_gives_an_empty_dict(self, mock_subprocess):
        mock_subprocess.setup(stdout=[])
        assert read_zfsbackup_properties(Filesystem('pool/data')) == {}

    def test_only_the_named_dataset_is_read(self, mock_subprocess):
        """A line group for any other dataset (a child, say) is not ours."""
        mock_subprocess.setup(stdout=(
            _zget('pool/data', [(PROP_CONFIG, 'parent', 'local')])
            + _zget('pool/data/child', [(PROP_CONFIG, 'child', 'local'),
                                         (A + 'x', 'y', 'local')])
        ))
        assert read_zfsbackup_properties(Filesystem('pool/data')) == {PROP_CONFIG: 'parent'}

    def test_dotted_and_hyphenated_destination_names(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/data', [
            (A + 'off-site.eu', 'snap', 'local'),
        ]))
        assert read_zfsbackup_properties(Filesystem('pool/data')) == {A + 'off-site.eu': 'snap'}

    def test_issues_a_single_zfs_get_for_the_dataset_and_nothing_else(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/data', []))
        read_zfsbackup_properties(Filesystem('pool/data'))
        (cmd,) = _cmds(mock_subprocess)
        assert cmd[1] == 'get'
        assert cmd[-1] == 'pool/data'
        assert 'all' in cmd
        assert '-r' not in cmd, 'a recursive get would read the whole subtree'

    def test_works_on_a_plain_dataset_object(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/data', [(PROP_CONFIG, 'c', 'local')]))
        assert read_zfsbackup_properties(Dataset('pool/data')) == {PROP_CONFIG: 'c'}

    def test_zfs_errors_propagate(self, mock_subprocess):
        _fail_get(mock_subprocess, "cannot open 'pool/data': dataset does not exist")
        with pytest.raises(Exception, match='dataset does not exist'):
            read_zfsbackup_properties(Filesystem('pool/data'))

    def test_prefix_constant(self):
        assert PROP_PREFIX == 'org.zfsbackup:'
        assert PROP_CONFIG.startswith(PROP_PREFIX) and PROP_ANCHOR_PREFIX.startswith(PROP_PREFIX)


class TestReadZfsbackupPropertiesRawParse:
    """Mutation counts (15.5, scratch copy, tests failing in this file's
    `ReadZfsbackupProperties*` classes): the filter moved after a `zfs.get`
    object build 11 (3 of them the unknown-builtin ones); the tab-preserving
    join reduced to `parts[2]` 2; the source filter dropped 10; the name filter
    dropped 2; `-s local,received` dropped from the command 2.

    Item 15.5: `read_zfsbackup_properties` reads the raw `zfs get` lines and
    filters BEFORE anything becomes a libzfseasy object. `zfs.get(...)` builds
    `ZFS` objects for every property of the dataset. libzfseasy used to raise
    `ValueError` there for a local/received built-in it does not know (zvol
    `volthreading`, FreeBSD `jailed`); it now keeps such names as extras, so the
    raw parse is about doing only the work needed (our names, exact values,
    local/received sources), not about avoiding that failure."""

    #: Real-shaped `-o name,property,value,received,source` output with
    #: built-in properties libzfseasy has no class for.
    UNKNOWN_BUILTINS = [
        ('volthreading', 'off', 'local'),
        ('jailed', 'on', 'local'),
        (PROP_CONFIG, 'cfg', 'local'),
    ]

    def test_unknown_builtin_local_properties_do_not_break_the_read(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/zv', self.UNKNOWN_BUILTINS, ptype='volume'))
        assert read_zfsbackup_properties(Dataset('pool/zv')) == {PROP_CONFIG: 'cfg'}

    @pytest.mark.parametrize('prop, value', [('volthreading', 'off'), ('jailed', 'on')])
    def test_each_unknown_builtin_alone_is_ignored_not_raised(self, mock_subprocess, prop, value):
        mock_subprocess.setup(stdout=_zget('pool/zv', [(prop, value, 'local')], ptype='volume'))
        assert read_zfsbackup_properties(Dataset('pool/zv')) == {}

    def test_the_object_path_now_accepts_the_same_output_as_extras(self, mock_subprocess):
        """Control: building ZFS objects from the identical output no longer raises (libzfseasy keeps well-formed
        native names it does not know in an extras store), so the raw parse is not needed to avoid a failure. It is
        kept for what the object path would still not give: only our user properties, exact tab-containing values,
        and local/received sources only."""
        import libzfseasy as zfs

        mock_subprocess.setup(stdout=_zget('pool/zv', [
            ('futurething', 'off', 'local'),      # an invented native name no libzfseasy class knows
            ('jailed', 'on', 'local'),
            (PROP_CONFIG, 'cfg', 'local'),
        ], ptype='volume'))
        (obj,) = list(zfs.get(Dataset('pool/zv'), properties=['all'], sources=['local', 'received']))
        assert obj['futurething'].value == 'off'
        assert obj['jailed'].value == 'on'
        assert sorted(obj._extra_props) == ['futurething', 'jailed']
        assert obj[PROP_CONFIG].value == 'cfg'

    def test_the_raw_parse_still_returns_only_our_names_with_exact_values(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/zv', self.UNKNOWN_BUILTINS + [
            (A + 'tabbed', 'left\tright', 'local'),
            (A + 'recv', 'x\ty', 'received'),
            (A + 'inh', 'nope', 'inherited from pool'),
        ], ptype='volume'))
        assert read_zfsbackup_properties(Dataset('pool/zv')) == {
            PROP_CONFIG: 'cfg', A + 'tabbed': 'left\tright', A + 'recv': 'x\ty',
        }

    def test_the_exact_command_line(self, mock_subprocess, monkeypatch):
        monkeypatch.setenv('ZFS_CMD', '/usr/bin/true')
        mock_subprocess.setup(stdout=_zget('pool/data', []))
        read_zfsbackup_properties(Filesystem('pool/data'))
        (cmd,) = _cmds(mock_subprocess)
        assert cmd == [
            '/usr/bin/true', 'get', '-H', '-o', 'name,property,value,received,source',
            '-s', 'local,received', 'all', 'pool/data',
        ]

    def test_the_binary_is_resolved_at_call_time(self, mock_subprocess, monkeypatch):
        mock_subprocess.setup(stdout=_zget('pool/data', []))
        monkeypatch.setenv('ZFS_CMD', '/opt/zfs/bin/zfs')
        read_zfsbackup_properties(Filesystem('pool/data'))
        assert _cmds(mock_subprocess)[0][0] == '/opt/zfs/bin/zfs'

    def test_an_inherited_zfsbackup_property_is_excluded(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/data/child', [
            (PROP_CONFIG, 'from-parent', 'inherited from pool/data'),
            (A + 'dest', 'mine', 'local'),
        ]))
        assert read_zfsbackup_properties(Filesystem('pool/data/child')) == {A + 'dest': 'mine'}

    def test_a_tab_inside_a_value_survives(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/data', [
            (A + 'tabbed', 'left\tright', 'local'),
            (A + 'two', 'a\tb\tc', 'local'),
        ]))
        assert read_zfsbackup_properties(Filesystem('pool/data')) == {
            A + 'tabbed': 'left\tright', A + 'two': 'a\tb\tc',
        }

    def test_a_received_value_without_a_tab_is_not_confused_with_its_received_column(
        self, mock_subprocess
    ):
        mock_subprocess.setup(stdout=_zget('pool/data', [(A + 'r', 'xy', 'received')]))
        assert read_zfsbackup_properties(Filesystem('pool/data')) == {A + 'r': 'xy'}

    def test_a_tabbed_received_value_survives(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/data', [(A + 'r', 'x\ty', 'received')]))
        assert read_zfsbackup_properties(Filesystem('pool/data')) == {A + 'r': 'x\ty'}

    def test_an_empty_value_is_an_empty_string(self, mock_subprocess):
        mock_subprocess.setup(stdout=['pool/data\t' + A + 'e\t\t-\tlocal\n'])
        assert read_zfsbackup_properties(Filesystem('pool/data')) == {A + 'e': ''}

    def test_short_lines_are_skipped(self, mock_subprocess):
        mock_subprocess.setup(stdout=[
            'pool/data\t' + A + 'bad\tvalue\n',
            'garbage\n',
            *_zget('pool/data', [(A + 'ok', 'v', 'local')]),
        ])
        assert read_zfsbackup_properties(Filesystem('pool/data')) == {A + 'ok': 'v'}

    def test_a_foreign_dataset_name_is_excluded_even_with_a_matching_property(
        self, mock_subprocess
    ):
        mock_subprocess.setup(stdout=_zget('pool/data2', [(A + 'x', 'y', 'local')]))
        assert read_zfsbackup_properties(Filesystem('pool/data')) == {}

    def test_a_property_that_merely_contains_the_prefix_is_excluded(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/data', [
            ('x' + PROP_CONFIG, 'v', 'local'),
            ('org.zfsbackupX:cfg', 'v', 'local'),
        ]))
        assert read_zfsbackup_properties(Filesystem('pool/data')) == {}

    def test_no_libzfseasy_object_is_built(self, mock_subprocess, mocker):
        """The filter runs on raw text: `ZFS.update` is never reached."""
        mock_subprocess.setup(stdout=_zget('pool/zv', self.UNKNOWN_BUILTINS, ptype='volume'))
        update = mocker.patch('libzfseasy.types.ZFS.update')
        read_zfsbackup_properties(Dataset('pool/zv'))
        update.assert_not_called()

    def test_through_a_real_process_with_a_fake_zfs_command(self, tmp_path, monkeypatch):
        """No `Popen` mock: a stand-in `zfs` writes real tab-separated output to
        a real pipe, so the tab in the value and the argument vector are
        proven end to end."""
        argv_log = tmp_path / 'argv'
        fake = tmp_path / 'zfs'
        fake.write_text(
            '#!/bin/sh\n'
            f'printf \'%s\\n\' "$@" > {argv_log}\n'
            "printf 'pool/data\\tvolthreading\\toff\\t-\\tlocal\\n'\n"
            "printf 'pool/data\\tjailed\\ton\\t-\\tlocal\\n'\n"
            "printf 'pool/data\\torg.zfsbackup:config\\ta\\tb\\t-\\tlocal\\n'\n"
            "printf 'pool/data\\torg.zfsbackup:anchor.d\\tsnap\\t-\\tinherited from pool\\n'\n"
            "printf 'pool/data\\torg.zfsbackup:anchor.r\\trv\\trv\\treceived\\n'\n"
        )
        fake.chmod(0o755)
        monkeypatch.setenv('ZFS_CMD', str(fake))
        assert read_zfsbackup_properties(Filesystem('pool/data')) == {
            PROP_CONFIG: 'a\tb',
            A + 'r': 'rv',
        }
        assert argv_log.read_text().split('\n')[:-1] == [
            'get', '-H', '-o', 'name,property,value,received,source',
            '-s', 'local,received', 'all', 'pool/data',
        ]


class TestCopyAnchorProperty:
    DS = staticmethod(lambda: Filesystem('pool/data'))

    def test_copy_anchor_property_copies_the_value_and_reports_copied(self, mock_subprocess):
        mock_subprocess.setup_multi(
            (_zget('pool/data', [(A + 'old', 'autosnap_20240101120000', 'local')]),),
            ('',),
        )
        assert copy_anchor_property(self.DS(), 'old', 'new') == 'copied'
        get_cmd, set_cmd = _cmds(mock_subprocess)
        assert get_cmd[1] == 'get'
        assert set_cmd[1:] == ['set', f'{A}new=autosnap_20240101120000', 'pool/data']

    def test_copy_anchor_property_never_deletes_the_old_property(self, mock_subprocess):
        mock_subprocess.setup_multi(
            (_zget('pool/data', [(A + 'old', 'snap', 'local')]),),
            ('',),
        )
        copy_anchor_property(self.DS(), 'old', 'new')
        assert _subcommands(mock_subprocess) == ['get', 'set']
        for cmd in _cmds(mock_subprocess):
            assert 'inherit' not in cmd and 'destroy' not in cmd
            assert f'{A}old' not in cmd[2:], 'the old property is not touched'

    def test_copy_anchor_property_sets_only_the_new_property(self, mock_subprocess):
        mock_subprocess.setup_multi(
            (_zget('pool/data', [(A + 'old', 'snap', 'local'), (A + 'other', 'x', 'local')]),),
            ('',),
        )
        copy_anchor_property(self.DS(), 'old', 'new')
        set_cmd = _cmds(mock_subprocess)[1]
        assert [a for a in set_cmd if '=' in a] == [f'{A}new=snap']

    def test_copy_anchor_property_no_source(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/data', [(PROP_CONFIG, 'c', 'local')]))
        assert copy_anchor_property(self.DS(), 'old', 'new') == 'no-source'
        assert _subcommands(mock_subprocess) == ['get']

    def test_copy_anchor_property_no_source_even_if_the_destination_exists(self, mock_subprocess):
        """Nothing to copy takes precedence over "target already there"."""
        mock_subprocess.setup(stdout=_zget('pool/data', [(A + 'new', 'x', 'local')]))
        assert copy_anchor_property(self.DS(), 'old', 'new') == 'no-source'
        assert _subcommands(mock_subprocess) == ['get']

    def test_copy_anchor_property_exists_is_left_alone(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/data', [
            (A + 'old', 'from-old', 'local'),
            (A + 'new', 'already-there', 'local'),
        ]))
        assert copy_anchor_property(self.DS(), 'old', 'new') == 'exists'
        assert _subcommands(mock_subprocess) == ['get'], 'must not overwrite'

    def test_copy_anchor_property_an_inherited_old_value_is_not_a_source(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/data', [
            (A + 'old', 'ancestors', 'inherited from pool'),
        ]))
        assert copy_anchor_property(self.DS(), 'old', 'new') == 'no-source'

    def test_copy_anchor_property_an_inherited_new_value_does_not_block_the_copy(
        self, mock_subprocess
    ):
        mock_subprocess.setup_multi(
            (_zget('pool/data', [
                (A + 'old', 'mine', 'local'),
                (A + 'new', 'ancestors', 'inherited from pool'),
            ]),),
            ('',),
        )
        assert copy_anchor_property(self.DS(), 'old', 'new') == 'copied'

    def test_copy_anchor_property_a_received_old_value_is_a_source(self, mock_subprocess):
        mock_subprocess.setup_multi(
            (_zget('pool/data', [(A + 'old', 'snap', 'received')]),),
            ('',),
        )
        assert copy_anchor_property(self.DS(), 'old', 'new') == 'copied'

    def test_copy_anchor_property_dry_run_issues_no_zfs_set(self, mock_subprocess, caplog):
        mock_subprocess.setup(stdout=_zget('pool/data', [(A + 'old', 'snap', 'local')]))
        with caplog.at_level(logging.INFO):
            result = copy_anchor_property(self.DS(), 'old', 'new', dry_run=True)
        assert result == 'dry-run'
        assert _subcommands(mock_subprocess) == ['get']
        assert 'DRY RUN' in caplog.text and f'{A}new=snap' in caplog.text

    def test_copy_anchor_property_dry_run_does_not_change_the_other_outcomes(
        self, mock_subprocess
    ):
        mock_subprocess.setup(stdout=_zget('pool/data', []))
        assert copy_anchor_property(self.DS(), 'old', 'new', dry_run=True) == 'no-source'
        mock_subprocess.setup(stdout=_zget('pool/data', [
            (A + 'old', 'a', 'local'), (A + 'new', 'b', 'local'),
        ]))
        assert copy_anchor_property(self.DS(), 'old', 'new', dry_run=True) == 'exists'
        assert 'set' not in _subcommands(mock_subprocess)

    def test_copy_anchor_property_dry_run_is_keyword_only(self):
        with pytest.raises(TypeError):
            copy_anchor_property(self.DS(), 'old', 'new', True)

    def test_copy_anchor_property_dotted_destination_names(self, mock_subprocess):
        mock_subprocess.setup_multi(
            (_zget('pool/data', [(A + 'off.site-1', 'snap', 'local')]),),
            ('',),
        )
        assert copy_anchor_property(self.DS(), 'off.site-1', 'off.site-2') == 'copied'
        assert _cmds(mock_subprocess)[1][2] == f'{A}off.site-2=snap'

    def test_copy_anchor_property_same_name_is_exists(self, mock_subprocess):
        mock_subprocess.setup(stdout=_zget('pool/data', [(A + 'same', 'snap', 'local')]))
        assert copy_anchor_property(self.DS(), 'same', 'same') == 'exists'
        assert _subcommands(mock_subprocess) == ['get']

    def test_copy_anchor_property_a_set_failure_propagates(self, mock_subprocess):
        mock_subprocess.setup_multi(
            (_zget('pool/data', [(A + 'old', 'snap', 'local')]),),
            ([], 'cannot set property: permission denied', 1),
        )
        with pytest.raises(Exception, match='permission denied'):
            copy_anchor_property(self.DS(), 'old', 'new')

    def test_copy_anchor_property_a_get_failure_propagates_and_nothing_is_set(
        self, mock_subprocess
    ):
        _fail_get(mock_subprocess, 'dataset does not exist')
        with pytest.raises(Exception, match='does not exist'):
            copy_anchor_property(self.DS(), 'old', 'new')
        assert _subcommands(mock_subprocess) == ['get']

    def test_copy_anchor_property_is_idempotent(self, mock_subprocess):
        """Second run sees the destination and does nothing."""
        mock_subprocess.setup_multi(
            (_zget('pool/data', [(A + 'old', 'snap', 'local')]),),
            ('',),
            (_zget('pool/data', [(A + 'old', 'snap', 'local'), (A + 'new', 'snap', 'local')]),),
        )
        assert copy_anchor_property(self.DS(), 'old', 'new') == 'copied'
        assert copy_anchor_property(self.DS(), 'old', 'new') == 'exists'
        assert _subcommands(mock_subprocess) == ['get', 'set', 'get']


class TestClearZfsbackupProperties:
    DS = staticmethod(lambda: Filesystem('pool/data'))

    FULL = [
        (PROP_CONFIG, 'cfg', 'local'),
        (PROP_PREFIX + 'client_id', 'cid', 'local'),
        (A + 'zeta', 's1', 'local'),
        (A + 'alpha', 's2', 'local'),
        (A + 'mid', 's3', 'received'),
        ('org.other:anchor.alpha', 'foreign', 'local'),
        ('compression', 'lz4', 'local'),
    ]

    def _arrange(self, mock_subprocess, props=None):
        n_inherits = 8
        mock_subprocess.setup_multi(
            (_zget('pool/data', self.FULL if props is None else props),),
            *[('',)] * n_inherits,
        )

    def _inherited(self, mock_subprocess):
        return [cmd[2] for cmd in _cmds(mock_subprocess) if cmd[1] == 'inherit']

    def test_config_true_clears_the_config_property(self, mock_subprocess):
        self._arrange(mock_subprocess)
        result = clear_zfsbackup_properties(self.DS(), config=True, anchors=False)
        assert result == [PROP_CONFIG]
        assert self._inherited(mock_subprocess) == [PROP_CONFIG]

    def test_inherit_command_shape(self, mock_subprocess):
        self._arrange(mock_subprocess)
        clear_zfsbackup_properties(self.DS(), config=True, anchors=False)
        inherit = _cmds(mock_subprocess)[1]
        assert inherit[1:] == ['inherit', PROP_CONFIG, 'pool/data']
        assert '-r' not in inherit

    def test_config_false_leaves_the_config_property(self, mock_subprocess):
        self._arrange(mock_subprocess)
        result = clear_zfsbackup_properties(self.DS(), config=False, anchors=False)
        assert result == []
        assert self._inherited(mock_subprocess) == []

    def test_config_true_but_not_set_is_a_no_op(self, mock_subprocess):
        self._arrange(mock_subprocess, [(A + 'x', 'y', 'local')])
        assert clear_zfsbackup_properties(self.DS(), config=True, anchors=False) == []
        assert _subcommands(mock_subprocess) == ['get']

    def test_anchors_true_clears_every_anchor_sorted_and_only_anchors(self, mock_subprocess):
        self._arrange(mock_subprocess)
        result = clear_zfsbackup_properties(self.DS(), config=False, anchors=True)
        assert result == [A + 'alpha', A + 'mid', A + 'zeta']
        assert self._inherited(mock_subprocess) == result
        # not the config, not client_id, not another namespace's "anchor."
        assert PROP_CONFIG not in result
        assert PROP_PREFIX + 'client_id' not in result
        assert 'org.other:anchor.alpha' not in result

    def test_anchors_list_clears_only_the_named_ones_that_are_set(self, mock_subprocess):
        self._arrange(mock_subprocess)
        result = clear_zfsbackup_properties(
            self.DS(), config=False, anchors=['zeta', 'absent', 'alpha'])
        assert result == [A + 'zeta', A + 'alpha'], 'request order, unset names skipped'
        assert self._inherited(mock_subprocess) == result

    def test_anchors_list_deduplicates(self, mock_subprocess):
        self._arrange(mock_subprocess)
        result = clear_zfsbackup_properties(
            self.DS(), config=False, anchors=['alpha', 'alpha', 'alpha'])
        assert result == [A + 'alpha']
        assert self._inherited(mock_subprocess) == [A + 'alpha']

    @pytest.mark.parametrize('empty', [False, None, [], (), set(), 0, ''])
    def test_falsy_anchors_clear_nothing(self, mock_subprocess, empty):
        self._arrange(mock_subprocess)
        assert clear_zfsbackup_properties(self.DS(), config=False, anchors=empty) == []
        assert _subcommands(mock_subprocess) == ['get']

    @pytest.mark.parametrize('bare', ['alpha', 'a', 'zeta', b'alpha', 'True', 'false'])
    def test_a_non_empty_bare_string_for_anchors_is_a_type_error(self, mock_subprocess, bare):
        """Iterating `'alpha'` would look up destinations `a`, `l`, `p`, `h`,
        `a` and silently clear nothing. Refused before ANY zfs call, so a
        caller that passes a name instead of `[name]` learns immediately.

        Mutation-checked (scratch copy): deleting the `isinstance(anchors,
        (str, bytes))` guard fails 7 tests: the 6 parametrizations here and
        `test_a_bare_string_is_refused_even_with_config_true_and_dry_run`."""
        self._arrange(mock_subprocess)
        with pytest.raises(TypeError, match='bare string'):
            clear_zfsbackup_properties(self.DS(), config=False, anchors=bare)
        assert _subcommands(mock_subprocess) == [], 'nothing, not even the read, may run'

    def test_a_bare_string_is_refused_even_with_config_true_and_dry_run(self, mock_subprocess):
        """No partial work: `config=True` must not clear the config property
        before the bad `anchors` argument is noticed."""
        self._arrange(mock_subprocess)
        for dry_run in (False, True):
            with pytest.raises(TypeError):
                clear_zfsbackup_properties(
                    self.DS(), config=True, anchors='alpha', dry_run=dry_run)
        assert _subcommands(mock_subprocess) == []

    def test_the_empty_string_stays_falsy_and_a_one_name_list_still_works(self, mock_subprocess):
        """The boundary of the new guard: `''` (falsy, like `False`) is not
        an error, and `['alpha']` is the supported spelling for one name."""
        self._arrange(mock_subprocess)
        assert clear_zfsbackup_properties(self.DS(), config=False, anchors='') == []
        self._arrange(mock_subprocess)
        assert clear_zfsbackup_properties(self.DS(), config=False, anchors=['alpha']) == [A + 'alpha']

    @pytest.mark.parametrize('make', [tuple, set, frozenset, iter])
    def test_anchors_accepts_any_iterable(self, mock_subprocess, make):
        self._arrange(mock_subprocess)
        result = clear_zfsbackup_properties(
            self.DS(), config=False, anchors=make(['alpha']))
        assert result == [A + 'alpha']

    def test_config_and_anchors_together_config_first(self, mock_subprocess):
        self._arrange(mock_subprocess)
        result = clear_zfsbackup_properties(self.DS(), config=True, anchors=True)
        assert result == [PROP_CONFIG, A + 'alpha', A + 'mid', A + 'zeta']
        assert self._inherited(mock_subprocess) == result

    def test_config_and_listed_anchors(self, mock_subprocess):
        self._arrange(mock_subprocess)
        result = clear_zfsbackup_properties(self.DS(), config=True, anchors=['mid'])
        assert result == [PROP_CONFIG, A + 'mid']

    def test_only_properties_actually_set_are_inherited(self, mock_subprocess):
        self._arrange(mock_subprocess, [(A + 'only', 'x', 'local')])
        result = clear_zfsbackup_properties(self.DS(), config=True, anchors=['only', 'ghost'])
        assert result == [A + 'only']
        assert self._inherited(mock_subprocess) == [A + 'only']

    def test_inherited_values_are_not_cleared(self, mock_subprocess):
        """`zfs inherit` on a dataset whose value is already inherited is a
        no-op at best; it must not even be attempted (idempotent clear)."""
        self._arrange(mock_subprocess, [
            (PROP_CONFIG, 'parents', 'inherited from pool'),
            (A + 'x', 'parents', 'inherited from pool'),
        ])
        assert clear_zfsbackup_properties(self.DS(), config=True, anchors=True) == []
        assert _subcommands(mock_subprocess) == ['get']

    def test_dry_run_returns_the_would_be_names_and_issues_no_inherit(
        self, mock_subprocess, caplog
    ):
        self._arrange(mock_subprocess)
        with caplog.at_level(logging.INFO):
            result = clear_zfsbackup_properties(
                self.DS(), config=True, anchors=True, dry_run=True)
        assert result == [PROP_CONFIG, A + 'alpha', A + 'mid', A + 'zeta']
        assert _subcommands(mock_subprocess) == ['get']
        assert caplog.text.count('DRY RUN') == 4

    def test_dry_run_with_nothing_set_returns_empty(self, mock_subprocess):
        self._arrange(mock_subprocess, [])
        assert clear_zfsbackup_properties(
            self.DS(), config=True, anchors=True, dry_run=True) == []

    def test_options_are_keyword_only(self):
        with pytest.raises(TypeError):
            clear_zfsbackup_properties(self.DS(), True, True)

    def test_returns_a_list_of_names(self, mock_subprocess):
        self._arrange(mock_subprocess)
        result = clear_zfsbackup_properties(self.DS(), config=True, anchors=['alpha'])
        assert isinstance(result, list)
        assert all(isinstance(n, str) for n in result)

    def test_an_inherit_failure_propagates_and_stops_at_the_failure(self, mock_subprocess):
        mock_subprocess.setup_multi(
            (_zget('pool/data', self.FULL),),
            ('',),                                                    # config ok
            ([], "cannot inherit: permission denied", 1),             # first anchor fails
            ('',),
        )
        with pytest.raises(Exception, match='permission denied'):
            clear_zfsbackup_properties(self.DS(), config=True, anchors=True)
        assert _subcommands(mock_subprocess) == ['get', 'inherit', 'inherit'], \
            'must not carry on past the failure'

    def test_a_get_failure_propagates_and_nothing_is_inherited(self, mock_subprocess):
        _fail_get(mock_subprocess, 'dataset does not exist')
        with pytest.raises(Exception, match='does not exist'):
            clear_zfsbackup_properties(self.DS(), config=True, anchors=True)
        assert _subcommands(mock_subprocess) == ['get']


# ---------------------------------------------------------------------------
# Anchor decision ("overwrite + protect all"): the pruner protects EVERY
# `org.zfsbackup:anchor.*` value on a dataset, configured destination or not;
# `copy_anchor_property(..., overwrite=True)` is what `rename destination` uses.
# ---------------------------------------------------------------------------


def _anchor_manager(*destinations):
    remote = [RemoteDatasetConfig(destination=d) for d in destinations]
    config = _make_config(datasets=[DatasetConfig(name='pool/data', remote=remote)])
    manager = DatasetManager(config)
    return manager, manager.datasets[0]


class TestGetAnchors:
    def test_every_local_anchor_counts_configured_or_not(self, mock_subprocess, mocker):
        manager, dsi = _anchor_manager('offsite')
        mock_subprocess.setup(stdout=_zget('pool/data', [
            (A + 'offsite', 'autosnap_1', 'local'),
            (A + 'renamed-away', 'autosnap_2', 'local'),
        ]))
        mocker.patch.object(manager, '_get_prop', return_value='autosnap_1')
        assert manager.get_anchors(dsi) == {'autosnap_1', 'autosnap_2'}

    def test_a_received_anchor_counts(self, mock_subprocess):
        # on a receiving server it is the client's incremental base
        manager, dsi = _anchor_manager()
        mock_subprocess.setup(stdout=_zget('pool/data', [(A + 'client', 'autosnap_3', 'received')]))
        assert manager.get_anchors(dsi) == {'autosnap_3'}

    def test_an_inherited_value_is_not_in_the_raw_read(self, mock_subprocess, mocker):
        manager, dsi = _anchor_manager('offsite')
        mock_subprocess.setup(stdout=_zget('pool/data', [
            (A + 'ancestors', 'autosnap_9', 'inherited from pool'),
        ]))
        mocker.patch.object(manager, '_get_prop', return_value=None)
        assert manager.get_anchors(dsi) == set()

    def test_the_configured_get_anchor_value_is_unioned_in(self, mock_subprocess, mocker):
        # `get_anchor` also sees inherited values: the result stays a superset
        # of what it was before the raw read existed.
        manager, dsi = _anchor_manager('offsite', 'dc2')
        mock_subprocess.setup(stdout=_zget('pool/data', [(A + 'offsite', 'autosnap_1', 'local')]))
        values = {'offsite': 'autosnap_1', 'dc2': 'autosnap_inherited'}
        mocker.patch.object(manager, 'get_anchor', side_effect=lambda dsi_, dest: values[dest])
        assert manager.get_anchors(dsi) == {'autosnap_1', 'autosnap_inherited'}

    @pytest.mark.parametrize('value', ['-', ''])
    def test_empty_and_dash_values_are_skipped(self, mock_subprocess, value):
        manager, dsi = _anchor_manager()
        mock_subprocess.setup(stdout=_zget('pool/data', [
            (A + 'x', value, 'local'), (A + 'y', 'autosnap_4', 'local'),
        ]))
        assert manager.get_anchors(dsi) == {'autosnap_4'}

    def test_other_zfsbackup_properties_and_builtins_are_ignored(self, mock_subprocess):
        manager, dsi = _anchor_manager()
        lines = _zget('pool/data', [
            (PROP_CONFIG, 'cfg', 'local'),
            ('volthreading', 'on', 'local'),  # a built-in libzfseasy would choke on
            (A + 'ok', 'autosnap_5', 'local'),
        ])
        mock_subprocess.setup(stdout=lines)
        assert manager.get_anchors(dsi) == {'autosnap_5'}

    def test_a_read_failure_raises(self, mock_subprocess):
        manager, dsi = _anchor_manager('offsite')
        _fail_get(mock_subprocess, 'dataset does not exist')
        with pytest.raises(Exception, match='does not exist'):
            manager.get_anchors(dsi)


class TestOrphanAnchorLogging:
    def orphans(self, caplog, level):
        return [r for r in caplog.records if r.levelno == level and 'configured destination' in r.getMessage()]

    def run_twice(self, manager, dsi, mock_subprocess, props, caplog, times=2):
        for _ in range(times):
            mock_subprocess.setup(stdout=_zget('pool/data', props))
            manager.get_anchors(dsi)

    def test_debug_every_call_and_info_once(self, mock_subprocess, caplog):
        manager, dsi = _anchor_manager()
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            self.run_twice(manager, dsi, mock_subprocess, [(A + 'gone', 'autosnap_1', 'local')], caplog, times=3)
        assert len(self.orphans(caplog, logging.DEBUG)) == 3
        info = [r for r in caplog.records if r.levelno == logging.INFO]
        assert len(info) == 1
        assert f'zfs inherit {A}gone pool/data' in info[0].getMessage()
        assert 'autosnap_1' in info[0].getMessage()

    def test_info_once_per_dataset_and_property(self, mock_subprocess, caplog):
        manager, dsi = _anchor_manager()
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            self.run_twice(manager, dsi, mock_subprocess, [(A + 'one', 's1', 'local')], caplog, times=2)
            self.run_twice(manager, dsi, mock_subprocess,
                           [(A + 'one', 's1', 'local'), (A + 'two', 's2', 'local')], caplog, times=1)
        infos = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
        assert sum(f'{A}one is not a configured' in m for m in infos) == 1
        assert sum(f'{A}two is not a configured' in m for m in infos) == 1

    def test_a_new_manager_announces_again(self, mock_subprocess, caplog):
        props = [(A + 'gone', 's1', 'local')]
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            for _ in range(2):
                manager, dsi = _anchor_manager()
                self.run_twice(manager, dsi, mock_subprocess, props, caplog, times=1)
        assert len([r for r in caplog.records if r.levelno == logging.INFO]) == 2

    def test_a_configured_destinations_anchor_is_not_an_orphan(self, mock_subprocess, mocker, caplog):
        manager, dsi = _anchor_manager('offsite')
        mocker.patch.object(manager, '_get_prop', return_value=None)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            self.run_twice(manager, dsi, mock_subprocess, [(A + 'offsite', 's1', 'local')], caplog, times=1)
        assert self.orphans(caplog, logging.DEBUG) == []
        assert [r for r in caplog.records if r.levelno == logging.INFO] == []


def _pruning_manager(destinations=()):
    manager, dsi = _anchor_manager(*destinations)
    dsi.config.retention_rules = [RetentionRule(timedelta(hours=1), timedelta(hours=2))]
    snaps = [_make_snap_info('autosnap', 5 + i) for i in range(4)]  # newest first, all past the 2h horizon
    return manager, dsi, snaps


class TestPruneHonoursEveryAnchor:
    def destroyed(self, mock_destroy):
        return [s for call in mock_destroy.call_args_list for s in call.args[0]]

    def test_an_orphan_anchored_snapshot_is_not_pruned(self, mock_subprocess, mocker):
        manager, dsi, snaps = _pruning_manager()
        mocker.patch.object(manager, 'list_snapshots', return_value=snaps)
        dsi.snapshots = snaps
        mock_subprocess.setup(stdout=_zget('pool/data', [(A + 'renamed-away', snaps[2].name, 'local')]))
        mock_destroy = mocker.patch('zfsbackup.backup_manager.zfs.destroy')
        manager.prune_snapshots(dsi)
        destroyed = self.destroyed(mock_destroy)
        assert snaps[2].snapshot not in destroyed
        assert destroyed == [snaps[1].snapshot, snaps[3].snapshot]

    def test_without_the_orphan_anchor_the_same_snapshot_is_pruned(self, mock_subprocess, mocker):
        manager, dsi, snaps = _pruning_manager()
        mocker.patch.object(manager, 'list_snapshots', return_value=snaps)
        dsi.snapshots = snaps
        mock_subprocess.setup(stdout=_zget('pool/data', []))
        mock_destroy = mocker.patch('zfsbackup.backup_manager.zfs.destroy')
        manager.prune_snapshots(dsi)
        assert self.destroyed(mock_destroy) == [s.snapshot for s in snaps[1:]]

    def test_the_newest_snapshot_survives_whatever_is_anchored(self, mock_subprocess, mocker):
        manager, dsi, snaps = _pruning_manager()
        mocker.patch.object(manager, 'list_snapshots', return_value=snaps)
        dsi.snapshots = snaps
        mock_subprocess.setup(stdout=_zget('pool/data', [(A + 'x', snaps[3].name, 'local')]))
        mock_destroy = mocker.patch('zfsbackup.backup_manager.zfs.destroy')
        manager.prune_snapshots(dsi)
        assert snaps[0].snapshot not in self.destroyed(mock_destroy)
        assert snaps[3].snapshot not in self.destroyed(mock_destroy)

    def test_a_configured_destinations_inherited_anchor_still_protects(self, mock_subprocess, mocker):
        manager, dsi, snaps = _pruning_manager(['offsite'])
        mocker.patch.object(manager, 'list_snapshots', return_value=snaps)
        dsi.snapshots = snaps
        mock_subprocess.setup(stdout=_zget('pool/data', []))
        mocker.patch.object(manager, '_get_prop', return_value=snaps[1].name)
        mock_destroy = mocker.patch('zfsbackup.backup_manager.zfs.destroy')
        manager.prune_snapshots(dsi)
        assert snaps[1].snapshot not in self.destroyed(mock_destroy)

    def test_an_anchor_read_failure_prunes_nothing_and_logs_an_error(self, mock_subprocess, mocker, caplog):
        manager, dsi, snaps = _pruning_manager()
        mocker.patch.object(manager, 'list_snapshots', return_value=snaps)
        dsi.snapshots = snaps
        _fail_get(mock_subprocess, 'cannot open: permission denied')
        mock_destroy = mocker.patch('zfsbackup.backup_manager.zfs.destroy')
        spy = mocker.spy(manager, 'needs_prunning')
        with caplog.at_level(logging.ERROR, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)  # must not raise
        mock_destroy.assert_not_called()
        spy.assert_not_called()
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert 'Cannot read anchors for pool/data' in errors[0] and 'permission denied' in errors[0]

    def test_a_read_failure_on_one_dataset_does_not_stop_the_next(self, mock_subprocess, mocker):
        config = _make_config(datasets=[DatasetConfig(name='pool/a'), DatasetConfig(name='pool/b')])
        manager = DatasetManager(config)
        first, second = manager.datasets
        calls = []

        def anchors(dsi):
            calls.append(dsi.name)
            if dsi.name == 'pool/a':
                raise RuntimeError('boom')
            return set()

        mocker.patch.object(manager, 'get_anchors', side_effect=anchors)
        mocker.patch.object(manager, 'list_snapshots', return_value=[_make_snap_info('autosnap', 1)])
        mocker.patch.object(manager, 'needs_prunning', return_value=[])
        manager.prune_snapshots(first)
        manager.prune_snapshots(second)
        assert calls == ['pool/a', 'pool/b']


class TestCopyAnchorPropertyOverwrite:
    DS = staticmethod(lambda: Filesystem('pool/data'))

    def props(self, old=None, new=None, new_source='local'):
        rows = []
        if old is not None:
            rows.append((A + 'old', old, 'local'))
        if new is not None:
            rows.append((A + 'new', new, new_source))
        return _zget('pool/data', rows)

    def test_overwrite_with_an_absent_target_is_copied(self, mock_subprocess):
        mock_subprocess.setup_multi((self.props('snap'),), ('',))
        assert copy_anchor_property(self.DS(), 'old', 'new', overwrite=True) == 'copied'
        assert _cmds(mock_subprocess)[1][1:] == ['set', f'{A}new=snap', 'pool/data']

    def test_overwrite_replaces_a_different_value(self, mock_subprocess):
        mock_subprocess.setup_multi((self.props('newer', 'older'),), ('',))
        assert copy_anchor_property(self.DS(), 'old', 'new', overwrite=True) == 'overwritten'
        assert _subcommands(mock_subprocess) == ['get', 'set']
        assert _cmds(mock_subprocess)[1][2] == f'{A}new=newer'

    def test_overwrite_with_an_equal_value_is_same_and_sets_nothing(self, mock_subprocess):
        mock_subprocess.setup(stdout=self.props('snap', 'snap'))
        assert copy_anchor_property(self.DS(), 'old', 'new', overwrite=True) == 'same'
        assert _subcommands(mock_subprocess) == ['get']

    def test_without_overwrite_any_existing_value_is_exists(self, mock_subprocess):
        for new in ('different', 'snap'):  # equal or not: left alone
            for kwargs in ({}, {'overwrite': False}):
                mock_subprocess.setup(stdout=self.props('snap', new))
                assert copy_anchor_property(self.DS(), 'old', 'new', **kwargs) == 'exists'
        assert 'set' not in _subcommands(mock_subprocess)

    def test_no_source_is_decided_first_whatever_overwrite_says(self, mock_subprocess):
        mock_subprocess.setup(stdout=self.props(None, 'something'))
        assert copy_anchor_property(self.DS(), 'old', 'new', overwrite=True) == 'no-source'
        assert _subcommands(mock_subprocess) == ['get']

    def test_dry_run_with_an_absent_target_is_dry_run(self, mock_subprocess):
        mock_subprocess.setup(stdout=self.props('snap'))
        assert copy_anchor_property(self.DS(), 'old', 'new', dry_run=True, overwrite=True) == 'dry-run'
        assert _subcommands(mock_subprocess) == ['get']

    def test_dry_run_with_a_different_target_names_the_overwrite(self, mock_subprocess, caplog):
        mock_subprocess.setup(stdout=self.props('newer', 'older'))
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            result = copy_anchor_property(self.DS(), 'old', 'new', dry_run=True, overwrite=True)
        assert result == 'dry-run'
        assert _subcommands(mock_subprocess) == ['get']
        assert 'Would overwrite' in caplog.text and 'older' in caplog.text and 'newer' in caplog.text

    def test_dry_run_with_an_equal_target_is_still_same(self, mock_subprocess):
        mock_subprocess.setup(stdout=self.props('snap', 'snap'))
        assert copy_anchor_property(self.DS(), 'old', 'new', dry_run=True, overwrite=True) == 'same'

    def test_dry_run_without_overwrite_on_an_existing_target_is_exists(self, mock_subprocess):
        mock_subprocess.setup(stdout=self.props('a', 'b'))
        assert copy_anchor_property(self.DS(), 'old', 'new', dry_run=True) == 'exists'

    def test_overwrite_is_keyword_only(self):
        with pytest.raises(TypeError):
            copy_anchor_property(self.DS(), 'old', 'new', False, True)
        with pytest.raises(TypeError):
            copy_anchor_property(self.DS(), 'old', 'new', True)

    def test_overwrite_defaults_to_false(self):
        import inspect

        parameter = inspect.signature(copy_anchor_property).parameters['overwrite']
        assert parameter.default is False and parameter.kind is inspect.Parameter.KEYWORD_ONLY

    def test_overwrite_never_touches_the_old_property(self, mock_subprocess):
        mock_subprocess.setup_multi((self.props('newer', 'older'),), ('',))
        copy_anchor_property(self.DS(), 'old', 'new', overwrite=True)
        for cmd in _cmds(mock_subprocess):
            assert 'inherit' not in cmd and f'{A}old' not in cmd[2:]

    def test_an_inherited_target_is_not_a_blocker_even_without_overwrite(self, mock_subprocess):
        mock_subprocess.setup_multi((self.props('snap', 'ancestors', 'inherited from pool'),), ('',))
        assert copy_anchor_property(self.DS(), 'old', 'new', overwrite=True) == 'copied'

    def test_a_received_target_counts_as_existing(self, mock_subprocess):
        mock_subprocess.setup_multi((self.props('newer', 'older', 'received'),), ('',))
        assert copy_anchor_property(self.DS(), 'old', 'new', overwrite=True) == 'overwritten'

    def test_overwrite_twice_is_overwritten_then_same(self, mock_subprocess):
        mock_subprocess.setup_multi(
            (self.props('newer', 'older'),),
            ('',),
            (self.props('newer', 'newer'),),
        )
        assert copy_anchor_property(self.DS(), 'old', 'new', overwrite=True) == 'overwritten'
        assert copy_anchor_property(self.DS(), 'old', 'new', overwrite=True) == 'same'
        assert _subcommands(mock_subprocess) == ['get', 'set', 'get']

    def test_a_failing_overwrite_set_propagates(self, mock_subprocess):
        mock_subprocess.setup_multi(
            (self.props('newer', 'older'),),
            ([], 'cannot set property: permission denied', 1),
        )
        with pytest.raises(Exception, match='permission denied'):
            copy_anchor_property(self.DS(), 'old', 'new', overwrite=True)


# Mutation counts for the anchor decision (scratch copy, one revert at a time;
# failing tests across the manager, rename and edit test files): raw anchor read
# ignored 15; `-` value protected 3; orphan INFO on every call 4; read failure
# prunes blind 3 / uncaught 4; configured destination's anchor dropped 4;
# `copy_anchor_property`: equal value not 'same' 7 / overwrite ignored 14 /
# overwrite positional 5 / 'overwritten' reported as 'copied' 7 / dry run sets 7.


class TestSyncConfigPropertyDryRun:
    """`--dry-run` must not write to ZFS: no `zfs set org.zfsbackup:config`."""

    def manager(self, dry_run):
        return DatasetManager(_make_config(dry_run=dry_run))

    def test_dry_run_makes_no_zfs_set_and_logs_what_it_would_do(self, mocker, caplog):
        manager = self.manager(True)
        dsi = manager.datasets[0]
        mocker.patch.object(manager, '_get_prop', return_value='something-else')
        zfs_set = mocker.patch('zfsbackup.backup_manager.zfs.set')
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.sync_config_property(dsi)
        zfs_set.assert_not_called()
        assert '[DRY RUN] Would set org.zfsbackup:config on pool/data' in caplog.text

    def test_dry_run_with_a_missing_property_also_sets_nothing(self, mocker):
        manager = self.manager(True)
        mocker.patch.object(manager, '_get_prop', return_value=None)
        zfs_set = mocker.patch('zfsbackup.backup_manager.zfs.set')
        manager.sync_config_property(manager.datasets[0])
        zfs_set.assert_not_called()

    def test_dry_run_with_an_up_to_date_property_says_nothing(self, mocker, caplog):
        manager = self.manager(True)
        dsi = manager.datasets[0]
        mocker.patch.object(manager, '_get_prop', return_value=dsi.config.to_property())
        zfs_set = mocker.patch('zfsbackup.backup_manager.zfs.set')
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.sync_config_property(dsi)
        zfs_set.assert_not_called()
        assert 'Would set' not in caplog.text

    def test_a_live_run_still_sets_it(self, mocker):
        manager = self.manager(False)
        mocker.patch.object(manager, '_get_prop', return_value='something-else')
        zfs_set = mocker.patch('zfsbackup.backup_manager.zfs.set')
        manager.sync_config_property(manager.datasets[0])
        zfs_set.assert_called_once()

    def test_dry_run_through_mock_subprocess_issues_no_set_command(self, mock_subprocess):
        manager = self.manager(True)
        mock_subprocess.setup(stdout=['pool/data\tfilesystem\tother\n'])
        manager.sync_config_property(manager.datasets[0])
        assert 'set' not in [c.args[0][1] for c in mock_subprocess.call_args_list]


# ---------------------------------------------------------------------------
# C1: `org.zfsbackup:inflight.<destination>` = `<from>,<to>`
# ---------------------------------------------------------------------------

from zfsbackup.backup_manager import PROP_INFLIGHT_PREFIX, _inflight_names  # noqa: E402

IF = PROP_INFLIGHT_PREFIX


class TestInflightNamesHelper:
    @pytest.mark.parametrize('value, names', [
        ('a,b', ['a', 'b']),
        (',b', ['b']),                       # a full send has no <from>
        ('a,', ['a']),
        (' a , b ', ['a', 'b']),
        ('a,b,c', ['a', 'b', 'c']),
        ('-', []),
        ('', []),
        (None, []),
        ('a,-', ['a']),
        ('-,b', ['b']),
        (',', []),
        ('solo', ['solo']),                  # malformed, but still protects what it names
    ])
    def test_names(self, value, names):
        assert _inflight_names(value) == names

    def test_the_prefix(self):
        assert PROP_INFLIGHT_PREFIX == 'org.zfsbackup:inflight.'
        assert PROP_INFLIGHT_PREFIX.startswith(PROP_PREFIX)


class TestInflightProperty:
    def _store(self, mocker):
        """A dict standing in for the dataset's properties."""
        store = {}

        def fake_set(ds, props):
            store.update(props)

        def fake_inherit(ds, prop):
            store.pop(prop, None)

        mocker.patch('zfsbackup.backup_manager.zfs.set', side_effect=fake_set)
        mocker.patch('zfsbackup.backup_manager.zfs.inherit', side_effect=fake_inherit)
        manager = DatasetManager(_make_config())
        mocker.patch.object(manager, '_get_prop', side_effect=lambda ds, prop: store.get(prop))
        return manager, manager.datasets[0], store

    def test_round_trip(self, mocker):
        manager, dsi, store = self._store(mocker)
        assert manager.set_inflight(dsi, 'offsite', 'autosnap_1', 'autosnap_2') is True
        assert store == {IF + 'offsite': 'autosnap_1,autosnap_2'}
        assert manager.get_inflight(dsi, 'offsite') == ('autosnap_1', 'autosnap_2')

    @pytest.mark.parametrize('empty', [None, ''])
    def test_a_full_send_has_an_empty_from(self, mocker, empty):
        manager, dsi, store = self._store(mocker)
        assert manager.set_inflight(dsi, 'offsite', empty, 'autosnap_2') is True
        assert store == {IF + 'offsite': ',autosnap_2'}
        assert manager.get_inflight(dsi, 'offsite') == ('', 'autosnap_2')

    def test_an_unset_property_is_none(self, mocker):
        manager, dsi, _ = self._store(mocker)
        assert manager.get_inflight(dsi, 'offsite') is None

    @pytest.mark.parametrize('value', ['-', '', 'nocomma', 'a,', ','])
    def test_a_malformed_value_is_none(self, mocker, value):
        manager, dsi, store = self._store(mocker)
        store[IF + 'offsite'] = value
        assert manager.get_inflight(dsi, 'offsite') is None

    def test_destinations_are_independent(self, mocker):
        manager, dsi, store = self._store(mocker)
        manager.set_inflight(dsi, 'one', 'a', 'b')
        manager.set_inflight(dsi, 'two.dotted', None, 'c')
        assert manager.get_inflight(dsi, 'one') == ('a', 'b')
        assert manager.get_inflight(dsi, 'two.dotted') == ('', 'c')

    def test_set_writes_exactly_one_property_with_the_documented_name(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        zfs_set = mocker.patch('zfsbackup.backup_manager.zfs.set')
        manager.set_inflight(dsi, 'offsite', 'f', 't')
        zfs_set.assert_called_once_with(dsi.dataset, {'org.zfsbackup:inflight.offsite': 'f,t'})

    def test_clear_inherits_the_property_and_returns_true(self, mocker):
        manager, dsi, store = self._store(mocker)
        manager.set_inflight(dsi, 'offsite', 'f', 't')
        assert manager.clear_inflight(dsi, 'offsite') is True
        assert store == {} and manager.get_inflight(dsi, 'offsite') is None

    def test_a_failed_set_is_false_with_a_warning_and_never_raises(self, mocker, caplog):
        mocker.patch('zfsbackup.backup_manager.zfs.set', side_effect=Exception('read-only'))
        manager = DatasetManager(_make_config())
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            assert manager.set_inflight(manager.datasets[0], 'offsite', 'f', 't') is False
        assert 'Failed to set in-flight property' in caplog.text and 'read-only' in caplog.text

    def test_a_failed_clear_is_false_with_a_warning_and_never_raises(self, mocker, caplog):
        mocker.patch('zfsbackup.backup_manager.zfs.inherit', side_effect=Exception('busy'))
        manager = DatasetManager(_make_config())
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            assert manager.clear_inflight(manager.datasets[0], 'offsite') is False
        assert 'Failed to clear in-flight property' in caplog.text

    def test_a_failed_read_is_none(self, mocker):
        manager = DatasetManager(_make_config())
        mocker.patch('zfsbackup.backup_manager.zfs.list', side_effect=Exception('err'))
        assert manager.get_inflight(manager.datasets[0], 'offsite') is None


class TestSetAnchorReturnsBool:
    def test_true_when_written(self, mocker):
        manager = DatasetManager(_make_config())
        mocker.patch('zfsbackup.backup_manager.zfs.set')
        assert manager.set_anchor(manager.datasets[0], 'offsite', 'autosnap_1') is True

    def test_false_with_a_warning_when_the_write_fails(self, mocker, caplog):
        manager = DatasetManager(_make_config())
        mocker.patch('zfsbackup.backup_manager.zfs.set', side_effect=Exception('nope'))
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            assert manager.set_anchor(manager.datasets[0], 'offsite', 'autosnap_1') is False
        assert 'Failed to set anchor property' in caplog.text


class TestGetAnchorsWithInflight:
    def _manager(self, destinations=('offsite',)):
        config = _make_config(datasets=[
            DatasetConfig(
                name='pool/data',
                remote=[RemoteDatasetConfig(destination=d) for d in destinations],
            ),
        ])
        return DatasetManager(config)

    def test_the_union_of_the_anchor_and_both_inflight_names(self, mock_subprocess, mocker):
        manager = self._manager()
        dsi = manager.datasets[0]
        mock_subprocess.setup(stdout=_zget('pool/data', [
            (PROP_ANCHOR_PREFIX + 'offsite', 'autosnap_1', 'local'),
            (IF + 'offsite', 'autosnap_1,autosnap_2', 'local'),
        ]))
        mocker.patch.object(manager, 'get_anchor', return_value='autosnap_1')
        assert manager.get_anchors(dsi) == {'autosnap_1', 'autosnap_2'}

    def test_the_from_side_alone_protects_an_older_base(self, mock_subprocess, mocker):
        manager = self._manager()
        mock_subprocess.setup(stdout=_zget('pool/data', [
            (PROP_ANCHOR_PREFIX + 'offsite', 'autosnap_3', 'local'),
            (IF + 'offsite', 'autosnap_1,autosnap_2', 'local'),
        ]))
        mocker.patch.object(manager, 'get_anchor', return_value='autosnap_3')
        assert manager.get_anchors(manager.datasets[0]) == {
            'autosnap_1', 'autosnap_2', 'autosnap_3',
        }

    def test_a_full_send_pin_protects_only_the_target(self, mock_subprocess, mocker):
        manager = self._manager()
        mock_subprocess.setup(stdout=_zget('pool/data', [(IF + 'offsite', ',autosnap_2', 'local')]))
        mocker.patch.object(manager, 'get_anchor', return_value=None)
        assert manager.get_anchors(manager.datasets[0]) == {'autosnap_2'}

    def test_a_received_inflight_pin_counts(self, mock_subprocess, mocker):
        manager = self._manager()
        mock_subprocess.setup(stdout=_zget('pool/data', [(IF + 'offsite', 'a,b', 'received')]))
        mocker.patch.object(manager, 'get_anchor', return_value=None)
        assert manager.get_anchors(manager.datasets[0]) == {'a', 'b'}

    def test_an_inherited_inflight_pin_does_not_count(self, mock_subprocess, mocker):
        manager = self._manager()
        mock_subprocess.setup(stdout=_zget('pool/data', [(IF + 'offsite', 'a,b', 'inherited from pool')]))
        mocker.patch.object(manager, 'get_anchor', return_value=None)
        assert manager.get_anchors(manager.datasets[0]) == set()

    @pytest.mark.parametrize('value', ['-', ''])
    def test_a_dash_or_empty_value_pins_nothing_and_says_nothing(
        self, mock_subprocess, mocker, caplog, value
    ):
        manager = self._manager(destinations=())
        mock_subprocess.setup(stdout=_zget('pool/data', [(IF + 'gone', value, 'local')]))
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            assert manager.get_anchors(manager.datasets[0]) == set()
        assert 'gone' not in caplog.text

    def test_an_unconfigured_destination_still_protects_and_is_announced_once(
        self, mock_subprocess, mocker, caplog
    ):
        manager = self._manager(destinations=())
        dsi = manager.datasets[0]
        props = [(IF + 'old-site', 'autosnap_1,autosnap_2', 'local')]
        mock_subprocess.setup_multi((_zget('pool/data', props),), (_zget('pool/data', props),))
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            assert manager.get_anchors(dsi) == {'autosnap_1', 'autosnap_2'}
            assert manager.get_anchors(dsi) == {'autosnap_1', 'autosnap_2'}
        infos = [r.getMessage() for r in caplog.records
                 if r.levelno == logging.INFO and 'old-site' in r.getMessage()]
        assert len(infos) == 1                              # once per (dataset, property)
        assert IF + 'old-site' in infos[0]
        assert 'autosnap_1, autosnap_2' in infos[0]
        assert 'not a configured destination' in infos[0]
        assert f'zfs inherit {IF}old-site pool/data' in infos[0]

    def test_a_configured_destination_is_not_announced(self, mock_subprocess, mocker, caplog):
        manager = self._manager()
        mock_subprocess.setup(stdout=_zget('pool/data', [(IF + 'offsite', 'a,b', 'local')]))
        mocker.patch.object(manager, 'get_anchor', return_value=None)
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.get_anchors(manager.datasets[0])
        assert 'not a configured destination' not in caplog.text

    def test_an_anchor_and_an_inflight_of_the_same_unconfigured_name_are_announced_separately(
        self, mock_subprocess, caplog
    ):
        manager = self._manager(destinations=())
        mock_subprocess.setup(stdout=_zget('pool/data', [
            (PROP_ANCHOR_PREFIX + 'x', 's1', 'local'),
            (IF + 'x', 's2,s3', 'local'),
        ]))
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            assert manager.get_anchors(manager.datasets[0]) == {'s1', 's2', 's3'}
        text = caplog.text
        assert text.count('not a configured destination') == 2
        assert f'{PROP_ANCHOR_PREFIX}x' in text and f'{IF}x' in text

    def test_an_unreadable_property_set_still_raises(self, mock_subprocess):
        """The pruner must not prune on an unknown protect-set."""
        manager = self._manager()
        proc = mock_subprocess.setup(stdout=[], stderr='cannot open', returncode=1)
        proc.poll.side_effect = None
        proc.poll.return_value = 1
        with pytest.raises(Exception):
            manager.get_anchors(manager.datasets[0])


class TestClearZfsbackupPropertiesWithInflight:
    DS = staticmethod(lambda: Filesystem('pool/data'))

    PROPS = [
        (PROP_CONFIG, 'cfg', 'local'),
        (A + 'alpha', 's1', 'local'),
        (A + 'zeta', 's2', 'local'),
        (IF + 'alpha', 'f1,t1', 'local'),
        (IF + 'mid', ',t2', 'received'),
        ('org.other:inflight.alpha', 'foreign', 'local'),
        ('compression', 'lz4', 'local'),
    ]

    def _arrange(self, mock_subprocess):
        mock_subprocess.setup_multi((_zget('pool/data', self.PROPS),), *[('',)] * 8)

    def _inherited(self, mock_subprocess):
        return [cmd[2] for cmd in _cmds(mock_subprocess) if cmd[1] == 'inherit']

    def test_anchors_true_clears_the_inflight_pins_with_the_anchors_sorted(self, mock_subprocess):
        self._arrange(mock_subprocess)
        result = clear_zfsbackup_properties(self.DS(), config=False, anchors=True)
        assert result == sorted([A + 'alpha', A + 'zeta', IF + 'alpha', IF + 'mid'])
        assert self._inherited(mock_subprocess) == result
        assert 'org.other:inflight.alpha' not in result and PROP_CONFIG not in result

    def test_the_iterable_form_clears_only_the_named_anchors_not_the_inflight_pins(
        self, mock_subprocess
    ):
        self._arrange(mock_subprocess)
        result = clear_zfsbackup_properties(self.DS(), config=False, anchors=['alpha', 'mid'])
        assert result == [A + 'alpha']              # `mid` has an inflight pin, no anchor
        assert self._inherited(mock_subprocess) == [A + 'alpha']
        assert IF + 'alpha' not in result

    def test_anchors_true_dry_run_names_them_and_inherits_nothing(self, mock_subprocess, caplog):
        self._arrange(mock_subprocess)
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            result = clear_zfsbackup_properties(
                self.DS(), config=False, anchors=True, dry_run=True)
        assert IF + 'alpha' in result and IF + 'mid' in result
        assert self._inherited(mock_subprocess) == []
        assert f'Would inherit {IF}alpha' in caplog.text

    def test_a_dataset_with_only_inflight_pins_is_cleared_by_anchors_true(self, mock_subprocess):
        mock_subprocess.setup_multi(
            (_zget('pool/data', [(IF + 'x', 'a,b', 'local')]),), ('',),
        )
        assert clear_zfsbackup_properties(self.DS(), config=False, anchors=True) == [IF + 'x']


# ---------------------------------------------------------------------------
# libzfseasy skips (and warns about, once per name) rows it cannot represent;
# the zfsbackup layer, driven through the REAL ListCommand, must carry on.
# ---------------------------------------------------------------------------

class TestUnrepresentableNamesInZfsOutput:
    @pytest.fixture(autouse=True)
    def _fresh_warning_registry(self):
        import libzfseasy.zfs as lz

        lz._warned_names.clear()
        yield
        lz._warned_names.clear()

    @staticmethod
    def _snap_name(hours_old):
        dt = datetime.now(timezone.utc) - timedelta(hours=hours_old)
        return f"autosnap_{dt.strftime('%Y%m%d%H%M%S')}"

    @staticmethod
    def _skip_warnings(caplog):
        return [
            r.getMessage() for r in caplog.records
            if r.name == 'libzfseasy' and r.levelno == logging.WARNING
            and 'cannot represent' in r.getMessage()
        ]

    def _manager(self):
        config = _make_config(datasets=[DatasetConfig(
            name='pool/data',
            retention_rules=[RetentionRule(timedelta(hours=1), timedelta(hours=2))],
        )])
        return DatasetManager(config)

    def _rows(self):
        """Newest first as `zfs list` sorts them; the two oddly named ones are
        what a VM actually showed (`zfs snapshot 'pool/fs@ manual'`)."""
        return {
            'newest': self._snap_name(0.2),
            'old1': self._snap_name(3),
            'old2': self._snap_name(4),
            'lines': [
                f'pool/data@{self._snap_name(4)}\tsnapshot\t-\n',
                'pool/data@ manual\tsnapshot\t-\n',
                f'pool/data@{self._snap_name(3)}\tsnapshot\t-\n',
                'pool/data@manual \tsnapshot\t-\n',
                f'pool/data@{self._snap_name(0.2)}\tsnapshot\t-\n',
            ],
        }

    def test_list_snapshots_returns_the_managed_ones_and_warns_once_per_name(
        self, mock_subprocess, caplog
    ):
        rows = self._rows()
        mock_subprocess.setup(stdout=rows['lines'])
        manager = self._manager()
        dsi = manager.datasets[0]
        with caplog.at_level(logging.WARNING):
            snaps = manager.list_snapshots(dsi)
        assert [s.name for s in snaps] == [rows['newest'], rows['old1'], rows['old2']]
        assert all(isinstance(s, SnapshotInfo) for s in snaps)
        warnings = self._skip_warnings(caplog)
        assert len(warnings) == 2
        assert any("'pool/data@ manual'" in w for w in warnings)
        assert any("'pool/data@manual '" in w for w in warnings)
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_the_warning_is_not_repeated_by_the_next_listing(self, mock_subprocess, caplog):
        rows = self._rows()
        mock_subprocess.setup_multi((rows['lines'],), (rows['lines'],))
        manager = self._manager()
        dsi = manager.datasets[0]
        with caplog.at_level(logging.WARNING):
            manager.list_snapshots(dsi)
            first = len(self._skip_warnings(caplog))
            manager.list_snapshots(dsi)
        assert first == 2 and len(self._skip_warnings(caplog)) == 2

    def test_a_listing_of_only_unrepresentable_rows_is_empty_not_an_error(
        self, mock_subprocess, caplog
    ):
        mock_subprocess.setup(stdout=['pool/data@ manual\tsnapshot\t-\n'])
        manager = self._manager()
        with caplog.at_level(logging.WARNING):
            assert manager.list_snapshots(manager.datasets[0]) == []
        assert len(self._skip_warnings(caplog)) == 1
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_pruning_still_destroys_only_the_valid_candidates(
        self, mock_subprocess, mocker, caplog
    ):
        rows = self._rows()
        # list (with the odd rows), then the anchor-property read for the protect-set
        mock_subprocess.setup_multi(
            (rows['lines'],),
            (['pool/data\tname\tpool/data\t-\t-\n', 'pool/data\ttype\tfilesystem\t-\t-\n'],),
        )
        destroy = mocker.patch('zfsbackup.backup_manager.zfs.destroy')
        manager = self._manager()
        with caplog.at_level(logging.WARNING):
            manager.prune_snapshots(manager.datasets[0])
        destroy.assert_called_once()
        destroyed = sorted(s.short for s in destroy.call_args.args[0])
        assert destroyed == sorted([rows['old1'], rows['old2']])      # never the newest, never the odd ones
        assert len(self._skip_warnings(caplog)) == 2
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_received_datasets_skips_an_unrepresentable_child(self, mock_subprocess, caplog):
        good = DatasetConfig(name='pool/backups/cid/data').to_property()
        other = DatasetConfig(name='pool/backups/cid/other').to_property()
        mock_subprocess.setup(stdout=[
            'pool/backups\tfilesystem\t-\t-\n',
            'pool/backups/cid\tfilesystem\t-\t-\n',
            f'pool/backups/cid/data\tfilesystem\tcid\t{good}\n',
            f'pool/backups/cid/.system\tfilesystem\tcid\t{good}\n',
            f'pool/backups/cid/other\tfilesystem\tcid\t{other}\n',
        ])
        config = _make_config(
            remote_backup=RemoteServerConfig(target_dataset='pool/backups'),
        )
        manager = DatasetManager(config)
        with caplog.at_level(logging.WARNING):
            received = manager.received_datasets()
        assert [d.name for d in received] == ['pool/backups/cid/data', 'pool/backups/cid/other']
        assert all(d.config.recursive is False for d in received)
        warnings = self._skip_warnings(caplog)
        assert len(warnings) == 1 and "'pool/backups/cid/.system'" in warnings[0]
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

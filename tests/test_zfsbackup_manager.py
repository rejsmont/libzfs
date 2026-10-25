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
from _snapshot_support import NO_TXG, list_row, listed_info, listed_snapshot
from _prune_support import (  # noqa: F401
    VERSIONS, PrunePool, hourly_name, prune_manager, prune_pool, records,
)
from zfsbackup.backup_manager import PROP_ANCHOR_PREFIX, PROP_INFLIGHT_PREFIX


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
    return listed_info(name, prefix)


def _stamp(age: timedelta, prefix: str = 'autosnap') -> str:
    """A managed snapshot name dated `age` ago."""
    return f"{prefix}_{(datetime.now(timezone.utc) - age).strftime('%Y%m%d%H%M%S')}"


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
        snap = listed_info(_stamp(timedelta(hours=2)))
        mocker.patch.object(manager, 'list_snapshots', return_value=[snap])
        assert manager.needs_snapshot(dsi) is True

    def test_no_snapshot_needed_when_recent(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        dsi.config.frequency = timedelta(hours=1)
        snap = listed_info(_stamp(timedelta(minutes=30)))
        mocker.patch.object(manager, 'list_snapshots', return_value=[snap])
        assert manager.needs_snapshot(dsi) is False


class TestCreateSnapshot:
    def test_creates_snapshot_and_returns_info(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        made = []

        def take(ds, name, **kwargs):
            made.append(name)
            return ZFSSnapshot(Filesystem('pool/data'), name)

        mocker.patch('zfsbackup.backup_manager.zfs.snapshot', side_effect=take)
        mocker.patch(
            'zfsbackup.backup_manager.zfs.list',
            side_effect=lambda **kw: [listed_snapshot(made[0], txg=77)],
        )
        result = manager.create_snapshot(dsi)
        assert result is not None
        assert result.name.startswith('autosnap_')
        assert result.createtxg == 77                  # the listed object, not the bare one

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
        mocker.patch(
            'zfsbackup.backup_manager.zfs.list',
            return_value=[listed_snapshot('autosnap_20240101120000')],
        )
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
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        manager.create_snapshot(dsi)
        assert captured['name'].startswith('mysnap_')


class TestListSnapshots:
    def test_returns_only_managed_snapshots(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        fs = Filesystem('pool/data')
        managed = listed_snapshot('autosnap_20240101120000')
        unmanaged = listed_snapshot('manual_20240101120000')
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[managed, unmanaged])
        result = manager.list_snapshots(dsi)
        assert len(result) == 1
        assert result[0].name == 'autosnap_20240101120000'

    def test_sorted_newest_first(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        fs = Filesystem('pool/data')
        older = listed_snapshot('autosnap_20240101120000')
        newer = listed_snapshot('autosnap_20240102120000')
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
        snap = listed_snapshot('autosnap_20240101120000')
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


class TestGetLocalAnchor:
    """`get_local_anchor`: the anchor as a LOCAL value of this dataset only. `get_anchor` reports
    the effective value, inherited ones included; the rollback permit (B-2b) must not count an
    anchor a parent passes down."""

    def manager(self):
        manager = DatasetManager(_make_config())
        return manager, manager.datasets[0]

    def test_it_reads_the_destination_property_with_the_local_source(self, mocker):
        manager, dsi = self.manager()
        value = mocker.patch('zfsbackup.backup_manager.zfs.get.value', return_value='autosnap_20240101120000')
        assert manager.get_local_anchor(dsi, 'offsite') == 'autosnap_20240101120000'
        value.assert_called_once_with(dsi.dataset, 'org.zfsbackup:anchor.offsite', sources='local')

    def test_unset_or_inherited_is_none(self, mocker):
        """`zfs.get.value(sources='local')` is None when the value is unset, inherited or received."""
        manager, dsi = self.manager()
        mocker.patch('zfsbackup.backup_manager.zfs.get.value', return_value=None)
        assert manager.get_local_anchor(dsi, 'offsite') is None

    def test_a_failed_read_is_unreadable_not_none(self, mocker):
        """"Unset" and "could not be read" are different evidence: a zfs failure raises
        `LocalAnchorUnreadable` carrying zfs's own message (the rollback permit reports it and sends
        nothing, instead of reading a failure as "no anchor")."""
        from zfsbackup.backup_manager import LocalAnchorUnreadable
        manager, dsi = self.manager()
        mocker.patch('zfsbackup.backup_manager.zfs.get.value', side_effect=RuntimeError('pool suspended'))
        with pytest.raises(LocalAnchorUnreadable, match='pool suspended') as exc:
            manager.get_local_anchor(dsi, 'offsite')
        assert str(exc.value) == 'pool suspended'
        assert isinstance(exc.value.__cause__, RuntimeError)

    def test_a_failure_without_a_message_still_says_something(self, mocker):
        from zfsbackup.backup_manager import LocalAnchorUnreadable
        manager, dsi = self.manager()
        mocker.patch('zfsbackup.backup_manager.zfs.get.value', side_effect=RuntimeError())
        with pytest.raises(LocalAnchorUnreadable) as exc:
            manager.get_local_anchor(dsi, 'offsite')
        assert str(exc.value) != ''

    def test_a_failed_zfs_through_the_real_get_is_unreadable(self, mock_subprocess):
        from zfsbackup.backup_manager import LocalAnchorUnreadable
        manager, dsi = self.manager()
        mock_subprocess.setup(stdout=[], stderr='cannot open pool/data: pool I/O is currently suspended', returncode=1)
        with pytest.raises(LocalAnchorUnreadable, match='suspended'):
            manager.get_local_anchor(dsi, 'offsite')

    def test_the_effective_reader_still_swallows_a_failure(self, mocker):
        """Only `get_local_anchor` changed; `get_anchor` keeps returning None on a failure."""
        manager, dsi = self.manager()
        mocker.patch('zfsbackup.backup_manager.zfs.get.value', side_effect=RuntimeError('pool suspended'))
        assert manager.get_anchor(dsi, 'offsite') is None

    def test_the_effective_reader_is_unchanged(self, mocker):
        manager, dsi = self.manager()
        value = mocker.patch('zfsbackup.backup_manager.zfs.get.value', return_value='inherited')
        assert manager.get_anchor(dsi, 'offsite') == 'inherited'
        assert value.call_args.kwargs == {}

    def test_through_the_real_get_it_asks_for_local_values_only(self, mock_subprocess):
        manager, dsi = self.manager()
        mock_subprocess.setup(stdout=['autosnap_20240101120000\n'])
        assert manager.get_local_anchor(dsi, 'offsite') == 'autosnap_20240101120000'
        argv = mock_subprocess.call_args_list[0][0][0][1:]
        assert argv[0] == 'get' and ('-s' in argv and argv[argv.index('-s') + 1] == 'local')
        assert argv[-2:] == ['org.zfsbackup:anchor.offsite', 'pool/data']

    def test_through_the_real_get_an_inherited_value_is_none(self, mock_subprocess):
        """With `-s local` zfs prints nothing for an inherited value."""
        manager, dsi = self.manager()
        mock_subprocess.setup(stdout=[])
        assert manager.get_local_anchor(dsi, 'offsite') is None


class TestPropertyHelpers:
    def test_get_prop_returns_value(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        value = mocker.patch('zfsbackup.backup_manager.zfs.get.value', return_value='testvalue')
        assert manager._get_prop(dsi.dataset, 'org.test:prop') == 'testvalue'
        value.assert_called_once_with(dsi.dataset, 'org.test:prop')

    def test_get_prop_reads_the_effective_value_whatever_its_source(self, mocker):
        """No `sources` filter: `get_anchor` relies on seeing an inherited anchor."""
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        value = mocker.patch('zfsbackup.backup_manager.zfs.get.value', return_value='v')
        manager._get_prop(dsi.dataset, 'org.test:prop')
        assert value.call_args.args == (dsi.dataset, 'org.test:prop')
        assert value.call_args.kwargs == {}

    @pytest.mark.parametrize('text', ['a\tb', 'line1\nline2', 'tab\there\nand a newline', ' lead and trail ',
                                      'x\ry'])
    def test_get_prop_returns_a_value_with_control_characters_exactly(self, mocker, text):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        mocker.patch('zfsbackup.backup_manager.zfs.get.value', return_value=text)
        assert manager._get_prop(dsi.dataset, 'org.test:prop') == text

    def test_get_prop_frames_a_multiline_value_through_the_real_get(self, mock_subprocess):
        """Through the real `zfs get -H -o value` (only Popen mocked): the lines of one value are
        joined, tabs kept -- a `zfs list -H` row could not carry it."""
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        mock_subprocess.setup(stdout=['first\tpart\n', 'second line\n'])
        assert manager._get_prop(dsi.dataset, 'org.test:prop') == 'first\tpart\nsecond line'
        argv = mock_subprocess.call_args_list[0][0][0][1:]
        assert argv[0] == 'get' and '-s' not in argv and argv[-2:] == ['org.test:prop', 'pool/data']

    def test_get_prop_returns_none_on_exception(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        value = mocker.patch('zfsbackup.backup_manager.zfs.get.value', side_effect=Exception('err'))
        assert manager._get_prop(dsi.dataset, 'org.test:prop') is None
        value.assert_called_once()

    def test_get_prop_returns_none_when_prop_is_none(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        value = mocker.patch('zfsbackup.backup_manager.zfs.get.value', return_value=None)
        assert manager._get_prop(dsi.dataset, 'org.test:prop') is None
        value.assert_called_once()

    def test_get_prop_never_uses_the_listing(self, mocker):
        manager = DatasetManager(_make_config())
        dsi = manager.datasets[0]
        listing = mocker.patch('zfsbackup.backup_manager.zfs.list')
        mocker.patch('zfsbackup.backup_manager.zfs.get.value', return_value='x')
        manager._get_prop(dsi.dataset, 'org.test:prop')
        listing.assert_not_called()

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

    def test_destroy_exception_does_not_propagate(self, prune_pool, caplog):
        """Every destroy fails (a send holds each name). Nothing escapes, the batch error is logged once,
        each member is retried alone, and each is remembered as stuck. (Before B-4 this passed only
        because a stub `zfs list` returned nothing.)"""
        manager, dsi = prune_manager()
        fresh, olds = prune_pool.populate('pool/data', old=3)
        for name in olds:
            prune_pool.send('pool/data', name)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)  # must not raise
        assert prune_pool.sizes == [3, 1, 1, 1]
        assert all(not c.ok for c in prune_pool.calls)
        assert len(records(caplog, logging.ERROR, 'Failed to prune snapshot batch')) == 1
        assert set(manager._prune_stuck) == {('pool/data', n) for n in olds}
        assert prune_pool.names_of('pool/data') == set(olds) | {fresh}
        assert records(caplog, logging.INFO, 'Successfully pruned') == []


def _destroyed(pool, dataset, before):
    """The short names that left `dataset` since `before` (a set of its names)."""
    return before - pool.names_of(dataset)


class TestPruneBatchFallbackAndStuck:
    """B-4 (decision B8): a failed batch destroys nothing, so each member is retried alone; held and
    cloned snapshots are skipped up front; a name that cannot go is remembered and retried alone."""

    DS = 'pool/data'

    def build(self, pool, old=20, **kw):
        manager, dsi = prune_manager(self.DS, **kw)
        fresh, olds = pool.populate(self.DS, old=old)
        return manager, dsi, fresh, olds

    # -- the pre-skip --------------------------------------------------------------------------------
    def test_one_held_snapshot_among_20_is_left_out_of_a_single_batch_of_19(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool)
        held = olds[7]
        prune_pool.hold(self.DS, held)
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.sizes == [19]
        assert held not in prune_pool.batches[0]
        assert prune_pool.names_of(self.DS) == {fresh, held}
        warnings = records(caplog, logging.WARNING)
        assert len(warnings) == 1
        assert f'zfs holds {self.DS}@{held}' in warnings[0]
        assert 'it is tried again alone every cycle' in warnings[0]
        assert records(caplog, logging.INFO, 'has 19 snapshots to prune')      # counted after the pre-skip
        assert records(caplog, logging.INFO, 'Successfully pruned 19 snapshots')
        assert records(caplog, logging.ERROR) == []

    def test_the_hold_count_is_named(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=3)
        prune_pool.hold(self.DS, olds[0], count=3)
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert '3 user hold(s)' in records(caplog, logging.WARNING)[0]

    def test_a_cloned_snapshot_is_left_out_and_the_warning_names_its_clones(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=5)
        prune_pool.clone(self.DS, olds[2], 'pool/clone-b', 'pool/clone-a')   # `clones` is in unstable order
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.sizes == [4]
        assert olds[2] not in prune_pool.batches[0]
        (warning,) = records(caplog, logging.WARNING)
        assert 'pool/clone-a' in warning and 'pool/clone-b' in warning
        assert olds[2] in warning
        assert prune_pool.names_of(self.DS) == {fresh, olds[2]}

    def test_the_pre_skip_also_applies_to_a_dry_run_which_never_destroys(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=5, dry_run=True)
        prune_pool.clone(self.DS, olds[1])
        prune_pool.hold(self.DS, olds[3])
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.calls == []
        assert len(records(caplog, logging.WARNING)) == 2
        (planned,) = records(caplog, logging.INFO, '[DRY RUN] Would prune snapshots')
        assert olds[1] not in planned and olds[3] not in planned
        assert all(olds[i] in planned for i in (0, 2, 4))
        assert prune_pool.names_of(self.DS) == set(olds) | {fresh}

    @pytest.mark.parametrize('dry_run', [False, True])
    def test_destroy_is_never_called_with_nothing_when_every_candidate_is_held_or_cloned(
            self, prune_pool, caplog, dry_run):
        manager, dsi, fresh, olds = self.build(prune_pool, old=4, dry_run=dry_run)
        for name in olds[:2]:
            prune_pool.hold(self.DS, name)
        for name in olds[2:]:
            prune_pool.clone(self.DS, name)
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            manager.prune_snapshots(dsi)
        assert prune_pool.calls == [] and prune_pool.empty_calls == []
        assert len(records(caplog, logging.WARNING)) == 4
        assert records(caplog, logging.INFO, 'Successfully pruned') == []

    def test_a_pre_skipped_name_warns_once_and_is_debug_afterwards(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=3)
        prune_pool.hold(self.DS, olds[1])
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            for _ in range(3):
                manager.prune_snapshots(dsi)
        assert len(records(caplog, logging.WARNING)) == 1
        assert len(records(caplog, logging.DEBUG, 'still not pruning')) == 2
        # cycle 1 destroyed the two free ones; while it stays held the name is skipped again each
        # cycle (a pre-skip, not a destroy attempt), so no further call is made.
        assert prune_pool.sizes == [2]

    def test_a_hold_released_later_lets_the_name_go_alone_with_one_info(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=4)
        held = olds[1]
        prune_pool.hold(self.DS, held)
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            assert prune_pool.names_of(self.DS) == {fresh, held}
            prune_pool.release(self.DS, held)
            manager.prune_snapshots(dsi)
        assert prune_pool.batches[1:] == [[held]]        # alone, never in a batch
        assert prune_pool.names_of(self.DS) == {fresh}
        assert manager._prune_stuck == {}
        (info,) = records(caplog, logging.INFO, 'which could not be destroyed earlier')
        assert held in info
        assert len(records(caplog, logging.WARNING)) == 1

    def test_a_cloned_snapshot_whose_clone_is_gone_goes_on_a_later_cycle(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=3)
        prune_pool.clone(self.DS, olds[0])
        manager.prune_snapshots(dsi)
        prune_pool.unclone(self.DS, olds[0])
        manager.prune_snapshots(dsi)
        assert prune_pool.names_of(self.DS) == {fresh}

    # -- the fallback --------------------------------------------------------------------------------
    @pytest.mark.parametrize('version', VERSIONS)
    def test_a_hold_that_appears_after_the_listing_fails_the_batch_and_each_member_goes_alone(
            self, mocker, caplog, version):
        pool = PrunePool(mocker, version)
        manager, dsi, fresh, olds = self.build(pool)
        late = olds[11]
        pool.before_destroy = lambda p, call: (
            p.hold(self.DS, late) if len(call.names) > 1 else None)
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert pool.sizes == [20] + [1] * 20
        assert not pool.calls[0].ok
        assert pool.batches[1:] == [[n] for n in pool.batches[0]]       # same members, one per call
        assert pool.names_of(self.DS) == {fresh, late}                  # 19 destroyed, only the late hold stays
        assert len(records(caplog, logging.ERROR, 'Failed to prune snapshot batch')) == 1
        (warning,) = records(caplog, logging.WARNING)
        assert late in warning and f'zfs holds {self.DS}@{late}' in warning     # re-listed: the hold is named
        assert ('pool/data', late) in manager._prune_stuck
        assert records(caplog, logging.INFO, 'Successfully pruned 19 snapshots')
        assert pool.empty_calls == []

    def test_a_snapshot_destroyed_by_someone_else_mid_prune_is_dropped_quietly(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=4)
        gone = olds[1]
        prune_pool.before_destroy = lambda p, call: (
            p.vanish(self.DS, gone) if gone in p.snaps[self.DS] else None)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.names_of(self.DS) == {fresh}
        assert manager._prune_stuck == {}
        assert records(caplog, logging.WARNING) == []
        assert records(caplog, logging.DEBUG, 'disappeared while pruning')

    @pytest.mark.parametrize('version', VERSIONS)
    def test_a_failing_single_destroy_is_retried_alone_next_cycle_with_no_second_warning(
            self, mocker, caplog, version):
        pool = PrunePool(mocker, version)
        manager, dsi, fresh, olds = self.build(pool, old=5)
        busy = olds[2]
        pool.send(self.DS, busy)       # `dataset is busy` on every version; the listing shows nothing
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            assert pool.names_of(self.DS) == {fresh, busy}
            pool.calls.clear()
            manager.prune_snapshots(dsi)
            manager.prune_snapshots(dsi)
        assert pool.batches == [[busy], [busy]]                  # alone each cycle, nothing else
        warnings = records(caplog, logging.WARNING)
        assert len(warnings) == 1 and busy in warnings[0]
        assert 'zfs would not destroy it' in warnings[0]
        assert 'send in progress' in warnings[0]
        assert len(records(caplog, logging.DEBUG, 'still not pruning')) == 2

    def test_a_stuck_name_never_rejoins_a_multi_snapshot_batch_and_gets_one_info_when_it_goes(
            self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=3)
        stuck = olds[0]
        prune_pool.send(self.DS, stuck)
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)                           # cycle 1: batch fails, 2 go, 1 stuck
            assert prune_pool.names_of(self.DS) == {fresh, stuck}
            prune_pool.calls.clear()
            more = [hourly_name(10 + i) for i in range(4)]          # four new candidates
            for name in more:
                prune_pool.add_snapshot(self.DS, name)
            manager.prune_snapshots(dsi)                           # cycle 2
            assert [sorted(b) for b in prune_pool.batches] == [sorted(more), [stuck]]
            assert prune_pool.sizes == [4, 1]
            prune_pool.end_send(self.DS, stuck)
            prune_pool.calls.clear()
            manager.prune_snapshots(dsi)                           # cycle 3: it finally goes
        assert prune_pool.batches == [[stuck]]
        assert prune_pool.names_of(self.DS) == {fresh}
        assert manager._prune_stuck == {}
        assert len(records(caplog, logging.INFO, 'which could not be destroyed earlier')) == 1
        assert len(records(caplog, logging.WARNING)) == 1

    def test_stuck_memory_is_per_dataset(self, prune_pool):
        other = 'pool/other'
        rules = [RetentionRule(timedelta(hours=1), timedelta(hours=2))]
        manager = DatasetManager(_make_config(datasets=[
            DatasetConfig(name=self.DS, retention_rules=rules),
            DatasetConfig(name=other, retention_rules=rules),
        ]))
        dsi, dso = manager.datasets
        _, olds = prune_pool.populate(self.DS, old=3)
        prune_pool.snaps[other] = list(olds) + [hourly_name(0, 10)]    # the same names on another dataset
        prune_pool.send(self.DS, olds[0])
        manager.prune_snapshots(dsi)
        manager.prune_snapshots(dso)
        assert olds[0] in prune_pool.names_of(self.DS)
        assert prune_pool.names_of(other) == {prune_pool.snaps[other][-1]}   # the other dataset pruned all its own
        assert set(manager._prune_stuck) == {(self.DS, olds[0])}

    # -- forgetting ----------------------------------------------------------------------------------
    def test_a_stuck_entry_is_forgotten_when_the_snapshot_vanishes(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=3)
        prune_pool.send(self.DS, olds[0])
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            assert manager._prune_stuck
            prune_pool.vanish(self.DS, olds[0])
            manager.prune_snapshots(dsi)
        assert manager._prune_stuck == {}
        assert records(caplog, logging.INFO, 'which could not be destroyed earlier') == []

    def test_a_stuck_entry_is_forgotten_when_every_snapshot_is_gone(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=2)
        prune_pool.send(self.DS, olds[0])
        manager.prune_snapshots(dsi)
        assert manager._prune_stuck
        prune_pool.snaps[self.DS] = []
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert manager._prune_stuck == {}
        assert records(caplog, logging.INFO, 'which could not be destroyed earlier') == []

    def test_a_stuck_entry_is_forgotten_when_the_snapshot_stops_being_a_candidate(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=3)
        prune_pool.send(self.DS, olds[0])
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            assert manager._prune_stuck
            # an anchor now pins it: retention no longer proposes it
            prune_pool.props[self.DS] = [(PROP_ANCHOR_PREFIX + 'offsite', olds[0], 'local')]
            prune_pool.end_send(self.DS, olds[0])
            prune_pool.calls.clear()
            manager.prune_snapshots(dsi)
        assert prune_pool.calls == []
        assert manager._prune_stuck == {}
        assert records(caplog, logging.INFO, 'which could not be destroyed earlier') == []
        assert olds[0] in prune_pool.names_of(self.DS)

    # -- counting ------------------------------------------------------------------------------------
    def test_the_success_count_is_the_number_of_snapshots_destroyed_never_the_returned_lines(
            self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=6, recursive=True)
        # a child with the same names: a recursive destroy returns 2 `destroy` lines per name
        prune_pool.snaps[self.DS + '/c'] = list(prune_pool.snaps[self.DS])
        prune_pool.send(self.DS, olds[0])
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert all(c.recursive for c in prune_pool.calls)
        assert prune_pool.sizes == [6] + [1] * 6
        assert len(prune_pool.names_of(self.DS)) == 2
        assert records(caplog, logging.INFO, 'Successfully pruned 5 snapshots')
        assert len(records(caplog, logging.INFO, 'Successfully pruned')) == 1

    def test_a_cycle_whose_every_destroy_failed_reports_no_success(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=2)
        for name in olds:
            prune_pool.send(self.DS, name)
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert records(caplog, logging.INFO, 'Successfully pruned') == []

    def test_a_failed_batch_of_a_full_cycle_counts_only_the_singles_that_went(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=45)
        prune_pool.send(self.DS, olds[30])                              # in the second batch of 20
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.sizes == [20, 20, 5] + [1] * 20
        assert prune_pool.ok_calls and sum(len(c.names) for c in prune_pool.ok_calls) == 44
        assert records(caplog, logging.INFO, 'Successfully pruned 44 snapshots')

    def test_45_candidates_make_calls_of_20_20_and_5(self, prune_pool):
        manager, dsi, fresh, olds = self.build(prune_pool, old=45)
        manager.prune_snapshots(dsi)
        assert prune_pool.sizes == [20, 20, 5]
        assert all(c.kwargs == {'destroy': True, 'recursive': False, 'clones': False} for c in prune_pool.calls)
        assert prune_pool.names_of(self.DS) == {fresh}

    # -- the explanation of a failure ----------------------------------------------------------------
    def test_a_failed_re_list_still_marks_the_names_stuck_with_the_generic_reason(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=3)
        prune_pool.send(self.DS, olds[0])
        # the cycle's own listing is the first; the explanation's re-list (the second) fails
        prune_pool.list_fails_from = 2
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.names_of(self.DS) == {fresh, olds[0]}
        (warning,) = records(caplog, logging.WARNING)
        assert olds[0] in warning and 'zfs would not destroy it' in warning
        assert ('pool/data', olds[0]) in manager._prune_stuck
        assert records(caplog, logging.DEBUG, 'Could not re-list')
        assert len(prune_pool.snapshot_lists) == 2

    def test_the_generic_reason_mentions_descendants_only_for_a_recursive_dataset(self, prune_pool):
        for recursive, expected in ((False, False), (True, True)):
            prune_pool.snaps.clear()
            prune_pool.sending.clear()
            manager, dsi, fresh, olds = self.build(prune_pool, old=2, recursive=recursive)
            prune_pool.send(self.DS, olds[0])
            manager.prune_snapshots(dsi)
            reason = manager._prune_stuck[(self.DS, olds[0])]
            assert ('descendant' in reason) is expected


def zfs_get_exec_out():
    """The mocked `zfs.get._exec_out` (installed by `PrunePool`)."""
    import libzfseasy as zfs
    return zfs.get._exec_out


class TestPruneReReadsProtectionsBeforeEveryDestroy:
    """The pass's first read of the anchors and in-flight pins goes stale: the remote worker pins, sends,
    anchors and re-pins while the fallback retries a failed batch. `_still_prunable` re-reads them right
    before every real destroy call (a batch and each single) and drops a name that became protected."""

    DS = 'pool/data'
    CH = 'pool/data/c'

    def build(self, pool, old=5, recursive=False, dry_run=False):
        manager, dsi = prune_manager(self.DS, recursive=recursive, dry_run=dry_run)
        fresh, olds = pool.populate(self.DS, old=old)
        if recursive:
            pool.snaps[self.CH] = list(pool.snaps[self.DS])
        return manager, dsi, fresh, olds

    def protect_after_the_batch_fails(self, pool, olds, props, dataset=None):
        """Set `props` on `dataset` as the first multi-name destroy call starts: after the pass's read
        and the re-read before the batch, before the re-reads before the singles."""
        def hook(p, call):
            if len(call.names) > 1 and dataset not in p.props:
                p.props[dataset or self.DS] = props
        pool.before_destroy = hook
        pool.send(self.DS, olds[0])      # a send in progress: the batch fails with EBUSY

    PINS = {
        'inflight pin': lambda olds: (PROP_INFLIGHT_PREFIX + 'offsite', f'{olds[2]},{olds[3]}', 'local'),
        'another destinations anchor': lambda olds: (PROP_ANCHOR_PREFIX + 'other-dest', olds[2], 'local'),
        'received anchor': lambda olds: (PROP_ANCHOR_PREFIX + 'offsite', olds[2], 'received'),
        'local anchor': lambda olds: (PROP_ANCHOR_PREFIX + 'offsite', olds[2], 'local'),
    }

    @pytest.mark.parametrize('kind', sorted(PINS))
    def test_a_protection_that_appears_after_the_batch_failed_stops_that_names_single_retry(
            self, prune_pool, caplog, kind):
        manager, dsi, fresh, olds = self.build(prune_pool)
        prop = self.PINS[kind](olds)
        protected = {olds[2], olds[3]} if kind == 'inflight pin' else {olds[2]}
        self.protect_after_the_batch_fails(prune_pool, olds, [prop])
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert not prune_pool.calls[0].ok and len(prune_pool.calls[0].names) == 5
        singles = [c.names[0] for c in prune_pool.calls[1:]]
        assert protected.isdisjoint(singles)
        assert sorted(singles) == sorted(set(olds) - protected)       # the others were still retried
        assert prune_pool.names_of(self.DS) == {fresh, olds[0]} | protected     # olds[0]: the sender holds it
        assert not (protected & {k[1] for k in manager._prune_stuck})  # protected is not "stuck"
        assert len(records(caplog, logging.INFO, 'became an anchor or in-flight snapshot')) == len(protected)

    def test_without_the_re_read_the_race_destroys_the_protected_snapshot(self, prune_pool, mocker):
        """The control: the same race against a pass that keeps its first protect-set."""
        manager, dsi, fresh, olds = self.build(prune_pool)
        mocker.patch.object(manager, '_still_prunable', side_effect=lambda d, group: group)
        self.protect_after_the_batch_fails(
            prune_pool, olds, [(PROP_ANCHOR_PREFIX + 'offsite', olds[2], 'local')])
        manager.prune_snapshots(dsi)
        assert olds[2] not in prune_pool.names_of(self.DS)

    def test_a_protection_that_appears_before_the_batch_filters_the_batch_call(self, prune_pool, mocker, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool)
        calls = []

        def anchors(d, own_properties_last=False):
            calls.append(1)
            return set() if len(calls) == 1 else {olds[1]}        # the re-read before the batch sees a pin

        mocker.patch.object(manager, 'get_anchors', side_effect=anchors)
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.sizes == [4]
        assert olds[1] not in prune_pool.batches[0]
        assert prune_pool.names_of(self.DS) == {fresh, olds[1]}
        assert manager._prune_stuck == {}
        assert records(caplog, logging.ERROR) == []

    # -- the order of the re-read: the dataset's own properties come LAST ------------------------
    @pytest.mark.parametrize('recursive', [False, True])
    @pytest.mark.parametrize('dests', [(), ('offsite',), ('offsite', 'cold', 'lab')])
    def test_the_datasets_own_property_read_is_the_last_zfs_call_before_each_destroy(
            self, prune_pool, recursive, dests):
        manager, dsi = prune_manager(self.DS, recursive=recursive, destinations=dests)
        fresh, olds = prune_pool.populate(self.DS, old=4)
        if recursive:
            prune_pool.snaps[self.CH] = list(prune_pool.snaps[self.DS])
        prune_pool.send(self.DS, olds[0])               # the batch fails: a batch call, then 4 singles
        manager.prune_snapshots(dsi)
        events = prune_pool.events
        destroys = [i for i, e in enumerate(events) if e[0] == 'destroy']
        assert len(destroys) == 5
        for i in destroys:
            assert events[i - 1] == ('get', self.DS, False), events[max(0, i - 6):i + 1]
            # before it: the per-destination reads, and (recursive) the descendants' read first of all
            tail = events[:i - 1]
            for dest in reversed(sorted(dests)):         # configured destinations are read sorted
                assert tail.pop() == ('value', self.DS, PROP_ANCHOR_PREFIX + dest)
            if recursive:
                assert tail.pop() == ('get', self.DS, True)

    def test_the_default_order_of_get_anchors_is_unchanged_and_the_flag_reverses_the_own_read(self, prune_pool):
        manager, dsi = prune_manager(self.DS, destinations=('offsite', 'cold'))
        prune_pool.populate(self.DS, old=1)
        manager.get_anchors(dsi)
        assert prune_pool.events == [
            ('get', self.DS, False),
            ('value', self.DS, PROP_ANCHOR_PREFIX + 'cold'),
            ('value', self.DS, PROP_ANCHOR_PREFIX + 'offsite'),
        ]
        prune_pool.events.clear()
        manager.get_anchors(dsi, own_properties_last=False)
        assert [e[0] for e in prune_pool.events] == ['get', 'value', 'value']
        prune_pool.events.clear()
        manager.get_anchors(dsi, own_properties_last=True)
        assert [e[0] for e in prune_pool.events] == ['value', 'value', 'get']

    def test_both_orders_return_the_same_set(self, prune_pool):
        manager, dsi = prune_manager(self.DS, destinations=('offsite',))
        fresh, olds = prune_pool.populate(self.DS, old=3)
        prune_pool.props[self.DS] = [
            (PROP_ANCHOR_PREFIX + 'offsite', olds[0], 'local'),
            (PROP_INFLIGHT_PREFIX + 'other', f'{olds[1]},{olds[2]}', 'local'),
        ]
        assert manager.get_anchors(dsi) == manager.get_anchors(dsi, own_properties_last=True) \
            == set(olds)

    def test_a_whole_chain_step_completing_during_the_descendants_read_loses_nothing(self, prune_pool, caplog):
        """The reviewer's probe, inverted. The remote worker pins (a, b), sends, anchors b, and clears the pin
        while the pruner's re-read is reading the descendants. The own-properties read comes after it, so
        the new anchor b is seen and kept; the old base a was never a candidate this pass."""
        manager, dsi, fresh, olds = self.build(prune_pool, old=5, recursive=True)
        old_base, new_anchor = olds[1], olds[2]
        prune_pool.props[self.DS] = [(PROP_ANCHOR_PREFIX + 'offsite', old_base, 'local')]
        recursive_reads = []
        real_get = prune_pool._get

        def get(cmd):
            if '-r' in cmd:
                recursive_reads.append(cmd)
                if len(recursive_reads) == 2:       # the re-read's descendant read (the pass made the first)
                    prune_pool.props[self.DS] = [(PROP_ANCHOR_PREFIX + 'offsite', new_anchor, 'local')]
            return real_get(cmd)

        mocker_target = zfs_get_exec_out()
        mocker_target.side_effect = get
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        for ds in (self.DS, self.CH):
            assert new_anchor in prune_pool.names_of(ds), ds       # the new anchor survived
            assert old_base in prune_pool.names_of(ds), ds         # the old base was not a candidate
        assert len(records(caplog, logging.INFO, 'became an anchor or in-flight snapshot')) == 1
        # the next pass: the old base is free now and goes, the anchor stays; the one after is stable
        manager.prune_snapshots(dsi)
        assert prune_pool.names_of(self.DS) == {fresh, new_anchor}
        assert prune_pool.names_of(self.CH) == {fresh, new_anchor}
        prune_pool.calls.clear()
        manager.prune_snapshots(dsi)
        assert prune_pool.calls == []
        assert prune_pool.names_of(self.DS) == {fresh, new_anchor}

    def test_a_pin_whose_to_snapshot_no_longer_exists_still_protects_its_from(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=4)
        gone = hourly_name(40)
        prune_pool.props[self.DS] = [(PROP_INFLIGHT_PREFIX + 'offsite', f'{olds[1]},{gone}', 'local')]
        assert manager.get_anchors(dsi) == {olds[1], gone}
        manager.prune_snapshots(dsi)
        assert olds[1] in prune_pool.names_of(self.DS)
        assert prune_pool.names_of(self.DS) == {fresh, olds[1]}
        assert olds[1] not in [n for c in prune_pool.calls for n in c.names]

    def test_a_pin_appearing_after_the_batch_failed_protects_its_from_though_its_to_is_gone(
            self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=5)
        gone = hourly_name(50)
        self.protect_after_the_batch_fails(
            prune_pool, olds, [(PROP_INFLIGHT_PREFIX + 'offsite', f'{olds[2]},{gone}', 'local')])
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert olds[2] in prune_pool.names_of(self.DS)
        assert olds[2] not in [c.names[0] for c in prune_pool.calls[1:]]

    def test_a_batch_whose_every_member_became_protected_is_never_destroyed_empty(self, prune_pool, mocker, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=3)
        mocker.patch.object(manager, 'get_anchors', side_effect=[set(), set(olds)])
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.calls == [] and prune_pool.empty_calls == []
        assert prune_pool.names_of(self.DS) == set(olds) | {fresh}
        assert len(records(caplog, logging.INFO, 'became an anchor or in-flight snapshot')) == 3
        assert records(caplog, logging.INFO, 'Successfully pruned') == []

    def test_a_re_read_that_raises_destroys_nothing_logs_an_error_and_leaves_the_stuck_memory(
            self, prune_pool, mocker, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=4)
        prune_pool.send(self.DS, olds[0])
        manager.prune_snapshots(dsi)                       # olds[0] is stuck now
        stuck = dict(manager._prune_stuck)
        assert list(stuck) == [(self.DS, olds[0])]
        prune_pool.calls.clear()
        mocker.patch.object(manager, 'get_anchors', side_effect=[set(), RuntimeError('pool I/O suspended')])
        caplog.clear()
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.calls == []
        assert manager._prune_stuck == stuck
        (error,) = records(caplog, logging.ERROR)
        assert 'Cannot re-read the anchors of pool/data' in error and 'pool I/O suspended' in error
        assert olds[0] in error

    def test_a_re_read_that_raises_after_a_failed_batch_issues_no_single_destroy(self, prune_pool, mocker, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=3)
        prune_pool.send(self.DS, olds[0])
        mocker.patch.object(
            manager, 'get_anchors', side_effect=[set(), set()] + [RuntimeError('boom')] * 3)
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.sizes == [3]                   # the batch only; every single was refused
        assert manager._prune_stuck == {}
        assert len(records(caplog, logging.ERROR, 'Cannot re-read the anchors')) == 3
        assert prune_pool.names_of(self.DS) == set(olds) | {fresh}

    @pytest.mark.parametrize('recursive', [False, True])
    def test_a_dry_run_re_reads_nothing_and_destroys_nothing(self, prune_pool, mocker, recursive):
        manager, dsi, fresh, olds = self.build(prune_pool, recursive=recursive, dry_run=True)
        anchors = mocker.spy(manager, 'get_anchors')
        still = mocker.spy(manager, '_still_prunable')
        prune_pool.send(self.DS, olds[0])
        manager.prune_snapshots(dsi)
        assert prune_pool.calls == []
        still.assert_not_called()
        assert anchors.call_count == 1                    # the pass's own read
        assert len(prune_pool.get_calls) == (2 if recursive else 1)    # + the one `get -r` of a recursive pass

    @pytest.mark.parametrize('recursive', [False, True])
    def test_one_re_read_per_real_destroy_call(self, prune_pool, mocker, recursive):
        manager, dsi, fresh, olds = self.build(prune_pool, old=25, recursive=recursive)
        prune_pool.send(self.DS, olds[-3])                # in the first batch (newest first): it fails, 20 singles follow
        anchors = mocker.spy(manager, 'get_anchors')
        manager.prune_snapshots(dsi)
        assert prune_pool.sizes == [20, 5] + [1] * 20
        assert anchors.call_count == 1 + len(prune_pool.calls)
        if recursive:
            gets = [c for c in prune_pool.get_calls if '-r' in c]
            assert len(gets) == 1 + len(prune_pool.calls)

    @pytest.mark.parametrize('kind', ['child anchor', 'child inflight pin'])
    def test_a_descendants_protection_appearing_after_the_batch_failed_stops_the_single(
            self, prune_pool, caplog, kind):
        manager, dsi, fresh, olds = self.build(prune_pool, recursive=True)
        prop = ((PROP_ANCHOR_PREFIX + 'offsite', olds[2], 'local') if kind == 'child anchor'
                else (PROP_INFLIGHT_PREFIX + 'offsite', f'{olds[1]},{olds[2]}', 'local'))
        protected = {olds[2]} if kind == 'child anchor' else {olds[1], olds[2]}
        self.protect_after_the_batch_fails(prune_pool, olds, [prop], dataset=self.CH)
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert all(c.recursive for c in prune_pool.calls)
        assert protected.isdisjoint(c.names[0] for c in prune_pool.calls[1:])
        assert prune_pool.names_of(self.DS) == {fresh, olds[0]} | protected
        assert prune_pool.names_of(self.CH) == {fresh, olds[0]} | protected     # nor the child's copy
        assert len(records(caplog, logging.INFO, 'became an anchor or in-flight snapshot')) == len(protected)


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


class FakeReceived:
    """`zfs.list` (the enumeration) and `zfs.get.value` (the per-dataset property reads) of
    `DatasetManager.received_datasets()`, in memory (plan B-1c).

    `datasets` is the list of names the listing returns; `local[name]` / `received[name]` map a
    property to the value `zfs get -s local|received` would print (a missing key reads as unset);
    `errors[(name, prop)]` makes that read raise. Every `zfs.get.value` call is recorded in `reads`
    as `(name, prop, sources)`."""

    def __init__(self, mocker, datasets=(), local=None, received=None, errors=None, list_error=None):
        self.datasets = list(datasets)
        self.local = local or {}
        self.received = received or {}
        self.errors = errors or {}
        self.list_error = list_error
        self.reads = []
        self.list = mocker.patch('zfsbackup.backup_manager.zfs.list', side_effect=self._list)
        self.value = mocker.patch('zfsbackup.backup_manager.zfs.get.value', side_effect=self._value)

    def _list(self, *args, **kwargs):
        if self.list_error is not None:
            raise self.list_error
        return [Filesystem(n) for n in self.datasets]

    def _value(self, ds, prop, sources=None, raw=False):
        self.reads.append((ds.name, prop, sources))
        if (ds.name, prop) in self.errors:
            raise self.errors[(ds.name, prop)]
        local = self.local.get(ds.name, {}).get(prop)
        received = self.received.get(ds.name, {}).get(prop)
        if sources == 'local':
            return local
        if sources == 'received':
            return received
        return local if local is not None else received

    def reads_of(self, name):
        return [(p, src) for n, p, src in self.reads if n == name]


def _remote_manager(target='pool/backups', **kwargs):
    config = _make_config(**kwargs)
    config.remote_backup = RemoteServerConfig(target_dataset=target, enabled=True)
    return DatasetManager(config)


def _client_props(client_id='client1', config=None):
    from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_CONFIG
    return {PROP_CLIENT_ID: client_id,
            PROP_CONFIG: config if config is not None else DatasetConfig(name='pool/data').to_property()}


class TestReceivedDatasetsSuccess:
    def _make_remote_config(self):
        config = _make_config()
        config.remote_backup = RemoteServerConfig(target_dataset='pool/backups', enabled=True)
        return config

    def test_returns_dataset_info_for_valid_props(self, mocker):
        FakeReceived(mocker, ['pool/backups/client1'], local={'pool/backups/client1': _client_props()})
        result = DatasetManager(self._make_remote_config()).received_datasets()
        assert len(result) == 1
        assert result[0].name == 'pool/backups/client1'
        assert isinstance(result[0], DatasetInfo)

    def test_skips_ds_with_missing_client_id(self, mocker):
        from zfsbackup.backup_manager import PROP_CLIENT_ID
        props = _client_props()
        del props[PROP_CLIENT_ID]
        fake = FakeReceived(mocker, ['pool/backups/client1'], local={'pool/backups/client1': props})
        assert DatasetManager(self._make_remote_config()).received_datasets() == []
        assert ('pool/backups/client1', PROP_CLIENT_ID, 'local') in fake.reads

    def test_skips_ds_with_invalid_config_prop(self, mocker):
        FakeReceived(mocker, ['pool/backups/client1'],
                     local={'pool/backups/client1': _client_props(config='not-valid-base64!!!')})
        assert DatasetManager(self._make_remote_config()).received_datasets() == []

    def test_the_decoded_config_is_the_one_returned(self, mocker):
        cfg = DatasetConfig(name='pool/src', recursive=True,
                            retention_rules=[RetentionRule(timedelta(hours=1), timedelta(hours=9))])
        FakeReceived(mocker, ['pool/backups/client1/src'],
                     local={'pool/backups/client1/src': _client_props(config=cfg.to_property())})
        (dsi,) = DatasetManager(self._make_remote_config()).received_datasets()
        assert dsi.config.name == 'pool/src'
        assert dsi.config.retention_rules[0].keep_for == timedelta(hours=9)
        assert dsi.config.recursive is False             # forced: received datasets are pruned flat


class TestReceivedDatasetsStopsAtASafePoint:
    """Plan B. Each received dataset costs two or three `zfs get` executions, so `received_datasets`
    takes the pruning worker's `should_stop` and checks it BEFORE each dataset's reads. Once it is true the
    datasets examined so far come back (a partial list, only good for leaving), and the skip memory
    `_received_skip_noted`, which is trimmed against a COMPLETE listing, is left alone."""

    T = 'pool/backups'
    NAMES = [f'pool/backups/c/d{i}' for i in range(5)]

    def local(self, names=None):
        return {n: _client_props('c') for n in (names or self.NAMES)}

    def manager(self):
        return _remote_manager(self.T)

    @staticmethod
    def stop_after(n, events=None):
        """A `should_stop` that is false for its first `n` calls, then true."""
        calls = []

        def should_stop():
            calls.append(1)
            if events is not None:
                events.append('stop?')
            return len(calls) > n
        should_stop.calls = calls
        return should_stop

    def test_without_a_check_the_listing_is_complete(self, mocker):
        FakeReceived(mocker, self.NAMES, local=self.local())
        assert [d.name for d in self.manager().received_datasets()] == self.NAMES

    def test_a_check_that_never_fires_changes_nothing(self, mocker):
        FakeReceived(mocker, self.NAMES, local=self.local())
        stop = self.stop_after(10 ** 6)
        assert [d.name for d in self.manager().received_datasets(should_stop=stop)] == self.NAMES
        assert len(stop.calls) == len(self.NAMES)             # once per dataset

    def test_a_stop_mid_way_returns_the_datasets_examined_so_far(self, mocker):
        fake = FakeReceived(mocker, self.NAMES, local=self.local())
        result = self.manager().received_datasets(should_stop=self.stop_after(2))
        assert [d.name for d in result] == self.NAMES[:2]
        assert all(isinstance(d, DatasetInfo) for d in result)
        for name in self.NAMES[2:]:
            assert fake.reads_of(name) == [], f'{name} was read after the stop'
        assert fake.reads_of(self.NAMES[1]) != []

    def test_the_check_comes_before_each_datasets_property_reads(self, mocker):
        events = []
        fake = FakeReceived(mocker, self.NAMES[:3], local=self.local())
        fake.value.side_effect = lambda ds, prop, sources=None, raw=False: (
            events.append(f'read:{ds.name}'), fake._value(ds, prop, sources))[1]
        self.manager().received_datasets(should_stop=self.stop_after(10 ** 6, events))
        for i, name in enumerate(self.NAMES[:3]):
            first_read = events.index(f'read:{name}')
            assert events[first_read - 1] == 'stop?', events

    def test_a_stop_before_the_first_dataset_reads_no_property(self, mocker):
        fake = FakeReceived(mocker, self.NAMES, local=self.local())
        result = self.manager().received_datasets(should_stop=lambda: True)
        assert result == []
        assert fake.reads == []                                # not one `zfs get`
        fake.list.assert_called_once()                         # the enumeration itself already ran

    def test_the_stop_ends_the_enumeration_not_just_one_dataset(self, mocker):
        fake = FakeReceived(mocker, self.NAMES, local=self.local())
        stop = self.stop_after(1)
        self.manager().received_datasets(should_stop=stop)
        assert len(stop.calls) == 2                            # not asked again after it said yes
        assert len(fake.reads) == len(fake.reads_of(self.NAMES[0]))

    def test_the_skip_memory_is_untouched_by_a_stopped_listing(self, mocker, caplog):
        gone = 'pool/backups/c/gone'
        later = self.NAMES[4]
        fake = FakeReceived(mocker, self.NAMES, local=self.local())
        manager = self.manager()
        manager._received_skip_noted.update({gone: 'old problem', later: 'old problem'})
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            manager.received_datasets(should_stop=self.stop_after(2))
        # `gone` is not in the (partial) listing and `later` was not reached: trimming against
        # that would forget two problems that were never re-examined
        assert manager._received_skip_noted == {gone: 'old problem', later: 'old problem'}
        # control: a complete listing does trim
        manager.received_datasets()
        assert manager._received_skip_noted == {}

    def test_a_dataset_examined_before_the_stop_is_still_noted_or_cleared(self, mocker):
        """The memory of datasets that WERE examined is kept current."""
        fake = FakeReceived(mocker, self.NAMES, local=self.local())
        manager = self.manager()
        manager._received_skip_noted[self.NAMES[0]] = 'old problem'
        manager.received_datasets(should_stop=self.stop_after(2))
        assert self.NAMES[0] not in manager._received_skip_noted        # accepted now: forgotten

    def test_no_warning_is_issued_by_a_stop(self, mocker, caplog):
        FakeReceived(mocker, self.NAMES, local=self.local())
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            self.manager().received_datasets(should_stop=self.stop_after(2))
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []

    def test_disabled_remote_backup_never_consults_the_check(self, mocker):
        FakeReceived(mocker)
        stop = self.stop_after(10 ** 6)
        assert DatasetManager(_make_config()).received_datasets(should_stop=stop) == []
        assert stop.calls == []

    def test_a_failing_listing_never_consults_the_check(self, mocker):
        FakeReceived(mocker, list_error=RuntimeError('pool is suspended'))
        stop = self.stop_after(10 ** 6)
        assert self.manager().received_datasets(should_stop=stop) == []
        assert stop.calls == []


class TestReceivedDatasetsForgedRows:
    """Plan B-1c. `received_datasets()` enumerates with `zfs list` (names only) and reads each
    dataset's `client_id` and `config` one at a time, LOCAL only; a dataset whose properties do not
    describe a client tree it lies in is skipped -- alone."""

    T = 'pool/backups'
    GOOD = DatasetConfig(name='pool/data').to_property()

    def manager(self, target=None):
        return _remote_manager(target or self.T)

    def run(self, mocker, datasets, local=None, received=None, errors=None, target=None):
        fake = FakeReceived(mocker, datasets, local, received, errors)
        return fake, self.manager(target).received_datasets()

    @staticmethod
    def warnings(caplog):
        return [r.getMessage() for r in caplog.records
                if r.name == 'zfsbackup.backup_manager' and r.levelno == logging.WARNING]

    @staticmethod
    def debugs(caplog):
        return [r.getMessage() for r in caplog.records
                if r.name == 'zfsbackup.backup_manager' and r.levelno == logging.DEBUG]

    # -- one case per skip reason ------------------------------------------------------------
    def test_a_dataset_with_neither_property_local_is_skipped_at_debug_only(self, mocker, caplog):
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            fake, result = self.run(mocker, ['pool/backups', 'pool/backups/client1'])
        assert result == []
        assert self.warnings(caplog) == []
        assert any('not a client dataset' in m for m in self.debugs(caplog))

    def test_such_a_dataset_with_a_received_config_is_a_warning(self, mocker, caplog):
        from zfsbackup.backup_manager import PROP_CONFIG
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            fake, result = self.run(mocker, ['pool/backups/client1'],
                                    received={'pool/backups/client1': {PROP_CONFIG: self.GOOD}})
        assert result == []
        (warning,) = self.warnings(caplog)
        assert 'pool/backups/client1' in warning and 'only received' in warning
        assert 'never trusted' in warning
        assert ('pool/backups/client1', PROP_CONFIG, 'received') in fake.reads

    def test_a_received_client_id_alone_is_not_looked_for(self, mocker):
        """Only the config is probed for a received copy: one extra read, not two."""
        from zfsbackup.backup_manager import PROP_CLIENT_ID
        fake, result = self.run(mocker, ['pool/backups/client1'],
                                received={'pool/backups/client1': {PROP_CLIENT_ID: 'client1'}})
        assert result == []
        assert [r for r in fake.reads if r[2] == 'received'] == [
            ('pool/backups/client1', 'org.zfsbackup:config', 'received')]

    @pytest.mark.parametrize('present', ['client_id', 'config'])
    def test_only_one_property_set_is_skipped_with_a_warning(self, mocker, caplog, present):
        from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_CONFIG
        props = _client_props()
        keep = PROP_CLIENT_ID if present == 'client_id' else PROP_CONFIG
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            fake, result = self.run(mocker, ['pool/backups/client1'],
                                    local={'pool/backups/client1': {keep: props[keep]}})
        assert result == []
        missing = PROP_CONFIG if present == 'client_id' else PROP_CLIENT_ID
        (warning,) = self.warnings(caplog)
        assert f'{missing} is not set locally' in warning

    @pytest.mark.parametrize('bad_id', ['../x', 'a/b', 'x y', '-lead', '.lead', 'a\nb', 'a\tb', 'é',
                                        'x' * 65, 'a:b'])
    def test_a_client_id_that_fails_the_grammar_is_skipped(self, mocker, caplog, bad_id):
        from zfsbackup.config import client_id_problem
        assert client_id_problem(bad_id) is not None
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            fake, result = self.run(mocker, ['pool/backups/client1'],
                                    local={'pool/backups/client1': _client_props(bad_id)})
        assert result == []
        (warning,) = self.warnings(caplog)
        assert 'is invalid' in warning
        assert '\n' not in warning and '\t' not in warning        # logged with !r

    @pytest.mark.parametrize('client_id, dataset', [
        ('client1', 'pool/backups/other/data'),                  # another client's tree
        ('B', 'pool/backups/Bx'),                                 # a prefix sibling of B
        ('B', 'pool/backups/Bx/data'),
        ('B', 'pool/backups/A/data'),                             # B's id planted in A's tree
        ('B', 'pool/backups'),                                    # the target itself
        ('B', 'pool/backups/b'),                                  # case differs
        ('B', 'pool/elsewhere/B/data'),                           # outside the target
        ('B', 'pool/backups/B2'),
        ('c1', 'pool/backups/c1x/c1'),
    ])
    def test_a_dataset_outside_its_clients_tree_is_skipped(self, mocker, caplog, client_id, dataset):
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            fake, result = self.run(mocker, [dataset], local={dataset: _client_props(client_id)})
        assert result == []
        (warning,) = self.warnings(caplog)
        assert 'lies outside its client' in warning
        assert f"'pool/backups/{client_id}'" in warning

    @pytest.mark.parametrize('client_id, dataset', [
        ('B', 'pool/backups/B'), ('B', 'pool/backups/B/x'), ('B', 'pool/backups/B/x/y/z'),
        ('b-1.x_y', 'pool/backups/b-1.x_y/data'),
    ])
    def test_the_container_itself_and_everything_below_it_is_accepted(self, mocker, client_id, dataset):
        fake, result = self.run(mocker, [dataset], local={dataset: _client_props(client_id)})
        assert [d.name for d in result] == [dataset]

    def test_a_trailing_slash_on_the_target_is_ignored(self, mocker):
        fake, result = self.run(mocker, ['pool/backups/c1/d'], target='pool/backups/',
                                local={'pool/backups/c1/d': _client_props('c1')})
        assert [d.name for d in result] == ['pool/backups/c1/d']

    @pytest.mark.parametrize('config', [
        'not-valid-base64!!!',
        'e30=',                                              # {} : no name / frequency
        'bnVsbA==',                                          # null
        '\ud800',                                            # a lone surrogate
        'café',                                         # non-ASCII
    ])
    def test_an_undecodable_config_is_skipped(self, mocker, caplog, config):
        ds = 'pool/backups/client1'
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            fake, result = self.run(mocker, [ds], local={ds: _client_props(config=config)})
        assert result == []
        (warning,) = self.warnings(caplog)
        assert 'does not decode' in warning

    def test_a_config_with_a_newline_or_tab_or_without_padding_is_skipped(self, mocker):
        ds = 'pool/backups/client1'
        for bad in (self.GOOD[:12] + '\n' + self.GOOD[12:], self.GOOD[:12] + '\t' + self.GOOD[12:],
                    self.GOOD.rstrip('=')[:-1]):
            fake, result = self.run(mocker, [ds], local={ds: _client_props(config=bad)})
            assert result == [], repr(bad)

    def test_each_dataset_gets_its_own_config(self, mocker):
        """The value read for a dataset is the value of THAT dataset."""
        a, b = 'pool/backups/A/data', 'pool/backups/B/data'
        cfg_a = DatasetConfig(name='pool/a').to_property()
        cfg_b = DatasetConfig(name='pool/b').to_property()
        fake, result = self.run(mocker, [a, b], local={a: _client_props('A', cfg_a), b: _client_props('B', cfg_b)})
        assert {d.name: d.config.name for d in result} == {a: 'pool/a', b: 'pool/b'}

    @pytest.mark.parametrize('failing', ['client_id', 'config', 'received'])
    def test_a_read_that_raises_skips_that_dataset_with_a_warning(self, mocker, caplog, failing):
        from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_CONFIG
        ds = 'pool/backups/client1'
        err = RuntimeError('zfs get failed')
        if failing == 'received':
            fake = FakeReceived(mocker, [ds])
            original = fake._value

            def value(d, prop, sources=None, raw=False):
                if sources == 'received':
                    raise err
                return original(d, prop, sources, raw)

            fake.value.side_effect = value
            with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
                result = self.manager().received_datasets()
        else:
            prop = PROP_CLIENT_ID if failing == 'client_id' else PROP_CONFIG
            with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
                fake, result = self.run(mocker, [ds], local={ds: _client_props()}, errors={(ds, prop): err})
        assert result == []
        (warning,) = self.warnings(caplog)
        assert 'cannot read its properties' in warning

    # -- reads are local-only -------------------------------------------------------------------
    def test_client_id_and_config_are_read_with_sources_local(self, mocker):
        from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_CONFIG
        ds = 'pool/backups/client1/data'
        fake, result = self.run(mocker, [ds], local={ds: _client_props()})
        assert len(result) == 1
        assert fake.reads_of(ds) == [(PROP_CLIENT_ID, 'local'), (PROP_CONFIG, 'local')]
        for call in fake.value.call_args_list:
            assert call.kwargs.get('sources') == 'local'

    def test_a_received_only_client_id_and_config_never_count(self, mocker):
        """What a `zfs send -p` stream can set: a RECEIVED pair is not a client dataset."""
        ds = 'pool/backups/client1/data'
        fake, result = self.run(mocker, [ds], received={ds: _client_props()})
        assert result == []

    def test_an_inherited_pair_never_counts(self, mocker):
        """A nested server dataset under another client's inherits nothing: the local read says unset."""
        parent, child = 'pool/backups/A', 'pool/backups/A/nested'
        fake, result = self.run(mocker, [parent, child], local={parent: _client_props('A')})
        assert [d.name for d in result] == [parent]

    # -- isolation ------------------------------------------------------------------------------
    def test_one_bad_dataset_does_not_empty_the_list(self, mocker, caplog):
        names = ['pool/backups/a/ok1', 'pool/backups/a/forged', 'pool/backups/a/ok2',
                 'pool/backups/a/broken', 'pool/backups/a/ok3']
        local = {n: _client_props('a') for n in names}
        local['pool/backups/a/forged'] = _client_props('b')                  # b's id in a's tree
        local['pool/backups/a/broken'] = _client_props('a', config='%%%')
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            fake, result = self.run(mocker, names, local=local)
        assert [d.name for d in result] == ['pool/backups/a/ok1', 'pool/backups/a/ok2', 'pool/backups/a/ok3']
        assert len(self.warnings(caplog)) == 2

    def test_a_read_error_on_one_dataset_leaves_the_others(self, mocker):
        names = ['pool/backups/a/x', 'pool/backups/a/y']
        local = {n: _client_props('a') for n in names}
        fake, result = self.run(mocker, names, local=local,
                                errors={(names[0], 'org.zfsbackup:config'): RuntimeError('boom')})
        assert [d.name for d in result] == [names[1]]

    # -- the listing ----------------------------------------------------------------------------
    def test_a_failed_listing_is_an_error_and_an_empty_list(self, mocker, caplog):
        fake = FakeReceived(mocker, list_error=RuntimeError('cannot open pool/backups'))
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            assert self.manager().received_datasets() == []
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1 and 'Failed to discover received datasets' in errors[0].getMessage()
        assert fake.reads == []

    def test_the_listing_asks_for_no_properties(self, mocker):
        fake, _ = self.run(mocker, ['pool/backups/c/d'], local={'pool/backups/c/d': _client_props('c')})
        (call,) = fake.list.call_args_list
        assert 'properties' not in call.kwargs
        assert call.kwargs['recursive'] is True and call.kwargs['types'] == 'filesystem'
        assert str(call.kwargs['roots']) == 'pool/backups'

    def test_the_real_listing_command_has_only_name_and_type_columns(self, mock_subprocess):
        mock_subprocess.setup(stdout=[])
        self.manager().received_datasets()
        argv = mock_subprocess.call_args_list[0][0][0][1:]
        assert argv[:2] == ['list', '-H'] and argv[argv.index('-o') + 1] == 'name,type'
        assert 'org.zfsbackup:config' not in ' '.join(argv)

    def test_disabled_or_unconfigured_remote_backup_lists_nothing(self, mocker):
        fake = FakeReceived(mocker)
        assert DatasetManager(_make_config()).received_datasets() == []
        fake.list.assert_not_called()

    # -- warn once ------------------------------------------------------------------------------
    def test_a_standing_problem_is_a_warning_once_then_debug(self, mocker, caplog):
        ds = 'pool/backups/c/d'
        FakeReceived(mocker, [ds], local={ds: _client_props('c', config='%%%')})
        manager = self.manager()
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            for _ in range(4):
                assert manager.received_datasets() == []
        assert len(self.warnings(caplog)) == 1
        assert len([m for m in self.debugs(caplog) if 'Skipping received dataset' in m]) == 3

    def test_a_changed_problem_warns_again(self, mocker, caplog):
        ds = 'pool/backups/c/d'
        fake = FakeReceived(mocker, [ds], local={ds: _client_props('c', config='%%%')})
        manager = self.manager()
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            manager.received_datasets()
            manager.received_datasets()
            fake.local[ds] = _client_props('x')                        # now outside its tree
            manager.received_datasets()
            manager.received_datasets()
        warnings = self.warnings(caplog)
        assert len(warnings) == 2 and 'does not decode' in warnings[0] and 'outside' in warnings[1]

    def test_a_recovered_dataset_warns_again_if_it_breaks_again(self, mocker, caplog):
        ds = 'pool/backups/c/d'
        fake = FakeReceived(mocker, [ds], local={ds: _client_props('c', config='%%%')})
        manager = self.manager()
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            manager.received_datasets()
            fake.local[ds] = _client_props('c')
            assert [d.name for d in manager.received_datasets()] == [ds]
            assert manager._received_skip_noted == {}
            fake.local[ds] = _client_props('c', config='%%%')
            manager.received_datasets()
        assert len(self.warnings(caplog)) == 2

    def test_names_that_disappeared_are_forgotten(self, mocker, caplog):
        ds = 'pool/backups/c/d'
        fake = FakeReceived(mocker, [ds], local={ds: _client_props('c', config='%%%')})
        manager = self.manager()
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            manager.received_datasets()
            assert list(manager._received_skip_noted) == [ds]
            fake.datasets = []
            manager.received_datasets()
            assert manager._received_skip_noted == {}
            fake.datasets = [ds]
            manager.received_datasets()
        assert len(self.warnings(caplog)) == 2

    def test_a_dataset_that_is_not_a_client_dataset_is_never_noted(self, mocker):
        FakeReceived(mocker, ['pool/backups', 'pool/backups/c'])
        manager = self.manager()
        manager.received_datasets()
        assert manager._received_skip_noted == {}

    def test_the_memory_is_per_manager(self, mocker, caplog):
        ds = 'pool/backups/c/d'
        FakeReceived(mocker, [ds], local={ds: _client_props('c', config='%%%')})
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            self.manager().received_datasets()
            self.manager().received_datasets()
        assert len(self.warnings(caplog)) == 2

    # -- _received_dataset_info ---------------------------------------------------------------------
    def test_info_for_a_valid_dataset(self, mocker):
        ds = 'pool/backups/c/d'
        FakeReceived(mocker, local={ds: _client_props('c')})
        dsi, problem = self.manager()._received_dataset_info(Filesystem(ds), self.T)
        assert problem is None and dsi.name == ds and dsi.config.recursive is False

    def test_info_for_a_non_client_dataset_is_none_none(self, mocker):
        FakeReceived(mocker)
        assert self.manager()._received_dataset_info(Filesystem('pool/backups/c'), self.T) == (None, None)

    def test_info_reports_a_failing_read_instead_of_raising(self, mocker):
        ds = 'pool/backups/c/d'
        FakeReceived(mocker, errors={(ds, 'org.zfsbackup:client_id'): RuntimeError('x')})
        dsi, problem = self.manager()._received_dataset_info(Filesystem(ds), self.T)
        assert dsi is None and 'cannot read its properties' in problem


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
    fills the `received` column, like the real command. The shape is what a
    real `zfs get -H -o name,property,value,received,source all <name>` prints:
    the `type` row first, then the properties.
    """
    # `zfs get all` opens with the `type` row (it has no `name` row: `name` is not a gettable
    # property of `all`); libzfseasy's lead rule keys on that first row.
    lines = [f'{name}\ttype\t{ptype}\t-\t-\n']
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
        orphan = _zget('pool/data', [(A + 'renamed-away', snaps[2].name, 'local')])
        mock_subprocess.setup_multi((orphan,), (orphan,))      # the pass's read, then the one before the destroy
        mock_destroy = mocker.patch('zfsbackup.backup_manager.zfs.destroy')
        manager.prune_snapshots(dsi)
        destroyed = self.destroyed(mock_destroy)
        assert snaps[2].snapshot not in destroyed
        assert destroyed == [snaps[1].snapshot, snaps[3].snapshot]

    def test_without_the_orphan_anchor_the_same_snapshot_is_pruned(self, mock_subprocess, mocker):
        manager, dsi, snaps = _pruning_manager()
        mocker.patch.object(manager, 'list_snapshots', return_value=snaps)
        dsi.snapshots = snaps
        none = _zget('pool/data', [])
        mock_subprocess.setup_multi((none,), (none,))
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
        value = mocker.patch('zfsbackup.backup_manager.zfs.get.value', side_effect=Exception('err'))
        assert manager.get_inflight(manager.datasets[0], 'offsite') is None
        value.assert_called_once()                      # the failure came from the read, not elsewhere


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
        """The per-name WARNINGs (one per name, deduplicated); not the listing's summary."""
        return [
            r.getMessage() for r in caplog.records
            if r.name == 'libzfseasy' and r.levelno == logging.WARNING
            and r.getMessage().startswith('skipping ') and 'cannot represent' in r.getMessage()
        ]

    @staticmethod
    def _summary_records(caplog):
        """`(level, text)` of the listing's one summary record ("skipped N names ... cannot represent
        ..."): WARNING when the listing also logged a per-name WARNING, DEBUG when it only met
        names it had already reported."""
        return [
            (r.levelno, r.getMessage()) for r in caplog.records
            if r.name == 'libzfseasy' and r.getMessage().startswith('skipped ')
            and 'cannot represent' in r.getMessage()
        ]

    def _manager(self):
        config = _make_config(datasets=[DatasetConfig(
            name='pool/data',
            retention_rules=[RetentionRule(timedelta(hours=1), timedelta(hours=2))],
        )])
        return DatasetManager(config)

    @staticmethod
    def _odd_row(short):
        """A row for a snapshot name libzfseasy cannot represent (leading/trailing space), with
        the listing's full column set."""
        return f'pool/data@{short}\tsnapshot\t5\t6\t0\t-\toff\t-\n'

    def _rows(self):
        """Newest first as `zfs list` sorts them; the two oddly named ones are
        what a VM actually showed (`zfs snapshot 'pool/fs@ manual'`)."""
        return {
            'newest': self._snap_name(0.2),
            'old1': self._snap_name(3),
            'old2': self._snap_name(4),
            'lines': [
                list_row(self._snap_name(4)),
                self._odd_row(' manual'),
                list_row(self._snap_name(3)),
                self._odd_row('manual '),
                list_row(self._snap_name(0.2)),
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
        # ...and one summary of the listing (libzfseasy D4): a WARNING, because names were new
        assert self._summary_records(caplog) == [
            (logging.WARNING, 'skipped 2 names libzfseasy cannot represent in zfs list output')]
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_the_warning_is_not_repeated_by_the_next_listing(self, mock_subprocess, caplog):
        rows = self._rows()
        mock_subprocess.setup_multi((rows['lines'],), (rows['lines'],))
        manager = self._manager()
        dsi = manager.datasets[0]
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            manager.list_snapshots(dsi)
            first = len(self._skip_warnings(caplog))
            assert [lv for lv, _ in self._summary_records(caplog)] == [logging.WARNING]
            manager.list_snapshots(dsi)
        assert first == 2 and len(self._skip_warnings(caplog)) == 2
        # the second listing's summary is DEBUG: the names persist, nothing new to warn about
        assert [lv for lv, _ in self._summary_records(caplog)] == [logging.WARNING, logging.DEBUG]

    def test_a_listing_of_only_unrepresentable_rows_is_empty_not_an_error(
        self, mock_subprocess, caplog
    ):
        mock_subprocess.setup(stdout=[self._odd_row(' manual')])
        manager = self._manager()
        with caplog.at_level(logging.WARNING):
            assert manager.list_snapshots(manager.datasets[0]) == []
        assert len(self._skip_warnings(caplog)) == 1
        assert self._summary_records(caplog) == [
            (logging.WARNING, 'skipped 1 name libzfseasy cannot represent in zfs list output')]
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
        assert len(self._summary_records(caplog)) == 1
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_received_datasets_skips_an_unrepresentable_child(self, mock_subprocess, mocker, caplog):
        good = DatasetConfig(name='pool/backups/cid/data').to_property()
        other = DatasetConfig(name='pool/backups/cid/other').to_property()
        mock_subprocess.setup(stdout=[
            'pool/backups\tfilesystem\n',
            'pool/backups/cid\tfilesystem\n',
            'pool/backups/cid/data\tfilesystem\n',
            'pool/backups/cid/.system\tfilesystem\n',
            'pool/backups/cid/other\tfilesystem\n',
        ])
        local = {
            'pool/backups/cid/data': {'org.zfsbackup:client_id': 'cid', 'org.zfsbackup:config': good},
            'pool/backups/cid/other': {'org.zfsbackup:client_id': 'cid', 'org.zfsbackup:config': other},
        }
        value = mocker.patch(
            'zfsbackup.backup_manager.zfs.get.value',
            side_effect=lambda ds, prop, sources=None, raw=False: local.get(ds.name, {}).get(prop),
        )
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
        assert self._summary_records(caplog) == [
            (logging.WARNING, 'skipped 1 name libzfseasy cannot represent in zfs list output')]
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
        # the unrepresentable row never reached a property read
        assert all(c.args[0].name != 'pool/backups/cid/.system' for c in value.call_args_list)


# ===========================================================================
# S-1 (plan B9, B11; table T-S1): ordering by createtxg, the one listing, the
# future-dated newest snapshot and the backwards-clock name collision.
# ===========================================================================

from datetime import datetime as _real_datetime  # noqa: E402

from _snapshot_support import FrozenClock, frozen_clock  # noqa: E402,F401  (fixture)
from zfsbackup.backup_manager import (  # noqa: E402
    SNAPSHOT_LIST_PROPERTIES, snapshot_newer, snapshot_order_key,
)

NTP_MESSAGE = 'system clock is behind the newest snapshot; check NTP'


def _records(caplog, text, level=None):
    return [r for r in caplog.records
            if text in r.getMessage() and (level is None or r.levelname == level)]


def _hourly_manager(**kwargs):
    config = _make_config(
        datasets=[DatasetConfig(name='pool/data', frequency=timedelta(hours=1),
                                retention_rules=kwargs.pop('rules', []))],
        **kwargs,
    )
    return DatasetManager(config)


class TestOneListingCarriesEverything:
    """S1-a."""

    COLUMNS = 'name,type,createtxg,guid,userrefs,clones,encryption,keystatus'

    def test_the_constant_is_the_six_columns_in_order(self):
        assert SNAPSHOT_LIST_PROPERTIES == [
            'createtxg', 'guid', 'userrefs', 'clones', 'encryption', 'keystatus']

    def test_exactly_one_zfs_list_with_the_six_columns(self, mock_subprocess):
        mock_subprocess.setup(stdout=[list_row('autosnap_20240101120000', txg=10)])
        manager = _hourly_manager()
        (snap,) = manager.list_snapshots(manager.datasets[0])
        assert mock_subprocess.call_count == 1
        argv = list(mock_subprocess.call_args_list[0].args[0])
        assert argv[1] == 'list'
        assert argv[argv.index('-o') + 1] == self.COLUMNS
        assert argv[argv.index('-t') + 1] == 'snapshot'
        assert argv[-1] == 'pool/data'
        assert snap.createtxg == 10

    def test_every_column_reaches_the_snapshot_info(self, mock_subprocess):
        mock_subprocess.setup(stdout=[
            'pool/data@autosnap_20240101120000\tsnapshot\t77\t123456\t3\tpool/c1,pool/c2\t'
            'aes-256-gcm\tavailable\n'
        ])
        manager = _hourly_manager()
        (snap,) = manager.list_snapshots(manager.datasets[0])
        assert (snap.createtxg, snap.guid, snap.userrefs) == (77, 123456, 3)
        assert snap.clones == ('pool/c1', 'pool/c2')
        assert snap.encrypted is True and snap.key_loaded is True

    def test_a_recursive_listing_passes_the_flag_and_the_same_columns(self, mocker):
        lst = mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        manager = _hourly_manager()
        manager.list_snapshots(manager.datasets[0], recursive=True)
        lst.assert_called_once()
        assert lst.call_args.kwargs['properties'] == SNAPSHOT_LIST_PROPERTIES
        assert lst.call_args.kwargs['recursive'] is True

    def test_the_list_passed_to_zfs_is_a_copy(self, mocker):
        """A caller mutating what it passed must not change the shared constant."""
        lst = mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        manager = _hourly_manager()
        manager.list_snapshots(manager.datasets[0])
        assert lst.call_args.kwargs['properties'] is not SNAPSHOT_LIST_PROPERTIES


class TestListingOrder:
    """S1-a: newest first by createtxg, then name timestamp, then name."""

    def listing(self, mocker, *snaps):
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=list(snaps))
        manager = _hourly_manager()
        dsi = manager.datasets[0]
        return manager.list_snapshots(dsi), dsi

    def test_txg_order_differs_from_name_order(self, mocker):
        a = listed_snapshot('autosnap_20240101120000', txg=30)
        b = listed_snapshot('autosnap_20240103120000', txg=10)       # latest NAME, oldest txg
        c = listed_snapshot('autosnap_20240102120000', txg=20)
        result, dsi = self.listing(mocker, a, b, c)
        assert [s.name for s in result] == [
            'autosnap_20240101120000', 'autosnap_20240102120000', 'autosnap_20240103120000']
        assert dsi.snapshots == result

    def test_a_hand_made_future_name_with_the_lowest_txg_is_the_oldest(self, mocker):
        future = listed_snapshot('autosnap_20990101000000', txg=1)
        recent = listed_snapshot('autosnap_20200101000000', txg=900)  # created later, dated earlier
        result, _ = self.listing(mocker, future, recent)
        assert [s.name for s in result] == ['autosnap_20200101000000', 'autosnap_20990101000000']

    def test_listing_order_from_zfs_does_not_matter(self, mocker):
        snaps = [listed_snapshot(f'autosnap_2024010{i}120000', txg=t)
                 for i, t in ((1, 3), (2, 1), (3, 2))]
        forward, _ = self.listing(mocker, *snaps)
        backward, _ = self.listing(mocker, *reversed(snaps))
        assert [s.name for s in forward] == [s.name for s in backward]

    def test_equal_txg_breaks_the_tie_by_name_timestamp(self, mocker):
        """`zfs snapshot a@x a@y` shares one txg."""
        older = listed_snapshot('autosnap_20240101120000', txg=50)
        newer = listed_snapshot('autosnap_20240102120000', txg=50)
        lonely = listed_snapshot('autosnap_20240103120000', txg=49)
        result, _ = self.listing(mocker, newer, lonely, older)
        assert [s.name for s in result] == [
            'autosnap_20240102120000', 'autosnap_20240101120000', 'autosnap_20240103120000']

    def test_equal_txg_is_deterministic_in_either_input_order(self, mocker):
        x = listed_snapshot('autosnap_20240101120000', txg=5)
        y = listed_snapshot('autosnap_20240102120000', txg=5)
        one, _ = self.listing(mocker, x, y)
        two, _ = self.listing(mocker, y, x)
        assert [s.name for s in one] == [s.name for s in two] == [
            'autosnap_20240102120000', 'autosnap_20240101120000']

    def test_unmanaged_snapshots_are_not_listed_whatever_their_txg(self, mocker):
        managed = listed_snapshot('autosnap_20240101120000', txg=1)
        manual = listed_snapshot('manual', txg=999)
        suffixed = listed_snapshot('autosnap_20240102120000_x', txg=998)
        result, _ = self.listing(mocker, managed, manual, suffixed)
        assert [s.name for s in result] == ['autosnap_20240101120000']

    def test_the_order_key_agrees_with_the_listing(self, mocker):
        snaps = [listed_snapshot(f'autosnap_2024010{i}120000', txg=t)
                 for i, t in ((1, 2), (2, 9), (3, 4))]
        result, _ = self.listing(mocker, *snaps)
        assert result == sorted(result, key=snapshot_order_key, reverse=True)


class TestSnapshotWithoutATxg:
    """S1-a: a managed snapshot with no createtxg is dropped, with one WARNING per newly seen name."""

    def manager_and_dsi(self):
        manager = _hourly_manager()
        return manager, manager.datasets[0]

    def listing(self, mocker, *snaps):
        return mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=list(snaps))

    def test_it_is_dropped_and_warned_about_once(self, mocker, caplog):
        good = listed_snapshot('autosnap_20240101120000', txg=5)
        bad = listed_snapshot('autosnap_20240102120000', txg=NO_TXG)
        self.listing(mocker, good, bad)
        manager, dsi = self.manager_and_dsi()
        with caplog.at_level(logging.WARNING):
            result = manager.list_snapshots(dsi)
        assert [s.name for s in result] == ['autosnap_20240101120000']
        (rec,) = _records(caplog, 'have no createtxg', 'WARNING')
        assert 'pool/data@autosnap_20240102120000' in rec.getMessage()
        assert 'pool/data@autosnap_20240101120000' not in rec.getMessage()

    def test_its_name_is_still_known_to_the_collision_check(self, mocker):
        good = listed_snapshot('autosnap_20240101120000', txg=5)
        bad = listed_snapshot('autosnap_20240102120000', txg=NO_TXG)
        self.listing(mocker, good, bad)
        manager, dsi = self.manager_and_dsi()
        manager.list_snapshots(dsi)
        assert dsi.listed_names == {'autosnap_20240101120000', 'autosnap_20240102120000'}

    def test_the_next_listing_does_not_repeat_the_warning(self, mocker, caplog):
        bad = listed_snapshot('autosnap_20240102120000', txg=NO_TXG)
        self.listing(mocker, bad)
        manager, dsi = self.manager_and_dsi()
        with caplog.at_level(logging.WARNING):
            for _ in range(4):
                manager.list_snapshots(dsi)
        assert len(_records(caplog, 'have no createtxg')) == 1

    def test_a_new_name_warns_again_naming_only_the_new_one(self, mocker, caplog):
        bad1 = listed_snapshot('autosnap_20240102120000', txg=NO_TXG)
        bad2 = listed_snapshot('autosnap_20240103120000', txg=NO_TXG)
        lst = self.listing(mocker, bad1)
        manager, dsi = self.manager_and_dsi()
        with caplog.at_level(logging.WARNING):
            manager.list_snapshots(dsi)
            lst.return_value = [bad1, bad2]
            manager.list_snapshots(dsi)
        first, second = _records(caplog, 'have no createtxg')
        assert 'autosnap_20240103120000' in second.getMessage()
        assert 'autosnap_20240102120000' not in second.getMessage()

    def test_it_is_tracked_per_dataset(self, mocker, caplog):
        bad = listed_snapshot('autosnap_20240102120000', txg=NO_TXG)
        self.listing(mocker, bad)
        config = _make_config(datasets=[DatasetConfig(name='pool/a'), DatasetConfig(name='pool/b')])
        manager = DatasetManager(config)
        with caplog.at_level(logging.WARNING):
            for dsi in manager.datasets:
                manager.list_snapshots(dsi)
        assert len(_records(caplog, 'have no createtxg')) == 2

    def test_a_dropped_future_dated_snapshot_is_never_taken_for_the_newest(self, mocker, frozen_clock, caplog):
        recent = listed_snapshot(frozen_clock.stamp(-timedelta(minutes=30)), txg=5)
        future = listed_snapshot(frozen_clock.stamp(timedelta(days=3)), txg=NO_TXG)
        self.listing(mocker, recent, future)
        manager, dsi = self.manager_and_dsi()
        with caplog.at_level(logging.ERROR):
            assert manager.needs_snapshot(dsi) is False         # 30 min < 1 h: not due
        assert not _records(caplog, NTP_MESSAGE)

    def test_it_can_never_be_pruned(self, mocker, frozen_clock):
        old = listed_snapshot(frozen_clock.stamp(-timedelta(days=30)), txg=NO_TXG)
        newest = listed_snapshot(frozen_clock.stamp(-timedelta(minutes=5)), txg=9)
        self.listing(mocker, old, newest)
        manager = _hourly_manager(rules=[RetentionRule(timedelta(hours=1), timedelta(hours=2))])
        dsi = manager.datasets[0]
        manager.list_snapshots(dsi)
        assert manager.needs_prunning(dsi) == []                # `old` is not even in the list


class TestRetentionOrderIsUnchanged:
    """B9: slots and ages come from the name timestamp and the OLDEST snapshot of a slot is kept;
    only 'always keep the newest' (and any newest/older comparison) follows createtxg."""

    RULE = RetentionRule(timedelta(hours=1), timedelta(days=1))

    def prune_names(self, manager, dsi, snaps):
        dsi.snapshots = snaps
        return sorted(s.name for s in manager.needs_prunning(dsi))

    def test_the_oldest_snapshot_of_a_slot_is_kept(self, frozen_clock):
        manager = _hourly_manager(rules=[self.RULE])
        dsi = manager.datasets[0]
        # three snapshots in the 08:00 slot, then the newest in the 10:00 slot
        a, b, c = (listed_info(f'autosnap_202610220{m}', txg=t)
                   for m, t in (('81000', 1), ('84000', 2), ('85500', 3)))
        newest = listed_info('autosnap_20261022102000', txg=4)
        pruned = self.prune_names(manager, dsi, [newest, c, b, a])
        assert pruned == ['autosnap_20261022084000', 'autosnap_20261022085500']

    def test_the_walk_does_not_depend_on_the_list_order(self, frozen_clock):
        manager = _hourly_manager(rules=[self.RULE])
        dsi = manager.datasets[0]
        snaps = [listed_info(n, txg=t) for n, t in (
            ('autosnap_20261022081000', 1), ('autosnap_20261022084000', 2),
            ('autosnap_20261022085500', 3), ('autosnap_20261022102000', 4))]
        expected = ['autosnap_20261022084000', 'autosnap_20261022085500']
        assert self.prune_names(manager, dsi, list(snaps)) == expected
        assert self.prune_names(manager, dsi, list(reversed(snaps))) == expected

    def test_keep_newest_follows_txg_not_the_name(self, frozen_clock):
        """A hand-made `prefix_20990101000000` has the lowest txg: it is NOT the newest. The
        newest by txg (`c`) is kept even though it shares a slot with an older `b`."""
        manager = _hourly_manager(rules=[self.RULE])
        dsi = manager.datasets[0]
        hand_made = listed_info('autosnap_20990101000000', txg=1)
        b = listed_info('autosnap_20261022080500', txg=5)
        c = listed_info('autosnap_20261022082000', txg=9)            # newest by txg; same slot as b
        for order in ([hand_made, b, c], [c, b, hand_made], [b, hand_made, c]):
            assert self.prune_names(manager, dsi, order) == []

    def test_without_the_txg_rule_c_would_have_been_pruned(self, frozen_clock):
        """Control for the test above: take `c` out of newest-by-txg (txg below `b`) and it is the
        slot's second snapshot, so it is pruned."""
        manager = _hourly_manager(rules=[self.RULE])
        dsi = manager.datasets[0]
        hand_made = listed_info('autosnap_20990101000000', txg=100)
        b = listed_info('autosnap_20261022080500', txg=5)
        c = listed_info('autosnap_20261022082000', txg=4)
        assert self.prune_names(manager, dsi, [hand_made, b, c]) == ['autosnap_20261022082000']

    def test_a_later_created_earlier_dated_snapshot_is_the_newest_and_kept(self, frozen_clock):
        manager = _hourly_manager(rules=[self.RULE])
        dsi = manager.datasets[0]
        regular = [listed_info(f'autosnap_2026102208{m}00', txg=t)
                   for m, t in (('10', 1), ('20', 2), ('30', 3))]
        late = listed_info('autosnap_20200101000000', txg=500)       # created last, dated 2020
        # by name `late` is ancient (older than the rule's 1 d horizon): never claims a slot,
        # but it is the newest by txg, so it survives
        pruned = self.prune_names(manager, dsi, regular + [late])
        assert 'autosnap_20200101000000' not in pruned
        assert pruned == ['autosnap_20261022082000', 'autosnap_20261022083000']

    def test_ages_come_from_the_name_timestamp(self, frozen_clock):
        """Past the keep_for horizon BY NAME means prunable, whatever the txg says."""
        manager = _hourly_manager(rules=[RetentionRule(timedelta(hours=1), timedelta(hours=2))])
        dsi = manager.datasets[0]
        old_name_high_txg = listed_info('autosnap_20261020000000', txg=800)
        newest = listed_info('autosnap_20261022101000', txg=900)
        assert self.prune_names(manager, dsi, [newest, old_name_high_txg]) == ['autosnap_20261020000000']

    def test_a_snapshot_with_no_txg_in_a_hand_built_list_sorts_oldest(self, frozen_clock):
        manager = _hourly_manager(rules=[self.RULE])
        dsi = manager.datasets[0]
        bare = listed_info('autosnap_20261022101000', txg=NO_TXG)
        stamped = listed_info('autosnap_20261022100500', txg=3)
        # the txg-less one is never "the newest" although its name is later: the stamped one is kept
        # as such, and the bare one (second in the same hour slot) is pruned
        assert self.prune_names(manager, dsi, [bare, stamped]) == ['autosnap_20261022101000']


class TestClockTolerance:
    """B11: `max(5 minutes, check_interval)`."""

    def test_the_default_is_five_minutes(self):
        manager = _hourly_manager()
        assert manager.config.check_interval == timedelta(minutes=5)
        assert manager.clock_tolerance() == timedelta(minutes=5)

    def test_a_longer_check_interval_widens_it(self):
        manager = _hourly_manager(check_interval=timedelta(minutes=30))
        assert manager.clock_tolerance() == timedelta(minutes=30)

    def test_a_shorter_check_interval_does_not_narrow_it(self):
        manager = _hourly_manager(check_interval=timedelta(seconds=10))
        assert manager.clock_tolerance() == timedelta(minutes=5)

    @pytest.mark.parametrize('check, ahead, behind', [
        (timedelta(minutes=5), timedelta(minutes=5), False),             # exactly the tolerance
        (timedelta(minutes=5), timedelta(minutes=5, seconds=1), True),
        (timedelta(minutes=5), timedelta(seconds=-1), False),            # in the past
        (timedelta(minutes=5), timedelta(0), False),
        (timedelta(minutes=30), timedelta(minutes=30), False),
        (timedelta(minutes=30), timedelta(minutes=30, seconds=1), True),
        (timedelta(minutes=30), timedelta(minutes=20), False),
        (timedelta(seconds=10), timedelta(minutes=5, seconds=1), True),
    ])
    def test_boundary(self, frozen_clock, caplog, check, ahead, behind):
        manager = _hourly_manager(check_interval=check)
        dsi = manager.datasets[0]
        snap = listed_info(frozen_clock.stamp(ahead))
        with caplog.at_level(logging.ERROR):
            assert manager.clock_behind(dsi, snap) is behind
        assert bool(_records(caplog, NTP_MESSAGE, 'ERROR')) is behind

    def test_a_snapshot_with_no_timestamp_is_never_behind(self):
        manager = _hourly_manager()
        stub = MagicMock()
        stub.timestamp = None
        assert manager.clock_behind(manager.datasets[0], stub) is False


class TestFutureDatedNewest:
    """S1-c / B11: a newest snapshot dated beyond the tolerance means the clock is wrong."""

    def setup_listing(self, mocker, frozen_clock, *snaps):
        self.lst = mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=list(snaps))
        manager = _hourly_manager()
        return manager, manager.datasets[0]

    def test_a_normal_newest_is_due_by_age_only(self, mocker, frozen_clock, caplog):
        old = listed_snapshot(frozen_clock.stamp(-timedelta(hours=3)), txg=1)
        recent = listed_snapshot(frozen_clock.stamp(-timedelta(minutes=20)), txg=2)
        manager, dsi = self.setup_listing(mocker, frozen_clock, old, recent)
        with caplog.at_level(logging.DEBUG):
            assert manager.needs_snapshot(dsi) is False
            frozen_clock.advance(timedelta(minutes=45))               # now 65 min old
            assert manager.needs_snapshot(dsi) is True
        assert not _records(caplog, NTP_MESSAGE)

    def test_a_future_dated_newest_is_due_and_the_error_is_logged_once(self, mocker, frozen_clock, caplog):
        future = listed_snapshot(frozen_clock.stamp(timedelta(hours=9)), txg=9)
        manager, dsi = self.setup_listing(mocker, frozen_clock, future)
        with caplog.at_level(logging.DEBUG):
            for _ in range(5):
                assert manager.needs_snapshot(dsi) is True
        errors = _records(caplog, NTP_MESSAGE, 'ERROR')
        assert len(errors) == 1
        assert 'pool/data' in errors[0].getMessage() and future.name.split('@')[-1] in errors[0].getMessage()

    def test_after_the_new_snapshot_it_is_not_due_and_stays_quiet(self, mocker, frozen_clock, caplog):
        future = listed_snapshot(frozen_clock.stamp(timedelta(hours=9)), txg=9)
        manager, dsi = self.setup_listing(mocker, frozen_clock, future)
        made = []

        def take(ds, name, **kw):
            made.append(name)
            return ZFSSnapshot(Filesystem('pool/data'), name)

        mocker.patch('zfsbackup.backup_manager.zfs.snapshot', side_effect=take)
        with caplog.at_level(logging.WARNING):
            assert manager.needs_snapshot(dsi) is True
            # the pool now has the future one and the new one, which is the newest by txg
            self.lst.side_effect = lambda **kw: [future, listed_snapshot(made[0], txg=10)]
            created = manager.create_snapshot(dsi)
            assert created is not None and created.createtxg == 10
            assert dsi.snapshots[0].name == made[0]
            for _ in range(3):
                assert manager.needs_snapshot(dsi) is False
        assert made == ['autosnap_20261022100000']                 # the aligned name, not the future one
        assert len(_records(caplog, NTP_MESSAGE)) == 1               # only the first, before the snapshot
        assert not _records(caplog, 'already exists')

    def test_it_is_due_again_when_the_new_snapshot_gets_old(self, mocker, frozen_clock):
        future = listed_snapshot(frozen_clock.stamp(timedelta(hours=9)), txg=9)
        fresh = listed_snapshot(frozen_clock.stamp(-timedelta(minutes=30)), txg=10)
        manager, dsi = self.setup_listing(mocker, frozen_clock, future, fresh)
        assert manager.needs_snapshot(dsi) is False
        frozen_clock.advance(timedelta(minutes=31))
        assert manager.needs_snapshot(dsi) is True

    def test_a_past_dated_name_with_a_newer_txg_is_due_once(self, mocker, frozen_clock, caplog):
        """The clock jumped forward, a snapshot was made, then it was corrected back: the newest by
        txg is dated years ago. Due; one snapshot fixes it."""
        ancient_new = listed_snapshot('autosnap_20200101000000', txg=50)
        future_old = listed_snapshot(frozen_clock.stamp(timedelta(hours=1)), txg=20)
        manager, dsi = self.setup_listing(mocker, frozen_clock, ancient_new, future_old)
        mocker.patch('zfsbackup.backup_manager.zfs.snapshot',
                     side_effect=lambda ds, name, **kw: ZFSSnapshot(Filesystem('pool/data'), name))
        with caplog.at_level(logging.WARNING):
            assert manager.needs_snapshot(dsi) is True             # age of the txg-newest: years
            made = manager.create_snapshot(dsi)                     # re-lists the same mock: no new one
        assert made is not None
        assert not _records(caplog, NTP_MESSAGE)                    # it is old, not future-dated
        self.lst.return_value = [ancient_new, future_old, listed_snapshot(
            frozen_clock.stamp(), txg=60)]
        assert manager.needs_snapshot(dsi) is False

    def test_the_error_is_logged_again_for_a_different_future_snapshot(self, mocker, frozen_clock, caplog):
        first = listed_snapshot(frozen_clock.stamp(timedelta(hours=9)), txg=9)
        second = listed_snapshot(frozen_clock.stamp(timedelta(hours=12)), txg=10)
        manager, dsi = self.setup_listing(mocker, frozen_clock, first)
        with caplog.at_level(logging.ERROR):
            manager.needs_snapshot(dsi)
            self.lst.return_value = [first, second]
            manager.needs_snapshot(dsi)
            manager.needs_snapshot(dsi)
        assert len(_records(caplog, NTP_MESSAGE)) == 2

    def test_the_error_is_logged_again_after_the_condition_cleared(self, mocker, frozen_clock, caplog):
        future = listed_snapshot(frozen_clock.stamp(timedelta(hours=9)), txg=9)
        normal = listed_snapshot(frozen_clock.stamp(-timedelta(minutes=10)), txg=10)
        manager, dsi = self.setup_listing(mocker, frozen_clock, future)
        with caplog.at_level(logging.ERROR):
            manager.needs_snapshot(dsi)                              # error 1
            self.lst.return_value = [future, normal]
            manager.needs_snapshot(dsi)                              # normal newest: clears the memory
            self.lst.return_value = [future]
            manager.needs_snapshot(dsi)                              # error 2: it is a new episode
        assert len(_records(caplog, NTP_MESSAGE)) == 2

    def test_the_error_is_tracked_per_dataset(self, mocker, frozen_clock, caplog):
        future = listed_snapshot(frozen_clock.stamp(timedelta(hours=9)), txg=9)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[future])
        config = _make_config(datasets=[
            DatasetConfig(name='pool/a', frequency=timedelta(hours=1)),
            DatasetConfig(name='pool/b', frequency=timedelta(hours=1)),
        ])
        manager = DatasetManager(config)
        a, b = manager.datasets
        with caplog.at_level(logging.ERROR):
            manager.needs_snapshot(a)
            manager.needs_snapshot(b)
            manager.needs_snapshot(a)
        assert len(_records(caplog, NTP_MESSAGE)) == 2

    def test_within_the_tolerance_a_future_name_is_not_an_error(self, mocker, frozen_clock, caplog):
        """Jitter (an NTP step of a few minutes): not due, no error."""
        near = listed_snapshot(frozen_clock.stamp(timedelta(minutes=4)), txg=9)
        manager, dsi = self.setup_listing(mocker, frozen_clock, near)
        with caplog.at_level(logging.DEBUG):
            assert manager.needs_snapshot(dsi) is False
        assert not _records(caplog, NTP_MESSAGE)

    def test_just_past_the_tolerance_it_is_due(self, mocker, frozen_clock, caplog):
        beyond = listed_snapshot(frozen_clock.stamp(timedelta(minutes=5, seconds=1)), txg=9)
        manager, dsi = self.setup_listing(mocker, frozen_clock, beyond)
        with caplog.at_level(logging.DEBUG):
            assert manager.needs_snapshot(dsi) is True
        assert len(_records(caplog, NTP_MESSAGE, 'ERROR')) == 1

    def test_the_check_interval_moves_the_boundary(self, mocker, frozen_clock):
        twenty = listed_snapshot(frozen_clock.stamp(timedelta(minutes=20)), txg=9)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[twenty])
        short = _hourly_manager()
        long = _hourly_manager(check_interval=timedelta(minutes=30))
        assert short.needs_snapshot(short.datasets[0]) is True       # beyond 5 min
        assert long.needs_snapshot(long.datasets[0]) is False         # within 30 min

    def test_the_newest_is_the_newest_by_txg(self, mocker, frozen_clock, caplog):
        """A future-dated snapshot that is OLDER by txg than a normal one is not 'the newest'."""
        future_old_txg = listed_snapshot(frozen_clock.stamp(timedelta(days=2)), txg=1)
        normal = listed_snapshot(frozen_clock.stamp(-timedelta(minutes=10)), txg=2)
        manager, dsi = self.setup_listing(mocker, frozen_clock, future_old_txg, normal)
        with caplog.at_level(logging.ERROR):
            assert manager.needs_snapshot(dsi) is False
        assert not _records(caplog, NTP_MESSAGE)


class TestGeneratedNameCollision:
    """S1-c: a clock that went backwards regenerates a name that exists."""

    def pool(self, mocker, frozen_clock):
        """The pool: today's 10:00 snapshot exists (txg 5) and the newest by txg is a future-dated
        one (the clock was ahead for a while, then was put back)."""
        existing = listed_snapshot('autosnap_20261022100000', txg=5)
        future = listed_snapshot(frozen_clock.stamp(timedelta(hours=9)), txg=9)
        lst = mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[existing, future])
        snapshot = mocker.patch('zfsbackup.backup_manager.zfs.snapshot')
        manager = _hourly_manager()
        return manager, manager.datasets[0], lst, snapshot

    def test_it_is_not_due_and_warns_once_per_name(self, mocker, frozen_clock, caplog):
        manager, dsi, _, snapshot = self.pool(mocker, frozen_clock)
        with caplog.at_level(logging.DEBUG):
            for _ in range(5):
                assert manager.needs_snapshot(dsi) is False
        (rec,) = _records(caplog, 'already exists', 'WARNING')
        assert 'autosnap_20261022100000' in rec.getMessage() and 'pool/data' in rec.getMessage()
        snapshot.assert_not_called()
        # the same fact repeats quietly at DEBUG
        assert len(_records(caplog, 'already exists', 'DEBUG')) == 4

    def test_the_future_dated_error_is_still_logged_once(self, mocker, frozen_clock, caplog):
        manager, dsi, _, _ = self.pool(mocker, frozen_clock)
        with caplog.at_level(logging.ERROR):
            for _ in range(3):
                manager.needs_snapshot(dsi)
        assert len(_records(caplog, NTP_MESSAGE)) == 1

    def test_the_worker_makes_no_zfs_snapshot_call_and_does_not_loop(self, mocker, frozen_clock, caplog):
        from zfsbackup.workers import SnapshotWorker
        manager, dsi, lst, snapshot = self.pool(mocker, frozen_clock)
        worker = SnapshotWorker.__new__(SnapshotWorker)
        with caplog.at_level(logging.INFO):
            for _ in range(10):
                worker._process_dataset(manager, dsi)
        snapshot.assert_not_called()
        assert lst.call_count == 10                       # one listing per cycle and nothing else
        assert not _records(caplog, 'needs snapshot')

    def test_a_new_hour_generates_a_new_name_and_is_due_again(self, mocker, frozen_clock, caplog):
        manager, dsi, _, _ = self.pool(mocker, frozen_clock)
        with caplog.at_level(logging.WARNING):
            assert manager.needs_snapshot(dsi) is False
            frozen_clock.advance(timedelta(hours=1))                 # 11:30 -> name ..110000
            assert manager.needs_snapshot(dsi) is True
        assert len(_records(caplog, 'already exists')) == 1

    def test_each_taken_name_warns_once(self, mocker, frozen_clock, caplog):
        manager, dsi, lst, _ = self.pool(mocker, frozen_clock)
        lst.return_value = lst.return_value + [listed_snapshot('autosnap_20261022110000', txg=6)]
        with caplog.at_level(logging.WARNING):
            manager.needs_snapshot(dsi)
            frozen_clock.advance(timedelta(hours=1))
            manager.needs_snapshot(dsi)
            manager.needs_snapshot(dsi)
        assert len(_records(caplog, 'already exists', 'WARNING')) == 2

    def test_a_taken_name_is_not_a_collision_when_not_due(self, mocker, frozen_clock, caplog):
        recent = listed_snapshot(frozen_clock.stamp(-timedelta(minutes=10)), txg=9)   # 10:20 (not aligned)
        existing = listed_snapshot('autosnap_20261022100000', txg=5)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[recent, existing])
        manager = _hourly_manager()
        with caplog.at_level(logging.DEBUG):
            assert manager.needs_snapshot(manager.datasets[0]) is False
        assert not _records(caplog, 'already exists')

    def test_a_name_listed_without_a_txg_still_counts_as_taken(self, mocker, frozen_clock, caplog):
        """The dropped snapshot is invisible to ordering but its NAME is occupied."""
        taken = listed_snapshot('autosnap_20261022100000', txg=NO_TXG)
        old = listed_snapshot(frozen_clock.stamp(-timedelta(hours=5)), txg=1)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[taken, old])
        manager = _hourly_manager()
        with caplog.at_level(logging.WARNING):
            assert manager.needs_snapshot(manager.datasets[0]) is False
        assert _records(caplog, 'already exists', 'WARNING')

    def test_create_snapshot_itself_does_not_recheck_the_name(self, mocker, frozen_clock):
        """The guard is in `needs_snapshot`; a direct `create_snapshot` reaches `zfs snapshot`,
        which fails for an existing name and is reported, not raised."""
        manager, dsi, _, snapshot = self.pool(mocker, frozen_clock)
        snapshot.side_effect = Exception("cannot create snapshot 'pool/data@autosnap_20261022100000': dataset already exists")
        assert manager.create_snapshot(dsi) is None
        snapshot.assert_called_once()


class TestCreateSnapshotRelists:
    """S1-a: `create_snapshot` re-lists after `zfs snapshot`; the bare object has no createtxg."""

    def setup(self, mocker, existing=()):
        made = []

        def take(ds, name, **kw):
            made.append(name)
            return ZFSSnapshot(Filesystem('pool/data'), name)

        snapshot = mocker.patch('zfsbackup.backup_manager.zfs.snapshot', side_effect=take)
        lst = mocker.patch('zfsbackup.backup_manager.zfs.list')
        lst.side_effect = lambda **kw: list(existing) + [listed_snapshot(made[0], txg=100)]
        manager = _hourly_manager()
        return manager, manager.datasets[0], made, snapshot, lst

    def test_the_listed_object_is_returned_and_is_first(self, mocker, frozen_clock):
        old = listed_snapshot(frozen_clock.stamp(-timedelta(hours=2)), txg=3)
        manager, dsi, made, snapshot, lst = self.setup(mocker, [old])
        result = manager.create_snapshot(dsi)
        assert result.name == made[0] and result.createtxg == 100
        assert dsi.snapshots[0] is result
        assert [s.name for s in dsi.snapshots] == [made[0], old.short]
        lst.assert_called_once()
        assert lst.call_args.kwargs['properties'] == SNAPSHOT_LIST_PROPERTIES

    def test_the_new_snapshot_is_the_newest_by_txg_even_if_dated_earlier(self, mocker, frozen_clock):
        """A snapshot named in the future exists; the new aligned one is still the newest."""
        future = listed_snapshot(frozen_clock.stamp(timedelta(days=1)), txg=3)
        manager, dsi, made, _, _ = self.setup(mocker, [future])
        result = manager.create_snapshot(dsi)
        assert dsi.snapshots[0] is result and dsi.snapshots[1].name == future.short

    def test_one_snapshot_call_and_one_listing(self, mocker, frozen_clock):
        manager, dsi, made, snapshot, lst = self.setup(mocker)
        manager.create_snapshot(dsi)
        snapshot.assert_called_once()
        lst.assert_called_once()

    def test_dry_run_neither_snapshots_nor_lists(self, mocker, frozen_clock):
        config = _make_config(datasets=[DatasetConfig(name='pool/data')], dry_run=True)
        manager = DatasetManager(config)
        snapshot = mocker.patch('zfsbackup.backup_manager.zfs.snapshot')
        lst = mocker.patch('zfsbackup.backup_manager.zfs.list')
        assert manager.create_snapshot(manager.datasets[0]) is None
        snapshot.assert_not_called()
        lst.assert_not_called()

    def test_a_failed_snapshot_does_not_list(self, mocker):
        manager = _hourly_manager()
        mocker.patch('zfsbackup.backup_manager.zfs.snapshot', side_effect=Exception('boom'))
        lst = mocker.patch('zfsbackup.backup_manager.zfs.list')
        assert manager.create_snapshot(manager.datasets[0]) is None
        lst.assert_not_called()

    def test_a_failed_relist_leaves_the_snapshots_unchanged(self, mocker, frozen_clock, caplog):
        old = listed_info(frozen_clock.stamp(-timedelta(hours=2)), txg=3)
        manager, dsi, made, _, lst = self.setup(mocker)
        dsi.snapshots = [old]
        names_before = list(dsi.snapshots)
        lst.side_effect = Exception('cannot open pool/data')
        with caplog.at_level(logging.INFO):
            result = manager.create_snapshot(dsi)
        assert dsi.snapshots == names_before and dsi.snapshots[0] is old
        assert result is not None and result.name == made[0]         # the snapshot exists
        assert result.createtxg is None                              # the bare object
        assert _records(caplog, 'could not re-list', 'WARNING')
        assert _records(caplog, 'Successfully created snapshot')
        assert not _records(caplog, 'Failed to create snapshot')

    def test_an_empty_relist_returns_the_bare_object(self, mocker, frozen_clock):
        manager, dsi, made, _, lst = self.setup(mocker)
        lst.side_effect = lambda **kw: []
        result = manager.create_snapshot(dsi)
        assert result is not None and result.name == made[0]
        assert dsi.snapshots == []

    def test_the_next_cycle_lists_again(self, mocker, frozen_clock):
        manager, dsi, made, _, lst = self.setup(mocker)
        lst.side_effect = Exception('transient')
        manager.create_snapshot(dsi)
        lst.side_effect = lambda **kw: [listed_snapshot(made[0], txg=100)]
        assert [s.name for s in manager.list_snapshots(dsi)] == [made[0]]


class TestServerPrunerRefusesUnusableConfig:
    """S1-a: `_received_dataset_info` runs the shared shape check. A zero age, keep_for or
    frequency is a division by zero in `needs_prunning`; the dataset is skipped instead."""

    PATH = 'pool/backups/client1/data'

    @staticmethod
    def wire(frequency=3600, rules=((3600, 86400),), remote=()):
        import base64
        import json
        return base64.b64encode(json.dumps({
            'name': 'pool/data', 'recursive': False, 'frequency': frequency, 'enabled': True,
            'retention_rules': [{'age': a, 'keep_for': k} for a, k in rules],
            'remote': [{'destination': d, 'frequency': f} for d, f in remote],
        }).encode()).decode()

    def received(self, mocker, value):
        FakeReceived(mocker, [self.PATH], local={self.PATH: _client_props(config=value)})
        return DatasetManager(_remote_manager().config)

    def test_a_usable_config_is_accepted(self, mocker):
        manager = self.received(mocker, self.wire())
        (dsi,) = manager.received_datasets()
        assert dsi.name == self.PATH

    @pytest.mark.parametrize('kwargs, why', [
        (dict(frequency=0), 'frequency is not positive'),
        (dict(rules=((0, 86400),)), 'a retention rule has a duration that is not positive'),
        (dict(rules=((3600, 0),)), 'a retention rule has a duration that is not positive'),
        (dict(rules=((3600, 86400), (0, 0))), 'a retention rule has a duration that is not positive'),
        (dict(remote=(('offsite', -5),)), 'a remote frequency is not positive'),
    ], ids=['zero-frequency', 'zero-age', 'zero-keep_for', 'second-rule-zero', 'negative-remote-frequency'])
    def test_it_is_skipped_with_the_unusable_message(self, mocker, caplog, kwargs, why):
        manager = self.received(mocker, self.wire(**kwargs))
        with caplog.at_level(logging.WARNING):
            assert manager.received_datasets() == []
        (rec,) = _records(caplog, 'Skipping received dataset', 'WARNING')
        assert f"org.zfsbackup:config is unusable ({why})" in rec.getMessage()
        assert 'not pruned until this is fixed' in rec.getMessage()

    def test_the_skip_is_announced_once_and_repeats_at_debug(self, mocker, caplog):
        manager = self.received(mocker, self.wire(frequency=0))
        with caplog.at_level(logging.DEBUG):
            for _ in range(3):
                manager.received_datasets()
        assert len(_records(caplog, 'Skipping received dataset', 'WARNING')) == 1
        assert len(_records(caplog, 'Skipping received dataset', 'DEBUG')) == 2

    def test_it_is_accepted_again_once_fixed(self, mocker, caplog):
        fake = FakeReceived(mocker, [self.PATH], local={self.PATH: _client_props(config=self.wire(frequency=0))})
        manager = DatasetManager(_remote_manager().config)
        assert manager.received_datasets() == []
        fake.local[self.PATH] = _client_props(config=self.wire())
        assert [d.name for d in manager.received_datasets()] == [self.PATH]

    def test_the_prune_cycle_never_divides_by_zero(self, mocker, frozen_clock):
        """What the skip prevents: `needs_prunning` on the zero-age config raises."""
        cfg = DatasetConfig(
            name='pool/data', frequency=timedelta(hours=1),
            retention_rules=[RetentionRule(timedelta(0), timedelta(hours=1))],
        )
        manager = DatasetManager(_make_config(datasets=[cfg]))
        dsi = manager.datasets[0]
        dsi.snapshots = [listed_info(frozen_clock.stamp(-timedelta(minutes=10)), txg=2),
                         listed_info(frozen_clock.stamp(-timedelta(minutes=20)), txg=1)]
        with pytest.raises(ZeroDivisionError):
            manager.needs_prunning(dsi)

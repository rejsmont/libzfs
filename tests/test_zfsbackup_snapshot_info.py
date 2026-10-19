"""Unit tests for SnapshotInfo and DatasetInfo computation."""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from libzfseasy.types import Dataset, Filesystem, Snapshot as ZFSSnapshot
from zfsbackup.backup_manager import DatasetInfo, SnapshotInfo
from zfsbackup.config import DatasetConfig, Duration


def _make_snap(short_name: str) -> ZFSSnapshot:
    fs = Filesystem('pool/data')
    return ZFSSnapshot(fs, short_name)


def _make_dsi(frequency: timedelta) -> DatasetInfo:
    ds = Dataset('pool/data')
    cfg = DatasetConfig(name='pool/data', frequency=frequency)
    return DatasetInfo(ds, cfg)


class TestSnapshotInfo:
    def test_valid_name_parses_timestamp(self):
        info = SnapshotInfo(_make_snap('autosnap_20240101120000'), 'autosnap')
        assert info.timestamp == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    def test_name_is_short(self):
        info = SnapshotInfo(_make_snap('autosnap_20240101120000'), 'autosnap')
        assert info.name == 'autosnap_20240101120000'

    def test_full_name(self):
        info = SnapshotInfo(_make_snap('autosnap_20240101120000'), 'autosnap')
        assert info.full_name == 'pool/data@autosnap_20240101120000'

    def test_is_managed_true(self):
        info = SnapshotInfo(_make_snap('autosnap_20240101120000'), 'autosnap')
        assert info.is_managed is True

    def test_is_managed_false_wrong_prefix(self):
        info = SnapshotInfo(_make_snap('manual_20240101120000'), 'autosnap')
        assert info.is_managed is False

    def test_is_managed_false_no_underscore(self):
        info = SnapshotInfo(_make_snap('autosnap'), 'autosnap')
        assert info.is_managed is False

    def test_is_managed_false_bad_timestamp(self):
        info = SnapshotInfo(_make_snap('autosnap_badformat'), 'autosnap')
        assert info.is_managed is False

    def test_timestamp_none_on_invalid(self):
        info = SnapshotInfo(_make_snap('autosnap_badformat'), 'autosnap')
        assert info.timestamp is None

    def test_timestamp_none_on_no_underscore(self):
        info = SnapshotInfo(_make_snap('autosnap'), 'autosnap')
        assert info.timestamp is None

    def test_age_positive_for_old_snapshot(self):
        info = SnapshotInfo(_make_snap('autosnap_20200101000000'), 'autosnap')
        assert info.age > timedelta(days=365)

    def test_age_zero_when_no_timestamp(self):
        info = SnapshotInfo(_make_snap('autosnap_badformat'), 'autosnap')
        assert info.age == timedelta(0)

    def test_repr_contains_full_name(self):
        info = SnapshotInfo(_make_snap('autosnap_20240101120000'), 'autosnap')
        assert 'pool/data@autosnap_20240101120000' in repr(info)

    def test_repr_contains_age(self):
        info = SnapshotInfo(_make_snap('autosnap_20240101120000'), 'autosnap')
        assert 'age=' in repr(info)

    def test_different_prefix(self):
        info = SnapshotInfo(_make_snap('mysnap_20240101120000'), 'mysnap')
        assert info.is_managed is True
        assert info.prefix == 'mysnap'


class TestDatasetInfo:
    def test_get_reference_time_aligned_to_hour(self):
        dsi = _make_dsi(timedelta(hours=1))
        now = datetime(2024, 6, 15, 13, 45, 30, tzinfo=timezone.utc)
        ref = dsi.get_reference_time(now)
        assert ref == datetime(2024, 6, 15, 13, 0, 0, tzinfo=timezone.utc)

    def test_get_reference_time_aligned_to_15min(self):
        dsi = _make_dsi(timedelta(minutes=15))
        now = datetime(2024, 6, 15, 13, 22, 0, tzinfo=timezone.utc)
        ref = dsi.get_reference_time(now)
        assert ref == datetime(2024, 6, 15, 13, 15, 0, tzinfo=timezone.utc)

    def test_get_reference_time_already_aligned(self):
        dsi = _make_dsi(timedelta(hours=1))
        now = datetime(2024, 6, 15, 13, 0, 0, tzinfo=timezone.utc)
        ref = dsi.get_reference_time(now)
        assert ref == now

    def test_get_reference_time_uses_now_when_none(self):
        dsi = _make_dsi(timedelta(hours=1))
        ref = dsi.get_reference_time()
        assert ref is not None
        assert ref.minute == 0
        assert ref.second == 0

    def test_get_reference_time_zero_frequency_raises(self):
        dsi = _make_dsi(timedelta(0))
        with pytest.raises(ValueError, match="must be positive"):
            dsi.get_reference_time()

    @pytest.mark.parametrize("frequency", [
        Duration("0m"),
        Duration(seconds=-1),
        Duration("-5d"),
    ])
    def test_non_positive_duration_frequency_raises(self, frequency):
        # Consumer-surface row of the Duration plan (`backup_manager.py:47`,
        # `self.config.frequency <= timedelta(0)`): a `Duration` is compared
        # against a bare `timedelta`. Zero AND negative are refused; this is
        # the runtime backstop behind the write-side rejection of
        # frequencies <= 0 (Checkpoint A, Q3).
        dsi = _make_dsi(frequency)
        with pytest.raises(ValueError, match="must be positive"):
            dsi.get_reference_time()

    @pytest.mark.parametrize("literal, expected", [
        ("1h", datetime(2024, 6, 15, 13, 0, 0, tzinfo=timezone.utc)),
        # Aligned to multiples of 5400s since the epoch, not to the clock.
        ("90m", datetime.fromtimestamp(
            (int(datetime(2024, 6, 15, 13, 45, 30, tzinfo=timezone.utc).timestamp()) // 5400) * 5400,
            tz=timezone.utc,
        )),
        ("1s", datetime(2024, 6, 15, 13, 45, 30, tzinfo=timezone.utc)),
    ])
    def test_positive_duration_frequency_aligns(self, literal, expected):
        dsi = _make_dsi(Duration(literal))
        now = datetime(2024, 6, 15, 13, 45, 30, tzinfo=timezone.utc)
        assert dsi.get_reference_time(now) == expected

    def test_duration_frequency_compares_both_ways_with_bare_timedelta(self):
        # `age >= dsi.frequency` where `age` is a bare timedelta
        # (`backup_manager.py:205-206`): the reflected comparison.
        dsi = _make_dsi(Duration("1h"))
        assert timedelta(hours=2) >= dsi.frequency
        assert not timedelta(minutes=59) >= dsi.frequency
        assert dsi.frequency > timedelta(0)

    def test_reference_time_property(self):
        dsi = _make_dsi(timedelta(hours=1))
        assert dsi.reference_time is not None

    def test_frequency_property(self):
        dsi = _make_dsi(timedelta(hours=2))
        assert dsi.frequency == timedelta(hours=2)

    def test_recursive_property_false(self):
        ds = Dataset('pool/data')
        cfg = DatasetConfig(name='pool/data', recursive=False)
        dsi = DatasetInfo(ds, cfg)
        assert dsi.recursive is False

    def test_recursive_property_true(self):
        ds = Dataset('pool/data')
        cfg = DatasetConfig(name='pool/data', recursive=True)
        dsi = DatasetInfo(ds, cfg)
        assert dsi.recursive is True

    def test_name_from_dataset(self):
        ds = Dataset('pool/mydata')
        cfg = DatasetConfig(name='pool/mydata')
        dsi = DatasetInfo(ds, cfg)
        assert dsi.name == 'pool/mydata'

    def test_repr(self):
        dsi = _make_dsi(timedelta(hours=1))
        assert 'DatasetInfo' in repr(dsi)
        assert 'pool/data' in repr(dsi)

    def test_snapshots_initially_empty(self):
        dsi = _make_dsi(timedelta(hours=1))
        assert dsi.snapshots == []


# ---------------------------------------------------------------------------
# F1.1: `{prefix}_{YYYYMMDDHHMMSS}[_anything]`, with a prefix that may itself
# contain underscores. The timestamp is the token right after `{prefix}_` and
# must be exactly 14 ASCII digits.
# ---------------------------------------------------------------------------

from zfsbackup.backup_manager import DatasetManager  # noqa: E402
from zfsbackup.config import BackupConfig  # noqa: E402

STAMP = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)


class TestPrefixedTimestampParsing:
    @pytest.mark.parametrize('prefix, name, managed', [
        ('zfs', 'zfs_20261019120000', True),
        ('zfs', 'zfs_20261019120000_manual', True),
        ('zfs', 'zfs_hourly_20261019120000', False),
        ('zfs', 'zfs_2026101912000', False),            # 13 digits
        ('zfs', 'zfsx_20261019120000', False),          # prefix is a string prefix of the name only
        ('zfs_hourly', 'zfs_hourly_20261019120000', True),
        ('zfs_hourly', 'zfs_20261019120000', False),
        ('a_b', 'a_b_c_20261019120000', False),
        ('zfs_', 'zfs__20261019120000', True),
        # Same length as `zfs_`, so slicing by length alone would find the stamp:
        ('zfs', 'foo_20261019120000', False),
        ('zfs', 'zfs-20261019120000', False),     # wrong separator
        ('zfs', 'ZFS_20261019120000', False),     # case-sensitive
        ('zfs', '20261019120000', False),         # no prefix at all
    ])
    def test_managed_table(self, prefix, name, managed):
        info = SnapshotInfo(_make_snap(name), prefix)
        assert info.is_managed is managed
        assert (info.timestamp == STAMP) is managed
        assert (info.timestamp is not None) is managed

    def test_a_bare_prefix_is_not_managed(self):
        assert SnapshotInfo(_make_snap('zfs'), 'zfs').timestamp is None
        assert SnapshotInfo(_make_snap('zfs_'), 'zfs').timestamp is None

    def test_a_suffix_after_the_timestamp_is_ignored(self):
        info = SnapshotInfo(_make_snap('zfs_20261019120000_a_b_c'), 'zfs')
        assert info.timestamp == STAMP

    def test_the_timestamp_is_utc_aware(self):
        info = SnapshotInfo(_make_snap('zfs_20261019120000'), 'zfs')
        assert info.timestamp.tzinfo is timezone.utc

    @pytest.mark.parametrize('token', [
        '202610191200001',        # 15 digits
        '2026101912000',          # 13 digits
        '',                       # nothing
        '2026101912000a',         # non-digit
        'abcdefghijklmn',         # 14 non-digits
        '2026-10-19T1200',        # separators
        '20261019 120000',        # space (15 chars)
        '-2026101912000',
    ])
    def test_a_malformed_token_is_unmanaged(self, token):
        info = SnapshotInfo(_make_snap(f'zfs_{token}'), 'zfs')
        assert info.timestamp is None
        assert info.is_managed is False

    @pytest.mark.parametrize('token', [
        '٢٠٢٦١٠١٩١٢٠٠٠٠',          # 14 Arabic-Indic digits: str.isdigit() and strptime accept them
        '２０２６１０１９１２００００',  # 14 full-width digits
        '²⁰²⁶¹⁰¹⁹¹²⁰⁰⁰⁰',            # 14 superscripts: isdigit() True, int() fails
        '+2026101912000',         # 14 chars with a sign
    ])
    def test_unicode_and_signed_tokens_are_unmanaged(self, token):
        """Such names cannot exist as a real ZFS snapshot (ASCII-only), so the
        snapshot object is a stub; the parser must still refuse them."""
        stub = MagicMock()
        stub.short = f'zfs_{token}'
        stub.__str__.return_value = f'pool/data@zfs_{token}'
        info = SnapshotInfo(stub, 'zfs')
        assert info.timestamp is None
        assert info.is_managed is False

    @pytest.mark.parametrize('token', [
        '20261340120000',         # month 13
        '20260230120000',         # Feb 30
        '20261019250000',         # hour 25
        '20261019126000',         # minute 60
        '00000000000000',
    ])
    def test_an_invalid_date_has_no_timestamp(self, token):
        info = SnapshotInfo(_make_snap(f'zfs_{token}'), 'zfs')
        assert info.timestamp is None
        assert info.is_managed is False
        assert info.age == timedelta(0)

    def test_the_old_loose_parse_is_gone(self):
        """The old parser took `name.split('_')[1][:8]` + `[8:]` with no
        length check, so a 12-digit or an hourly-prefixed name parsed (or
        mis-parsed) as a snapshot of the shorter prefix."""
        assert SnapshotInfo(_make_snap('zfs_202610191200'), 'zfs').timestamp is None
        assert SnapshotInfo(_make_snap('zfs_hourly_20261019120000'), 'zfs').timestamp is None

    @pytest.mark.parametrize('prefix', ['zfs', 'zfs_hourly', 'a_b', 'autosnap'])
    def test_generated_names_round_trip(self, prefix):
        config = BackupConfig(
            datasets=[DatasetConfig(name='pool/data', frequency=timedelta(minutes=1))],
            snapshot_prefix=prefix,
        )
        manager = DatasetManager(config)
        dsi = manager.datasets[0]
        name = manager._generate_snapshot_name(dsi, STAMP)
        assert name == f'{prefix}_20261019120000'
        info = SnapshotInfo(_make_snap(name), prefix)
        assert info.is_managed is True
        assert info.timestamp == STAMP

    def test_a_generated_name_is_not_managed_under_a_longer_or_shorter_sibling_prefix(self):
        config = BackupConfig(
            datasets=[DatasetConfig(name='pool/data', frequency=timedelta(minutes=1))],
            snapshot_prefix='zfs_hourly',
        )
        name = DatasetManager(config)._generate_snapshot_name(
            DatasetManager(config).datasets[0], STAMP)
        assert SnapshotInfo(_make_snap(name), 'zfs').is_managed is False
        assert SnapshotInfo(_make_snap(name), 'zfs_hourly_x').is_managed is False


class TestListSnapshotsWithUnderscoredPrefix:
    def test_only_snapshots_of_the_exact_prefix_are_managed(self, mocker):
        config = BackupConfig(
            datasets=[DatasetConfig(name='pool/data')], snapshot_prefix='zfs_hourly',
        )
        manager = DatasetManager(config)
        dsi = manager.datasets[0]
        fs = Filesystem('pool/data')
        names = [
            'zfs_hourly_20261019120000',          # managed
            'zfs_hourly_20261019130000_manual',   # managed (suffix)
            'zfs_20261019140000',                 # a different prefix
            'zfs_daily_20261019150000',
            'zfs_hourly_2026101916000',           # 13 digits
            'zfs_hourly_badstamp',
            'zfs_hourly',
            'manual',
        ]
        mocker.patch('zfsbackup.backup_manager.zfs.list',
                     return_value=[ZFSSnapshot(fs, n) for n in names])
        result = manager.list_snapshots(dsi)
        assert [s.name for s in result] == [
            'zfs_hourly_20261019130000_manual',
            'zfs_hourly_20261019120000',
        ]
        assert [s.name for s in dsi.snapshots] == [s.name for s in result]

    def test_the_short_prefix_manages_none_of_the_hourly_ones(self, mocker):
        config = BackupConfig(datasets=[DatasetConfig(name='pool/data')], snapshot_prefix='zfs')
        manager = DatasetManager(config)
        fs = Filesystem('pool/data')
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[
            ZFSSnapshot(fs, 'zfs_hourly_20261019120000'),
            ZFSSnapshot(fs, 'zfs_20261019120000'),
        ])
        result = manager.list_snapshots(manager.datasets[0])
        assert [s.name for s in result] == ['zfs_20261019120000']

"""Unit tests for SnapshotInfo and DatasetInfo computation."""

import pytest
from datetime import datetime, timedelta, timezone

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

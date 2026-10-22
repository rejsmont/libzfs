"""Unit tests for SnapshotInfo and DatasetInfo computation."""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from libzfseasy.types import Dataset, Filesystem, Snapshot as ZFSSnapshot
from zfsbackup.backup_manager import DatasetInfo, SnapshotInfo
from zfsbackup.config import DatasetConfig, Duration
from _snapshot_support import NO_TXG, listed_info, listed_snapshot


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
# F1.1 / S1-b: `{prefix}_{YYYYMMDDHHMMSS}`, with a prefix that may itself
# contain underscores. The WHOLE name must match: the timestamp is exactly 14
# ASCII digits right after `{prefix}_` and nothing follows (a suffixed name is
# not ours since S-1).
# ---------------------------------------------------------------------------

from zfsbackup.backup_manager import DatasetManager  # noqa: E402
from zfsbackup.config import BackupConfig  # noqa: E402

STAMP = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)


class TestPrefixedTimestampParsing:
    @pytest.mark.parametrize('prefix, name, managed', [
        ('zfs', 'zfs_20261019120000', True),
        ('zfs', 'zfs_20261019120000_manual', False),    # S1-b: a suffix makes it unmanaged
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

    def test_a_suffix_after_the_timestamp_makes_the_name_unmanaged(self):
        info = SnapshotInfo(_make_snap('zfs_20261019120000_a_b_c'), 'zfs')
        assert info.timestamp is None
        assert info.is_managed is False

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
        names = [
            'zfs_hourly_20261019120000',          # managed
            'zfs_hourly_20261019130000_manual',   # suffixed: not ours (S1-b)
            'zfs_20261019140000',                 # a different prefix
            'zfs_daily_20261019150000',
            'zfs_hourly_2026101916000',           # 13 digits
            'zfs_hourly_badstamp',
            'zfs_hourly',
            'manual',
        ]
        mocker.patch('zfsbackup.backup_manager.zfs.list',
                     return_value=[listed_snapshot(n) for n in names])
        result = manager.list_snapshots(dsi)
        assert [s.name for s in result] == ['zfs_hourly_20261019120000']
        assert [s.name for s in dsi.snapshots] == [s.name for s in result]

    def test_the_short_prefix_manages_none_of_the_hourly_ones(self, mocker):
        config = BackupConfig(datasets=[DatasetConfig(name='pool/data')], snapshot_prefix='zfs')
        manager = DatasetManager(config)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[
            listed_snapshot('zfs_hourly_20261019120000'),
            listed_snapshot('zfs_20261019120000'),
        ])
        result = manager.list_snapshots(manager.datasets[0])
        assert [s.name for s in result] == ['zfs_20261019120000']


# ---------------------------------------------------------------------------
# S1-b: the strict parse. `re.fullmatch(re.escape(prefix) + '_([0-9]{14})', name)`
# plus a valid calendar date: anything else is unmanaged (never pruned, sent or
# taken for the newest).
# ---------------------------------------------------------------------------


class TestStrictNameParseTable:
    @staticmethod
    def _object(name):
        """The real libzfseasy snapshot, or -- for a name libzfseasy itself refuses (a newline, a
        space, a bracket: not a ZFS name) -- a stub, so the PARSER's refusal is what is tested."""
        try:
            return _make_snap(name)
        except ValueError:
            stub = MagicMock()
            stub.short = name
            stub.__str__.return_value = f'pool/data@{name}'
            return stub

    @pytest.mark.parametrize('prefix, name, managed', [
        # the one accepted shape
        ('autosnap', 'autosnap_20261019120000', True),
        ('auto_snap', 'auto_snap_20261019120000', True),            # a prefix containing `_`
        ('a_b_c', 'a_b_c_20261019120000', True),
        ('a.b', 'a.b_20261019120000', True),                        # `.` in a prefix is literal ...
        ('a.b', 'aXb_20261019120000', False),                       # ... not "any character"
        ('a+', 'a+_20261019120000', True),
        ('a+', 'aaa_20261019120000', False),                        # `+` is not a quantifier
        ('(x)', '(x)_20261019120000', True),
        ('x|y', 'y_20261019120000', False),                         # `|` is not an alternation
        ('x|y', 'x|y_20261019120000', True),
        # anything after the 14 digits makes the name someone else's
        ('autosnap', 'autosnap_20261019120000_x', False),
        ('autosnap', 'autosnap_20261019120000_', False),
        ('autosnap', 'autosnap_20261019120000x', False),
        ('autosnap', 'autosnap_20261019120000 ', False),
        ('autosnap', 'autosnap_20261019120000\n', False),           # `$` would have accepted this
        ('autosnap', 'autosnap_20261019120000\r', False),
        ('autosnap', 'autosnap_20261019120000\x00', False),
        # anything before the prefix, or a prefix that merely starts the name
        ('autosnap', ' autosnap_20261019120000', False),
        ('autosnap', 'xautosnap_20261019120000', False),
        ('auto', 'autosnap_20261019120000', False),                 # a longer name sharing the prefix
        ('auto_snap', 'auto_20261019120000', False),
        ('autosnap', 'autosnap__20261019120000', False),            # two separators
        # the stamp itself: exactly 14 ASCII digits
        ('autosnap', 'autosnap_2026101912000', False),
        ('autosnap', 'autosnap_202610191200000', False),
        ('autosnap', 'autosnap_2026-10-19T12:00', False),
        ('autosnap', 'autosnap_', False),
        ('autosnap', 'autosnap', False),
        ('autosnap', '20261019120000', False),
        # a valid shape that is not a date
        ('autosnap', 'autosnap_20261319120000', False),
        ('autosnap', 'autosnap_20260230120000', False),
        ('autosnap', 'autosnap_20261019246000', False),
    ])
    def test_table(self, prefix, name, managed):
        info = SnapshotInfo(self._object(name), prefix)
        assert info.is_managed is managed
        assert (info.timestamp is not None) is managed

    @pytest.mark.parametrize('digits', [
        '٢٠٢٦١٠١٩١٢٠٠٠٠',                # Arabic-Indic: str.isdigit() and int() accept them
        '２０２６１０１９１２００００',      # full-width
        '²⁰²⁶¹⁰¹⁹¹²⁰⁰⁰⁰',                  # superscripts
        '２０２６１０１９120000',            # mixed
    ])
    @pytest.mark.parametrize('suffix', ['', '_x'])
    def test_non_ascii_digits_are_never_a_stamp(self, digits, suffix):
        """Such a name cannot exist in ZFS; the parser still refuses it (`[0-9]`, not `\\d`)."""
        stub = MagicMock()
        stub.short = f'autosnap_{digits}{suffix}'
        stub.__str__.return_value = f'pool/data@autosnap_{digits}{suffix}'
        info = SnapshotInfo(stub, 'autosnap')
        assert info.timestamp is None and info.is_managed is False

    def test_a_clean_name_still_parses_to_utc(self):
        info = SnapshotInfo(_make_snap('autosnap_20261019120000'), 'autosnap')
        assert info.timestamp == STAMP and info.timestamp.tzinfo is timezone.utc


# ---------------------------------------------------------------------------
# S1-a: what a listed snapshot carries.
# ---------------------------------------------------------------------------


class TestSnapshotInfoListedFields:
    NAME = 'autosnap_20261019120000'

    def info(self, **props):
        return SnapshotInfo(
            ZFSSnapshot(Filesystem('pool/data'), self.NAME, {k: str(v) for k, v in props.items()}),
            'autosnap',
        )

    def test_every_field_is_read_from_the_listed_properties(self):
        info = self.info(
            createtxg=12345, guid=9876543210123456789, userrefs=2,
            clones='pool/c1,pool/c2', encryption='aes-256-gcm', keystatus='available',
        )
        assert info.createtxg == 12345
        assert info.guid == 9876543210123456789
        assert info.userrefs == 2
        assert info.clones == ('pool/c1', 'pool/c2') and isinstance(info.clones, tuple)
        assert info.encrypted is True and info.key_loaded is True

    def test_an_unlisted_snapshot_has_none_everywhere(self):
        info = self.info()
        assert info.createtxg is None and info.guid is None and info.userrefs is None
        assert info.clones == ()
        assert info.encrypted is None and info.key_loaded is None

    @pytest.mark.parametrize('column', ['createtxg', 'guid', 'userrefs'])
    @pytest.mark.parametrize('raw', ['-', '', 'garbage', '1.5'])
    def test_a_dash_or_junk_number_is_none(self, column, raw):
        assert getattr(self.info(**{column: raw}), column) is None

    def test_a_dash_clones_is_empty(self):
        assert self.info(clones='-').clones == ()

    def test_a_single_clone(self):
        assert self.info(clones='pool/only').clones == ('pool/only',)

    @pytest.mark.parametrize('encryption, keystatus, encrypted, loaded', [
        ('off', '-', False, None),
        ('aes-256-gcm', 'available', True, True),
        ('aes-128-ccm', 'unavailable', True, False),
        ('-', '-', None, None),
    ])
    def test_encryption_and_key_state(self, encryption, keystatus, encrypted, loaded):
        info = self.info(encryption=encryption, keystatus=keystatus)
        assert info.encrypted is encrypted and info.key_loaded is loaded

    def test_the_listing_helper_builds_the_same_thing(self):
        info = listed_info(self.NAME, txg=7, guid=8)
        assert info.createtxg == 7 and info.guid == 8 and info.userrefs == 0
        assert info.encrypted is False and info.clones == ()
        assert listed_info(self.NAME, txg=NO_TXG).createtxg is None


class TestSnapshotOrderHelpers:
    def snap(self, name, txg):
        return listed_info(name, txg=txg)

    def test_order_key_is_txg_then_timestamp_then_name(self):
        from zfsbackup.backup_manager import snapshot_order_key
        a = self.snap('autosnap_20240101000000', 5)
        b = self.snap('autosnap_20240102000000', 5)      # same txg, later name stamp
        c = self.snap('autosnap_20240103000000', 4)      # later name, EARLIER txg
        assert sorted([b, c, a], key=snapshot_order_key) == [c, a, b]

    def test_a_missing_txg_sorts_oldest(self):
        from zfsbackup.backup_manager import snapshot_order_key
        no_txg = self.snap('autosnap_20990101000000', NO_TXG)
        some = self.snap('autosnap_20200101000000', 1)
        assert snapshot_order_key(no_txg) < snapshot_order_key(some)

    def test_newer_is_by_txg_not_by_name(self):
        from zfsbackup.backup_manager import snapshot_newer
        early_name_late_txg = self.snap('autosnap_20200101000000', 100)
        late_name_early_txg = self.snap('autosnap_20990101000000', 1)
        assert snapshot_newer(early_name_late_txg, late_name_early_txg) is True
        assert snapshot_newer(late_name_early_txg, early_name_late_txg) is False

    def test_equal_txg_falls_back_to_the_name_stamp(self):
        from zfsbackup.backup_manager import snapshot_newer
        a = self.snap('autosnap_20240101000000', 9)
        b = self.snap('autosnap_20240102000000', 9)
        assert snapshot_newer(b, a) is True and snapshot_newer(a, b) is False

    def test_a_snapshot_is_not_newer_than_itself(self):
        from zfsbackup.backup_manager import snapshot_newer
        a = self.snap('autosnap_20240101000000', 9)
        assert snapshot_newer(a, a) is False

    @pytest.mark.parametrize('which', ['a', 'b', 'both'])
    def test_an_unknown_order_is_never_newer(self, which):
        """No txg, no verdict -- in either direction."""
        from zfsbackup.backup_manager import snapshot_newer
        known = self.snap('autosnap_20240101000000', 1)
        unknown = self.snap('autosnap_20990101000000', NO_TXG)
        a, b = {'a': (unknown, known), 'b': (known, unknown), 'both': (unknown, unknown)}[which]
        assert snapshot_newer(a, b) is False
        assert snapshot_newer(b, a) is False

    def test_none_is_never_newer(self):
        from zfsbackup.backup_manager import snapshot_newer
        known = self.snap('autosnap_20240101000000', 1)
        assert snapshot_newer(None, known) is False
        assert snapshot_newer(known, None) is False
        assert snapshot_newer(None, None) is False

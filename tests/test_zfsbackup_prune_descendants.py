"""F2: a recursive dataset's prune must not destroy a snapshot a descendant needs.

`zfs destroy -r pool/d@snap` also destroys `@snap` on every descendant. So
`DatasetManager.prune_snapshots` for a recursive dataset first reads, in one
`zfs get -r`, every `org.zfsbackup:anchor.*` set on a descendant, and asks the
retention of every enabled, configured descendant which snapshots it keeps;
those names are dropped from the candidates BEFORE batching. Anything it cannot
establish makes it skip the dataset for the cycle.

Fixture: parent `pool/d` (recursive) has snapshots A (newest), B and C (oldest).
Its retention (interval 1h, keep 2h) keeps only A, so B and C are the
candidates. Descendants are described by the fake `zfs` layer below.
"""

import logging
from datetime import datetime, timedelta, timezone

import pytest

import libzfseasy as zfs
from libzfseasy.types import Filesystem, Snapshot as ZFSSnapshot
from libzfseasy.zfs import ZFSOutputError
from _get_support import GetReader, ZfsWorld, get_lines, list_lines as list_lines_
from _snapshot_support import listed_snapshot
from _prune_support import (  # noqa: F401
    VERSIONS, PrunePool, hourly_name, prune_manager, prune_pool, records,
)
from zfsbackup.backup_manager import (
    PROP_ANCHOR_PREFIX as ANCHOR,
    PROP_CLIENT_ID,
    PROP_CONFIG,
    DatasetInfo,
    DatasetManager,
    SnapshotInfo,
    read_descendant_zfsbackup_properties,
    read_zfsbackup_properties,
)
from libzfseasy.types import Dataset
from zfsbackup.config import BackupConfig, DatasetConfig, RetentionRule

PARENT = 'pool/d'
CHILD = 'pool/d/c'
PREFIX = 'autosnap'


def snap_name(hours_old, minutes_old=0):
    dt = datetime.now(timezone.utc) - timedelta(hours=hours_old, minutes=minutes_old)
    return f"{PREFIX}_{dt.strftime('%Y%m%d%H%M%S')}"


# Distinct names, newest first. A is 10 minutes old; B and C are older than the
# parent's 2 h keep, so the parent's retention prunes exactly B and C.
A = snap_name(0, 10)
B = snap_name(3)
C = snap_name(4)
D = snap_name(0, 5)          # a snapshot only a child has, newer than all of A, B, C


def retention(keep_hours, interval_hours=1):
    return [RetentionRule(timedelta(hours=interval_hours), timedelta(hours=keep_hours))]


def zget(name, props, ptype='filesystem'):
    """`zfs get -H -o name,property,value,source all <name>` lines (plan B-1d: no `received` column)."""
    return get_lines(name, props, ptype)


class FakeZfs(GetReader):
    """Replaces `GetCommand._exec_lines` (the row reads the real `zfs.get.rows` parses, plus its name-only
    cross-check list), `zfs.list` and `zfs.destroy` as the prune sees them, and records what was asked."""

    def __init__(self, mocker):
        self.snaps = {PARENT: [A, B, C]}
        self.props = {}                      # dataset -> [(property, value, source)]
        self.extra_rows = []                 # raw lines appended to a `-r` listing
        self.recursive_get_error = None
        self.plain_get_error = None
        self.list_errors = set()
        self.missing = set()                 # datasets `zfs.exists` reports absent
        self.exists_errors = {}              # dataset -> message `zfs.exists` raises
        self.exists_calls = []
        self.get_cmds = []
        self.list_roots = []
        self.list_recursive = []             # (root, recursive) of each snapshot listing
        self.received = []                   # datasets `received_datasets()` finds
        self.received_values = {}            # dataset -> {property: local value} for `zfs.get.value`
        self._init_reader()
        self.destroy = mocker.patch('zfsbackup.backup_manager.zfs.destroy')
        # `zfs.get.rows` is a classmethod: it reaches `_exec_lines` through the class
        mocker.patch.object(type(zfs.get), '_exec_lines', side_effect=self._exec)
        mocker.patch('zfsbackup.backup_manager.zfs.list', side_effect=self._list)
        mocker.patch.object(zfs.get, 'value', side_effect=self._value)

    # -- zfs get -H -o value (B-1c: the received datasets' properties, one at a time) ------------
    def _value(self, ds, prop, sources=None, raw=False):
        if sources not in (None, 'local', 'received'):
            raise AssertionError(f'unexpected sources={sources!r}')
        return self.received_values.get(ds.name, {}).get(prop)

    # -- zfs get -------------------------------------------------------------
    def _note_get(self, root, recursive, cmd):
        self.get_cmds.append(list(cmd))
        if recursive and self.recursive_get_error:
            raise RuntimeError(self.recursive_get_error)
        if not recursive and self.plain_get_error:
            raise RuntimeError(self.plain_get_error)

    def _props_of(self, name):
        # the received datasets' client_id/config are written locally by the server's negotiate
        return self.props.get(name, []) + [(p, v, 'local') for p, v in self.received_values.get(name, {}).items()]

    def _get(self, cmd):
        lines = super()._get(cmd)
        if '-r' in cmd:
            lines = lines + self.extra_rows       # forged rows a hostile value could print
        return lines

    # -- zfs list ------------------------------------------------------------
    def _list(self, roots=None, types=None, recursive=False, properties=None, **kw):
        if types is None:
            # `zfs.exists(ds)` -> module-level `list(roots=ds)`
            self.exists_calls.append(roots.name)
            if roots.name in self.exists_errors:
                raise RuntimeError(self.exists_errors[roots.name])
            if roots.name in self.missing:
                raise RuntimeError(f"cannot open '{roots.name}': dataset does not exist")
            return [Filesystem(roots.name)]
        if types == 'filesystem,volume':     # DatasetManager.received_datasets()
            return list(self.received)
        assert types == 'snapshot', f'unexpected zfs.list(types={types!r})'
        self.list_roots.append(roots.name)
        self.list_recursive.append((roots.name, recursive))
        if roots.name in self.list_errors:
            raise RuntimeError(f'cannot open {roots.name}')
        return [listed_snapshot(n, roots.name) for n in self.snaps.get(roots.name, [])]

    # -- results -------------------------------------------------------------
    @property
    def recursive_gets(self):
        return [c for c in self.get_cmds if '-r' in c]

    @property
    def destroyed(self):
        """Short names of every snapshot given to `zfs.destroy`, in order."""
        return [s.short for call in self.destroy.call_args_list for s in call.args[0]]

    @property
    def destroy_batches(self):
        return [[s.short for s in call.args[0]] for call in self.destroy.call_args_list]


@pytest.fixture
def fz(mocker):
    return FakeZfs(mocker)


def make_manager(*children, recursive=True, dry_run=False, parent_rules=None):
    """A manager with the recursive parent plus `children` (DatasetConfigs)."""
    parent = DatasetConfig(
        name=PARENT, recursive=recursive,
        retention_rules=retention(2) if parent_rules is None else parent_rules,
    )
    config = BackupConfig(
        datasets=[parent, *children], snapshot_prefix=PREFIX, dry_run=dry_run,
    )
    manager = DatasetManager(config)
    parent_dsi = next(d for d in manager.datasets if d.name == PARENT)
    return manager, parent_dsi


def child_cfg(name=CHILD, keep_hours=2, enabled=True):
    return DatasetConfig(name=name, retention_rules=retention(keep_hours), enabled=enabled)


def prune(manager, dsi):
    manager.prune_snapshots(dsi)


class TestUnconfiguredDescendants:
    def test_a_child_without_anchors_protects_nothing(self, fz):
        fz.props[CHILD] = []
        manager, dsi = make_manager()
        prune(manager, dsi)
        assert fz.destroyed == [B, C]
        call = fz.destroy.call_args
        assert call.kwargs == {'destroy': True, 'recursive': True}
        # one `zfs get -r` for the pass and one immediately before the single destroy call
        assert len(fz.recursive_gets) == 1 + len(fz.destroy.call_args_list)

    def test_a_local_child_anchor_keeps_that_snapshot_and_only_that(self, fz, caplog):
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        manager, dsi = make_manager()
        with caplog.at_level(logging.INFO):
            prune(manager, dsi)
        assert fz.destroyed == [B]
        assert f'{PARENT}@{C}' in caplog.text
        assert f'{ANCHOR}x on {CHILD}' in caplog.text
        assert '(recursive prune)' in caplog.text
        assert 'has 1 snapshots to prune' in caplog.text

    def test_a_received_child_anchor_keeps_it_too(self, fz):
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'received')]
        manager, dsi = make_manager()
        prune(manager, dsi)
        assert fz.destroyed == [B]

    def test_a_grandchild_anchor_counts(self, fz):
        fz.props[CHILD + '/g'] = [(ANCHOR + 'x', B, 'local')]
        manager, dsi = make_manager()
        prune(manager, dsi)
        assert fz.destroyed == [C]

    def test_anchors_of_several_children_protect_the_union(self, fz):
        fz.props['pool/d/c1'] = [(ANCHOR + 'x', B, 'local')]
        fz.props['pool/d/c2'] = [(ANCHOR + 'y', C, 'local')]
        manager, dsi = make_manager()
        prune(manager, dsi)
        fz.destroy.assert_not_called()

    def test_a_child_anchor_naming_a_non_candidate_protects_nothing_here(self, fz):
        fz.props[CHILD] = [(ANCHOR + 'x', A, 'local'), (ANCHOR + 'y', 'ghost', 'local')]
        manager, dsi = make_manager()
        prune(manager, dsi)
        assert fz.destroyed == [B, C]

    @pytest.mark.parametrize('value', ['-', ''])
    def test_a_blank_child_anchor_protects_nothing(self, fz, value):
        fz.props[CHILD] = [(ANCHOR + 'x', value, 'local')]
        manager, dsi = make_manager()
        prune(manager, dsi)
        assert fz.destroyed == [B, C]

    def test_a_non_anchor_child_property_protects_nothing(self, fz):
        fz.props[CHILD] = [(PROP_CONFIG, C, 'local'), ('org.zfsbackup:client_id', C, 'local')]
        manager, dsi = make_manager()
        prune(manager, dsi)
        assert fz.destroyed == [B, C]

    def test_an_unconfigured_child_is_never_listed(self, fz):
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        manager, dsi = make_manager()
        prune(manager, dsi)
        assert CHILD not in fz.list_roots

    def test_a_child_anchor_released_next_cycle_lets_the_snapshot_go(self, fz):
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        manager, dsi = make_manager()
        prune(manager, dsi)
        assert fz.destroyed == [B]
        fz.props[CHILD] = []
        fz.destroy.reset_mock()
        prune(manager, dsi)
        assert fz.destroyed == [B, C]


class TestConfiguredDescendants:
    def test_a_child_whose_retention_keeps_c_keeps_it_for_the_parent(self, fz, caplog):
        # The child's own snapshots: D (newest, always kept) and C (4h old, inside 10h).
        fz.snaps[CHILD] = [D, C]
        manager, dsi = make_manager(child_cfg(keep_hours=10))
        with caplog.at_level(logging.INFO):
            prune(manager, dsi)
        assert fz.destroyed == [B]
        assert f'retention of {CHILD} keeps it' in caplog.text
        assert CHILD in fz.list_roots

    def test_a_generous_child_protects_everything_so_nothing_is_destroyed(self, fz):
        fz.snaps[CHILD] = [A, B, C]
        manager, dsi = make_manager(child_cfg(keep_hours=100))
        prune(manager, dsi)
        fz.destroy.assert_not_called()

    def test_the_childs_own_newest_snapshot_is_kept_even_if_its_retention_would_prune_it(self, fz):
        # B is the child's only snapshot, so its newest: always kept, whatever the rules say.
        fz.snaps[CHILD] = [B]
        manager, dsi = make_manager(child_cfg(keep_hours=1))
        prune(manager, dsi)
        assert fz.destroyed == [C]

    def test_a_child_whose_retention_prunes_c_does_not_protect_it(self, fz):
        fz.snaps[CHILD] = [A, B, C]
        manager, dsi = make_manager(child_cfg(keep_hours=2))
        prune(manager, dsi)
        assert fz.destroyed == [B, C]

    def test_a_child_anchor_still_protects_what_its_retention_would_prune(self, fz):
        fz.snaps[CHILD] = [A, B, C]
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        manager, dsi = make_manager(child_cfg(keep_hours=2))
        prune(manager, dsi)
        assert fz.destroyed == [B]

    def test_listing_a_child_does_not_replace_the_managers_own_snapshot_list(self, fz):
        fz.snaps[CHILD] = [D, C]
        manager, dsi = make_manager(child_cfg(keep_hours=10))
        child_dsi = next(d for d in manager.datasets if d.name == CHILD)
        sentinel = []
        child_dsi.snapshots = sentinel
        prune(manager, dsi)
        assert child_dsi.snapshots is sentinel and sentinel == []

    def test_a_configured_sibling_with_a_shared_name_prefix_is_not_a_descendant(self, fz):
        fz.snaps['pool/dx'] = [C]
        manager, dsi = make_manager(child_cfg(name='pool/dx', keep_hours=100))
        prune(manager, dsi)
        assert fz.destroyed == [B, C]
        assert 'pool/dx' not in fz.list_roots

    def test_the_parent_itself_is_not_its_own_descendant(self, fz):
        manager, dsi = make_manager()
        prune(manager, dsi)
        assert fz.list_roots.count(PARENT) == 1      # only the parent's own snapshot listing

    def test_nothing_to_prune_means_no_descendant_evaluation(self, fz):
        fz.snaps = {PARENT: [A]}
        fz.list_errors.add(CHILD)
        manager, dsi = make_manager(child_cfg())
        prune(manager, dsi)
        fz.destroy.assert_not_called()
        assert CHILD not in fz.list_roots


class TestDisabledDescendants:
    def test_a_disabled_child_with_an_anchor_still_protects(self, fz):
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        manager, dsi = make_manager(child_cfg(enabled=False))
        assert CHILD not in [d.name for d in manager.datasets]
        prune(manager, dsi)
        assert fz.destroyed == [B]

    def test_a_disabled_child_without_an_anchor_protects_nothing_by_retention(self, fz):
        # Its retention would keep everything, but a disabled dataset is not evaluated.
        fz.snaps[CHILD] = [A, B, C]
        manager, dsi = make_manager(child_cfg(keep_hours=100, enabled=False))
        prune(manager, dsi)
        assert fz.destroyed == [B, C]
        assert CHILD not in fz.list_roots


class TestParentAnchorsUnchanged:
    def test_a_parent_anchor_keeps_its_snapshot(self, fz, caplog):
        fz.props[PARENT] = [(ANCHOR + 'offsite', C, 'local')]
        manager, dsi = make_manager()
        with caplog.at_level(logging.INFO):
            prune(manager, dsi)
        assert fz.destroyed == [B]
        assert '(recursive prune)' not in caplog.text       # existing path, not the descendant filter

    def test_a_parent_anchor_is_not_misread_as_a_descendant_one(self, fz):
        fz.props[PARENT] = [(ANCHOR + 'offsite', C, 'local')]
        manager, dsi = make_manager()
        prune(manager, dsi)
        # the recursive read does return the parent's rows; only the parent path may use them
        assert fz.destroyed == [B]
        assert len(fz.recursive_gets) == 1 + len(fz.destroy.call_args_list)     # the pass + the re-read

    def test_parent_and_child_anchors_combine(self, fz):
        fz.props[PARENT] = [(ANCHOR + 'offsite', B, 'local')]
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        manager, dsi = make_manager()
        prune(manager, dsi)
        fz.destroy.assert_not_called()

    def test_a_forged_snapshot_or_bookmark_row_in_the_recursive_listing_skips_the_prune(self, fz, caplog):
        """B-1d: a snapshot or bookmark row cannot come from `zfs get -t filesystem,volume`. The old reader
        filtered such rows out and went on; the row reader refuses a listing that holds one, so the
        protect-set is unknown, the dataset is skipped this cycle, and nothing is destroyed."""
        fz.extra_rows = [
            f'{PARENT}@{A}\t{ANCHOR}x\t{C}\tlocal\n',
            f'{PARENT}#bm\t{ANCHOR}x\t{C}\tlocal\n',
        ]
        manager, dsi = make_manager()
        with caplog.at_level(logging.INFO):
            prune(manager, dsi)
        fz.destroy.assert_not_called()
        assert f'Cannot read anchors for {PARENT}; skipping pruning this cycle' in caplog.text
        fz.extra_rows = []
        prune(manager, dsi)                    # the next, clean cycle prunes
        assert fz.destroyed == [B, C]


class TestDryRun:
    def test_dry_run_logs_only_unprotected_names_and_destroys_nothing(self, fz, caplog):
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        manager, dsi = make_manager(dry_run=True)
        with caplog.at_level(logging.INFO):
            prune(manager, dsi)
        fz.destroy.assert_not_called()
        would = [r.getMessage() for r in caplog.records if 'Would prune snapshots' in r.getMessage()]
        assert len(would) == 1
        assert f'{PARENT}@{B}' in would[0]
        assert f'{PARENT}@{C}' not in would[0]

    def test_dry_run_with_everything_protected_logs_no_would_prune(self, fz, caplog):
        fz.props[CHILD] = [(ANCHOR + 'x', B, 'local'), (ANCHOR + 'y', C, 'local')]
        manager, dsi = make_manager(dry_run=True)
        with caplog.at_level(logging.INFO):
            prune(manager, dsi)
        assert 'Would prune' not in caplog.text
        assert 'has 0 snapshots to prune' in caplog.text


class TestNonRecursiveParent:
    def test_no_recursive_get_and_children_are_not_listed(self, fz):
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        fz.snaps[CHILD] = [A, B, C]
        manager, dsi = make_manager(child_cfg(keep_hours=100), recursive=False)
        prune(manager, dsi)
        assert fz.recursive_gets == []
        assert CHILD not in fz.list_roots
        assert fz.destroyed == [B, C]
        assert fz.destroy.call_args.kwargs == {'destroy': True, 'recursive': False}

    def test_the_single_get_is_for_the_dataset_alone(self, fz):
        manager, dsi = make_manager(recursive=False)
        prune(manager, dsi)
        # the dataset alone: the pass's read, then the re-read before the one destroy call
        assert [c[-1] for c in fz.get_cmds] == [PARENT] * (1 + len(fz.destroy.call_args_list))
        assert not fz.recursive_gets


class TestUnknownProtectSetSkipsThePrune:
    def test_a_failing_recursive_get_skips_pruning_with_an_error(self, fz, caplog):
        fz.recursive_get_error = "cannot open 'pool/d': dataset does not exist"
        manager, dsi = make_manager()
        with caplog.at_level(logging.INFO):
            prune(manager, dsi)
        fz.destroy.assert_not_called()
        errors = [r for r in caplog.records if r.levelname == 'ERROR']
        assert len(errors) == 1
        assert f'Cannot read anchors for {PARENT}; skipping pruning this cycle' in errors[0].getMessage()
        assert 'dataset does not exist' in errors[0].getMessage()

    def test_a_failing_plain_get_skips_pruning_too(self, fz, caplog):
        fz.plain_get_error = 'boom'
        manager, dsi = make_manager()
        with caplog.at_level(logging.INFO):
            prune(manager, dsi)
        fz.destroy.assert_not_called()
        assert 'Cannot read anchors' in caplog.text and fz.recursive_gets == []

    def test_a_failing_child_listing_skips_pruning_with_an_error(self, fz, caplog):
        fz.list_errors.add(CHILD)
        manager, dsi = make_manager(child_cfg())
        with caplog.at_level(logging.INFO):
            prune(manager, dsi)
        fz.destroy.assert_not_called()
        errors = [r for r in caplog.records if r.levelname == 'ERROR']
        assert len(errors) == 1
        assert f'Cannot evaluate descendants of {PARENT}; skipping pruning this cycle' in errors[0].getMessage()
        assert f'cannot open {CHILD}' in errors[0].getMessage()

    def test_a_failing_child_listing_does_not_skip_a_non_recursive_parent(self, fz):
        fz.list_errors.add(CHILD)
        manager, dsi = make_manager(child_cfg(), recursive=False)
        prune(manager, dsi)
        assert fz.destroyed == [B, C]

    def test_a_good_cycle_after_a_failed_one_prunes(self, fz):
        fz.recursive_get_error = 'transient'
        manager, dsi = make_manager()
        prune(manager, dsi)
        fz.destroy.assert_not_called()
        fz.recursive_get_error = None
        prune(manager, dsi)
        assert fz.destroyed == [B, C]


class TestAnnouncedOncePerDatasetAndName:
    def info_lines(self, caplog, snap):
        return [r for r in caplog.records
                if r.levelname == 'INFO' and f'keeping {PARENT}@{snap}' in r.getMessage()]

    def test_repeated_cycles_log_the_hold_once(self, fz, caplog):
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        manager, dsi = make_manager()
        with caplog.at_level(logging.INFO):
            for _ in range(4):
                prune(manager, dsi)
        assert len(self.info_lines(caplog, C)) == 1
        assert manager._descendant_protect_noted == {(PARENT, C)}

    def test_each_name_is_announced_once_when_it_first_appears(self, fz, caplog):
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        manager, dsi = make_manager()
        with caplog.at_level(logging.INFO):
            prune(manager, dsi)
            prune(manager, dsi)
            fz.props[CHILD] = [(ANCHOR + 'x', C, 'local'), (ANCHOR + 'y', B, 'local')]
            prune(manager, dsi)
            prune(manager, dsi)
        assert len(self.info_lines(caplog, C)) == 1
        assert len(self.info_lines(caplog, B)) == 1

    def test_a_new_manager_announces_again(self, fz, caplog):
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        with caplog.at_level(logging.INFO):
            for _ in range(2):
                manager, dsi = make_manager()
                prune(manager, dsi)
        assert len(self.info_lines(caplog, C)) == 2

    def test_the_same_name_on_another_dataset_is_a_separate_announcement(self, fz, caplog):
        other = 'pool/e'
        fz.snaps[other] = [A, B, C]
        fz.props['pool/e/c'] = [(ANCHOR + 'x', C, 'local')]
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        parent2 = DatasetConfig(name=other, recursive=True, retention_rules=retention(2))
        manager, dsi = make_manager(parent2)
        dsi2 = next(d for d in manager.datasets if d.name == other)
        with caplog.at_level(logging.INFO):
            prune(manager, dsi)
            prune(manager, dsi2)
        assert manager._descendant_protect_noted == {(PARENT, C), (other, C)}


class TestBatching:
    def test_protected_names_are_dropped_before_batching(self, fz):
        names = [snap_name(3 + i) for i in range(45)]
        fz.snaps[PARENT] = [A, *names]
        protected = names[5:10]
        fz.props[CHILD] = [(ANCHOR + f'p{i}', n, 'local') for i, n in enumerate(protected)]
        manager, dsi = make_manager()
        prune(manager, dsi)
        assert [len(b) for b in fz.destroy_batches] == [20, 20]
        assert not set(protected) & set(fz.destroyed)
        assert set(fz.destroyed) == set(names) - set(protected)


# ---------------------------------------------------------------------------
# The reader on its own
# ---------------------------------------------------------------------------

def _fail_get(mock_subprocess, stderr):
    proc = mock_subprocess.setup(stdout=[], stderr=stderr, returncode=1)
    proc.poll.side_effect = None
    proc.poll.return_value = 1
    return proc


def _world(mock_subprocess, **datasets):
    """A `ZfsWorld` over `pool/d` and its subtree: `datasets` maps a name to its property list."""
    world = ZfsWorld(mock_subprocess)
    for name, props in datasets.items():
        world.add(name, props)
    return world


class TestReadDescendantZfsbackupProperties:
    """`read_descendant_zfsbackup_properties` over a Popen-level `zfs`: the real row reader parses the bytes."""

    def test_the_commands_are_one_recursive_filesystem_and_volume_get_and_its_name_only_list(
            self, mock_subprocess):
        world = ZfsWorld(mock_subprocess)
        world.add(PARENT)
        world.add(CHILD, [(ANCHOR + 'x', 'v', 'local')])
        read_descendant_zfsbackup_properties(Filesystem(PARENT))
        (get_argv,), (list_argv,) = world.gets, world.verify_lists
        assert get_argv == ['get', '-H', '-o', 'name,property,value,source', '-r', '-t', 'filesystem,volume',
                            'all', PARENT]
        assert '-s' not in get_argv and 'received' not in ','.join(get_argv)
        assert '-r' in list_argv and list_argv[-1] == PARENT
        assert [a[0] for a in world.argvs] == ['get', 'list']

    def test_the_parent_is_excluded_and_only_strict_descendants_count(self, mock_subprocess):
        world = ZfsWorld(mock_subprocess)
        world.add(PARENT, [(ANCHOR + 'p', 'parent-snap', 'local')])
        world.add(CHILD, [(ANCHOR + 'x', 'child-snap', 'local')])
        world.add(CHILD + '/g', [(PROP_CONFIG, 'cfg', 'local')])
        world.add('pool/dx', [(ANCHOR + 'z', 'sibling', 'local')])              # not below PARENT: never read
        assert read_descendant_zfsbackup_properties(Filesystem(PARENT)) == {
            CHILD: {ANCHOR + 'x': 'child-snap'},
            CHILD + '/g': {PROP_CONFIG: 'cfg'},
        }

    def test_a_descendant_without_properties_does_not_appear(self, mock_subprocess):
        world = ZfsWorld(mock_subprocess)
        world.add(PARENT)
        world.add(CHILD)
        assert read_descendant_zfsbackup_properties(Filesystem(PARENT)) == {}

    @pytest.mark.parametrize('row', [
        f'{CHILD}@s\t{ANCHOR}x\tv\tlocal\n',
        f'{PARENT}@s\t{ANCHOR}x\tv\tlocal\n',
        f'{CHILD}#bm\t{ANCHOR}x\tv\tlocal\n',
    ], ids=['child snapshot', 'parent snapshot', 'child bookmark'])
    def test_a_snapshot_or_bookmark_row_makes_the_read_fail(self, mock_subprocess, row):
        """Not filtered out any more (B-1d): such a row can only be forged, and a listing that holds a
        forged row is not trusted."""
        mock_subprocess.setup_multi(
            (zget(PARENT, []) + zget(CHILD, [(ANCHOR + 'y', 'kept', 'local')]) + [row],),
            (list_lines_([PARENT, CHILD]),),
        )
        with pytest.raises(ZFSOutputError):
            read_descendant_zfsbackup_properties(Filesystem(PARENT))

    @pytest.mark.parametrize('source', ['inherited from pool/d', 'default', 'temporary', '-'])
    def test_inherited_default_and_temporary_rows_are_ignored(self, mock_subprocess, source):
        _world(mock_subprocess, **{PARENT: [], CHILD: [(ANCHOR + 'x', 'v', source), (PROP_CONFIG, 'c', source)]})
        assert read_descendant_zfsbackup_properties(Filesystem(PARENT)) == {}

    def test_a_received_row_is_read_with_its_value(self, mock_subprocess):
        _world(mock_subprocess, **{PARENT: [], CHILD: [(ANCHOR + 'x', 'snap-1', 'received')]})
        assert read_descendant_zfsbackup_properties(Filesystem(PARENT)) == {
            CHILD: {ANCHOR + 'x': 'snap-1'},
        }

    def test_a_tab_inside_a_value_is_preserved(self, mock_subprocess):
        _world(mock_subprocess, **{PARENT: [], CHILD: [(ANCHOR + 'tab', 'va\tlue', 'local'),
                                                       (ANCHOR + 'rtab', 're\tceived', 'received')]})
        assert read_descendant_zfsbackup_properties(Filesystem(PARENT)) == {
            CHILD: {ANCHOR + 'tab': 'va\tlue', ANCHOR + 'rtab': 're\tceived'},
        }

    def test_non_zfsbackup_properties_are_ignored(self, mock_subprocess):
        _world(mock_subprocess, **{PARENT: [], CHILD: [('compression', 'lz4', 'local'),
                                                       ('org.other:anchor.x', 'v', 'local')]})
        assert read_descendant_zfsbackup_properties(Filesystem(PARENT)) == {}

    @pytest.mark.parametrize('stdout', [
        ['garbage\n'],
        [f'{CHILD}\t{ANCHOR}x\n'],
    ], ids=['garbage', 'short line'])
    def test_malformed_output_for_a_listed_subtree_fails_the_read(self, mock_subprocess, stdout):
        """The old reader skipped short lines and returned the rest; the listing says the subtree has
        datasets the get never described, so the result is untrusted."""
        mock_subprocess.setup_multi((stdout,), (list_lines_([PARENT, CHILD]),))
        with pytest.raises(ZFSOutputError):
            read_descendant_zfsbackup_properties(Filesystem(PARENT))

    def test_empty_output_for_an_empty_listing_is_empty(self, mock_subprocess):
        mock_subprocess.setup_multi(([],), ([],))
        assert read_descendant_zfsbackup_properties(Filesystem(PARENT)) == {}

    def test_works_on_a_plain_dataset_object(self, mock_subprocess):
        _world(mock_subprocess, **{PARENT: [], CHILD: [(ANCHOR + 'x', 'v', 'local')]})
        assert read_descendant_zfsbackup_properties(Dataset(PARENT)) == {CHILD: {ANCHOR + 'x': 'v'}}

    def test_a_zfs_get_failure_raises(self, mock_subprocess):
        _fail_get(mock_subprocess, "cannot open 'pool/d': dataset does not exist")
        with pytest.raises(Exception, match='dataset does not exist'):
            read_descendant_zfsbackup_properties(Filesystem(PARENT))


class TestSingleDatasetReaderIsFramingSafe:
    def test_the_command_has_no_recursion_and_the_type_filter_of_the_row_reader(self, mock_subprocess):
        world = ZfsWorld(mock_subprocess)
        world.add('pool/data')
        read_zfsbackup_properties(Filesystem('pool/data'))
        (get_argv,) = world.gets
        assert get_argv == ['get', '-H', '-o', 'name,property,value,source', '-t', 'filesystem,volume',
                            'all', 'pool/data']
        assert '-r' not in get_argv

    def test_descendant_rows_are_not_accepted_for_the_single_read(self, mock_subprocess):
        """B-1d: the single read is not recursive, so a descendant's rows in its output are out of scope.
        The old reader quietly dropped them; the row reader refuses the read."""
        mock_subprocess.setup_multi(
            (zget('pool/data', [(PROP_CONFIG, 'parent', 'local')])
             + zget('pool/data/child', [(ANCHOR + 'x', 'child', 'local')]),),
        )
        with pytest.raises(ZFSOutputError):
            read_zfsbackup_properties(Filesystem('pool/data'))


# ---------------------------------------------------------------------------
# Review follow-ups
# ---------------------------------------------------------------------------

def levels_of(caplog, text):
    return [r.levelname for r in caplog.records if text in r.getMessage()]


class TestReceivedDatasetsArePrunedNonRecursively:
    """The client's `recursive` describes how the CLIENT snapshots. Every
    received child is its own client dataset with its own retention, so a
    server-side `zfs destroy -r` of the parent's retention would eat the
    children's history."""

    RECEIVED = 'pool/backups/c1'
    RECEIVED_CHILD = 'pool/backups/c1/sub'

    def received_ds(self, fz, recursive=True, name=None):
        name = name or self.RECEIVED
        cfg = DatasetConfig(name='pool/src', recursive=recursive, retention_rules=retention(2))
        ds = Filesystem(name)
        fz.received_values[name] = {PROP_CLIENT_ID: 'c1', PROP_CONFIG: cfg.to_property()}
        fz.received.append(ds)
        return cfg

    def manager(self):
        from zfsbackup.config import RemoteServerConfig
        config = BackupConfig(
            datasets=[], snapshot_prefix=PREFIX,
            remote_backup=RemoteServerConfig(target_dataset='pool/backups', enabled=True),
        )
        return DatasetManager(config)

    def test_a_recursive_client_config_gives_a_non_recursive_dataset_info(self, fz):
        cfg = self.received_ds(fz, recursive=True)
        (dsi,) = self.manager().received_datasets()
        assert cfg.recursive is True                  # the decoded source config is not the object kept
        assert dsi.recursive is False and dsi.config.recursive is False
        assert dsi.name == self.RECEIVED
        assert dsi.config.retention_rules[0].keep_for == timedelta(hours=2)   # nothing else changed

    def test_a_non_recursive_client_config_stays_non_recursive(self, fz):
        self.received_ds(fz, recursive=False)
        (dsi,) = self.manager().received_datasets()
        assert dsi.recursive is False

    def test_pruning_one_destroys_non_recursively_and_leaves_received_children_alone(self, fz):
        self.received_ds(fz, recursive=True)
        fz.snaps[self.RECEIVED] = [A, B, C]
        fz.snaps[self.RECEIVED_CHILD] = [A, B, C]
        fz.props[self.RECEIVED_CHILD] = [(ANCHOR + 'x', C, 'received')]
        manager = self.manager()
        (dsi,) = manager.received_datasets()
        assert len(fz.recursive_gets) == 1             # B-1d: the one batch read of the target's properties
        fz.get_cmds.clear()
        manager.prune_snapshots(dsi)
        assert fz.destroyed == [B, C]                  # no descendant protection was consulted
        assert [str(sn) for call in fz.destroy.call_args_list for sn in call.args[0]] == [
            f'{self.RECEIVED}@{B}', f'{self.RECEIVED}@{C}',
        ]
        assert fz.destroy.call_args.kwargs == {'destroy': True, 'recursive': False}
        assert fz.recursive_gets == []
        assert self.RECEIVED_CHILD not in fz.list_roots
        assert fz.list_recursive == [(self.RECEIVED, False)]


class TestRecursiveConfiguredDescendant:
    def test_a_recursive_child_protects_through_its_own_retention(self, fz):
        fz.snaps[CHILD] = [D, C]
        child = DatasetConfig(name=CHILD, recursive=True, retention_rules=retention(10))
        manager, dsi = make_manager(child)
        prune(manager, dsi)
        assert fz.destroyed == [B]
        # the child's snapshots are listed for the child alone, not for its subtree
        assert (CHILD, False) in fz.list_recursive

    def test_a_grandchild_below_a_recursive_child_is_evaluated_too(self, fz):
        grand = CHILD + '/g'
        fz.snaps[CHILD] = [D, C]
        fz.snaps[grand] = [D, B]
        manager, dsi = make_manager(
            DatasetConfig(name=CHILD, recursive=True, retention_rules=retention(10)),
            DatasetConfig(name=grand, retention_rules=retention(10)),
        )
        prune(manager, dsi)
        fz.destroy.assert_not_called()                 # B held by the grandchild, C by the child

    def test_only_the_grandchild_holds_when_the_child_prunes(self, fz):
        grand = CHILD + '/g'
        fz.snaps[CHILD] = [A, B, C]
        fz.snaps[grand] = [D, C]
        manager, dsi = make_manager(
            DatasetConfig(name=CHILD, recursive=True, retention_rules=retention(2)),
            DatasetConfig(name=grand, retention_rules=retention(10)),
        )
        prune(manager, dsi)
        assert fz.destroyed == [B]


class TestNotedSetBookkeeping:
    def held(self, manager):
        return manager._descendant_protect_noted

    def test_hold_release_shrinks_and_a_new_hold_is_announced_again(self, fz, caplog):
        manager, dsi = make_manager()
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        with caplog.at_level(logging.INFO):
            prune(manager, dsi)
            assert self.held(manager) == {(PARENT, C)}
            fz.props[CHILD] = []
            prune(manager, dsi)
            assert self.held(manager) == set()
            fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
            prune(manager, dsi)
        assert len(levels_of(caplog, f'keeping {PARENT}@{C}')) == 2

    def test_releasing_one_of_two_names_keeps_only_the_other(self, fz):
        manager, dsi = make_manager()
        fz.props[CHILD] = [(ANCHOR + 'x', B, 'local'), (ANCHOR + 'y', C, 'local')]
        prune(manager, dsi)
        assert self.held(manager) == {(PARENT, B), (PARENT, C)}
        fz.props[CHILD] = [(ANCHOR + 'x', B, 'local')]
        prune(manager, dsi)
        assert self.held(manager) == {(PARENT, B)}

    def test_a_name_that_is_no_longer_a_candidate_is_forgotten(self, fz):
        manager, dsi = make_manager()
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        prune(manager, dsi)
        fz.snaps[PARENT] = [A, B]                      # C is gone from the parent
        prune(manager, dsi)
        assert self.held(manager) == set()

    def test_nothing_to_prune_empties_the_set(self, fz):
        manager, dsi = make_manager()
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        prune(manager, dsi)
        assert self.held(manager)
        fz.snaps[PARENT] = [A]
        prune(manager, dsi)
        assert self.held(manager) == set()

    def test_a_dataset_that_stopped_being_recursive_empties_its_entries(self, fz):
        manager, dsi = make_manager()
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        prune(manager, dsi)
        assert self.held(manager)
        dsi.config.recursive = False
        prune(manager, dsi)
        assert self.held(manager) == set()

    def test_another_datasets_entries_are_left_alone(self, fz):
        manager, dsi = make_manager()
        manager._descendant_protect_noted.add(('pool/other', 'autosnap_1'))
        fz.props[CHILD] = [(ANCHOR + 'x', C, 'local')]
        prune(manager, dsi)
        fz.props[CHILD] = []
        prune(manager, dsi)
        assert self.held(manager) == {('pool/other', 'autosnap_1')}


class TestMissingAndFailingDescendants:
    def test_a_missing_configured_child_is_skipped_and_the_parent_prunes(self, fz, caplog):
        fz.missing.add(CHILD)
        fz.snaps[CHILD] = [A, B, C]                    # would protect everything if it were listed
        manager, dsi = make_manager(child_cfg(keep_hours=100))
        with caplog.at_level(logging.DEBUG):
            prune(manager, dsi)
        assert fz.destroyed == [B, C]
        assert CHILD in fz.exists_calls and CHILD not in fz.list_roots
        assert 'ERROR' not in [r.levelname for r in caplog.records]
        assert levels_of(caplog, f'Descendant {CHILD} of {PARENT} does not exist') == ['DEBUG']

    def test_a_missing_child_does_not_hide_an_existing_one(self, fz):
        other = 'pool/d/e'
        fz.missing.add(CHILD)
        fz.snaps[other] = [D, C]
        manager, dsi = make_manager(child_cfg(keep_hours=100), child_cfg(name=other, keep_hours=10))
        prune(manager, dsi)
        assert fz.destroyed == [B]

    def test_an_existing_child_whose_listing_fails_skips_the_parent(self, fz, caplog):
        fz.list_errors.add(CHILD)
        manager, dsi = make_manager(child_cfg())
        with caplog.at_level(logging.DEBUG):
            prune(manager, dsi)
        fz.destroy.assert_not_called()
        assert levels_of(caplog, 'Cannot evaluate descendants') == ['ERROR']

    def test_the_error_is_logged_once_while_it_persists_then_debug(self, fz, caplog):
        fz.list_errors.add(CHILD)
        manager, dsi = make_manager(child_cfg())
        with caplog.at_level(logging.DEBUG):
            for _ in range(4):
                prune(manager, dsi)
        assert levels_of(caplog, 'Cannot evaluate descendants') == ['ERROR', 'DEBUG', 'DEBUG', 'DEBUG']
        fz.destroy.assert_not_called()
        assert manager._descendant_error_noted == {PARENT}

    def test_the_error_is_logged_again_after_an_intervening_success(self, fz, caplog):
        manager, dsi = make_manager(child_cfg())
        with caplog.at_level(logging.DEBUG):
            fz.list_errors.add(CHILD)
            prune(manager, dsi)
            prune(manager, dsi)
            fz.list_errors.clear()
            prune(manager, dsi)                        # succeeds: clears the note
            assert manager._descendant_error_noted == set()
            fz.list_errors.add(CHILD)
            prune(manager, dsi)
        assert levels_of(caplog, 'Cannot evaluate descendants') == ['ERROR', 'DEBUG', 'ERROR']

    def test_nothing_to_prune_also_clears_the_error_note(self, fz, caplog):
        manager, dsi = make_manager(child_cfg())
        fz.list_errors.add(CHILD)
        prune(manager, dsi)
        assert manager._descendant_error_noted == {PARENT}
        fz.snaps[PARENT] = [A]
        prune(manager, dsi)
        assert manager._descendant_error_noted == set()

    def test_an_exists_error_that_is_not_does_not_exist_skips_the_parent(self, fz, caplog):
        fz.exists_errors[CHILD] = "cannot open 'pool/d/c': permission denied"
        manager, dsi = make_manager(child_cfg())
        with caplog.at_level(logging.DEBUG):
            prune(manager, dsi)
        fz.destroy.assert_not_called()
        errors = [r for r in caplog.records if r.levelname == 'ERROR']
        assert len(errors) == 1
        assert 'Cannot evaluate descendants' in errors[0].getMessage()
        assert 'permission denied' in errors[0].getMessage()
        assert CHILD not in fz.list_roots

    def test_a_failing_child_does_not_affect_a_non_recursive_parent(self, fz):
        fz.exists_errors[CHILD] = 'permission denied'
        manager, dsi = make_manager(child_cfg(), recursive=False)
        prune(manager, dsi)
        assert fz.destroyed == [B, C]
        assert fz.exists_calls == []


# ---------------------------------------------------------------------------
# C1: `org.zfsbackup:inflight.<dest>` = `<from>,<to>` pins both ends of a send
# ---------------------------------------------------------------------------

from zfsbackup.backup_manager import PROP_INFLIGHT_PREFIX as INFLIGHT  # noqa: E402


class TestInflightPinsProtectFromPruning:
    """The parent's retention prunes B and C (A is newest and kept). A pin that
    names either end of a running or resumable send keeps it."""

    # -- the dataset's own pin ----------------------------------------------

    @pytest.mark.parametrize('recursive', [False, True], ids=['plain', 'recursive'])
    def test_a_pin_naming_both_ends_keeps_both(self, fz, recursive):
        fz.props[PARENT] = [(INFLIGHT + 'offsite', f'{B},{C}', 'local')]
        manager, dsi = make_manager(recursive=recursive)
        prune(manager, dsi)
        fz.destroy.assert_not_called()

    @pytest.mark.parametrize('recursive', [False, True], ids=['plain', 'recursive'])
    def test_the_from_side_alone_is_kept(self, fz, recursive):
        fz.props[PARENT] = [(INFLIGHT + 'offsite', f'{B},{A}', 'local')]
        manager, dsi = make_manager(recursive=recursive)
        prune(manager, dsi)
        assert fz.destroyed == [C]                      # B is the `from` end: kept

    @pytest.mark.parametrize('recursive', [False, True], ids=['plain', 'recursive'])
    def test_the_to_side_alone_is_kept(self, fz, recursive):
        fz.props[PARENT] = [(INFLIGHT + 'offsite', f'{A},{C}', 'local')]
        manager, dsi = make_manager(recursive=recursive)
        prune(manager, dsi)
        assert fz.destroyed == [B]

    def test_a_full_send_pin_with_an_empty_from_keeps_the_target(self, fz):
        fz.props[PARENT] = [(INFLIGHT + 'offsite', f',{B}', 'local')]
        manager, dsi = make_manager(recursive=False)
        prune(manager, dsi)
        assert fz.destroyed == [C]

    def test_a_received_pin_counts(self, fz):
        fz.props[PARENT] = [(INFLIGHT + 'offsite', f'{B},{C}', 'received')]
        manager, dsi = make_manager(recursive=False)
        prune(manager, dsi)
        fz.destroy.assert_not_called()

    def test_a_pin_of_a_destination_that_is_not_configured_still_protects(self, fz):
        fz.props[PARENT] = [(INFLIGHT + 'long-gone', f'{B},{C}', 'local')]
        manager, dsi = make_manager(recursive=False)
        prune(manager, dsi)
        fz.destroy.assert_not_called()

    def test_a_pin_and_an_anchor_combine(self, fz):
        fz.props[PARENT] = [
            (ANCHOR + 'offsite', B, 'local'),
            (INFLIGHT + 'offsite', f'{B},{C}', 'local'),
        ]
        manager, dsi = make_manager(recursive=False)
        prune(manager, dsi)
        fz.destroy.assert_not_called()

    def test_a_malformed_pin_still_keeps_the_name_it_carries(self, fz):
        fz.props[PARENT] = [(INFLIGHT + 'offsite', B, 'local')]       # no comma
        manager, dsi = make_manager(recursive=False)
        prune(manager, dsi)
        assert fz.destroyed == [C]

    def test_a_dash_pin_protects_nothing(self, fz):
        fz.props[PARENT] = [(INFLIGHT + 'offsite', '-', 'local')]
        manager, dsi = make_manager(recursive=False)
        prune(manager, dsi)
        assert fz.destroyed == [B, C]

    def test_without_a_pin_both_are_pruned(self, fz):
        """The control for every test above."""
        fz.props[PARENT] = []
        manager, dsi = make_manager(recursive=False)
        prune(manager, dsi)
        assert fz.destroyed == [B, C]

    def test_dry_run_destroys_nothing_either_way(self, fz):
        fz.props[PARENT] = [(INFLIGHT + 'offsite', f'{B},{A}', 'local')]
        manager, dsi = make_manager(recursive=False, dry_run=True)
        prune(manager, dsi)
        fz.destroy.assert_not_called()

    # -- a descendant's pin (the parent's recursive destroy would take it) ----

    def test_a_child_pin_keeps_both_names_on_a_recursive_parent(self, fz, caplog):
        fz.props[CHILD] = [(INFLIGHT + 'offsite', f'{B},{C}', 'local')]
        manager, dsi = make_manager()
        with caplog.at_level(logging.INFO):
            prune(manager, dsi)
        fz.destroy.assert_not_called()
        assert f'{INFLIGHT}offsite on {CHILD}' in caplog.text

    def test_a_child_pin_from_side_is_kept(self, fz):
        fz.props[CHILD] = [(INFLIGHT + 'offsite', f'{B},{A}', 'local')]
        manager, dsi = make_manager()
        prune(manager, dsi)
        assert fz.destroyed == [C]

    def test_a_child_pin_with_an_empty_from_keeps_only_the_target(self, fz):
        fz.props[CHILD] = [(INFLIGHT + 'offsite', f',{C}', 'local')]
        manager, dsi = make_manager()
        prune(manager, dsi)
        assert fz.destroyed == [B]

    def test_a_grandchild_pin_counts(self, fz):
        fz.props[CHILD + '/g'] = [(INFLIGHT + 'x', f'{B},{A}', 'received')]
        manager, dsi = make_manager()
        prune(manager, dsi)
        assert fz.destroyed == [C]

    def test_a_pin_on_the_parent_and_one_on_a_child_combine_on_a_recursive_parent(self, fz):
        fz.props[PARENT] = [(INFLIGHT + 'offsite', f'{A},{B}', 'local')]
        fz.props[CHILD] = [(INFLIGHT + 'other', f',{C}', 'local')]
        manager, dsi = make_manager()
        prune(manager, dsi)
        fz.destroy.assert_not_called()

    def test_a_non_recursive_parent_ignores_a_childs_pin(self, fz):
        fz.props[CHILD] = [(INFLIGHT + 'offsite', f'{B},{C}', 'local')]
        manager, dsi = make_manager(recursive=False)
        prune(manager, dsi)
        assert fz.destroyed == [B, C]                      # the child is its own dataset

    def test_the_descendant_protection_reports_pin_names_and_reasons(self, fz):
        fz.props[CHILD] = [
            (INFLIGHT + 'offsite', f'{B},{C}', 'local'),
            (ANCHOR + 'x', A, 'local'),
        ]
        manager, dsi = make_manager()
        protected = manager._descendant_anchor_protection(dsi)
        assert set(protected) == {A, B, C}
        assert protected[B] == f'{INFLIGHT}offsite on {CHILD}'
        assert protected[A] == f'{ANCHOR}x on {CHILD}'

    def test_a_descendant_pin_with_a_dash_or_nothing_adds_no_names(self, fz):
        fz.props[CHILD] = [(INFLIGHT + 'a', '-', 'local'), (INFLIGHT + 'b', ',', 'local')]
        manager, dsi = make_manager()
        assert manager._descendant_anchor_protection(dsi) == {}


# ---------------------------------------------------------------------------
# B-4: a held or cloned DESCENDANT copy fails the whole recursive destroy
# ---------------------------------------------------------------------------

class TestRecursiveBatchFallback:
    """`zfs destroy -r pool/d@a,b,c` is atomic across the names AND the descendants. A child's hold or
    clone is invisible in the parent's listing (`userrefs` 0), so only the fallback finds the name."""

    P = 'pool/d'
    C = 'pool/d/c'

    def build(self, pool, old=6, dry_run=False):
        manager, dsi = prune_manager(self.P, recursive=True, dry_run=dry_run)
        fresh, olds = pool.populate(self.P, old=old)
        pool.snaps[self.C] = list(pool.snaps[self.P])          # the child has every name too
        return manager, dsi, fresh, olds

    @pytest.mark.parametrize('version', VERSIONS)
    def test_a_child_snapshot_hold_keeps_only_that_name_and_the_reason_mentions_descendants(
            self, mocker, caplog, version):
        pool = PrunePool(mocker, version)
        manager, dsi, fresh, olds = self.build(pool)
        held = olds[2]
        pool.hold(self.C, held)
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert pool.sizes == [6] + [1] * 6
        assert not pool.calls[0].ok
        assert all(c.recursive for c in pool.calls)
        assert pool.names_of(self.P) == {fresh, held}
        assert pool.names_of(self.C) == {fresh, held}           # the child's other copies went with the parent's
        (warning,) = records(caplog, logging.WARNING)
        assert held in warning and 'descendant' in warning
        assert len(records(caplog, logging.ERROR, 'Failed to prune snapshot batch')) == 1
        assert records(caplog, logging.INFO, 'Successfully pruned 5 snapshots')

    def test_it_is_retried_alone_and_recursively_with_no_second_warning_and_goes_once_released(
            self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool)
        held = olds[2]
        prune_pool.hold(self.C, held)
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            prune_pool.calls.clear()
            manager.prune_snapshots(dsi)
            assert prune_pool.batches == [[held]] and prune_pool.calls[0].recursive
            prune_pool.release(self.C, held)
            prune_pool.calls.clear()
            manager.prune_snapshots(dsi)
        assert prune_pool.batches == [[held]]
        assert prune_pool.names_of(self.P) == {fresh} == prune_pool.names_of(self.C)
        assert len(records(caplog, logging.WARNING)) == 1
        assert len(records(caplog, logging.INFO, 'which could not be destroyed earlier')) == 1
        assert manager._prune_stuck == {}

    def test_a_child_snapshot_clone_is_found_the_same_way(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=4)
        prune_pool.clone(self.C, olds[0], 'pool/elsewhere')
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.names_of(self.P) == {fresh, olds[0]}
        (warning,) = records(caplog, logging.WARNING)
        assert 'descendant' in warning

    def test_a_parent_snapshot_hold_or_clone_is_pre_skipped_without_a_failed_batch(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=5)
        prune_pool.hold(self.P, olds[0])
        prune_pool.clone(self.P, olds[1])
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.sizes == [3] and prune_pool.ok_calls == prune_pool.calls
        assert len(records(caplog, logging.WARNING)) == 2
        assert records(caplog, logging.ERROR) == []

    def test_a_recursive_dry_run_makes_no_destroy_calls_even_with_a_cloned_name(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=4, dry_run=True)
        prune_pool.clone(self.P, olds[0])
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.calls == []
        assert len(records(caplog, logging.WARNING)) == 1

    def test_the_success_count_is_not_doubled_by_the_descendants_destroy_lines(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=4)
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.sizes == [4]
        assert records(caplog, logging.INFO, 'Successfully pruned 4 snapshots')

    def test_a_stuck_name_a_descendant_later_protects_is_forgotten_not_retried(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, old=3)
        prune_pool.hold(self.C, olds[0])
        manager.prune_snapshots(dsi)
        assert ('pool/d', olds[0]) in manager._prune_stuck
        prune_pool.props[self.C] = [(ANCHOR + 'x', olds[0], 'local')]      # a child's anchor now pins it
        prune_pool.calls.clear()
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.calls == []
        assert manager._prune_stuck == {}
        assert records(caplog, logging.INFO, 'which could not be destroyed earlier') == []

    def test_45_candidates_make_recursive_calls_of_20_20_and_5(self, prune_pool):
        manager, dsi, fresh, olds = self.build(prune_pool, old=45)
        manager.prune_snapshots(dsi)
        assert prune_pool.sizes == [20, 20, 5]
        assert all(c.kwargs['recursive'] is True for c in prune_pool.calls)

    def test_a_non_recursive_destroy_ignores_a_childs_hold(self, prune_pool):
        manager, dsi = prune_manager(self.P, recursive=False)
        fresh, olds = prune_pool.populate(self.P, old=3)
        prune_pool.snaps[self.C] = list(prune_pool.snaps[self.P])
        prune_pool.hold(self.C, olds[0])
        manager.prune_snapshots(dsi)
        assert prune_pool.sizes == [3] and prune_pool.calls[0].ok
        assert prune_pool.names_of(self.C) == set(olds) | {fresh}

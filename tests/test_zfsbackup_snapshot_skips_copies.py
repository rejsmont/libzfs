"""A2c: the snapshot of a RECURSIVE dataset keeps out of the backup copies below it ("skip copies").

`zfs snapshot -r pool/data@s` also snapshots every descendant, a backup copy included, and a snapshot the source
did not send makes the copy NEWER than its source, which stops its incremental receives. So
`DatasetManager.create_snapshot` hands a recursive dataset to `_snapshot_tree`:

* no copy below it (`_copy_tops`: a dataset with a LOCAL / RECEIVED / INHERITED `org.zfsbackup:client_id` or
  `source_dataset`, or anything at or below a receive target): plain `zfs snapshot -r`, as always;
* copies: a strict `zfs list -H -r -t filesystem,volume -o name,type <ds>`, minus every copy top's subtree
  (component-wise), then ONE atomic `zfs snapshot <ds>@s <child>@s ...` (`snapshot.many`, the dataset first);
  one ERROR per (dataset, copy top) per episode, logged after the snapshot succeeded; INFO when the copy is gone;
  a listing refused as untrusted (a name libzfseasy cannot represent) is retried as a WALK that never lists a copy
  top (`_walk_sources`), so a bad name INSIDE a copy no longer costs the atomic snapshot;
  `DatasetManager.last_snapshot_note` says what was left out (the status record carries it);
* if the copies cannot be told apart (ANY `Exception` from the copy read or the listing, or a `ValueError` from
  `many`'s own checks, e.g. the argv bound) the old plain `snapshot -r` runs, with one ERROR per dataset per
  episode (DEBUG on repeats, INFO on recovery): a stray snapshot in a copy is recoverable, a missed source
  snapshot is not. A `KeyboardInterrupt` propagates; a real zfs failure of `many` (a name collision) fails the
  cycle, as `snapshot -r` would;
* the dataset itself at or below a receive target: nothing is snapshotted, `create_snapshot` is `None` and says why;
  a dataset that only INHERITS a marker from above is snapshotted alone (its children inherit the marker), with
  ONE ERROR per (dataset, origin of the marker) instead of one per child.

The fake is `tests/_snapshot_world.py::SnapWorld`: real libzfseasy over a `Popen`-level `zfs` that takes and
destroys snapshots (atomic, one txg per command, a recursive destroy takes the name out of every descendant), so
"no snapshot exists in a copy" is read off the world. The worker's side (the status record) is in
`test_zfsbackup_status_workers.py`, the real pool in `test_zfsbackup_real.py`.

Layout used throughout: the recursive `pool/data` with the source children `pool/data/c` (and `c/g`) and the
volume `pool/data/v`, and the copy `pool/data/bk` (local markers) with `bk/h1` and `bk/h1/pool` below it.
"""

import logging
from datetime import timedelta

import pytest

from libzfseasy.types import Filesystem
from libzfseasy.zfs import SnapshotCommand, ZFSOutputError, ZFSTimeoutError, _zfs_cmd
from _get_support import copy_read_argv
from _prune_support import hourly_name, records
from _snapshot_support import CLOCK_START, frozen_clock  # noqa: F401  (fixture)
from _snapshot_world import LOCAL, RECEIVED, SnapWorld, inherited, make_manager
from zfsbackup.backup_manager import (
    PROP_CLIENT_ID, PROP_SOURCE_DATASET, SNAPSHOT_LIST_PROPERTIES, DatasetManager,
)
from zfsbackup.config import DatasetConfig, Destination

DS = 'pool/data'
CH = 'pool/data/c'
GC = 'pool/data/c/g'
VOL = 'pool/data/v'
COPY = 'pool/data/bk'
NAME = 'autosnap_20261022100000'            # what the frozen clock (10:30, hourly) generates

ERR, INFO, DEBUG = logging.ERROR, logging.INFO, logging.DEBUG
LOGGER = 'zfsbackup.backup_manager'

TREE_LIST = ['list', '-H', '-r', '-t', 'filesystem,volume', '-o', 'name,type', DS]
SNAPSHOT_LIST = ['list', '-H', '-t', 'snapshot', '-o', 'name,type,' + ','.join(SNAPSHOT_LIST_PROPERTIES), DS]


def exists_argv(name):
    return ['list', '-H', '-o', 'name,type', name]


def build_world(mock_subprocess, copy=True, copy_props=LOCAL):
    """`pool/data` (+ `c`, `c/g`, the volume `v`) and, with `copy`, the marked copy `bk` (+ `bk/h1/pool`)."""
    world = SnapWorld(mock_subprocess)
    world.add(DS).add(CH).add(GC).add(VOL, ptype='volume')
    if copy:
        world.add(COPY, copy_props).add(COPY + '/h1').add(COPY + '/h1/pool')
    return world


@pytest.fixture
def world(mock_subprocess):
    return build_world(mock_subprocess)


@pytest.fixture
def clock(frozen_clock):
    return frozen_clock


def at(clock, offset=timedelta(0)):
    """The name the manager generates `offset` after the frozen clock's current time (hourly alignment)."""
    when = clock.now + offset
    when = when.replace(minute=0, second=0, microsecond=0)
    return f"autosnap_{when.strftime('%Y%m%d%H%M%S')}"


def make(name=DS, **kw):
    kw.setdefault('retention', False)
    return make_manager(name, **kw)


def errors(caplog, containing=''):
    return records(caplog, ERR, containing)


def copy_errors(caplog, top=None):
    # (`' is a backup copy ('`: the per-copy message, not the dataset-level one's "If pool is a backup copy,")
    return errors(caplog, ' is a backup copy (' if top is None else f'{top} is a backup copy (')


def fallback_errors(caplog):
    return errors(caplog, 'Cannot keep the backup copies out of the snapshot')


def inside_errors(caplog, dataset=None):
    """The ERROR for a recursive dataset that ITSELF has or inherits a copy marker (`_inside_copy_message`)."""
    return errors(caplog, 'itself sits inside a backup copy' if dataset is None
                  else f'{dataset} itself sits inside a backup copy')


def next_cycle(clock):
    clock.advance(timedelta(hours=1))


# ---------------------------------------------------------------------------
# What does not change
# ---------------------------------------------------------------------------


class TestNonRecursiveIsUnchanged:
    def test_one_snapshot_command_and_no_extra_read(self, world, clock):
        manager, dsi = make(recursive=False)
        snap = manager.create_snapshot(dsi)
        assert world.argvs == [['snapshot', f'{DS}@{NAME}'], SNAPSHOT_LIST]
        assert snap.full_name == f'{DS}@{NAME}' and snap.createtxg is not None
        assert world.snapshots_below(DS) == {DS: {NAME}}

    def test_copies_below_are_never_looked_for(self, world, clock, mocker):
        manager, dsi = make(recursive=False, targets=[COPY], server_target='pool/data/srv')
        tops = mocker.patch.object(manager, '_copy_tops')
        manager.create_snapshot(dsi)
        tops.assert_not_called()
        assert [a[0] for a in world.argvs] == ['snapshot', 'list']

    def test_a_failure_is_reported_as_before(self, world, clock, caplog):
        world.add_snap(DS, NAME)
        manager, dsi = make(recursive=False)
        with caplog.at_level(INFO, logger=LOGGER):
            assert manager.create_snapshot(dsi) is None
        assert manager.last_snapshot_error.startswith(f'Failed to create snapshot {DS}@{NAME}: ')
        assert 'already exists' in manager.last_snapshot_error
        assert len(errors(caplog, 'Failed to create snapshot')) == 1


class TestDryRun:
    def test_nothing_runs_not_even_a_read(self, world, mock_subprocess, clock, caplog):
        manager, dsi = make(dry_run=True, targets=[COPY])
        with caplog.at_level(INFO, logger=LOGGER):
            assert manager.create_snapshot(dsi) is None
        assert mock_subprocess.call_count == 0 and world.argvs == []
        assert manager.last_snapshot_error is None
        assert records(caplog, INFO, f'[DRY RUN] Would create snapshot: {DS}@{NAME}')
        assert manager._snapshot_copy_noted == set() and manager._snapshot_fallback_noted == set()
        assert errors(caplog) == []


# ---------------------------------------------------------------------------
# No copy: plain `snapshot -r`
# ---------------------------------------------------------------------------


class TestNoCopyBelow:
    def test_plain_recursive_snapshot_with_exact_argv(self, mock_subprocess, clock):
        world = build_world(mock_subprocess, copy=False)
        manager, dsi = make()
        snap = manager.create_snapshot(dsi)
        assert world.argvs == [
            copy_read_argv(DS),
            ['snapshot', '-r', f'{DS}@{NAME}'],         # libzfseasy execs FIRST ...
            TREE_LIST,                                  # ... and lists the tree afterwards, best effort
            SNAPSHOT_LIST,
        ]
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']]
        assert snap.full_name == f'{DS}@{NAME}'

    def test_every_dataset_and_the_volume_gets_it_in_one_txg(self, mock_subprocess, clock):
        world = build_world(mock_subprocess, copy=False)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert set(world.snapshots_below(DS)) == {DS, CH, GC, VOL}
        assert len({txg for _, _, txg in world.created}) == 1

    def test_nothing_is_logged_or_remembered(self, mock_subprocess, clock, caplog):
        build_world(mock_subprocess, copy=False)
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert errors(caplog) == [] and records(caplog, logging.WARNING) == []
        assert manager._snapshot_copy_noted == set() and manager._snapshot_fallback_noted == set()

    def test_a_marker_on_the_dataset_itself_is_not_a_copy_below_it(self, mock_subprocess, clock):
        world = build_world(mock_subprocess, copy=False)
        world.set_props(DS, LOCAL)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']]

    def test_a_copy_outside_the_tree_is_not_one_below_it(self, mock_subprocess, clock):
        world = build_world(mock_subprocess, copy=False)
        world.add('pool/data2', LOCAL).add('pool/other/bk', LOCAL)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']]
        assert world.snapshots_below('pool/data2') == {} and world.snapshots_below('pool/other') == {}

    @pytest.mark.parametrize('props', [
        [('org.zfsbackup:config', 'x', 'local')],
        [('org.zfsbackup:anchor.off', 'x', 'local'), ('org.zfsbackup:anchor.off2', 'x', 'received')],
        [('org.other:client_id', 'h1', 'local')],
    ], ids=['config', 'anchors', 'foreign-namespace'])
    def test_other_properties_do_not_make_a_copy(self, mock_subprocess, clock, props):
        world = build_world(mock_subprocess, copy=False)
        world.add(COPY, props)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']]
        assert world.names_of(COPY) == {NAME}

    def test_a_receive_target_that_does_not_exist_holds_nothing(self, mock_subprocess, clock, caplog):
        world = build_world(mock_subprocess, copy=False)
        manager, dsi = make(targets=[COPY])
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert exists_argv(COPY) in world.argvs                          # checked, found missing
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']]
        assert errors(caplog) == []

    def test_a_target_outside_the_tree_is_not_checked(self, mock_subprocess, clock):
        world = build_world(mock_subprocess, copy=False)
        manager, dsi = make(targets=['bk/copies'], server_target='srv/backups')
        manager.create_snapshot(dsi)
        assert [a for a in world.argvs if a[:4] == ['list', '-H', '-o', 'name,type']] == []
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']]


# ---------------------------------------------------------------------------
# Copies: ONE atomic snapshot of everything else
# ---------------------------------------------------------------------------


class TestCopiesAreSkipped:
    def test_exact_argv_the_dataset_first_then_the_sources_in_listing_order(self, world, clock):
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert world.argvs == [
            copy_read_argv(DS),
            TREE_LIST,
            ['snapshot', f'{DS}@{NAME}', f'{CH}@{NAME}', f'{GC}@{NAME}', f'{VOL}@{NAME}'],
            SNAPSHOT_LIST,
        ]

    def test_no_snapshot_exists_in_the_copy_and_every_source_has_one_in_one_txg(self, world, clock):
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert world.snapshots_below(COPY) == {}
        assert world.created_in(COPY) == []
        assert world.snapshots_below(DS) == {d: {NAME} for d in (DS, CH, GC, VOL)}
        assert len({txg for _, _, txg in world.created}) == 1, 'one atomic command, one txg'
        assert len(world.commands_of('snapshot')) == 1 and '-r' not in world.commands_of('snapshot')[0]

    def test_the_listing_is_the_strict_name_only_one(self, world, clock, mocker):
        import libzfseasy as zfs
        real = zfs.list
        calls = []

        def spy(**kw):
            calls.append(kw)
            return real(**kw)

        mocker.patch('zfsbackup.backup_manager.zfs.list', side_effect=spy)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        tree = [c for c in calls if c.get('types') == 'filesystem,volume']
        assert len(tree) == 1
        assert tree[0]['strict'] is True and tree[0]['recursive'] is True and tree[0]['roots'].name == DS
        assert 'properties' not in tree[0] or not tree[0]['properties']      # names only: no value can forge a row

    def test_the_returned_snapshot_is_the_datasets_own_and_ordered(self, world, clock):
        manager, dsi = make()
        snap = manager.create_snapshot(dsi)
        assert snap.full_name == f'{DS}@{NAME}' and snap.createtxg == world.txg

    def test_the_tree_returns_nothing_and_the_command_names_the_dataset_first(self, world, clock):
        """`_snapshot_tree` returns None (the caller builds the snapshot object from `dsi` and the name, so no
        shape of `many`'s or `snapshot -r`'s result is relied on); the command lists the dataset first."""
        manager, dsi = make()
        assert manager._snapshot_tree(dsi, NAME) is None
        (cmd,) = world.commands_of('snapshot')
        assert [a.partition('@')[0] for a in cmd[1:]] == [DS, CH, GC, VOL]
        assert cmd[1] == f'{DS}@{NAME}'

    def test_one_error_per_copy_names_it_why_and_the_remedy(self, world, clock, caplog):
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        (msg,) = copy_errors(caplog)
        assert msg.startswith(f'{COPY} is a backup copy (it has {PROP_CLIENT_ID} or {PROP_SOURCE_DATASET}, source: local)')
        assert f'below the recursive dataset {DS}' in msg and 'NOT snapshotted' in msg
        assert f'{DS}@{NAME}' in msg and f'zfs rename {COPY}' in msg
        assert records(caplog, INFO, f'Successfully created snapshot: {DS}@{NAME}')

    def test_a_failed_relist_leaves_the_bare_snapshot_and_still_reports_the_copy(self, world, clock, caplog):
        world.fail(lambda a: a[:1] == ['list'] and 'snapshot' in a and '-r' not in a, 'cannot open: busy')
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            snap = manager.create_snapshot(dsi)
        assert snap.full_name == f'{DS}@{NAME}' and snap.createtxg is None
        assert records(caplog, logging.WARNING, f'Created {DS}@{NAME} but could not re-list {DS}')
        assert len(copy_errors(caplog)) == 1 and world.snapshots_below(COPY) == {}

    def test_the_error_comes_after_the_snapshot_was_taken(self, world, clock, caplog):
        manager, dsi = make()
        seen = []
        world.on_exec = lambda argv: seen.append((argv[0], len(copy_errors(caplog))))
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert ('snapshot', 0) in seen and len(copy_errors(caplog)) == 1

    @pytest.mark.parametrize('props, source', [
        (LOCAL, 'local'),
        (RECEIVED, 'received'),
        ([(PROP_CLIENT_ID, 'h1', 'local')], 'local'),
        ([(PROP_SOURCE_DATASET, 'pool/x', 'received')], 'received'),
    ], ids=['local-both', 'received-both', 'client-id-only', 'source-only'])
    def test_either_marker_from_either_source_makes_a_copy(self, mock_subprocess, clock, caplog, props, source):
        world = build_world(mock_subprocess, copy_props=props)
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert world.snapshots_below(COPY) == {}
        assert f'source: {source})' in copy_errors(caplog, COPY)[0]

    def test_a_copy_that_inherits_its_marker_is_one_top_with_its_whole_subtree(self, mock_subprocess, clock, caplog):
        world = build_world(mock_subprocess, copy=False)
        world.add(COPY, LOCAL).add(COPY + '/h1', inherited(COPY)).add(COPY + '/h1/pool', inherited(COPY))
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert world.snapshots_below(COPY) == {}
        assert len(copy_errors(caplog)) == 1, 'the topmost one only'

    def test_a_nested_marked_copy_below_a_copy_is_not_a_second_top(self, world, clock, caplog):
        world.set_props(COPY + '/h1/pool', LOCAL)
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert len(copy_errors(caplog)) == 1 and copy_errors(caplog, COPY)

    def test_two_copies_are_two_errors_and_still_one_atomic_command(self, world, clock, caplog):
        world.add('pool/data/bk2', RECEIVED).add(CH + '/bk3', LOCAL)
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert len(world.commands_of('snapshot')) == 1
        assert sorted(m.split(' is a backup copy')[0] for m in copy_errors(caplog)) == [
            COPY, 'pool/data/bk2', CH + '/bk3']
        assert world.snapshots_below(COPY) == {} and world.snapshots_below('pool/data/bk2') == {}
        assert world.snapshots_below(CH + '/bk3') == {}
        assert set(world.snapshots_below(DS)) == {DS, CH, GC, VOL}


class TestExclusionIsByComponent:
    """`c` and `c/x` are dropped with the copy `c`; `cc` and `c-old` are not below it."""

    def test_siblings_sharing_a_name_prefix_stay_sources(self, mock_subprocess, clock):
        world = build_world(mock_subprocess, copy=False)
        for sibling in (COPY + 'x', COPY + '2', COPY + '-old', COPY + '.d'):
            world.add(sibling)
        world.add(COPY, LOCAL).add(COPY + '/x').add(COPY + '/x/y', ptype='volume')
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert world.snapshots_below(COPY) == {}
        (cmd,) = world.commands_of('snapshot')
        targets = [a.partition('@')[0] for a in cmd[1:]]
        assert targets[0] == DS
        assert set(targets) == {DS, CH, GC, VOL, COPY + 'x', COPY + '2', COPY + '-old', COPY + '.d'}
        assert COPY not in targets and COPY + '/x' not in targets and COPY + '/x/y' not in targets

    def test_sources_below_a_source_with_a_copy_inside_it(self, mock_subprocess, clock):
        world = build_world(mock_subprocess, copy=False)
        world.add(GC + '/deep').add(CH + '/bk', LOCAL).add(CH + '/bk/in')
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert world.snapshots_below(CH + '/bk') == {}
        assert set(world.snapshots_below(DS)) == {DS, CH, GC, GC + '/deep', VOL}

    def test_the_copy_may_be_a_volume(self, mock_subprocess, clock):
        world = build_world(mock_subprocess, copy=False)
        world.add(COPY + 'vol', LOCAL, ptype='volume')
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert COPY + 'vol' not in world.snapshots_below(DS)
        assert set(world.snapshots_below(DS)) == {DS, CH, GC, VOL}

    def test_sources_of_unit(self, mocker):
        """`_snapshot_sources` over a listing with `c`, `c/x`, `cc` and `c-old`: the dataset first, the order kept."""
        manager, dsi = make()
        listed = [Filesystem(n) for n in (DS, CH, CH + '/x', CH + 'c', CH + '-old', CH + '/x/y', 'pool/data/d')]
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=listed)
        out = manager._snapshot_sources(dsi, {CH: 'why'})
        assert [d.name for d in out] == [DS, CH + 'c', CH + '-old', 'pool/data/d']

    def test_sources_of_keeps_the_dataset_first_whatever_the_listing_order(self, mocker):
        manager, dsi = make()
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[Filesystem(CH), Filesystem(DS)])
        assert [d.name for d in manager._snapshot_sources(dsi, {'pool/data/zz': 'w'})] == [DS, CH]


class TestReceiveTargetCopies:
    """A copy with no marker at or below a known receive target (a `send -R` replica, a copy whose properties
    were cleared)."""

    def tree(self, mock_subprocess):
        world = build_world(mock_subprocess, copy=False)
        world.add(COPY).add(COPY + '/h1').add(COPY + '/h1/pool')               # no markers anywhere
        return world

    def test_a_local_target_below_the_dataset_is_a_top(self, mock_subprocess, clock, caplog):
        world = self.tree(mock_subprocess)
        manager, dsi = make(targets=[COPY])
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert exists_argv(COPY) in world.argvs
        assert world.snapshots_below(COPY) == {}
        assert set(world.snapshots_below(DS)) == {DS, CH, GC, VOL}
        (msg,) = copy_errors(caplog, COPY)
        assert f'it is the receive target {COPY}' in msg

    def test_the_server_target_counts_whether_or_not_remote_backup_is_enabled(self, mock_subprocess, clock):
        world = self.tree(mock_subprocess)
        manager, dsi = make(server_target=COPY)
        manager.config.remote_backup.enabled = False
        manager.create_snapshot(dsi)
        assert world.snapshots_below(COPY) == {}

    def test_a_shared_target_is_one_top(self, mock_subprocess, clock, caplog):
        world = self.tree(mock_subprocess)
        manager, dsi = make(targets=[COPY], server_target=COPY)
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert len(copy_errors(caplog)) == 1 and world.snapshots_below(COPY) == {}

    def test_two_targets_two_tops(self, mock_subprocess, clock, caplog):
        world = self.tree(mock_subprocess)
        world.add('pool/data/lcl').add('pool/data/lcl/h1')
        manager, dsi = make(targets=[COPY, 'pool/data/lcl'])
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert len(copy_errors(caplog)) == 2 and len(world.commands_of('snapshot')) == 1
        assert world.snapshots_below('pool/data/lcl') == {} and world.snapshots_below(COPY) == {}

    def test_a_trailing_slash_on_the_target_is_ignored(self, mock_subprocess, clock):
        world = self.tree(mock_subprocess)
        manager, dsi = make(targets=[COPY + '/'])
        manager.create_snapshot(dsi)
        assert world.snapshots_below(COPY) == {}

    def test_a_target_deeper_than_a_marked_copy_adds_no_top(self, mock_subprocess, clock, caplog):
        world = self.tree(mock_subprocess)
        world.set_props(COPY, LOCAL)
        manager, dsi = make(targets=[COPY + '/h1'])
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert len(copy_errors(caplog)) == 1 and copy_errors(caplog, COPY)
        assert world.snapshots_below(COPY) == {}

    def test_a_marker_and_a_target_on_one_dataset_are_one_error_with_the_marker_as_the_reason(
            self, mock_subprocess, clock, caplog):
        world = self.tree(mock_subprocess)
        world.set_props(COPY, LOCAL)
        manager, dsi = make(targets=[COPY])
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        (msg,) = copy_errors(caplog)
        assert 'source: local' in msg
        assert world.snapshots_below(COPY) == {}


# ---------------------------------------------------------------------------
# The dataset inside a copy, and one that merely inherits a marker
# ---------------------------------------------------------------------------


class TestTheDatasetItself:
    def inside(self, mock_subprocess, target='bk', dataset='bk/x'):
        world = SnapWorld(mock_subprocess)
        world.add(target).add(dataset).add(dataset + '/c').add(dataset + '/v', ptype='volume')
        return world

    @pytest.mark.parametrize('dataset', ['bk/x', 'bk'], ids=['below', 'equal'])
    def test_at_or_below_a_receive_target_nothing_is_snapshotted(self, mock_subprocess, clock, caplog, dataset):
        world = self.inside(mock_subprocess, dataset=dataset) if dataset != 'bk' else SnapWorld(mock_subprocess)
        if dataset == 'bk':
            world.add('bk').add('bk/c')
        manager, dsi = make(dataset, targets=['bk'])
        with caplog.at_level(INFO, logger=LOGGER):
            assert manager.create_snapshot(dsi) is None
        assert world.commands_of('snapshot') == [], 'neither `-r` nor the dataset alone'
        assert world.created == []
        assert manager.last_snapshot_error.startswith(f'Failed to create snapshot {dataset}@{NAME}: ')
        assert f'{dataset} is itself inside a backup copy' in manager.last_snapshot_error
        assert 'receive target bk' in manager.last_snapshot_error
        (msg,) = errors(caplog, 'Failed to create snapshot')
        assert 'rename' in msg or 'remove it from the configuration' in msg

    def test_it_is_neither_a_fallback_nor_a_copy_episode(self, mock_subprocess, clock, caplog):
        self.inside(mock_subprocess)
        manager, dsi = make('bk/x', targets=['bk'])
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert manager._snapshot_fallback_noted == set() and manager._snapshot_copy_noted == set()
        assert fallback_errors(caplog) == [] and copy_errors(caplog) == []

    def test_the_server_target_is_one_too(self, mock_subprocess, clock):
        world = self.inside(mock_subprocess)
        manager, dsi = make('bk/x', server_target='bk')
        assert manager.create_snapshot(dsi) is None
        assert world.commands_of('snapshot') == []
        assert 'receive target bk' in manager.last_snapshot_error

    def test_every_cycle_fails_the_same_way_and_a_later_one_still_snapshots_nothing(self, mock_subprocess, clock):
        world = self.inside(mock_subprocess)
        manager, dsi = make('bk/x', targets=['bk'])
        for _ in range(3):
            assert manager.create_snapshot(dsi) is None
            next_cycle(clock)
        assert world.commands_of('snapshot') == []

    def test_a_dataset_that_does_not_exist_is_a_fallback_that_zfs_then_refuses(self, mock_subprocess, clock, caplog):
        """Nothing to read: the copy read fails, and the fallback's plain `snapshot -r` is exec'd (libzfseasy no
        longer lists first) and refused by zfs: nothing is created, and there is no listing after it."""
        world = SnapWorld(mock_subprocess)
        manager, dsi = make('bk/x', targets=['bk'])
        with caplog.at_level(INFO, logger=LOGGER):
            assert manager.create_snapshot(dsi) is None
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'bk/x@{NAME}']] and world.created == []
        assert world.listings() == [] and world.argvs[-1][0] == 'snapshot', 'nothing is listed after a refusal'
        assert 'does not exist' in manager.last_snapshot_error
        assert len(fallback_errors(caplog)) == 1

    def test_the_refusal_is_an_error_once_then_debug_then_an_info_when_it_is_resolved(
            self, mock_subprocess, clock, caplog):
        """`_InsideReceiveTarget`: ERROR the first cycle, DEBUG after (`last_snapshot_error` is set EVERY cycle, so
        the status record still says why), INFO when the dataset is no longer in a target."""
        world = self.inside(mock_subprocess)
        manager, dsi = make('bk/x', targets=['bk'])
        with caplog.at_level(DEBUG, logger=LOGGER):
            for _ in range(3):
                manager.last_snapshot_error = None
                assert manager.create_snapshot(dsi) is None
                assert manager.last_snapshot_error.startswith(f'Failed to create snapshot bk/x@')
                next_cycle(clock)
            assert len(errors(caplog, 'Failed to create snapshot')) == 1
            assert len(records(caplog, DEBUG, 'Failed to create snapshot')) == 2
            assert manager._snapshot_inside_target_noted == {'bk/x'}
            manager.config.destinations.clear()                      # the target is no longer configured
            assert manager.create_snapshot(dsi) is not None and manager.last_snapshot_error is None
        (back,) = records(caplog, INFO, 'is no longer at or below a receive target')
        assert back.startswith('bk/x ')
        assert manager._snapshot_inside_target_noted == set()
        assert len(errors(caplog, 'Failed to create snapshot')) == 1
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'bk/x@{at(clock)}']]
        # a later episode is announced again
        manager.config.destinations['l'] = Destination(url='zfs:bk')
        next_cycle(clock)
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert manager.create_snapshot(dsi) is None
        assert len(errors(caplog, 'Failed to create snapshot')) == 2

    def test_each_dataset_has_its_own_target_episode(self, mock_subprocess, clock, caplog):
        world = self.inside(mock_subprocess)
        world.add('bk/y').add('bk/y/c')
        manager, first = make('bk/x', targets=['bk'], extra=[DatasetConfig(name='bk/y', recursive=True)])
        second = next(d for d in manager.datasets if d.name == 'bk/y')
        with caplog.at_level(DEBUG, logger=LOGGER):
            manager.create_snapshot(first)
            manager.create_snapshot(second)
            manager.create_snapshot(first)
        assert len(errors(caplog, 'Failed to create snapshot')) == 2
        assert manager._snapshot_inside_target_noted == {'bk/x', 'bk/y'}

    def test_a_dry_run_never_touches_the_target_episode(self, mock_subprocess, clock):
        self.inside(mock_subprocess)
        manager, dsi = make('bk/x', targets=['bk'], dry_run=True)
        manager.create_snapshot(dsi)
        assert manager._snapshot_inside_target_noted == set()

    def test_a_non_recursive_dataset_in_a_target_is_not_this_code(self, mock_subprocess, clock):
        """Unchanged behaviour (`validate_config` refuses the layout): the plain single snapshot."""
        world = self.inside(mock_subprocess)
        manager, dsi = make('bk/x', recursive=False, targets=['bk'])
        assert manager.create_snapshot(dsi) is not None
        assert world.commands_of('snapshot') == [['snapshot', f'bk/x@{NAME}']]


class TestInheritedMarker:
    """`pool/data` inherits a marker from above (it sits inside a copy, but is configured explicitly): it is
    snapshotted alone, its children inherit the marker and are copies BY INHERITANCE: one ERROR about the dataset
    (per origin, per episode) replaces one per child, whose `zfs rename <child>` remedy would be wrong."""

    def world(self, mock_subprocess):
        world = SnapWorld(mock_subprocess)
        world.add(DS, inherited('pool')).add(CH, inherited('pool')).add(GC, inherited('pool'))
        world.add(VOL, inherited('pool'), ptype='volume')
        return world

    def test_only_the_dataset_is_snapshotted_by_one_command(self, mock_subprocess, clock, caplog):
        world = self.world(mock_subprocess)
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            snap = manager.create_snapshot(dsi)
        assert world.argvs == [copy_read_argv(DS), TREE_LIST, ['snapshot', f'{DS}@{NAME}'], SNAPSHOT_LIST]
        assert world.snapshots_below(DS) == {DS: {NAME}}
        assert snap.full_name == f'{DS}@{NAME}'

    def test_one_error_about_the_dataset_and_none_per_child(self, mock_subprocess, clock, caplog):
        self.world(mock_subprocess)
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert copy_errors(caplog) == [], 'no per-child "is a backup copy" ERROR (and no `zfs rename <child>` advice)'
        (msg,) = inside_errors(caplog, DS)
        assert 'it inherits org.zfsbackup:client_id or org.zfsbackup:source_dataset from pool' in msg
        assert f'only {DS} itself is snapshotted (as {DS}@{NAME}); nothing below it is' in msg
        assert f'move {DS} out of it (zfs rename) or make {DS} non-recursive' in msg
        assert 'zfs inherit org.zfsbackup:client_id pool; zfs inherit org.zfsbackup:source_dataset pool' in msg
        assert 'only masks it' in msg and 'masked marker hides the copy' in msg
        assert manager._snapshot_inside_copy_noted == {(DS, 'pool')} and manager._snapshot_copy_noted == set()

    def test_it_is_not_a_failure(self, mock_subprocess, clock):
        self.world(mock_subprocess)
        manager, dsi = make()
        assert manager.create_snapshot(dsi) is not None and manager.last_snapshot_error is None

    def test_the_error_is_logged_once_across_cycles(self, mock_subprocess, clock, caplog):
        self.world(mock_subprocess)
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            for _ in range(3):
                manager.create_snapshot(dsi)
                next_cycle(clock)
        assert len(inside_errors(caplog)) == 1

    def test_the_info_when_nothing_below_inherits_the_marker_any_more_and_a_new_episode_logs_again(
            self, mock_subprocess, clock, caplog):
        world = self.world(mock_subprocess)
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
            for name in (DS, CH, GC, VOL):
                world.set_props(name, [])                              # the markers are cleared above
            next_cycle(clock)
            manager.create_snapshot(dsi)
            (back,) = records(caplog, INFO, 'by inheritance from pool any more')
            assert back.startswith(f'Nothing below {DS} is a backup copy by inheritance')
            assert manager._snapshot_inside_copy_noted == set()
            assert world.commands_of('snapshot')[-1][1] == '-r', 'plain recursive again'
            for name in (DS, CH, GC, VOL):
                world.set_props(name, inherited('pool'))
            next_cycle(clock)
            manager.create_snapshot(dsi)
        assert len(inside_errors(caplog)) == 2

    def test_a_childless_dataset_that_inherits_a_marker_stays_quiet(self, mock_subprocess, clock, caplog):
        """Nothing below it is a copy by inheritance, so there is nothing to say (and nothing to leave out)."""
        world = SnapWorld(mock_subprocess)
        world.add(DS, inherited('pool'))
        manager, dsi = make()
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert manager.create_snapshot(dsi) is not None
        assert errors(caplog) == [] and manager._snapshot_inside_copy_noted == set()
        assert manager.last_snapshot_note is None
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']]

    @pytest.mark.parametrize('props, shown, origin', [
        (LOCAL, 'it carries a local org.zfsbackup:client_id or org.zfsbackup:source_dataset', DS),
        (RECEIVED, 'it carries a received org.zfsbackup:client_id or org.zfsbackup:source_dataset', DS),
    ], ids=['local', 'received'])
    def test_a_marker_the_dataset_carries_itself_has_the_dataset_as_its_origin(
            self, mock_subprocess, clock, caplog, props, shown, origin):
        world = SnapWorld(mock_subprocess)
        world.add(DS, props).add(CH, inherited(DS)).add(GC, inherited(DS))
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        (msg,) = inside_errors(caplog, DS)
        assert shown in msg and f'clear the markers on {origin}' in msg
        assert world.snapshots_below(DS) == {DS: {NAME}}
        assert manager._snapshot_inside_copy_noted == {(DS, origin)}

    def test_a_real_copy_below_still_gets_its_own_error_beside_the_inheritance_one(
            self, mock_subprocess, clock, caplog):
        """`c` carries a LOCAL marker of its own (not the inherited one): it is a copy in its own right (and its
        `zfs rename` remedy is right), while `v`, which merely inherits, is covered by the dataset's message."""
        world = self.world(mock_subprocess)
        world.set_props(CH, LOCAL)
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert [m.split(' is a backup copy')[0] for m in copy_errors(caplog)] == [CH]
        assert len(inside_errors(caplog, DS)) == 1
        assert manager._snapshot_copy_noted == {(DS, CH)} and manager._snapshot_inside_copy_noted == {(DS, 'pool')}
        assert world.snapshots_below(DS) == {DS: {NAME}}

    def test_a_dataset_whose_own_marker_is_local_and_a_child_that_is_also_local_is_a_real_top(
            self, mock_subprocess, clock, caplog):
        world = SnapWorld(mock_subprocess)
        world.add(DS, LOCAL).add(CH, LOCAL).add(VOL, inherited(DS), ptype='volume')
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert [m.split(' is a backup copy')[0] for m in copy_errors(caplog)] == [CH]
        assert len(inside_errors(caplog, DS)) == 1


class TestSnapshotNote:
    """`DatasetManager.last_snapshot_note`: the standing warning of the last snapshot that WAS created (the
    snapshot worker puts it in the dataset's status record, `check` shows it)."""

    def test_no_copy_no_note(self, mock_subprocess, clock):
        build_world(mock_subprocess, copy=False)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_note is None

    def test_a_non_recursive_dataset_has_none(self, world, clock):
        manager, dsi = make(recursive=False)
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_note is None

    def test_one_copy(self, world, clock):
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_note == f'1 backup copy below was not snapshotted: {COPY}'

    def test_two_copies_sorted(self, world, clock):
        world.add('pool/data/aa', LOCAL)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_note == f'2 backup copies below were not snapshotted: pool/data/aa, {COPY}'

    def test_more_than_three_are_cut_with_a_count(self, mock_subprocess, clock):
        world = build_world(mock_subprocess, copy=False)
        for n in 'edcba':
            world.add(f'pool/data/z{n}', LOCAL)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_note == (
            '5 backup copies below were not snapshotted: pool/data/za, pool/data/zb, pool/data/zc (+2 more)')

    def test_exactly_three_has_no_count(self, mock_subprocess, clock):
        world = build_world(mock_subprocess, copy=False)
        for n in 'cab':
            world.add(f'pool/data/z{n}', LOCAL)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_note == (
            '3 backup copies below were not snapshotted: pool/data/za, pool/data/zb, pool/data/zc')

    def test_a_receive_target_copy_counts_like_a_marked_one(self, mock_subprocess, clock):
        world = TestReceiveTargetCopies().tree(mock_subprocess)
        manager, dsi = make(targets=[COPY])
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_note == f'1 backup copy below was not snapshotted: {COPY}'

    def test_the_dataset_inside_a_copy(self, mock_subprocess, clock):
        TestInheritedMarker().world(mock_subprocess)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_note == (
            f'{DS} sits inside a backup copy (pool): the datasets below it were not snapshotted')

    def test_the_inheritance_note_names_the_dataset_itself_for_a_local_marker(self, mock_subprocess, clock):
        world = SnapWorld(mock_subprocess)
        world.add(DS, LOCAL).add(CH, inherited(DS))
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_note == (
            f'{DS} sits inside a backup copy ({DS}): the datasets below it were not snapshotted')

    def test_a_real_copy_below_an_inheriting_dataset_is_the_inheritance_note(self, mock_subprocess, clock):
        """The note about the dataset itself wins: it says the whole subtree was left out."""
        world = TestInheritedMarker().world(mock_subprocess)
        world.set_props(CH, LOCAL)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_note.startswith(f'{DS} sits inside a backup copy (pool)')

    def test_the_fallback_note(self, world, clock):
        world.fail(READ_ARGV, 'cannot open: pool I/O is currently suspended')
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_note.startswith(
            'backup copies below could not be told apart, so `zfs snapshot -r` was used and may have '
            'snapshotted into one (RuntimeError: ')
        assert manager.last_snapshot_note.endswith(')') and 'pool I/O is currently suspended' in manager.last_snapshot_note

    def test_the_note_is_there_every_cycle_not_only_when_the_error_is_logged(self, world, clock, caplog):
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            for _ in range(3):
                manager.last_snapshot_note = None
                manager.create_snapshot(dsi)
                assert manager.last_snapshot_note == f'1 backup copy below was not snapshotted: {COPY}'
                next_cycle(clock)
        assert len(copy_errors(caplog)) == 1

    def test_the_note_is_reset_by_the_next_snapshot(self, world, clock):
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_note is not None
        for name in (COPY, COPY + '/h1', COPY + '/h1/pool'):
            world.drop(name)
        next_cycle(clock)
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_note is None

    def test_a_failed_snapshot_leaves_no_note(self, world, clock):
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_note is not None
        world.add_snap(DS, at(clock, timedelta(hours=1)))                 # the next name collides
        next_cycle(clock)
        assert manager.create_snapshot(dsi) is None
        assert manager.last_snapshot_note is None and manager.last_snapshot_error is not None

    def test_a_refusal_inside_a_target_leaves_no_note(self, mock_subprocess, clock):
        world = SnapWorld(mock_subprocess)
        world.add('bk').add('bk/x').add('bk/x/c')
        manager, dsi = make('bk/x', targets=['bk'])
        assert manager.create_snapshot(dsi) is None and manager.last_snapshot_note is None

    def test_a_dry_run_has_none(self, world, clock):
        manager, dsi = make(dry_run=True, targets=[COPY])
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_note is None


class TestCreateSnapshotDoesNotDependOnWhatZfsSnapshotReturns:
    """`create_snapshot` builds the snapshot object from `dsi` and the name: a result that is empty, lacks the
    dataset or lists a child first is no problem (it used to be `result[0]`)."""

    @pytest.mark.parametrize('result', [[], [Filesystem(CH)], None, 'x'], ids=['empty', 'child-only', 'none', 'str'])
    def test_a_non_recursive_dataset(self, mocker, clock, result):
        manager, dsi = make(recursive=False)
        mocker.patch('zfsbackup.backup_manager.zfs.snapshot', return_value=result)
        mocker.patch.object(manager, '_list_snapshots_strict', side_effect=Exception('no list'), create=True)
        snap = manager.create_snapshot(dsi)
        assert snap is not None and snap.full_name == f'{DS}@{NAME}'
        assert manager.last_snapshot_error is None

    @pytest.mark.parametrize('result', [[], [Filesystem(CH), Filesystem(DS)], None], ids=['empty', 'child-first', 'none'])
    def test_a_recursive_dataset_without_a_copy(self, mocker, clock, result):
        manager, dsi = make()
        mocker.patch.object(manager, '_snapshot_tree', side_effect=lambda d, n: None)
        calls = mocker.patch('zfsbackup.backup_manager.zfs.snapshot', return_value=result)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        snap = manager.create_snapshot(dsi)
        assert snap is not None and snap.full_name == f'{DS}@{NAME}'

    @pytest.mark.parametrize('result', [[], [Filesystem(CH)], [Filesystem(CH), Filesystem(DS)]],
                             ids=['empty', 'no-dataset', 'child-first'])
    def test_the_real_tree_path_returns_the_datasets_own_snapshot_whatever_snapshot_r_returned(
            self, world, clock, mocker, result):
        """No copy below, so the plain `snapshot -r` runs through the real code: only its exec is faked."""
        manager, dsi = make()
        for name in (COPY, COPY + '/h1', COPY + '/h1/pool'):
            world.drop(name)
        real = SnapshotCommand._snapshot.__func__
        mocker.patch.object(SnapshotCommand, '_snapshot', classmethod(lambda cls, ds, name, **kw: (
            real(cls, ds, name, **kw) and result)))
        snap = manager.create_snapshot(dsi)
        assert snap.full_name == f'{DS}@{NAME}'


# ---------------------------------------------------------------------------
# The fallback to plain `snapshot -r`
# ---------------------------------------------------------------------------

READ_ARGV = lambda a: a[:1] == ['get'] and 'local,received,inherited' in a                # noqa: E731
LIST_TREE_ARGV = lambda a: a[:1] == ['list'] and '-r' in a and 'filesystem,volume' in a   # noqa: E731


def inject_copy_read_failure(world, mocker, manager):
    world.fail(READ_ARGV, 'cannot open: pool I/O is currently suspended')
    return 'RuntimeError', 'cannot read the backup-copy markers'


def inject_timeout_from_copy_read(world, mocker, manager):
    mocker.patch.object(DatasetManager, '_copy_roots', side_effect=ZFSTimeoutError('zfs get took too long'))
    return 'ZFSTimeoutError', 'zfs get took too long'


def inject_output_error(world, mocker, manager):
    mocker.patch.object(manager, '_copy_tops', side_effect=ZFSOutputError('output could not be trusted'))
    return 'ZFSOutputError', 'output could not be trusted'


def inject_target_exists_failure(world, mocker, manager):
    world.fail(lambda a: a == exists_argv(COPY), 'cannot open: pool I/O is currently suspended')
    return 'RuntimeError', f'cannot check whether {COPY}'


def inject_target_exists_timeout(world, mocker, manager):
    mocker.patch('zfsbackup.backup_manager.zfs.exists', side_effect=ZFSTimeoutError('exists timed out'))
    return 'ZFSTimeoutError', 'exists timed out'


def inject_listing_failure(world, mocker, manager):
    world.fail_once(LIST_TREE_ARGV, 'cannot open: pool I/O is currently suspended')
    return 'RuntimeError', 'cannot list the datasets of pool/data'


def inject_untrusted_listing(world, mocker, manager):
    world.list_always([f'{DS}\tfilesystem\n', f'{CH}\tfilesystem\textra\n'])
    return 'RuntimeError', 'could not be trusted'


def inject_unrepresentable_name(world, mocker, manager):
    world.list_always([f'{DS}\tfilesystem\n', f'{DS}/h\u00e9llo\tfilesystem\n', f'{CH}\tfilesystem\n'])
    return 'RuntimeError', 'could not be trusted'


def inject_listing_without_the_dataset(world, mocker, manager):
    world.list_once([f'{CH}\tfilesystem\n'])
    return 'RuntimeError', f'does not contain {DS} itself'


def inject_duplicate_name(world, mocker, manager):
    world.list_always([f'{DS}\tfilesystem\n', f'{CH}\tfilesystem\n', f'{CH}\tfilesystem\n'])
    return 'RuntimeError', 'could not be trusted'


CAUSES = {
    'copy-read-fails': inject_copy_read_failure,
    'copy-read-timeout': inject_timeout_from_copy_read,
    'output-error': inject_output_error,
    'target-exists-fails': inject_target_exists_failure,
    'target-exists-timeout': inject_target_exists_timeout,
    'listing-fails': inject_listing_failure,
    'untrusted-listing': inject_untrusted_listing,
    'unrepresentable-name': inject_unrepresentable_name,
    'listing-lacks-dataset': inject_listing_without_the_dataset,
    'duplicate-name': inject_duplicate_name,
}


def arm(name, world, mocker, manager):
    return CAUSES[name](world, mocker, manager)


@pytest.fixture
def world_with_target(mock_subprocess):
    """The standard tree plus a copy WITHOUT markers at the local target `COPY` (so the `exists` check runs too)."""
    world = SnapWorld(mock_subprocess)
    world.add(DS).add(CH).add(GC).add(VOL, ptype='volume').add(COPY).add(COPY + '/h1')
    return world


class TestFallbackToPlainRecursive:
    @pytest.mark.parametrize('cause', sorted(CAUSES))
    def test_each_cause_ends_in_one_plain_recursive_snapshot_and_one_error(
            self, world_with_target, clock, caplog, mocker, cause):
        world = world_with_target
        manager, dsi = make(targets=[COPY])
        kind, text = arm(cause, world, mocker, manager)
        with caplog.at_level(DEBUG, logger=LOGGER):
            snap = manager.create_snapshot(dsi)
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']], 'never a partial `many`'
        assert snap is not None and manager.last_snapshot_error is None
        (msg,) = fallback_errors(caplog)
        assert f'snapshot of {DS} ({kind}: ' in msg and text in msg
        assert 'falling back to `zfs snapshot -r`' in msg and 'This repeats until the read passes' in msg
        assert copy_errors(caplog) == [], 'no per-copy error when the copies are not known'
        assert manager._snapshot_fallback_noted == {DS} and manager._snapshot_copy_noted == set()
        # `-r` takes the snapshot into the copy too: that is the stated trade
        assert NAME in world.names_of(DS) and NAME in world.names_of(COPY)

    def test_a_listing_failure_is_not_retried_and_the_copy_read_was_made_first(self, world_with_target, clock, mocker):
        world = world_with_target
        manager, dsi = make(targets=[COPY])
        arm('listing-fails', world, mocker, manager)
        manager.create_snapshot(dsi)
        verbs = [a[0] for a in world.argvs]
        assert verbs.count('snapshot') == 1
        assert world.argvs[0] == copy_read_argv(DS)

    def test_the_argv_bound_falls_back_before_anything_is_created(self, world, clock, caplog, mocker):
        mocker.patch.object(SnapshotCommand, 'MANY_MAX_ARGV_BYTES', 40)
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            assert manager.create_snapshot(dsi) is not None
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']]
        (msg,) = fallback_errors(caplog)
        assert 'ValueError: Too many datasets for one atomic snapshot command' in msg
        assert copy_errors(caplog) == [], 'logged after `many` succeeds, which it did not'
        assert NAME in world.names_of(COPY)

    @pytest.mark.parametrize('delta, falls_back', [(0, False), (-1, True)], ids=['at-the-bound', 'one-over'])
    def test_the_bound_is_on_the_argv_size_in_bytes(self, world, clock, mocker, delta, falls_back):
        cmd = [_zfs_cmd(), 'snapshot'] + [f'{d}@{NAME}' for d in (DS, CH, GC, VOL)]
        size = sum(len(a.encode()) + 1 for a in cmd)
        mocker.patch.object(SnapshotCommand, 'MANY_MAX_ARGV_BYTES', size + delta)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        (cmd_run,) = world.commands_of('snapshot')
        assert (cmd_run[1] == '-r') is falls_back
        assert (NAME in world.names_of(COPY)) is falls_back

    def test_a_dataset_that_zfs_refuses_to_name_falls_back_too(self, world, clock, mocker):
        """`many`'s own validation raises before zfs runs (here: an invalid name in the listing's objects)."""
        manager, dsi = make()
        mocker.patch.object(manager, '_snapshot_sources', return_value=[Filesystem(DS), Filesystem('other/x')])
        manager.create_snapshot(dsi)                 # two pools: `many` raises ValueError
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']]

    def test_sources_raises_for_a_name_outside_the_dataset(self, mocker):
        manager, dsi = make()
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[Filesystem(DS), Filesystem('pool/other')])
        with pytest.raises(RuntimeError, match='pool/other, which is outside it'):
            manager._snapshot_sources(dsi, {COPY: 'why'})

    def test_sources_raises_for_a_name_that_only_shares_the_prefix(self, mocker):
        manager, dsi = make()
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[Filesystem(DS), Filesystem(DS + '2/x')])
        with pytest.raises(RuntimeError, match='outside it'):
            manager._snapshot_sources(dsi, {COPY: 'why'})

    def test_sources_raises_when_the_listing_lacks_the_dataset(self, mocker):
        manager, dsi = make()
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[Filesystem(CH)])
        with pytest.raises(RuntimeError, match=f'does not contain {DS} itself'):
            manager._snapshot_sources(dsi, {COPY: 'why'})

    def test_sources_wraps_a_list_failure_with_the_cause_chained(self, mocker):
        manager, dsi = make()
        mocker.patch('zfsbackup.backup_manager.zfs.list', side_effect=OSError('gone'))
        with pytest.raises(RuntimeError, match=f'cannot list the datasets of {DS}: gone') as info:
            manager._snapshot_sources(dsi, {COPY: 'why'})
        assert isinstance(info.value.__cause__, OSError)

    def test_sources_lists_untimed_and_strict(self, world, mocker):
        """Source datasets are never timed: the listing runs outside any `timeout()` context."""
        import libzfseasy as zfs
        import libzfseasy.zfs as lz
        real = zfs.list
        seen = []

        def spy(**kw):
            seen.append((kw.get('strict'), lz._timeout_var.get()))
            return real(**kw)

        mocker.patch('zfsbackup.backup_manager.zfs.list', side_effect=spy)
        manager, dsi = make()
        manager._snapshot_sources(dsi, {COPY: 'why'})
        assert seen == [(True, None)]


BAD = 'h\u00e9llo'                                  # a name libzfseasy cannot represent: a strict listing refuses it
DEPTH1 = lambda name: ['list', '-H', '-d', '1', '-t', 'filesystem,volume', '-o', 'name,type', name]  # noqa: E731
RECURSIVE = lambda name: ['list', '-H', '-r', '-t', 'filesystem,volume', '-o', 'name,type', name]    # noqa: E731


class TestWalkAroundTheCopies:
    """`_snapshot_sources` lists the tree once, recursively. Only when that listing is refused as untrusted does it
    WALK instead (`_walk_sources`): each dataset with a copy top below it is listed one level (`-d 1`), every other
    child recursively, and a copy top is never listed, so a bad name INSIDE a copy is never read and the atomic
    snapshot survives it; a bad name anywhere else, or one that is itself a top, still ends in the fallback."""

    def bad_inside_copy(self, mock_subprocess, name=COPY + '/h1/' + BAD):
        world = build_world(mock_subprocess)
        world.add(name)
        return world

    def test_a_bad_name_inside_a_copy_keeps_the_atomic_snapshot(self, mock_subprocess, clock, caplog):
        world = self.bad_inside_copy(mock_subprocess)
        manager, dsi = make()
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert manager.create_snapshot(dsi) is not None
        (cmd,) = world.commands_of('snapshot')
        assert cmd == ['snapshot', f'{DS}@{NAME}', f'{CH}@{NAME}', f'{GC}@{NAME}', f'{VOL}@{NAME}'], 'atomic, not -r'
        assert world.snapshots_below(COPY) == {} and world.created_in(COPY) == []
        assert fallback_errors(caplog) == [] and manager._snapshot_fallback_noted == set()
        assert len(copy_errors(caplog, COPY)) == 1, 'the copy is still announced'
        assert manager.last_snapshot_note == f'1 backup copy below was not snapshotted: {COPY}'

    def test_the_exact_listings_and_a_top_is_never_listed(self, mock_subprocess, clock):
        world = self.bad_inside_copy(mock_subprocess)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert world.listings() == [
            RECURSIVE(DS),                     # refused as untrusted
            DEPTH1(DS),                        # the walk: one level of the dataset with a top below it
            RECURSIVE(CH),                     # every other child, recursively (its names are strict too)
            RECURSIVE(VOL),
        ]
        for argv in world.listings():
            target = argv[-1]
            assert target != COPY and not target.startswith(COPY + '/'), 'a top (or anything in it) is never listed'

    def test_the_recursive_listing_runs_once_and_the_walk_only_after_it_is_refused(self, world, clock):
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert world.listings(recursive=True) == [RECURSIVE(DS)] and world.listings(depth=1) == []

    def test_a_copy_inside_a_source_child_is_walked_through_that_child(self, mock_subprocess, clock):
        world = build_world(mock_subprocess, copy=False)
        world.add(CH + '/bk', LOCAL).add(CH + '/bk/' + BAD)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        (cmd,) = world.commands_of('snapshot')
        assert cmd == ['snapshot', f'{DS}@{NAME}', f'{CH}@{NAME}', f'{GC}@{NAME}', f'{VOL}@{NAME}']
        assert world.listings() == [RECURSIVE(DS), DEPTH1(DS), DEPTH1(CH), RECURSIVE(GC), RECURSIVE(VOL)]
        assert world.snapshots_below(CH + '/bk') == {}

    def test_two_copies_at_different_depths(self, mock_subprocess, clock):
        world = build_world(mock_subprocess, copy=False)
        world.add(COPY, LOCAL).add(COPY + '/' + BAD).add(GC + '/bk', LOCAL).add(GC + '/bk/' + BAD)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        (cmd,) = world.commands_of('snapshot')
        assert cmd == ['snapshot', f'{DS}@{NAME}', f'{CH}@{NAME}', f'{GC}@{NAME}', f'{VOL}@{NAME}']
        assert world.snapshots_below(COPY) == {} and world.snapshots_below(GC + '/bk') == {}

    def test_a_bad_name_outside_the_copies_still_falls_back(self, mock_subprocess, clock, caplog):
        world = build_world(mock_subprocess)
        world.add(CH + '/' + BAD)
        manager, dsi = make()
        with caplog.at_level(DEBUG, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']]
        (msg,) = fallback_errors(caplog)
        assert 'could not be trusted' in msg and f'cannot list the datasets of {DS}' in msg
        assert NAME in world.names_of(COPY), 'the stated trade: a missed source snapshot is worse'

    def test_a_bad_name_directly_below_the_dataset_falls_back(self, mock_subprocess, clock, caplog):
        world = build_world(mock_subprocess)
        world.add(DS + '/' + BAD)
        manager, dsi = make()
        with caplog.at_level(DEBUG, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']]
        assert len(fallback_errors(caplog)) == 1
        assert world.listings(depth=1) == [DEPTH1(DS)], 'the walk was tried, and refused at the first level'

    def test_a_bad_name_that_is_itself_a_copy_top_falls_back(self, mock_subprocess, clock, caplog):
        """The top's own name is in the one-level listing of its parent (a top is never listed, but its parent
        shows it): it cannot be told from a source's bad name."""
        world = build_world(mock_subprocess, copy=False)
        world.add(DS + '/' + BAD, LOCAL)
        manager, dsi = make()
        with caplog.at_level(DEBUG, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']]
        assert len(fallback_errors(caplog)) == 1 and copy_errors(caplog) == []

    def test_a_non_trust_error_is_not_walked(self, world, clock, caplog):
        world.fail_once(LIST_TREE_ARGV, 'cannot open: pool I/O is currently suspended')
        manager, dsi = make()
        with caplog.at_level(DEBUG, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert world.listings(depth=1) == [], 'only "could not be trusted" is walked'
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']]
        assert len(fallback_errors(caplog)) == 1

    @pytest.mark.parametrize('message, walks', [
        ('zfs list output could not be trusted: 1 row skipped, the first: x', True),
        ('Zfs list output could not be trusted', False),
        ('the zfs list output could not be trusted', False),
        ('zfs get output could not be trusted: 1 row refused', False),
    ], ids=['the-message', 'case', 'not-at-the-start', 'a-get-error'])
    def test_the_trigger_is_the_start_of_the_message(self, mock_subprocess, clock, mocker, message, walks):
        world = build_world(mock_subprocess)
        manager, dsi = make()
        import libzfseasy as zfs
        real = zfs.list
        calls = []

        def fake(**kw):
            calls.append(kw)
            if kw.get('recursive') and kw['roots'].name == DS and not kw.get('depth'):
                raise Exception(message)
            return real(**kw)

        mocker.patch('zfsbackup.backup_manager.zfs.list', side_effect=fake)
        manager.create_snapshot(dsi)
        assert any(c.get('depth') == 1 for c in calls) is walks
        assert ([a[1] == '-r' for a in world.commands_of('snapshot')] == [False]) is walks

    def test_a_failing_walk_listing_falls_back_with_its_message(self, mock_subprocess, clock, caplog):
        world = self.bad_inside_copy(mock_subprocess)
        world.fail(lambda a: a == DEPTH1(DS), 'cannot open: pool I/O is currently suspended')
        manager, dsi = make()
        with caplog.at_level(DEBUG, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']]
        (msg,) = fallback_errors(caplog)
        assert 'pool I/O is currently suspended' in msg

    def test_a_walk_listing_with_a_name_outside_its_node_falls_back(self, mock_subprocess, clock, caplog):
        world = self.bad_inside_copy(mock_subprocess)
        world.list_hook = lambda argv: ([f'{DS}\tfilesystem\n', 'pool/other\tfilesystem\n']
                                        if argv == DEPTH1(DS) else None)
        manager, dsi = make()
        with caplog.at_level(DEBUG, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']]
        (msg,) = fallback_errors(caplog)
        assert 'pool/other' in msg and f'cannot list the datasets of {DS}' in msg

    def test_a_walk_listing_without_the_dataset_falls_back(self, mock_subprocess, clock, caplog):
        world = self.bad_inside_copy(mock_subprocess)
        world.list_hook = lambda argv: [f'{CH}\tfilesystem\n'] if argv == DEPTH1(DS) else None
        manager, dsi = make()
        with caplog.at_level(DEBUG, logger=LOGGER):
            manager.create_snapshot(dsi)
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']]
        assert 'outside it' in fallback_errors(caplog)[0] or 'does not contain' in fallback_errors(caplog)[0]

    def test_the_walk_is_untimed_and_strict(self, mock_subprocess, clock, mocker):
        self.bad_inside_copy(mock_subprocess)
        import libzfseasy as zfs
        import libzfseasy.zfs as lz
        real = zfs.list
        seen = []

        def spy(**kw):
            if kw.get('types') == 'filesystem,volume':
                seen.append((kw.get('strict'), kw.get('depth'), lz._timeout_var.get()))
            return real(**kw)

        mocker.patch('zfsbackup.backup_manager.zfs.list', side_effect=spy)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert len(seen) == 4 and all(strict is True and timeout is None for strict, _, timeout in seen)
        assert (True, 1, None) in seen

    def test_the_walk_unit_lists_levels_and_never_a_top(self, mock_subprocess):
        world = build_world(mock_subprocess)
        manager, dsi = make()
        found = DatasetManager._walk_sources(dsi, {COPY: 'why'})
        assert [d.name for d in found] == [DS, CH, GC, VOL]
        assert world.listings() == [DEPTH1(DS), RECURSIVE(CH), RECURSIVE(VOL)]

    def test_a_walk_that_hits_the_recovery_is_an_ordinary_recovery(self, mock_subprocess, clock, caplog):
        """A bad name in a source child ends in the fallback; once it is renamed, the next cycle is atomic again and
        the recovery INFO says the copies are kept out again."""
        world = build_world(mock_subprocess)
        world.add(CH + '/' + BAD)
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
            world.drop(CH + '/' + BAD)
            next_cycle(clock)
            manager.create_snapshot(dsi)
        (back,) = records(caplog, INFO, 'can be told apart again')
        assert back.endswith('; they are kept out of its snapshot again')
        assert world.commands_of('snapshot')[-1][1] != '-r'


class TestKeyboardInterrupt:
    @pytest.mark.parametrize('where', ['copy-read', 'listing', 'snapshot'])
    def test_an_interrupt_propagates_and_nothing_falls_back(self, world, clock, caplog, mocker, where):
        manager, dsi = make()

        def interrupt(argv):
            if where == 'listing' and LIST_TREE_ARGV(argv) or where == 'snapshot' and argv[0] == 'snapshot':
                raise KeyboardInterrupt

        if where == 'copy-read':
            mocker.patch.object(DatasetManager, '_copy_roots', side_effect=KeyboardInterrupt)
        world.on_exec = interrupt
        with caplog.at_level(DEBUG, logger=LOGGER), pytest.raises(KeyboardInterrupt):
            manager.create_snapshot(dsi)
        assert world.commands == [] and world.created == [], 'no `many`, and above all no fallback `-r`'
        assert manager._snapshot_fallback_noted == set() and manager._snapshot_copy_noted == set()
        assert fallback_errors(caplog) == []
        assert manager.last_snapshot_error is None, 'an interrupt is not a failed snapshot'


# ---------------------------------------------------------------------------
# A real zfs failure is not a fallback
# ---------------------------------------------------------------------------


class TestCollision:
    def test_a_name_that_exists_in_one_source_fails_the_whole_cycle_and_creates_nothing(
            self, world, clock, caplog):
        world.add_snap(CH, NAME)                         # one child already has the name
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            assert manager.create_snapshot(dsi) is None
        assert 'already exists' in manager.last_snapshot_error and 'no snapshots were created' in manager.last_snapshot_error
        assert world.commands_of('snapshot') == [['snapshot', f'{DS}@{NAME}', f'{CH}@{NAME}', f'{GC}@{NAME}', f'{VOL}@{NAME}']]
        assert world.created == [] and world.names_of(DS) == set() and world.snapshots_below(COPY) == {}
        assert fallback_errors(caplog) == [], 'a zfs failure is not "cannot tell the copies apart"'
        assert copy_errors(caplog) == [], 'the copy error follows a successful snapshot'
        assert len(errors(caplog, f'Failed to create snapshot {DS}@{NAME}')) == 1

    def test_a_failure_of_many_never_retries_with_dash_r(self, world, clock):
        world.add_snap(GC, NAME)
        manager, dsi = make()
        manager.create_snapshot(dsi)
        assert all(a[1] != '-r' for a in world.commands_of('snapshot'))

    def test_the_next_cycle_after_a_collision_logs_the_copy_error_then(self, world, clock, caplog):
        world.add_snap(CH, NAME)
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            assert manager.create_snapshot(dsi) is None
            next_cycle(clock)
            assert manager.create_snapshot(dsi) is not None
        assert len(copy_errors(caplog)) == 1
        assert world.snapshots_below(COPY) == {}

    def test_the_same_failure_without_a_copy_is_what_snapshot_r_does(self, mock_subprocess, clock):
        world = build_world(mock_subprocess, copy=False)
        world.add_snap(CH, NAME)
        manager, dsi = make()
        assert manager.create_snapshot(dsi) is None
        assert world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']] and world.created == []
        assert 'already exists' in manager.last_snapshot_error

    def test_a_dataset_whose_own_name_exists_collides_in_the_one_command_too(self, world, clock):
        world.add_snap(DS, NAME)
        manager, dsi = make()
        assert manager.create_snapshot(dsi) is None and world.created == []


# ---------------------------------------------------------------------------
# One ERROR per episode, INFO on recovery: the two memories
# ---------------------------------------------------------------------------


class TestCopyEpisodes:
    def test_the_error_is_logged_once_across_cycles(self, world, clock, caplog):
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            for _ in range(4):
                assert manager.create_snapshot(dsi) is not None
                next_cycle(clock)
        assert len(copy_errors(caplog)) == 1
        assert manager._snapshot_copy_noted == {(DS, COPY)}
        assert world.snapshots_below(COPY) == {}
        assert world.names_of(DS) == {at(clock, timedelta(hours=-h)) for h in range(1, 5)}

    def test_a_gone_copy_is_an_info_and_the_next_cycle_is_plain_recursive_again(self, world, clock, caplog):
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
            for name in (COPY, COPY + '/h1', COPY + '/h1/pool'):
                world.drop(name)
            next_cycle(clock)
            manager.create_snapshot(dsi)
        (gone,) = records(caplog, INFO, f'{COPY}, a backup copy below {DS}, is gone')
        assert f'{DS} is snapshotted recursively again' in gone
        assert world.commands_of('snapshot')[-1] == ['snapshot', '-r', f'{DS}@{at(clock)}']
        assert manager._snapshot_copy_noted == set()

    def test_the_info_is_logged_once(self, world, clock, caplog):
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
            for name in (COPY, COPY + '/h1', COPY + '/h1/pool'):
                world.drop(name)
            for _ in range(3):
                next_cycle(clock)
                manager.create_snapshot(dsi)
        assert len(records(caplog, INFO, 'is gone')) == 1

    def test_a_copy_that_comes_back_is_a_new_episode(self, world, clock, caplog):
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
            for name in (COPY, COPY + '/h1', COPY + '/h1/pool'):
                world.drop(name)
            next_cycle(clock)
            manager.create_snapshot(dsi)
            world.add(COPY, LOCAL).add(COPY + '/h1').add(COPY + '/h1/pool')
            next_cycle(clock)
            manager.create_snapshot(dsi)
        assert len(copy_errors(caplog)) == 2 and len(records(caplog, INFO, 'is gone')) == 1
        assert world.snapshots_below(COPY) == {}

    def test_one_of_two_copies_going_is_reported_alone(self, world, clock, caplog):
        world.add('pool/data/bk2', LOCAL)
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
            world.drop('pool/data/bk2')
            next_cycle(clock)
            manager.create_snapshot(dsi)
        assert len(copy_errors(caplog)) == 2
        (gone,) = records(caplog, INFO, 'is gone')
        assert 'pool/data/bk2, a backup copy' in gone
        assert manager._snapshot_copy_noted == {(DS, COPY)}

    def test_each_dataset_has_its_own_episode(self, mock_subprocess, clock, caplog):
        world = build_world(mock_subprocess)
        world.add('pool/two').add('pool/two/bk', LOCAL)
        manager, first = make(extra=[DatasetConfig(name='pool/two', recursive=True)])
        second = next(d for d in manager.datasets if d.name == 'pool/two')
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(first)
            manager.create_snapshot(second)
            next_cycle(clock)
            manager.create_snapshot(first)
            manager.create_snapshot(second)
        assert len(copy_errors(caplog)) == 2
        assert manager._snapshot_copy_noted == {(DS, COPY), ('pool/two', 'pool/two/bk')}

    def test_the_error_of_a_receive_target_copy_is_remembered_the_same_way(self, mock_subprocess, clock, caplog):
        world = SnapWorld(mock_subprocess)
        world.add(DS).add(COPY).add(COPY + '/h1')
        manager, dsi = make(targets=[COPY])
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
            next_cycle(clock)
            manager.create_snapshot(dsi)
        assert len(copy_errors(caplog)) == 1


class TestFallbackEpisodes:
    def test_one_error_then_debug_then_an_info_on_recovery_then_a_new_episode(
            self, world_with_target, clock, caplog, mocker):
        world = world_with_target
        manager, dsi = make(targets=[COPY])
        with caplog.at_level(DEBUG, logger=LOGGER):
            for _ in range(3):                                      # three failing cycles
                world.fail_once(LIST_TREE_ARGV, 'cannot open: pool I/O is currently suspended')
                manager.create_snapshot(dsi)
                next_cycle(clock)
            assert len(fallback_errors(caplog)) == 1
            assert len(records(caplog, DEBUG, f'Still cannot tell the copies below {DS} apart')) == 2
            manager.create_snapshot(dsi)                             # the read passes again
            (back,) = records(caplog, INFO, f'The backup copies below {DS} can be told apart again')
            assert back.endswith('; they are kept out of its snapshot again')
            assert manager._snapshot_fallback_noted == set()
            next_cycle(clock)
            world.fail_once(LIST_TREE_ARGV, 'cannot open: pool I/O is currently suspended')
            manager.create_snapshot(dsi)
        assert len(fallback_errors(caplog)) == 2, 'a new episode'
        assert [a[1] == '-r' for a in world.commands_of('snapshot')] == [True, True, True, False, True]

    def test_a_recovery_with_no_copy_left_is_still_an_info(self, world_with_target, clock, caplog):
        world = world_with_target
        manager, dsi = make(targets=[COPY])
        with caplog.at_level(INFO, logger=LOGGER):
            world.fail(READ_ARGV, 'cannot open: pool I/O is currently suspended')
            manager.create_snapshot(dsi)
            world.failures.clear()
            for name in (COPY, COPY + '/h1'):
                world.drop(name)
            next_cycle(clock)
            manager.create_snapshot(dsi)
        (back,) = records(caplog, INFO, 'can be told apart again')
        assert back.endswith(' (none was found)') and 'kept out of its snapshot' not in back

    def test_each_dataset_has_its_own_fallback_episode(self, mock_subprocess, clock, caplog):
        world = build_world(mock_subprocess)
        world.add('pool/two').add('pool/two/c')
        manager, first = make(extra=[DatasetConfig(name='pool/two', recursive=True)])
        second = next(d for d in manager.datasets if d.name == 'pool/two')
        world.fail(READ_ARGV, 'cannot open: pool I/O is currently suspended')
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(first)
            manager.create_snapshot(second)
            next_cycle(clock)
            manager.create_snapshot(first)
            manager.create_snapshot(second)
        assert len(fallback_errors(caplog)) == 2
        assert manager._snapshot_fallback_noted == {DS, 'pool/two'}

    def test_a_dry_run_never_touches_the_memory(self, world, clock, mocker):
        manager, dsi = make(dry_run=True)
        manager.create_snapshot(dsi)
        assert manager._snapshot_fallback_noted == set()


class TestTheTwoSnapshotMemoriesAreIndependent:
    def test_a_fallback_does_not_forget_a_known_copy_nor_log_it_again_after_recovery(
            self, world, clock, caplog):
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)                                        # copy error #1
            next_cycle(clock)
            world.fail(READ_ARGV, 'cannot open: pool I/O is currently suspended')
            manager.create_snapshot(dsi)                                        # fallback
            assert manager._snapshot_copy_noted == {(DS, COPY)}, 'the fallback leaves it alone'
            world.failures.clear()
            next_cycle(clock)
            manager.create_snapshot(dsi)                                        # recovered
        assert len(copy_errors(caplog)) == 1 and len(fallback_errors(caplog)) == 1
        assert len(records(caplog, INFO, 'can be told apart again')) == 1
        assert manager._snapshot_fallback_noted == set() and manager._snapshot_copy_noted == {(DS, COPY)}

    def test_a_gone_copy_during_a_fallback_is_not_announced_until_the_read_works(self, world, clock, caplog):
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
            for name in (COPY, COPY + '/h1', COPY + '/h1/pool'):
                world.drop(name)
            world.fail(READ_ARGV, 'cannot open: pool I/O is currently suspended')
            next_cycle(clock)
            manager.create_snapshot(dsi)
            assert records(caplog, INFO, 'is gone') == []
            world.failures.clear()
            next_cycle(clock)
            manager.create_snapshot(dsi)
        assert len(records(caplog, INFO, 'is gone')) == 1

    def test_the_pruners_memory_is_separate(self, world, clock, caplog):
        """`_copy_protect_noted` (the pruner's ERROR) and `_snapshot_copy_noted` (this one) are two sets: each
        logs its own ERROR for the same copy, once, and its own INFO when the copy goes, without touching the
        other."""
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
            assert manager._copy_protect_noted == set() and manager._snapshot_copy_noted == {(DS, COPY)}
            manager._copy_protection(dsi)
            assert manager._copy_protect_noted == {(DS, COPY)} and manager._snapshot_copy_noted == {(DS, COPY)}
            next_cycle(clock)
            manager.create_snapshot(dsi)
            manager._copy_protection(dsi)
            assert len(copy_errors(caplog)) == 2                                    # one of each, no repeats
            assert len(errors(caplog, 'NOT snapshotted')) == 1
            assert len(errors(caplog, 'a recursive destroy')) == 1
            for name in (COPY, COPY + '/h1', COPY + '/h1/pool'):
                world.drop(name)
            manager._copy_protection(dsi)
            assert manager._copy_protect_noted == set() and manager._snapshot_copy_noted == {(DS, COPY)}
            assert len(records(caplog, INFO, 'no longer held back')) == 1
            assert records(caplog, INFO, 'snapshotted recursively again') == []
            next_cycle(clock)
            manager.create_snapshot(dsi)
        assert manager._snapshot_copy_noted == set()
        assert len(records(caplog, INFO, 'snapshotted recursively again')) == 1

    def test_the_pruner_first_does_not_silence_the_snapshot_error(self, world, clock, caplog):
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager._copy_protection(dsi)
            manager.create_snapshot(dsi)
        assert len(errors(caplog, 'NOT snapshotted')) == 1 and len(errors(caplog, 'a recursive destroy')) == 1

    def test_the_snapshot_error_does_not_silence_the_pruners(self, world, clock, caplog):
        manager, dsi = make()
        with caplog.at_level(INFO, logger=LOGGER):
            manager.create_snapshot(dsi)
            manager._copy_protection(dsi)
        assert len(errors(caplog, 'a recursive destroy')) == 1

    def test_a_pruner_fallback_state_does_not_exist(self, world, clock):
        """The pruner has no fallback memory: only the snapshot side has `_snapshot_fallback_noted`."""
        manager, dsi = make()
        assert manager._snapshot_fallback_noted == set()
        manager._copy_protection(dsi)
        assert manager._snapshot_fallback_noted == set()


# ---------------------------------------------------------------------------
# The snapshot worker and the pruner, together, over several cycles
# ---------------------------------------------------------------------------


class TestSnapshotAndPruneNeverTouchACopy:
    """One shared world, two managers (the workers are separate processes), eight hourly cycles: the snapshot
    worker takes a snapshot, the pruner prunes. The copy and the copy below it must end every cycle with exactly
    the snapshots they were given."""

    def seed(self, world, clock, hours):
        """`pool/data` and every source child hold hourly snapshots 6..2 hours ago; the copy holds some of the same
        names (a `zfs send` of them), `bk/h1` another."""
        names = {h: at(clock, timedelta(hours=-h)) for h in hours}
        for h, name in names.items():
            for d in (DS, CH, GC, VOL):
                world.add_snap(d, name)
        return names

    def test_eight_cycles(self, world, clock, caplog):
        names = self.seed(world, clock, range(2, 8))
        for h in (5, 4):
            world.add_snap(COPY, names[h])
        world.add_snap(COPY + '/h1', names[3])
        world.add_snap(COPY + '/h1/pool', names[6])
        copy_before = world.snapshots_below(COPY)
        snap_mgr, snap_dsi = make(retention=True)
        prune_mgr, prune_dsi = make(retention=True)
        newest = []
        with caplog.at_level(INFO, logger=LOGGER):
            for cycle in range(8):
                assert snap_mgr.needs_snapshot(snap_dsi) is True
                created = snap_mgr.create_snapshot(snap_dsi)
                assert created is not None and snap_mgr.last_snapshot_error is None
                newest.append(created.name)
                result = prune_mgr.prune_snapshots(prune_dsi)
                assert result.ok, result
                assert world.snapshots_below(COPY) == copy_before, f'cycle {cycle}: the copy changed'
                assert world.created_in(COPY) == [] and world.destroyed_in(COPY) == []
                next_cycle(clock)
        # every new snapshot went to every source dataset (and only those), all of one cycle in one txg
        by_name = {}
        for dataset, short, txg in world.created:
            by_name.setdefault(short, []).append((dataset, txg))
        assert list(by_name) == newest
        for short, made in by_name.items():
            assert sorted(d for d, _ in made) == sorted((DS, CH, GC, VOL)), short
            assert len({t for _, t in made}) == 1, short
        assert all(newest[-1] in world.names_of(d) for d in (DS, CH, GC, VOL))
        # the names the copy holds are kept in the source (the recursive destroy would take them out of the copy)
        assert {names[5], names[4]} <= world.names_of(DS) and names[3] in world.names_of(DS)
        assert names[6] in world.names_of(DS)
        # and the pruner did prune a name the copy does not hold, so this is not a no-op
        assert names[7] not in world.names_of(DS) and names[2] not in world.names_of(DS)
        assert len(copy_errors(caplog)) == 2 and len(errors(caplog, 'NOT snapshotted')) == 1
        assert fallback_errors(caplog) == []

    def test_the_control_with_the_old_behaviour_the_copy_does_get_snapshots(self, world, clock, mocker):
        """The same world with `snapshot -r` (what A2c replaces) puts a snapshot in the copy, so the cycle test
        above does detect it."""
        manager, dsi = make(retention=True)
        mocker.patch.object(manager, '_copy_tops', return_value={})
        manager.create_snapshot(dsi)
        assert world.snapshots_below(COPY) != {}

    def test_the_deliberate_fallback_is_the_one_way_a_name_gets_into_the_copy_and_it_is_then_kept(self, world, clock):
        """With the copy read failing the copy DOES get the name (a stray snapshot is recoverable); once the read
        works the pruner keeps that name in the source for as long as the copy holds it."""
        world.fail(READ_ARGV, 'cannot open: pool I/O is currently suspended')
        snap_mgr, snap_dsi = make(retention=True)
        snap_mgr.create_snapshot(snap_dsi)
        assert NAME in world.names_of(COPY)
        world.failures.clear()
        clock.advance(timedelta(hours=8))
        for d in (DS, CH, GC, VOL):
            world.add_snap(d, at(clock))                          # newer ones, so NAME is an old candidate
        prune_mgr, prune_dsi = make(retention=True)
        assert prune_mgr.prune_snapshots(prune_dsi).ok
        assert NAME in world.names_of(COPY) and NAME in world.names_of(DS)
        assert world.destroyed_in(COPY) == []

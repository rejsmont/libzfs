"""A2b: a recursive dataset's prune must not destroy a snapshot name a BACKUP COPY below it holds.

`zfs destroy -r pool/data@snap` also destroys `@snap` in every descendant, and a backup copy (the copies of a
`zfs:` destination that was removed from the config, a `send -R` replica, another host's copy with the same
prefix) carries the very names its source has. `DatasetManager._copy_protection` therefore reads, for a RECURSIVE
dataset, the datasets strictly below it that carry `org.zfsbackup:client_id`/`source_dataset` (local, received or
inherited: one value-free recursive `zfs get -s local,received,inherited`), lists the snapshots under each
topmost one (one strict `zfs list -r -t snapshot` per top) and keeps every name found, in `_prune_snapshots`
(merged into the descendants' protect-set, so a failure skips the dataset) and again in `_still_prunable`
(before every destroy call, before the descendants' anchors, the own-properties read still last).

The fake is `tests/_prune_support.py::PrunePool` (a stateful list/get/destroy whose recursive destroy removes the
name from every descendant, copies included), so "the copy keeps its names" is read off the pool, not off a call.
`TestTheRealCommands` runs the same code over the real libzfseasy parsers (only `Popen` is faked).

Layout used throughout: `pool/data` (recursive; retention 1h/2h, so every `old` snapshot is a candidate and the
newest is kept), a source child `pool/data/c` with the same names, and the copy root `pool/data/bk`.
"""

import logging
from datetime import timedelta

import pytest

import libzfseasy as zfs
from libzfseasy.types import Dataset
from libzfseasy.zfs import ZFSOutputError, ZFSTimeoutError
from _get_support import ZfsWorld, copy_read_argv
from _prune_support import (  # noqa: F401
    PREFIX, PrunePool, hourly_name, prune_manager, prune_pool, records, retention_1h_2h,
)
from zfsbackup.backup_manager import (
    PROP_ANCHOR_PREFIX, PROP_CLIENT_ID, PROP_CONFIG, PROP_SOURCE_DATASET, DatasetManager,
)
from zfsbackup.config import (
    BackupConfig, DatasetConfig, Destination, RemoteDatasetConfig, RemoteServerConfig,
)

DS = 'pool/data'
CH = 'pool/data/c'
COPY = 'pool/data/bk'
LOCAL = [(PROP_CLIENT_ID, 'h1', 'local'), (PROP_SOURCE_DATASET, 'pool/x', 'local')]
INHERITED = [(PROP_CLIENT_ID, 'h1', 'inherited from pool/data/bk'),
             (PROP_SOURCE_DATASET, 'pool/x', 'inherited from pool/data/bk')]

ERR = logging.ERROR
INFO = logging.INFO


def build(pool, old=6, recursive=True, dry_run=False, destinations=()):
    """`(manager, dsi, fresh, olds)`: `pool/data` and the source child `pool/data/c` hold the same names."""
    manager, dsi = prune_manager(DS, recursive=recursive, dry_run=dry_run, destinations=destinations)
    fresh, olds = pool.populate(DS, old=old)
    pool.snaps[CH] = list(pool.snaps[DS])
    return manager, dsi, fresh, olds


def add_copy(pool, root, names, props=LOCAL):
    """A backup copy `root` holding `names` (marked by `props`)."""
    pool.snaps[root] = list(names)
    pool.props[root] = list(props)


def copy_errors(caplog, root=None):
    # (`' is a backup copy ('`: the per-copy message; the dataset-level one says "If pool is a backup copy,")
    return records(caplog, ERR, ' is a backup copy (' if root is None else f'{root} is a backup copy (')


def inside_errors(caplog, dataset=None):
    """The ERROR for a recursive dataset that ITSELF has or inherits a copy marker (`_inside_copy_message`)."""
    return records(caplog, ERR, 'itself sits inside a backup copy' if dataset is None
                   else f'{dataset} itself sits inside a backup copy')


def evaluation_errors(caplog):
    return records(caplog, ERR, 'Cannot evaluate descendants')


# ---------------------------------------------------------------------------
# The core: what a copy holds stays, what it lacks goes
# ---------------------------------------------------------------------------


class TestCopyNamesAreKept:
    def test_names_the_copy_holds_are_kept_and_names_it_lacks_are_destroyed(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, [olds[1], olds[3]])
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            result = manager.prune_snapshots(dsi)
        kept = {fresh, olds[1], olds[3]}
        assert prune_pool.names_of(DS) == kept
        assert prune_pool.names_of(CH) == kept, 'the source child is pruned for every name the copy lacks'
        assert prune_pool.names_of(COPY) == {olds[1], olds[3]}, 'the copy lost nothing'
        assert result.ok and result.pruned == 4
        (call,) = prune_pool.calls
        assert call.recursive and sorted(call.names) == sorted(set(olds) - {olds[1], olds[3]})
        held = records(caplog, INFO, f'keeping {DS}@{olds[1]} (recursive prune)')
        assert held and f'the backup copy {COPY}@{olds[1]}' in held[0]

    def test_the_control_an_unmarked_child_is_an_ordinary_one_and_its_names_go(self, prune_pool):
        """The same layout without the markers: `destroy -r` takes the names out of `pool/data/bk` too, which is
        exactly what the protection exists to prevent (and no listing is made for it)."""
        manager, dsi, fresh, olds = build(prune_pool)
        prune_pool.snaps[COPY] = [olds[1], olds[3]]
        manager.prune_snapshots(dsi)
        assert prune_pool.names_of(COPY) == set()
        assert prune_pool.names_of(DS) == {fresh}
        assert prune_pool.snapshot_lists == [(DS, False)]

    def test_only_the_dataset_owned_listing_and_one_recursive_listing_per_top_copy_are_made(self, prune_pool):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, [olds[1]])
        manager.prune_snapshots(dsi)
        # the pass lists the dataset itself (not recursive), then the copy root (recursive); the re-read lists
        # the copy root again before the destroy
        assert prune_pool.snapshot_lists == [(DS, False), (COPY, True), (COPY, True)]
        assert prune_pool.copy_reads == 2

    def test_the_copy_listing_is_strict_snapshot_only_and_rooted_at_the_copy(self, prune_pool):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, [olds[1]])
        manager.prune_snapshots(dsi)
        recursive_lists = [c for c in zfs.list.call_args_list if c.kwargs.get('recursive')]
        assert recursive_lists
        for call in recursive_lists:
            assert call.kwargs['strict'] is True and call.kwargs['types'] == 'snapshot'
            assert call.kwargs['roots'].name == COPY

    @pytest.mark.parametrize('props', [
        [(PROP_CLIENT_ID, 'h1', 'local')],
        [(PROP_SOURCE_DATASET, 'pool/x', 'local')],
        [(PROP_CLIENT_ID, 'h1', 'received')],
        [(PROP_SOURCE_DATASET, 'pool/x', 'received')],
        [(PROP_CLIENT_ID, 'h1', 'received'), (PROP_SOURCE_DATASET, 'pool/x', 'received')],
        [(PROP_CLIENT_ID, 'h1', 'local'), (PROP_SOURCE_DATASET, 'pool/x', 'received')],
    ], ids=['local-client-id', 'local-source', 'received-client-id', 'received-source', 'both-received',
            'mixed'])
    def test_either_marker_from_either_source_makes_a_copy(self, prune_pool, caplog, props):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, [olds[2]], props)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.names_of(COPY) == {olds[2]}
        assert olds[2] in prune_pool.names_of(DS)
        assert len(copy_errors(caplog, COPY)) == 1

    @pytest.mark.parametrize('props', [
        [(PROP_CONFIG, 'x', 'local')],
        [(PROP_ANCHOR_PREFIX + 'off', 'x', 'local')],
        [(PROP_ANCHOR_PREFIX + 'off', 'x', 'received'), (PROP_CONFIG, 'x', 'received')],
        [('org.other:client_id', 'h1', 'local')],
        [],
    ], ids=['config', 'anchor', 'received-anchor-and-config', 'foreign-namespace', 'none'])
    def test_other_properties_do_not_make_a_copy(self, prune_pool, caplog, props):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, [olds[2]], props)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.snapshot_lists == [(DS, False)]
        assert copy_errors(caplog) == []

    def test_a_marker_on_the_dataset_itself_is_not_a_copy_below_it(self, prune_pool, caplog):
        """The dataset's own local marker (or a config it carries) is not "strictly below": a source that was
        once a replica keeps pruning its own tree."""
        manager, dsi, fresh, olds = build(prune_pool)
        prune_pool.props[DS] = list(LOCAL)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.names_of(DS) == {fresh}
        assert prune_pool.snapshot_lists == [(DS, False)]
        assert copy_errors(caplog) == []

    def test_a_copy_that_holds_every_name_keeps_all_and_the_dataset_is_not_blindly_skipped(self, prune_pool):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, olds)
        result = manager.prune_snapshots(dsi)
        assert result.ok and result.pruned == 0
        assert prune_pool.calls == [] and prune_pool.empty_calls == []
        assert prune_pool.names_of(DS) == set(olds) | {fresh}

    def test_a_copy_with_no_snapshots_is_still_listed(self, prune_pool):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, [])
        manager.prune_snapshots(dsi)
        assert prune_pool.names_of(DS) == {fresh}
        assert (COPY, True) in prune_pool.snapshot_lists

    def test_several_copies_protect_the_union(self, prune_pool):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, [olds[0]])
        add_copy(prune_pool, 'pool/data/bk2', [olds[1]])
        add_copy(prune_pool, 'pool/data/c/bk3', [olds[2]], [(PROP_SOURCE_DATASET, 'a', 'received')])
        manager.prune_snapshots(dsi)
        assert prune_pool.names_of(DS) == {fresh, olds[0], olds[1], olds[2]}
        assert prune_pool.names_of(COPY) == {olds[0]}
        assert prune_pool.names_of('pool/data/bk2') == {olds[1]}
        assert prune_pool.names_of('pool/data/c/bk3') == {olds[2]}

    def test_the_copy_protection_and_a_descendants_anchor_combine(self, prune_pool):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, [olds[0]])
        prune_pool.props[CH] = [(PROP_ANCHOR_PREFIX + 'off', olds[1], 'local')]
        manager.prune_snapshots(dsi)
        assert prune_pool.names_of(DS) == {fresh, olds[0], olds[1]}


class TestCopyShapes:
    def test_a_copy_nested_below_a_copy_root_is_one_top_one_listing_one_error(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool)
        nested = COPY + '/h1/pool/data'
        add_copy(prune_pool, COPY, [olds[0]])
        add_copy(prune_pool, nested, [olds[1]])             # its own LOCAL markers: still below the same top
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert [l for l in prune_pool.snapshot_lists if l[1]] == [(COPY, True)] * 2      # pass + re-read
        assert nested not in [l[0] for l in prune_pool.snapshot_lists]
        assert prune_pool.names_of(DS) == {fresh, olds[0], olds[1]}
        assert len(copy_errors(caplog)) == 1 and len(copy_errors(caplog, COPY)) == 1

    def test_a_copy_that_inherits_its_marker_is_covered_by_its_topmost_root(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, [olds[0]])
        add_copy(prune_pool, COPY + '/h1', [olds[1]], INHERITED)
        add_copy(prune_pool, COPY + '/h1/pool', [olds[2]], INHERITED)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert {l[0] for l in prune_pool.snapshot_lists if l[1]} == {COPY}
        assert prune_pool.names_of(DS) == {fresh, olds[0], olds[1], olds[2]}
        assert len(copy_errors(caplog)) == 1

    def test_a_name_only_a_descendant_of_the_copy_holds_is_kept(self, prune_pool):
        """The copy root holds nothing of that name; two levels down a replica does."""
        manager, dsi, fresh, olds = build(prune_pool)
        deep = COPY + '/h1/pool/data'
        add_copy(prune_pool, COPY, [olds[0]])
        prune_pool.snaps[deep] = [olds[3]]                  # no markers of its own, inherited from the root
        manager.prune_snapshots(dsi)
        assert olds[3] in prune_pool.names_of(DS) and prune_pool.names_of(deep) == {olds[3]}
        assert prune_pool.names_of(DS) == {fresh, olds[0], olds[3]}

    def test_two_sibling_copies_are_two_tops_with_one_listing_each(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, [olds[0]])
        add_copy(prune_pool, 'pool/data/bk2', [olds[1]])
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert sorted({l[0] for l in prune_pool.snapshot_lists if l[1]}) == [COPY, 'pool/data/bk2']
        assert len(copy_errors(caplog)) == 2

    def test_a_copy_that_shares_a_name_prefix_with_another_is_not_below_it(self, prune_pool):
        """`pool/data/bk2` is not inside `pool/data/bk`: both are tops (a string prefix is not ancestry)."""
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, [olds[0]])
        add_copy(prune_pool, COPY + '2', [olds[1]])
        manager.prune_snapshots(dsi)
        assert {l[0] for l in prune_pool.snapshot_lists if l[1]} == {COPY, COPY + '2'}
        assert prune_pool.names_of(DS) == {fresh, olds[0], olds[1]}

    def test_a_copy_of_a_sibling_dataset_outside_the_tree_is_not_read(self, prune_pool):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, 'pool/data2/bk', [olds[0]])
        add_copy(prune_pool, 'pool/other', [olds[1]])
        manager.prune_snapshots(dsi)
        assert prune_pool.snapshot_lists == [(DS, False)]

    def test_the_dataset_inside_a_copy_makes_every_child_a_copy(self, prune_pool, caplog):
        """`pool/data` inherits the marker from `pool` (it is itself inside a copy, e.g. a recursive dataset
        configured on a replica): every child inherits it too, so every child counts and is listed, but they are
        copies only BY INHERITANCE: ONE ERROR about the dataset (its origin `pool`), none per child."""
        manager, dsi, fresh, olds = build(prune_pool)
        source = [(PROP_CLIENT_ID, 'h1', 'inherited from pool'), (PROP_SOURCE_DATASET, 'x', 'inherited from pool')]
        for ds in (DS, CH, 'pool/data/c2'):
            prune_pool.props[ds] = list(source)
        prune_pool.snaps[CH] = [olds[0]]
        prune_pool.snaps['pool/data/c2'] = [olds[1]]
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.names_of(DS) == {fresh, olds[0], olds[1]}
        assert prune_pool.names_of(CH) == {olds[0]} and prune_pool.names_of('pool/data/c2') == {olds[1]}
        assert sorted({l[0] for l in prune_pool.snapshot_lists if l[1]}) == [CH, 'pool/data/c2']
        assert copy_errors(caplog) == [], 'no per-child ERROR (its `zfs rename <child>` remedy would be wrong)'
        (error,) = inside_errors(caplog, DS)
        assert 'it inherits org.zfsbackup:client_id or org.zfsbackup:source_dataset from pool' in error
        assert f'a recursive destroy of {DS}@<snapshot> would destroy that name in every copy below it' in error
        assert f'every snapshot name they hold is kept in {DS}' in error
        assert f'move {DS} out of it (zfs rename) or make {DS} non-recursive' in error
        assert 'zfs inherit org.zfsbackup:client_id pool; zfs inherit org.zfsbackup:source_dataset pool' in error
        assert 'only masks it' in error
        assert manager._copy_protect_inside_noted == {(DS, 'pool')} and manager._copy_protect_noted == set()

    def test_a_dataset_inside_a_copy_with_no_children_has_nothing_to_protect(self, prune_pool, caplog):
        manager, dsi = prune_manager(DS, recursive=True)
        fresh, olds = prune_pool.populate(DS, old=3)
        prune_pool.props[DS] = [(PROP_CLIENT_ID, 'h1', 'inherited from pool')]
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.names_of(DS) == {fresh}
        assert prune_pool.snapshot_lists == [(DS, False)]
        assert copy_errors(caplog) == [] and inside_errors(caplog) == [], 'nothing below is a copy: quiet'
        assert manager._copy_protect_inside_noted == set()


class TestARemovedLocalDestinationsCopies:
    """The real-world shape: a `zfs:` destination `tank/bk` was removed from the config, but its copies stay at
    `<T>/<id>/<pool>/<path>` under the recursive dataset `tank`, and carry the managed prefix on every snapshot
    the sender ever made. `tank/bk/h1` (the id level, created by the sender) and `tank/bk/h1/tank` carry only
    `client_id`; the copy itself carries `client_id`, `source_dataset` and `config`. `tank/bk` (the operator's
    target) carries nothing."""

    ROOT = 'tank'
    SRC = 'tank/data'
    TARGET = 'tank/bk'
    COPY_ROOT = 'tank/bk/h1'
    COPY = 'tank/bk/h1/tank/data'

    def build(self, pool):
        manager, dsi = prune_manager(self.ROOT, recursive=True)
        fresh, olds = pool.populate(self.ROOT, old=6)
        pool.snaps[self.SRC] = list(pool.snaps[self.ROOT])
        pool.snaps[self.TARGET] = []
        pool.props[self.COPY_ROOT] = [(PROP_CLIENT_ID, 'h1', 'local')]
        pool.props['tank/bk/h1/tank'] = [(PROP_CLIENT_ID, 'h1', 'local')]
        pool.props[self.COPY] = [
            (PROP_CLIENT_ID, 'h1', 'local'), (PROP_SOURCE_DATASET, self.SRC, 'local'), (PROP_CONFIG, '{}', 'local'),
        ]
        # the copy holds the newest 3 old names; the pool-level copy holds one more of its own
        pool.snaps[self.COPY] = olds[-3:]
        pool.snaps['tank/bk/h1/tank'] = [olds[1]]
        return manager, dsi, fresh, olds

    def test_the_copies_names_are_kept_and_the_rest_of_the_tree_is_pruned(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            result = manager.prune_snapshots(dsi)
        kept = {fresh, olds[1], *olds[-3:]}
        assert prune_pool.names_of(self.ROOT) == kept
        assert prune_pool.names_of(self.SRC) == kept
        assert prune_pool.names_of(self.COPY) == set(olds[-3:])
        assert prune_pool.names_of('tank/bk/h1/tank') == {olds[1]}
        assert result.ok and result.pruned == len(olds) - 4
        assert {l[0] for l in prune_pool.snapshot_lists if l[1]} == {self.COPY_ROOT}, (
            'the id level is the topmost copy root: one listing covers the whole tree')
        (error,) = copy_errors(caplog)
        assert error.startswith(f'{self.COPY_ROOT} is a backup copy')
        assert f'recursive dataset {self.ROOT}' in error and 'zfs rename' in error

    def test_the_next_cycles_are_stable_and_quiet(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool)
        manager.prune_snapshots(dsi)
        prune_pool.calls.clear()
        caplog.clear()
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            for _ in range(3):
                assert manager.prune_snapshots(dsi).ok
        assert prune_pool.calls == []
        assert copy_errors(caplog) == []

    def test_once_the_copy_is_moved_away_its_names_are_pruned_again(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool)
        manager.prune_snapshots(dsi)
        for ds in (self.COPY_ROOT, 'tank/bk/h1/tank', self.COPY):      # `zfs rename` out of the tree
            prune_pool.props.pop(ds)
            prune_pool.snaps.pop(ds, None)
        # the source still has the copy's names (they were kept); the next pass finds them prunable
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.names_of(self.ROOT) == {fresh}
        assert records(caplog, INFO, f'{self.COPY_ROOT}, a backup copy below {self.ROOT}, is gone')


# ---------------------------------------------------------------------------
# One ERROR per episode and (dataset, copy root); INFO when it is gone
# ---------------------------------------------------------------------------


class TestEpisodes:
    def make(self, pool):
        manager, dsi, fresh, olds = build(pool)
        add_copy(pool, COPY, olds)            # holds everything: every pass still has candidates, destroys none
        return manager, dsi, fresh, olds

    def test_the_error_names_the_copy_the_dataset_the_source_and_the_remedy(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.make(prune_pool)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        (error,) = copy_errors(caplog)
        assert error.startswith(f'{COPY} is a backup copy')
        assert PROP_CLIENT_ID in error and PROP_SOURCE_DATASET in error
        assert 'source: local' in error
        assert f'below the recursive dataset {DS}' in error
        assert f'zfs rename {COPY} ' in error and 'non-recursive' in error
        assert f'zfs snapshot -r of {DS}' in error                      # the replica caveat

    def test_the_error_is_logged_once_per_episode_not_every_cycle(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.make(prune_pool)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            for _ in range(4):
                manager.prune_snapshots(dsi)
        assert len(copy_errors(caplog)) == 1
        assert manager._copy_protect_noted == {(DS, COPY)}

    def test_each_new_copy_root_gets_its_own_error_and_only_its_own(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.make(prune_pool)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            add_copy(prune_pool, 'pool/data/bk2', olds[:2])
            manager.prune_snapshots(dsi)
            manager.prune_snapshots(dsi)
        assert len(copy_errors(caplog, COPY)) == 1 and len(copy_errors(caplog, 'pool/data/bk2')) == 1
        assert manager._copy_protect_noted == {(DS, COPY), (DS, 'pool/data/bk2')}

    def test_an_info_when_the_copy_is_gone_then_an_error_again_if_it_returns(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.make(prune_pool)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            prune_pool.props.pop(COPY)
            manager.prune_snapshots(dsi)
            manager.prune_snapshots(dsi)
            assert manager._copy_protect_noted == set()
            gone = records(caplog, INFO, f'{COPY}, a backup copy below {DS}, is gone')
            assert len(gone) == 1, 'announced once'
            prune_pool.props[COPY] = list(LOCAL)
            prune_pool.snaps[COPY] = list(olds)
            prune_pool.snaps[DS] = olds + [fresh]           # (the gone pass pruned them: due again)
            manager.prune_snapshots(dsi)
        assert len(copy_errors(caplog, COPY)) == 1 + 1
        assert manager._copy_protect_noted == {(DS, COPY)}

    def test_the_gone_info_names_only_the_roots_that_left(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.make(prune_pool)
        add_copy(prune_pool, 'pool/data/bk2', olds)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            prune_pool.props.pop('pool/data/bk2')
            manager.prune_snapshots(dsi)
        assert len(records(caplog, INFO, 'is gone')) == 1
        assert 'pool/data/bk2, a backup copy' in records(caplog, INFO, 'is gone')[0]
        assert manager._copy_protect_noted == {(DS, COPY)}

    def test_the_memory_is_per_dataset(self, prune_pool):
        """`pool/data` and `pool/data/sub` (both recursive) each announce a copy below both of them."""
        sub = 'pool/data/sub'
        config = BackupConfig(
            datasets=[
                DatasetConfig(name=DS, recursive=True, retention_rules=retention_1h_2h()),
                DatasetConfig(name=sub, recursive=True, retention_rules=retention_1h_2h()),
            ], snapshot_prefix='autosnap')
        manager = DatasetManager(config)
        add_copy(prune_pool, sub + '/bk', [], LOCAL)
        for dsi in manager.datasets:
            assert manager._copy_protection(dsi) == {}
        assert manager._copy_protect_noted == {(DS, sub + '/bk'), (sub, sub + '/bk')}

    def test_the_memory_is_cleared_when_the_dataset_stops_being_recursive(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.make(prune_pool)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            assert manager._copy_protect_noted == {(DS, COPY)}
            reads = prune_pool.copy_reads
            dsi.config.recursive = False                    # the operator changed the config
            manager.prune_snapshots(dsi)
            assert manager._copy_protect_noted == set()
            assert prune_pool.copy_reads == reads, 'a non-recursive pass reads no copies'
            dsi.config.recursive = True
            prune_pool.snaps[DS] = olds + [fresh]           # (the non-recursive pass pruned them: due again)
            manager.prune_snapshots(dsi)
        assert len(copy_errors(caplog, COPY)) == 2, 'a new episode is announced again'

    def test_the_memory_of_other_datasets_survives_a_non_recursive_pass(self, prune_pool):
        manager, dsi, fresh, olds = self.make(prune_pool)
        manager._copy_protect_noted.add(('pool/elsewhere', 'pool/elsewhere/bk'))
        dsi.config.recursive = False
        manager.prune_snapshots(dsi)
        assert manager._copy_protect_noted == {('pool/elsewhere', 'pool/elsewhere/bk')}

    def test_a_pass_with_nothing_to_prune_reads_no_copies_and_keeps_the_episode(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.make(prune_pool)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            reads = prune_pool.copy_reads
            prune_pool.snaps[DS] = [fresh]                  # nothing is due any more
            manager.prune_snapshots(dsi)
            assert prune_pool.copy_reads == reads
            assert manager._copy_protect_noted == {(DS, COPY)}
            prune_pool.snaps[DS] = olds + [fresh]           # due again: still the same episode
            manager.prune_snapshots(dsi)
        assert len(copy_errors(caplog)) == 1


class TestTheDatasetInsideACopyIsOneEpisode:
    """L6: `pool/data` itself has or inherits a copy marker, so every child is a copy by inheritance. The pruner
    keeps the names the children hold (every child is listed) but says it ONCE per (dataset, origin of the marker)
    per episode, with an INFO when it ends. The pruner's memory is its own (`_copy_protect_inside_noted`)."""

    ORIGIN = 'inherited from pool'
    SOURCE = [(PROP_CLIENT_ID, 'h1', ORIGIN), (PROP_SOURCE_DATASET, 'x', ORIGIN)]

    def make(self, pool, props=None, child_props=None):
        manager, dsi, fresh, olds = build(pool)
        for ds in (CH, 'pool/data/c2'):
            pool.props[ds] = list(child_props or self.SOURCE)
        pool.props[DS] = list(props or self.SOURCE)
        pool.snaps[CH] = list(olds)                      # every pass has candidates, every one held by a child
        pool.snaps['pool/data/c2'] = list(olds)
        return manager, dsi, fresh, olds

    def test_one_error_across_cycles_and_the_re_read_of_every_pass(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.make(prune_pool)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            for _ in range(4):
                manager.prune_snapshots(dsi)
        assert len(inside_errors(caplog)) == 1 and copy_errors(caplog) == []
        assert prune_pool.names_of(DS) == {fresh, *olds}, 'the held names stay'

    def test_an_info_when_it_ends_and_a_new_episode_is_announced_again(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.make(prune_pool)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            for ds in (DS, CH, 'pool/data/c2'):
                prune_pool.props[ds] = []                    # the markers are cleared above
            manager.prune_snapshots(dsi)
            (back,) = records(caplog, INFO, 'by inheritance from pool any more')
            assert back.startswith(f'Nothing below {DS} is a backup copy by inheritance')
            assert manager._copy_protect_inside_noted == set()
            for ds in (DS, CH, 'pool/data/c2'):
                prune_pool.props[ds] = list(self.SOURCE)
            prune_pool.snaps[DS] = [fresh, *olds]
            manager.prune_snapshots(dsi)
        assert len(inside_errors(caplog)) == 2

    def test_a_marker_the_dataset_carries_itself_has_the_dataset_as_origin(self, prune_pool, caplog):
        local = [(PROP_CLIENT_ID, 'h1', 'local'), (PROP_SOURCE_DATASET, 'x', 'local')]
        manager, dsi, fresh, olds = self.make(
            prune_pool, props=local, child_props=[(n, v, f'inherited from {DS}') for n, v, _ in local])
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        (error,) = inside_errors(caplog, DS)
        assert 'it carries a local org.zfsbackup:client_id or org.zfsbackup:source_dataset' in error
        assert f'clear the markers on {DS}' in error
        assert manager._copy_protect_inside_noted == {(DS, DS)} and copy_errors(caplog) == []

    def test_a_real_copy_below_gets_its_own_error_beside_the_inheritance_one(self, prune_pool, caplog):
        """`c` carries a LOCAL marker of its own: it is a copy in its own right (its `zfs rename` remedy is right);
        `c2`, which merely inherits, is covered by the dataset's message."""
        manager, dsi, fresh, olds = self.make(prune_pool)
        prune_pool.props[CH] = list(LOCAL)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert [m.split(' is a backup copy')[0] for m in copy_errors(caplog)] == [CH]
        assert len(inside_errors(caplog, DS)) == 1
        assert manager._copy_protect_noted == {(DS, CH)} and manager._copy_protect_inside_noted == {(DS, 'pool')}

    def test_a_childless_dataset_says_nothing(self, prune_pool, caplog):
        manager, dsi = prune_manager(DS, recursive=True)
        prune_pool.populate(DS, old=3)
        prune_pool.props[DS] = list(self.SOURCE)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager._copy_protection(dsi)
        assert copy_errors(caplog) == [] and inside_errors(caplog) == []

    def test_the_covered_children_leave_the_per_copy_memory_when_the_marker_arrives(self, prune_pool, caplog):
        """A child that was announced as a copy of its own and then ONLY inherits the dataset's marker is covered:
        its per-copy episode ends (INFO `is gone`), the dataset-level one begins."""
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, olds)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            assert manager._copy_protect_noted == {(DS, COPY)}
            prune_pool.props[DS] = list(self.SOURCE)
            prune_pool.props[COPY] = list(self.SOURCE)
            prune_pool.snaps[DS] = [fresh, *olds]
            manager.prune_snapshots(dsi)
        assert len(copy_errors(caplog, COPY)) == 1
        assert len(records(caplog, INFO, f'{COPY}, a backup copy below {DS}, is gone')) == 1
        assert manager._copy_protect_noted == set() and manager._copy_protect_inside_noted == {(DS, 'pool')}

    def test_the_two_memories_of_the_pruner_and_the_snapshot_worker_are_separate(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.make(prune_pool)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager._copy_protection(dsi)
        assert manager._copy_protect_inside_noted == {(DS, 'pool')}
        assert manager._snapshot_inside_copy_noted == set(), 'the snapshot worker announces its own'

    def test_a_dataset_that_stops_being_inside_a_copy_while_a_pass_has_nothing_due_keeps_its_episode(
            self, prune_pool, caplog):
        """Like `_copy_protect_noted` (A2b): a pass with nothing due reads no copies, so no episode ends in it."""
        manager, dsi, fresh, olds = self.make(prune_pool)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            prune_pool.snaps[DS] = [fresh]
            manager.prune_snapshots(dsi)
        assert manager._copy_protect_inside_noted == {(DS, 'pool')} and len(inside_errors(caplog)) == 1


# ---------------------------------------------------------------------------
# Anything unknown skips the dataset: the existing "Cannot evaluate descendants" path
# ---------------------------------------------------------------------------


class TestUnknownCopiesDestroyNothing:
    FAILURES = {
        'copy-read-raises': lambda p: setattr(p, 'copy_read_error', RuntimeError('pool I/O is currently suspended')),
        'copy-read-untrusted': lambda p: setattr(p, 'copy_read_error', ZFSOutputError('a row was refused')),
        'listing-raises': lambda p: p.list_errors.update({COPY: RuntimeError('cannot open pool/data/bk: busy')}),
        'listing-untrusted': lambda p: p.list_errors.update({COPY: ZFSOutputError('a name was skipped')}),
    }

    @staticmethod
    def heal(pool):
        pool.copy_read_error = None
        pool.list_errors.clear()

    def make(self, pool, kind):
        manager, dsi, fresh, olds = build(pool)
        add_copy(pool, COPY, [olds[1]])
        self.FAILURES[kind](pool)
        return manager, dsi, fresh, olds

    @pytest.mark.parametrize('kind', sorted(FAILURES))
    def test_nothing_is_destroyed_the_pass_is_skipped_and_the_error_is_logged_once(self, prune_pool, caplog, kind):
        manager, dsi, fresh, olds = self.make(prune_pool, kind)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            results = [manager.prune_snapshots(dsi) for _ in range(3)]
        assert prune_pool.calls == [] and prune_pool.names_of(DS) == set(olds) | {fresh}
        assert prune_pool.names_of(CH) == set(olds) | {fresh} and prune_pool.names_of(COPY) == {olds[1]}
        assert all(r.skipped and r.skipped.startswith('cannot evaluate the descendants') and not r.ok
                   for r in results)
        assert all(r.pruned == 0 for r in results)
        (error,) = evaluation_errors(caplog)
        assert f'Cannot evaluate descendants of {DS}; skipping pruning this cycle' in error
        assert len(records(caplog, logging.DEBUG, 'Cannot evaluate descendants')) == 2
        assert copy_errors(caplog) == [], 'no copy was established, so none is announced'
        assert manager._descendant_error_noted == {DS}
        assert manager._copy_protect_noted == set()

    @pytest.mark.parametrize('kind', sorted(FAILURES))
    def test_the_next_good_cycle_prunes_and_rearms_the_error(self, prune_pool, caplog, kind):
        manager, dsi, fresh, olds = self.make(prune_pool, kind)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            self.heal(prune_pool)
            result = manager.prune_snapshots(dsi)
            assert result.ok and result.pruned == len(olds) - 1
            assert manager._descendant_error_noted == set()
            self.make_fail_again(prune_pool, kind)
            prune_pool.snaps[DS] = olds + [fresh]
            manager.prune_snapshots(dsi)
        assert len(evaluation_errors(caplog)) == 2
        assert olds[1] in prune_pool.names_of(DS) and prune_pool.names_of(COPY) == {olds[1]}

    def make_fail_again(self, pool, kind):
        self.FAILURES[kind](pool)

    @pytest.mark.parametrize('kind', sorted(FAILURES))
    def test_a_non_recursive_dataset_is_not_affected(self, prune_pool, kind):
        manager, dsi, fresh, olds = build(prune_pool, recursive=False)
        add_copy(prune_pool, COPY, [olds[1]])
        self.FAILURES[kind](prune_pool)
        result = manager.prune_snapshots(dsi)
        assert result.ok and prune_pool.names_of(DS) == {fresh}
        assert prune_pool.copy_reads == 0 and all(not c.recursive for c in prune_pool.calls)

    def test_a_copy_read_that_fails_after_the_descendant_reads_still_skips(self, prune_pool):
        """The descendants' anchors are fine (read first), the copy read is not: still nothing is destroyed."""
        manager, dsi, fresh, olds = self.make(prune_pool, 'copy-read-raises')
        manager.prune_snapshots(dsi)
        assert prune_pool.copy_reads == 1 and prune_pool.calls == []
        assert [e for e in prune_pool.events if e[0] == 'discover' and e[2]] == [('discover', DS, True)]

    def test_a_failing_listing_of_one_of_two_copies_protects_neither_destroys_nothing(self, prune_pool):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, [olds[0]])
        add_copy(prune_pool, 'pool/data/bk2', [olds[1]])
        prune_pool.list_errors['pool/data/bk2'] = RuntimeError('boom')
        result = manager.prune_snapshots(dsi)
        assert prune_pool.calls == [] and result.skipped
        assert prune_pool.names_of(DS) == set(olds) | {fresh}


# ---------------------------------------------------------------------------
# The re-read before every destroy
# ---------------------------------------------------------------------------


class TestStillPrunableReadsTheCopies:
    def test_a_copy_that_appears_between_the_decision_and_the_destroy_is_protected(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool)

        def appears(pool, n):
            if n == 2:                                     # the pass made the first read; this is the re-read
                add_copy(pool, COPY, [olds[1], olds[4]])

        prune_pool.on_copy_read = appears
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            result = manager.prune_snapshots(dsi)
        assert prune_pool.copy_reads == 2
        (call,) = prune_pool.calls
        assert olds[1] not in call.names and olds[4] not in call.names
        assert len(call.names) == len(olds) - 2
        assert prune_pool.names_of(COPY) == {olds[1], olds[4]}
        assert prune_pool.names_of(DS) == {fresh, olds[1], olds[4]}
        assert result.ok
        assert len(records(caplog, INFO, 'it became protected')) == 2
        assert all(f'{DS}@{n}' in ' '.join(records(caplog, INFO, 'it became protected')) for n in (olds[1], olds[4]))

    def test_without_the_re_read_the_race_destroys_the_copys_snapshots(self, prune_pool, mocker):
        """The control: a pass that keeps its first protect-set destroys the name the copy has just received."""
        manager, dsi, fresh, olds = build(prune_pool)
        mocker.patch.object(
            manager, '_still_prunable',
            side_effect=lambda d, group: (add_copy(prune_pool, COPY, [olds[1]]), group)[1])
        manager.prune_snapshots(dsi)
        assert prune_pool.names_of(COPY) == set(), 'the recursive destroy took the copy\'s snapshot with it'

    def test_a_snapshot_a_known_copy_receives_after_the_decision_is_protected(self, prune_pool):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, [olds[0]])

        def received(pool, n):
            if n == 2:
                pool.add_snapshot(COPY, olds[3])

        prune_pool.on_copy_read = received
        manager.prune_snapshots(dsi)
        assert prune_pool.names_of(COPY) == {olds[0], olds[3]}
        assert prune_pool.names_of(DS) == {fresh, olds[0], olds[3]}

    def test_a_copy_that_appears_after_the_batch_failed_stops_that_names_single_retry(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool)
        prune_pool.send(DS, olds[0])                       # the batch fails with EBUSY, the singles follow

        def hook(pool, call):
            if len(call.names) > 1 and COPY not in pool.props:
                add_copy(pool, COPY, [olds[2], olds[3]])

        prune_pool.before_destroy = hook
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert not prune_pool.calls[0].ok and len(prune_pool.calls[0].names) == len(olds)
        singles = [c.names[0] for c in prune_pool.calls[1:]]
        assert olds[2] not in singles and olds[3] not in singles
        assert sorted(singles) == sorted(set(olds) - {olds[2], olds[3]})    # (olds[0]: tried, the sender holds it)
        assert prune_pool.names_of(COPY) == {olds[2], olds[3]}
        assert prune_pool.names_of(DS) == {fresh, olds[0], olds[2], olds[3]}
        assert manager._prune_stuck.keys().isdisjoint({(DS, olds[2]), (DS, olds[3])})

    @pytest.mark.parametrize('kind', ['read', 'listing'])
    def test_a_copy_read_that_fails_in_the_re_read_destroys_nothing(self, prune_pool, caplog, kind):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, [olds[1]])

        def fail(pool, n):
            if n == 2:
                if kind == 'read':
                    pool.copy_read_error = RuntimeError('pool I/O is currently suspended')
                else:
                    pool.list_errors[COPY] = RuntimeError('pool I/O is currently suspended')

        prune_pool.on_copy_read = fail
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            result = manager.prune_snapshots(dsi)
        assert prune_pool.calls == [] and prune_pool.names_of(DS) == set(olds) | {fresh}
        (error,) = records(caplog, ERR, 'Cannot re-read what protects')
        assert 'pool I/O is currently suspended' in error and f'{DS}@{olds[0]}' in error
        assert DS in error
        # the stored reason carries the context (what could not be read, and where)
        assert 'pool I/O is currently suspended' in manager._reread_error
        assert manager._reread_error.startswith(
            'cannot read the backup-copy markers' if kind == 'read'
            else f'cannot list the snapshots of the backup copy {COPY} below {DS}')
        assert result.pruned == 0

    def test_after_a_failed_batch_every_single_re_reads_the_copies_and_none_is_destroyed_when_it_fails(
            self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool, old=3)
        prune_pool.send(DS, olds[0])
        add_copy(prune_pool, COPY, [olds[1]])
        reads = []

        def after_batch(pool, call):
            if len(call.names) > 1:
                reads.append(pool.copy_reads)
                pool.copy_read_error = RuntimeError('boom')

        prune_pool.before_destroy = after_batch
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.sizes == [2]                     # the batch only (olds[1] was kept); no single went
        # one ERROR for the pass (the re-read before the batch), the refused singles are repeats: DEBUG
        assert len(records(caplog, ERR, 'Cannot re-read what protects')) == 1
        assert len(records(caplog, logging.DEBUG, 'Cannot re-read what protects')) >= 1
        assert prune_pool.names_of(DS) == set(olds) | {fresh}

    def test_the_own_property_read_is_still_the_last_zfs_call_before_every_destroy(self, prune_pool):
        manager, dsi, fresh, olds = build(prune_pool, old=4)
        add_copy(prune_pool, COPY, [olds[1]])
        prune_pool.send(DS, olds[0])
        manager.prune_snapshots(dsi)
        events = prune_pool.events
        destroys = [i for i, e in enumerate(events) if e[0] == 'destroy']
        assert len(destroys) >= 3                          # the batch + the singles
        for i in destroys:
            assert events[i - 2:i] == [('discover', DS, False), ('get', DS, False)], events[max(0, i - 8):i + 1]
            start = max(j for j in range(i) if events[j] == ('copies', DS))
            window = events[start:i - 2]
            # the re-read: the copies first (the read, then the listing of the top), then the descendants'
            assert window[:3] == [('copies', DS), ('list', COPY, True), ('discover', DS, True)], window

    def test_the_copies_are_read_before_the_descendants_anchors_in_the_re_read(self, prune_pool):
        manager, dsi, fresh, olds = build(prune_pool, old=2)
        manager.prune_snapshots(dsi)
        events = prune_pool.events
        i = max(j for j, e in enumerate(events) if e[0] == 'destroy')
        reread = events[events.index(('copies', DS), events.index(('copies', DS)) + 1):i]
        assert reread[0] == ('copies', DS) and reread[1] == ('discover', DS, True)
        assert reread[-2:] == [('discover', DS, False), ('get', DS, False)]

    def test_a_dry_run_reads_the_copies_once_for_the_decision_and_re_reads_nothing(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool, dry_run=True)
        add_copy(prune_pool, COPY, [olds[0], olds[1]])
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            result = manager.prune_snapshots(dsi)
        assert prune_pool.calls == [] and prune_pool.copy_reads == 1
        assert result.ok
        (would,) = records(caplog, INFO, '[DRY RUN] Would prune snapshots')
        assert olds[0] not in would and olds[1] not in would and olds[2] in would
        assert records(caplog, INFO, f'keeping {DS}@{olds[0]}')


# ---------------------------------------------------------------------------
# The read is only for recursive source datasets
# ---------------------------------------------------------------------------


class TestNoCopyReadWhenNotRecursive:
    def test_a_non_recursive_dataset_never_reads_copies_and_destroys_non_recursively(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool, recursive=False)
        add_copy(prune_pool, COPY, [olds[1]])
        prune_pool.send(DS, olds[0])                       # force the singles too
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.copy_reads == 0
        assert {l[0] for l in prune_pool.snapshot_lists} == {DS} and not any(l[1] for l in prune_pool.snapshot_lists)
        assert prune_pool.calls and all(not c.recursive for c in prune_pool.calls)
        assert prune_pool.names_of(COPY) == {olds[1]}, 'a non-recursive destroy cannot touch a descendant'
        assert not [a for a in prune_pool.source_calls if '-r' in a]
        assert copy_errors(caplog) == []

    def test_a_recursive_dataset_with_nothing_due_reads_no_copies(self, prune_pool):
        manager, dsi = prune_manager(DS, recursive=True)
        fresh, olds = prune_pool.populate(DS, old=0)
        add_copy(prune_pool, COPY, [fresh])
        manager.prune_snapshots(dsi)
        assert prune_pool.copy_reads == 0 and prune_pool.snapshot_lists == [(DS, False)]

    def test_a_dataset_without_snapshots_reads_nothing(self, prune_pool):
        manager, dsi = prune_manager(DS, recursive=True)
        manager.prune_snapshots(dsi)
        assert prune_pool.copy_reads == 0 and prune_pool.events == []


# ---------------------------------------------------------------------------
# The same code over the real libzfseasy commands (only Popen is faked)
# ---------------------------------------------------------------------------


class TestTheRealCommands:
    @pytest.fixture
    def world(self, mock_subprocess):
        w = ZfsWorld(mock_subprocess)
        w.add(DS)
        w.add(CH)
        w.add(COPY, LOCAL)
        w.add(COPY + '/h1', [(PROP_CLIENT_ID, 'h1', 'inherited from pool/data/bk')])
        w.add(f'{COPY}@autosnap_20200101000000', ptype='snapshot')
        w.add(f'{COPY}/h1@autosnap_20200102000000', ptype='snapshot')
        w.add(f'{CH}@autosnap_20200103000000', ptype='snapshot')           # a source child's: not read
        return w

    def manager(self):
        manager, dsi = prune_manager(DS, recursive=True)
        return manager, dsi

    def test_the_exact_argv_of_the_copy_read_and_the_one_listing_per_top(self, world):
        manager, dsi = self.manager()
        protected = manager._copy_protection(dsi)
        assert protected == {
            'autosnap_20200101000000': f'it is also held by the backup copy {COPY}@autosnap_20200101000000',
            'autosnap_20200102000000': f'it is also held by the backup copy {COPY}/h1@autosnap_20200102000000',
        }
        assert world.argvs[0] == copy_read_argv(DS)
        assert len(world.argvs) == 2, 'one copy read and one listing (one top)'
        listing = world.argvs[1]
        assert listing[0] == 'list' and listing[-1] == COPY
        assert {'-H', '-r'} <= set(listing)
        assert listing[listing.index('-t') + 1] == 'snapshot'

    def test_a_listing_with_a_name_libzfseasy_cannot_represent_is_untrusted_and_raises(self, world):
        """The listing is strict: a skipped row means the protect-set might be incomplete."""
        world.add(f'{COPY}@bad!', ptype='snapshot')
        manager, dsi = self.manager()
        with pytest.raises(Exception):
            manager._copy_protection(dsi)

    def test_a_failing_copy_read_raises(self, world):
        world.fail(lambda argv: argv[0] == 'get' and 'inherited' in ' '.join(argv), 'cannot open pool/data: busy')
        manager, dsi = self.manager()
        with pytest.raises(Exception, match='busy'):
            manager._copy_protection(dsi)

    def test_a_failing_listing_raises(self, world):
        world.fail(lambda argv: argv[0] == 'list', 'cannot open pool/data/bk: I/O error')
        manager, dsi = self.manager()
        with pytest.raises(Exception, match='I/O error'):
            manager._copy_protection(dsi)

    def test_no_copy_means_one_read_and_no_listing(self, mock_subprocess):
        world = ZfsWorld(mock_subprocess)
        world.add(DS)
        world.add(CH, [(PROP_CONFIG, 'x', 'local')])
        manager, dsi = self.manager()
        assert manager._copy_protection(dsi) == {}
        assert [a[0] for a in world.argvs] == ['get']


# ---------------------------------------------------------------------------
# A2b review fixes
# ---------------------------------------------------------------------------

TGT = 'pool/data/vault'          # a receive target strictly below the recursive dataset


def targeted(dataset=DS, *, server=None, local=(), recursive=True, dry_run=False):
    """`(manager, dsi)` for `dataset` with the server target `server` and the local `zfs:` targets `local`."""
    parent = DatasetConfig(name=dataset, recursive=recursive, retention_rules=retention_1h_2h())
    cfg = BackupConfig(
        datasets=[parent], snapshot_prefix=PREFIX, dry_run=dry_run,
        destinations={f'loc{i}': Destination(url=f'zfs:{t}') for i, t in enumerate(local)},
        remote_backup=RemoteServerConfig(target_dataset=server) if server else None,
    )
    manager = DatasetManager(cfg)
    return manager, next(d for d in manager.datasets if d.name == dataset)


def exists_calls():
    """The datasets `zfs.exists` (a `zfs.list` with no `types`) was asked about, in order."""
    return [c.kwargs['roots'].name for c in zfs.list.call_args_list if c.kwargs.get('types') is None]


@pytest.fixture
def tree(prune_pool):
    """`pool/data` and `pool/data/c` with the same six old names and a fresh one; no copy anywhere."""
    fresh, olds = prune_pool.populate(DS, old=6)
    prune_pool.snaps[CH] = list(prune_pool.snaps[DS])
    return prune_pool, fresh, olds


class TestCopyTopsFromReceiveTargets:
    """#2: a receive target strictly below the recursive dataset is a copy top with no marker at all (a `send
    -R` replica, a copy whose properties were cleared). It is checked with `zfs.exists` (one `zfs list -H -o
    name,type <target>`) and dropped when it does not exist yet."""

    @pytest.mark.parametrize('flavour', ['server', 'server-trailing-slash', 'local'])
    def test_a_markerless_target_below_the_dataset_is_protected(self, tree, caplog, flavour):
        pool, fresh, olds = tree
        pool.snaps[TGT] = [olds[1], olds[3]]                     # received there, no markers on it
        manager, dsi = (targeted(server=TGT + ('/' if 'slash' in flavour else ''))
                        if flavour.startswith('server') else targeted(local=[TGT]))
        (top, why), = manager._copy_tops(dsi).items()
        assert top == TGT and 'receive target' in why
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            result = manager.prune_snapshots(dsi)
        kept = {fresh, olds[1], olds[3]}
        assert pool.names_of(TGT) == {olds[1], olds[3]}, 'the target lost nothing'
        assert pool.names_of(DS) == kept and pool.names_of(CH) == kept
        assert result.ok
        assert (TGT, True) in pool.snapshot_lists
        (error,) = copy_errors(caplog)
        assert error.startswith(f'{TGT} is a backup copy') and 'receive target' in error

    def test_the_control_without_the_target_in_the_config_the_same_names_go(self, tree):
        pool, fresh, olds = tree
        pool.snaps[TGT] = [olds[1], olds[3]]
        manager, dsi = targeted()                                 # no receive target configured
        manager.prune_snapshots(dsi)
        assert pool.names_of(TGT) == set()

    def test_a_target_below_is_not_made_of_a_name_prefix(self, tree):
        pool, fresh, olds = tree
        pool.snaps['pool/datax'] = [olds[1]]
        manager, dsi = targeted(local=['pool/datax', 'pool/data2/vault'])
        assert manager._copy_tops(dsi) == {}
        assert exists_calls() == []

    def test_a_nonexistent_target_is_dropped_after_one_existence_check(self, tree, caplog):
        pool, fresh, olds = tree
        pool.missing.add(TGT)
        manager, dsi = targeted(server=TGT)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            assert manager._copy_tops(dsi) == {}
            result = manager.prune_snapshots(dsi)
        assert exists_calls()[0] == TGT and set(exists_calls()) == {TGT}
        assert all(l[0] != TGT for l in pool.snapshot_lists), 'nothing is listed under a target that is not there'
        assert result.ok and pool.names_of(DS) == {fresh}, 'the tree is pruned as if it had no target'
        assert copy_errors(caplog) == [] and manager._copy_protect_noted == set()

    def test_the_existence_check_is_the_exact_zfs_list_argv(self, mock_subprocess):
        world = ZfsWorld(mock_subprocess)
        world.add(DS)
        world.add(CH)
        world.add(TGT)
        manager, dsi = targeted(local=[TGT])
        assert list(manager._copy_tops(dsi)) == [TGT]
        assert world.argvs[0][:2] == ['get', '-H']                # the copy read first
        assert world.argvs[1] == ['list', '-H', '-o', 'name,type', TGT]
        assert len(world.argvs) == 2

    def test_a_nonexistent_target_is_dropped_over_the_real_command(self, mock_subprocess):
        world = ZfsWorld(mock_subprocess)
        world.add(DS)
        world.add(CH)                                               # the target is not in the world
        manager, dsi = targeted(local=[TGT])
        assert manager._copy_tops(dsi) == {}
        assert world.argvs[1] == ['list', '-H', '-o', 'name,type', TGT]
        assert len(world.argvs) == 2

    def test_an_existence_check_that_fails_raises_a_runtime_error_naming_both(self, mock_subprocess):
        world = ZfsWorld(mock_subprocess)
        world.add(DS)
        world.add(TGT)
        world.fail(lambda argv: argv[0] == 'list', 'cannot open: pool I/O is currently suspended')
        manager, dsi = targeted(local=[TGT])
        with pytest.raises(RuntimeError) as info:
            manager._copy_tops(dsi)
        message = str(info.value)
        assert TGT in message and DS in message and 'suspended' in message
        assert isinstance(info.value.__cause__, Exception) and 'suspended' in str(info.value.__cause__)

    def test_an_existence_check_that_fails_skips_the_prune_and_destroys_nothing(self, tree, mocker, caplog):
        pool, fresh, olds = tree
        mocker.patch('zfsbackup.backup_manager.zfs.exists', side_effect=OSError('pool I/O suspended'))
        manager, dsi = targeted(local=[TGT])
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            result = manager.prune_snapshots(dsi)
        assert pool.calls == [] and pool.names_of(DS) == set(olds) | {fresh}
        assert result.skipped and result.skipped.startswith('cannot evaluate the descendants')
        (error,) = evaluation_errors(caplog)
        assert TGT in error and 'pool I/O suspended' in error

    def test_a_timeout_in_the_existence_check_passes_through_unchanged(self, tree, mocker):
        pool, fresh, olds = tree
        timeout = ZFSTimeoutError('too slow')
        mocker.patch('zfsbackup.backup_manager.zfs.exists', side_effect=timeout)
        manager, dsi = targeted(local=[TGT])
        with pytest.raises(ZFSTimeoutError) as info:
            manager._copy_tops(dsi)
        assert info.value is timeout

    def test_a_target_found_by_a_marker_too_reports_the_marker_and_is_not_checked(self, tree):
        pool, fresh, olds = tree
        add_copy(pool, TGT, [olds[1]])                              # LOCAL markers on the target itself
        manager, dsi = targeted(local=[TGT])
        (top, why), = manager._copy_tops(dsi).items()
        assert top == TGT and PROP_CLIENT_ID in why and 'source: local' in why
        assert 'receive target' not in why
        assert exists_calls() == [], 'the marker proves it exists: no zfs.exists'

    def test_a_marker_less_copy_outside_any_receive_target_is_not_detected_documented_limit(self, tree):
        """The remaining blind spot (the docstring's "limit"): with no markers and no known target above it, a
        copy cannot be told from a source child, so its names are destroyed with the tree's."""
        pool, fresh, olds = tree
        pool.snaps[COPY] = [olds[1]]
        manager, dsi = targeted(local=['pool/elsewhere/vault'], server='pool/other')
        assert manager._copy_tops(dsi) == {}
        manager.prune_snapshots(dsi)
        assert pool.names_of(COPY) == set()
        assert pool.names_of(DS) == {fresh}

    def test_the_topmost_wins_a_target_above_a_marker_copy(self, tree):
        pool, fresh, olds = tree
        add_copy(pool, TGT + '/h1/pool/x', [olds[1]])               # a marked copy inside the target
        pool.snaps[TGT] = []
        manager, dsi = targeted(local=[TGT])
        (top, why), = manager._copy_tops(dsi).items()
        assert top == TGT and 'receive target' in why
        assert exists_calls() == [TGT]

    def test_the_topmost_wins_a_marker_above_a_target(self, tree):
        pool, fresh, olds = tree
        add_copy(pool, 'pool/data/bk', [olds[1]])
        manager, dsi = targeted(local=['pool/data/bk/vault'])
        (top, why), = manager._copy_tops(dsi).items()
        assert top == 'pool/data/bk' and PROP_CLIENT_ID in why
        assert exists_calls() == [], 'the nested target is covered by the marker top and never checked'

    def test_nested_targets_pick_the_topmost_and_siblings_are_both_tops(self, tree):
        pool, fresh, olds = tree
        manager, dsi = targeted(server=TGT + '/srv', local=[TGT, 'pool/data/other'])
        assert sorted(manager._copy_tops(dsi)) == sorted([TGT, 'pool/data/other'])
        assert exists_calls() == sorted([TGT, 'pool/data/other']), 'the nested server target is never checked'

    def test_a_nonexistent_topmost_target_is_dropped_even_with_a_nested_one(self, tree):
        pool, fresh, olds = tree
        pool.missing.add(TGT)
        manager, dsi = targeted(server=TGT + '/srv', local=[TGT])
        assert manager._copy_tops(dsi) == {}
        assert exists_calls() == [TGT]

    @pytest.mark.parametrize('target', ['pool', DS, DS + '/'], ids=['above', 'equal', 'equal-slash'])
    def test_a_target_at_or_above_the_dataset_makes_the_dataset_the_top(self, tree, target):
        pool, fresh, olds = tree
        manager, dsi = targeted(server=target) if target.endswith('/') else targeted(local=[target])
        (top, why), = manager._copy_tops(dsi).items()
        assert top == DS and 'itself at or below the receive target' in why
        assert exists_calls() == [DS]

    def test_only_snapshots_strictly_below_the_dataset_are_protected_when_it_is_the_top(self, tree):
        pool, fresh, olds = tree
        pool.snaps[CH] = [olds[2]]                                  # the child holds one name; DS holds all
        manager, dsi = targeted(local=['pool'])
        assert manager._copy_protection(dsi) == {
            olds[2]: f'it is also held by the backup copy {CH}@{olds[2]}'}
        assert (DS, True) in pool.snapshot_lists
        assert [l for l in pool.snapshot_lists if l[1]] == [(DS, True)], 'one listing, rooted at the dataset'

    def test_a_dataset_inside_a_target_with_markers_below_it_still_makes_it_the_top(self, tree):
        pool, fresh, olds = tree
        add_copy(pool, COPY, [olds[1]])
        manager, dsi = targeted(local=['pool'])
        assert list(manager._copy_tops(dsi)) == [DS]               # the marker top is nested below it


class TestTheCopyErrorCarriesTheRemedy:
    """M1: the ERROR says how to tell a stray snapshot (`zfs snapshot -r` of an older version, or the snapshot
    worker's fallback) from the copy's received backup history: a stray has the SAME createtxg as `<ds>@<name>`;
    every other snapshot in the copy is history and must never be destroyed. (It used to say to destroy the copy's
    `<prefix>_*` snapshots its source does not have, which is exactly how a replica's history is lost.)"""

    def check(self, error, dataset, top):
        assert f"zfs get -H -o name,value createtxg {dataset}@<name> {top}@<name>" in error
        assert f'has the SAME createtxg as {dataset}@<name>' in error
        assert 'must never be destroyed' in error and 'received backup history' in error
        assert 'older versions' in error and "the snapshot worker's fallback" in error
        assert error.index('zfs rename') < error.index('SAME createtxg'), 'the order: move it out, then the check'
        for old in ("its source does not have", f"@{PREFIX}_*", 'moved out'):
            assert old not in error, f'the old advice is gone: {old!r}'
        # The word "destroy" survives in exactly two places: what a RECURSIVE destroy of the source would do to the
        # copy (the reason for the protection), and "must never be destroyed". Nothing tells the operator to
        # destroy anything in the copy.
        rest = error.replace(f"recursive destroy of {dataset}@<snapshot> would destroy the copy's snapshot of "
                             "that name", '').replace('must never be destroyed', '')
        assert 'destroy' not in rest.lower(), rest

    def test_the_error_names_the_createtxg_discriminator_and_that_history_is_never_destroyed(
            self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, olds)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        (error,) = copy_errors(caplog)
        self.check(error, DS, COPY)

    def test_the_remedy_is_in_the_target_case_too(self, tree, caplog):
        pool, fresh, olds = tree
        pool.snaps[TGT] = list(olds)
        manager, dsi = targeted(local=[TGT])
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        (error,) = copy_errors(caplog)
        assert TGT in error
        self.check(error, DS, TGT)


class TestCopyReadErrorsNameTheirContext:
    """#4a: a failed read is a `RuntimeError` that says what was being read and where, with the cause chained; a
    `ZFSTimeoutError` is never re-wrapped (the timed callers branch on it)."""

    @pytest.fixture
    def world(self, mock_subprocess):
        w = ZfsWorld(mock_subprocess)
        w.add(DS)
        w.add(CH)
        w.add(COPY, LOCAL)
        w.add(f'{COPY}@autosnap_20200101000000', ptype='snapshot')
        return w

    @staticmethod
    def manager():
        return prune_manager(DS, recursive=True)

    def test_a_failed_marker_read_names_the_markers_and_the_dataset_and_chains_the_cause(self, world):
        world.fail(lambda argv: argv[0] == 'get' and 'inherited' in ' '.join(argv),
                   'cannot open pool/data: pool I/O is currently suspended')
        with pytest.raises(RuntimeError) as info:
            DatasetManager._copy_roots(DS)
        message = str(info.value)
        assert message.startswith('cannot read the backup-copy markers')
        assert PROP_CLIENT_ID in message and PROP_SOURCE_DATASET in message
        assert f'at or below {DS}' in message and 'currently suspended' in message
        cause = info.value.__cause__
        assert cause is not None and 'currently suspended' in str(cause)

    def test_the_same_error_reaches_copy_tops_and_copy_protection_unchanged(self, world):
        world.fail(lambda argv: argv[0] == 'get' and 'inherited' in ' '.join(argv), 'boom')
        manager, dsi = self.manager()
        for call in (manager._copy_tops, manager._copy_protection):
            with pytest.raises(RuntimeError, match='cannot read the backup-copy markers'):
                call(dsi)

    def test_a_failed_listing_names_the_copy_and_the_dataset_and_chains_the_cause(self, world):
        world.fail(lambda argv: argv[0] == 'list', 'cannot open pool/data/bk: I/O error')
        manager, dsi = self.manager()
        with pytest.raises(RuntimeError) as info:
            manager._copy_protection(dsi)
        assert str(info.value).startswith(f'cannot list the snapshots of the backup copy {COPY} below {DS}')
        assert 'I/O error' in str(info.value)
        assert info.value.__cause__ is not None and 'I/O error' in str(info.value.__cause__)

    def test_an_untrusted_listing_is_wrapped_too_with_the_original_as_the_cause(self, world):
        world.add(f'{COPY}@bad!', ptype='snapshot')
        manager, dsi = self.manager()
        with pytest.raises(RuntimeError) as info:
            manager._copy_protection(dsi)
        assert f'backup copy {COPY} below {DS}' in str(info.value)
        assert 'could not be trusted' in str(info.value.__cause__)

    def test_a_timeout_in_the_marker_read_is_not_wrapped(self, mocker):
        timeout = ZFSTimeoutError('slow')
        mocker.patch('zfsbackup.backup_manager.zfs.get.property_sources', side_effect=timeout)
        with pytest.raises(ZFSTimeoutError) as info:
            DatasetManager._copy_roots(DS)
        assert info.value is timeout and info.value.__cause__ is None

    def test_a_timeout_in_the_listing_is_not_wrapped(self, prune_pool, mocker):
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, [olds[1]])
        timeout = ZFSTimeoutError('slow')
        prune_pool.list_errors[COPY] = timeout
        with pytest.raises(ZFSTimeoutError) as info:
            manager._copy_protection(dsi)
        assert info.value is timeout

    def test_a_timed_re_read_lets_the_timeout_escape_and_an_untimed_one_logs_an_error(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool)
        group = manager.list_snapshots(dsi)
        prune_pool.copy_read_error = ZFSTimeoutError('slow')
        manager._prune_timed = True                                # a local copy's pass runs under timeouts
        with pytest.raises(ZFSTimeoutError):
            manager._still_prunable(dsi, group)
        assert prune_pool.calls == []
        manager._prune_timed = False
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            assert manager._still_prunable(dsi, group) == []
        assert len(records(caplog, ERR, 'Cannot re-read what protects')) == 1
        assert manager._reread_error == 'slow'

    def test_expand_recursive_reports_the_failed_marker_read_with_its_context(self, world):
        world.fail(lambda argv: argv[0] == 'get' and 'inherited' in ' '.join(argv),
                   'cannot open pool/data: pool I/O is currently suspended')
        cfg = BackupConfig(
            datasets=[DatasetConfig(name=DS, recursive=True, retention_rules=retention_1h_2h(),
                                    remote=[RemoteDatasetConfig(destination='off')])],
            snapshot_prefix=PREFIX, destinations={'off': Destination(url='https://h:8443')})
        expansion = DatasetManager(cfg).expand_recursive()
        assert expansion.children == [] and list(expansion.failed) == [DS]
        failed = expansion.failed[DS]
        assert failed.startswith('RuntimeError: cannot read the backup-copy markers')
        assert f'at or below {DS}' in failed and 'currently suspended' in failed


class TestRereadLogsOncePerPass:
    """#4c: the re-read before each destroy call used to log the same ERROR per batch and per single retry. The
    first occurrence in a pass is ERROR (a failed read) or INFO (a protected name), repeats are DEBUG, and the
    next pass starts afresh."""

    NOISY = 'Cannot re-read what protects'

    def test_repeated_re_read_failures_in_one_pass_are_one_error_then_debug(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool, old=4)
        prune_pool.send(DS, olds[0])                               # the batch fails; every single is re-read

        def after_batch(pool, call):
            if len(call.names) > 1:
                pool.copy_read_error = RuntimeError('pool I/O is currently suspended')

        prune_pool.before_destroy = after_batch
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        errors = records(caplog, ERR, self.NOISY)
        debugs = records(caplog, logging.DEBUG, self.NOISY)
        assert len(errors) == 1
        assert len(debugs) == len(olds) - 1, 'every other refused single retry is a repeat'
        assert len(errors) + len(debugs) == prune_pool.copy_reads - 2
        assert 'currently suspended' in errors[0] and 'currently suspended' in debugs[0]
        assert prune_pool.sizes == [len(olds)], 'only the batch reached zfs'

    def test_the_next_pass_logs_its_first_failure_at_error_again(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool, old=25)          # two batches: two re-reads per pass
        start = {'reads': 0}

        def fail_after_the_first_read_of_the_pass(pool, n):
            pool.copy_read_error = RuntimeError('boom') if n > start['reads'] + 1 else None

        prune_pool.on_copy_read = fail_after_the_first_read_of_the_pass
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            for passes in (1, 2, 3):
                manager.prune_snapshots(dsi)
                assert len(records(caplog, ERR, self.NOISY)) == passes, 'one ERROR per pass'
                start['reads'] = prune_pool.copy_reads
        assert len(records(caplog, logging.DEBUG, self.NOISY)) == 3, 'the second batch of each pass'
        assert prune_pool.calls == []

    def test_two_direct_re_reads_in_one_pass_are_one_error_then_debug(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool, old=3)
        group = manager.list_snapshots(dsi)
        prune_pool.copy_read_error = RuntimeError('boom')
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            manager._still_prunable(dsi, group)
            manager._still_prunable(dsi, group)
        assert len(records(caplog, ERR, self.NOISY)) == 1
        assert len(records(caplog, logging.DEBUG, self.NOISY)) == 1

    def test_a_protected_name_is_info_once_in_a_pass_then_debug(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool, old=3, recursive=False)
        prune_pool.props[DS] = [(PROP_ANCHOR_PREFIX + 'offsite', olds[1], 'local')]
        group = [s for s in manager.list_snapshots(dsi) if s.name == olds[1]]
        assert group
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            assert manager._still_prunable(dsi, group) == []
            assert manager._still_prunable(dsi, group) == []        # the same name again, same pass
        infos = records(caplog, INFO, 'it became protected')
        debugs = records(caplog, logging.DEBUG, 'it became protected')
        assert len(infos) == 1 and len(debugs) == 1
        assert f'{DS}@{olds[1]}' in infos[0] and 'a backup copy below it' in infos[0]

    def test_a_protected_name_is_info_again_in_the_next_pass(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool, old=3)
        manager.prune_snapshots(dsi)                                # nothing special: the pass resets the memory
        assert manager._reread_logged == set()
        calls = []

        def protect_the_second(d, own_properties_last=False):
            calls.append(1)
            return set() if len(calls) == 1 else {olds[1]}          # seen only by the re-read

        manager.get_anchors = protect_the_second
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            for _ in range(2):
                prune_pool.snaps[DS] = olds + [fresh]
                prune_pool.snaps[CH] = olds + [fresh]
                calls.clear()
                manager.prune_snapshots(dsi)
        assert len(records(caplog, INFO, 'it became protected')) == 2, 'one per pass'
        assert records(caplog, logging.DEBUG, 'it became protected') == []

    def test_the_memory_is_reset_by_each_pass_not_by_a_dataset(self, prune_pool):
        manager, dsi, fresh, olds = build(prune_pool, old=2)
        manager._reread_logged.add((DS, None))
        manager.prune_snapshots(dsi)
        assert (DS, None) not in manager._reread_logged


class TestDescendantRecoveryIsAnnounced:
    """#4b: the ERROR "Cannot evaluate descendants" has its matching INFO, both when the descendants are read
    again and when the pass has nothing due (so it does not evaluate them)."""

    @staticmethod
    def fail(pool):
        pool.copy_read_error = RuntimeError('pool I/O is currently suspended')

    @staticmethod
    def heal(pool):
        pool.copy_read_error = None

    def test_info_when_the_descendants_can_be_evaluated_again(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool)
        self.fail(prune_pool)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            assert records(caplog, INFO, 'can be evaluated again') == []
            self.heal(prune_pool)
            result = manager.prune_snapshots(dsi)
        (info,) = records(caplog, INFO, 'can be evaluated again')
        assert info.startswith(f'The descendants of {DS} ') and 'pruning resumes' in info
        assert result.ok and manager._descendant_error_noted == set()
        assert len(evaluation_errors(caplog)) == 1

    def test_info_once_and_a_later_failure_is_a_new_episode(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            self.fail(prune_pool)
            manager.prune_snapshots(dsi)
            self.heal(prune_pool)
            manager.prune_snapshots(dsi)
            prune_pool.snaps[DS] = olds + [fresh]
            manager.prune_snapshots(dsi)                           # healthy again: no second INFO
            self.fail(prune_pool)
            prune_pool.snaps[DS] = olds + [fresh]
            manager.prune_snapshots(dsi)
        assert len(records(caplog, INFO, 'can be evaluated again')) == 1
        assert len(evaluation_errors(caplog)) == 2

    def test_info_when_nothing_is_due_so_the_descendants_are_not_evaluated(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool)
        self.fail(prune_pool)
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
            reads = prune_pool.copy_reads
            prune_pool.snaps[DS] = [fresh]                          # the candidates went another way
            result = manager.prune_snapshots(dsi)
            repeat = manager.prune_snapshots(dsi)
        assert prune_pool.copy_reads == reads, 'nothing due: no copy read'
        (info,) = records(caplog, INFO, 'Nothing is due for pruning in')
        assert DS in info and 'not evaluated this cycle' in info and 'pruning resumes' in info
        assert records(caplog, INFO, 'can be evaluated again') == []
        assert manager._descendant_error_noted == set()
        assert result.ok and repeat.ok

    def test_a_quiet_pass_with_nothing_due_logs_no_recovery_when_there_was_no_error(self, prune_pool, caplog):
        manager, dsi, fresh, olds = build(prune_pool)
        prune_pool.snaps[DS] = [fresh]
        with caplog.at_level(INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert records(caplog, INFO, 'Nothing is due for pruning in') == []
        assert records(caplog, INFO, 'can be evaluated again') == []


class TestTheFakesAnswerTheExplicitPropertyListToo:
    """The copy read may name the two markers instead of `all` (`property_sources(properties=[...])`): zfs then
    prints exactly those rows, an unset one as a source `-` row, and libzfseasy drops those. The fakes answer both
    forms (`unset_rows` prints the `-` rows even under `-s`, which real zfs does not), so a test is indifferent to
    which one `_copy_roots` sends."""

    MARKERS = [PROP_CLIENT_ID, PROP_SOURCE_DATASET]

    @pytest.fixture
    def world(self, mock_subprocess):
        w = ZfsWorld(mock_subprocess)
        w.add(DS)
        w.add(CH, [(PROP_CONFIG, 'x', 'local')])                       # another property of ours: not a marker
        w.add(COPY, [(PROP_CLIENT_ID, 'h1', 'local')])                 # one marker set, the other unset
        w.add(COPY + '/h1', INHERITED)
        w.unset_rows = True
        return w

    def read(self, **kw):
        return zfs.get.property_sources(
            Dataset(DS),
            types='filesystem,volume', sources=['local', 'received', 'inherited'], recursive=True,
            prefix=self.MARKERS, **kw)

    def test_the_explicit_list_form_ignores_the_unset_rows_and_matches_the_all_form(self, world):
        explicit = self.read(properties=self.MARKERS)
        argv = world.argvs[-1]
        assert argv[argv.index('-t') + 2] == ','.join(self.MARKERS)
        assert world.argvs[-1][-1] == DS
        plain = self.read()
        key = lambda rows: sorted((r.name, r.property, r.source) for r in rows)       # noqa: E731
        assert key(explicit) == key(plain)
        assert (COPY, PROP_SOURCE_DATASET) not in {(r.name, r.property) for r in explicit}, 'the `-` row is dropped'

    @pytest.mark.parametrize('unset_rows', [False, True])
    def test_copy_roots_is_the_same_with_and_without_unset_rows(self, world, unset_rows):
        world.unset_rows = unset_rows
        assert DatasetManager._copy_roots(DS) == {COPY: 'local', COPY + '/h1': 'inherited from pool/data/bk'}

    def test_the_pruner_over_a_world_with_unset_rows_protects_the_same_names(self, world):
        world.add(f'{COPY}@autosnap_20200101000000', ptype='snapshot')
        manager, dsi = prune_manager(DS, recursive=True)
        assert manager._copy_protection(dsi) == {
            'autosnap_20200101000000': f'it is also held by the backup copy {COPY}@autosnap_20200101000000'}

    def test_the_prune_pool_ignores_unset_rows_too(self, prune_pool):
        prune_pool.unset_rows = True
        manager, dsi, fresh, olds = build(prune_pool)
        add_copy(prune_pool, COPY, [olds[1]], [(PROP_CLIENT_ID, 'h1', 'local')])
        manager.prune_snapshots(dsi)
        assert prune_pool.names_of(COPY) == {olds[1]} and olds[1] in prune_pool.names_of(DS)


# Mutation counts (A2b; zfsbackup/backup_manager.py only, one revert at a time; failing tests across this file,
# test_zfsbackup_prune_descendants, test_zfsbackup_manager and test_zfsbackup_prune_local_copies): the copy
# protection returns nothing 41; not merged into the pass's protect-set (re-read only) 22; no copy read in
# `_still_prunable` 18; a non-recursive pass reads copies 18; the copy read without `inherited` 26; the copy read
# after the pins in `_still_prunable` 8; a failure in the pass swallowed 9; ERROR every pass 15; descendants of a
# copy not protected 6; tops not reduced to the topmost 4; the dataset itself counted as a copy 3; the INFO when
# a copy is gone dropped 3; listing not strict 2; episode memory cleared for a recursive dataset too 1.

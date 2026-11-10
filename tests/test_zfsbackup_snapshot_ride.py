"""A2e: a configured child rides along with its enabled recursive configured ancestor's snapshot.

Two datasets of one tree, both configured, both hourly: `pool/data` (recursive) and `pool/data/c`. Both generate
the same name for the period. If the child went FIRST (config order) it took `@N`, and the parent's `snapshot -r`
(or the A2c atomic list) then failed with "already exists" every period, so the parent's whole tree was never
snapshotted. Now:

* `SnapshotWorker._get_datasets` depth-sorts (ancestors first, stable) and `begin_snapshot_cycle()` forgets the
  last cycle's records;
* `create_snapshot` of a recursive dataset records `_tree_snapshots[name] = (snapshot name, cover)` ONLY after the
  recursive command succeeded (`cover`: the names the atomic list covered, `None` for `snapshot -r`, every
  descendant). The record is dropped at the start of the next `create_snapshot` and on every failure; a dry run, a
  refusal and a failure leave none;
* `ridden_snapshot(dsi)` is a `SnapshotRide` when that record covers `dsi` for the name `dsi` itself would generate
  for this period AND one listing of `dsi` confirms the name exists; otherwise `None`;
* the worker then records `ok=True, detail=<name>` and creates nothing, with no WARNING. A name that merely EXISTS
  (no record: a clock that went backwards) is still the WARNING-once, DEBUG-after of `needs_snapshot`, except that
  a covering record makes it DEBUG.

The real manager and the real `SnapshotWorker` over `tests/_snapshot_world.py::SnapWorld` (a `zfs` that takes
snapshots atomically), so "exactly one snapshot command" and "the child holds the name" are read off the world.
"""

import logging
from datetime import timedelta
from types import SimpleNamespace

import pytest

from _prune_support import records
from _snapshot_support import frozen_clock  # noqa: F401  (fixture)
from _snapshot_world import LOCAL, SnapWorld, inherited, make_manager
from zfsbackup.backup_manager import DatasetManager, SnapshotRide
from zfsbackup.config import DatasetConfig, RetentionRule
from zfsbackup.stopflag import StopFlag
from zfsbackup.workers import SnapshotWorker

DS = 'pool/data'
CH = 'pool/data/c'
GC = 'pool/data/c/g'
VOL = 'pool/data/v'
COPY = 'pool/data/bk'
NAME = 'autosnap_20261022100000'            # what the frozen clock (10:30) generates hourly
QUARTER = 'autosnap_20261022103000'         # ... and every 15 minutes
LOGGER = 'zfsbackup.backup_manager'
WARN, DEBUG = logging.WARNING, logging.DEBUG

pytestmark = pytest.mark.unit


class Rig:
    """The world, the manager and a real `SnapshotWorker` whose `_record` is spied on."""

    def __init__(self, mocker, mock_subprocess, *, child_first=False, children=(), copy=False,
                 parent_recursive=True, dry_run=False, parent_frequency=None, targets=()):
        self.world = SnapWorld(mock_subprocess)
        self.world.add(DS).add(CH).add(GC).add(VOL, ptype='volume')
        if copy:
            self.world.add(COPY, LOCAL).add(COPY + '/h1', inherited(COPY)).add(COPY + '/h1/pool', inherited(COPY))
        kids = list(children) if children else [DatasetConfig(name=CH)]
        self.manager, self.parent = make_manager(
            DS, recursive=parent_recursive, dry_run=dry_run, retention=False, targets=targets,
            frequency=parent_frequency, **({'first': kids} if child_first else {'extra': kids}),
        )
        self.worker = SnapshotWorker(self.manager.config, StopFlag())
        self.recorded = []
        mocker.patch.object(self.worker, '_record', side_effect=self._spy)

    def _spy(self, kind, dataset, destination=None, *, ok, error=None, detail=None):
        self.recorded.append(SimpleNamespace(kind=kind, dataset=dataset, ok=ok, error=error, detail=detail))

    def dsi(self, name):
        return next(d for d in self.manager.datasets if d.name == name)

    def cycle(self, after=None):
        """What `BaseWorker.run` does each cycle: `_get_datasets`, then `_process_dataset` for each. `after`
        maps a dataset name to a callable run right after that dataset's turn (the clock stepping across a
        period boundary inside one cycle)."""
        self.recorded.clear()
        order = self.worker._get_datasets(self.manager)
        for dsi in order:
            self.worker._process_dataset(self.manager, dsi)
            if after and dsi.name in after:
                after[dsi.name]()
        return [d.name for d in order]

    def record_of(self, name):
        (rec,) = [r for r in self.recorded if r.dataset == name]
        return rec


@pytest.fixture
def make_rig(mocker, mock_subprocess, frozen_clock):
    def build(**kw):
        return Rig(mocker, mock_subprocess, **kw)
    return build


def next_cycle(clock, delta=timedelta(hours=1)):
    clock.advance(delta)


def at(clock):
    """The hourly name for the frozen clock's current period."""
    return clock.now.replace(minute=0, second=0, microsecond=0).strftime('autosnap_%Y%m%d%H%M%S')


# ---------------------------------------------------------------------------
# The worker: order and the ride
# ---------------------------------------------------------------------------



class TestChildListedBeforeParent:
    def test_the_worker_snapshots_the_parent_first(self, make_rig):
        rig = make_rig(child_first=True)
        assert [d.name for d in rig.manager.datasets] == [CH, DS], 'config order: the child is first'
        assert rig.cycle() == [DS, CH]

    def test_one_command_no_warning_and_the_child_is_ok_with_the_parents_name(self, make_rig, caplog):
        rig = make_rig(child_first=True)
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle()
        assert rig.world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']], 'exactly one command'
        assert records(caplog, WARN) == [] and records(caplog, logging.ERROR) == []
        parent, child = rig.record_of(DS), rig.record_of(CH)
        assert (parent.ok, parent.detail) == (True, NAME)
        assert (child.ok, child.detail, child.error) == (True, NAME, None)
        assert rig.world.names_of(CH) == {NAME} and rig.world.names_of(DS) == {NAME}
        assert rig.world.names_of(GC) == {NAME}, 'the whole tree, the unconfigured grandchild included'
        assert len({txg for _, _, txg in rig.world.created}) == 1, 'one txg: the one command'

    def test_the_ride_is_the_same_in_config_order_parent_first(self, make_rig, caplog):
        rig = make_rig()
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle()
        assert len(rig.world.commands_of('snapshot')) == 1 and records(caplog, WARN) == []
        assert (rig.record_of(CH).ok, rig.record_of(CH).detail) == (True, NAME)

    def test_every_period_has_the_whole_tree_snapshotted_and_the_child_ok(self, make_rig, frozen_clock, caplog):
        """The regression: the child used to take `@N` first and the parent's command failed every period."""
        rig = make_rig(child_first=True)
        seen = []
        with caplog.at_level(DEBUG, logger=LOGGER):
            for _ in range(4):
                rig.cycle()
                name = at(frozen_clock)
                seen.append(name)
                for dataset in (DS, CH, GC, VOL):
                    assert name in rig.world.names_of(dataset), (dataset, name)
                assert len(rig.recorded) == 2 and all(r.ok and r.detail == name for r in rig.recorded)
                next_cycle(frozen_clock)
        assert len(set(seen)) == 4 and rig.world.names_of(CH) == set(seen) == rig.world.names_of(DS)
        assert len(rig.world.commands_of('snapshot')) == 4, 'one command a period'
        assert records(caplog, WARN) == [] and records(caplog, logging.ERROR) == []

    def test_a_check_inside_the_period_does_nothing_and_records_nothing(self, make_rig, frozen_clock):
        rig = make_rig(child_first=True)
        rig.cycle()
        next_cycle(frozen_clock, timedelta(minutes=10))
        rig.cycle()
        assert rig.recorded == [] and len(rig.world.commands_of('snapshot')) == 1


class TestCoverage:
    def test_a_child_covered_by_the_atomic_list_rides(self, make_rig, caplog):
        rig = make_rig(child_first=True, copy=True)
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle()
        (cmd,) = rig.world.commands_of('snapshot')
        assert '-r' not in cmd and f'{CH}@{NAME}' in cmd, 'the A2c atomic list, which names the child'
        name, cover = rig.manager._tree_snapshots[DS]
        assert name == NAME and cover == frozenset({DS, CH, GC, VOL})
        assert (rig.record_of(CH).ok, rig.record_of(CH).detail) == (True, NAME)
        assert records(caplog, WARN) == []
        assert rig.world.snapshots_below(COPY) == {}

    def test_a_child_that_is_a_copy_top_is_not_covered_and_takes_its_own_snapshot(self, make_rig, caplog):
        """A configured dataset that is itself (inside) a backup copy is left out of the atomic list: it does not
        ride, it takes its own non-recursive snapshot, and nothing says "already exists"."""
        kid = DatasetConfig(name=COPY + '/h1')
        rig = make_rig(child_first=True, copy=True, children=[kid])
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle()
        _, cover = rig.manager._tree_snapshots[DS]
        assert COPY + '/h1' not in cover
        assert rig.manager.ridden_snapshot(rig.dsi(COPY + '/h1')) is None
        assert ['snapshot', f'{COPY}/h1@{NAME}'] in rig.world.commands_of('snapshot'), 'its own command'
        assert rig.record_of(COPY + '/h1').ok is True
        assert records(caplog, WARN) == []

    def test_the_cover_of_snapshot_r_is_none_meaning_every_descendant(self, make_rig):
        rig = make_rig()
        rig.cycle()
        assert rig.manager._tree_snapshots == {DS: (NAME, None)}
        assert rig.manager.ridden_snapshot(rig.dsi(CH)) == SnapshotRide(NAME, DS)

    def test_the_cover_of_the_fallback_is_none_too(self, make_rig, caplog):
        rig = make_rig(copy=True)
        rig.world.fail(lambda argv: argv[0] == 'get' and '-r' in argv, 'cannot read: I/O error')
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle()
        assert rig.world.commands_of('snapshot') == [['snapshot', '-r', f'{DS}@{NAME}']], 'the fallback ran'
        assert rig.manager._tree_snapshots == {DS: (NAME, None)}
        assert (rig.record_of(CH).ok, rig.record_of(CH).detail) == (True, NAME)
        assert records(caplog, WARN) == []

    def test_a_grandchild_rides_too(self, make_rig):
        rig = make_rig(children=[DatasetConfig(name=GC)])
        rig.cycle()
        assert rig.manager.ridden_snapshot(rig.dsi(GC)) == SnapshotRide(NAME, DS)
        assert len(rig.world.commands_of('snapshot')) == 1

    def test_a_dataset_that_merely_shares_the_name_prefix_never_rides(self, make_rig):
        rig = make_rig(children=[DatasetConfig(name='pool/datax')])
        rig.world.add('pool/datax')
        rig.cycle()
        assert rig.manager.ridden_snapshot(rig.dsi('pool/datax')) is None, 'a name prefix is not a parent'
        assert rig.world.names_of('pool/datax') == {NAME}, 'it took its own snapshot'
        assert len(rig.world.commands_of('snapshot')) == 2

    def test_a_non_recursive_parent_covers_nothing(self, make_rig):
        rig = make_rig(parent_recursive=False)
        rig.cycle()
        assert rig.manager._tree_snapshots == {}
        assert rig.manager.ridden_snapshot(rig.dsi(CH)) is None
        assert rig.world.names_of(CH) == {NAME}, 'the child took its own'
        assert len(rig.world.commands_of('snapshot')) == 2

    def test_a_record_of_a_non_recursive_dataset_never_covers(self, make_rig):
        """(Cannot be made by `create_snapshot`; a hand-set record must not count anyway.)"""
        rig = make_rig(parent_recursive=False)
        rig.world.add_snap(CH, NAME)
        rig.manager._tree_snapshots[DS] = (NAME, None)
        assert rig.manager._covering_snapshot(rig.dsi(CH), NAME) is None
        assert rig.manager.ridden_snapshot(rig.dsi(CH)) is None

    def test_only_an_ancestor_counts_not_a_sibling_or_a_descendant(self, make_rig):
        rig = make_rig()
        rig.manager._tree_snapshots[CH] = (NAME, None)            # a record of the CHILD, whatever made it
        rig.world.add_snap(DS, NAME)
        assert rig.manager.ridden_snapshot(rig.parent) is None, 'a descendant never covers its ancestor'
        assert rig.manager._covering_snapshot(rig.parent, NAME) is None

    def test_a_disabled_ancestor_does_not_count(self, make_rig):
        rig = make_rig()
        rig.parent.config.enabled = False
        manager = DatasetManager(rig.manager.config)               # `datasets` holds the enabled ones only
        assert [d.name for d in manager.datasets] == [CH]
        rig.world.add_snap(CH, NAME)
        manager._tree_snapshots[DS] = (NAME, None)                 # (cannot be made; must not count anyway)
        assert manager.ridden_snapshot(manager.datasets[0]) is None
        assert manager._covering_snapshot(manager.datasets[0], NAME) is None

    def test_a_disabled_parent_leaves_the_child_to_snapshot_by_itself(self, make_rig):
        rig = make_rig(child_first=True)
        rig.parent.config.enabled = False
        rig.manager = DatasetManager(rig.manager.config)
        rig.cycle()
        assert rig.world.commands_of('snapshot') == [['snapshot', f'{CH}@{NAME}']]
        assert rig.world.names_of(DS) == set()


class TestFrequencies:
    def test_a_child_with_a_higher_frequency_takes_its_own_name_on_top_of_the_ride(self, make_rig, caplog):
        kid = DatasetConfig(name=CH, frequency=timedelta(minutes=15))
        rig = make_rig(child_first=True, children=[kid])
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle()
        assert rig.world.names_of(DS) == {NAME}
        assert rig.manager.ridden_snapshot(rig.dsi(CH)) is None, 'its name for this period is another one'
        assert rig.world.names_of(CH) == {NAME, QUARTER}, 'the parent\'s, and its own'
        assert (rig.record_of(CH).ok, rig.record_of(CH).detail) == (True, QUARTER)
        assert records(caplog, WARN) == []

    def test_several_periods_with_different_frequencies(self, make_rig, frozen_clock, caplog):
        """Parent hourly, child every 15 minutes, a check every 15 minutes, the child listed first: the child is
        ok at every check where it is due, rides at the top of the hour and takes its own name otherwise."""
        kid = DatasetConfig(name=CH, frequency=timedelta(minutes=15))
        rig = make_rig(child_first=True, children=[kid])
        details = []
        with caplog.at_level(DEBUG, logger=LOGGER):
            for _ in range(4):                                     # 10:30 10:45 11:00 11:15
                rig.cycle()
                details.append({r.dataset: r for r in rig.recorded}.get(CH))
                next_cycle(frozen_clock, timedelta(minutes=15))
        assert all(d is not None and d.ok for d in details)
        assert [d.detail for d in details] == [
            'autosnap_20261022103000', 'autosnap_20261022104500', 'autosnap_20261022110000',
            'autosnap_20261022111500']
        assert rig.world.names_of(DS) == {NAME, 'autosnap_20261022110000'}
        assert rig.world.names_of(CH) == {NAME} | {d.detail for d in details}
        assert len(rig.world.commands_of('snapshot')) == 2 + 3, 'the parent twice (one `-r` each), the child three times'
        assert records(caplog, WARN) == [] and records(caplog, logging.ERROR) == []

    def test_a_parent_with_a_higher_frequency_does_not_make_the_child_ride_nor_fail(self, make_rig, caplog):
        """The parent's name is the quarter-hour one; the child's for the period is the hourly one: no ride. The
        child's newest snapshot is the parent's (age 0), so it is not due, and it is no failure."""
        rig = make_rig(parent_frequency=timedelta(minutes=15))
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle()
        assert rig.world.names_of(DS) == {QUARTER} and rig.world.names_of(CH) == {QUARTER}
        assert rig.manager.ridden_snapshot(rig.dsi(CH)) is None
        assert [r for r in rig.recorded if r.dataset == CH] == []
        assert records(caplog, WARN) == []


# ---------------------------------------------------------------------------
# What is not a ride
# ---------------------------------------------------------------------------


def _backwards_world(mock_subprocess, name=CH):
    """`name` whose generated name for this period (NAME) EXISTS but is not its newest snapshot by txg: the
    newest is dated 08:00, so it is due, and `zfs snapshot` would fail with "already exists" every cycle."""
    world = SnapWorld(mock_subprocess)
    world.add(DS).add(name)
    world.add_snap(name, NAME, txg=10)
    world.add_snap(name, 'autosnap_20261022080000', txg=20)
    return world


class TestAnExistingNameIsNotARide:
    def test_a_name_that_exists_with_no_record_warns_once_then_debug(self, mock_subprocess, frozen_clock, caplog):
        """The unchanged backwards-clock path of `needs_snapshot`."""
        world = _backwards_world(mock_subprocess)
        manager, dsi = make_manager(CH, recursive=False, retention=False)
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert [manager.needs_snapshot(dsi) for _ in range(4)] == [False] * 4
        (warning,) = records(caplog, WARN)
        assert NAME in warning and 'already exists' in warning and 'went backwards' in warning
        assert len(records(caplog, DEBUG, 'already exists; not creating it')) == 3
        assert records(caplog, DEBUG, 'counts as its own') == []
        assert world.commands_of('snapshot') == []

    def test_through_the_worker_nothing_is_created_and_nothing_recorded(self, make_rig, caplog):
        """The child's name exists and the child is due (clock went backwards); the non-recursive parent beside
        it is due too and simply takes its own snapshot, which is no record the child can ride."""
        rig = make_rig(parent_recursive=False)
        rig.world.add_snap(CH, NAME, txg=10)
        rig.world.add_snap(CH, 'autosnap_20261022080000', txg=20)
        rig.world.add_snap(DS, 'autosnap_20261022080000')
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle()
        assert rig.world.commands_of('snapshot') == [['snapshot', f'{DS}@{NAME}']], 'only the parent took one'
        assert [r for r in rig.recorded if r.dataset == CH] == []
        assert len(records(caplog, WARN, 'already exists')) == 1

    def test_a_record_of_this_name_over_the_dataset_makes_it_a_debug_not_a_warning(
            self, mock_subprocess, frozen_clock, caplog):
        """Defensive: `needs_snapshot` reached with the ancestor's record for this very name. Not a clock going
        backwards: DEBUG, not due, and nothing is remembered as warned."""
        _backwards_world(mock_subprocess)
        manager, dsi = make_manager(DS, recursive=True, retention=False, extra=[DatasetConfig(name=CH)])
        child = next(d for d in manager.datasets if d.name == CH)
        manager._tree_snapshots[DS] = (NAME, None)
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert manager.needs_snapshot(child) is False
        assert records(caplog, WARN) == []
        (note,) = records(caplog, DEBUG, 'counts as its own')
        assert DS in note and NAME in note
        assert manager._name_taken_noted == set()

    @pytest.mark.parametrize('record', [
        ('autosnap_20261022090000', None),
        (NAME, frozenset({'pool/other'})),
    ], ids=['another-name', 'cover-without-it'])
    def test_a_record_that_does_not_cover_it_keeps_the_warning(self, mock_subprocess, frozen_clock, caplog, record):
        _backwards_world(mock_subprocess)
        manager, dsi = make_manager(DS, recursive=True, retention=False, extra=[DatasetConfig(name=CH)])
        child = next(d for d in manager.datasets if d.name == CH)
        manager._tree_snapshots[DS] = record
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert manager.needs_snapshot(child) is False
        assert len(records(caplog, WARN, 'already exists')) == 1

    def test_the_record_of_a_non_ancestor_keeps_the_warning(self, mock_subprocess, frozen_clock, caplog):
        _backwards_world(mock_subprocess)
        manager, dsi = make_manager('pool/other', recursive=True, retention=False,
                                    extra=[DatasetConfig(name=CH)])
        child = next(d for d in manager.datasets if d.name == CH)
        manager._tree_snapshots['pool/other'] = (NAME, None)
        with caplog.at_level(DEBUG, logger=LOGGER):
            manager.needs_snapshot(child)
        assert len(records(caplog, WARN, 'already exists')) == 1


class TestTheRecordLifecycle:
    def test_a_parent_whose_own_name_exists_makes_no_record(self, make_rig, caplog):
        """A clock that went backwards on the PARENT: it is not due, so no command and no record; the child does
        not ride and (nothing exists for it) takes its own snapshot."""
        rig = make_rig(child_first=True)
        rig.world.add_snap(DS, NAME, txg=10)
        rig.world.add_snap(DS, 'autosnap_20261022080000', txg=20)
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle()
        assert rig.manager._tree_snapshots == {}
        assert [c for c in rig.world.commands_of('snapshot') if '-r' in c] == []
        assert rig.world.commands_of('snapshot') == [['snapshot', f'{CH}@{NAME}']], 'the child went its own way'
        assert [r.dataset for r in rig.recorded] == [CH]
        assert len(records(caplog, WARN, 'already exists')) == 1, 'the parent\'s, as before'

    def test_a_failed_snapshot_leaves_no_record_and_drops_the_old_one(self, make_rig, caplog):
        rig = make_rig(child_first=True)
        rig.manager._tree_snapshots[DS] = ('autosnap_20200101000000', None)     # an older, stale record
        rig.world.fail(lambda argv: argv[0] == 'snapshot' and '-r' in argv, 'cannot create snapshot: out of space')
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle()
        assert rig.manager._tree_snapshots == {}
        assert rig.record_of(DS).ok is False and 'out of space' in rig.record_of(DS).error
        assert rig.manager.ridden_snapshot(rig.dsi(CH)) is None
        assert rig.world.commands_of('snapshot')[-1] == ['snapshot', f'{CH}@{NAME}'], 'the child went its own way'
        assert rig.record_of(CH).ok is True

    def test_a_collision_is_retried_and_the_record_covers_the_rest_but_not_the_holder(self, make_rig):
        """A2e review follow-up: the atomic command collides with the child that holds `@N`, the retry without
        it succeeds, and the record's cover is the SOURCES minus the holder (never the holder)."""
        rig = make_rig(copy=True)
        rig.world.add_snap(CH, NAME)
        rig.cycle()
        assert rig.manager._tree_snapshots == {DS: (NAME, frozenset({DS, GC, VOL}))}
        assert rig.record_of(DS).ok is True
        assert [(d, short) for d, short, _ in rig.world.created] == [(DS, NAME), (GC, NAME), (VOL, NAME)]
        assert rig.manager._covering_snapshot(rig.dsi(CH), NAME) is None, 'the holder is not ridden'

    def test_a_failed_retry_leaves_no_record_either(self, make_rig):
        rig = make_rig(copy=True)
        rig.world.add_snap(CH, NAME)                                # a collision: the atomic command fails whole
        rig.world.fail(RETRY_ONLY, 'out of space')
        rig.cycle()
        assert rig.manager._tree_snapshots == {}
        assert rig.world.created == [] and rig.record_of(DS).ok is False
        assert 'retried once' in rig.record_of(DS).error

    def test_a_dry_run_leaves_no_record(self, make_rig):
        rig = make_rig(child_first=True, dry_run=True)
        rig.cycle()
        assert rig.manager._tree_snapshots == {}
        assert rig.world.commands_of('snapshot') == [] and rig.world.created == []

    def test_a_refusal_inside_a_receive_target_leaves_no_record(self, make_rig):
        rig = make_rig(child_first=True, targets=[DS])
        rig.manager._tree_snapshots[DS] = (NAME, None)
        rig.cycle()
        assert rig.manager._tree_snapshots == {}
        assert rig.world.commands_of('snapshot') == [['snapshot', f'{CH}@{NAME}']], 'only the child, on its own'

    def test_a_dry_run_drops_a_stale_record(self, make_rig):
        rig = make_rig(dry_run=True)
        rig.manager._tree_snapshots[DS] = (NAME, None)
        assert rig.manager.create_snapshot(rig.parent) is None
        assert rig.manager._tree_snapshots == {}

    def test_a_failure_after_the_command_succeeded_leaves_no_record(self, make_rig, mocker):
        """Whatever raises once the recursive command has run (here building the returned snapshot) drops the
        record it just made: the cycle counts the snapshot as failed."""
        rig = make_rig()
        mocker.patch('zfsbackup.backup_manager.ZFSSnapshot', side_effect=RuntimeError('boom'))
        assert rig.manager.create_snapshot(rig.parent) is None
        assert rig.world.names_of(DS) == {NAME}, 'the command did run'
        assert rig.manager._tree_snapshots == {}
        assert rig.manager.ridden_snapshot(rig.dsi(CH)) is None

    def test_a_fresh_create_snapshot_replaces_the_record_of_the_last_cycle(self, make_rig, frozen_clock):
        rig = make_rig()
        rig.cycle()
        assert rig.manager._tree_snapshots[DS][0] == NAME
        next_cycle(frozen_clock)
        assert rig.manager.create_snapshot(rig.parent) is not None
        assert rig.manager._tree_snapshots[DS][0] == at(frozen_clock)

    def test_begin_snapshot_cycle_clears_the_records(self, make_rig):
        rig = make_rig()
        rig.cycle()
        assert rig.manager._tree_snapshots
        rig.manager.begin_snapshot_cycle()
        assert rig.manager._tree_snapshots == {}
        assert rig.manager.ridden_snapshot(rig.dsi(CH)) is None

    def test_an_old_record_never_rides_in_a_later_cycle(self, make_rig, frozen_clock):
        """Same period, next check: the parent is not due, nothing was made in THIS cycle, so the child records
        nothing (it would claim a ride on a snapshot that no command of this cycle took)."""
        rig = make_rig(child_first=True)
        rig.cycle()
        assert len(rig.recorded) == 2
        next_cycle(frozen_clock, timedelta(minutes=10))
        rig.cycle()
        assert rig.recorded == [] and rig.manager._tree_snapshots == {}


class TestRideNeedsTheSnapshotToExist:
    def test_a_covered_child_that_does_not_exist_does_not_ride(self, make_rig, caplog):
        rig = make_rig(child_first=True)
        rig.world.drop(CH)                                          # configured, but there is no such dataset
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle()
        assert rig.manager._tree_snapshots == {DS: (NAME, None)}
        assert rig.manager.ridden_snapshot(rig.dsi(CH)) is None
        assert rig.manager.last_list_error is not None and 'does not exist' in rig.manager.last_list_error
        child = rig.record_of(CH)
        assert child.ok is False, 'its normal path: the failure is its own'

    def test_a_failed_relist_does_not_ride(self, make_rig):
        rig = make_rig()
        rig.manager.begin_snapshot_cycle()
        assert rig.manager.create_snapshot(rig.parent) is not None
        rig.world.fail(lambda argv: argv[:2] == ['list', '-H'] and argv[-1] == CH and 'snapshot' in argv,
                       'cannot open: pool I/O is currently suspended')
        assert rig.manager.ridden_snapshot(rig.dsi(CH)) is None
        assert rig.manager.last_list_error is not None

    def test_a_listing_without_the_name_does_not_ride(self, make_rig):
        """Covered by the record, but the dataset does not hold the name (created after the ancestor's listing)."""
        rig = make_rig()
        rig.cycle()
        del rig.world.snaps[CH][NAME]
        assert rig.manager.ridden_snapshot(rig.dsi(CH)) is None

    def test_one_listing_of_the_child_and_nothing_else(self, make_rig):
        rig = make_rig()
        rig.manager.begin_snapshot_cycle()
        rig.manager.create_snapshot(rig.parent)
        before = len(rig.world.argvs)
        assert rig.manager.ridden_snapshot(rig.dsi(CH)) is not None
        (argv,) = rig.world.argvs[before:]
        assert argv[0] == 'list' and 'snapshot' in argv and argv[-1] == CH

    def test_no_record_no_listing(self, make_rig):
        rig = make_rig()
        before = len(rig.world.argvs)
        assert rig.manager.ridden_snapshot(rig.dsi(CH)) is None
        assert len(rig.world.argvs) == before, 'nothing was made this cycle: not even a read'


class TestTheChildKeepsItsOwnRetention:
    def test_the_childs_pruning_is_its_own_after_a_ride(self, make_rig, frozen_clock):
        kid = DatasetConfig(name=CH, retention_rules=[RetentionRule(timedelta(hours=1), timedelta(hours=2))])
        rig = make_rig(child_first=True, children=[kid])
        old = ['autosnap_20261022050000', 'autosnap_20261022060000']
        for short in old:
            rig.world.add_snap(CH, short)
        rig.cycle()
        assert rig.world.names_of(CH) == {*old, NAME}, 'it rode: nothing of its own was taken'
        rig.manager.prune_snapshots(rig.dsi(CH))
        assert {d for d in rig.world.destroyed if d[0] == CH} == {(CH, short) for short in old}
        assert rig.world.names_of(CH) == {NAME}, 'the ridden snapshot is the newest: always kept'
        assert rig.world.names_of(DS) == {NAME} and [d for d in rig.world.destroyed if d[0] == DS] == []


# ---------------------------------------------------------------------------
# A2e review follow-up: a collision on the tree command is retried once without the holders
# ---------------------------------------------------------------------------
#
# A period boundary can fall between the parent's turn and a configured child's in ONE cycle (the child then takes
# `@N` alone), and one failed parent command lets the child snapshot itself the same way. The parent's `-r` (or
# atomic `many`) then collides. `_run_tree_command` answers ANY failure of the command with one non-strict
# `zfs list -H -r -t snapshot -o name,type <ds>`, filtered to the short name `N`: the source datasets that hold
# it are left out of ONE atomic `many` retry over the rest. Everything else re-raises the original error.

from datetime import datetime, timezone                          # noqa: E402

N_NEXT = 'autosnap_20261022110000'                              # the hour after NAME
ALREADY = 'already exists'


def RETRY_ONLY(argv):
    """The retry of a collision: an atomic `snapshot` that does not name the holder `CH` (the first command is
    `-r`, or names it)."""
    return argv[0] == 'snapshot' and '-r' not in argv and f'{CH}@{NAME}' not in argv

HOLDERS_INFO = 'already held'


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


def seed(world, name=NAME, datasets=(DS, CH, GC, VOL)):
    for dataset in datasets:
        world.add_snap(dataset, name)


def info(caplog, containing=HOLDERS_INFO):
    return records(caplog, logging.INFO, containing)


def error_lines(caplog):
    return records(caplog, logging.ERROR)


class TestTheBoundaryRetry:
    @pytest.fixture(params=['plain-r', 'atomic-many'])
    def rig(self, request, make_rig):
        """The same tree with and without a backup copy below it: `snapshot -r` vs the A2c atomic list."""
        return make_rig(copy=request.param == 'atomic-many')

    @staticmethod
    def first_command(rig, name):
        return (['snapshot', '-r', f'{DS}@{name}'] if not rig.world.datasets.get(COPY)
                else ['snapshot', f'{DS}@{name}', f'{CH}@{name}', f'{GC}@{name}', f'{VOL}@{name}'])

    # -- 1: the clock crossing the boundary INSIDE one cycle ----------------------------------

    def test_the_boundary_between_the_parents_and_the_childs_turn_in_one_cycle(
            self, rig, frozen_clock, caplog):
        seed(rig.world)
        frozen_clock.set(utc(2026, 10, 22, 10, 59, 59, 800000))
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle(after={DS: lambda: frozen_clock.set(utc(2026, 10, 22, 11, 0, 0, 300000))})
        assert [r.dataset for r in rig.recorded] == [CH], 'the parent was not due; the child was'
        assert rig.world.snapshot_argvs() == [['snapshot', f'{CH}@{N_NEXT}']], 'the child took @1100 alone'
        assert rig.world.names_of(DS) == {NAME} and rig.world.names_of(GC) == {NAME}

        frozen_clock.set(utc(2026, 10, 22, 11, 0, 10))
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle()
        attempts = rig.world.snapshot_argvs()[1:]
        assert attempts == [
            self.first_command(rig, N_NEXT),
            ['snapshot', f'{DS}@{N_NEXT}', f'{GC}@{N_NEXT}', f'{VOL}@{N_NEXT}'],
        ], 'the tree command, then ONE `many` over the tree without the child'
        assert [r.dataset for r in rig.recorded] == [DS], 'the child takes no second snapshot, and is not recorded'
        assert rig.record_of(DS).ok is True and rig.record_of(DS).detail.split(';')[0] == N_NEXT
        assert len(info(caplog)) == 1 and rig.world.holder_listings() == [
            ['list', '-H', '-r', '-t', 'snapshot', '-o', 'name,type', DS]]
        assert error_lines(caplog) == [] if not rig.world.datasets.get(COPY) else len(error_lines(caplog)) == 1
        for dataset in (DS, CH, GC, VOL):
            assert N_NEXT in rig.world.names_of(dataset), dataset
        assert rig.world.names_of(CH) == {NAME, N_NEXT}
        assert [c[:2] for c in rig.world.created if c[1] == N_NEXT and c[0] == CH] == [(CH, N_NEXT)]
        assert len({txg for d, s, txg in rig.world.created if s == N_NEXT and d != CH}) == 1, 'one atomic retry'
        assert rig.world.snapshots_below(COPY) == {}

    def test_a_third_cycle_in_the_period_does_nothing(self, rig, frozen_clock):
        seed(rig.world)
        frozen_clock.set(utc(2026, 10, 22, 10, 59, 59, 800000))
        rig.cycle(after={DS: lambda: frozen_clock.set(utc(2026, 10, 22, 11, 0, 0, 300000))})
        frozen_clock.set(utc(2026, 10, 22, 11, 0, 10))
        rig.cycle()
        before = len(rig.world.snapshot_argvs())
        frozen_clock.set(utc(2026, 10, 22, 11, 5, 0))
        rig.cycle()
        assert rig.recorded == [] and len(rig.world.snapshot_argvs()) == before

    # -- 2: a source child destroyed between the listing and `many` --------------------------

    def test_a_source_child_destroyed_between_the_listing_and_many_is_retried_by_the_next_cycle(
            self, make_rig, frozen_clock, caplog):
        rig = make_rig(copy=True)
        rig.world.fail_once(lambda argv: argv[0] == 'snapshot' and f'{GC}@{NAME}' in argv,
                            f"cannot open '{GC}': dataset does not exist")
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle()
        parent = rig.record_of(DS)
        assert parent.ok is False and 'does not exist' in parent.error and 'retried' not in parent.error
        assert rig.world.holder_listings() == [['list', '-H', '-r', '-t', 'snapshot', '-o', 'name,type', DS]]
        assert len(rig.world.snapshot_argvs()) == 2, 'the failed `many`, then the child\'s own: no retry'
        assert rig.manager._tree_snapshots == {}
        assert rig.world.names_of(CH) == {NAME} and rig.world.names_of(DS) == set(), 'the child took N itself'
        assert (rig.record_of(CH).ok, rig.record_of(CH).detail) == (True, NAME)

        frozen_clock.advance(timedelta(minutes=10))
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle()
        assert rig.record_of(DS).ok is True, 'the next cycle retried and succeeded'
        assert len(info(caplog)) == 1
        for dataset in (DS, CH, GC, VOL):
            assert NAME in rig.world.names_of(dataset), f'{dataset} missed the period'
        assert rig.world.snapshots_below(COPY) == {}

    # -- 3: the retry also fails ----------------------------------------------------------------

    def test_a_failed_retry_is_exactly_two_commands_and_a_runtime_error_naming_both(self, rig, caplog):
        rig.world.add_snap(CH, NAME)
        rig.world.fail(RETRY_ONLY, 'cannot create: out of space')
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert rig.manager.create_snapshot(rig.parent) is None
        assert len(rig.world.snapshot_argvs()) == 2, 'the tree command and ONE retry: no third'
        error = rig.manager.last_snapshot_error
        assert 'retried once' in error and 'out of space' in error and ALREADY in error
        assert rig.world.created == [] and rig.manager._tree_snapshots == {}
        assert info(caplog) == [], 'the INFO is for a retry that worked'
        assert len(records(caplog, logging.ERROR, 'Failed to create snapshot')) == 1

    def test_a_value_error_of_the_retry_is_a_failure_not_a_second_fallback(self, make_rig, mocker, caplog):
        """The retry's own `ValueError` (argv bound, names) must not escape: the caller would take it for a
        `many` refusal and fall back to `snapshot -r`, which collides again and hides the cause."""
        import zfsbackup.backup_manager as bm
        rig = make_rig(copy=True)
        rig.world.add_snap(CH, NAME)
        real = bm.zfs.snapshot.many
        calls = []

        def many(sources, name):
            calls.append([d.name for d in sources])
            if len(calls) == 2:
                raise ValueError('argv too long')
            return real(sources, name)
        mocker.patch.object(bm.zfs.snapshot, 'many', side_effect=many)
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert rig.manager.create_snapshot(rig.parent) is None
        assert len(calls) == 2 and CH not in calls[1]
        assert 'retried once' in rig.manager.last_snapshot_error and 'argv too long' in rig.manager.last_snapshot_error
        assert all(a[1] != '-r' for a in rig.world.snapshot_argvs())
        assert records(caplog, logging.ERROR, 'Cannot keep the backup copies out') == []

    # -- 4: the holder listing fails -------------------------------------------------------------

    def test_an_unreadable_holder_listing_surfaces_the_original_error_with_no_retry(self, rig, caplog):
        rig.world.add_snap(CH, NAME)
        rig.world.fail(lambda a: a[0] == 'list' and '-r' in a and 'snapshot' in a, 'cannot open: I/O error')
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert rig.manager.create_snapshot(rig.parent) is None
        assert len(rig.world.snapshot_argvs()) == 1, 'no retry'
        assert ALREADY in rig.manager.last_snapshot_error and 'retried' not in rig.manager.last_snapshot_error
        assert 'I/O error' not in rig.manager.last_snapshot_error, 'the original error, not the listing\'s'
        (note,) = records(caplog, DEBUG, 'could not look for holders')
        assert 'I/O error' in note and NAME in note
        assert rig.world.created == [] and info(caplog) == []

    # -- 5: a failure with no holder ----------------------------------------------------------

    def test_a_failure_with_no_holder_is_unchanged_and_costs_one_listing(self, rig, caplog):
        rig.world.fail(lambda argv: argv[0] == 'snapshot', 'cannot create snapshot: out of space')
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert rig.manager.create_snapshot(rig.parent) is None
        assert len(rig.world.snapshot_argvs()) == 1, 'no extra snapshot command'
        assert len(rig.world.holder_listings()) == 1
        assert rig.world.listings(recursive=True) == [] or rig.world.datasets.get(COPY), \
            'no holder: the source datasets are not read either (a copy-less `-r` made no such read)'
        assert 'out of space' in rig.manager.last_snapshot_error and 'retried' not in rig.manager.last_snapshot_error
        assert records(caplog, DEBUG, 'could not look for holders') == [], 'no holder is not an unreadable listing'
        assert rig.world.created == [] and rig.manager._tree_snapshots == {}

    def test_the_holder_listing_is_not_strict(self, rig, mocker):
        """A strict listing would raise for a row it cannot represent and lose the retry for the whole tree; the
        default one skips the row (a holder it misses just makes the retry fail like the first command)."""
        import zfsbackup.backup_manager as bm
        rig.world.add_snap(CH, NAME)
        spy = mocker.spy(bm.zfs, 'list')
        rig.manager.create_snapshot(rig.parent)
        (call,) = [c for c in spy.call_args_list if c.kwargs.get('types') == 'snapshot' and c.kwargs.get('recursive')]
        assert call.kwargs.get('strict', False) is False
        assert call.kwargs == {'roots': rig.parent.dataset, 'types': 'snapshot', 'recursive': True}

    # -- 6: dsi itself holds N -------------------------------------------------------------------

    def test_when_the_dataset_itself_holds_the_name_the_original_error_stands(self, rig, caplog):
        rig.world.add_snap(DS, NAME)
        rig.world.add_snap(CH, NAME)
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert rig.manager.create_snapshot(rig.parent) is None
        assert len(rig.world.snapshot_argvs()) == 1, 'no retry: a backwards clock is not this code\'s'
        assert ALREADY in rig.manager.last_snapshot_error and 'retried' not in rig.manager.last_snapshot_error
        assert rig.world.created == [] and info(caplog) == []

    # -- a holder that is not a source -------------------------------------------------------

    def test_a_holder_outside_the_sources_is_no_holder(self, make_rig, caplog):
        """The copy holds `@N` (a stray) and the command fails for another reason: the copy is not a source, so
        nothing is excluded and the original error stands."""
        rig = make_rig(copy=True)
        rig.world.add_snap(COPY, NAME)
        rig.world.fail(lambda argv: argv[0] == 'snapshot', 'cannot create snapshot: out of space')
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert rig.manager.create_snapshot(rig.parent) is None
        assert len(rig.world.snapshot_argvs()) == 1
        assert 'out of space' in rig.manager.last_snapshot_error and 'retried' not in rig.manager.last_snapshot_error

    def test_a_holder_in_the_copy_is_never_a_reason_to_exclude_a_source(self, make_rig):
        rig = make_rig(copy=True)
        rig.world.add_snap(CH, NAME)
        rig.world.add_snap(COPY, NAME)                              # (a stray in the copy too)
        rig.manager.create_snapshot(rig.parent)
        assert rig.world.snapshot_argvs()[-1] == ['snapshot', f'{DS}@{NAME}', f'{GC}@{NAME}', f'{VOL}@{NAME}']
        assert rig.world.names_of(COPY) == {NAME}, 'untouched: it held the stray before, nothing was added'
        assert [c[0] for c in rig.world.created] == [DS, GC, VOL]

    # -- 7: plain -r takes the retry through `_snapshot_sources` ------------------------------

    def test_plain_r_with_no_copy_reads_the_sources_for_the_retry(self, make_rig):
        rig = make_rig()
        rig.world.add_snap(CH, NAME)
        rig.manager.create_snapshot(rig.parent)
        assert rig.world.snapshot_argvs() == [
            ['snapshot', '-r', f'{DS}@{NAME}'],
            ['snapshot', f'{DS}@{NAME}', f'{GC}@{NAME}', f'{VOL}@{NAME}'],
        ]
        # `_snapshot_sources(dsi, {})`: ONE recursive name-only dataset listing; a copy-less `-r` made none
        assert len(rig.world.listings(recursive=True)) == 1, 'only the retry reads the source datasets'
        argvs = rig.world.argvs
        assert (argvs.index(rig.world.holder_listings()[0]) + 1
                == argvs.index(rig.world.listings(recursive=True)[0])), 'the holders first, then the sources'
        assert rig.manager._tree_snapshots == {DS: (NAME, frozenset({DS, GC, VOL}))}

    # -- 8: more than five holders -------------------------------------------------------------

    def test_more_than_five_holders_are_cut_with_a_count(self, make_rig, caplog):
        rig = make_rig(copy=True)
        kids = [f'{DS}/k{i}' for i in range(7)]
        for kid in kids:
            rig.world.add(kid)
            rig.world.add_snap(kid, NAME)
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert rig.manager.create_snapshot(rig.parent) is not None
        (line,) = info(caplog)
        assert '7 dataset(s)' in line and '+2' in line
        assert ', '.join(sorted(kids)[:5]) in line and sorted(kids)[5] not in line
        names = rig.manager._tree_snapshots[DS][1]
        assert names == frozenset({DS, CH, GC, VOL}), 'no holder is covered'

    def test_exactly_five_holders_have_no_count(self, make_rig, caplog):
        rig = make_rig(copy=True)
        kids = [f'{DS}/k{i}' for i in range(5)]
        for kid in kids:
            rig.world.add(kid)
            rig.world.add_snap(kid, NAME)
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.manager.create_snapshot(rig.parent)
        (line,) = info(caplog)
        assert ', '.join(kids) in line and ' +' not in line

    # -- 9: INFO once per episode, then DEBUG -------------------------------------------------

    def test_the_info_is_once_per_dataset_and_name_then_debug(self, make_rig, frozen_clock, caplog):
        rig = make_rig()
        rig.world.add_snap(CH, NAME)
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert rig.manager.create_snapshot(rig.parent) is not None
            assert len(info(caplog)) == 1
            for dataset in (DS, GC, VOL):                              # the same period again: the tree lost its
                del rig.world.snaps[dataset][NAME]                    # snapshots (destroyed), the holder kept its
            assert rig.manager.create_snapshot(rig.parent) is not None
        assert len(info(caplog)) == 1, 'the same (dataset, name): not announced twice'
        (debug,) = records(caplog, DEBUG, 'left out the holders')
        assert CH in debug and NAME in debug
        # a new period is a new episode, and the memory keeps only this dataset's newest key
        frozen_clock.advance(timedelta(hours=1))
        rig.world.add_snap(CH, at(frozen_clock))
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert rig.manager.create_snapshot(rig.parent) is not None
        assert len(info(caplog)) == 2
        assert rig.manager._snapshot_holders_noted == {(DS, at(frozen_clock))}

    def test_each_recursive_dataset_has_its_own_episode(self, make_rig, caplog):
        other = 'pool/other'
        rig = make_rig(children=[DatasetConfig(name=other, recursive=True)])
        rig.world.add(other).add(other + '/x')
        rig.world.add_snap(CH, NAME)
        rig.world.add_snap(other + '/x', NAME)
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert rig.manager.create_snapshot(rig.parent) is not None
            assert rig.manager.create_snapshot(rig.dsi(other)) is not None
            assert len(info(caplog)) == 2, 'two datasets, two episodes'
            for dataset in (DS, GC, VOL):
                del rig.world.snaps[dataset][NAME]
            assert rig.manager.create_snapshot(rig.parent) is not None
        assert len(info(caplog)) == 2, 'the other dataset\'s episode did not reset this one'
        assert rig.manager._snapshot_holders_noted == {(DS, NAME), (other, NAME)}

    # -- 10: nested recursive configurations --------------------------------------------------

    def test_nested_recursive_configs_across_a_day_boundary(self, make_rig, frozen_clock, caplog):
        """P daily, P/c hourly and recursive. The day boundary falls between their turns: P/c takes the
        midnight name for its tree alone, then P's daily `-r` collides and is retried without P/c's tree."""
        kid = DatasetConfig(name=CH, recursive=True)
        rig = make_rig(children=[kid], parent_frequency=timedelta(days=1))
        day = 'autosnap_20261022000000'
        midnight = 'autosnap_20261023000000'
        seed(rig.world, day, (DS, VOL))
        seed(rig.world, 'autosnap_20261022230000', (CH, GC))
        frozen_clock.set(utc(2026, 10, 22, 23, 59, 59, 800000))
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle(after={DS: lambda: frozen_clock.set(utc(2026, 10, 23, 0, 0, 0, 300000))})
        assert rig.world.snapshot_argvs() == [['snapshot', '-r', f'{CH}@{midnight}']], 'the child took it, its tree too'
        assert rig.world.names_of(GC) >= {midnight} and midnight not in rig.world.names_of(DS)

        frozen_clock.set(utc(2026, 10, 23, 0, 0, 10))
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.cycle()
        assert rig.record_of(DS).ok is True and rig.record_of(DS).detail == midnight
        assert rig.world.snapshot_argvs()[-1] == ['snapshot', f'{DS}@{midnight}', f'{VOL}@{midnight}'], \
            'the holders are the child AND its grandchild'
        for dataset in (DS, CH, GC, VOL):
            assert midnight in rig.world.names_of(dataset), dataset
        assert len(info(caplog)) == 1 and 'pool/data/c, pool/data/c/g' in info(caplog)[0]
        assert [r.dataset for r in rig.recorded] == [DS], 'the child is not due again'

    # -- 11: the `-r` fallback with an unreadable source list ---------------------------------

    def test_the_fallback_with_an_unreadable_source_list_keeps_todays_behaviour(self, make_rig, caplog):
        rig = make_rig(copy=True)
        rig.world.add_snap(CH, NAME)
        rig.world.fail(lambda argv: argv[0] == 'get' and '-r' in argv, 'cannot read: I/O error')
        rig.world.fail(lambda argv: argv[0] == 'list' and 'filesystem,volume' in argv, 'cannot list: I/O error')
        with caplog.at_level(DEBUG, logger=LOGGER):
            assert rig.manager.create_snapshot(rig.parent) is None
        assert rig.world.snapshot_argvs() == [['snapshot', '-r', f'{DS}@{NAME}']], 'no retry without a source list'
        assert ALREADY in rig.manager.last_snapshot_error and 'retried' not in rig.manager.last_snapshot_error
        (note,) = records(caplog, DEBUG, 'could not look for holders')
        assert 'cannot list the datasets' in note
        assert rig.world.created == [] and rig.manager._tree_snapshots == {}
        assert len(records(caplog, logging.ERROR, 'Cannot keep the backup copies out')) == 1

    # -- 12: Finding 8 -------------------------------------------------------------------------

    def test_a_failing_relist_in_ridden_snapshot_and_needs_snapshot_is_exactly_one_error(
            self, make_rig, caplog):
        rig = make_rig()
        rig.manager.begin_snapshot_cycle()
        assert rig.manager.create_snapshot(rig.parent) is not None
        rig.world.fail(lambda a: a[:2] == ['list', '-H'] and a[-1] == CH and 'snapshot' in a,
                       'cannot open: pool I/O is currently suspended')
        caplog.clear()
        with caplog.at_level(DEBUG, logger=LOGGER):
            rig.worker._process_dataset(rig.manager, rig.dsi(CH))
        # ONE ERROR for the failed listing (it used to be two: `ridden_snapshot`'s and `needs_snapshot`'s). The
        # second ERROR is the creation that follows `needs_snapshot`'s "no snapshots" answer (the fake still holds
        # the name), which is a different failure.
        assert len(records(caplog, logging.ERROR, 'Failed to list snapshots')) == 1
        assert len(records(caplog, logging.ERROR)) == 2
        (debug,) = records(caplog, DEBUG, 'could not confirm the ridden snapshot')
        assert NAME in debug and 'suspended' in debug
        assert len([a for a in rig.world.argvs if a[-1] == CH and a[0] == 'list']) == 2, 'both listings were made'
        assert rig.manager.last_list_error is not None

    def test_ridden_snapshot_alone_sets_the_list_failure_for_its_caller(self, make_rig):
        rig = make_rig()
        rig.manager.begin_snapshot_cycle()
        rig.manager.create_snapshot(rig.parent)
        rig.world.fail(lambda a: a[:2] == ['list', '-H'] and a[-1] == CH and 'snapshot' in a, 'cannot open: suspended')
        assert rig.manager.ridden_snapshot(rig.dsi(CH)) is None
        assert 'suspended' in rig.manager.last_list_error and rig.manager._last_list_failure is not None

    # -- 13: the race every period -------------------------------------------------------------

    def test_the_boundary_race_every_period_never_loses_a_period_for_any_dataset(
            self, make_rig, frozen_clock, caplog):
        """The child (every 15 minutes) is more frequent than the parent (hourly) and takes the top-of-the-hour
        name alone EVERY hour: the parent's `-r` collides every period and is retried, so every source dataset
        ends with every period's snapshot."""
        kid = DatasetConfig(name=CH, frequency=timedelta(minutes=15))
        rig = make_rig(children=[kid])
        seed(rig.world)
        expected = [NAME]
        with caplog.at_level(DEBUG, logger=LOGGER):
            for hour in range(11, 15):
                frozen_clock.set(utc(2026, 10, 22, hour - 1, 59, 59, 800000))
                rig.cycle(after={DS: lambda h=hour: frozen_clock.set(utc(2026, 10, 22, h, 0, 0, 300000))})
                assert [r.dataset for r in rig.recorded] == [CH], hour
                frozen_clock.set(utc(2026, 10, 22, hour, 0, 10))
                rig.cycle()
                assert [(r.dataset, r.ok) for r in rig.recorded] == [(DS, True)], hour
                expected.append(f'autosnap_202610221{hour - 10}0000')
        for dataset in (DS, CH, GC, VOL):
            assert rig.world.names_of(dataset) == set(expected), dataset
        assert len(info(caplog)) == 4, 'one episode per period'
        assert error_lines(caplog) == [] and records(caplog, WARN) == []

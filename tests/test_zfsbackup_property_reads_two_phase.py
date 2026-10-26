"""The two-phase `org.zfsbackup:*` property read (`backup_manager._read_own_properties`).

Phase 1 is `zfs get -H -o name,property,source -s local,received -t filesystem,volume [-r] all <ds>`: no value
column, so nothing an owner writes can disturb it. Phase 2 is the strict explicit-list `zfs.get.rows` of exactly the
names phase 1 found (plus, for the own read made by `get_anchors`, the anchor and in-flight names of every configured
destination), `received=False`, no name-only `zfs list`. The two must agree about which (object, property) pairs have a
local or received value, or the pair is read once more, and a second disagreement raises.

Only `subprocess.Popen` is faked (`ZfsWorld`, `PrunePool`): the real libzfseasy parsers run over bytes shaped like
the real commands' output, a value is printed raw (so a newline in it is several physical lines).

Mutation counts (scratch copy of backup_manager.py; tests failing across this file, test_zfsbackup_property_reads_forged,
test_zfsbackup_manager, test_zfsbackup_prune_descendants and test_zfsbackup_prune_forged_bytes):
  * mismatch rule removed (`seen - allowed != expected` never true)          11 (all here)
  * the re-read removed (first mismatch raises)                              11 (all here)
  * only disappearances checked / only appearances checked                    2 / 9 (all here)
  * `also` additions accepted for any name                                    2 (here)
  * `strict=True` dropped from phase 2                                       16 (11 forged, 2 here, 2 descendants, 1 manager)
  * `prefix=` dropped from the discovery                                     57
  * `sources=` of the discovery widened to all                               38
  * the in-scope filter of the discovery removed                             13
  * the recursive nothing-found shortcut removed                              9
  * the own `type` check removed                                              3
  * `get_anchors` not passing `also`                                         10 (1 here, 9 manager)
  (`or ['type']` -> `or []` is an equivalent mutant: `rows()` sends `name,type` either way.)
"""

import logging
import os
import stat

import pytest

from _get_support import ZfsWorld
from _prune_support import PREFIX, hourly_name, prune_manager, prune_pool, records  # noqa: F401 (fixture)
from libzfseasy.types import Filesystem
from libzfseasy.zfs import ZFSOutputError
from zfsbackup.backup_manager import (
    PROP_ANCHOR_PREFIX as A, PROP_CONFIG, PROP_INFLIGHT_PREFIX as IF,
    read_descendant_zfsbackup_properties, read_zfsbackup_properties,
)

pytestmark = pytest.mark.unit

POOL, DS = 'pool', 'pool/data'
CH0, CH1, CH2 = 'pool/data/c0', 'pool/data/c1', 'pool/data/c2'
SNAP = 'autosnap_20240101120000'
SNAP2 = 'autosnap_20240102120000'
NEWLINE = 'line one\nline two'

DISCOVERY_COLUMNS = 'name,property,source'
VALUE_COLUMNS = 'name,property,value,source'


def columns(argv):
    return argv[argv.index('-o') + 1]


@pytest.fixture
def world(mock_subprocess):
    return ZfsWorld(mock_subprocess)


def phase_two(world):
    return [a for a in world.argvs if columns(a) == VALUE_COLUMNS]


def phase_one(world):
    return [a for a in world.argvs if columns(a) == DISCOVERY_COLUMNS]


class TestUnrelatedPropertiesDoNotBlock:
    """A value that cannot be framed only matters if it is the value of one of OUR properties."""

    @pytest.mark.parametrize('source', ['local', 'received'])
    def test_an_unrelated_newline_user_property_on_the_dataset(self, world, source):
        world.add(DS, [('user:note', NEWLINE, source), (A + 'offsite', SNAP, 'local'), (PROP_CONFIG, 'cfg', 'local')])
        assert read_zfsbackup_properties(Filesystem(DS)) == {A + 'offsite': SNAP, PROP_CONFIG: 'cfg'}
        assert len(world.argvs) == 2

    def test_a_forging_unrelated_property_with_a_whole_fake_group(self, world):
        value = f'x\nlocal\n{DS}\ttype\tfilesystem\t-\n{DS}\t{A}offsite\tFORGED\tlocal\n{DS}\tz\tz'
        world.add(DS, [('user:note', value, 'local'), (A + 'offsite', SNAP, 'local')])
        assert read_zfsbackup_properties(Filesystem(DS)) == {A + 'offsite': SNAP}

    @pytest.mark.parametrize('name', ['-x:y', 'a.b-c_d:e', 'user:with.dot', 'UPPER:case'])
    def test_an_unrelated_property_with_an_odd_but_well_shaped_name(self, world, name):
        world.add(DS, [(name, NEWLINE, 'local'), (A + 'offsite', SNAP, 'local')])
        assert read_zfsbackup_properties(Filesystem(DS)) == {A + 'offsite': SNAP}

    def test_an_unrelated_newline_property_on_a_descendant_and_on_the_dataset_itself(self, world):
        world.add(DS, [('user:note', NEWLINE, 'local'), (A + 'p', SNAP, 'local')])
        world.add(CH0, [('user:note', NEWLINE, 'local'), (A + 'c', SNAP2, 'local')])
        world.add(CH1, [('user:note', NEWLINE, 'received')])
        assert read_descendant_zfsbackup_properties(Filesystem(DS)) == {CH0: {A + 'c': SNAP2}}
        assert read_zfsbackup_properties(Filesystem(DS)) == {A + 'p': SNAP}

    def test_a_newline_value_inherited_from_the_pool_root_does_not_block_the_own_read(self, world):
        world.add(POOL, [('user:note', NEWLINE, 'local'), (A + 'root', NEWLINE, 'local')])
        world.add(DS, [('user:note', NEWLINE, f'inherited from {POOL}'), (A + 'root', NEWLINE, f'inherited from {POOL}'),
                       (A + 'offsite', SNAP, 'local')])
        assert read_zfsbackup_properties(Filesystem(DS)) == {A + 'offsite': SNAP}

    def test_a_newline_value_inherited_from_the_pool_root_does_not_block_the_recursive_read(self, world):
        world.add(POOL, [(A + 'root', NEWLINE, 'local')])
        world.add(DS, [(A + 'root', NEWLINE, f'inherited from {POOL}')])
        world.add(CH0, [(A + 'root', NEWLINE, f'inherited from {POOL}'), (A + 'c', SNAP, 'local')])
        assert read_descendant_zfsbackup_properties(Filesystem(DS)) == {CH0: {A + 'c': SNAP}}

    def test_the_documented_residual_a_descendant_setting_the_inherited_name_itself_does_raise(self, world):
        """Phase 2 asks every object for the names any descendant set, so `c1`'s inherited bad value of a name
        `c0` sets locally is in c1's group."""
        world.add(POOL, [(A + 'shared', NEWLINE, 'local')])
        world.add(DS, [(A + 'shared', NEWLINE, f'inherited from {POOL}')])
        world.add(CH0, [(A + 'shared', 'fine', 'local')])
        world.add(CH1, [(A + 'shared', NEWLINE, f'inherited from {POOL}')])
        with pytest.raises(ZFSOutputError):
            read_descendant_zfsbackup_properties(Filesystem(DS))

    def test_a_configured_destinations_inherited_bad_value_does_raise_the_own_read(self, world):
        """The other half of the same residual: `also` asks the own read for the anchor and in-flight names of a
        configured destination, inherited or not."""
        world.add(DS, [(A + 'offsite', NEWLINE, f'inherited from {POOL}')])
        with pytest.raises(ZFSOutputError):
            read_zfsbackup_properties(Filesystem(DS), also=[A + 'offsite', IF + 'offsite'])
        assert read_zfsbackup_properties(Filesystem(DS), also=[A + 'unrelated']) == {}

    def test_our_properties_are_still_returned_beside_unrelated_ones_everywhere(self, world):
        world.add(DS, [('user:a', NEWLINE, 'local'), (A + 'x', SNAP, 'local'), ('user:b', NEWLINE, 'received')])
        world.add(CH0, [(A + 'y', SNAP2, 'received'), ('user:c', NEWLINE, 'local')])
        world.add(CH1, [('user:d', NEWLINE, 'local')])
        world.add(CH2, [(IF + 'z', f'{SNAP},{SNAP2}', 'local')])
        assert read_descendant_zfsbackup_properties(Filesystem(DS)) == {
            CH0: {A + 'y': SNAP2}, CH2: {IF + 'z': f'{SNAP},{SNAP2}'},
        }


class TestOurOwnValuesWithNewlinesRaise:
    @pytest.mark.parametrize('source', ['local', 'received'])
    def test_a_newline_in_an_org_zfsbackup_value_of_the_dataset(self, world, source):
        world.add(DS, [(A + 'offsite', NEWLINE, source)])
        with pytest.raises(ZFSOutputError):
            read_zfsbackup_properties(Filesystem(DS))

    @pytest.mark.parametrize('value', ['x\n', '\nx', 'a\n\nb', f'{SNAP}\n{DS}\ttype\tfilesystem\t-'])
    def test_each_shape_of_newline_in_our_value(self, world, value):
        world.add(DS, [(A + 'offsite', value, 'local')])
        with pytest.raises(ZFSOutputError):
            read_zfsbackup_properties(Filesystem(DS))

    @pytest.mark.parametrize('source', ['local', 'received'])
    def test_a_newline_in_an_org_zfsbackup_value_of_a_descendant(self, world, source):
        world.add(DS)
        world.add(CH0, [(A + 'offsite', SNAP, 'local')])
        world.add(CH1, [(A + 'offsite', NEWLINE, source)])
        with pytest.raises(ZFSOutputError):
            read_descendant_zfsbackup_properties(Filesystem(DS))

    def test_a_tab_in_our_value_is_recovered_not_refused(self, world):
        """Four columns and the source last: whatever lies between the property and the source is the value."""
        world.add(DS, [(A + 'tab', 'a\tb\tc', 'local')])
        world.add(CH0, [(A + 'tab', 'left\tright', 'received')])
        assert read_zfsbackup_properties(Filesystem(DS)) == {A + 'tab': 'a\tb\tc'}
        assert read_descendant_zfsbackup_properties(Filesystem(DS)) == {CH0: {A + 'tab': 'left\tright'}}

    def test_a_tab_in_an_unrelated_value_is_never_seen(self, world):
        world.add(DS, [('user:tab', 'a\tb', 'local'), (A + 'x', SNAP, 'local')])
        assert read_zfsbackup_properties(Filesystem(DS)) == {A + 'x': SNAP}

    def test_a_forged_complete_group_for_a_nonexistent_descendant_raises(self, world):
        """A value that prints a COMPLETE, well-formed group (`name`, `type`, the requested property) for a
        descendant that does not exist: phase 1 never named it, so the two phases disagree, twice."""
        ghost = f'{DS}/ghost'
        forged = f'x\treceived\n{ghost}\tname\t{ghost}\t-\n{ghost}\ttype\tfilesystem\t-\n{ghost}\t{A}evil\t{SNAP}'
        world.add(DS)
        world.add(CH0, [(A + 'evil', forged, 'received')])
        with pytest.raises(ZFSOutputError, match='changed while'):
            read_descendant_zfsbackup_properties(Filesystem(DS))
        assert len(world.argvs) == 4                      # one re-read of the pair, then the refusal


class TestPropertiesChangedBetweenThePhases:
    """Phase 2 is what is trusted; phase 1 is the oracle for completeness. A disagreement is read once more."""

    def hook(self, world, change, nth=2):
        """Run `change()` as the `nth` zfs call starts (the 2nd exec is phase 2 of the first read)."""
        calls = []

        def on_exec(argv):
            calls.append(argv)
            if len(calls) == nth:
                change()
        world.on_exec = on_exec

    def test_a_property_that_disappears_is_read_again_and_the_result_is_coherent(self, world, caplog):
        world.add(DS, [(A + 'a', SNAP, 'local'), (A + 'b', SNAP2, 'local')])
        self.hook(world, lambda: world.set_props(DS, [(A + 'a', SNAP, 'local')]))
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            assert read_zfsbackup_properties(Filesystem(DS)) == {A + 'a': SNAP}
        assert len(world.argvs) == 4
        assert records(caplog, logging.DEBUG, 'reading again') != []
        assert records(caplog, logging.WARNING) == [] and records(caplog, logging.ERROR) == []

    def test_a_persistent_disagreement_raises_after_exactly_one_re_read(self, world):
        world.add(DS, [(A + 'a', SNAP, 'local'), (A + 'b', SNAP2, 'local')])

        def drop_b_whenever_phase_two_starts(argv):
            if columns(argv) == VALUE_COLUMNS:
                world.set_props(DS, [(A + 'a', SNAP, 'local')])
            else:
                world.set_props(DS, [(A + 'a', SNAP, 'local'), (A + 'b', SNAP2, 'local')])
        world.on_exec = drop_b_whenever_phase_two_starts
        with pytest.raises(ZFSOutputError, match='changed while they were being read'):
            read_zfsbackup_properties(Filesystem(DS))
        assert len(world.argvs) == 4

    def test_a_cleared_inflight_and_a_new_anchor_never_read_as_nothing_pinned(self, world):
        """The remote worker finishes a chain step between the two execs: the pin is cleared, the anchor set. Phase 2
        asks for the pin only (no `also`): it comes back unset, which is a disappearance, which is a re-read."""
        world.add(DS, [(IF + 'offsite', f'{SNAP},{SNAP2}', 'local')])
        self.hook(world, lambda: world.set_props(DS, [(A + 'offsite', SNAP2, 'local')]))
        assert read_zfsbackup_properties(Filesystem(DS)) == {A + 'offsite': SNAP2}
        assert len(world.argvs) == 4

    def test_the_same_with_the_configured_names_asked_for(self, world):
        """With `also` the new anchor is accepted at once, and the vanished pin still forces the re-read."""
        world.add(DS, [(IF + 'offsite', f'{SNAP},{SNAP2}', 'local')])
        self.hook(world, lambda: world.set_props(DS, [(A + 'offsite', SNAP2, 'local')]))
        assert read_zfsbackup_properties(Filesystem(DS), also=[A + 'offsite', IF + 'offsite']) == {
            A + 'offsite': SNAP2}
        assert len(world.argvs) == 4

    def test_an_also_property_set_between_the_phases_is_returned_without_a_re_read(self, world):
        """Only an addition: a pin the remote worker creates after the discovery. Seen, accepted, two execs."""
        world.add(DS, [(A + 'offsite', SNAP, 'local')])
        self.hook(world, lambda: world.set_props(DS, [(A + 'offsite', SNAP, 'local'), (IF + 'offsite', f'{SNAP},{SNAP2}', 'local')]))
        assert read_zfsbackup_properties(Filesystem(DS), also=[A + 'offsite', IF + 'offsite']) == {
            A + 'offsite': SNAP, IF + 'offsite': f'{SNAP},{SNAP2}'}
        assert len(world.argvs) == 2

    def test_an_addition_that_is_not_in_also_is_never_asked_for_so_it_is_not_seen(self, world):
        """A name no one asked for cannot appear in phase 2 at all: the result is the discovery's set (the
        protect-set at the discovery's moment), never a smaller one."""
        world.add(DS, [(A + 'offsite', SNAP, 'local')])
        self.hook(world, lambda: world.set_props(DS, [(A + 'offsite', SNAP, 'local'), (IF + 'other', SNAP2, 'local')]))
        assert read_zfsbackup_properties(Filesystem(DS), also=[A + 'offsite', IF + 'offsite']) == {A + 'offsite': SNAP}
        assert len(world.argvs) == 2

    def test_a_property_that_appears_on_another_descendant_is_a_mismatch_and_is_read_again(self, world):
        """Phase 2 asks every object for every discovered name, so a descendant that gained one shows up as a pair
        phase 1 did not list. The re-read sees it in both phases."""
        world.add(DS)
        world.add(CH0, [(A + 'x', SNAP, 'local')])
        world.add(CH1)
        self.hook(world, lambda: world.set_props(CH1, [(A + 'x', SNAP2, 'local')]))
        assert read_descendant_zfsbackup_properties(Filesystem(DS)) == {CH0: {A + 'x': SNAP}, CH1: {A + 'x': SNAP2}}
        assert len(world.argvs) == 4

    def test_a_local_property_that_became_inherited_is_a_disappearance(self, world):
        world.add(POOL, [(A + 'a', SNAP2, 'local')])
        world.add(DS, [(A + 'a', SNAP, 'local')])
        self.hook(world, lambda: world.set_props(DS, [(A + 'a', SNAP2, f'inherited from {POOL}')]))
        assert read_zfsbackup_properties(Filesystem(DS)) == {}
        assert len(world.argvs) == 4

    def test_a_local_property_that_became_default_or_unset_is_a_disappearance(self, world):
        world.add(DS, [(A + 'a', SNAP, 'local')])
        self.hook(world, lambda: world.set_props(DS, []))
        assert read_zfsbackup_properties(Filesystem(DS)) == {}
        assert len(world.argvs) == 4

    def test_a_local_property_that_became_received_is_fine(self, world):
        world.add(DS, [(A + 'a', SNAP, 'local')])
        self.hook(world, lambda: world.set_props(DS, [(A + 'a', SNAP2, 'received')]))
        assert read_zfsbackup_properties(Filesystem(DS)) == {A + 'a': SNAP2}
        assert len(world.argvs) == 2

    def test_a_descendant_destroyed_between_the_phases_is_read_again(self, world):
        world.add(DS)
        world.add(CH0, [(A + 'x', SNAP, 'local')])
        world.add(CH1, [(A + 'x', SNAP2, 'local')])
        self.hook(world, lambda: world.datasets.pop(CH1))
        assert read_descendant_zfsbackup_properties(Filesystem(DS)) == {CH0: {A + 'x': SNAP}}
        assert len(world.argvs) == 4

    def test_the_dataset_destroyed_between_the_phases_raises_zfs_own_error(self, world):
        world.add(DS, [(A + 'a', SNAP, 'local')])
        self.hook(world, lambda: world.datasets.pop(DS))
        with pytest.raises(Exception, match='does not exist'):
            read_zfsbackup_properties(Filesystem(DS))


class TestEmptyDiscovery:
    def test_the_own_read_still_asks_for_type_and_never_reads_as_no_anchors_without_it(self, world):
        world.add(DS, [('compression', 'lz4', 'local')])
        assert read_zfsbackup_properties(Filesystem(DS)) == {}
        discovery, values = world.argvs
        assert columns(discovery) == DISCOVERY_COLUMNS and columns(values) == VALUE_COLUMNS
        assert values[-2:] == ['name,type', DS]

    def test_a_missing_type_row_raises(self, mock_subprocess):
        """Phase 2 printed the `name` row and nothing else for the dataset: it was not read."""
        from _get_support import source_lines
        mock_subprocess.setup_multi(([],), ([f'{DS}\tname\t{DS}\t-\n'],))
        with pytest.raises(ZFSOutputError):
            read_zfsbackup_properties(Filesystem(DS))

    def test_a_zfs_printing_nothing_with_exit_zero_is_not_read_as_no_anchors(self, tmp_path, monkeypatch):
        stub = tmp_path / 'zfs'
        stub.write_text('#!/bin/sh\nexit 0\n')
        stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
        monkeypatch.setenv('ZFS_CMD', str(stub))
        with pytest.raises(ZFSOutputError, match='no usable rows'):
            read_zfsbackup_properties(Filesystem(DS))

    def test_the_own_read_with_configured_names_and_no_properties_asks_type_and_those_names(self, world):
        world.add(DS)
        assert read_zfsbackup_properties(Filesystem(DS), also=[A + 'offsite', IF + 'offsite']) == {}
        assert phase_two(world)[0][-2:] == [f'name,type,{A}offsite,{IF}offsite', DS]
        assert len(world.argvs) == 2

    def test_the_recursive_read_with_nothing_found_is_one_exec_and_empty(self, world):
        world.add(DS, [(A + 'mine', SNAP, 'local')])
        world.add(CH0, [('user:n', NEWLINE, 'local')])
        assert read_descendant_zfsbackup_properties(Filesystem(DS)) == {}
        assert len(world.argvs) == 1 and columns(world.argvs[0]) == DISCOVERY_COLUMNS

    def test_the_recursive_read_of_a_missing_dataset_still_fails(self, world):
        with pytest.raises(Exception, match='does not exist'):
            read_descendant_zfsbackup_properties(Filesystem(DS))

    def test_also_with_the_recursive_read_is_refused_up_front(self, world):
        from zfsbackup.backup_manager import _read_own_properties
        with pytest.raises(ValueError):
            _read_own_properties(Filesystem(DS), True, (A + 'x',))
        assert world.argvs == []


class TestExecShapes:
    def test_the_own_read_with_configured_destinations(self, world, monkeypatch):
        """Discovered names first (first-seen order, once), then `also`, in the order the caller gave."""
        monkeypatch.setenv('ZFS_CMD', '/usr/bin/true')
        world.add(DS, [(IF + 'offsite', 'a,b', 'local'), (A + 'zeta', SNAP, 'received'), ('compression', 'lz4', 'local')])
        read_zfsbackup_properties(Filesystem(DS), also=[A + 'offsite', IF + 'offsite'])
        assert world.argvs == [
            ['get', '-H', '-o', DISCOVERY_COLUMNS, '-s', 'local,received', '-t', 'filesystem,volume', 'all', DS],
            ['get', '-H', '-o', VALUE_COLUMNS, '-t', 'filesystem,volume',
             f'name,type,{IF}offsite,{A}zeta,{A}offsite', DS],
        ]

    def test_the_recursive_read(self, world):
        world.add(DS)
        world.add(CH0, [(A + 'x', SNAP, 'local')])
        world.add(CH1, [(A + 'y', SNAP, 'local'), (A + 'x', SNAP, 'local')])
        read_descendant_zfsbackup_properties(Filesystem(DS))
        assert world.argvs == [
            ['get', '-H', '-o', DISCOVERY_COLUMNS, '-s', 'local,received', '-r', '-t', 'filesystem,volume', 'all', DS],
            ['get', '-H', '-o', VALUE_COLUMNS, '-r', '-t', 'filesystem,volume', f'name,type,{A}x,{A}y', DS],
        ]

    @pytest.mark.parametrize('recursive', [False, True])
    def test_no_name_only_list_and_no_received_column_anywhere(self, world, recursive):
        world.add(DS, [(A + 'p', SNAP, 'local')])
        world.add(CH0, [(A + 'c', SNAP, 'local')])
        if recursive:
            read_descendant_zfsbackup_properties(Filesystem(DS))
        else:
            read_zfsbackup_properties(Filesystem(DS))
        assert world.verbs('list') == []
        assert all('received' not in columns(a).split(',') for a in world.argvs)
        assert all('-s' not in a for a in phase_two(world))

    def test_get_anchors_asks_for_every_configured_destination_sorted(self, world):
        from zfsbackup.backup_manager import DatasetManager
        from zfsbackup.config import BackupConfig, DatasetConfig, RemoteDatasetConfig
        world.add(DS)
        config = BackupConfig(datasets=[DatasetConfig(
            name=DS, remote=[RemoteDatasetConfig(destination=d) for d in ('offsite', 'cold')])],
            snapshot_prefix=PREFIX)
        manager = DatasetManager(config)
        manager.get_anchors(manager.datasets[0])
        (values,) = phase_two(world)
        assert values[-2] == f'name,type,{A}cold,{IF}cold,{A}offsite,{IF}offsite'


class TestThroughThePruner:
    """The user-visible half: what the pruner does with the read."""
    DS = DS

    def build(self, pool, recursive=False, old=4, destinations=()):
        manager, dsi = prune_manager(self.DS, recursive=recursive, destinations=destinations)
        fresh, olds = pool.populate(self.DS, old=old)
        if recursive:
            pool.snaps[CH0] = list(pool.snaps[self.DS])
        return manager, dsi, fresh, olds

    def cycle(self, manager, dsi, caplog):
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)

    def test_unrelated_newline_properties_do_not_stop_pruning(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool, recursive=True)
        prune_pool.props[POOL] = [('user:note', NEWLINE, 'local')]
        prune_pool.props[self.DS] = [('user:note', NEWLINE, f'inherited from {POOL}'), (A + 'offsite', olds[1], 'local')]
        prune_pool.props[CH0] = [('user:note', NEWLINE, 'local'), ('user:other', NEWLINE, f'inherited from {POOL}')]
        self.cycle(manager, dsi, caplog)
        assert records(caplog, logging.ERROR) == []
        assert prune_pool.names_of(self.DS) == {fresh, olds[1]}          # the anchor kept, the rest pruned
        assert prune_pool.names_of(CH0) == {fresh, olds[1]}

    @pytest.mark.parametrize('where', ['own', 'descendant'])
    def test_a_newline_in_our_value_is_one_error_with_the_remedy_then_quiet_then_recovery(
            self, prune_pool, caplog, where):
        manager, dsi, fresh, olds = self.build(prune_pool, recursive=where == 'descendant')
        holder = self.DS if where == 'own' else CH0
        good = [(A + 'offsite', olds[1], 'local')]
        prune_pool.props[holder] = [(A + 'offsite', NEWLINE, 'local')]
        self.cycle(manager, dsi, caplog)
        (error,) = records(caplog, logging.ERROR, f'Cannot read anchors for {self.DS}')
        assert 'zfs get <property>' in error and 'zfs inherit' in error
        assert 'Unrelated' in error and 'a newline' in error
        assert prune_pool.calls == [] and prune_pool.names_of(self.DS) == set(olds) | {fresh}
        self.cycle(manager, dsi, caplog)
        assert records(caplog, logging.ERROR) == []
        assert records(caplog, logging.DEBUG, f'Cannot read anchors for {self.DS}') != []
        prune_pool.props[holder] = good
        self.cycle(manager, dsi, caplog)
        assert records(caplog, logging.INFO, 'can be read again') != []
        assert prune_pool.names_of(self.DS) == {fresh, olds[1]}

    def test_a_persistent_disagreement_makes_still_prunable_return_nothing(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool)
        prune_pool.props[self.DS] = [(A + 'a', olds[1], 'local'), (A + 'b', olds[2], 'local')]

        def flap(phase, ds, recursive):
            # the discovery sees both; phase 2 always finds `b` gone
            prune_pool.props[self.DS] = ([(A + 'a', olds[1], 'local')] if phase == 'get'
                                         else [(A + 'a', olds[1], 'local'), (A + 'b', olds[2], 'local')])
        prune_pool.on_read = flap
        group = manager.list_snapshots(dsi)
        assert group, 'the control: there is something to destroy, so [] is the read\'s doing'
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            assert manager._still_prunable(dsi, group) == []
        (error,) = records(caplog, logging.ERROR, 'Cannot re-read the anchors')
        assert 'changed while they were being read' in error
        assert prune_pool.calls == []

    def test_the_first_pass_with_a_persistent_disagreement_skips_the_dataset(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.build(prune_pool)
        base = [(A + 'a', olds[1], 'local'), (A + 'b', olds[2], 'local')]

        def flap(phase, ds, recursive):
            prune_pool.props[self.DS] = base[:1] if phase == 'get' else base
        prune_pool.on_read = flap
        self.cycle(manager, dsi, caplog)
        (error,) = records(caplog, logging.ERROR, f'Cannot read anchors for {self.DS}')
        assert 'changed while' in error and 'next cycle reads it again' in error
        assert prune_pool.calls == []

    def test_a_transient_disagreement_is_invisible_to_the_pruner(self, prune_pool, caplog):
        """One flap, then stable: the re-read succeeds, nothing is logged above DEBUG, the prune proceeds."""
        manager, dsi, fresh, olds = self.build(prune_pool)
        base = [(A + 'a', olds[1], 'local'), (A + 'b', olds[2], 'local')]
        prune_pool.props[self.DS] = base
        seen = []

        def flap_once(phase, ds, recursive):
            seen.append(phase)
            if len(seen) == 2:
                prune_pool.props[self.DS] = base[:1]
        prune_pool.on_read = flap_once
        self.cycle(manager, dsi, caplog)
        assert records(caplog, logging.ERROR) == [] and records(caplog, logging.WARNING) == []
        assert prune_pool.names_of(self.DS) == {fresh, olds[1]}

    def test_an_inflight_cleared_and_an_anchor_set_between_the_phases_still_protects_the_base(self, prune_pool):
        """The scenario the explicit re-read exists for: a chain step finishes between the two execs."""
        manager, dsi, fresh, olds = self.build(prune_pool, destinations=('offsite',))
        prune_pool.props[self.DS] = [(IF + 'offsite', f'{olds[1]},{olds[2]}', 'local')]
        seen = []

        def step_completes(phase, ds, recursive):
            seen.append(phase)
            if len(seen) == 2:                     # between the first read's phases
                prune_pool.props[self.DS] = [(A + 'offsite', olds[2], 'local')]
        prune_pool.on_read = step_completes
        protected = manager.get_anchors(dsi)
        assert protected == {olds[2]}               # never the empty set

    def test_an_orphan_anchor_of_an_unconfigured_destination_still_protects_and_is_discovered(self, prune_pool):
        manager, dsi, fresh, olds = self.build(prune_pool, destinations=('offsite',))
        prune_pool.props[self.DS] = [(A + 'renamed-away', olds[1], 'local'), (IF + 'also-gone', f'{olds[2]},{olds[3]}', 'received')]
        assert manager.get_anchors(dsi) >= {olds[1], olds[2], olds[3]}
        manager.prune_snapshots(dsi)
        assert prune_pool.names_of(self.DS) == {fresh, olds[1], olds[2], olds[3]}

    def test_the_own_properties_phase_two_is_the_last_call_before_every_destroy(self, prune_pool):
        manager, dsi, fresh, olds = self.build(prune_pool, recursive=True, destinations=('offsite', 'cold'))
        prune_pool.props[CH0] = [(A + 'elsewhere', olds[0], 'local')]
        prune_pool.send(self.DS, olds[1])               # the batch fails: singles follow
        manager.prune_snapshots(dsi)
        events = prune_pool.events
        destroys = [i for i, e in enumerate(events) if e[0] == 'destroy']
        assert len(destroys) >= 2
        for i in destroys:
            assert events[i - 2:i] == [('discover', self.DS, False), ('get', self.DS, False)]

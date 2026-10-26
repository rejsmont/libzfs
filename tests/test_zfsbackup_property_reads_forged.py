"""Plan B-1d: the property readers over FORGED `zfs get -H` bytes.

`read_zfsbackup_properties` / `read_descendant_zfsbackup_properties` are a two-phase read: a discovery that
carries no value (`-o name,property,source`, nothing an owner writes can disturb it), then the framing-safe
`zfs.get.rows(<only the names found>, received=False, strict=True)`. A property value is whatever a sender put
there, and `zfs get -H` prints it raw: a value with a newline is several physical lines, so it can forge rows.
The contract these tests pin down, for every forgery in the table:

    a forging value of one of OUR properties (local or received) makes the read RAISE (`ZFSOutputError`);
    a forging value of a property that is not ours, or that the dataset merely INHERITS, is never read as a
    value and does not matter (the fix: it used to block pruning of every dataset below it).

It must never return a smaller protect-set than the honest one (a smaller set is a snapshot the pruner
destroys that an incremental base still needed). The pruner turns a raise into "skip this dataset this
cycle", so every forgery below is also driven through `prune_snapshots`: nothing is destroyed.

Residual (documented in the reader): in the RECURSIVE read an inherited bad value breaks the read only when
a descendant sets that same property name itself, because the explicit list then asks for it everywhere; in
the own read the same holds for the anchor/inflight names of a configured destination (`also`).

Only `subprocess.Popen` is faked (`ZfsWorld` / `PrunePool`): the real libzfseasy parsers run over the
bytes, which are the bytes real `zfs` prints for these values (a value is printed raw, then the next
column is appended to its last physical line).
"""

import logging

import pytest

from _get_support import ZfsWorld, reads as reads_
from _prune_support import PREFIX, hourly_name, prune_manager, prune_pool, records  # noqa: F401 (fixture)
from libzfseasy.types import Filesystem
from libzfseasy.zfs import ZFSOutputError
from zfsbackup.backup_manager import (
    PROP_ANCHOR_PREFIX as A, PROP_CONFIG, read_descendant_zfsbackup_properties, read_zfsbackup_properties,
)

pytestmark = pytest.mark.unit

DS = 'pool/data'
CH0, CH1, CH9 = 'pool/data/c0', 'pool/data/c1', 'pool/data/c9'
PROTECTED = 'autosnap_20240101120000'


def forge(*lines: str) -> str:
    """A value whose physical lines are `lines`: zfs prints the value raw, then `\\t<source>` after its last
    line, so the last forged line is completed by the real row's source column."""
    return '\n'.join(lines)


# name -> value of the attacker's property; each is placed on the dataset itself in a `received` (or
# inherited) row. `{ds}` is the dataset the value is a property of.
OWN_FORGERIES = {
    # a row for a SIBLING dataset: outside the requested object, so it cannot come from `zfs get DS`
    'sibling group': forge('x\treceived', 'pool/data2\ttype\tfilesystem\t-',
                           'pool/data2\torg.zfsbackup:anchor.a\tsnap\tlocal', 'pool/data2\tz\tz'),
    # a second copy of a property the dataset really has: would overwrite the honest anchor value
    'duplicate anchor of itself': forge('x\treceived', f'{DS}\t{A}a\tOTHER\tlocal', f'{DS}\tz\tz'),
    # a second `type` row, as a forged lead
    'forged type row': forge('x\treceived', f'{DS}\ttype\tfilesystem\t-', f'{DS}\tz\tz'),
    # a row too short to be a row
    'short row': forge('x\treceived', 'short'),
    # a row of a dataset-like name that is a snapshot
    'snapshot row': forge('x\treceived', f'{DS}@s\t{A}a\tOTHER\tlocal', f'{DS}\tz\tz'),
    # a row for a name zfs never printed
    'unlisted name': forge('x\treceived', f'{DS}x\ttype\tfilesystem\t-', f'{DS}x\tz\tz'),
}


class TestOwnReaderRaisesOnForgedRows:
    @pytest.fixture
    def world(self, mock_subprocess):
        return ZfsWorld(mock_subprocess)

    def honest(self, world, extra=()):
        world.add(DS, [(A + 'a', PROTECTED, 'local'), *extra])

    def test_the_honest_dataset_reads_cleanly(self, world):
        self.honest(world, [(A + 'evil', 'plain value', 'received')])
        assert read_zfsbackup_properties(Filesystem(DS)) == {A + 'a': PROTECTED, A + 'evil': 'plain value'}

    @pytest.mark.parametrize('source', ['received', 'local'])
    @pytest.mark.parametrize('name', sorted(OWN_FORGERIES))
    def test_a_forged_row_makes_the_read_fail(self, world, name, source):
        """A received or local value of one of OUR properties that forges rows is refused whole -- the honest
        anchor is not returned in a doctored result."""
        self.honest(world, [(A + 'evil', OWN_FORGERIES[name], source)])
        with pytest.raises(ZFSOutputError):
            read_zfsbackup_properties(Filesystem(DS))

    @pytest.mark.parametrize('name', sorted(OWN_FORGERIES))
    def test_the_same_forgery_in_an_inherited_value_does_not_matter(self, world, name):
        """The feature: an INHERITED value is not discovered (`-s local,received`), so it is never requested,
        never read, and cannot break the group. The honest anchor is returned."""
        self.honest(world, [(A + 'evil', OWN_FORGERIES[name], 'inherited from pool')])
        assert read_zfsbackup_properties(Filesystem(DS)) == {A + 'a': PROTECTED}

    @pytest.mark.parametrize('name', sorted(OWN_FORGERIES))
    def test_the_same_forgery_in_a_property_that_is_not_ours_does_not_matter(self, world, name):
        self.honest(world, [('org.other:evil', OWN_FORGERIES[name], 'local'), ('user:evil', OWN_FORGERIES[name], 'received')])
        assert read_zfsbackup_properties(Filesystem(DS)) == {A + 'a': PROTECTED}

    def test_a_forged_extra_property_of_the_dataset_itself_now_fails_the_read(self, world):
        """It used to be the one forgery the reader could not tell from a real row (a NEW property in the
        dataset's own group, which could only grow the set). With an explicit list the group must be exactly
        `name, type` and the requested properties, so the extra row refuses the read."""
        self.honest(world, [(A + 'evil', forge('x\treceived', f'{DS}\t{A}forged\t{PROTECTED}\tlocal', f'{DS}\tz\tz'),
                             'received')])
        with pytest.raises(ZFSOutputError):
            read_zfsbackup_properties(Filesystem(DS))

    def test_an_inherited_forging_value_of_a_configured_destinations_name_does_break_the_own_read(self, world):
        """The pinned residual of `also`: the anchor/inflight names of a configured destination are asked for in
        phase 2 whatever phase 1 found, so a newline in an INHERITED value of exactly such a name is in the
        group and refuses it."""
        world.add(DS, [(A + 'offsite', OWN_FORGERIES['forged type row'], 'inherited from pool')])
        with pytest.raises(ZFSOutputError):
            read_zfsbackup_properties(Filesystem(DS), also=[A + 'offsite'])
        assert read_zfsbackup_properties(Filesystem(DS), also=[A + 'other']) == {}

    def test_a_tab_in_a_value_is_kept_verbatim_and_never_moves_a_column(self, world):
        """Four columns and the source LAST: whatever precedes it is the value. A value that looks like
        `v<TAB>-<TAB>local` is still a received value `v\\t-\\tlocal`; it cannot become a local row."""
        self.honest(world, [(A + 'tab', 'v\t-\tlocal', 'received'), (A + 'tab2', 'a\tb', 'local')])
        assert read_zfsbackup_properties(Filesystem(DS)) == {
            A + 'a': PROTECTED, A + 'tab': 'v\t-\tlocal', A + 'tab2': 'a\tb',
        }

    def test_the_stray_fragment_of_a_value_ending_in_a_newline_is_refused(self, world):
        """A value that ends with a newline leaves the source column alone on a line of its own, which is
        no row: the read fails (one row skipped) instead of ignoring the fragment."""
        self.honest(world, [(A + 'evil', 'FORGED\tlocal\n', 'received')])
        with pytest.raises(ZFSOutputError):
            read_zfsbackup_properties(Filesystem(DS))

    def test_empty_output_fails_the_read(self, mock_subprocess):
        """`zfs get` printed nothing for the dataset (a lost group): unknown protect-set, not an empty one.
        The own-dataset read runs no cross-check list; it requires the dataset's own `type` row instead."""
        mock_subprocess.setup_multi(([],), ([],))
        with pytest.raises(ZFSOutputError):
            read_zfsbackup_properties(Filesystem(DS))

    def test_a_read_is_two_execs_and_no_list(self, world):
        self.honest(world)
        read_zfsbackup_properties(Filesystem(DS))
        assert [a[0] for a in world.argvs] == ['get', 'get']

    def test_rows_forged_for_the_dataset_when_get_printed_none_of_its_own_fail_the_read(self, mock_subprocess):
        """Everything `zfs get` printed is a forgery for ANOTHER name: the dataset's own group is absent."""
        mock_subprocess.setup_multi(*reads_(DS, [(A + 'a', PROTECTED, 'local')])[:1], (['pool/data\tx\ty\n'],))
        with pytest.raises(ZFSOutputError):
            read_zfsbackup_properties(Filesystem(DS))


class TestDescendantReaderRaisesOnForgedRows:
    """`read_descendant_zfsbackup_properties(DS)`: one recursive read. A forgery in ANY descendant's value
    that touches another group makes the whole read fail, so the ancestor is skipped this cycle."""

    @pytest.fixture
    def world(self, mock_subprocess):
        w = ZfsWorld(mock_subprocess)
        w.add(DS)
        return w

    def group(self, name, extra=''):
        """The forged group of `name` as a value: `type` lead, an anchor naming PROTECTED."""
        return forge(f'x\treceived', f'{name}\ttype\tfilesystem\t-', f'{name}\t{A}a\t{PROTECTED}\tlocal',
                     f'{name}\tz\tz')

    def test_the_honest_subtree_reads_cleanly(self, world):
        world.add(CH0, [(A + 'a', PROTECTED, 'local')])
        world.add(CH1, [(A + 'evil', 'plain', 'received')])
        world.add(CH9, [(A + 'b', 'other', 'local')])
        assert read_descendant_zfsbackup_properties(Filesystem(DS)) == {
            CH0: {A + 'a': PROTECTED}, CH1: {A + 'evil': 'plain'}, CH9: {A + 'b': 'other'},
        }

    @pytest.mark.parametrize('victim', [CH0, CH9], ids=['forges an earlier sibling', 'forges a later sibling'])
    def test_a_value_forging_a_whole_sibling_group_fails_the_read(self, world, victim):
        """c0 and c9 really exist (the honest group of the sibling comes before or after c1's forged copy):
        the name comes back, so neither copy is trusted."""
        world.add(CH0, [(A + 'real0', 'snap0', 'local')])
        world.add(CH1, [(A + 'evil', self.group(victim), 'received')])
        world.add(CH9, [(A + 'real9', 'snap9', 'local')])
        with pytest.raises(ZFSOutputError):
            read_descendant_zfsbackup_properties(Filesystem(DS))

    def test_a_forged_group_for_a_dataset_that_does_not_exist_fails_the_read(self, world):
        world.add(CH1, [(A + 'evil', self.group('pool/data/ghost'), 'received')])
        with pytest.raises(ZFSOutputError):
            read_descendant_zfsbackup_properties(Filesystem(DS))

    def test_a_value_forging_a_duplicate_anchor_of_its_own_dataset_fails_the_read(self, world):
        world.add(CH1, [(A + 'a', PROTECTED, 'local'),
                        (A + 'evil', forge('x\treceived', f'{CH1}\t{A}a\tOTHER\tlocal', f'{CH1}\tz\tz'), 'received')])
        with pytest.raises(ZFSOutputError):
            read_descendant_zfsbackup_properties(Filesystem(DS))

    def test_a_value_forging_a_type_lead_fails_the_read(self, world):
        world.add(CH0, [(A + 'real0', 'snap0', 'local')])
        world.add(CH1, [(A + 'evil', forge('x\treceived', f'{CH0}\ttype\tfilesystem\t-', f'{CH0}\tz\tz'), 'received')])
        with pytest.raises(ZFSOutputError):
            read_descendant_zfsbackup_properties(Filesystem(DS))

    def test_an_inherited_forging_value_does_not_fail_the_read(self, world):
        """Not discovered, so never requested: the read returns the honest descendants."""
        world.add(CH0, [(A + 'real0', 'snap0', 'local')])
        world.add(CH1, [(A + 'evil', self.group(CH0), 'inherited from pool/data')])
        assert read_descendant_zfsbackup_properties(Filesystem(DS)) == {CH0: {A + 'real0': 'snap0'}}

    def test_an_inherited_forging_value_fails_the_read_when_a_descendant_sets_that_name_itself(self, world):
        """The documented residual: phase 2 asks every object for the names ANY descendant set, so the inherited
        bad value of that same name is in its object's group."""
        world.add(CH0, [(A + 'evil', 'plain', 'local')])
        world.add(CH1, [(A + 'evil', self.group(CH0), 'inherited from pool/data')])
        with pytest.raises(ZFSOutputError):
            read_descendant_zfsbackup_properties(Filesystem(DS))

    def test_a_value_forging_a_row_of_the_ancestor_fails_the_read(self, world):
        world.add(CH1, [(A + 'evil', forge('x\treceived', f'{DS}\t{A}a\tOTHER\tlocal', f'{DS}\tz\tz'), 'received')])
        with pytest.raises(ZFSOutputError):
            read_descendant_zfsbackup_properties(Filesystem(DS))


# ---------------------------------------------------------------------------------------------
# Through the pruner: a refused read destroys nothing
# ---------------------------------------------------------------------------------------------

class TestForgedRowsNeverShrinkThePruneProtectSet:
    DS = 'pool/data'

    def old(self, prune_pool, recursive=False):
        manager, dsi = prune_manager(self.DS, recursive=recursive)
        fresh, olds = prune_pool.populate(self.DS, old=4)
        if recursive:
            prune_pool.snaps['pool/data/c1'] = list(prune_pool.snaps[self.DS])
        return manager, dsi, fresh, olds

    def test_control_the_honest_anchor_is_kept_and_the_rest_pruned(self, prune_pool):
        manager, dsi, fresh, olds = self.old(prune_pool)
        prune_pool.props[self.DS] = [(A + 'offsite', olds[1], 'local')]
        manager.prune_snapshots(dsi)
        assert prune_pool.names_of(self.DS) == {fresh, olds[1]}

    @pytest.mark.parametrize('name', ['duplicate anchor of itself', 'forged type row', 'short row', 'sibling group'])
    def test_a_forged_row_on_the_dataset_skips_its_prune(self, prune_pool, caplog, name):
        manager, dsi, fresh, olds = self.old(prune_pool)
        # the forgery tries to REPLACE the honest anchor (olds[1]) by another name
        payload = OWN_FORGERIES[name].replace('OTHER', olds[3])
        prune_pool.props[self.DS] = [(A + 'a', olds[1], 'local'), (A + 'evil', payload, 'received')]
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.calls == []
        assert prune_pool.names_of(self.DS) == set(olds) | {fresh}
        assert records(caplog, logging.ERROR, f'Cannot read anchors for {self.DS}')

    def test_an_inherited_forgery_in_a_descendant_does_not_skip_the_recursive_ancestor(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.old(prune_pool, recursive=True)
        child = 'pool/data/c1'
        payload = forge('x\treceived', f'{child}\t{A}offsite\t{olds[3]}\tlocal', f'{child}\tz\tz')
        prune_pool.props[child] = [(A + 'offsite', olds[1], 'local'), (A + 'evil', payload, 'inherited from pool/data')]
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert records(caplog, logging.ERROR) == []
        assert olds[1] in prune_pool.names_of(self.DS) and olds[1] in prune_pool.names_of(child)   # still protected
        assert prune_pool.names_of(self.DS) == {fresh, olds[1]}                                       # and the rest pruned

    def test_a_forgery_in_a_descendant_skips_the_recursive_ancestor(self, prune_pool, caplog):
        manager, dsi, fresh, olds = self.old(prune_pool, recursive=True)
        child = 'pool/data/c1'
        payload = forge('x\treceived', f'{child}\t{A}offsite\t{olds[3]}\tlocal', f'{child}\tz\tz')
        prune_pool.props[child] = [(A + 'offsite', olds[1], 'local'), (A + 'evil', payload, 'received')]
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)
        assert prune_pool.calls == []
        assert prune_pool.names_of(self.DS) == set(olds) | {fresh}
        assert prune_pool.names_of(child) == set(olds) | {fresh}

    def test_a_forged_extra_anchor_now_skips_the_prune(self, prune_pool):
        """It used to only protect more; the explicit list no longer lets an extra row into the group."""
        manager, dsi, fresh, olds = self.old(prune_pool)
        payload = forge('x\treceived', f'{self.DS}\t{A}forged\t{olds[2]}\tlocal', f'{self.DS}\tz\tz')
        prune_pool.props[self.DS] = [(A + 'offsite', olds[1], 'local'), (A + 'evil', payload, 'received')]
        manager.prune_snapshots(dsi)
        assert prune_pool.calls == [] and prune_pool.names_of(self.DS) == set(olds) | {fresh}


class TestTheRefusalIsReportedOncePerDataset:
    """(g) A refused read repeats every cycle until someone fixes the property; the ERROR is logged once per
    dataset (DEBUG afterwards), carries the remedy, an INFO says when it recovers, and it is armed again."""
    DS = 'pool/data'

    def setup(self, prune_pool):
        manager, dsi = prune_manager(self.DS)
        fresh, olds = prune_pool.populate(self.DS, old=3)
        good = [(A + 'a', olds[1], 'local')]
        bad = good + [(A + 'evil', forge('x\treceived', f'{self.DS}\t{A}a\tOTHER\tlocal', f'{self.DS}\tz\tz'),
                       'received')]
        return manager, dsi, fresh, olds, good, bad

    def cycle(self, manager, dsi, caplog):
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            manager.prune_snapshots(dsi)

    def test_one_error_per_dataset_then_recovery_then_rearmed(self, prune_pool, caplog):
        manager, dsi, fresh, olds, good, bad = self.setup(prune_pool)
        prune_pool.props[self.DS] = bad
        self.cycle(manager, dsi, caplog)
        (error,) = records(caplog, logging.ERROR, f'Cannot read anchors for {self.DS}')
        assert 'zfs get <property>' in error and 'zfs inherit' in error          # the remedy
        assert prune_pool.calls == []
        self.cycle(manager, dsi, caplog)                                          # still broken: quiet
        assert records(caplog, logging.ERROR) == []
        assert records(caplog, logging.DEBUG, f'Cannot read anchors for {self.DS}')
        prune_pool.props[self.DS] = good                                          # fixed
        self.cycle(manager, dsi, caplog)
        assert records(caplog, logging.INFO, 'can be read again')
        assert prune_pool.names_of(self.DS) == {fresh, olds[1]}                   # and pruning resumed
        prune_pool.props[self.DS] = bad                                           # broken again
        self.cycle(manager, dsi, caplog)
        assert len(records(caplog, logging.ERROR, f'Cannot read anchors for {self.DS}')) == 1

    def test_another_dataset_has_its_own_first_error(self, prune_pool, caplog):
        from datetime import timedelta
        from zfsbackup.backup_manager import DatasetManager
        from zfsbackup.config import BackupConfig, DatasetConfig, RetentionRule
        rules = [RetentionRule(timedelta(hours=1), timedelta(hours=2))]
        manager = DatasetManager(BackupConfig(
            datasets=[DatasetConfig(name=n, retention_rules=rules) for n in ('pool/data', 'pool/other')],
            snapshot_prefix=PREFIX))
        for name in ('pool/data', 'pool/other'):
            fresh, olds = prune_pool.populate(name, old=2)
            prune_pool.props[name] = [
                (A + 'a', olds[0], 'local'),
                (A + 'evil', forge('x\treceived', f'{name}\t{A}a\tOTHER\tlocal', f'{name}\tz\tz'), 'received')]
        first, second = manager.datasets
        self.cycle(manager, first, caplog)
        self.cycle(manager, second, caplog)
        assert len(records(caplog, logging.ERROR, 'Cannot read anchors for pool/other')) == 1
        self.cycle(manager, first, caplog)
        self.cycle(manager, second, caplog)
        assert records(caplog, logging.ERROR) == []

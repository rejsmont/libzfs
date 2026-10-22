"""Plan B-1 end to end: forged `zfs -H` bytes through the REAL libzfseasy parsers into the pruner.

`DatasetManager.received_datasets()` and `prune_snapshots()` run unmodified over the real
`zfs.list` / `zfs.get.value` / `zfs.destroy` commands; only `subprocess.Popen` is replaced (the
`mock_subprocess` fixture), by a fake `zfs` that answers by argv with the BYTES a hostile client's
property values could make real `zfs` print. Nothing is stubbed above that: not `zfs.list`, not
`get.value`, not the row parser.

The world: pool target `bk`, a victim client `A` whose datasets keep everything (a 100 year
retention) and a neighbour `Ax` -- a prefix-sibling, `bk/Ax/data` starts with `bk/A` -- whose own
retention keeps nothing but the newest snapshot. If the neighbour's configuration ever reached the
victim's dataset, the victim's old snapshots would be destroyed. Every scenario therefore asserts

* the victim's snapshots are never destroyed,
* no dataset outside `bk/<client id>/` (the dataset's own client's tree) is pruned or listed for
  snapshots,
* and, as the live control, that the neighbour's OWN snapshots are pruned under its own config
  (so a "nothing was destroyed" result cannot come from a test that never prunes).
"""

import dataclasses
from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from zfsbackup.backup_manager import DatasetManager, PROP_CLIENT_ID, PROP_CONFIG
from zfsbackup.config import (
    BackupConfig, DatasetConfig, RemoteServerConfig, RetentionRule,
)

TARGET = 'bk'
VICTIM = 'A'
NEIGHBOUR = 'Ax'
VICTIM_DS = 'bk/A/data'
NEIGHBOUR_DS = 'bk/Ax/data'

#: Old, managed snapshots (`autosnap_<UTC time>`); the newest is always kept by the pruner.
OLD = [f'autosnap_2020010{i}000000' for i in range(1, 6)]            # 5 snapshots, 2020-01-01..05

VICTIM_CONFIG = DatasetConfig(
    name='tank/data', frequency=timedelta(hours=1),
    retention_rules=[RetentionRule(timedelta(days=1), timedelta(days=365 * 100))],
).to_property()
#: The neighbour keeps one hour of history, nothing else.
NEIGHBOUR_CONFIG = DatasetConfig(
    name='tank/other', frequency=timedelta(hours=1),
    retention_rules=[RetentionRule(timedelta(hours=1), timedelta(hours=1))],
).to_property()


def _b(text):
    return text.encode()


class FakeZfs:
    """A `zfs` that answers by argv. `listing` is the raw bytes `zfs list -r -t filesystem` prints;
    `props[(dataset, property, source)]` the raw bytes `zfs get -H -o value -s <source>` prints
    for that property (absent: `-\\n`, i.e. unset); `snapshots[dataset]` the short names `zfs list
    -t snapshot` finds. Every command is recorded in `argvs`."""

    def __init__(self, mock_subprocess):
        self.listing = []
        self.props = {}
        self.snapshots = {}
        self.argvs = []
        mock_subprocess.side_effect = self._popen

    @staticmethod
    def _proc(lines, rc=0, text=False):
        """A finished `zfs`. `text`: `universal_newlines=True` callers (`_exec_out`) get `str`
        lines, the others (`_exec_lines`: list and `get.value`) bytes."""
        proc = MagicMock()
        proc.stdout.readline.side_effect = list(lines) + ['' if text else b'']
        proc.stderr.readline.return_value = ''
        proc.poll.side_effect = [None] * len(lines) + [rc] * 50
        proc.wait.return_value = rc
        proc.returncode = rc
        return proc

    def _popen(self, cmd, *args, **kwargs):
        argv = list(cmd[1:])
        self.argvs.append(argv)
        verb = argv[0]
        if verb == 'list':
            types = argv[argv.index('-t') + 1] if '-t' in argv else None
            root = argv[-1]
            if types == 'filesystem':
                return self._proc(self.listing)
            assert types == 'snapshot', argv
            rows = [_b(f'{root}@{s}\tsnapshot\t-\n') for s in self.snapshots.get(root, [])]
            return self._proc(rows)
        if verb == 'get':
            if '-r' in argv:
                return self._proc([], text=True)
            columns = argv[argv.index('-o') + 1]
            name = argv[-1]
            if columns == 'value':                                   # zfs.get.value
                source = argv[argv.index('-s') + 1] if '-s' in argv else None
                prop = argv[-2]
                raw = self.props.get((name, prop, source))
                return self._proc([b'-\n'] if raw is None else raw)
            # read_zfsbackup_properties: `zfs get ... all <name>`: `type` first, no zfsbackup props
            return self._proc([f'{name}\ttype\tfilesystem\t-\t-\n'], text=True)
        if verb == 'destroy':
            return self._proc([], text=True)
        raise AssertionError(f'unexpected zfs command {argv}')

    # -- what the pruner did ----------------------------------------------------------------------
    @property
    def destroyed(self):
        """Every full snapshot name given to `zfs destroy`, in order."""
        names = []
        for argv in self.argvs:
            if argv[0] == 'destroy':
                # `zfs destroy ds@snap1,snap2,...`: one dataset, a comma list of short names
                dataset, _, shorts = argv[-1].partition('@')
                names += [f'{dataset}@{short}' for short in shorts.split(',')]
        return names

    @property
    def destroy_argvs(self):
        return [a for a in self.argvs if a[0] == 'destroy']

    @property
    def snapshot_listings(self):
        """The datasets whose snapshots were listed."""
        return [a[-1] for a in self.argvs if a[0] == 'list' and '-t' in a and a[a.index('-t') + 1] == 'snapshot']

    def reads_of(self, name):
        return [a for a in self.argvs if a[-1] == name]


def lines(*rows):
    return [_b(r + '\n') for r in rows]


def legit_world(zfs):
    """The honest tree: containers `bk`, `bk/A`, `bk/Ax`, and the two client datasets."""
    zfs.listing = lines(
        'bk\tfilesystem', 'bk/A\tfilesystem', VICTIM_DS + '\tfilesystem',
        'bk/Ax\tfilesystem', NEIGHBOUR_DS + '\tfilesystem',
    )
    zfs.props[(VICTIM_DS, PROP_CLIENT_ID, 'local')] = lines(VICTIM)
    zfs.props[(VICTIM_DS, PROP_CONFIG, 'local')] = lines(VICTIM_CONFIG)
    zfs.props[(NEIGHBOUR_DS, PROP_CLIENT_ID, 'local')] = lines(NEIGHBOUR)
    zfs.props[(NEIGHBOUR_DS, PROP_CONFIG, 'local')] = lines(NEIGHBOUR_CONFIG)
    zfs.snapshots[VICTIM_DS] = list(OLD)
    zfs.snapshots[NEIGHBOUR_DS] = list(OLD)


@pytest.fixture
def zfs(mock_subprocess):
    return FakeZfs(mock_subprocess)


def manager():
    config = BackupConfig(
        datasets=[DatasetConfig(name='tank/data')], snapshot_prefix='autosnap',
        remote_backup=RemoteServerConfig(target_dataset=TARGET, enabled=True),
    )
    return DatasetManager(config)


def prune_everything(zfs):
    """What `PruningWorker` does with the received datasets: enumerate, prune each. Returns the
    `DatasetInfo` list `received_datasets()` produced."""
    mgr = manager()
    found = mgr.received_datasets()
    for dsi in found:
        mgr.prune_snapshots(dsi)
    return found


def assert_contained(zfs, found):
    """The invariants of every scenario."""
    # the victim's snapshots are never destroyed, and nothing outside bk/<own id>/ is pruned
    assert [d for d in zfs.destroyed if d.startswith(f'{VICTIM_DS}@') or d.startswith('bk/A@')] == []
    for dsi in found:
        owner = dsi.name.split('/')[1]
        assert dsi.name == f'{TARGET}/{owner}' or dsi.name.startswith(f'{TARGET}/{owner}/'), dsi.name
    for name in zfs.snapshot_listings:
        assert name.startswith('bk/'), name
    for full in zfs.destroyed:
        assert full.startswith('bk/'), full
    # every destroy is of a dataset the enumeration accepted
    accepted = {d.name for d in found}
    for full in zfs.destroyed:
        assert full.split('@')[0] in accepted, full
    # nothing outside the target is read at all
    for argv in zfs.argvs:
        for arg in argv:
            assert not arg.startswith('evil'), argv


# ---------------------------------------------------------------------------
# The honest tree, and the control that proves the pruner is live
# ---------------------------------------------------------------------------


class TestControl:
    def test_each_client_is_pruned_under_its_own_config_only(self, zfs):
        legit_world(zfs)
        found = prune_everything(zfs)
        assert sorted(d.name for d in found) == [VICTIM_DS, NEIGHBOUR_DS]
        assert_contained(zfs, found)
        # the neighbour keeps one hour of history: all but its newest snapshot are destroyed
        assert zfs.destroyed and all(d.startswith(f'{NEIGHBOUR_DS}@') for d in zfs.destroyed)
        assert sorted(d.split('@')[1] for d in zfs.destroyed) == sorted(OLD[:-1])
        # the victim's 100 year retention keeps everything
        assert not [d for d in zfs.destroyed if d.startswith(VICTIM_DS)]
        assert VICTIM_DS in zfs.snapshot_listings

    def test_the_enumeration_asks_only_for_names_and_types(self, zfs):
        legit_world(zfs)
        manager().received_datasets()
        (listing,) = [a for a in zfs.argvs if a[0] == 'list']
        assert listing[listing.index('-o') + 1] == 'name,type'
        assert PROP_CONFIG not in ' '.join(listing)


# ---------------------------------------------------------------------------
# The prefix-sibling: bk/A vs bk/Ax
# ---------------------------------------------------------------------------


class TestPrefixSibling:
    def test_a_neighbour_claiming_the_victims_id_is_skipped_not_pruned_into_it(self, zfs):
        """`bk/Ax/data` starts with `bk/A`, so a check on the bare prefix passes. Its client_id
        says `A` and its config keeps one hour: the victim's id with the neighbour's retention."""
        legit_world(zfs)
        zfs.props[(NEIGHBOUR_DS, PROP_CLIENT_ID, 'local')] = lines(VICTIM)
        found = prune_everything(zfs)
        assert_contained(zfs, found)
        assert [d.name for d in found] == [VICTIM_DS]                  # the forger is not accepted
        assert zfs.destroyed == []                                     # and the victim is intact
        assert NEIGHBOUR_DS not in zfs.snapshot_listings

    def test_the_other_direction_the_victim_cannot_claim_the_neighbour_either(self, zfs):
        legit_world(zfs)
        zfs.props[(VICTIM_DS, PROP_CLIENT_ID, 'local')] = lines(NEIGHBOUR)
        found = prune_everything(zfs)
        assert_contained(zfs, found)
        assert [d.name for d in found] == [NEIGHBOUR_DS]
        assert not [d for d in zfs.destroyed if d.startswith(VICTIM_DS)]

    def test_the_container_of_the_neighbour_is_not_the_victims_tree(self, zfs):
        """`bk/Ax` (a container) carrying the victim's id and config is outside `bk/A/` too."""
        legit_world(zfs)
        zfs.props[('bk/Ax', PROP_CLIENT_ID, 'local')] = lines(VICTIM)
        zfs.props[('bk/Ax', PROP_CONFIG, 'local')] = lines(NEIGHBOUR_CONFIG)
        zfs.snapshots['bk/Ax'] = list(OLD)
        found = prune_everything(zfs)
        assert_contained(zfs, found)
        assert 'bk/Ax' not in [d.name for d in found]
        assert 'bk/Ax' not in zfs.snapshot_listings

    def test_the_victims_own_container_may_carry_its_id(self, zfs):
        """The positive side of the boundary: `<target>/<id>` itself is the client's."""
        legit_world(zfs)
        zfs.props[('bk/A', PROP_CLIENT_ID, 'local')] = lines(VICTIM)
        zfs.props[('bk/A', PROP_CONFIG, 'local')] = lines(VICTIM_CONFIG)
        found = prune_everything(zfs)
        assert sorted(d.name for d in found) == ['bk/A', VICTIM_DS, NEIGHBOUR_DS]
        assert_contained(zfs, found)


# ---------------------------------------------------------------------------
# Forged bytes in the listing
# ---------------------------------------------------------------------------


class TestForgedListing:
    def test_a_forged_row_naming_the_victim_with_an_extra_column(self, zfs):
        """A row that carries a third column (a value smuggled through a property in the old
        `zfs list -o name,type,client_id,config` design)."""
        legit_world(zfs)
        zfs.listing += lines(f'{VICTIM_DS}\tfilesystem\t{NEIGHBOUR}\t{NEIGHBOUR_CONFIG}')
        found = prune_everything(zfs)
        assert_contained(zfs, found)
        # the parser is fail-closed: it may also drop the honest row that follows the malformed
        # one; what matters is that nothing outside the honest set is accepted
        assert {d.name for d in found} <= {VICTIM_DS, NEIGHBOUR_DS}
        assert not [d for d in zfs.destroyed if d.startswith(VICTIM_DS)]

    def test_a_duplicate_row_for_the_victim_is_not_acted_on_twice_or_differently(self, zfs):
        legit_world(zfs)
        zfs.listing += lines(f'{VICTIM_DS}\tfilesystem')
        found = prune_everything(zfs)
        assert_contained(zfs, found)
        assert [d.name for d in found].count(VICTIM_DS) <= 1
        assert not [d for d in zfs.destroyed if d.startswith(VICTIM_DS)]

    def test_a_row_outside_the_target_is_never_read_or_pruned(self, zfs):
        legit_world(zfs)
        zfs.listing += lines('evil/x\tfilesystem', 'evil\tfilesystem')
        zfs.props[('evil/x', PROP_CLIENT_ID, 'local')] = lines(VICTIM)
        zfs.props[('evil/x', PROP_CONFIG, 'local')] = lines(NEIGHBOUR_CONFIG)
        zfs.snapshots['evil/x'] = list(OLD)
        found = prune_everything(zfs)
        assert_contained(zfs, found)
        assert 'evil/x' not in [d.name for d in found]

    def test_a_row_of_a_sibling_pool_dataset_with_the_targets_prefix_is_not_the_target(self, zfs):
        """`bkx/A/data` shares the characters of `bk` but is not under it."""
        legit_world(zfs)
        zfs.listing += lines('bkx/A/data\tfilesystem')
        zfs.props[('bkx/A/data', PROP_CLIENT_ID, 'local')] = lines(VICTIM)
        zfs.props[('bkx/A/data', PROP_CONFIG, 'local')] = lines(NEIGHBOUR_CONFIG)
        zfs.snapshots['bkx/A/data'] = list(OLD)
        found = prune_everything(zfs)
        assert 'bkx/A/data' not in [d.name for d in found]
        assert not [d for d in zfs.destroyed if d.startswith('bkx')]

    def test_a_listing_row_split_across_lines_by_a_value_is_no_dataset(self, zfs):
        """What a value holding a newline would do to a framed listing: a row that is half of
        another. A name with a tab or an empty column is dropped, never guessed at."""
        legit_world(zfs)
        zfs.listing += lines('bk/Ax/da', 'ta\tfilesystem', f'{VICTIM_DS}')
        found = prune_everything(zfs)
        assert_contained(zfs, found)
        assert {d.name for d in found} <= {VICTIM_DS, NEIGHBOUR_DS}
        assert not [d for d in zfs.destroyed if d.startswith(VICTIM_DS)]


# ---------------------------------------------------------------------------
# Forged bytes in the per-dataset property reads
# ---------------------------------------------------------------------------


class TestForgedPropertyValues:
    def forged_config(self, *tail):
        """A neighbour's config value followed by extra lines, as `zfs get -H -o value` frames a
        value with embedded newlines: the lines are the value."""
        return [_b(NEIGHBOUR_CONFIG + '\n')] + [_b(t + '\n') for t in tail]

    def test_a_config_with_a_forged_row_appended_does_not_decode_and_is_skipped(self, zfs):
        legit_world(zfs)
        zfs.props[(NEIGHBOUR_DS, PROP_CONFIG, 'local')] = self.forged_config(
            f'{VICTIM_DS}\t{PROP_CONFIG}\t{NEIGHBOUR_CONFIG}\t-\tlocal')
        found = prune_everything(zfs)
        assert_contained(zfs, found)
        assert [d.name for d in found] == [VICTIM_DS]
        assert zfs.destroyed == []

    def test_a_client_id_with_a_forged_second_line_is_invalid(self, zfs):
        legit_world(zfs)
        zfs.props[(NEIGHBOUR_DS, PROP_CLIENT_ID, 'local')] = lines(NEIGHBOUR, VICTIM)
        found = prune_everything(zfs)
        assert_contained(zfs, found)
        assert [d.name for d in found] == [VICTIM_DS]
        assert zfs.destroyed == []

    @pytest.mark.parametrize('client_id', [
        'A/../Ax', 'A\tAx', 'Ax ', ' A', '', '.', '..', 'A/', '/A', 'A\r', 'A\x00', 'bk/A',
        'Ax/data', '../A',
    ])
    def test_an_odd_client_id_never_lets_a_dataset_into_another_clients_tree(self, zfs, client_id):
        legit_world(zfs)
        zfs.props[(NEIGHBOUR_DS, PROP_CLIENT_ID, 'local')] = lines(client_id) if client_id else lines('')
        found = prune_everything(zfs)
        assert_contained(zfs, found)
        assert not [d for d in zfs.destroyed if d.startswith(VICTIM_DS) or d.startswith('bk/A@')]
        assert NEIGHBOUR_DS not in [d.name for d in found]

    @pytest.mark.parametrize('payload', [
        b'\xff\xfe' + _b(NEIGHBOUR_CONFIG),                              # invalid UTF-8
        _b(NEIGHBOUR_CONFIG[:20]) + b'\n' + _b(NEIGHBOUR_CONFIG[20:]),   # newline inside the base64
        _b(NEIGHBOUR_CONFIG[:20]) + b'\r' + _b(NEIGHBOUR_CONFIG[20:]),
        _b(NEIGHBOUR_CONFIG) + b'\t-\tlocal',
    ], ids=['bad-utf8', 'newline-in-base64', 'cr-in-base64', 'trailing-columns'])
    def test_a_corrupt_config_is_skipped_for_that_dataset_only(self, zfs, payload):
        legit_world(zfs)
        zfs.props[(NEIGHBOUR_DS, PROP_CONFIG, 'local')] = [payload + b'\n']
        found = prune_everything(zfs)
        assert_contained(zfs, found)
        assert [d.name for d in found] == [VICTIM_DS]                    # the victim is still served
        assert zfs.destroyed == []

    def test_an_inherited_or_received_copy_is_not_a_local_value(self, zfs):
        """Only `-s local` is ever asked: a value the source filter hides reads as unset."""
        legit_world(zfs)
        del zfs.props[(NEIGHBOUR_DS, PROP_CLIENT_ID, 'local')]
        del zfs.props[(NEIGHBOUR_DS, PROP_CONFIG, 'local')]
        zfs.props[(NEIGHBOUR_DS, PROP_CONFIG, 'received')] = lines(NEIGHBOUR_CONFIG)
        found = prune_everything(zfs)
        assert [d.name for d in found] == [VICTIM_DS]
        assert zfs.destroyed == []
        value_reads = [a for a in zfs.argvs if a[0] == 'get' and a[a.index('-o') + 1] == 'value']
        assert value_reads
        assert {a[a.index('-s') + 1] for a in value_reads} <= {'local', 'received'}   # never unfiltered

    def test_a_zfs_get_failure_for_one_dataset_skips_it_and_serves_the_rest(self, zfs, mock_subprocess):
        legit_world(zfs)
        real = zfs._popen

        def failing(cmd, *args, **kwargs):
            argv = list(cmd[1:])
            if argv[0] == 'get' and argv[-1] == NEIGHBOUR_DS:
                zfs.argvs.append(argv)
                return zfs._proc([], rc=1)
            return real(cmd, *args, **kwargs)

        mock_subprocess.side_effect = failing
        found = prune_everything(zfs)
        assert [d.name for d in found] == [VICTIM_DS]
        assert zfs.destroyed == []


# ---------------------------------------------------------------------------
# What a forged config could do if it did get through (the reason for all of the above)
# ---------------------------------------------------------------------------


class TestWhyItMatters:
    def test_the_neighbours_config_on_the_victims_dataset_would_destroy_its_snapshots(self, zfs):
        """The attack the containment exists for: the same pruner, the victim's dataset, the
        neighbour's retention. Established directly with the honest `DatasetInfo` of the victim and
        the neighbour's config, to show the scenarios above are not vacuous."""
        legit_world(zfs)
        mgr = manager()
        (dsi,) = [d for d in mgr.received_datasets() if d.name == VICTIM_DS]
        forged = dataclasses.replace(dsi.config, **{
            'retention_rules': DatasetConfig.from_property(NEIGHBOUR_CONFIG).retention_rules})
        dsi.config = forged
        mgr.prune_snapshots(dsi)
        assert sorted(d.split('@')[1] for d in zfs.destroyed if d.startswith(VICTIM_DS)) == sorted(OLD[:-1])

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
import logging
import re
from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from _snapshot_support import list_row
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
    """A `zfs` that answers by argv. `listing` is the raw bytes `zfs list -r -t filesystem,volume` prints;
    `props[(dataset, property, source)]` the raw bytes `zfs get -H -o value -s <source>` prints
    for that property (absent: `-\\n`, i.e. unset); `snapshots[dataset]` the short names `zfs list
    -t snapshot` finds. Every command is recorded in `argvs`."""

    def __init__(self, mock_subprocess):
        self.listing = []
        self.props = {}
        self.snapshots = {}
        self.argvs = []
        self.batch_reads = 0                 # how many batch `get -r name,type,client_id,config` ran
        self.batch_override = None           # raw bytes lines to print instead of the honest batch
        self.batch_error = None              # stderr: the batch exits 1
        self.batch_omit = set()              # datasets the honest batch leaves out (a lost group)
        mock_subprocess.side_effect = self._popen

    @staticmethod
    def _proc(lines, rc=0, text=False, stderr=''):
        """A finished `zfs` printing the byte `lines` (`_exec_lines` reads bytes)."""
        lines = re.findall(rb'[^\n]*\n', b''.join(lines))      # the physical lines on the pipe
        proc = MagicMock()
        proc.stdout.readline.side_effect = list(lines) + ['' if text else b'']
        proc.stderr.readline.side_effect = [stderr.rstrip('\n') + '\n', ''] if stderr else ['']
        proc.poll.side_effect = [None] * len(lines) + [rc] * 50
        proc.wait.return_value = rc
        proc.returncode = rc
        return proc

    def _listed(self):
        """`(name, type)` of every well-formed listing line, in order."""
        out = []
        for raw in self.listing:
            cols = raw.decode('utf-8', 'surrogateescape').rstrip('\n').split('\t')
            if len(cols) == 2:
                out.append((cols[0], cols[1]))
        return out

    def _effective(self, name, prop):
        """`(raw value lines, source)` as `zfs get` would print the property: local, else received, else unset."""
        for source in ('local', 'received'):
            raw = self.props.get((name, prop, source))
            if raw is not None:
                return raw, source
        return [b'-\n'], '-'

    @staticmethod
    def _row(name, prop, raw, source):
        """One `zfs get -H -o name,property,value,source` row; the value is printed RAW (its own newlines
        become lines of the output, which is how a hostile value forges rows)."""
        return [(f'{name}\t{prop}\t').encode() + b''.join(raw)[:-1] + f'\t{source}\n'.encode()]

    def _popen(self, cmd, *args, **kwargs):
        argv = list(cmd[1:])
        self.argvs.append(argv)
        verb = argv[0]
        if verb == 'list':
            types = argv[argv.index('-t') + 1] if '-t' in argv else None
            root = argv[-1]
            if types == 'filesystem,volume':
                if '-r' in argv:                     # the enumeration under the target: as it was given
                    return self._proc(self.listing)
                # the name-only cross-check `zfs.get.rows` runs for one dataset
                return self._proc([f'{n}\t{t}\n'.encode() for n, t in self._listed() if n == root])
            assert types == 'snapshot', argv
            rows = [_b(list_row(s, root)) for s in self.snapshots.get(root, [])]
            return self._proc(rows)
        if verb == 'get':
            columns = argv[argv.index('-o') + 1]
            name = argv[-1]
            if columns == 'value':                                   # zfs.get.value
                source = argv[argv.index('-s') + 1] if '-s' in argv else None
                prop = argv[-2]
                raw = self.props.get((name, prop, source))
                return self._proc([b'-\n'] if raw is None else raw)
            if columns == 'name,property,source':
                # phase 1 of the own-property read (names and sources, `-s`, no value): the honest world has
                # no `org.zfsbackup:*` property on the datasets the pruner itself reads, so it prints nothing
                assert '-s' in argv and argv[-2] == 'all', argv
                return self._proc([])
            assert columns == 'name,property,value,source', argv     # never the `received` column
            assert '-s' not in argv, argv                            # it would drop the `type` rows
            if '-r' in argv and argv[-2].startswith('name,type,'):
                # B-1d: the ONE batch read of the received datasets' client_id and config
                self.batch_reads += 1
                if self.batch_error:
                    return self._proc([], rc=1, stderr=self.batch_error)
                if self.batch_override is not None:
                    return self._proc(self.batch_override)
                requested = argv[-2].split(',')[2:]
                out, seen = [], set()
                for dsname, dstype in self._listed():
                    if dsname in seen or dsname in self.batch_omit or \
                            not (dsname == name or dsname.startswith(name + '/')):
                        continue
                    seen.add(dsname)
                    out += [f'{dsname}\tname\t{dsname}\t-\n'.encode(), f'{dsname}\ttype\t{dstype}\t-\n'.encode()]
                    for prop in requested:
                        raw, source = self._effective(dsname, prop)
                        out += self._row(dsname, prop, raw, source)
                return self._proc(out)
            # phase 2 of the own read, `get ... name,type[,props] <name>`: exactly `name`, `type`, then each
            # requested property (unset: `-`)
            assert argv[-2].startswith('name,type'), argv
            assert '-r' not in argv, argv       # nothing below a received dataset carries a property here
            dstype = dict(self._listed()).get(name, 'filesystem')
            out = [f'{name}\tname\t{name}\t-\n'.encode(), f'{name}\ttype\t{dstype}\t-\n'.encode()]
            for prop in argv[-2].split(',')[2:]:
                out += [f'{name}\t{prop}\t-\t-\n'.encode()]
            return self._proc(out)
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
    def values(self):
        """The `zfs get -H -o value` reads (one property of one dataset)."""
        return [a for a in self.argvs if a[0] == 'get' and a[a.index('-o') + 1] == 'value']

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
# A received volume (B-2a: a volume stream is received as a volume)
# ---------------------------------------------------------------------------

NEIGHBOUR_VOL = 'bk/Ax/vol'
VICTIM_VOL = 'bk/A/vol'


class TestReceivedVolume:
    """A client volume is received as a volume (`volmode=none`, never a device). It is enumerated,
    pruned under its own config and destroyed by snapshot name like a filesystem: nothing about it
    assumes a mount."""

    def world(self, zfs):
        legit_world(zfs)
        zfs.listing += lines(f'{NEIGHBOUR_VOL}\tvolume', f'{VICTIM_VOL}\tvolume')
        zfs.props[(NEIGHBOUR_VOL, PROP_CLIENT_ID, 'local')] = lines(NEIGHBOUR)
        zfs.props[(NEIGHBOUR_VOL, PROP_CONFIG, 'local')] = lines(NEIGHBOUR_CONFIG)
        zfs.props[(VICTIM_VOL, PROP_CLIENT_ID, 'local')] = lines(VICTIM)
        zfs.props[(VICTIM_VOL, PROP_CONFIG, 'local')] = lines(VICTIM_CONFIG)
        zfs.snapshots[NEIGHBOUR_VOL] = list(OLD)
        zfs.snapshots[VICTIM_VOL] = list(OLD)

    def test_the_enumeration_returns_volumes_as_volumes(self, zfs):
        from libzfseasy.types import Volume
        self.world(zfs)
        found = manager().received_datasets()
        by_name = {d.name: d for d in found}
        assert sorted(by_name) == sorted([VICTIM_DS, NEIGHBOUR_DS, VICTIM_VOL, NEIGHBOUR_VOL])
        assert isinstance(by_name[NEIGHBOUR_VOL].dataset, Volume)
        (listing,) = [a for a in zfs.argvs if a[0] == 'list']
        assert listing[listing.index('-t') + 1] == 'filesystem,volume'

    def test_a_received_volume_is_pruned_under_its_own_config(self, zfs):
        self.world(zfs)
        found = prune_everything(zfs)
        assert_contained(zfs, found)
        assert sorted(d.split('@')[1] for d in zfs.destroyed if d.startswith(NEIGHBOUR_VOL + '@')) == sorted(OLD[:-1])
        assert not [d for d in zfs.destroyed if d.startswith((VICTIM_VOL, VICTIM_DS))]     # 100 years: all kept
        assert NEIGHBOUR_VOL in zfs.snapshot_listings

    def test_the_destroy_names_snapshots_only_and_never_touches_a_mount(self, zfs):
        self.world(zfs)
        prune_everything(zfs)
        volume_destroys = [a for a in zfs.destroy_argvs if a[-1].startswith(NEIGHBOUR_VOL + '@')]
        assert volume_destroys
        for argv in volume_destroys:
            assert argv[0] == 'destroy' and '@' in argv[-1]
        everything = ' '.join(' '.join(a) for a in zfs.argvs)
        for word in ('canmount', 'mountpoint', 'unmount', 'mount', 'volmode'):
            assert word not in everything, word

    def test_a_volume_outside_its_clients_tree_is_skipped_like_a_filesystem(self, zfs):
        self.world(zfs)
        zfs.props[(NEIGHBOUR_VOL, PROP_CLIENT_ID, 'local')] = lines(VICTIM)         # the forgery
        found = prune_everything(zfs)
        assert NEIGHBOUR_VOL not in [d.name for d in found]
        assert not [d for d in zfs.destroyed if d.startswith(NEIGHBOUR_VOL)]


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
        """Only LOCAL values count: the batch prints the received one with its source, and the
        pruner will not take it (it is the sender's, a `zfs send -p` stream)."""
        legit_world(zfs)
        del zfs.props[(NEIGHBOUR_DS, PROP_CLIENT_ID, 'local')]
        del zfs.props[(NEIGHBOUR_DS, PROP_CONFIG, 'local')]
        zfs.props[(NEIGHBOUR_DS, PROP_CONFIG, 'received')] = lines(NEIGHBOUR_CONFIG)
        found = prune_everything(zfs)
        assert [d.name for d in found] == [VICTIM_DS]
        assert zfs.destroyed == []

    @pytest.mark.parametrize('source', ['local', 'received'])
    def test_the_per_dataset_fallback_never_asks_for_an_unfiltered_value(self, zfs, source):
        """When the batch cannot be used the old exact path runs: `-s local` / `-s received` only."""
        legit_world(zfs)
        zfs.batch_error = 'pool I/O is currently suspended'
        del zfs.props[(NEIGHBOUR_DS, PROP_CLIENT_ID, 'local')]
        del zfs.props[(NEIGHBOUR_DS, PROP_CONFIG, 'local')]
        zfs.props[(NEIGHBOUR_DS, PROP_CONFIG, source)] = lines(NEIGHBOUR_CONFIG)
        found = prune_everything(zfs)
        assert [d.name for d in found] == [VICTIM_DS]
        assert zfs.destroyed == []
        value_reads = zfs.values
        assert value_reads
        assert {a[a.index('-s') + 1] for a in value_reads} <= {'local', 'received'}   # never unfiltered

    def test_a_zfs_get_failure_for_one_dataset_skips_it_and_serves_the_rest(self, zfs, mock_subprocess):
        """The batch is down, so each dataset is read on its own; one of those reads fails."""
        legit_world(zfs)
        zfs.batch_error = 'pool I/O is currently suspended'
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

    def test_an_anchor_read_failure_for_one_dataset_skips_only_its_prune(self, zfs, mock_subprocess):
        """The batch served both; the pruner's own anchor read of the neighbour then fails: its prune is
        skipped this cycle (an unknown protect-set), the victim is untouched."""
        legit_world(zfs)
        real = zfs._popen

        def failing(cmd, *args, **kwargs):
            argv = list(cmd[1:])
            if argv[0] == 'get' and argv[-1] == NEIGHBOUR_DS and argv[-2] == 'all':
                zfs.argvs.append(argv)
                return zfs._proc([], rc=1)
            return real(cmd, *args, **kwargs)

        mock_subprocess.side_effect = failing
        found = prune_everything(zfs)
        assert sorted(d.name for d in found) == [VICTIM_DS, NEIGHBOUR_DS]
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


# ---------------------------------------------------------------------------
# Plan B-1d: the ONE batch read of the received datasets' properties
# ---------------------------------------------------------------------------

ATTACKER = 'bk/Ax/att'
BATCH_WARNING = 'The batched property read of bk skipped'


def warnings_of(caplog):
    return [r.getMessage() for r in caplog.records
            if r.name == 'zfsbackup.backup_manager' and r.levelno == logging.WARNING]


def attacker_value(*lines):
    """A received `config` value that forges `lines` (physical lines of `zfs get` output). zfs prints
    the value raw and then `\treceived`, so the first line is completed to a valid row by a leading
    `x\treceived`, and the last forged line has `\treceived` appended to it."""
    return ''.join(f'{l}\n' for l in ('x\treceived',) + lines)[:-1]


def raw(value):
    """The raw `zfs get -H -o value` lines of `value`, in the fake's per-property storage."""
    return [(part + '\n').encode() for part in value.split('\n')]


def victim_group(client_id=VICTIM, config=NEIGHBOUR_CONFIG, name=VICTIM_DS):
    """The rows of a forged, complete-looking group for `name` (the last row gets `\treceived`)."""
    return (f'{name}\tname\t{name}\t-', f'{name}\ttype\tfilesystem\t-',
            f'{name}\t{PROP_CLIENT_ID}\t{client_id}\tlocal', f'{name}\t{PROP_CONFIG}\t{config}\tlocal')


class TestBatchRead:
    def keeps_victim(self, zfs, found):
        by_name = {d.name: d for d in found}
        assert VICTIM_DS in by_name, 'the victim vanished from pruning'
        assert by_name[VICTIM_DS].config.retention_rules == DatasetConfig.from_property(VICTIM_CONFIG).retention_rules
        assert ATTACKER not in by_name

    def prune_all(self, zfs):
        found = prune_everything(zfs)
        assert_contained(zfs, found)
        assert not [d for d in zfs.destroyed if d.startswith(VICTIM_DS) or d.startswith('bk/A@')]
        # the live control: the neighbour is still pruned under its own config
        assert sorted(d.split('@')[1] for d in zfs.destroyed if d.startswith(NEIGHBOUR_DS)) == sorted(OLD[:-1])
        return found

    # -- cost ---------------------------------------------------------------------------------
    def test_n_clean_datasets_cost_one_list_and_one_get(self, zfs):
        legit_world(zfs)
        for i in range(3):                                 # five client datasets in all
            name = f'bk/A/extra{i}'
            zfs.listing += lines(f'{name}\tfilesystem')
            zfs.props[(name, PROP_CLIENT_ID, 'local')] = lines(VICTIM)
            zfs.props[(name, PROP_CONFIG, 'local')] = lines(VICTIM_CONFIG)
        found = manager().received_datasets()
        assert len(found) == 5
        assert [a[0] for a in zfs.argvs] == ['list', 'get']
        assert zfs.batch_reads == 1 and zfs.values == []

    def test_the_batch_names_exactly_the_two_properties_recursively_without_the_received_column(self, zfs):
        legit_world(zfs)
        manager().received_datasets()
        (get,) = zfs.argvs[1:]
        assert get == ['get', '-H', '-o', 'name,property,value,source', '-r', '-t', 'filesystem,volume',
                       f'name,type,{PROP_CLIENT_ID},{PROP_CONFIG}', TARGET]

    # -- the batch is down ----------------------------------------------------------------------
    def test_a_failing_batch_sends_every_dataset_to_the_exact_path_with_one_warning(self, zfs, caplog):
        legit_world(zfs)
        zfs.batch_error = 'pool I/O is currently suspended'
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            found = self.prune_all(zfs)
        assert sorted(d.name for d in found) == [VICTIM_DS, NEIGHBOUR_DS]
        (warning,) = [w for w in warnings_of(caplog) if 'in one call' in w]
        assert 'pool i/o is currently suspended' in warning.lower()
        # five listed datasets: bk, bk/A, bk/Ax (no local properties: 3 reads each) + two clients (2 each)
        assert len(zfs.values) == 3 * 3 + 2 * 2

    # -- a group that is simply not there: only that dataset is re-read -------------------------------
    def test_a_dataset_the_batch_lacks_alone_falls_back(self, zfs):
        legit_world(zfs)
        zfs.batch_omit = {NEIGHBOUR_DS}
        found = self.prune_all(zfs)
        assert sorted(d.name for d in found) == [VICTIM_DS, NEIGHBOUR_DS]
        assert {a[-1] for a in zfs.values} == {NEIGHBOUR_DS}
        assert len(zfs.values) == 2                        # client_id and config, nothing else

    # -- local beats received ---------------------------------------------------------------------
    def test_local_and_received_values_together_the_local_one_wins(self, zfs):
        legit_world(zfs)
        zfs.props[(VICTIM_DS, PROP_CLIENT_ID, 'received')] = lines(NEIGHBOUR)
        zfs.props[(VICTIM_DS, PROP_CONFIG, 'received')] = lines(NEIGHBOUR_CONFIG)
        found = self.prune_all(zfs)
        self.keeps_victim(zfs, found)
        assert zfs.values == []                            # the batch alone served them

    def test_a_received_only_config_is_not_trusted_and_is_a_warning(self, zfs, caplog):
        legit_world(zfs)
        del zfs.props[(NEIGHBOUR_DS, PROP_CLIENT_ID, 'local')]
        del zfs.props[(NEIGHBOUR_DS, PROP_CONFIG, 'local')]
        zfs.props[(NEIGHBOUR_DS, PROP_CONFIG, 'received')] = lines(NEIGHBOUR_CONFIG)
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            found = prune_everything(zfs)
        assert [d.name for d in found] == [VICTIM_DS]
        assert zfs.destroyed == []
        (warning,) = [w for w in warnings_of(caplog) if NEIGHBOUR_DS in w]
        assert 'only received' in warning and 'never trusted' in warning
        assert zfs.values == []

    def test_a_received_config_ending_in_a_fake_local_row_is_not_taken_as_local(self, zfs, caplog):
        """(a) The stray fragment: the received-only config `FORGED<TAB>local<NL>` prints as
        `bk/Ax/data<TAB>config<TAB>FORGED<TAB>local` -- a valid-looking LOCAL row -- and then
        `<TAB>received` alone on the next line. The fragment makes the batch incomplete, so no dataset is
        taken from it: all fall back to the exact per-dataset path, where the config is only received."""
        legit_world(zfs)
        del zfs.props[(NEIGHBOUR_DS, PROP_CLIENT_ID, 'local')]
        del zfs.props[(NEIGHBOUR_DS, PROP_CONFIG, 'local')]
        zfs.props[(NEIGHBOUR_DS, PROP_CONFIG, 'received')] = [b'FORGED\tlocal\n', b'\n']
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            found = prune_everything(zfs)
        assert [d.name for d in found] == [VICTIM_DS]
        assert zfs.destroyed == []
        warnings = warnings_of(caplog)
        assert any(BATCH_WARNING in w for w in warnings)
        (only_received,) = [w for w in warnings if NEIGHBOUR_DS in w and 'only received' in w]
        assert 'never trusted' in only_received

    # -- forged groups ----------------------------------------------------------------------------
    def attacker_world(self, zfs, value, before_victim):
        legit_world(zfs)
        names = [n for n, _ in zfs._listed()]
        at = names.index(VICTIM_DS) if before_victim else len(names)
        names.insert(at, ATTACKER)
        zfs.listing = lines(*[f'{n}\tfilesystem' for n in names])
        zfs.props[(ATTACKER, PROP_CONFIG, 'received')] = raw(value)

    @pytest.mark.parametrize('before_victim', [True, False], ids=['forged group first', 'forged group last'])
    def test_a_forged_group_naming_the_victim_leaves_the_victim_with_its_own_retention(
            self, zfs, caplog, before_victim):
        """The received config of an attacker dataset forges a complete group for the victim (claiming
        the victim's id and the NEIGHBOUR's one-hour retention). The victim's name then appears twice, so
        neither copy is trusted: the victim is read exactly, returned, and keeps its 100 year retention."""
        self.attacker_world(zfs, attacker_value(*victim_group()), before_victim)
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            found = self.prune_all(zfs)
        self.keeps_victim(zfs, found)
        assert len([w for w in warnings_of(caplog) if BATCH_WARNING in w]) == 1

    @pytest.mark.parametrize('name, group', [
        ('duplicate client_id of itself', (f'{ATTACKER}\t{PROP_CLIENT_ID}\t{VICTIM}\tlocal',
                                          f'{ATTACKER}\t{PROP_CONFIG}\t{NEIGHBOUR_CONFIG}\tlocal')),
        ('forged type lead', (f'{VICTIM_DS}\ttype\tfilesystem\t-',
                              f'{VICTIM_DS}\t{PROP_CLIENT_ID}\t{VICTIM}\tlocal',
                              f'{VICTIM_DS}\t{PROP_CONFIG}\t{NEIGHBOUR_CONFIG}\tlocal')),
        ('group of a name outside the target', victim_group(name='evil/x')),
        ('too short a row', ('short',)),
    ])
    def test_every_other_forgery_in_the_table_reaches_the_exact_path(self, zfs, caplog, name, group):
        self.attacker_world(zfs, attacker_value(*group), before_victim=True)
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            found = self.prune_all(zfs)
        self.keeps_victim(zfs, found)
        assert any(BATCH_WARNING in w for w in warnings_of(caplog)), name

    def test_a_tab_in_a_value_is_one_value_and_never_a_second_column(self, zfs, caplog):
        """The config carries `X<TAB>-<TAB>local`: four columns and the source last, so it is just a
        (non-decoding) value. Nothing about it reaches another dataset, and the batch stays in use."""
        legit_world(zfs)
        zfs.props[(NEIGHBOUR_DS, PROP_CONFIG, 'local')] = lines('not-base64\t-\tlocal')
        with caplog.at_level(logging.WARNING, logger='zfsbackup.backup_manager'):
            found = prune_everything(zfs)
        assert [d.name for d in found] == [VICTIM_DS]
        assert zfs.destroyed == []
        assert not any(BATCH_WARNING in w for w in warnings_of(caplog))
        assert zfs.values == []

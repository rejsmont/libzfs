"""Plan item 3.2 (docs/local_destination_plan.md): pruning the copies under a local destination.

* `DatasetManager.local_copies(client_id, should_stop)` discovers THIS host's copies under each local
  destination's target (`zfs:<dataset>`): `<target>/<client_id>` and what lies under it, found and checked
  as the server's `received_datasets()` finds its own (`_discover_under`), but rooted at our id, so another
  host's tree -- including a prefix-sibling (`A` vs `Ax`) -- is never listed, read or destroyed;
* every `zfs` call of the local discovery runs under `libzfseasy.timeout` (60 s each, 600 s per target) and a
  target that does not answer ends in an error (one WARNING per episode), never in a wedged worker; the
  server's discovery is untimed, as it always was;
* `PruningWorker(client_id=)` appends the copies to its datasets and records one `discover` status record per
  local target under the destination's name; `zfsbackup-config check` grades them.

Everything runs the REAL libzfseasy parsers over `subprocess.Popen` replaced by a fake `zfs` that answers by
argv (`_get_support.ZfsWorld`, extended here with snapshot listings, `destroy` and timeout recording). Nothing
reaches a real `zfs`.

The world: target `bk`, this host `A`, a prefix-sibling `Ax` and a stranger `B`, each with a container
(`bk/<id>`), an intermediate parent (`bk/<id>/tank`), a filesystem copy and a volume copy. Every tree's copies
keep one hour of history, so ANY prune that reaches a tree it should not shows up as a destroy.

Mutation-checked on a scratch copy (failing tests in this file): discovery rooted at the target instead of
`<target>/<id>` 24; the batch rooted at the target 12; no refusal of a copy stamped with another id 2; the
local scope not timed 5; no `min(budget, left)` 1; no budget expiry 2; the batch timeout falling back to
per-dataset reads 1; a timeout on a per-dataset read swallowed 2 (loop) + 2 (value reads); WARNING on every
failed cycle instead of once per episode 3; no INFO on recovery 1; a missing target treated as "no copies yet"
3; recursive kept 1; no target dedupe 1; no client id validation 6; no stop check per target 1 / after a
target 1; one WARNING per incarnation lost 1; `retain` despite a failed target 4; the local keys left out of
`_status_keys` 8; a copy already listed not dropped 1; `should_stop` not passed 1; asking without a local
destination 2; no client id still discovering 2; `local_copies` raising unhandled 1; an error record written
ok 5; the record without the destination name 8. Surviving, and equivalent: dropping the `is_local` filter in
`local_copies` (a remote destination has no `target_dataset`, so the next line skips it anyway).
Timeout re-raise sites of a copy's pass (failing tests each): `_get_prop` 1; the listing's DEBUG branch 1; the
empty-listing re-raise 3; the anchor read 2; a batch destroy 2; a single destroy 1; `_still_prunable` 1; the
re-list that explains a failed destroy 1; the destroy scope off 1; destroy budget equal to the command budget 1;
the pass untimed 2; no block of the rest of the target 2; the block never reset 2; `local_target` unset 18;
stuck class folded into timeout 1; unused-destination silence 3; invalid id warned every time 1; the container's
existence not asked 3; no INFO on a recovered prune 1; prune WARNING every time 1; stuck suffix dropped 2;
no per-target keep of status records 6; `last_local_skip` not honoured 1; all targets unknown without a client
id 1.
"""

import base64
import json
import logging
import time
from datetime import timedelta
from types import SimpleNamespace
from typing import Callable, Dict, List, Optional
from unittest.mock import MagicMock

import pytest

import libzfseasy
from libzfseasy import zfs as zfs_module
from libzfseasy.types import Volume
from libzfseasy.zfs import ZFSTimeoutError, _timeout_var

import zfsbackup.backup_manager as bm
from _clock_support import FakeTime
from _get_support import ZfsWorld, parse_argv
from _prune_support import records
from _snapshot_support import list_row
from _worker_status_support import rec as status_rec, write_worker
from zfsbackup import health
from zfsbackup.backup_manager import (
    DatasetManager, LocalDiscovery, PROP_CLIENT_ID, PROP_CONFIG,
    LOCAL_DISCOVER_BUDGET, LOCAL_DISCOVER_COMMAND_BUDGET,
)
from zfsbackup.cli.check import worker_entries
from zfsbackup.cli.status import Workers
from zfsbackup.config import (
    BackupConfig, DatasetConfig, Destination, RemoteDatasetConfig, RemoteServerConfig, RetentionRule,
)
from zfsbackup.health import StatusWriter, ensure_status_dir, read_worker_status
from zfsbackup.stopflag import StopFlag
from zfsbackup.workers import ExitReason, PruningWorker

pytestmark = pytest.mark.unit

TARGET = 'bk'
OWN = 'A'
NEIGHBOUR = 'Ax'              # `bk/Ax/...` starts with `bk/A`
STRANGER = 'B'
PROP_SOURCE = 'org.zfsbackup:source_dataset'
OLD = [f'autosnap_2020010{i}000000' for i in range(1, 6)]      # 5 old managed snapshots, the newest kept

#: One hour of history: every old snapshot but the newest is prunable.
COPY_CONFIG = DatasetConfig(
    name='tank/data', frequency=timedelta(hours=1), recursive=True,
    retention_rules=[RetentionRule(timedelta(hours=1), timedelta(hours=1))],
).to_property()


def copy_props(owner, config=COPY_CONFIG, source='tank/data'):
    """What `LocalBackupManager` stamps on a copy: LOCAL client id, source dataset and config."""
    props = [(PROP_CLIENT_ID, owner, 'local'), (PROP_SOURCE, source, 'local')]
    return props + ([] if config is None else [(PROP_CONFIG, config, 'local')])


# ---------------------------------------------------------------------------
# The fake zfs
# ---------------------------------------------------------------------------


class LocalWorld(ZfsWorld):
    """`ZfsWorld` plus managed-snapshot listings (`snaps`), `destroy`, the `libzfseasy.timeout` in effect for
    every command (`timeouts`, parallel to `argvs`) and three ways to corrupt the one batched property read."""

    def __init__(self, mock_subprocess):
        super().__init__(mock_subprocess)
        self.snaps: Dict[str, List[str]] = {}
        self.destroyed: List[str] = []
        self.timeouts: List[Optional[float]] = []
        self.mutate: Optional[Callable[[List[str]], List[str]]] = None   # rewrites the batch's output lines
        self.omit_from_batch: set = set()                                # datasets the batch leaves out

    # the corrupting hook sees the lines of exactly one answer (the next batch)
    _mutate_now = None

    def _proc(self, lines, rc=0, stderr=''):
        if self._mutate_now is not None:
            lines = self._mutate_now(list(lines))
        return ZfsWorld._proc(lines, rc, stderr)

    @staticmethod
    def is_batch(argv):
        return (argv[0] == 'get' and argv[argv.index('-o') + 1] == 'name,property,value,source'
                and '-r' in argv and argv[-2].startswith('name,type,'))

    def _popen(self, cmd, *args, **kwargs):
        argv = list(cmd[1:])
        if argv[0] == 'list' and '-t' in argv and argv[argv.index('-t') + 1] == 'snapshot':
            self.argvs.append(argv)
            self.timeouts.append(_timeout_var.get())
            if self.on_exec is not None:
                self.on_exec(argv)
            root = argv[-1]
            return ZfsWorld._proc([list_row(s, root) for s in self.snaps.get(root, [])])
        if argv[0] == 'destroy':
            self.argvs.append(argv)
            self.timeouts.append(_timeout_var.get())
            if self.on_exec is not None:
                self.on_exec(argv)
            dataset, _, shorts = argv[-1].partition('@')
            for short in shorts.split(','):
                self.destroyed.append(f'{dataset}@{short}')
                if short in self.snaps.get(dataset, []):
                    self.snaps[dataset].remove(short)
            return ZfsWorld._proc([f'{dataset}@{short}\n' for short in shorts.split(',')])
        self.timeouts.append(_timeout_var.get())
        batch = self.is_batch(argv)
        hidden = {}
        if batch:
            hidden = {n: self.datasets.pop(n) for n in list(self.omit_from_batch) if n in self.datasets}
            self._mutate_now, self.mutate = self.mutate, None
        try:
            return super()._popen(cmd, *args, **kwargs)
        finally:
            self.datasets.update(hidden)
            self._mutate_now = None

    # -- what was asked / done -------------------------------------------------------------------
    def roots(self, verb=None):
        """The dataset each (listing or property) command was rooted at."""
        return [a[-1] for a in self.argvs if verb is None or a[0] == verb]

    @property
    def batches(self):
        return [a for a in self.argvs if self.is_batch(a)]

    @property
    def listings(self):
        return [a for a in self.argvs if a[0] == 'list' and '-r' in a and '-t' in a
                and a[a.index('-t') + 1] == 'filesystem,volume']

    def mentions(self, needle):
        return [a for a in self.argvs if any(arg == needle or arg.startswith(needle + '/')
                                             or arg.startswith(needle + '@') for arg in a)]

    @property
    def destroy_argvs(self):
        return [a for a in self.argvs if a[0] == 'destroy']


def build_world(world):
    """The honest tree, for `A`, `Ax` and `B`."""
    world.add(TARGET)
    for owner in (OWN, NEIGHBOUR, STRANGER):
        base = f'{TARGET}/{owner}'
        world.add(base, [(PROP_CLIENT_ID, owner, 'local')])
        world.add(f'{base}/tank', [(PROP_CLIENT_ID, owner, 'local')])
        world.add(f'{base}/tank/data', copy_props(owner))
        world.add(f'{base}/tank/vol', copy_props(owner, source='tank/vol'), 'volume')
        world.snaps[f'{base}/tank/data'] = list(OLD)
        world.snaps[f'{base}/tank/vol'] = list(OLD)
    return world


@pytest.fixture
def world(mock_subprocess):
    return build_world(LocalWorld(mock_subprocess))


def add_second_target(world):
    """A second target `bk2` holding one copy of ours."""
    world.add('bk2')
    world.add('bk2/A', [(PROP_CLIENT_ID, OWN, 'local')])
    world.add('bk2/A/tank', [(PROP_CLIENT_ID, OWN, 'local')])
    world.add('bk2/A/tank/data', copy_props(OWN))
    world.snaps['bk2/A/tank/data'] = list(OLD)


def own(*tail, owner=OWN):
    return [f'{TARGET}/{owner}/{t}' for t in tail]


def config(*targets, extra_destinations=None, datasets=None, **kw):
    """A config with one local destination per target (`usb`, `usb2`, ...), the first named `usb`."""
    destinations = {}
    for i, target in enumerate(targets):
        destinations['usb' if i == 0 else f'usb{i + 1}'] = Destination(url=f'zfs:{target}')
    destinations.update(extra_destinations or {})
    if datasets is None:
        datasets = [DatasetConfig(
            name='tank/data', remote=[RemoteDatasetConfig(destination=d) for d in destinations]
        )]
    return BackupConfig(datasets=datasets, destinations=destinations, snapshot_prefix='autosnap', **kw)


def manager(*targets, **kw):
    return DatasetManager(config(*(targets or (TARGET,)), **kw))


def names(found):
    return sorted(d.name for d in found)


def warnings_of(caplog, containing=''):
    return records(caplog, logging.WARNING, containing)


# ---------------------------------------------------------------------------
# (a) The discovery and its scope
# ---------------------------------------------------------------------------


class TestDiscoveryScope:
    """Only `<target>/<own id>` and what lies under it: not listed, not read, not destroyed otherwise."""

    def test_the_datasets_of_our_tree_are_found_filesystems_and_volumes(self, world):
        found = manager().local_copies(OWN)
        assert names(found) == own('tank/data', 'tank/vol')
        by_name = {d.name: d for d in found}
        assert isinstance(by_name[f'{TARGET}/A/tank/vol'].dataset, Volume)

    def test_the_container_and_the_intermediate_parent_are_not_copies(self, world, caplog):
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            found = manager().local_copies(OWN)
        assert f'{TARGET}/A' not in names(found) and f'{TARGET}/A/tank' not in names(found)
        assert TARGET not in names(found)
        assert warnings_of(caplog) == []                       # quietly: they are stamped containers

    def test_the_listing_and_the_batch_are_rooted_at_the_own_id(self, world):
        manager().local_copies(OWN)
        (listing,) = world.listings
        assert listing[-1] == f'{TARGET}/A'
        (batch,) = world.batches
        assert batch[-1] == f'{TARGET}/A'
        assert '-r' in batch
        assert parse_argv(batch)[2] == 'filesystem,volume'

    def test_the_batch_asks_for_the_client_id_and_the_config_only_and_never_for_the_received_column(self, world):
        manager().local_copies(OWN)
        (batch,) = world.batches
        assert batch[batch.index('-o') + 1] == 'name,property,value,source'
        assert batch[-2] == f'name,type,{PROP_CLIENT_ID},{PROP_CONFIG}'
        assert '-s' not in batch

    @pytest.mark.parametrize('own_id, other', [(OWN, NEIGHBOUR), (NEIGHBOUR, OWN), (OWN, STRANGER)])
    def test_another_hosts_tree_is_never_listed_read_or_destroyed(self, world, own_id, other):
        """`A` against its prefix-sibling `Ax`, the reverse, and a stranger: the other tree appears in no
        argv at all, while ours is pruned (the live control)."""
        mgr = manager()
        found = mgr.local_copies(own_id)
        for dsi in found:
            mgr.prune_snapshots(dsi)
        assert names(found) == own('tank/data', 'tank/vol', owner=own_id)
        assert world.mentions(f'{TARGET}/{other}') == []
        assert [a for a in world.argvs if a[-1] == TARGET] == []                    # not even the target
        assert world.destroyed, 'the control: our own copies are pruned'
        assert all(d.startswith(f'{TARGET}/{own_id}/') for d in world.destroyed)
        assert sorted(world.snaps[f'{TARGET}/{other}/tank/data']) == OLD            # untouched

    @pytest.mark.parametrize('own_id, other', [(OWN, NEIGHBOUR), (NEIGHBOUR, OWN)])
    def test_a_copy_stamped_with_the_neighbours_id_inside_our_tree_is_refused(self, world, caplog, own_id, other):
        world.set_props(f'{TARGET}/{own_id}/tank/data', copy_props(other))
        mgr = manager()
        found = mgr.local_copies(own_id)
        assert names(found) == own('tank/vol', owner=own_id)
        (warning,) = warnings_of(caplog, f'{TARGET}/{own_id}/tank/data')
        assert f"is not this host's id {own_id!r}" in warning and 'never pruned from here' in warning
        for dsi in found:
            mgr.prune_snapshots(dsi)
        assert not [d for d in world.destroyed if d.startswith(f'{TARGET}/{own_id}/tank/data@')]

    def test_a_copy_without_a_local_client_id_is_refused_with_a_warning(self, world, caplog):
        world.set_props(f'{TARGET}/A/tank/data', [(PROP_CONFIG, COPY_CONFIG, 'local')])
        found = manager().local_copies(OWN)
        assert names(found) == own('tank/vol')
        (warning,) = warnings_of(caplog, 'bk/A/tank/data')
        assert f'{PROP_CLIENT_ID} is not set locally' in warning

    def test_an_inherited_client_id_is_not_local(self, world, caplog):
        world.set_props(
            f'{TARGET}/A/tank/data',
            [(PROP_CLIENT_ID, OWN, 'inherited from bk/A'), (PROP_CONFIG, COPY_CONFIG, 'local')],
        )
        assert names(manager().local_copies(OWN)) == own('tank/vol')
        assert any(f'{PROP_CLIENT_ID} is not set locally' in w for w in warnings_of(caplog, 'bk/A/tank/data'))

    def test_a_dataset_with_neither_property_is_quietly_not_a_copy(self, world, caplog):
        world.set_props(f'{TARGET}/A/tank/data', [])
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            found = manager().local_copies(OWN)
        assert names(found) == own('tank/vol')
        assert warnings_of(caplog) == []

    def test_a_received_only_config_is_refused_with_a_warning(self, world, caplog):
        world.set_props(f'{TARGET}/A/tank/data', [(PROP_CONFIG, COPY_CONFIG, 'received')])
        found = manager().local_copies(OWN)
        assert names(found) == own('tank/vol')
        (warning,) = warnings_of(caplog, 'bk/A/tank/data')
        assert 'only received' in warning and 'never trusted' in warning

    def test_a_config_that_does_not_decode_is_skipped_with_a_warning(self, world, caplog):
        world.set_props(f'{TARGET}/A/tank/data', copy_props(OWN, config='this is not a config'))
        found = manager().local_copies(OWN)
        assert names(found) == own('tank/vol')
        (warning,) = warnings_of(caplog, 'bk/A/tank/data')
        assert 'does not decode' in warning

    def test_a_config_with_a_zero_duration_is_skipped_with_a_warning(self, world, caplog):
        document = json.loads(base64.b64decode(COPY_CONFIG))
        document['retention_rules'][0]['age'] = 0.0                  # a division by zero in the pruner
        zero = base64.b64encode(json.dumps(document).encode()).decode()
        assert zero != COPY_CONFIG
        world.set_props(f'{TARGET}/A/tank/data', copy_props(OWN, config=zero))
        found = manager().local_copies(OWN)
        assert names(found) == own('tank/vol')
        (warning,) = warnings_of(caplog, 'bk/A/tank/data')
        assert 'is unusable' in warning

    def test_a_dataset_stamped_with_our_id_outside_our_tree_is_never_even_listed(self, world, caplog):
        """Stamped with our id but living outside `bk/A/`: only a path under `<target>/<id>` is ours."""
        world.add('bk/A2', copy_props(OWN))                    # not under bk/A; also not listed from bk/A
        assert 'bk/A2' not in names(manager().local_copies(OWN))
        assert world.mentions('bk/A2') == []

    def test_the_copy_is_pruned_non_recursively_whatever_its_config_says(self, world):
        mgr = manager()
        found = mgr.local_copies(OWN)
        assert DatasetConfig.from_property(COPY_CONFIG).recursive is True       # the stamped config says -r
        assert [d.recursive for d in found] == [False, False]
        for dsi in found:
            mgr.prune_snapshots(dsi)
        assert world.destroy_argvs and all('-r' not in a for a in world.destroy_argvs)

    def test_a_pass_keeps_the_newest_snapshot_and_removes_the_rest(self, world):
        mgr = manager()
        found = {d.name: d for d in mgr.local_copies(OWN)}
        result = mgr.prune_snapshots(found[f'{TARGET}/A/tank/data'])
        assert result.ok and result.pruned == len(OLD) - 1
        assert world.snaps[f'{TARGET}/A/tank/data'] == [OLD[-1]]
        assert sorted(d.split('@')[1] for d in world.destroyed) == sorted(OLD[:-1])

    def test_a_copy_of_a_dataset_that_is_no_longer_configured_is_still_pruned(self, world):
        """The source left the configuration, the copies stay on the target and keep their retention."""
        world.add('bk/A/gone', copy_props(OWN, source='gone/data'))
        world.snaps['bk/A/gone'] = list(OLD)
        mgr = manager(datasets=[DatasetConfig(name='other/data')])
        found = mgr.local_copies(OWN)
        assert 'bk/A/gone' in names(found)
        assert 'gone/data' not in [d.name for d in mgr.datasets]
        mgr.prune_snapshots(next(d for d in found if d.name == 'bk/A/gone'))
        assert world.snaps['bk/A/gone'] == [OLD[-1]]

    def test_a_skipped_copy_is_warned_about_once_then_debug_and_the_memory_is_forgotten(self, world, caplog):
        world.set_props(f'{TARGET}/A/tank/data', [(PROP_CONFIG, COPY_CONFIG, 'received')])
        mgr = manager()
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            mgr.local_copies(OWN)
            mgr.local_copies(OWN)
            assert len(warnings_of(caplog, 'bk/A/tank/data')) == 1
            assert records(caplog, logging.DEBUG, 'Skipping local copy')
            world.set_props(f'{TARGET}/A/tank/data', copy_props(OWN))      # fixed
            assert 'bk/A/tank/data' in names(mgr.local_copies(OWN))
            world.set_props(f'{TARGET}/A/tank/data', [(PROP_CONFIG, COPY_CONFIG, 'received')])
            mgr.local_copies(OWN)
        assert len(warnings_of(caplog, 'bk/A/tank/data')) == 2             # a new episode

    def test_the_local_wording_names_a_local_copy_and_the_servers_does_not_change(self, world, caplog):
        world.set_props(f'{TARGET}/A/tank/data', [(PROP_CONFIG, COPY_CONFIG, 'received')])
        manager().local_copies(OWN)
        assert warnings_of(caplog, "Skipping local copy 'bk/A/tank/data'")
        caplog.clear()
        server = DatasetManager(BackupConfig(
            datasets=[DatasetConfig(name='tank/data')], snapshot_prefix='autosnap',
            remote_backup=RemoteServerConfig(target_dataset=TARGET, enabled=True),
        ))
        server.received_datasets()
        assert warnings_of(caplog, "Skipping received dataset 'bk/A/tank/data'")


class TestWhichTargets:
    """Local destinations only, once per distinct target, in configuration order."""

    def test_no_client_id_discovers_nothing_and_runs_nothing(self, world):
        mgr = manager()
        assert mgr.local_copies(None) == []
        assert world.argvs == [] and mgr.last_local_discovery == []

    @pytest.mark.parametrize('bad', ['', '-x', 'a b', 'a/b', 'A\nB', '.'])
    def test_an_invalid_client_id_discovers_nothing_with_a_warning(self, world, caplog, bad):
        mgr = manager()
        assert mgr.local_copies(bad) == []
        assert world.argvs == [] and mgr.last_local_discovery == []
        assert warnings_of(caplog, 'is invalid')

    def test_remote_destinations_are_not_touched(self, world):
        mgr = DatasetManager(config(TARGET, extra_destinations={'off': Destination(url='https://h.example:8443')}))
        mgr.local_copies(OWN)
        assert [d.destination for d in mgr.last_local_discovery] == ['usb']
        assert all('h.example' not in ' '.join(a) for a in world.argvs)

    def test_a_config_with_only_remote_destinations_runs_nothing(self, world):
        mgr = DatasetManager(BackupConfig(
            datasets=[DatasetConfig(name='tank/data')],
            destinations={'off': Destination(url='https://h.example:8443')},
        ))
        assert mgr.local_copies(OWN) == [] and world.argvs == [] and mgr.last_local_discovery == []

    def test_every_local_target_is_discovered_in_order_with_its_own_outcome(self, world):
        add_second_target(world)
        mgr = manager(TARGET, 'bk2')
        found = mgr.local_copies(OWN)
        assert names(found) == own('tank/data', 'tank/vol') + ['bk2/A/tank/data']
        assert mgr.last_local_discovery == [
            LocalDiscovery('usb', TARGET, 2, None), LocalDiscovery('usb2', 'bk2', 1, None),
        ]
        assert [a[-1] for a in world.listings] == ['bk/A', 'bk2/A']

    def test_two_destinations_on_one_target_list_it_once_and_do_not_duplicate_copies(self, world):
        mgr = manager(TARGET, TARGET)
        found = mgr.local_copies(OWN)
        assert names(found) == own('tank/data', 'tank/vol')
        assert len(world.listings) == 1
        assert mgr.last_local_discovery == [LocalDiscovery('usb', TARGET, 2, None)]

    def test_a_failing_target_costs_only_its_own_copies(self, world):
        add_second_target(world)
        world.fail(lambda a: a[0] == 'list' and a[-1] == 'bk/A', 'cannot open: pool I/O is currently suspended')
        mgr = manager(TARGET, 'bk2')
        found = mgr.local_copies(OWN)
        assert names(found) == ['bk2/A/tank/data']
        first, second = mgr.last_local_discovery
        assert (first.destination, first.target, first.count) == ('usb', TARGET, 0)
        assert first.error and 'pool I/O is currently suspended' in first.error
        assert second == LocalDiscovery('usb2', 'bk2', 1, None)

    def test_the_outcomes_are_rebuilt_on_every_call(self, world):
        mgr = manager()
        mgr.local_copies(OWN)
        assert len(mgr.last_local_discovery) == 1
        mgr.local_copies(None)
        assert mgr.last_local_discovery == []

    def test_a_stop_before_a_target_lists_nothing(self, world):
        mgr = manager()
        assert mgr.local_copies(OWN, should_stop=lambda: True) == []
        assert world.argvs == [] and mgr.last_local_discovery == []

    def test_a_stop_during_the_enumeration_returns_what_is_safe_and_records_no_outcome(self, world):
        mgr = manager()
        stop = {'now': False}
        world.on_exec = lambda argv: stop.update(now=True) if argv[0] == 'list' else None
        assert mgr.local_copies(OWN, should_stop=lambda: stop['now']) == []
        assert mgr.last_local_discovery == []

    def test_a_stop_after_the_first_target_discards_the_rest_without_touching_it(self, world):
        """The stop arrives while the first target's batch runs: that target is dropped as a cut-short
        enumeration and the second target is never asked about."""
        add_second_target(world)
        mgr = manager(TARGET, 'bk2')
        stop = {'now': False}
        world.on_exec = lambda argv: stop.update(now=True) if world.is_batch(argv) else None
        found = mgr.local_copies(OWN, should_stop=lambda: stop['now'])
        assert found == []
        assert all(not a[-1].startswith('bk2') for a in world.argvs)


# ---------------------------------------------------------------------------
# (a) Forged bytes: mirrors test_zfsbackup_prune_forged_bytes.py, rooted at our id
# ---------------------------------------------------------------------------


def contained(world, own_id, found):
    """The invariants of every hostile scenario: nothing but our own tree is returned, read or destroyed."""
    for other in {OWN, NEIGHBOUR, STRANGER} - {own_id}:
        assert world.mentions(f'{TARGET}/{other}') == [], other
    for dsi in found:
        assert dsi.name.startswith(f'{TARGET}/{own_id}/'), dsi.name
    for full in world.destroyed:
        assert full.startswith(f'{TARGET}/{own_id}/'), full
    assert [a for a in world.argvs if a[-1] == TARGET] == []


OWN_TREE = {'bk/A', 'bk/A/tank', 'bk/A/tank/data', 'bk/A/tank/vol'}


class TestForgedBytes:
    """A hostile value (a newline in a property) can forge rows of the one batched read. The batch is then
    untrusted whole and every dataset is read exactly, one `get -o value` each -- a row for a name outside
    our tree is never believed, and the neighbour's tree never appears in a command."""

    @pytest.mark.parametrize('own_id', [OWN, NEIGHBOUR])
    def test_a_newline_in_a_config_forging_a_neighbours_row_skips_that_dataset_only(self, world, own_id, caplog):
        other = NEIGHBOUR if own_id == OWN else OWN
        forged = COPY_CONFIG + f'\n{TARGET}/{other}/tank/data\t{PROP_CLIENT_ID}\t{own_id}\tlocal'
        world.set_props(f'{TARGET}/{own_id}/tank/data', copy_props(own_id, config=forged))
        mgr = manager()
        found = mgr.local_copies(own_id)
        assert names(found) == own('tank/vol', owner=own_id)
        for dsi in found:
            mgr.prune_snapshots(dsi)
        contained(world, own_id, found)
        assert warnings_of(caplog, 'reading the datasets one at a time')
        assert any('does not decode' in w for w in warnings_of(caplog, f'{TARGET}/{own_id}/tank/data'))
        assert not [d for d in world.destroyed if d.startswith(f'{TARGET}/{own_id}/tank/data@')]
        assert world.destroyed, 'the control: the honest copy is still pruned'

    def test_a_row_injected_into_the_batch_naming_the_neighbour_is_not_believed(self, world, caplog):
        world.mutate = lambda lines: lines + [f'{TARGET}/Ax/tank/data\t{PROP_CLIENT_ID}\t{OWN}\tlocal\n']
        found = manager().local_copies(OWN)
        assert names(found) == own('tank/data', 'tank/vol')              # the honest result, via the exact path
        assert {a[-1] for a in world.values} == OWN_TREE
        assert world.mentions('bk/Ax') == []
        assert any('name outside the requested objects' in w for w in caplog.messages)
        assert warnings_of(caplog, 'reading the datasets one at a time')

    def test_a_row_for_a_stranger_outside_the_root_is_not_believed(self, world):
        world.mutate = lambda lines: lines + [f'bk/B/tank/data\t{PROP_CLIENT_ID}\t{OWN}\tlocal\n']
        found = manager().local_copies(OWN)
        assert names(found) == own('tank/data', 'tank/vol')
        assert world.mentions('bk/B') == []

    def test_a_forged_row_that_would_make_the_neighbours_dataset_ours_changes_nothing(self, world):
        """The two rows that, if believed, would hand `bk/Ax/tank/data` to us with the neighbour's config."""
        world.mutate = lambda lines: lines + [
            f'{TARGET}/Ax/tank/data\t{PROP_CONFIG}\t{COPY_CONFIG}\tlocal\n',
            f'{TARGET}/Ax/tank/data\t{PROP_CLIENT_ID}\t{OWN}\tlocal\n',
        ]
        mgr = manager()
        found = mgr.local_copies(OWN)
        for dsi in found:
            mgr.prune_snapshots(dsi)
        assert names(found) == own('tank/data', 'tank/vol')
        contained(world, OWN, found)
        assert sorted(world.snaps['bk/Ax/tank/data']) == OLD

    def test_a_batch_that_exits_nonzero_falls_back_to_exact_reads(self, world, caplog):
        world.fail(lambda a: world.is_batch(a), 'internal error: out of memory')
        found = manager().local_copies(OWN)
        assert names(found) == own('tank/data', 'tank/vol')
        assert warnings_of(caplog, 'Cannot read the properties of bk/A in one call')
        assert {a[-1] for a in world.values} == OWN_TREE

    def test_a_dataset_the_batch_dropped_is_read_exactly_and_only_that_one(self, world):
        world.omit_from_batch = {'bk/A/tank/vol'}
        found = manager().local_copies(OWN)
        assert names(found) == own('tank/data', 'tank/vol')
        assert {a[-1] for a in world.values} == {'bk/A/tank/vol'} and len(world.values) == 2

    def test_a_duplicated_group_makes_the_batch_untrusted_whole(self, world, caplog):
        world.mutate = lambda lines: lines + [ln for ln in lines if ln.startswith('bk/A/tank/data\t')]
        found = manager().local_copies(OWN)
        assert names(found) == own('tank/data', 'tank/vol')
        assert any('are not contiguous' in m for m in caplog.messages)
        assert {a[-1] for a in world.values} == OWN_TREE
        assert world.mentions('bk/Ax') == []

    def test_a_client_id_with_a_newline_forging_a_row_is_refused(self, world, caplog):
        forged = f'{OWN}\nbk/A/tank/vol\t{PROP_CLIENT_ID}\t{OWN}\tlocal'
        world.set_props('bk/A/tank/data', [(PROP_CLIENT_ID, forged, 'local'), (PROP_CONFIG, COPY_CONFIG, 'local')])
        mgr = manager()
        found = mgr.local_copies(OWN)
        assert names(found) == own('tank/vol')
        for dsi in found:
            mgr.prune_snapshots(dsi)
        assert any('is invalid' in w for w in warnings_of(caplog, 'bk/A/tank/data'))
        assert not [d for d in world.destroyed if d.startswith('bk/A/tank/data@')]
        contained(world, OWN, found)

    @pytest.mark.parametrize('own_id', [OWN, NEIGHBOUR])
    def test_the_neighbours_snapshots_are_never_destroyed_whatever_is_forged(self, world, own_id):
        other = NEIGHBOUR if own_id == OWN else OWN
        world.mutate = lambda lines: lines + [
            f'{TARGET}/{other}/tank/data\t{PROP_CONFIG}\t{COPY_CONFIG}\tlocal\n',
            f'{TARGET}/{other}/tank/data\t{PROP_CLIENT_ID}\t{own_id}\tlocal\n',
        ]
        mgr = manager()
        found = mgr.local_copies(own_id)
        for dsi in found:
            mgr.prune_snapshots(dsi)
        contained(world, own_id, found)
        assert world.destroyed, 'the control: our own copies are pruned'
        assert sorted(world.snaps[f'{TARGET}/{other}/tank/data']) == OLD


# ---------------------------------------------------------------------------
# (b) Missing datasets, timeouts and the budget
# ---------------------------------------------------------------------------


class TestMissingDatasets:
    def test_a_missing_own_container_is_no_copies_and_no_error(self, world, caplog):
        mgr = DatasetManager(config(TARGET))
        for gone in ('bk/A/tank/data', 'bk/A/tank/vol', 'bk/A/tank', 'bk/A'):
            world.datasets.pop(gone)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            assert mgr.local_copies(OWN) == []
        assert mgr.last_local_discovery == [LocalDiscovery('usb', TARGET, 0, None)]
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
        # the listing, whether `bk/A` itself exists (it does not), whether the target does
        assert [a[-1] for a in world.argvs if a[0] == 'list'] == ['bk/A', 'bk/A', 'bk']

    def test_a_missing_target_is_an_error(self, mock_subprocess, caplog):
        world = LocalWorld(mock_subprocess)                    # an exported pool reads the same
        mgr = manager()
        assert mgr.local_copies(OWN) == []
        assert mgr.last_local_discovery == [
            LocalDiscovery('usb', TARGET, 0, f'the target dataset {TARGET} does not exist')
        ]
        (warning,) = warnings_of(caplog, 'Cannot discover the local copies')
        assert 'under bk (destination usb)' in warning and 'does not exist' in warning
        assert 'they are not pruned until this passes' in warning

    def test_a_missing_target_is_still_an_error_after_the_container_appeared_and_went(self, world):
        mgr = manager()
        assert mgr.local_copies(OWN)
        world.datasets.clear()
        mgr.local_copies(OWN)
        assert mgr.last_local_discovery[0].error is not None and mgr.last_local_discovery[0].count == 0

    def test_a_missing_own_container_does_not_swallow_other_listing_errors(self, world):
        world.fail(lambda a: a[0] == 'list' and a[-1] == 'bk/A', 'cannot open: permission denied')
        mgr = manager()
        assert mgr.local_copies(OWN) == []
        assert 'permission denied' in mgr.last_local_discovery[0].error

    def test_a_timeout_while_checking_the_target_is_an_error(self, world):
        world.datasets.pop('bk/A/tank/data'), world.datasets.pop('bk/A/tank/vol')
        for gone in ('bk/A/tank', 'bk/A'):
            world.datasets.pop(gone)

        def on(argv):
            if argv[0] == 'list' and argv[-1] == TARGET:
                raise ZFSTimeoutError('zfs list bk timed out after 60 s')

        world.on_exec = on
        mgr = manager()
        assert mgr.local_copies(OWN) == []
        assert mgr.last_local_discovery == [LocalDiscovery('usb', TARGET, 0, 'ZFSTimeoutError: zfs list bk timed out after 60 s')]


class Hang:
    """Make selected `zfs` commands raise `ZFSTimeoutError` while `active`."""

    def __init__(self, world, predicate):
        self.active = True
        self.world = world
        self.predicate = predicate
        world.on_exec = self

    def __call__(self, argv):
        if self.active and self.predicate(argv):
            raise ZFSTimeoutError(f"zfs {' '.join(argv[:2])} timed out after 60 s")


def is_list(argv):
    return argv[0] == 'list' and '-r' in argv


def is_batch(argv):
    return LocalWorld.is_batch(argv)


def is_value(argv):
    return argv[0] == 'get' and argv[argv.index('-o') + 1] == 'value'


def is_destroy(argv):
    return argv[0] == 'destroy'


def is_snapshot_list(argv):
    return argv[0] == 'list' and '-t' in argv and argv[argv.index('-t') + 1] == 'snapshot'


def is_anchor_read(argv):
    return argv[0] == 'get' and argv[argv.index('-o') + 1] == 'name,property,source'


class TestTimeouts:
    def test_a_timeout_on_the_listing_is_an_error_with_no_datasets(self, world):
        Hang(world, is_list)
        mgr = manager()
        assert mgr.local_copies(OWN) == []
        (outcome,) = mgr.last_local_discovery
        assert outcome.count == 0 and outcome.error == 'ZFSTimeoutError: zfs list -H timed out after 60 s'
        assert world.batches == [] and world.values == []

    def test_a_timeout_on_the_batch_is_an_error_and_does_not_fall_back_to_per_dataset_reads(self, world):
        Hang(world, is_batch)
        mgr = manager()
        assert mgr.local_copies(OWN) == []
        (outcome,) = mgr.last_local_discovery
        assert outcome.error.startswith('ZFSTimeoutError: ')
        assert world.values == [], '60 s per dataset against a pool that does not answer'

    def test_a_timeout_on_a_per_dataset_read_is_an_error_and_no_datasets(self, world):
        """The batch fails (so each dataset is read), the second read hangs: nothing is returned, not
        even the dataset read before it."""
        world.fail(is_batch, 'internal error')
        seen = []

        def on(argv):
            if is_value(argv):
                seen.append(argv[-1])
                if len(seen) > 2:
                    raise ZFSTimeoutError('zfs get timed out after 60 s')

        world.on_exec = on
        mgr = manager()
        assert mgr.local_copies(OWN) == []
        (outcome,) = mgr.last_local_discovery
        assert outcome.count == 0 and outcome.error == 'ZFSTimeoutError: zfs get timed out after 60 s'
        assert len(set(seen)) >= 2, 'some datasets were read before the one that hung'

    def test_a_non_timeout_failure_of_the_listing_is_an_error_too(self, world):
        world.fail(is_list, 'cannot open: pool I/O is currently suspended')
        mgr = manager()
        assert mgr.local_copies(OWN) == []
        assert 'pool I/O is currently suspended' in mgr.last_local_discovery[0].error

    def test_the_episode_is_a_warning_once_then_debug_then_info_on_recovery(self, world, caplog):
        hang = Hang(world, is_list)
        mgr = manager()
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            mgr.local_copies(OWN)
            mgr.local_copies(OWN)
            mgr.local_copies(OWN)
            (warning,) = warnings_of(caplog, 'Cannot discover the local copies')
            assert 'under bk (destination usb): ZFSTimeoutError: zfs list -H timed out after 60 s;' in warning
            assert len(records(caplog, logging.DEBUG, 'Still cannot discover the local copies under bk')) == 2
            assert records(caplog, logging.INFO, 'can be discovered again') == []
            hang.active = False
            assert names(mgr.local_copies(OWN)) == own('tank/data', 'tank/vol')
            assert records(caplog, logging.INFO, 'The local copies under bk can be discovered again') != []
            mgr.local_copies(OWN)
            assert len(records(caplog, logging.INFO, 'can be discovered again')) == 1
            hang.active = True
            mgr.local_copies(OWN)
        assert len(warnings_of(caplog, 'Cannot discover the local copies')) == 2        # a new episode

    def test_the_episode_memory_is_per_target(self, world, caplog):
        add_second_target(world)
        Hang(world, lambda a: is_list(a) and a[-1] == 'bk/A')
        mgr = manager(TARGET, 'bk2')
        mgr.local_copies(OWN)
        mgr.local_copies(OWN)
        assert len(warnings_of(caplog, 'Cannot discover the local copies')) == 1
        assert [d.error is None for d in mgr.last_local_discovery] == [False, True]

    def test_a_removed_target_forgets_its_episode(self, world, caplog):
        world.add('bk2')
        hang = Hang(world, is_list)
        mgr = manager(TARGET)
        mgr.local_copies(OWN)
        mgr.config.destinations.pop('usb')
        mgr.config.destinations['other'] = Destination(url='zfs:bk2')
        mgr.local_copies(OWN)
        assert mgr._local_error_noted.keys() <= {'bk2'}
        assert TARGET not in mgr._local_skip_noted
        hang.active = False
        mgr.config.destinations['other'] = Destination(url=f'zfs:{TARGET}')
        mgr.local_copies(OWN)
        assert not records(caplog, logging.INFO, 'can be discovered again')        # not an old episode ending


class TestCommandsRunUnderTheTimeout:
    def test_every_command_of_a_clean_discovery_is_timed_at_the_command_budget(self, world):
        manager().local_copies(OWN)
        assert world.argvs and len(world.timeouts) == len(world.argvs)
        assert set(world.timeouts) == {LOCAL_DISCOVER_COMMAND_BUDGET} == {60.0}

    def test_the_per_dataset_fallback_is_timed_too(self, world):
        world.fail(is_batch, 'internal error')
        manager().local_copies(OWN)
        assert world.values and set(world.timeouts) == {60.0}

    def test_the_target_existence_check_is_timed_too(self, world):
        for gone in ('bk/A/tank/data', 'bk/A/tank/vol', 'bk/A/tank', 'bk/A'):
            world.datasets.pop(gone)
        manager().local_copies(OWN)
        assert [a[-1] for a in world.argvs if a[0] == 'list'] == ['bk/A', 'bk/A', 'bk']
        assert set(world.timeouts) == {60.0}

    def test_the_servers_discovery_is_not_timed(self, world):
        server = DatasetManager(BackupConfig(
            datasets=[DatasetConfig(name='tank/data')], snapshot_prefix='autosnap',
            remote_backup=RemoteServerConfig(target_dataset=TARGET, enabled=True),
        ))
        assert server.received_datasets()
        assert world.argvs and set(world.timeouts) == {None}

    def test_the_timeout_is_lifted_after_the_discovery(self, world):
        manager().local_copies(OWN)
        assert _timeout_var.get() is None

    def test_a_timed_out_child_that_is_still_stuck_fails_fast_before_any_command(self, world, monkeypatch):
        class Alive:
            pid = 4242
            stdout = stderr = None

            def poll(self):
                return None

        stuck = zfs_module._StuckChild(Alive(), None, 'zfs list bk/A', owns_stdout=False)
        monkeypatch.setattr(zfs_module, '_stuck', [stuck])
        assert libzfseasy.stuck_children()
        mgr = manager()
        assert mgr.local_copies(OWN) == []
        (outcome,) = mgr.last_local_discovery
        assert outcome.count == 0 and outcome.error.startswith('ZFSTimeoutError: ') and 'not started' in outcome.error
        assert world.argvs == [], 'refused before any child was started'

    def test_the_servers_untimed_discovery_is_never_refused_by_a_stuck_child(self, world, monkeypatch):
        class Alive:
            pid = 4242
            stdout = stderr = None

            def poll(self):
                return None

        monkeypatch.setattr(zfs_module, '_stuck', [zfs_module._StuckChild(Alive(), None, 'zfs list', owns_stdout=False)])
        server = DatasetManager(BackupConfig(
            datasets=[DatasetConfig(name='tank/data')], snapshot_prefix='autosnap',
            remote_backup=RemoteServerConfig(target_dataset=TARGET, enabled=True),
        ))
        assert names(server.received_datasets()) == own('tank/data', 'tank/vol') + own(
            'tank/data', 'tank/vol', owner=NEIGHBOUR) + own('tank/data', 'tank/vol', owner=STRANGER)
        assert server.last_received_error is None


class TestTheOverallBudget:
    def test_the_constants(self):
        assert LOCAL_DISCOVER_COMMAND_BUDGET == 60.0 and LOCAL_DISCOVER_BUDGET == 600.0

    def test_the_command_budget_equals_the_local_managers(self):
        pytest.importorskip('zfsbackup.local')
        from zfsbackup import local
        assert local.LOCAL_ZFS_BUDGET == LOCAL_DISCOVER_COMMAND_BUDGET

    def test_the_budget_shrinks_the_command_limit_near_the_deadline_and_then_runs_out(self, world, mocker):
        """100 s in all, 30 s per command on a fake monotonic clock: 60 s, 60 s, then what is left (40 s),
        then the budget is spent and the discovery ends in an error without another command."""
        clock = FakeTime()
        clock.frozen = 1000.0
        mocker.patch.object(bm, 'time', clock)
        mocker.patch.object(bm, 'LOCAL_DISCOVER_BUDGET', 100.0)
        world.fail(is_batch, 'internal error')                  # so the datasets are read one by one
        world.on_exec = lambda argv: clock.advance(30.0)
        mgr = manager()
        assert mgr.local_copies(OWN) == []
        (outcome,) = mgr.last_local_discovery
        assert outcome.count == 0
        assert outcome.error == 'ZFSTimeoutError: the 100 s budget for discovering local copies is spent'
        # list (left 100) and batch (left 70) at the command budget, then the first dataset's two reads
        # (left 40) at what is left
        assert world.timeouts[:2] == [60.0, 60.0]
        assert world.timeouts[2:] == [40.0, 40.0]
        assert len(world.argvs) == 4, 'nothing runs after the budget is spent'

    def test_a_spent_budget_before_the_first_command_runs_nothing(self, world, mocker):
        clock = FakeTime()
        clock.frozen = 1000.0
        mocker.patch.object(bm, 'time', clock)
        mocker.patch.object(bm, 'LOCAL_DISCOVER_BUDGET', 0.0)
        mgr = manager()
        assert mgr.local_copies(OWN) == []
        assert 'budget' in mgr.last_local_discovery[0].error
        assert world.argvs == []

    def test_each_target_gets_its_own_budget(self, world, mocker):
        add_second_target(world)
        clock = FakeTime()
        clock.frozen = 1000.0
        mocker.patch.object(bm, 'time', clock)
        mocker.patch.object(bm, 'LOCAL_DISCOVER_BUDGET', 100.0)
        world.on_exec = lambda argv: clock.advance(30.0)        # 2 commands per target: within 100 s
        mgr = manager(TARGET, 'bk2')
        found = mgr.local_copies(OWN)
        assert [d.error for d in mgr.last_local_discovery] == [None, None]
        assert 'bk2/A/tank/data' in names(found)

    def test_the_budget_error_is_one_warning_per_episode(self, world, mocker, caplog):
        clock = FakeTime()
        clock.frozen = 0.0
        mocker.patch.object(bm, 'time', clock)
        mocker.patch.object(bm, 'LOCAL_DISCOVER_BUDGET', 0.0)
        mgr = manager()
        mgr.local_copies(OWN)
        mgr.local_copies(OWN)
        assert len(warnings_of(caplog, 'Cannot discover the local copies')) == 1


class TestServerDiscoveryIsUnchanged:
    """`received_datasets` after the refactor: untimed, the same error text, the same log, the same attribute."""

    def server(self):
        return DatasetManager(BackupConfig(
            datasets=[DatasetConfig(name='tank/data')], snapshot_prefix='autosnap',
            remote_backup=RemoteServerConfig(target_dataset=TARGET, enabled=True),
        ))

    def test_it_lists_the_whole_target_not_one_id(self, world):
        found = self.server().received_datasets()
        assert len(found) == 6
        (listing,) = world.listings
        assert listing[-1] == TARGET
        (batch,) = world.batches
        assert batch[-1] == TARGET

    def test_a_missing_target_is_the_old_error_and_log(self, mock_subprocess, caplog):
        LocalWorld(mock_subprocess)
        server = self.server()
        assert server.received_datasets() == []
        assert server.last_received_error == "Exception: Cannot open 'bk': dataset does not exist"
        (message,) = records(caplog, logging.ERROR, 'Failed to discover received datasets')
        assert message == "Failed to discover received datasets: Exception(\"Cannot open 'bk': dataset does not exist\")"

    def test_a_timeout_is_reported_like_any_other_failure(self, world, caplog):
        Hang(world, is_list)
        server = self.server()
        assert server.received_datasets() == []
        assert server.last_received_error == 'ZFSTimeoutError: zfs list -H timed out after 60 s'
        (message,) = records(caplog, logging.ERROR, 'Failed to discover received datasets')
        assert message == "Failed to discover received datasets: ZFSTimeoutError('zfs list -H timed out after 60 s')"
        assert server.last_local_discovery == []

    def test_a_timeout_on_the_batch_still_falls_back_for_the_server(self, world):
        """The server keeps its old behaviour: a failed batch (a timeout included) is read dataset by dataset."""
        Hang(world, is_batch)
        found = self.server().received_datasets()
        assert len(found) == 6 and world.values

    def test_it_never_fills_the_local_discovery_state(self, world):
        server = self.server()
        server.received_datasets()
        assert server.last_local_discovery == [] and server._local_skip_noted == {} and server._local_error_noted == {}

    def test_the_server_and_the_local_skip_memories_are_separate(self, world, caplog):
        world.set_props('bk/A/tank/data', [(PROP_CONFIG, COPY_CONFIG, 'received')])
        mgr = DatasetManager(BackupConfig(
            datasets=[DatasetConfig(name='tank/data')], snapshot_prefix='autosnap',
            destinations={'usb': Destination(url=f'zfs:{TARGET}')},
            remote_backup=RemoteServerConfig(target_dataset=TARGET, enabled=True),
        ))
        mgr.received_datasets()
        mgr.local_copies(OWN)
        assert len(warnings_of(caplog, "Skipping received dataset 'bk/A/tank/data'")) == 1
        assert len(warnings_of(caplog, "Skipping local copy 'bk/A/tank/data'")) == 1


# ---------------------------------------------------------------------------
# The rest of the discovery's contract: unused destinations, skips, failure classes
# ---------------------------------------------------------------------------


class TestUnusedDestinations:
    """A local destination no enabled dataset sends to: its existing copies are still ours to prune, but a
    target that was never created is no failing discovery every cycle."""

    UNUSED = dict(datasets=[DatasetConfig(name='tank/data')])

    def test_an_unused_destination_with_a_missing_target_is_silent_and_leaves_no_outcome(self, mock_subprocess, caplog):
        LocalWorld(mock_subprocess)
        mgr = manager(**self.UNUSED)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            assert mgr.local_copies(OWN) == []
        assert mgr.last_local_discovery == []
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
        assert records(caplog, logging.DEBUG, 'is not used by any dataset')

    def test_an_unused_destination_whose_target_exists_is_still_discovered(self, world):
        mgr = manager(**self.UNUSED)
        assert names(mgr.local_copies(OWN)) == own('tank/data', 'tank/vol')
        assert mgr.last_local_discovery == [LocalDiscovery('usb', TARGET, 2, None)]

    def test_a_used_destination_with_a_missing_target_is_an_error(self, mock_subprocess):
        LocalWorld(mock_subprocess)
        mgr = manager()
        mgr.local_copies(OWN)
        assert mgr.last_local_discovery[0].error is not None

    def test_a_disabled_dataset_does_not_use_the_destination(self, mock_subprocess):
        LocalWorld(mock_subprocess)
        mgr = manager(datasets=[DatasetConfig(
            name='tank/data', enabled=False, remote=[RemoteDatasetConfig(destination='usb')])])
        mgr.local_copies(OWN)
        assert mgr.last_local_discovery == []

    def test_an_unused_destination_that_fails_for_another_reason_is_still_an_error(self, world):
        Hang(world, is_list)
        mgr = manager(**self.UNUSED)
        mgr.local_copies(OWN)
        assert mgr.last_local_discovery[0].error.startswith('ZFSTimeoutError')

    def test_an_unused_missing_target_forgets_an_old_episode(self, mock_subprocess, caplog):
        world = build_world(LocalWorld(mock_subprocess))
        hang = Hang(world, is_list)
        mgr = manager(**self.UNUSED)
        mgr.local_copies(OWN)                                   # an episode starts (a timeout)
        world.datasets.clear()
        hang.active = False
        mgr.local_copies(OWN)                                   # now the target is gone and unused: quiet
        assert mgr._local_error_noted == {}
        world.datasets.update(build_world(LocalWorld(mock_subprocess)).datasets)
        assert not records(caplog, logging.INFO, 'can be discovered again')


class TestSkippedDiscovery:
    """`last_local_skip`: why nothing was discovered for the WHOLE configuration."""

    def test_no_client_id_says_so(self, world):
        mgr = manager()
        mgr.local_copies(None)
        assert mgr.last_local_skip == 'this host has no client id'

    def test_an_invalid_id_says_so_and_warns_once_per_manager(self, world, caplog):
        mgr = manager()
        for _ in range(3):
            assert mgr.local_copies('bad id') == []
        assert mgr.last_local_skip.startswith("the client id 'bad id' is invalid")
        assert len(warnings_of(caplog, 'the client id')) == 1
        mgr.local_copies('other bad')
        assert len(warnings_of(caplog, 'the client id')) == 2           # a different id: its own warning

    def test_a_good_call_clears_it(self, world):
        mgr = manager()
        mgr.local_copies(None)
        mgr.local_copies(OWN)
        assert mgr.last_local_skip is None

    def test_a_normal_discovery_has_none(self, world):
        mgr = manager()
        mgr.local_copies(OWN)
        assert mgr.last_local_skip is None


class TestFailureClasses:
    """The once-per-episode WARNING is keyed on the CLASS of the failure, not its text (the text carries
    seconds and pids), so a hung target does not re-announce itself with every changed message."""

    def test_a_changing_message_of_the_same_class_is_one_episode(self, world, caplog):
        counter = [0]

        def on(argv):
            if is_list(argv):
                counter[0] += 1
                raise ZFSTimeoutError(f'zfs list timed out after {60 + counter[0]} s')

        world.on_exec = on
        mgr = manager()
        for _ in range(3):
            mgr.local_copies(OWN)
        assert len(warnings_of(caplog, 'Cannot discover the local copies')) == 1

    def test_a_different_class_is_a_new_episode(self, world, caplog):
        hang = Hang(world, is_list)
        mgr = manager()
        mgr.local_copies(OWN)
        hang.active = False
        world.fail(is_list, 'cannot open: permission denied')            # an ordinary failure now
        mgr.local_copies(OWN)
        assert len(warnings_of(caplog, 'Cannot discover the local copies')) == 2

    def test_a_stuck_child_is_its_own_class_and_the_message_says_what_is_hung(self, world, caplog, monkeypatch):
        hang = Hang(world, is_list)
        mgr = manager()
        mgr.local_copies(OWN)                                           # a plain timeout first
        hang.active = False

        class Alive:
            pid = 4242
            stdout = stderr = None

            def poll(self):
                return None

        monkeypatch.setattr(zfs_module, '_stuck', [zfs_module._StuckChild(Alive(), None, 'zfs destroy bk/A/x@s', owns_stdout=False)])
        mgr.local_copies(OWN)
        warnings = warnings_of(caplog, 'Cannot discover the local copies')
        assert len(warnings) == 2
        assert 'zfs is not answering: `zfs destroy bk/A/x@s` (pid 4242) timed out' in warnings[1]
        assert 'refused at once' in warnings[1]
        assert 'zfs is not answering' in mgr.last_local_discovery[0].error

    def test_a_plain_timeout_does_not_mention_a_stuck_child(self, world):
        Hang(world, is_list)
        mgr = manager()
        mgr.local_copies(OWN)
        assert 'not answering' not in mgr.last_local_discovery[0].error


# ---------------------------------------------------------------------------
# Pruning a local copy runs under timeouts; a source dataset never does
# ---------------------------------------------------------------------------


class TestLocalCopyPruneTimeouts:
    """A target on a suspended pool may still answer a listing while `zfs destroy` sits in D state. A COPY's
    pass runs every command under `libzfseasy.timeout` (60 s; a destroy 300 s) and a timeout skips the rest of
    that target's copies for the cycle; a SOURCE dataset is untimed as always."""

    def copies(self, world, mgr=None, **kw):
        mgr = mgr or manager(**kw)
        found = {d.name: d for d in mgr.local_copies(OWN)}
        return mgr, found

    def test_the_copies_know_their_target_and_the_others_do_not(self, world):
        mgr, found = self.copies(world)
        assert {d.local_target for d in found.values()} == {TARGET}
        assert all(d.local_target is None for d in mgr.datasets)
        server = DatasetManager(BackupConfig(
            datasets=[DatasetConfig(name='tank/data')], snapshot_prefix='autosnap',
            remote_backup=RemoteServerConfig(target_dataset=TARGET, enabled=True)))
        assert {d.local_target for d in server.received_datasets()} == {None}

    def test_the_commands_of_a_copys_pass_are_timed_and_the_destroy_has_the_longer_budget(self, world):
        mgr, found = self.copies(world)
        start = len(world.argvs)
        result = mgr.prune_snapshots(found['bk/A/tank/data'])
        assert result.ok and result.pruned == len(OLD) - 1
        verbs = [(a[0], t) for a, t in zip(world.argvs[start:], world.timeouts[start:])]
        assert verbs and all(t == (300.0 if v == 'destroy' else 60.0) for v, t in verbs), verbs
        assert any(v == 'destroy' for v, _ in verbs)

    def test_a_source_dataset_is_pruned_untimed(self, world):
        world.add('tank/data')
        world.snaps['tank/data'] = list(OLD)
        retention = [RetentionRule(timedelta(hours=1), timedelta(hours=1))]
        mgr = DatasetManager(config(TARGET, datasets=[DatasetConfig(name='tank/data', retention_rules=retention)]))
        (source,) = mgr.datasets
        result = mgr.prune_snapshots(source)
        assert result.pruned == len(OLD) - 1
        assert world.timeouts and set(world.timeouts) == {None}

    def test_the_timeout_scope_does_not_leak_out_of_the_pass(self, world):
        mgr, found = self.copies(world)
        mgr.prune_snapshots(found['bk/A/tank/data'])
        assert _timeout_var.get() is None and mgr._prune_timed is False

    def test_a_destroy_that_times_out_skips_the_pass_and_is_not_retried_one_by_one(self, world):
        mgr, found = self.copies(world)
        Hang(world, is_destroy)
        result = mgr.prune_snapshots(found['bk/A/tank/data'])
        assert not result.ok and "did not answer while pruning bk/A/tank/data" in result.skipped
        assert len(world.destroy_argvs) == 1, 'a hung pool is not tried snapshot by snapshot'
        assert mgr._prune_stuck == {}

    def test_the_rest_of_that_targets_copies_are_skipped_without_a_zfs_call(self, world, caplog):
        mgr, found = self.copies(world)
        Hang(world, is_destroy)
        mgr.prune_snapshots(found['bk/A/tank/data'])
        before = len(world.argvs)
        other = mgr.prune_snapshots(found['bk/A/tank/vol'])
        assert len(world.argvs) == before, 'no zfs call for the second copy'
        assert other.skipped == 'the local target bk did not answer earlier in this cycle'

    def test_a_new_cycle_re_arms_the_target_and_a_recovery_is_announced(self, world, caplog):
        mgr, found = self.copies(world)
        hang = Hang(world, is_destroy)
        mgr.prune_snapshots(found['bk/A/tank/data'])
        hang.active = False
        found = {d.name: d for d in mgr.local_copies(OWN)}                    # the next cycle
        with caplog.at_level(logging.INFO, logger='zfsbackup.backup_manager'):
            result = mgr.prune_snapshots(found['bk/A/tank/data'])
        assert result.ok and result.pruned == len(OLD) - 1
        assert records(caplog, logging.INFO, 'The local copies under bk can be pruned again')

    def test_one_warning_per_episode_then_debug(self, world, caplog):
        mgr, found = self.copies(world)
        Hang(world, is_destroy)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            for _ in range(3):
                mgr.prune_snapshots(found['bk/A/tank/data'])
                found = {d.name: d for d in mgr.local_copies(OWN)}                 # a new cycle each time
        assert len(warnings_of(caplog, 'Not pruning the local copies under bk')) == 1
        assert len(records(caplog, logging.DEBUG, 'Still not pruning the local copies under bk')) == 2

    def test_a_timeout_listing_the_snapshots_skips_the_pass_and_destroys_nothing(self, world):
        mgr, found = self.copies(world)
        Hang(world, is_snapshot_list)
        result = mgr.prune_snapshots(found['bk/A/tank/data'])
        assert 'did not answer while pruning' in result.skipped and world.destroyed == []

    def test_a_timeout_reading_the_anchors_never_shrinks_the_protect_set(self, world):
        """An unreadable anchor must be "unknown, do not act", not "unset"."""
        mgr, found = self.copies(world)
        Hang(world, is_anchor_read)
        result = mgr.prune_snapshots(found['bk/A/tank/data'])
        assert 'did not answer while pruning' in result.skipped
        assert world.destroyed == []

    def test_only_the_hung_target_is_blocked(self, world):
        add_second_target(world)
        mgr = manager(TARGET, 'bk2')
        found = {d.name: d for d in mgr.local_copies(OWN)}
        Hang(world, lambda a: is_destroy(a) and a[-1].startswith('bk/'))
        assert mgr.prune_snapshots(found['bk/A/tank/data']).skipped
        result = mgr.prune_snapshots(found['bk2/A/tank/data'])
        assert result.ok and result.pruned == len(OLD) - 1

    def test_a_stuck_child_fails_a_copy_fast_but_never_a_source_dataset(self, world, monkeypatch):
        world.add('tank/data')
        world.snaps['tank/data'] = list(OLD)
        retention = [RetentionRule(timedelta(hours=1), timedelta(hours=1))]
        mgr = DatasetManager(config(TARGET, datasets=[DatasetConfig(name='tank/data', retention_rules=retention)]))
        found = {d.name: d for d in mgr.local_copies(OWN)}

        class Alive:
            pid = 4242
            stdout = stderr = None

            def poll(self):
                return None

        monkeypatch.setattr(zfs_module, '_stuck', [zfs_module._StuckChild(Alive(), None, 'zfs destroy x', owns_stdout=False)])
        before = len(world.argvs)
        result = mgr.prune_snapshots(found['bk/A/tank/data'])
        assert 'did not answer while pruning' in result.skipped and len(world.argvs) == before
        (source,) = mgr.datasets
        assert mgr.prune_snapshots(source).pruned == len(OLD) - 1               # untimed: never refused

    def test_the_stuck_class_is_announced_apart_from_a_plain_timeout(self, world, caplog, monkeypatch):
        mgr, found = self.copies(world)
        hang = Hang(world, is_destroy)
        mgr.prune_snapshots(found['bk/A/tank/data'])
        hang.active = False

        class Alive:
            pid = 4242
            stdout = stderr = None

            def poll(self):
                return None

        monkeypatch.setattr(zfs_module, '_stuck', [zfs_module._StuckChild(Alive(), None, 'zfs destroy y', owns_stdout=False)])
        found = {d.name: d for d in mgr.local_copies(OWN)}              # discovery itself is refused now
        assert found == {}
        assert len(warnings_of(caplog, 'Not pruning the local copies under bk')) == 1
        assert len(warnings_of(caplog, 'Cannot discover the local copies under bk')) == 1

    def test_the_pruning_workers_record_of_a_skipped_copy_is_an_error(self, world, sdir, mocker):
        Hang(world, is_destroy)
        worker = pruning_worker(sdir)
        run_cycles(worker, mocker)
        recs = statuses(sdir)
        assert recs[('prune', 'bk/A/tank/data', None)].result == 'error'
        assert 'did not answer while pruning' in recs[('prune', 'bk/A/tank/data', None)].last_error
        assert recs[('prune', 'bk/A/tank/vol', None)].last_error == 'the local target bk did not answer earlier in this cycle'
        assert recs[('discover', TARGET, 'usb')].result == 'ok'


class TestEveryTimeoutSiteOfACopysPass:
    """Each place a copy's pass can meet a `ZFSTimeoutError` re-raises it into a `skipped` result: never a
    smaller protect-set, never the B-4 one-by-one retry, never `_prune_stuck`."""

    def setup_pass(self, world):
        mgr = manager()
        found = {d.name: d for d in mgr.local_copies(OWN)}
        return mgr, found['bk/A/tank/data']

    def assert_skipped_and_untouched(self, mgr, result, world):
        assert not result.ok and 'did not answer while pruning bk/A/tank/data' in result.skipped
        assert mgr._prune_stuck == {}
        assert world.snaps['bk/A/tank/data'] == OLD or world.destroyed == []

    def test_the_single_property_read_raises_a_timeout_only_inside_a_copys_pass(self, world):
        """`_get_prop` (behind `get_anchor`/`get_inflight`): "could not read" is "unset" for the senders and
        the source datasets, but inside a copy's pass a timeout must propagate, never read as unset."""
        mgr, dsi = self.setup_pass(world)
        Hang(world, is_value)
        assert mgr.get_anchor(dsi, 'usb') is None                     # outside a pass: as before
        assert mgr.get_inflight(dsi, 'usb') is None
        mgr._prune_timed = True
        with pytest.raises(ZFSTimeoutError):
            mgr.get_anchor(dsi, 'usb')
        with pytest.raises(ZFSTimeoutError):
            mgr.get_inflight(dsi, 'usb')

    def test_the_snapshot_listing_is_debug_not_the_failed_to_list_error(self, world, caplog):
        mgr, dsi = self.setup_pass(world)
        Hang(world, is_snapshot_list)
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            result = mgr.prune_snapshots(dsi)
        self.assert_skipped_and_untouched(mgr, result, world)
        assert not records(caplog, logging.ERROR, 'Failed to list snapshots')
        assert records(caplog, logging.DEBUG, 'Failed to list snapshots for bk/A/tank/data')

    def test_a_source_datasets_listing_failure_keeps_its_error_and_its_skipped_wording(self, world):
        world.add('tank/data')
        world.snaps['tank/data'] = list(OLD)
        mgr = DatasetManager(config(TARGET, datasets=[DatasetConfig(name='tank/data')]))
        (source,) = mgr.datasets

        def boom(argv):
            if is_snapshot_list(argv):
                raise RuntimeError('pool is suspended')

        world.on_exec = boom
        result = mgr.prune_snapshots(source)
        assert result.skipped == 'cannot list snapshots: RuntimeError: pool is suspended'

    def test_the_anchor_read(self, world):
        mgr, dsi = self.setup_pass(world)
        Hang(world, is_anchor_read)
        self.assert_skipped_and_untouched(mgr, mgr.prune_snapshots(dsi), world)
        assert world.destroyed == []

    def test_the_protection_re_read_right_before_a_destroy(self, world):
        mgr, dsi = self.setup_pass(world)
        reads = []

        def on(argv):
            if is_anchor_read(argv):
                reads.append(argv)
                if len(reads) == 2:               # the first read decided; the re-read hangs
                    raise ZFSTimeoutError('zfs get timed out after 60 s')

        world.on_exec = on
        result = mgr.prune_snapshots(dsi)
        self.assert_skipped_and_untouched(mgr, result, world)
        assert world.destroyed == [] and len(reads) == 2

    def test_a_batch_destroy(self, world):
        mgr, dsi = self.setup_pass(world)
        Hang(world, is_destroy)
        result = mgr.prune_snapshots(dsi)
        self.assert_skipped_and_untouched(mgr, result, world)
        assert len(world.destroy_argvs) == 1

    def test_a_single_destroy_after_a_failed_batch(self, world):
        mgr, dsi = self.setup_pass(world)
        calls = []

        def on(argv):
            if is_destroy(argv):
                calls.append(argv[-1])
                if ',' in argv[-1]:
                    raise RuntimeError('dataset is busy')                  # the batch fails: retry alone
                raise ZFSTimeoutError('zfs destroy timed out after 300 s')

        world.on_exec = on
        result = mgr.prune_snapshots(dsi)
        self.assert_skipped_and_untouched(mgr, result, world)
        assert len(calls) == 2 and ',' in calls[0] and ',' not in calls[1], 'the first single destroy hung: stop'

    def test_the_re_list_that_explains_a_failed_destroy(self, world):
        mgr, dsi = self.setup_pass(world)
        state = {'destroy_failed': False}

        def on(argv):
            if is_destroy(argv):
                state['destroy_failed'] = True
                raise RuntimeError('dataset is busy')
            if is_snapshot_list(argv) and state['destroy_failed']:
                raise ZFSTimeoutError('zfs list timed out after 60 s')

        world.on_exec = on
        result = mgr.prune_snapshots(dsi)
        self.assert_skipped_and_untouched(mgr, result, world)
        assert world.destroyed == []

    def test_the_same_failures_without_a_timeout_still_use_the_old_paths(self, world):
        """A plain failure of a copy's destroy is the B-4 fallback as before: batch, then one by one."""
        mgr, dsi = self.setup_pass(world)

        def on(argv):
            if is_destroy(argv):
                raise RuntimeError('dataset is busy')

        world.on_exec = on
        result = mgr.prune_snapshots(dsi)
        assert result.failed == len(OLD) - 1 and 'did not answer' not in (result.skipped or '')
        assert len(world.destroy_argvs) == 1 + (len(OLD) - 1)


class TestAMissingContainerIsAskedNotRead:
    def test_a_listing_failure_whose_text_says_does_not_exist_is_an_error_when_the_container_exists(self, world):
        """A dataset NAME may contain "dataset does not exist"; only `zfs list` of `<target>/<id>` itself says."""
        world.fail(lambda a: is_list(a) and a[-1] == 'bk/A', "cannot open 'bk/A': dataset does not exist")
        mgr = manager()
        assert mgr.local_copies(OWN) == []
        (outcome,) = mgr.last_local_discovery
        assert outcome.error and 'dataset does not exist' in outcome.error
        assert [a[-1] for a in world.argvs if a[0] == 'list'] == ['bk/A', 'bk/A']

    def test_the_existence_of_the_container_is_checked_before_the_targets(self, world):
        for gone in ('bk/A/tank/data', 'bk/A/tank/vol', 'bk/A/tank', 'bk/A'):
            world.datasets.pop(gone)
        mgr = manager()
        mgr.local_copies(OWN)
        assert [a[-1] for a in world.argvs if a[0] == 'list'] == ['bk/A', 'bk/A', 'bk']
        assert mgr.last_local_discovery == [LocalDiscovery('usb', TARGET, 0, None)]

    def test_a_failure_checking_the_container_is_the_error(self, world):
        for gone in ('bk/A/tank/data', 'bk/A/tank/vol', 'bk/A/tank', 'bk/A'):
            world.datasets.pop(gone)
        seen = []

        def on(argv):
            if argv[0] == 'list':
                seen.append(argv[-1])
                if len(seen) == 2:
                    raise ZFSTimeoutError('zfs list timed out after 60 s')

        world.on_exec = on
        mgr = manager()
        mgr.local_copies(OWN)
        assert mgr.last_local_discovery[0].error == 'ZFSTimeoutError: zfs list timed out after 60 s'


# ---------------------------------------------------------------------------
# (c) The pruning worker
# ---------------------------------------------------------------------------


@pytest.fixture
def sdir(tmp_path):
    (tmp_path / 'config.db').write_bytes(b'')
    directory = tmp_path / 'config.db.status'
    ensure_status_dir(directory)
    return directory


def pruning_worker(sdir=None, cfg=None, client_id=OWN, stop=None, **kw):
    return PruningWorker(cfg or config(TARGET), stop or StopFlag(), status_dir=sdir, client_id=client_id, **kw)


def run_cycles(worker, mocker, cycles=1):
    """`worker.run()` over a REAL `DatasetManager` for `cycles` cycles, then a stop."""
    left = [cycles - 1]

    def wait(interval):
        if left[0] > 0:
            left[0] -= 1
            return None
        return worker._exit_reason() or ExitReason.STOP

    mocker.patch.object(worker, '_wait', side_effect=wait)
    worker.run()
    return worker


def statuses(sdir):
    st = read_worker_status(sdir, 'pruning-worker')
    return {} if st is None else {r.key: r for r in st.records}


def local_manager_mock(copies=(), discoveries=(), destinations=None):
    """A `DatasetManager` mock with local destinations, for the `_get_datasets` unit tests."""
    mgr = MagicMock(name='manager')
    mgr.datasets = []
    mgr.received_datasets.return_value = []
    mgr.config.remote_backup = None
    mgr.config.destinations = destinations if destinations is not None else {
        'usb': Destination(url=f'zfs:{TARGET}')}
    mgr.local_copies.return_value = list(copies)
    mgr.last_local_discovery = list(discoveries)
    mgr.last_local_skip = None
    return mgr


def dsi(name):
    return SimpleNamespace(name=name, recursive=False)


class TestPruningWorkerGetDatasets:
    def test_the_copies_are_appended_after_the_configured_and_received_datasets(self):
        mgr = local_manager_mock(copies=[dsi('bk/A/x')])
        mgr.datasets = [dsi('tank/data')]
        mgr.received_datasets.return_value = [dsi('srv/c/y')]
        worker = pruning_worker()
        assert [d.name for d in worker._get_datasets(mgr)] == ['tank/data', 'srv/c/y', 'bk/A/x']
        mgr.local_copies.assert_called_once()
        (cid,), kwargs = mgr.local_copies.call_args
        assert cid == OWN and kwargs['should_stop'] == worker._should_stop

    def test_a_copy_already_in_the_list_is_dropped(self):
        mgr = local_manager_mock(copies=[dsi('tank/data'), dsi('srv/c/y'), dsi('bk/A/x')])
        mgr.datasets = [dsi('tank/data')]
        mgr.received_datasets.return_value = [dsi('srv/c/y')]
        names_ = [d.name for d in pruning_worker()._get_datasets(mgr)]
        assert names_ == ['tank/data', 'srv/c/y', 'bk/A/x']

    def test_nothing_is_asked_without_a_local_destination(self):
        mgr = local_manager_mock(destinations={'off': Destination(url='https://h.example:8443')})
        mgr.datasets = [dsi('tank/data')]
        assert [d.name for d in pruning_worker()._get_datasets(mgr)] == ['tank/data']
        mgr.local_copies.assert_not_called()

    def test_nothing_is_asked_without_any_destination(self):
        mgr = local_manager_mock(destinations={})
        pruning_worker()._get_datasets(mgr)
        mgr.local_copies.assert_not_called()

    def test_a_manager_without_local_copies_is_tolerated(self):
        mgr = local_manager_mock()
        del mgr.local_copies
        mgr.datasets = [dsi('tank/data')]
        assert [d.name for d in pruning_worker()._get_datasets(mgr)] == ['tank/data']

    def test_no_client_id_means_no_copies_and_one_warning_per_incarnation(self, caplog):
        mgr = local_manager_mock(copies=[dsi('bk/A/x')])
        worker = pruning_worker(client_id=None)
        with caplog.at_level(logging.WARNING, logger='zfsbackup.workers'):
            for _ in range(3):
                assert worker._get_datasets(mgr) == []
        mgr.local_copies.assert_not_called()
        assert len(records(caplog, logging.WARNING, 'has no client id', 'zfsbackup.workers')) == 1
        assert 'identity set-client-id' in caplog.text
        assert pruning_worker(client_id=None)._no_client_id_warned is False        # per incarnation

    def test_no_client_id_keeps_the_records_under_every_local_target(self, sdir):
        """Not knowing what exists is not "nothing exists": the `discover` record and the copies' records
        under the local targets stay; whatever else is gone from the configuration goes as usual."""
        seed = StatusWriter(sdir, 'pruning-worker')
        seed.record('discover', TARGET, 'usb', ok=True, detail='3 local copies')
        seed.record('prune', 'bk/A/gone', ok=False, error='old')
        seed.record('prune', 'pool/gone', ok=False, error='old')
        seed.flush(force=True)
        worker = pruning_worker(sdir, client_id=None)
        worker._status = StatusWriter(sdir, 'pruning-worker')
        worker._status.load()
        datasets = worker._get_datasets(local_manager_mock())
        assert worker._status_keys(datasets) == {(TARGET, 'usb'), ('bk/A/gone', None)}

    def test_a_raising_local_copies_is_handled_logged_and_keeps_the_old_records(self, caplog):
        mgr = local_manager_mock()
        mgr.datasets = [dsi('tank/data')]
        mgr.local_copies.side_effect = RuntimeError('boom')
        worker = pruning_worker()
        with caplog.at_level(logging.ERROR, logger='zfsbackup.workers'):
            result = worker._get_datasets(mgr)
        assert [d.name for d in result] == ['tank/data']
        (message,) = records(caplog, logging.ERROR, 'could not discover the local copies', 'zfsbackup.workers')
        assert "RuntimeError('boom')" in message
        assert caplog.records[-1].exc_info is not None
        assert worker._local_unknown == (TARGET,)                   # the records under bk are kept

    def test_a_stop_during_the_discovery_returns_nothing_to_prune(self):
        stop = StopFlag()
        mgr = local_manager_mock(copies=[dsi('bk/A/x')])
        mgr.datasets = [dsi('tank/data')]

        def found(cid, should_stop=None):
            stop.set()
            return [dsi('bk/A/x')]

        mgr.local_copies.side_effect = found
        assert pruning_worker(stop=stop)._get_datasets(mgr) == []

    def test_a_stop_before_the_discovery_returns_nothing_and_asks_nothing(self):
        stop = StopFlag()
        stop.set()
        mgr = local_manager_mock(copies=[dsi('bk/A/x')])
        assert pruning_worker(stop=stop)._get_datasets(mgr) == []
        mgr.local_copies.assert_not_called()


class TestDiscoverRecords:
    """One `discover` record per local target, under the destination's name."""

    def cycle(self, worker, mgr):
        worker._status = StatusWriter(worker._status_dir, 'pruning-worker')
        worker._status.load()
        datasets = worker._get_datasets(mgr)
        keep = worker._status_keys(datasets)
        if keep is not None:
            worker._status.retain(keep)
        worker._status.flush(force=True)
        return datasets

    def test_an_ok_record_carries_the_count(self, sdir):
        mgr = local_manager_mock(
            copies=[dsi('bk/A/x'), dsi('bk/A/y')], discoveries=[LocalDiscovery('usb', TARGET, 2, None)])
        self.cycle(pruning_worker(sdir), mgr)
        record = statuses(sdir)[('discover', TARGET, 'usb')]
        assert record.result == 'ok' and record.detail == '2 local copies'

    def test_the_count_wording_is_singular_for_one(self, sdir):
        mgr = local_manager_mock(copies=[dsi('bk/A/x')], discoveries=[LocalDiscovery('usb', TARGET, 1, None)])
        self.cycle(pruning_worker(sdir), mgr)
        assert statuses(sdir)[('discover', TARGET, 'usb')].detail == '1 local copy'

    def test_a_failed_target_is_an_error_record(self, sdir):
        mgr = local_manager_mock(discoveries=[LocalDiscovery('usb', TARGET, 0, 'ZFSTimeoutError: zfs list timed out')])
        self.cycle(pruning_worker(sdir), mgr)
        record = statuses(sdir)[('discover', TARGET, 'usb')]
        assert record.result == 'error' and record.consecutive_failures == 1
        assert record.last_error == 'cannot list the local copies: ZFSTimeoutError: zfs list timed out'

    def test_one_record_per_target_and_destination_and_failures_accumulate(self, sdir):
        discoveries = [
            LocalDiscovery('usb', TARGET, 1, None), LocalDiscovery('nas', 'bk2', 0, 'boom'),
        ]
        mgr = local_manager_mock(
            copies=[dsi('bk/A/x')], discoveries=discoveries,
            destinations={'usb': Destination(url='zfs:bk'), 'nas': Destination(url='zfs:bk2')})
        worker = pruning_worker(sdir)
        for _ in range(3):
            self.cycle(worker, mgr)
        recs = statuses(sdir)
        assert {k for k in recs if k[0] == 'discover'} == {('discover', TARGET, 'usb'), ('discover', 'bk2', 'nas')}
        assert recs[('discover', 'bk2', 'nas')].consecutive_failures == 3
        assert recs[('discover', TARGET, 'usb')].consecutive_failures == 0

    def test_a_failed_target_keeps_its_own_records_and_the_rest_is_retained_as_usual(self, sdir):
        seed = StatusWriter(sdir, 'pruning-worker')
        seed.record('prune', 'bk/A/tank/data', ok=True, detail='pruned 3')
        seed.record('discover', TARGET, 'usb', ok=True)
        seed.record('prune', 'bk2/A/tank/data', ok=False, error='old')       # no longer configured
        seed.record('prune', 'pool/gone', ok=False, error='old')
        seed.flush(force=True)
        discoveries = [LocalDiscovery('usb', TARGET, 0, 'boom')]
        mgr = local_manager_mock(discoveries=discoveries)
        self.cycle(pruning_worker(sdir), mgr)
        recs = statuses(sdir)
        assert ('prune', 'bk/A/tank/data', None) in recs, 'the target could not be listed: not the truth'
        assert recs[('discover', TARGET, 'usb')].result == 'error'
        assert ('prune', 'bk2/A/tank/data', None) not in recs and ('prune', 'pool/gone', None) not in recs

    def test_a_target_name_is_matched_by_path_not_by_prefix(self, sdir):
        """`bk2/...` is not under the unknown target `bk`."""
        seed = StatusWriter(sdir, 'pruning-worker')
        seed.record('prune', 'bk2/A/x', ok=True)
        seed.record('prune', 'bk/A/x', ok=True)
        seed.flush(force=True)
        mgr = local_manager_mock(discoveries=[LocalDiscovery('usb', TARGET, 0, 'boom')])
        self.cycle(pruning_worker(sdir), mgr)
        recs = statuses(sdir)
        assert ('prune', 'bk/A/x', None) in recs and ('prune', 'bk2/A/x', None) not in recs

    def test_a_good_target_is_retained_normally_while_another_is_unknown(self, sdir):
        seed = StatusWriter(sdir, 'pruning-worker')
        seed.record('prune', 'bk/A/old', ok=True)
        seed.record('prune', 'bk2/A/old', ok=True)
        seed.flush(force=True)
        mgr = local_manager_mock(
            copies=[dsi('bk2/A/new')],
            discoveries=[LocalDiscovery('usb', TARGET, 0, 'boom'), LocalDiscovery('nas', 'bk2', 1, None)],
            destinations={'usb': Destination(url='zfs:bk'), 'nas': Destination(url='zfs:bk2')})
        self.cycle(pruning_worker(sdir), mgr)
        recs = statuses(sdir)
        assert ('prune', 'bk/A/old', None) in recs                    # bk is unknown: kept
        assert ('prune', 'bk2/A/old', None) not in recs               # bk2 listed fine: gone is gone

    def test_a_skipped_discovery_keeps_the_records_under_every_target(self, sdir):
        """`last_local_skip` (an invalid client id, say): nothing was discovered for the whole configuration."""
        seed = StatusWriter(sdir, 'pruning-worker')
        seed.record('prune', 'bk/A/old', ok=True)
        seed.record('prune', 'bk2/A/old', ok=True)
        seed.flush(force=True)
        mgr = local_manager_mock(
            destinations={'usb': Destination(url='zfs:bk'), 'nas': Destination(url='zfs:bk2')})
        mgr.last_local_skip = "the client id 'x y' is invalid (no spaces)"
        worker = pruning_worker(sdir)
        self.cycle(worker, mgr)
        assert {k for k in statuses(sdir)} == {('prune', 'bk/A/old', None), ('prune', 'bk2/A/old', None)}
        assert worker._local_unknown == (TARGET, 'bk2')

    def test_a_removed_local_destination_drops_its_record_after_a_good_cycle(self, sdir):
        seed = StatusWriter(sdir, 'pruning-worker')
        seed.record('discover', 'bk2', 'nas', ok=True)
        seed.record('discover', TARGET, 'usb', ok=True)
        seed.flush(force=True)
        mgr = local_manager_mock(discoveries=[LocalDiscovery('usb', TARGET, 0, None)])
        self.cycle(pruning_worker(sdir), mgr)
        recs = statuses(sdir)
        assert ('discover', 'bk2', 'nas') not in recs and ('discover', TARGET, 'usb') in recs

    def test_with_no_local_destination_left_every_local_record_is_dropped(self, sdir):
        seed = StatusWriter(sdir, 'pruning-worker')
        seed.record('discover', TARGET, 'usb', ok=True)
        seed.flush(force=True)
        mgr = local_manager_mock(destinations={'off': Destination(url='https://h.example:8443')})
        self.cycle(pruning_worker(sdir), mgr)
        assert statuses(sdir) == {}

    def test_it_coexists_with_the_servers_record(self, sdir):
        mgr = local_manager_mock(
            copies=[dsi('bk/A/x')], discoveries=[LocalDiscovery('usb', TARGET, 1, None)])
        mgr.config.remote_backup = SimpleNamespace(enabled=True, target_dataset='srv/backups/')
        mgr.received_datasets.return_value = [dsi('srv/backups/c/y')]
        mgr.last_received_error = None
        self.cycle(pruning_worker(sdir), mgr)
        recs = statuses(sdir)
        assert recs[('discover', 'srv/backups', None)].detail == '1 received dataset(s)'
        assert recs[('discover', TARGET, 'usb')].detail == '1 local copy'

    def test_a_failed_server_listing_still_keeps_the_local_records_alive(self, sdir):
        mgr = local_manager_mock(discoveries=[LocalDiscovery('usb', TARGET, 0, None)])
        mgr.config.remote_backup = SimpleNamespace(enabled=True, target_dataset='srv/backups')
        mgr.last_received_error = 'Exception: pool suspended'
        worker = pruning_worker(sdir)
        self.cycle(worker, mgr)
        recs = statuses(sdir)
        assert recs[('discover', 'srv/backups', None)].result == 'error'
        assert recs[('discover', TARGET, 'usb')].result == 'ok'

    def test_nothing_is_recorded_without_a_status_directory(self):
        mgr = local_manager_mock(discoveries=[LocalDiscovery('usb', TARGET, 0, None)])
        worker = pruning_worker()
        assert worker._get_datasets(mgr) == []                              # and no AttributeError

    def test_the_status_keys_name_each_local_target_and_destination(self):
        mgr = local_manager_mock(discoveries=[LocalDiscovery('usb', TARGET, 0, None)])
        worker = pruning_worker()
        datasets = worker._get_datasets(mgr)
        assert worker._status_keys(datasets) == {(TARGET, 'usb')}

    def test_a_discovery_cut_short_by_a_stop_records_nothing(self, sdir):
        stop = StopFlag()
        mgr = local_manager_mock(discoveries=[LocalDiscovery('usb', TARGET, 0, 'boom')])

        def found(cid, should_stop=None):
            stop.set()
            return []

        mgr.local_copies.side_effect = found
        worker = pruning_worker(sdir, stop=stop)
        worker._status = StatusWriter(sdir, 'pruning-worker')
        assert worker._get_datasets(mgr) == []
        worker._status.flush(force=True)
        assert statuses(sdir) == {}


class TestPruningWorkerEndToEnd:
    """The real `run()` loop over a real `DatasetManager` and the fake zfs."""

    def test_a_cycle_prunes_the_copies_records_them_and_leaves_the_neighbour_alone(self, world, sdir, mocker):
        run_cycles(pruning_worker(sdir), mocker)
        assert sorted(d for d in world.destroyed) and all(d.startswith('bk/A/') for d in world.destroyed)
        for tail in ('data', 'vol'):
            assert world.snaps[f'bk/A/tank/{tail}'] == [OLD[-1]]
            assert world.snaps[f'bk/Ax/tank/{tail}'] == OLD and world.snaps[f'bk/B/tank/{tail}'] == OLD
        recs = statuses(sdir)
        assert recs[('discover', TARGET, 'usb')].detail == '2 local copies'
        assert recs[('prune', 'bk/A/tank/data', None)].detail == f'pruned {len(OLD) - 1}'
        assert ('prune', 'bk/A/tank/vol', None) in recs
        assert not [k for k in recs if 'bk/Ax' in k[1] or 'bk/B' in k[1]]

    def test_the_configured_dataset_and_the_copies_are_each_pruned_once(self, world, sdir, mocker):
        world.add('tank/data')                                      # the source dataset: no snapshots
        run_cycles(pruning_worker(sdir), mocker)
        keys = [k for k in statuses(sdir) if k[0] == 'prune']
        assert sorted(keys) == [
            ('prune', 'bk/A/tank/data', None), ('prune', 'bk/A/tank/vol', None), ('prune', 'tank/data', None)]
        assert len([a for a in world.argvs if a[0] == 'destroy' and a[-1].startswith('bk/A/tank/data@')]) == 1

    def test_a_failed_target_prunes_nothing_there_and_keeps_its_records(self, world, sdir, mocker):
        seed = StatusWriter(sdir, 'pruning-worker')
        seed.record('prune', 'bk/A/tank/data', ok=True, detail='pruned 9')
        seed.record('prune', 'gone/dataset', ok=True)
        seed.flush(force=True)
        Hang(world, is_list)
        run_cycles(pruning_worker(sdir), mocker, cycles=2)
        recs = statuses(sdir)
        assert world.destroyed == []
        assert recs[('discover', TARGET, 'usb')].result == 'error'
        assert recs[('discover', TARGET, 'usb')].consecutive_failures == 2
        assert ('prune', 'bk/A/tank/data', None) in recs, 'the failed target is not the truth about what exists'
        assert ('prune', 'gone/dataset', None) not in recs, 'whatever else is retained as usual'

    def test_a_recovered_target_prunes_again_and_retains_normally(self, world, sdir, mocker):
        hang = Hang(world, is_list)
        worker = pruning_worker(sdir)
        left = [0]

        def wait(interval):
            if left[0] == 0:
                left[0] = 1
                hang.active = False
                return None
            return worker._exit_reason() or ExitReason.STOP

        mocker.patch.object(worker, '_wait', side_effect=wait)
        worker.run()
        recs = statuses(sdir)
        assert recs[('discover', TARGET, 'usb')].result == 'ok'
        assert recs[('discover', TARGET, 'usb')].consecutive_failures == 0
        assert world.snaps['bk/A/tank/data'] == [OLD[-1]]

    def test_a_removed_local_destination_drops_its_discover_record_but_not_the_others(self, world, sdir, mocker):
        world.add('bk2')
        first = pruning_worker(sdir, cfg=config(TARGET, 'bk2'))
        run_cycles(first, mocker)
        assert {k for k in statuses(sdir) if k[0] == 'discover'} == {('discover', TARGET, 'usb'), ('discover', 'bk2', 'usb2')}
        run_cycles(pruning_worker(sdir, cfg=config(TARGET)), mocker)
        assert {k for k in statuses(sdir) if k[0] == 'discover'} == {('discover', TARGET, 'usb')}

    def test_a_target_that_is_gone_is_a_failing_record_not_a_silent_skip(self, mock_subprocess, sdir, mocker):
        LocalWorld(mock_subprocess)
        run_cycles(pruning_worker(sdir), mocker)
        record = statuses(sdir)[('discover', TARGET, 'usb')]
        assert record.result == 'error' and 'does not exist' in record.last_error

    def test_a_stop_during_discovery_prunes_and_records_nothing(self, world, sdir, mocker):
        stop = StopFlag()
        world.on_exec = lambda argv: stop.set() if is_list(argv) else None
        run_cycles(pruning_worker(sdir, stop=stop), mocker)
        assert world.destroyed == []
        assert [k for k in statuses(sdir) if k[0] == 'discover'] == []

    def test_a_stuck_childs_account_reaches_the_status_file(self, world, sdir, mocker, monkeypatch):
        class Alive:
            pid = 4242
            stdout = stderr = None

            def poll(self):
                return None

        monkeypatch.setattr(zfs_module, '_stuck', [zfs_module._StuckChild(Alive(), None, 'zfs destroy bk/A/x@s', owns_stdout=False)])
        run_cycles(pruning_worker(sdir), mocker)
        record = statuses(sdir)[('discover', TARGET, 'usb')]
        assert record.result == 'error'
        assert 'zfs is not answering: `zfs destroy bk/A/x@s` (pid 4242)' in record.last_error
        assert world.destroyed == []

    def test_a_dry_run_discovers_but_destroys_and_records_nothing(self, world, sdir, mocker):
        cfg = config(TARGET, dry_run=True)
        run_cycles(pruning_worker(sdir, cfg=cfg), mocker)
        assert world.listings and world.destroyed == []
        assert statuses(sdir) == {}


# ---------------------------------------------------------------------------
# (c) `zfsbackup-config check` grades the records
# ---------------------------------------------------------------------------


class TestCheckGradesTheLocalDiscoverRecords:
    def entries(self, tmp_path, records_, now=None):
        db = tmp_path / 'config.db'
        db.write_bytes(b'')
        write_worker(db, 'pruning-worker', records_)
        result = health.read_status_dir(health.status_dir(db))
        workers = Workers(directory=str(health.status_dir(db)), present=True, statuses=tuple(result.statuses))
        out = worker_entries(health, workers, now or time.time(), 2.0, 4.0)
        return {(e.source, e.subject, e.peer): e for e in out}

    def test_one_entry_per_target_and_destination_each_graded_on_its_own(self, tmp_path):
        entries = self.entries(tmp_path, [
            status_rec('discover', 'bk', 'usb', failures=4, error='cannot list the local copies: boom'),
            status_rec('discover', 'bk2', 'nas', detail='3 local copies'),
            status_rec('discover', 'bk', 'spare', failures=2),
        ])
        assert set(entries) == {
            ('worker_discover', 'bk', 'usb'), ('worker_discover', 'bk2', 'nas'), ('worker_discover', 'bk', 'spare'),
        }
        assert entries[('worker_discover', 'bk', 'usb')].state == health.STATE_CRIT
        assert entries[('worker_discover', 'bk', 'spare')].state == health.STATE_WARN
        assert entries[('worker_discover', 'bk2', 'nas')].state == health.STATE_OK
        assert 'cannot list the local copies: boom' in entries[('worker_discover', 'bk', 'usb')].reason
        assert '(3 local copies)' in entries[('worker_discover', 'bk2', 'nas')].reason

    def test_they_sit_next_to_the_servers_record_for_the_same_name(self, tmp_path):
        entries = self.entries(tmp_path, [
            status_rec('discover', 'bk', failures=4), status_rec('discover', 'bk', 'usb'),
        ])
        assert set(entries) == {('worker_discover', 'bk', None), ('worker_discover', 'bk', 'usb')}
        assert entries[('worker_discover', 'bk', None)].state == health.STATE_CRIT
        assert entries[('worker_discover', 'bk', 'usb')].state == health.STATE_OK

    def test_a_real_workers_records_are_graded_after_enough_failed_cycles(self, world, sdir, mocker, tmp_path):
        """The records the real worker writes (not hand-built ones) reach CRIT at the 4th failed cycle."""
        Hang(world, is_list)
        run_cycles(pruning_worker(sdir), mocker, cycles=4)
        st = read_worker_status(sdir, 'pruning-worker')
        workers = Workers(directory=str(sdir), present=True, statuses=(st,))
        entries = {(e.source, e.subject, e.peer): e for e in worker_entries(health, workers, time.time(), 2.0, 4.0)}
        entry = entries[('worker_discover', TARGET, 'usb')]
        assert entry.state == health.STATE_CRIT and entry.consecutive_failures == 4
        assert 'ZFSTimeoutError' in entry.reason

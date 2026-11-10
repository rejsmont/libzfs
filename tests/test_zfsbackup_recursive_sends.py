"""Sender rework, Track A, item A0: characterization of two current gaps, as strict xfails.

Plan: `docs/sender_rework_plan.md` (Track A). Each test below states the DESIRED behaviour. A test
for an item that has not landed is marked `xfail(strict=True)`; when the named item lands it turns
into an XPASS, which strict mode makes a hard failure -- the fixing change then deletes the marker.
A1 has landed (its tests are plain, and `TestA1OwnAnchorMatrix` is its matrix); A2 has not.

* A1 (the inherited anchor; LANDED: the due check and `health_collect` now read `get_own_anchor` /
  `get_own_inflight`, `_record_step` / `_settle_up_to_date` the raising `get_local_anchor`, all
  `-s local`). Before it, `DatasetManager.get_anchor` read the EFFECTIVE value of
  `org.zfsbackup:anchor.<dest>` (`_get_prop` -> `zfs.get.value(ds, prop)`,
  zfsbackup/backup_manager.py:663-681, 712-715), so a dataset with no local anchor reports the
  one its parent holds. Two readers then misjudge a child that was never sent:
    - the remote worker's due check, `RemoteBackupWorker._send_to_destination`
      (zfsbackup/workers.py:1353-1370), finds the parent's anchor name among the child's own
      snapshots (a recursive snapshot gives both the same name) and says "not due yet";
    - `health_collect._observe_local` (zfsbackup/health_collect.py:145-150) grades the child as
      freshly sent.
* A2 (recursive sends). `recursive: true` makes the snapshot worker run `zfs snapshot -r`
  (backup_manager.py:1547), but `DatasetManager._get_datasets` (backup_manager.py:616-621) builds
  only configured entries and `RemoteBackupWorker._get_datasets` (workers.py:1248-1262) adds
  nothing, so no descendant is ever sent.

The ZFS world is faked one level below libzfseasy, at `subprocess.Popen`, so the INHERITANCE IS
REAL AT THE `zfs get` LEVEL: `zfs get -H -o value -s local <prop> <ds>` prints nothing for a
dataset whose value comes from its parent, and `zfs get -H -o value <prop> <ds>` prints the
parent's value. No method of `DatasetManager` is patched. Nothing here reaches a real `zfs`.
"""

import io
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set, Tuple

import pytest

from _snapshot_support import list_row
from libzfseasy.zfs import ZFSTimeoutError
from zfsbackup.backup_manager import (
    PROP_ANCHOR_PREFIX,
    PROP_INFLIGHT_PREFIX,
    SNAPSHOT_LIST_PROPERTIES,
    DatasetManager,
)
from zfsbackup.chain import _ANCHOR_FAILED, _OK, ChainSender, _SendStream
from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Destination,
    Duration,
    RemoteDatasetConfig,
)
from zfsbackup.health import STATE_OK, evaluate
from zfsbackup.health_collect import collect
from zfsbackup.stopflag import StopFlag
from zfsbackup.workers import ExitReason, RemoteBackupWorker

PREFIX = 'autosnap'
DEST = 'off'
PARENT = 'pool/data'
CHILD = 'pool/data/child'


def stamp(age: timedelta) -> str:
    """A managed snapshot name dated `age` before now (the code under test uses the real clock)."""
    return f"{PREFIX}_{(datetime.now(timezone.utc) - age).strftime('%Y%m%d%H%M%S')}"


# ---------------------------------------------------------------------------------------------
# A fake `zfs` with property inheritance, at the Popen level
# ---------------------------------------------------------------------------------------------

class _Proc:
    def __init__(self, stdout: bytes = b'', stderr: bytes = b'', rc: int = 0):
        self.stdout = io.BytesIO(stdout)
        self.stderr = io.BytesIO(stderr)
        self.returncode = rc
        self.pid = 4242

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        return self.returncode

    def terminate(self):
        pass

    kill = terminate


class InheritingZfs:
    """A small pool as `zfs list` and `zfs get` print it, with ZFS's user-property inheritance.

    * `datasets[name]` -- `'filesystem'` or `'volume'`
    * `snaps[name]` -- the snapshot names (short) of a dataset
    * `props[(name, prop)]` -- a user property set LOCALLY on a dataset
    * `received[(name, prop)]` -- a property whose value was RECEIVED (source `received`; it wins
      over an inherited value, as in ZFS)
    * `unreadable` -- `(name, prop)` pairs whose `zfs get` fails (exit 1, a pool error)

    `zfs get -H -o value [-s SOURCES] <prop> <ds>` prints the effective value (local, else
    received, else the nearest ancestor's, source `inherited`) or `-` when unset; with `-s` it
    prints NOTHING when the value's source is not among those asked for -- exactly what
    `zfs.get.value(..., sources='local')` relies on. `zfs get -H -o value guid <ds>@<snap>`
    answers the listing's guid (the post-step presence check). `zfs list -H -t snapshot` answers
    the six-column listing; `zfs list -H -o name,type [-r] -t filesystem,volume` the name
    enumeration. Every argv (after the binary) is in `calls`.

    Any other command raises at once, so a write cannot succeed silently -- unless the test sets
    `allow_writes`: `zfs set prop=value ds` and `zfs inherit prop ds` then change `props` and are
    recorded in `writes` as `('set', ds, prop, value)` / `('inherit', ds, prop)`."""

    def __init__(self, mocker):
        self.datasets: Dict[str, str] = {}
        self.snaps: Dict[str, List[str]] = {}
        self.props: Dict[Tuple[str, str], str] = {}
        self.received: Dict[Tuple[str, str], str] = {}
        self.unreadable: Set[Tuple[str, str]] = set()
        self.allow_writes = False
        self.writes: List[Tuple[str, ...]] = []
        self.calls: List[List[str]] = []
        mocker.patch('subprocess.Popen', side_effect=self._popen)

    # -- building the world ---------------------------------------------------------------------
    def add(self, name, dstype='filesystem', snaps=()):
        self.datasets[name] = dstype
        self.snaps[name] = list(snaps)
        return self

    def set_prop(self, name, prop, value):
        self.props[(name, prop)] = value

    def value_reads(self, name):
        return [a for a in self.calls if a[:4] == ['get', '-H', '-o', 'value'] and a[-1] == name]

    # -- the fake -------------------------------------------------------------------------------
    def _popen(self, argv, *args, **kwargs):
        rest = [os.fspath(a) for a in argv][1:]
        self.calls.append(rest)
        if rest[:4] == ['get', '-H', '-o', 'value']:
            return self._get_value(rest)
        if rest[:1] == ['list']:
            return self._list(rest)
        if self.allow_writes and rest[:1] == ['set']:
            for item in rest[1:-1]:
                prop, _, value = item.partition('=')
                self.writes.append(('set', rest[-1], prop, value))
                self.props[(rest[-1], prop)] = value
            return _Proc()
        if self.allow_writes and rest[:1] == ['inherit']:
            self.writes.append(('inherit', rest[-1], rest[1]))
            self.props.pop((rest[-1], rest[1]), None)
            return _Proc()
        raise AssertionError(f'fake zfs: unexpected command {rest}')

    def _effective(self, name, prop):
        """(value, source) as zfs reports a user property; (None, None) when no one sets it."""
        if (name, prop) in self.props:
            return self.props[(name, prop)], 'local'
        if (name, prop) in self.received:
            return self.received[(name, prop)], 'received'
        parent = name.rpartition('/')[0]
        while parent:
            for source in (self.props, self.received):
                if (parent, prop) in source:
                    return source[(parent, prop)], 'inherited'
            parent = parent.rpartition('/')[0]
        return None, None

    def _get_value(self, rest):
        name, prop = rest[-1], rest[-2]
        dataset, at, snap = name.partition('@')
        if at:                                              # a snapshot's own property (guid)
            if snap not in self.snaps.get(dataset, []):
                return _Proc(b'', f"cannot open '{name}': dataset does not exist\n".encode(), 1)
            cells = dict(zip(['name', 'type'] + list(SNAPSHOT_LIST_PROPERTIES),
                             list_row(snap, dataset).rstrip('\n').split('\t')))
            return _Proc(f"{cells[prop]}\n".encode())
        if name not in self.datasets:
            return _Proc(b'', f"cannot open '{name}': dataset does not exist\n".encode(), 1)
        if (name, prop) in self.unreadable:
            return _Proc(b'', f"cannot open '{name}': pool I/O is currently suspended\n".encode(), 1)
        value, source = self._effective(name, prop)
        if '-s' in rest and source not in rest[rest.index('-s') + 1].split(','):
            return _Proc()                                  # a source filter that excludes it: nothing
        return _Proc(f"{'-' if value is None else value}\n".encode())

    def _list(self, rest):
        root = rest[-1]
        columns = rest[rest.index('-o') + 1].split(',')
        types = rest[rest.index('-t') + 1] if '-t' in rest else 'filesystem,volume'
        if root not in self.datasets:
            return _Proc(b'', f"cannot open '{root}': dataset does not exist\n".encode(), 1)
        if types == 'snapshot':
            known = ['name', 'type'] + list(SNAPSHOT_LIST_PROPERTIES)
            lines = []
            for snap in self.snaps.get(root, []):
                cells = dict(zip(known, list_row(snap, root).rstrip('\n').split('\t')))
                lines.append('\t'.join(cells.get(c, '-') for c in columns))
            return _Proc(''.join(line + '\n' for line in lines).encode())
        wanted = types.split(',')
        names = [root] + (
            [n for n in self.datasets if n.startswith(root + '/')] if '-r' in rest else []
        )
        lines = ['\t'.join({'name': n, 'type': self.datasets[n]}.get(c, '-') for c in columns)
                 for n in names if self.datasets[n] in wanted]
        return _Proc(''.join(line + '\n' for line in lines).encode())


@pytest.fixture
def zfs_world(mocker):
    return InheritingZfs(mocker)


def anchored_pair(world: InheritingZfs, snapshot: str) -> None:
    """`PARENT` holds a LOCAL anchor for `DEST` naming `snapshot`; `CHILD` has none (so zfs reports
    the parent's as INHERITED) and carries a snapshot of that very name (a recursive snapshot)."""
    world.add(PARENT, snaps=[snapshot])
    world.add(CHILD, snaps=[snapshot])
    world.set_prop(PARENT, f'{PROP_ANCHOR_PREFIX}{DEST}', snapshot)


def config(*datasets, **kw) -> BackupConfig:
    kw.setdefault('snapshot_prefix', PREFIX)
    kw.setdefault('destinations', {DEST: Destination(url='https://h:8443')})
    return BackupConfig(datasets=list(datasets), **kw)


def dataset(name, **kw) -> DatasetConfig:
    return DatasetConfig(
        name=name, frequency=Duration('1h'), remote=[RemoteDatasetConfig(destination=DEST)], **kw
    )


class RecordingSender:
    """Stands in for `RemoteBackupManager`: records `backup_dataset` and reports success."""

    def __init__(self):
        self.attempts: List[Tuple[str, str]] = []

    def begin_cycle(self):
        pass

    def backup_dataset(self, dsi, remote_cfg, should_stop=None):
        self.attempts.append((dsi.name, remote_cfg.destination))
        return True

    def last_problem(self, dataset, destination):
        return None


# ---------------------------------------------------------------------------------------------
# The fake itself: the inheritance the other tests lean on is real at the `zfs get` level
# ---------------------------------------------------------------------------------------------

@pytest.mark.unit
class TestInheritingFakeZfs:
    def test_the_child_reads_the_parents_anchor_as_inherited_and_has_no_local_one(self, zfs_world):
        name = stamp(timedelta(minutes=10))
        anchored_pair(zfs_world, name)
        manager = DatasetManager(config(dataset(PARENT, recursive=True), dataset(CHILD)))
        child = next(d for d in manager.datasets if d.name == CHILD)
        parent = next(d for d in manager.datasets if d.name == PARENT)
        assert manager.get_anchor(child, DEST) == name               # effective: the parent's
        assert manager.get_local_anchor(child, DEST) is None         # but not the child's own
        assert manager.get_local_anchor(parent, DEST) == name


# ---------------------------------------------------------------------------------------------
# A1: the inherited anchor
# ---------------------------------------------------------------------------------------------

@pytest.mark.unit
class TestInheritedAnchorInTheDueCheck:
    """Scenario: `pool/data` (recursive) and `pool/data/child` are both configured, with the same
    destination and a 1h frequency. The parent was sent a few minutes ago, so it holds a LOCAL
    `anchor.off`. The child was never sent: it has no local anchor, `zfs get` reports the parent's
    as `inherited`, and the child has a snapshot of that name.

    Path: `RemoteBackupWorker._send_to_destination`. It used to read `manager.get_anchor`, which
    returned the inherited name; `list_snapshots` found it among the child's snapshots and
    `anchor.age < freq` said "not due yet". It now reads `get_own_anchor` (`-s local`).
    Desired: the child has no anchor of its own, so it is due and `backup_dataset` runs."""

    def test_a_child_without_a_local_anchor_is_due(self, zfs_world, mocker):
        anchored_pair(zfs_world, stamp(timedelta(minutes=10)))
        cfg = config(dataset(PARENT, recursive=True), dataset(CHILD))
        manager = DatasetManager(cfg)
        child = next(d for d in manager.datasets if d.name == CHILD)

        worker = RemoteBackupWorker(cfg, StopFlag())
        sender = RecordingSender()
        worker._remote_manager = sender

        worker._send_to_destination(manager, child, child.config.remote[0], mocker.MagicMock())

        assert sender.attempts == [(CHILD, DEST)], 'the child has no anchor of its own: it is due'

    def test_a_parent_with_a_fresh_local_anchor_is_not_due(self, zfs_world, mocker):
        # The control: the same worker, the same fake, a LOCAL fresh anchor -> not due. Passes now
        # and after A1; it proves the fake and the setup, so the xfail above fails for its reason.
        anchored_pair(zfs_world, stamp(timedelta(minutes=10)))
        cfg = config(dataset(PARENT, recursive=True), dataset(CHILD))
        manager = DatasetManager(cfg)
        parent = next(d for d in manager.datasets if d.name == PARENT)

        worker = RemoteBackupWorker(cfg, StopFlag())
        sender = RecordingSender()
        worker._remote_manager = sender

        worker._send_to_destination(manager, parent, parent.config.remote[0], mocker.MagicMock())

        assert sender.attempts == []


@pytest.mark.unit
class TestInheritedAnchorInCheck:
    """Scenario as above. Path: `health_collect._observe_local` used to build the anchor
    observation from `manager.get_anchor` (now `get_own_anchor`), so the never-sent child's anchor
    observation carries the parent's snapshot time and `evaluate` grades it OK.
    Desired: no anchor observation timestamp for the child (never sent) and a verdict that is
    not OK, exactly as for a dataset with no anchor at all."""

    @staticmethod
    def anchor_observation(observations, subject):
        (obs,) = [o for o in observations
                  if getattr(o, 'source', None) == 'anchor' and o.subject == subject]
        return obs

    def test_the_parents_anchor_observation_is_fresh(self, zfs_world):
        # The control: the parent's own anchor is local and fresh -> OK, now and after A1.
        anchored_pair(zfs_world, stamp(timedelta(minutes=10)))
        observations = collect(config(dataset(PARENT, recursive=True), dataset(CHILD)))
        parent = self.anchor_observation(observations, PARENT)
        assert parent.newest_ts is not None
        (entry,) = [e for e in evaluate([parent], datetime.now(timezone.utc).timestamp())]
        assert entry.state == STATE_OK

    def test_a_never_sent_child_has_no_anchor_and_does_not_grade_ok(self, zfs_world):
        anchored_pair(zfs_world, stamp(timedelta(minutes=10)))
        observations = collect(config(dataset(PARENT, recursive=True), dataset(CHILD)))

        child = self.anchor_observation(observations, CHILD)
        assert child.error is None
        assert child.newest_ts is None, 'the child was never sent: it has no anchor of its own'
        (entry,) = evaluate([child], datetime.now(timezone.utc).timestamp())
        assert entry.state != STATE_OK


# ---------------------------------------------------------------------------------------------
# A2: descendants of a recursive dataset
# ---------------------------------------------------------------------------------------------

@pytest.mark.unit
class TestDescendantsOfARecursiveDataset:
    """Scenario: only `pool/data` is configured, `recursive: true`, with remote destination `off`.
    It has a child filesystem and a child zvol, neither configured. The snapshot worker already
    snapshots them (`zfs snapshot -r`, backup_manager.py:1547).

    Path: `RemoteBackupWorker.run` -> `_get_datasets` (workers.py:1248-1262) ->
    `DatasetManager.datasets` (backup_manager.py:616-621), which lists configured entries only,
    so `_send_to_destinations` is called for the parent alone.
    Desired: one `backup_dataset` attempt per child x destination, after the parent."""

    @staticmethod
    def one_cycle(cfg, mocker):
        worker = RemoteBackupWorker(cfg, StopFlag())
        sender = RecordingSender()
        mocker.patch.object(worker, '_before_loop',
                            side_effect=lambda *a: setattr(worker, '_remote_manager', sender))
        mocker.patch.object(worker, '_wait', return_value=ExitReason.STOP)   # exactly one cycle
        worker.run()
        return sender

    def tree(self, world):
        snap = stamp(timedelta(minutes=10))
        world.add(PARENT, snaps=[snap])
        world.add(f'{PARENT}/fs', 'filesystem', snaps=[snap])
        world.add(f'{PARENT}/vol', 'volume', snaps=[snap])

    def test_only_the_configured_parent_is_attempted_today(self, zfs_world, mocker):
        # Documents the current behaviour (and guards the harness): when A2 lands this assertion
        # is deleted together with the xfail below.
        self.tree(zfs_world)
        sender = self.one_cycle(config(dataset(PARENT, recursive=True)), mocker)
        assert sender.attempts == [(PARENT, DEST)]

    @pytest.mark.xfail(
        strict=True,
        reason="A2: descendants of a recursive dataset are never sent "
               "(workers.py:1248-1262, backup_manager.py:616-621)",
    )
    def test_each_descendant_is_sent_to_each_destination_after_its_parent(self, zfs_world, mocker):
        self.tree(zfs_world)
        sender = self.one_cycle(config(dataset(PARENT, recursive=True)), mocker)

        assert sender.attempts[:1] == [(PARENT, DEST)], 'the parent goes first'
        assert sorted(sender.attempts[1:]) == [(f'{PARENT}/fs', DEST), (f'{PARENT}/vol', DEST)]
        assert len(sender.attempts) == 3, 'one attempt per dataset x destination'


# ---------------------------------------------------------------------------------------------
# A1: the matrix. Sources of the CHILD's anchor / pin x the consumers that read it
# ---------------------------------------------------------------------------------------------

ANCHOR = f'{PROP_ANCHOR_PREFIX}{DEST}'
PIN = f'{PROP_INFLIGHT_PREFIX}{DEST}'

#: Where the child's value of a property can come from. `unreadable`: the child's own `zfs get` of
#: it fails. In every row but `unset` the PARENT holds a local value as well, so an effective read
#: (the pre-A1 behaviour) would see a value for the child in all of them.
SOURCES = ['local', 'inherited', 'received', 'unset', 'unreadable']
#: The rows in which the child has NO usable own value (a local one is the only one that counts).
NOT_OWN = [s for s in SOURCES if s != 'local']


def give(world: InheritingZfs, source: str, prop: str, value: str) -> None:
    """Make the CHILD's `prop` come from `source`."""
    if source == 'local':
        world.set_prop(CHILD, prop, value)
    elif source == 'inherited':
        world.set_prop(PARENT, prop, value)
    elif source == 'received':
        world.received[(CHILD, prop)] = value
        world.set_prop(PARENT, prop, value)
    elif source == 'unreadable':
        world.unreadable.add((CHILD, prop))
        world.set_prop(PARENT, prop, value)
    else:
        assert source == 'unset'


def two_snapshot_pair(world: InheritingZfs):
    """PARENT and CHILD each hold OLD and NEW (a recursive snapshot gives both the same names)."""
    old, new = stamp(timedelta(minutes=30)), stamp(timedelta(minutes=10))
    world.add(PARENT, snaps=[old, new])
    world.add(CHILD, snaps=[old, new])
    return old, new


def debug_reads(caplog, prop=ANCHOR):
    return [r for r in caplog.records if r.levelname == 'DEBUG'
            and f'Failed to read local property {prop} from {CHILD}' in r.getMessage()]


@pytest.mark.unit
class TestA1MatrixDueCheck:
    """`RemoteBackupWorker._send_to_destination` (the due check) reads `get_own_anchor`."""

    @pytest.mark.parametrize('source', SOURCES)
    def test_only_a_local_fresh_anchor_makes_the_child_not_due(self, zfs_world, mocker, caplog, source):
        old, new = two_snapshot_pair(zfs_world)
        give(zfs_world, source, ANCHOR, new)
        cfg = config(dataset(PARENT, recursive=True), dataset(CHILD))
        manager = DatasetManager(cfg)
        child = next(d for d in manager.datasets if d.name == CHILD)
        worker = RemoteBackupWorker(cfg, StopFlag())
        sender = RecordingSender()
        worker._remote_manager = sender
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            worker._send_to_destination(manager, child, child.config.remote[0], mocker.MagicMock())

        assert sender.attempts == ([] if source == 'local' else [(CHILD, DEST)])
        # one `-s local` read of the child's own anchor, and no effective read of it
        assert [a for a in zfs_world.value_reads(CHILD) if a[-2] == ANCHOR] == [
            ['get', '-H', '-o', 'value', '-s', 'local', ANCHOR, CHILD]]

    def test_an_unreadable_anchor_is_due_with_a_debug_line_and_no_warning(self, zfs_world, mocker, caplog):
        old, new = two_snapshot_pair(zfs_world)
        give(zfs_world, 'unreadable', ANCHOR, new)
        cfg = config(dataset(CHILD))
        manager = DatasetManager(cfg)
        worker = RemoteBackupWorker(cfg, StopFlag())
        worker._remote_manager = sender = RecordingSender()
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            worker._send_to_destination(manager, manager.datasets[0], manager.datasets[0].config.remote[0],
                                        mocker.MagicMock())
        assert sender.attempts == [(CHILD, DEST)]
        assert len(debug_reads(caplog)) == 1 and 'pool I/O is currently suspended' in debug_reads(caplog)[0].getMessage()
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


@pytest.mark.unit
class TestA1MatrixHealth:
    """`health_collect._observe_local` reads `get_own_anchor` and `get_own_inflight`."""

    @staticmethod
    def child_anchor(observations):
        (obs,) = [o for o in observations if getattr(o, 'source', None) == 'anchor' and o.subject == CHILD]
        return obs

    @pytest.mark.parametrize('source', SOURCES)
    def test_the_anchor(self, zfs_world, source):
        old, new = two_snapshot_pair(zfs_world)
        give(zfs_world, source, ANCHOR, new)
        obs = self.child_anchor(collect(config(dataset(PARENT, recursive=True), dataset(CHILD))))
        assert obs.error is None
        now = datetime.now(timezone.utc).timestamp()
        if source == 'local':
            assert obs.newest_ts is not None and obs.detail == new
            (entry,) = evaluate([obs], now)
            assert entry.state == STATE_OK
        else:
            assert obs.newest_ts is None, 'no own anchor: the child shows none'
            (entry,) = evaluate([obs], now)
            assert entry.state != STATE_OK

    @pytest.mark.parametrize('source', SOURCES)
    def test_the_pin(self, zfs_world, source):
        old, new = two_snapshot_pair(zfs_world)
        give(zfs_world, source, PIN, f'{old},{new}')
        obs = self.child_anchor(collect(config(dataset(PARENT, recursive=True), dataset(CHILD))))
        note = f'send in flight: {old} -> {new}'
        assert (note in (obs.detail or '')) == (source == 'local')

    def test_every_read_of_the_childs_own_values_is_local(self, zfs_world):
        old, new = two_snapshot_pair(zfs_world)
        give(zfs_world, 'inherited', ANCHOR, new)
        give(zfs_world, 'inherited', PIN, f'{old},{new}')
        collect(config(dataset(CHILD)))
        reads = [a for a in zfs_world.value_reads(CHILD) if a[-2] in (ANCHOR, PIN)]
        assert sorted(a[-2] for a in reads) == sorted([ANCHOR, PIN])
        assert all(a[4:6] == ['-s', 'local'] for a in reads)


@pytest.mark.unit
class TestA1MatrixSender:
    """`_record_step` and `_settle_up_to_date` (the real `ChainSender` code over a real
    `DatasetManager` and the inheriting fake, which now accepts `zfs set` / `zfs inherit` and
    records them). The anchor is read with the RAISING `get_local_anchor`: an unreadable one fails
    closed, it is not 'unset'. The pin read in settle is `get_own_inflight` (unreadable = no pin)."""

    class Sender(ChainSender):
        def _ready_transport(self, destination, dest):
            raise NotImplementedError

        def _negotiate(self, *a, **k):
            raise NotImplementedError

        def _fetch_resume_token(self, *a, **k):
            raise NotImplementedError

        def _stream_step(self, *a, **k):
            raise NotImplementedError

    def rig(self, world):
        old, new = two_snapshot_pair(world)
        world.allow_writes = True
        cfg = config(dataset(CHILD))
        manager = DatasetManager(cfg)
        dsi = manager.datasets[0]
        sender = self.Sender(cfg, manager, client_id='client1')
        snapshots = manager.list_snapshots(dsi)
        return sender, dsi, snapshots, {s.name: s for s in snapshots}, old, new

    # -- _record_step: the never-backwards rule ---------------------------------------------------

    @pytest.mark.parametrize('source', SOURCES)
    def test_record_step_never_moves_a_local_anchor_backwards(self, zfs_world, caplog, source):
        """OLD is sent (a resume); the anchor is on NEW. Only a LOCAL anchor on NEW holds it back."""
        sender, dsi, snapshots, by_name, old, new = self.rig(zfs_world)
        give(zfs_world, source, ANCHOR, new)
        give(zfs_world, 'local', PIN, f'{old},{old}')
        started = _SendStream(None, 'resume', True, by_name[old], None)
        with caplog.at_level(logging.DEBUG):
            outcome = sender._record_step(None, dsi, DEST, snapshots, started)

        if source == 'local':
            assert outcome == _OK and zfs_world.writes == []        # kept; the pin is not released
        elif source == 'unreadable':
            assert outcome == _ANCHOR_FAILED and zfs_world.writes == []   # fails closed, pin kept
            warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
            assert len(warnings) == 1 and 'cannot read the local anchor property' in warnings[0].getMessage()
        else:
            assert outcome == _OK
            assert zfs_world.writes == [('set', CHILD, ANCHOR, old), ('inherit', CHILD, PIN)]
            assert zfs_world.props[(CHILD, ANCHOR)] == old

    # -- _settle_up_to_date: the anchor -----------------------------------------------------------

    @pytest.mark.parametrize('source', SOURCES)
    def test_settle_anchor(self, zfs_world, caplog, source):
        """OLD is already on the server; the anchor is on NEW (newer: left alone when it is ours)."""
        sender, dsi, snapshots, by_name, old, new = self.rig(zfs_world)
        give(zfs_world, source, ANCHOR, new)
        with caplog.at_level(logging.WARNING):
            settled = sender._settle_up_to_date(dsi, by_name[old], DEST, snapshots)

        if source == 'local':
            assert settled is True and zfs_world.writes == []
        elif source == 'unreadable':
            assert settled is False and zfs_world.writes == []
            (rec,) = caplog.records
            assert 'cannot read the local anchor property' in rec.getMessage()
        else:
            assert settled is True
            assert zfs_world.writes == [('set', CHILD, ANCHOR, old)], 'the child gets its OWN anchor'

    @pytest.mark.parametrize('source', SOURCES)
    def test_settle_a_missing_own_anchor_is_written_at_latest(self, zfs_world, source):
        """The plain case: NEW is on the server and the anchor value (where it exists) says NEW."""
        sender, dsi, snapshots, by_name, old, new = self.rig(zfs_world)
        give(zfs_world, source, ANCHOR, new)
        settled = sender._settle_up_to_date(dsi, by_name[new], DEST, snapshots)
        expected = {'local': [], 'unreadable': []}.get(source, [('set', CHILD, ANCHOR, new)])
        assert zfs_world.writes == expected
        assert settled is (source != 'unreadable')

    # -- _settle_up_to_date: the pin --------------------------------------------------------------

    @pytest.mark.parametrize('source', SOURCES)
    def test_settle_pin(self, zfs_world, source):
        """The anchor is right (a local one on NEW); only the child's OWN pin is released."""
        sender, dsi, snapshots, by_name, old, new = self.rig(zfs_world)
        give(zfs_world, 'local', ANCHOR, new)
        give(zfs_world, source, PIN, f'{old},{new}')
        assert sender._settle_up_to_date(dsi, by_name[new], DEST, snapshots) is True
        released = [('inherit', CHILD, PIN)] if source == 'local' else []
        assert zfs_world.writes == released
        if source == 'local':
            assert (CHILD, PIN) not in zfs_world.props

    def test_settle_with_an_unreadable_pin_clears_nothing_and_says_so_at_debug(self, zfs_world, caplog):
        sender, dsi, snapshots, by_name, old, new = self.rig(zfs_world)
        give(zfs_world, 'local', ANCHOR, new)
        give(zfs_world, 'unreadable', PIN, f'{old},{new}')
        with caplog.at_level(logging.DEBUG):
            assert sender._settle_up_to_date(dsi, by_name[new], DEST, snapshots) is True
        assert zfs_world.writes == []
        assert len(debug_reads(caplog, PIN)) == 1
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


@pytest.mark.unit
class TestA1OwnReadersDirectly:
    """`get_own_anchor` / `get_own_inflight`: exact argv, parsing, and the timeout contract."""

    @pytest.fixture
    def child(self, zfs_world):
        two_snapshot_pair(zfs_world)
        manager = DatasetManager(config(dataset(CHILD)))
        return manager, manager.datasets[0]

    def test_exact_argv(self, zfs_world, child):
        manager, dsi = child
        zfs_world.set_prop(CHILD, ANCHOR, 'x')
        zfs_world.set_prop(CHILD, PIN, 'a,b')
        assert manager.get_own_anchor(dsi, DEST) == 'x'
        assert manager.get_own_inflight(dsi, DEST) == ('a', 'b')
        assert zfs_world.calls == [
            ['get', '-H', '-o', 'value', '-s', 'local', ANCHOR, CHILD],
            ['get', '-H', '-o', 'value', '-s', 'local', PIN, CHILD],
        ]

    @pytest.mark.parametrize('source', ['inherited', 'received', 'unset', 'unreadable'])
    def test_not_a_local_value_is_none(self, zfs_world, child, source):
        manager, dsi = child
        give(zfs_world, source, ANCHOR, 'x')
        give(zfs_world, source, PIN, 'a,b')
        assert manager.get_own_anchor(dsi, DEST) is None
        assert manager.get_own_inflight(dsi, DEST) is None

    @pytest.mark.parametrize('call', ['get_own_anchor', 'get_own_inflight'])
    def test_a_timeout_returns_none_outside_a_copys_pass(self, mocker, child, call, caplog):
        manager, dsi = child
        mocker.patch('zfsbackup.backup_manager.zfs.get.value', side_effect=ZFSTimeoutError('hung'))
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            assert getattr(manager, call)(dsi, DEST) is None
        assert any('Failed to read local property' in r.getMessage() for r in caplog.records)

    @pytest.mark.parametrize('call', ['get_own_anchor', 'get_own_inflight'])
    def test_a_timeout_propagates_while_a_copy_is_being_pruned(self, mocker, child, call):
        manager, dsi = child
        mocker.patch('zfsbackup.backup_manager.zfs.get.value', side_effect=ZFSTimeoutError('hung'))
        manager._prune_timed = True
        with pytest.raises(ZFSTimeoutError):
            getattr(manager, call)(dsi, DEST)

    @pytest.mark.parametrize('call', ['get_own_anchor', 'get_own_inflight'])
    def test_any_other_failure_is_none_even_while_pruning(self, mocker, child, call):
        manager, dsi = child
        mocker.patch('zfsbackup.backup_manager.zfs.get.value', side_effect=RuntimeError('boom'))
        manager._prune_timed = True
        assert getattr(manager, call)(dsi, DEST) is None


@pytest.mark.unit
class TestA1ProtectionOnlyWidens:
    """The pruner's protect-set still includes an INHERITED anchor (`get_anchors` -> `get_anchor`, the
    effective read): a parent's anchor on a child's same-named snapshot is kept. The dataset's own
    `org.zfsbackup:*` two-step read is stubbed out (it is not under test here)."""

    @pytest.mark.parametrize('own_properties_last', [False, True])
    def test_an_inherited_anchor_is_in_the_protect_set(self, zfs_world, mocker, own_properties_last):
        old, new = two_snapshot_pair(zfs_world)
        zfs_world.set_prop(PARENT, ANCHOR, old)             # the child inherits it
        mocker.patch('zfsbackup.backup_manager.read_zfsbackup_properties', return_value={})
        manager = DatasetManager(config(dataset(PARENT, recursive=True), dataset(CHILD)))
        child = next(d for d in manager.datasets if d.name == CHILD)
        assert manager.get_own_anchor(child, DEST) is None            # not its own history ...
        assert manager.get_anchors(child, own_properties_last=own_properties_last) == {old}   # ... but kept
        assert manager.get_anchor(child, DEST) == old

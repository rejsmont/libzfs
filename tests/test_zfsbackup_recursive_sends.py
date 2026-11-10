"""Sender rework, Track A, item A0: characterization of two current gaps, as strict xfails.

Plan: `docs/sender_rework_plan.md` (Track A). Each test below states the DESIRED behaviour and
is marked `xfail(strict=True)` because the code does not have it yet. Today they report
"xfailed"; when the named item lands they turn into an XPASS, which strict mode makes a hard
failure -- the fixing change then deletes the marker (and A1/A2 extend this file).

* A1 (the inherited anchor). `DatasetManager.get_anchor` reads the EFFECTIVE value of
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
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import pytest

from _snapshot_support import list_row
from zfsbackup.backup_manager import (
    PROP_ANCHOR_PREFIX,
    SNAPSHOT_LIST_PROPERTIES,
    DatasetManager,
)
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

    `zfs get -H -o value [-s SOURCES] <prop> <ds>` prints the effective value (local, else the
    nearest ancestor's, source `inherited`) or `-` when unset; with `-s` it prints NOTHING when
    the value's source is not among those asked for -- exactly what `zfs.get.value(...,
    sources='local')` relies on. `zfs list -H -t snapshot` answers the six-column listing; `zfs
    list -H -o name,type [-r] -t filesystem,volume` the name enumeration. Every argv (after the
    binary) is in `calls`; any other command raises at once, so a write cannot succeed silently."""

    def __init__(self, mocker):
        self.datasets: Dict[str, str] = {}
        self.snaps: Dict[str, List[str]] = {}
        self.props: Dict[Tuple[str, str], str] = {}
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
        raise AssertionError(f'fake zfs: unexpected command {rest}')

    def _effective(self, name, prop):
        """(value, source) as zfs reports a user property; (None, None) when no one sets it."""
        if (name, prop) in self.props:
            return self.props[(name, prop)], 'local'
        parent = name.rpartition('/')[0]
        while parent:
            if (parent, prop) in self.props:
                return self.props[(parent, prop)], 'inherited'
            parent = parent.rpartition('/')[0]
        return None, None

    def _get_value(self, rest):
        name, prop = rest[-1], rest[-2]
        if name not in self.datasets:
            return _Proc(b'', f"cannot open '{name}': dataset does not exist\n".encode(), 1)
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

    Path: `RemoteBackupWorker._send_to_destination` (workers.py:1353-1370):
    `manager.get_anchor(dsi, dest)` returns the inherited name, `list_snapshots` finds it among
    the child's snapshots, `anchor.age < freq` -> "not due yet", `backup_dataset` is never called.
    Desired: the child has no anchor of its own, so it is due and `backup_dataset` runs."""

    @pytest.mark.xfail(
        strict=True,
        reason="A1: the due check reads the effective anchor, so a child inherits its parent's "
               "and is 'not due' for ever (workers.py:1353, backup_manager.py:663-681, 712-715)",
    )
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
    """Scenario as above. Path: `health_collect._observe_local` (health_collect.py:145-150)
    builds the anchor observation from `manager.get_anchor`, so the never-sent child's anchor
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

    @pytest.mark.xfail(
        strict=True,
        reason="A1: health_collect reads the effective anchor, so a never-sent child is graded "
               "as fresh from its parent's anchor (health_collect.py:145-150)",
    )
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

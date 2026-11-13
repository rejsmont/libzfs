"""Sender rework, Track A: the A0 characterization (now plain tests), the A1 matrix, the A2 tests.

Plan: `docs/sender_rework_plan.md` (Track A). A0 wrote each test as the DESIRED behaviour, marked
`xfail(strict=True)` until its item landed; A1 and A2 have landed and no marker is left. The A1 tests
are `TestA1*`; the A2 ones are `TestExpand*` (`DatasetManager.expand_recursive`) and `TestA2*` (the
remote worker: depth order, R-2, status keys, the cap warning). The copy-name-length refusal of A2 is
tested where its pieces live: `test_zfsbackup_receiver.py` (the helper), `test_zfsbackup_local.py`,
`test_zfsbackup_api_negotiate.py` and `test_zfsbackup_remote.py`.

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
* A2 (recursive sends; LANDED). `recursive: true` makes the snapshot worker run `zfs snapshot -r`
  (backup_manager.py:1547). Before A2 `DatasetManager._get_datasets` built only configured entries and
  `RemoteBackupWorker._get_datasets` added nothing, so no descendant was ever sent; now
  `DatasetManager.expand_recursive` names them and the remote worker appends them.

The ZFS world is faked one level below libzfseasy, at `subprocess.Popen`, so the INHERITANCE IS
REAL AT THE `zfs get` LEVEL: `zfs get -H -o value -s local <prop> <ds>` prints nothing for a
dataset whose value comes from its parent, and `zfs get -H -o value <prop> <ds>` prints the
parent's value. No method of `DatasetManager` is patched. Nothing here reaches a real `zfs`.
"""

import io
import logging
import os
import re
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set, Tuple

import pytest

import libzfseasy as libzfseasy_module
from _get_support import copy_read_argv
from _sendloop_support import LoopRig, age_status
from _snapshot_support import list_row
from libzfseasy.zfs import ZFSTimeoutError
from zfsbackup.backup_manager import (
    PROP_ANCHOR_PREFIX,
    PROP_INFLIGHT_PREFIX,
    SNAPSHOT_LIST_PROPERTIES,
    DatasetInfo,
    DatasetManager,
)
from zfsbackup.chain import _ANCHOR_FAILED, _FAILED, _OK, ChainSender, _SendStream
from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Destination,
    Duration,
    RemoteDatasetConfig,
)
from zfsbackup.health import STATE_CRIT, STATE_OK, STATE_WARN, evaluate
from zfsbackup.health_collect import collect
from _remote_support import snapshot_presence  # noqa: F401  (fixture)
from test_zfsbackup_chain import DEST as CHAIN_DEST, DS as CHAIN_DS, S1, S2, S3, Rig as ChainRig, listing, sinfo
from zfsbackup.sendpair import PAIR_DONE, PAIR_NOT_DUE, send_pair
from zfsbackup.sendmemory import (
    COPY_CONFIRMED, DRY_RUN_SENT, EXPAND_ERROR, EXPAND_TARGET, WAITING, SendMemory,
)
from zfsbackup.stopflag import StopFlag
from zfsbackup.workers import BaseWorker, ExitReason, RemoteBackupWorker

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
    * `list_failures[root]` -- the stderr of a `zfs list ... <root>` that fails (exit 1); remove
      the entry to make it work again (A2: a failing discovery, then its recovery)
    * `list_extra[root]` -- raw `(name, type)` rows a recursive listing of `root` prints before
      the datasets (names a real `zfs list -r` never prints for `root`: siblings,
      snapshots, hostile bytes), to prove the reader's own scope check
    * `copy_read_failures[root]` -- the stderr of the value-free recursive `zfs get -H -o
      name,property,source -s local,received,inherited -r ... all <root>` (the backup-copy read of
      A2) when it fails; the fake answers it with `local`, `received` and `inherited from <ds>` rows
    * `snapshot_list_failures` -- datasets whose snapshot listing (`zfs list -t snapshot`) fails
    * `tree_list_failures` -- roots whose RECURSIVE snapshot listing (`zfs list -r -t snapshot`, the
      health check's one read of a recursive parent's whole tree) fails
    * `rows_failures` -- roots whose phase-2 value batch (`zfs get -H -o name,property,value,source
      -r ... <root>`) fails; so does one that asks for a property in `unreadable` below the root
    * `encryption[name]` -- the `encryption` property of a dataset's snapshots (default `off`;
      anything else reads as encrypted with its key `available`)

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
        self.list_failures: Dict[str, str] = {}
        self.list_extra: Dict[str, List[Tuple[str, str]]] = {}
        self.copy_read_failures: Dict[str, str] = {}
        self.snapshot_list_failures: Set[str] = set()
        self.tree_list_failures: Set[str] = set()
        self.rows_failures: Set[str] = set()
        self.encryption: Dict[str, str] = {}
        self.allow_writes = False
        self.unset_rows = False        # print a source `-` row for an unset requested property of the copy read
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
        if rest[:4] == ['get', '-H', '-o', 'name,property,source']:
            return self._get_sources(rest)
        if rest[:4] == ['get', '-H', '-o', 'name,property,value,source']:
            return self._get_rows(rest)
        if rest[:1] == ['list']:
            return self._list(rest)
        if self.allow_writes and rest[:1] == ['set']:
            for item in rest[1:-1]:
                prop, _, value = item.partition('=')
                self.writes.append(('set', rest[-1], prop, value))
                self.props[(rest[-1], prop)] = value
            return _Proc()
        if self.allow_writes and rest[:1] == ['inherit']:
            assert rest[1:-2] in ([], ['-S']), f'every option comes before the property: {rest}'
            self.writes.append(('inherit', rest[-1], rest[-2]))
            self.props.pop((rest[-1], rest[-2]), None)
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
                             self._snapshot_row(snap, dataset).rstrip('\n').split('\t')))
            return _Proc(f"{cells[prop]}\n".encode())
        if name not in self.datasets:
            return _Proc(b'', f"cannot open '{name}': dataset does not exist\n".encode(), 1)
        if (name, prop) in self.unreadable:
            return _Proc(b'', f"cannot open '{name}': pool I/O is currently suspended\n".encode(), 1)
        value, source = self._effective(name, prop)
        if '-s' in rest and source not in rest[rest.index('-s') + 1].split(','):
            return _Proc()                                  # a source filter that excludes it: nothing
        return _Proc(f"{'-' if value is None else value}\n".encode())

    def _source_of(self, name, prop):
        """The source string zfs prints for a user property of `name`: `local`, `received`,
        `inherited from <ancestor>` (the nearest one that has it), or None when nobody sets it."""
        if (name, prop) in self.props:
            return 'local'
        if (name, prop) in self.received:
            return 'received'
        parent = name.rpartition('/')[0]
        while parent:
            if (parent, prop) in self.props or (parent, prop) in self.received:
                return f'inherited from {parent}'
            parent = parent.rpartition('/')[0]
        return None

    def _get_sources(self, rest):
        """`zfs get -H -o name,property,source -s <sources> [-r] -t <types> all <root>`: one row per
        user property (`props`, `received`, and what the datasets inherit) whose source is among
        `sources` (`inherited` matches `inherited from X`), the root and with `-r` every dataset
        below it, a dataset's rows together; no value is printed."""
        root = rest[-1]
        requested = None if rest[-2] == 'all' else rest[-2].split(',')       # `all`, or an explicit list
        wanted = rest[rest.index('-s') + 1].split(',')
        if root in self.copy_read_failures:
            return _Proc(b'', self.copy_read_failures[root].encode() + b'\n', 1)
        if root not in self.datasets:
            return _Proc(b'', f"cannot open '{root}': dataset does not exist\n".encode(), 1)
        names = [root] + ([n for n in self.datasets if n.startswith(root + '/')] if '-r' in rest else [])
        known = requested if requested is not None else sorted(
            {prop for (_, prop) in list(self.props) + list(self.received)})
        lines = []
        for n in names:
            for prop in known:
                source = self._source_of(n, prop)
                if source is not None and source.split(' ')[0] in wanted:
                    lines.append(f"{n}\t{prop}\t{source}\n")
                elif requested is not None and self.unset_rows and source is None:
                    lines.append(f"{n}\t{prop}\t-\n")      # an unset requested property: `-` (the reader drops it)
        return _Proc(''.join(lines).encode())

    def _get_rows(self, rest):
        """`zfs get -H -o name,property,value,source [-r] -t <types> name,type,<props> <root>`, the strict phase 2
        of a two-step read: per dataset (the root, then with `-r` everything below) its `name` and `type`
        rows, then each requested property in order -- value and source (`local`, `received`, `inherited from
        X`), or `-` `-` when nobody sets it. A failing batch exits 1."""
        root = rest[-1]
        requested = rest[-2].split(',')[2:]
        if root in self.rows_failures:
            return _Proc(b'', f"cannot open '{root}': pool I/O is currently suspended\n".encode(), 1)
        if root not in self.datasets:
            return _Proc(b'', f"cannot open '{root}': dataset does not exist\n".encode(), 1)
        names = [root] + ([n for n in self.datasets if n.startswith(root + '/')] if '-r' in rest else [])
        lines = []
        for n in names:
            lines.append(f"{n}\tname\t{n}\t-\n")
            lines.append(f"{n}\ttype\t{self.datasets[n]}\t-\n")
            for prop in requested:
                if (n, prop) in self.unreadable:
                    return _Proc(b'', f"cannot open '{n}': pool I/O is currently suspended\n".encode(), 1)
                value, _ = self._effective(n, prop)
                source = self._source_of(n, prop)
                lines.append(f"{n}\t{prop}\t-\t-\n" if value is None else f"{n}\t{prop}\t{value}\t{source}\n")
        return _Proc(''.join(lines).encode())

    def name_sources_reads(self, root=None):
        """The backup-copy reads asked for (`get -H -o name,property,source ...`), of `root` if given."""
        return [a for a in self.calls if a[:4] == ['get', '-H', '-o', 'name,property,source']
                and (root is None or a[-1] == root)]

    def _snapshot_row(self, snap, dataset):
        encryption = self.encryption.get(dataset, 'off')
        return list_row(snap, dataset, encryption=encryption,
                        keystatus='-' if encryption == 'off' else 'available')

    def name_lists(self, root=None):
        """The name enumerations asked for (`zfs list -r -t filesystem,volume`), of `root` if given."""
        return [a for a in self.calls if a[:1] == ['list'] and '-r' in a and 'snapshot' not in a
                and (root is None or a[-1] == root)]

    def _list(self, rest):
        root = rest[-1]
        columns = rest[rest.index('-o') + 1].split(',')
        types = rest[rest.index('-t') + 1] if '-t' in rest else 'filesystem,volume'
        if root in self.list_failures:
            return _Proc(b'', self.list_failures[root].encode() + b'\n', 1)
        if root not in self.datasets:
            return _Proc(b'', f"cannot open '{root}': dataset does not exist\n".encode(), 1)
        if types == 'snapshot':
            if root in self.snapshot_list_failures:
                return _Proc(b'', f"cannot open '{root}': pool I/O is currently suspended\n".encode(), 1)
            known = ['name', 'type'] + list(SNAPSHOT_LIST_PROPERTIES)
            lines = []
            recursive = '-r' in rest
            if recursive and root in self.tree_list_failures:
                return _Proc(b'', f"cannot open '{root}': pool I/O is currently suspended\n".encode(), 1)
            below = [root] + ([n for n in self.datasets if n.startswith(root + '/')] if recursive else [])
            for name in below:
                for snap in self.snaps.get(name, []):
                    cells = dict(zip(known, self._snapshot_row(snap, name).rstrip('\n').split('\t')))
                    lines.append('\t'.join(cells.get(c, '-') for c in columns))
            return _Proc(''.join(line + '\n' for line in lines).encode())
        wanted = types.split(',')
        names = [root] + (
            [n for n in self.datasets if n.startswith(root + '/')] if '-r' in rest else []
        )
        rows = [(n, self.datasets[n]) for n in names if self.datasets[n] in wanted]
        if '-r' in rest:
            # BEFORE the real rows: the reader distrusts the row in front of an out-of-scope one
            rows = self.list_extra.get(root, []) + rows
        lines = ['\t'.join({'name': n, 'type': t}.get(c, '-') for c in columns) for n, t in rows]
        return _Proc(''.join(line + '\n' for line in lines).encode('utf-8', 'surrogateescape'))


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
    """Stands in for `RemoteBackupManager`: records `backup_dataset` and reports success.

    Given a `world`, a success also does what a real acknowledged send does: the dataset's LOCAL
    `anchor.<destination>` is set to its newest snapshot (what R-2 reads as "this pair has been
    sent"). Without one it reports success and leaves no trace (`backup_dataset` also returns True
    for a dataset with nothing to send)."""

    def __init__(self, world=None):
        self.world = world
        self.attempts: List[Tuple[str, str]] = []
        #: The asks of `copy_confirmed` (R-2 no longer makes any: it reads the memory).
        self.confirm_calls: List[Tuple[str, str]] = []
        #: What a real sender's memory is: ONE `SendMemory` per worker (`make_worker` hands this one to
        #: the worker). R-2 reads the confirmed copies from it, never from the sender (B1 fix 1c).
        self.memory = SendMemory()
        #: `expanded_from` of every dataset attempted (`None` for a configured pair).
        self.expanded: Dict[str, object] = {}

    @property
    def confirmed(self) -> Set[Tuple[str, str]]:
        """The pairs this sender confirmed: the memory's live `COPY_CONFIRMED` latch."""
        return self.memory.latch_view(COPY_CONFIRMED)

    @property
    def _memory(self):
        """What `RemoteBackupWorker._shares_memory` reads: the sender uses the worker's one memory."""
        return self.memory

    def begin_cycle(self):
        pass

    def report_client_cert_expiry(self):
        pass

    def close(self):
        pass

    def backup_dataset(self, dsi, remote_cfg, should_stop=None):
        self.attempts.append((dsi.name, remote_cfg.destination))
        self.expanded[dsi.name] = dsi.expanded_from
        self._acknowledged(dsi.name, remote_cfg.destination)
        self.confirmed.add((dsi.name, remote_cfg.destination))
        return True

    def copy_confirmed(self, dataset, destination):
        self.confirm_calls.append((dataset, destination))
        return (dataset, destination) in self.confirmed

    def _acknowledged(self, dataset, destination):
        if self.world is not None and self.world.snaps.get(dataset):
            self.world.set_prop(dataset, f'{PROP_ANCHOR_PREFIX}{destination}', self.world.snaps[dataset][-1])

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

    def test_a_child_without_a_local_anchor_is_due(self, zfs_world, monkeypatch):
        anchored_pair(zfs_world, stamp(timedelta(minutes=10)))
        cfg = config(dataset(PARENT, recursive=True), dataset(CHILD))
        sender = RecordingSender()
        make_rig(monkeypatch, cfg, sender).run()
        # the parent goes first (R-2); the child, released by its confirmation, has no anchor of its own
        assert (CHILD, DEST) in sender.attempts, 'the child has no anchor of its own: it is due'

    def test_a_parent_with_a_fresh_local_anchor_is_sent_when_the_schedule_dispatches_it(self, zfs_world, monkeypatch):
        """The schedule decides when a pair sends: every dispatch is forced past the anchor-age check (only the
        re-run of a slot after a clock step back keeps it), so a LOCAL fresh anchor does not stop a dispatch. The
        anchor is still READ (`-s local`), so a future-dated one is announced."""
        anchored_pair(zfs_world, stamp(timedelta(minutes=10)))
        sender = RecordingSender()
        make_rig(monkeypatch, config(dataset(PARENT)), sender).run()
        assert sender.attempts == [(PARENT, DEST)]

    def test_the_anchor_age_check_itself_still_calls_that_parent_not_due(self, zfs_world):
        """The control, `force_confirm=False` (what the clamp's re-run of a slot gets): the same fake, a LOCAL
        fresh anchor, nothing waiting on it -> NOT_DUE and nothing sent."""
        anchored_pair(zfs_world, stamp(timedelta(minutes=10)))
        sender = RecordingSender()
        manager = DatasetManager(config(dataset(PARENT)))
        dsi = manager.datasets[0]
        outcome = send_pair(manager, dsi, dsi.config.remote[0], sender, lambda: False, lambda: False)
        assert outcome.kind == PAIR_NOT_DUE and sender.attempts == []


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

    Path: `RemoteBackupWorker.run` -> the scheduler's discovery thread -> `DatasetManager.expand_recursive(parent=)`.
    Before A2 only the configured parent was attempted. Now: one `backup_dataset` attempt per
    child x destination, after the parent."""

    @staticmethod
    def one_cycle(cfg, monkeypatch, world=None):
        """The real remote worker's scheduler, until it is quiet: every pair attempted once."""
        sender = RecordingSender(world)
        make_rig(monkeypatch, cfg, sender).run()
        return sender

    def tree(self, world):
        snap = stamp(timedelta(minutes=10))
        world.add(PARENT, snaps=[snap])
        world.add(f'{PARENT}/fs', 'filesystem', snaps=[snap])
        world.add(f'{PARENT}/vol', 'volume', snaps=[snap])

    def test_each_descendant_is_sent_to_each_destination_after_its_parent(self, zfs_world, monkeypatch):
        self.tree(zfs_world)
        sender = self.one_cycle(config(dataset(PARENT, recursive=True)), monkeypatch, zfs_world)

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
    def test_only_a_local_fresh_anchor_makes_the_child_not_due(self, zfs_world, monkeypatch, caplog, source):
        """The anchor-age check (`force_confirm=False`, the one a clock step back's re-run keeps): only the child's
        OWN (local) anchor can say "not due"; an inherited or received one never does."""
        old, new = two_snapshot_pair(zfs_world)
        give(zfs_world, source, ANCHOR, new)
        sender = RecordingSender()
        manager = DatasetManager(config(dataset(CHILD)))
        dsi = manager.datasets[0]
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            outcome = send_pair(manager, dsi, dsi.config.remote[0], sender, lambda: False, lambda: False)

        assert outcome.kind == (PAIR_NOT_DUE if source == 'local' else PAIR_DONE)
        assert sender.attempts == ([] if source == 'local' else [(CHILD, DEST)])
        # one `-s local` read of the child's own anchor, and no effective read of it
        assert [a for a in zfs_world.value_reads(CHILD) if a[-2] == ANCHOR] == [
            ['get', '-H', '-o', 'value', '-s', 'local', ANCHOR, CHILD]]

    @pytest.mark.parametrize('source', SOURCES)
    def test_a_scheduled_dispatch_sends_whatever_the_source_of_the_anchor(
            self, zfs_world, monkeypatch, caplog, source):
        """Through the real loop every dispatch is forced: the child is sent for every source of its anchor, and
        its own anchor is still read, once, as a `-s local` value."""
        old, new = two_snapshot_pair(zfs_world)
        give(zfs_world, source, ANCHOR, new)
        sender = RecordingSender()
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            make_rig(monkeypatch, config(dataset(CHILD)), sender).run()
        assert sender.attempts == [(CHILD, DEST)]
        assert [a for a in zfs_world.value_reads(CHILD) if a[-2] == ANCHOR] == [
            ['get', '-H', '-o', 'value', '-s', 'local', ANCHOR, CHILD]]

    def test_an_unreadable_anchor_is_due_with_a_debug_line_and_no_warning(self, zfs_world, monkeypatch, caplog):
        old, new = two_snapshot_pair(zfs_world)
        give(zfs_world, 'unreadable', ANCHOR, new)
        sender = RecordingSender()
        with caplog.at_level(logging.DEBUG, logger='zfsbackup.backup_manager'):
            make_rig(monkeypatch, config(dataset(CHILD)), sender).run()
        assert sender.attempts == [(CHILD, DEST)]
        assert len(debug_reads(caplog)) == 1 and 'pool I/O is currently suspended' in debug_reads(caplog)[0].getMessage()
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


@pytest.mark.unit
class TestA1MatrixHealth:
    """`health_collect._observe_local` reads the anchor and the pin as LOCAL values (`-s local`) of the
    dataset itself; since A3 through the RAISING `get_local_anchor` / `get_local_inflight`."""

    @staticmethod
    def child_anchor(observations):
        (obs,) = [o for o in observations if getattr(o, 'source', None) == 'anchor' and o.subject == CHILD]
        return obs

    @pytest.mark.parametrize('source', SOURCES)
    def test_the_anchor(self, zfs_world, source):
        old, new = two_snapshot_pair(zfs_world)
        give(zfs_world, source, ANCHOR, new)
        obs = self.child_anchor(collect(config(dataset(PARENT, recursive=True), dataset(CHILD))))
        now = datetime.now(timezone.utc).timestamp()
        if source == 'unreadable':
            # A3: the raising read. "could not be read" is an error observation (CRIT "cannot read"),
            # not "never sent" (the child shows no anchor, and no WARN "not yet sent" either)
            assert obs.newest_ts is None
            assert obs.error and obs.error.startswith('the anchor could not be read: ')
            assert 'pool I/O is currently suspended' in obs.error
            (entry,) = evaluate([obs], now)
            assert entry.state == STATE_CRIT and entry.reason.startswith('cannot read: ')
            return
        assert obs.error is None
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
        if source == 'unreadable':
            # an unreadable PIN does not discard the (here absent) anchor: no error observation, the
            # detail says the pin could not be read, never "no pin"
            assert obs.error is None
            assert obs.detail.startswith('send pin could not be read: ')
            assert 'pool I/O is currently suspended' in obs.detail

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


# =============================================================================================
# A2: `DatasetManager.expand_recursive`
# =============================================================================================

import base64  # noqa: E402
import dataclasses  # noqa: E402
import json  # noqa: E402

from zfsbackup.backup_manager import Expansion  # noqa: E402
from zfsbackup.config import RemoteServerConfig, RetentionRule  # noqa: E402

BACKUP_LOGGER = 'zfsbackup.backup_manager'
WORKER_LOGGER = 'zfsbackup.workers'


def rdataset(name, destinations=(DEST,), **kw) -> DatasetConfig:
    """A dataset config sending to every destination in `destinations` (1h frequency)."""
    kw.setdefault('frequency', Duration('1h'))
    return DatasetConfig(
        name=name, remote=[RemoteDatasetConfig(destination=d) for d in destinations], **kw
    )


def add_tree(world: InheritingZfs, *names, snaps=True, dstype='filesystem') -> None:
    """Add every name (the parents implied by `a/b/c` need their own entry), each with one snapshot."""
    snap = stamp(timedelta(minutes=10))
    for name in names:
        world.add(name, dstype, snaps=[snap] if snaps else [])


def expand(cfg, should_stop=None):
    manager = DatasetManager(cfg)
    return manager, manager.expand_recursive(should_stop)


def child_names(expansion) -> List[str]:
    return [c.name for c in expansion.children]


def records_at(caplog, level, logger=BACKUP_LOGGER):
    return [r.getMessage() for r in caplog.records if r.name == logger and r.levelno == level]


def decode(prop: str) -> dict:
    return json.loads(base64.b64decode(prop))


@pytest.mark.unit
class TestExpandTrees:
    """What is listed, and how: kinds, depth, the exact command."""

    def test_filesystems_and_volumes_below_the_parent_become_children(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/fs')
        add_tree(zfs_world, f'{PARENT}/vol', dstype='volume')
        manager, exp = expand(config(dataset(PARENT, recursive=True)))

        assert isinstance(exp, Expansion) and exp.failed == {}
        assert sorted(child_names(exp)) == [f'{PARENT}/fs', f'{PARENT}/vol']
        assert PARENT not in child_names(exp), 'the parent is a configured pair, not a child'
        assert all(c.expanded_from == PARENT for c in exp.children)

    def test_a_deep_tree_is_expanded_to_every_level(self, zfs_world):
        names = [f'{PARENT}/a', f'{PARENT}/a/b', f'{PARENT}/a/b/c', f'{PARENT}/a/b/c/d', f'{PARENT}/e']
        add_tree(zfs_world, PARENT, *names)
        _, exp = expand(config(dataset(PARENT, recursive=True)))
        assert sorted(child_names(exp)) == sorted(names)
        listed = child_names(exp)
        for name in names:                       # zfs prints a parent before its descendants
            parent = name.rpartition('/')[0]
            assert parent == PARENT or listed.index(parent) < listed.index(name)

    def test_a_dataset_that_is_not_configured_recursive_has_no_children(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/a')
        _, exp = expand(config(dataset(PARENT)))
        assert exp == Expansion([], {})
        assert zfs_world.calls == [], 'nothing is asked of zfs when no dataset is a recursive root'

    @pytest.mark.parametrize('kw', [
        pytest.param(dict(enabled=False), id='disabled'),
        pytest.param(dict(remote=[]), id='no-remote'),
    ])
    def test_a_recursive_dataset_that_is_disabled_or_has_no_remote_is_no_root(self, zfs_world, kw):
        add_tree(zfs_world, PARENT, f'{PARENT}/a')
        cfg = dataset(PARENT, recursive=True)
        cfg = dataclasses.replace(cfg, **kw)
        _, exp = expand(config(cfg))
        assert exp == Expansion([], {}) and zfs_world.calls == []

    def test_the_listing_is_one_untimed_name_only_command_per_root(self, zfs_world, mocker):
        add_tree(zfs_world, PARENT, f'{PARENT}/a', f'{PARENT}/a/b')
        timeout = mocker.spy(libzfseasy_module, 'timeout')
        expand(config(dataset(PARENT, recursive=True)))
        call, _copy_read = zfs_world.calls
        assert call[0] == 'list' and call[-1] == PARENT
        assert '-r' in call and call[call.index('-t') + 1] == 'filesystem,volume'
        assert call[call.index('-o') + 1] == 'name,type', 'names and types only: no property to forge'
        assert timeout.call_count == 0, 'a source dataset is never read under a timeout'

    def test_the_backup_copy_read_is_one_value_free_local_recursive_get_per_root(self, zfs_world, mocker):
        add_tree(zfs_world, PARENT, f'{PARENT}/a', 'pool/other', 'pool/other/b')
        timeout = mocker.spy(libzfseasy_module, 'timeout')
        expand(config(dataset(PARENT, recursive=True), dataset('pool/other', recursive=True)))
        assert zfs_world.name_sources_reads() == [
            copy_read_argv(root)
            for root in (PARENT, 'pool/other')], 'name, property and source only: no value is ever read'
        assert [c[0] for c in zfs_world.calls] == ['list', 'get', 'list', 'get'], 'the listing first, then the read'
        assert timeout.call_count == 0

    def test_children_carry_no_snapshots_until_listed(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/a')
        manager, exp = expand(config(dataset(PARENT, recursive=True)))
        assert [d.name for d in manager.datasets] == [PARENT], 'the manager keeps configured datasets only'
        assert exp.children[0].snapshots == []

    def test_expanding_twice_gives_equal_results(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/a', f'{PARENT}/b')
        manager = DatasetManager(config(dataset(PARENT, recursive=True)))
        first, second = manager.expand_recursive(), manager.expand_recursive()
        assert child_names(first) == child_names(second)
        assert [d.name for d in manager.datasets] == [PARENT]


@pytest.mark.unit
class TestExpandOwnership:
    """Which configured ancestor a name belongs to; explicit configuration, R-1."""

    def test_the_nearest_enabled_recursive_root_with_remote_wins(self, zfs_world):
        mid = f'{PARENT}/mid'
        add_tree(zfs_world, PARENT, f'{PARENT}/top', mid, f'{mid}/x', f'{mid}/x/y')
        cfg = config(
            rdataset(PARENT, recursive=True),
            rdataset(mid, ['other'], recursive=True),
            destinations={DEST: Destination(url='https://h:8443'), 'other': Destination(url='https://o:8443')},
        )
        _, exp = expand(cfg)
        owners = {c.name: c.expanded_from for c in exp.children}
        assert owners == {f'{PARENT}/top': PARENT, f'{mid}/x': mid, f'{mid}/x/y': mid}
        by_name = {c.name: c for c in exp.children}
        assert [r.destination for r in by_name[f'{mid}/x'].config.remote] == ['other']
        assert [r.destination for r in by_name[f'{PARENT}/top'].config.remote] == [DEST]
        assert len(child_names(exp)) == len(set(child_names(exp))), 'a pair is unique by dataset'

    def test_each_root_is_listed_once_and_its_nested_root_lists_its_own_subtree(self, zfs_world):
        mid = f'{PARENT}/mid'
        add_tree(zfs_world, PARENT, mid, f'{mid}/x')
        expand(config(rdataset(PARENT, recursive=True), rdataset(mid, recursive=True)))
        assert [c[-1] for c in zfs_world.name_lists()] == [PARENT, mid]

    def test_an_enabled_recursive_descendant_without_remote_ends_the_walk(self, zfs_world):
        # `remote: []` on a recursive entry means "snapshot locally, send nothing": its subtree is
        # not the outer parent's to send.
        mid = f'{PARENT}/mid'
        add_tree(zfs_world, PARENT, f'{PARENT}/top', mid, f'{mid}/x', f'{mid}/x/y')
        cfg = config(rdataset(PARENT, recursive=True), DatasetConfig(name=mid, recursive=True))
        _, exp = expand(cfg)
        assert child_names(exp) == [f'{PARENT}/top']

    def test_an_enabled_non_recursive_descendant_without_remote_is_passed_through(self, zfs_world):
        mid = f'{PARENT}/mid'
        add_tree(zfs_world, PARENT, mid, f'{mid}/x')
        cfg = config(rdataset(PARENT, recursive=True), DatasetConfig(name=mid))
        _, exp = expand(cfg)
        assert {c.name: c.expanded_from for c in exp.children} == {f'{mid}/x': PARENT}

    def test_an_explicitly_configured_descendant_is_not_expanded(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/a', f'{PARENT}/b')
        _, exp = expand(config(rdataset(PARENT, recursive=True), dataset(f'{PARENT}/a')))
        assert child_names(exp) == [f'{PARENT}/b']

    def test_a_configured_non_recursive_descendants_children_expand_from_the_recursive_ancestor(
        self, zfs_world,
    ):
        a = f'{PARENT}/a'
        add_tree(zfs_world, PARENT, a, f'{a}/x', f'{a}/x/y')
        cfg = config(
            rdataset(PARENT, ['other', DEST], recursive=True),
            rdataset(a, [DEST]),                           # enabled, NOT recursive, its own remote
            destinations={DEST: Destination(url='https://h:8443'), 'other': Destination(url='https://o:8443')},
        )
        _, exp = expand(cfg)
        by_name = {c.name: c for c in exp.children}
        assert sorted(by_name) == [f'{a}/x', f'{a}/x/y'], 'a itself is explicit, so not expanded'
        for child in by_name.values():
            assert child.expanded_from == PARENT
            # the ancestor's destinations and frequency, not the explicit entry's
            assert [r.destination for r in child.config.remote] == ['other', DEST]

    def test_a_disabled_descendant_excludes_its_whole_subtree(self, zfs_world):
        off, ok = f'{PARENT}/off', f'{PARENT}/ok'
        add_tree(zfs_world, PARENT, off, f'{off}/x', f'{off}/x/y', f'{off}/z', ok, f'{ok}/w')
        cfg = config(rdataset(PARENT, recursive=True), dataclasses.replace(dataset(off), enabled=False))
        _, exp = expand(cfg)
        assert sorted(child_names(exp)) == [ok, f'{ok}/w']

    def test_a_disabled_recursive_descendant_excludes_its_subtree_too(self, zfs_world):
        off = f'{PARENT}/off'
        add_tree(zfs_world, PARENT, off, f'{off}/x')
        cfg = config(rdataset(PARENT, recursive=True), dataclasses.replace(dataset(off, recursive=True), enabled=False))
        _, exp = expand(cfg)
        assert child_names(exp) == []

    def test_an_enabled_root_below_a_disabled_descendant_still_expands_its_own_subtree(self, zfs_world):
        # Explicit configuration wins: the enabled recursive entry is its own root even inside an
        # excluded subtree; what the disabled entry excludes is what only the outer root would add.
        off, inner = f'{PARENT}/off', f'{PARENT}/off/inner'
        add_tree(zfs_world, PARENT, off, f'{off}/x', inner, f'{inner}/leaf')
        cfg = config(
            rdataset(PARENT, recursive=True),
            dataclasses.replace(dataset(off), enabled=False),
            rdataset(inner, recursive=True),
        )
        _, exp = expand(cfg)
        assert {c.name: c.expanded_from for c in exp.children} == {f'{inner}/leaf': inner}

    def test_a_configured_dataset_is_excluded_even_when_listed_by_two_roots(self, zfs_world):
        mid = f'{PARENT}/mid'
        add_tree(zfs_world, PARENT, mid, f'{mid}/x')
        _, exp = expand(config(rdataset(PARENT, recursive=True), rdataset(mid, recursive=True)))
        assert child_names(exp) == [f'{mid}/x'], 'mid is configured; x is mid\'s, once'


@pytest.mark.unit
class TestExpandSkippedNames:
    def test_names_out_of_scope_or_with_snapshot_or_bookmark_marks_are_skipped(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/good')
        zfs_world.list_extra[PARENT] = [
            (f'{PARENT}2/lookalike', 'filesystem'),        # shares the parent's prefix, not its subtree
            ('pool/other/x', 'filesystem'),
            (f'{PARENT}/s@snap', 'filesystem'),
            (f'{PARENT}/b#mark', 'filesystem'),
            (f'{PARENT}/r%recv', 'filesystem'),
        ]
        _, exp = expand(config(dataset(PARENT, recursive=True)))
        assert child_names(exp) == [f'{PARENT}/good']
        assert exp.failed == {}

    def test_a_name_listed_twice_is_never_two_children(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/good', f'{PARENT}/other')
        zfs_world.list_extra[PARENT] = [(f'{PARENT}/good', 'filesystem'), (PARENT, 'filesystem')]
        _, exp = expand(config(dataset(PARENT, recursive=True)))
        assert child_names(exp).count(f'{PARENT}/good') <= 1      # the reader fails closed on a duplicate
        assert PARENT not in child_names(exp) and f'{PARENT}/other' in child_names(exp)

    def test_a_name_libzfseasy_cannot_represent_is_skipped(self, zfs_world, caplog):
        add_tree(zfs_world, PARENT, f'{PARENT}/good')
        zfs_world.list_extra[PARENT] = [(f'{PARENT}/bad\x01name', 'filesystem'), (f'{PARENT}/caf\u00e9', 'filesystem'), (f'{PARENT}/new\tline', 'filesystem')]
        with caplog.at_level(logging.WARNING):
            _, exp = expand(config(dataset(PARENT, recursive=True)))
        # ascii() messages, and no surrogate in the fake's output: a lone surrogate in a failure report
        # (or in a captured log line) takes an xdist worker down
        assert ascii(child_names(exp)) == ascii([f'{PARENT}/good']), ascii(child_names(exp))
        assert exp.failed == {}, ascii(exp.failed)

    def test_a_non_filesystem_row_is_not_a_child(self, zfs_world):
        add_tree(zfs_world, PARENT)
        zfs_world.list_extra[PARENT] = [(f'{PARENT}/snapshot-typed', 'snapshot')]
        _, exp = expand(config(dataset(PARENT, recursive=True)))
        assert child_names(exp) == []


@pytest.mark.unit
class TestExpandReceiveTargets:
    """A name at or below a receive target is never a source (validation refuses such a config; this
    is the defence in depth): one ERROR per root and target and episode, and its subtree is skipped."""

    TARGET_DS = f'{PARENT}/bk'

    def tree(self, world):
        add_tree(world, PARENT, f'{PARENT}/ok', self.TARGET_DS, f'{self.TARGET_DS}/c1',
                 f'{self.TARGET_DS}/c1/x', f'{PARENT}/bkx')

    def server_config(self, **kw):
        return config(rdataset(PARENT, recursive=True),
                      remote_backup=RemoteServerConfig(target_dataset=self.TARGET_DS + kw.pop('slash', ''), **kw))

    def local_config(self, name='vault', url=None):
        dests = {DEST: Destination(url='https://h:8443'), name: Destination(url=url or f'zfs:{self.TARGET_DS}')}
        return config(rdataset(PARENT, recursive=True), destinations=dests)

    @pytest.mark.parametrize('flavour', ['server', 'server-trailing-slash', 'local'])
    def test_the_target_and_everything_below_it_is_skipped_with_one_error(self, zfs_world, caplog, flavour):
        self.tree(zfs_world)
        cfg = {'server': lambda: self.server_config(),
               'server-trailing-slash': lambda: self.server_config(slash='/'),
               'local': lambda: self.local_config()}[flavour]()
        manager = DatasetManager(cfg)
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            exp = manager.expand_recursive()
        assert sorted(child_names(exp)) == [f'{PARENT}/bkx', f'{PARENT}/ok'], 'the lookalike name is no target'
        (error,) = records_at(caplog, logging.ERROR)
        assert self.TARGET_DS in error and 'receives copies' in error and PARENT in error

    def test_the_error_is_once_per_episode_and_comes_back_with_a_new_one(self, zfs_world, caplog):
        self.tree(zfs_world)
        manager = DatasetManager(self.server_config())
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            for _ in range(3):
                manager.expand_recursive()
            assert len(records_at(caplog, logging.ERROR)) == 1
            for name in [n for n in zfs_world.datasets if n == self.TARGET_DS or n.startswith(self.TARGET_DS + '/')]:
                del zfs_world.datasets[name]               # the offending datasets are gone: episode over
            manager.expand_recursive()
            assert len(records_at(caplog, logging.ERROR)) == 1
            zfs_world.add(self.TARGET_DS)                  # and back
            manager.expand_recursive()
        assert len(records_at(caplog, logging.ERROR)) == 2

    def test_a_disabled_server_target_is_still_a_receive_target(self, zfs_world, caplog):
        # a former server still holds other clients' received copies there
        self.tree(zfs_world)
        manager = DatasetManager(self.server_config(enabled=False))
        with caplog.at_level(logging.ERROR, logger=BACKUP_LOGGER):
            exp = manager.expand_recursive()
        assert sorted(child_names(exp)) == [f'{PARENT}/bkx', f'{PARENT}/ok']
        (error,) = records_at(caplog, logging.ERROR)
        assert self.TARGET_DS in error and 'receives copies' in error

    def test_a_target_elsewhere_changes_nothing(self, zfs_world, caplog):
        self.tree(zfs_world)
        cfg = self.local_config(url='zfs:other/vault')
        with caplog.at_level(logging.ERROR, logger=BACKUP_LOGGER):
            exp = DatasetManager(cfg).expand_recursive()
        assert len(exp.children) == 5 and records_at(caplog, logging.ERROR) == []

    def test_one_error_per_target_when_there_are_two(self, zfs_world, caplog):
        self.tree(zfs_world)
        zfs_world.add(f'{PARENT}/vault2')
        cfg = config(rdataset(PARENT, recursive=True),
                     destinations={DEST: Destination(url='https://h:8443'),
                                   'a': Destination(url=f'zfs:{self.TARGET_DS}'),
                                   'b': Destination(url=f'zfs:{PARENT}/vault2')},
                     )
        with caplog.at_level(logging.ERROR, logger=BACKUP_LOGGER):
            DatasetManager(cfg).expand_recursive()
        assert len(records_at(caplog, logging.ERROR)) == 2


@pytest.mark.unit
class TestExpandBackupCopies:
    """A dataset with a LOCAL `org.zfsbackup:client_id` or `source_dataset` is a backup copy: it and its
    whole subtree are never sources (one value-free `zfs get -r -s local` per parent, name/property/source)."""

    CID, SRC = 'org.zfsbackup:client_id', 'org.zfsbackup:source_dataset'
    COPY = f'{PARENT}/copy'

    def tree(self, world):
        add_tree(world, PARENT, f'{PARENT}/ok', self.COPY, f'{self.COPY}/c1', f'{self.COPY}/c1/x', f'{PARENT}/copyx')

    def errors(self, caplog):
        return records_at(caplog, logging.ERROR)

    @pytest.mark.parametrize('prop', [CID, SRC])
    def test_a_local_copy_marker_skips_it_and_its_whole_subtree_with_one_error(self, zfs_world, caplog, prop):
        self.tree(zfs_world)
        zfs_world.set_prop(self.COPY, prop, 'whatever')
        manager = DatasetManager(config(dataset(PARENT, recursive=True)))
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            exp = manager.expand_recursive()
        assert sorted(child_names(exp)) == [f'{PARENT}/copyx', f'{PARENT}/ok'], 'a prefix lookalike is no copy'
        (error,) = self.errors(caplog)
        assert self.COPY in error and 'backup copy' in error and f'zfs inherit {self.CID} {self.COPY}' in error

    def test_the_marker_deep_in_the_tree_skips_its_subtree_only(self, zfs_world, caplog):
        self.tree(zfs_world)
        zfs_world.set_prop(f'{self.COPY}/c1', self.CID, 'x')
        exp = DatasetManager(config(dataset(PARENT, recursive=True))).expand_recursive()
        assert sorted(child_names(exp)) == sorted([f'{PARENT}/ok', self.COPY, f'{PARENT}/copyx'])

    def test_one_error_per_topmost_copy_root_however_many_nested_markers(self, zfs_world, caplog):
        self.tree(zfs_world)
        for name in (self.COPY, f'{self.COPY}/c1', f'{self.COPY}/c1/x'):
            zfs_world.set_prop(name, self.CID, 'x')
        zfs_world.set_prop(f'{PARENT}/ok', self.SRC, 'y')                   # a second, separate copy root
        manager = DatasetManager(config(dataset(PARENT, recursive=True)))
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            for _ in range(3):
                exp = manager.expand_recursive()
        assert sorted(child_names(exp)) == [f'{PARENT}/copyx']
        assert len(self.errors(caplog)) == 2
        assert any(f'{self.COPY} carries' in e for e in self.errors(caplog))
        assert not any(f'{self.COPY}/c1 carries' in e for e in self.errors(caplog))

    def test_a_copy_root_equal_to_the_parent_skips_every_child(self, zfs_world, caplog):
        self.tree(zfs_world)
        zfs_world.set_prop(PARENT, self.CID, 'x')
        manager = DatasetManager(config(dataset(PARENT, recursive=True)))
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            exp = manager.expand_recursive()
        assert exp.children == [] and exp.failed == {}
        (error,) = self.errors(caplog)
        assert error.startswith(f'{PARENT} carries')

    def test_other_zfsbackup_properties_do_not_make_a_copy(self, zfs_world, caplog):
        self.tree(zfs_world)
        for prop in ('org.zfsbackup:config', 'org.zfsbackup:anchor.off', 'org.zfsbackup:inflight.off',
                     'org.zfsbackup:client_idx', 'com.example:client_id'):
            zfs_world.set_prop(f'{PARENT}/ok', prop, 'v')
        with caplog.at_level(logging.ERROR, logger=BACKUP_LOGGER):
            exp = DatasetManager(config(dataset(PARENT, recursive=True))).expand_recursive()
        assert len(exp.children) == 5 and self.errors(caplog) == []

    def test_the_read_asks_for_local_received_and_inherited_and_no_value(self, zfs_world):
        self.tree(zfs_world)
        DatasetManager(config(dataset(PARENT, recursive=True))).expand_recursive()
        assert zfs_world.name_sources_reads() == [
            copy_read_argv(PARENT)]

    def test_a_replica_with_received_markers_is_never_sent(self, zfs_world, caplog):
        # what `zfs send -R | zfs receive` leaves: the markers are RECEIVED, and `zfs inherit` cannot clear them
        self.tree(zfs_world)
        for prop in (self.CID, self.SRC):
            zfs_world.received[(self.COPY, prop)] = 'orig'
        manager = DatasetManager(config(dataset(PARENT, recursive=True)))
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            for _ in range(2):
                exp = manager.expand_recursive()
        assert sorted(child_names(exp)) == [f'{PARENT}/copyx', f'{PARENT}/ok']
        (error,) = self.errors(caplog)
        assert f'{self.COPY} carries a received' in error and 'zfs inherit` (`zfs inherit -S`' in error
        assert f'zfs rename {self.COPY}' in error

    def test_a_marker_the_child_only_inherits_from_the_copy_root_names_the_copy_root_once(self, zfs_world, caplog):
        self.tree(zfs_world)
        zfs_world.set_prop(self.COPY, self.CID, 'x')                  # c1, x inherit it
        manager = DatasetManager(config(dataset(PARENT, recursive=True)))
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            exp = manager.expand_recursive()
        assert sorted(child_names(exp)) == [f'{PARENT}/copyx', f'{PARENT}/ok']
        (error,) = self.errors(caplog)
        assert error.startswith(f'{self.COPY} carries a local')

    def test_a_parent_inside_a_copy_inherits_the_marker_and_sends_no_descendant(self, zfs_world, caplog):
        self.tree(zfs_world)
        zfs_world.add('pool')
        zfs_world.set_prop('pool', self.CID, 'x')                     # above the parent: the parent inherits it
        manager = DatasetManager(config(dataset(PARENT, recursive=True)))
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            exp = manager.expand_recursive()
        assert exp.children == [] and exp.failed == {}
        (error,) = self.errors(caplog)
        assert error.startswith(f'{PARENT} itself is inside a backup copy') and 'inherits' in error
        assert 'none of its descendants is sent' in error

    def test_a_received_marker_above_the_parent_counts_too(self, zfs_world, caplog):
        self.tree(zfs_world)
        zfs_world.add('pool')
        zfs_world.received[('pool', self.SRC)] = 'x'
        with caplog.at_level(logging.ERROR, logger=BACKUP_LOGGER):
            exp = DatasetManager(config(dataset(PARENT, recursive=True))).expand_recursive()
        assert exp.children == [] and len(self.errors(caplog)) == 1

    def test_copy_roots_maps_each_name_to_its_source_and_local_or_received_beats_inherited(self, zfs_world):
        self.tree(zfs_world)
        zfs_world.set_prop(PARENT, self.CID, 'x')                       # local on the parent; children inherit it
        zfs_world.set_prop(self.COPY, self.SRC, 'y')                    # local on a child, which also inherits
        zfs_world.received[(f'{PARENT}/ok', self.SRC)] = 'z'
        roots = DatasetManager._copy_roots(PARENT)
        assert roots[PARENT] == 'local' and roots[self.COPY] == 'local' and roots[f'{PARENT}/ok'] == 'received'
        assert roots[f'{self.COPY}/c1'] == f'inherited from {PARENT}'
        assert roots[f'{self.COPY}/c1/x'] == f'inherited from {PARENT}'

    def test_copy_roots_ignores_every_other_property(self, zfs_world):
        self.tree(zfs_world)
        for prop in ('org.zfsbackup:config', 'org.zfsbackup:anchor.off', 'com.example:client_id'):
            zfs_world.set_prop(self.COPY, prop, 'v')
            zfs_world.received[(f'{PARENT}/ok', prop)] = 'v'
        assert DatasetManager._copy_roots(PARENT) == {}

    @pytest.mark.parametrize('source, needles', [
        ('local', ['carries a local', 'zfs inherit org.zfsbackup:client_id pool/data/copy', 'zfs rename']),
        ('received', ['carries a received', 'is not removed by `zfs inherit`', 'move the copy out of pool/data',
                      'a plain `zfs inherit` of a received value only masks it',
                      'a masked marker makes this check stop seeing the copy']),
        ('inherited from pool', ['itself is inside a backup copy', 'inherits', 'from pool', 'none of its descendants is sent',
                                 'works only for local values (a plain `zfs inherit` of a received value only masks it']),
    ])
    def test_the_three_wordings(self, source, needles):
        message = DatasetManager._copy_root_message(PARENT, self.COPY if source != 'inherited from pool' else PARENT, source)
        for needle in needles:
            assert needle in message, (needle, message)
        others = {'local': 'received', 'received': 'local'}
        if source in others:
            assert f'carries a {others[source]}' not in message

    def test_the_episode_ends_when_the_marker_goes_and_a_new_one_is_announced(self, zfs_world, caplog):
        self.tree(zfs_world)
        manager = DatasetManager(config(dataset(PARENT, recursive=True)))
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            zfs_world.set_prop(self.COPY, self.CID, 'x')
            manager.expand_recursive()
            assert len(self.errors(caplog)) == 1
            del zfs_world.props[(self.COPY, self.CID)]
            exp = manager.expand_recursive()
            assert self.COPY in child_names(exp), 'cleared: it is a source again'
            zfs_world.set_prop(self.COPY, self.CID, 'x')
            manager.expand_recursive()
        assert len(self.errors(caplog)) == 2

    def test_a_failing_copy_read_fails_the_parents_expansion(self, zfs_world, caplog):
        self.tree(zfs_world)
        zfs_world.copy_read_failures[PARENT] = 'pool I/O is currently suspended'
        manager = DatasetManager(config(dataset(PARENT, recursive=True)))
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            for _ in range(3):
                exp = manager.expand_recursive()
            assert exp.children == [] and list(exp.failed) == [PARENT]
            assert 'i/o is currently suspended' in exp.failed[PARENT].lower()
            (error,) = self.errors(caplog)
            assert 'Cannot list the descendants' in error
            del zfs_world.copy_read_failures[PARENT]
            exp = manager.expand_recursive()
        assert exp.failed == {} and len(exp.children) == 5, 'never expanded without the read, always with it'
        (info,) = records_at(caplog, logging.INFO)
        assert 'listed again' in info

    def test_the_read_follows_the_listing_so_a_failed_listing_never_reads(self, zfs_world):
        self.tree(zfs_world)
        zfs_world.list_failures[PARENT] = 'boom'
        DatasetManager(config(dataset(PARENT, recursive=True))).expand_recursive()
        assert zfs_world.name_sources_reads() == []

    def test_one_failing_parent_does_not_stop_the_next_one(self, zfs_world):
        self.tree(zfs_world)
        add_tree(zfs_world, 'pool/other', 'pool/other/b')
        zfs_world.copy_read_failures[PARENT] = 'boom'
        exp = DatasetManager(config(dataset(PARENT, recursive=True), dataset('pool/other', recursive=True))).expand_recursive()
        assert child_names(exp) == ['pool/other/b'] and list(exp.failed) == [PARENT]


@pytest.mark.unit
class TestExpandedConfigIsOwnedByTheChild:
    """Each child owns copies of the parent's `retention_rules` and `remote` (mutable dataclasses)."""

    def make(self, world):
        add_tree(world, PARENT, A, f'{PARENT}/b')
        parent = DatasetConfig(
            name=PARENT, recursive=True, frequency=Duration('1h'),
            retention_rules=[RetentionRule(Duration('1h'), Duration('2d'))],
            remote=[RemoteDatasetConfig(destination=DEST, retention_rules=[RetentionRule(Duration('1d'), Duration('9d'))])])
        _, exp = expand(config(parent))
        return parent, {c.name: c for c in exp.children}

    def test_the_lists_and_their_elements_are_not_shared(self, zfs_world):
        parent, kids = self.make(zfs_world)
        a, b = kids[A], kids[f'{PARENT}/b']
        assert a.config.retention_rules == parent.retention_rules and a.config.remote == parent.remote
        for mine, theirs in ((a.config.retention_rules, parent.retention_rules),
                             (a.config.remote, parent.remote),
                             (a.config.remote[0].retention_rules, parent.remote[0].retention_rules),
                             (a.config.retention_rules, b.config.retention_rules)):
            assert mine is not theirs and mine[0] is not theirs[0]

    def test_changing_a_child_leaves_the_parent_and_its_siblings_alone(self, zfs_world):
        parent, kids = self.make(zfs_world)
        a, b = kids[A], kids[f'{PARENT}/b']
        a.config.retention_rules.append(RetentionRule(Duration('2d'), Duration('3d')))
        a.config.retention_rules[0].keep_for = Duration('99d')
        a.config.remote.append(RemoteDatasetConfig(destination='x'))
        a.config.remote[0].retention_rules.clear()
        a.config.remote[0].frequency = Duration('5m')
        assert len(parent.retention_rules) == 1 and parent.retention_rules[0].keep_for == Duration('2d')
        assert [r.destination for r in parent.remote] == [DEST] and len(parent.remote[0].retention_rules) == 1
        assert parent.remote[0].frequency is None
        assert len(b.config.retention_rules) == 1 and len(b.config.remote[0].retention_rules) == 1


@pytest.mark.unit
class TestExpandDiscoveryFailure:
    def test_a_failing_listing_is_reported_in_failed_and_nothing_is_raised(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/a')
        zfs_world.list_failures[PARENT] = 'cannot open: pool I/O is currently suspended'
        manager, exp = expand(config(dataset(PARENT, recursive=True)))
        assert exp.children == []
        assert list(exp.failed) == [PARENT] and 'pool I/O is currently suspended' in exp.failed[PARENT]
        assert [d.name for d in manager.datasets] == [PARENT], 'the parent itself is unaffected'

    def test_error_once_per_episode_info_on_recovery_and_a_new_episode(self, zfs_world, caplog):
        add_tree(zfs_world, PARENT, f'{PARENT}/a')
        manager = DatasetManager(config(dataset(PARENT, recursive=True)))
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            zfs_world.list_failures[PARENT] = 'pool I/O is currently suspended'
            for _ in range(3):
                assert manager.expand_recursive().failed
            (error,) = records_at(caplog, logging.ERROR)
            assert PARENT in error and 'not sent until this passes' in error
            assert records_at(caplog, logging.INFO) == []

            del zfs_world.list_failures[PARENT]
            exp = manager.expand_recursive()
            assert child_names(exp) == [f'{PARENT}/a'] and exp.failed == {}
            (info,) = records_at(caplog, logging.INFO)
            assert PARENT in info and 'listed again' in info
            manager.expand_recursive()
            assert len(records_at(caplog, logging.INFO)) == 1, 'INFO once'

            zfs_world.list_failures[PARENT] = 'again'
            manager.expand_recursive()
        assert len(records_at(caplog, logging.ERROR)) == 2, 'a new episode is announced again'

    def test_one_failing_parent_does_not_hide_another_ones_children(self, zfs_world):
        other = 'pool/other'
        add_tree(zfs_world, PARENT, f'{PARENT}/a', other, f'{other}/b')
        zfs_world.list_failures[PARENT] = 'boom'
        _, exp = expand(config(dataset(PARENT, recursive=True), dataset(other, recursive=True)))
        assert child_names(exp) == [f'{other}/b'] and list(exp.failed) == [PARENT]

    def test_a_missing_parent_is_a_failed_discovery_not_an_empty_one(self, zfs_world):
        _, exp = expand(config(dataset(PARENT, recursive=True)))        # the world has no PARENT at all
        assert exp.children == [] and list(exp.failed) == [PARENT]

    def test_should_stop_true_from_the_start_lists_nothing(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/a')
        _, exp = expand(config(dataset(PARENT, recursive=True)), should_stop=lambda: True)
        assert exp == Expansion([], {}) and zfs_world.calls == []

    def test_should_stop_during_the_expansion_gives_a_partial_result(self, zfs_world):
        other = 'pool/other'
        add_tree(zfs_world, PARENT, f'{PARENT}/a', other, f'{other}/b')
        asked = []
        def stop_after_first():
            asked.append(1)
            return len(asked) > 1
        _, exp = expand(config(dataset(PARENT, recursive=True), dataset(other, recursive=True)), stop_after_first)
        assert child_names(exp) == [f'{PARENT}/a'] and exp.failed == {}
        assert [c[-1] for c in zfs_world.name_lists()] == [PARENT], 'checked before each parent'

    def test_a_stopped_pass_does_not_forget_a_failing_parent(self, zfs_world, caplog):
        other = 'pool/other'
        add_tree(zfs_world, other, f'{other}/b')
        zfs_world.list_failures[PARENT] = 'boom'
        zfs_world.add(PARENT)
        manager = DatasetManager(config(dataset(PARENT, recursive=True), dataset(other, recursive=True)))
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            manager.expand_recursive()
            manager.expand_recursive(should_stop=lambda: True)        # reaches nobody
            manager.expand_recursive()
        assert len(records_at(caplog, logging.ERROR)) == 1


@pytest.mark.unit
class TestExpandOneParent:
    """`expand_recursive(parent=)`: what the remote worker's discovery threads call (one per recursive
    parent). It expands only that parent and forgets only ITS episodes: the `EXPAND_*` latches of the other
    parents belong to other threads' episodes and must survive (mutation-checked: an unscoped `retain` fails
    the three latch tests)."""

    OTHER = 'pool/other'

    def two(self, world):
        add_tree(world, PARENT, f'{PARENT}/a', self.OTHER, f'{self.OTHER}/b')
        return config(dataset(PARENT, recursive=True), dataset(self.OTHER, recursive=True))

    def test_only_the_named_parent_is_listed_and_expanded(self, zfs_world):
        manager = DatasetManager(self.two(zfs_world))
        exp = manager.expand_recursive(parent=self.OTHER)
        assert child_names(exp) == [f'{self.OTHER}/b'] and exp.failed == {}
        assert [c[-1] for c in zfs_world.name_lists()] == [self.OTHER], 'the other parent is not even listed'

    def test_the_children_are_stamped_like_the_unscoped_expansion(self, zfs_world):
        cfg = self.two(zfs_world)
        full = {c.name: c for c in DatasetManager(cfg).expand_recursive().children}
        one = {c.name: c for c in DatasetManager(cfg).expand_recursive(parent=PARENT).children}
        assert set(one) == {f'{PARENT}/a'}
        assert one[f'{PARENT}/a'].expanded_from == PARENT
        assert one[f'{PARENT}/a'].config == full[f'{PARENT}/a'].config

    @pytest.mark.parametrize('name', ['pool/nowhere', 'pool/plain', ''], ids=['unknown', 'not-recursive', 'empty'])
    def test_a_name_that_is_not_an_enabled_recursive_parent_expands_nothing_and_lists_nothing(self, zfs_world, name):
        add_tree(zfs_world, PARENT, 'pool/plain')
        cfg = config(dataset(PARENT, recursive=True), dataset('pool/plain'))
        exp = DatasetManager(cfg).expand_recursive(parent=name)
        assert exp.children == [] and exp.failed == {} and zfs_world.name_lists() == []

    def test_a_disabled_parent_expands_nothing(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/a')
        cfg = config(dataclasses.replace(dataset(PARENT, recursive=True), enabled=False))
        exp = DatasetManager(cfg).expand_recursive(parent=PARENT)
        assert exp.children == [] and zfs_world.name_lists() == []

    def test_a_failing_parent_reports_itself_in_failed_and_its_error_is_once_per_episode(self, zfs_world, caplog):
        cfg = self.two(zfs_world)
        zfs_world.list_failures[PARENT] = 'pool I/O is currently suspended'
        manager = DatasetManager(cfg)
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            for _ in range(3):
                assert list(manager.expand_recursive(parent=PARENT).failed) == [PARENT]
        assert len(records_at(caplog, logging.ERROR)) == 1

    def test_one_parents_recovery_does_not_end_another_parents_episode(self, zfs_world, caplog):
        cfg = self.two(zfs_world)
        zfs_world.list_failures[PARENT] = 'boom'
        zfs_world.list_failures[self.OTHER] = 'boom'
        manager = DatasetManager(cfg)
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            manager.expand_recursive(parent=PARENT)
            manager.expand_recursive(parent=self.OTHER)
            assert len(records_at(caplog, logging.ERROR)) == 2
            del zfs_world.list_failures[PARENT]
            assert child_names(manager.expand_recursive(parent=PARENT)) == [f'{PARENT}/a']
            assert manager._memory.noted(EXPAND_ERROR, self.OTHER), 'the other parent is still in its episode'
            manager.expand_recursive(parent=self.OTHER)
            assert len(records_at(caplog, logging.ERROR)) == 2, 'its episode is not announced again'

    def test_an_unrelated_parents_call_never_clears_a_latch(self, zfs_world):
        cfg = self.two(zfs_world)
        zfs_world.list_failures[PARENT] = 'boom'
        manager = DatasetManager(cfg)
        manager.expand_recursive(parent=PARENT)
        assert manager._memory.noted(EXPAND_ERROR, PARENT)
        manager.expand_recursive(parent=self.OTHER)               # a healthy parent, a different thread
        assert manager._memory.noted(EXPAND_ERROR, PARENT)
        manager.expand_recursive(parent='pool/nowhere')           # not a parent at all
        assert manager._memory.noted(EXPAND_ERROR, PARENT)

    def test_a_target_episode_is_scoped_to_its_parent(self, zfs_world, caplog):
        target = f'{PARENT}/bk'
        add_tree(zfs_world, PARENT, f'{PARENT}/ok', target, self.OTHER, f'{self.OTHER}/b')
        cfg = config(
            dataset(PARENT, recursive=True), dataset(self.OTHER, recursive=True),
            destinations={DEST: Destination(url='https://h:8443'), 'vault': Destination(url=f'zfs:{target}')},
        )
        manager = DatasetManager(cfg)
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            manager.expand_recursive(parent=PARENT)
            assert len(records_at(caplog, logging.ERROR)) == 1
            assert manager._memory.noted(EXPAND_TARGET, (PARENT, target))
            manager.expand_recursive(parent=self.OTHER)               # a clean parent: PARENT's latch stays
            assert manager._memory.noted(EXPAND_TARGET, (PARENT, target))
            manager.expand_recursive(parent=PARENT)
            assert len(records_at(caplog, logging.ERROR)) == 1, 'still the same episode'

    def test_without_parent_the_whole_set_is_still_forgotten_as_before(self, zfs_world, caplog):
        cfg = self.two(zfs_world)
        zfs_world.list_failures[PARENT] = 'boom'
        manager = DatasetManager(cfg)
        manager.expand_recursive()
        del zfs_world.list_failures[PARENT]
        manager.expand_recursive()
        assert not manager._memory.noted(EXPAND_ERROR, PARENT)

    def test_should_stop_true_lists_nothing(self, zfs_world):
        cfg = self.two(zfs_world)
        exp = DatasetManager(cfg).expand_recursive(should_stop=lambda: True, parent=PARENT)
        assert exp == Expansion([], {}) and zfs_world.calls == []


@pytest.mark.unit
class TestExpandStampedConfig:
    """`dataclasses.replace(parent_cfg, name=child, recursive=False)`, per destination."""

    @staticmethod
    def parent_config():
        rules = [RetentionRule(Duration('1h'), Duration('2d')), RetentionRule(Duration('1d'), Duration('30d'))]
        override = [RetentionRule(Duration('1d'), Duration('365d'))]
        return DatasetConfig(
            name=PARENT, recursive=True, frequency=Duration('30m'), retention_rules=rules,
            remote=[RemoteDatasetConfig(destination='off', retention_rules=override),
                    RemoteDatasetConfig(destination='second', frequency=Duration('6h'), raw=True)],
        )

    def make(self, world):
        add_tree(world, PARENT, f'{PARENT}/a', f'{PARENT}/a/b')
        parent = self.parent_config()
        cfg = config(parent, destinations={'off': Destination(url='https://h:8443'),
                                           'second': Destination(url='https://s:8443')})
        _, exp = expand(cfg)
        return parent, {c.name: c for c in exp.children}

    def test_the_child_config_is_the_parents_with_its_own_name_and_no_recursion(self, zfs_world):
        parent, children = self.make(zfs_world)
        for name, child in children.items():
            assert child.config == dataclasses.replace(parent, name=name, recursive=False)
            assert child.config.name == name and child.config.recursive is False and child.config.enabled
        assert parent.recursive is True and parent.name == PARENT, 'the parent is untouched'

    def test_the_property_stamped_for_each_destination(self, zfs_world):
        parent, children = self.make(zfs_world)
        child = children[f'{PARENT}/a/b']
        off, second = decode(child.config.to_property('off')), decode(child.config.to_property('second'))
        assert off['name'] == second['name'] == f'{PARENT}/a/b'
        assert off['recursive'] is False and second['recursive'] is False
        assert off['frequency'] == second['frequency'] == 1800.0
        # the destination's own override replaces the dataset-level set; the other inherits it
        assert off['retention_rules'] == [{'age': 86400.0, 'keep_for': 365 * 86400.0}]
        assert second['retention_rules'] == [{'age': 3600.0, 'keep_for': 2 * 86400.0},
                                             {'age': 86400.0, 'keep_for': 30 * 86400.0}]
        assert off['remote'] == second['remote'] == [{'destination': 'off', 'frequency': None},
                                                      {'destination': 'second', 'frequency': 21600.0}]

    def test_each_destinations_frequency_and_raw_are_the_parents(self, zfs_world):
        _, children = self.make(zfs_world)
        remotes = children[f'{PARENT}/a'].config.remote
        assert [(r.destination, r.frequency, r.raw) for r in remotes] == [
            ('off', None, None), ('second', Duration('6h'), True)]

    def test_the_stamp_equals_what_a_configured_child_with_the_same_settings_would_carry(self, zfs_world):
        parent, children = self.make(zfs_world)
        twin = dataclasses.replace(parent, name=f'{PARENT}/a', recursive=False)
        for dest in ('off', 'second', None):
            assert children[f'{PARENT}/a'].config.to_property(dest) == twin.to_property(dest)


@pytest.mark.unit
class TestExpandedChildrenAreOrdinary:
    def test_a_clone_is_an_ordinary_child_and_origin_is_not_read(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/orig', f'{PARENT}/clone')
        _, exp = expand(config(dataset(PARENT, recursive=True)))
        assert sorted(child_names(exp)) == [f'{PARENT}/clone', f'{PARENT}/orig']
        assert [c[0] for c in zfs_world.calls] == ['list', 'get'], 'the listing and the value-free copy read only'
        assert 'origin' not in ' '.join(' '.join(c) for c in zfs_world.calls)

    def test_a_child_without_snapshots_is_still_a_child(self, zfs_world):
        add_tree(zfs_world, PARENT)
        add_tree(zfs_world, f'{PARENT}/new', snaps=False)
        manager, exp = expand(config(dataset(PARENT, recursive=True)))
        assert child_names(exp) == [f'{PARENT}/new']
        assert manager.list_snapshots(exp.children[0]) == []

    def test_a_vanished_child_is_simply_not_listed_again(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/a', f'{PARENT}/b')
        manager = DatasetManager(config(dataset(PARENT, recursive=True)))
        assert sorted(child_names(manager.expand_recursive())) == [f'{PARENT}/a', f'{PARENT}/b']
        del zfs_world.datasets[f'{PARENT}/b']
        assert child_names(manager.expand_recursive()) == [f'{PARENT}/a']

    def test_a_renamed_child_appears_under_its_new_name_only(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/a')
        manager = DatasetManager(config(dataset(PARENT, recursive=True)))
        manager.expand_recursive()
        zfs_world.datasets[f'{PARENT}/a2'] = zfs_world.datasets.pop(f'{PARENT}/a')
        zfs_world.snaps[f'{PARENT}/a2'] = zfs_world.snaps.pop(f'{PARENT}/a')
        exp = manager.expand_recursive()
        assert child_names(exp) == [f'{PARENT}/a2']
        assert exp.children[0].expanded_from == PARENT


# =============================================================================================
# A2: the remote worker
# =============================================================================================

from zfsbackup import health  # noqa: E402
from zfsbackup.health import StatusWriter, ensure_status_dir  # noqa: E402
from zfsbackup.workers import PruningWorker, SnapshotWorker  # noqa: E402


class ScriptedSender(RecordingSender):
    """`RecordingSender` whose answer per `(dataset, destination)` is scripted (default: success)
    and may be changed between cycles; `raises` makes a pair's `backup_dataset` raise. With a
    `world`, a success leaves the anchor a real send leaves."""

    def __init__(self, world=None):
        super().__init__(world)
        self.outcomes: Dict[Tuple[str, str], bool] = {}
        self.raises: Set[Tuple[str, str]] = set()
        #: (`confirmed`, from `RecordingSender`: the pairs a real sender has confirmed,
        #: `ChainSender.copy_confirmed`)
        #: pairs whose True does NOT confirm (nothing to send, a copy that is not there ...)
        self.unconfirmed_success: Set[Tuple[str, str]] = set()
        self.confirm_raises = False

    def backup_dataset(self, dsi, remote_cfg, should_stop=None):
        key = (dsi.name, remote_cfg.destination)
        self.attempts.append(key)
        self.expanded[dsi.name] = dsi.expanded_from
        if key in self.raises:
            raise RuntimeError('boom')
        done = self.outcomes.get(key, True)
        if done:
            self._acknowledged(*key)
            if key not in self.unconfirmed_success:
                self.confirmed.add(key)
        return done

    def copy_confirmed(self, dataset, destination):
        if self.confirm_raises:
            self.confirm_calls.append((dataset, destination))
            raise RuntimeError('boom')
        return super().copy_confirmed(dataset, destination)


@pytest.fixture
def sdir(tmp_path):
    (tmp_path / 'config.db').write_bytes(b'')
    directory = tmp_path / 'config.db.status'
    ensure_status_dir(directory)
    return directory


def make_rig(monkeypatch, cfg, sender=None, status_dir=None, local=None, **kw):
    """The REAL remote worker (`_sendloop_support.LoopRig`: its scheduler, discovery and sender threads)
    over `sender` (and `local` for `zfs:` destinations) and the fake zfs world patched below libzfseasy;
    `run()` runs it until it is quiet: every due pair attempted once, every child its discovery found."""
    return LoopRig(
        cfg, monkeypatch, remote=sender if sender is not None else ScriptedSender(), local=local,
        world=True, status_dir=status_dir, **kw,
    )


def status_record(rig, dataset, destination, kind=health.KIND_SEND):
    """The record the worker's status writer holds for the pair (a lookup over `records`), or None."""
    return next((r for r in rig.worker._status.records
                 if (r.kind, r.dataset, r.destination) == (kind, dataset, destination)), None)


def worker_warnings(caplog):
    return [r.getMessage() for r in caplog.records if r.name == WORKER_LOGGER and r.levelno == logging.WARNING]


A, AB = f'{PARENT}/a', f'{PARENT}/a/b'


@pytest.mark.unit
class TestA2ListAndOrder:
    """The pairs the remote worker sends: the configured ones, then the descendants of every enabled
    recursive dataset (its discovery thread: `DatasetManager.expand_recursive(parent=)`), each an
    ordinary pair of the schedule, an ancestor always before its descendants."""

    def test_children_follow_their_parent(self, zfs_world, monkeypatch):
        add_tree(zfs_world, PARENT, A, AB, f'{PARENT}/c', 'pool/other', 'pool/other/z')
        cfg = config(rdataset(PARENT, recursive=True), rdataset('pool/other'))
        sender = ScriptedSender(zfs_world)
        make_rig(monkeypatch, cfg, sender, caps=(1, 1)).run()
        names = [d for d, _ in sender.attempts]
        assert names[0] == PARENT, 'the first configured pair goes first'
        assert sorted(names) == sorted([PARENT, 'pool/other', A, AB, f'{PARENT}/c'])
        for name in names:
            for other in names:
                if name.startswith(other + '/'):
                    assert names.index(other) < names.index(name), 'an ancestor is always before its descendant'
        assert sender.expanded[PARENT] is None and sender.expanded[AB] == PARENT
        assert sender.expanded['pool/other'] is None
        assert 'pool/other/z' not in names, 'pool/other is not recursive'

    def test_a_run_attempts_every_pair_parents_first(self, zfs_world, monkeypatch):
        add_tree(zfs_world, PARENT, A, AB, f'{PARENT}/c')
        sender = ScriptedSender(zfs_world)
        cfg = config(rdataset(PARENT, ['off', 'second'], recursive=True),
                     destinations={'off': Destination(url='https://h:8443'),
                                   'second': Destination(url='https://s:8443')})
        make_rig(monkeypatch, cfg, sender).run()
        assert sorted(sender.attempts) == sorted(
            (d, dest) for d in (PARENT, A, AB, f'{PARENT}/c') for dest in ('off', 'second'))
        for dest in ('off', 'second'):
            on = [d for d, x in sender.attempts if x == dest]
            assert on[0] == PARENT and on.index(A) < on.index(AB)

    def test_through_run_the_children_are_attempted(self, zfs_world, monkeypatch):
        add_tree(zfs_world, PARENT, A, AB)
        sender = TestDescendantsOfARecursiveDataset.one_cycle(
            config(dataset(PARENT, recursive=True)), monkeypatch, zfs_world)
        assert sender.attempts[0] == (PARENT, DEST) and sorted(sender.attempts) == sorted(
            [(PARENT, DEST), (A, DEST), (AB, DEST)])

    def test_the_snapshot_and_pruning_workers_never_see_a_child(self, zfs_world, mocker):
        add_tree(zfs_world, PARENT, A, AB, 'pool/other')
        cfg = config(rdataset(PARENT, recursive=True), dataset('pool/other'))
        for cls in (SnapshotWorker, PruningWorker):
            worker = cls(cfg, StopFlag())
            seen = []
            mocker.patch.object(worker, '_before_loop')
            mocker.patch.object(worker, '_process_dataset', side_effect=lambda m, d: seen.append(d.name))
            mocker.patch.object(worker, '_wait', return_value=ExitReason.STOP)
            worker.run()
            assert seen == [PARENT, 'pool/other'], cls.__name__
            assert all(getattr(d, 'expanded_from', None) is None for d in worker._get_datasets(DatasetManager(cfg)))
        assert zfs_world.name_lists() == [], 'only the remote worker enumerates descendants'

    def test_the_schedulers_own_manager_never_holds_a_child(self, zfs_world, monkeypatch):
        add_tree(zfs_world, PARENT, A)
        cfg = config(rdataset(PARENT, recursive=True))
        rig = make_rig(monkeypatch, cfg).run()
        assert [d.name for d in rig.loop._configured] == [PARENT]
        assert [d.name for d in DatasetManager(cfg).datasets] == [PARENT]
        assert sorted(k.dataset for k in rig.loop.schedule.keys()) == [PARENT, A], 'the schedule has the child'

    def test_a_failed_discovery_still_sends_the_parent(self, zfs_world, monkeypatch, caplog):
        add_tree(zfs_world, PARENT, A)
        zfs_world.list_failures[PARENT] = 'pool I/O is currently suspended'
        sender = ScriptedSender(zfs_world)
        cfg = config(rdataset(PARENT, recursive=True))
        rig = make_rig(monkeypatch, cfg, sender)
        with caplog.at_level(logging.INFO, logger=BACKUP_LOGGER):
            rig.run()
        assert sender.attempts == [(PARENT, DEST)]
        assert list(rig.loop._failed) == [PARENT]
        assert len(records_at(caplog, logging.ERROR)) == 1

    def test_an_expansion_that_raises_is_logged_and_the_parent_is_still_sent(self, zfs_world, monkeypatch, caplog):
        add_tree(zfs_world, PARENT, A)
        sender = ScriptedSender(zfs_world)
        cfg = config(rdataset(PARENT, recursive=True))

        def boom(self_, *a, **k):
            raise RuntimeError('boom')

        monkeypatch.setattr(DatasetManager, 'expand_recursive', boom)
        with caplog.at_level(logging.ERROR, logger=WORKER_LOGGER):
            rig = make_rig(monkeypatch, cfg, sender).run()
        assert sender.attempts == [(PARENT, DEST)]
        assert any('could not expand' in r.getMessage() for r in caplog.records)
        assert list(rig.loop._failed) == [PARENT]

    def test_a_pending_stop_lists_no_descendants(self, zfs_world, monkeypatch):
        add_tree(zfs_world, PARENT, A)
        cfg = config(rdataset(PARENT, recursive=True))
        sender = ScriptedSender(zfs_world)
        rig = make_rig(monkeypatch, cfg, sender)
        rig.stop.set()
        rig.run()
        assert zfs_world.name_lists() == [] and sender.attempts == [] and rig.loop is None


@pytest.mark.unit
class TestA2ParentFirst:
    """R-2 in the scheduler: EVERY child pair (an expanded one or a configured one) waits until the nearest
    ancestor pair on the same destination has been CONFIRMED in this incarnation (`PairOutcome.confirmed`
    from the senders' `SendMemory`: a negotiate found the copy and the call returned True, or a stream
    committed). No anchor and no status record is evidence; an unconfirmed ancestor with waiting
    dependents is DUE whatever its anchor says. One WARNING per (ancestor, destination) EPISODE with the
    number of waiting children once the hold has lasted `WAITING_NOTICE` (these tests set it to 0 where
    they look at it), INFO when released; nothing recorded for a held pair. (The senders' own
    `AncestorRule` is a backstop for expanded children: `test_zfsbackup_sendpair.py::TestHold`.)"""

    @pytest.fixture
    def rig(self, zfs_world, sdir, monkeypatch):
        add_tree(zfs_world, PARENT, A, AB)
        cfg = config(rdataset(PARENT, recursive=True))
        sender = ScriptedSender(zfs_world)
        loop_rig = make_rig(monkeypatch, cfg, sender, sdir)
        return SimpleNamespace(world=zfs_world, cfg=cfg, sender=sender, rig=loop_rig, sdir=sdir,
                               worker=loop_rig.worker, run=loop_rig.run)

    @pytest.fixture
    def rig0(self, zfs_world, sdir, monkeypatch):
        """As `rig`, but a held child is announced at once (`WAITING_NOTICE` 0)."""
        add_tree(zfs_world, PARENT, A, AB)
        cfg = config(rdataset(PARENT, recursive=True))
        sender = ScriptedSender(zfs_world)
        loop_rig = make_rig(monkeypatch, cfg, sender, sdir, notice=0.0)
        return SimpleNamespace(world=zfs_world, cfg=cfg, sender=sender, rig=loop_rig, sdir=sdir,
                               worker=loop_rig.worker, run=loop_rig.run)

    @staticmethod
    def newest(world, name):
        return world.snaps[name][-1]

    @staticmethod
    def aggregated(caplog):
        """`[(count, ancestor, destination)]` of the aggregated WARNINGs."""
        out = []
        for w in worker_warnings(caplog):
            m = re.match(r'(\d+) descendants? of (\S+) wait until its copy at (\S+) is confirmed in this run', w)
            assert m, w
            out.append((int(m.group(1)), m.group(2), m.group(3)))
        return sorted(out)

    # -- waiting, and what is logged / recorded --------------------------------------------------

    def test_children_wait_for_a_parent_that_is_not_confirmed(self, rig0, caplog):
        rig0.sender.outcomes[(PARENT, DEST)] = False
        with caplog.at_level(logging.DEBUG, logger=WORKER_LOGGER):
            rig0.run()
        assert rig0.sender.attempts == [(PARENT, DEST)]
        # `a` waits for the parent and `a/b` for its nearest, `a`: only the topmost holder, the parent (the
        # pair that is not confirmed and not itself held), is announced; `a` is held, so it never is
        assert self.aggregated(caplog) == [(1, PARENT, DEST)]
        assert not rig0.worker._mem().noted(WAITING, (A, DEST))

    def test_the_warning_waits_for_the_notice_a_normal_start_logs_nothing(self, rig, caplog):
        """A child held behind an ancestor that negotiates first is normal at every start: no WARNING
        until the hold has lasted `WAITING_NOTICE` (60 s)."""
        rig.sender.outcomes[(PARENT, DEST)] = False
        with caplog.at_level(logging.DEBUG, logger=WORKER_LOGGER):
            rig.run()
        assert rig.sender.attempts == [(PARENT, DEST)] and worker_warnings(caplog) == []

    def test_the_warning_counts_the_waiting_descendants_once(self, zfs_world, monkeypatch, caplog):
        add_tree(zfs_world, PARENT, *[f'{PARENT}/c{i}' for i in range(3)])
        cfg = config(rdataset(PARENT, recursive=True))
        sender = ScriptedSender(zfs_world)
        sender.outcomes[(PARENT, DEST)] = False
        with caplog.at_level(logging.DEBUG, logger=WORKER_LOGGER):
            make_rig(monkeypatch, cfg, sender, notice=0.0).run()
        assert self.aggregated(caplog) == [(3, PARENT, DEST)]
        assert f'3 descendants of {PARENT} wait' in worker_warnings(caplog)[0]

    def test_nothing_is_recorded_for_a_waiting_pair(self, rig):
        rig.sender.outcomes[(PARENT, DEST)] = False
        rig.run()
        keys = {r.key for r in health.read_worker_status(rig.sdir, rig.worker.name).records}
        assert keys == {(health.KIND_SEND, PARENT, DEST)}
        assert status_record(rig, A, DEST) is None
        assert status_record(rig, PARENT, DEST).consecutive_failures == 1

    def test_the_parents_confirmation_releases_the_children_in_the_same_run(self, rig0, caplog, monkeypatch):
        rig0.sender.outcomes[(PARENT, DEST)] = False
        rig0.run()                                                 # the wait was announced
        assert rig0.worker._mem().noted(WAITING, (PARENT, DEST))
        rig0.sender.attempts.clear()
        rig0.sender.outcomes[(PARENT, DEST)] = True
        monkeypatch.setattr('zfsbackup.sendloop.WAITING_NOTICE', 60.0)     # a normal start: no notice
        age_status(rig0.worker)
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger=WORKER_LOGGER):
            rig0.run()                                             # a new incarnation of the loop
        assert rig0.sender.attempts[0] == (PARENT, DEST)
        assert sorted(rig0.sender.attempts) == sorted([(PARENT, DEST), (A, DEST), (AB, DEST)])
        assert worker_warnings(caplog) == []
        # a new loop starts with no announced episode: the old one's latch is dropped quietly
        assert rig0.worker._mem().latch_view(WAITING) == set()

    def test_the_wait_is_announced_again_after_it_ended(self, rig0, caplog, monkeypatch):
        rig0.sender.outcomes[(PARENT, DEST)] = False
        with caplog.at_level(logging.DEBUG, logger=WORKER_LOGGER):
            rig0.run()
            n = len(self.aggregated(caplog))
            assert n == 1, 'only the topmost holder is announced'
            monkeypatch.setattr('zfsbackup.sendloop.WAITING_NOTICE', 60.0)
            rig0.sender.outcomes[(PARENT, DEST)] = True
            rig0.run()                                          # ended: nothing announced
            assert len(self.aggregated(caplog)) == n
            monkeypatch.setattr('zfsbackup.sendloop.WAITING_NOTICE', 0.0)
            rig0.sender.confirmed.clear()                       # a new incarnation of the sender
            rig0.sender.outcomes[(PARENT, DEST)] = False
            rig0.run()
        assert len(self.aggregated(caplog)) == 2 * n, 'the episode is announced again'

    # -- the evidence: only this incarnation's confirmation -----------------------------------------

    def test_a_status_success_alone_releases_nothing(self, zfs_world, sdir, monkeypatch):
        add_tree(zfs_world, PARENT, A)
        cfg = config(rdataset(PARENT, recursive=True))
        earlier = StatusWriter(sdir, 'remote-backup-worker')
        earlier.record(health.KIND_SEND, PARENT, DEST, ok=True)
        earlier.flush(force=True)
        sender = ScriptedSender()
        sender.outcomes[(PARENT, DEST)] = False
        make_rig(monkeypatch, cfg, sender, sdir).run()
        assert sender.attempts == [(PARENT, DEST)]

    def test_a_true_return_that_confirmed_nothing_releases_nothing(self, rig):
        # `backup_dataset` is True for "nothing to send" too; only the confirmation in the memory counts
        rig.sender.unconfirmed_success.add((PARENT, DEST))
        rig.run()
        assert rig.sender.attempts == [(PARENT, DEST)]

    @pytest.mark.parametrize('age', [timedelta(minutes=10), timedelta(hours=5)], ids=['fresh', 'stale'])
    def test_an_anchor_alone_is_no_evidence_fresh_or_stale(self, rig, age):
        # the M2 scenario: the anchor names a live snapshot but the copy at this destination may be gone
        # (URL switched, another client id): the parent is confirmed again before its children go
        old = stamp(age)
        rig.world.snaps[PARENT].insert(0, old)
        rig.world.set_prop(PARENT, ANCHOR, old)
        rig.sender.outcomes[(PARENT, DEST)] = False
        rig.run()
        assert rig.sender.attempts == [(PARENT, DEST)], 'no child follows a parent that failed to confirm'

    def test_a_fresh_anchor_does_not_stop_an_unconfirmed_parent_with_dependents_being_negotiated(self, rig):
        rig.world.set_prop(PARENT, ANCHOR, self.newest(rig.world, PARENT))      # not due by its anchor
        rig.run()
        assert rig.sender.attempts[0] == (PARENT, DEST) and len(rig.sender.attempts) == 3, \
            'forced due, confirmed, and its children follow in the same run'

    def test_the_forced_negotiate_ends_with_the_confirmation(self, rig):
        rig.world.set_prop(PARENT, ANCHOR, self.newest(rig.world, PARENT))
        rig.run()
        assert rig.sender.attempts.count((PARENT, DEST)) == 1
        assert rig.rig.loop.schedule.wants_confirmation((PARENT, DEST)) is False, 'confirmed: the anchor decides again'

    def test_the_forced_negotiate_records_an_ok_when_the_copy_was_up_to_date(self, rig):
        rig.world.set_prop(PARENT, ANCHOR, self.newest(rig.world, PARENT))
        rig.run()
        record = status_record(rig, PARENT, DEST)
        assert record.result == health.RESULT_OK and record.consecutive_failures == 0

    def test_a_parent_without_dependents_is_sent_on_its_schedule_whatever_its_anchor_says(self, zfs_world, monkeypatch):
        add_tree(zfs_world, PARENT)                                            # recursive, but no child
        zfs_world.set_prop(PARENT, ANCHOR, zfs_world.snaps[PARENT][-1])
        cfg = config(rdataset(PARENT, recursive=True))
        sender = ScriptedSender(zfs_world)
        make_rig(monkeypatch, cfg, sender).run()
        assert sender.attempts == [(PARENT, DEST)], 'the schedule decides: a fresh anchor does not stop it'

    def test_a_configured_parent_goes_first_and_releases_a_configured_child(self, zfs_world, monkeypatch):
        """The schedule applies R-2 to EVERY child pair, so a configured parent with a configured child is
        negotiated first, and the child follows its confirmation; both carry a fresh anchor of their own, which
        stops neither (every dispatch is forced)."""
        add_tree(zfs_world, PARENT, A)
        zfs_world.set_prop(A, ANCHOR, zfs_world.snaps[A][-1])
        zfs_world.set_prop(PARENT, ANCHOR, zfs_world.snaps[PARENT][-1])
        cfg = config(dataset(PARENT), dataset(A))
        sender = ScriptedSender(zfs_world)
        make_rig(monkeypatch, cfg, sender).run()
        assert sender.attempts == [(PARENT, DEST), (A, DEST)]

    def test_unrelated_configured_pairs_are_each_sent_whatever_their_anchors_say(self, zfs_world, monkeypatch):
        add_tree(zfs_world, PARENT, 'pool/other')
        for name in (PARENT, 'pool/other'):
            zfs_world.set_prop(name, ANCHOR, zfs_world.snaps[name][-1])
        sender = ScriptedSender(zfs_world)
        make_rig(monkeypatch, config(dataset(PARENT), dataset('pool/other')), sender).run()
        assert sorted(sender.attempts) == sorted([(PARENT, DEST), ('pool/other', DEST)])

    def test_an_anchor_naming_a_vanished_snapshot_or_unreadable_changes_nothing(self, rig):
        rig.world.set_prop(PARENT, ANCHOR, stamp(timedelta(minutes=5)))
        rig.world.unreadable.add((A, ANCHOR))
        rig.sender.outcomes[(PARENT, DEST)] = False
        rig.run()
        assert rig.sender.attempts == [(PARENT, DEST)]

    def test_a_parent_with_no_snapshots_never_confirms_so_its_children_wait(self, zfs_world, monkeypatch):
        add_tree(zfs_world, PARENT, snaps=False)
        add_tree(zfs_world, A)
        cfg = config(rdataset(PARENT, recursive=True))
        sender = ScriptedSender(zfs_world)
        sender.unconfirmed_success.add((PARENT, DEST))        # backup_dataset says True, nothing is confirmed
        make_rig(monkeypatch, cfg, sender).run()
        assert sender.attempts == [(PARENT, DEST)]

    def test_a_parent_whose_send_raises_does_not_release_them(self, rig):
        rig.sender.raises.add((PARENT, DEST))
        rig.run()
        assert rig.sender.attempts == [(PARENT, DEST)]
        assert status_record(rig, PARENT, DEST).result == health.RESULT_ERROR

    def test_without_a_status_directory_the_confirmation_is_the_evidence(self, zfs_world, monkeypatch):
        add_tree(zfs_world, PARENT, A)
        cfg = config(rdataset(PARENT, recursive=True))
        sender = ScriptedSender(zfs_world)
        rig = make_rig(monkeypatch, cfg, sender)                           # no status writer
        sender.outcomes[(PARENT, DEST)] = False
        rig.run()
        sender.outcomes[(PARENT, DEST)] = True
        rig.run()
        assert sender.attempts == [(PARENT, DEST), (PARENT, DEST), (A, DEST)]

    # -- how the confirmation is learned ------------------------------------------------------------

    def test_the_sender_is_never_asked_whether_a_copy_is_confirmed(self, rig):
        """R-2 reads `COPY_CONFIRMED` from the memory (B1 fix 1c): a sender whose `copy_confirmed`
        raises changes nothing, and is not even called."""
        rig.sender.confirm_raises = True
        rig.run()
        assert rig.sender.confirm_calls == []
        assert rig.sender.attempts[0] == (PARENT, DEST) and len(rig.sender.attempts) == 3

    def test_only_the_memory_confirms_a_copy_a_senders_claim_does_not(self, rig, mocker):
        mocker.patch.object(rig.sender, 'copy_confirmed', return_value=True)       # the sender claims it
        rig.sender.unconfirmed_success.add((PARENT, DEST))                          # the memory has nothing
        rig.run()
        assert rig.sender.attempts == [(PARENT, DEST)]

    def test_a_local_sender_that_was_not_imported_means_not_confirmed(self, zfs_world, monkeypatch, sdir):
        add_tree(zfs_world, PARENT, A)
        cfg = config(rdataset(PARENT, ['vault'], recursive=True), destinations={'vault': Destination(url='zfs:bk/vault')})
        remote = ScriptedSender(zfs_world)
        rig = make_rig(monkeypatch, cfg, remote, sdir)
        worker = rig.worker
        real = worker._before_loop

        def before(config_, manager):
            real(config_, manager)
            worker._local_cls = None                                 # the local class was never imported

        monkeypatch.setattr(worker, '_before_loop', before)
        rig.run()
        assert remote.attempts == []
        record = status_record(rig, PARENT, 'vault')
        assert record.result == health.RESULT_ERROR and 'not imported at start-up' in record.last_error
        assert status_record(rig, A, 'vault') is None, 'the child never left R-2'

    def test_the_confirmation_is_per_destination_and_each_kind_of_sender_confirms_its_own(self, zfs_world, monkeypatch):
        add_tree(zfs_world, PARENT, A)
        cfg = config(rdataset(PARENT, ['off', 'vault'], recursive=True),
                     destinations={'off': Destination(url='https://h:8443'), 'vault': Destination(url='zfs:bk/vault')})
        remote, local = ScriptedSender(zfs_world), ScriptedSender(zfs_world)
        rig = make_rig(monkeypatch, cfg, remote, local=local).run()
        assert remote.confirm_calls == [] and local.confirm_calls == [], 'no manager is asked'
        assert (A, 'off') in remote.attempts and (A, 'vault') in local.attempts
        assert rig.memory.noted(COPY_CONFIRMED, (PARENT, 'off'))
        assert rig.memory.noted(COPY_CONFIRMED, (PARENT, 'vault'))

    def test_a_confirmation_at_one_destination_does_not_release_the_other(self, zfs_world, monkeypatch):
        add_tree(zfs_world, PARENT, A)
        cfg = config(rdataset(PARENT, ['off', 'second'], recursive=True),
                     destinations={'off': Destination(url='https://h:8443'),
                                   'second': Destination(url='https://s:8443')})
        sender = ScriptedSender(zfs_world)
        sender.unconfirmed_success.add((PARENT, 'second'))
        make_rig(monkeypatch, cfg, sender).run()
        assert (A, 'off') in sender.attempts and (A, 'second') not in sender.attempts

    # -- dry run -----------------------------------------------------------------------------------

    def dry(self, world, monkeypatch):
        add_tree(world, PARENT, A)
        cfg = config(rdataset(PARENT, recursive=True), dry_run=True)
        sender = ScriptedSender()                    # a dry run sends nothing and confirms nothing
        sender.unconfirmed_success.add((PARENT, DEST))
        return cfg, sender, make_rig(monkeypatch, cfg, sender)

    def test_in_a_dry_run_the_parents_true_return_passes(self, zfs_world, monkeypatch):
        cfg, sender, rig = self.dry(zfs_world, monkeypatch)
        rig.run()
        assert sender.attempts == [(PARENT, DEST), (A, DEST)]

    def test_in_a_dry_run_a_failed_parent_still_holds_its_children(self, zfs_world, monkeypatch):
        cfg, sender, rig = self.dry(zfs_world, monkeypatch)
        sender.outcomes[(PARENT, DEST)] = False
        rig.run()
        assert sender.attempts == [(PARENT, DEST)]

    def test_outside_a_dry_run_a_true_return_is_never_evidence(self, zfs_world, monkeypatch):
        add_tree(zfs_world, PARENT, A)
        cfg = config(rdataset(PARENT, recursive=True))
        sender = ScriptedSender()
        sender.unconfirmed_success.add((PARENT, DEST))
        make_rig(monkeypatch, cfg, sender).run()
        assert sender.attempts == [(PARENT, DEST)]

    # -- a child with nothing to send, a child whose listing fails ---------------------------------------

    def test_a_held_child_without_snapshots_is_not_negotiated_warned_about_or_recorded(self, zfs_world, sdir, monkeypatch, caplog):
        """Held by the schedule, so it never reaches its sender (before B the sender's R-2 answered "nothing
        to send" and the pair was recorded as done): no `backup_dataset`, no WARNING, no record."""
        add_tree(zfs_world, PARENT)
        add_tree(zfs_world, A, snaps=False)
        cfg = config(rdataset(PARENT, recursive=True))
        sender = ScriptedSender(zfs_world)
        sender.outcomes[(PARENT, DEST)] = False
        rig = make_rig(monkeypatch, cfg, sender, sdir)
        with caplog.at_level(logging.DEBUG, logger=WORKER_LOGGER):
            rig.run()
        assert sender.attempts == [(PARENT, DEST)]
        assert worker_warnings(caplog) == [] and status_record(rig, A, DEST) is None

    def test_a_child_whose_listing_fails_is_held_silently_and_not_recorded(self, zfs_world, sdir, monkeypatch, caplog):
        add_tree(zfs_world, PARENT, A)
        zfs_world.snapshot_list_failures.add(A)
        cfg = config(rdataset(PARENT, recursive=True))
        sender = ScriptedSender(zfs_world)
        sender.outcomes[(PARENT, DEST)] = False
        rig = make_rig(monkeypatch, cfg, sender, sdir)
        with caplog.at_level(logging.DEBUG, logger=WORKER_LOGGER):
            rig.run()
        assert sender.attempts == [(PARENT, DEST)]
        assert worker_warnings(caplog) == [] and status_record(rig, A, DEST) is None

    def test_a_confirmed_parent_releases_a_child_without_snapshots_to_the_sender(self, zfs_world, monkeypatch):
        add_tree(zfs_world, PARENT)
        add_tree(zfs_world, A, snaps=False)
        cfg = config(rdataset(PARENT, recursive=True))
        sender = ScriptedSender(zfs_world)
        make_rig(monkeypatch, cfg, sender).run()
        assert sender.attempts == [(PARENT, DEST), (A, DEST)]

    # -- per destination, local destinations, configured pairs -------------------------------------

    def test_the_wait_is_per_destination(self, zfs_world, monkeypatch):
        add_tree(zfs_world, PARENT, A)
        cfg = config(rdataset(PARENT, ['off', 'second'], recursive=True),
                     destinations={'off': Destination(url='https://h:8443'),
                                   'second': Destination(url='https://s:8443')})
        sender = ScriptedSender(zfs_world)
        sender.outcomes[(PARENT, 'second')] = False
        make_rig(monkeypatch, cfg, sender).run()
        assert sorted(sender.attempts) == sorted([(PARENT, 'off'), (PARENT, 'second'), (A, 'off')])

    def test_a_local_destination_waits_the_same_way(self, zfs_world, monkeypatch, caplog):
        add_tree(zfs_world, PARENT, A)
        cfg = config(rdataset(PARENT, ['vault'], recursive=True),
                     destinations={'vault': Destination(url='zfs:bk/vault')})
        local = ScriptedSender(zfs_world)
        local.outcomes[(PARENT, 'vault')] = False
        rig = make_rig(monkeypatch, cfg, None, local=local, notice=0.0)
        with caplog.at_level(logging.WARNING, logger=WORKER_LOGGER):
            rig.run()
        assert local.attempts == [(PARENT, 'vault')] and rig.remote.attempts == []
        assert self.aggregated(caplog) == [(1, PARENT, 'vault')]
        local.outcomes[(PARENT, 'vault')] = True
        rig.run()
        assert local.attempts[-2:] == [(PARENT, 'vault'), (A, 'vault')]

    def test_configured_child_pairs_are_held_too(self, zfs_world, monkeypatch, caplog):
        """Changed by B: the schedule holds EVERY child pair behind its unconfirmed ancestor (before B a
        configured pair was never held)."""
        add_tree(zfs_world, PARENT, A, AB)
        cfg = config(rdataset(PARENT, recursive=True), dataset(A))      # `a` is configured, not expanded
        sender = ScriptedSender(zfs_world)
        sender.outcomes[(PARENT, DEST)] = False
        make_rig(monkeypatch, cfg, sender).run()
        assert sender.attempts == [(PARENT, DEST)]

    def test_a_confirmed_parent_releases_a_configured_child_and_the_expanded_one_below_it(self, zfs_world, monkeypatch, caplog):
        add_tree(zfs_world, PARENT, A, AB)
        cfg = config(rdataset(PARENT, recursive=True), dataset(A))
        sender = ScriptedSender(zfs_world)
        with caplog.at_level(logging.WARNING, logger=WORKER_LOGGER):
            make_rig(monkeypatch, cfg, sender).run()
        assert sender.attempts == [(PARENT, DEST), (A, DEST), (AB, DEST)]
        assert sender.expanded[A] is None and sender.expanded[AB] == PARENT
        assert worker_warnings(caplog) == []

    def test_an_expanded_pair_waits_for_the_nearest_ancestor_pair_even_if_configured(self, zfs_world, monkeypatch, caplog):
        add_tree(zfs_world, PARENT, A, AB)
        cfg = config(rdataset(PARENT, recursive=True), dataset(A))
        sender = ScriptedSender(zfs_world)
        sender.outcomes[(A, DEST)] = False
        with caplog.at_level(logging.WARNING, logger=WORKER_LOGGER):
            make_rig(monkeypatch, cfg, sender, notice=0.0).run()
        assert sender.attempts == [(PARENT, DEST), (A, DEST)]
        assert self.aggregated(caplog) == [(1, A, DEST)]

    def test_the_nearest_ancestor_is_the_nearest_with_a_pair_on_that_destination(self, zfs_world, monkeypatch, caplog):
        add_tree(zfs_world, PARENT, A, AB)
        cfg = config(rdataset(PARENT, ['off', 'second'], recursive=True),
                     rdataset(A, ['second']),                           # `a` sends to `second` only
                     destinations={'off': Destination(url='https://h:8443'),
                                   'second': Destination(url='https://s:8443')})
        sender = ScriptedSender(zfs_world)
        sender.outcomes[(A, 'second')] = False
        with caplog.at_level(logging.WARNING, logger=WORKER_LOGGER):
            make_rig(monkeypatch, cfg, sender, notice=0.0).run()
        assert (AB, 'off') in sender.attempts, 'on `off` the nearest ancestor pair is the parent (confirmed)'
        assert (AB, 'second') not in sender.attempts, 'on `second` it is `a`, which failed'
        assert self.aggregated(caplog) == [(1, A, 'second')]

    # -- the fail-closed fallbacks of the senders' rule are `test_zfsbackup_sendpair.py::TestHold`: a
    # -- discovery builds a child's pair from its parent's own `remote:` entries, so an expanded pair
    # -- without its ancestor pair cannot arise from `expand_recursive`; the per-episode orphan latch is
    # -- `test_zfsbackup_sendloop.py::TestRuleWiring`.


@pytest.mark.unit
class TestA2StatusKeys:
    """The status records the scheduler keeps: one per (dataset, destination) pair incl. the discovered
    children; `retain` drops the records of pairs that are gone, except under a parent whose children are
    UNKNOWN (never discovered in this incarnation, or its last discovery failed)."""

    def test_the_schedule_covers_every_pair_incl_expanded_children_per_destination(self, zfs_world, monkeypatch):
        add_tree(zfs_world, PARENT, A, AB)
        cfg = config(rdataset(PARENT, ['off', 'second'], recursive=True),
                     destinations={'off': Destination(url='https://h:8443'),
                                   'second': Destination(url='https://s:8443')})
        rig = make_rig(monkeypatch, cfg).run()
        assert set(rig.loop.schedule.keys()) == {
            (d, dest) for d in (PARENT, A, AB) for dest in ('off', 'second')}

    def seeded(self, zfs_world, sdir, monkeypatch):
        add_tree(zfs_world, PARENT, A, AB, 'pool/data2', 'pool/data2/x')
        cfg = config(rdataset(PARENT, recursive=True))
        rig = make_rig(monkeypatch, cfg, ScriptedSender(zfs_world), sdir).run()
        rig.worker._status.record(health.KIND_SEND, 'pool/data2/x', DEST, ok=True)     # a lookalike prefix
        rig.worker._status.record(health.KIND_PRUNE, AB, ok=True)                        # not a send record
        rig.worker._flush_status(force=True)
        return cfg

    def again(self, monkeypatch, cfg, sdir, zfs_world):
        return make_rig(monkeypatch, cfg, ScriptedSender(zfs_world), sdir).run()

    @staticmethod
    def held(rig):
        return {r.key for r in rig.worker._status.records}

    def test_records_of_a_failing_parents_children_survive_the_retain(self, zfs_world, sdir, monkeypatch):
        cfg = self.seeded(zfs_world, sdir, monkeypatch)
        zfs_world.list_failures[PARENT] = 'pool I/O is currently suspended'
        rig = self.again(monkeypatch, cfg, sdir, zfs_world)
        assert sorted(k.dataset for k in rig.loop.schedule.keys()) == [PARENT]
        held = self.held(rig)
        assert {(health.KIND_SEND, A, DEST), (health.KIND_SEND, AB, DEST)} <= held, 'unknown, not gone'
        assert (health.KIND_SEND, 'pool/data2/x', DEST) not in held, 'the sibling with a common prefix goes'
        assert (health.KIND_PRUNE, AB, None) not in held, 'only send records are kept'

    def test_a_vanished_childs_record_goes_when_discovery_works(self, zfs_world, sdir, monkeypatch):
        cfg = self.seeded(zfs_world, sdir, monkeypatch)
        del zfs_world.datasets[AB]
        held = self.held(self.again(monkeypatch, cfg, sdir, zfs_world))
        assert (health.KIND_SEND, AB, DEST) not in held and (health.KIND_SEND, A, DEST) in held

    def test_records_are_dropped_once_discovery_recovers_and_the_child_is_gone(self, zfs_world, sdir, monkeypatch):
        cfg = self.seeded(zfs_world, sdir, monkeypatch)
        zfs_world.list_failures[PARENT] = 'boom'
        assert (health.KIND_SEND, AB, DEST) in self.held(self.again(monkeypatch, cfg, sdir, zfs_world))
        del zfs_world.list_failures[PARENT]
        del zfs_world.datasets[AB]
        assert (health.KIND_SEND, AB, DEST) not in self.held(self.again(monkeypatch, cfg, sdir, zfs_world))

    def test_a_renamed_childs_old_record_goes_and_the_new_one_starts(self, zfs_world, sdir, monkeypatch):
        cfg = self.seeded(zfs_world, sdir, monkeypatch)
        zfs_world.datasets[f'{PARENT}/a2'] = zfs_world.datasets.pop(AB)
        zfs_world.snaps[f'{PARENT}/a2'] = zfs_world.snaps.pop(AB)
        held = self.held(self.again(monkeypatch, cfg, sdir, zfs_world))
        assert (health.KIND_SEND, AB, DEST) not in held
        assert (health.KIND_SEND, f'{PARENT}/a2', DEST) in held        # the new name was attempted

    def test_a_renamed_child_with_a_stale_anchor_is_due_under_its_new_name(self, zfs_world, monkeypatch):
        old = stamp(timedelta(hours=3))
        zfs_world.add(PARENT, snaps=[stamp(timedelta(minutes=10))])
        zfs_world.add(f'{PARENT}/a2', snaps=[old])
        zfs_world.set_prop(f'{PARENT}/a2', ANCHOR, old)              # travelled with the rename
        sender = ScriptedSender(zfs_world)
        make_rig(monkeypatch, config(rdataset(PARENT, recursive=True)), sender).run()
        assert (f'{PARENT}/a2', DEST) in sender.attempts

    def test_a_child_with_no_snapshots_is_attempted_and_the_sender_decides(self, zfs_world, monkeypatch):
        add_tree(zfs_world, PARENT)
        add_tree(zfs_world, f'{PARENT}/new', snaps=False)
        sender = ScriptedSender(zfs_world)
        make_rig(monkeypatch, config(rdataset(PARENT, recursive=True)), sender).run()
        assert (f'{PARENT}/new', DEST) in sender.attempts

    def test_the_over_cap_warning_is_given_once_per_episode(self, monkeypatch, caplog, sdir):
        monkeypatch.setattr(health, 'STATUS_MAX_RECORDS', 3)
        cfg = config(*[dataset(f'pool/d{i}') for i in range(4)])
        rig = make_rig(monkeypatch, cfg, None, sdir)
        with caplog.at_level(logging.WARNING, logger=WORKER_LOGGER):
            rig.run()
            loop = rig.loop
            loop.leave.clear()                       # `run()` set it on the way out: `retain` is skipped while leaving
            loop._retain_status()
            loop._retain_status()
        (warning,) = worker_warnings(caplog)
        assert '4 dataset x destination pairs' in warning and 'more than the 3' in warning

    def test_the_over_cap_episode_ends_when_the_count_falls_back_and_can_start_again(self, monkeypatch, caplog, sdir):
        monkeypatch.setattr(health, 'STATUS_MAX_RECORDS', 3)
        cfg = config(*[dataset(f'pool/d{i}') for i in range(4)])
        rig = make_rig(monkeypatch, cfg, None, sdir).run()
        loop = rig.loop
        loop.leave.clear()
        full = dict(loop._pairs)
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger=WORKER_LOGGER):
            loop._pairs = dict(list(full.items())[:3])                 # at the cap: fine, episode over
            loop._retain_status()
            assert worker_warnings(caplog) == []
            loop._pairs = full
            loop._retain_status()
        assert len(worker_warnings(caplog)) == 1

    def test_nothing_is_dropped_while_the_worker_is_leaving(self, monkeypatch, sdir):
        """A list cut short by a stop or reload is not the truth about what is configured."""
        cfg = config(dataset('pool/d0'))
        rig = make_rig(monkeypatch, cfg, None, sdir).run()
        rig.worker._status.record(health.KIND_SEND, 'pool/gone', DEST, ok=True)
        assert rig.loop.leave.is_set()
        rig.loop._retain_status()
        assert (health.KIND_SEND, 'pool/gone', DEST) in {r.key for r in rig.worker._status.records}

    def test_expanded_children_count_towards_the_cap(self, zfs_world, monkeypatch, caplog, sdir):
        monkeypatch.setattr(health, 'STATUS_MAX_RECORDS', 3)
        add_tree(zfs_world, PARENT, A, f'{PARENT}/b', f'{PARENT}/c')
        cfg = config(rdataset(PARENT, recursive=True))
        with caplog.at_level(logging.WARNING, logger=WORKER_LOGGER):
            make_rig(monkeypatch, cfg, ScriptedSender(zfs_world), sdir).run()
        assert len(worker_warnings(caplog)) == 1

    def test_the_default_cap_is_not_reached_by_a_small_tree(self, zfs_world, monkeypatch, caplog, sdir):
        add_tree(zfs_world, PARENT, A, AB)
        cfg = config(rdataset(PARENT, recursive=True))
        with caplog.at_level(logging.WARNING, logger=WORKER_LOGGER):
            make_rig(monkeypatch, cfg, ScriptedSender(zfs_world), sdir).run()
        assert worker_warnings(caplog) == []


@pytest.mark.unit
class TestA2ExpansionCrash:
    """`expand_recursive` is documented never to raise; if it does, on the parent's discovery thread, the
    children missing from the schedule are unknown, not gone: their records are kept (a crash is a failed
    discovery of that parent only)."""

    def test_a_crash_keeps_the_childrens_records_and_recovers_the_next_run(self, zfs_world, sdir, monkeypatch, caplog):
        add_tree(zfs_world, PARENT, A, AB)
        cfg = config(rdataset(PARENT, recursive=True))
        rig = make_rig(monkeypatch, cfg, ScriptedSender(zfs_world), sdir).run()
        assert {r.dataset for r in rig.worker._status.records} == {PARENT, A, AB}
        rig.worker._status.record(health.KIND_SEND, 'pool/gone', DEST, ok=True)
        rig.worker._flush_status(force=True)
        real = DatasetManager.expand_recursive

        def boom(self_, *a, **k):
            raise RuntimeError('boom')

        monkeypatch.setattr(DatasetManager, 'expand_recursive', boom)
        with caplog.at_level(logging.ERROR, logger=WORKER_LOGGER):
            rig2 = make_rig(monkeypatch, cfg, ScriptedSender(zfs_world), sdir).run()
        assert any('could not expand' in r.getMessage() for r in caplog.records)
        # the parent's children are UNKNOWN (kept); a record that belongs to no parent is gone (the
        # serial worker kept it for that cycle: it dropped nothing at all after a crash)
        assert {r.dataset for r in rig2.worker._status.records} == {PARENT, A, AB}
        monkeypatch.setattr(DatasetManager, 'expand_recursive', real)
        rig3 = make_rig(monkeypatch, cfg, ScriptedSender(zfs_world), sdir).run()
        assert {r.dataset for r in rig3.worker._status.records} == {PARENT, A, AB}

    def test_a_failed_discovery_is_not_a_crash(self, zfs_world, monkeypatch, caplog):
        add_tree(zfs_world, PARENT, A)
        zfs_world.list_failures[PARENT] = 'boom'
        cfg = config(rdataset(PARENT, recursive=True))
        with caplog.at_level(logging.ERROR, logger=WORKER_LOGGER):
            rig = make_rig(monkeypatch, cfg).run()
        assert not any('could not expand' in r.getMessage() for r in caplog.records)
        assert list(rig.loop.schedule.keys()) == [(PARENT, DEST)]


@pytest.mark.unit
class TestA2FailedListingIsAFailure:
    """`ChainSender.backup_dataset`: `[]` from `list_snapshots` is "nothing to send" (True) only when no
    listing error was recorded; a FAILED listing is a failure with its own status text."""

    Sender = TestA1MatrixSender.Sender

    class Ready(Sender):
        def begin_cycle(self):
            pass

        def _ready_transport(self, destination, dest):
            return object()

        def _negotiate(self, *a, **k):
            raise AssertionError('negotiated although the snapshots could not be listed')

    def rig(self, world):
        add_tree(world, PARENT)
        cfg = config(dataset(PARENT))
        manager = DatasetManager(cfg)
        return cfg, manager, self.Ready(cfg, manager, client_id='client1'), manager.datasets[0]

    def test_a_failed_listing_returns_false_with_the_error_as_the_problem(self, zfs_world):
        cfg, manager, sender, dsi = self.rig(zfs_world)
        zfs_world.snapshot_list_failures.add(PARENT)
        assert sender.backup_dataset(dsi, dsi.config.remote[0]) is False
        problem = sender.last_problem(PARENT, DEST)
        assert problem.startswith('cannot list snapshots: ') and 'suspended' in problem

    def test_an_empty_listing_is_still_nothing_to_send(self, zfs_world):
        add_tree(zfs_world, PARENT, snaps=False)
        cfg = config(dataset(PARENT))
        manager = DatasetManager(cfg)
        sender = self.Ready(cfg, manager, client_id='client1')
        dsi = manager.datasets[0]
        assert sender.backup_dataset(dsi, dsi.config.remote[0]) is True
        assert sender.last_problem(PARENT, DEST) is None

    def test_the_problem_is_cleared_by_the_next_call(self, zfs_world):
        cfg, manager, sender, dsi = self.rig(zfs_world)
        zfs_world.snapshot_list_failures.add(PARENT)
        sender.backup_dataset(dsi, dsi.config.remote[0])
        zfs_world.snapshot_list_failures.clear()
        zfs_world.snaps[PARENT] = []
        assert sender.backup_dataset(dsi, dsi.config.remote[0]) is True
        assert sender.last_problem(PARENT, DEST) is None

    def test_the_worker_records_it_as_a_failure_and_the_count_rises(self, zfs_world, sdir, monkeypatch):
        cfg, manager, sender, dsi = self.rig(zfs_world)
        zfs_world.snapshot_list_failures.add(PARENT)
        rig = make_rig(monkeypatch, cfg, sender, sdir).run()
        for expected in (1, 2, 3):                        # one failure per incarnation, each due again (aged)
            record = status_record(rig, PARENT, DEST)
            assert record.result == health.RESULT_ERROR and record.consecutive_failures == expected
            age_status(rig.worker)
            if expected < 3:
                rig = make_rig(monkeypatch, cfg, self.Ready(cfg, manager, client_id='client1'), sdir).run()
        assert record.last_error.startswith('cannot list snapshots: ')
        zfs_world.snapshot_list_failures.clear()
        zfs_world.snaps[PARENT] = []                      # listing works again and finds nothing to send
        age_status(rig.worker)
        rig = make_rig(monkeypatch, cfg, self.Ready(cfg, manager, client_id='client1'), sdir).run()
        assert status_record(rig, PARENT, DEST).consecutive_failures == 0


@pytest.mark.unit
class TestCopyConfirmedInTheChain:
    """`ChainSender.copy_confirmed`: the destination's copy was confirmed in THIS incarnation (a negotiate
    reported a copy and the call returned True, or a stream was acknowledged); never from an anchor."""

    @pytest.fixture(autouse=True)
    def _presence(self, snapshot_presence):
        return snapshot_presence

    @staticmethod
    def rig(mocker, *listings, copy='none', **kw):
        rig = ChainRig(mocker, *listings, **kw)
        real = rig.sender._negotiate

        def negotiate(transport, client_id, dsi, snapshots, destination):
            common = real(transport, client_id, dsi, snapshots, destination)
            rig.sender._server_info[(dsi.name, destination)] = (copy, 'bk/c1/pool/data')
            return common

        rig.sender._negotiate = negotiate
        return rig

    @staticmethod
    def confirmed(rig):
        return rig.sender.copy_confirmed(CHAIN_DS, CHAIN_DEST)

    def test_nothing_is_confirmed_at_first(self, mocker):
        rig = self.rig(mocker, listing(S1, S2), common=S2, copy='plain')
        assert self.confirmed(rig) is False

    @pytest.mark.parametrize('copy', ['plain', 'raw'])
    def test_a_reported_copy_that_is_up_to_date_confirms(self, mocker, copy):
        names = (S1, S2)
        snaps = ([sinfo(n, encryption='aes-256-gcm', keystatus='available') for n in reversed(names)]
                 if copy == 'raw' else listing(*names))       # a raw copy goes with an encrypted dataset
        rig = self.rig(mocker, snaps, common=S2, copy=copy)
        assert rig.run() is True and rig.sender.stream_calls == []
        assert self.confirmed(rig) is True

    def test_a_named_common_snapshot_confirms_even_without_a_copy_report(self, mocker):
        rig = self.rig(mocker, listing(S1, S2, S3), common=S1, copy=None)
        assert rig.run() is True and len(rig.sender.stream_calls) == 2
        assert self.confirmed(rig) is True

    def test_a_first_full_send_confirms_when_the_receiver_acknowledges(self, mocker):
        rig = self.rig(mocker, listing(S1, S2), common=None, copy='none')
        assert rig.run() is True
        assert self.confirmed(rig) is True

    def test_an_acknowledged_stream_confirms_even_if_the_anchor_write_then_fails(self, mocker):
        rig = self.rig(mocker, listing(S1, S2), common=None, copy='none')
        rig.ledger.anchor_writes_succeed = False
        rig.run()                                  # the anchor bookkeeping fails; the stream was acknowledged
        assert rig.ledger.anchor is None and self.confirmed(rig) is True

    def test_a_later_step_failing_keeps_the_confirmation_of_the_earlier_ack(self, mocker):
        from zfsbackup.chain import _FAILED
        rig = self.rig(mocker, listing(S1, S2, S3), common=S1, copy='plain', outcomes=[_OK, _FAILED])
        assert rig.run() is False
        assert self.confirmed(rig) is True

    def test_a_failed_first_stream_confirms_nothing(self, mocker):
        rig = self.rig(mocker, listing(S1, S2), common=S1, copy='plain', outcomes=[_FAILED])
        assert rig.run() is False
        assert self.confirmed(rig) is False

    def test_a_dry_run_confirms_nothing(self, mocker):
        rig = self.rig(mocker, listing(S1, S2), common=S2, copy='plain')
        rig.config.dry_run = True
        assert rig.run() is True
        assert self.confirmed(rig) is False

    def test_a_later_negotiate_that_finds_no_copy_unsets_it(self, mocker):
        rig = self.rig(mocker, listing(S1, S2), common=[S2, None], copy='plain')
        assert rig.run() is True and self.confirmed(rig) is True
        # the destination lost its copy: the next negotiate reports none and no common snapshot
        real = rig.sender._negotiate

        def lost(transport, client_id, dsi, snapshots, destination):
            common = real(transport, client_id, dsi, snapshots, destination)
            rig.sender._server_info[(dsi.name, destination)] = ('none', None)
            return common

        rig.sender._negotiate = lost
        rig.sender.outcomes = [_FAILED]                      # and the fresh stream then fails
        assert rig.run() is False
        assert self.confirmed(rig) is False

    def test_it_is_per_dataset_and_destination(self, mocker):
        rig = self.rig(mocker, listing(S1, S2), common=S2, copy='plain')
        rig.run()
        assert rig.sender.copy_confirmed(CHAIN_DS, 'elsewhere') is False
        assert rig.sender.copy_confirmed('pool/other', CHAIN_DEST) is False

    def test_it_is_in_memory_only(self, mocker):
        rig = self.rig(mocker, listing(S1, S2), common=S2, copy='plain')
        rig.run()
        rig2 = self.rig(mocker, listing(S1, S2), common=S2, copy='plain')
        assert self.confirmed(rig2) is False


@pytest.mark.unit
class TestBareWorkerHelper:
    """`tests/_remote_worker_support.py`: a worker without `__init__`. The pair state of the serial loop is
    gone (the scheduler owns it); what remains is the memory, made on first use."""

    def test_it_has_no_pair_state_left(self):
        from _remote_worker_support import bare_remote_worker
        bare = bare_remote_worker()
        real = RemoteBackupWorker(config(dataset(PARENT)), StopFlag())
        for name in ('_pairs', '_dependents', '_expansion_failed', '_expansion_crashed', '_pairs_over_cap_noted'):
            assert not hasattr(bare, name) and not hasattr(real, name), name

    def test_the_waiting_and_dry_run_names_are_the_memorys_latches(self):
        """B1 fix 13: read-only views over the worker's `SendMemory`, made on first use (`_mem()`)."""
        from _remote_worker_support import bare_remote_worker
        bare = bare_remote_worker()
        assert bare._waiting_noted is bare._mem().latch_view(WAITING)
        assert bare._dry_run_sent is bare._mem().latch_view(DRY_RUN_SENT)
        real = RemoteBackupWorker(config(dataset(PARENT)), StopFlag())
        assert real._waiting_noted is real._mem().latch_view(WAITING)
        assert real._dry_run_sent is real._mem().latch_view(DRY_RUN_SENT)
        for name in ('_waiting_noted', '_dry_run_sent'):
            with pytest.raises(AttributeError):
                setattr(real, name, set())


@pytest.mark.unit
class TestA2PerChildSendMode:
    """`raw: auto` is decided per child, from its own newest snapshot (the real `_send_mode`)."""

    Sender = TestA1MatrixSender.Sender

    def rig(self, world, raw=None):
        add_tree(world, PARENT, f'{PARENT}/secret', f'{PARENT}/open', f'{PARENT}/secret/inner')
        world.encryption[f'{PARENT}/secret'] = 'aes-256-gcm'
        world.encryption[f'{PARENT}/secret/inner'] = 'aes-256-gcm'      # inherits the encryption
        cfg = config(DatasetConfig(
            name=PARENT, recursive=True, frequency=Duration('1h'),
            remote=[RemoteDatasetConfig(destination=DEST, raw=raw)]))
        manager = DatasetManager(cfg)
        children = {c.name: c for c in manager.expand_recursive().children}
        children[PARENT] = manager.datasets[0]
        return cfg, manager, children, self.Sender(cfg, manager, client_id='client1')

    def mode(self, sender, manager, dsi):
        newest = manager.list_snapshots(dsi)[0]
        return sender._send_mode(dsi, dsi.config.remote[0], newest)

    def test_auto_is_raw_for_an_encrypted_child_and_plain_for_the_unencrypted_parent(self, zfs_world):
        cfg, manager, ds, sender = self.rig(zfs_world)
        assert self.mode(sender, manager, ds[PARENT]) == 'plain'
        assert self.mode(sender, manager, ds[f'{PARENT}/secret']) == 'raw'
        assert self.mode(sender, manager, ds[f'{PARENT}/secret/inner']) == 'raw'
        assert self.mode(sender, manager, ds[f'{PARENT}/open']) == 'plain'

    def test_raw_true_on_the_parent_refuses_an_unencrypted_child_with_one_error_each(self, zfs_world, caplog):
        cfg, manager, ds, sender = self.rig(zfs_world, raw=True)
        with caplog.at_level(logging.DEBUG):
            for _ in range(2):
                assert self.mode(sender, manager, ds[f'{PARENT}/secret']) == 'raw'
                assert self.mode(sender, manager, ds[f'{PARENT}/open']) is None
                assert self.mode(sender, manager, ds[PARENT]) is None
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 2, 'one per refused pair, however many cycles'
        assert any(e.startswith(f'{PARENT}/open -> {DEST}: raw is true but the dataset is not encrypted') for e in errors)
        assert any(e.startswith(f'{PARENT} -> {DEST}: raw is true') for e in errors)

    def test_a_child_without_snapshots_is_a_successful_nothing_to_send(self, zfs_world):
        add_tree(zfs_world, PARENT)
        add_tree(zfs_world, f'{PARENT}/new', snaps=False)
        cfg = config(rdataset(PARENT, recursive=True))
        manager = DatasetManager(cfg)
        (child,) = manager.expand_recursive().children

        class Sender(self.Sender):
            def _ready_transport(self, destination, dest):
                return object()

            def _negotiate(self, *a, **k):
                raise AssertionError('negotiated for a dataset that has nothing to send')

        sender = Sender(cfg, manager, client_id='client1')
        assert sender.backup_dataset(child, child.config.remote[0]) is True


# ---------------------------------------------------------------------------------------------
# A3: `health_collect` observes the descendants (the inheriting fake: fast, real `zfs get` inheritance)
# ---------------------------------------------------------------------------------------------

SECOND = 'second'
ANCHOR2 = f'{PROP_ANCHOR_PREFIX}{SECOND}'


def two_destination_parent(name=PARENT, frequency='1h'):
    return DatasetConfig(
        name=name, frequency=Duration(frequency), recursive=True,
        remote=[RemoteDatasetConfig(destination=DEST), RemoteDatasetConfig(destination=SECOND, frequency=Duration('6h'))],
    )


@pytest.mark.unit
class TestA3Health:
    """The descendants as `collect` sees them: one anchor observation per child and destination (the
    PARENT's interval, no snapshot observation), a failed discovery one WARN `descendants` entry, an
    unreadable anchor or pin an error observation for that destination only."""

    NOW = lambda self: datetime.now(timezone.utc).timestamp()   # noqa: E731

    @staticmethod
    def of(observations, subject, source='anchor'):
        return [o for o in observations if getattr(o, 'source', None) == source and o.subject == subject]

    def test_the_child_anchor_is_the_childs_own_local_value_never_the_parents(self, zfs_world):
        old, new = two_snapshot_pair(zfs_world)
        zfs_world.set_prop(PARENT, ANCHOR, new)                 # the child inherits it
        observations = collect(config(dataset(PARENT, recursive=True)))
        (child,) = self.of(observations, CHILD)
        assert child.newest_ts is None and child.error is None and child.detail is None
        (entry,) = evaluate([child], self.NOW())
        assert entry.state == STATE_WARN and entry.reason == 'not yet sent'
        zfs_world.set_prop(CHILD, ANCHOR, new)
        (child,) = self.of(collect(config(dataset(PARENT, recursive=True))), CHILD)
        assert child.detail == new
        assert evaluate([child], self.NOW())[0].state == STATE_OK

    def test_a_child_has_anchor_observations_only_and_the_parents_interval(self, zfs_world):
        two_snapshot_pair(zfs_world)
        observations = collect(config(two_destination_parent()))
        assert self.of(observations, CHILD, 'snapshot') == []
        first, second = self.of(observations, CHILD)
        assert (first.peer, second.peer) == (DEST, SECOND)
        assert (first.expected_seconds, second.expected_seconds) == (3600, 6 * 3600)
        parent = self.of(observations, PARENT)
        assert [o.expected_seconds for o in parent] == [first.expected_seconds, second.expected_seconds]
        assert self.of(observations, PARENT, 'snapshot') != []

    @pytest.mark.parametrize('layout', ['local', 'received', 'inherited-from-above', 'local-source-dataset'])
    def test_a_backup_copy_is_never_a_child(self, zfs_world, layout):
        add_tree(zfs_world, 'pool', PARENT, CHILD, f'{PARENT}/copy', f'{PARENT}/copy/sub')
        marker = 'org.zfsbackup:client_id'
        if layout == 'local':
            zfs_world.set_prop(f'{PARENT}/copy', marker, 'c')
        elif layout == 'received':
            zfs_world.received[(f'{PARENT}/copy', marker)] = 'c'
        elif layout == 'local-source-dataset':
            zfs_world.set_prop(f'{PARENT}/copy', 'org.zfsbackup:source_dataset', 'c')
        else:
            zfs_world.set_prop('pool', marker, 'c')
        subjects = [o.subject for o in collect(config(dataset(PARENT, recursive=True)))
                    if getattr(o, 'source', None) == 'anchor' and o.subject != PARENT]
        assert subjects == ([] if layout == 'inherited-from-above' else [CHILD])

    def test_a_failed_discovery_is_one_warn_entry_and_everything_else_is_still_observed(self, zfs_world):
        two_snapshot_pair(zfs_world)
        zfs_world.add('pool/plain', snaps=[stamp(timedelta(minutes=5))])
        zfs_world.list_failures[PARENT] = 'cannot open: pool I/O is currently suspended'
        cfg = config(dataset(PARENT, recursive=True), dataset('pool/plain'))
        collected = collect(cfg)
        (entry,) = [o for o in collected if not hasattr(o, 'expected_seconds')]
        assert (entry.source, entry.subject, entry.state) == ('descendants', PARENT, STATE_WARN)
        assert 'pool I/O is currently suspended' in entry.reason and 'their copies are not checked' in entry.reason
        assert self.of(collected, 'pool/plain', 'snapshot') and self.of(collected, PARENT, 'snapshot')
        assert self.of(collected, CHILD) == []
        out = evaluate(collected, self.NOW())
        assert [e.state for e in out if e.source == 'descendants'] == [STATE_WARN]

    def test_the_entry_goes_when_the_listing_works_again(self, zfs_world):
        two_snapshot_pair(zfs_world)
        zfs_world.list_failures[PARENT] = 'pool I/O is currently suspended'
        cfg = config(dataset(PARENT, recursive=True))
        assert [o.source for o in collect(cfg) if not hasattr(o, 'expected_seconds')] == ['descendants']
        del zfs_world.list_failures[PARENT]
        assert [o.source for o in collect(cfg) if not hasattr(o, 'expected_seconds')] == []
        assert self.of(collect(cfg), CHILD) != []

    def test_a_failed_tree_listing_leaves_anchor_error_observations_only(self, zfs_world):
        two_snapshot_pair(zfs_world)
        zfs_world.tree_list_failures.add(PARENT)       # the parent's tree is ONE listing: the batch is the unit
        collected = collect(config(two_destination_parent()))
        assert self.of(collected, CHILD, 'snapshot') == []
        errors = self.of(collected, CHILD)
        assert [o.peer for o in errors] == [DEST, SECOND]
        assert all(o.error and 'pool I/O is currently suspended' in o.error for o in errors)
        assert [e.state for e in evaluate(errors, self.NOW())] == [STATE_CRIT, STATE_CRIT]
        assert not self.of(collected, PARENT)[0].error

    @pytest.mark.parametrize('prop', [ANCHOR, PIN])
    def test_an_unreadable_anchor_or_pin_of_a_child_is_the_error_of_the_whole_batch(self, zfs_world, prop):
        """The children are read in ONE batch per parent: a value that cannot be read makes every child and
        destination of that parent an error observation ("the anchor could not be read"), never a smaller or
        a wrong answer; the parent itself, read on its own, is untouched."""
        old, new = two_snapshot_pair(zfs_world)
        zfs_world.set_prop(CHILD, ANCHOR2, new)
        zfs_world.set_prop(CHILD, prop, new if prop == ANCHOR else f'{old},{new}')   # phase 1 finds it ...
        zfs_world.unreadable.add((CHILD, prop))                                       # ... phase 2 cannot read it
        collected = collect(config(two_destination_parent()))
        first, second = self.of(collected, CHILD)
        assert (first.peer, second.peer) == (DEST, SECOND)
        assert all(o.error and o.error.startswith('the anchor could not be read: ') for o in (first, second))
        assert all('pool I/O is currently suspended' in o.error for o in (first, second))
        assert [e.state for e in evaluate([first, second], self.NOW())] == [STATE_CRIT, STATE_CRIT]
        assert all(o.error is None for o in self.of(collected, PARENT))

    @pytest.mark.parametrize('prop', [ANCHOR, PIN])
    def test_an_unreadable_value_of_a_configured_dataset_is_per_destination(self, zfs_world, prop):
        """A CONFIGURED dataset (here the child, listed explicitly) is not batched: an unreadable anchor is
        the error of that destination only; an unreadable pin only a note on a readable anchor."""
        old, new = two_snapshot_pair(zfs_world)
        zfs_world.set_prop(CHILD, ANCHOR2, new)
        zfs_world.set_prop(CHILD, ANCHOR, new)
        zfs_world.unreadable.add((CHILD, prop))
        two = DatasetConfig(name=CHILD, frequency=Duration('1h'),
                            remote=[RemoteDatasetConfig(destination=DEST), RemoteDatasetConfig(destination=SECOND)])
        collected = collect(config(dataset(PARENT), two, destinations={
            DEST: Destination(url='https://h:8443'), SECOND: Destination(url='https://h2:8443')}))
        first, second = self.of(collected, CHILD)
        assert second.error is None and second.detail == new
        if prop == ANCHOR:
            assert first.error.startswith('the anchor could not be read: ') and first.newest_ts is None
            assert [e.state for e in evaluate([first, second], self.NOW())] == [STATE_CRIT, STATE_OK]
        else:
            assert first.error is None and first.newest_ts is not None
            assert first.detail.startswith(new) and 'send pin could not be read' in first.detail
            assert [e.state for e in evaluate([first, second], self.NOW())] == [STATE_OK, STATE_OK]

    def test_an_unreadable_anchor_of_a_configured_dataset_is_an_error_too(self, zfs_world):
        old, new = two_snapshot_pair(zfs_world)
        zfs_world.unreadable.add((PARENT, ANCHOR))
        collected = collect(config(dataset(PARENT)))
        (snapshot,) = self.of(collected, PARENT, 'snapshot')
        (anchor,) = self.of(collected, PARENT)
        assert snapshot.error is None and anchor.error and anchor.newest_ts is None

    def test_collecting_only_reads(self, zfs_world):
        two_snapshot_pair(zfs_world)
        collect(config(two_destination_parent()))
        assert zfs_world.writes == [] and {c[0] for c in zfs_world.calls} == {'list', 'get'}


# ---------------------------------------------------------------------------------------------
# `Expansion.skipped`, `quiet=` and `scope=` (the check reports the copies itself)
# ---------------------------------------------------------------------------------------------

CID_PROP, SRC_PROP = 'org.zfsbackup:client_id', 'org.zfsbackup:source_dataset'
PROPS_TEXT = f'{CID_PROP} or {SRC_PROP}'
TARGET_WHY = 'a receive target, which holds copies'


@pytest.mark.unit
class TestExpansionSkipped:
    """`Expansion.skipped`: for each recursive parent, `(name, why)` of every backup copy or receive target
    left out, topmost only, one entry per name, in listing order; no key for a parent with none, nor for one
    whose discovery failed."""

    COPY = f'{PARENT}/copy'

    def tree(self, world):
        add_tree(world, PARENT, f'{PARENT}/ok', self.COPY, f'{self.COPY}/c1', f'{self.COPY}/c1/x', f'{PARENT}/zcopy')

    def skipped(self, cfg, **kw):
        return DatasetManager(cfg).expand_recursive(**kw).skipped

    def test_the_default_is_an_empty_read_only_mapping(self):
        exp = Expansion([], {})
        assert dict(exp.skipped) == {} and Expansion([], {}).skipped is exp.skipped
        with pytest.raises(TypeError):
            exp.skipped['pool/data'] = []

    def test_a_config_without_recursive_roots_has_an_empty_mapping(self, zfs_world):
        exp = DatasetManager(config(dataset(PARENT))).expand_recursive()
        assert dict(exp.skipped) == {}

    def test_a_local_marker(self, zfs_world):
        self.tree(zfs_world)
        zfs_world.set_prop(self.COPY, CID_PROP, 'x')
        assert dict(self.skipped(config(dataset(PARENT, recursive=True)))) == {
            PARENT: [(self.COPY, f'carries a local {PROPS_TEXT}')]}

    def test_the_other_marker_too(self, zfs_world):
        self.tree(zfs_world)
        zfs_world.set_prop(self.COPY, SRC_PROP, 'x')
        assert self.skipped(config(dataset(PARENT, recursive=True)))[PARENT] == [
            (self.COPY, f'carries a local {PROPS_TEXT}')]

    def test_a_received_marker(self, zfs_world):
        self.tree(zfs_world)
        zfs_world.received[(self.COPY, CID_PROP)] = 'orig'
        assert self.skipped(config(dataset(PARENT, recursive=True)))[PARENT] == [
            (self.COPY, f'carries a received {PROPS_TEXT}')]

    def test_an_inherited_marker_names_the_origin_and_only_the_topmost_is_listed(self, zfs_world):
        """A marker on `copy` is inherited by `c1` and `x`: one entry, the topmost, whose marker is its own."""
        self.tree(zfs_world)
        zfs_world.set_prop(self.COPY, CID_PROP, 'x')
        assert self.skipped(config(dataset(PARENT, recursive=True)))[PARENT] == [
            (self.COPY, f'carries a local {PROPS_TEXT}')]

    def test_a_parent_inside_a_copy_is_the_one_entry_and_names_where_the_marker_comes_from(self, zfs_world):
        self.tree(zfs_world)
        zfs_world.add('pool')
        zfs_world.set_prop('pool', CID_PROP, 'x')
        assert self.skipped(config(dataset(PARENT, recursive=True)))[PARENT] == [
            (PARENT, f'inherits {PROPS_TEXT} from pool')]

    def test_a_received_marker_above_the_parent(self, zfs_world):
        self.tree(zfs_world)
        zfs_world.add('pool')
        zfs_world.received[('pool', SRC_PROP)] = 'x'
        assert self.skipped(config(dataset(PARENT, recursive=True)))[PARENT] == [
            (PARENT, f'inherits {PROPS_TEXT} from pool')]

    def test_several_copies_are_listed_in_listing_order_one_entry_per_name(self, zfs_world):
        self.tree(zfs_world)
        zfs_world.set_prop(self.COPY, CID_PROP, 'x')
        zfs_world.set_prop(f'{self.COPY}/c1', SRC_PROP, 'y')             # nested under the first: no entry
        zfs_world.set_prop(f'{PARENT}/zcopy', SRC_PROP, 'z')
        got = self.skipped(config(dataset(PARENT, recursive=True)))[PARENT]
        assert [name for name, _ in got] == [self.COPY, f'{PARENT}/zcopy']

    def test_a_receive_target_below_the_parent(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/ok', f'{PARENT}/srv', f'{PARENT}/srv/c/x')
        cfg = config(dataset(PARENT, recursive=True),
                     remote_backup=RemoteServerConfig(target_dataset=f'{PARENT}/srv', enabled=False))
        assert self.skipped(cfg)[PARENT] == [(f'{PARENT}/srv', TARGET_WHY)]

    def test_a_local_destination_target_below_the_parent(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/ok', f'{PARENT}/vault', f'{PARENT}/vault/c')
        cfg = config(dataset(PARENT, recursive=True), destinations={DEST: Destination(url=f'zfs:{PARENT}/vault')})
        assert self.skipped(cfg)[PARENT] == [(f'{PARENT}/vault', TARGET_WHY)]

    def test_a_parent_inside_a_receive_target_names_the_target(self, zfs_world):
        add_tree(zfs_world, 'bk', 'bk/data', 'bk/data/a')
        cfg = config(dataset('bk/data', recursive=True), destinations={DEST: Destination(url='zfs:bk')})
        assert self.skipped(cfg)['bk/data'] == [('bk', TARGET_WHY)]

    def test_the_target_wins_over_a_marker_on_the_same_name(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/vault')
        zfs_world.set_prop(f'{PARENT}/vault', CID_PROP, 'x')
        cfg = config(dataset(PARENT, recursive=True), destinations={DEST: Destination(url=f'zfs:{PARENT}/vault')})
        assert self.skipped(cfg)[PARENT] == [(f'{PARENT}/vault', TARGET_WHY)]

    def test_a_parent_with_none_has_no_key_and_neither_has_one_that_failed(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/a', 'pool/other', 'pool/other/c', 'pool/bad', 'pool/bad/c')
        zfs_world.set_prop('pool/other/c', CID_PROP, 'x')
        zfs_world.list_failures['pool/bad'] = 'pool I/O is currently suspended'
        cfg = config(dataset(PARENT, recursive=True), dataset('pool/other', recursive=True),
                     dataset('pool/bad', recursive=True))
        exp = DatasetManager(cfg).expand_recursive()
        assert list(exp.skipped) == ['pool/other'] and list(exp.failed) == ['pool/bad']

    def test_a_marker_read_that_fails_leaves_no_entry(self, zfs_world):
        self.tree(zfs_world)
        zfs_world.set_prop(self.COPY, CID_PROP, 'x')
        zfs_world.copy_read_failures[PARENT] = 'pool I/O is currently suspended'
        exp = DatasetManager(config(dataset(PARENT, recursive=True))).expand_recursive()
        assert dict(exp.skipped) == {} and PARENT in exp.failed

    def test_a_configured_child_is_not_a_skipped_copy(self, zfs_world):
        """A descendant that is configured explicitly is not expanded at all: nothing to report for it."""
        add_tree(zfs_world, PARENT, f'{PARENT}/ok', self.COPY)
        zfs_world.set_prop(self.COPY, CID_PROP, 'x')
        cfg = config(dataset(PARENT, recursive=True), dataset(self.COPY))
        assert dict(self.skipped(cfg)) == {}

    def test_two_parents_have_their_own_lists(self, zfs_world):
        add_tree(zfs_world, 'pool/a', 'pool/a/c', 'pool/b', 'pool/b/c')
        zfs_world.set_prop('pool/a/c', CID_PROP, 'x')
        zfs_world.set_prop('pool/b/c', SRC_PROP, 'y')
        got = self.skipped(config(dataset('pool/a', recursive=True), dataset('pool/b', recursive=True)))
        assert {k: [n for n, _ in v] for k, v in got.items()} == {'pool/a': ['pool/a/c'], 'pool/b': ['pool/b/c']}

    def test_the_result_is_equal_whether_quiet_or_not(self, zfs_world):
        self.tree(zfs_world)
        zfs_world.set_prop(self.COPY, CID_PROP, 'x')
        cfg = config(dataset(PARENT, recursive=True))
        assert self.skipped(cfg) == self.skipped(cfg, quiet=True)
        assert child_names(DatasetManager(cfg).expand_recursive()) == child_names(
            DatasetManager(cfg).expand_recursive(quiet=True))


@pytest.mark.unit
class TestExpansionQuiet:
    """`quiet=True` (the health check): the two copy ERRORs go to DEBUG and the episodes are NOT marked, so
    the daemon's own once-per-episode ERROR is unaffected; `quiet=False` is what it always was."""

    COPY = f'{PARENT}/copy'

    def setup_world(self, world, target=False):
        add_tree(world, PARENT, f'{PARENT}/ok', self.COPY, f'{self.COPY}/c1')
        world.set_prop(self.COPY, CID_PROP, 'x')
        if target:
            add_tree(world, f'{PARENT}/vault', f'{PARENT}/vault/c')
        kw = {'destinations': {DEST: Destination(url=f'zfs:{PARENT}/vault')}} if target else {}
        return DatasetManager(config(dataset(PARENT, recursive=True), **kw))

    @pytest.mark.parametrize('target', [False, True], ids=['copy', 'copy+target'])
    def test_no_error_is_logged_and_the_episodes_stay_unmarked(self, zfs_world, caplog, target):
        manager = self.setup_world(zfs_world, target)
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            for _ in range(3):
                exp = manager.expand_recursive(quiet=True)
        assert records_at(caplog, logging.ERROR) == []
        assert manager._expand_copy_noted == set() and manager._expand_target_noted == set()
        assert sorted(child_names(exp)) == [f'{PARENT}/ok']
        debug = [r.getMessage() for r in caplog.records if r.levelno == logging.DEBUG]
        assert any('is a backup copy: not sent' in m and self.COPY in m for m in debug)
        if target:
            assert any('receive target' in m and f'{PARENT}/vault' in m for m in debug)

    def test_a_quiet_pass_does_not_use_up_the_daemons_error(self, zfs_world, caplog):
        manager = self.setup_world(zfs_world, target=True)
        manager.expand_recursive(quiet=True)
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            manager.expand_recursive()
            manager.expand_recursive()
        errors = records_at(caplog, logging.ERROR)
        assert len(errors) == 2 and any(self.COPY in e for e in errors) and any('receives copies' in e for e in errors)

    def test_not_quiet_logs_once_per_episode_and_marks_it(self, zfs_world, caplog):
        manager = self.setup_world(zfs_world, target=True)
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            for _ in range(3):
                manager.expand_recursive()
        assert len(records_at(caplog, logging.ERROR)) == 2
        assert manager._expand_copy_noted == {(PARENT, self.COPY)}
        assert manager._expand_target_noted == {(PARENT, f'{PARENT}/vault')}

    def test_the_default_is_not_quiet(self, zfs_world, caplog):
        manager = self.setup_world(zfs_world)
        with caplog.at_level(logging.ERROR, logger=BACKUP_LOGGER):
            manager.expand_recursive()
        assert len(records_at(caplog, logging.ERROR)) == 1

    def test_a_quiet_pass_still_logs_a_failed_discovery_as_before(self, zfs_world, caplog):
        manager = self.setup_world(zfs_world)
        zfs_world.list_failures[PARENT] = 'pool I/O is currently suspended'
        with caplog.at_level(logging.DEBUG, logger=BACKUP_LOGGER):
            exp = manager.expand_recursive(quiet=True)
        assert PARENT in exp.failed
        assert len(records_at(caplog, logging.ERROR)) == 1          # `quiet` covers the two copy ERRORs only


class _Scope:
    """A `scope()` for `expand_recursive` / `_copy_roots` / `received_datasets`: counts the contexts entered."""

    def __init__(self, error=None, fail_on=None):
        self.entered = 0
        self.error = error
        self.fail_on = fail_on
        self.calls = 0

    def __call__(self):
        self.calls += 1
        if self.error is not None and (self.fail_on is None or self.calls == self.fail_on):
            raise self.error
        return self

    def __enter__(self):
        self.entered += 1
        return self

    def __exit__(self, *exc):
        return False


@pytest.mark.unit
class TestExpansionScope:
    def test_every_zfs_call_of_the_discovery_runs_in_a_scope(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/a', 'pool/two', 'pool/two/b')
        scope = _Scope()
        DatasetManager(config(dataset(PARENT, recursive=True), dataset('pool/two', recursive=True))
                       ).expand_recursive(scope=scope)
        assert scope.entered == scope.calls == 4 and len(zfs_world.calls) == 4     # (list + marker read) x 2 parents

    def test_without_one_nothing_changes(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/a')
        assert child_names(DatasetManager(config(dataset(PARENT, recursive=True))).expand_recursive()) == [
            f'{PARENT}/a']

    @pytest.mark.parametrize('fail_on', [1, 2], ids=['listing', 'marker-read'])
    def test_a_timeout_from_the_scope_is_that_parents_failure_and_the_next_parent_goes_on(self, zfs_world, fail_on):
        add_tree(zfs_world, PARENT, f'{PARENT}/a', 'pool/two', 'pool/two/b')
        scope = _Scope(error=ZFSTimeoutError('the time limit is spent'), fail_on=fail_on)
        exp = DatasetManager(config(dataset(PARENT, recursive=True), dataset('pool/two', recursive=True))
                             ).expand_recursive(scope=scope)
        assert exp.failed == {PARENT: 'ZFSTimeoutError: the time limit is spent'}
        assert child_names(exp) == ['pool/two/b']
        assert PARENT not in exp.skipped

    def test_a_timeout_from_a_call_is_that_parents_failure(self, zfs_world, mocker):
        add_tree(zfs_world, PARENT, f'{PARENT}/a')
        mocker.patch('zfsbackup.backup_manager.zfs.get.property_sources',
                     side_effect=ZFSTimeoutError('zfs get took too long'))
        exp = DatasetManager(config(dataset(PARENT, recursive=True))).expand_recursive(scope=_Scope())
        assert exp.failed == {PARENT: 'ZFSTimeoutError: zfs get took too long'} and exp.children == []

    def test_copy_roots_runs_its_read_in_the_scope(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/copy')
        zfs_world.set_prop(f'{PARENT}/copy', CID_PROP, 'x')
        scope = _Scope()
        assert DatasetManager._copy_roots(PARENT, scope=scope) == {f'{PARENT}/copy': 'local'}
        assert scope.entered == 1 and len(zfs_world.calls) == 1

    def test_copy_roots_without_a_scope_is_unchanged(self, zfs_world):
        add_tree(zfs_world, PARENT, f'{PARENT}/copy')
        zfs_world.set_prop(f'{PARENT}/copy', CID_PROP, 'x')
        assert DatasetManager._copy_roots(PARENT) == {f'{PARENT}/copy': 'local'}

    def test_copy_roots_passes_a_scope_timeout_through_unchanged(self, zfs_world):
        add_tree(zfs_world, PARENT)
        error = ZFSTimeoutError('over the limit')
        with pytest.raises(ZFSTimeoutError) as caught:
            DatasetManager._copy_roots(PARENT, scope=_Scope(error=error))
        assert caught.value is error and zfs_world.calls == []

    def test_copy_roots_wraps_any_other_scope_failure_naming_the_parent(self, zfs_world):
        add_tree(zfs_world, PARENT)
        with pytest.raises(RuntimeError, match=f'at or below {PARENT}'):
            DatasetManager._copy_roots(PARENT, scope=_Scope(error=ValueError('odd')))

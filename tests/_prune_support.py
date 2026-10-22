"""A stateful stand-in for ZFS as the pruner sees it (plan B-4: batch fallback, held/cloned pre-skip).

`PrunePool` replaces `zfs.list` (snapshot listings with `userrefs`/`clones`), `zfs.exists`,
`zfs.get._exec_out` (the anchor reads) and `zfs.destroy` in `zfsbackup.backup_manager`. Its `destroy`
follows what the captures under tests/fixtures/openzfs/*/M3_Z8.txt show on OpenZFS 2.1.5, 2.2.2 and 2.4.1:

* a destroy call is ATOMIC: if any member is held, cloned, being sent or gone, the call raises (the bare
  `Exception` libzfseasy raises, carrying the captured stderr) and destroys NOTHING;
* a held snapshot is `dataset is busy` on 2.1/2.2 and `it's being held` on 2.4 (the dry run succeeds);
* a cloned snapshot is `snapshot has dependent clones` (the dry run fails too);
* a snapshot being sent is `dataset is busy` on every version (the dry run succeeds), and nothing in the
  listing shows it;
* with `recursive=True` the same check covers every descendant's copy of the name, and a hold on a CHILD's
  snapshot fails the whole recursive call although the parent's listing shows `userrefs 0`;
* a successful call returns one `destroy` line per object destroyed (a recursive one also lists the
  descendants' copies), so a test that counted returned lines would be off.

Every destroy call is recorded in `calls`; `empty_calls` collects any call made with no snapshot (the
`prune_pool` fixture fails the test at teardown if there was one). Nothing here touches ZFS.
"""

from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Set, Tuple

import pytest

import libzfseasy as zfs
from libzfseasy.types import Filesystem
from _openzfs_fixtures import VERSIONS, load_section
from _snapshot_support import listed_snapshot
from zfsbackup.backup_manager import DatasetManager
from zfsbackup.config import BackupConfig, DatasetConfig, RemoteDatasetConfig, RetentionRule

PREFIX = 'autosnap'
DEFAULT_VERSION = '2.2.2'


def hourly_name(hours_old: int, minutes_old: int = 0) -> str:
    """A managed snapshot name dated `hours_old` hours (+ minutes) ago, to the second."""
    dt = datetime.now(timezone.utc) - timedelta(hours=hours_old, minutes=minutes_old)
    return f"{PREFIX}_{dt.strftime('%Y%m%d%H%M%S')}"


def captured_stderr(version: str, case: str) -> str:
    """The stderr a real `zfs destroy` printed in the M3_Z8 capture `case` (e.g. `held.30_single`)."""
    return load_section(version, 'M3_Z8')['M3_Z8/' + case].stderr


class DestroyCall:
    def __init__(self, names: List[str], datasets: List[str], destroy: bool, recursive: bool, kwargs: dict):
        self.names = names          # short names, in call order
        self.datasets = datasets    # the dataset of each member
        self.destroy = destroy
        self.recursive = recursive
        self.kwargs = kwargs
        self.error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None

    def __repr__(self):
        return f'DestroyCall({len(self.names)} names, recursive={self.recursive}, error={self.error!r})'


class PrunePool:
    def __init__(self, mocker, version: str = DEFAULT_VERSION):
        self.version = version
        self.snaps: Dict[str, List[str]] = {}               # dataset -> short names
        self.holds: Dict[Tuple[str, str], int] = {}         # (dataset, short) -> hold count
        self.clones: Dict[Tuple[str, str], List[str]] = {}  # (dataset, short) -> clone dataset names
        self.sending: Set[Tuple[str, str]] = set()          # (dataset, short) a `zfs send` has open
        self.props: Dict[str, List[Tuple[str, str, str]]] = {}   # dataset -> [(property, value, source)]
        self.missing: Set[str] = set()                      # datasets `zfs.exists` reports absent
        self.calls: List[DestroyCall] = []
        self.get_calls: List[List[str]] = []                # the argv of every `zfs get` (anchor reads)
        #: every zfs call in order: ('get', dataset, recursive), ('value', dataset, property), ('destroy', names)
        self.events: List[tuple] = []
        self.empty_calls: List[DestroyCall] = []
        self.snapshot_lists: List[Tuple[str, bool]] = []    # (dataset, recursive) of each snapshot listing
        #: `fn(pool, call)` runs as a destroy call starts, before the checks (race injection).
        self.before_destroy: Optional[Callable[['PrunePool', DestroyCall], None]] = None
        #: the Nth (1-based) and later snapshot listings raise.
        self.list_fails_from: Optional[int] = None
        mocker.patch('zfsbackup.backup_manager.zfs.destroy', side_effect=self._destroy)
        mocker.patch('zfsbackup.backup_manager.zfs.list', side_effect=self._list)
        mocker.patch.object(zfs.get, '_exec_out', side_effect=self._get)
        mocker.patch.object(zfs.get, 'value', side_effect=self._value)

    # -- building the pool -----------------------------------------------------------------------
    def populate(self, dataset: str, old: int, fresh: bool = True) -> Tuple[str, List[str]]:
        """Give `dataset` one fresh snapshot (always kept) and `old` snapshots a retention of 1h/2h
        prunes, each in its own hour. -> `(fresh name, old names oldest first)`."""
        fresh_name = hourly_name(0, 10)
        olds = [hourly_name(3 + i) for i in reversed(range(old))]
        self.snaps[dataset] = olds + ([fresh_name] if fresh else [])
        return fresh_name, olds

    def add_snapshot(self, dataset: str, name: str) -> None:
        self.snaps.setdefault(dataset, []).append(name)

    def vanish(self, dataset: str, name: str) -> None:
        """Another process destroyed it."""
        self.snaps[dataset].remove(name)
        self.holds.pop((dataset, name), None)
        self.clones.pop((dataset, name), None)
        self.sending.discard((dataset, name))

    def hold(self, dataset: str, name: str, count: int = 1) -> None:
        self.holds[(dataset, name)] = count

    def release(self, dataset: str, name: str) -> None:
        self.holds.pop((dataset, name), None)

    def clone(self, dataset: str, name: str, *clone_names: str) -> None:
        self.clones[(dataset, name)] = list(clone_names or ('pool/clone1',))

    def unclone(self, dataset: str, name: str) -> None:
        self.clones.pop((dataset, name), None)

    def send(self, dataset: str, name: str) -> None:
        self.sending.add((dataset, name))

    def end_send(self, dataset: str, name: str) -> None:
        self.sending.discard((dataset, name))

    # -- results ---------------------------------------------------------------------------------
    @property
    def batches(self) -> List[List[str]]:
        """The short names of every destroy call, in order (failed ones included)."""
        return [c.names for c in self.calls]

    @property
    def sizes(self) -> List[int]:
        return [len(c.names) for c in self.calls]

    @property
    def ok_calls(self) -> List[DestroyCall]:
        return [c for c in self.calls if c.ok]

    def names_of(self, dataset: str) -> Set[str]:
        return set(self.snaps.get(dataset, []))

    # -- the fake zfs ----------------------------------------------------------------------------
    def _descendants(self, dataset: str) -> List[str]:
        return sorted(d for d in self.snaps if d.startswith(dataset + '/'))

    def _list(self, roots=None, types=None, recursive=False, properties=None, **kw):
        if types is None:                    # zfs.exists(ds)
            if roots.name in self.missing:
                raise RuntimeError(f"cannot open '{roots.name}': dataset does not exist")
            return [Filesystem(roots.name)]
        assert types == 'snapshot', f'unexpected zfs.list(types={types!r})'
        self.snapshot_lists.append((roots.name, recursive))
        if self.list_fails_from is not None and len(self.snapshot_lists) >= self.list_fails_from:
            raise RuntimeError(f"cannot open '{roots.name}': pool I/O is currently suspended")
        rows = []
        for name in self.snaps.get(roots.name, []):
            key = (roots.name, name)
            rows.append(listed_snapshot(
                name, roots.name,
                userrefs=str(self.holds.get(key, 0)),
                clones=','.join(self.clones[key]) if self.clones.get(key) else '-',
            ))
        return rows

    def _get(self, cmd):
        self.get_calls.append(list(cmd))
        root = cmd[-1]
        self.events.append(('get', root, '-r' in cmd))
        if '-r' in cmd:
            lines = []
            for name in sorted(self.props):
                if name == root or name.startswith(root + '/'):
                    lines += self._zget(name)
            return iter(lines)
        return iter(self._zget(root))

    def _value(self, ds, prop, sources=None, raw=False):
        """`zfs get -H -o value <prop> <ds>` (the per-destination anchor read): the effective value."""
        self.events.append(('value', ds.name, prop))
        for p, value, source in self.props.get(ds.name, []):
            if p == prop:
                return value
        return None

    def _zget(self, name):
        lines = [f'{name}\ttype\tfilesystem\t-\t-\n']
        for prop, value, source in self.props.get(name, []):
            received = value if source == 'received' else '-'
            lines.append(f'{name}\t{prop}\t{value}\t{received}\t{source}\n')
        return lines

    def _failure(self, members: List[Tuple[str, str]], recursive: bool, dry: bool) -> Optional[str]:
        """The captured stderr of the first member (in call order) zfs would refuse, or None."""
        v = self.version
        for ds, name in members:
            if name not in self.snaps.get(ds, []):
                return f"cannot destroy snapshot {ds}@{name}: dataset does not exist\n"
            scope = [ds] + (self._descendants(ds) if recursive else [])
            for d in scope:
                if (d, name) in self.clones and self.clones[(d, name)]:
                    return captured_stderr(v, 'cloned.30_single')
            if dry:
                continue                      # holds and sends only fail a real destroy
            for d in scope:
                if self.holds.get((d, name)):
                    return captured_stderr(v, 'held.30_single' if d == ds else 'rec.30_r_batch')
                if (d, name) in self.sending:
                    return captured_stderr(v, 'busy.30_single')
        return None

    def _destroy(self, snapshots, destroy=False, recursive=False, clones=False, **kw):
        if not isinstance(snapshots, (list, tuple)):
            snapshots = [snapshots]
        call = DestroyCall(
            [s.short for s in snapshots], [s.dataset.name for s in snapshots], destroy, recursive,
            dict(destroy=destroy, recursive=recursive, clones=clones, **kw),
        )
        self.calls.append(call)
        self.events.append(('destroy', tuple(call.names)))
        if not snapshots:
            self.empty_calls.append(call)
        if self.before_destroy is not None:
            self.before_destroy(self, call)
        members = list(zip(call.datasets, call.names))
        stderr = self._failure(members, recursive, dry=not destroy)
        if stderr is not None:
            call.error = stderr
            raise Exception(stderr.strip())          # libzfseasy raises the bare Exception
        lines = []
        for ds, name in members:
            scope = [ds] + (self._descendants(ds) if recursive else [])
            for d in scope:
                if name in self.snaps.get(d, []):
                    lines.append(f'{d}@{name}')
                    if destroy:
                        self.snaps[d].remove(name)
        return lines


@pytest.fixture
def prune_pool(mocker):
    pool = PrunePool(mocker)
    yield pool
    assert not pool.empty_calls, f'zfs.destroy was called with no snapshot: {pool.empty_calls}'


def retention_1h_2h() -> List[RetentionRule]:
    return [RetentionRule(timedelta(hours=1), timedelta(hours=2))]


def prune_manager(dataset: str = 'pool/data', *, recursive: bool = False, dry_run: bool = False,
                  children: Tuple[DatasetConfig, ...] = (), destinations: Tuple[str, ...] = ()):
    """`(manager, dsi)`: `dataset` pruned with a retention of interval 1h / keep 2h, optionally with
    configured remote destinations (each one costs a per-destination anchor read)."""
    parent = DatasetConfig(name=dataset, recursive=recursive, retention_rules=retention_1h_2h(),
                           remote=[RemoteDatasetConfig(destination=d) for d in destinations])
    config = BackupConfig(datasets=[parent, *children], snapshot_prefix=PREFIX, dry_run=dry_run)
    manager = DatasetManager(config)
    return manager, next(d for d in manager.datasets if d.name == dataset)


def records(caplog, level: int, containing: str = '', logger: str = 'zfsbackup.backup_manager') -> List[str]:
    """The messages of `caplog` records of exactly `level` (from `logger`) that contain `containing`."""
    return [r.getMessage() for r in caplog.records
            if r.levelno == level and r.name == logger and containing in r.getMessage()]

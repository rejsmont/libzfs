"""A `zfs` that takes snapshots and destroys them, for the A2c tests (the snapshot worker keeps out of backup copies).

`SnapWorld` is `_get_support.ZfsWorld` (the property reads and the name-only listings, answered by argv at the
`Popen` level, so the REAL libzfseasy builds every argv and parses every reply) plus the three things a snapshot
test needs it to do:

* `zfs snapshot [-r] [-o k=v] ds@n [ds2@n ...]`: atomic, like zfs: if any target already has the name (or does
  not exist) nothing is created and zfs exits 1 with `no snapshots were created`. Everything one command creates
  gets one txg (`created` records `(dataset, short, txg)` and `commands` the argv of each);
* `zfs list -H [-r] -t snapshot -o name,type,<SNAPSHOT_LIST_PROPERTIES> <ds>`: the six-column listing of
  `DatasetManager._list_snapshots_strict` (and of the pruner's copy read);
* `zfs destroy -v -p [-n] [-r] ds@a,b`: a recursive destroy removes the name from EVERY descendant, a copy
  included (`destroyed` records `(dataset, short)`), which is what the protection in front of it exists for.

So "no snapshot was created in the copy" is read off the world (`snapshots_below`), not off a call. Nothing
touches ZFS. The fakes of `list_hook` / `on_exec` / `fail` (ZfsWorld) inject failures at the `zfs` level.
"""

from typing import Callable, Dict, Iterable, List, Optional, Tuple

from _get_support import ZfsWorld, parse_argv
from _snapshot_support import default_guid, list_row
from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_SOURCE_DATASET
from zfsbackup.config import (
    BackupConfig, DatasetConfig, Destination, RemoteServerConfig, RetentionRule,
)

from datetime import timedelta

LOCAL = [(PROP_CLIENT_ID, 'h1', 'local'), (PROP_SOURCE_DATASET, 'pool/x', 'local')]
RECEIVED = [(PROP_CLIENT_ID, 'h1', 'received'), (PROP_SOURCE_DATASET, 'pool/x', 'received')]


def inherited(origin: str):
    return [(PROP_CLIENT_ID, 'h1', f'inherited from {origin}'),
            (PROP_SOURCE_DATASET, 'pool/x', f'inherited from {origin}')]


class SnapWorld(ZfsWorld):
    def __init__(self, mock_subprocess):
        super().__init__(mock_subprocess)
        self.snaps: Dict[str, Dict[str, int]] = {}         # dataset -> {short name: txg}
        self.txg = 1000
        self.created: List[Tuple[str, str, int]] = []      # (dataset, short, txg)
        self.destroyed: List[Tuple[str, str]] = []         # (dataset, short)
        self.commands: List[Tuple[str, List[str]]] = []    # ('snapshot' | 'destroy', argv after the binary)
        #: `fn(argv)` -> the physical lines to print for a recursive (`-r`) or depth-limited (`-d N`) `zfs list -t
        #: filesystem,volume` (the enumerations `_snapshot_sources` and `_walk_sources` make), or None to answer
        #: it from the world.
        self.list_hook: Optional[Callable[[List[str]], Optional[List[str]]]] = None

    # -- building ----------------------------------------------------------------------------
    def add(self, name, props=(), ptype='filesystem', snaps: Iterable[str] = ()):
        super().add(name, props, ptype)
        self.snaps.setdefault(name, {})
        for short in snaps:
            self.add_snap(name, short)
        return self

    def add_snap(self, dataset: str, short: str, txg: Optional[int] = None) -> None:
        if txg is None:
            self.txg += 1
            txg = self.txg
        self.snaps.setdefault(dataset, {})[short] = txg

    def drop(self, name: str) -> None:
        """The dataset (and its snapshots) is gone."""
        self.datasets.pop(name, None)
        self.snaps.pop(name, None)

    def fail_once(self, predicate: Callable[[List[str]], bool], stderr: str) -> None:
        """The next zfs call matching `predicate` exits 1; later ones are answered normally."""
        def once(argv):
            if predicate(argv):
                self.failures.remove(entry)
                return True
            return False
        entry = (once, stderr)
        self.failures.append(entry)

    def list_once(self, lines: List[str]) -> None:
        """The next recursive or `-d N` `zfs list -t filesystem,volume` prints `lines`; later ones are answered
        normally (the plain `snapshot -r` runs the same listing, which a test of the fallback wants answered
        truthfully)."""
        def hook(argv):
            self.list_hook = None
            return lines
        self.list_hook = hook

    def list_always(self, lines) -> None:
        """EVERY recursive or `-d N` `zfs list -t filesystem,volume` prints `lines` (or `lines(argv)`, which may
        return None to answer from the world). What a bad name in the pool is: `_snapshot_sources` meets it in the
        one recursive listing AND again in the walk around the copies, so a test that wants the fallback needs a
        listing that is bad every time."""
        self.list_hook = lines if callable(lines) else (lambda argv: lines)

    def listings(self, depth: Optional[int] = None, recursive: Optional[bool] = None) -> List[List[str]]:
        """The `zfs list -t filesystem,volume` argvs run so far, optionally only the `-d <depth>` ones or only the
        recursive (`-r`) ones. (A listing of snapshots, or of one name to see whether it exists, is not one.)"""
        out = []
        for argv in self.argvs:
            if argv[0] != 'list' or '-t' not in argv or argv[argv.index('-t') + 1] != 'filesystem,volume':
                continue
            if depth is not None and ('-d' not in argv or argv[argv.index('-d') + 1] != str(depth)):
                continue
            if recursive is not None and ('-r' in argv) != recursive:
                continue
            out.append(argv)
        return out

    # -- reading it back ---------------------------------------------------------------------
    def names_of(self, dataset: str) -> set:
        return set(self.snaps.get(dataset, {}))

    def snapshots_below(self, root: str, include_root: bool = True) -> Dict[str, set]:
        """`{dataset: short names}` for `root` (when `include_root`) and everything below it that holds any."""
        return {d: set(s) for d, s in self.snaps.items()
                if s and ((include_root and d == root) or d.startswith(root + '/'))}

    def created_in(self, root: str) -> List[Tuple[str, str, int]]:
        """What any snapshot command created at or below `root`."""
        return [c for c in self.created if c[0] == root or c[0].startswith(root + '/')]

    def destroyed_in(self, root: str) -> List[Tuple[str, str]]:
        return [d for d in self.destroyed if d[0] == root or d[0].startswith(root + '/')]

    def commands_of(self, verb: str) -> List[List[str]]:
        return [a for v, a in self.commands if v == verb]

    def _below(self, dataset: str) -> List[str]:
        return [d for d in self.datasets if d.startswith(dataset + '/')]

    # -- the fake process --------------------------------------------------------------------
    def _popen(self, cmd, *args, **kwargs):
        argv = list(cmd[1:])
        verb = argv[0]
        types = None
        if verb == 'list' and '-t' in argv:
            types = argv[argv.index('-t') + 1]
        mine = verb in ('snapshot', 'destroy') or (verb == 'list' and types == 'snapshot')
        hooked = None
        if (verb == 'list' and self.list_hook is not None and types == 'filesystem,volume'
                and ('-r' in argv or '-d' in argv)):
            hooked = self.list_hook(argv)
        if not mine and hooked is None:
            return super()._popen(cmd, *args, **kwargs)
        self.argvs.append(argv)
        if self.on_exec is not None:
            self.on_exec(argv)
        for predicate, stderr in self.failures:
            if predicate(argv):
                return self._proc([], rc=1, stderr=stderr)
        if hooked is not None:
            return self._proc(hooked)
        if verb == 'snapshot':
            return self._snapshot(argv)
        if verb == 'destroy':
            return self._destroy(argv)
        return self._list_snapshots(argv)

    def _list_snapshots(self, argv):
        columns, recursive, types, _, rest = parse_argv(argv)
        out: List[str] = []
        for root in rest:
            if root not in self.datasets:
                return self._proc([], rc=1, stderr=f"cannot open '{root}': dataset does not exist")
            for d in [root] + (self._below(root) if recursive else []):
                for short, txg in sorted(self.snaps.get(d, {}).items(), key=lambda kv: kv[1]):
                    if columns == 'name,type':                          # the pruner's name-only copy listing
                        out.append(f'{d}@{short}\tsnapshot\n')
                    else:
                        out.append(list_row(short, d, txg=txg, guid=default_guid(f'{d}@{short}')))
        return self._proc(out)

    def _snapshot(self, argv):
        args = argv[1:]
        recursive = '-r' in args
        targets: List[str] = []
        skip = False
        for a in args:
            if skip:
                skip = False
            elif a == '-o':
                skip = True
            elif not a.startswith('-'):
                targets.append(a)
        self.commands.append(('snapshot', list(argv)))
        members: List[Tuple[str, str]] = []
        for target in targets:
            ds, _, short = target.partition('@')
            if ds not in self.datasets:
                return self._proc([], rc=1, stderr=f"cannot open '{ds}': dataset does not exist")
            for d in [ds] + (self._below(ds) if recursive else []):
                if (d, short) not in members:
                    members.append((d, short))
        for d, short in members:
            if short in self.snaps.get(d, {}):
                return self._proc(
                    [], rc=1,
                    stderr=f"cannot create snapshot '{d}@{short}': dataset already exists\nno snapshots were created",
                )
        self.txg += 1
        for d, short in members:
            self.snaps.setdefault(d, {})[short] = self.txg
            self.created.append((d, short, self.txg))
        return self._proc([])

    def _destroy(self, argv):
        flags = [a for a in argv[1:] if a.startswith('-')]
        (target,) = [a for a in argv[1:] if not a.startswith('-')]
        recursive = '-r' in flags or '-R' in flags
        dry = '-n' in flags
        self.commands.append(('destroy', list(argv)))
        ds, _, shorts = target.partition('@')
        lines: List[str] = []
        for short in shorts.split(','):
            if short not in self.snaps.get(ds, {}):
                return self._proc([], rc=1, stderr=f"cannot destroy snapshot {ds}@{short}: dataset does not exist")
            for d in [ds] + (self._below(ds) if recursive else []):
                if short in self.snaps.get(d, {}):
                    lines.append(f'destroy\t{d}@{short}\n')
                    if not dry:
                        del self.snaps[d][short]
                        self.destroyed.append((d, short))
        lines.append('reclaim\t0\n')
        return self._proc(lines)


def make_manager(name: str = 'pool/data', *, recursive: bool = True, dry_run: bool = False,
                 targets: Iterable[str] = (), server_target: Optional[str] = None,
                 retention: bool = True, extra: Iterable[DatasetConfig] = ()):
    """`(manager, dsi)` for `name`: hourly, retention interval 1h / keep 2h when `retention` (so every old
    snapshot is a candidate and the newest is kept), `targets` as `zfs:` destinations and `server_target` as the
    enabled server's receive target. No `remote:` entries, so no anchor reads."""
    from zfsbackup.backup_manager import DatasetManager
    parent = DatasetConfig(
        name=name, recursive=recursive,
        retention_rules=[RetentionRule(timedelta(hours=1), timedelta(hours=2))] if retention else [],
    )
    config = BackupConfig(
        datasets=[parent, *extra], snapshot_prefix='autosnap', dry_run=dry_run,
        destinations={f'local{i}': Destination(url=f'zfs:{t}') for i, t in enumerate(targets)},
        remote_backup=RemoteServerConfig(target_dataset=server_target, enabled=True) if server_target else None,
    )
    manager = DatasetManager(config)
    return manager, next(d for d in manager.datasets if d.name == name)

"""Realistic `zfs get -H` / `zfs list -H` text for the tests of the property readers (plan B-1d).

`read_zfsbackup_properties` and friends are a TWO-PHASE read (`backup_manager._read_own_properties`):

    zfs get -H -o name,property,source -s local,received -t filesystem,volume [-r] all <ds>
        phase 1, discovery: no value column; with `-s` zfs prints NO `type` row, only the matching properties
    zfs get -H -o name,property,value,source [-r] -t filesystem,volume name,type,<names...> <ds>
        phase 2, strict `zfs.get.rows(received=False, verify_names=False)`: the names phase 1 found

and no name-only `zfs list` anywhere. A fake has to print what the real commands print, because the real
libzfseasy parsers run over it:

* phase 1 rows are `name<TAB>property<TAB>source`, one per matching property, an object's rows together;
* in phase 2 an explicit list prints EXACTLY `name`, `type`, then the requested properties, in order, one row
  each (an unset property prints `-` for value and source, an inherited one its `inherited from` source);
* with the property list `all` (other readers: health, rename) an object's rows START with its `type` row;
* the rows of one object are contiguous, objects come parent first.

`reads()` gives the Popen results of one own-dataset read (both phases) as `mock_subprocess.setup_multi`
configs; `source_lines` / `get_lines` / `list_lines` / `explicit_lines` give the text for a hand-built sequence or
a Popen-level router. Nothing here touches ZFS.
"""

import re
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from unittest.mock import MagicMock

Prop = Tuple[str, str, str]          # (property, value, source)


def _row(name: str, prop: str, value: str, source: str) -> str:
    return f'{name}\t{prop}\t{value}\t{source}\n'


def physical(lines: Iterable[str]) -> List[str]:
    """Re-cut rows into the physical `\\n`-terminated lines zfs's stdout really has: a VALUE that contains a
    newline is several lines on the wire, which is exactly how a hostile value forges rows."""
    return re.findall(r'[^\n]*\n', ''.join(lines))


def get_lines(name: str, props: Iterable[Prop], ptype: str = 'filesystem') -> List[str]:
    """`zfs get -H -o name,property,value,source all <name>`: the `type` row first, then `props`."""
    lines = [_row(name, 'type', ptype, '-')]
    for prop, value, source in props:
        lines.append(_row(name, prop, value, source))
    return physical(lines)


def explicit_lines(name: str, requested: Sequence[str], values: dict, ptype: str = 'filesystem') -> List[str]:
    """`zfs get -H -o name,property,value,source name,type,<requested...> <name>` for one object: exactly the
    rows zfs prints. `values[prop]` is `(value, source)`; a property missing from it is unset (`-`, `-`)."""
    lines = [_row(name, 'name', name, '-'), _row(name, 'type', ptype, '-')]
    for prop in requested:
        value, source = values.get(prop, ('-', '-'))
        lines.append(_row(name, prop, value, source))
    return physical(lines)


def list_lines(names: Iterable[str], ptype: str = 'filesystem') -> List[str]:
    """`zfs list -H -o name,type [-r] -t filesystem,volume <ds>`."""
    return [f'{n}\t{ptype}\n' for n in names]


OWN_SOURCES = ('local', 'received')


def source_lines(name: str, props: Iterable[Prop], sources: Optional[str] = 'local,received',
                 ptype: str = 'filesystem') -> List[str]:
    """`zfs get -H -o name,property,source [-s sources] -t T all <name>` for one object: `name<TAB>property<TAB>
    source`. With `-s` zfs prints only the properties from those sources and NO `type` row; without it, the
    `type` row first."""
    wanted = sources.split(',') if sources else None
    rows = [] if wanted else [('type', '', '-')]   # (the value column does not exist in this output)
    rows += [(p, v, s) for p, v, s in props if wanted is None or s in wanted]
    return physical(f'{name}\t{p}\t{s}\n' for p, _, s in rows)


def own_names(props: Iterable[Prop], prefix: str = 'org.zfsbackup:') -> List[str]:
    """What phase 1 finds in `props`: the `prefix` properties with a local/received source, first-seen order,
    each once (the names phase 2 asks for)."""
    return list(dict.fromkeys(p for p, _, s in props if p.startswith(prefix) and s in OWN_SOURCES))


def reads(name: str, props: Iterable[Prop], ptype: str = 'filesystem', also: Sequence[str] = ()) -> Tuple[tuple, tuple]:
    """The `setup_multi` configs of ONE `read_zfsbackup_properties(<name>[, also])`: phase 1 (the sources of its
    `org.zfsbackup:*` properties) then phase 2 (the strict explicit-list read of exactly those names, then the
    `also` names the caller always asks for, or `type` alone when there are none). No name-only `zfs list`."""
    props = list(props)
    values = {}
    for p, v, s in props:
        values.setdefault(p, (v, s))
    return (
        (source_lines(name, props, ptype=ptype),),
        (explicit_lines(name, list(dict.fromkeys(own_names(props) + list(also))), values, ptype),),
    )


def parse_argv(argv: List[str]):
    """`(columns, recursive, types, sources, positional)` of a get/list argv, flags in any order."""
    columns, recursive, types, sources, rest = None, False, None, None, []
    i = 1
    while i < len(argv):
        arg = argv[i]
        if arg == '-H':
            pass
        elif arg == '-r':
            recursive = True
        elif arg in ('-o', '-t', '-s'):
            i += 1
            if arg == '-o':
                columns = argv[i]
            elif arg == '-t':
                types = argv[i]
            else:
                sources = argv[i]
        elif arg.startswith('-') and len(arg) > 1:
            raise AssertionError(f'unexpected flag {arg} in {argv}')
        else:
            rest.append(arg)
        i += 1
    return columns, recursive, types, sources, rest


class ZfsWorld:
    """A `zfs` as the property readers see it, installed as `Popen`'s side effect: a world of datasets
    (`name -> (type, [(property, value, source), ...])`, in the order zfs would print them) answering, by argv,

    * `get -H -o name,property,source [-s S] [-r] [-t T] all <ds>...` (phase 1: no value; `-s` drops the `type` row);
    * `get -H -o name,property,value,source[,received] [-r] [-t T] <props> <ds>...` (`all`, or an explicit list
      printed as `name`, `type`, then each requested property, unset ones as `-`);
    * `get -H -o value [-s SOURCE] <prop> <ds>` (what `zfs.get.value` runs: the value as printed, unset `-`);
    * `list -H -o name,type [-r] [-t T] <ds>` (the name-only enumeration / cross-check);
    * anything else (`set`, `inherit`, `destroy`): an empty success, recorded in `argvs`, unless `other(argv)`
      returns lines.

    Values are printed RAW, so a value with a newline is several physical lines, the way a hostile one forges
    rows. `fail(predicate, stderr)` makes the matching argv exit 1. Every argv (after the binary) is in `argvs`.
    """

    def __init__(self, mock_subprocess):
        self.datasets: Dict[str, Tuple[str, List[Prop]]] = {}
        self.argvs: List[List[str]] = []
        self.failures: List[Tuple[Callable[[List[str]], bool], str]] = []
        self.other: Optional[Callable[[List[str]], Optional[List[str]]]] = None
        #: `fn(argv)` runs as every zfs call starts, before it is answered (inject a race between the two phases:
        #: change `self.datasets` when the argv is the second exec).
        self.on_exec: Optional[Callable[[List[str]], None]] = None
        mock_subprocess.side_effect = self._popen

    # -- building the world ------------------------------------------------------------------
    def add(self, name: str, props: Iterable[Prop] = (), ptype: str = 'filesystem') -> 'ZfsWorld':
        self.datasets[name] = (ptype, list(props))
        return self

    def set_props(self, name: str, props: Iterable[Prop]) -> None:
        """Replace the properties of an existing dataset (another process changed them)."""
        self.datasets[name] = (self.datasets[name][0], list(props))

    def fail(self, predicate: Callable[[List[str]], bool], stderr: str) -> None:
        self.failures.append((predicate, stderr))

    # -- what was asked ----------------------------------------------------------------------
    def verbs(self, verb: str) -> List[List[str]]:
        return [a for a in self.argvs if a[0] == verb]

    @property
    def gets(self) -> List[List[str]]:
        """The row reads (`-o name,property,...`, both phases), not the single-value ones."""
        return [a for a in self.verbs('get') if a[a.index('-o') + 1] != 'value']

    @property
    def discoveries(self) -> List[List[str]]:
        """Phase 1 of the two-phase read (`-o name,property,source`)."""
        return [a for a in self.verbs('get') if a[a.index('-o') + 1] == 'name,property,source']

    @property
    def value_reads(self) -> List[List[str]]:
        """Phase 2 of the two-phase read (`-o name,property,value,source`)."""
        return [a for a in self.verbs('get') if a[a.index('-o') + 1] == 'name,property,value,source']

    @property
    def values(self) -> List[List[str]]:
        return [a for a in self.verbs('get') if a[a.index('-o') + 1] == 'value']

    @property
    def verify_lists(self) -> List[List[str]]:
        return [a for a in self.verbs('list') if a[a.index('-o') + 1] == 'name,type']

    # -- the fake process --------------------------------------------------------------------
    @staticmethod
    def _proc(lines: List[str], rc: int = 0, stderr: str = ''):
        data = [line.encode('utf-8', 'surrogateescape') for line in lines]
        proc = MagicMock()
        proc.stdout.readline.side_effect = data + [b'']
        proc.stderr.readline.side_effect = [stderr.rstrip('\n') + '\n', ''] if stderr else ['']
        proc.poll.side_effect = [None] * len(data) + [rc] * 50
        proc.wait.return_value = rc
        proc.returncode = rc
        return proc

    def _scope(self, targets: List[str], recursive: bool, types: Optional[str]):
        wanted = None if types in (None, 'all') else set(types.split(','))
        names = []
        for target in targets:
            if target not in self.datasets:
                return None, f"cannot open '{target}': dataset does not exist"
            for name, (ptype, _) in self.datasets.items():
                if name == target or (recursive and name.startswith(target + '/')):
                    if (wanted is None or ptype in wanted) and name not in names:
                        names.append(name)
        return names, ''

    def _popen(self, cmd, *args, **kwargs):
        argv = list(cmd[1:])
        self.argvs.append(argv)
        if self.on_exec is not None:
            self.on_exec(argv)
        for predicate, stderr in self.failures:
            if predicate(argv):
                return self._proc([], rc=1, stderr=stderr)
        verb = argv[0]
        if verb in ('get', 'list') and '-o' in argv:
            columns, recursive, types, sources, rest = parse_argv(argv)
            if verb == 'get' and columns == 'name,property,source':          # property_sources (phase 1)
                assert rest[0] == 'all', argv
                names, error = self._scope(rest[1:], recursive, types)
                if names is None:
                    return self._proc([], rc=1, stderr=error)
                return self._proc(physical(
                    line for n in names
                    for line in source_lines(n, self.datasets[n][1], sources, self.datasets[n][0])))
            if verb == 'list':
                assert columns == 'name,type', argv
                names, error = self._scope(rest, recursive, types)
                if names is None:
                    return self._proc([], rc=1, stderr=error)
                return self._proc([f'{n}\t{self.datasets[n][0]}\n' for n in names])
            if columns == 'value':                                       # zfs.get.value
                prop, name = rest
                if name not in self.datasets:
                    return self._proc([], rc=1, stderr=f"cannot open '{name}': dataset does not exist")
                for p, value, source in self.datasets[name][1]:
                    if p == prop and (sources is None or source in sources.split(',')):
                        return self._proc(physical([value + '\n']))
                return self._proc(['-\n'] if sources is None else [])
            assert columns in ('name,property,value,source', 'name,property,value,received,source'), argv
            properties, *targets = rest
            names, error = self._scope(targets, recursive, types)
            if names is None:
                return self._proc([], rc=1, stderr=error)
            out: List[str] = []
            for name in names:
                ptype, props = self.datasets[name]
                if properties == 'all':
                    rows = [('type', ptype, '-')] + list(props)
                else:
                    requested = properties.split(',')
                    values = {}
                    for p, value, source in props:
                        values.setdefault(p, (value, source))
                    values.setdefault('name', (name, '-'))
                    values.setdefault('type', (ptype, '-'))
                    rows = [(p,) + values.get(p, ('-', '-')) for p in requested]
                for p, value, source in rows:
                    if columns == 'name,property,value,source':
                        out.append(_row(name, p, value, source))
                    else:
                        out.append(f'{name}\t{p}\t{value}\t{value if source == "received" else "-"}\t{source}\n')
            return self._proc(physical(out))
        lines = self.other(argv) if self.other is not None else None
        return self._proc(lines or [])


class GetReader:
    """The mixin behind `PrunePool` and the descendants' `FakeZfs`: they patch `GetCommand._exec_lines` (the
    class, because `zfs.get.rows` is a classmethod) with `_exec`, which answers the two commands `rows()` runs
    from the host's own state:

    * `self.props`   dataset -> [(property, value, source)] (datasets with no entry have no properties)
    * `self.snaps`   dataset -> [short names] (only to make the dataset exist)
    * `self.datasets` extra datasets that exist with neither

    A recursive read lists the root and every dataset below it, sorted; the root always exists. Nothing is
    answered for any other zfs subcommand (the host's other patches handle those).
    """

    def _init_reader(self):
        self.get_calls: List[List[str]] = []        # the argv of every phase-2 / `all` row read (`zfs get`)
        self.source_calls: List[List[str]] = []     # the argv of every phase-1 discovery (`-o name,property,source`)
        self.verify_calls: List[List[str]] = []     # the argv of every name-only cross-check `zfs list`
        self.datasets = set()

    def _props_of(self, name: str):
        return self.props.get(name, [])

    def _note_get(self, root: str, recursive: bool, cmd: List[str]) -> None:
        """Hook: a row read is starting (a test can record it, raise, or change the world)."""

    def _note_discover(self, root: str, recursive: bool, cmd: List[str]) -> None:
        """Hook: a phase-1 discovery is starting. By default it is announced like any row read."""
        self._note_get(root, recursive, cmd)

    def _exec(self, cmd):
        """The `GetCommand._exec_lines` stand-in: lines framed on `\\n` only, without it. A generator, because
        `property_sources` closes what it was given."""
        assert cmd[1] in ('get', 'list'), f'unexpected zfs command {cmd}'
        if cmd[1] == 'get' and cmd[cmd.index('-o') + 1] == 'name,property,source':
            text = ''.join(self._sources(cmd))
        else:
            text = ''.join(self._get(cmd) if cmd[1] == 'get' else self._verify(cmd))
        return (line for line in text.split('\n')[:-1])

    def _scope(self, root, recursive):
        known = set(self.props) | set(self.snaps) | self.datasets | {root}
        return sorted(d for d in known if d == root or (recursive and d.startswith(root + '/')))

    def _verify(self, cmd):
        """`zfs list -H -o name,type [-r] -t filesystem,volume <ds>`: the cross-check of `zfs.get.rows`."""
        columns, recursive, types, _, (root,) = parse_argv(cmd[1:])
        assert columns == 'name,type', cmd
        self.verify_calls.append(list(cmd))
        return list_lines(self._scope(root, recursive))

    def _sources(self, cmd):
        """Phase 1: `zfs get -H -o name,property,source [-s S] [-r] -t T all <ds>`."""
        columns, recursive, types, sources, (properties, root) = parse_argv(cmd[1:])
        assert properties == 'all', cmd
        self.source_calls.append(list(cmd))
        self._note_discover(root, recursive, cmd)
        lines = []
        for name in self._scope(root, recursive):
            lines += source_lines(name, self._props_of(name), sources)
        return lines

    def _get(self, cmd):
        """`zfs get -H -o name,property,value,source [-r] -t T <props> <ds>`: 4 columns, `type` first."""
        columns, recursive, types, _, (properties, root) = parse_argv(cmd[1:])
        assert columns == 'name,property,value,source', cmd
        self.get_calls.append(list(cmd))
        self._note_get(root, recursive, cmd)
        lines = []
        for name in self._scope(root, recursive):
            props = self._props_of(name)
            if properties == 'all':
                lines += get_lines(name, props)
            else:
                requested = properties.split(',')[2:]            # after `name,type`
                values = {}
                for prop, value, source in props:
                    values.setdefault(prop, (value, source))
                lines += explicit_lines(name, requested, values)
        return lines

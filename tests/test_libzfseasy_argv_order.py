"""Every option comes before the operands in the argv libzfseasy builds.

BSD, macOS and illumos `getopt` stop at the first operand (glibc permutes), so `zfs inherit compression -S ds` fails
with `cannot open '-S'` there while working on Linux. The guard below runs every command builder that can be called
with a fake `Popen` over arguments that switch on as many options as the builder offers, and checks the argv it
produced: once an operand has been seen, no later token may look like an option.

Option arguments (`-o k=v`, `-t types`, `-d 2`) are not operands; which options take one depends on the subcommand
(`-d` takes a depth for `list`/`get` and nothing for `allow`; `-S` takes a property for `list` and nothing for
`inherit`), hence the table.
"""

import io
import subprocess

import pytest

import libzfseasy as zfs
from libzfseasy.types import Bookmark, Dataset, Filesystem, Snapshot, Volume

pytestmark = [pytest.mark.unit, pytest.mark.subprocess]

TAKES_ARGUMENT = {
    'list': {'-d', '-o', '-t', '-s', '-S'},
    'get': {'-d', '-o', '-t', '-s'},
    'allow': {'-u', '-g'},
    'unallow': {'-u', '-g'},
    'send': {'-i', '-I', '-t'},
    'load-key': {'-L'},
    'create': {'-V', '-o'},
}
DEFAULT_TAKES_ARGUMENT = {'-o', '-x'}


class _Proc:
    def __init__(self):
        self.stdout = io.BufferedReader(io.BytesIO(b''))
        self.stderr = io.BufferedReader(io.BytesIO(b''))
        self.stdin = io.BytesIO()
        self.returncode = 0

    def poll(self):
        return 0

    def wait(self, timeout=None):
        return 0

    def terminate(self):
        pass

    kill = terminate


@pytest.fixture
def argvs(monkeypatch):
    seen = []

    def popen(cmd, *args, **kwargs):
        seen.append(list(cmd))
        return _Proc()

    monkeypatch.setattr(subprocess, 'Popen', popen)
    return seen


def operand_after_option(argv):
    """The first option-looking token that follows an operand in `argv` (without the binary), or None."""
    subcommand, tokens = argv[0], argv[1:]
    takes = TAKES_ARGUMENT.get(subcommand, DEFAULT_TAKES_ARGUMENT)
    operand_seen, skip = False, False
    for token in tokens:
        if skip:
            skip = False
        elif token.startswith('-') and len(token) > 1:
            if operand_seen:
                return token
            skip = token in takes
        else:
            operand_seen = True
    return None


FS = Filesystem('p/a')
SNAP = Snapshot.from_name('p/a@s')
SINCE = Snapshot.from_name('p/a@t')
PROPS = {'org.t:k': 'v'}

BUILDERS = {
    'list -r sort': lambda: list(zfs.list(FS, types='filesystem', recursive=True, properties=['used'], sort='used',
                                           asc=False)),
    'list -d': lambda: list(zfs.list(FS, depth=2)),
    'create filesystem': lambda: zfs.create.filesystem('p/n', properties=PROPS, parents=True, mount=False),
    'create volume': lambda: zfs.create.volume('p/n', '1G', properties=PROPS, sparse=True, parents=True),
    'snapshot': lambda: zfs.snapshot(FS, 's', properties=PROPS),
    'snapshot -r': lambda: zfs.snapshot(FS, 's', recursive=True, properties=PROPS),
    'snapshot many': lambda: zfs.snapshot.many([FS, Volume('p/v')], 's', properties=PROPS),
    'bookmark': lambda: zfs.bookmark(SNAP, 'bm'),
    'destroy dataset': lambda: zfs.destroy(FS, destroy=True, recursive=True, clones=True, force=True),
    'destroy snapshots': lambda: zfs.destroy.snapshots([SNAP, SINCE], destroy=True, recursive=True, clones=True,
                                                       force=True),
    'destroy bookmark': lambda: zfs.destroy.bookmark(Bookmark.from_name('p/a#b')),
    'rename': lambda: zfs.rename.filesystem(FS, 'p/b', force=True, parents=True, mount=False),
    'allow': lambda: zfs.allow(FS, ['send', 'snapshot'], users=['u'], groups=['g'], children=False),
    'allow everyone': lambda: zfs.allow(FS, ['send'], everyone=True, dataset=False),
    'allow create': lambda: zfs.allow.create(FS, ['send']),
    'unallow': lambda: zfs.unallow(FS, ['send'], users=['u'], groups=['g'], recursive=True),
    'clone': lambda: zfs.clone(SNAP, 'p/c', properties=PROPS, parents=True),
    'get': lambda: list(zfs.get(FS, properties=['compression'], types='filesystem', sources='local', recursive=True)),
    'get -d': lambda: list(zfs.get(FS, depth=1)),
    'get rows': lambda: zfs.get.rows(FS, properties=['compression'], recursive=True),
    'get value': lambda: zfs.get.value(FS, 'compression', sources='local'),
    'get property_sources': lambda: zfs.get.property_sources(FS, sources='local', recursive=True, prefix='org:'),
    'get property_sources properties': lambda: zfs.get.property_sources(FS, recursive=True, properties=['org.t:k']),
    'set': lambda: zfs.set(FS, {'org.t:k': 'v', 'compression': 'lz4'}),
    'inherit': lambda: zfs.inherit(FS, ['compression', 'org.t:k'], recursive=True, received=True),
    'load-key location': lambda: zfs.load_key(FS, location='file:///k'),
    'load-key -r': lambda: zfs.load_key(FS, recursive=True),
    'unload-key': lambda: zfs.unload_key(FS, recursive=True),
    'change-key': lambda: zfs.change_key(FS, load=True, location='file:///k', fmt='raw', iterations=1000),
    'change-key inherit': lambda: zfs.change_key(FS, inherit=True, load=True),
    'mount': lambda: zfs.mount(FS, flags='ro', properties={'atime': 'off'}, overlay=True, load_keys=True, force=True),
    'unmount': lambda: zfs.unmount(FS, force=True, unload_keys=True),
    'send snapshot': lambda: zfs.send.snapshot(SNAP, since=SINCE, intermediate=True, replicate=True, holds=True,
                                               properties=True, raw=True, compressed=True, embed=True,
                                               large_blocks=True, skip_missing=True, peek=False).close(),
    'send dataset': lambda: zfs.send.dataset(Dataset('p/a'), raw=True, compressed=True, large_blocks=True,
                                             peek=False).close(),
    'send resume': lambda: zfs.send.resume('1-abc-def', peek=False).close(),
    'receive filesystem': lambda: zfs.receive.filesystem(FS, props=PROPS, reset=['quota'], origin=SNAP, force=True,
                                                         holds=False, unmount=True, save=True, mount=False,
                                                         ignore_first=True).close(),
    'receive dataset': lambda: zfs.receive.dataset(FS, props=PROPS, force=True, save=True).close(),
    'receive abort': lambda: zfs.receive.abort(FS),
}


class TestOptionsPrecedeOperands:
    @pytest.mark.parametrize('name', sorted(BUILDERS))
    def test_no_option_follows_an_operand(self, argvs, name):
        BUILDERS[name]()
        assert argvs, name + ' ran no command'
        for argv in argvs:
            argv = argv[1:]                                 # without the zfs binary
            assert operand_after_option(argv) is None, 'option after an operand in %s' % argv

    def test_the_guard_notices_the_old_inherit_order(self):
        assert operand_after_option(['inherit', 'compression', '-r', '-S', 'p/a']) == '-r'
        assert operand_after_option(['inherit', '-r', '-S', 'compression', 'p/a']) is None

    def test_the_guard_skips_option_arguments_but_not_operands(self):
        assert operand_after_option(['snapshot', '-o', '-weird', 'p/a@s']) is None   # `-o`'s argument
        assert operand_after_option(['snapshot', 'p/a@s', '-o', 'k=v']) == '-o'
        assert operand_after_option(['list', '-d', '2', '-t', 'all', 'p/a']) is None
        assert operand_after_option(['inherit', '-S', 'compression', 'p/a']) is None  # `-S` is a flag here

    def test_every_builder_in_the_table_ran_something(self, argvs):
        for name, build in BUILDERS.items():
            argvs.clear()
            build()
            assert argvs, name

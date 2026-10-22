"""Framing safety of `zfs list -H` / `zfs get -H` (production-readiness plan, libzfseasy B1, B2, B3).

A property value may contain a newline or a tab, and `-H` output has no escaping, so a value can forge whole rows.
The three layers under test:

* B1  `Validate.value` refuses `\\n \\r \\t \\0` on every write path, before any `zfs` runs.
* B2  `Command._exec_lines` and the `list` / `get` parsers fail closed per row: a row that cannot have come from
      zfs's own iteration is skipped with a WARNING, never raised, and never turned into an object.
* B3  `zfs.get.value()` reads one property of one object, unambiguously (all of stdout is the value).

Nothing here runs `zfs`: processes are fakes fed bytes, or `sys.executable` children. The `FORGED_*` fixtures are the
bytes real `zfs` printed on OpenZFS 2.1.5, 2.2.2 and 2.4.1 (they were byte-identical on all three) for the pool built
by scratchpad `b123_setup.sh`; the live equivalents are in tests/test_real_zfs.py (`TestRealFraming`).
"""

import importlib
import io
import logging
import os
import random
import subprocess
import sys
import threading
import time
from unittest.mock import MagicMock

import pytest

import libzfseasy as zfs
from libzfseasy.types import (
    Bookmark, Dataset, Filesystem, InvalidNameError, InvalidValueError, Property, Snapshot, Validate, Volume, ZFS,
)
from libzfseasy.zfs import (
    Command, GetCommand, ListCommand, _MAX_QUOTED, _MAX_ROW_WARNINGS, _Scope, _skip_invalid_name, _wanted_types,
    _zfs_cmd,
)

zfs_module = importlib.import_module('libzfseasy.zfs')

# ---------------------------------------------------------------------------------------------------------------------
# Captured real output (identical bytes on OpenZFS 2.1.5, 2.2.2 and 2.4.1)
# ---------------------------------------------------------------------------------------------------------------------
# Pool `b123p`: datasets attacker ("line1\nline2"), forger ("X\nb123p/victim\tfilesystem\tFORGED"), tab ("a\tb"), ff
# (b"\xff\xfe"), empty, trail ("trail  "), cr ("cr\rx"), victim ("victimcfg"), recv (a `send -p` stream received with the
# forged value as a RECEIVED property), recv2 (received "r1\tr2"), recv3 (received "r1\tr2", then a local "l1\tl2").
# `zfs list -H -r -o name,type,org.t:v b123p` and `zfs get -H -r -o name,property,value,received,source name,type,org.t:v b123p`:
FORGED_LIST = (
    b'b123p\tfilesystem\t-\n'
    b'b123p/attacker\tfilesystem\tline1\n'
    b'line2\n'
    b'b123p/cr\tfilesystem\tcr\rx\n'
    b'b123p/empty\tfilesystem\t\n'
    b'b123p/ff\tfilesystem\t\xff\xfe\n'
    b'b123p/forger\tfilesystem\tX\n'
    b'b123p/victim\tfilesystem\tFORGED\n'
    b'b123p/recv\tfilesystem\tX\n'
    b'b123p/victim\tfilesystem\tRECV\n'
    b'b123p/recv2\tfilesystem\tr1\tr2\n'
    b'b123p/recv3\tfilesystem\tl1\tl2\n'
    b'b123p/src\tfilesystem\tX\n'
    b'b123p/victim\tfilesystem\tRECV\n'
    b'b123p/src2\tfilesystem\tr1\tr2\n'
    b'b123p/tab\tfilesystem\ta\tb\n'
    b'b123p/trail\tfilesystem\ttrail  \n'
    b'b123p/victim\tfilesystem\tvictimcfg\n'
)

FORGED_GET = (
    b'b123p\tname\tb123p\t-\t-\n'
    b'b123p\ttype\tfilesystem\t-\t-\n'
    b'b123p\torg.t:v\t-\t-\t-\n'
    b'b123p/attacker\tname\tb123p/attacker\t-\t-\n'
    b'b123p/attacker\ttype\tfilesystem\t-\t-\n'
    b'b123p/attacker\torg.t:v\tline1\n'
    b'line2\t-\tlocal\n'
    b'b123p/cr\tname\tb123p/cr\t-\t-\n'
    b'b123p/cr\ttype\tfilesystem\t-\t-\n'
    b'b123p/cr\torg.t:v\tcr\rx\t-\tlocal\n'
    b'b123p/empty\tname\tb123p/empty\t-\t-\n'
    b'b123p/empty\ttype\tfilesystem\t-\t-\n'
    b'b123p/empty\torg.t:v\t\t-\tlocal\n'
    b'b123p/ff\tname\tb123p/ff\t-\t-\n'
    b'b123p/ff\ttype\tfilesystem\t-\t-\n'
    b'b123p/ff\torg.t:v\t\xff\xfe\t-\tlocal\n'
    b'b123p/forger\tname\tb123p/forger\t-\t-\n'
    b'b123p/forger\ttype\tfilesystem\t-\t-\n'
    b'b123p/forger\torg.t:v\tX\n'
    b'b123p/victim\tfilesystem\tFORGED\t-\tlocal\n'
    b'b123p/recv\tname\tb123p/recv\t-\t-\n'
    b'b123p/recv\ttype\tfilesystem\t-\t-\n'
    b'b123p/recv\torg.t:v\tX\n'
    b'b123p/victim\tfilesystem\tRECV\tX\n'
    b'b123p/victim\tfilesystem\tRECV\treceived\n'
    b'b123p/recv@s\tname\tb123p/recv@s\t-\t-\n'
    b'b123p/recv@s\ttype\tsnapshot\t-\t-\n'
    b'b123p/recv@s\torg.t:v\tX\n'
    b'b123p/victim\tfilesystem\tRECV\t-\tinherited from b123p/recv\n'
    b'b123p/recv2\tname\tb123p/recv2\t-\t-\n'
    b'b123p/recv2\ttype\tfilesystem\t-\t-\n'
    b'b123p/recv2\torg.t:v\tr1\tr2\tr1\tr2\treceived\n'
    b'b123p/recv2@s\tname\tb123p/recv2@s\t-\t-\n'
    b'b123p/recv2@s\ttype\tsnapshot\t-\t-\n'
    b'b123p/recv2@s\torg.t:v\tr1\tr2\t-\tinherited from b123p/recv2\n'
    b'b123p/recv3\tname\tb123p/recv3\t-\t-\n'
    b'b123p/recv3\ttype\tfilesystem\t-\t-\n'
    b'b123p/recv3\torg.t:v\tl1\tl2\tr1\tr2\tlocal\n'
    b'b123p/recv3@s\tname\tb123p/recv3@s\t-\t-\n'
    b'b123p/recv3@s\ttype\tsnapshot\t-\t-\n'
    b'b123p/recv3@s\torg.t:v\tl1\tl2\t-\tinherited from b123p/recv3\n'
    b'b123p/src\tname\tb123p/src\t-\t-\n'
    b'b123p/src\ttype\tfilesystem\t-\t-\n'
    b'b123p/src\torg.t:v\tX\n'
    b'b123p/victim\tfilesystem\tRECV\t-\tlocal\n'
    b'b123p/src@s\tname\tb123p/src@s\t-\t-\n'
    b'b123p/src@s\ttype\tsnapshot\t-\t-\n'
    b'b123p/src@s\torg.t:v\tX\n'
    b'b123p/victim\tfilesystem\tRECV\t-\tinherited from b123p/src\n'
    b'b123p/src2\tname\tb123p/src2\t-\t-\n'
    b'b123p/src2\ttype\tfilesystem\t-\t-\n'
    b'b123p/src2\torg.t:v\tr1\tr2\t-\tlocal\n'
    b'b123p/src2@s\tname\tb123p/src2@s\t-\t-\n'
    b'b123p/src2@s\ttype\tsnapshot\t-\t-\n'
    b'b123p/src2@s\torg.t:v\tr1\tr2\t-\tinherited from b123p/src2\n'
    b'b123p/tab\tname\tb123p/tab\t-\t-\n'
    b'b123p/tab\ttype\tfilesystem\t-\t-\n'
    b'b123p/tab\torg.t:v\ta\tb\t-\tlocal\n'
    b'b123p/trail\tname\tb123p/trail\t-\t-\n'
    b'b123p/trail\ttype\tfilesystem\t-\t-\n'
    b'b123p/trail\torg.t:v\ttrail  \t-\tlocal\n'
    b'b123p/victim\tname\tb123p/victim\t-\t-\n'
    b'b123p/victim\ttype\tfilesystem\t-\t-\n'
    b'b123p/victim\torg.t:v\tvictimcfg\t-\tlocal\n'
)

# `zfs get -H -o value org.t:v b123p/<name>` (all sources) and the same with `-s local`.
FORGED_VALUE = {
    'victim': b'victimcfg\n',
    'attacker': b'line1\nline2\n',
    'forger': b'X\nb123p/victim\tfilesystem\tFORGED\n',
    'tab': b'a\tb\n',
    'ff': b'\xff\xfe\n',
    'empty': b'\n',
    'trail': b'trail  \n',
    'cr': b'cr\rx\n',
    'recv': b'X\nb123p/victim\tfilesystem\tRECV\n',
    'recv2': b'r1\tr2\n',
    'recv3': b'l1\tl2\n',
}
FORGED_VALUE_LOCAL = {
    'victim': b'victimcfg\n',
    'attacker': b'line1\nline2\n',
    'forger': b'X\nb123p/victim\tfilesystem\tFORGED\n',
    'tab': b'a\tb\n',
    'ff': b'\xff\xfe\n',
    'empty': b'\n',
    'trail': b'trail  \n',
    'cr': b'cr\rx\n',
    'recv': b'',
    'recv2': b'',
    'recv3': b'l1\tl2\n',
}


# ---------------------------------------------------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------------------------------------------------

UNFRAMEABLE = [('\n', 'newline'), ('\r', 'carriage return'), ('\t', 'tab'), ('\x00', 'NUL')]


class _Proc:
    """A finished child whose pipes hold exactly these bytes (what `Popen(stdout=PIPE, stderr=PIPE)` gives)."""

    def __init__(self, stdout=b'', stderr=b'', returncode=0):
        self.stdout = io.BufferedReader(io.BytesIO(stdout))
        self.stderr = io.BufferedReader(io.BytesIO(stderr))
        self.returncode = returncode

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        return self.returncode

    def terminate(self):
        pass

    kill = terminate


class _Feed:
    """Replaces `subprocess.Popen`: every call gets a fresh process holding the configured bytes."""

    def __init__(self):
        self.calls = []
        self.set()

    def set(self, stdout=b'', stderr=b'', returncode=0):
        self._args = (stdout, stderr, returncode)
        return self

    def __call__(self, cmd, *args, **kwargs):
        assert 'text' not in kwargs and 'universal_newlines' not in kwargs, kwargs   # bytes, decoded by us
        self.calls.append(list(cmd))
        return _Proc(*self._args)

    @property
    def argv(self):
        """The argv of the last call, without the leading zfs binary."""
        return self.calls[-1][1:]


@pytest.fixture
def feed(monkeypatch):
    feeder = _Feed()
    monkeypatch.setattr(subprocess, 'Popen', feeder)
    return feeder


@pytest.fixture(autouse=True)
def _fresh_warned_names(monkeypatch):
    """The once-per-name registry is process-global; every test starts with an empty, never-expired one."""
    monkeypatch.setattr(zfs_module, '_warned_names', set())
    monkeypatch.setattr(zfs_module, '_warned_names_since', None)


def _warnings(caplog):
    return [r.getMessage() for r in caplog.records if r.name == 'libzfseasy' and r.levelno == logging.WARNING]


def _rows(*rows):
    """`-H` output from rows given as tuples of columns or ready-made strings."""
    return ''.join(('\t'.join(r) if isinstance(r, tuple) else r) + '\n' for r in rows).encode('utf-8', 'surrogateescape')


def _get_rows(name, dstype='filesystem', explicit=False, **props):
    """The `name,property,value,received,source` rows of one object: the rows zfs prints first, then each property as
    local. zfs opens an object with `type` for `all` and with `name`, `type` for an explicit property list (which
    libzfseasy sends as `name,type,<the caller's properties>`): pass `explicit=True` for the latter."""
    rows = [(name, 'name', name, '-', '-')] if explicit else []
    rows.append((name, 'type', dstype, '-', '-'))
    for prop, value in props.items():
        rows.append((name, prop.replace('__', ':'), value, '-', 'local'))
    return rows


def _bounded(fn, timeout=30):
    """Run `fn()` on a thread; fail the test (rather than hang the suite) if it does not finish."""
    box = {}

    def target():
        try:
            box['result'] = fn()
        except BaseException as exc:      # noqa: BLE001 - handed back to the test
            box['exc'] = exc

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        pytest.fail('did not finish within %ss: a pipe deadlock or spin regression' % timeout)
    return box.get('result'), box.get('exc')


@pytest.fixture
def children(monkeypatch):
    """Records every real child `Popen` starts, and kills any survivor when the test ends."""
    procs = []
    real_popen = subprocess.Popen       # the (guarded) class; subclassing keeps the guard

    class Recording(real_popen):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            procs.append(self)

    monkeypatch.setattr(subprocess, 'Popen', Recording)
    yield procs
    for proc in procs:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=10)


def _py(script):
    return [sys.executable, '-c', script]


# ---------------------------------------------------------------------------------------------------------------------
# B1: every write path refuses before Popen
# ---------------------------------------------------------------------------------------------------------------------

def _stream_process():
    """A receive child that is inert: its pipes are at EOF (a MagicMock readline would never return '' and spin the
    stderr reader thread forever)."""
    process = MagicMock()
    process.stdin = io.BufferedWriter(io.BytesIO())
    process.stdout.readline.return_value = ''
    process.stderr.readline.return_value = ''
    process.poll.return_value = None
    return process


# id -> callable(bad_value); each puts the value on one write path
WRITE_PATHS = {
    'set': lambda bad: zfs.set(Filesystem('testpool/fs'), {'org.t:v': bad}),
    'set-Property-object': lambda bad: zfs.set(Filesystem('testpool/fs'), {'org.t:v': Property(bad, 'local')}),
    'set-native': lambda bad: zfs.set(Filesystem('testpool/fs'), {'compression': bad}),
    'create.filesystem': lambda bad: zfs.create.filesystem('testpool/new', {'org.t:v': bad}),
    'create.volume-prop': lambda bad: zfs.create.volume('testpool/vol', '1G', {'org.t:v': bad}),
    'create.volume-size': lambda bad: zfs.create.volume('testpool/vol', '1G' + bad),
    'snapshot': lambda bad: zfs.snapshot(Filesystem('testpool/fs'), 'snap', properties={'org.t:v': bad}),
    'snapshot-recursive': lambda bad: zfs.snapshot(Filesystem('testpool/fs'), 'snap', recursive=True,
                                                   properties={'org.t:v': bad}),
    'clone': lambda bad: zfs.clone(Snapshot(Filesystem('testpool/fs'), 'snap'), 'testpool/clone',
                                   properties={'org.t:v': bad}),
    'clone-Property-object': lambda bad: zfs.clone(Snapshot(Filesystem('testpool/fs'), 'snap'), 'testpool/clone',
                                                   properties={'org.t:v': Property(bad, 'local')}),
    'receive.filesystem': lambda bad: zfs.receive.filesystem(Filesystem('testpool/fs'), props={'org.t:v': bad}),
    'receive.dataset': lambda bad: zfs.receive.dataset(Volume('testpool/vol'), props={'org.t:v': bad}),
    'receive-dispatch': lambda bad: zfs.receive(Filesystem('testpool/fs'), props={'org.t:v': bad}),
    'mount-properties': lambda bad: zfs.mount(Filesystem('testpool/fs'), properties={'context': bad}),
    'mount-flags-list': lambda bad: zfs.mount(Filesystem('testpool/fs'), flags=['ro', 'x' + bad + 'y']),
    'mount-flags-str': lambda bad: zfs.mount(Filesystem('testpool/fs'), flags='ro,x' + bad + 'y'),
    'change_key-location': lambda bad: zfs.change_key(Filesystem('testpool/fs'), location='file:///k' + bad),
    'change_key-format': lambda bad: zfs.change_key(Filesystem('testpool/fs'), fmt='raw' + bad),
}

# id -> (callable(clean_value), a substring each argv must then contain)
CLEAN_PATHS = {
    'set': (lambda v: zfs.set(Filesystem('testpool/fs'), {'org.t:v': v}), 'org.t:v='),
    'create.filesystem': (lambda v: zfs.create.filesystem('testpool/new', {'org.t:v': v}), 'org.t:v='),
    'create.volume': (lambda v: zfs.create.volume('testpool/vol', '1G', {'org.t:v': v}), 'org.t:v='),
    'snapshot': (lambda v: zfs.snapshot(Filesystem('testpool/fs'), 'snap', properties={'org.t:v': v}), 'org.t:v='),
    'clone': (lambda v: zfs.clone(Snapshot(Filesystem('testpool/fs'), 'snap'), 'testpool/clone',
                                  properties={'org.t:v': v}), 'org.t:v='),
    'mount-properties': (lambda v: zfs.mount(Filesystem('testpool/fs'), properties={'context': v}), 'context='),
    'mount-flags': (lambda v: zfs.mount(Filesystem('testpool/fs'), flags=['ro', v]), 'ro'),
    'change_key-location': (lambda v: zfs.change_key(Filesystem('testpool/fs'), location=v), 'keylocation='),
    'change_key-format': (lambda v: zfs.change_key(Filesystem('testpool/fs'), fmt=v), 'keyformat='),
}


class TestWritePathsRefuseBeforeExec:
    """B1: a value `-H` output cannot frame never reaches `zfs` -- no Popen at all, on any write path."""

    @pytest.fixture(autouse=True)
    def _inert_children(self, mock_subprocess):
        """If a regression ever let a value through, the command would run against this inert child (EOF on every
        pipe) and the test would fail on `assert_not_called`, instead of spinning on a bare MagicMock."""
        mock_subprocess.setup(stdout='')

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('path', sorted(WRITE_PATHS))
    @pytest.mark.parametrize('char,what', UNFRAMEABLE, ids=[w for _, w in UNFRAMEABLE])
    def test_refused_and_popen_never_called(self, mock_subprocess, path, char, what):
        mock_subprocess.setup(stdout='')
        mock_subprocess.reset_mock()
        with pytest.raises(ValueError) as info:
            WRITE_PATHS[path]('line1' + char + 'line2')
        assert isinstance(info.value, InvalidValueError)
        assert what in str(info.value)
        assert 'line1' not in str(info.value)          # the value itself is never echoed
        mock_subprocess.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_recursive_snapshot_refuses_before_the_child_listing_runs(self, mock_subprocess):
        """The recursive form lists the children (a `zfs list`) before it snapshots; the check comes first."""
        mock_subprocess.setup_multi((['testpool/fs\tfilesystem\n'],), ('',))
        with pytest.raises(InvalidValueError):
            zfs.snapshot(Filesystem('testpool/fs'), 'snap', recursive=True, properties={'org.t:v': 'a\nb'})
        assert mock_subprocess.call_count == 0

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_refused_set_leaves_the_object_unchanged(self, mock_subprocess):
        fs = Filesystem('testpool/fs')
        with pytest.raises(InvalidValueError):
            zfs.set(fs, {'org.t:v': 'a\tb'})
        assert fs['org.t:v'] is None
        mock_subprocess.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_one_bad_value_among_good_ones_refuses_the_whole_call(self, mock_subprocess):
        with pytest.raises(InvalidValueError, match=r"'org.t:bad'"):
            zfs.set(Filesystem('testpool/fs'), {'compression': 'lz4', 'org.t:bad': 'x\ny', 'org.t:ok': 'fine'})
        mock_subprocess.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_huge_value_is_not_echoed_by_a_write_path(self, mock_subprocess):
        with pytest.raises(InvalidValueError) as info:
            zfs.set(Filesystem('testpool/fs'), {'org.t:v': 'SECRET' * 20000 + '\n'})
        assert 'SECRET' not in str(info.value)
        assert len(str(info.value)) < 300
        assert 'offset 120000' in str(info.value)

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('path', sorted(CLEAN_PATHS))
    def test_clean_values_still_reach_zfs(self, mock_subprocess, path):
        """The guard is not a blanket refusal: spaces, quotes, unicode, `=` and `%` pass through to argv."""
        mock_subprocess.setup_multi(('',), (['testpool/clone\tfilesystem\n'],), ('',))     # (clone lists the result)
        value = "it's a 'clean' value = 100% \u00e9\u4e2d"
        fn, expect = CLEAN_PATHS[path]
        if path == 'mount-flags':
            value = 'noatime'
        elif path == 'change_key-location':
            value = 'file:///keys/k 1'
        elif path == 'change_key-format':
            value = 'passphrase'
        fn(value)
        assert mock_subprocess.call_count >= 1
        argv = list(mock_subprocess.call_args_list[0 if path != 'snapshot' else -1][0][0])
        assert any(expect in a for a in argv)
        if path in ('set', 'create.filesystem', 'create.volume', 'snapshot', 'clone', 'mount-properties'):
            assert any(a.endswith(value) for a in argv)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_clean_receive_props_reach_zfs(self, mock_subprocess):
        mock_subprocess.return_value = _stream_process()
        zfs.receive.filesystem(Filesystem('testpool/fs'), props={'org.t:v': 'fine value'})
        assert 'org.t:v=fine value' in list(mock_subprocess.call_args[0][0])

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_clean_recursive_snapshot_still_lists_then_snapshots(self, mock_subprocess):
        mock_subprocess.setup_multi((['testpool/fs\tfilesystem\n'],), ('',))
        zfs.snapshot(Filesystem('testpool/fs'), 'snap', recursive=True, properties={'org.t:v': 'fine'})
        assert mock_subprocess.call_count == 2

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_non_string_values_are_checked_as_their_str(self, mock_subprocess):
        """Every command puts `str(value)` into argv, so that is what is checked."""
        class Weird:
            def __str__(self):
                return 'a\nb'

        with pytest.raises(InvalidValueError):
            zfs.set(Filesystem('testpool/fs'), {'org.t:v': Weird()})
        mock_subprocess.assert_not_called()
        mock_subprocess.setup(stdout='')
        zfs.set(Filesystem('testpool/fs'), {'quota': 1024, 'org.t:flag': True})      # ints/bools pass
        assert mock_subprocess.call_count == 1

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_change_key_inherit_without_location_is_unaffected(self, mock_subprocess):
        mock_subprocess.setup(stdout='')
        zfs.change_key(Filesystem('testpool/fs'), inherit=True)
        assert mock_subprocess.call_count == 1


# ---------------------------------------------------------------------------------------------------------------------
# B2: Command._exec_lines
# ---------------------------------------------------------------------------------------------------------------------

class TestExecLinesMocked:
    """`_exec_lines` yields stdout exactly as printed: only the line's own `\\n` goes."""

    @staticmethod
    def _lines(feed, stdout, **kw):
        feed.set(stdout, **kw)
        return list(Command._exec_lines(['zfs', 'x']))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_plain_lines(self, feed):
        assert self._lines(feed, b'a\nb\nc\n') == ['a', 'b', 'c']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_carriage_return_is_data_not_a_line_break(self, feed):
        assert self._lines(feed, b'cr\rx\nend\r\n') == ['cr\rx', 'end\r']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_trailing_empty_column_is_kept(self, feed):
        """`name\\t\\n` is a row whose value is empty: stripping it would turn two columns into one."""
        assert self._lines(feed, b'name\t\n\t\t\n') == ['name\t', '\t\t']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_trailing_and_leading_spaces_are_kept(self, feed):
        assert self._lines(feed, b'  lead\ntrail  \n \n') == ['  lead', 'trail  ', ' ']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_blank_lines_are_yielded(self, feed):
        assert self._lines(feed, b'a\n\n\nb\n\n') == ['a', '', '', 'b', '']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_last_line_without_a_newline_is_kept(self, feed):
        assert self._lines(feed, b'a\nb') == ['a', 'b']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_no_output_yields_nothing(self, feed):
        assert self._lines(feed, b'') == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_invalid_utf8_becomes_surrogates_that_round_trip(self, feed):
        (line,) = self._lines(feed, b'v\xff\xfe\n')
        assert line == 'v\udcff\udcfe'
        assert line.encode('utf-8', 'surrogateescape') == b'v\xff\xfe'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_valid_multibyte_utf8_is_decoded(self, feed):
        assert self._lines(feed, 'caf\u00e9 \u4e2d\u6587 \U0001F600\n'.encode('utf-8')) == ['caf\u00e9 \u4e2d\u6587 \U0001F600']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_nul_in_output_is_kept(self, feed):
        assert self._lines(feed, b'a\x00b\n') == ['a\x00b']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_str_lines_from_a_text_mode_mock_work_too(self, mock_subprocess):
        mock_subprocess.setup(stdout=['a\t\n', '\n', 'b  \n'])
        assert list(Command._exec_lines(['zfs', 'x'])) == ['a\t', '', 'b  ']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_non_zero_exit_raises_a_bare_exception_with_stderr(self, feed):
        feed.set(b'', stderr=b"cannot open 'p/x': dataset does not exist\nsecond line\n", returncode=1)
        with pytest.raises(Exception) as info:
            list(Command._exec_lines(['zfs', 'x']))
        assert type(info.value) is Exception
        assert str(info.value) == "Cannot open 'p/x': dataset does not exist\nsecond line"

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('lead', [b'', b'\n', b'  \n\n'], ids=['no-blank', 'blank-first', 'whitespace-first'])
    def test_only_the_first_non_empty_stderr_line_is_capitalised(self, feed, lead):
        """The dependent-clones failure: the continuation lines (a dataset name!) are the real zfs text.
        Mutation check: per-line capitalisation restored in `_start_stderr_reader_lines` fails these 3."""
        feed.set(b'', returncode=1, stderr=lead + b"cannot destroy 'p/fs@s': snapshot has dependent clones\n"
                                                 b"use '-R' to destroy the following datasets:\np/clone3\n")
        with pytest.raises(Exception) as info:
            list(Command._exec_lines(['zfs', 'destroy', 'p/fs@s']))
        assert str(info.value) == ("Cannot destroy 'p/fs@s': snapshot has dependent clones\n"
                                   "use '-R' to destroy the following datasets:\np/clone3")

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_every_line_is_yielded_before_the_failure_is_raised(self, feed):
        feed.set(b'a\nb\n', stderr=b'boom\n', returncode=2)
        seen = []
        with pytest.raises(Exception, match='Boom'):
            for line in Command._exec_lines(['zfs', 'x']):
                seen.append(line)
        assert seen == ['a', 'b']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_stderr_bytes_with_invalid_utf8_do_not_kill_the_reader(self, feed):
        feed.set(b'', stderr=b'bad \xff byte\n', returncode=1)
        with pytest.raises(Exception) as info:
            list(Command._exec_lines(['zfs', 'x']))
        assert 'Bad \ufffd byte' == str(info.value)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_stderr_str_lines_from_a_text_mode_mock_work_too(self, mock_subprocess):
        mock_subprocess.setup(stdout=['a\n'], stderr=['oops\n'], returncode=1)
        with pytest.raises(Exception, match='^Oops$'):
            list(Command._exec_lines(['zfs', 'x']))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_blank_stderr_lines_are_dropped_and_success_ignores_stderr(self, feed):
        feed.set(b'a\n', stderr=b'\n  \nwarn\n', returncode=1)
        with pytest.raises(Exception, match='^Warn$'):
            list(Command._exec_lines(['zfs', 'x']))
        feed.set(b'a\n', stderr=b'chatter\n', returncode=0)
        assert list(Command._exec_lines(['zfs', 'x'])) == ['a']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_failure_without_stderr_still_raises(self, feed):
        feed.set(b'', stderr=b'', returncode=1)
        with pytest.raises(Exception):
            list(Command._exec_lines(['zfs', 'x']))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_abandoned_generator_terminates_and_reaps_the_child(self, mock_subprocess):
        process = MagicMock()
        process.stdout.readline.side_effect = [b'one\n', b'two\n']
        process.stderr.readline.return_value = b''
        process.poll.return_value = None            # still running when abandoned
        mock_subprocess.return_value = process
        gen = Command._exec_lines(['zfs', 'x'])
        assert next(gen) == 'one'
        gen.close()
        process.terminate.assert_called_once()
        assert process.wait.called

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_abandoned_generator_escalates_to_kill(self, mock_subprocess, monkeypatch):
        monkeypatch.setattr(zfs_module, '_TERMINATE_TIMEOUT', 0.01)
        process = MagicMock()
        process.stdout.readline.side_effect = [b'one\n', b'two\n']
        process.stderr.readline.return_value = b''
        process.poll.return_value = None
        process.wait.side_effect = [subprocess.TimeoutExpired('zfs', 0.01), 0, 0, 0]
        mock_subprocess.return_value = process
        gen = Command._exec_lines(['zfs', 'x'])
        next(gen)
        gen.close()
        process.terminate.assert_called_once()
        process.kill.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_fully_consumed_generator_does_not_terminate_anything(self, feed, monkeypatch):
        terminated = []
        feed.set(b'a\n')
        monkeypatch.setattr(_Proc, 'terminate', lambda self: terminated.append(1))
        assert list(Command._exec_lines(['zfs', 'x'])) == ['a']
        assert terminated == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exec_out_is_unchanged(self, mock_subprocess):
        """zfsbackup calls `_exec_out` directly (plan X4): it still strips and drops blank lines."""
        mock_subprocess.setup(stdout=['  a  \n', '\n', 'b\t\n'])
        assert list(Command._exec_out(['zfs', 'x'])) == ['a', 'b']


class TestExecLinesRealChildren:
    """The same contract against real pipes (a `sys.executable` child, never `zfs`)."""

    @staticmethod
    def _emit(data):
        return _py('import sys; sys.stdout.buffer.write(%r); sys.stdout.flush()' % (data,))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_framing_bytes_survive_a_real_pipe(self, children):
        data = b'cr\rx\ntab\t\nspaces  \n\n\nlast\xff\xfe\nno-newline'
        lines, exc = _bounded(lambda: list(Command._exec_lines(self._emit(data))))
        assert exc is None
        assert lines == ['cr\rx', 'tab\t', 'spaces  ', '', '', 'last\udcff\udcfe', 'no-newline']
        assert children[0].returncode == 0

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_non_zero_exit_raises_stderr_after_all_lines(self, children):
        script = ('import sys; sys.stdout.write("a\\nb\\n"); sys.stdout.flush(); '
                  'sys.stderr.write("cannot open: gone\\nsecond\\n"); sys.exit(1)')
        seen = []

        def consume():
            for line in Command._exec_lines(_py(script)):
                seen.append(line)

        _, exc = _bounded(consume)
        assert type(exc) is Exception
        assert str(exc) == 'Cannot open: gone\nsecond'
        assert seen == ['a', 'b']
        assert children[0].returncode == 1

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_exit_after_stdout_closes_is_waited_for(self, children):
        """stdout hits EOF while the child is still alive: wait for the real status (no spin, no early success)."""
        script = ('import sys, time, os; sys.stdout.write("a\\n"); sys.stdout.flush(); os.close(1); '
                  'time.sleep(0.4); sys.stderr.write("late\\n"); sys.exit(3)')
        _, exc = _bounded(lambda: list(Command._exec_lines(_py(script))))
        assert type(exc) is Exception and str(exc) == 'Late'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_large_interleaved_output_does_not_deadlock(self, children):
        script = ('import sys\n'
                  'for i in range(300):\n'
                  '    sys.stderr.write("warn %d " % i + "e" * 300 + "\\n"); sys.stderr.flush()\n'
                  '    sys.stdout.write("row %d " % i + "o" * 300 + "\\n"); sys.stdout.flush()\n'
                  'sys.exit(5)\n')
        seen = []

        def consume():
            for line in Command._exec_lines(_py(script)):
                seen.append(line)

        _, exc = _bounded(consume)
        assert len(seen) == 300
        assert len(str(exc).split('\n')) == 300

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_abandoned_generator_reaps_a_real_child(self, children):
        script = 'import time; print("first", flush=True); time.sleep(60)'

        def run():
            gen = Command._exec_lines(_py(script))
            first = next(gen)
            gen.close()
            return first

        baseline = threading.active_count()
        first, exc = _bounded(run)
        assert exc is None and first == 'first'
        assert children[0].returncode is not None            # terminated and waited for, not left running/zombie
        assert children[0].returncode < 0
        deadline = time.monotonic() + 5
        while threading.active_count() > baseline and time.monotonic() < deadline:
            time.sleep(0.05)
        assert threading.active_count() <= baseline           # the stderr reader thread did not leak


# ---------------------------------------------------------------------------------------------------------------------
# B2: _Scope and the `list` parser
# ---------------------------------------------------------------------------------------------------------------------

#: (roots, _Scope kwargs, {name: in scope}). Deliberately a little wider than zfs, never narrower.
SCOPE_TABLE = [
    pytest.param(['p/a'], {}, {'p/a': True, 'p/a@s': True, 'p/a#b': True, 'p/a/b': False, 'p/a/b@s': False,
                               'p/b': False, 'p/ab': False, 'p': False}, id='root-no-flags-snapshots-depth-1'),
    pytest.param(['p/a'], {'recursive': True}, {'p/a': True, 'p/a@s': True, 'p/a/b': True, 'p/a/b@s': True,
                                                'p/a/b/c': True, 'p/ab': False, 'p/b': False, 'p': False},
                 id='root-recursive'),
    pytest.param(['p/a'], {'depth': 1}, {'p/a': True, 'p/a@s': True, 'p/a/b': True, 'p/a/b@s': False,
                                         'p/a/b/c': False, 'p/b': False}, id='root-depth-1'),
    pytest.param(['p/a'], {'depth': 2}, {'p/a/b': True, 'p/a/b@s': True, 'p/a/b/c': True, 'p/a/b/c@s': False},
                 id='root-depth-2'),
    pytest.param(['p'], {}, {'p': True, 'p@s': True, 'p/a': False, 'p/a@s': False}, id='pool-root-no-flags'),
    pytest.param(['p'], {'depth': 1}, {'p': True, 'p@s': True, 'p/a': True, 'p/a@s': False, 'p/a/b': False},
                 id='pool-root-depth-1'),
    pytest.param(['p/a', 'p/b'], {}, {'p/a': True, 'p/b': True, 'p/c': False, 'p/a/x': False}, id='two-roots'),
    pytest.param([], {}, {'p': True, 'p/a/b/c': True, 'p/a@s': True}, id='no-roots-no-flags'),
    pytest.param([], {'recursive': True}, {'p': True, 'p/a/b/c@x': True}, id='no-roots-recursive'),
    pytest.param([], {'depth': 1}, {'p': True, 'p@s': True, 'p/a': True, 'p/a@s': False, 'p/a/b': False},
                 id='no-roots-depth-1-counts-from-the-pool'),
    pytest.param(['p/a@s'], {}, {'p/a@s': True, 'p/a/b@s': False, 'p/a/b@t': False, 'p/a': False},
                 id='snapshot-root'),
    pytest.param(['p/a@s'], {'recursive': True}, {'p/a@s': True, 'p/a/b@s': True, 'p/a/b/c@s': True,
                                                  'p/a/b@t': False, 'p/a': False}, id='snapshot-root-recursive'),
    pytest.param(['p/a@s'], {'depth': 1}, {'p/a@s': True, 'p/a/b@s': True, 'p/a/b@t': False, 'p/a/b/c@s': False},
                 id='snapshot-root-depth-1'),
    pytest.param(['p/a#b'], {'recursive': True}, {'p/a#b': True, 'p/a/c#b': True, 'p/a/c@b': False, 'p/a#c': False},
                 id='bookmark-root-recursive'),
]


class TestScope:

    @pytest.mark.unit
    @pytest.mark.parametrize('roots,kwargs,expected', SCOPE_TABLE)
    def test_scope(self, roots, kwargs, expected):
        scope = _Scope(roots, **kwargs)
        assert {name: scope(name) for name in expected} == expected

    @pytest.mark.unit
    def test_a_sibling_sharing_a_prefix_is_not_a_descendant(self):
        scope = _Scope(['p/a'], recursive=True)
        assert not scope('p/ab') and not scope('p/ab/c') and not scope('p/a2@s')

    @pytest.mark.unit
    def test_non_positive_depth_means_no_depth(self):
        assert not _Scope(['p/a'], depth=0)('p/a/b')
        assert not _Scope(['p/a'], depth=-1)('p/a/b')

    @pytest.mark.unit
    def test_roots_may_be_objects(self):
        scope = _Scope([Snapshot(Filesystem('p/a'), 's')], recursive=True)
        assert scope('p/a/b@s') and not scope('p/a/b@t') and not scope('p/a/b')
        scope = _Scope([Filesystem('p/a'), Filesystem('p/b')], depth=1)
        assert scope('p/a/x') and scope('p/b/x') and not scope('p/c/x')

    @pytest.mark.unit
    @pytest.mark.parametrize('types,expected', [
        (None, None), ('', None), ('all', None), (['all'], None), ('filesystem,volume', {'filesystem', 'volume'}),
        (['snapshot'], {'snapshot'}), ('FILESYSTEM', {'filesystem'}), ('snapshot,all', None),
    ])
    def test_wanted_types(self, types, expected):
        assert _wanted_types(types) == expected


class TestListSkipRules:
    """Every reason `ListCommand` skips a row. Each is skipped with a WARNING, never raised."""

    GOOD = (('pool/a', 'filesystem'), ('pool/b', 'volume'), ('pool/c', 'filesystem'))

    def _list(self, feed, rows, **kw):
        feed.set(_rows(*rows))
        return ListCommand()(**kw)

    def _names(self, feed, rows, **kw):
        return [o.name for o in self._list(feed, rows, **kw)]

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_all_good_rows_pass(self, feed):
        result = self._list(feed, self.GOOD)
        assert [(type(o), o.name) for o in result] == [(Filesystem, 'pool/a'), (Volume, 'pool/b'),
                                                       (Filesystem, 'pool/c')]

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('bad', [
        pytest.param(('pool/x',), id='one-column'),
        pytest.param(('pool/x', 'filesystem', 'extra'), id='one-too-many'),
        pytest.param(('pool/x', 'filesystem', 'a', 'b', 'c'), id='three-too-many'),
        pytest.param(('pool/x', 'bogus'), id='unknown-type'),
        pytest.param(('pool/x', ''), id='empty-type'),
        pytest.param(('pool/x', 'FILESYSTEM'), id='type-case-differs'),
        pytest.param(('pool/x@s', 'filesystem'), id='snapshot-name-filesystem-type'),
        pytest.param(('pool/x', 'snapshot'), id='dataset-name-snapshot-type'),
        pytest.param(('pool/x#b', 'snapshot'), id='bookmark-name-snapshot-type'),
        pytest.param(('pool/x@s', 'bookmark'), id='snapshot-name-bookmark-type'),
        pytest.param(('pool/x', 'bookmark'), id='dataset-name-bookmark-type'),
        pytest.param(('pool/x#b', 'volume'), id='bookmark-name-volume-type'),
    ])
    def test_foreign_row_between_good_rows(self, feed, caplog, bad):
        """A row zfs's own iteration cannot print. It is skipped, and (being the tail of a possible newline-bearing
        value) so is the row right before it."""
        rows = [self.GOOD[0], bad, self.GOOD[1], self.GOOD[2]]
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            names = self._names(feed, rows)
        assert names == ['pool/b', 'pool/c']
        assert len(_warnings(caplog)) == 2

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_continuation_rule_drops_only_the_row_before(self, feed, caplog):
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            names = self._names(feed, [self.GOOD[0], self.GOOD[1], ('tail',), self.GOOD[2]])
        assert names == ['pool/a', 'pool/c']
        assert any('continues on the next line' in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_consecutive_foreign_rows_do_not_drop_further_back(self, feed):
        names = self._names(feed, [self.GOOD[0], self.GOOD[1], ('t1',), ('t2',), self.GOOD[2]])
        assert names == ['pool/a', 'pool/c']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_foreign_first_row_has_no_predecessor_to_drop(self, feed):
        assert self._names(feed, [('tail',), self.GOOD[0]]) == ['pool/a']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_requested_types_filter_the_rows(self, feed):
        fs, snap, bm, vol = ('pool/a', 'filesystem'), ('pool/a@s', 'snapshot'), ('pool/a#b', 'bookmark'), \
            ('pool/v', 'volume')
        # (each listing is fed what zfs would print for that -t: a row of any other type cannot be genuine)
        assert self._names(feed, [snap], types='snapshot') == ['pool/a@s']
        assert self._names(feed, [fs, vol], types=['filesystem', 'volume']) == ['pool/a', 'pool/v']
        assert self._names(feed, [bm], types='bookmark') == ['pool/a#b']
        assert self._names(feed, [fs, snap, bm, vol], types='filesystem,volume,snapshot,bookmark') == \
            ['pool/a', 'pool/a@s', 'pool/a#b', 'pool/v']
        assert self._names(feed, [fs, snap, bm, vol], types='all') == ['pool/a', 'pool/a@s', 'pool/a#b', 'pool/v']
        assert self._names(feed, [fs, snap, bm, vol]) == ['pool/a', 'pool/a@s', 'pool/a#b', 'pool/v']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_type_that_was_not_requested_is_skipped_with_a_warning(self, feed, caplog):
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            names = self._names(feed, [('pool/a', 'filesystem'), ('pool/a@s', 'snapshot')], types='filesystem')
        assert names == []                  # the snapshot row is foreign, so the filesystem before it goes too
        assert any('type snapshot was not requested' in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_name_outside_the_roots_is_skipped(self, feed, caplog):
        rows = [('pool/a', 'filesystem'), ('pool/a/b', 'filesystem'), ('other/x', 'filesystem')]
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            assert self._names(feed, rows, roots=Filesystem('pool/a'), recursive=True) == ['pool/a']
        assert any('outside the requested roots' in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_scope_end_to_end(self, feed):
        rows = [('pool/a', 'filesystem'), ('pool/a@s', 'snapshot'), ('pool/a/b', 'filesystem'),
                ('pool/a/b@s', 'snapshot'), ('pool/a/b/c', 'filesystem')]
        root = Filesystem('pool/a')
        assert self._names(feed, rows[:2], roots=root, types='all') == ['pool/a', 'pool/a@s']      # implicit depth 1
        assert self._names(feed, rows[:3], roots=root, depth=1) == ['pool/a', 'pool/a@s', 'pool/a/b']
        assert self._names(feed, rows, roots=root, recursive=True) == [r[0] for r in rows]
        # a snapshot root: only that snapshot (a dataset row cannot have come from `zfs list pool/a@s`)
        assert self._names(feed, [rows[0], rows[1]], roots=Snapshot(root, 's')) == ['pool/a@s']
        assert self._names(feed, [('pool/a', 'filesystem'), ('pool/b', 'filesystem')]) == ['pool/a', 'pool/b']  # no roots

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_snapshots_directly_under_a_root_are_in_scope_without_r_or_d(self, feed):
        """`zfs list -t snapshot pool/a` lists pool/a's snapshots: depth 1 is implicit for a snapshot listing."""
        rows = [('pool/a@s1', 'snapshot'), ('pool/a@s2', 'snapshot')]
        assert self._names(feed, rows, roots=Filesystem('pool/a'), types='snapshot') == ['pool/a@s1', 'pool/a@s2']
        assert feed.argv[:3] == ['list', '-H', '-t']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_empty_line_is_skipped(self, feed, caplog):
        feed.set(b'pool/a\tfilesystem\n\npool/b\tfilesystem\n')
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            names = [o.name for o in ListCommand()()]
        assert names == ['pool/b']           # the blank line is foreign: it takes the row before it with it
        assert any('empty line' in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_trailing_blank_line_is_skipped_and_drops_the_last_row(self, feed):
        feed.set(b'pool/a\tfilesystem\npool/b\tfilesystem\n\n')
        assert [o.name for o in ListCommand()()] == ['pool/a']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unrepresentable_name_is_skipped_without_dropping_its_predecessor(self, feed):
        """zfs allows the name but libzfseasy cannot represent it: nothing about the row is forged, so only it goes."""
        rows = [('pool/a', 'filesystem'), ('pool/.system', 'filesystem'), ('pool/c', 'filesystem')]
        assert self._names(feed, rows) == ['pool/a', 'pool/c']
        rows = [('pool/a', 'filesystem'), ('pool/a@ spaced', 'snapshot'), ('pool/c', 'filesystem')]
        assert self._names(feed, rows) == ['pool/a', 'pool/c']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_name_that_fails_to_parse_with_another_error_is_foreign(self, feed, monkeypatch, caplog):
        """Any other ValueError from `ZFS.from_name` is skipped too (and is not an unrepresentable-name case)."""
        real = ZFS.from_name

        def from_name(name, dstype=None, properties=None):
            if name == 'pool/b':
                raise ValueError('odd')
            return real(name, dstype, properties)

        monkeypatch.setattr(ZFS, 'from_name', staticmethod(from_name))
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            names = self._names(feed, self.GOOD)
        assert names == ['pool/c']        # b skipped, a (its predecessor) dropped as well
        assert any('cannot parse the name (odd)' in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_properties_that_cannot_be_applied_skip_the_row(self, feed, monkeypatch, caplog):
        def update(self, props, lenient=False):
            raise ValueError('nope')

        monkeypatch.setattr(Filesystem, 'update', update)
        feed.set(_rows(('pool/a', 'filesystem', 'lz4'), ('pool/b', 'volume', '-')))
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = ListCommand()(properties='compression')
        assert [o.name for o in result] == ['pool/b']
        assert any('cannot apply the properties (nope)' in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_columns_are_exactly_the_requested_ones(self, feed):
        feed.set(_rows(('pool/a', 'filesystem', 'lz4', 'on')))
        (a,) = ListCommand()(properties=['compression', 'atime'])
        assert (a['compression'].value, a['atime'].value) == ('lz4', 'on')
        feed.set(_rows(('pool/a', 'filesystem', 'lz4')))
        assert ListCommand()(properties=['compression', 'atime']) == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_empty_trailing_column_is_an_empty_value_not_a_missing_one(self, feed):
        feed.set(b'pool/a\tfilesystem\t\npool/b\tfilesystem\tlz4\n')
        a, b = ListCommand()(properties='org.t:v')
        assert a['org.t:v'].value == '' and b['org.t:v'].value == 'lz4'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_values_with_cr_trailing_spaces_and_surrogates_are_kept(self, feed):
        feed.set(_rows(('pool/a', 'filesystem', 'cr\rx'), ('pool/b', 'filesystem', 'trail  '),
                       ('pool/c', 'filesystem', '\udcff\udcfe')))
        a, b, c = ListCommand()(properties='org.t:v')
        assert (a['org.t:v'].value, b['org.t:v'].value) == ('cr\rx', 'trail  ')
        assert c['org.t:v'].value.encode('utf-8', 'surrogateescape') == b'\xff\xfe'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_dash_is_an_unset_value(self, feed):
        feed.set(_rows(('pool/a', 'filesystem', '-')))
        (a,) = ListCommand()(properties='org.t:v')
        assert a['org.t:v'] is None

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_failure_of_zfs_itself_still_raises(self, feed):
        feed.set(b'', stderr=b"cannot open 'pool/x': dataset does not exist\n", returncode=1)
        with pytest.raises(Exception, match='dataset does not exist'):
            ListCommand()(roots=Filesystem('pool/x'))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_argv_is_unchanged(self, feed):
        feed.set(_rows(('pool/a', 'filesystem', 'lz4')))
        ListCommand()(roots=Filesystem('pool/a'), recursive=True, types='filesystem', properties='compression')
        assert feed.argv == ['list', '-H', '-r', '-t', 'filesystem', '-o', 'name,type,compression', 'pool/a']


class TestListDuplicateNames:
    """zfs prints each object once: a name that comes back is a forgery, and nothing says which row is real."""

    def _rows(self):
        return [('pool/a', 'filesystem', '1'), ('pool/b', 'filesystem', '2'), ('pool/a', 'filesystem', '3'),
                ('pool/c', 'filesystem', '4'), ('pool/a', 'filesystem', '5')]

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_every_row_of_a_duplicated_name_is_dropped(self, feed, caplog):
        feed.set(_rows(*self._rows()))
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = ListCommand()(properties='org.t:v')
        assert [(o.name, o['org.t:v'].value) for o in result] == [('pool/b', '2'), ('pool/c', '4')]
        assert any('name appears 3 times' in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_both_rows_of_a_pair_go_whichever_was_the_forgery(self, feed):
        for rows in ([('pool/v', 'filesystem', 'real'), ('pool/v', 'filesystem', 'forged')],
                     [('pool/v', 'filesystem', 'forged'), ('pool/v', 'filesystem', 'real')]):
            feed.set(_rows(*rows))
            assert ListCommand()(properties='org.t:v') == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_distinct_names_are_not_duplicates(self, feed):
        feed.set(_rows(('pool/a', 'filesystem', '1'), ('pool/a@s', 'snapshot', '2'), ('pool/a#s', 'bookmark', '3')))
        assert len(ListCommand()(properties='org.t:v')) == 3

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_lazy_mode_applies_per_row_checks_only(self, feed):
        """A generator cannot take back an object it already yielded: documented limitation of lazy mode."""
        feed.set(_rows(*self._rows()))
        result = ListCommand()(properties='org.t:v', lazy=True)
        assert hasattr(result, '__next__')
        assert [o['org.t:v'].value for o in result] == ['1', '2', '3', '4', '5']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_lazy_mode_still_skips_foreign_rows(self, feed):
        feed.set(_rows(('pool/a', 'filesystem'), ('pool/b', 'filesystem'), ('tail',), ('pool/c', 'filesystem'),
                       ('pool/z', 'bogus'), ('pool/d', 'filesystem')))
        # b is the row before the one-column fragment, c the row before the bogus type
        assert [o.name for o in ListCommand()(lazy=True)] == ['pool/a', 'pool/d']


class TestListWarnings:
    """The first few skipped rows of a listing are logged one by one (`%r`, clipped, credentials redacted), then one
    summary line with the total. The count is per call, so a row that stays malformed is reported every time."""

    def _bad(self, n):
        return [('pool/bad%d' % i, 'bogus') for i in range(n)]

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('lazy', [False, True], ids=['list', 'lazy'])
    @pytest.mark.parametrize('n,summary', [(1, False), (_MAX_ROW_WARNINGS, False), (_MAX_ROW_WARNINGS + 1, True),
                                           (8, True), (40, True)])
    def test_cap_and_summary(self, feed, caplog, lazy, n, summary):
        feed.set(_rows(*self._bad(n)))
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = ListCommand()(lazy=lazy)
            assert list(result) == []
        warnings = _warnings(caplog)
        singles = [w for w in warnings if w.startswith('skipping a row of zfs list output')]
        assert len(singles) == min(n, _MAX_ROW_WARNINGS)
        totals = [w for w in warnings if w.startswith('skipped ')]
        assert len(totals) == (1 if summary else 0)
        if summary:
            assert totals[0].startswith('skipped %d malformed rows of zfs list output' % n)
        assert len(warnings) == len(singles) + len(totals)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_warnings_repeat_on_every_call(self, feed, caplog):
        feed.set(_rows(*self._bad(7)))
        counts = []
        for _ in range(3):
            caplog.clear()
            with caplog.at_level(logging.WARNING, logger='libzfseasy'):
                ListCommand()()
            counts.append(len(_warnings(caplog)))
        assert counts == [6, 6, 6]

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_row_text_is_quoted_with_repr(self, feed, caplog):
        feed.set(b'pool/a\tbogus\n')
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            ListCommand()()
        (w,) = _warnings(caplog)
        assert w.endswith(repr('pool/a\tbogus'))          # escapes visible, not raw tabs

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_long_row_is_clipped(self, feed, caplog):
        feed.set(('pool/a\tbogus\t' + 'Z' * 5000 + '\n').encode())
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            ListCommand()()
        (w,) = _warnings(caplog)
        assert len(w) < 300
        assert w.count('Z') < _MAX_QUOTED
        assert '...' in w

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_credentials_in_a_skipped_row_are_redacted(self, feed, caplog):
        feed.set(b'pool/a\tbogus\tfile+https://user:hunter2@host/key\n')
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            ListCommand()()
        text = '\n'.join(_warnings(caplog))
        assert 'hunter2' not in text and 'user:' not in text
        assert '***@host' in text

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_huge_hostile_value_cannot_flood_the_log(self, feed, caplog):
        # 8000 bytes: close to the largest user-property value zfs accepts (8 KiB)
        rows = b''.join(('pool/a%d\tfilesystem\t%s\tx\n' % (i, 'Q' * 8000)).encode() for i in range(50))
        feed.set(rows)
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            ListCommand()()
        warnings = _warnings(caplog)
        assert len(warnings) == _MAX_ROW_WARNINGS + 1
        assert sum(len(w) for w in warnings) < 3000


class TestSkipInvalidNameRegistry:
    """`_skip_invalid_name` logs a name once, and again after the registry's hourly reset."""

    @staticmethod
    def _clock(monkeypatch, start=1000.0):
        now = [start]

        class Clock:
            def __getattr__(self, name):
                return getattr(time, name)

            @staticmethod
            def monotonic():
                return now[0]

        monkeypatch.setattr(zfs_module, 'time', Clock())
        return now

    @pytest.mark.unit
    def test_ttl_is_an_hour(self):
        assert zfs_module._WARNED_NAMES_TTL == 3600.0

    @pytest.mark.unit
    def test_logged_once_then_quiet(self, caplog, monkeypatch):
        self._clock(monkeypatch)
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            for _ in range(5):
                _skip_invalid_name('pool/.system', ValueError('bad'))
        assert len(_warnings(caplog)) == 1

    @pytest.mark.unit
    def test_logged_again_after_the_ttl(self, caplog, monkeypatch):
        now = self._clock(monkeypatch)
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            _skip_invalid_name('pool/.system', ValueError('bad'))
            now[0] += zfs_module._WARNED_NAMES_TTL - 1
            _skip_invalid_name('pool/.system', ValueError('bad'))
            assert len(_warnings(caplog)) == 1                      # not yet
            now[0] += 1
            _skip_invalid_name('pool/.system', ValueError('bad'))
            assert len(_warnings(caplog)) == 2                      # the registry was emptied
            _skip_invalid_name('pool/.system', ValueError('bad'))
            assert len(_warnings(caplog)) == 2                      # and it is quiet again for another hour
            now[0] += zfs_module._WARNED_NAMES_TTL
            _skip_invalid_name('pool/.system', ValueError('bad'))
            assert len(_warnings(caplog)) == 3

    @pytest.mark.unit
    def test_reset_covers_every_name(self, caplog, monkeypatch):
        now = self._clock(monkeypatch)
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            for n in ('pool/.a', 'pool/.b', 'pool/.c'):
                _skip_invalid_name(n, ValueError('bad'))
            now[0] += zfs_module._WARNED_NAMES_TTL
            for n in ('pool/.a', 'pool/.b', 'pool/.c'):
                _skip_invalid_name(n, ValueError('bad'))
        assert len(_warnings(caplog)) == 6

    @pytest.mark.unit
    def test_the_name_is_logged_with_repr_and_clipped(self, caplog, monkeypatch):
        self._clock(monkeypatch)
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            _skip_invalid_name('pool/a\nb', ValueError('bad'))
            _skip_invalid_name('pool/' + 'n' * 1000, ValueError('bad'))
        first, second = _warnings(caplog)
        assert repr('pool/a\nb') in first and '\n' not in first
        assert len(second) < 400 and '...' in second


# ---------------------------------------------------------------------------------------------------------------------
# B2: the `get` parser
# ---------------------------------------------------------------------------------------------------------------------

def _row(name, prop, value, received='-', source='local'):
    return (name, prop, value, received, source)


class TestGetTabRecovery:
    """A value containing tabs shifts the `value,received,source` columns. It is recovered only when exactly one split
    is plausible: received is `-` on a non-received source, or received equals the value."""

    def _get(self, feed, rows, **kw):
        feed.set(_rows(*(_get_rows('pool/a', explicit=True) + rows)))
        return GetCommand()(Filesystem('pool/a'), properties='org.t:v', **kw)

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('value,received,source,expected', [
        pytest.param('a\tb', '-', 'local', ('a\tb', '-'), id='local-tab-value'),
        pytest.param('a\t\tb', '-', 'local', ('a\t\tb', '-'), id='empty-field-inside'),
        pytest.param('a\tb\tc', '-', 'inherited from pool', ('a\tb\tc', '-'), id='inherited-tab-value'),
        pytest.param('\t', '-', 'local', ('\t', '-'), id='only-a-tab'),
        pytest.param('r1\tr2', 'r1\tr2', 'received', ('r1\tr2', 'r1\tr2'), id='received-equal-halves'),
        pytest.param('x\ty\tz', 'x\ty\tz', 'received', ('x\ty\tz', 'x\ty\tz'), id='received-three-fields'),
        pytest.param('a\tb', 'a\tb', 'local', ('a\tb', 'a\tb'), id='local-equal-to-received'),
    ])
    def test_unambiguous(self, feed, value, received, source, expected):
        (obj,) = self._get(feed, [_row('pool/a', 'org.t:v', value, received, source)])
        prop = obj['org.t:v']
        assert (prop.value, prop.received) == expected
        assert prop.source == source

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('value,received,source', [
        pytest.param('l1\tl2', 'r1\tr2', 'local', id='local-over-received'),
        pytest.param('v', 'r1\tr2', 'local', id='plain-value-tab-received'),
        pytest.param('a\tb', '-', 'received', id='received-source-dash'),
        pytest.param('a\tb', 'c', 'received', id='received-source-unequal'),
        pytest.param('x\ty\tz', 'r1', 'inherited from pool', id='tab-value-with-a-received-value'),
        pytest.param('a\t-', 'a\t-', 'local', id='two-splits-both-plausible'),
    ])
    def test_ambiguous_is_skipped(self, feed, caplog, value, received, source):
        # sanity: the "two splits" case has TWO plausible splits ('a\t-\ta' | '-' and 'a\t-' | 'a\t-')
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            (obj,) = self._get(feed, [_row('pool/a', 'org.t:v', value, received, source)])
        assert obj.name == 'pool/a'
        assert obj['org.t:v'] is None
        assert any('cannot tell the value from the received column' in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_recovery_never_crosses_into_the_next_row(self, feed):
        """The row is recovered from its own columns only: the neighbours keep their own values."""
        rows = [_row('pool/a', 'compression', 'lz4'), _row('pool/a', 'org.t:v', 'a\tb'), _row('pool/a', 'atime', 'off')]
        (obj,) = self._get(feed, rows)
        assert (obj['compression'].value, obj['org.t:v'].value, obj['atime'].value) == ('lz4', 'a\tb', 'off')

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_fewer_than_five_columns_is_skipped(self, feed, caplog):
        rows = [('pool/a', 'org.t:v', 'v', '-'), ('pool/a', 'org.t:w'), ('pool/a',), ('stray',),
                _row('pool/a', 'compression', 'lz4')]
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            (obj,) = self._get(feed, rows)
        assert obj['org.t:v'] is None and obj['compression'].value == 'lz4'
        assert sum('expected 5 columns' in w for w in _warnings(caplog)) == 4

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_empty_value_and_trailing_space_and_cr_survive(self, feed):
        rows = [_row('pool/a', 'org.t:v', ''), _row('pool/a', 'org.t:w', 'trail  '), _row('pool/a', 'org.t:x', 'cr\rx')]
        feed.set(_rows(*(_get_rows('pool/a', explicit=True) + rows)))
        (obj,) = GetCommand()(Filesystem('pool/a'), properties=['org.t:v', 'org.t:w', 'org.t:x'])
        assert (obj['org.t:v'].value, obj['org.t:w'].value, obj['org.t:x'].value) == ('', 'trail  ', 'cr\rx')


class TestGetObjectRules:
    """What drops a property row, or a whole object, from a `get` result."""

    def _get(self, feed, rows, ds=None, **kw):
        feed.set(_rows(*rows))
        return GetCommand()(ds, properties=kw.pop('properties', 'all'), **kw)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_invalid_property_name_skips_just_that_row(self, feed, caplog):
        rows = _get_rows('pool/a', compression='lz4') + [_row('pool/a', 'Bad-Prop', 'v'), _row('pool/a', 'a b', 'v')] + \
            [_row('pool/a', 'atime', 'off')]
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            (obj,) = self._get(feed, rows)
        assert (obj['compression'].value, obj['atime'].value) == ('lz4', 'off')
        assert sum('invalid property name' in w for w in _warnings(caplog)) == 2

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_user_and_unknown_native_properties_are_kept(self, feed):
        rows = _get_rows('pool/a') + [_row('pool/a', 'org.ex:note', 'hello'), _row('pool/a', 'futureprop', 'x')]
        (obj,) = self._get(feed, rows)
        assert obj['org.ex:note'].value == 'hello' and obj['futureprop'].value == 'x'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_the_same_property_twice_drops_the_object(self, feed, caplog):
        rows = _get_rows('pool/a', compression='lz4') + [_row('pool/a', 'compression', 'off')] + \
            _get_rows('pool/b', compression='gzip')
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = self._get(feed, rows)
        assert [o.name for o in result] == ['pool/b']
        assert any("property 'compression' appears twice for 'pool/a'" in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_type_row_twice_drops_the_object(self, feed):
        rows = _get_rows('pool/a') + [_row('pool/a', 'type', 'snapshot', source='-')] + _get_rows('pool/b')
        assert [o.name for o in self._get(feed, rows)] == ['pool/b']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_no_type_row_drops_the_object(self, feed, caplog):
        """`all`: an object whose first row is not `type` (here `compression`) never has a type row where zfs prints it."""
        rows = [_row('pool/a', 'compression', 'lz4')] + _get_rows('pool/b', compression='gzip')
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = self._get(feed, rows)
        assert [o.name for o in result] == ['pool/b']
        assert result[0]['compression'].value == 'gzip'
        assert any("rows of 'pool/a' do not start with the 'type' row" in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_no_type_row_drops_the_object_in_an_explicit_list(self, feed, caplog):
        """An explicit list: a group that is only its `name` row (the `type` row never came) has no type."""
        rows = [_row('pool/a', 'name', 'pool/a', '-', '-')] + _get_rows('pool/b', explicit=True, compression='gzip')
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = self._get(feed, rows, properties='compression')
        assert [o.name for o in result] == ['pool/b']
        assert result[0]['compression'].value == 'gzip'
        assert any("no type row for 'pool/a'" in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('name,dstype,why', [
        ('pool/a', 'bogus', "unknown type 'bogus'"),
        ('pool/a', 'snapshot', 'name does not match type snapshot'),
        ('pool/a@s', 'filesystem', 'name does not match type filesystem'),
        ('pool/a#b', 'snapshot', 'name does not match type snapshot'),
        ('pool/a', 'bookmark', 'name does not match type bookmark'),
    ])
    def test_a_type_problem_drops_the_object(self, feed, caplog, name, dstype, why):
        rows = _get_rows(name, dstype, compression='lz4') + _get_rows('pool/ok', compression='gzip')
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = self._get(feed, rows)
        assert [o.name for o in result] == ['pool/ok']
        assert any(why in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_type_that_was_not_requested_drops_the_object(self, feed):
        rows = _get_rows('pool/a') + _get_rows('pool/a@s', 'snapshot')
        assert [o.name for o in self._get(feed, rows, types='filesystem')] == ['pool/a']
        assert [o.name for o in self._get(feed, rows, types='snapshot')] == ['pool/a@s']
        assert [o.name for o in self._get(feed, rows, types='filesystem,snapshot')] == ['pool/a', 'pool/a@s']
        assert [o.name for o in self._get(feed, rows, types='all')] == ['pool/a', 'pool/a@s']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_build_error_drops_the_object(self, feed, monkeypatch, caplog):
        real = GetCommand._build

        def build(name, dstype, properties):
            if name == 'pool/a':
                raise ValueError('cannot build')
            return real(name, dstype, properties)

        monkeypatch.setattr(GetCommand, '_build', staticmethod(build))
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = self._get(feed, _get_rows('pool/a') + _get_rows('pool/b'))
        assert [o.name for o in result] == ['pool/b']
        assert any("cannot build 'pool/a' (cannot build)" in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_unrepresentable_name_is_still_skipped_quietly(self, feed):
        rows = _get_rows('pool/a') + _get_rows('pool/.system', compression='off') + _get_rows('pool/c')
        assert [o.name for o in self._get(feed, rows)] == ['pool/a', 'pool/c']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_source_filtering_keeps_type(self, feed):
        rows = _get_rows('pool/a') + [_row('pool/a', 'compression', 'lz4', source='default'),
                                      _row('pool/a', 'atime', 'off', source='local')]
        (obj,) = self._get(feed, rows, sources='local')
        assert obj['atime'].value == 'off' and obj['compression'] is None


class TestGetContiguity:
    """zfs prints all of an object's rows together; a name that comes back was forged."""

    def _rows(self):
        return _get_rows('pool/a', compression='lz4') + _get_rows('pool/b', compression='gzip') + \
            [_row('pool/a', 'atime', 'off')] + _get_rows('pool/c', compression='zstd')

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_non_contiguous_name_drops_both_in_list_mode(self, feed, caplog):
        feed.set(_rows(*self._rows()))
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = GetCommand()(properties='all')
        assert [o.name for o in result] == ['pool/b', 'pool/c']
        assert any("rows of 'pool/a' are not contiguous" in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_lazy_mode_drops_the_returning_rows_but_cannot_take_back_the_first(self, feed):
        feed.set(_rows(*self._rows()))
        result = GetCommand()(properties='all', lazy=True)
        assert hasattr(result, '__next__')
        names = [o.name for o in result]
        assert names == ['pool/a', 'pool/b', 'pool/c']
        assert names.count('pool/a') == 1                   # the second appearance is never yielded
        feed.set(_rows(*self._rows()))
        a = next(iter(GetCommand()(properties='all', lazy=True)))
        assert a['atime'] is None                           # and its rows are not merged into the first

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_three_appearances_still_drop_it(self, feed):
        rows = _get_rows('pool/a') + _get_rows('pool/b') + _get_rows('pool/a') + _get_rows('pool/c') + \
            _get_rows('pool/a')
        feed.set(_rows(*rows))
        assert [o.name for o in GetCommand()(properties='all')] == ['pool/b', 'pool/c']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_properties_of_a_forged_return_do_not_reach_neighbours(self, feed):
        rows = _get_rows('pool/a') + _get_rows('pool/b', compression='gzip') + \
            [_row('pool/a', 'compression', 'FORGED')] + [_row('pool/b', 'atime', 'off')]
        feed.set(_rows(*rows))
        names = {o.name: o for o in GetCommand()(properties='all')}
        assert 'pool/a' not in names
        assert 'pool/b' not in names                        # b comes back after the forged a row too
        assert names == {}


class TestGetLeadRule:
    """Every object's group of `zfs get -H` rows starts with the rows zfs prints first: `name`, `type` for an explicit
    property list (libzfseasy sends them first), `type` alone for `all` (no `name` row). A group that starts anything
    else is dropped whole.

    The attack it closes: object A's last value holds `...\\n<name of B>\\t<property>\\t<forged value>`. zfs prints that
    as a row for B directly before B's REAL group, so without the rule the forged property merges into B's group (the
    name is contiguous, and the real group does not repeat a property it was not asked for). The forged row cannot
    also be a lead row without repeating the real group's own lead rows ("appears twice")."""

    PROP = 'org.t:v'
    FORGED = 'org.zfsbackup:config'

    @staticmethod
    def _text(*rows):
        """What `zfs get -H` prints for these `(name, property, value, received, source)` rows: tab-joined, one per
        line, and a newline inside a value is simply more lines."""
        return ('\n'.join('\t'.join(r) for r in rows) + '\n').encode('utf-8', 'surrogateescape')

    @staticmethod
    def _object(name, explicit, dstype='filesystem', **props):
        return _get_rows(name, dstype, explicit=explicit, **props)

    def _stream(self, explicit, hostile_value, a_prop=None, b_real_prop=None):
        """A (with the hostile value as its last property) then the real group of B, as one zfs run prints them."""
        a_prop = a_prop or self.PROP
        a = self._object('bk/A/x', explicit)
        a.append(('bk/A/x', a_prop, hostile_value, '-', 'local'))
        b = self._object('bk/B/y', explicit)
        if b_real_prop:
            b.append(('bk/B/y', b_real_prop, 'real', '-', 'local'))
        return self._text(*(a + b))

    def _get(self, feed, data, explicit, lazy=False, **kw):
        feed.set(data)
        properties = [self.PROP] if explicit else 'all'
        result = GetCommand()(Filesystem('bk'), properties=properties, recursive=True, lazy=lazy, **kw)
        return list(result)

    # --- a forged row right before the next real group -------------------------------------------------------------

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('explicit', [True, False], ids=['explicit-list', 'all'])
    @pytest.mark.parametrize('lazy', [False, True], ids=['list', 'lazy'])
    def test_a_forged_row_before_the_next_real_group_drops_that_object(self, feed, caplog, explicit, lazy):
        hostile = 'v\t-\tlocal\nbk/B/y\t%s\tFORGEDCFG' % self.FORGED
        data = self._stream(explicit, hostile, b_real_prop=self.PROP)
        # sanity: this is the shape the attack gives zfs's output (the forged row carries the real row's tail)
        assert data.decode().split('\n')[-5 if explicit else -4] == 'bk/B/y\t%s\tFORGEDCFG\t-\tlocal' % self.FORGED
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = self._get(feed, data, explicit, lazy=lazy)
        assert [o.name for o in result] == ['bk/A/x']
        (a,) = result
        assert a[self.PROP].value == 'v'
        assert a[self.FORGED] is None                                  # the forged property is on neither object
        assert not any('FORGEDCFG' in str(o.properties) for o in result)
        lead = 'name' if explicit else 'type'
        assert any("rows of 'bk/B/y' do not start with the %r row zfs prints first (got %r)" % (lead, self.FORGED) in w
                   for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('explicit', [True, False], ids=['explicit-list', 'all'])
    def test_the_forged_property_is_not_merged_when_the_real_group_does_not_repeat_it(self, feed, explicit):
        """The point of the lead rule: B's real group holds no `org.zfsbackup:config` row (that is the case whenever the
        forged property is not one that was asked for / is unset), so nothing else would reveal the merge."""
        hostile = 'v\t-\tlocal\nbk/B/y\t%s\tFORGEDCFG' % self.FORGED
        data = self._stream(explicit, hostile)
        assert [o.name for o in self._get(feed, data, explicit)] == ['bk/A/x']

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('explicit', [True, False], ids=['explicit-list', 'all'])
    def test_the_object_after_the_dropped_one_is_unharmed(self, feed, explicit):
        hostile = 'v\t-\tlocal\nbk/B/y\t%s\tFORGEDCFG' % self.FORGED
        a = self._object('bk/A/x', explicit) + [('bk/A/x', self.PROP, hostile, '-', 'local')]
        b = self._object('bk/B/y', explicit)
        c = self._object('bk/C/z', explicit) + [('bk/C/z', self.PROP, 'cval', '-', 'local')]
        result = self._get(feed, self._text(*(a + b + c)), explicit)
        assert [o.name for o in result] == ['bk/A/x', 'bk/C/z']
        assert result[1][self.PROP].value == 'cval' and result[1][self.FORGED] is None

    # --- a forged prefix that mimics the lead rows --------------------------------------------------------------------

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('explicit', [True, False], ids=['explicit-list', 'all'])
    @pytest.mark.parametrize('lazy', [False, True], ids=['list', 'lazy'])
    def test_a_forged_prefix_that_mimics_the_lead_rows_is_dropped_as_a_repeat(self, feed, caplog, explicit, lazy):
        forged = ['bk/B/y\tname\tbk/B/y\t-\t-'] if explicit else []
        forged += ['bk/B/y\ttype\tfilesystem\t-\t-', 'bk/B/y\t%s\tFORGEDCFG' % self.FORGED]
        hostile = 'v\t-\tlocal\n' + '\n'.join(forged)
        data = self._stream(explicit, hostile)
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = self._get(feed, data, explicit, lazy=lazy)
        assert [o.name for o in result] == ['bk/A/x']
        assert result[0][self.FORGED] is None
        repeated = 'name' if explicit else 'type'                    # the real group repeats its own first row
        assert any("property %r appears twice for 'bk/B/y'" % repeated in w for w in _warnings(caplog))

    # --- the received-source variant -------------------------------------------------------------------------------

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('explicit', [True, False], ids=['explicit-list', 'all'])
    @pytest.mark.parametrize('lazy', [False, True], ids=['list', 'lazy'])
    def test_a_received_property_forges_exactly_one_row_and_the_object_is_still_dropped(self, feed, caplog, explicit,
                                                                                         lazy):
        """A received property prints its value twice (`value`, `received`) and zfs shows `received` as the source, so a
        newline in it forges one 6-column row (recovered by the tab rule) and one 4-column row (skipped)."""
        value = 'x\t-\tlocal\nbk/B/y\t%s\tFORGED' % self.FORGED
        a = self._object('bk/A/x', explicit) + [('bk/A/x', self.PROP, value, value, 'received')]
        b = self._object('bk/B/y', explicit)
        data = self._text(*(a + b))
        lines = data.decode().split('\n')
        forged = [l for l in lines if l.startswith('bk/B/y\t%s' % self.FORGED)]
        assert len(forged) == 2
        # exactly one of them is a row the parser accepts, and it carries the (tab-recovered) forged value
        skips = _SkipCounter()
        rows = [GetCommand._split_row(l, skips) for l in forged]
        assert [r is not None for r in rows] == [True, False]
        assert rows[0] == ('bk/B/y', self.FORGED, 'FORGED\tx', '-', 'local')
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = self._get(feed, data, explicit, lazy=lazy)
        assert [o.name for o in result] == ['bk/A/x']
        assert result[0][self.FORGED] is None
        warnings = _warnings(caplog)
        assert any("rows of 'bk/B/y' do not start with" in w for w in warnings)
        assert sum('expected 5 columns' in w for w in warnings) == 1

    # --- the lead rule did not break legitimate output ---------------------------------------------------------------

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_explicit_list_output_parses(self, feed):
        rows = (self._object('bk/A/x', True, **{self.PROP: 'va'}) + self._object('bk/B/y', True, **{self.PROP: 'vb'}))
        result = self._get(feed, self._text(*rows), True)
        assert [o.name for o in result] == ['bk/A/x', 'bk/B/y']
        assert all(isinstance(o, Filesystem) for o in result)
        assert [o[self.PROP].value for o in result] == ['va', 'vb']
        assert feed.argv == ['get', '-H', '-o', 'name,property,value,received,source', '-r', '-t', 'all',
                             'name,type,%s' % self.PROP, 'bk']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_all_output_parses_and_has_no_name_row(self, feed):
        rows = self._object('bk/A/x', False, compression='lz4') + self._object('bk/B/y', False, 'volume',
                                                                                  compression='off')
        data = self._text(*rows)
        assert b'\tname\t' not in data
        result = self._get(feed, data, False)
        assert [(type(o), o.name) for o in result] == [(Filesystem, 'bk/A/x'), (Volume, 'bk/B/y')]
        assert [o['compression'].value for o in result] == ['lz4', 'off']
        assert feed.argv == ['get', '-H', '-o', 'name,property,value,received,source', '-r', '-t', 'all', 'all', 'bk']

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('lazy', [False, True], ids=['list', 'lazy'])
    def test_all_recursive_with_snapshots_and_bookmarks_parses(self, feed, lazy):
        rows = []
        for name, dstype in (('bk', 'filesystem'), ('bk@s', 'snapshot'), ('bk#b', 'bookmark'),
                             ('bk/c', 'filesystem'), ('bk/c@s', 'snapshot'), ('bk/v', 'volume')):
            rows += self._object(name, False, dstype, creation='x')
        feed.set(self._text(*rows))
        result = list(GetCommand()(Filesystem('bk'), properties='all', recursive=True, types='all', lazy=lazy))
        assert [(type(o), o.name) for o in result] == [
            (Filesystem, 'bk'), (Snapshot, 'bk@s'), (Bookmark, 'bk#b'), (Filesystem, 'bk/c'), (Snapshot, 'bk/c@s'),
            (Volume, 'bk/v')]
        assert feed.argv == ['get', '-H', '-o', 'name,property,value,received,source', '-r', '-t', 'all', 'all', 'bk']

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('types,expected', [
        ('snapshot', ['bk@s', 'bk/c@s']), ('bookmark', ['bk#b']), (['filesystem', 'volume'], ['bk', 'bk/c', 'bk/v']),
    ])
    @pytest.mark.parametrize('explicit', [True, False], ids=['explicit-list', 'all'])
    def test_types_selects_what_the_output_may_hold(self, feed, types, expected, explicit):
        rows = []
        for name, dstype in (('bk', 'filesystem'), ('bk@s', 'snapshot'), ('bk#b', 'bookmark'),
                             ('bk/c', 'filesystem'), ('bk/c@s', 'snapshot'), ('bk/v', 'volume')):
            rows += self._object(name, explicit, dstype)
        feed.set(self._text(*rows))
        result = GetCommand()(Filesystem('bk'), properties=[self.PROP] if explicit else 'all', recursive=True,
                              types=types)
        assert sorted(o.name for o in result) == sorted(n for n in expected)
        assert '-t' in feed.argv

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('sources', [None, 'all', 'local', 'default', 'inherited', 'temporary', 'received',
                                         ['local', 'inherited'], ['default', 'temporary', 'received']])
    @pytest.mark.parametrize('explicit', [True, False], ids=['explicit-list', 'all'])
    def test_every_sources_value_keeps_every_object_and_never_passes_dash_s(self, feed, sources, explicit):
        """Sources are filtered client-side: `-s` would remove the `type` row from the output and with it the lead."""
        props = {self.PROP: 'v'}
        rows = []
        for name, dstype in (('bk', 'filesystem'), ('bk@s', 'snapshot'), ('bk/v', 'volume')):
            rows += self._object(name, explicit, dstype, **props)
        feed.set(self._text(*rows))
        kwargs = {} if sources is None else {'sources': sources}
        result = GetCommand()(Filesystem('bk'), properties=[self.PROP] if explicit else 'all', recursive=True,
                              types='all', **kwargs)
        assert [o.name for o in result] == ['bk', 'bk@s', 'bk/v']
        assert [type(o) for o in result] == [Filesystem, Snapshot, Volume]
        assert '-s' not in feed.argv

    # --- a real lead row that was skipped earlier ------------------------------------------------------------------------

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('explicit', [True, False], ids=['explicit-list', 'all'])
    def test_a_lead_row_skipped_as_ambiguous_makes_the_group_bad(self, feed, caplog, explicit):
        """The first row of B's group is skipped (its tab-split is ambiguous); the next row would then be taken for the
        first one, so the group has to be treated as not starting with the lead rows."""
        lead_prop = 'name' if explicit else 'type'
        ambiguous = 'bk/B/y\t%s\ta\t-\ta\t-\t-' % lead_prop            # two plausible value/received splits
        assert GetCommand._split_row(ambiguous, _SkipCounter()) is None
        a = self._object('bk/A/x', explicit, **{self.PROP: 'va'})
        b = self._object('bk/B/y', explicit, **{self.PROP: 'vb'})
        b[0] = ambiguous
        c = self._object('bk/C/z', explicit, **{self.PROP: 'vc'})
        lines = [self._text(r).decode().rstrip('\n') for r in a] + b[:1] + \
            [self._text(r).decode().rstrip('\n') for r in b[1:] + c]
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = self._get(feed, ('\n'.join(lines) + '\n').encode(), explicit)
        assert [o.name for o in result] == ['bk/A/x', 'bk/C/z']
        warnings = _warnings(caplog)
        assert any('cannot tell the value from the received column' in w for w in warnings)
        assert any("rows of 'bk/B/y' do not start with" in w for w in warnings)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_every_lead_row_is_checked_not_just_the_first(self, feed, caplog):
        """Explicit list: `name` right, then something other than `type`, is not what zfs prints."""
        a = self._object('bk/A/x', True, **{self.PROP: 'va'})
        b = [('bk/B/y', 'name', 'bk/B/y', '-', '-'), ('bk/B/y', self.PROP, 'vb', '-', 'local'),
             ('bk/B/y', 'type', 'filesystem', '-', '-')]
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = self._get(feed, self._text(*(a + b)), True)
        assert [o.name for o in result] == ['bk/A/x']
        assert any("rows of 'bk/B/y' do not start with the 'type' row zfs prints first (got %r)" % self.PROP in w
                   for w in _warnings(caplog))

    # --- `_get_options` de-duplication -----------------------------------------------------------------------------------

    @pytest.mark.unit
    @pytest.mark.parametrize('properties,expected', [
        (['type'], ['name,type']),
        (['name'], ['name,type']),
        (['type', 'name'], ['name,type']),
        (['type', 'used', 'name', 'used'], ['name,type,used']),
        (['used', 'used', 'used'], ['name,type,used']),
        (['compression', 'type', 'atime', 'compression'], ['name,type,compression,atime']),
        ([], ['name,type']),
        (['all'], ['all']),
        (['type', 'all'], ['all']),
        (['used', 'all', 'used'], ['all']),
    ])
    def test_get_options_removes_duplicate_properties(self, properties, expected):
        assert GetCommand._get_options(properties=properties) == expected

    @pytest.mark.unit
    def test_get_options_keeps_the_other_options_around_the_property_list(self):
        argv = GetCommand._get_options(properties=['type', 'used'], recursive=True, depth=2, types='all',
                                       datasets=['bk/a', 'bk/b'])
        assert argv == ['-r', '-d', '2', '-t', 'all', 'name,type,used', 'bk/a', 'bk/b']

    @pytest.mark.unit
    @pytest.mark.parametrize('properties,lead', [
        (['used'], ('name', 'type')), (['type'], ('name', 'type')), ([], ('name', 'type')),
        (['all'], ('type',)), (['used', 'all'], ('type',)),
    ])
    def test_lead_properties(self, properties, lead):
        assert GetCommand._lead_properties(properties) == lead

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_end_to_end_with_type_as_the_only_property(self, feed):
        feed.set(self._text(*(self._object('bk/A/x', True) + self._object('bk/B/y', True, 'volume'))))
        result = zfs.get(Filesystem('bk'), properties=['type'], recursive=True)
        assert feed.argv == ['get', '-H', '-o', 'name,property,value,received,source', '-r', '-t', 'all', 'name,type', 'bk']
        assert [(type(o), o.name) for o in result] == [(Filesystem, 'bk/A/x'), (Volume, 'bk/B/y')]

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_end_to_end_with_duplicates_in_the_property_list(self, feed):
        feed.set(self._text(*(self._object('bk/A/x', True, **{self.PROP: 'v'}))))
        (obj,) = GetCommand()(Filesystem('bk'), properties=[self.PROP, 'type', self.PROP], recursive=True)
        assert feed.argv[-2:] == ['name,type,%s' % self.PROP, 'bk']             # sent once each, `type` not twice
        assert obj[self.PROP].value == 'v'


class _SkipCounter:
    """A `_SkippedRows` stand-in for calling `GetCommand._split_row` directly."""

    def __init__(self):
        self.count = 0

    def skip(self, reason, row=None):
        self.count += 1


class TestGetScope:

    def _rows(self):
        return _get_rows('pool/a') + _get_rows('pool/a@s', 'snapshot') + _get_rows('pool/a/b') + \
            _get_rows('pool/a/b@s', 'snapshot') + _get_rows('pool/a/b/c') + _get_rows('other/x')

    def _names(self, feed, rows, ds, **kw):
        feed.set(_rows(*rows))
        return [o.name for o in GetCommand()(ds, properties='all', **kw)]

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_name_outside_the_requested_object_is_skipped(self, feed, caplog):
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            names = self._names(feed, self._rows(), Filesystem('pool/a'), recursive=True)
        assert names == ['pool/a', 'pool/a@s', 'pool/a/b', 'pool/a/b@s', 'pool/a/b/c']
        assert any('name outside the requested objects' in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_non_recursive_means_the_object_and_its_snapshots(self, feed):
        assert self._names(feed, self._rows(), Filesystem('pool/a')) == ['pool/a', 'pool/a@s']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_depth(self, feed):
        assert self._names(feed, self._rows(), Filesystem('pool/a'), depth=1) == ['pool/a', 'pool/a@s', 'pool/a/b']
        assert self._names(feed, self._rows(), Filesystem('pool/a'), depth=2) == \
            ['pool/a', 'pool/a@s', 'pool/a/b', 'pool/a/b@s', 'pool/a/b/c']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_no_object_means_everything(self, feed):
        assert len(self._names(feed, self._rows(), None)) == 6

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_several_objects(self, feed):
        rows = _get_rows('pool/a') + _get_rows('pool/b') + _get_rows('pool/c')
        assert self._names(feed, rows, [Filesystem('pool/a'), Filesystem('pool/c')]) == ['pool/a', 'pool/c']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_forged_group_naming_an_unrelated_dataset_is_skipped(self, feed):
        """The attack shape: a forged row naming a dataset the command was never asked about."""
        rows = _get_rows('pool/a') + [_row('pool/victim', 'org.t:v', 'FORGED')] + _get_rows('pool/a/b')
        assert self._names(feed, rows, Filesystem('pool/a'), recursive=True) == ['pool/a', 'pool/a/b']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_last_object_keeps_its_own_name_when_trailing_rows_are_skipped(self, feed):
        """S5: the final object is built under the name of its own rows, not whatever name the loop saw last."""
        rows = _get_rows('pool/a', compression='lz4') + [_row('pool/zzz', 'type', 'filesystem', '-', '-'),
                                                         ('stray', 'short')]
        feed.set(_rows(*rows))
        result = GetCommand()(Filesystem('pool/a'), properties='all')
        assert [o.name for o in result] == ['pool/a']
        assert result[0]['compression'].value == 'lz4'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_name_property_row_does_not_rename_the_object(self, feed):
        """A `name` row whose value differs from the first column (a forged one) never renames the object."""
        rows = _get_rows('pool/a', compression='lz4') + [_row('pool/a', 'name', 'pool/victim')]
        feed.set(_rows(*rows))
        (obj,) = GetCommand()(Filesystem('pool/a'), properties='all')
        assert obj.name == 'pool/a'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_an_object_after_a_skipped_one_is_not_lost(self, feed):
        rows = [_row('pool/a', 'compression', 'lz4')] + _get_rows('pool/b', compression='gzip')
        assert self._names(feed, rows, None) == ['pool/b']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_argv_is_unchanged(self, feed):
        feed.set(b'')
        GetCommand()(Filesystem('pool/a'), properties='compression', recursive=True, types='filesystem')
        assert feed.argv == ['get', '-H', '-o', 'name,property,value,received,source', '-r', '-t', 'filesystem',
                             'name,type,compression', 'pool/a']


class TestGetWarnings:

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('lazy', [False, True], ids=['list', 'lazy'])
    def test_cap_summary_and_repeat(self, feed, caplog, lazy):
        bad = [('pool/a', 'p%d' % i, 'v') for i in range(9)]          # 9 rows with too few columns
        feed.set(_rows(*(_get_rows('pool/a') + bad)))
        for _ in range(2):
            caplog.clear()
            with caplog.at_level(logging.WARNING, logger='libzfseasy'):
                list(GetCommand()(properties='all', lazy=lazy))
            warnings = _warnings(caplog)
            assert len([w for w in warnings if w.startswith('skipping a row of zfs get output')]) == _MAX_ROW_WARNINGS
            assert [w for w in warnings if w.startswith('skipped 9 malformed rows of zfs get output')]
            assert len(warnings) == _MAX_ROW_WARNINGS + 1

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_rows_are_clipped_and_redacted(self, feed, caplog):
        feed.set(('pool/a\torg.t:v\tfile+https://user:hunter2@host/' + 'Z' * 5000 + '\n').encode())
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            GetCommand()(properties='all')
        text = '\n'.join(_warnings(caplog))
        assert 'hunter2' not in text and len(text) < 400


# ---------------------------------------------------------------------------------------------------------------------
# B3: zfs.get.value()
# ---------------------------------------------------------------------------------------------------------------------

class TestGetValue:

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_argv_without_sources(self, feed):
        feed.set(b'lz4\n')
        assert zfs.get.value(Filesystem('pool/a'), 'compression') == 'lz4'
        assert feed.argv == ['get', '-H', '-o', 'value', 'compression', 'pool/a']

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('sources,flag', [
        ('local', ['-s', 'local']),
        (['local', 'received'], ['-s', 'local,received']),
        ('local,received', ['-s', 'local,received']),
        ('inherited', ['-s', 'inherited']),
        (['temporary'], ['-s', 'temporary']),
        ('all', []),
        (['ALL'], []),
        (['local', 'all'], []),
        (None, []),
        ([], []),
        ('', []),
    ])
    def test_argv_with_sources(self, feed, sources, flag):
        feed.set(b'x\n')
        zfs.get.value(Filesystem('pool/a'), 'org.t:v', sources=sources)
        assert feed.argv == ['get', '-H', '-o', 'value'] + flag + ['org.t:v', 'pool/a']

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('ds', [
        pytest.param(Filesystem('pool/a'), id='Filesystem'),
        pytest.param(Dataset('pool/a'), id='Dataset'),
        pytest.param('pool/a', id='name'),
    ])
    def test_object_or_name(self, feed, ds):
        feed.set(b'v\n')
        assert zfs.get.value(ds, 'org.t:v') == 'v'
        assert feed.argv[-1] == 'pool/a'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_snapshot_bookmark_volume_and_names_of_them(self, feed):
        feed.set(b'v\n')
        fs = Filesystem('pool/a')
        for ds, name in [(Snapshot(fs, 's'), 'pool/a@s'), (Bookmark(fs, 'b'), 'pool/a#b'), (Volume('pool/v'), 'pool/v'),
                         ('pool/a@s', 'pool/a@s'), ('pool/a#b', 'pool/a#b')]:
            assert zfs.get.value(ds, 'org.t:v') == 'v'
            assert feed.argv[-1] == name

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_all_of_stdout_is_the_value(self, feed):
        for out, want in [(b'victimcfg\n', 'victimcfg'), (b'line1\nline2\n', 'line1\nline2'),
                          (b'a\tb\n', 'a\tb'), (b'cr\rx\n', 'cr\rx'), (b'trail  \n', 'trail  '),
                          (b'  lead\n', '  lead'), (b'\xff\xfe\n', '\udcff\udcfe'),
                          (b'X\npool/victim\tfilesystem\tFORGED\n', 'X\npool/victim\tfilesystem\tFORGED'),
                          ('caf\u00e9\n'.encode(), 'caf\u00e9')]:
            feed.set(out)
            assert zfs.get.value('pool/a', 'org.t:v') == want

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_value_ending_in_a_newline_keeps_it(self, feed):
        """zfs prints the value then `\\n`; a value that itself ends in `\\n` comes back with it."""
        feed.set(b'a\n\n')
        assert zfs.get.value('pool/a', 'org.t:v') == 'a\n'
        feed.set(b'a\n\n\n')
        assert zfs.get.value('pool/a', 'org.t:v') == 'a\n\n'
        feed.set(b'\n\n')
        assert zfs.get.value('pool/a', 'org.t:v') == '\n'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_dash_is_none_unless_raw(self, feed):
        feed.set(b'-\n')
        assert zfs.get.value('pool/a', 'org.t:v') is None
        assert zfs.get.value('pool/a', 'org.t:v', raw=True) == '-'

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_dash_only_means_unset_for_the_whole_value(self, feed):
        feed.set(b'-\n-\n')
        assert zfs.get.value('pool/a', 'org.t:v') == '-\n-'
        feed.set(b'- \n')
        assert zfs.get.value('pool/a', 'org.t:v') == '- '

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_no_output_is_none_and_an_empty_value_is_empty_string(self, feed):
        """`-s local` on a property with another source prints nothing; an empty local value prints one blank line."""
        feed.set(b'')
        assert zfs.get.value('pool/a', 'org.t:v', sources='local') is None
        assert zfs.get.value('pool/a', 'org.t:v', sources='local', raw=True) is None
        feed.set(b'\n')
        assert zfs.get.value('pool/a', 'org.t:v', sources='local') == ''
        assert zfs.get.value('pool/a', 'org.t:v', sources='local', raw=True) == ''

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('prop', ['all', '-x', '-s', '--help', 'name,type', 'org.t:v,org.t:w', 'Compression',
                                      'ALL', '', 'a b', 'org.t:v\n', 'volsize ', None, 5])
    @pytest.mark.parametrize('ds', [Filesystem('pool/a'), Dataset('pool/a'), 'pool/a'], ids=['Filesystem', 'Dataset', 'name'])
    def test_property_refusals_happen_before_exec(self, feed, ds, prop):
        """Checked for a plain Dataset and a bare name too: they skip the per-type check, so the name validation
        (and the explicit refusal of `all` and a leading `-`) is all that stands between the caller and zfs."""
        with pytest.raises(ValueError):
            zfs.get.value(ds, prop)
        assert feed.calls == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('ds,prop', [
        pytest.param(Filesystem('pool/a'), 'volsize', id='volsize-on-filesystem'),
        pytest.param(Volume('pool/v'), 'mountpoint', id='mountpoint-on-volume'),
        pytest.param(Filesystem('pool/a'), 'bogus-prop', id='bad-chars'),
        pytest.param(Snapshot(Filesystem('pool/a'), 's'), 'quota', id='quota-on-snapshot'),
    ])
    def test_the_property_must_fit_the_object_type(self, feed, ds, prop):
        with pytest.raises(ValueError):
            zfs.get.value(ds, prop)
        assert feed.calls == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_plain_dataset_or_a_name_may_ask_for_either_kind(self, feed):
        """A `Dataset` (or a bare name) does not know whether it is a filesystem or a volume."""
        feed.set(b'1G\n')
        assert zfs.get.value(Dataset('pool/a'), 'volsize') == '1G'
        assert zfs.get.value('pool/a', 'mountpoint') == '1G'

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('prop', ['compression', 'org.t:v', 'user:Mixed.Case_1', 'defaultuserquota', 'type',
                                      'guid', 'receive_resume_token', 'org.zfsbackup:anchor.dest'])
    def test_valid_properties_pass(self, feed, prop):
        feed.set(b'x\n')
        zfs.get.value(Filesystem('pool/a'), prop)
        assert feed.argv[-2] == prop

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('ds', [None, 5, 3.5, ['pool/a'], b'pool/a', object(), ('pool/a',)])
    def test_ds_that_is_not_a_zfs_object_or_name(self, feed, ds):
        with pytest.raises(ValueError):
            zfs.get.value(ds, 'org.t:v')
        assert feed.calls == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('name', ['', 'pool//a', 'pool/a ', 'pool/-x', '-r', 'pool/a\n'])
    def test_an_invalid_name_string_is_refused_before_exec(self, feed, name):
        with pytest.raises(ValueError):
            zfs.get.value(name, 'org.t:v')
        assert feed.calls == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_an_invalid_source_is_refused_before_exec(self, feed):
        for bad in ('bogus', ['local', 'nope'], 'local;rm'):
            with pytest.raises(ValueError):
                zfs.get.value('pool/a', 'org.t:v', sources=bad)
        assert feed.calls == []

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_zfs_failure_raises_a_bare_exception_with_stderr(self, feed):
        feed.set(b'', stderr=b"cannot open 'pool/missing': dataset does not exist\n", returncode=1)
        with pytest.raises(Exception) as info:
            zfs.get.value('pool/missing', 'org.t:v')
        assert type(info.value) is Exception            # not a ValueError: callers tell "refused" from "failed"
        assert str(info.value) == "Cannot open 'pool/missing': dataset does not exist"

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_failure_after_partial_output_still_raises(self, feed):
        feed.set(b'partial\n', stderr=b'io error\n', returncode=1)
        with pytest.raises(Exception, match='Io error'):
            zfs.get.value('pool/a', 'org.t:v')

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_works_on_a_mock_subprocess_with_text_lines(self, mock_subprocess):
        mock_subprocess.setup(stdout=['a\n', 'b\n'])
        assert zfs.get.value('pool/a', 'org.t:v') == 'a\nb'

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('real,expected', [
        ('victim', 'victimcfg'), ('attacker', 'line1\nline2'), ('forger', 'X\nb123p/victim\tfilesystem\tFORGED'),
        ('tab', 'a\tb'), ('ff', '\udcff\udcfe'), ('empty', ''), ('trail', 'trail  '), ('cr', 'cr\rx'),
        ('recv', 'X\nb123p/victim\tfilesystem\tRECV'), ('recv2', 'r1\tr2'), ('recv3', 'l1\tl2'),
    ])
    def test_real_zfs_bytes(self, feed, real, expected):
        feed.set(FORGED_VALUE[real])
        assert zfs.get.value(Filesystem('b123p/' + real), 'org.t:v') == expected
        assert feed.argv == ['get', '-H', '-o', 'value', 'org.t:v', 'b123p/' + real]

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('real,expected', [
        ('victim', 'victimcfg'), ('attacker', 'line1\nline2'), ('forger', 'X\nb123p/victim\tfilesystem\tFORGED'),
        ('tab', 'a\tb'), ('ff', '\udcff\udcfe'), ('empty', ''), ('trail', 'trail  '), ('cr', 'cr\rx'),
        ('recv', None), ('recv2', None), ('recv3', 'l1\tl2'),
    ])
    def test_real_zfs_bytes_local_only(self, feed, real, expected):
        """`-s local`: a received property that was never overridden prints nothing, so it reads as None."""
        feed.set(FORGED_VALUE_LOCAL[real])
        assert zfs.get.value('b123p/' + real, 'org.t:v', sources='local') == expected
        assert feed.argv == ['get', '-H', '-o', 'value', '-s', 'local', 'org.t:v', 'b123p/' + real]


# ---------------------------------------------------------------------------------------------------------------------
# B2: the real forged-row byte sequences through the parsers
# ---------------------------------------------------------------------------------------------------------------------

def _v(obj, prop='org.t:v'):
    p = obj[prop]
    return None if p is None else p.value


class TestForgedRowsFromRealZfs:
    """The bytes `zfs list -H` / `zfs get -H` really printed (OpenZFS 2.1.5, 2.2.2 and 2.4.1 alike) for datasets whose
    `org.t:v` holds newlines, tabs, forged rows, a received (`send -p`) forged row, `\\xff`, an empty value, trailing
    spaces and `\\r`. No forged object may come out of either parser."""

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_recursive(self, feed, caplog):
        feed.set(FORGED_LIST)
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = ListCommand()(roots=Filesystem('b123p'), recursive=True, properties='org.t:v')
        by = {o.name: o for o in result}
        # the forged "victim" rows (and the real one: one name, three rows, nothing says which is real) are all gone
        assert 'b123p/victim' not in by
        assert len(result) == len(by)
        # `attacker` held "line1\nline2": its row ends early and "line2" is a foreign one-column row
        assert 'b123p/attacker' not in by
        # tab values shift the columns: not recoverable in `list`
        assert not {'b123p/tab', 'b123p/recv2', 'b123p/recv3', 'b123p/src2'} & set(by)
        assert _v(by['b123p/empty']) == ''
        assert _v(by['b123p/trail']) == 'trail  '
        assert _v(by['b123p/cr']) == 'cr\rx'
        assert _v(by['b123p/ff']).encode('utf-8', 'surrogateescape') == b'\xff\xfe'
        assert _v(by['b123p/forger']) == 'X'
        assert all(isinstance(o, Filesystem) for o in result)
        assert feed.argv == ['list', '-H', '-r', '-o', 'name,type,org.t:v', 'b123p']
        text = '\n'.join(_warnings(caplog))
        assert 'continues on the next line' in text and 'malformed rows of zfs list output in total' in text

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_list_recursive_lazy(self, feed):
        feed.set(FORGED_LIST)
        result = list(ListCommand()(roots=Filesystem('b123p'), recursive=True, properties='org.t:v', lazy=True))
        names = [o.name for o in result]
        # lazy mode has only per-row checks: the pairs of the duplicated name cannot be taken back,
        # but the rest of the rules (and the continuation rule) hold
        assert 'b123p/attacker' not in names and 'b123p/tab' not in names
        assert names.count('b123p/victim') == 2        # the forged one and the real one
        assert {'b123p', 'b123p/empty', 'b123p/ff', 'b123p/forger', 'b123p/trail', 'b123p/cr'} <= set(names)

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_a_forged_row_outside_the_requested_root_takes_its_predecessor_with_it(self, feed, caplog):
        """`zfs list b123p/recv` itself prints the received value's tail as a second row, outside its root."""
        feed.set(b'b123p/recv\tfilesystem\tX\nb123p/victim\tfilesystem\tRECV\n')
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = ListCommand()(roots=Filesystem('b123p/recv'), properties='org.t:v')
        assert result == []
        assert any('outside the requested roots' in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_get_recursive(self, feed, caplog):
        feed.set(FORGED_GET)
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            result = GetCommand()(Filesystem('b123p'), properties=['org.t:v'], recursive=True)
        by = {o.name: o for o in result}
        assert 'b123p/victim' not in by                       # forged and real rows alike: not contiguous
        assert _v(by['b123p/tab']) == 'a\tb'
        assert (_v(by['b123p/recv2']), by['b123p/recv2']['org.t:v'].received) == ('r1\tr2', 'r1\tr2')
        assert _v(by['b123p/src2']) == 'r1\tr2'
        assert _v(by['b123p/recv3']) is None                  # local tab value over a received one: ambiguous
        assert _v(by['b123p/empty']) == ''
        assert _v(by['b123p/trail']) == 'trail  '
        assert _v(by['b123p/cr']) == 'cr\rx'
        assert _v(by['b123p/ff']).encode('utf-8', 'surrogateescape') == b'\xff\xfe'
        assert _v(by['b123p/attacker']) is None               # "line1" / "line2" are 3-column fragments
        assert all(isinstance(o, (Filesystem, Snapshot)) for o in result)
        assert 'b123p/recv@s' in by and 'b123p/src@s' in by
        assert feed.argv == ['get', '-H', '-o', 'name,property,value,received,source', '-r', '-t', 'all',
                             'name,type,org.t:v', 'b123p']

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_get_ambiguous_local_over_received_is_reported(self, feed, caplog):
        recv3 = b''.join(l + b'\n' for l in FORGED_GET.split(b'\n') if l.startswith(b'b123p/recv3\t'))
        feed.set(recv3)
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            (obj,) = GetCommand()(Filesystem('b123p/recv3'), properties=['org.t:v'])
        assert obj.name == 'b123p/recv3' and obj['org.t:v'] is None
        assert any('cannot tell the value from the received column' in w for w in _warnings(caplog))

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_get_recursive_lazy(self, feed):
        feed.set(FORGED_GET)
        result = list(GetCommand()(Filesystem('b123p'), properties=['org.t:v'], recursive=True, lazy=True))
        names = [o.name for o in result]
        assert names.count('b123p/victim') <= 1               # never the real+forged pair twice
        by = {o.name: o for o in result}
        assert _v(by['b123p/tab']) == 'a\tb'
        assert _v(by['b123p/recv3']) is None

    @pytest.mark.unit
    @pytest.mark.subprocess
    def test_get_all_properties_of_the_pool_keeps_the_name_and_type(self, feed):
        # (FORGED_GET is the explicit-list output `name,type,org.t:v`; an `all` listing has no `name` row)
        feed.set(FORGED_GET)
        result = GetCommand()(Filesystem('b123p'), properties='org.t:v', recursive=True)
        assert 'b123p' in [o.name for o in result]
        assert all(o.name.startswith('b123p') for o in result)


class TestNeverRaisesOnHostileOutput:
    """Property test: whatever bytes a hostile value turns into, the parsers return objects or skip -- they never
    raise, and every object returned is one the (here, fixed) scope admits."""

    ALPHABET = [b'pool', b'/', b'a', b'b', b'@', b'#', b's', b'\t', b'\n', b'\r', b'filesystem', b'volume', b'snapshot',
                b'bookmark', b'type', b'name', b'org.t:v', b'compression', b'-', b'local', b'received', b'\xff', b'',
                b' ', b'inherited from pool']

    def _blob(self, rng):
        return b''.join(rng.choice(self.ALPHABET) for _ in range(rng.randint(0, 40)))

    @pytest.mark.unit
    @pytest.mark.subprocess
    @pytest.mark.parametrize('seed', range(60))
    def test_list_and_get_never_raise(self, feed, seed):
        rng = random.Random(seed)
        data = b'\n'.join(self._blob(rng) for _ in range(rng.randint(0, 12))) + rng.choice([b'', b'\n'])
        feed.set(data)
        for lazy in (False, True):
            for roots, kw in [(None, {}), (Filesystem('pool/a'), {'recursive': True}), (Filesystem('pool'), {})]:
                scope = _Scope([str(roots)] if roots else [], recursive=kw.get('recursive', False))
                listed = list(ListCommand()(roots=roots, properties='org.t:v', lazy=lazy, **kw))
                assert all(isinstance(o, ZFS) and scope(o.name) for o in listed)
                got = list(GetCommand()(roots, properties='org.t:v', lazy=lazy, **kw))
                assert all(isinstance(o, ZFS) and scope(o.name) for o in got)
                if not lazy:
                    assert len({o.name for o in listed}) == len(listed)
                    assert len({o.name for o in got}) == len(got)

"""The parent's ends of a child's pipes are closed on every way out of the exec paths (no `ResourceWarning`).

`Command._exec_lines` and `Command._exec_collect` used to leave `process.stdout` / `process.stderr` (two
`BufferedReader`s, two fds) to the garbage collector after every command. `Command._close_child_pipes` now closes them in
the innermost `finally`: stdout unconditionally, stderr only once its reader thread has ended (a stuck or abandoned
child keeps stderr open: closing an fd another thread may be blocked in risks fd reuse). It never waits.

Two kinds of test:

* **Leak checks over real children.** Every exec path is driven against real hermetic children (`sys.executable -c ...`
  for the low-level paths, a stub script behind `$ZFS_CMD` for the public calls; the conftest guard only objects to a real
  `zfs`/`zpool`, and none is ever started). `no_leaks()` asserts two independent things: no descriptor opened inside the
  block is still open when it ends *before any garbage collection* (the fix closes explicitly, so a collector is not
  allowed to do it), and no `ResourceWarning` appears after a `gc.collect()` either. The warning is recorded, not turned
  into an error: an error raised from a deallocator is only an "unraisable" exception that a test would never see.
* **`_close_child_pipes` on fake processes**: the rule itself (stdout always, stderr only when the reader thread is not
  alive, never a `wait`/`poll`/`terminate`/`kill`, idempotent, a failing close swallowed) and its position in the
  `finally` of both callers.

Fork: a child belongs to the process that spawned it. A process forked from it inherits the generator, the `Popen`
and the pipes but cannot wait for the child (`Popen.poll()`/`wait()` answer ECHILD, which CPython turns into a return
code of 0), so resuming or continuing a call there raises `ZFSForkError` (and `StreamHandle.close()/poll()/terminate()`
do too) and touches nothing; the cleanup of an abandoned generator in a fork touches nothing either. Those guards are
pinned in-process (`_forked` patched) and over real forks in bounded driver interpreters, including a "Linux path"
variant (a selector that survives the fork, as epoll does, and an ECHILD-style `poll()`), so macOS runs the case that
only Linux used to reach.

Waits are shortened where a child is killed (`_TERMINATE_TIMEOUT`, `_EXPIRY_WAIT`); every real child is reaped by the end
of its test.
"""

import contextlib
import gc
import json
import os
import signal
import subprocess
import sys
import threading
import time
import warnings

import pytest

import libzfseasy
from libzfseasy import zfs as zfs_module
from libzfseasy.types import Filesystem, Snapshot
from _hermetic import hermetic_env
from libzfseasy.zfs import Command, ReceiveCommand, SendCommand, StreamHandle, ZFSForkError, ZFSTimeoutError

pytestmark = [pytest.mark.unit, pytest.mark.subprocess]


# ---------------------------------------------------------------------------------------------------------------------
# children
# ---------------------------------------------------------------------------------------------------------------------

LINES = 'import sys\nsys.stdout.write("one\\ntwo\\nthree\\n")\n'
LINES_ERR_OK = 'import sys\nsys.stdout.write("one\\ntwo\\n")\nsys.stderr.write("note\\n")\n'
FAIL = 'import sys\nsys.stderr.write("boom\\n")\nsys.exit(1)\n'
OUT_THEN_FAIL = 'import sys\nsys.stdout.write("one\\ntwo\\n")\nsys.stderr.write("partial\\n")\nsys.exit(1)\n'
NOTHING = ''
HELLO_THEN_SLEEP = 'import sys, time\nsys.stdout.write("first\\n"); sys.stdout.flush()\ntime.sleep(120)\n'
SLEEP = 'import time\ntime.sleep(120)\n'
# Two lines, then a long sleep: a guard that failed to stop a resumed read gets the second line at once (a failing test
# instead of a 120 s block), while the child is still running when the guard did its job.
TWO_THEN_SLEEP = 'import sys, time\nsys.stdout.write("first\\nsecond\\n"); sys.stdout.flush()\ntime.sleep(120)\n'
BINARY = 'import sys\nsys.stdout.buffer.write(b"x" * 100)\n'
BINARY_THEN_FAIL = 'import sys\nsys.stdout.buffer.write(b"x" * 100)\nsys.stderr.write("cut\\n")\nsys.exit(1)\n'
# What `zfs send -nv -t` prints on stdout for a good token (see `test_openzfs_formats.py` for the captures).
RESUME_CONTENTS = (
    'import sys\n'
    'sys.stdout.write("resume token contents:\\n")\n'
    'sys.stdout.write("nvlist version: 0\\n")\n'
    'sys.stdout.write("\\tfromguid = 0x1\\n")\n'
    'sys.stdout.write("\\ttoguid = 0x2\\n")\n'
    'sys.stdout.write("\\ttoname = pool/a@s\\n")\n'
)


def py(script):
    return [sys.executable, '-c', script]


@pytest.fixture
def quick(monkeypatch):
    """Short kill waits, so an expiry or an abandoned child costs a fraction of a second."""
    monkeypatch.setattr(zfs_module, '_TERMINATE_TIMEOUT', 0.5)
    monkeypatch.setattr(zfs_module, '_EXPIRY_WAIT', 0.5)


class Kids:
    """Records every real child started through `subprocess.Popen` and kills/reaps survivors when the test ends."""

    def __init__(self, monkeypatch):
        self.procs = []
        kids = self
        real_popen = subprocess.Popen   # the (guarded) class; subclassing keeps the guard

        class Recording(real_popen):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                kids.procs.append(self)

        monkeypatch.setattr(subprocess, 'Popen', Recording)

    @property
    def last(self):
        return self.procs[-1]

    def kill_all(self):
        for proc in self.procs:
            if proc.returncode is None:
                try:
                    proc.kill()
                except OSError:
                    pass
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass


@pytest.fixture
def kids(monkeypatch):
    k = Kids(monkeypatch)
    yield k
    k.kill_all()
    for proc in k.procs:                       # nothing here may keep a descriptor after the test
        for pipe in (proc.stdin, proc.stdout, proc.stderr):
            if pipe is not None:
                pipe.close()


@pytest.fixture(autouse=True)
def clean_stuck_registry():
    """`_stuck` is module state: every test starts with it empty and the original entries are put back afterwards."""
    saved_lock = zfs_module._stuck_lock
    saved_entries = list(zfs_module._stuck)
    zfs_module._stuck.clear()
    yield
    zfs_module._stuck_lock = saved_lock
    zfs_module._stuck[:] = saved_entries


# ---------------------------------------------------------------------------------------------------------------------
# the leak check
# ---------------------------------------------------------------------------------------------------------------------

def open_fds() -> set:
    return {int(name) for name in os.listdir('/dev/fd') if name.isdigit()}


@contextlib.contextmanager
def no_leaks():
    """Fail if the block leaves a descriptor open (looked at BEFORE any garbage collection, so a collector cannot be
    what closes it) or if collecting afterwards reports an unclosed file.

    A `ResourceWarning` is only recorded here (never raised): it is emitted from a deallocator, where an exception
    cannot propagate and only shows up as an "unraisable" message that nothing would fail on."""
    gc.collect()                                # earlier tests' garbage must not be blamed on this one
    before = open_fds()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always', ResourceWarning)
        yield
        leaked_fds = open_fds() - before
        gc.collect()
    unclosed = [str(w.message) for w in caught if issubclass(w.category, ResourceWarning)]
    assert not leaked_fds, f'descriptors still open when the call returned (before any gc): {sorted(leaked_fds)}'
    assert not unclosed, f'ResourceWarning(s): {unclosed}'


def drain(cmd):
    return list(Command._exec_lines(cmd))


def drain_expecting(cmd, exc_type=Exception):
    with pytest.raises(exc_type):
        drain(cmd)


# ---------------------------------------------------------------------------------------------------------------------
# _exec_lines
# ---------------------------------------------------------------------------------------------------------------------

class TestExecLinesLeaks:

    def test_a_clean_exit_leaves_nothing_open(self):
        with no_leaks():
            assert drain(py(LINES)) == ['one', 'two', 'three']

    def test_a_clean_exit_with_stderr_output_leaves_nothing_open(self):
        with no_leaks():
            assert drain(py(LINES_ERR_OK)) == ['one', 'two']

    def test_an_empty_clean_exit_leaves_nothing_open(self):
        with no_leaks():
            assert drain(py(NOTHING)) == []

    def test_a_non_zero_exit_leaves_nothing_open(self):
        with no_leaks():
            with pytest.raises(Exception, match='(?i)boom'):
                drain(py(FAIL))

    def test_a_non_zero_exit_after_output_leaves_nothing_open(self):
        with no_leaks():
            with pytest.raises(Exception, match='(?i)partial'):
                drain(py(OUT_THEN_FAIL))

    def test_a_generator_closed_after_one_line_leaves_nothing_open(self, quick):
        with no_leaks():
            lines = Command._exec_lines(py(HELLO_THEN_SLEEP))
            assert next(lines) == 'first\n'[:-1]
            lines.close()

    def test_a_generator_dropped_without_closing_leaves_nothing_open(self, quick):
        with no_leaks():
            lines = Command._exec_lines(py(HELLO_THEN_SLEEP))
            next(lines)
            del lines                           # CPython closes it at once; the finally must still run

    def test_an_exception_raised_into_the_generator_leaves_nothing_open(self, quick):
        with no_leaks():
            lines = Command._exec_lines(py(HELLO_THEN_SLEEP))
            next(lines)
            with pytest.raises(KeyboardInterrupt):
                lines.throw(KeyboardInterrupt())

    def test_exec_and_exec_out_leave_nothing_open(self, quick):
        with no_leaks():
            Command._exec(py(LINES))
            assert list(Command._exec_out(py(LINES))) == ['one', 'two', 'three']
            with pytest.raises(Exception, match='(?i)boom'):
                Command._exec(py(FAIL))
            out = Command._exec_out(py(HELLO_THEN_SLEEP))
            next(out)
            out.close()

    def test_both_pipes_of_the_child_are_closed_once_the_call_returns(self, kids):
        drain(py(LINES_ERR_OK))
        assert kids.last.stdout.closed and kids.last.stderr.closed

    def test_both_pipes_are_closed_after_a_failure(self, kids):
        with pytest.raises(Exception):
            drain(py(FAIL))
        assert kids.last.stdout.closed and kids.last.stderr.closed

    def test_both_pipes_are_closed_after_the_generator_is_closed_early(self, kids, quick):
        lines = Command._exec_lines(py(HELLO_THEN_SLEEP))
        next(lines)
        lines.close()
        assert kids.last.stdout.closed and kids.last.stderr.closed
        assert kids.last.returncode is not None


class TestTimedExecLinesLeaks:
    """The deadline path (`_read_until` over `select`) and its `_expire` / `_reap_timed` exits."""

    def test_a_call_finishing_inside_the_budget_leaves_nothing_open(self):
        with no_leaks():
            with libzfseasy.timeout(30):
                assert drain(py(LINES)) == ['one', 'two', 'three']

    def test_a_failing_call_inside_the_budget_leaves_nothing_open(self):
        with no_leaks():
            with libzfseasy.timeout(30):
                with pytest.raises(Exception, match='(?i)boom'):
                    drain(py(FAIL))

    def test_an_expiry_leaves_nothing_open(self, quick):
        with no_leaks():
            with libzfseasy.timeout(0.5):
                with pytest.raises(ZFSTimeoutError):
                    drain(py(SLEEP))

    def test_an_expiry_after_some_output_leaves_nothing_open(self, quick):
        with no_leaks():
            with libzfseasy.timeout(1):
                with pytest.raises(ZFSTimeoutError):
                    drain(py(HELLO_THEN_SLEEP))

    def test_a_timed_generator_closed_early_leaves_nothing_open(self, quick):
        with no_leaks():
            with libzfseasy.timeout(30):
                lines = Command._exec_lines(py(HELLO_THEN_SLEEP))
                next(lines)
                lines.close()

    def test_an_exception_raised_into_a_timed_generator_leaves_nothing_open(self, quick):
        with no_leaks():
            with libzfseasy.timeout(30):
                lines = Command._exec_lines(py(HELLO_THEN_SLEEP))
                next(lines)
                with pytest.raises(KeyboardInterrupt):
                    lines.throw(KeyboardInterrupt())

    def test_both_pipes_are_closed_after_an_expiry(self, kids, quick):
        with libzfseasy.timeout(0.5):
            with pytest.raises(ZFSTimeoutError):
                drain(py(SLEEP))
        assert kids.last.stdout.closed and kids.last.stderr.closed


# ---------------------------------------------------------------------------------------------------------------------
# _exec_collect
# ---------------------------------------------------------------------------------------------------------------------

class TestExecCollectLeaks:

    @pytest.mark.parametrize('raw_stderr', [False, True], ids=['stderr-stripped', 'raw-stderr'])
    def test_a_clean_exit_leaves_nothing_open(self, raw_stderr):
        with no_leaks():
            lines, errors, rc = Command._exec_collect(py(LINES_ERR_OK), raw_stderr=raw_stderr)
        assert lines == ['one', 'two'] and rc == 0
        assert [e.strip().lower() for e in errors] == ['note']

    @pytest.mark.parametrize('raw_stderr', [False, True], ids=['stderr-stripped', 'raw-stderr'])
    def test_a_non_zero_exit_leaves_nothing_open(self, raw_stderr):
        with no_leaks():
            lines, errors, rc = Command._exec_collect(py(OUT_THEN_FAIL), raw_stderr=raw_stderr)
        assert lines == ['one', 'two'] and rc == 1
        assert [e.strip().lower() for e in errors] == ['partial']

    @pytest.mark.parametrize('raw_stderr', [False, True], ids=['stderr-stripped', 'raw-stderr'])
    def test_no_output_at_all_leaves_nothing_open(self, raw_stderr):
        with no_leaks():
            assert Command._exec_collect(py(NOTHING), raw_stderr=raw_stderr) == ([], [], 0)

    @pytest.mark.parametrize('raw_stderr', [False, True], ids=['stderr-stripped', 'raw-stderr'])
    def test_both_pipes_are_closed_afterwards(self, kids, raw_stderr):
        Command._exec_collect(py(LINES_ERR_OK), raw_stderr=raw_stderr)
        assert kids.last.stdout.closed and kids.last.stderr.closed

    @pytest.mark.parametrize('raw_stderr', [False, True], ids=['stderr-stripped', 'raw-stderr'])
    def test_an_interrupt_after_the_spawn_leaves_nothing_open(self, kids, monkeypatch, raw_stderr):
        """A caller interrupted while the child runs (here: while the watchdog is set up, after the stderr reader
        started): the child is killed and reaped, and both pipes are closed."""
        def interrupted(cls, cmd, pid):
            raise KeyboardInterrupt

        monkeypatch.setattr(zfs_module._SlowCommandWatchdog, 'for_command', classmethod(interrupted))
        with no_leaks():
            with pytest.raises(KeyboardInterrupt):
                Command._exec_collect(py(HELLO_THEN_SLEEP), raw_stderr=raw_stderr)
        process = kids.last
        assert process.returncode is not None
        assert process.stdout.closed and process.stderr.closed

    def test_an_interrupt_after_the_spawn_leaves_nothing_open_in_exec_lines(self, kids, monkeypatch, quick):
        def interrupted(cls, cmd, pid):
            raise KeyboardInterrupt

        monkeypatch.setattr(zfs_module._SlowCommandWatchdog, 'for_command', classmethod(interrupted))
        with no_leaks():
            with pytest.raises(KeyboardInterrupt):
                drain(py(HELLO_THEN_SLEEP))
        process = kids.last
        assert process.returncode is not None
        assert process.stdout.closed and process.stderr.closed


# ---------------------------------------------------------------------------------------------------------------------
# streams
# ---------------------------------------------------------------------------------------------------------------------

class TestStreamLeaks:
    """The streaming variants closed their pipes before the fix; these pin that so a later change cannot regress it."""

    def test_a_failing_peek_leaves_nothing_open(self):
        with no_leaks():
            with pytest.raises(Exception, match='(?i)boom'):
                SendCommand._exec_stream(py(FAIL))

    def test_a_failing_peek_leaves_nothing_open_with_a_dying_child_after_output(self):
        """Output first, then exit 1: the peek succeeds, the failure is `close()`'s."""
        with no_leaks():
            handle = SendCommand._exec_stream(py(BINARY_THEN_FAIL))
            assert handle.read(100) == b'x' * 100
            with pytest.raises(Exception, match='(?i)cut'):
                handle.close()

    def test_close_after_a_successful_stream_leaves_nothing_open(self):
        with no_leaks():
            handle = SendCommand._exec_stream(py(BINARY))
            assert handle.read() == b'x' * 100
            handle.close()

    def test_an_empty_successful_stream_leaves_nothing_open(self):
        with no_leaks():
            handle = SendCommand._exec_stream(py(NOTHING))
            assert handle.read() == b''
            handle.close()

    def test_a_context_managed_stream_leaves_nothing_open(self):
        with no_leaks():
            with SendCommand._exec_stream(py(BINARY)) as handle:
                handle.read()

    def test_peek_false_with_an_immediate_failure_leaves_nothing_open(self):
        with no_leaks():
            handle = SendCommand._exec_stream(py(FAIL), peek=False)
            assert handle.read() == b''
            with pytest.raises(Exception, match='(?i)boom'):
                handle.close()

    def test_a_stream_closed_before_it_is_read_leaves_nothing_open(self, quick):
        with no_leaks():
            handle = SendCommand._exec_stream(py('import sys, time\nsys.stdout.write("x"); sys.stdout.flush()\n'
                                             'time.sleep(120)\n'))
            handle.terminate()
            with contextlib.suppress(Exception):
                handle.close()

    def test_stream_in_with_an_immediate_failure_leaves_nothing_open(self, monkeypatch):
        """`_exec_stream_in` polls once right after the spawn: make sure that poll sees the child's exit."""
        real_popen = subprocess.Popen

        class Settled(real_popen):
            def poll(self):
                try:
                    self.wait(timeout=10)       # let the failing child exit first, so the single poll sees it
                except subprocess.TimeoutExpired:
                    pass
                return super().poll()

        monkeypatch.setattr(subprocess, 'Popen', Settled)
        with no_leaks():
            with pytest.raises(Exception, match='(?i)boom'):
                ReceiveCommand._exec_stream_in(py(FAIL))

    def test_stream_in_closed_after_a_late_failure_leaves_nothing_open(self):
        with no_leaks():
            handle = ReceiveCommand._exec_stream_in(py('import sys\nsys.stdin.buffer.read()\nsys.stderr.write("late\\n")\n'
                                                'sys.exit(1)\n'))
            with pytest.raises(Exception, match='(?i)late'):
                handle.close()

    def test_stream_in_closed_after_success_leaves_nothing_open(self):
        with no_leaks():
            handle = ReceiveCommand._exec_stream_in(py('import sys\nsys.stdin.buffer.read()\n'))
            handle.write(b'data')
            handle.close()


# ---------------------------------------------------------------------------------------------------------------------
# the public calls, through a stub behind $ZFS_CMD
# ---------------------------------------------------------------------------------------------------------------------

STUB_SOURCE = r'''
import os, sys
mode = os.environ.get('LEAKSTUB_MODE', 'ok')
argv = sys.argv[1:]
sub = argv[0] if argv else ''
if mode == 'missing':
    sys.stderr.write("cannot open 'pool/a': dataset does not exist\n")
    sys.exit(1)
if mode == 'fail':
    sys.stderr.write("boom\n")
    sys.exit(1)
if mode == 'noisy':
    sys.stderr.write("warning: noise\n")
if sub == 'send':
    if '-t' in argv:
        for line in os.environ['LEAKSTUB_RESUME'].split('|'):
            sys.stdout.write(line.replace('~', '\t') + '\n')
    else:
        sys.stdout.buffer.write(b'x' * 100)
elif sub == 'list':
    o = argv[argv.index('-o') + 1].split(',') if '-o' in argv else ['name']
    row = {'name': 'pool/a', 'type': 'filesystem'}
    sys.stdout.write('\t'.join(row.get(c, '-') for c in o) + '\n')
elif sub == 'get':
    o = argv[argv.index('-o') + 1].split(',')
    name = argv[-1]
    if o == ['value']:
        sys.stdout.write('filesystem\n')
    elif o == ['name', 'property', 'source']:
        sys.stdout.write('%s\torg.x:a\tlocal\n' % name)
        sys.stdout.write('%s\torg.x:b\tlocal\n' % name)
    else:                                   # an explicit property list: exactly the properties asked for, in order
        for p in argv[-2].split(','):
            cols = {'name': name, 'property': p, 'value': {'name': name, 'type': 'filesystem'}.get(p, 'lz4'),
                    'received': '-', 'source': '-'}
            sys.stdout.write('\t'.join(cols[c] for c in o) + '\n')
'''


@pytest.fixture
def stub(tmp_path, monkeypatch):
    """`$ZFS_CMD` pointing at a hermetic stub script; `LEAKSTUB_MODE` picks its behaviour (`ok`, `fail`, `missing`,
    `noisy`). The script is `#!/bin/sh` + `exec python`, so it is never mistaken for a compiled real `zfs`."""
    body = tmp_path / 'stub_body.py'
    body.write_text(STUB_SOURCE)
    script = tmp_path / 'leakstub'
    script.write_text('#!/bin/sh\nexec "%s" "%s" "$@"\n' % (sys.executable, body))
    script.chmod(0o755)
    monkeypatch.setenv('ZFS_CMD', str(script))
    monkeypatch.setenv('LEAKSTUB_MODE', 'ok')
    monkeypatch.setenv('LEAKSTUB_RESUME', 'resume token contents:|nvlist version: 0|~fromguid = 0x1|~toguid = 0x2|'
                                          '~toname = pool/a@s')
    return script


FS = Filesystem('pool/a')


class TestPublicCallsLeak:

    @pytest.mark.parametrize('mode', ['ok', 'noisy'])
    def test_exists(self, stub, monkeypatch, mode):
        monkeypatch.setenv('LEAKSTUB_MODE', mode)
        with no_leaks():
            assert libzfseasy.exists(FS) is True

    def test_exists_for_a_missing_dataset(self, stub, monkeypatch):
        monkeypatch.setenv('LEAKSTUB_MODE', 'missing')
        with no_leaks():
            assert libzfseasy.exists(FS) is False

    def test_exists_when_zfs_fails(self, stub, monkeypatch):
        monkeypatch.setenv('LEAKSTUB_MODE', 'fail')
        with no_leaks():
            with pytest.raises(Exception, match='(?i)boom'):
                libzfseasy.exists(FS)

    @pytest.mark.parametrize('mode', ['ok', 'noisy'])
    def test_list(self, stub, monkeypatch, mode):
        monkeypatch.setenv('LEAKSTUB_MODE', mode)
        with no_leaks():
            result = libzfseasy.list(roots=FS)
        assert [d.name for d in result] == ['pool/a']

    def test_list_when_zfs_fails(self, stub, monkeypatch):
        monkeypatch.setenv('LEAKSTUB_MODE', 'fail')
        with no_leaks():
            with pytest.raises(Exception, match='(?i)boom'):
                libzfseasy.list(roots=FS)

    def test_list_strict(self, stub):
        with no_leaks():
            assert [d.name for d in libzfseasy.list(roots=FS, strict=True)] == ['pool/a']

    @pytest.mark.parametrize('mode', ['ok', 'noisy'])
    def test_get_value(self, stub, monkeypatch, mode):
        monkeypatch.setenv('LEAKSTUB_MODE', mode)
        with no_leaks():
            assert libzfseasy.get.value(FS, 'type') == 'filesystem'

    def test_get_value_when_zfs_fails(self, stub, monkeypatch):
        monkeypatch.setenv('LEAKSTUB_MODE', 'fail')
        with no_leaks():
            with pytest.raises(Exception, match='(?i)boom'):
                libzfseasy.get.value(FS, 'type')

    @pytest.mark.parametrize('mode', ['ok', 'noisy'])
    def test_get_property_sources(self, stub, monkeypatch, mode):
        monkeypatch.setenv('LEAKSTUB_MODE', mode)
        with no_leaks():
            result = libzfseasy.get.property_sources(FS, prefix='org.x:')
        assert sorted(r.property for r in result) == ['org.x:a', 'org.x:b']

    def test_get_property_sources_when_zfs_fails(self, stub, monkeypatch):
        monkeypatch.setenv('LEAKSTUB_MODE', 'fail')
        with no_leaks():
            with pytest.raises(Exception, match='(?i)boom'):
                libzfseasy.get.property_sources(FS, prefix='org.x:')

    @pytest.mark.parametrize('mode', ['ok', 'noisy'])
    def test_get_rows(self, stub, monkeypatch, mode):
        monkeypatch.setenv('LEAKSTUB_MODE', mode)
        with no_leaks():
            rows = libzfseasy.get.rows(FS, properties=['compression'], received=False)
        assert rows.complete and [r.property for r in rows] == ['name', 'type', 'compression']

    def test_get_rows_when_zfs_fails(self, stub, monkeypatch):
        monkeypatch.setenv('LEAKSTUB_MODE', 'fail')
        with no_leaks():
            with pytest.raises(Exception, match='(?i)boom'):
                libzfseasy.get.rows(FS, properties=['compression'], received=False)

    @pytest.mark.parametrize('mode', ['ok', 'noisy'])
    def test_send_resume_info(self, stub, monkeypatch, mode):
        monkeypatch.setenv('LEAKSTUB_MODE', mode)
        with no_leaks():
            info = libzfseasy.send.resume_info('1-abc-def')
        assert info.toname == 'pool/a@s'

    def test_send_resume_info_when_zfs_fails(self, stub, monkeypatch):
        monkeypatch.setenv('LEAKSTUB_MODE', 'fail')
        with no_leaks():
            with pytest.raises(Exception, match='(?i)boom'):
                libzfseasy.send.resume_info('1-abc-def')

    def test_a_send_stream_read_and_closed(self, stub):
        with no_leaks():
            handle = libzfseasy.send(Snapshot(FS, 's'))
            assert handle.read() == b'x' * 100
            handle.close()

    def test_a_send_that_fails_immediately(self, stub, monkeypatch):
        monkeypatch.setenv('LEAKSTUB_MODE', 'fail')
        with no_leaks():
            with pytest.raises(Exception, match='(?i)boom'):
                libzfseasy.send(Snapshot(FS, 's'))

    def test_a_timed_public_call(self, stub):
        with no_leaks():
            with libzfseasy.timeout(30):
                assert libzfseasy.exists(FS) is True
                assert libzfseasy.get.value(FS, 'type') == 'filesystem'


# ---------------------------------------------------------------------------------------------------------------------
# an abandoned child: stderr stays open while its reader lives, stdout never does
# ---------------------------------------------------------------------------------------------------------------------

class TestAbandonedChildPipes:
    """The rule of `_close_child_pipes` through the real callers, with a real child that the cleanup above it reports
    as abandoned (survived SIGKILL): the child is left running, so its stderr reader is still blocked in the pipe."""

    @pytest.fixture
    def readers(self, monkeypatch):
        """The stderr reader threads started by `_exec_lines`."""
        threads = []
        real = Command._start_stderr_reader_lines

        def recording(process, errors):
            thread = real(process, errors)
            threads.append(thread)
            return thread

        monkeypatch.setattr(Command, '_start_stderr_reader_lines', staticmethod(recording))
        return threads

    @staticmethod
    def close_bounded(lines, process):
        """`lines.close()` on a thread: closing a pipe whose reader thread sits inside it blocks until that read returns,
        so a regression there would wedge the suite. Past 15 s the child is killed (which releases the reader) and the
        test fails."""
        thread = threading.Thread(target=lines.close, daemon=True)
        thread.start()
        thread.join(15)
        if thread.is_alive():
            process.kill()
            thread.join(10)
            pytest.fail('closing the generator blocked: a pipe was closed while its reader thread was inside it')

    @staticmethod
    def finish(process, readers):
        """Clean up the way the stuck-child sweep would: the child goes, the reader ends, then the pipe can close."""
        process.kill()
        process.wait(timeout=10)
        for thread in readers:
            thread.join(10)
            assert not thread.is_alive()
        process.stderr.close()

    def test_stdout_closed_and_stderr_left_open_for_an_abandoned_child(self, kids, readers, monkeypatch, quick):
        monkeypatch.setattr(Command, '_terminate_child', classmethod(lambda cls, process, cmd: True))
        lines = Command._exec_lines(py(HELLO_THEN_SLEEP))
        next(lines)
        process = kids.last
        self.close_bounded(lines, process)
        assert process.stdout.closed, 'this call is the only reader of stdout: closed even when the child is abandoned'
        assert readers[0].is_alive() and not process.stderr.closed, 'the reader may be in the pipe: the fd stays open'
        assert process.poll() is None, 'the call must not have waited for, or ended, the child'
        self.finish(process, readers)

    def test_the_same_for_a_timed_call_that_ends_early(self, kids, readers, monkeypatch, quick):
        monkeypatch.setattr(Command, '_reap_timed', classmethod(lambda cls, process, thread, cmd, owner=None: True))
        with libzfseasy.timeout(30):
            lines = Command._exec_lines(py(HELLO_THEN_SLEEP))
            next(lines)
            process = kids.last
            self.close_bounded(lines, process)
        assert process.stdout.closed
        assert readers[0].is_alive() and not process.stderr.closed
        assert process.poll() is None
        self.finish(process, readers)


# ---------------------------------------------------------------------------------------------------------------------
# Command._close_child_pipes on fake processes
# ---------------------------------------------------------------------------------------------------------------------

class FakePipe:
    """Counts `close()` calls and, like a real pipe, reports `closed` afterwards."""

    def __init__(self, fail=False):
        self.closed = False
        self.close_calls = 0
        self.fail = fail

    def close(self):
        self.close_calls += 1
        if self.fail:
            raise OSError('close failed')
        self.closed = True


class FakeProcess:
    """A process whose only legal use is its two pipes: any attempt to wait for, poll or signal it fails the test."""

    def __init__(self, stdout=True, stderr=True):
        self.stdout = FakePipe() if stdout is True else stdout
        self.stderr = FakePipe() if stderr is True else stderr
        self.pid = 4242
        self.calls = []

    def _forbidden(self, name):
        def call(*args, **kwargs):
            self.calls.append(name)
            raise AssertionError(f'_close_child_pipes must not call {name}()')
        return call

    def __getattr__(self, name):
        if name in ('wait', 'poll', 'terminate', 'kill', 'send_signal', 'communicate'):
            return self._forbidden(name)
        raise AttributeError(name)


class FakeThread:
    def __init__(self, alive):
        self.alive = alive
        self.joined = []

    def is_alive(self):
        return self.alive

    def join(self, timeout=None):
        self.joined.append(timeout)
        raise AssertionError('_close_child_pipes must never join the reader')


class TestCloseChildPipes:

    def test_both_pipes_are_closed_when_the_reader_has_ended(self):
        process = FakeProcess()
        Command._close_child_pipes(process, FakeThread(alive=False))
        assert process.stdout.closed and process.stderr.closed

    def test_both_pipes_are_closed_without_a_reader_thread(self):
        process = FakeProcess()
        Command._close_child_pipes(process, None)
        assert process.stdout.closed and process.stderr.closed

    def test_stderr_stays_open_while_the_reader_thread_is_alive(self):
        process = FakeProcess()
        Command._close_child_pipes(process, FakeThread(alive=True))
        assert process.stdout.closed
        assert not process.stderr.closed and process.stderr.close_calls == 0

    def test_stdout_is_closed_even_for_an_abandoned_child(self):
        """An abandoned child (survived SIGKILL, registered in `stuck_children()`): its reader is still blocked."""
        process = FakeProcess()
        zfs_module._register_stuck(process, FakeThread(alive=True), 'zfs list')
        Command._close_child_pipes(process, FakeThread(alive=True))
        assert process.stdout.closed and not process.stderr.closed

    def test_stderr_closes_on_a_later_call_once_the_reader_has_ended(self):
        process = FakeProcess()
        thread = FakeThread(alive=True)
        Command._close_child_pipes(process, thread)
        assert not process.stderr.closed
        thread.alive = False
        Command._close_child_pipes(process, thread)
        assert process.stderr.closed
        assert process.stdout.close_calls == 1, 'stdout was already closed: not closed a second time'

    @pytest.mark.parametrize('alive', [True, False])
    def test_it_never_waits_for_or_signals_the_child_or_joins_the_reader(self, alive):
        process = FakeProcess()
        thread = FakeThread(alive=alive)
        Command._close_child_pipes(process, thread)
        assert process.calls == [] and thread.joined == []

    @pytest.mark.parametrize('alive', [True, False])
    def test_it_is_idempotent(self, alive):
        process = FakeProcess()
        thread = FakeThread(alive=alive)
        for _ in range(3):
            Command._close_child_pipes(process, thread)
        assert process.stdout.close_calls == 1
        assert process.stderr.close_calls == (0 if alive else 1)

    def test_pipes_already_closed_by_reap_timed_are_not_closed_again(self):
        process = FakeProcess()
        process.stdout.closed = process.stderr.closed = True
        Command._close_child_pipes(process, FakeThread(alive=False))
        assert process.stdout.close_calls == 0 and process.stderr.close_calls == 0

    def test_a_pipe_whose_close_fails_does_not_stop_the_other(self):
        process = FakeProcess(stdout=FakePipe(fail=True))
        Command._close_child_pipes(process, FakeThread(alive=False))
        assert process.stdout.close_calls == 1
        assert process.stderr.closed

    def test_a_process_without_pipes_is_fine(self):
        process = FakeProcess(stdout=None, stderr=None)
        Command._close_child_pipes(process, None)
        Command._close_child_pipes(object(), None)

    def test_a_process_without_stderr_still_has_stdout_closed(self):
        process = FakeProcess(stderr=None)
        Command._close_child_pipes(process, FakeThread(alive=False))
        assert process.stdout.closed


# ---------------------------------------------------------------------------------------------------------------------
# where the close sits in the callers' finally
# ---------------------------------------------------------------------------------------------------------------------

class RecordingWatchdog:
    def __init__(self, events):
        self.events = events

    def start(self):
        pass

    def cancel(self):
        self.events.append('watchdog.cancel')


class TestCloseSitsInTheFinally:

    @pytest.fixture
    def events(self, monkeypatch):
        events = []
        real = Command._close_child_pipes
        monkeypatch.setattr(Command, '_close_child_pipes',
                            staticmethod(lambda process, thread, owner=None: (events.append('close_pipes'),
                                                                                  real(process, thread, owner))))
        monkeypatch.setattr(zfs_module._SlowCommandWatchdog, 'for_command',
                            classmethod(lambda cls, cmd, pid: RecordingWatchdog(events)))
        return events

    def test_exec_lines_closes_the_pipes_before_cancelling_the_watchdog(self, events):
        drain(py(LINES))
        assert events == ['close_pipes', 'watchdog.cancel']

    def test_exec_collect_closes_the_pipes_before_cancelling_the_watchdog(self, events):
        Command._exec_collect(py(LINES))
        assert events == ['close_pipes', 'watchdog.cancel']

    def test_exec_lines_cancels_the_watchdog_even_if_the_close_raises(self, monkeypatch):
        events = []
        monkeypatch.setattr(zfs_module._SlowCommandWatchdog, 'for_command',
                            classmethod(lambda cls, cmd, pid: RecordingWatchdog(events)))

        def broken(process, thread, owner=None):
            raise RuntimeError('close blew up')

        monkeypatch.setattr(Command, '_close_child_pipes', staticmethod(broken))
        with pytest.raises(RuntimeError, match='close blew up'):
            drain(py(LINES))
        assert events == ['watchdog.cancel']

    def test_exec_collect_cancels_the_watchdog_even_if_the_close_raises(self, monkeypatch):
        events = []
        monkeypatch.setattr(zfs_module._SlowCommandWatchdog, 'for_command',
                            classmethod(lambda cls, cmd, pid: RecordingWatchdog(events)))

        def broken(process, thread, owner=None):
            raise RuntimeError('close blew up')

        monkeypatch.setattr(Command, '_close_child_pipes', staticmethod(broken))
        with pytest.raises(RuntimeError, match='close blew up'):
            Command._exec_collect(py(LINES))
        assert events == ['watchdog.cancel']

    def test_the_pipes_are_closed_even_if_terminating_the_child_raises(self, kids, monkeypatch):
        """The close is in the innermost `finally`: an exception out of the child cleanup above it must not skip it."""
        def broken(cls, process, cmd):
            raise RuntimeError('terminate blew up')

        monkeypatch.setattr(Command, '_terminate_child', classmethod(broken))
        lines = Command._exec_lines(py(HELLO_THEN_SLEEP))
        next(lines)
        process = kids.last
        with pytest.raises(RuntimeError, match='terminate blew up'):
            lines.close()
        assert process.stdout.closed
        process.kill()
        process.wait(timeout=10)

    def test_the_pipes_are_closed_even_if_reaping_a_timed_child_raises(self, kids, monkeypatch):
        def broken(cls, process, thread, cmd, owner=None):
            raise RuntimeError('reap blew up')

        monkeypatch.setattr(Command, '_reap_timed', classmethod(broken))
        with libzfseasy.timeout(30):
            lines = Command._exec_lines(py(HELLO_THEN_SLEEP))
            next(lines)
            process = kids.last
            with pytest.raises(RuntimeError, match='reap blew up'):
                lines.close()
        assert process.stdout.closed
        process.kill()
        process.wait(timeout=10)

    def test_exec_collect_closes_the_pipes_when_the_popen_fails_after_spawn(self, kids, monkeypatch):
        """A failure between the spawn and the read loop (the stderr reader cannot start) still closes both pipes."""
        def broken(process, errors):
            raise RuntimeError('no reader')

        monkeypatch.setattr(Command, '_start_stderr_reader_lines', staticmethod(broken))
        with pytest.raises(RuntimeError, match='no reader'):
            Command._exec_collect(py(NOTHING))
        assert kids.last.stdout.closed and kids.last.stderr.closed


# ---------------------------------------------------------------------------------------------------------------------
# fork: a forked process must leave the parent's child and pipes alone
# ---------------------------------------------------------------------------------------------------------------------

class TestForkedGuardUnit:
    """`Command._forked(owner)` and the `owner` argument of the cleanup helpers, on fake processes (no fork)."""

    OTHER = os.getpid() + 1          # never this process's pid

    def test_forked_is_false_before_the_spawn_and_in_the_spawning_process(self):
        assert Command._forked(None) is False
        assert Command._forked(os.getpid()) is False

    def test_forked_is_true_in_any_other_process(self):
        assert Command._forked(self.OTHER) is True

    def test_close_child_pipes_closes_nothing_in_a_forked_process(self):
        process = FakeProcess()
        Command._close_child_pipes(process, FakeThread(alive=False), self.OTHER)
        assert process.stdout.close_calls == 0 and process.stderr.close_calls == 0

    def test_close_child_pipes_still_closes_in_the_spawning_process(self):
        process = FakeProcess()
        Command._close_child_pipes(process, FakeThread(alive=False), os.getpid())
        assert process.stdout.closed and process.stderr.closed

    def test_reap_timed_does_nothing_in_a_forked_process(self):
        """No TERM, KILL, wait, poll, stuck registration or close: the child and its pipes are the parent's."""
        process = FakeProcess()
        assert Command._reap_timed(process, FakeThread(alive=False), ['zfs', 'list'], self.OTHER) is False
        assert process.calls == []
        assert process.stdout.close_calls == 0 and process.stderr.close_calls == 0
        assert zfs_module._stuck == []

    def test_expire_in_a_forked_process_raises_the_timeout_without_touching_the_child(self):
        process = FakeProcess()
        error = Command._expire(process, FakeThread(alive=False), ['zfs', 'list'], 5, self.OTHER)
        assert isinstance(error, ZFSTimeoutError)
        assert process.calls == [] and process.stdout.close_calls == 0
        assert zfs_module._stuck == []

    def test_zfs_fork_error_is_exported_and_a_plain_exception(self):
        assert libzfseasy.ZFSForkError is zfs_module.ZFSForkError is ZFSForkError
        assert issubclass(ZFSForkError, Exception)
        assert not issubclass(ZFSForkError, (OSError, ValueError, RuntimeError))

    @pytest.mark.parametrize('action', ['read', 'closed', 'polled', 'terminated'])
    def test_refuse_fork_raises_with_the_documented_message_in_another_process(self, action):
        with pytest.raises(ZFSForkError) as raised:
            Command._refuse_fork(self.OTHER, ['/sbin/zfs', 'send', 'pool/a@s'], action)
        assert str(raised.value) == (f'zfs send: this command was started by process {self.OTHER} and cannot be '
                                     f'{action} from a forked process (this is process {os.getpid()})')

    def test_refuse_fork_is_silent_for_the_owner_and_before_the_spawn(self):
        Command._refuse_fork(os.getpid(), ['zfs', 'list'])
        Command._refuse_fork(None, ['zfs', 'list'])

    def test_abort_stream_does_nothing_for_a_foreign_owner(self):
        """The failed-start cleanup of `_exec_stream`/`_exec_stream_in`: in a fork the child and pipes are the parent's."""
        process = FakeProcess()
        Command._abort_stream(process, FakeThread(alive=False), self.OTHER)
        assert process.calls == []
        assert process.stdout.close_calls == 0 and process.stderr.close_calls == 0
        assert zfs_module._stuck == []

    @pytest.mark.parametrize('owner', ['omitted', 'own'])
    def test_abort_stream_still_reaps_for_the_owner_and_without_an_owner(self, kids, quick, owner):
        """`owner` is optional: a direct call without it (the old signature) behaves as before."""
        process = subprocess.Popen(py(HELLO_THEN_SLEEP), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        if owner == 'omitted':
            Command._abort_stream(process, None)
        else:
            Command._abort_stream(process, None, os.getpid())
        assert process.returncode is not None
        assert process.stdin.closed and process.stdout.closed and process.stderr.closed

    def test_read_until_without_an_owner_is_unaffected(self, kids):
        """`owner` and `cmd` are optional: the old three-argument call reads to the end and returns the exit status."""
        process = subprocess.Popen(py(LINES), stdout=subprocess.PIPE)
        gen = Command._read_until(process, process.stdout.fileno(), time.monotonic() + 30)
        lines = []
        while True:
            try:
                lines.append(next(gen))
            except StopIteration as stop:
                rc = stop.value
                break
        assert lines == ['one', 'two', 'three'] and rc == 0

    def test_read_until_for_a_foreign_owner_raises_after_the_first_line_and_touches_nothing(self, kids):
        process = subprocess.Popen(py('import sys, time\nsys.stdout.write("one\\ntwo\\n"); sys.stdout.flush()\n'
                                      'time.sleep(120)\n'), stdout=subprocess.PIPE)
        gen = Command._read_until(process, process.stdout.fileno(), time.monotonic() + 30, self.OTHER, ['zfs', 'list'])
        assert next(gen) == 'one'
        with pytest.raises(ZFSForkError, match='zfs list: .*cannot be read from a forked process'):
            next(gen)
        assert process.poll() is None and not process.stdout.closed, 'the parent\'s child and pipe are left alone'


class FakeStream:
    """A buffered stream over a `raw` descriptor (`FakePipe`, which counts `close()` calls and is what a fork must
    close); its I/O methods fail the test if they are reached; status queries answer."""

    name = 'fake'
    mode = 'rb'

    def __init__(self):
        self.raw = FakePipe()
        self.io_calls = []

    @property
    def closed(self):
        return self.raw.closed

    def _io(self, name):
        def call(*args, **kwargs):
            self.io_calls.append(name)
            raise AssertionError(f'{name}() reached the stream in a forked process')
        return call

    def __getattr__(self, name):
        if name in ('read', 'write', 'peek', 'readinto', 'flush', 'fileno', 'buffer', 'readline', 'close'):
            return self._io(name)
        raise AttributeError(name)

    def readable(self):
        return True


class TestStreamHandleInAForkUnit:
    """`StreamHandle` records its creator; in another process `poll()` and `terminate()` raise `ZFSForkError` before
    touching anything, `close()` only lets go of the fork's own descriptor copies (no flush, no wait, no signal) and
    raises, and the I/O attributes of the wrapped stream (`read`, `write`, ...) are refused while status queries
    still answer. The fake process fails the test if it is waited for, polled or signalled."""

    OTHER = os.getpid() + 1

    @pytest.fixture
    def handle(self):
        process, stream = FakeProcess(), FakeStream()
        process.stdout = stream                   # a send: the stream is the process's stdout
        handle = StreamHandle(process, stream, [], None)
        assert handle._owner == os.getpid()
        handle._owner = self.OTHER                # "this handle was created by another process"
        return handle

    @pytest.mark.parametrize('method,action', [('poll', 'polled'), ('terminate', 'terminated')])
    def test_poll_and_terminate_raise_and_touch_nothing(self, handle, method, action):
        with pytest.raises(ZFSForkError, match=f'cannot be {action} from a forked process'):
            getattr(handle, method)()
        assert handle._process.calls == []
        assert handle._stream.raw.close_calls == 0 and handle._process.stderr.close_calls == 0
        assert not handle._closed
        assert zfs_module._stuck == []

    def test_close_lets_go_of_the_forks_descriptors_only_and_raises(self, handle):
        with pytest.raises(ZFSForkError, match='cannot be closed from a forked process'):
            handle.close()
        assert handle._stream.raw.close_calls == 1, 'the fork\'s copy of the stream must be released at the descriptor'
        assert handle._stream.io_calls == [], 'no flush (nor Buffered.close): buffered bytes belong to the creator'
        assert handle._process.stderr.close_calls == 0, 'stderr is the creator\'s reader\'s'
        assert handle._process.calls == [], 'no wait, poll or signal'
        assert not handle._closed, 'only the creator can close the handle for good'
        assert zfs_module._stuck == []
        with pytest.raises(ZFSForkError):         # a second call: still refused, nothing closed twice
            handle.close()
        assert handle._stream.raw.close_calls == 1

    @pytest.mark.parametrize('name', ['read', 'write', 'peek', 'flush', 'fileno', 'raw', 'buffer', 'readinto'])
    def test_io_attributes_of_the_stream_are_refused(self, handle, name):
        with pytest.raises(ZFSForkError):
            getattr(handle, name)
        assert handle._stream.io_calls == []

    def test_an_unknown_name_is_refused_but_underscore_names_are_forwarded(self, handle):
        handle._stream._private = 'p'
        assert handle._private == 'p', 'underscore names are forwarded as before'
        with pytest.raises(ZFSForkError):
            handle.no_such_method
        assert handle._stream.io_calls == []

    def test_an_explicit_owner_decides_not_the_current_pid(self):
        """`owner=X` (what the stream cores pass: the pid taken before the spawn) is what the handle compares with; the
        default is the creating process."""
        foreign = StreamHandle(FakeProcess(), FakeStream(), [], None, owner=self.OTHER)
        assert foreign._owner == self.OTHER
        with pytest.raises(ZFSForkError, match='cannot be polled from a forked process'):
            foreign.poll()
        with pytest.raises(ZFSForkError):
            foreign.read
        own = StreamHandle(FakeProcess(), FakeStream(), [], None, owner=os.getpid())
        assert own._owner == os.getpid()
        with pytest.raises(AssertionError, match=r'read\(\) reached the stream'):   # forwarded: not refused
            own.read()
        assert StreamHandle(FakeProcess(), FakeStream(), [])._owner == os.getpid()

    def test_status_queries_and_pid_still_answer(self, handle):
        assert handle.closed is False and handle.readable() is True
        assert handle.name == 'fake' and handle.mode == 'rb'
        assert handle.pid == 4242

    def test_close_of_an_already_closed_handle_stays_a_no_op(self, handle):
        handle._closed = True
        handle.close()
        assert handle._process.calls == [] and handle._stream.raw.close_calls == 0

    def test_the_creator_still_gets_its_stream(self):
        """In the creating process the same attribute is forwarded: the fake stream (which fails the test) is reached."""
        handle = StreamHandle(FakeProcess(), FakeStream(), [], None)
        with pytest.raises(AssertionError, match=r'read\(\) reached the stream'):
            handle.read()


class Foreign(list):
    """What the `foreign` fixture yields: the cleanup calls it recorded (a list) and `forked`, the answer `_forked`
    gives right now. A test that must get a call past its fork guard first sets `forked = False`, and flips it after the
    first `next()`: the process then "forks" exactly while the generator is suspended."""

    forked = True


class TestForkedCleanupInProcess:
    """The callers' own `_forked` branches, without a fork: `Command._forked` is made to report "another process" and a
    real child is run through each exit. A real fork cannot tell these branches from the helpers' own guards (a forked
    `Popen.poll()` finds no such child and reports an exit by itself), so this is where they are pinned: nothing is
    signalled, waited for, joined, registered or closed. Resuming a generator or continuing a call in the fork raises
    `ZFSForkError` (the guard sits at the top of every read), so a test about the cleanup lets the first read happen as
    the owner and flips `foreign.forked` while the generator is suspended."""

    @pytest.fixture
    def foreign(self, kids, monkeypatch):
        calls = Foreign()
        monkeypatch.setattr(Command, '_forked', staticmethod(lambda owner: calls.forked))
        for name in ('_terminate_child', '_reap_timed', '_join_stderr_reader'):
            monkeypatch.setattr(Command, name, staticmethod(lambda *a, _n=name, **k: calls.append(_n)))
        yield calls
        # the real owner would clean up: here the test does, so nothing outlives it
        kids.kill_all()

    def assert_untouched(self, process, calls):
        assert calls == [], f'the forked process must not run {calls}'
        assert process.poll() is None, 'the parent\'s child must still be running'
        assert not process.stdout.closed and not process.stderr.closed
        assert zfs_module._stuck == []

    def test_an_exec_lines_generator_closed_early_touches_nothing(self, kids, foreign):
        foreign.forked = False                  # the owner reads the first line ...
        lines = Command._exec_lines(py(HELLO_THEN_SLEEP))
        next(lines)
        foreign.forked = True                   # ... and the generator is then closed in a fork
        lines.close()
        self.assert_untouched(kids.last, foreign)

    def test_a_timed_exec_lines_generator_closed_early_touches_nothing(self, kids, foreign):
        with libzfseasy.timeout(30):
            lines = Command._exec_lines(py(HELLO_THEN_SLEEP))
            next(lines)
            lines.close()
        self.assert_untouched(kids.last, foreign)

    def test_an_exception_raised_into_the_generator_touches_nothing(self, kids, foreign):
        foreign.forked = False
        lines = Command._exec_lines(py(HELLO_THEN_SLEEP))
        next(lines)
        foreign.forked = True
        with pytest.raises(KeyboardInterrupt):
            lines.throw(KeyboardInterrupt())
        self.assert_untouched(kids.last, foreign)

    @pytest.mark.parametrize('timed', [False, True], ids=['untimed', 'timed'])
    def test_a_generator_resumed_in_a_fork_raises_and_touches_nothing(self, kids, foreign, timed):
        """The whole fix in one line: resumed in a process that does not own the child, the generator refuses (it could
        neither read the parent's output nor trust a `poll()` that would answer ECHILD as a clean exit), and its own
        cleanup still leaves the child and the pipes alone."""
        foreign.forked = False
        with libzfseasy.timeout(30) if timed else contextlib.nullcontext():
            lines = Command._exec_lines(py(TWO_THEN_SLEEP))
            assert next(lines) == 'first'
            foreign.forked = True
            with pytest.raises(ZFSForkError, match='cannot be read from a forked process'):
                next(lines)
        self.assert_untouched(kids.last, foreign)

    @pytest.mark.parametrize('raw_stderr', [False, True], ids=['stderr-stripped', 'raw-stderr'])
    def test_an_exec_collect_continued_in_a_fork_raises_and_touches_nothing(self, kids, foreign, monkeypatch,
                                                                           raw_stderr):
        """The `_SlowCommandWatchdog.for_command` hook is where a fork can happen inside `_exec_collect` (a signal
        handler that forks and returns): the hook here makes the process "a fork" and RETURNS, so the call goes on in it.
        It must refuse at its next read instead of stealing the parent's output or trusting `wait()`."""
        foreign.forked = False

        def forks_and_returns(cls, cmd, pid):
            foreign.forked = True
            return None

        monkeypatch.setattr(zfs_module._SlowCommandWatchdog, 'for_command', classmethod(forks_and_returns))
        # Without the guard this call would read until the child's EOF (120 s): the timer ends the child instead, so a
        # regression fails the assertion below rather than hanging the suite.
        backstop = threading.Timer(15, lambda: [p.kill() for p in kids.procs if p.returncode is None])
        backstop.start()
        try:
            with pytest.raises(ZFSForkError, match='cannot be read from a forked process'):
                Command._exec_collect(py(HELLO_THEN_SLEEP), raw_stderr=raw_stderr)
        finally:
            backstop.cancel()
        self.assert_untouched(kids.last, foreign)

    @pytest.mark.parametrize('raw_stderr', [False, True], ids=['stderr-stripped', 'raw-stderr'])
    def test_an_interrupted_exec_collect_touches_nothing(self, kids, foreign, monkeypatch, raw_stderr):
        def interrupted(cls, cmd, pid):
            raise KeyboardInterrupt

        monkeypatch.setattr(zfs_module._SlowCommandWatchdog, 'for_command', classmethod(interrupted))
        with pytest.raises(KeyboardInterrupt):
            Command._exec_collect(py(HELLO_THEN_SLEEP), raw_stderr=raw_stderr)
        self.assert_untouched(kids.last, foreign)


class TestForkedExpiryInProcess:
    def test_an_expiry_in_a_forked_process_hands_the_owner_to_the_reaper(self, kids, monkeypatch):
        """Kept as a second layer behind `ZFSForkError`: a resumed generator in a fork now raises before it can reach
        `_expire`, so the fork guard (`_refuse_fork`) is switched off here to pin the other layer, that `_exec_lines`
        passes the spawner's pid to `_expire`. `_forked` is true only for a recorded owner, so a call that forgot to
        pass it would reap (kill) the child here. `TestForkedGuardUnit` covers `_expire` itself; only this test covers
        the argument."""
        monkeypatch.setattr(Command, '_forked', staticmethod(lambda owner: owner is not None))
        monkeypatch.setattr(Command, '_refuse_fork', staticmethod(lambda owner, cmd, action='read': None))
        with libzfseasy.timeout(0.5):
            with pytest.raises(ZFSTimeoutError):
                drain(py(HELLO_THEN_SLEEP))
        process = kids.last
        assert process.poll() is None, 'the expiry killed the child although the process is not its owner'
        assert not process.stdout.closed and not process.stderr.closed
        assert zfs_module._stuck == []


class TestStreamStartInAForkInProcess:
    """`_exec_stream` and `_exec_stream_in` check the process after the spawn, before they trust the peek or the single
    `poll()` (both would be the parent's child seen from a fork: a bogus clean exit). `_forked` is made true here (the
    call "continues in a fork", e.g. a signal handler that forked and returned); the failed-start cleanup then leaves
    the parent's child and pipes alone."""

    @pytest.fixture
    def forked(self, kids, monkeypatch):
        monkeypatch.setattr(Command, '_forked', staticmethod(lambda owner: owner is not None))

    def test_exec_stream_refuses_and_leaves_the_child_alone(self, kids, forked):
        with pytest.raises(ZFSForkError, match='cannot be read from a forked process'):
            SendCommand._exec_stream(py('import sys, time\nsys.stdout.write("x"); sys.stdout.flush()\n'
                                        'time.sleep(120)\n'))
        process = kids.last
        assert process.poll() is None, 'the failed-start cleanup killed the parent\'s child'
        assert not process.stdout.closed and not process.stderr.closed
        assert zfs_module._stuck == []

    def test_exec_stream_in_refuses_and_leaves_the_child_alone(self, kids, forked):
        with pytest.raises(ZFSForkError, match='cannot be read from a forked process'):
            ReceiveCommand._exec_stream_in(py('import sys\nsys.stdin.buffer.read()\n'))
        process = kids.last
        assert process.poll() is None, 'the failed-start cleanup killed the parent\'s child'
        assert not process.stdin.closed and not process.stderr.closed
        assert zfs_module._stuck == []

    def test_exec_stream_without_peek_refuses_before_building_the_handle(self, kids, forked):
        with pytest.raises(ZFSForkError, match='cannot be read from a forked process'):
            SendCommand._exec_stream(py(HELLO_THEN_SLEEP), peek=False)
        process = kids.last
        assert process.poll() is None, 'the failed-start cleanup killed the parent\'s child'
        assert not process.stdout.closed and not process.stderr.closed
        assert zfs_module._stuck == []

    def test_exec_capture_bin_with_a_foreign_owner_touches_nothing(self):
        """Spies: the peek (which takes the parent reader's buffer lock), `poll()`, `wait()` and every close are never
        reached; a fork's `poll()` would be an ECHILD "clean exit" and its close could wait for ever on a lock."""
        process = FakeProcess(stdout=FakeStream())
        process.args = ['zfs', 'send', 'pool/a@s']
        with pytest.raises(ZFSForkError, match='zfs send: .*cannot be read from a forked process'):
            SendCommand._exec_capture_bin(process, [], FakeThread(alive=False), TestForkedGuardUnit.OTHER)
        assert process.stdout.io_calls == [] and process.calls == []
        assert process.stdout.raw.close_calls == 0 and process.stderr.close_calls == 0

    def test_exec_capture_bin_without_an_owner_is_unchanged(self):
        """`owner` is optional: without it (the old signature) the peek decides, as before."""
        from unittest import mock
        process = mock.MagicMock()
        process.stdout.peek.return_value = b'data'
        assert SendCommand._exec_capture_bin(process, []) is process.stdout
        process = mock.MagicMock()
        process.stdout.peek.return_value = b''
        process.poll.return_value = 0
        assert SendCommand._exec_capture_bin(process, [], None) is False

    def test_the_owner_is_unaffected(self):
        """The same calls in the spawning process (no patch) work as before."""
        with SendCommand._exec_stream(py(BINARY)) as handle:
            assert handle.read() == b'x' * 100
        handle = ReceiveCommand._exec_stream_in(py('import sys\nsys.stdin.buffer.read()\n'))
        handle.write(b'data')
        handle.close()


class TestOwnerIsTakenBeforeTheSpawn:
    """The spawner's pid is read BEFORE `Popen` in all four cores, so there is no gap between a child existing and its
    owner being known. The module's `os` is replaced by one whose `getpid()` answers differently from the moment the
    spawn has returned (as if a fork happened right after it): a core that read the pid after the spawn would take the
    new answer as its own and carry on, one that read it before sees another process and refuses."""

    @pytest.mark.parametrize('core', ['lines', 'collect', 'stream', 'stream_in'])
    def test_a_pid_change_right_after_the_spawn_is_seen_as_a_fork(self, kids, monkeypatch, core):
        spawned = []
        real_os = zfs_module.os

        class OsAfterAFork:
            def __getattr__(self, name):
                return getattr(real_os, name)

            @staticmethod
            def getpid():
                return real_os.getpid() + (1 if spawned else 0)

        recording = subprocess.Popen                     # the `kids` recording class

        class Marking(recording):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                spawned.append(self)

        monkeypatch.setattr(subprocess, 'Popen', Marking)
        monkeypatch.setattr(zfs_module, 'os', OsAfterAFork())
        with pytest.raises(ZFSForkError):
            if core == 'lines':
                list(Command._exec_lines(py(LINES)))
            elif core == 'collect':
                Command._exec_collect(py(LINES))
            elif core == 'stream':
                SendCommand._exec_stream(py(BINARY))
            else:
                ReceiveCommand._exec_stream_in(py('import sys\nsys.stdin.buffer.read()\n'))
        assert len(spawned) == 1


# One scenario per run, in a fresh interpreter: a fork of the pytest process (threads, plugins, a coverage tracer) is
# exactly what must not happen. The driver forks, lets the fork do something with an inherited generator / a call in
# progress, and reports as JSON lines. Everything is bounded: the parent by a 90 s alarm, a forked child by its own 10 s
# alarm (reported as "killed by signal 14", never waited for), and the test kills the whole process group on a timeout.
#
# The sixth argument, `linux`, makes macOS run what only Linux used to reach: a timed read polls through a selector,
# and a kqueue (macOS) is closed in a fork while an epoll (Linux) survives it, so the fork's resumed read went on and
# `Popen.poll()` there (ECHILD, which CPython reports as return code 0) looked like a clean exit. `linux=1` swaps in a
# selector that survives the fork (`SelectSelector`) and makes `poll()`/`wait()` answer 0 in the fork, so it is the
# `ZFSForkError` guard, not the platform, that stops the fork.
FORK_DRIVER = r'''
import contextlib, gc, json, os, signal, subprocess, sys, time, warnings
sys.path.insert(0, sys.argv[1])
mode, timed, action = sys.argv[2], sys.argv[3] == '1', sys.argv[4]
linux = len(sys.argv) > 5 and sys.argv[5] == '1'
import selectors
import libzfseasy
from libzfseasy import zfs as Z
if linux:
    selectors.DefaultSelector = selectors.SelectSelector      # a selector that survives the fork, as epoll does
MAIN = os.getpid()

signal.alarm(90)
STUB = [sys.executable, '-c',
        'import sys, time\n'
        'print("a", flush=True)\n'
        'time.sleep(%s)\n'
        'print("b", flush=True)\n'
        'time.sleep(0.2)\n' % ('60' if action == 'expire' else '0.8')]
procs = []
real_popen = subprocess.Popen


class Recording(real_popen):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        procs.append(self)

    def poll(self):
        if linux and os.getpid() != MAIN:
            self.returncode = 0                  # what CPython does for the ECHILD a fork gets for the parent's child
            return 0
        return super().poll()

    def wait(self, timeout=None):
        if linux and os.getpid() != MAIN:
            self.returncode = 0
            return 0
        return super().wait(timeout=timeout)


subprocess.Popen = Recording
report = {}


def say(who, **fields):
    print(who + ' ' + json.dumps(fields), flush=True)


def child_status(pid):
    _, status = os.waitpid(pid, 0)
    return -os.WTERMSIG(status) if os.WIFSIGNALED(status) else os.WEXITSTATUS(status)


with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always', ResourceWarning)
    context = libzfseasy.timeout(1.0 if action == 'expire' else 30) if timed else contextlib.nullcontext()
    with context:
        if mode == 'lines':
            gen = Z.Command._exec_lines(STUB)
            first = next(gen)
            time.sleep(0.4)          # the stderr reader is now blocked in readline, holding the pipe's buffer lock
            pid = os.fork()
            if pid == 0:
                signal.alarm(10)
                out = {'first': first}
                if linux:                        # the premise: a fork's own poll() of the parent's child says 0
                    out['native_poll'] = real_popen.poll(procs[0])
                try:
                    if action == 'finish':
                        out['rest'] = list(gen)
                    elif action == 'close':
                        gen.close()
                    elif action == 'drop':
                        del gen
                        gc.collect()
                    elif action == 'throw':
                        try:
                            gen.throw(KeyboardInterrupt())
                        except KeyboardInterrupt:
                            out['raised'] = True
                    elif action == 'expire':
                        try:
                            list(gen)
                        except Z.ZFSTimeoutError as exc:
                            out['raised'] = type(exc).__name__
                    out['ok'] = True
                except BaseException as exc:
                    out['error'] = type(exc).__name__ + ': ' + str(exc)
                say('CHILD', **out)
                os._exit(0)
            report['child_exit'] = child_status(pid)
            proc = procs[0]
            report['stub_alive_after_child'] = proc.poll() is None
            if action == 'expire':
                gen.close()                      # the original process still cleans up after itself
            else:
                try:
                    report['rest'] = list(gen)
                except BaseException as exc:
                    report['error'] = type(exc).__name__ + ': ' + str(exc)
        else:                                    # _exec_collect, forked while it is running
            forked = []

            def hook(cls, cmd, pid):
                time.sleep(0.4)
                pid = os.fork()
                if pid == 0:
                    signal.alarm(10)
                    if action == 'continue':
                        return None              # the fork carries on with the call (a signal handler that forked)
                    raise KeyboardInterrupt      # the fork's copy of the call is interrupted
                forked.append(pid)

            Z._SlowCommandWatchdog.for_command = classmethod(hook)
            try:
                lines, errors, rc = Z.Command._exec_collect(STUB, raw_stderr=(action == 'raw'))
            except KeyboardInterrupt:
                say('CHILD', interrupted=True)
                os._exit(0)
            except BaseException as exc:
                if os.getpid() == MAIN:
                    raise
                say('CHILD', error=type(exc).__name__ + ': ' + str(exc))
                os._exit(0)
            if os.getpid() != MAIN:              # the fork got a result: it must never go on as the parent
                say('CHILD', returned=True, lines=lines, rc=rc)
                os._exit(0)
            report['lines'], report['rc'] = lines, rc
            report['child_exit'] = child_status(forked[0])
            proc = procs[0]
    gc.collect()
    report['returncode'] = proc.returncode
    report['pipes_closed'] = bool(proc.stdout.closed and proc.stderr.closed)
    report['warnings'] = [str(w.message) for w in caught if issubclass(w.category, ResourceWarning)]
say('PARENT', **report)
'''


def run_driver(source, args, label):
    """Run a fork driver in a fresh interpreter (`argv[1]` = the libzfseasy import dir, then `args`); returns
    (child_report, parent_report). Fails (never hangs) if it takes over 100 s; the whole process group is killed after."""
    libs_dir = os.path.dirname(os.path.dirname(os.path.abspath(libzfseasy.__file__)))
    env = hermetic_env()
    proc = subprocess.Popen([sys.executable, '-c', source, libs_dir, *args],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
                            start_new_session=True)
    try:
        out, err = proc.communicate(timeout=100)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.communicate()
        pytest.fail(f'fork scenario {label} did not finish within 100 s (a close or wait in the fork is wedged)')
    finally:
        try:
            os.killpg(proc.pid, signal.SIGKILL)        # nothing the scenario started may outlive it
        except ProcessLookupError:
            pass
    assert proc.returncode == 0, f'driver exit {proc.returncode}\nstdout: {out}\nstderr: {err}'
    reports = {}
    for line in out.splitlines():
        who, _, payload = line.partition(' ')
        if who in ('CHILD', 'PARENT'):
            reports[who] = json.loads(payload)
    assert 'PARENT' in reports, f'no parent report\nstdout: {out}\nstderr: {err}'
    return reports.get('CHILD'), reports['PARENT']


def run_fork_scenario(mode, timed, action, linux=False):
    """Run FORK_DRIVER; returns (child_report, parent_report). `linux=True`: see the comment above the driver."""
    return run_driver(FORK_DRIVER, [mode, '1' if timed else '0', action, '1' if linux else '0'],
                      f'{mode}/{timed}/{action}/{"linux" if linux else "native"}')


LINUX_PATH = pytest.mark.parametrize('linux', [False, True], ids=['native', 'linux-path'])


needs_fork = pytest.mark.skipif(not hasattr(os, 'fork'), reason='needs os.fork')


@needs_fork
class TestForkLeavesTheParentsChildAlone:
    """`os.fork()` while a call is in flight: the fork inherits the suspended generator, the `Popen` and its pipes, but
    the child is the parent's. The fork's cleanup must not close a pipe whose buffer lock the (not forked) stderr reader
    thread held (that wedges for ever), and must not signal or reap the parent's child; resuming the generator there
    raises `ZFSForkError` (a fork cannot wait for the child: its `poll()` says ECHILD, i.e. a clean exit, which used to
    end a truncated listing as if it were complete). The original process must still get its complete result and close
    its own pipes. The `linux-path` variants run what only Linux used to reach (see the comment above `FORK_DRIVER`)."""

    @pytest.mark.parametrize('timed', [False, True], ids=['untimed', 'timed'])
    @pytest.mark.parametrize('action', ['close', 'drop', 'throw'])
    def test_a_fork_that_abandons_the_generator_neither_hangs_nor_kills_the_child(self, timed, action):
        child, parent = run_fork_scenario('lines', timed, action)
        assert child['ok'] is True and 'error' not in child, child
        assert parent['child_exit'] == 0, 'the fork must end on its own, not by the alarm'
        assert parent['stub_alive_after_child'] is True, 'the fork signalled or reaped the parent\'s child'
        assert parent['rest'] == ['b'], 'the parent still gets the rest of its output'
        assert parent['returncode'] == 0
        assert parent['pipes_closed'] is True
        assert parent['warnings'] == []

    @LINUX_PATH
    @pytest.mark.parametrize('timed', [False, True], ids=['untimed', 'timed'])
    def test_a_fork_that_resumes_the_generator_gets_zfs_fork_error_and_takes_no_lines(self, timed, linux):
        """The fork must neither read the shared pipe (taking the parent's lines) nor treat the ECHILD `poll()` as a
        clean exit and end the listing: it raises `ZFSForkError` and gets nothing, while the original process still
        gets the rest of its output, a clean exit and closed pipes."""
        child, parent = run_fork_scenario('lines', timed, 'finish', linux)
        assert child.get('error', '').startswith('ZFSForkError:'), child
        assert 'rest' not in child and child.get('ok') is not True, child
        if linux:
            assert child['native_poll'] == 0, 'premise: a fork\'s poll() of the parent\'s child reports a clean exit'
        assert parent['child_exit'] == 0, 'the fork must end on its own, not by the alarm'
        assert parent['stub_alive_after_child'] is True, 'the fork signalled or reaped the parent\'s child'
        assert parent.get('error') is None, parent
        assert parent['rest'] == ['b'], 'the parent still gets the rest of its output'
        assert parent['returncode'] == 0
        assert parent['pipes_closed'] is True
        assert parent['warnings'] == []

    @LINUX_PATH
    def test_a_timed_expiry_in_the_fork_does_not_kill_the_parents_child(self, linux):
        """The fork resumes the timed generator and is refused with `ZFSForkError` on every OS (before, Linux ran
        `_expire` there and kqueue made the read fail first); the parent's child must be left running, and the parent
        then cleans up after itself."""
        child, parent = run_fork_scenario('lines', True, 'expire', linux)
        assert child.get('error', '').startswith('ZFSForkError:'), child
        assert parent['child_exit'] == 0, 'the fork must end on its own, not by the alarm'
        assert parent['stub_alive_after_child'] is True, 'the fork must not terminate the parent\'s child'
        assert parent['returncode'] is not None, 'the parent\'s own close ends the child'
        assert parent['pipes_closed'] is True
        assert parent['warnings'] == []

    @pytest.mark.parametrize('action', ['plain', 'raw'])
    def test_exec_collect_interrupted_in_the_fork_neither_hangs_nor_kills_the_child(self, action):
        child, parent = run_fork_scenario('collect', False, action)
        assert child == {'interrupted': True}
        assert parent['child_exit'] == 0
        assert parent['lines'] == ['a', 'b'] and parent['rc'] == 0, 'the parent\'s call must complete normally'
        assert parent['pipes_closed'] is True
        assert parent['warnings'] == []

    @LINUX_PATH
    def test_exec_collect_continued_in_the_fork_raises_zfs_fork_error(self, linux):
        """The watchdog hook forks and RETURNS in the fork, so `_exec_collect` goes on there: it must raise
        `ZFSForkError` instead of reading the parent's output and taking an ECHILD `wait()` for a clean exit, while the
        parent's call completes normally."""
        child, parent = run_fork_scenario('collect', False, 'continue', linux)
        assert child.get('error', '').startswith('ZFSForkError:'), child
        assert 'returned' not in child, child
        assert parent['child_exit'] == 0
        assert parent['lines'] == ['a', 'b'] and parent['rc'] == 0, 'the parent\'s call must complete normally'
        assert parent['pipes_closed'] is True
        assert parent['warnings'] == []


# A `StreamHandle` forked while it is open. The child is the creator's, so in the fork: `poll()`/`terminate()` raise
# `ZFSForkError` and signal and wait for nothing; `read`/`write`/`peek`/`flush`/`fileno`/`raw` raise it too; `close()`
# closes the fork's own descriptor copies of the stream at the descriptor (no flush, no lock) and then raises it. The
# creator's `poll()`/`close()` still work, and, because the fork closed its copies, the creator's `close()` does not
# wait for a fork that is still ALIVE (a copy of a receive's write end left open in it would keep `zfs receive` from
# seeing EOF; a copy of a send's read end would keep `zfs send` from getting EPIPE). Modes: `read` = `_exec_stream`
# (a `send`) read to its end; `write` = `_exec_stream_in` (a `receive`), 4 bytes buffered (not yet in the pipe) at the
# fork, so a fork that flushed them while closing would deliver them twice; `epipe` = a send that never stops writing,
# closed by the creator while the fork lives on; `write_gc` = the write case where the fork never calls `close()` and
# just drops its references (garbage collection of the unflushed writer). Bounds: a 90 s alarm in the creator, 30 s in the fork, a 15 s timer
# that kills the child of the creator's blocking close(), the process group killed afterwards.
STREAM_FORK_DRIVER = r"""
import gc, json, os, signal, sys, threading, time
sys.path.insert(0, sys.argv[1])
mode, outfile = sys.argv[2], sys.argv[3]
from libzfseasy import zfs as Z

signal.alarm(90)


def say(who, **fields):
    print(who + ' ' + json.dumps(fields), flush=True)


if mode == 'read':
    handle = Z.SendCommand._exec_stream([sys.executable, '-c',
        'import sys, time\n'
        'sys.stdout.buffer.write(b"hello"); sys.stdout.buffer.flush()\n'
        'time.sleep(2)\n'
        'sys.stdout.buffer.write(b" world")\n'])
elif mode == 'epipe':
    handle = Z.SendCommand._exec_stream([sys.executable, '-c',
        'import sys\n'
        'while True:\n'
        '    sys.stdout.buffer.write(b"x" * 65536); sys.stdout.buffer.flush()\n'])
    handle.read(10)
else:
    handle = Z.ReceiveCommand._exec_stream_in([sys.executable, '-c',
        'import sys\n'
        'data = sys.stdin.buffer.read()\n'
        'open(%r, "wb").write(data)\n' % outfile])
    handle.write(b'AAAA')                      # buffered in the writer, not in the pipe yet
proc = handle._process
streams = [p for p in (proc.stdin, proc.stdout) if p is not None]

report_r, report_w = os.pipe()                  # fork -> creator: its report
go_r, go_w = os.pipe()                          # creator -> fork: "you may exit now"
pid = os.fork()
if pid == 0:
    signal.alarm(30)
    os.close(report_r)
    os.close(go_w)
    if mode == 'write_gc':                      # the fork lets go of the handle without closing it
        signal.alarm(30)
        del handle, proc, streams
        gc.collect()
        os.write(report_w, b'{}')
        os.close(report_w)
        os.read(go_r, 1)
        os._exit(0)

    def attempt(fn):
        try:
            fn()
            return 'returned'
        except Z.ZFSForkError:
            return 'ZFSForkError'
        except BaseException as exc:
            return type(exc).__name__

    out = {'status_attr': bool(handle.closed) is False and handle.pid == proc.pid}
    out['read'] = attempt(lambda: handle.read(1))
    out['write'] = attempt(lambda: handle.write(b'XXXX'))
    out['peek'] = attempt(lambda: handle.peek(1))
    out['flush'] = attempt(lambda: handle.flush())
    out['fileno'] = attempt(lambda: handle.fileno())
    out['raw'] = attempt(lambda: handle.raw)
    out['poll'] = attempt(handle.poll)
    out['terminate'] = attempt(handle.terminate)
    out['streams_open_before_close'] = all(not p.closed for p in streams)
    out['close'] = attempt(handle.close)
    out['streams_closed_after_close'] = all(p.closed for p in streams)
    out['stderr_closed'] = bool(proc.stderr.closed)
    out['handle_closed'] = bool(handle._closed)
    os.write(report_w, json.dumps(out).encode())
    os.close(report_w)
    os.read(go_r, 1)                            # stay ALIVE until the creator is done with its own close()
    os._exit(0)

os.close(report_w)
os.close(go_r)
raw = b''
while True:
    chunk = os.read(report_r, 65536)
    if not chunk:
        break
    raw += chunk
say('CHILD', **json.loads(raw.decode()))

report = {'alive_after_fork': handle.poll() is None}   # the creator's poll() still works and sees it running
backstop = {'fired': False}


def kill_child():
    backstop['fired'] = True
    proc.kill()


timer = threading.Timer(15, kill_child)
timer.start()
started = time.monotonic()
try:
    if mode == 'read':
        report['data'] = handle.read().decode()
    elif mode in ('write', 'write_gc'):
        handle.write(b'BBBB')
    handle.close()                              # the fork is still alive: this must not depend on it
    report['close'] = 'ok'
except BaseException as exc:
    report['close'] = type(exc).__name__ + ': ' + str(exc)
timer.cancel()
report['close_seconds'] = round(time.monotonic() - started, 2)
report['backstop_fired'] = backstop['fired']
report['returncode'] = proc.returncode
report['streams_closed'] = all(p.closed for p in streams)
if mode in ('write', 'write_gc'):
    report['received'] = open(outfile, 'rb').read().decode()
os.write(go_w, b'x')
_, status = os.waitpid(pid, 0)
report['child_exit'] = -os.WTERMSIG(status) if os.WIFSIGNALED(status) else os.WEXITSTATUS(status)
say('PARENT', **report)
"""


@needs_fork
class TestStreamHandleInARealFork:
    """The fix's `StreamHandle` half over real forks (the child survives the fork, real pipes, the fork stays alive while
    the creator closes its handle)."""

    FORK_REFUSES = {'status_attr': True, 'read': 'ZFSForkError', 'write': 'ZFSForkError', 'peek': 'ZFSForkError',
                    'flush': 'ZFSForkError', 'fileno': 'ZFSForkError', 'raw': 'ZFSForkError',
                    'poll': 'ZFSForkError', 'terminate': 'ZFSForkError', 'streams_open_before_close': True,
                    'close': 'ZFSForkError', 'streams_closed_after_close': True, 'stderr_closed': False,
                    'handle_closed': False}

    @pytest.mark.parametrize('mode', ['read', 'write'])
    def test_a_fork_cannot_use_the_handle_and_the_creator_still_can(self, mode, tmp_path):
        child, parent = run_driver(STREAM_FORK_DRIVER, [mode, str(tmp_path / 'received')], f'stream/{mode}')
        assert child == self.FORK_REFUSES, child
        assert parent['child_exit'] == 0, 'the fork must end on its own, not by the alarm'
        assert parent['alive_after_fork'] is True, 'the fork signalled or reaped the creator\'s child'
        assert parent['close'] == 'ok', parent
        assert parent['backstop_fired'] is False and parent['close_seconds'] < 10, parent
        assert parent['returncode'] == 0
        assert parent['streams_closed'] is True
        if mode == 'read':
            assert parent['data'] == 'hello world', 'the fork must not take the creator\'s bytes'
        else:
            assert parent['received'] == 'AAAABBBB', 'the fork must neither write nor flush into the shared pipe'

    def test_a_send_still_gets_epipe_when_the_creator_closes_while_a_fork_lives(self, tmp_path):
        """The fork's copy of the read end was closed by its `close()`: once the creator closes its own, the child sees
        EPIPE and fails, instead of blocking on a full pipe for as long as the fork lives."""
        child, parent = run_driver(STREAM_FORK_DRIVER, ['epipe', str(tmp_path / 'received')], 'stream/epipe')
        assert child == self.FORK_REFUSES, child
        assert parent['backstop_fired'] is False, 'the send child never got EPIPE: the fork kept its pipe open'
        assert parent['close'] != 'ok' and parent['returncode'] not in (0, None), parent
        assert parent['child_exit'] == 0

    @pytest.mark.xfail(strict=True, raises=AssertionError,
                       reason='follow-up: at-fork dup2 hook (see the StreamHandle docstring, "Not covered")')
    def test_a_fork_that_drops_the_unflushed_handle_does_not_flush_into_the_creators_receive(self, tmp_path):
        """KNOWN GAP, pinned: a fork that never calls `close()` and lets its copy of a receive's unflushed
        `BufferedWriter` be garbage collected flushes the buffered bytes into the shared pipe, so the receive gets them
        twice. Fixed by an `os.register_at_fork(after_in_child=...)` hook; when that lands this test XPASSes (strict)
        and the marker is to be removed."""
        child, parent = run_driver(STREAM_FORK_DRIVER, ['write_gc', str(tmp_path / 'received')], 'stream/write_gc')
        assert parent['close'] == 'ok' and parent['backstop_fired'] is False, parent
        assert parent['received'] == 'AAAABBBB', parent['received']

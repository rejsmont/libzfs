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
from libzfseasy.zfs import Command, ReceiveCommand, SendCommand, StreamHandle, ZFSTimeoutError

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


class TestForkedCleanupInProcess:
    """The callers' own `_forked` branches, without a fork: `Command._forked` is made to report "another process" and a
    real child is run through each exit. A real fork cannot tell these branches from the helpers' own guards (a forked
    `Popen.poll()` finds no such child and reports an exit by itself), so this is where they are pinned: nothing is
    signalled, waited for, joined, registered or closed."""

    @pytest.fixture
    def foreign(self, kids, monkeypatch):
        monkeypatch.setattr(Command, '_forked', staticmethod(lambda owner: True))
        calls = []
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
        lines = Command._exec_lines(py(HELLO_THEN_SLEEP))
        next(lines)
        lines.close()
        self.assert_untouched(kids.last, foreign)

    def test_a_timed_exec_lines_generator_closed_early_touches_nothing(self, kids, foreign):
        with libzfseasy.timeout(30):
            lines = Command._exec_lines(py(HELLO_THEN_SLEEP))
            next(lines)
            lines.close()
        self.assert_untouched(kids.last, foreign)

    def test_an_exception_raised_into_the_generator_touches_nothing(self, kids, foreign):
        lines = Command._exec_lines(py(HELLO_THEN_SLEEP))
        next(lines)
        with pytest.raises(KeyboardInterrupt):
            lines.throw(KeyboardInterrupt())
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
        """`_exec_lines` passes the spawner's pid to `_expire`: `_forked` is made true only for a recorded owner, so a
        call that forgot to pass it would reap (kill) the child here. (A real fork reaches this on Linux only: a kqueue
        does not survive a fork on macOS.)"""
        monkeypatch.setattr(Command, '_forked', staticmethod(lambda owner: owner is not None))
        with libzfseasy.timeout(0.5):
            with pytest.raises(ZFSTimeoutError):
                drain(py(HELLO_THEN_SLEEP))
        process = kids.last
        assert process.poll() is None, 'the expiry killed the child although the process is not its owner'
        assert not process.stdout.closed and not process.stderr.closed
        assert zfs_module._stuck == []


# One scenario per run, in a fresh interpreter: a fork of the pytest process (threads, plugins, a coverage tracer) is
# exactly what must not happen. The driver forks, lets the fork do something with an inherited generator / a call in
# progress, and reports as JSON lines. Everything is bounded: the parent by a 90 s alarm, a forked child by its own 10 s
# alarm (reported as "killed by signal 14", never waited for), and the test kills the whole process group on a timeout.
FORK_DRIVER = r'''
import contextlib, gc, json, os, signal, subprocess, sys, time, warnings
sys.path.insert(0, sys.argv[1])
mode, timed, action = sys.argv[2], sys.argv[3] == '1', sys.argv[4]
import libzfseasy
from libzfseasy import zfs as Z

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
                    raise KeyboardInterrupt      # the fork's copy of the call is interrupted
                forked.append(pid)

            Z._SlowCommandWatchdog.for_command = classmethod(hook)
            try:
                lines, errors, rc = Z.Command._exec_collect(STUB, raw_stderr=(action == 'raw'))
            except KeyboardInterrupt:
                say('CHILD', interrupted=True)
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


def run_fork_scenario(mode, timed, action):
    """Run FORK_DRIVER; returns (child_report, parent_report). Fails (never hangs) if it takes over 100 s."""
    libs_dir = os.path.dirname(os.path.dirname(os.path.abspath(libzfseasy.__file__)))
    env = hermetic_env()
    proc = subprocess.Popen([sys.executable, '-c', FORK_DRIVER, libs_dir, mode, '1' if timed else '0', action],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
                            start_new_session=True)
    try:
        out, err = proc.communicate(timeout=100)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.communicate()
        pytest.fail(f'fork scenario {mode}/{timed}/{action} did not finish within 100 s (a close or wait in the fork '
                    f'is wedged)')
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


needs_fork = pytest.mark.skipif(not hasattr(os, 'fork'), reason='needs os.fork')


@needs_fork
class TestForkLeavesTheParentsChildAlone:
    """`os.fork()` while a call is in flight: the fork inherits the suspended generator, the `Popen` and its pipes, but
    the child is the parent's. The fork's cleanup must not close a pipe whose buffer lock the (not forked) stderr reader
    thread held (that wedges for ever), and must not signal or reap the parent's child. The original process must still
    get its complete result and close its own pipes."""

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

    @pytest.mark.parametrize('timed', [False, True], ids=['untimed', 'timed'])
    def test_a_fork_that_finishes_the_generator_does_not_hang(self, timed):
        """The fork reads the shared pipe to its end (and so takes the remaining lines: one pipe, two readers); what
        matters is that it ends, and that the original process still ends cleanly with its pipes closed. A timed read
        polls through a selector, and a kqueue (macOS, BSD) is not inherited by a fork: there the fork's resumed read
        fails with a ValueError of its own, which ends it just as well."""
        child, parent = run_fork_scenario('lines', timed, 'finish')
        if child.get('ok'):
            assert child['rest'] == ['b'], child
        else:
            assert timed and 'closed' in child['error'], child
        assert parent['child_exit'] == 0, 'the fork must end on its own, not by the alarm'
        assert parent.get('error') is None, parent
        assert parent['pipes_closed'] is True
        assert parent['warnings'] == []

    def test_a_timed_expiry_in_the_fork_does_not_kill_the_parents_child(self):
        """The fork resumes the timed generator past its deadline: `_expire` runs there (where the selector survives the
        fork, as on Linux) and must leave the parent's child alone; where it does not (kqueue), the read fails first.
        Either way the fork ends, the parent's child is still running, and the parent then cleans up after itself."""
        child, parent = run_fork_scenario('lines', True, 'expire')
        assert child.get('raised') == 'ZFSTimeoutError' or 'closed' in child.get('error', ''), child
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

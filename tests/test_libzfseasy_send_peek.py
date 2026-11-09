"""`peek=False` on the send entry points (`Command._exec_stream(cmd, *, peek=True)` and `SendCommand.snapshot/dataset/
redact/resume/partial`, reached also through `zfs.send(...)`).

With `peek=True` (the default, unchanged) `_exec_stream` blocks in `process.stdout.peek(8)` until the first byte or EOF, so a
zfs that fails at once raises from the call. With `peek=False` nothing is read: the `StreamHandle` is returned right after
the stderr reader starts, an immediate failure shows as EOF on the stream, and `close()` raises the usual bare Exception
built from stderr (or the exit status). terminate/poll/close and the stuck-child registry are unchanged.

Layers: mocked Popen (peek call count, argv, signature); real hermetic children (`sh -c 'exec ...'` stubs started through the
real `_exec_stream`, and a stub `zfs` script behind `ZFS_CMD` for the public entry points) for the behaviour. Every real child
is reaped or killed by the end of its test; no real zfs is ever started.
"""

import inspect
import os
import select
import signal
import subprocess
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

import libzfseasy
from libzfseasy import zfs as zfs_module
from libzfseasy.types import Bookmark, Filesystem, Snapshot
from libzfseasy.zfs import SendCommand, StreamHandle

pytestmark = [pytest.mark.unit, pytest.mark.subprocess]


def sh(script):
    """A hermetic child: `sh -c <script>` (scripts `exec` their final command so signals reach the real program)."""
    return ['/bin/sh', '-c', script]


FAIL_QUIETLY = 'exit 3'
FAIL_WITH_STDERR = 'echo "cannot send tank/fs@a: boom" >&2; exit 1'
EMPTY_OK = 'exit 0'
BLOCK_BEFORE_WRITING = 'exec sleep 120'


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


@pytest.fixture(autouse=True)
def clean_stuck_registry():
    """`_stuck` is module state: every test starts with it empty and the original entries (and lock) are put back."""
    saved_lock = zfs_module._stuck_lock
    saved_entries = list(zfs_module._stuck)
    zfs_module._stuck.clear()
    yield
    zfs_module._stuck_lock = saved_lock
    zfs_module._stuck[:] = saved_entries


@pytest.fixture
def kids(monkeypatch):
    k = Kids(monkeypatch)
    yield k
    k.kill_all()


@pytest.fixture
def quick(monkeypatch):
    """Short TERM/KILL waits."""
    monkeypatch.setattr(zfs_module, '_TERMINATE_TIMEOUT', 0.3)


@pytest.fixture
def stub_zfs(tmp_path, monkeypatch):
    """`stub_zfs(body)`: point `ZFS_CMD` at a shell script (a `#!` script is not a "real zfs" to the conftest guard) that
    appends its argv to `argv.txt` and then runs `body`. Returns the argv file."""
    argv_file = tmp_path / 'argv.txt'

    def make(body):
        script = tmp_path / 'zfs'
        script.write_text(f'#!/bin/sh\necho "$@" >> {argv_file}\n{body}\n')
        script.chmod(0o755)
        monkeypatch.setenv('ZFS_CMD', str(script))
        return argv_file

    return make


def run_in_thread(fn):
    box = {}

    def target():
        try:
            box['result'] = fn()
        except BaseException as exc:   # noqa: BLE001 - reported to the test
            box['exc'] = exc

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread, box


def join_or_fail(thread, limit=30):
    thread.join(limit)
    assert not thread.is_alive(), f'thread did not finish within {limit}s'


FS = Filesystem('testpool/filesystem')
SNAP1 = Snapshot(FS, 'snap1')
SNAP2 = Snapshot(FS, 'snap2')
BOOKMARK = Bookmark(FS, 'bm1')

# name -> (callable taking **kwargs, expected sub-argv present in the Popen argv)
ENTRY_POINTS = {
    'snapshot': (lambda **kw: SendCommand.snapshot(SNAP2, since=SNAP1, **kw), '-i'),
    'dataset': (lambda **kw: SendCommand.dataset(FS, since=SNAP1, **kw), '-i'),
    'redact': (lambda **kw: SendCommand.redact(SNAP2, BOOKMARK, **kw), '--redact'),
    'resume': (lambda **kw: SendCommand.resume('1-abc_def-xyz', **kw), '-t'),
    'partial': (lambda **kw: SendCommand.partial(FS, **kw), '-S'),
    'call_snapshot': (lambda **kw: libzfseasy.send(SNAP2, **kw), 'testpool/filesystem@snap2'),
    'call_dataset': (lambda **kw: libzfseasy.send(FS, **kw), 'testpool/filesystem'),
}
ENTRY_IDS = list(ENTRY_POINTS)


def mocked_process(peek=b'ZFS_DATA'):
    process = MagicMock()
    process.stdout.peek.return_value = peek
    process.poll.return_value = None
    process.wait.return_value = 0
    return process


# ---------------------------------------------------------------------------------------------------------------------
# (1) mocked Popen: no peek, immediate handle, keyword-only, same argv, default still peeks
# ---------------------------------------------------------------------------------------------------------------------

class TestMockedEntryPoints:

    @pytest.mark.parametrize('name', ENTRY_IDS)
    def test_peek_false_never_peeks_and_returns_a_handle_at_once(self, mock_subprocess, name):
        call, _ = ENTRY_POINTS[name]
        process = mocked_process()
        mock_subprocess.return_value = process

        handle = call(peek=False)

        assert isinstance(handle, StreamHandle)
        process.stdout.peek.assert_not_called()
        process.poll.assert_not_called()          # no early-failure capture either
        process.wait.assert_not_called()
        assert handle._process is process and handle._stream is process.stdout
        assert mock_subprocess.call_count == 1

    @pytest.mark.parametrize('name', ENTRY_IDS)
    def test_the_default_still_peeks(self, mock_subprocess, name):
        call, _ = ENTRY_POINTS[name]
        process = mocked_process()
        mock_subprocess.return_value = process

        handle = call()

        assert isinstance(handle, StreamHandle)
        process.stdout.peek.assert_called_once_with(8)

    @pytest.mark.parametrize('name', ENTRY_IDS)
    def test_peek_true_is_the_same_as_the_default(self, mock_subprocess, name):
        call, _ = ENTRY_POINTS[name]
        process = mocked_process()
        mock_subprocess.return_value = process
        call(peek=True)
        process.stdout.peek.assert_called_once_with(8)

    @pytest.mark.parametrize('name', ENTRY_IDS)
    def test_argv_and_popen_kwargs_are_identical_in_both_modes(self, mock_subprocess, name):
        call, marker = ENTRY_POINTS[name]
        mock_subprocess.return_value = mocked_process()
        call()
        default_call = mock_subprocess.call_args
        mock_subprocess.reset_mock()
        mock_subprocess.return_value = mocked_process()
        call(peek=False)
        no_peek_call = mock_subprocess.call_args

        assert default_call == no_peek_call
        assert marker in default_call[0][0]
        assert 'peek' not in ' '.join(default_call[0][0])
        assert 'peek' not in default_call[1]

    @pytest.mark.parametrize('method', ['snapshot', 'dataset', 'redact', 'resume', 'partial'])
    def test_peek_is_keyword_only_with_default_true(self, method):
        parameter = inspect.signature(getattr(SendCommand, method)).parameters['peek']
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is True

    def test_exec_stream_peek_is_keyword_only_with_default_true(self):
        parameter = inspect.signature(SendCommand._exec_stream).parameters['peek']
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY and parameter.default is True
        with pytest.raises(TypeError):
            SendCommand._exec_stream(['zfs', 'send', 'x'], False)

    def test_peek_cannot_be_passed_positionally(self, mock_subprocess):
        mock_subprocess.return_value = mocked_process()
        with pytest.raises(TypeError):
            SendCommand.snapshot(SNAP2, SNAP1, False, False, False, False, False, False, False, False, False,
                                 False, False)          # a 13th positional would have been `peek`
        with pytest.raises(TypeError):
            SendCommand.dataset(FS, SNAP1, False, False, False, False, False)
        with pytest.raises(TypeError):
            SendCommand.redact(SNAP2, BOOKMARK, None, False, False, False, False, False)
        with pytest.raises(TypeError):
            SendCommand.resume('1-abc', False, False)
        with pytest.raises(TypeError):
            SendCommand.partial(FS, None, False)
        mock_subprocess.assert_not_called()

    def test_peek_false_still_validates_before_exec(self, mock_subprocess):
        with pytest.raises(libzfseasy.zfs.ResumeTokenError):
            SendCommand.resume('-bad', peek=False)
        mock_subprocess.assert_not_called()

    def test_peek_false_handle_closes_cleanly_like_any_other(self, mock_subprocess):
        process = mocked_process()
        process.wait.return_value = 0
        process.returncode = 0
        mock_subprocess.return_value = process
        with SendCommand.snapshot(SNAP2, peek=False) as stream:
            assert stream is process.stdout
        process.stdout.peek.assert_not_called()


# ---------------------------------------------------------------------------------------------------------------------
# (2) early failure with a real child
# ---------------------------------------------------------------------------------------------------------------------

class TestEarlyFailure:

    def test_no_peek_returns_a_handle_that_reads_eof_and_close_raises_with_the_exit_status(self, kids):
        handle = SendCommand._exec_stream(sh(FAIL_QUIETLY), peek=False)

        assert isinstance(handle, StreamHandle)
        assert handle.read() == b''
        with pytest.raises(Exception, match='^Command failed with exit code 3$'):
            handle.close()
        assert kids.procs[0].returncode == 3

    def test_no_peek_close_raises_the_childs_stderr(self, kids):
        handle = SendCommand._exec_stream(sh(FAIL_WITH_STDERR), peek=False)
        assert handle.read() == b''
        with pytest.raises(Exception) as info:
            handle.close()
        assert str(info.value) == 'Cannot send tank/fs@a: boom'
        assert type(info.value) is Exception            # the usual bare Exception
        assert kids.procs[0].returncode == 1

    def test_no_peek_context_manager_raises_on_exit(self, kids):
        handle = SendCommand._exec_stream(sh(FAIL_WITH_STDERR), peek=False)
        with pytest.raises(Exception, match='boom'):
            with handle as stream:
                assert stream.read() == b''

    def test_the_default_raises_from_the_call(self, kids):
        with pytest.raises(Exception) as info:
            SendCommand._exec_stream(sh(FAIL_WITH_STDERR))
        assert str(info.value) == 'Cannot send tank/fs@a: boom'
        assert kids.procs[0].returncode == 1
        with pytest.raises(Exception, match='^Command failed with exit code 3$'):
            SendCommand._exec_stream(sh(FAIL_QUIETLY))

    def test_a_successful_empty_stream_closes_cleanly_without_peek(self, kids):
        handle = SendCommand._exec_stream(sh(EMPTY_OK), peek=False)
        assert handle.read() == b''
        handle.close()
        assert kids.procs[0].returncode == 0
        assert handle.poll() == 0

    def test_a_successful_stream_with_data_without_peek(self, kids):
        handle = SendCommand._exec_stream(sh('printf data'), peek=False)
        with handle as stream:
            assert stream.read() == b'data'
        assert kids.procs[0].returncode == 0

    @pytest.mark.parametrize('name', ['snapshot', 'dataset', 'redact', 'resume', 'partial', 'call_snapshot',
                                      'call_dataset'])
    def test_public_entry_points_with_a_failing_stub_zfs(self, kids, stub_zfs, name):
        call, marker = ENTRY_POINTS[name]
        argv_file = stub_zfs('echo "cannot open: no such snapshot" >&2; exit 1')

        with pytest.raises(Exception, match='^Cannot open: no such snapshot$'):
            call()                                              # default: raised from the call
        handle = call(peek=False)                               # peek=False: a handle, the failure is in close()
        assert isinstance(handle, StreamHandle)
        assert handle.read() == b''
        with pytest.raises(Exception, match='^Cannot open: no such snapshot$'):
            handle.close()

        first, second = argv_file.read_text().splitlines()
        assert first == second and 'send' in first.split() and marker in first
        assert [p.returncode for p in kids.procs] == [1, 1]

    def test_public_entry_point_with_a_working_stub_zfs(self, kids, stub_zfs):
        stub_zfs('printf STREAM')
        with libzfseasy.send(SNAP2, peek=False) as stream:
            assert stream.read() == b'STREAM'
        with libzfseasy.send(SNAP2) as stream:
            assert stream.read() == b'STREAM'


# ---------------------------------------------------------------------------------------------------------------------
# (3) a child that blocks before writing
# ---------------------------------------------------------------------------------------------------------------------

class TestBlockingChild:

    def test_the_call_returns_at_once_while_the_default_would_block(self, kids):
        started = time.monotonic()
        handle = SendCommand._exec_stream(sh(BLOCK_BEFORE_WRITING), peek=False)
        elapsed = time.monotonic() - started
        try:
            assert elapsed < 5
            assert handle.poll() is None                # still running
            assert kids.procs[0].returncode is None
        finally:
            handle.terminate()
            with pytest.raises(Exception):
                handle.close()

    def test_the_default_blocks_until_the_child_speaks(self, kids):
        """The contrast: with the peek the call does not return while the child is silent."""
        thread, box = run_in_thread(lambda: SendCommand._exec_stream(sh('sleep 0.8; printf x; exec sleep 120')))
        time.sleep(0.3)
        assert thread.is_alive(), 'the default call should still be inside peek()'
        join_or_fail(thread)
        handle = box['result']
        handle.terminate()
        with pytest.raises(Exception):
            handle.close()

    def test_terminate_and_poll_work_on_a_no_peek_handle(self, kids):
        handle = SendCommand._exec_stream(sh(BLOCK_BEFORE_WRITING), peek=False)
        proc = kids.procs[0]
        assert isinstance(handle.pid, int) and handle.pid == proc.pid
        assert handle.poll() is None

        assert handle.terminate() is False

        assert proc.returncode == -signal.SIGTERM
        assert handle.poll() == -signal.SIGTERM
        assert handle.read() == b''                     # the pipe is at EOF
        with pytest.raises(Exception, match='^Command killed by signal SIGTERM$'):
            handle.close()
        with pytest.raises(ProcessLookupError):         # reaped
            os.kill(proc.pid, 0)

    def test_a_reader_blocked_on_the_silent_child_is_released_by_terminate(self, kids):
        handle = SendCommand._exec_stream(sh(BLOCK_BEFORE_WRITING), peek=False)
        thread, box = run_in_thread(handle.read)
        time.sleep(0.3)
        assert thread.is_alive()

        handle.terminate()

        join_or_fail(thread)
        assert box.get('result') == b''
        with pytest.raises(Exception, match='SIGTERM'):
            handle.close()

    def test_a_child_that_ignores_sigterm_is_killed(self, kids, quick):
        handle = SendCommand._exec_stream(sh('trap "" TERM; while :; do sleep 1; done'), peek=False)
        proc = kids.procs[0]
        time.sleep(0.2)
        assert handle.terminate() is False
        assert proc.returncode == -signal.SIGKILL
        with pytest.raises(Exception, match='SIGKILL'):
            handle.close()

    def test_a_child_that_survives_kill_is_registered_as_stuck(self, kids, quick, caplog):
        handle = SendCommand._exec_stream(sh(BLOCK_BEFORE_WRITING), peek=False)
        proc = kids.procs[0]

        def never_exits(timeout=None):
            raise subprocess.TimeoutExpired('zfs', timeout)

        try:
            with patch.object(proc, 'wait', side_effect=never_exits), patch.object(proc, 'poll', return_value=None):
                assert libzfseasy.stuck_children() == ()
                assert handle.terminate() is True           # survived SIGKILL (as far as the handle can tell)
                assert handle.terminate() is True           # sticky, nothing more is sent
                (child,) = libzfseasy.stuck_children()
                assert child.pid == proc.pid
                assert child.label == 'sh -c'                # the label is the command's first two words
                with pytest.raises(libzfseasy.zfs.ZFSTimeoutError, match=str(proc.pid)):
                    zfs_module._admit_timed(['zfs', 'list'])
        finally:
            if proc.returncode is None:
                proc.kill()
            proc.wait(10)
        # With the child really gone (and the patches undone) the sweep releases it.
        assert libzfseasy.stuck_children() == ()

    def test_an_abandoned_no_peek_handle_closes_without_waiting(self, kids, quick):
        handle = SendCommand._exec_stream(sh(BLOCK_BEFORE_WRITING), peek=False)
        proc = kids.procs[0]

        def never_exits(timeout=None):
            raise subprocess.TimeoutExpired('zfs', timeout)

        try:
            with patch.object(proc, 'wait', side_effect=never_exits), patch.object(proc, 'poll', return_value=None):
                assert handle.terminate() is True
                thread, box = run_in_thread(handle.close)
                join_or_fail(thread, 10)                    # never blocks on the stuck child
                assert 'exc' in box and isinstance(box['exc'], Exception)
        finally:
            if proc.returncode is None:
                proc.kill()
            proc.wait(10)


# ---------------------------------------------------------------------------------------------------------------------
# (4) stderr is still drained concurrently
# ---------------------------------------------------------------------------------------------------------------------

class TestStderrStillDrains:

    BIG_STDERR = 'head -c 1048576 /dev/zero | tr "\\0" "e" >&2; echo >&2; echo "final: line" >&2'

    def test_a_chatty_failing_child_does_not_deadlock_close(self, kids):
        handle = SendCommand._exec_stream(sh(f'{self.BIG_STDERR}; exit 1'), peek=False)

        def finish():
            assert handle.read() == b''
            handle.close()

        thread, box = run_in_thread(finish)
        join_or_fail(thread, 30)
        assert isinstance(box.get('exc'), Exception), box
        assert 'final: line' in str(box['exc'])
        assert kids.procs[0].returncode == 1
        # The captured text is bounded, not a megabyte.
        assert len(str(box['exc'])) < 1 << 20

    def test_a_chatty_child_that_succeeds_closes_cleanly(self, kids):
        handle = SendCommand._exec_stream(sh(f'{self.BIG_STDERR}; printf data'), peek=False)

        def finish():
            with handle as stream:
                return stream.read()

        thread, box = run_in_thread(finish)
        join_or_fail(thread, 30)
        assert box.get('result') == b'data', box
        assert kids.procs[0].returncode == 0

    def test_the_stderr_reader_thread_is_running_and_joined_by_close(self, kids):
        handle = SendCommand._exec_stream(sh('echo "oops" >&2; exit 2'), peek=False)
        assert handle._stderr_thread is not None
        assert handle.read() == b''
        with pytest.raises(Exception, match='^Oops$'):
            handle.close()
        assert not handle._stderr_thread.is_alive()


# ---------------------------------------------------------------------------------------------------------------------
# (5) non-blocking reads on the handle
# ---------------------------------------------------------------------------------------------------------------------

class TestNonBlockingRead:

    def test_select_then_read_gets_the_first_bytes(self, kids):
        handle = SendCommand._exec_stream(sh('sleep 0.5; printf "first-bytes"; exec sleep 120'), peek=False)
        try:
            fd = handle.fileno()
            os.set_blocking(fd, False)

            ready, _, _ = select.select([fd], [], [], 0)
            assert ready == []                              # nothing yet, and we did not block finding out
            assert handle.read1(11) is None or handle.read1(11) == b''   # non-blocking: no data, no wait

            ready, _, _ = select.select([fd], [], [], 15)
            assert ready == [fd]
            data = handle.read1(11)
            while data is None or len(data) < 11:           # a short first read is allowed
                select.select([fd], [], [], 15)
                more = handle.read1(11 - len(data or b''))
                data = (data or b'') + (more or b'')
            assert data == b'first-bytes'
        finally:
            handle.terminate()
            with pytest.raises(Exception):
                handle.close()

    def test_select_reports_eof_of_a_failed_child_as_readable(self, kids):
        handle = SendCommand._exec_stream(sh(FAIL_WITH_STDERR), peek=False)
        fd = handle.fileno()
        os.set_blocking(fd, False)
        ready, _, _ = select.select([fd], [], [], 15)
        assert ready == [fd]
        assert handle.read1(8) == b''                       # EOF
        with pytest.raises(Exception, match='boom'):
            handle.close()

    def test_a_watchdog_loop_can_bound_the_first_read(self, kids):
        """The point of peek=False: the caller bounds the wait for the first byte itself, then terminates."""
        handle = SendCommand._exec_stream(sh(BLOCK_BEFORE_WRITING), peek=False)
        fd = handle.fileno()
        os.set_blocking(fd, False)
        started = time.monotonic()
        ready, _, _ = select.select([fd], [], [], 0.4)
        assert ready == [] and 0.3 <= time.monotonic() - started < 5
        assert handle.poll() is None
        handle.terminate()
        with pytest.raises(Exception, match='SIGTERM'):
            handle.close()

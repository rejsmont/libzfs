"""`StreamHandle.terminate()` and `StreamHandle.pid` (plan item 1.3), plus the shared `Command._terminate_child`.

`terminate()` stops the child from another thread (a stalled-transfer watchdog) without touching the stream: a thread
blocked in `read()` / `write()` then sees EOF / `BrokenPipeError`, and `close()` stays the call that closes the pipes,
joins the stderr reader and raises the usual subprocess error. It escalates SIGTERM -> SIGKILL like the interrupted
`_exec_lines` path, abandons a child that survives KILL with one WARNING, and is idempotent.

An abandoned child (survived SIGKILL) is registered in the stuck-child registry; `close()` then never waits for it
(`StreamHandle._close_abandoned`) and a sweep releases it once it exits (`owns_stdout=False`: the handle's streams
are the handle's to close).

Real children are `sys.executable -c ...` (the conftest guard only objects to a real zfs/zpool), started through the
real `_exec_stream` / `_exec_stream_in`; the survives-SIGKILL case needs an unkillable process, so it is driven with a
fake one. Every real child is reaped (or killed) by the end of its test. Waits are shortened by patching
`_TERMINATE_TIMEOUT`.
"""

import logging
import os
import signal
import subprocess
import sys
import threading
import time
import types
from unittest.mock import MagicMock, call

import pytest

from libzfseasy import zfs as zfs_module
import libzfseasy
from libzfseasy.zfs import Command, ReceiveCommand, SendCommand, StreamHandle, ZFSTimeoutError

pytestmark = [pytest.mark.unit, pytest.mark.subprocess]

READY_THEN_SLEEP = 'import sys, time\nsys.stdout.write("ready\\n"); sys.stdout.flush()\ntime.sleep(120)\n'
READY_IGNORE_TERM = (
    'import signal, sys, time\n'
    'signal.signal(signal.SIGTERM, signal.SIG_IGN)\n'
    'sys.stdout.write("ready\\n"); sys.stdout.flush()\n'
    'time.sleep(120)\n'
)
SLEEP_READING_NOTHING = (
    'import signal, sys, time\n'
    'time.sleep(120)\n'
)
def ignore_term_script(ready_file):
    """A child with stdout discarded (a receive) cannot say "ready" on a pipe: it touches a file instead."""
    return (
        'import signal, time\n'
        'signal.signal(signal.SIGTERM, signal.SIG_IGN)\n'
        f'open({str(ready_file)!r}, "w").close()\n'
        'time.sleep(120)\n'
    )
PRINT_AND_EXIT = 'import sys\nsys.stdout.write("data")\n'


def py(script):
    return [sys.executable, '-c', script]


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
    """`_stuck` is module state and the fake-process abandon tests register MagicMocks in it: every test starts with it
    empty and the original entries (and lock) are put back afterwards."""
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
    """Short TERM/KILL waits; the SIGTERM-ignoring tests rely on this."""
    monkeypatch.setattr(zfs_module, '_TERMINATE_TIMEOUT', 0.3)


def run_in_thread(fn):
    """Start `fn()` on a daemon thread; returns (thread, box) where box gets 'result' / 'exc'."""
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


def spy(monkeypatch, proc, name):
    """Count calls to `proc.<name>()` while still performing them."""
    calls = []
    real = getattr(proc, name)

    def wrapper(*args, **kwargs):
        calls.append(1)
        return real(*args, **kwargs)

    monkeypatch.setattr(proc, name, wrapper)
    return calls


def fake_process(*, pid=4242, args=('zfs', 'send', 'tank/fs@a'), poll=None, wait=None):
    process = MagicMock()
    process.pid = pid
    process.args = list(args)
    process.poll.return_value = poll
    process.stderr = None
    if wait is not None:
        process.wait.side_effect = wait
    return process


def fake_handle(process):
    stream = MagicMock()
    stream.closed = False
    return StreamHandle(process, stream, [], None)


def never_exits():
    """A `wait` that always times out, whatever it is given (a child stuck in uninterruptible sleep)."""
    def wait(timeout=None):
        raise subprocess.TimeoutExpired('zfs', timeout)
    return wait


# ---------------------------------------------------------------------------------------------------------------------
# pid
# ---------------------------------------------------------------------------------------------------------------------

class TestPid:

    def test_pid_is_the_childs_pid(self, kids):
        handle = SendCommand._exec_stream(py(READY_THEN_SLEEP))
        try:
            assert handle.pid == kids.procs[0].pid
            assert isinstance(handle.pid, int) and handle.pid > 0
            os.kill(handle.pid, 0)             # it is a live process
        finally:
            handle.terminate()
            with pytest.raises(Exception):
                handle.close()

    def test_pid_of_a_receive_handle(self, kids):
        handle = ReceiveCommand._exec_stream_in(py(SLEEP_READING_NOTHING))
        try:
            assert handle.pid == kids.procs[0].pid
        finally:
            handle.terminate()
            with pytest.raises(Exception):
                handle.close()

    def test_pid_survives_close(self, kids):
        handle = SendCommand._exec_stream(py(PRINT_AND_EXIT))
        pid = handle.pid
        assert handle.read() == b'data'
        handle.close()
        assert handle.pid == pid

    def test_pid_is_none_without_a_process_or_without_a_pid(self):
        stream = MagicMock()
        assert StreamHandle(None, stream, [], None).pid is None
        assert StreamHandle(types.SimpleNamespace(), stream, [], None).pid is None

    def test_pid_is_read_from_the_process_object(self):
        assert fake_handle(fake_process(pid=31337)).pid == 31337


# ---------------------------------------------------------------------------------------------------------------------
# terminate() on real children
# ---------------------------------------------------------------------------------------------------------------------

class TestTerminateRealChild:

    def test_a_reader_blocked_in_read_sees_eof_and_close_raises(self, kids):
        handle = SendCommand._exec_stream(py(READY_THEN_SLEEP))
        proc = kids.procs[0]
        thread, box = run_in_thread(handle.read)     # to EOF: blocks until the child is gone
        time.sleep(0.3)
        assert thread.is_alive(), 'the reader should be blocked on the live child'
        started = time.monotonic()

        assert handle.terminate() is False           # killed, not abandoned

        assert time.monotonic() - started < 5        # SIGTERM was enough: no 5 s escalation wait
        join_or_fail(thread)
        assert 'exc' not in box and box['result'] == b'ready\n'     # EOF, not an error
        assert proc.returncode == -signal.SIGTERM
        with pytest.raises(Exception, match='^Command killed by signal SIGTERM$'):
            handle.close()
        assert handle._closed is True
        with pytest.raises(ProcessLookupError):      # reaped: nothing left behind
            os.kill(proc.pid, 0)

    def test_terminate_only_signals_it_leaves_the_stream_and_flag_alone(self, kids):
        handle = SendCommand._exec_stream(py(READY_THEN_SLEEP))
        stream = handle._stream

        handle.terminate()

        assert handle._closed is False
        assert stream.closed is False
        assert handle._stderr_thread is not None      # not dropped; close() joins it
        with pytest.raises(Exception, match='SIGTERM'):
            handle.close()
        assert stream.closed is True

    def test_close_reports_the_childs_stderr_after_terminate(self, kids):
        script = ('import sys, time\nsys.stdout.write("ready\\n"); sys.stdout.flush()\n'
                  'sys.stderr.write("cannot send: stuck\\n"); sys.stderr.flush()\ntime.sleep(120)\n')
        handle = SendCommand._exec_stream(py(script))
        time.sleep(0.2)
        handle.terminate()
        with pytest.raises(Exception) as info:
            handle.close()
        assert str(info.value) == 'Cannot send: stuck'      # close() collects stderr, as for any failed child

    def test_terminate_alone_ends_the_child_before_any_close(self, kids):
        handle = SendCommand._exec_stream(py(READY_THEN_SLEEP))
        os.kill(handle.pid, 0)
        handle.terminate()
        assert kids.procs[0].returncode == -signal.SIGTERM
        with pytest.raises(Exception):
            handle.close()

    def test_a_child_that_ignores_sigterm_is_killed(self, kids, quick):
        handle = SendCommand._exec_stream(py(READY_IGNORE_TERM))
        proc = kids.procs[0]
        thread, box = run_in_thread(handle.read)
        time.sleep(0.2)

        started = time.monotonic()
        assert handle.terminate() is False              # KILL worked: not abandoned
        elapsed = time.monotonic() - started

        assert 0.25 <= elapsed < 10                     # waited out the TERM window, then KILLed
        assert proc.returncode == -signal.SIGKILL
        join_or_fail(thread)
        assert box['result'] == b'ready\n'
        with pytest.raises(Exception, match='^Command killed by signal SIGKILL$'):
            handle.close()

    def test_it_sends_one_sigterm_and_a_second_terminate_is_a_noop(self, kids, monkeypatch):
        handle = SendCommand._exec_stream(py(READY_THEN_SLEEP))
        proc = kids.procs[0]
        terminated = spy(monkeypatch, proc, 'terminate')
        killed = spy(monkeypatch, proc, 'kill')

        assert [handle.terminate(), handle.terminate(), handle.terminate()] == [False, False, False]

        assert len(terminated) == 1 and killed == []
        with pytest.raises(Exception):
            handle.close()

    def test_terminate_after_close_is_a_noop(self, kids, monkeypatch):
        handle = SendCommand._exec_stream(py(PRINT_AND_EXIT))
        proc = kids.procs[0]
        assert handle.read() == b'data'
        handle.close()
        terminated = spy(monkeypatch, proc, 'terminate')
        killed = spy(monkeypatch, proc, 'kill')

        assert handle.terminate() is False              # exited and reaped by close()

        assert terminated == [] and killed == []
        assert proc.returncode == 0

    def test_terminate_after_a_failed_close_is_a_noop(self, kids, monkeypatch):
        handle = SendCommand._exec_stream(py('import sys\nsys.stdout.write("x"); sys.exit(3)\n'))
        proc = kids.procs[0]
        handle.read()
        with pytest.raises(Exception, match='exit code 3'):
            handle.close()
        terminated = spy(monkeypatch, proc, 'terminate')
        assert handle.terminate() is False
        assert terminated == [] and proc.returncode == 3

    def test_an_already_exited_child_gets_no_signal(self, kids, monkeypatch):
        """The child has exited but nobody reaped it (a zombie): poll() reaps it and nothing is signalled."""
        handle = SendCommand._exec_stream(py(PRINT_AND_EXIT))
        proc = kids.procs[0]
        assert handle.read() == b'data'                 # to EOF: the child is exiting
        deadline = time.monotonic() + 10
        while proc.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        terminated = spy(monkeypatch, proc, 'terminate')
        killed = spy(monkeypatch, proc, 'kill')

        assert handle.terminate() is False

        assert terminated == [] and killed == []
        handle.close()                                  # a clean exit stays clean: close() does not raise
        assert proc.returncode == 0

    def test_terminate_does_not_hide_a_clean_exit_from_close(self, kids):
        """terminate() racing with a child that exits 0 on its own must not turn into an error."""
        handle = SendCommand._exec_stream(py('import sys, time\nsys.stdout.write("d")\ntime.sleep(0.2)\n'))
        handle.read(1)
        time.sleep(0.6)
        handle.terminate()
        handle.close()

    def test_concurrent_terminates_send_one_sigterm(self, kids, monkeypatch):
        handle = SendCommand._exec_stream(py(READY_THEN_SLEEP))
        proc = kids.procs[0]
        terminated = spy(monkeypatch, proc, 'terminate')
        killed = spy(monkeypatch, proc, 'kill')
        barrier = threading.Barrier(4)

        def go():
            barrier.wait(10)
            return handle.terminate()

        runs = [run_in_thread(go) for _ in range(4)]
        for t, _ in runs:
            join_or_fail(t)
        assert [box['result'] for _, box in runs] == [False] * 4

        assert len(terminated) == 1 and killed == []
        assert proc.returncode == -signal.SIGTERM
        with pytest.raises(Exception):
            handle.close()

    def test_terminate_from_another_thread_while_close_waits(self, kids):
        """close() is blocked in wait() (the caller finished writing, zfs is stuck); a watchdog terminate frees it."""
        handle = ReceiveCommand._exec_stream_in(py('import sys, time\nsys.stdin.buffer.read()\ntime.sleep(120)\n'))
        thread, box = run_in_thread(handle.close)
        time.sleep(0.4)
        assert thread.is_alive(), 'close() should be waiting on the live child'

        handle.terminate()

        join_or_fail(thread)
        assert isinstance(box.get('exc'), Exception) and 'SIGTERM' in str(box['exc'])
        assert kids.procs[0].returncode == -signal.SIGTERM


class TestTerminateWriterSide:

    def test_a_writer_blocked_in_write_gets_broken_pipe(self, kids):
        handle = ReceiveCommand._exec_stream_in(py(SLEEP_READING_NOTHING))
        proc = kids.procs[0]
        chunk = b'x' * (1 << 20)

        def writer():
            while True:                                 # the child reads nothing: this blocks once the pipe is full
                handle.write(chunk)

        thread, box = run_in_thread(writer)
        time.sleep(0.5)
        assert thread.is_alive(), 'the writer should be blocked on a full pipe'

        handle.terminate()

        join_or_fail(thread)
        assert isinstance(box.get('exc'), BrokenPipeError), box
        assert proc.returncode == -signal.SIGTERM
        with pytest.raises(Exception, match='SIGTERM') as info:
            handle.close()                              # the flush of the pending buffer fails too; rc is reported
        assert isinstance(info.value.__cause__, (BrokenPipeError, type(None)))

    def test_terminate_does_not_close_the_stdin_pipe(self, kids):
        handle = ReceiveCommand._exec_stream_in(py(SLEEP_READING_NOTHING))
        handle.terminate()
        assert handle._stream.closed is False and handle._closed is False
        with pytest.raises(Exception, match='SIGTERM'):
            handle.close()
        assert handle._stream.closed is True

    def test_a_sigterm_ignoring_receiver_is_killed(self, kids, quick, tmp_path):
        ready = tmp_path / 'ready'
        handle = ReceiveCommand._exec_stream_in(py(ignore_term_script(ready)))
        deadline = time.monotonic() + 20
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ready.exists()                           # the handler is installed
        handle.terminate()
        assert kids.procs[0].returncode == -signal.SIGKILL
        with pytest.raises(Exception, match='SIGKILL'):
            handle.close()


# ---------------------------------------------------------------------------------------------------------------------
# poll()
# ---------------------------------------------------------------------------------------------------------------------

EXIT_3_AFTER_DATA = ('import sys, time\nsys.stdout.write("data"); sys.stdout.flush()\ntime.sleep(0.4)\nsys.exit(3)\n')
LINGER_AFTER_STDIN_EOF = 'import sys, time\nsys.stdin.buffer.read()\ntime.sleep(120)\n'


def forbid_signals(proc):
    """Any attempt to signal or wait for the real `Popen` object fails the test. Returns the undo function (the
    `kids` fixture reaps the child with `wait()` before a `monkeypatch` would be undone)."""
    def boom(name):
        def fail(*args, **kwargs):
            raise AssertionError(f'poll() must not call {name}()')
        return fail
    names = ('send_signal', 'terminate', 'kill', 'wait')
    for name in names:
        setattr(proc, name, boom(name))

    def undo():
        for name in names:
            proc.__dict__.pop(name, None)
    return undo


class TestPoll:

    def test_running_child_is_none_then_the_return_code_and_close_still_reports_it(self, kids):
        handle = SendCommand._exec_stream(py(EXIT_3_AFTER_DATA))
        proc = kids.procs[0]
        undo = forbid_signals(proc)
        assert handle.read(4) == b'data'
        assert handle.poll() is None                    # still sleeping
        deadline = time.monotonic() + 10
        rc = None
        while rc is None and time.monotonic() < deadline:
            rc = handle.poll()
            time.sleep(0.02)
        assert rc == 3
        assert handle.poll() == 3                       # stable
        assert handle._closed is False                  # a return code closes nothing
        assert handle._stream.closed is False
        undo()                                          # close() waits (the child is already reaped)
        with pytest.raises(Exception) as info:
            handle.close()
        assert 'signal' not in str(info.value)          # the exit status, not a signal
        assert handle._closed is True and handle._stream.closed is True

    def test_a_zero_exit_is_a_return_code_too_and_close_is_still_required(self, kids):
        handle = SendCommand._exec_stream(py(PRINT_AND_EXIT))
        proc = kids.procs[0]
        assert handle.read() == b'data'
        deadline = time.monotonic() + 10
        while handle.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert handle.poll() == 0
        assert handle._closed is False and handle._stream.closed is False
        handle.close()                                  # clean exit: no raise
        assert handle._closed is True
        assert proc.returncode == 0

    def test_it_never_signals_a_running_child(self, kids, monkeypatch):
        handle = SendCommand._exec_stream(py(READY_THEN_SLEEP))
        proc = kids.procs[0]
        undo = forbid_signals(proc)
        sent = []
        monkeypatch.setattr(os, 'kill', lambda *a: sent.append(a))
        for _ in range(5):
            assert handle.poll() is None
        assert sent == []
        undo()
        monkeypatch.undo()
        handle.terminate()                              # real cleanup
        with pytest.raises(Exception, match='SIGTERM'):
            handle.close()

    def test_a_signal_death_is_reported_as_the_negative_signal(self, kids):
        handle = SendCommand._exec_stream(py(READY_THEN_SLEEP))
        proc = kids.procs[0]
        os.kill(proc.pid, signal.SIGKILL)               # not through the handle
        deadline = time.monotonic() + 10
        while handle.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert handle.poll() == -signal.SIGKILL
        with pytest.raises(Exception, match='SIGKILL'):
            handle.close()

    def test_no_process_is_none(self):
        stream = MagicMock()
        assert StreamHandle(None, stream, [], None).poll() is None
        stream.close.assert_not_called()

    def test_with_a_fake_process_it_only_polls(self):
        process = fake_process(poll=None)
        handle = fake_handle(process)
        assert handle.poll() is None
        process.poll.return_value = 7
        assert handle.poll() == 7
        process.terminate.assert_not_called()
        process.kill.assert_not_called()
        process.send_signal.assert_not_called()
        process.wait.assert_not_called()
        assert handle._closed is False
        assert handle._terminate_lock.acquire(blocking=False)   # does not take the terminate lock either
        handle._terminate_lock.release()

    def test_poll_does_not_wait_for_a_terminate_in_progress(self, quick):
        """poll() is non-blocking: it must not queue behind a terminate() holding the lock."""
        gate = threading.Event()
        process = fake_process()

        def wait(timeout=None):
            gate.wait(10)
            return -15

        process.wait.side_effect = wait
        handle = fake_handle(process)
        thread, _ = run_in_thread(handle.terminate)
        try:
            time.sleep(0.1)
            started = time.monotonic()
            assert handle.poll() is None
            assert time.monotonic() - started < 0.5
        finally:
            gate.set()
            join_or_fail(thread)

    def test_the_bounded_close_pattern_end_to_end(self, kids, quick):
        """A receive-like child that lingers after stdin EOF (a final commit stuck on a suspended pool): poll in
        slices, terminate after a small bound, then close() takes the normal path quickly and reports SIGTERM."""
        handle = ReceiveCommand._exec_stream_in(py(LINGER_AFTER_STDIN_EOF))
        proc = kids.procs[0]
        handle.write(b'the stream')
        handle._stream.close()                          # EOF to the child: it now lingers
        started = time.monotonic()
        bound = started + 0.4
        while handle.poll() is None and time.monotonic() < bound:
            time.sleep(0.05)
        assert handle.poll() is None                    # still lingering at the bound
        assert time.monotonic() - started >= 0.4

        assert handle.terminate() is False              # bounded, SIGTERM is enough
        assert handle.poll() == -signal.SIGTERM
        closing = time.monotonic()
        with pytest.raises(Exception, match='^Command killed by signal SIGTERM$'):
            handle.close()
        assert time.monotonic() - closing < 3
        assert time.monotonic() - started < 5
        assert handle._closed is True
        assert proc.returncode == -signal.SIGTERM
        with pytest.raises(ProcessLookupError):
            os.kill(proc.pid, 0)

    def test_the_bounded_close_pattern_when_the_child_finishes_inside_the_bound(self, kids):
        handle = ReceiveCommand._exec_stream_in(py('import sys\nsys.stdin.buffer.read()\n'))
        handle.write(b'x')
        handle._stream.close()
        deadline = time.monotonic() + 10
        while handle.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert handle.poll() == 0
        handle.close()                                  # no terminate needed, no raise
        assert kids.procs[0].returncode == 0


# ---------------------------------------------------------------------------------------------------------------------
# terminate() with fake processes: abandon, idempotence, interrupts
# ---------------------------------------------------------------------------------------------------------------------

def warnings_of(caplog):
    return [r for r in caplog.records if r.levelno == logging.WARNING and r.name.startswith('libzfseasy')]


class TestTerminateAbandon:

    def test_a_child_that_survives_sigkill_is_abandoned_with_one_warning(self, caplog, quick):
        process = fake_process(pid=4242, wait=never_exits())
        handle = fake_handle(process)

        started = time.monotonic()
        with caplog.at_level(logging.DEBUG, logger='libzfseasy'):
            assert handle.terminate() is True           # abandoned; no raise
            assert handle.terminate() is True           # second call: still True, silent
        assert time.monotonic() - started < 5

        process.terminate.assert_called_once_with()
        process.kill.assert_called_once_with()
        assert process.wait.call_args_list == [call(timeout=0.3), call(timeout=0.3)]   # never an unbounded wait
        records = warnings_of(caplog)
        assert len(records) == 1
        assert records[0].getMessage() == ('zfs send (pid 4242) did not exit after SIGKILL (uninterruptible, likely '
                                           'a hung pool); abandoning it')

    def test_the_abandon_warning_comes_from_the_libzfseasy_logger(self, caplog, quick):
        """(The developer's note says `libzfseasy.zfs`; the module logger is `libzfseasy`, as for `_exec_lines`.)"""
        handle = fake_handle(fake_process(wait=never_exits()))
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            assert handle.terminate() is True
        assert [r.name for r in warnings_of(caplog)] == ['libzfseasy']

    def test_the_abandon_warning_uses_the_base_name_and_redacts_a_url_credential(self, caplog, quick):
        process = fake_process(pid=9, args=('/usr/local/sbin/zfs', 'send', 'x'), wait=never_exits())
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            fake_handle(process).terminate()
        message = warnings_of(caplog)[0].getMessage()
        assert message.startswith('zfs send (pid 9) did not exit') and '/usr/local' not in message

    def test_a_process_without_args_is_labelled_command(self, caplog, quick):
        process = types.SimpleNamespace(
            pid=5, poll=lambda: None, terminate=lambda: None, kill=lambda: None, wait=never_exits(), stderr=None)
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            fake_handle(process).terminate()
        assert warnings_of(caplog)[0].getMessage().startswith('command (pid 5) did not exit after SIGKILL')

    def test_abandoning_signals_and_registers_but_does_not_close_the_stream_or_set_the_closed_flag(self, quick):
        """terminate() alone only signals and flags the abandon: `close()` is still the call that closes and reports."""
        process = fake_process(wait=never_exits())
        handle = fake_handle(process)
        assert handle.terminate() is True
        assert handle._abandoned is True and handle._terminate_done is True
        assert handle._closed is False
        handle._stream.close.assert_not_called()
        handle._stream.raw.close.assert_not_called()
        # the wait calls were the two bounded ones of the escalation: nothing else touched the process
        assert [c[0] for c in process.method_calls if c[0] in ('terminate', 'kill', 'wait', 'poll')] == \
            ['poll', 'terminate', 'wait', 'kill', 'wait']

    def test_sigkill_that_works_is_not_an_abandon(self, caplog, quick):
        process = fake_process(wait=[subprocess.TimeoutExpired('zfs', 0.3), 0])
        handle = fake_handle(process)
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            assert handle.terminate() is False
        process.terminate.assert_called_once_with()
        process.kill.assert_called_once_with()
        assert warnings_of(caplog) == []
        assert handle._abandoned is False and libzfseasy.stuck_children() == ()   # not registered

    def test_sigterm_that_works_never_kills(self, caplog, quick):
        process = fake_process(wait=[0])
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            assert fake_handle(process).terminate() is False
        process.terminate.assert_called_once_with()
        process.kill.assert_not_called()
        assert warnings_of(caplog) == []

    def test_a_process_already_exited_is_not_signalled(self):
        process = fake_process(poll=-15)
        assert fake_handle(process).terminate() is False
        process.terminate.assert_not_called()
        process.kill.assert_not_called()
        process.wait.assert_not_called()

    def test_a_process_that_vanishes_underneath_is_swallowed(self):
        process = fake_process()
        process.terminate.side_effect = ProcessLookupError()
        handle = fake_handle(process)
        assert handle.terminate() is False              # no raise; the child is gone
        assert handle.terminate() is False
        process.terminate.assert_called_once_with()
        assert libzfseasy.stuck_children() == ()

    def test_concurrent_callers_serialise_and_only_the_first_escalates(self, quick):
        """The first caller is inside the TERM wait when the others arrive: they wait for it, send nothing."""
        state = {'dead': False}
        process = fake_process()
        process.poll.side_effect = lambda: 0 if state['dead'] else None

        def wait(timeout=None):
            time.sleep(0.2)
            state['dead'] = True
            return -15

        process.wait.side_effect = wait
        handle = fake_handle(process)
        barrier = threading.Barrier(3)
        done = {}

        def go(i):
            barrier.wait(10)
            done[i] = (handle.terminate(), time.monotonic())

        t0 = time.monotonic()
        threads = [run_in_thread(lambda i=i: go(i))[0] for i in range(3)]
        for t in threads:
            join_or_fail(t)

        process.terminate.assert_called_once_with()
        process.kill.assert_not_called()
        assert process.wait.call_count == 1
        assert all(v - t0 >= 0.19 for _, v in done.values())   # nobody returned before the escalation finished
        assert [r for r, _ in done.values()] == [False] * 3

    def test_an_interrupt_during_the_wait_kills_the_child_and_propagates(self):
        process = fake_process(wait=KeyboardInterrupt())
        handle = fake_handle(process)
        with pytest.raises(KeyboardInterrupt):
            handle.terminate()
        process.terminate.assert_called_once_with()
        process.kill.assert_called_once_with()          # a SIGTERM-ignoring child is not left running
        # The handle stays retryable: the child is still running, so a second call escalates again (and is
        # interrupted again by this fake's wait).
        with pytest.raises(KeyboardInterrupt):
            handle.terminate()
        assert process.terminate.call_count == 2
        assert process.kill.call_count == 2
        assert handle._abandoned is False
        assert libzfseasy.stuck_children() == ()

    def test_a_retry_after_an_interrupt_finds_the_child_dead_and_sends_nothing(self):
        process = fake_process(wait=KeyboardInterrupt())
        handle = fake_handle(process)
        with pytest.raises(KeyboardInterrupt):
            handle.terminate()
        process.poll.return_value = 0                   # the SIGKILL the helper sent has taken effect
        assert handle.terminate() is False
        process.terminate.assert_called_once_with()     # no new signals
        process.kill.assert_called_once_with()
        assert handle._abandoned is False
        assert handle.terminate() is False              # and now it is done for good
        process.terminate.assert_called_once_with()

    def test_a_retry_after_an_interrupt_can_still_abandon_and_register(self, caplog, quick):
        process = fake_process(pid=909, wait=KeyboardInterrupt())
        handle = fake_handle(process)
        with pytest.raises(KeyboardInterrupt):
            handle.terminate()
        process.wait.side_effect = never_exits()        # the child survives the next escalation
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            assert handle.terminate() is True
        assert handle._abandoned is True
        assert [c.pid for c in libzfseasy.stuck_children()] == [909]
        assert len(warnings_of(caplog)) == 1
        assert handle.terminate() is True               # abandoned: sticky again, nothing more is sent
        assert process.terminate.call_count == 2

    def test_an_interrupt_before_the_wait_also_leaves_the_handle_retryable(self):
        process = fake_process()
        process.terminate.side_effect = [KeyboardInterrupt(), None]
        process.wait.side_effect = [0]
        handle = fake_handle(process)
        with pytest.raises(KeyboardInterrupt):
            handle.terminate()
        assert handle._terminate_done is False
        assert handle.terminate() is False
        assert process.terminate.call_count == 2

    def test_an_interrupt_while_the_kill_fails_still_propagates(self):
        process = fake_process(wait=KeyboardInterrupt())
        process.kill.side_effect = ProcessLookupError()
        with pytest.raises(KeyboardInterrupt):
            fake_handle(process).terminate()

    def test_an_interrupted_terminate_releases_the_lock(self):
        handle = fake_handle(fake_process(wait=KeyboardInterrupt()))
        with pytest.raises(KeyboardInterrupt):
            handle.terminate()
        assert handle._terminate_lock.acquire(blocking=False)
        handle._terminate_lock.release()


# ---------------------------------------------------------------------------------------------------------------------
# The shared helper and the existing _exec_lines behaviour it was extracted from
# ---------------------------------------------------------------------------------------------------------------------

class TestTerminateChild:

    def test_returns_false_for_an_exited_child(self):
        process = fake_process(poll=0)
        assert Command._terminate_child(process, ['zfs', 'list']) is False
        process.terminate.assert_not_called()

    def test_returns_false_when_sigterm_is_enough(self, quick):
        process = fake_process(wait=[0])
        assert Command._terminate_child(process, ['zfs', 'list']) is False

    def test_returns_false_when_sigkill_is_enough(self, quick):
        process = fake_process(wait=[subprocess.TimeoutExpired('zfs', 1), 0])
        assert Command._terminate_child(process, ['zfs', 'list']) is False

    def test_returns_true_and_warns_once_when_the_child_survives_sigkill(self, caplog, quick):
        process = fake_process(pid=11, wait=never_exits())
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            assert Command._terminate_child(process, ['zfs', 'destroy', 'tank/x']) is True
        assert [r.getMessage() for r in warnings_of(caplog)] == [
            'zfs destroy (pid 11) did not exit after SIGKILL (uninterruptible, likely a hung pool); abandoning it']

    def test_kills_and_reraises_on_any_base_exception(self):
        process = fake_process(wait=SystemExit(2))
        with pytest.raises(SystemExit):
            Command._terminate_child(process, ['zfs', 'list'])
        process.kill.assert_called_once_with()

    def test_a_real_sigterm_ignoring_child_is_killed_and_reaped(self, kids, quick):
        process = subprocess.Popen(py(READY_IGNORE_TERM), stdout=subprocess.PIPE)
        assert process.stdout.readline() == b'ready\n'
        try:
            assert Command._terminate_child(process, process.args) is False
            assert process.returncode == -signal.SIGKILL
        finally:
            process.stdout.close()

    def test_exec_lines_second_interrupt_in_the_wait_kills_once_and_propagates(self, mock_subprocess):
        """`_exec_lines` keeps its behaviour: one SIGTERM, a further interrupt while waiting -> one SIGKILL, the
        interrupt propagates, nothing is abandoned or registered (no retry state exists on that path)."""
        process = MagicMock()
        process.pid = 78
        process.stdout.readline.side_effect = KeyboardInterrupt()
        process.stderr.readline.return_value = ''
        process.poll.return_value = None
        process.wait.side_effect = KeyboardInterrupt()
        mock_subprocess.return_value = process
        with pytest.raises(KeyboardInterrupt):
            list(Command._exec_lines(['/sbin/zfs', 'destroy', 'tank/x']))
        process.terminate.assert_called_once_with()
        process.kill.assert_called_once_with()
        assert libzfseasy.stuck_children() == ()

    def test_exec_lines_abandon_still_works_through_the_helper(self, mock_subprocess, caplog, quick):
        """Regression for the refactor: an interrupted `_exec_lines` abandons a KILL-proof child with one WARNING."""
        process = MagicMock()
        process.pid = 77
        process.stdout.readline.side_effect = KeyboardInterrupt()
        process.stderr.readline.return_value = ''
        process.poll.return_value = None
        process.wait.side_effect = never_exits()
        mock_subprocess.return_value = process
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            with pytest.raises(KeyboardInterrupt):
                list(Command._exec_lines(['/sbin/zfs', 'destroy', 'tank/x']))
        process.terminate.assert_called_once_with()
        process.kill.assert_called_once_with()
        assert len(warnings_of(caplog)) == 1
        assert 'did not exit after SIGKILL' in warnings_of(caplog)[0].getMessage()


# ---------------------------------------------------------------------------------------------------------------------
# Abandon: terminate() return values, the stuck-child registry, close() of an abandoned handle
# ---------------------------------------------------------------------------------------------------------------------

def open_pipe_file():
    """A real, open, readable file object (stands in for a child's stdout pipe); the caller closes it."""
    r, w = os.pipe()
    os.close(w)
    return os.fdopen(r, 'rb')


def pipe_mock():
    pipe = MagicMock()
    pipe.closed = False

    def close():
        pipe.closed = True

    pipe.close.side_effect = close
    return pipe


def abandonable(*, errors=None, reader=None, pid=4242, stdout_is_stream=False):
    """A fake process that survives SIGKILL, plus a handle over it whose stream, stdout and stderr are close-tracking
    mocks. Returns (process, handle)."""
    process = fake_process(pid=pid, wait=never_exits())
    process.stderr = pipe_mock()
    stream = pipe_mock()
    stream.raw = pipe_mock()
    process.stdout = stream if stdout_is_stream else pipe_mock()
    handle = StreamHandle(process, stream, list(errors or []), reader)
    return process, handle


def blocked_reader():
    """A live stand-in for the stderr reader (the child holds stderr open, so it does not end). Returns (thread, release)."""
    gate = threading.Event()
    thread = threading.Thread(target=gate.wait, daemon=True)
    thread.start()
    return thread, gate.set


ABANDON_TEXT = ('zfs send (pid 4242) was abandoned in uninterruptible sleep (likely a hung pool) after SIGTERM and '
                'SIGKILL and is still running')


class TestTerminateReturnValue:

    def test_no_process_returns_false_and_is_checked_first(self):
        handle = StreamHandle(None, MagicMock(), [], None)
        assert handle.terminate() is False
        assert handle._terminate_done is False          # returned before touching the lock or the flags
        assert handle.terminate() is False

    def test_abandon_returns_true_then_true_again_without_a_signal_or_a_second_warning(self, caplog, quick):
        process, handle = abandonable()
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            assert handle.terminate() is True
            polls, waits = process.poll.call_count, process.wait.call_count
            assert handle.terminate() is True
            assert handle.terminate() is True
        assert process.terminate.call_count == 1 and process.kill.call_count == 1
        assert (process.poll.call_count, process.wait.call_count) == (polls, waits)     # nothing touched the process
        assert len(warnings_of(caplog)) == 1
        assert len(zfs_module._stuck) == 1                                              # registered once

    def test_exited_child_returns_false_and_is_not_registered(self):
        process, handle = abandonable()
        process.poll.return_value = 0
        assert handle.terminate() is False
        assert zfs_module._stuck == [] and handle._abandoned is False

    def test_a_repeat_after_a_non_abandon_stays_false(self, quick):
        process = fake_process(wait=[0])
        handle = fake_handle(process)
        assert handle.terminate() is False
        assert handle.terminate() is False
        assert handle._terminate_done is True

    def test_concurrent_callers_all_see_true_for_an_abandon(self, caplog, quick):
        process, handle = abandonable()
        barrier = threading.Barrier(3)

        def go():
            barrier.wait(10)
            return handle.terminate()

        runs = [run_in_thread(go) for _ in range(3)]
        for t, _ in runs:
            join_or_fail(t)
        assert [box['result'] for _, box in runs] == [True] * 3
        assert process.terminate.call_count == 1 and process.kill.call_count == 1
        assert len(zfs_module._stuck) == 1


class TestAbandonRegistry:

    def test_the_abandoned_child_is_registered_with_its_reader_and_owns_no_stdout(self, quick):
        reader, release = blocked_reader()
        try:
            process, handle = abandonable(reader=reader)
            assert handle.terminate() is True
            (entry,) = zfs_module._stuck
            assert entry.process is process and entry.stderr_thread is reader
            assert entry.owns_stdout is False
            assert entry.label == 'zfs send'
        finally:
            release()
            reader.join(5)

    def test_stuck_children_reports_it(self, quick):
        process, handle = abandonable(pid=777)
        before = time.monotonic()
        handle.terminate()
        (child,) = libzfseasy.stuck_children()
        assert child.pid == 777 and child.label == 'zfs send'
        assert before <= child.since <= time.monotonic()
        assert libzfseasy.stuck_children() == (child,)  # a non-destructive snapshot while it lives

    def test_a_timed_call_fails_fast_while_it_lives(self, quick, monkeypatch):
        process, handle = abandonable(pid=777)
        handle.terminate()
        with pytest.raises(ZFSTimeoutError, match=r'1 earlier command\(s\) that timed out are still running \(pid 777\)'):
            zfs_module._admit_timed(['zfs', 'list'])

    def test_a_timed_command_is_refused_before_any_child_starts_and_an_untimed_one_is_not(self, quick, monkeypatch,
                                                                                         mock_subprocess):
        process, handle = abandonable()
        handle.terminate()
        mock_subprocess.setup(stdout=['a'], returncode=0)
        with libzfseasy.timeout(5):
            with pytest.raises(ZFSTimeoutError):
                list(Command._exec_lines(['zfs', 'list']))
        mock_subprocess.assert_not_called()             # failed fast: nothing started
        assert list(Command._exec_lines(['zfs', 'list'])) == ['a']      # untimed calls are never refused

    def test_the_sweep_drops_it_once_the_child_has_exited(self, quick):
        process, handle = abandonable()
        handle.terminate()
        assert len(libzfseasy.stuck_children()) == 1
        process.poll.return_value = -9                  # it finally died
        assert libzfseasy.stuck_children() == ()
        assert zfs_module._stuck == []
        zfs_module._admit_timed(['zfs', 'list'])        # timed calls are admitted again

    def test_the_sweep_closes_stderr_but_never_the_stream_or_stdout(self, quick):
        process, handle = abandonable()
        handle.terminate()
        process.poll.return_value = -9
        libzfseasy.stuck_children()
        process.stderr.close.assert_called_once_with()
        process.stdout.close.assert_not_called()
        handle._stream.close.assert_not_called()
        handle._stream.raw.close.assert_not_called()

    def test_the_sweep_leaves_stderr_open_while_its_reader_lives(self, quick):
        reader, release = blocked_reader()
        try:
            process, handle = abandonable(reader=reader)
            handle.terminate()
            process.poll.return_value = -9
            assert libzfseasy.stuck_children() == ()    # dropped from the registry ...
            process.stderr.close.assert_not_called()    # ... but the pipe a thread is blocked on is not closed
        finally:
            release()
            reader.join(5)

    def test_a_dead_child_that_poll_cannot_judge_is_dropped(self, quick):
        process, handle = abandonable()
        handle.terminate()
        process.poll.side_effect = OSError('gone')
        assert libzfseasy.stuck_children() == ()


class TestStuckChildOwnership:
    """`owns_stdout`: a `_reap_timed` entry owns stdout (its only reader was the timed call); a handle's does not."""

    def test_default_registration_owns_stdout_and_a_sweep_closes_it(self):
        stdout = open_pipe_file()
        process = fake_process(poll=None)
        process.stdout = stdout
        try:
            zfs_module._register_stuck(process, None, 'zfs list')
            assert zfs_module._stuck[0].owns_stdout is True
            process.poll.return_value = 0
            assert libzfseasy.stuck_children() == ()
            assert stdout.closed is True
        finally:
            stdout.close()

    def test_owns_stdout_false_leaves_a_distinct_process_stdout_open_on_sweep(self):
        stdout = open_pipe_file()
        process = fake_process(poll=None)
        process.stdout = stdout
        try:
            zfs_module._register_stuck(process, None, 'zfs send', owns_stdout=False)
            process.poll.return_value = 0
            assert libzfseasy.stuck_children() == ()
            assert stdout.closed is False               # a pump thread may still be inside this fd
        finally:
            stdout.close()

    def test_owns_stdout_false_still_closes_stderr(self):
        stderr = open_pipe_file()
        process = fake_process(poll=0)
        process.stderr = stderr
        zfs_module._register_stuck(process, None, 'zfs send', owns_stdout=False)
        libzfseasy.stuck_children()
        assert stderr.closed is True

    def test_reap_timed_registers_with_stdout_owned_and_closes_it_at_once(self, monkeypatch):
        monkeypatch.setattr(zfs_module, '_EXPIRY_WAIT', 0.05)
        stdout = open_pipe_file()
        process = fake_process(pid=31, wait=never_exits())
        process.stdout = stdout
        try:
            assert Command._reap_timed(process, None, ['zfs', 'list']) is True
            assert zfs_module._stuck[0].owns_stdout is True
            assert stdout.closed is True                # closed when the timed call gave up on the child
        finally:
            stdout.close()

    def test_reap_timed_entry_releases_stdout_again_on_sweep(self, monkeypatch):
        """The release step itself (not only the registration-time close) closes the stdout of an owning entry. A
        mock whose `closed` stays False shows the second close."""
        monkeypatch.setattr(zfs_module, '_EXPIRY_WAIT', 0.05)
        process = fake_process(pid=31, wait=never_exits())
        process.stdout = MagicMock()
        process.stdout.closed = False
        Command._reap_timed(process, None, ['zfs', 'list'])
        assert process.stdout.close.call_count == 1
        process.poll.return_value = 0
        libzfseasy.stuck_children()
        assert process.stdout.close.call_count == 2
        assert zfs_module._stuck == []


class TestCloseAbandoned:

    def abandoned(self, **kwargs):
        process, handle = abandonable(**kwargs)
        assert handle.terminate() is True
        process.wait.reset_mock()
        process.poll.reset_mock()
        process.poll.return_value = None
        return process, handle

    def test_close_raises_the_bare_abandon_exception_without_waiting(self, quick):
        process, handle = self.abandoned()
        with pytest.raises(Exception) as info:
            handle.close()
        assert type(info.value) is Exception            # bare, like the subprocess-error Exception
        assert str(info.value) == ABANDON_TEXT
        process.wait.assert_not_called()                # never blocks on the child
        assert handle._closed is True

    def test_close_drops_pending_bytes_instead_of_flushing(self, quick):
        process, handle = self.abandoned()
        with pytest.raises(Exception):
            handle.close()
        handle._stream.flush.assert_not_called()
        handle._stream.close.assert_not_called()        # the buffered writer's close would flush; only raw is closed
        handle._stream.raw.close.assert_called_once_with()

    def test_close_falls_back_to_the_stream_itself_without_a_raw(self, quick):
        process = fake_process(wait=never_exits())
        stream = pipe_mock()
        del stream.raw                                  # not a buffered object
        handle = StreamHandle(process, stream, [], None)
        handle.terminate()
        with pytest.raises(Exception, match='abandoned'):
            handle.close()
        stream.close.assert_called_once_with()

    def test_close_closes_a_distinct_stdout_but_closes_a_shared_one_once(self, quick):
        process, handle = self.abandoned()
        with pytest.raises(Exception):
            handle.close()
        process.stdout.close.assert_called_once_with()

        process, handle = self.abandoned(stdout_is_stream=True)
        with pytest.raises(Exception):
            handle.close()
        handle._stream.raw.close.assert_called_once_with()
        handle._stream.close.assert_not_called()

    def test_close_closes_stderr_when_its_reader_is_absent_or_ended(self, quick):
        process, handle = self.abandoned()              # no reader thread
        with pytest.raises(Exception):
            handle.close()
        process.stderr.close.assert_called_once_with()

        done = threading.Thread(target=lambda: None)
        done.start()
        done.join()
        process, handle = self.abandoned(reader=done)
        with pytest.raises(Exception):
            handle.close()
        process.stderr.close.assert_called_once_with()

    def test_close_leaves_stderr_open_while_its_reader_is_alive_and_bounds_the_join(self, quick, monkeypatch):
        reader, release = blocked_reader()
        try:
            process, handle = self.abandoned(reader=reader)
            monkeypatch.setattr(zfs_module, '_TERMINATE_TIMEOUT', 30)   # an unbounded/default join would take this long
            started = time.monotonic()
            with pytest.raises(Exception):
                handle.close()
            assert time.monotonic() - started < 2       # `_EXPIRY_JOIN` (0.05 s), not the 30 s default join
            process.stderr.close.assert_not_called()    # the reader is inside it; the registry closes it later
            assert handle._closed is True
        finally:
            release()
            reader.join(5)

    def test_the_collected_stderr_follows_the_message(self, quick):
        process, handle = self.abandoned(errors=['cannot send: pool suspended', 'second line'])
        with pytest.raises(Exception) as info:
            handle.close()
        assert str(info.value) == ABANDON_TEXT + '\ncannot send: pool suspended\nsecond line'

    def test_a_failing_stream_close_is_the_cause_not_the_message(self, quick):
        process, handle = self.abandoned()
        boom = BrokenPipeError('pipe')
        handle._stream.raw.close.side_effect = boom
        with pytest.raises(Exception) as info:
            handle.close()
        assert str(info.value) == ABANDON_TEXT and info.value.__cause__ is boom
        process.stderr.close.assert_called_once_with()  # the rest of the cleanup still ran
        assert handle._closed is True

    def test_close_is_idempotent(self, quick):
        process, handle = self.abandoned()
        with pytest.raises(Exception):
            handle.close()
        raw_calls = handle._stream.raw.close.call_count
        handle.close()                                  # already closed: silent no-op
        handle.close()
        assert handle._stream.raw.close.call_count == raw_calls
        process.wait.assert_not_called()

    def test_the_child_stays_registered_after_close(self, quick):
        process, handle = self.abandoned(pid=55)
        with pytest.raises(Exception):
            handle.close()
        assert [c.pid for c in libzfseasy.stuck_children()] == [55]

    def test_a_poll_failure_counts_as_still_running(self, quick):
        process, handle = self.abandoned()
        process.poll.side_effect = OSError('weird')
        with pytest.raises(Exception, match='abandoned in uninterruptible sleep'):
            handle.close()
        process.wait.assert_not_called()

    def test_an_abandoned_child_that_exited_before_close_reports_its_real_status(self, quick):
        process, handle = self.abandoned(errors=['cannot send: gone'])
        process.poll.return_value = 3                   # the pool recovered and the child died
        process.wait.side_effect = None
        process.wait.return_value = 3
        with pytest.raises(Exception) as info:
            handle.close()
        assert str(info.value) == 'cannot send: gone'    # the child's stderr, not the abandon message
        process.wait.assert_called_once_with()
        handle._stream.close.assert_called_once_with()  # the normal path closes the stream (and flushes)
        assert handle._closed is True

    def test_an_abandoned_child_that_exited_cleanly_closes_without_raising(self, quick):
        process, handle = self.abandoned()
        process.poll.return_value = 0
        process.wait.side_effect = None
        process.wait.return_value = 0
        handle.close()
        assert handle._closed is True
        process.wait.assert_called_once_with()
        process.poll.return_value = 0
        assert libzfseasy.stuck_children() == ()        # and the sweep drops it

    def test_a_non_abandoned_handle_still_waits_in_close(self, quick):
        process = fake_process(wait=[0])
        handle = fake_handle(process)
        assert handle.terminate() is False
        process.wait.reset_mock()
        process.wait.side_effect = None
        process.wait.return_value = -15
        process.poll.return_value = -15
        with pytest.raises(Exception, match='SIGTERM'):
            handle.close()
        process.wait.assert_called_once_with()


class TestCloseAbandonedRealChild:
    """A real child on a full stdin pipe with buffered bytes pending: a flush would block for ever. SIGKILL is made
    to "not take effect" by replacing the process's terminate/kill/wait; the child is killed for real at the end."""

    def test_close_returns_quickly_without_flushing_and_the_child_is_reaped_afterwards(self, kids, quick):
        handle = ReceiveCommand._exec_stream_in(py(SLEEP_READING_NOTHING))
        proc = kids.procs[0]
        stream = handle._stream
        fd = stream.fileno()
        try:
            # fill the kernel pipe so any flush of the buffered writer would block
            os.set_blocking(fd, False)
            while stream.raw.write(b'x' * 65536) is not None:
                pass
            os.set_blocking(fd, True)
            stream.write(b'y' * 1000)                   # pending in the BufferedWriter

            def never(timeout=None):
                assert timeout is not None, 'unbounded wait on an abandoned child'
                raise subprocess.TimeoutExpired('zfs', timeout)

            proc.terminate = lambda: None               # "uninterruptible": the signals have no effect
            proc.kill = lambda: None
            proc.wait = never
            assert handle.terminate() is True
            assert [c.pid for c in libzfseasy.stuck_children()] == [proc.pid]

            thread, box = run_in_thread(handle.close)
            join_or_fail(thread, limit=5)               # a flush into the full pipe would hang here
            assert 'was abandoned in uninterruptible sleep (likely a hung pool)' in str(box['exc'])
            assert f'(pid {proc.pid})' in str(box['exc'])
            assert stream.closed is True and handle._closed is True
            assert proc.returncode is None              # still running: close() neither waited nor killed it
            assert proc.stderr.closed is False          # its reader is alive on it
            handle.close()                              # idempotent
            assert [c.pid for c in libzfseasy.stuck_children()] == [proc.pid]
        finally:
            for name in ('terminate', 'kill', 'wait'):
                proc.__dict__.pop(name, None)           # back to the real methods
            os.kill(proc.pid, signal.SIGKILL)
            proc.wait(timeout=10)
            if handle._stderr_thread is not None:
                handle._stderr_thread.join(10)

        # the child has now really exited: the sweep drops it and closes stderr (its reader has ended)
        assert libzfseasy.stuck_children() == ()
        assert proc.stderr.closed is True
        assert proc.returncode == -signal.SIGKILL
        with pytest.raises(ProcessLookupError):
            os.kill(proc.pid, 0)

    def test_close_after_the_real_child_dies_reports_its_exit_status(self, kids, quick):
        """Abandon, then the child dies before close(): the normal path runs and reports the real status."""
        handle = ReceiveCommand._exec_stream_in(py(SLEEP_READING_NOTHING))
        proc = kids.procs[0]
        try:
            proc.terminate = lambda: None
            proc.kill = lambda: None
            real_wait = proc.wait

            def never(timeout=None):
                if timeout is None:
                    return real_wait()
                raise subprocess.TimeoutExpired('zfs', timeout)

            proc.wait = never
            assert handle.terminate() is True
            for name in ('terminate', 'kill'):
                proc.__dict__.pop(name, None)
            os.kill(proc.pid, signal.SIGKILL)
            real_wait(timeout=10)                       # exited (and reaped by us, as Popen would later)
            with pytest.raises(Exception, match='SIGKILL'):
                handle.close()
        finally:
            proc.__dict__.pop('wait', None)
        assert libzfseasy.stuck_children() == ()


class TestCloseTakesTheTerminateLockOnlyForTheDecision:
    """`close()` waits (bounded) for a `terminate()` escalating in another thread before it decides whether the child
    was abandoned, and never holds the lock across its own wait for the child."""

    @staticmethod
    def escalating_process(gate, waits):
        """A process whose first (SIGTERM) wait blocks on `gate`, then every wait times out: it survives KILL."""
        process = fake_process(pid=5151)

        def wait(timeout=None):
            waits.append(timeout)
            if timeout is None:
                raise AssertionError('unbounded wait() on a child that is being abandoned')
            if len(waits) == 1:
                gate.wait(10)
            raise subprocess.TimeoutExpired('zfs', timeout)

        process.wait.side_effect = wait
        return process

    def test_a_concurrent_close_waits_for_the_escalation_then_takes_the_abandoned_path(self, quick):
        gate = threading.Event()
        waits = []
        process = self.escalating_process(gate, waits)
        handle = fake_handle(process)
        term_thread, term_box = run_in_thread(handle.terminate)
        try:
            deadline = time.monotonic() + 5
            while not waits and time.monotonic() < deadline:
                time.sleep(0.01)
            assert waits, 'terminate() should be inside its first wait'
            close_thread, close_box = run_in_thread(handle.close)
            time.sleep(0.3)
            assert close_thread.is_alive(), 'close() must wait for the escalating terminate()'
            assert handle._closed is False
        finally:
            gate.set()
        join_or_fail(term_thread)
        join_or_fail(close_thread)
        assert term_box['result'] is True
        assert 'was abandoned in uninterruptible sleep' in str(close_box['exc'])
        assert None not in waits                        # close() never called wait() with no bound
        assert handle._closed is True
        assert [c.pid for c in libzfseasy.stuck_children()] == [5151]

    def test_without_a_concurrent_terminate_the_lock_is_free_and_released(self):
        process = fake_process(poll=0, wait=[0])
        handle = fake_handle(process)
        handle.close()
        assert handle._terminate_lock.acquire(blocking=False)
        handle._terminate_lock.release()

    def test_close_does_not_hold_the_lock_across_its_wait(self):
        """A terminate() started while close() is inside wait() is neither blocked by close() nor deadlocked."""
        release_close = threading.Event()
        in_wait = threading.Event()
        process = fake_process()

        def wait(timeout=None):
            if timeout is None:                         # close()'s own wait
                in_wait.set()
                release_close.wait(10)
            return -15

        process.wait.side_effect = wait
        handle = fake_handle(process)
        close_thread, close_box = run_in_thread(handle.close)
        try:
            assert in_wait.wait(5), 'close() should reach its wait()'
            assert handle._terminate_lock.acquire(blocking=False), 'close() holds the lock across wait()'
            handle._terminate_lock.release()
            started = time.monotonic()
            term_thread, term_box = run_in_thread(handle.terminate)
            join_or_fail(term_thread, limit=5)
            assert time.monotonic() - started < 2
            assert term_box['result'] is False
            assert close_thread.is_alive(), 'close() is still waiting for the child'
        finally:
            release_close.set()
        join_or_fail(close_thread)
        assert 'SIGTERM' in str(close_box['exc'])

    def test_the_acquire_is_bounded_by_twice_the_terminate_timeout_plus_one(self, quick):
        """Recording lock: the timeout argument is exactly 2 * _TERMINATE_TIMEOUT + 1, and the lock is released."""
        acquires = []
        releases = []

        class RecordingLock:
            def acquire(self, blocking=True, timeout=-1):
                acquires.append(timeout)
                return True

            def release(self):
                releases.append(1)

        handle = fake_handle(fake_process(poll=0, wait=[0]))
        handle._terminate_lock = RecordingLock()
        handle.close()
        assert acquires == [pytest.approx(2 * 0.3 + 1)]
        assert releases == [1]

    def test_a_timed_out_acquire_proceeds_and_releases_nothing(self, quick):
        class TimingOutLock:
            def __init__(self):
                self.releases = 0

            def acquire(self, blocking=True, timeout=-1):
                return False

            def release(self):
                self.releases += 1

        process = fake_process(poll=0, wait=[0])
        handle = fake_handle(process)
        lock = handle._terminate_lock = TimingOutLock()
        with pytest.raises(Exception, match='SIGTERM'):
            process.wait.side_effect = [-15]
            handle.close()
        assert lock.releases == 0
        assert handle._closed is True

    def test_a_real_lock_held_elsewhere_times_out_and_close_proceeds(self, monkeypatch):
        """With the real lock held by someone else for good, close() gives up after the bound (1 s here), takes the
        normal path and leaves the other holder's lock alone."""
        monkeypatch.setattr(zfs_module, '_TERMINATE_TIMEOUT', 0)
        process = fake_process(poll=0, wait=[0])
        handle = fake_handle(process)
        assert handle._terminate_lock.acquire()
        try:
            thread, box = run_in_thread(handle.close)
            time.sleep(0.3)
            assert thread.is_alive(), 'close() should still be waiting for the lock'
            join_or_fail(thread, limit=10)
            assert 'exc' not in box
            assert handle._closed is True
            process.wait.assert_called_once_with()
            assert handle._terminate_lock.locked(), 'close() released a lock it never acquired'
        finally:
            handle._terminate_lock.release()

    def test_a_timed_out_acquire_still_honours_an_abandon_already_recorded(self, quick):
        """The decision is taken either way: an abandon recorded before the lock timed out is still seen."""
        class TimingOutLock:
            def acquire(self, blocking=True, timeout=-1):
                return False

            def release(self):
                raise AssertionError('not acquired')

        process = fake_process(wait=never_exits())
        handle = fake_handle(process)
        assert handle.terminate() is True
        handle._terminate_lock = TimingOutLock()
        with pytest.raises(Exception, match='abandoned in uninterruptible sleep'):
            handle.close()
        assert all(c.args[0] is not None for c in process.wait.call_args_list if c.args)

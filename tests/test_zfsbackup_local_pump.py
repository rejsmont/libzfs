"""`zfsbackup.local._Pump` over REAL child processes (plan item 3.1).

The pump copies one `zfs send` stream into one `zfs receive` on the worker thread with non-blocking
descriptors. Here the two children are hermetic Python stubs started through libzfseasy's own
`SendCommand._exec_stream` / `ReceiveCommand._exec_stream_in` (so the `StreamHandle`s, the start-up
peek that buffers the first bytes, `terminate()` and `close()` are the real ones); nothing runs
`zfs`. The stubs `exec` nothing, so SIGTERM reaches the process the handle holds.

Pinned here: a 7 MiB round trip hash-equal (including the bytes libzfseasy's peek had already
buffered), a receive that exits mid-stream (`receive-gone`, the send stopped, the receive's stderr
kept), a send that fails (its stderr kept, the receive gets EOF), a stalled send and a receive that
hangs after EOF (both bounded, both children stopped), a receive that exits 0 before the send's EOF
with nothing left to give it (no failure), a child that ignores SIGTERM (KILL), `close()` idempotent,
no child left behind, and the pumping process killed with SIGKILL (both children see EOF/EPIPE and
exit). `TestPumpOverFakeHandles` runs the same pump over `tests/_local_support` doubles for the
paths a real child cannot produce (an abandoned child, a failing pipe read).

Wall time is bounded by shrinking `LOCAL_STALL_TIMEOUT`, `LOCAL_COMMIT_TIMEOUT`, `PUMP_SLICE` and
libzfseasy's `_TERMINATE_TIMEOUT`.
"""

import hashlib
import os
import signal
import subprocess
import sys
import time
import types

import pytest

from _hermetic import hermetic_env
from _local_support import FakeReceive, FakeSend
from libzfseasy import zfs as zfs_module
from libzfseasy.zfs import ReceiveCommand, SendCommand
from zfsbackup import local

pytestmark = [pytest.mark.integration]

PY = sys.executable

# ---------------------------------------------------------------------------------------------
# The children (Python stubs; `sys.argv[1:]` after `-c <code>`)
# ---------------------------------------------------------------------------------------------

#: `zfs send`: the file `argv[1]` to stdout.
SEND_FILE = (
    "import sys, shutil\n"
    "shutil.copyfileobj(open(sys.argv[1], 'rb'), sys.stdout.buffer)\n"
    "sys.stdout.buffer.flush()\n"
)
#: `zfs send` that writes `argv[2]` bytes of the file, then dies with a message (status 4).
SEND_THEN_FAIL = (
    "import sys\n"
    "sys.stdout.buffer.write(open(sys.argv[1], 'rb').read(int(sys.argv[2])))\n"
    "sys.stdout.buffer.flush()\n"
    "sys.stderr.write('send-boom\\n'); sys.stderr.flush()\n"
    "sys.exit(4)\n"
)
#: `zfs send` that writes `argv[2]` bytes and is killed by SIGKILL.
SEND_THEN_KILLED = (
    "import os, signal, sys\n"
    "sys.stdout.buffer.write(open(sys.argv[1], 'rb').read(int(sys.argv[2])))\n"
    "sys.stdout.buffer.flush()\n"
    "os.kill(os.getpid(), signal.SIGKILL)\n"
)
#: `zfs send` that writes `argv[2]` bytes, then sleeps (a stalled send). With `argv[3] == 'ignore'`
#: it ignores SIGTERM first.
SEND_THEN_HANG = (
    "import signal, sys, time\n"
    "if len(sys.argv) > 3 and sys.argv[3] == 'ignore':\n"
    "    signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
    "sys.stdout.buffer.write(open(sys.argv[1], 'rb').read(int(sys.argv[2])))\n"
    "sys.stdout.buffer.flush()\n"
    "time.sleep(120)\n"
)
#: `zfs send` that writes the whole file, then takes `argv[2]` seconds to exit (status 0).
SEND_SLOW_END = (
    "import sys, time\n"
    "sys.stdout.buffer.write(open(sys.argv[1], 'rb').read())\n"
    "sys.stdout.buffer.flush()\n"
    "time.sleep(float(sys.argv[2]))\n"
)
#: `zfs send` that never writes a byte.
SEND_SILENT = "import time\ntime.sleep(120)\n"
#: `zfs send` that fails at once, without a byte.
SEND_FAIL_NOW = "import sys\nsys.stderr.write('cannot open: no such snapshot\\n'); sys.exit(1)\n"
#: `zfs send` that writes the file, closes its stdout (EOF) and then lingers instead of exiting.
SEND_LINGER_AFTER_EOF = (
    "import os, sys, time\n"
    "sys.stdout.buffer.write(open(sys.argv[1], 'rb').read())\n"
    "sys.stdout.buffer.flush(); os.close(1)\n"
    "time.sleep(120)\n"
)
#: `zfs receive`: stdin to the file `argv[1]`.
RECV_SINK = (
    "import sys, shutil\n"
    "out = open(sys.argv[1], 'wb')\n"
    "shutil.copyfileobj(sys.stdin.buffer, out)\n"
    "out.close()\n"
)
#: `zfs receive` that reads 100000 bytes, complains and exits 3.
RECV_FAIL = (
    "import sys\n"
    "sys.stdin.buffer.read(100000)\n"
    "sys.stderr.write('recv-boom\\n'); sys.stderr.flush()\n"
    "sys.exit(3)\n"
)
#: `zfs receive` that reads 1000 bytes and exits 0 (it stopped reading, without an error).
RECV_EXIT0_EARLY = "import sys\nsys.stdin.buffer.read(1000)\n"
#: `zfs receive` that reads exactly `argv[2]` bytes into `argv[1]` and exits at once, not waiting
#: for EOF (it has read the end record).
RECV_READ_N = (
    "import sys\n"
    "data = sys.stdin.buffer.read(int(sys.argv[2]))\n"
    "open(sys.argv[1], 'wb').write(data)\n"
)
#: `zfs receive` that copies stdin to `argv[1]` until EOF and then never commits.
RECV_HANG_AFTER_EOF = (
    "import sys, shutil, time\n"
    "out = open(sys.argv[1], 'wb')\n"
    "shutil.copyfileobj(sys.stdin.buffer, out)\n"
    "out.close()\n"
    "time.sleep(120)\n"
)
#: An endless `zfs send` and a sink; both write their pid to `argv[1]` first.
ENDLESS_SEND = (
    "import os, sys\n"
    "open(sys.argv[1], 'w').write(str(os.getpid()))\n"
    "block = b'x' * 65536\n"
    "while True:\n"
    "    try:\n"
    "        sys.stdout.buffer.write(block); sys.stdout.buffer.flush()\n"
    "    except BrokenPipeError:\n"
    "        sys.exit(0)\n"
)
ENDLESS_SINK = (
    "import os, sys\n"
    "open(sys.argv[1], 'w').write(str(os.getpid()))\n"
    "while sys.stdin.buffer.read(65536):\n"
    "    pass\n"
)


def script(code, *args):
    return [PY, '-c', code, *map(str, args)]


def alive(pid) -> bool:
    """Is `pid` a live process (a zombie is not)?"""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    state = subprocess.run(['ps', '-o', 'stat=', '-p', str(pid)], capture_output=True, text=True).stdout.strip()
    return bool(state) and not state.startswith('Z')


def wait_dead(pids, bound=15.0) -> bool:
    deadline = time.monotonic() + bound
    while time.monotonic() < deadline:
        if not any(alive(p) for p in pids):
            return True
        time.sleep(0.05)
    return not any(alive(p) for p in pids)


# ---------------------------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def fast(mocker):
    """Short slices and short child-stop bounds; a stall/commit bound large enough that only the
    tests about them can trip it."""
    mocker.patch.object(local, 'PUMP_SLICE', 0.05)
    mocker.patch.object(local, 'PUMP_POLL_SLICE', 0.02)
    mocker.patch.object(local, 'LOCAL_STALL_TIMEOUT', 15.0)
    mocker.patch.object(local, 'LOCAL_COMMIT_TIMEOUT', 60.0)
    mocker.patch.object(zfs_module, '_TERMINATE_TIMEOUT', 1.0)


@pytest.fixture
def children():
    """Starts the stub children and, whatever the test did, stops them at the end and fails the
    test if one outlived its handle."""
    started = []

    class Children:
        def send(self, cmd, peek=True):
            handle = SendCommand._exec_stream(cmd, peek=peek)
            started.append(handle)
            return handle

        def receive(self, cmd):
            handle = ReceiveCommand._exec_stream_in(cmd)
            started.append(handle)
            return handle

    yield Children()
    leaked = []
    for handle in started:
        pid = handle.pid
        if alive(pid):
            leaked.append(pid)
            handle.terminate()
        try:
            handle.close()
        except Exception:
            pass
    assert not leaked, f"children left running by the pump: {leaked}"
    assert not any(alive(h.pid) for h in started)


@pytest.fixture
def payload(tmp_path):
    """A 7 MiB random file (not a multiple of any chunk) and where a sink may write."""
    src = tmp_path / 'src.bin'
    src.write_bytes(os.urandom(7 * 1024 * 1024 + 123))
    return types.SimpleNamespace(src=src, dst=tmp_path / 'dst.bin', data=src.read_bytes())


def pump_run(children, send_cmd, recv_cmd, *, hook=None, peek=True):
    """prepare -> read_head(312) -> attach_receive -> run -> close, as `_receive_step` does.
    Returns what happened; `hook(pump)` runs after the receive is attached."""
    send = children.send(send_cmd, peek=peek)
    pump = local._Pump(send)
    result = types.SimpleNamespace(pump=pump, send=send, receive=None, head=None, outcome=None)
    started = time.monotonic()
    try:
        pump.prepare()
        result.head = pump.read_head(312)
        result.receive = children.receive(recv_cmd)
        pump.attach_receive(result.receive)
        if hook is not None:
            hook(pump)
        result.outcome = pump.run()
    finally:
        pump.close()
    result.elapsed = time.monotonic() - started
    return result


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------------------------
# A clean transfer
# ---------------------------------------------------------------------------------------------


class TestRoundTrip:
    def test_seven_mib_arrive_hash_equal(self, children, payload):
        run = pump_run(children, script(SEND_FILE, payload.src), script(RECV_SINK, payload.dst))
        assert run.outcome == local.PUMP_OK
        assert run.pump.send_error is None and run.pump.receive_error is None
        assert sha(payload.dst.read_bytes()) == sha(payload.data)

    def test_the_bytes_libzfseasys_peek_had_buffered_are_kept_and_in_order(self, children, payload):
        send = children.send(script(SEND_FILE, payload.src))
        buffered = send.peek(0)
        assert 0 < len(buffered) <= len(payload.data), "libzfseasy's peek buffered the first bytes"
        pump = local._Pump(send)
        try:
            pump.prepare()
            head = pump.read_head(312)
            assert len(head) > 312, "the head read returns everything already buffered, not 312 bytes"
            assert head == payload.data[:len(head)]
            assert head.startswith(buffered)
            receive = children.receive(script(RECV_SINK, payload.dst))
            pump.attach_receive(receive)
            assert pump.run() == local.PUMP_OK
        finally:
            pump.close()
        assert payload.dst.read_bytes() == payload.data

    def test_started_without_the_peek_nothing_is_buffered_and_the_bytes_still_all_arrive(
            self, children, payload):
        """What `LocalBackupManager` does (`_send_peek = False`): the head comes from the descriptor."""
        send = children.send(script(SEND_FILE, payload.src), peek=False)
        pump = local._Pump(send)
        try:
            pump.prepare()
            head = pump.read_head(312)
            assert len(head) >= 312 and head == payload.data[:len(head)]
            pump.attach_receive(children.receive(script(RECV_SINK, payload.dst)))
            assert pump.run() == local.PUMP_OK
        finally:
            pump.close()
        assert sha(payload.dst.read_bytes()) == sha(payload.data)

    def test_read_head_returns_at_least_the_asked_size_and_keeps_it_to_be_fed(self, children, payload):
        send = children.send(script(SEND_FILE, payload.src))
        pump = local._Pump(send)
        try:
            pump.prepare()
            head = pump.read_head(local.PUMP_CHUNK // 2)
            assert len(head) >= local.PUMP_CHUNK // 2
            assert head == payload.data[:len(head)]
            assert pump.read_head(10) == head, "the head stays in the buffer: asking again changes nothing"
        finally:
            pump.close()

    def test_a_stream_shorter_than_the_header_is_returned_as_it_is(self, children, tmp_path):
        short = tmp_path / 'short.bin'
        short.write_bytes(b'abc')
        send = children.send(script(SEND_FILE, short))
        pump = local._Pump(send)
        try:
            pump.prepare()
            assert pump.read_head(312) == b'abc'
        finally:
            pump.close()

    def test_the_descriptors_are_left_to_the_handles(self, children, payload):
        run = pump_run(children, script(SEND_FILE, payload.src), script(RECV_SINK, payload.dst))
        assert run.outcome == local.PUMP_OK
        assert run.send.poll() is not None and run.receive.poll() is not None


# ---------------------------------------------------------------------------------------------
# Failures on either side
# ---------------------------------------------------------------------------------------------


class TestReceiveFails:
    def test_receive_exits_nonzero_mid_stream(self, children, payload):
        run = pump_run(children, script(SEND_FILE, payload.src), script(RECV_FAIL))
        assert run.outcome == local.PUMP_RECEIVE_GONE
        assert 'recv-boom' in run.pump.receive_error.lower()
        assert run.send.poll() is not None, "the send was stopped, not left writing into nothing"
        assert run.pump.send_error is not None and 'SIGTERM' in run.pump.send_error
        assert run.elapsed < 20

    def test_receive_exits_zero_early_while_data_is_pending_is_gone_too(self, children, payload):
        run = pump_run(children, script(SEND_FILE, payload.src), script(RECV_EXIT0_EARLY))
        assert run.outcome == local.PUMP_RECEIVE_GONE
        assert run.pump.receive_error is None
        assert run.send.poll() is not None

    def test_a_pipe_error_on_the_send_side_gives_the_receive_eof_and_stops_the_send(
            self, children, payload, mocker):
        def break_the_read(pump):
            mocker.patch.object(pump, '_fill', side_effect=OSError(5, 'Input/output error'))

        run = pump_run(
            children, script(SEND_FILE, payload.src), script(RECV_SINK, payload.dst),
            hook=break_the_read,
        )
        assert run.outcome == local.PUMP_SEND_FAILED
        assert run.send.poll() is not None
        assert run.receive.poll() == 0, "the receive was given EOF and ended by itself"
        assert len(payload.dst.read_bytes()) < len(payload.data)


class TestSendFails:
    def test_send_dies_mid_stream_its_stderr_is_reported_and_the_receive_gets_eof(self, children, payload):
        run = pump_run(
            children, script(SEND_THEN_FAIL, payload.src, 2_000_000), script(RECV_SINK, payload.dst),
        )
        assert run.outcome == local.PUMP_OK, "EOF is EOF: the failure is the child's exit status"
        assert 'send-boom' in run.pump.send_error.lower()
        assert run.pump.receive_error is None
        assert payload.dst.read_bytes() == payload.data[:2_000_000], "the receive saw the truncated stream and EOF"

    def test_send_killed_by_a_signal_is_reported(self, children, payload):
        run = pump_run(
            children, script(SEND_THEN_KILLED, payload.src, 300_000), script(RECV_SINK, payload.dst),
        )
        assert run.outcome == local.PUMP_OK
        assert 'SIGKILL' in run.pump.send_error
        assert payload.dst.read_bytes() == payload.data[:300_000]


# ---------------------------------------------------------------------------------------------
# Stalls, commits and stubborn children
# ---------------------------------------------------------------------------------------------


@pytest.mark.slow
class TestBounds:
    def test_a_send_that_stalls_stops_both_children(self, children, payload, mocker):
        mocker.patch.object(local, 'LOCAL_STALL_TIMEOUT', 0.8)
        run = pump_run(
            children, script(SEND_THEN_HANG, payload.src, 5000), script(RECV_SINK, payload.dst),
        )
        assert run.outcome == local.PUMP_STALLED
        assert run.send.poll() is not None and run.receive.poll() is not None
        assert 'SIGTERM' in run.pump.send_error and 'SIGTERM' in run.pump.receive_error
        assert run.pump.commit_timed_out is False, "the stream had not ended: this is a stall"
        assert run.pump.abandoned == []
        assert run.elapsed < 20

    def test_no_first_record_in_time_raises_and_close_stops_the_send(self, children, payload, mocker):
        mocker.patch.object(local, 'LOCAL_STALL_TIMEOUT', 0.8)
        send = children.send(script(SEND_THEN_HANG, payload.src, 100))
        pump = local._Pump(send)
        try:
            pump.prepare()
            with pytest.raises(local._Stalled):
                pump.read_head(312)
        finally:
            pump.close()
        assert send.poll() is not None
        assert 'SIGTERM' in pump.send_error

    def test_a_receive_that_does_not_commit_after_eof_is_stopped_at_the_commit_bound(
            self, children, payload, mocker):
        mocker.patch.object(local, 'LOCAL_COMMIT_TIMEOUT', 1.0)
        run = pump_run(children, script(SEND_FILE, payload.src), script(RECV_HANG_AFTER_EOF, payload.dst))
        assert run.outcome == local.PUMP_STALLED
        assert run.pump.commit_timed_out is True
        assert run.send.poll() == 0
        assert run.pump.send_error is None
        assert 'SIGTERM' in run.pump.receive_error
        assert payload.dst.read_bytes() == payload.data, "everything had been delivered before the bound"
        assert run.elapsed < 30

    def test_a_send_that_never_writes_a_byte_is_cut_at_the_stall_bound(self, children, mocker):
        """No blocking peek: a send stuck before its first byte is the pump's to stop."""
        mocker.patch.object(local, 'LOCAL_STALL_TIMEOUT', 0.6)
        send = children.send(script(SEND_SILENT), peek=False)
        pump = local._Pump(send)
        started = time.monotonic()
        try:
            pump.prepare()
            with pytest.raises(local._Stalled):
                pump.read_head(312)
        finally:
            pump.close()
        assert send.poll() is not None and 'SIGTERM' in pump.send_error
        assert time.monotonic() - started < 20

    def test_a_send_that_lingers_after_its_eof_is_stopped_and_the_stream_still_counts(
            self, children, tmp_path, mocker):
        # 8000 bytes fit any pipe buffer: no byte waits for a child that is still starting, so
        # the only bound that can fire is the one on the send's late exit.
        data = os.urandom(8000)
        src, dst = tmp_path / 'small.bin', tmp_path / 'out.bin'
        src.write_bytes(data)
        mocker.patch.object(local, 'LOCAL_STALL_TIMEOUT', 1.5)
        run = pump_run(
            children, script(SEND_LINGER_AFTER_EOF, src), script(RECV_SINK, dst), peek=False,
        )
        assert run.outcome == local.PUMP_OK, "the receive read everything and exited 0"
        assert run.pump.send_cut_after_eof is True
        assert 'SIGTERM' in run.pump.send_error
        assert run.pump.receive_error is None
        assert dst.read_bytes() == data

    def test_a_send_that_exits_in_time_is_not_cut_after_eof(self, children, payload):
        run = pump_run(children, script(SEND_FILE, payload.src), script(RECV_SINK, payload.dst))
        assert run.pump.send_cut_after_eof is False and run.pump.commit_timed_out is False

    def test_a_child_that_ignores_sigterm_is_killed(self, children, payload, mocker):
        mocker.patch.object(local, 'LOCAL_STALL_TIMEOUT', 0.8)
        mocker.patch.object(zfs_module, '_TERMINATE_TIMEOUT', 0.6)
        run = pump_run(
            children, script(SEND_THEN_HANG, payload.src, 5000, 'ignore'), script(RECV_SINK, payload.dst),
        )
        assert run.outcome == local.PUMP_STALLED
        assert 'SIGKILL' in run.pump.send_error, "TERM was ignored: the escalation reached KILL"
        assert run.send.poll() is not None
        assert run.pump.abandoned == []


class TestReceiveExitsBeforeTheSendsEof:
    def test_it_is_not_a_failure_when_nothing_is_left_to_give_it(self, children, tmp_path):
        data = os.urandom(300_000)
        src = tmp_path / 'small.bin'
        src.write_bytes(data)
        dst = tmp_path / 'out.bin'
        run = pump_run(
            children, script(SEND_SLOW_END, src, 0.7), script(RECV_READ_N, dst, len(data)),
        )
        assert run.outcome == local.PUMP_OK
        assert run.pump.send_error is None and run.pump.receive_error is None
        assert dst.read_bytes() == data


# ---------------------------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------------------------


class TestClose:
    def test_close_is_idempotent(self, children, payload):
        run = pump_run(children, script(SEND_FILE, payload.src), script(RECV_SINK, payload.dst))
        errors = (run.pump.send_error, run.pump.receive_error)
        run.pump.close()
        run.pump.close()
        assert (run.pump.send_error, run.pump.receive_error) == errors

    def test_close_after_prepare_only_stops_the_running_send(self, children, payload):
        send = children.send(script(SEND_THEN_HANG, payload.src, 5000))
        pump = local._Pump(send)
        pump.prepare()
        pump.close()
        assert send.poll() is not None
        assert pump.send_error is not None

    def test_close_before_prepare_is_harmless(self, children, payload):
        send = children.send(script(SEND_THEN_HANG, payload.src, 5000))
        pump = local._Pump(send)
        pump.close()
        pump.close()
        assert send.poll() is not None

    def test_close_with_a_receive_attached_but_never_run_stops_both(self, children, payload):
        send = children.send(script(SEND_THEN_HANG, payload.src, 5000))
        pump = local._Pump(send)
        receive = children.receive(script(RECV_SINK, payload.dst))
        try:
            pump.prepare()
            pump.attach_receive(receive)
        finally:
            pump.close()
        assert send.poll() is not None and receive.poll() is not None
        assert pump.receive_error is not None

    def test_finish_send_waits_for_a_send_that_ended_and_reports_its_status(self, children, tmp_path):
        src = tmp_path / 'tiny.bin'
        src.write_bytes(b'z' * 100)
        send = children.send(script(SEND_THEN_FAIL, src, 100))
        pump = local._Pump(send)
        try:
            pump.prepare()
            assert pump.read_head(312) == b'z' * 100, "EOF before the header is complete"
            error = pump.finish_send()
        finally:
            pump.close()
        assert error is not None and 'send-boom' in error.lower()

    def test_a_send_that_fails_at_once_without_a_byte_is_eof_and_its_error_comes_from_finish(
            self, children):
        send = children.send(script(SEND_FAIL_NOW), peek=False)
        pump = local._Pump(send)
        try:
            pump.prepare()
            assert pump.read_head(312) == b''
            error = pump.finish_send()
        finally:
            pump.close()
        assert error is not None and 'no such snapshot' in error

    def test_finish_send_stops_a_send_that_is_still_producing(self, children, payload):
        send = children.send(script(SEND_THEN_HANG, payload.src, 5000))
        pump = local._Pump(send)
        try:
            pump.prepare()
            pump.read_head(312)
            error = pump.finish_send()
        finally:
            pump.close()
        assert send.poll() is not None
        assert error is None, "our own stop is not the send's failure"
        assert 'SIGTERM' in pump.send_error, "but it stays recorded"


# ---------------------------------------------------------------------------------------------
# The pumping process dies
# ---------------------------------------------------------------------------------------------


def _driver(send_pidfile, recv_pidfile) -> str:
    return (
        "import sys\n"
        "from libzfseasy.zfs import ReceiveCommand, SendCommand\n"
        "from zfsbackup import local\n"
        f"py = {PY!r}\n"
        f"send = SendCommand._exec_stream([py, '-c', {ENDLESS_SEND!r}, {str(send_pidfile)!r}])\n"
        "pump = local._Pump(send)\n"
        "pump.prepare()\n"
        "pump.read_head(312)\n"
        f"recv = ReceiveCommand._exec_stream_in([py, '-c', {ENDLESS_SINK!r}, {str(recv_pidfile)!r}])\n"
        "pump.attach_receive(recv)\n"
        "pump.run()\n"
    )


@pytest.mark.slow
@pytest.mark.skipif(not hasattr(os, 'killpg'), reason="needs process groups")
class TestThePumpingProcessIsKilled:
    def test_both_children_see_eof_or_epipe_and_exit(self, tmp_path):
        send_pidfile, recv_pidfile = tmp_path / 'send.pid', tmp_path / 'recv.pid'
        driver = subprocess.Popen(
            [PY, '-c', _driver(send_pidfile, recv_pidfile)],
            env=hermetic_env(), start_new_session=True,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )
        pids = []
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if driver.poll() is not None:
                    pytest.fail(f"the driver exited early: {driver.stderr.read().decode()[-2000:]}")
                if all(p.exists() and p.read_text().strip() for p in (send_pidfile, recv_pidfile)):
                    break
                time.sleep(0.05)
            else:
                pytest.fail("the children did not start")
            pids = [int(send_pidfile.read_text()), int(recv_pidfile.read_text())]
            time.sleep(0.5)                       # the stream is flowing
            assert all(alive(p) for p in pids)
            os.kill(driver.pid, signal.SIGKILL)
            driver.wait(timeout=10)
            assert wait_dead(pids, 20), "a child outlived the pumping process"
        finally:
            for pid in pids:
                if alive(pid):
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            try:
                os.killpg(driver.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            driver.wait(timeout=10)
            if driver.stderr is not None:
                driver.stderr.close()


# ---------------------------------------------------------------------------------------------
# The pump over fake handles: what a real child cannot be made to do
# ---------------------------------------------------------------------------------------------


class TestPumpOverFakeHandles:
    @pytest.fixture
    def parts(self):
        made = []

        def build(send, receive=None):
            made.append((send, receive))
            pump = local._Pump(send)
            pump.prepare()
            if receive is not None:
                pump.attach_receive(receive)
            return pump

        yield build
        for _, receive in made:
            if receive is not None:
                receive.release()

    def test_an_abandoned_child_is_recorded_and_close_does_not_wait(self, parts):
        send = FakeSend(b'h' * 400, running=True, keep_open=True, abandon=True, pid=777)
        pump = parts(send)
        pump.read_head(312)
        pump.close()
        assert pump.abandoned == [('send', 777)]
        assert 'abandoned' in pump.send_error
        assert send.terminates == 1 and send.closes == 1

    def test_a_stalled_pair_with_abandoned_children_lists_both(self, parts, mocker):
        mocker.patch.object(local, 'LOCAL_STALL_TIMEOUT', 0.1)
        send = FakeSend(b'h' * 400, running=True, keep_open=True, abandon=True, pid=11)
        receive = FakeReceive(hang=True, abandon=True, pid=22)
        pump = parts(send, receive)
        pump.read_head(312)
        try:
            assert pump.run() == local.PUMP_STALLED
        finally:
            pump.close()
        assert pump.abandoned == [('send', 11), ('receive', 22)]
        assert 'abandoned' in pump.send_error and 'abandoned' in pump.receive_error

    def test_a_finished_child_is_not_terminated(self, parts):
        send = FakeSend(b'h' * 400, rc=0)
        pump = parts(send)
        pump.read_head(312)
        pump.close()
        assert send.terminates == 0 and send.closes == 1 and pump.send_error is None

    def test_a_child_is_terminated_at_most_once(self, parts):
        send = FakeSend(b'h' * 400, running=True, keep_open=True)
        pump = parts(send)
        pump.read_head(312)
        pump._terminate('send')
        pump._terminate('send')
        pump.close()
        assert send.terminates == 1

    def test_an_error_closing_the_send_does_not_keep_the_receive_open(self, parts):
        send = FakeSend(b'h' * 400, rc=9, stderr='send exploded')
        receive = FakeReceive()
        pump = parts(send, receive)
        pump.read_head(312)
        pump.close()
        assert pump.send_error == 'send exploded'
        assert receive.closes == 1, "the receive is closed in a finally: whatever the send's close raised"

    def test_the_receive_is_written_only_at_its_descriptor(self, parts):
        data = b'S' * 1000
        send = FakeSend(data)
        receive = FakeReceive()
        pump = parts(send, receive)
        assert pump.read_head(312) == data
        assert pump.run() == local.PUMP_OK
        pump.close()
        assert receive.received() == data

    def test_a_broken_pipe_to_the_receive_is_receive_gone(self, parts):
        send = FakeSend(b'S' * 1000, running=True, keep_open=True)
        receive = FakeReceive(dead_rc=1, stderr='cannot receive: bad stream')
        pump = parts(send, receive)
        pump.read_head(312)
        assert pump.run() == local.PUMP_RECEIVE_GONE
        pump.close()
        assert pump.receive_error == 'cannot receive: bad stream'
        assert send.terminated, "the send was stopped"


# ---------------------------------------------------------------------------------------------
# B3: several pumps in one process (the remote worker runs up to four sends at once)
# ---------------------------------------------------------------------------------------------


class TestConcurrentPumps:
    """A smoke test of the concurrency the remote worker now has: four sender threads each run a `_Pump` over
    a real send/receive pair while another thread keeps spawning children (the other senders' `zfs list`/`get`,
    a discovery). Pinned: all four streams arrive hash-equal within a hard bound, every receive saw EOF (none
    hangs at the commit bound), the spawning thread really did spawn meanwhile, and nothing is left running.

    It does NOT detect a missing `close_fds` on the children: since PEP 446 every pipe fd is created
    non-inheritable, so a child spawned by one thread cannot inherit another pump's pipe ends on this platform
    either way (the `close_fds=False` mutation survives, by design)."""

    PUMPS = 4

    def test_four_pumps_at_once_every_receive_sees_eof_while_other_threads_spawn_children(self, children, tmp_path):
        import threading

        from _sendloop_support import bound

        payloads = []
        for i in range(self.PUMPS):
            src = tmp_path / f'src{i}.bin'
            src.write_bytes(os.urandom(2 * 1024 * 1024 + 17 * i))
            payloads.append(types.SimpleNamespace(src=src, dst=tmp_path / f'dst{i}.bin', data=src.read_bytes()))
        results, errors = {}, []
        go = threading.Barrier(self.PUMPS + 1)
        stop, warm = threading.Event(), threading.Event()

        def pump_thread(i):
            p = payloads[i]
            try:
                go.wait(bound())
                warm.wait(bound())                  # spawning is under way before the first pump starts
                results[i] = pump_run(children, script(SEND_FILE, p.src), script(RECV_SINK, p.dst))
            except BaseException as e:  # noqa: BLE001
                errors.append((i, repr(e)))

        spawned = []

        def spawner():
            try:
                go.wait(bound())
                while not stop.is_set():
                    handle = children.send(script('pass'), peek=False)
                    spawned.append(handle)
                    handle.close()
                    if len(spawned) >= 3:
                        warm.set()
                    time.sleep(0.005)
            except BaseException as e:  # noqa: BLE001
                errors.append(('spawner', repr(e)))
                warm.set()

        threads = [threading.Thread(target=pump_thread, args=(i,), daemon=True) for i in range(self.PUMPS)]
        spawn = threading.Thread(target=spawner, daemon=True)
        for t in threads + [spawn]:
            t.start()
        for t in threads:
            t.join(bound(90))
        stop.set()
        spawn.join(bound())
        assert not any(t.is_alive() for t in threads), 'a pump never finished: a receive did not see EOF'
        assert errors == []
        assert len(spawned) >= 3, 'the other threads really did spawn children meanwhile (and before the pumps began)'
        for i, p in enumerate(payloads):
            assert results[i].outcome == local.PUMP_OK, i
            assert results[i].pump.send_error is None and results[i].pump.receive_error is None
            assert sha(p.dst.read_bytes()) == sha(p.data), f'pump {i}: the stream arrived intact'

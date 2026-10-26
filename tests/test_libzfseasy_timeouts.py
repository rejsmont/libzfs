"""Milestone D, D6 and D7 in libzfseasy: the slow-command watchdog and the opt-in `timeout()`.

* D6  `slow_command_warning`: a command still running after N seconds is reported at WARNING on the 'libzfseasy'
      logger, and again at each further multiple, by one daemon thread per watched call. The watchdog only logs.
* D7  `with libzfseasy.timeout(s):` bounds every non-streaming command to `s` seconds each. The deadline is read at the
      first `next()` of `Command._exec_lines`; an expired call ends its child (TERM, then KILL, each waited for at most
      `_EXPIRY_WAIT`) and raises `ZFSTimeoutError`. A child that survives KILL is registered in `_stuck`, and every later
      *timed* call is refused before `Popen` until it exits.

Real children are `sys.executable -c ...` (the guard in conftest only objects to a real zfs/zpool); the stuck-child
registry is driven with fake process objects, since a real unkillable process cannot be made on demand. Nothing here
needs a pool. Time bounds are generous: the host is sometimes loaded.
"""

import gc
import logging
import os
import random
import re
import selectors
import signal
import subprocess
import sys
import threading
import time
import types
from unittest.mock import MagicMock

import pytest

import libzfseasy
from libzfseasy.types import Filesystem
from libzfseasy import zfs as zfs_module
from libzfseasy.zfs import (
    Command, ReceiveCommand, SendCommand, StreamHandle, ZFSTimeoutError, _SlowCommandWatchdog, _StuckChild,
    _timeout_var, timeout as zfs_timeout,
)

from _hermetic import hermetic_env
from _timer_support import coarse_timer_reason, timer_stretch

pytestmark = [pytest.mark.unit, pytest.mark.subprocess]

WATCHDOG = 'libzfseasy-watchdog'
SLEEP = 'import time\ntime.sleep(120)\n'
HELLO_THEN_SLEEP = 'import sys, time\nsys.stdout.write("first\\n"); sys.stdout.flush()\ntime.sleep(120)\n'
READY_THEN_SLEEP = 'import sys, time\nsys.stdout.write("ready\\n"); sys.stdout.flush()\ntime.sleep(120)\n'
READY_IGNORE_TERM = (
    'import signal, sys, time\n'
    'signal.signal(signal.SIGTERM, signal.SIG_IGN)\n'
    'sys.stdout.write("ready\\n"); sys.stdout.flush()\n'
    'time.sleep(120)\n'
)


def py(script, *args):
    return [sys.executable, '-c', script, *args]


def label_of(cmd):
    """What a message calls `cmd`: the binary's base name and its first argument."""
    return f'{os.path.basename(cmd[0])} {cmd[1]}'


# ---------------------------------------------------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clean_state():
    """The registry and the warning setting are module state: every test gets them empty/off and the originals are
    put back afterwards. The warning is OFF by default here so a test about the timeout is not also about a watchdog
    thread; the watchdog tests switch it on."""
    saved_lock = zfs_module._stuck_lock
    saved_entries = list(zfs_module._stuck)
    saved_warning = zfs_module.slow_command_warning
    zfs_module._stuck.clear()
    zfs_module.slow_command_warning = None
    assert _timeout_var.get() is None
    yield
    zfs_module._stuck_lock = saved_lock
    zfs_module._stuck[:] = saved_entries
    zfs_module.slow_command_warning = saved_warning


class Kids:
    """Records every real child started through `subprocess.Popen` (and how often it was terminated/killed), the stderr
    reader threads, and kills every survivor (and any registered extra pid) when the test ends."""

    def __init__(self, monkeypatch):
        self.procs = []
        self.terminated = {}
        self.killed = {}
        self.stderr_threads = []
        self.extra_pids = []
        self.unkillable = False   # while True: poll() says "running" and a timed wait() times out, whatever really is
        kids = self
        real_popen = subprocess.Popen  # the (guarded) class; subclassing keeps the guard

        class Recording(real_popen):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                kids.procs.append(self)

            def poll(self):
                return None if kids.unkillable else super().poll()

            def wait(self, timeout=None):
                if kids.unkillable and timeout is not None:
                    raise subprocess.TimeoutExpired(self.args, timeout)
                return super().wait(timeout=timeout)

            def terminate(self):
                kids.terminated[self.pid] = kids.terminated.get(self.pid, 0) + 1
                super().terminate()

            def kill(self):
                kids.killed[self.pid] = kids.killed.get(self.pid, 0) + 1
                super().kill()

        monkeypatch.setattr(subprocess, 'Popen', Recording)
        real_start = Command._start_stderr_reader_lines

        def recording_start(process, errors):
            thread = real_start(process, errors)
            kids.stderr_threads.append(thread)
            return thread

        monkeypatch.setattr(Command, '_start_stderr_reader_lines', staticmethod(recording_start))

    def bounded(self, fn, limit=60):
        """`fn()` on a thread; (result, exception). On overrun the children are killed and the test fails, so a
        deadline regression is a failure and not a hang of the suite."""
        box = {}

        def target():
            try:
                box['result'] = fn()
            except BaseException as exc:  # noqa: BLE001 - reported to the test
                box['exc'] = exc

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(limit)
        if thread.is_alive():
            self.kill_all()
            thread.join(10)
            pytest.fail(f'did not finish within {limit}s: the deadline is not enforced')
        return box.get('result'), box.get('exc')

    def kill_all(self):
        self.unkillable = False
        for pid in self.extra_pids:
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
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


@pytest.fixture
def selectors_seen(monkeypatch):
    """Every selector `_read_until` creates, with a `closed` flag; `interrupt_after` makes the Nth `select` raise
    KeyboardInterrupt."""
    base = selectors.DefaultSelector
    state = types.SimpleNamespace(instances=[], interrupt_after=None, calls=0)

    class Recording(base):
        def __init__(self):
            super().__init__()
            self.closed = False
            state.instances.append(self)

        def select(self, timeout=None):
            state.calls += 1
            if state.interrupt_after is not None and state.calls > state.interrupt_after:
                raise KeyboardInterrupt()
            return super().select(timeout)

        def close(self):
            self.closed = True
            super().close()

    monkeypatch.setattr(selectors, 'DefaultSelector', Recording)
    return state


def run_lines(cmd, seconds=None, sink=None):
    """All lines of `cmd` through `_exec_lines`, optionally under `timeout(seconds)`. Lines go to `sink` as they
    arrive, so a failing run still reports what came before the exception."""
    sink = [] if sink is None else sink
    if seconds is None:
        for line in Command._exec_lines(cmd):
            sink.append(line)
    else:
        with zfs_timeout(seconds):
            for line in Command._exec_lines(cmd):
                sink.append(line)
    return sink


def outcome(kids_, cmd, seconds=None):
    sink = []
    _, exc = kids_.bounded(lambda: run_lines(cmd, seconds, sink))
    return sink, (None if exc is None else type(exc)), (None if exc is None else str(exc))


def watchdog_threads():
    return [t for t in threading.enumerate() if t.name == WATCHDOG and t.is_alive()]


def wait_until(predicate, limit=10.0):
    end = time.monotonic() + limit
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def no_new_watchdogs(before):
    return wait_until(lambda: not [t for t in watchdog_threads() if t not in before])


def slow_records(caplog):
    return [r for r in caplog.records
            if r.name == 'libzfseasy' and r.levelno == logging.WARNING and 'is still running after' in r.getMessage()]


def open_fds():
    return len(os.listdir('/dev/fd'))


HAS_DEV_FD = os.path.isdir('/dev/fd')


# ---------------------------------------------------------------------------------------------------------------------
# D6: the slow-command watchdog
# ---------------------------------------------------------------------------------------------------------------------

class TestSlowCommandWatchdog:

    def test_a_sleeping_child_is_reported_once_per_interval(self, caplog, kids):
        zfs_module.slow_command_warning = 0.2
        cmd = py('import sys, time\nsys.stdout.write("x\\n"); sys.stdout.flush()\ntime.sleep(1.0)\n')
        started = time.monotonic()
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            lines, exc = kids.bounded(lambda: run_lines(cmd))
        elapsed = time.monotonic() - started
        assert exc is None and lines == ['x']

        records = slow_records(caplog)
        assert 2 <= len(records) <= int(elapsed / 0.2) + 1
        pid = kids.procs[0].pid
        for record in records:
            message = record.getMessage()
            assert message.startswith(f'{label_of(cmd)} (pid {pid}) is still running after ')
            assert 'every 0.2 s' in message
        elapsed_values = [float(re.search(r'running after ([0-9.]+) s', r.getMessage()).group(1)) for r in records]
        assert elapsed_values == sorted(elapsed_values) and elapsed_values[0] >= 0.1
        assert elapsed_values[-1] <= elapsed + 0.2

    def test_a_fast_child_logs_nothing(self, caplog, kids):
        zfs_module.slow_command_warning = 5
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            assert run_lines(py('print("a")')) == ['a']
        assert slow_records(caplog) == []

    def test_the_thread_is_alive_while_the_child_runs_and_gone_afterwards(self, kids):
        zfs_module.slow_command_warning = 30
        before = set(watchdog_threads())
        gen = Command._exec_lines(py('import sys, time\nprint("a", flush=True)\ntime.sleep(0.5)\n'))
        assert next(gen) == 'a'
        running = [t for t in watchdog_threads() if t not in before]
        assert len(running) == 1 and running[0].daemon
        assert list(gen) == []
        assert no_new_watchdogs(before)

    @pytest.mark.parametrize('how', ['failure', 'close', 'delete', 'timeout'])
    def test_no_thread_survives_any_way_out(self, kids, how):
        zfs_module.slow_command_warning = 30
        before = set(watchdog_threads())
        if how == 'failure':
            with pytest.raises(Exception, match='Boom'):
                list(Command._exec_lines(py('import sys\nsys.stderr.write("boom\\n")\nsys.exit(1)\n')))
        elif how in ('close', 'delete'):
            gen = Command._exec_lines(py(READY_THEN_SLEEP))
            assert next(gen) == 'ready'
            assert [t for t in watchdog_threads() if t not in before]
            if how == 'close':
                gen.close()
            else:
                del gen
                gc.collect()
        else:
            _, exc = kids.bounded(lambda: run_lines(py(SLEEP), 0.4))
            assert isinstance(exc, ZFSTimeoutError)
        assert no_new_watchdogs(before)

    def test_exec_collect_is_watched_too(self, caplog, kids):
        zfs_module.slow_command_warning = 0.2
        before = set(watchdog_threads())
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            lines, errors, rc = Command._exec_collect(py('import time\ntime.sleep(0.8)\nprint("done")\n'))
        assert (lines, errors, rc) == (['done'], [], 0)
        assert len(slow_records(caplog)) >= 2
        assert no_new_watchdogs(before)

    @pytest.mark.parametrize('value', [None, 0, 0.0, -1, -0.5, True, False, '3', [], float('nan'), float('inf'),
                                       object()])
    def test_an_off_or_unusable_setting_starts_no_thread_and_does_not_fail_the_command(self, monkeypatch, kids, value):
        started = []

        class Recording(threading.Thread):
            def start(self):
                started.append(self)
                super().start()

        monkeypatch.setattr(zfs_module, '_Thread', Recording)
        zfs_module.slow_command_warning = value
        assert run_lines(py('print("a")')) == ['a']
        assert started == []

    def test_a_usable_setting_does_start_exactly_one_thread(self, monkeypatch, kids):
        """The positive control of the test above: without it, a recording that never records would pass."""
        started = []

        class Recording(threading.Thread):
            def start(self):
                started.append(self)
                super().start()

        monkeypatch.setattr(zfs_module, '_Thread', Recording)
        for value in (60, 0.5, 5.5):
            started.clear()
            zfs_module.slow_command_warning = value
            assert run_lines(py('print("a")')) == ['a']
            assert len(started) == 1 and started[0].name == WATCHDOG

    def test_the_setting_is_read_per_command(self, caplog, kids):
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            zfs_module.slow_command_warning = None
            run_lines(py('import time\ntime.sleep(0.5)\n'))
            assert slow_records(caplog) == []
            zfs_module.slow_command_warning = 0.2
            run_lines(py('import time\ntime.sleep(0.6)\n'))
            assert slow_records(caplog)

    def test_a_thread_that_cannot_start_does_not_fail_the_command(self, monkeypatch, caplog, kids):
        class Refusing(threading.Thread):
            def start(self):
                raise RuntimeError("can't start new thread")

        monkeypatch.setattr(zfs_module, '_Thread', Refusing)
        zfs_module.slow_command_warning = 0.1
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            assert run_lines(py('import time\nprint("a")\ntime.sleep(0.4)\n')) == ['a']
            assert Command._exec_collect(py('print("b")'))[0] == ['b']
        assert slow_records(caplog) == []

    def test_a_failing_watchdog_start_does_not_leave_a_cancelled_thread_member(self):
        dog = _SlowCommandWatchdog(['zfs', 'list'], 1, 5.0)

        class Refusing(threading.Thread):
            def start(self):
                raise RuntimeError('no threads')

        original = zfs_module._Thread
        zfs_module._Thread = Refusing
        try:
            dog.start()
        finally:
            zfs_module._Thread = original
        assert dog._thread is None
        dog.cancel()  # still safe

    def test_nothing_is_redacted_until_a_warning_is_logged(self, monkeypatch, caplog, kids):
        calls = []
        real = zfs_module._redact
        monkeypatch.setattr(zfs_module, '_redact', lambda arg: calls.append(arg) or real(arg))
        zfs_module.slow_command_warning = 30
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            assert run_lines(py('print("a")')) == ['a']
        assert calls == []                       # not for the debug line (WARNING level), not for the watchdog

        zfs_module.slow_command_warning = 0.1
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            run_lines(py('import time\ntime.sleep(0.6)\n'))
        assert slow_records(caplog) and calls    # redacted only for the label of a warning actually logged

    def test_a_label_that_cannot_be_built_still_logs_the_warning(self, monkeypatch, caplog, kids):
        def broken(cmd):
            raise RuntimeError('no label')

        monkeypatch.setattr(zfs_module, '_command_label', broken)
        zfs_module.slow_command_warning = 0.1
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            run_lines(py('import time\ntime.sleep(0.5)\n'))
        records = slow_records(caplog)
        assert records and records[0].getMessage().startswith('command (pid ')

    def test_credentials_in_the_argv_never_reach_the_warning(self, caplog, kids):
        zfs_module.slow_command_warning = 0.1
        cmd = py('import time\ntime.sleep(0.5)\n', 'keylocation=https://user:secret@host/key')
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            run_lines(cmd)
        assert slow_records(caplog)
        assert 'secret' not in caplog.text

    # -- the watchdog thread itself ---------------------------------------------------------------------------------

    @staticmethod
    def _stub_logger(monkeypatch, stall=None):
        records = []

        class Stub:
            def warning(self, fmt, *args):
                records.append(time.monotonic())
                if stall is not None and len(records) == 1:
                    stall()

            def isEnabledFor(self, level):
                return False

            def debug(self, *a):
                pass

        monkeypatch.setattr(zfs_module, 'logger', Stub())
        return records

    def test_cancel_joins_so_no_thread_outlives_the_call_even_without_polling(self):
        dog = _SlowCommandWatchdog(['zfs', 'list'], 1, 30.0)
        dog.start()
        thread = dog._thread
        assert thread is not None and thread.is_alive()
        dog.cancel()
        assert not thread.is_alive()
        dog.cancel()                                   # idempotent

    def test_a_finished_call_leaves_no_watchdog_thread_at_the_moment_it_returns(self, kids):
        zfs_module.slow_command_warning = 30
        before = set(watchdog_threads())
        assert run_lines(py('print("a")')) == ['a']
        assert [t for t in watchdog_threads() if t not in before] == []

    def test_cancel_does_not_wait_for_a_thread_stuck_in_a_log_call(self, monkeypatch):
        release = threading.Event()
        entered = threading.Event()
        records = self._stub_logger(monkeypatch, stall=lambda: (entered.set(), release.wait(30)))
        dog = _SlowCommandWatchdog(['zfs', 'list'], 1, 0.1)
        dog.start()
        try:
            assert entered.wait(15), 'the watchdog never logged'
            started = time.monotonic()
            dog.cancel()
            assert time.monotonic() - started < zfs_module._WATCHDOG_JOIN + 2.0   # bounded by the join, not the stall
            assert dog._thread.is_alive()                                         # left behind, not waited for
        finally:
            release.set()
            dog._thread.join(15)
        assert not dog._thread.is_alive()

    def test_a_stalled_log_call_does_not_cause_a_burst_of_catch_up_warnings(self, monkeypatch):
        records = self._stub_logger(monkeypatch, stall=lambda: time.sleep(0.9))
        dog = _SlowCommandWatchdog(['zfs', 'list'], 1, 0.2)
        dog.start()
        time.sleep(1.8)
        dog.cancel()
        assert len(records) >= 2
        stall_end = records[0] + 0.9
        # Without the skip, every multiple the stall swallowed (0.4, 0.6, 0.8, 1.0) is logged back to back the moment it
        # ends. With it, at most one warning for the stall itself.
        assert sum(1 for t in records[1:] if t - stall_end < 0.05) <= 1, [t - stall_end for t in records[1:]]
        assert len(records) <= 7

    def test_a_watchdog_cancelled_between_wake_up_and_logging_stays_silent(self, monkeypatch):
        records = self._stub_logger(monkeypatch)

        class Racy:
            """wait() reports a timeout (time to log) but the watchdog was cancelled in the meantime."""
            calls = 0

            def wait(self, timeout=None):
                Racy.calls += 1
                return Racy.calls > 1          # the second wait ends the loop, so a missing check cannot spin

            def is_set(self):
                return True

            def set(self):
                pass

        dog = _SlowCommandWatchdog(['zfs', 'list'], 1, 0.1)
        dog._stop = Racy()
        dog._run()
        assert records == []

    def test_cancel_from_the_watchdog_thread_itself_does_not_join_itself(self, monkeypatch):
        errors = []
        dog = _SlowCommandWatchdog(['zfs', 'list'], 1, 0.05)

        def logging_stub():
            try:
                dog.cancel()
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        self._stub_logger(monkeypatch, stall=logging_stub)
        dog.start()
        assert wait_until(lambda: not dog._thread.is_alive(), 10)
        assert errors == []

    def test_the_default_is_60_seconds(self):
        proc = subprocess.run(
            [sys.executable, '-I', '-c', 'import libzfseasy, libzfseasy.zfs as z; '
             'print(libzfseasy.slow_command_warning, z.slow_command_warning)'],
            capture_output=True, text=True, timeout=60,
            env=hermetic_env(PYTHONPATH=os.pathsep.join(sys.path)))
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.split() == ['60', '60']


# ---------------------------------------------------------------------------------------------------------------------
# D6: the setting, package vs submodule
# ---------------------------------------------------------------------------------------------------------------------

class TestSlowCommandWarningSetting:

    def test_package_and_submodule_are_one_setting(self):
        libzfseasy.slow_command_warning = 12.5
        assert zfs_module.slow_command_warning == 12.5 and libzfseasy.slow_command_warning == 12.5
        zfs_module.slow_command_warning = 7
        assert libzfseasy.slow_command_warning == 7
        libzfseasy.slow_command_warning = None
        assert zfs_module.slow_command_warning is None and libzfseasy.slow_command_warning is None

    def test_the_package_is_still_a_module_and_import_as_works(self):
        import libzfseasy as zfs
        assert isinstance(zfs, types.ModuleType)
        assert sys.modules['libzfseasy'] is zfs and zfs.list is libzfseasy.list
        zfs.slow_command_warning = 3
        assert zfs_module.slow_command_warning == 3

    @pytest.mark.parametrize('value', [True, False, '3', '', [], b'1', float('nan'), object()])
    def test_the_package_setter_rejects_nonsense_and_keeps_the_old_value(self, value):
        libzfseasy.slow_command_warning = 9
        with pytest.raises(ValueError, match='slow_command_warning'):
            libzfseasy.slow_command_warning = value
        assert zfs_module.slow_command_warning == 9

    @pytest.mark.parametrize('value', [1, 0.001, 60, 3600.5, None, 0, 0.0, -1, -0.5, float('inf'), float('-inf')])
    def test_the_package_setter_accepts_numbers_and_none(self, value):
        """One rule for both spellings: positive finite on; None, zero, negative, infinite off (all accepted)."""
        libzfseasy.slow_command_warning = value
        assert zfs_module.slow_command_warning == value
        assert libzfseasy.slow_command_warning == value

    @pytest.mark.parametrize('value, interval', [(1, 1.0), (0.25, 0.25), (60, 60.0), (None, None), (0, None),
                                                 (-1, None), (-0.5, None), (float('inf'), None),
                                                 (float('-inf'), None), (True, None), ('3', None),
                                                 (float('nan'), None), ([], None)])
    def test_both_spellings_read_the_same_rule(self, value, interval):
        zfs_module.slow_command_warning = value          # the submodule is not validated; a read treats bad as off
        assert zfs_module._slow_interval() == interval

    def test_the_default_constant_and_the_deleter(self):
        assert zfs_module.DEFAULT_SLOW_COMMAND_WARNING == 60
        libzfseasy.slow_command_warning = 0.5
        del libzfseasy.slow_command_warning
        assert zfs_module.slow_command_warning == 60 and libzfseasy.slow_command_warning == 60
        libzfseasy.slow_command_warning = None
        del libzfseasy.slow_command_warning
        assert libzfseasy.slow_command_warning == 60

    def test_mock_patch_object_on_the_package_works_and_restores_the_default(self, kids, caplog):
        from unittest import mock
        libzfseasy.slow_command_warning = 60
        with mock.patch.object(libzfseasy, 'slow_command_warning', 0.2):
            assert zfs_module.slow_command_warning == 0.2
            with caplog.at_level(logging.WARNING, logger='libzfseasy'):
                run_lines(py('import time\ntime.sleep(0.7)\n'))
            assert slow_records(caplog)
        assert zfs_module.slow_command_warning == 60 and libzfseasy.slow_command_warning == 60

    def test_mock_patch_object_with_none_turns_it_off_inside_the_block(self):
        from unittest import mock
        libzfseasy.slow_command_warning = 60
        with mock.patch.object(libzfseasy, 'slow_command_warning', None):
            assert zfs_module._slow_interval() is None
        assert zfs_module._slow_interval() == 60.0

    def test_the_submodule_is_not_validated_and_a_bad_value_just_turns_the_warning_off(self, kids):
        zfs_module.slow_command_warning = 'soon'
        assert zfs_module._slow_interval() is None
        assert run_lines(py('print("a")')) == ['a']

    def test_a_setting_made_on_the_package_takes_effect(self, caplog, kids):
        libzfseasy.slow_command_warning = 0.2
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            run_lines(py('import time\ntime.sleep(0.8)\n'))
        assert len(slow_records(caplog)) >= 2

    def test_the_exports(self):
        assert libzfseasy.timeout is zfs_module.timeout
        assert libzfseasy.ZFSTimeoutError is ZFSTimeoutError
        assert issubclass(ZFSTimeoutError, Exception)
        assert not issubclass(ZFSTimeoutError, (OSError, ValueError, TimeoutError))


# ---------------------------------------------------------------------------------------------------------------------
# D6: StreamHandle.close
# ---------------------------------------------------------------------------------------------------------------------

class TestStreamHandleWatchdog:

    @staticmethod
    def _handle(wait):
        process = MagicMock()
        process.args = ['zfs', 'send', 'pool/fs@snap']
        process.pid = 4242
        process.wait.side_effect = wait
        process.stderr = None
        stream = MagicMock()
        stream.closed = False
        return StreamHandle(process, stream, [], None), process

    def test_a_slow_wait_is_logged_once_and_the_thread_ends_with_the_close(self, caplog):
        zfs_module.slow_command_warning = 0.5
        before = set(watchdog_threads())
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            def wait():
                assert wait_until(lambda: len(slow_records(caplog)) >= 1, 15), 'no warning came'
                return 0

            handle, process = self._handle(wait)
            handle.close()
            assert len(slow_records(caplog)) == 1
            assert no_new_watchdogs(before)
            time.sleep(0.8)                      # past the next multiple: a cancelled watchdog must stay silent
            assert len(slow_records(caplog)) == 1
        message = slow_records(caplog)[0].getMessage()
        assert message.startswith('zfs send (pid 4242) is still running after ')
        process.wait.assert_called_once_with()

    def test_a_fast_wait_logs_nothing_and_leaves_no_thread(self, caplog):
        zfs_module.slow_command_warning = 0.2
        before = set(watchdog_threads())
        handle, _ = self._handle(lambda: 0)
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            handle.close()
            time.sleep(0.5)
        assert slow_records(caplog) == []
        assert no_new_watchdogs(before)

    def test_an_interrupted_wait_cancels_the_watchdog_and_leaves_the_handle_open(self, caplog):
        zfs_module.slow_command_warning = 0.2
        before = set(watchdog_threads())
        calls = []

        def wait():
            calls.append(1)
            if len(calls) == 1:
                raise KeyboardInterrupt()
            return 0

        handle, _ = self._handle(wait)
        with pytest.raises(KeyboardInterrupt):
            handle.close()
        assert no_new_watchdogs(before)
        assert handle._closed is False
        handle.close()
        assert handle._closed is True

    def test_off_means_no_thread_for_the_close(self, monkeypatch):
        started = []

        class Recording(threading.Thread):
            def start(self):
                started.append(1)
                super().start()

        monkeypatch.setattr(zfs_module, '_Thread', Recording)
        zfs_module.slow_command_warning = None
        handle, _ = self._handle(lambda: 0)
        handle.close()
        assert started == []

    def test_a_process_without_args_or_pid_is_still_watched_and_closed(self, caplog):
        zfs_module.slow_command_warning = 0.2
        process = types.SimpleNamespace(wait=lambda: (time.sleep(0.6), 0)[1], stderr=None)
        stream = MagicMock()
        stream.closed = False
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            StreamHandle(process, stream, [], None).close()
        records = slow_records(caplog)
        assert records and records[0].getMessage().startswith('command (pid None) is still running')


# ---------------------------------------------------------------------------------------------------------------------
# D7: the timeout() context manager
# ---------------------------------------------------------------------------------------------------------------------

class TestTimeoutContext:

    @pytest.mark.parametrize('value', [0, 0.0, -1, -0.5, float('nan'), float('inf'), float('-inf'), True, False, '5',
                                       [], b'1', 1 + 0j])
    def test_invalid_values_are_refused_on_entry_and_nothing_is_set(self, value):
        with pytest.raises(ValueError, match='timeout'):
            with zfs_timeout(value):
                pytest.fail('the block must not run')
        assert _timeout_var.get() is None

    @pytest.mark.parametrize('value', [1, 7, 0.25, 3600])
    def test_valid_values_are_kept_as_float(self, value):
        with zfs_timeout(value):
            assert _timeout_var.get() == float(value) and isinstance(_timeout_var.get(), float)
        assert _timeout_var.get() is None

    def test_nesting_innermost_wins_and_each_exit_restores(self):
        seen = []
        with zfs_timeout(5):
            seen.append(_timeout_var.get())
            with zfs_timeout(2):
                seen.append(_timeout_var.get())
                with zfs_timeout(9):
                    seen.append(_timeout_var.get())
                seen.append(_timeout_var.get())
            seen.append(_timeout_var.get())
        seen.append(_timeout_var.get())
        assert seen == [5.0, 2.0, 9.0, 2.0, 5.0, None]

    def test_none_lifts_an_outer_limit(self):
        with zfs_timeout(5):
            with zfs_timeout(None):
                assert _timeout_var.get() is None
            assert _timeout_var.get() == 5.0

    def test_an_invalid_inner_value_leaves_the_outer_limit_alone(self):
        with zfs_timeout(5):
            with pytest.raises(ValueError):
                with zfs_timeout(-1):
                    pass
            assert _timeout_var.get() == 5.0

    def test_the_limit_is_reset_when_the_block_exits_by_exception(self):
        with pytest.raises(RuntimeError):
            with zfs_timeout(3):
                raise RuntimeError('x')
        assert _timeout_var.get() is None
        with zfs_timeout(5):
            with pytest.raises(RuntimeError):
                with zfs_timeout(3):
                    raise RuntimeError('x')
            assert _timeout_var.get() == 5.0

    def test_the_limit_is_per_thread_and_a_copied_context_inherits_it(self):
        import contextvars
        seen = {}
        with zfs_timeout(3):
            thread = threading.Thread(target=lambda: seen.setdefault('thread', _timeout_var.get()))
            thread.start()
            thread.join(10)
            seen['copy'] = contextvars.copy_context().run(_timeout_var.get)
        assert seen == {'thread': None, 'copy': 3.0}

    def test_a_new_thread_runs_untimed_under_its_parents_timeout(self, kids):
        out = {}

        def worker():
            out['lines'] = run_lines(py('import time\ntime.sleep(0.8)\nprint("late")\n'))

        with zfs_timeout(0.2):
            thread = threading.Thread(target=worker)
            thread.start()
            thread.join(60)
        assert out == {'lines': ['late']}


# ---------------------------------------------------------------------------------------------------------------------
# D7: a timed run that finishes is the same as an untimed one
# ---------------------------------------------------------------------------------------------------------------------

_EQUIVALENCE_CHILDREN = {
    'plain lines': 'import sys\nsys.stdout.buffer.write(b"a\\nb\\nc\\n")\n',
    'empty output': 'pass\n',
    'trailing fragment': 'import sys\nsys.stdout.buffer.write(b"a\\nlast without newline")\n',
    'only a fragment': 'import sys\nsys.stdout.buffer.write(b"no newline at all")\n',
    'invalid utf-8': 'import sys\nsys.stdout.buffer.write(b"a \\xff\\xfe b\\nok\\n")\n',
    'cr kept': 'import sys\nsys.stdout.buffer.write(b"keep\\rinside\\nclean\\r\\n\\r\\nlast\\n\\r")\n',
    'blank lines': 'import sys\nsys.stdout.buffer.write(b"\\n\\na\\n\\n\\n")\n',
    'tabs and spaces kept': 'import sys\nsys.stdout.buffer.write(b"  lead\\ttab\\t\\ntrail  \\n \\n")\n',
    'a line split across writes': ('import sys, time\nsys.stdout.write("ab"); sys.stdout.flush()\ntime.sleep(0.2)\n'
                                   'sys.stdout.write("cd\\nef\\n")\n'),
    'big output with stderr noise': ('import sys\n'
                                     'for i in range(3000):\n'
                                     '    sys.stderr.write("warn %d " % i + "e" * 200 + "\\n")\n'
                                     '    sys.stdout.write("row %d " % i + "o" * 200 + "\\n")\n'),
    'a very long line': 'import sys\nsys.stdout.write("x" * 300000 + "\\nshort\\n")\n',
    'stdout closed early, exit later': ('import os, time\nos.write(1, b"a\\n")\nos.close(1)\ntime.sleep(0.3)\n'),
    'non-zero rc with stderr': ('import sys\nsys.stdout.write("one\\ntwo\\n"); sys.stdout.flush()\n'
                                'sys.stderr.write("cannot do it\\n")\nsys.exit(1)\n'),
    'non-zero rc, no stderr': 'import sys\nsys.stdout.write("one\\n")\nsys.exit(3)\n',
    'rc, invalid utf-8 on stderr': ('import sys\nsys.stderr.buffer.write(b"bad \\xff thing\\n")\nsys.exit(2)\n'),
}


class TestTimedRunEqualsUntimedRun:

    @pytest.mark.parametrize('name', sorted(_EQUIVALENCE_CHILDREN))
    @pytest.mark.parametrize('mode', ['timed', 'watched', 'timed+watched'])
    def test_same_lines_same_exception(self, kids, name, mode):
        cmd = py(_EQUIVALENCE_CHILDREN[name])
        reference = outcome(kids, cmd)
        zfs_module.slow_command_warning = 0.05 if 'watched' in mode else None
        got = outcome(kids, cmd, seconds=60 if mode.startswith('timed') else None)
        assert got == reference
        sink, exc_type, text = got
        if 'non-zero' in name or name.startswith('rc,'):
            assert exc_type is Exception                  # the bare Exception, after the last line
        else:
            assert exc_type is None

    def test_the_concrete_framing(self, kids):
        """The same cases, pinned to values rather than to each other."""
        cases = [
            ('trailing fragment', ['a', 'last without newline']),
            ('invalid utf-8', ['a \udcff\udcfe b', 'ok']),
            ('cr kept', ['keep\rinside', 'clean\r', '\r', 'last', '\r']),
            ('blank lines', ['', '', 'a', '', '']),
            ('tabs and spaces kept', ['  lead\ttab\t', 'trail  ', ' ']),
            ('a line split across writes', ['abcd', 'ef']),
            ('stdout closed early, exit later', ['a']),
        ]
        for name, expected in cases:
            sink, exc_type, _ = outcome(kids, py(_EQUIVALENCE_CHILDREN[name]), seconds=60)
            assert (name, sink, exc_type) == (name, expected, None)

    def test_a_failing_child_raises_after_the_last_line_with_its_stderr(self, kids):
        sink, exc_type, text = outcome(kids, py(_EQUIVALENCE_CHILDREN['non-zero rc with stderr']), seconds=60)
        assert sink == ['one', 'two'] and exc_type is Exception and text == 'Cannot do it'
        sink, exc_type, text = outcome(kids, py(_EQUIVALENCE_CHILDREN['non-zero rc, no stderr']), seconds=60)
        assert sink == ['one'] and exc_type is Exception and text == 'Command failed with exit code 3'

    def test_a_finished_timed_call_leaves_nothing_behind(self, kids):
        before = set(watchdog_threads())
        lines, exc = kids.bounded(lambda: run_lines(py('print("a")'), 60))
        assert exc is None and lines == ['a']
        proc = kids.procs[0]
        assert proc.returncode == 0 and zfs_module._stuck == []
        assert not any(t.is_alive() for t in kids.stderr_threads)
        assert no_new_watchdogs(before)

    def test_each_command_gets_the_whole_budget(self, kids):
        stretch = timer_stretch()
        if stretch > 5:
            pytest.skip(coarse_timer_reason(stretch))
        started = time.monotonic()
        with zfs_timeout(2.5):
            for _ in range(2):
                assert run_lines(py('import time\ntime.sleep(1.4)\nprint("ok")\n')) == ['ok']
        assert time.monotonic() - started > 2.5            # the two together outlast one budget

    def test_a_lazy_consumer_pays_for_the_time_it_holds_the_iterator(self, kids):
        def consume():
            with zfs_timeout(0.5):
                gen = Command._exec_lines(py(READY_THEN_SLEEP))     # the child is still running
                assert next(gen) == 'ready'
                time.sleep(1.0)                           # the consumer is slow
                return list(gen)

        _, exc = kids.bounded(consume)
        assert isinstance(exc, ZFSTimeoutError) and 'timed out after 0.5s' in str(exc)
        assert kids.procs[0].returncode == -signal.SIGTERM and zfs_module._stuck == []

    def test_a_slow_consumer_of_a_child_that_has_finished_still_gets_every_row(self, kids):
        """Past the deadline, a child that has exited is drained, not reported as a timeout: its output is complete."""
        def consume():
            with zfs_timeout(0.5):
                gen = Command._exec_lines(py('import time\nprint("a", flush=True)\nprint("b", flush=True)\n'))
                assert next(gen) == 'a'
                time.sleep(1.0)
                return list(gen)

        result, exc = kids.bounded(consume)
        assert exc is None and result == ['b']
        assert kids.procs[0].returncode == 0 and zfs_module._stuck == []

    def test_the_limit_is_read_at_the_first_next_not_when_the_generator_is_made(self, kids):
        def created_inside_started_outside():
            with zfs_timeout(0.3):
                gen = Command._exec_lines(py('import time\ntime.sleep(1.0)\nprint("late")\n'))
            return list(gen)

        def created_outside_started_inside():
            gen = Command._exec_lines(py(SLEEP))
            with zfs_timeout(0.4):
                next(gen)

        result, exc = kids.bounded(created_inside_started_outside)
        assert exc is None and result == ['late']
        _, exc = kids.bounded(created_outside_started_inside)
        assert isinstance(exc, ZFSTimeoutError)

    def test_the_deadline_outlives_the_block_it_was_read_in(self, kids):
        def run():
            with zfs_timeout(1.0):
                gen = Command._exec_lines(py(READY_THEN_SLEEP))
                assert next(gen) == 'ready'
            return next(gen)                              # outside the block, same call

        started = time.monotonic()
        _, exc = kids.bounded(run)
        assert isinstance(exc, ZFSTimeoutError)
        assert time.monotonic() - started < 15

    def test_none_lifts_the_limit_for_a_command_inside_it(self, kids):
        def run():
            with zfs_timeout(0.2):
                with zfs_timeout(None):
                    return run_lines(py('import time\ntime.sleep(0.8)\nprint("done")\n'))

        result, exc = kids.bounded(run)
        assert exc is None and result == ['done']


# ---------------------------------------------------------------------------------------------------------------------
# D7: expiry against real children
# ---------------------------------------------------------------------------------------------------------------------

class TestTimeoutExpiry:

    @pytest.fixture(autouse=True)
    def _strict_join(self, monkeypatch):
        """Give the stderr reader time to end so that "stderr closed" is a fact and not a race with a loaded host."""
        monkeypatch.setattr(zfs_module, '_EXPIRY_JOIN', 3.0)

    @staticmethod
    def _assert_gone(pid):
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)

    def test_a_child_that_never_exits_is_stopped_at_the_deadline(self, kids, selectors_seen):
        cmd = py(HELLO_THEN_SLEEP)
        sink = []
        started = time.monotonic()
        _, exc = kids.bounded(lambda: run_lines(cmd, 0.6, sink))
        elapsed = time.monotonic() - started

        assert isinstance(exc, ZFSTimeoutError) and type(exc) is ZFSTimeoutError
        assert str(exc) == f'{label_of(cmd)} timed out after 0.6s; outcome unknown'
        assert exc.__cause__ is None and exc.__suppress_context__
        assert sink == ['first']
        assert 0.5 <= elapsed < 0.6 + 10
        proc = kids.procs[0]
        assert proc.returncode == -signal.SIGTERM                     # TERM was enough: no KILL
        assert kids.terminated == {proc.pid: 1} and kids.killed == {}
        self._assert_gone(proc.pid)
        assert zfs_module._stuck == []
        assert proc.stdout.closed and proc.stderr.closed
        assert not any(t.is_alive() for t in kids.stderr_threads)
        assert selectors_seen.instances and all(s.closed for s in selectors_seen.instances)

    def test_seconds_are_formatted_with_g(self, kids):
        _, exc = kids.bounded(lambda: run_lines(py(SLEEP), 1.25))
        assert str(exc).endswith('timed out after 1.25s; outcome unknown')
        _, exc = kids.bounded(lambda: run_lines(py(SLEEP), 1))
        assert str(exc).endswith('timed out after 1s; outcome unknown')

    def test_a_child_that_ignores_sigterm_is_killed_after_the_wait(self, kids):
        cmd = py(READY_IGNORE_TERM)
        sink = []
        started = time.monotonic()
        _, exc = kids.bounded(lambda: run_lines(cmd, 2.0, sink))
        elapsed = time.monotonic() - started

        assert isinstance(exc, ZFSTimeoutError), exc
        if sink != ['ready']:
            pytest.skip('the child had not set up before the deadline (loaded host)')
        assert str(exc) == f'{label_of(cmd)} timed out after 2s; outcome unknown'   # not "did not exit after SIGKILL"
        assert 2.0 + zfs_module._EXPIRY_WAIT - 0.2 <= elapsed < 2.0 + zfs_module._EXPIRY_WAIT + 10
        proc = kids.procs[0]
        assert proc.returncode == -signal.SIGKILL
        assert kids.terminated == {proc.pid: 1} and kids.killed == {proc.pid: 1}
        self._assert_gone(proc.pid)
        assert zfs_module._stuck == []
        assert proc.stdout.closed and proc.stderr.closed

    def test_the_expiry_waits_are_the_module_constants(self, kids, monkeypatch):
        monkeypatch.setattr(zfs_module, '_EXPIRY_WAIT', 0.3)
        cmd = py(READY_IGNORE_TERM)
        sink = []
        started = time.monotonic()
        _, exc = kids.bounded(lambda: run_lines(cmd, 1.5, sink))
        elapsed = time.monotonic() - started
        assert isinstance(exc, ZFSTimeoutError)
        if sink != ['ready']:
            pytest.skip('the child had not set up before the deadline (loaded host)')
        assert 1.5 + 0.25 <= elapsed < 1.5 + 0.3 + 8
        assert kids.procs[0].returncode == -signal.SIGKILL

    def test_a_child_that_closed_stdout_and_hangs_is_stopped_too(self, kids):
        script = 'import os, time\nos.write(1, b"a\\n")\nos.close(1)\ntime.sleep(120)\n'
        sink = []
        _, exc = kids.bounded(lambda: run_lines(py(script), 0.6, sink))
        assert isinstance(exc, ZFSTimeoutError) and sink == ['a']
        assert kids.procs[0].returncode == -signal.SIGTERM and zfs_module._stuck == []

    def test_a_child_that_exits_nonzero_before_the_deadline_is_a_plain_failure(self, kids):
        _, exc = kids.bounded(lambda: run_lines(py('import sys\nsys.stderr.write("nope\\n")\nsys.exit(4)\n'), 30))
        assert type(exc) is Exception and str(exc) == 'Nope'

    @staticmethod
    def _grandchild_script():
        return (
            'import subprocess, sys, time, os\n'
            'kw = {} if sys.argv[2] == "1" else {"stderr": subprocess.DEVNULL}\n'
            'g = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"], **kw)\n'
            'open(sys.argv[1] + ".tmp", "w").write(str(g.pid))\n'
            'os.rename(sys.argv[1] + ".tmp", sys.argv[1])\n'
            'sys.stdout.write("out\\n"); sys.stdout.flush()\n'
            'if sys.argv[3] == "1":\n'
            '    time.sleep(120)\n'
            'sys.exit(int(sys.argv[4]))\n')

    @pytest.mark.parametrize('child_lingers', [True, False])
    @pytest.mark.parametrize('hold_stderr', [True, False])
    def test_a_grandchild_holding_the_pipes_cannot_hold_the_caller_past_the_deadline(
            self, kids, tmp_path, monkeypatch, child_lingers, hold_stderr):
        """A child that is still running at the deadline is a timeout. One that has EXITED (only a grandchild holds the
        pipes) is not: it is drained without blocking and the call completes with its own, complete output."""
        monkeypatch.setattr(zfs_module, '_EXPIRY_JOIN', 0.05)
        pid_file = tmp_path / 'grandchild.pid'
        cmd = py(self._grandchild_script(), str(pid_file), '1' if hold_stderr else '0', '1' if child_lingers else '0', '0')
        sink = []
        started = time.monotonic()
        try:
            _, exc = kids.bounded(lambda: run_lines(cmd, 1.0, sink))
            elapsed = time.monotonic() - started
            assert wait_until(pid_file.exists, 5), 'the child never started its grandchild'
            kids.extra_pids.append(int(pid_file.read_text()))
            assert 0.9 <= elapsed < 1.0 + 10                       # about the deadline, not the grandchild's 120 s
            assert sink == ['out']
            if child_lingers:
                assert isinstance(exc, ZFSTimeoutError)
            else:
                assert exc is None                                  # exited: not a timeout
            assert zfs_module._stuck == []                           # the child itself is gone
            proc = kids.procs[0]
            assert proc.returncode is not None and proc.stdout.closed
            if hold_stderr:
                assert not proc.stderr.closed                        # its reader is blocked on it: never closed under it
            else:
                assert wait_until(lambda: not any(t.is_alive() for t in kids.stderr_threads), 5)
        finally:
            for pid in kids.extra_pids:
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
        # With the grandchild dead the reader ends by itself; nothing is left running.
        assert wait_until(lambda: not any(t.is_alive() for t in kids.stderr_threads), 10)

    def test_an_exited_failing_child_with_a_grandchild_holding_stdout_is_a_plain_failure(self, kids, tmp_path):
        pid_file = tmp_path / 'grandchild.pid'
        cmd = py(self._grandchild_script(), str(pid_file), '0', '0', '3')
        sink = []
        try:
            _, exc = kids.bounded(lambda: run_lines(cmd, 1.0, sink))
            assert wait_until(pid_file.exists, 5)
            kids.extra_pids.append(int(pid_file.read_text()))
        finally:
            kids.kill_all()
        assert type(exc) is Exception and not isinstance(exc, ZFSTimeoutError)
        assert sink == ['out'] and 'exit code 3' in str(exc)

    def test_the_stderr_wait_after_a_clean_exit_is_bounded_by_the_budget(self, kids, tmp_path, monkeypatch):
        """The child exited, stdout is at EOF, a grandchild holds only stderr: the final stderr join must not run for
        _TERMINATE_TIMEOUT (5 s) past a 1 s budget."""
        pid_file = tmp_path / 'g.pid'
        script = (
            'import subprocess, sys, os\n'
            'g = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"], stdout=subprocess.DEVNULL)\n'
            'open(sys.argv[1] + ".tmp", "w").write(str(g.pid)); os.rename(sys.argv[1] + ".tmp", sys.argv[1])\n'
            'sys.stdout.write("out\\n")\n')
        monkeypatch.setattr(zfs_module, '_TERMINATE_TIMEOUT', 6)
        sink = []
        started = time.monotonic()
        try:
            _, exc = kids.bounded(lambda: run_lines(py(script, str(pid_file)), 1.0, sink))
            elapsed = time.monotonic() - started
            assert wait_until(pid_file.exists, 5)
            kids.extra_pids.append(int(pid_file.read_text()))
        finally:
            kids.kill_all()
        assert exc is None and sink == ['out']
        assert elapsed < 4.0                       # about the 1 s budget; the unbounded join would take 6 s

    def test_the_untimed_stderr_wait_is_still_the_terminate_timeout(self, kids, tmp_path, monkeypatch):
        pid_file = tmp_path / 'g.pid'
        script = (
            'import subprocess, sys, os\n'
            'g = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"], stdout=subprocess.DEVNULL)\n'
            'open(sys.argv[1] + ".tmp", "w").write(str(g.pid)); os.rename(sys.argv[1] + ".tmp", sys.argv[1])\n')
        monkeypatch.setattr(zfs_module, '_TERMINATE_TIMEOUT', 1.0)
        started = time.monotonic()
        try:
            _, exc = kids.bounded(lambda: run_lines(py(script, str(pid_file))))
            elapsed = time.monotonic() - started
            assert wait_until(pid_file.exists, 5)
            kids.extra_pids.append(int(pid_file.read_text()))
        finally:
            kids.kill_all()
        assert exc is None
        assert 0.9 <= elapsed < 8

    @pytest.mark.skipif(not HAS_DEV_FD, reason='no /dev/fd on this platform')
    def test_several_threads_each_in_their_own_timeout_all_raise_and_leak_nothing(self, kids):
        count = 6
        gc.collect()
        before_fds = open_fds()
        before_threads = threading.active_count()
        before_dogs = set(watchdog_threads())
        zfs_module.slow_command_warning = 30
        results = []
        lock = threading.Lock()
        barrier = threading.Barrier(count)

        def worker():
            barrier.wait(30)
            try:
                with zfs_timeout(0.6):
                    list(Command._exec_lines(py(HELLO_THEN_SLEEP)))
                outcome_ = ('returned',)
            except BaseException as exc:  # noqa: BLE001
                outcome_ = (type(exc).__name__, str(exc))
            with lock:
                results.append(outcome_)

        threads = [threading.Thread(target=worker) for _ in range(count)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        assert not any(t.is_alive() for t in threads)

        assert len(results) == count
        assert all(r[0] == 'ZFSTimeoutError' and r[1].endswith('timed out after 0.6s; outcome unknown')
                   for r in results), results
        assert len(kids.procs) == count
        assert all(p.returncode == -signal.SIGTERM for p in kids.procs)
        assert zfs_module._stuck == []
        # The Popen objects stay referenced by `kids`, so what is closed here was closed by the library.
        assert all(p.stdout.closed and p.stderr.closed for p in kids.procs)
        assert no_new_watchdogs(before_dogs)

        def settled():
            gc.collect()
            return open_fds() <= before_fds and threading.active_count() <= before_threads
        assert wait_until(settled, 10), (open_fds(), before_fds, threading.active_count(), before_threads)


# ---------------------------------------------------------------------------------------------------------------------
# D7: interrupts, abandoned generators and cleanup that must not run twice
# ---------------------------------------------------------------------------------------------------------------------

class TestTimedCleanup:

    @pytest.fixture(autouse=True)
    def _bounds(self, monkeypatch):
        monkeypatch.setattr(zfs_module, '_TERMINATE_TIMEOUT', 0.5)
        monkeypatch.setattr(zfs_module, '_EXPIRY_WAIT', 0.5)

    @pytest.mark.parametrize('how', ['close', 'delete'])
    @pytest.mark.parametrize('script,signal_expected,kills', [
        (READY_THEN_SLEEP, signal.SIGTERM, 0),
        (READY_IGNORE_TERM, signal.SIGKILL, 1),
    ])
    def test_an_abandoned_timed_generator_reaps_the_child_like_an_untimed_one(
            self, kids, selectors_seen, how, script, signal_expected, kills):
        before = set(watchdog_threads())
        zfs_module.slow_command_warning = 30
        with zfs_timeout(60):
            gen = Command._exec_lines(py(script))
            assert next(gen) == 'ready'
        if how == 'close':
            gen.close()
        else:
            del gen
            gc.collect()
        proc = kids.procs[0]
        assert proc.returncode == -signal_expected
        assert kids.terminated == {proc.pid: 1} and sum(kids.killed.values()) == kills
        assert selectors_seen.instances and all(s.closed for s in selectors_seen.instances)
        assert not any(t.is_alive() for t in kids.stderr_threads)
        assert zfs_module._stuck == []
        with pytest.raises(ProcessLookupError):
            os.kill(proc.pid, 0)
        assert no_new_watchdogs(before)
        assert _timeout_var.get() is None

    @pytest.mark.parametrize('script,signal_expected,kills', [
        (READY_THEN_SLEEP, signal.SIGTERM, 0),
        (READY_IGNORE_TERM, signal.SIGKILL, 1),
    ])
    def test_a_keyboard_interrupt_mid_read_reaps_the_child(self, kids, selectors_seen, script, signal_expected, kills):
        selectors_seen.interrupt_after = 1                    # the 1st select delivers 'ready', the 2nd is interrupted
        sink = []
        started = time.monotonic()
        with pytest.raises(KeyboardInterrupt):
            run_lines(py(script), 60, sink)
        assert time.monotonic() - started < 30
        assert sink == ['ready']
        proc = kids.procs[0]
        assert proc.returncode == -signal_expected
        assert kids.terminated == {proc.pid: 1} and sum(kids.killed.values()) == kills
        assert all(s.closed for s in selectors_seen.instances)
        assert not any(t.is_alive() for t in kids.stderr_threads)
        assert zfs_module._stuck == []
        with pytest.raises(ProcessLookupError):
            os.kill(proc.pid, 0)
        assert _timeout_var.get() is None

    def test_an_expired_call_is_cleaned_up_once(self, kids, monkeypatch):
        """After `_expire` the generator's own cleanup must not terminate/kill/join again."""
        joins = []
        real_join = Command._join_stderr_reader
        monkeypatch.setattr(Command, '_join_stderr_reader',
                            staticmethod(lambda thread, *a, **k: joins.append(thread) or real_join(thread, *a, **k)))
        _, exc = kids.bounded(lambda: run_lines(py(SLEEP), 0.5))
        assert isinstance(exc, ZFSTimeoutError)
        proc = kids.procs[0]
        assert kids.terminated == {proc.pid: 1} and kids.killed == {}
        assert joins == []

    def test_an_interrupt_while_expiring_kills_the_child_and_propagates(self, kids, monkeypatch):
        real_wait = subprocess.Popen.wait
        calls = []

        def interrupted_wait(self, timeout=None):
            calls.append(timeout)
            if len(calls) == 1:
                raise KeyboardInterrupt()
            return real_wait(self, timeout=timeout)

        monkeypatch.setattr(subprocess.Popen, 'wait', interrupted_wait)
        # The read path waits for the exit only after EOF; here the deadline hits first, so the first wait is _expire's.
        with pytest.raises(KeyboardInterrupt):
            run_lines(py(SLEEP), 0.4)
        proc = kids.procs[0]
        assert wait_until(lambda: proc.poll() is not None, 10)
        assert proc.returncode in (-signal.SIGTERM, -signal.SIGKILL)
        assert zfs_module._stuck == []

    # -- early exits under a timeout use the timed sequence (TERM, _EXPIRY_WAIT, KILL, _EXPIRY_WAIT, register) ----

    def test_a_timed_early_exit_waits_by_the_expiry_constant_not_the_terminate_timeout(self, kids, monkeypatch):
        monkeypatch.setattr(zfs_module, '_TERMINATE_TIMEOUT', 60)
        monkeypatch.setattr(zfs_module, '_EXPIRY_WAIT', 0.4)
        with zfs_timeout(60):
            gen = Command._exec_lines(py(READY_IGNORE_TERM))
            assert next(gen) == 'ready'
        started = time.monotonic()
        gen.close()
        assert 0.3 <= time.monotonic() - started < 20
        assert kids.procs[0].returncode == -signal.SIGKILL
        assert kids.terminated == {kids.procs[0].pid: 1} and kids.killed == {kids.procs[0].pid: 1}

    def test_an_untimed_early_exit_still_waits_by_the_terminate_timeout(self, kids, monkeypatch):
        monkeypatch.setattr(zfs_module, '_TERMINATE_TIMEOUT', 0.4)
        monkeypatch.setattr(zfs_module, '_EXPIRY_WAIT', 60)
        gen = Command._exec_lines(py(READY_IGNORE_TERM))
        assert next(gen) == 'ready'
        started = time.monotonic()
        gen.close()
        assert 0.3 <= time.monotonic() - started < 20
        assert kids.procs[0].returncode == -signal.SIGKILL

    @staticmethod
    def _timed_generator(kids):
        with zfs_timeout(60):
            gen = Command._exec_lines(py(READY_THEN_SLEEP))
            assert next(gen) == 'ready'
        return gen

    @pytest.mark.parametrize('how', ['close', 'throw', 'interrupt'])
    def test_a_child_that_survives_kill_on_an_early_exit_is_registered_and_the_original_exception_propagates(
            self, kids, selectors_seen, caplog, how):
        gen = self._timed_generator(kids)
        proc = kids.procs[0]
        kids.unkillable = True                       # wait() times out and poll() says running, whatever is real
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            if how == 'close':
                gen.close()
            elif how == 'throw':
                with pytest.raises(RuntimeError, match='consumer failed'):
                    gen.throw(RuntimeError('consumer failed'))
            else:
                selectors_seen.interrupt_after = selectors_seen.calls   # the next select is interrupted
                with pytest.raises(KeyboardInterrupt):
                    next(gen)
        assert stuck_pids() == [proc.pid]
        entry = zfs_module._stuck[0]
        assert entry.label == label_of(py(''))
        assert proc.stdout.closed and not proc.stderr.closed
        assert kids.terminated == {proc.pid: 1} and kids.killed == {proc.pid: 1}
        warnings_ = [r for r in caplog.records if 'did not exit after SIGTERM and SIGKILL' in r.getMessage()]
        assert len(warnings_) == 1 and f'pid {proc.pid}' in warnings_[0].getMessage()
        assert all(s_.closed for s_ in selectors_seen.instances)
        with zfs_timeout(5):                         # and it blocks the next timed call
            with pytest.raises(ZFSTimeoutError, match=rf'not started: 1 earlier .*\(pid {proc.pid}\)'):
                next(Command._exec_lines(py('print(1)')))

    @pytest.mark.parametrize('how', ['close', 'throw'])
    def test_a_child_that_dies_on_an_early_exit_registers_nothing(self, kids, how):
        gen = self._timed_generator(kids)
        if how == 'close':
            gen.close()
        else:
            with pytest.raises(RuntimeError, match='consumer failed'):
                gen.throw(RuntimeError('consumer failed'))
        assert zfs_module._stuck == [] and kids.procs[0].returncode == -signal.SIGTERM

    def test_a_timeout_with_an_unkillable_child_reports_its_pid_and_registers_it(self, kids, monkeypatch):
        monkeypatch.setattr(zfs_module, '_EXPIRY_WAIT', 0.2)
        cmd = py(HELLO_THEN_SLEEP)
        kids.unkillable = True
        _, exc = kids.bounded(lambda: run_lines(cmd, 0.4))
        proc = kids.procs[0]
        assert isinstance(exc, ZFSTimeoutError)
        assert str(exc) == (f'{label_of(cmd)} timed out after 0.4s; outcome unknown '
                            f'(pid {proc.pid} did not exit after SIGKILL)')
        assert stuck_pids() == [proc.pid] and proc.stdout.closed and not proc.stderr.closed
        # the "pool wakes up": the child is really gone (the kill landed), the sweep drops it and frees stderr
        kids.unkillable = False
        assert wait_until(lambda: proc.poll() is not None, 10)
        assert wait_until(lambda: not any(t.is_alive() for t in kids.stderr_threads), 10)
        assert zfs_module._sweep_stuck() == [] and zfs_module._stuck == []
        assert proc.stderr.closed

    @pytest.mark.parametrize('fileno', ['mock', 'oserror', 'valueerror', 'attributeerror', 'bool', 'none'])
    def test_a_pipe_without_an_integer_fileno_falls_back_to_readline(self, mock_subprocess, fileno):
        process = mock_subprocess.setup(stdout=['a\n', 'b\n'])
        if fileno == 'oserror':
            process.stdout.fileno.side_effect = OSError('nope')
        elif fileno == 'valueerror':
            process.stdout.fileno.side_effect = ValueError('closed file')
        elif fileno == 'attributeerror':
            process.stdout.fileno.side_effect = AttributeError('no fileno')
        elif fileno == 'bool':
            process.stdout.fileno.return_value = True
        elif fileno == 'none':
            process.stdout.fileno.return_value = None
        assert run_lines(['zfs', 'list'], seconds=5) == ['a', 'b']
        assert process.stdout.readline.call_count == 3
        assert zfs_module._stuck == []

    def test_a_mocked_failure_still_raises_under_a_timeout(self, mock_subprocess):
        mock_subprocess.setup(stdout=['a\n'], stderr=['cannot list\n'], returncode=1)
        with pytest.raises(Exception, match='Cannot list'):
            run_lines(['zfs', 'list'], seconds=5)


# ---------------------------------------------------------------------------------------------------------------------
# D7: the stuck-child registry, with fake processes
# ---------------------------------------------------------------------------------------------------------------------

class FakePipe:
    def __init__(self):
        self.closed = False
        self.close_calls = 0

    def close(self):
        self.closed = True
        self.close_calls += 1


class FakeThread:
    def __init__(self, alive):
        self.alive = alive
        self.joins = []

    def is_alive(self):
        return self.alive

    def join(self, timeout=None):
        self.joins.append(timeout)


class FakeProcess:
    """A child that cannot be reaped: `wait(timeout=...)` always times out and `poll()` says it is running, until the
    test changes `poll_value`."""

    def __init__(self, pid=777, poll_value=None, wait_ends=False):
        self.pid = pid
        self.poll_value = poll_value
        self.poll_raises = None
        self.wait_ends = wait_ends
        self.wait_calls = []
        self.terminated = 0
        self.killed = 0
        self.stdout = FakePipe()
        self.stderr = FakePipe()

    def poll(self):
        if self.poll_raises is not None:
            raise self.poll_raises
        return self.poll_value

    def terminate(self):
        self.terminated += 1

    def kill(self):
        self.killed += 1

    def wait(self, timeout=None):
        self.wait_calls.append(timeout)
        if self.wait_ends:
            self.poll_value = -15
            return -15
        raise subprocess.TimeoutExpired('fake', timeout)


def stuck_pids():
    return [e.process.pid for e in zfs_module._stuck]


class TestStuckChildRegistry:

    def test_expire_registers_a_child_that_survives_kill(self, caplog):
        fake = FakeProcess(pid=777)
        thread = FakeThread(alive=True)
        cmd = ['/sbin/zfs', 'list', '-H']
        with caplog.at_level(logging.WARNING, logger='libzfseasy'):
            error = Command._expire(fake, thread, cmd, 0.5)

        assert isinstance(error, ZFSTimeoutError)
        assert str(error) == 'zfs list timed out after 0.5s; outcome unknown (pid 777 did not exit after SIGKILL)'
        assert 'timed out after' in str(error) and 'outcome unknown' in str(error)
        assert (fake.terminated, fake.killed) == (1, 1)
        assert fake.wait_calls == [zfs_module._EXPIRY_WAIT, zfs_module._EXPIRY_WAIT]
        assert fake.stdout.closed and not fake.stderr.closed
        assert thread.joins == []                           # nobody waits for a reader of a stuck child's stderr
        assert stuck_pids() == [777]
        entry = zfs_module._stuck[0]
        assert isinstance(entry, _StuckChild) and entry.process is fake and entry.stderr_thread is thread
        assert entry.label == 'zfs list'
        warnings = [r for r in caplog.records if 'did not exit after SIGTERM and SIGKILL' in r.getMessage()]
        assert len(warnings) == 1 and 'pid 777' in warnings[0].getMessage()

    def test_expire_of_a_child_that_dies_after_term_registers_nothing(self):
        fake = FakeProcess(wait_ends=True)
        thread = FakeThread(alive=False)
        error = Command._expire(fake, thread, ['zfs', 'list'], 2)
        assert str(error) == 'zfs list timed out after 2s; outcome unknown'
        assert (fake.terminated, fake.killed) == (1, 0)
        assert fake.stdout.closed and fake.stderr.closed
        assert thread.joins == [zfs_module._EXPIRY_JOIN]
        assert zfs_module._stuck == []

    def test_expire_leaves_stderr_open_while_its_reader_is_still_alive(self):
        fake = FakeProcess(wait_ends=True)
        thread = FakeThread(alive=True)
        Command._expire(fake, thread, ['zfs', 'list'], 2)
        assert fake.stdout.closed and not fake.stderr.closed
        assert thread.joins == [zfs_module._EXPIRY_JOIN] and zfs_module._stuck == []

    def test_expire_without_a_reader_closes_stderr(self):
        fake = FakeProcess(wait_ends=True)
        Command._expire(fake, None, ['zfs', 'list'], 2)
        assert fake.stderr.closed

    def test_expire_of_a_child_that_already_exited_signals_nothing(self):
        fake = FakeProcess(poll_value=0)
        thread = FakeThread(alive=False)
        error = Command._expire(fake, thread, ['zfs', 'list'], 2)
        assert (fake.terminated, fake.killed, fake.wait_calls) == (0, 0, [])
        assert str(error) == 'zfs list timed out after 2s; outcome unknown'
        assert fake.stdout.closed and fake.stderr.closed and zfs_module._stuck == []

    def test_an_interrupt_while_expire_waits_kills_and_propagates_without_registering(self):
        fake = FakeProcess()

        def interrupted(timeout=None):
            raise KeyboardInterrupt()

        fake.wait = interrupted
        with pytest.raises(KeyboardInterrupt):
            Command._expire(fake, FakeThread(True), ['zfs', 'list'], 2)
        assert fake.killed == 1 and zfs_module._stuck == []

    def test_a_timed_call_is_refused_before_popen_while_a_stuck_child_lives(self, mocker):
        popen = mocker.patch('subprocess.Popen')
        Command._expire(FakeProcess(pid=777), FakeThread(True), ['zfs', 'list'], 1)
        Command._expire(FakeProcess(pid=888), FakeThread(True), ['zfs', 'list'], 1)

        for entry in ('_exec', '_exec_out', '_exec_lines'):
            with zfs_timeout(5):
                with pytest.raises(ZFSTimeoutError) as info:
                    result = getattr(Command, entry)(['/sbin/zfs', 'destroy', 'p/fs'])
                    if entry != '_exec':
                        list(result)
            assert str(info.value) == ('zfs destroy not started: 2 earlier command(s) that timed out are still '
                                       'running (pid 777, 888); zfs is not answering')
        popen.assert_not_called()
        assert stuck_pids() == [777, 888]

    def test_the_public_api_is_refused_too(self, mocker):
        popen = mocker.patch('subprocess.Popen')
        Command._expire(FakeProcess(pid=777), FakeThread(True), ['zfs', 'list'], 1)
        with zfs_timeout(5):
            with pytest.raises(ZFSTimeoutError, match='zfs is not answering'):
                libzfseasy.list(Filesystem('testpool'))
            with pytest.raises(ZFSTimeoutError, match='zfs is not answering'):
                libzfseasy.snapshot(Filesystem('testpool/fs'), 'snap')
        popen.assert_not_called()

    def test_an_untimed_call_still_runs(self, mock_subprocess):
        Command._expire(FakeProcess(pid=777), FakeThread(True), ['zfs', 'list'], 1)
        mock_subprocess.setup(stdout=['x\n'])
        assert list(Command._exec_lines(['zfs', 'list'])) == ['x']
        subprocess.Popen.assert_called_once()
        assert stuck_pids() == [777]                         # still registered: it is still running

    def test_a_timeout_none_block_inside_a_timeout_is_not_refused(self, mock_subprocess):
        Command._expire(FakeProcess(pid=777), FakeThread(True), ['zfs', 'list'], 1)
        mock_subprocess.setup(stdout=['x\n'])
        with zfs_timeout(5):
            with zfs_timeout(None):
                assert list(Command._exec_lines(['zfs', 'list'])) == ['x']

    def test_a_refused_call_does_not_start_a_watchdog(self, mocker):
        mocker.patch('subprocess.Popen')
        zfs_module.slow_command_warning = 30
        Command._expire(FakeProcess(pid=777), FakeThread(True), ['zfs', 'list'], 1)
        before = set(watchdog_threads())
        with zfs_timeout(5):
            with pytest.raises(ZFSTimeoutError):
                list(Command._exec_lines(['zfs', 'list']))
        assert no_new_watchdogs(before)

    def test_once_the_stuck_child_exits_timed_calls_run_again_and_its_pipes_are_released(self, mock_subprocess):
        fake = FakeProcess(pid=777)
        thread = FakeThread(alive=False)
        Command._expire(fake, thread, ['zfs', 'list'], 1)
        mock_subprocess.setup(stdout=['x\n'])
        with zfs_timeout(5):
            with pytest.raises(ZFSTimeoutError):
                list(Command._exec_lines(['zfs', 'list']))
            assert not fake.stderr.closed
            fake.poll_value = 0                              # the pool woke up and the child finally exited
            assert list(Command._exec_lines(['zfs', 'list'])) == ['x']
        assert zfs_module._stuck == []
        assert fake.stdout.closed and fake.stderr.closed     # its reader is dead: stderr is closed too

    def test_only_the_exited_ones_are_dropped(self):
        a, b, c = FakeProcess(pid=1), FakeProcess(pid=2), FakeProcess(pid=3)
        for fake in (a, b, c):
            zfs_module._register_stuck(fake, None, 'zfs list')
        b.poll_value = 0
        alive = zfs_module._sweep_stuck()
        assert [e.process.pid for e in alive] == [1, 3] and stuck_pids() == [1, 3]
        assert b.stdout.closed and b.stderr.closed
        assert not a.stdout.closed and not c.stdout.closed

    @pytest.mark.parametrize('reader, stderr_closed', [('dead', True), ('alive', False), ('none', True)])
    def test_sweeping_an_exited_child_closes_stdout_and_stderr_only_if_its_reader_is_dead(self, reader, stderr_closed):
        fake = FakeProcess(pid=5, poll_value=0)
        thread = None if reader == 'none' else FakeThread(alive=(reader == 'alive'))
        zfs_module._register_stuck(fake, thread, 'zfs list')
        assert zfs_module._sweep_stuck() == []
        assert zfs_module._stuck == []
        assert fake.stdout.closed
        assert fake.stderr.closed is stderr_closed

    @pytest.mark.parametrize('exc', [OSError('gone'), ValueError('bad'), RuntimeError('odd')])
    def test_a_child_whose_poll_raises_counts_as_exited(self, exc):
        fake = FakeProcess(pid=6)
        fake.poll_raises = exc
        zfs_module._register_stuck(fake, FakeThread(False), 'zfs list')
        assert zfs_module._sweep_stuck() == []
        assert zfs_module._stuck == [] and fake.stdout.closed and fake.stderr.closed

    def test_sweeping_an_empty_registry(self):
        assert zfs_module._sweep_stuck() == []

    def test_register_and_sweep_from_many_threads_lose_nothing(self):
        rng = random.Random(7)
        total, writers, sweepers = 400, 4, 3
        fakes = [FakeProcess(pid=1000 + i) for i in range(total)]
        dying = {f.pid for f in fakes if rng.random() < 0.5}
        stop = threading.Event()
        errors = []

        def register(chunk):
            try:
                for fake in chunk:
                    zfs_module._register_stuck(fake, FakeThread(alive=False), 'zfs list')
                    if fake.pid in dying:
                        fake.poll_value = 0             # exits right after registration: a sweep may drop it at once
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def sweep():
            try:
                while not stop.is_set():
                    zfs_module._sweep_stuck()
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        sweep_threads = [threading.Thread(target=sweep) for _ in range(sweepers)]
        reg_threads = [threading.Thread(target=register, args=(fakes[i::writers],)) for i in range(writers)]
        for t in sweep_threads + reg_threads:
            t.start()
        for t in reg_threads:
            t.join(60)
        stop.set()
        for t in sweep_threads:
            t.join(60)
        assert not errors, errors

        zfs_module._sweep_stuck()
        assert sorted(stuck_pids()) == sorted(f.pid for f in fakes if f.pid not in dying)   # none lost, none kept
        for fake in fakes:
            assert fake.stdout.closed is (fake.pid in dying)
            assert fake.stdout.close_calls <= 1

    def test_reset_after_fork_empties_the_list_and_replaces_the_lock(self):
        zfs_module._register_stuck(FakeProcess(pid=1), None, 'zfs list')
        same_list, old_lock = zfs_module._stuck, zfs_module._stuck_lock
        assert old_lock.acquire(blocking=False)               # held, as by a thread that does not exist in a fork child
        try:
            zfs_module._reset_stuck_after_fork()
            assert zfs_module._stuck is same_list and zfs_module._stuck == []
            assert zfs_module._stuck_lock is not old_lock
            assert zfs_module._stuck_lock.acquire(blocking=False)
            zfs_module._stuck_lock.release()
        finally:
            old_lock.release()

    @pytest.mark.skipif(not hasattr(os, 'fork'), reason='no fork on this platform')
    def test_a_forked_child_sees_an_empty_registry_and_a_usable_lock_and_the_parent_keeps_its_own(self):
        import warnings
        fake = FakeProcess(pid=4242)
        zfs_module._register_stuck(fake, None, 'zfs list')
        parent_lock = zfs_module._stuck_lock
        assert parent_lock.acquire(blocking=False)             # another thread "holds" it at the moment of the fork
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')               # "fork() in a multi-threaded process"
                pid = os.fork()
            if pid == 0:
                code = 0
                try:
                    if zfs_module._stuck:
                        code |= 1
                    if not zfs_module._stuck_lock.acquire(blocking=False):
                        code |= 2
                    else:
                        zfs_module._stuck_lock.release()
                    try:
                        zfs_module._sweep_stuck()             # must not deadlock on the inherited, held lock
                    except BaseException:
                        code |= 8
                except BaseException:
                    code |= 4
                os._exit(code)
            status = None
            end = time.monotonic() + 30
            try:
                while time.monotonic() < end:
                    done, code_ = os.waitpid(pid, os.WNOHANG)
                    if done:
                        status = code_
                        break
                    time.sleep(0.02)
            finally:
                if status is None:
                    os.kill(pid, signal.SIGKILL)
                    os.waitpid(pid, 0)
            assert status is not None, 'the forked child did not finish'
        finally:
            parent_lock.release()
        assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, status
        assert stuck_pids() == [4242] and zfs_module._stuck_lock is parent_lock    # the parent's registry is intact


# ---------------------------------------------------------------------------------------------------------------------
# D7: streaming, collecting and resume_info ignore the timeout
# ---------------------------------------------------------------------------------------------------------------------

class TestStreamingIgnoresTheTimeout:

    @pytest.fixture
    def admit(self, monkeypatch):
        calls = []
        real = zfs_module._admit_timed
        monkeypatch.setattr(zfs_module, '_admit_timed', lambda cmd: calls.append(cmd) or real(cmd))
        return calls

    def test_the_spy_does_see_a_timed_call(self, admit, kids):
        """Positive control: a spy that never fires would make every test below pass."""
        with zfs_timeout(30):
            assert list(Command._exec_lines(py('print("a")'))) == ['a']
        assert len(admit) == 1

    def test_send_stream_is_not_bounded_nor_admission_checked(self, admit, kids):
        script = 'import sys, time\nsys.stdout.write("DATA"); sys.stdout.flush()\ntime.sleep(0.6)\n'
        started = time.monotonic()
        with zfs_timeout(0.1):
            handle = SendCommand._exec_stream(py(script))
            assert handle.read() == b'DATA'
            handle.close()
        assert time.monotonic() - started >= 0.5            # outlasted the 0.1 s limit without being touched
        assert admit == [] and kids.procs[0].returncode == 0 and kids.terminated == {}

    def test_receive_stream_is_not_bounded_nor_admission_checked(self, admit, kids):
        script = 'import sys, time\nsys.stdin.buffer.read()\ntime.sleep(0.6)\n'
        started = time.monotonic()
        with zfs_timeout(0.1):
            handle = ReceiveCommand._exec_stream_in(py(script))
            handle.write(b'payload')
            handle.close()
        assert time.monotonic() - started >= 0.5
        assert admit == [] and kids.procs[0].returncode == 0 and kids.terminated == {}

    def test_a_streaming_command_is_not_refused_by_a_stuck_child(self, admit, kids):
        zfs_module._register_stuck(FakeProcess(pid=777), None, 'zfs list')
        with zfs_timeout(5):
            handle = SendCommand._exec_stream(py('import sys\nsys.stdout.write("DATA")\n'))
            assert handle.read() == b'DATA'
            handle.close()
            handle = ReceiveCommand._exec_stream_in(py('import sys\nsys.stdin.buffer.read()\n'))
            handle.write(b'x')
            handle.close()
            lines, errors, rc = Command._exec_collect(py('print("c")'))
        assert (lines, errors, rc) == (['c'], [], 0)
        assert admit == []

    def test_exec_collect_which_resume_info_uses_is_not_bounded(self, admit, kids):
        script = 'import time\ntime.sleep(0.6)\nprint("token info")\n'
        with zfs_timeout(0.1):
            assert Command._exec_collect(py(script)) == (['token info'], [], 0)
        assert admit == []

    def test_resume_info_ignores_the_timeout(self, admit, mock_subprocess):
        mock_subprocess.setup_multi((['resume token\n'], [], 0))
        token = '1-aaaa-bbbb'
        zfs_module.slow_command_warning = None
        with zfs_timeout(0.1), pytest.raises(Exception):
            SendCommand.resume_info(token)                  # whatever it makes of the mock, it is not a timeout
        assert admit == []

    def test_the_timeout_context_does_not_alter_what_streaming_returns(self, kids):
        with zfs_timeout(30):
            handle = SendCommand._exec_stream(py('import sys\nsys.stdout.buffer.write(b"\\x00\\xffraw")\n'))
            assert handle.read() == b'\x00\xffraw'
            handle.close()


# ---------------------------------------------------------------------------------------------------------------------
# D7: end to end through the public API with a fake zfs that hangs
# ---------------------------------------------------------------------------------------------------------------------

class TestPublicApiTimeout:

    @pytest.fixture
    def hanging_zfs(self, tmp_path, monkeypatch):
        script = tmp_path / 'zfs'
        script.write_text(f'#!{sys.executable}\nimport time\ntime.sleep(120)\n')
        script.chmod(0o755)
        monkeypatch.setenv('ZFS_CMD', str(script))
        return script

    def test_list_and_snapshot_time_out_and_the_child_is_reaped(self, hanging_zfs, kids):
        started = time.monotonic()
        with zfs_timeout(0.6):
            with pytest.raises(ZFSTimeoutError, match=r'^zfs list timed out after 0\.6s; outcome unknown$'):
                libzfseasy.list(Filesystem('testpool'))
        assert time.monotonic() - started < 30
        with zfs_timeout(0.6):
            with pytest.raises(ZFSTimeoutError, match=r'^zfs snapshot timed out after 0\.6s; outcome unknown$'):
                libzfseasy.snapshot(Filesystem('testpool/fs'), 'snap')
        assert len(kids.procs) == 2
        assert all(p.returncode == -signal.SIGTERM for p in kids.procs)
        assert zfs_module._stuck == []

    def test_without_a_timeout_the_same_call_is_not_bounded(self, hanging_zfs, kids):
        """Opt-in: no `timeout()` block, no deadline (the call is stopped by the test's own watchdog instead)."""
        result = {}

        def call():
            try:
                libzfseasy.list(Filesystem('testpool'))
            except BaseException as exc:  # noqa: BLE001
                result['exc'] = exc

        thread = threading.Thread(target=call, daemon=True)
        thread.start()
        thread.join(1.5)
        try:
            assert thread.is_alive() and 'exc' not in result
        finally:
            kids.kill_all()
            thread.join(20)
        assert isinstance(result.get('exc'), Exception) and not isinstance(result['exc'], ZFSTimeoutError)


class TestStuckChildrenAccessor:
    """`libzfseasy.stuck_children()`: the public, snapshot-only view of the registry (sweeps, never blocks)."""

    def test_empty_registry_gives_an_empty_tuple(self):
        assert libzfseasy.stuck_children() == ()

    def test_an_alive_stuck_child_is_reported_as_plain_data(self):
        fake = FakeProcess(pid=777)
        zfs_module._register_stuck(fake, FakeThread(True), 'zfs destroy')
        result = libzfseasy.stuck_children()
        assert isinstance(result, tuple) and len(result) == 1
        child = result[0]
        assert isinstance(child, libzfseasy.StuckChild)
        assert (child.pid, child.label) == (777, 'zfs destroy')
        assert child.since == zfs_module._stuck[0].since
        assert not any(value is fake for value in child)          # no live process object leaks
        assert fake.wait_calls == [] and fake.terminated == fake.killed == 0

    def test_after_the_child_exits_the_next_call_sweeps_it(self):
        fake = FakeProcess(pid=777)
        zfs_module._register_stuck(fake, FakeThread(False), 'zfs list')
        assert [c.pid for c in libzfseasy.stuck_children()] == [777]
        fake.poll_value = 0
        assert libzfseasy.stuck_children() == ()
        assert zfs_module._stuck == [] and fake.stdout.closed and fake.stderr.closed

    def test_it_never_waits_for_a_child(self):
        fakes = [FakeProcess(pid=i) for i in (1, 2, 3)]
        for fake in fakes:
            zfs_module._register_stuck(fake, FakeThread(True), 'zfs list')
        assert [c.pid for c in libzfseasy.stuck_children()] == [1, 2, 3]   # oldest first
        assert all(f.wait_calls == [] for f in fakes)

    def test_it_takes_the_registry_lock(self):
        zfs_module._register_stuck(FakeProcess(pid=9), FakeThread(True), 'zfs list')
        done = threading.Event()
        seen = []

        def read():
            seen.append(libzfseasy.stuck_children())
            done.set()

        with zfs_module._stuck_lock:
            thread = threading.Thread(target=read)
            thread.start()
            assert not done.wait(0.2)                              # blocked behind the held lock
        assert done.wait(10)
        thread.join(10)
        assert [c.pid for c in seen[0]] == [9]

    def test_concurrent_readers_and_registrations_are_safe(self):
        errors = []
        stop = threading.Event()

        def read():
            try:
                while not stop.is_set():
                    for child in libzfseasy.stuck_children():
                        assert isinstance(child.pid, int)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        readers = [threading.Thread(target=read) for _ in range(3)]
        for t in readers:
            t.start()
        for i in range(200):
            zfs_module._register_stuck(FakeProcess(pid=2000 + i), FakeThread(False), 'zfs list')
        stop.set()
        for t in readers:
            t.join(60)
        assert not errors, errors
        assert len(libzfseasy.stuck_children()) == 200

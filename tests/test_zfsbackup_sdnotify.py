"""Batch D: systemd readiness notifications (`zfsbackup.sdnotify` and the daemon's use of it).

Three layers:

* `zfsbackup.sdnotify` on its own -- parsing of `$NOTIFY_SOCKET`, the wire format
  against a REAL `AF_UNIX`/`SOCK_DGRAM` server, descriptor hygiene, "never raises".
* `BackupDaemon` with a recording notifier over the scripted `World` of
  `test_zfsbackup_reload.py`: what is sent, in which order, on which tick.
* A REAL daemon process (hermetic env, fake `zfs`, `--dry-run`) talking to a REAL
  datagram socket, under every available multiprocessing start method.

Socket paths are short (`/tmp/zsn...`): `sun_path` is ~104 bytes on macOS and pytest's
`tmp_path` is far longer than that.
"""

import errno
import logging
import multiprocessing
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import List, Optional

import pytest

import zfsbackup.daemon as daemon_module
from _hermetic import hermetic_env
from test_zfsbackup_daemon import _args, _stub_daemon, no_hard_exit  # noqa: F401  (fixture)
from test_zfsbackup_reload import (  # noqa: F401  (fixtures and helpers)
    ROOT,
    STORE,
    RealDaemon,
    World,
    _debug_logging,
    _listening_socket,
    _private_home,
    _restore_signal_handlers,
    cfg,
    world,
)
from test_zfsbackup_startup_waits import FakeTime
from zfsbackup import daemonlock, sdnotify
from zfsbackup.config import BackupConfig
from zfsbackup.daemon import RELOAD_POLL_SECONDS, BackupDaemon
from zfsbackup.sdnotify import NOTIFY_SOCKET_ENV, Notifier

IS_LINUX = sys.platform.startswith("linux")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def sockdir():
    """A SHORT private directory for datagram socket files."""
    path = tempfile.mkdtemp(prefix="zsn", dir="/tmp")
    os.chmod(path, 0o700)
    yield path
    shutil.rmtree(path, ignore_errors=True)


class Server:
    """A bound datagram socket standing in for systemd's notify socket."""

    def __init__(self, address):
        self.address = address
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.sock.bind(address)
        self.sock.settimeout(0.2)
        self.consumed: List[str] = []

    def recv(self, timeout: float = 2.0) -> Optional[str]:
        deadline = time.monotonic() + timeout
        while True:
            try:
                data = self.sock.recv(65536).decode()
            except socket.timeout:
                if time.monotonic() >= deadline:
                    return None
                continue
            self.consumed.append(data)
            return data

    def drain(self, seconds: float = 0.3) -> List[str]:
        out = []
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            msg = self.recv(timeout=0.05)
            if msg is not None:
                out.append(msg)
        return out

    def take_until(self, predicate, timeout: float, alive=None, context=lambda: ""):
        """Messages received up to and including the first one `predicate` accepts."""
        got = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            msg = self.recv(timeout=0.25)
            if msg is None:
                if alive is not None and not alive():
                    raise AssertionError(f"daemon exited; received {got!r}\n{context()}")
                continue
            got.append(msg)
            if predicate(msg):
                return got
        raise AssertionError(f"timed out after {timeout}s; received {got!r}\n{context()}")

    def close(self):
        self.sock.close()


@pytest.fixture
def server(sockdir):
    srv = Server(os.path.join(sockdir, "n.sock"))
    yield srv
    srv.close()


def open_fd_count() -> int:
    for d in ("/proc/self/fd", "/dev/fd"):
        if os.path.isdir(d):
            return len(os.listdir(d))
    pytest.skip("cannot enumerate open descriptors here")


def lines(msg: str) -> List[str]:
    return msg.split("\n")


# ---------------------------------------------------------------------------
# Notifier.from_environ
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFromEnviron:
    def test_an_absolute_path_is_the_address_and_is_removed_from_the_environment(self, monkeypatch):
        monkeypatch.setenv(NOTIFY_SOCKET_ENV, "/run/systemd/notify")
        n = Notifier.from_environ()
        assert n.enabled and n._address == "/run/systemd/notify"
        assert NOTIFY_SOCKET_ENV not in os.environ

    def test_an_at_name_is_the_abstract_namespace_with_a_leading_nul(self, monkeypatch):
        monkeypatch.setenv(NOTIFY_SOCKET_ENV, "@zfsbackup/notify")
        n = Notifier.from_environ()
        assert n.enabled and n._address == "\0zfsbackup/notify"
        assert NOTIFY_SOCKET_ENV not in os.environ

    @pytest.mark.parametrize(
        "raw", ["relative.sock", "./n.sock", "@", "vsock:2:1234", "unix:/run/x", " /run/x"],
        ids=["relative", "dotted", "bare-at", "vsock", "unix-scheme", "leading-space"],
    )
    def test_an_unusable_value_is_a_no_op_with_one_warning_and_is_still_popped(
        self, monkeypatch, caplog, raw
    ):
        monkeypatch.setenv(NOTIFY_SOCKET_ENV, raw)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.sdnotify"):
            n = Notifier.from_environ()
        assert not n.enabled
        assert NOTIFY_SOCKET_ENV not in os.environ
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1 and NOTIFY_SOCKET_ENV in warnings[0].getMessage()
        assert repr(raw) in warnings[0].getMessage()
        assert n.send("READY=1") is False and n.ready() is False

    def test_an_empty_value_is_a_silent_no_op_and_is_still_popped(self, monkeypatch, caplog):
        monkeypatch.setenv(NOTIFY_SOCKET_ENV, "")
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.sdnotify"):
            n = Notifier.from_environ()
        assert not n.enabled and NOTIFY_SOCKET_ENV not in os.environ
        assert caplog.records == []

    def test_an_unset_variable_is_a_silent_no_op(self, monkeypatch, caplog):
        monkeypatch.delenv(NOTIFY_SOCKET_ENV, raising=False)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.sdnotify"):
            n = Notifier.from_environ()
        assert not n.enabled and caplog.records == []

    def test_the_environ_seam_reads_the_mapping_and_touches_nothing(self, monkeypatch):
        monkeypatch.setenv(NOTIFY_SOCKET_ENV, "/must/not/be/read")
        mapping = {NOTIFY_SOCKET_ENV: "/seam/notify"}
        n = Notifier.from_environ(environ=mapping)
        assert n._address == "/seam/notify"
        assert mapping == {NOTIFY_SOCKET_ENV: "/seam/notify"}
        assert os.environ[NOTIFY_SOCKET_ENV] == "/must/not/be/read"

    def test_the_environ_seam_with_an_invalid_value_warns_and_is_a_no_op(self, caplog):
        with caplog.at_level(logging.WARNING, logger="zfsbackup.sdnotify"):
            n = Notifier.from_environ(environ={NOTIFY_SOCKET_ENV: "nope"})
        assert not n.enabled
        assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1

    def test_the_default_notifier_is_disabled(self):
        assert not Notifier().enabled


# ---------------------------------------------------------------------------
# Notifier.send against a real datagram server
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.subprocess
class TestSend:
    def test_the_lines_arrive_as_one_newline_joined_datagram(self, server):
        n = Notifier(server.address)
        assert n.send("READY=1", "STATUS=up") is True
        assert server.recv() == "READY=1\nSTATUS=up"
        assert server.recv(0.3) is None            # exactly one datagram

    def test_empty_lines_are_dropped_and_an_empty_payload_sends_nothing(self, server):
        n = Notifier(server.address)
        assert n.send("", "READY=1", "") is True
        assert server.recv() == "READY=1"
        assert n.send() is False and n.send("", "") is False
        assert server.recv(0.3) is None

    def test_newlines_nuls_and_carriage_returns_in_a_line_cannot_split_the_message(self, server):
        n = Notifier(server.address)
        n.status("a\nREADY=1\0b\rc")
        msg = server.recv()
        assert msg == "STATUS=a READY=1 b c"
        assert len(lines(msg)) == 1 and "\0" not in msg

    def test_non_ascii_text_is_utf8(self, server):
        Notifier(server.address).status("génération")
        assert server.recv() == "STATUS=génération"

    def test_a_no_op_notifier_sends_nothing_and_opens_no_socket(self, server, monkeypatch):
        def boom(*a, **k):
            raise AssertionError("a no-op notifier must not create a socket")

        n = Notifier(None)
        monkeypatch.setattr(socket, "socket", boom)
        assert n.send("READY=1") is False
        assert n.ready("x") is False and n.reloading() is False and n.stopping() is False
        assert n.status("x") is False and n.extend_timeout(5) is False
        monkeypatch.undo()
        assert server.recv(0.3) is None

    def test_no_descriptor_survives_a_call_on_success_or_on_failure(self, server, sockdir):
        good = Notifier(server.address)
        bad = Notifier(os.path.join(sockdir, "absent.sock"))
        before = open_fd_count()
        for _ in range(25):
            assert good.send("READY=1") is True
            assert bad.send("READY=1") is False
        assert open_fd_count() == before

    def test_each_message_gets_its_own_closed_non_inheritable_unix_datagram_socket(
        self, server, monkeypatch
    ):
        real = socket.socket
        seen = []

        class Spy(real):
            def close(self):
                seen.append({
                    "family": self.family,
                    "type": self.type,
                    "inheritable": self.get_inheritable(),
                    "timeout": self.gettimeout(),
                })
                super().close()

        monkeypatch.setattr(socket, "socket", Spy)
        n = Notifier(server.address)
        n.send("READY=1")
        n.send("STOPPING=1")
        monkeypatch.undo()
        assert len(seen) == 2
        for info in seen:
            assert info["family"] == socket.AF_UNIX
            assert info["type"] == socket.SOCK_DGRAM
            assert info["inheritable"] is False
            assert info["timeout"] == sdnotify._SEND_TIMEOUT == 1.0
        assert [server.recv(), server.recv()] == ["READY=1", "STOPPING=1"]

    @pytest.mark.skipif(not hasattr(socket, "SOCK_CLOEXEC"), reason="no SOCK_CLOEXEC here")
    def test_the_socket_is_created_close_on_exec_where_the_flag_exists(self, server, monkeypatch):
        real = socket.socket
        types = []

        def factory(family=-1, type=-1, *a, **k):
            types.append(type)
            return real(family, type, *a, **k)

        monkeypatch.setattr(socket, "socket", factory)
        Notifier(server.address).send("READY=1")
        monkeypatch.undo()
        assert types and types[0] & socket.SOCK_CLOEXEC

    def test_a_missing_path_never_raises(self, sockdir):
        n = Notifier(os.path.join(sockdir, "absent.sock"))
        assert n.send("READY=1") is False
        assert n.ready("s") is False and n.reloading() is False
        assert n.stopping() is False and n.extend_timeout(1) is False

    def test_a_regular_file_in_place_of_the_socket_never_raises(self, sockdir):
        path = os.path.join(sockdir, "file")
        Path(path).write_text("not a socket")
        assert Notifier(path).send("READY=1") is False

    def test_a_socket_nobody_listens_on_never_raises(self, sockdir):
        path = os.path.join(sockdir, "dead.sock")
        srv = Server(path)
        srv.close()                                   # the file stays, the listener is gone
        assert os.path.exists(path)
        assert Notifier(path).send("READY=1") is False

    def test_a_directory_without_search_permission_never_raises(self, sockdir):
        if os.geteuid() == 0:
            pytest.skip("root ignores directory permissions")
        sub = os.path.join(sockdir, "locked")
        os.mkdir(sub)
        srv = Server(os.path.join(sub, "n.sock"))
        try:
            os.chmod(sub, 0)
            assert Notifier(os.path.join(sub, "n.sock")).send("READY=1") is False
        finally:
            os.chmod(sub, 0o700)
            srv.close()

    def test_a_full_receive_queue_returns_within_the_timeout_and_never_raises(
        self, server, monkeypatch
    ):
        monkeypatch.setattr(sdnotify, "_SEND_TIMEOUT", 0.05)
        server.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2048)
        n = Notifier(server.address)
        payload = "X" * 1500
        started = time.monotonic()
        results = [n.send(f"STATUS={payload}") for _ in range(400)]
        elapsed = time.monotonic() - started
        assert all(isinstance(r, bool) for r in results)
        assert False in results, "the queue never filled; the test proved nothing"
        assert elapsed < 60
        assert open_fd_count() < 200

    @pytest.mark.skipif(not IS_LINUX, reason="the abstract socket namespace is Linux only")
    def test_an_abstract_address_is_reachable(self, monkeypatch):
        name = f"zsn-test-{os.getpid()}-{time.monotonic_ns()}"
        srv = Server("\0" + name)
        try:
            monkeypatch.setenv(NOTIFY_SOCKET_ENV, "@" + name)
            assert Notifier.from_environ().send("READY=1") is True
            assert srv.recv() == "READY=1"
        finally:
            srv.close()


@pytest.mark.unit
class TestFailureLogging:
    def test_the_first_failure_is_a_warning_and_every_later_one_is_debug(self, sockdir, caplog):
        n = Notifier(os.path.join(sockdir, "absent.sock"))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.sdnotify"):
            for _ in range(4):
                assert n.send("READY=1") is False
        levels = [r.levelno for r in caplog.records]
        assert levels == [logging.WARNING] + [logging.DEBUG] * 3
        assert NOTIFY_SOCKET_ENV in caplog.records[0].getMessage()
        assert "further failures are logged at debug" in caplog.records[0].getMessage()

    def test_a_success_does_not_re_arm_the_warning(self, server, sockdir, caplog):
        n = Notifier(os.path.join(sockdir, "absent.sock"))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.sdnotify"):
            n.send("READY=1")
            n._address = server.address
            assert n.send("READY=1") is True
            n._address = os.path.join(sockdir, "absent.sock")
            n.send("READY=1")
        assert [r.levelno for r in caplog.records] == [logging.WARNING, logging.DEBUG]

    def test_a_socket_creation_failure_is_reported_the_same_way(self, monkeypatch, caplog):
        def refuse(*a, **k):
            raise OSError(errno.EMFILE, "Too many open files")

        n = Notifier("/run/systemd/notify")
        monkeypatch.setattr(socket, "socket", refuse)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.sdnotify"):
            assert n.send("READY=1") is False
            assert n.send("READY=1") is False
        monkeypatch.undo()
        assert [r.levelno for r in caplog.records] == [logging.WARNING, logging.DEBUG]

    def test_failures_of_separate_notifiers_are_counted_separately(self, sockdir, caplog):
        a = Notifier(os.path.join(sockdir, "a"))
        b = Notifier(os.path.join(sockdir, "b"))
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.sdnotify"):
            a.send("READY=1")
            b.send("READY=1")
        assert [r.levelno for r in caplog.records] == [logging.WARNING, logging.WARNING]


# ---------------------------------------------------------------------------
# Message shapes
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.subprocess
class TestMessageShapes:
    def test_ready_without_and_with_a_status(self, server):
        n = Notifier(server.address)
        n.ready()
        n.ready("generation 4")
        assert server.recv() == "READY=1"
        assert server.recv() == "READY=1\nSTATUS=generation 4"

    def test_reloading_carries_the_monotonic_clock_in_microseconds(self, server):
        n = Notifier(server.address)
        low = time.monotonic_ns() // 1000
        n.reloading()
        high = time.monotonic_ns() // 1000
        first, second = lines(server.recv())
        assert first == "RELOADING=1"
        assert second.startswith("MONOTONIC_USEC=")
        value = int(second.split("=", 1)[1])
        assert low <= value <= high

    def test_stopping_and_status(self, server):
        n = Notifier(server.address)
        n.stopping()
        n.status("down")
        assert server.recv() == "STOPPING=1"
        assert server.recv() == "STATUS=down"

    def test_extend_timeout_without_and_with_a_status(self, server):
        n = Notifier(server.address)
        n.extend_timeout(30_000_000)
        n.extend_timeout(5.9, "waiting")
        assert server.recv() == "EXTEND_TIMEOUT_USEC=30000000"
        assert server.recv() == "EXTEND_TIMEOUT_USEC=5\nSTATUS=waiting"

    def test_the_helpers_return_whether_the_datagram_went(self, server, sockdir):
        assert Notifier(server.address).ready() is True
        assert Notifier(os.path.join(sockdir, "absent")).ready() is False


# ---------------------------------------------------------------------------
# The daemon, with a recording notifier
# ---------------------------------------------------------------------------


class Recorder(Notifier):
    """An enabled `Notifier` whose `send` records instead of sending, so the
    helpers' real wire shapes are what the daemon tests see."""

    def __init__(self, log=None):
        super().__init__("/recorder/not/a/socket")
        self.sent: List[tuple] = []
        self.log = log

    def send(self, *msg):
        msg = tuple(m for m in msg if m)
        self.sent.append(msg)
        if self.log is not None:
            self.log.append("notify:" + msg[0])
        return True

    # -- views -------------------------------------------------------------

    def heads(self) -> List[str]:
        return [m[0] for m in self.sent]

    def statuses(self) -> List[Optional[str]]:
        out = []
        for m in self.sent:
            found = [x[len("STATUS="):] for x in m if x.startswith("STATUS=")]
            out.append(found[0] if found else None)
        return out


@pytest.fixture
def rec(world):
    r = Recorder(log=world.events)
    world.daemon._notify = r
    return r


def stop_after_first_sleep(world, mocker):
    mocker.patch.object(
        world.daemon, "_supervisor_sleep",
        side_effect=lambda s: world.daemon._stop_event.set(),
    )


def settle(world, rec):
    """The state `run()` leaves after READY, with the record cleared."""
    world.daemon._notify_ready()
    rec.sent.clear()


@pytest.mark.unit
class TestNoNotifier:
    def test_a_daemon_built_without_one_is_a_disabled_no_op(self, world):
        fresh = BackupDaemon(world.running_config, world.resolved, key=(STORE, 0))
        assert isinstance(fresh._notify, Notifier) and not fresh._notify.enabled

    def test_run_without_a_notifier_touches_no_socket(self, world, mocker, monkeypatch):
        stop_after_first_sleep(world, mocker)

        def boom(*a, **k):
            raise AssertionError("no socket may be created without a notifier")

        monkeypatch.setattr(sdnotify.socket, "socket", boom)
        world.daemon._hup_requested = True
        assert world.daemon.run() == 0

    def test_the_startup_notifier_default_is_disabled(self):
        assert not daemon_module._startup_notifier.enabled


@pytest.mark.unit
class TestRunSequence:
    def test_ready_comes_after_every_worker_is_forked_and_stopping_after_ready(
        self, world, rec, mocker
    ):
        stop_after_first_sleep(world, mocker)
        assert world.daemon.run() == 0
        ev = world.events
        assert world.forks() == ["fork:snapshot", "fork:pruning", "fork:api"]
        ready = ev.index("notify:READY=1")
        assert ready > max(ev.index(f) for f in world.forks())
        assert ready < ev.index("notify:STOPPING=1")
        assert rec.heads() == ["READY=1", "STOPPING=1"]
        assert rec.statuses() == ["generation 0", None]

    def test_the_startup_tick_sends_nothing_even_when_it_reloads(self, world, rec):
        world.write((STORE, 1), prefix="landed")
        world.daemon._hup_requested = True               # carried over from `_StartupHup`
        world.daemon._reload_tick(startup=True)
        assert world.daemon._handoff.key == (STORE, 1)    # it did accept...
        assert rec.sent == []                             # ...and said nothing

    def test_the_startup_tick_sends_nothing_when_it_rejects(self, world, rec):
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.daemon._hup_requested = True
        world.daemon._reload_tick(startup=True)
        assert world.daemon._rejected == (STORE, 1)
        assert rec.sent == []

    def test_the_startup_tick_sends_nothing_for_a_poll_either(self, world, rec):
        world.write((STORE, 1), prefix="landed")
        world.daemon._reload_tick(startup=True)
        assert rec.sent == []

    def test_ready_after_a_startup_reload_reports_the_applied_generation(self, world, rec, mocker):
        stop_after_first_sleep(world, mocker)
        world.write((STORE, 3), prefix="landed")
        world.daemon.run()
        assert rec.heads() == ["READY=1", "STOPPING=1"]
        assert rec.statuses()[0] == "generation 3"

    def test_no_ready_when_the_stop_flag_is_already_up(self, world, rec):
        world.daemon._stop_event.set()
        world.daemon.run()
        assert world.forks() == []
        assert rec.heads() == ["STOPPING=1"]

    def test_no_ready_when_a_stop_lands_during_the_startup_tick(self, world, rec):
        world.on_load = world.daemon._stop_event.set
        world.write((STORE, 1), prefix="changed")
        world.daemon._hup_requested = True
        world.daemon.run()
        assert rec.heads() == ["STOPPING=1"]

    def test_no_ready_when_a_stop_lands_while_the_workers_start(self, world, rec, mocker):
        real_start = world.daemon._start_workers

        def start_then_stop():
            real_start()
            world.daemon._stop_event.set()

        mocker.patch.object(world.daemon, "_start_workers", side_effect=start_then_stop)
        world.daemon.run()
        assert world.forks()                       # the workers were started...
        assert rec.heads() == ["STOPPING=1"]       # ...but READY was not sent into a stop

    def test_stopping_is_the_first_act_of_the_finally(self, world, rec, mocker):
        d = world.daemon
        orig_shutdown, orig_report = d._shutdown_workers, d._report_stop_signal

        def shutdown():
            world.events.append("shutdown")
            orig_shutdown()

        def report():
            world.events.append("report")
            orig_report()

        mocker.patch.object(d, "_shutdown_workers", side_effect=shutdown)
        mocker.patch.object(d, "_report_stop_signal", side_effect=report)
        mocker.patch.object(
            d, "_supervisor_sleep", side_effect=lambda s: d._stop_event.set()
        )
        d.run()
        ev = world.events
        stopping = ev.index("notify:STOPPING=1")
        assert stopping < ev.index("shutdown")
        assert stopping < ev.index("report")
        assert ev.count("notify:STOPPING=1") == 1

    def test_the_finally_raises_the_stop_flag_before_it_sends_stopping(self, world, rec, mocker):
        """On the crash path nothing else has set the flag, so this pins the
        order inside the `finally`: flag first (workers only see the stop
        through it), then the possibly slow `STOPPING=1`."""
        d = world.daemon
        seen = {}
        orig = rec.send

        def send(*msg):
            if msg and msg[0] == "STOPPING=1":
                seen["flag"] = d._stop_event.is_set()
            return orig(*msg)

        rec.send = send
        mocker.patch.object(d, "_supervisor_sleep")
        mocker.patch.object(d, "_check_workers", side_effect=RuntimeError("boom"))
        with pytest.raises(RuntimeError, match="boom"):
            d.run()
        assert seen == {"flag": True}

    def test_a_blocked_stopping_send_does_not_hold_the_stop_flag_back(self, world, rec, mocker):
        """systemd's queue is full and `STOPPING=1` blocks (up to the send
        timeout): the workers must already see the stop meanwhile."""
        import threading

        d = world.daemon
        entered, release = threading.Event(), threading.Event()
        flag_while_blocked = {}
        orig = rec.send

        def send(*msg):
            if msg and msg[0] == "STOPPING=1":
                flag_while_blocked["before"] = d._stop_event.is_set()
                entered.set()
                assert release.wait(20), "the test never released the blocked send"
            return orig(*msg)

        rec.send = send
        orig_shutdown = d._shutdown_workers

        def shutdown():
            world.events.append("shutdown")
            orig_shutdown()

        mocker.patch.object(d, "_shutdown_workers", side_effect=shutdown)
        mocker.patch.object(d, "_supervisor_sleep")
        mocker.patch.object(d, "_check_workers", side_effect=RuntimeError("boom"))
        outcome = {}

        def run():
            try:
                d.run()
            except BaseException as e:      # noqa: BLE001 - handed to the test thread
                outcome["exc"] = e

        t = threading.Thread(target=run, daemon=True)
        t.start()
        try:
            assert entered.wait(20), "STOPPING=1 was never sent"
            # The send is still in progress; the flag is already up and no
            # worker has been shut down yet.
            assert d._stop_event.is_set()
            assert "shutdown" not in world.events
        finally:
            release.set()
            t.join(20)
        assert not t.is_alive()
        assert flag_while_blocked == {"before": True}
        assert isinstance(outcome.get("exc"), RuntimeError)

    def test_stopping_is_sent_once_also_when_run_raises(self, world, rec, mocker):
        d = world.daemon
        orig_shutdown = d._shutdown_workers

        def shutdown():
            world.events.append("shutdown")
            orig_shutdown()

        mocker.patch.object(d, "_shutdown_workers", side_effect=shutdown)
        mocker.patch.object(d, "_supervisor_sleep")                 # does not stop the loop
        mocker.patch.object(d, "_check_workers", side_effect=RuntimeError("boom"))
        with pytest.raises(RuntimeError, match="boom"):
            d.run()
        assert rec.heads() == ["READY=1", "STOPPING=1"]
        assert world.events.index("notify:STOPPING=1") < world.events.index("shutdown")

    def test_stopping_is_sent_when_starting_the_workers_raises(self, world, rec, mocker):
        mocker.patch.object(world.daemon, "_start_workers", side_effect=OSError("EAGAIN"))
        with pytest.raises(OSError):
            world.daemon.run()
        assert rec.heads() == ["STOPPING=1"]

    def test_a_loop_iteration_with_a_sighup_brackets_the_reload_inside_run(
        self, world, rec, mocker
    ):
        d = world.daemon
        calls = []

        def sleep(seconds):
            calls.append(seconds)
            if len(calls) == 1:
                d._sighup_handler(signal.SIGHUP, None)       # what the real handler does
            else:
                d._stop_event.set()

        mocker.patch.object(d, "_supervisor_sleep", side_effect=sleep)
        d.run()
        assert rec.heads() == ["READY=1", "RELOADING=1", "READY=1", "STOPPING=1"]


@pytest.mark.unit
class TestForcedReloadBracket:
    """A SIGHUP-forced, non-startup tick: RELOADING=1 first, READY=1 on EVERY
    outcome (the reload is decided, not finished)."""

    def assert_bracket(self, rec, status):
        assert rec.heads() == ["RELOADING=1", "READY=1"]
        assert rec.sent[0][1].startswith("MONOTONIC_USEC=")
        assert rec.statuses() == [None, status]

    def test_unchanged(self, world, rec):
        settle(world, rec)
        world.hup()
        self.assert_bracket(rec, "generation 0")

    def test_an_identical_candidate(self, world, rec):
        settle(world, rec)
        world.write((STORE, 1))                              # same config, new generation
        world.hup()
        assert world.daemon._handoff.key == (STORE, 1)
        self.assert_bracket(rec, "generation 1")

    def test_an_accepted_candidate(self, world, rec):
        settle(world, rec)
        world.write((STORE, 1), prefix="changed")
        world.start_workers()
        world.hup()
        assert world.daemon._handoff.key == (STORE, 1)
        self.assert_bracket(rec, "generation 1")

    def test_a_rejected_candidate_still_says_ready_and_names_the_rejection(self, world, rec):
        settle(world, rec)
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.hup()
        assert world.daemon._rejected == (STORE, 1)
        self.assert_bracket(rec, "generation 0; generation 1 rejected")

    def test_a_read_that_fails(self, world, rec):
        settle(world, rec)
        world.load_error = RuntimeError("database is damaged")
        world.hup()
        self.assert_bracket(rec, "generation 0")

    def test_try_reload_raising_still_sends_ready_and_lets_the_exception_out(
        self, world, rec, mocker
    ):
        settle(world, rec)
        mocker.patch.object(world.daemon, "_try_reload", side_effect=RuntimeError("boom"))
        world.daemon._sighup_handler(signal.SIGHUP, None)
        with pytest.raises(RuntimeError, match="boom"):
            world.daemon._reload_tick()
        self.assert_bracket(rec, "generation 0")

    def test_live_reload_off_because_the_daemon_has_no_key(self, mocker, tmp_path, caplog):
        w = World(mocker, tmp_path, daemon_key=None)
        r = Recorder()
        w.daemon._notify = r
        w.daemon._notify_ready()
        r.sent.clear()
        w.hup()
        assert r.heads() == ["RELOADING=1", "READY=1"]
        assert r.statuses() == [None, "generation unknown"]
        assert "live reload is off" in caplog.text

    def test_a_stop_landing_mid_tick(self, world, rec):
        settle(world, rec)
        world.on_load = world.daemon._stop_event.set
        world.write((STORE, 1), prefix="changed")
        world.hup()
        self.assert_bracket(rec, "generation 0")

    def test_a_stop_already_up_when_the_tick_runs_sends_no_bracket_at_all(self, world, rec):
        """A SIGTERM that raced the SIGHUP: RELOADING -> READY -> STOPPING
        would claim a finished reload on a daemon that is going down."""
        settle(world, rec)
        world.daemon._stop_event.set()
        world.daemon._sighup_handler(signal.SIGHUP, None)
        world.events.clear()
        world.daemon._reload_tick()
        assert rec.sent == []
        assert world.daemon._hup_requested is False        # the forced flag is consumed
        assert "load" not in world.events                  # and nothing was read

    def test_reloading_precedes_the_read(self, world, rec):
        settle(world, rec)
        world.write((STORE, 1), prefix="changed")
        world.events.clear()
        world.hup()
        ev = world.events
        assert ev.index("notify:RELOADING=1") < ev.index("load") < ev.index("notify:READY=1")

    def test_the_monotonic_stamp_is_current(self, world, rec):
        settle(world, rec)
        low = time.monotonic_ns() // 1000
        world.hup()
        high = time.monotonic_ns() // 1000
        assert low <= int(rec.sent[0][1].split("=", 1)[1]) <= high

    def test_several_hups_in_a_row_each_get_a_bracket(self, world, rec):
        settle(world, rec)
        world.hup()
        world.hup()
        assert rec.heads() == ["RELOADING=1", "READY=1"] * 2


@pytest.mark.unit
class TestSighupAndPollTriggers:
    def test_the_signal_handler_only_sets_the_flag_and_sends_nothing(self, world, rec):
        settle(world, rec)
        world.daemon._sighup_handler(signal.SIGHUP, None)
        assert world.daemon._hup_requested is True
        assert rec.sent == []

    def test_a_tick_without_the_flag_never_sends_reloading_not_even_when_the_poll_is_due(
        self, world, rec
    ):
        settle(world, rec)
        world.write((STORE, 1), prefix="changed")
        world.start_workers()
        world.poll()
        assert "RELOADING=1" not in rec.heads() and "READY=1" not in rec.heads()
        assert world.daemon._handoff.key == (STORE, 1)

    def test_a_tick_that_is_not_due_sends_nothing(self, world, rec):
        settle(world, rec)
        world.daemon._reload_tick()
        assert rec.sent == []

    def test_a_poll_accept_sends_only_a_status(self, world, rec):
        settle(world, rec)
        world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.poll()
        assert rec.heads() == ["STATUS=generation 1"]

    def test_a_poll_reject_sends_only_a_status_that_names_it(self, world, rec):
        settle(world, rec)
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.poll()
        assert rec.heads() == ["STATUS=generation 0; generation 1 rejected"]

    def test_an_unchanged_status_is_not_sent_again(self, world, rec):
        settle(world, rec)
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.poll()
        world.poll()                                     # the rejected key is not retried
        world.poll()
        assert rec.heads() == ["STATUS=generation 0; generation 1 rejected"]

    def test_a_poll_with_nothing_new_sends_nothing(self, world, rec):
        settle(world, rec)
        world.poll()
        world.poll()
        assert rec.sent == []

    def test_a_later_poll_accept_replaces_the_rejection_in_the_status(self, world, rec):
        settle(world, rec)
        world.start_workers()
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.poll()
        world.write((STORE, 2), prefix="fixed")
        world.poll()
        assert rec.heads() == [
            "STATUS=generation 0; generation 1 rejected",
            "STATUS=generation 2",
        ]

    def test_a_later_accepted_forced_reload_drops_the_rejection_from_its_ready(self, world, rec):
        settle(world, rec)
        world.start_workers()
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.hup()
        world.write((STORE, 2), prefix="fixed")
        world.hup()
        assert rec.heads() == ["RELOADING=1", "READY=1"] * 2
        assert rec.statuses() == [
            None, "generation 0; generation 1 rejected",
            None, "generation 2",
        ]
        assert "rejected" not in rec.statuses()[-1]

    def test_a_status_sent_by_ready_is_not_repeated_by_the_next_poll(self, world, rec):
        settle(world, rec)
        world.start_workers()
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.hup()                                      # READY already carried the rejection
        rec.sent.clear()
        world.poll()
        assert rec.sent == []

    def test_a_poll_whose_key_read_fails_sends_no_status(self, world, rec):
        settle(world, rec)
        world.poll_error = RuntimeError("busy")
        world.poll()
        assert rec.sent == []

    def test_the_rejection_in_the_status_is_the_current_one_only(self, world, rec):
        settle(world, rec)
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.poll()
        world.write((STORE, 2), config=BackupConfig(datasets=[]))
        world.poll()
        assert rec.heads()[-1] == "STATUS=generation 0; generation 2 rejected"


# -- the startup waits ------------------------------------------------------


@pytest.mark.unit
class TestStartupWaitsExtendTheTimeout:
    def test_the_extension_is_far_above_the_poll_interval(self):
        assert daemon_module._STARTUP_EXTEND_USEC == 30_000_000
        assert daemon_module._STARTUP_EXTEND_USEC > 10 * daemon_module._STARTUP_WAIT_POLL * 1_000_000

    def test_the_presence_lock_wait_extends_on_every_poll(self, monkeypatch):
        rec = Recorder()
        clock = FakeTime(monkeypatch)
        attempts = []
        sentinel = object()

        def acquire(resolved):
            attempts.append(1)
            if len(attempts) <= 3:
                raise daemonlock.DaemonAlreadyRunning(Path("/x/pid"), 4242, 1, "tok")
            return sentinel

        monkeypatch.setattr(daemon_module.daemonlock, "acquire", acquire)
        monkeypatch.setattr(daemon_module.procident, "identity", lambda pid, start: "gone")
        monkeypatch.setattr(daemon_module, "_startup_notifier", rec)
        assert daemon_module._acquire_presence_lock("db") is sentinel
        assert clock.sleeps == [1.0, 1.0, 1.0]
        assert len(rec.sent) == 3
        for msg in rec.sent:
            assert msg[0] == "EXTEND_TIMEOUT_USEC=30000000"
            assert msg[1].startswith("STATUS=waiting for the previous daemon")

    def test_a_refused_lock_extends_nothing(self, monkeypatch):
        rec = Recorder()
        FakeTime(monkeypatch)

        def acquire(resolved):
            raise daemonlock.DaemonAlreadyRunning(Path("/x/pid"), 4242, 1, "tok")

        monkeypatch.setattr(daemon_module.daemonlock, "acquire", acquire)
        monkeypatch.setattr(daemon_module.procident, "identity", lambda pid, start: "match")
        monkeypatch.setattr(daemon_module, "_startup_notifier", rec)
        with pytest.raises(daemonlock.DaemonAlreadyRunning):
            daemon_module._acquire_presence_lock("db")
        assert rec.sent == []

    def test_a_free_lock_extends_nothing(self, monkeypatch):
        rec = Recorder()
        FakeTime(monkeypatch)
        monkeypatch.setattr(daemon_module.daemonlock, "acquire", lambda resolved: "lock")
        monkeypatch.setattr(daemon_module, "_startup_notifier", rec)
        assert daemon_module._acquire_presence_lock("db") == "lock"
        assert rec.sent == []

    def test_the_workers_wait_extends_on_every_poll(self, monkeypatch):
        rec = Recorder()
        clock = FakeTime(monkeypatch)
        answers = iter([True, True, True, False])
        monkeypatch.setattr(
            daemon_module.daemonlock, "ensure_workers_lock", lambda resolved: Path("/x/workers.lock")
        )
        monkeypatch.setattr(daemon_module.daemonlock, "workers_present", lambda resolved: next(answers))
        monkeypatch.setattr(daemon_module, "_startup_notifier", rec)
        daemon_module._wait_for_previous_workers("db")
        assert clock.sleeps == [1.0, 1.0, 1.0]
        assert rec.heads() == ["EXTEND_TIMEOUT_USEC=30000000"] * 3
        assert all(s.startswith("waiting for the previous daemon") for s in rec.statuses())

    def test_nothing_held_extends_nothing(self, monkeypatch):
        rec = Recorder()
        FakeTime(monkeypatch)
        monkeypatch.setattr(
            daemon_module.daemonlock, "ensure_workers_lock", lambda resolved: Path("/x/workers.lock")
        )
        monkeypatch.setattr(daemon_module.daemonlock, "workers_present", lambda resolved: False)
        monkeypatch.setattr(daemon_module, "_startup_notifier", rec)
        daemon_module._wait_for_previous_workers("db")
        assert rec.sent == []

    # -- the extension cap (C4) -------------------------------------------

    CAP_MESSAGE = "no longer extending the systemd start timeout"

    def error_records(self, caplog):
        return [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_the_production_cap_is_one_hour(self):
        assert daemon_module._STARTUP_EXTEND_MAX == 3600.0

    def fast_clock(self, monkeypatch, step):
        """Each wait sleep advances `_startup_monotonic` by `step` seconds."""
        now = [5000.0]
        sleeps = []

        def sleep(seconds):
            sleeps.append(seconds)
            if len(sleeps) > 500:
                raise RuntimeError("runaway startup wait")
            now[0] += step

        monkeypatch.setattr(daemon_module, "_startup_sleep", sleep)
        monkeypatch.setattr(daemon_module, "_startup_monotonic", lambda: now[0])
        return sleeps

    def test_the_workers_wait_stops_extending_at_the_cap_logs_one_error_and_keeps_waiting(
        self, monkeypatch, caplog
    ):
        rec = Recorder()
        sleeps = self.fast_clock(monkeypatch, 600.0)          # ten minutes per poll
        polls = {"n": 0}

        def present(resolved):
            polls["n"] += 1
            return polls["n"] <= 12                           # held for 12 polls = two hours

        monkeypatch.setattr(
            daemon_module.daemonlock, "ensure_workers_lock", lambda resolved: Path("/x/workers.lock")
        )
        monkeypatch.setattr(daemon_module.daemonlock, "workers_present", present)
        monkeypatch.setattr(daemon_module, "_startup_notifier", rec)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemon"):
            daemon_module._wait_for_previous_workers("db")
        assert len(sleeps) == 12                              # it kept waiting to the end
        # waited = 0, 600, ..., 3000 are under the hour; 3600 and later are not.
        assert rec.heads() == ["EXTEND_TIMEOUT_USEC=30000000"] * 6
        (error,) = self.error_records(caplog)
        assert self.CAP_MESSAGE in error.getMessage()
        assert "/x/workers.lock" in error.getMessage() and "60 minutes" in error.getMessage()

    def test_the_presence_lock_wait_stops_extending_at_the_cap_logs_one_error_and_keeps_waiting(
        self, monkeypatch, caplog
    ):
        rec = Recorder()
        sleeps = self.fast_clock(monkeypatch, 600.0)
        attempts = []
        sentinel = object()

        def acquire(resolved):
            attempts.append(1)
            if len(attempts) <= 12:
                raise daemonlock.DaemonAlreadyRunning(Path("/x/pid"), 4242, 1, "tok")
            return sentinel

        monkeypatch.setattr(daemon_module.daemonlock, "acquire", acquire)
        monkeypatch.setattr(daemon_module.procident, "identity", lambda pid, start: "gone")
        monkeypatch.setattr(daemon_module, "_startup_notifier", rec)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemon"):
            assert daemon_module._acquire_presence_lock("db") is sentinel
        assert len(sleeps) == 12
        assert rec.heads() == ["EXTEND_TIMEOUT_USEC=30000000"] * 6
        (error,) = self.error_records(caplog)
        assert self.CAP_MESSAGE in error.getMessage()

    @pytest.mark.parametrize("which", ["workers", "lock"])
    def test_with_a_small_cap_exactly_the_polls_before_it_extend(self, monkeypatch, caplog, which):
        """One-second polls, a five-second cap: polls at waited = 0..4
        extend, 5 and later do not; one ERROR."""
        monkeypatch.setattr(daemon_module, "_STARTUP_EXTEND_MAX", 5.0)
        rec = Recorder()
        sleeps = self.fast_clock(monkeypatch, 1.0)
        count = {"n": 0}
        monkeypatch.setattr(daemon_module, "_startup_notifier", rec)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemon"):
            if which == "workers":
                monkeypatch.setattr(
                    daemon_module.daemonlock, "ensure_workers_lock", lambda r: Path("/x/w.lock")
                )

                def present(resolved):
                    count["n"] += 1
                    return count["n"] <= 9

                monkeypatch.setattr(daemon_module.daemonlock, "workers_present", present)
                daemon_module._wait_for_previous_workers("db")
            else:
                def acquire(resolved):
                    count["n"] += 1
                    if count["n"] <= 9:
                        raise daemonlock.DaemonAlreadyRunning(Path("/x/pid"), 1, 1, "t")
                    return "lock"

                monkeypatch.setattr(daemon_module.daemonlock, "acquire", acquire)
                monkeypatch.setattr(daemon_module.procident, "identity", lambda p, s: "mismatch")
                assert daemon_module._acquire_presence_lock("db") == "lock"
        assert len(sleeps) == 9
        assert len(rec.sent) == 5
        assert len(self.error_records(caplog)) == 1

    def test_each_wait_has_its_own_budget(self, monkeypatch, caplog):
        """The budget is per wait: the second wait extends again after the
        first used up its hour."""
        monkeypatch.setattr(daemon_module, "_STARTUP_EXTEND_MAX", 3.0)
        rec = Recorder()
        self.fast_clock(monkeypatch, 1.0)
        monkeypatch.setattr(daemon_module, "_startup_notifier", rec)
        monkeypatch.setattr(
            daemon_module.daemonlock, "ensure_workers_lock", lambda r: Path("/x/w.lock")
        )
        polls = iter([True] * 6 + [False] + [True] * 6 + [False])

        monkeypatch.setattr(daemon_module.daemonlock, "workers_present", lambda r: next(polls))
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemon"):
            daemon_module._wait_for_previous_workers("db")
            first = len(rec.sent)
            daemon_module._wait_for_previous_workers("db")
        assert first == 3 and len(rec.sent) == 6
        assert len(self.error_records(caplog)) == 2

    def test_a_wait_shorter_than_the_cap_logs_no_error(self, monkeypatch, caplog):
        rec = Recorder()
        self.fast_clock(monkeypatch, 1.0)
        answers = iter([True, True, False])
        monkeypatch.setattr(
            daemon_module.daemonlock, "ensure_workers_lock", lambda r: Path("/x/w.lock")
        )
        monkeypatch.setattr(daemon_module.daemonlock, "workers_present", lambda r: next(answers))
        monkeypatch.setattr(daemon_module, "_startup_notifier", rec)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.daemon"):
            daemon_module._wait_for_previous_workers("db")
        assert len(rec.sent) == 2 and self.error_records(caplog) == []

    def test_the_default_startup_notifier_makes_the_waits_harmless(self, monkeypatch):
        clock = FakeTime(monkeypatch)
        answers = iter([True, False])
        monkeypatch.setattr(
            daemon_module.daemonlock, "ensure_workers_lock", lambda resolved: Path("/x/workers.lock")
        )
        monkeypatch.setattr(daemon_module.daemonlock, "workers_present", lambda resolved: next(answers))
        assert not daemon_module._startup_notifier.enabled
        daemon_module._wait_for_previous_workers("db")
        assert clock.sleeps == [1.0]


# -- _run -------------------------------------------------------------------


@pytest.mark.unit
class TestRunHandlesTheEnvironment:
    def watch_startup(self, monkeypatch):
        """Record what `_startup_notifier` is while the config is being read."""
        seen = {}
        real = daemon_module.load_runtime_config

        def load(resolved):
            seen["startup_enabled"] = daemon_module._startup_notifier.enabled
            seen["startup_notifier"] = daemon_module._startup_notifier
            seen["env"] = os.environ.get(NOTIFY_SOCKET_ENV)
            return real(resolved)

        monkeypatch.setattr(daemon_module, "load_runtime_config", load)
        return seen

    def test_the_variable_is_gone_before_the_daemon_is_built_and_the_notifier_is_handed_in(
        self, sqlite_config_db, mocker, lock_spy, no_hard_exit, monkeypatch, sockdir
    ):
        address = os.path.join(sockdir, "n.sock")
        monkeypatch.setenv(NOTIFY_SOCKET_ENV, address)
        seen = self.watch_startup(monkeypatch)
        factory, stub = _stub_daemon(mocker)
        built = {}

        def build(*a, **k):
            built["env"] = os.environ.get(NOTIFY_SOCKET_ENV)
            built["notifier"] = k.get("notifier")
            return stub

        factory.side_effect = build
        assert daemon_module._run(_args(sqlite_config_db.path)) == 0
        assert seen["env"] is None and built["env"] is None
        assert NOTIFY_SOCKET_ENV not in os.environ
        assert built["notifier"].enabled and built["notifier"]._address == address
        assert seen["startup_notifier"] is built["notifier"]      # the waits report through it

    def test_nothing_is_sent_by_run_itself_before_the_daemon_runs(
        self, sqlite_config_db, mocker, lock_spy, no_hard_exit, monkeypatch, server
    ):
        monkeypatch.setenv(NOTIFY_SOCKET_ENV, server.address)
        _stub_daemon(mocker)
        assert daemon_module._run(_args(sqlite_config_db.path)) == 0
        assert server.recv(0.3) is None

    def test_test_config_removes_the_variable_and_sends_nothing(
        self, sqlite_config_db, monkeypatch, server
    ):
        monkeypatch.setenv(NOTIFY_SOCKET_ENV, server.address)
        seen = self.watch_startup(monkeypatch)
        assert daemon_module._run(_args(sqlite_config_db.path, test_config=True)) == 0
        assert NOTIFY_SOCKET_ENV not in os.environ and seen["env"] is None
        assert seen["startup_enabled"] is False
        assert server.recv(0.3) is None

    def test_the_startup_notifier_is_a_disabled_one_again_after_run(
        self, sqlite_config_db, mocker, lock_spy, no_hard_exit, monkeypatch, sockdir
    ):
        monkeypatch.setenv(NOTIFY_SOCKET_ENV, os.path.join(sockdir, "n.sock"))
        seen = self.watch_startup(monkeypatch)
        _stub_daemon(mocker)
        daemon_module._run(_args(sqlite_config_db.path))
        assert seen["startup_enabled"] is True
        assert not daemon_module._startup_notifier.enabled
        assert daemon_module._startup_notifier is not seen["startup_notifier"]

    def test_the_startup_notifier_is_reset_when_run_refuses(self, tmp_path, monkeypatch, sockdir):
        monkeypatch.setenv(NOTIFY_SOCKET_ENV, os.path.join(sockdir, "n.sock"))
        assert daemon_module._run(_args(tmp_path / "missing" / "config.db")) == 1
        assert NOTIFY_SOCKET_ENV not in os.environ
        assert not daemon_module._startup_notifier.enabled

    def test_the_startup_notifier_is_reset_when_the_daemon_fails(
        self, sqlite_config_db, mocker, lock_spy, no_hard_exit, monkeypatch, sockdir
    ):
        monkeypatch.setenv(NOTIFY_SOCKET_ENV, os.path.join(sockdir, "n.sock"))
        _stub_daemon(mocker, run_effect=RuntimeError("boom"))
        assert daemon_module._run(_args(sqlite_config_db.path)) == 1
        assert not daemon_module._startup_notifier.enabled

    def test_an_invalid_value_is_popped_and_the_daemon_gets_a_no_op(
        self, sqlite_config_db, mocker, lock_spy, no_hard_exit, monkeypatch
    ):
        monkeypatch.setenv(NOTIFY_SOCKET_ENV, "relative.sock")
        factory, stub = _stub_daemon(mocker)
        assert daemon_module._run(_args(sqlite_config_db.path)) == 0
        assert NOTIFY_SOCKET_ENV not in os.environ
        assert factory.call_args.kwargs["notifier"].enabled is False

    def test_without_the_variable_the_daemon_still_gets_a_disabled_notifier(
        self, sqlite_config_db, mocker, lock_spy, no_hard_exit, monkeypatch
    ):
        monkeypatch.delenv(NOTIFY_SOCKET_ENV, raising=False)
        factory, stub = _stub_daemon(mocker)
        assert daemon_module._run(_args(sqlite_config_db.path)) == 0
        assert factory.call_args.kwargs["notifier"].enabled is False


# ---------------------------------------------------------------------------
# A real daemon process
# ---------------------------------------------------------------------------

READY_WAIT = 60.0
STEP_WAIT = 40.0
POLL_WAIT = 40.0      # the poll interval is 10 s; the supervisor loop ticks every 5 s

_WRAPPER = (
    "import multiprocessing, sys\n"
    "if sys.argv[1] != 'default':\n"
    "    multiprocessing.set_start_method(sys.argv[1])\n"
    "from zfsbackup import daemon\n"
    "sys.argv = ['zfsbackup.daemon'] + sys.argv[2:]\n"
    "sys.exit(daemon.main())\n"
)

START_METHODS = ["fork", "spawn", "forkserver"]


class NotifyDaemon(RealDaemon):
    """A `RealDaemon` (hermetic env, stub `zfs`, `--dry-run`-able) whose
    environment names a notify socket and whose start method can be chosen."""

    def __init__(self, tmp_path, monkeypatch, *, notify_socket, method="default", dump=None):
        super().__init__(tmp_path, monkeypatch)
        self.method = method
        self.dump = dump
        if notify_socket is not None:
            self.env[NOTIFY_SOCKET_ENV] = notify_socket
        else:
            self.env.pop(NOTIFY_SOCKET_ENV, None)
        if dump is not None:
            stub = tmp_path / "dump_zfs"
            stub.write_text(
                f"#!/bin/sh\n{{ /usr/bin/env; echo \"--- $*\"; }} >> '{dump}'\nexit 0\n"
            )
            stub.chmod(0o755)
            self.env["ZFS_CMD"] = str(stub)
            self.env["ZPOOL_CMD"] = str(stub)

    def start(self, *flags) -> None:
        self._log_fh = open(self.log_path, "wb")
        self.proc = subprocess.Popen(
            [sys.executable, "-c", _WRAPPER, self.method, "-c", str(self.db), *flags],
            stdout=self._log_fh, stderr=subprocess.STDOUT, cwd=ROOT, env=self.env,
            start_new_session=True,
        )

    def alive(self) -> bool:
        return self.proc.poll() is None

    def terminate(self, timeout=60) -> int:
        os.kill(self.proc.pid, signal.SIGTERM)
        return self.proc.wait(timeout=timeout)


@pytest.fixture
def notify_daemon(tmp_path, monkeypatch, sockdir):
    made = []
    servers = []

    def make(*, method="default", serve=True, address=None, dump=False, abstract=None):
        server = None
        notify = address
        if serve:
            if abstract is not None:
                server = Server("\0" + abstract)
                notify = "@" + abstract
            else:
                server = Server(os.path.join(sockdir, "n.sock"))
                notify = server.address
            servers.append(server)
        d = NotifyDaemon(
            tmp_path, monkeypatch, notify_socket=notify, method=method,
            dump=(tmp_path / "zfs_env.txt") if dump else None,
        )
        d.import_config()
        made.append(d)
        return d, server

    yield make
    for d in made:
        d.stop()
    for s in servers:
        s.close()


def method_or_skip(method):
    if method not in multiprocessing.get_all_start_methods():
        pytest.skip(f"no {method} start method here")


def take(server, d, predicate, timeout=STEP_WAIT):
    return server.take_until(predicate, timeout, alive=d.alive, context=d.log)


def is_ready(msg):
    return msg.startswith("READY=1")


def process_group_members(pgid):
    """pids still in the daemon's own process group (it leads a session)."""
    out = subprocess.run(
        ["ps", "-ax", "-o", "pid=,pgid="], capture_output=True, text=True
    ).stdout
    return [
        int(parts[0]) for parts in (line.split() for line in out.splitlines())
        if len(parts) == 2 and parts[1] == str(pgid)
    ]


@pytest.mark.integration
@pytest.mark.slow
class TestRealDaemonNotifies:
    @pytest.mark.parametrize("method", START_METHODS)
    def test_the_full_sequence_ready_reload_ready_stop(self, notify_daemon, method):
        method_or_skip(method)
        d, srv = notify_daemon(method=method)
        d.start("--dry-run")

        # READY is the first thing sent: the startup tick says nothing.
        got = take(srv, d, is_ready, READY_WAIT)
        assert got == ["READY=1\nSTATUS=generation 0"]

        # SIGHUP, nothing changed: RELOADING=1 + MONOTONIC_USEC, then READY=1.
        low = time.monotonic_ns() // 1000
        os.kill(d.supervisor, signal.SIGHUP)
        got = take(srv, d, is_ready)
        high = time.monotonic_ns() // 1000
        assert len(got) == 2, got
        first, second = lines(got[0]), lines(got[1])
        assert first[0] == "RELOADING=1" and first[1].startswith("MONOTONIC_USEC=")
        usec = int(first[1].split("=", 1)[1])
        assert low - 5_000_000 <= usec <= high + 5_000_000     # the same CLOCK_MONOTONIC
        assert second == ["READY=1", "STATUS=generation 0"]

        # A committed change plus SIGHUP: the new generation is in the status.
        d.raw_update("snapshot_prefix = 'reloaded'")
        os.kill(d.supervisor, signal.SIGHUP)
        got = take(srv, d, is_ready)
        assert [lines(m)[0] for m in got] == ["RELOADING=1", "READY=1"], got
        assert lines(got[1])[1] == "STATUS=generation 1"
        d.wait_applied(1)
        quiet = srv.drain(0.3)
        assert not [m for m in quiet if m.startswith(("READY", "RELOADING", "STOPPING"))], quiet

        # SIGTERM: STOPPING=1, the last thing sent, and a clean exit.
        os.kill(d.supervisor, signal.SIGTERM)
        got = take(srv, d, lambda m: m.startswith("STOPPING=1"), 30)
        assert got[-1] == "STOPPING=1"
        assert d.proc.wait(timeout=90) == 0, d.log()
        assert srv.drain(0.3) == []
        assert NOTIFY_SOCKET_ENV not in d.log()
        deadline = time.monotonic() + 15
        while process_group_members(d.supervisor) and time.monotonic() < deadline:
            time.sleep(0.2)
        assert process_group_members(d.supervisor) == []

    def test_a_finally_rejected_candidate_still_sends_ready_and_names_the_rejection(
        self, notify_daemon
    ):
        """An address that does not exist on this host (EADDRNOTAVAIL, no DNS
        involved) is a final rejection, not a deferral."""
        d, srv = notify_daemon()
        d.start("--dry-run")
        take(srv, d, is_ready, READY_WAIT)
        d.raw_update("api_host = '10.255.255.1'")
        os.kill(d.supervisor, signal.SIGHUP)
        got = take(srv, d, is_ready)
        assert [lines(m)[0] for m in got] == ["RELOADING=1", "READY=1"], got
        assert lines(got[1])[1] == "STATUS=generation 0; generation 1 rejected"
        assert "deferred" not in got[1]
        d.wait_for_regex(r"configuration generation 1 rejected \(trigger: SIGHUP\)", 10)

        # A fix is a new write: the SIGHUP after it accepts and drops the rejection.
        d.raw_update("api_host = '127.0.0.1'")
        os.kill(d.supervisor, signal.SIGHUP)
        got = take(srv, d, is_ready)
        assert [lines(m)[0] for m in got] == ["RELOADING=1", "READY=1"], got
        assert lines(got[1])[1] == "STATUS=generation 2"
        # Back to the running config: only the key moves, no worker is turned over.
        d.wait_for_regex(r"configuration generation 2 is identical to running generation 0", 10)
        assert d.terminate() == 0

    def test_a_candidate_whose_port_is_busy_is_deferred_then_applied_by_the_poll(
        self, notify_daemon
    ):
        d, srv = notify_daemon()
        d.start("--dry-run")
        take(srv, d, is_ready, READY_WAIT)
        blocker, port = _listening_socket()
        try:
            d.raw_update(f"api_port = {port}")
            os.kill(d.supervisor, signal.SIGHUP)
            got = take(srv, d, is_ready)
            assert [lines(m)[0] for m in got] == ["RELOADING=1", "READY=1"], got
            assert lines(got[1])[1] == "STATUS=generation 0; generation 1 deferred"
            assert "rejected" not in got[1]
            d.wait_for_regex(r"configuration generation 1 deferred \(trigger: SIGHUP\)", 10)
        finally:
            blocker.close()

        # No signal and no new write: the 10 s poll re-checks the deferred key,
        # finds the port free and applies it -- a STATUS change only.
        got = take(srv, d, lambda m: m.startswith("STATUS="), POLL_WAIT)
        assert got == ["STATUS=generation 1"], got
        d.wait_applied(1)
        assert d.alive()
        assert d.terminate() == 0

    def test_no_zfs_invocation_ever_sees_the_notify_socket(self, notify_daemon, tmp_path):
        d, srv = notify_daemon(dump=True)
        d.start("--dry-run")
        take(srv, d, is_ready, READY_WAIT)
        dump = tmp_path / "zfs_env.txt"
        d.wait_for(
            lambda _t: dump.exists() and dump.read_text().count("--- ") >= 2,
            STEP_WAIT, "the stub zfs to be invoked by the workers",
        )
        os.kill(d.supervisor, signal.SIGHUP)                 # and again after a reload
        take(srv, d, is_ready)
        os.kill(d.supervisor, signal.SIGTERM)
        assert d.proc.wait(timeout=90) == 0, d.log()
        text = dump.read_text()
        assert text.count("--- ") >= 2
        assert "PATH=" in text and "ZFSB_TEST_SESSION" in text  # it did dump a real environment
        assert NOTIFY_SOCKET_ENV not in text

    @pytest.mark.parametrize(
        "address_kind", ["unreachable", "invalid"], ids=["unreachable", "invalid"]
    )
    def test_an_unusable_socket_does_not_stop_the_daemon_starting_or_stopping(
        self, notify_daemon, sockdir, address_kind
    ):
        address = (
            os.path.join(sockdir, "nobody.sock") if address_kind == "unreachable" else "relative.sock"
        )
        d, _ = notify_daemon(serve=False, address=address)
        d.start("--dry-run")
        d.wait_ready()
        assert d.alive()
        os.kill(d.supervisor, signal.SIGHUP)                 # more sends, more failures
        d.wait_for_regex(r"SIGHUP: configuration unchanged", 10)
        assert d.terminate() == 0, d.log()
        log = d.log()
        if address_kind == "unreachable":
            assert log.count("cannot notify systemd") == 1
            assert "Traceback" not in log
        else:
            assert log.count(f"ignoring {NOTIFY_SOCKET_ENV}='relative.sock'") == 1
            assert "cannot notify systemd" not in log

    def test_without_the_variable_nothing_changes(self, notify_daemon):
        d, _ = notify_daemon(serve=False, address=None)
        d.start("--dry-run")
        d.wait_ready()
        assert d.terminate() == 0
        assert "notify" not in d.log().lower()

    @pytest.mark.skipif(not IS_LINUX, reason="the abstract socket namespace is Linux only")
    def test_an_abstract_socket_name_works(self, notify_daemon):
        d, srv = notify_daemon(abstract=f"zsn-{os.getpid()}-{time.monotonic_ns()}")
        d.start("--dry-run")
        assert take(srv, d, is_ready, READY_WAIT) == ["READY=1\nSTATUS=generation 0"]
        os.kill(d.supervisor, signal.SIGTERM)
        take(srv, d, lambda m: m == "STOPPING=1", 30)
        assert d.proc.wait(timeout=90) == 0

    def test_test_config_sends_nothing(self, notify_daemon):
        d, srv = notify_daemon()
        result = subprocess.run(
            [sys.executable, "-m", "zfsbackup.daemon", "-c", str(d.db), "--test-config"],
            cwd=ROOT, env=d.env, capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "Configuration is valid" in result.stdout + result.stderr
        assert srv.recv(1.0) is None

    def test_a_start_that_waits_for_a_previous_daemons_workers_extends_the_timeout(
        self, notify_daemon
    ):
        """Mirrors the developer's verify_extend.py: a lock holder keeps the
        start waiting; every 1 s poll asks systemd for 30 s more; READY comes
        last and only once the wait is over."""
        d, srv = notify_daemon()
        from zfsbackup.config.store import resolve_config_path

        resolved = resolve_config_path(str(d.db))
        path = daemonlock.ensure_workers_lock(resolved)
        fd = daemonlock.hold_workers_lock(path)
        released = False
        try:
            d.start("--dry-run")
            got = []
            deadline = time.monotonic() + 20
            while sum("EXTEND_TIMEOUT_USEC=30000000" in m for m in got) < 3:
                assert time.monotonic() < deadline, (got, d.log())
                msg = srv.recv(0.5)
                if msg is not None:
                    got.append(msg)
            assert not any(m.startswith("READY") for m in got)
            os.close(fd)
            released = True
            got += take(srv, d, is_ready, READY_WAIT)
        finally:
            if not released:
                os.close(fd)
        assert got[-1] == "READY=1\nSTATUS=generation 0"
        assert all(m.startswith("EXTEND_TIMEOUT_USEC=30000000") for m in got[:-1])
        assert d.terminate() == 0

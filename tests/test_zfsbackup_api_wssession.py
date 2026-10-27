"""`_SessionSocket`: the socket a websocket session gets from the API server.

Background. The route of a `/stream` websocket returns while simple-websocket's reader thread
may still be inside `SSL_read` on the connection's `SSLSocket`. Werkzeug then closed the
connection, the descriptor number was free, and the NEXT accepted connection got it: the stale
reader consumed that peer's ClientHello and sent its alert to the wrong peer. Several websocket
sessions back to back in one process then failed intermittently on Linux/OpenSSL 3.0
(4 runs in 30 on Python 3.12). The fix is in `workers._api_server_class()`:

* `ApiRequestHandler.make_environ` puts a `_SessionSocket` in `environ['werkzeug.socket']` for
  websocket requests only (a plain request keeps the raw connection);
* writes go through one lock and `send` sends fully; reads are unlocked, with an in-flight
  counter; `close()` is a no-op; `force_eof()` is a plain `socket.socket.shutdown(SHUT_RDWR)`;
* `end(timeout)` refuses new I/O (recv -> EOF, write -> EPIPE), waits for the calls in flight,
  forces EOF and waits again; it is False if a call never leaves;
* `run_wsgi`'s `finally` calls `end(server.ws_end_timeout)` before werkzeug closes the
  connection, and logs a WARNING when it returns False;
* `api._end_websocket` uses `ws.sock.force_eof()` when it exists.

Layout
------

* `TestSessionSocketUnit` -- the class itself, on fakes and on real loopback sockets (no TLS).
* `TestSimpleWebsocketContract` -- every method simple-websocket calls on its socket is offered.
* `TestEndWebsocketForceEof` -- `api._end_websocket` prefers `force_eof()`.
* `TestMakeEnviron` -- the session socket is installed for websocket requests only.
* `TestEndWiring` -- `run_wsgi` ends the session (timeout, WARNING) and `ws_end_timeout` bounds
  the slot of a silent client on a plain flask-sock route.
* `TestEndWebsocketOverTheSessionSocket` -- `_end_websocket` on a real session.
* `TestFdNotClosedUnderIo` -- the regression test: server-side `SSLSocket` I/O is instrumented,
  and no connection may be closed (or its TLS layer torn down) while another thread is inside
  a read or write on it. Fails without the fix (the descriptor is closed under the reader on
  every session whose client stays silent), deterministically.

K-2 (one thread inside OpenSSL at a time) adds, at the end of the file:

* `ScriptTls` -- a real `AF_UNIX` socket playing the non-blocking `SSLSocket`: scripted
  `recv`/`send` outcomes (`SSLWantReadError`, `SSLWantWriteError`, `BlockingIOError`, partial
  counts), real readiness (`peer.sendall`, `fill()`/`drain()`, `force_eof`), a record of every
  call and of whether the session's TLS lock was held during it.
* `TestNonBlockingIsLazy`, `TestNonBlockingModeOverTheServer` -- `settimeout(0.0)` at the first
  TLS call only; a refusal before the upgrade is still written to a blocking socket.
* `TestRetryProtocol`, `TestPendingShortcut`, `TestTimeouts`, `TestForcedEofWakesTheWait`,
  `TestFairness`, `TestPayloads`, `TestAFailedWriteBreaksTheWrites` -- the `_io` protocol.
* `TestRealTlsReaderAndWriter`, `TestRealTlsKeyUpdates` -- the regression test: a reader and a
  writer thread over one real TLS 1.3 connection (and `openssl s_server` KeyUpdates).

Hygiene: sockets are `127.0.0.1` port 0, every server is shut down in a `finally`, threads are
joined, and a network test finishes by waiting for all connection slots to come back.
"""

import ast
import collections
import contextlib
import errno
import inspect
import json
import logging
import math
import re
import selectors
import shutil
import socket
import ssl
import subprocess
import textwrap
import threading
import time
from array import array
from types import SimpleNamespace

import pytest
import websocket
from flask import Flask
from flask_sock import Sock

from _tls_support import (
    IO_TIMEOUT,
    LOOPBACK,
    client_identity,
    insecure_client_context,
    make_gateway,
    server_identity,
    trusted_pin,
)
from zfsbackup import api, workers

WORKERS_LOGGER = "zfsbackup.workers"
API_LOGGER = "zfsbackup.api"


# ---------------------------------------------------------------------------
# Rig
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def pki(tmp_path_factory):
    base = tmp_path_factory.mktemp("wssession_pki")
    gateway, _server, trust = make_gateway(base / "srv.tls")
    alice = client_identity(base / "alice.tls")
    trust.set_pins([trusted_pin(alice, "alice")], generation=2)
    gateway.refresh()
    return SimpleNamespace(
        gateway=gateway,
        ctx_pinned=insecure_client_context(alice),
        ctx_anon=insecure_client_context(),
    )


@pytest.fixture(scope="module")
def handler_cls(pki):
    server = workers._api_server_class()(LOOPBACK, 0, Flask("x"), pki.gateway)
    try:
        return server.RequestHandlerClass
    finally:
        server.server_close()


@pytest.fixture(scope="module")
def session_cls(handler_cls):
    """The `_SessionSocket` class (local to `_api_server_class`), found through the closure of
    `make_environ`, which instantiates it."""
    cls = inspect.getclosurevars(handler_cls.make_environ).nonlocals.get("_SessionSocket")
    assert cls is not None, "make_environ no longer refers to _SessionSocket"
    return cls


class Serving:
    """An `ApiServer` served from a thread; shut down on exit, every slot checked back."""

    def __init__(self, app, gateway, **kwargs):
        self.server = workers._api_server_class()(LOOPBACK, 0, app, gateway, **kwargs)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.port = self.server.server_port

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                assert self.wait_slots_free(10.0), self.slot_state()
        finally:
            self.server.shutdown()
            self.thread.join(10)
            self.server.server_close()
        assert not self.thread.is_alive()

    def slots_free(self) -> bool:
        s = self.server
        return s._slots._value == s.max_connections and s._anonymous_slots._value == s.max_anonymous

    def slot_state(self) -> str:
        s = self.server
        return (f"slots free {s._slots._value}/{s.max_connections}, "
                f"anonymous free {s._anonymous_slots._value}/{s.max_anonymous}")

    def wait_slots_free(self, timeout: float) -> bool:
        return eventually(self.slots_free, timeout, 0.02)

    def spy_handlers(self, hook):
        """Subclass the handler class so `hook(handler, environ)` runs in `make_environ`
        (after the real one). Call before the first connection."""
        base = self.server.RequestHandlerClass

        class SpyHandler(base):
            def make_environ(self):
                environ = super().make_environ()
                hook(self, environ)
                return environ

        self.server.RequestHandlerClass = SpyHandler
        return self


def eventually(predicate, timeout=10.0, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return bool(predicate())


def errors(caplog):
    return [r for r in caplog.records if r.levelno >= logging.ERROR]


def make_app():
    """A `/stream` websocket that waits for `{"done": true}`, acks, and returns -- with no
    `_end_websocket`: the server alone must see the reader out."""
    app = Flask("wssession_tests")
    sock = Sock(app)

    @sock.route("/stream")
    def stream(ws):
        while True:
            data = ws.receive(timeout=10)
            if data is None:
                return
            if isinstance(data, str) and json.loads(data).get("done"):
                break
        ws.send(json.dumps({"status": "ok"}))

    @app.route("/plain", methods=["GET"])
    def plain():
        return "hello"

    return app


def tls_connect(port, ctx):
    raw = socket.create_connection((LOOPBACK, port), timeout=IO_TIMEOUT)
    return ctx.wrap_socket(raw)


def open_stream(port, ctx, path="/stream"):
    """-> `(tls, ws)` with the websocket handshake done."""
    tls = tls_connect(port, ctx)
    ws = websocket.WebSocket()
    ws.connect(f"wss://{LOOPBACK}:{port}{path}", socket=tls, timeout=IO_TIMEOUT)
    return tls, ws


def finish_session(tls, ws):
    """`done`, then the ack."""
    ws.send_binary(b"x" * 4096)
    ws.send(json.dumps({"done": True}))
    assert json.loads(ws.recv()) == {"status": "ok"}


def reset(sock):
    import struct
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    sock.close()


def tcp_pair():
    listener = socket.socket()
    listener.bind((LOOPBACK, 0))
    listener.listen(1)
    a = socket.create_connection(listener.getsockname())
    b, _ = listener.accept()
    listener.close()
    return a, b


def in_thread(fn, *args):
    """Run `fn(*args)` in a thread -> `(thread, box)`; `box` gets `value` or `exc`."""
    box = SimpleNamespace(value=None, exc=None)

    def run():
        try:
            box.value = fn(*args)
        except BaseException as exc:    # noqa: BLE001 -- handed to the test
            box.exc = exc

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, box


def bounded(fn, *args, limit=5.0):
    """`fn(*args)` on a thread, which must return within `limit`: a call another thread's wait
    can block (a lock held across a `poll`) fails the test instead of hanging it."""
    thread, box = in_thread(fn, *args)
    thread.join(limit)
    if thread.is_alive():
        pytest.fail(f"{getattr(fn, '__name__', fn)} did not return within {limit}s: blocked "
                    "behind another thread's wait")
    if box.exc is not None:
        raise box.exc
    return box.value


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeTls:
    """A stand-in for the `SSLSocket` (not a real socket: `force_eof` is stubbed out on the
    session under test). Records every call; `recv` can block until released; `send` can take
    time, and notices overlapping writers.

    The session reads `gettimeout()` once, when it is created, and puts the connection in
    non-blocking mode (`settimeout(0.0)`) at its first TLS call; it writes with `send` (a slice
    at a time, until all of it is gone) and never calls `sendall`. Both calls run inside the
    session's TLS lock here, so a `recv_gate` or `send_delay` is a thread stuck inside
    OpenSSL -- a real non-blocking call never waits (see `ScriptTls` for the waiting cases)."""

    def __init__(self, *, recv_gate=None, send_delay=0.0, recv_data=b"data", timeout=None):
        self.recv_gate = recv_gate
        self.send_delay = send_delay
        self.recv_data = recv_data
        self.timeout = timeout
        self.timeouts_set = []
        self.recv_entered = threading.Event()
        self.recv_returned = False
        self.calls = []
        self.sent = []
        self._writers = 0
        self.max_writers = 0
        self._lock = threading.Lock()
        self.closed_called = False

    def gettimeout(self):
        return self.timeout

    def settimeout(self, value):
        self.timeouts_set.append(value)
        self.timeout = value

    def recv(self, bufsize, flags=0):
        self.calls.append(("recv", bufsize, flags))
        self.recv_entered.set()
        if self.recv_gate is not None:
            assert self.recv_gate.wait(IO_TIMEOUT), "test bug: recv never released"
        self.recv_returned = True
        return self.recv_data

    def send(self, data, flags=0):
        self.calls.append(("send", bytes(data), flags))
        with self._lock:
            self._writers += 1
            self.max_writers = max(self.max_writers, self._writers)
        try:
            if self.send_delay:
                time.sleep(self.send_delay)
            self.sent.append(bytes(data))
            return len(data)
        finally:
            with self._lock:
                self._writers -= 1

    def sendall(self, data, flags=0):        # the session must never call this one
        raise AssertionError("the session used sendall(), not send()")

    def fileno(self):
        return 42

    def close(self):
        self.closed_called = True
        raise AssertionError("the session closed the connection")


class SockTls(socket.socket):
    """A real connected loopback socket standing in for the `SSLSocket`. `hold` (an Event) makes
    `recv` block on it instead of on the network -- and `shutdown(2)` does not wake it, like a
    thread stuck in a TLS call that ignores EOF. `entered` is set when a `recv` is blocked."""

    @classmethod
    def pair(cls, hold=None):
        a, b = tcp_pair()
        me = cls(fileno=a.detach())
        me.hold = hold
        me.entered = threading.Event()
        me.shutdown_calls = []
        return me, b

    def recv(self, bufsize, flags=0):
        self.entered.set()
        if self.hold is not None:
            assert self.hold.wait(IO_TIMEOUT), "test bug: never released"
            return b""
        return super().recv(bufsize, flags)

    def shutdown(self, how):                 # SSLSocket.shutdown analogue: must NOT be what runs
        self.shutdown_calls.append(how)
        super().shutdown(how)


_DONTWAIT = getattr(socket, "MSG_DONTWAIT", 0)
_PASS = object()                                 # a script step: behave like the plain socket
WantRead, WantWrite = ssl.SSLWantReadError, ssl.SSLWantWriteError


def tls_lock_held(sess, wait=0.05):
    """True iff somebody holds the session's TLS lock: another thread cannot take it within
    `wait`. (Called from inside a fake's `recv`/`send`, that is "the call runs under the
    lock".) Only the context-manager protocol of the lock is used."""
    got = threading.Event()

    def probe():
        with sess._tls_lock:
            got.set()

    thread = threading.Thread(target=probe, daemon=True)
    thread.start()
    held = not got.wait(wait)
    if not held:
        thread.join(IO_TIMEOUT)
    return held


def tls_lock_free(sess, wait=5.0):
    """True iff the session's TLS lock can be taken (generously: returns as soon as it is)."""
    return not tls_lock_held(sess, wait)


class ScriptTls(socket.socket):
    """A real connected `AF_UNIX` socket playing the non-blocking `SSLSocket`.

    The session polls its descriptor, so what a thread waits for is real: `peer.sendall(..)`
    makes it readable, `fill()` makes it unwritable until `drain()`, `force_eof()` shuts it
    down. `recv` / `send` follow a script: each call takes the next step of `scripts[op]`
    -- an exception class or instance (raised), a callable (called with the argument), or a
    value (returned: bytes for `recv`, the byte count for `send`) -- and once the script is
    empty they act as a non-blocking plain socket (`BlockingIOError` when nothing can be
    done, which the session reads as "wait for the descriptor").

    Records every call (`log`), how many threads are inside a call at once (`max_inside`:
    more than 1 means the session let two threads into the "SSL object"), and -- once
    `session` is set -- whether the session's TLS lock was held during each call
    (`lock_checks`). `pending()` is OpenSSL's `SSL_pending`."""

    @classmethod
    def pair(cls, timeout=None):
        a, b = socket.socketpair()
        me = cls(fileno=a.detach())
        me.peer = b
        me.session = None
        me.guard = threading.Lock()
        me.scripts = {"recv": collections.deque(), "send": collections.deque()}
        me.log = []
        me.lock_checks = []
        me.pending_checks = []              # same, for each pending() call
        me.inside = 0
        me.max_inside = 0
        me.pending_result = False
        me.pending_calls = 0
        me.settimeout(timeout)
        return me

    def script(self, op, *steps):
        with self.guard:
            self.scripts[op].extend(steps)

    def attempts(self, op):
        with self.guard:
            return sum(1 for entry in self.log if entry[0] == op)

    def payloads(self):
        """The bytes of every `send` call, in order."""
        with self.guard:
            return [entry[2] for entry in self.log if entry[0] == "send"]

    def _run(self, op, arg, passthrough):
        with self.guard:
            self.log.append((op, threading.current_thread().name,
                             bytes(arg) if op == "send" else arg))
            self.inside += 1
            self.max_inside = max(self.max_inside, self.inside)
            script = self.scripts[op]
            step = script.popleft() if script else _PASS
        try:
            if self.session is not None:
                self.lock_checks.append((op, tls_lock_held(self.session)))
            if step is _PASS:
                return passthrough()
            if isinstance(step, type) and issubclass(step, BaseException):
                raise step()
            if isinstance(step, BaseException):
                raise step
            if callable(step):
                return step(arg)
            return step
        finally:
            with self.guard:
                self.inside -= 1

    def recv(self, bufsize, flags=0):
        return self._run("recv", bufsize,
                         lambda: socket.socket.recv(self, bufsize, flags | _DONTWAIT))

    def send(self, data, flags=0):
        return self._run("send", data,
                         lambda: socket.socket.send(self, data, flags | _DONTWAIT))

    def pending(self):
        with self.guard:
            self.pending_calls += 1
        if self.session is not None:
            self.pending_checks.append(tls_lock_held(self.session))
        result = self.pending_result
        return result() if callable(result) else result

    def fill(self):
        """Fill the send buffer, so the next write has to wait for the peer to read."""
        previous = self.gettimeout()
        self.settimeout(0.0)
        total = 0
        try:
            while True:
                try:
                    total += socket.socket.send(self, b"f" * 4096)
                except (BlockingIOError, InterruptedError):
                    return total
        finally:
            self.settimeout(previous)

    def drain(self):
        """The peer reads everything the session sent: the descriptor is writable again."""
        self.peer.setblocking(False)
        total = 0
        try:
            while True:
                try:
                    data = self.peer.recv(65536)
                except (BlockingIOError, InterruptedError):
                    return total
                if not data:
                    return total
                total += len(data)
        finally:
            self.peer.setblocking(True)

    def dispose(self):
        for sock in (self.peer, self):
            try:
                sock.close()
            except OSError:
                pass


@pytest.fixture
def script_tls():
    made = []

    def make(timeout=None):
        tls = ScriptTls.pair(timeout)
        made.append(tls)
        return tls

    yield make
    for tls in made:
        tls.dispose()


def bound(session_cls, tls):
    """A session over `tls`, with the fake told which session it serves."""
    sess = session_cls(tls)
    tls.session = sess
    return sess


# ---------------------------------------------------------------------------
# The class on its own
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSessionSocketUnit:
    def test_recv_and_sendall_pass_through_while_the_session_lives(self, session_cls):
        tls = FakeTls()
        sess = session_cls(tls)
        assert sess.recv(10, 3) == b"data"
        sess.sendall(b"abc", 7)
        assert tls.calls == [("recv", 10, 3), ("send", b"abc", 7)]

    def test_recv_after_end_is_eof_and_does_not_touch_the_connection(self, session_cls):
        tls = FakeTls()
        sess = session_cls(tls)
        assert sess.end(1.0) is True
        assert sess.recv(4096) == b""
        assert sess.recv(1, 0) == b""
        assert tls.calls == []

    @pytest.mark.parametrize("write", ["sendall", "send"])
    def test_writes_after_end_raise_epipe_and_do_not_touch_the_connection(self, session_cls, write):
        tls = FakeTls()
        sess = session_cls(tls)
        assert sess.end(1.0) is True
        with pytest.raises(BrokenPipeError) as info:
            getattr(sess, write)(b"late")
        assert info.value.errno == errno.EPIPE
        assert tls.calls == []

    def test_end_with_nothing_in_flight_returns_true_at_once(self, session_cls):
        sess = session_cls(FakeTls())
        started = time.monotonic()
        assert sess.end(30.0) is True
        assert time.monotonic() - started < 1.0

    def test_end_is_repeatable(self, session_cls):
        sess = session_cls(FakeTls())
        assert sess.end(1.0) is True
        assert sess.end(1.0) is True

    def test_end_waits_for_a_blocked_recv_that_leaves_on_its_own(self, session_cls):
        gate = threading.Event()
        tls = FakeTls(recv_gate=gate)
        sess = session_cls(tls)
        forced = []
        sess.force_eof = lambda: forced.append(True)        # must not be needed
        reader, box = in_thread(sess.recv, 10)
        assert tls.recv_entered.wait(IO_TIMEOUT)
        timer = threading.Timer(0.3, gate.set)
        timer.start()
        try:
            started = time.monotonic()
            assert sess.end(10.0) is True
            elapsed = time.monotonic() - started
        finally:
            timer.cancel()
            gate.set()
            reader.join(IO_TIMEOUT)
        assert 0.2 < elapsed < 5.0, elapsed
        assert tls.recv_returned, "end() returned while a recv was still inside the connection"
        assert forced == []
        assert box.value == b"data" and box.exc is None

    def test_end_forces_eof_after_the_timeout_and_the_reader_leaves(self, session_cls):
        tls, peer = SockTls.pair()                           # the peer never sends
        try:
            sess = session_cls(tls)
            forced = []
            real_force = sess.force_eof
            sess.force_eof = lambda: (forced.append(time.monotonic()), real_force())
            reader, box = in_thread(sess.recv, 10)
            assert tls.entered.wait(IO_TIMEOUT)
            time.sleep(0.05)                                 # inside the kernel's recv
            started = time.monotonic()
            assert sess.end(0.4) is True
            elapsed = time.monotonic() - started
            reader.join(IO_TIMEOUT)
            assert not reader.is_alive()
            assert box.value == b"" and box.exc is None      # EOF, not an error
            assert len(forced) == 1
            assert forced[0] - started >= 0.35, "forced before the timeout"
            assert elapsed < 5.0
        finally:
            tls.close()
            peer.close()

    def test_end_returns_false_when_a_call_never_leaves(self, session_cls):
        hold = threading.Event()
        tls, peer = SockTls.pair(hold)
        try:
            sess = session_cls(tls)
            forced = []
            real_force = sess.force_eof
            sess.force_eof = lambda: (forced.append(True), real_force())
            reader, box = in_thread(sess.recv, 10)
            assert tls.entered.wait(IO_TIMEOUT)
            started = time.monotonic()
            assert sess.end(0.3) is False
            elapsed = time.monotonic() - started
            assert 0.55 < elapsed < 5.0, elapsed             # waited twice
            assert forced == [True]
            assert reader.is_alive()
            hold.set()                                       # now it leaves ...
            reader.join(IO_TIMEOUT)
            assert not reader.is_alive()
            assert sess.end(1.0) is True                     # ... and the next end is clean
        finally:
            hold.set()
            tls.close()
            peer.close()

    def test_end_waits_for_a_blocked_write_and_forces_it_out(self, session_cls):
        tls, peer = SockTls.pair()
        try:
            tls.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
            peer.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            sess = session_cls(tls)
            writer, box = in_thread(sess.sendall, b"z" * (64 * 1024 * 1024))   # the peer never reads
            time.sleep(0.3)
            assert writer.is_alive(), "the write did not block"
            started = time.monotonic()
            assert sess.end(0.3) is True
            assert time.monotonic() - started < 5.0
            writer.join(IO_TIMEOUT)
            assert not writer.is_alive()
            assert isinstance(box.exc, OSError)
        finally:
            tls.close()
            peer.close()

    def test_a_writer_queued_on_the_lock_when_end_runs_gets_epipe(self, session_cls):
        tls = FakeTls(send_delay=0.4)
        sess = session_cls(tls)
        first, box1 = in_thread(sess.sendall, b"first")
        assert eventually(lambda: tls._writers == 1, 5.0, 0.005)
        second, box2 = in_thread(sess.sendall, b"second")    # queued on the write lock
        time.sleep(0.1)
        assert sess.end(10.0) is True                        # waits for "first" only
        first.join(IO_TIMEOUT)
        second.join(IO_TIMEOUT)
        assert box1.exc is None
        assert isinstance(box2.exc, BrokenPipeError) and box2.exc.errno == errno.EPIPE
        assert tls.sent == [b"first"]

    def test_writes_from_two_threads_never_overlap(self, session_cls):
        tls = FakeTls(send_delay=0.05)
        sess = session_cls(tls)
        start = threading.Barrier(4)

        def write(tag):
            start.wait(IO_TIMEOUT)
            for i in range(3):
                sess.sendall(b"%s%d" % (tag, i))

        threads = [threading.Thread(target=write, args=(t,), daemon=True)
                   for t in (b"a", b"b", b"c", b"d")]
        for t in threads:
            t.start()
        for t in threads:
            t.join(IO_TIMEOUT)
        assert tls.max_writers == 1, f"{tls.max_writers} writers inside the connection at once"
        assert len(tls.sent) == 12

    def test_a_read_is_not_blocked_by_a_write_in_progress(self, session_cls, script_tls):
        """The reader thread must be able to receive (a peer's close frame) while the route's
        thread is in the middle of a write that is waiting for a peer that does not read. The
        write waits for the descriptor, not inside the TLS lock."""
        tls = script_tls()
        tls.fill()                                           # the peer never reads
        sess = bound(session_cls, tls)
        writer, wbox = in_thread(sess.sendall, b"slow" * 1000)
        assert eventually(lambda: tls.attempts("send") >= 1, 5.0, 0.005)
        time.sleep(0.1)
        assert writer.is_alive(), "the write did not have to wait"
        tls.peer.sendall(b"ping")
        started = time.monotonic()
        assert bounded(sess.recv, 10) == b"ping"
        assert time.monotonic() - started < 0.4
        assert writer.is_alive()
        tls.drain()                                          # the peer reads at last
        writer.join(IO_TIMEOUT)
        assert not writer.is_alive() and wbox.exc is None

    def test_close_does_nothing(self, session_cls):
        tls, peer = SockTls.pair()
        try:
            sess = session_cls(tls)
            assert sess.close() is None
            assert tls.fileno() >= 0
            peer.sendall(b"still open")
            assert sess.recv(64) == b"still open"
            sess.end(1.0)
            assert sess.close() is None
            assert tls.fileno() >= 0
        finally:
            tls.close()
            peer.close()

    def test_close_never_reaches_the_connection(self, session_cls):
        tls = FakeTls()                                      # its close() raises
        sess = session_cls(tls)
        sess.close()
        sess.close()
        assert tls.closed_called is False

    @pytest.mark.parametrize("data", [
        b"abcdef",
        bytearray(b"abcdef"),
        memoryview(b"abcdef"),
        memoryview(array("I", [1, 2, 3])),                   # 12 bytes, 3 items
    ], ids=["bytes", "bytearray", "memoryview", "wide-memoryview"])
    def test_send_returns_the_full_length_and_sends_it_all(self, session_cls, data):
        tls = FakeTls()                                      # its sendall() raises: only send
        sess = session_cls(tls)
        n = sess.send(data, 5)
        assert n == memoryview(data).nbytes
        assert tls.sent == [bytes(data)]
        assert tls.calls == [("send", bytes(data), 5)]

    def test_send_over_a_real_socket_delivers_every_byte(self, session_cls):
        tls, peer = SockTls.pair()
        try:
            sess = session_cls(tls)
            payload = bytes(range(256)) * 4096               # 1 MiB: more than one send() would take
            reader, box = in_thread(lambda: _read_exactly(peer, len(payload)))
            assert sess.send(payload) == len(payload)
            reader.join(IO_TIMEOUT)
            assert box.value == payload
        finally:
            tls.close()
            peer.close()

    def test_fileno_is_the_connections(self, session_cls):
        assert session_cls(FakeTls()).fileno() == 42

    def test_it_works_in_a_selector_as_simple_websocket_registers_it(self, session_cls):
        tls, peer = SockTls.pair()
        sel = selectors.DefaultSelector()
        try:
            sess = session_cls(tls)
            sel.register(sess, selectors.EVENT_READ, True)
            assert sel.select(0.05) == []
            peer.sendall(b"x")
            assert len(sel.select(IO_TIMEOUT)) == 1
        finally:
            sel.close()
            tls.close()
            peer.close()

    def test_nothing_else_is_offered(self, session_cls):
        """Anything not needed fails loudly rather than bypassing the write lock or the
        in-flight accounting."""
        sess = session_cls(FakeTls())
        for name in ("settimeout", "shutdown", "detach", "makefile", "setsockopt", "recv_into",
                     "do_handshake", "unwrap", "sock"):
            with pytest.raises(AttributeError):
                getattr(sess, name)

    def test_force_eof_is_the_plain_socket_shutdown(self, session_cls):
        """Not `SSLSocket.shutdown` (the subclass override here): that would drop the TLS
        object from under a thread inside it. The descriptor stays open; the peer sees EOF."""
        tls, peer = SockTls.pair()
        try:
            sess = session_cls(tls)
            sess.force_eof()
            assert tls.shutdown_calls == []
            assert tls.fileno() >= 0
            peer.settimeout(IO_TIMEOUT)
            assert peer.recv(10) == b""
        finally:
            tls.close()
            peer.close()

    def test_force_eof_wakes_a_reader_blocked_in_recv(self, session_cls):
        tls, peer = SockTls.pair()
        try:
            sess = session_cls(tls)
            reader, box = in_thread(sess.recv, 10)
            assert tls.entered.wait(IO_TIMEOUT)
            time.sleep(0.05)
            sess.force_eof()
            reader.join(IO_TIMEOUT)
            assert not reader.is_alive()
            assert box.value == b"" and box.exc is None
        finally:
            tls.close()
            peer.close()

    def test_force_eof_never_raises(self, session_cls):
        a, b = tcp_pair()
        sess = session_cls(a)
        a.close()
        sess.force_eof()                                     # EBADF swallowed
        b.close()
        lonely = socket.socket()
        try:
            session_cls(lonely).force_eof()                  # ENOTCONN swallowed
        finally:
            lonely.close()


def _read_exactly(sock, n):
    sock.settimeout(IO_TIMEOUT)
    chunks, got = [], 0
    while got < n:
        data = sock.recv(65536)
        if not data:
            break
        chunks.append(data)
        got += len(data)
    return b"".join(chunks)


# ---------------------------------------------------------------------------
# The contract with simple-websocket
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSimpleWebsocketContract:
    @staticmethod
    def _methods_used(cls):
        source = textwrap.dedent(inspect.getsource(cls))
        used = set()
        for node in ast.walk(ast.parse(source)):
            # self.sock.<attr>
            if (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Attribute)
                    and node.value.attr == "sock" and isinstance(node.value.value, ast.Name)
                    and node.value.value.id == "self"):
                used.add(node.attr)
        return used

    def test_every_method_the_server_side_calls_on_its_socket_is_offered(self, session_cls):
        from simple_websocket import ws as swws
        used = self._methods_used(swws.Base) | self._methods_used(swws.Server)
        assert {"recv", "send", "close"} <= used, f"the scan found {used}: the library changed"
        missing = sorted(name for name in used if not hasattr(session_cls, name))
        assert not missing, f"simple-websocket calls {missing} on its socket"

    def test_the_socket_itself_is_registered_with_a_selector(self, session_cls):
        """`Base._thread` does `sel.register(self.sock, ...)` (the ping interval): `fileno`."""
        from simple_websocket import ws as swws
        source = inspect.getsource(swws.Base)
        assert re.search(r"register\(\s*self\.sock", source)
        assert hasattr(session_cls, "fileno")

    def test_api_needs_force_eof_and_the_run_wsgi_needs_end(self, session_cls):
        for name in ("force_eof", "end", "sendall"):
            assert callable(getattr(session_cls, name))


# ---------------------------------------------------------------------------
# api._end_websocket prefers force_eof()
# ---------------------------------------------------------------------------


class FakeWs:
    def __init__(self, thread, sock):
        self.thread = thread
        self.sock = sock
        self.closes = 0

    def close(self):
        self.closes += 1


@pytest.mark.unit
class TestEndWebsocketForceEof:
    @pytest.fixture(autouse=True)
    def _short_join(self, monkeypatch):
        monkeypatch.setattr(api, "WS_READER_JOIN_TIMEOUT", 0.25)

    @staticmethod
    def _blocked_reader():
        gate = threading.Event()
        thread = threading.Thread(target=gate.wait, daemon=True)
        thread.start()
        return gate, thread

    def test_force_eof_is_called_once_for_a_reader_that_stays_and_it_leaves(self, monkeypatch):
        gate, thread = self._blocked_reader()
        raw = []
        monkeypatch.setattr(socket.socket, "shutdown", lambda self, how: raw.append(how))
        calls = []
        sock = SimpleNamespace(force_eof=lambda: (calls.append(1), gate.set()))
        started = time.monotonic()
        api._end_websocket(FakeWs(thread, sock))
        assert calls == [1]
        assert not thread.is_alive()
        assert time.monotonic() - started < 3.0
        assert raw == []

    def test_it_is_preferred_over_the_raw_shutdown_of_a_socket(self, monkeypatch):
        """`ws.sock` that is a real `socket.socket` AND has `force_eof`: the latter is used."""
        gate, thread = self._blocked_reader()
        a, b = tcp_pair()
        raw = []
        monkeypatch.setattr(socket.socket, "shutdown", lambda self, how: raw.append(how))
        calls = []
        try:
            class Both(socket.socket):
                def force_eof(self):
                    calls.append(1)
                    gate.set()

            both = Both(fileno=a.detach())
            api._end_websocket(FakeWs(thread, both))
            assert calls == [1] and raw == []
            both.close()
        finally:
            gate.set()
            b.close()

    def test_it_is_not_called_when_the_reader_leaves_by_itself(self):
        thread = threading.Thread(target=lambda: time.sleep(0.05))
        thread.start()
        calls = []
        ws = FakeWs(thread, SimpleNamespace(force_eof=lambda: calls.append(1)))
        api._end_websocket(ws)
        assert ws.closes == 1 and calls == [] and not thread.is_alive()

    def test_an_oserror_from_force_eof_is_swallowed_and_the_reader_is_given_another_wait(self):
        gate, thread = self._blocked_reader()
        calls = []

        def broken():
            calls.append(1)
            raise OSError(errno.ENOTCONN, "not connected")

        try:
            started = time.monotonic()
            api._end_websocket(FakeWs(thread, SimpleNamespace(force_eof=broken)))
            assert calls == [1]
            assert time.monotonic() - started >= 0.45        # two joins
            assert thread.is_alive()
        finally:
            gate.set()
            thread.join(2)

    @pytest.mark.parametrize("sock", [None, SimpleNamespace(force_eof=None), SimpleNamespace()],
                             ids=["none", "not-callable", "no-force-eof"])
    def test_a_socket_without_force_eof_that_is_not_a_socket_is_left_alone(self, sock):
        gate, thread = self._blocked_reader()
        try:
            api._end_websocket(FakeWs(thread, sock))         # does not raise
            assert thread.is_alive()
        finally:
            gate.set()
            thread.join(2)

    def test_the_raw_socket_fallback_is_still_there(self, monkeypatch):
        gate, thread = self._blocked_reader()
        a, b = tcp_pair()
        raw = []
        monkeypatch.setattr(socket.socket, "shutdown",
                            lambda self, how: (raw.append(how), gate.set()))
        try:
            api._end_websocket(FakeWs(thread, a))
            assert raw == [socket.SHUT_RDWR]
        finally:
            gate.set()
            a.close()
            b.close()


# ---------------------------------------------------------------------------
# make_environ
# ---------------------------------------------------------------------------

WS_HEADERS = (
    "Upgrade: websocket\r\nConnection: Upgrade\r\n"
    "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
)


def raw_http(port, ctx, request, *, read_timeout=IO_TIMEOUT):
    """Send `request` over TLS, read until the server closes. -> the bytes."""
    tls = tls_connect(port, ctx)
    try:
        tls.sendall(request)
        tls.settimeout(read_timeout)
        got = b""
        while True:
            try:
                data = tls.recv(4096)
            except (OSError, ssl.SSLError):
                break
            if not data:
                break
            got += data
        return got
    finally:
        tls.close()


@pytest.mark.integration
class TestMakeEnviron:
    def test_the_handler_class_defaults(self, handler_cls):
        assert handler_cls.session_socket is None

    def test_a_websocket_request_gets_the_session_socket(self, pki, session_cls):
        seen = []
        with Serving(make_app(), pki.gateway, ws_end_timeout=0.3).spy_handlers(
            lambda h, env: seen.append(SimpleNamespace(
                handler=h, sock=env["werkzeug.socket"], session=h.session_socket))
        ) as s:
            tls, ws = open_stream(s.port, pki.ctx_pinned)
            finish_session(tls, ws)
            assert s.wait_slots_free(10.0), s.slot_state()
            tls.close()
        (rec,) = seen
        assert isinstance(rec.sock, session_cls)
        assert rec.session is rec.sock
        assert rec.sock._tls is rec.handler.connection
        assert isinstance(rec.handler.connection, ssl.SSLSocket)

    @pytest.mark.parametrize("who", ["pinned", "anonymous"])
    def test_a_plain_request_keeps_the_raw_connection(self, pki, session_cls, who):
        ctx = pki.ctx_pinned if who == "pinned" else pki.ctx_anon
        seen = []
        with Serving(make_app(), pki.gateway).spy_handlers(
            lambda h, env: seen.append(SimpleNamespace(
                handler=h, sock=env["werkzeug.socket"], session=h.session_socket))
        ) as s:
            got = raw_http(s.port, ctx,
                           b"GET /plain HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            assert got.startswith(b"HTTP/1.1 200")
        (rec,) = seen
        assert rec.sock is rec.handler.connection
        assert not isinstance(rec.sock, session_cls)
        assert rec.session is None

    @pytest.mark.parametrize("headers", [
        "Upgrade: websocket\r\n",                               # no `Connection: Upgrade`
        "Connection: Upgrade\r\nUpgrade: h2c\r\n",              # an upgrade, but not a websocket
        "Connection: close\r\nUpgrade: websocket\r\n",
    ], ids=["no-connection-upgrade", "not-websocket", "connection-close"])
    def test_an_upgrade_look_alike_is_not_a_websocket_request(self, pki, session_cls, headers):
        seen = []
        with Serving(make_app(), pki.gateway).spy_handlers(
            lambda h, env: seen.append(SimpleNamespace(
                ws=h.is_websocket_request(), sock=env["werkzeug.socket"],
                conn=h.connection, session=h.session_socket))
        ) as s:
            raw_http(s.port, pki.ctx_pinned,
                     b"GET /plain HTTP/1.1\r\nHost: x\r\n" + headers.encode() + b"\r\n")
        (rec,) = seen
        assert rec.ws is False
        assert rec.sock is rec.conn and rec.session is None

    def test_each_websocket_request_gets_its_own_session_socket(self, pki):
        socks = []
        with Serving(make_app(), pki.gateway, ws_end_timeout=0.3).spy_handlers(
            lambda h, env: socks.append(env["werkzeug.socket"])
        ) as s:
            for _ in range(3):
                tls, ws = open_stream(s.port, pki.ctx_pinned)
                finish_session(tls, ws)
                assert s.wait_slots_free(10.0), s.slot_state()
                tls.close()
        assert len(socks) == 3 and len({id(x) for x in socks}) == 3


# ---------------------------------------------------------------------------
# run_wsgi ends the session
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestEndWiring:
    def test_the_constant_and_the_server_keyword(self, pki):
        assert workers.API_WS_END_TIMEOUT == 2.0
        default = workers._api_server_class()(LOOPBACK, 0, Flask("x"), pki.gateway)
        custom = workers._api_server_class()(LOOPBACK, 0, Flask("x"), pki.gateway,
                                             ws_end_timeout=0.25)
        try:
            assert default.ws_end_timeout == workers.API_WS_END_TIMEOUT
            assert custom.ws_end_timeout == 0.25
        finally:
            default.server_close()
            custom.server_close()

    def test_end_is_called_once_with_the_servers_timeout_and_the_connection_is_not_reused(
        self, pki, caplog
    ):
        record = []

        def hook(handler, environ):
            session = handler.session_socket
            real_end = session.end

            def end(timeout):
                result = real_end(timeout)
                record.append((timeout, result, handler))
                return result

            session.end = end

        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway, ws_end_timeout=0.7).spy_handlers(hook) as s:
                tls, ws = open_stream(s.port, pki.ctx_pinned)
                finish_session(tls, ws)
                assert s.wait_slots_free(10.0), s.slot_state()
                tls.close()
        (timeout, result, handler), = record
        assert timeout == 0.7
        assert result is True
        assert handler.close_connection is True
        assert not [r for r in caplog.records if "did not leave" in r.getMessage()]

    def test_a_reader_that_does_not_leave_logs_a_warning_and_the_slot_is_still_released(
        self, pki, caplog
    ):
        """`end()` is stubbed to report failure (after really succeeding), so the server treats
        the connection as handed over and leaves its descriptor to the session: the test closes
        it itself (nothing is left to the garbage collector)."""
        conns = []

        def hook(handler, environ):
            session = handler.session_socket
            real_end = session.end
            conns.append(handler.connection)

            def end(timeout):
                real_end(timeout)
                return False

            session.end = end

        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway, ws_end_timeout=0.2).spy_handlers(hook) as s:
                tls, ws = open_stream(s.port, pki.ctx_pinned)
                finish_session(tls, ws)
                assert s.wait_slots_free(10.0), s.slot_state()   # the slot is released anyway
                tls.close()
        for conn in conns:
            conn.close()
        warnings = [r for r in caplog.records
                    if r.name == WORKERS_LOGGER and r.levelno == logging.WARNING
                    and "did not leave" in r.getMessage()]
        assert len(warnings) == 1
        assert "websocket reader thread" in warnings[0].getMessage()
        assert warnings[0].getMessage().endswith("it will be closed when the reader returns")
        assert errors(caplog) == []

    def test_end_is_called_when_the_app_never_took_the_socket(self, pki):
        """A websocket-looking request to a non-websocket route: the session socket was made, so
        it is ended (the `finally`) all the same."""
        ended = []

        def hook(handler, environ):
            session = handler.session_socket
            real_end = session.end

            def end(timeout):
                ended.append(timeout)
                return real_end(timeout)

            session.end = end

        with Serving(make_app(), pki.gateway, ws_end_timeout=0.4).spy_handlers(hook) as s:
            raw_http(s.port, pki.ctx_pinned,
                     ("GET /plain HTTP/1.1\r\nHost: x\r\n" + WS_HEADERS + "\r\n").encode())
        assert ended == [0.4]

    @pytest.mark.parametrize("timeout", [0.3, 1.2])
    def test_ws_end_timeout_bounds_the_slot_of_a_silent_client(self, pki, timeout):
        """A plain flask-sock route (no `_end_websocket`) and a client that sends `done`, takes
        the ack and then neither reads nor closes: the reader thread sits in `recv`. The slot is
        released after the graceful wait plus the forced EOF, not held for the session."""
        with Serving(make_app(), pki.gateway, ws_end_timeout=timeout) as s:
            tls, ws = open_stream(s.port, pki.ctx_pinned)
            finish_session(tls, ws)
            started = time.monotonic()
            assert s.wait_slots_free(timeout + 8.0), s.slot_state()
            elapsed = time.monotonic() - started
            assert elapsed >= timeout * 0.7 - 0.1, f"slot freed after {elapsed:.2f}s: no graceful wait"
            assert elapsed < timeout + 5.0, f"slot held for {elapsed:.2f}s"
            # the server then closed the connection: the silent client sees the end
            tls.settimeout(IO_TIMEOUT)
            try:
                tail = tls.recv(4096)
            except (OSError, ssl.SSLError):
                tail = b""
            while tail:                                     # the close frame the route sent
                try:
                    tail = tls.recv(4096)
                except (OSError, ssl.SSLError):
                    break
            tls.close()

    def test_a_client_that_answers_the_close_frame_does_not_wait_for_the_timeout(self, pki):
        with Serving(make_app(), pki.gateway, ws_end_timeout=30.0) as s:
            tls, ws = open_stream(s.port, pki.ctx_pinned)
            finish_session(tls, ws)
            started = time.monotonic()
            ws.close()                                      # the close reply ends the reader
            assert s.wait_slots_free(10.0), s.slot_state()
            assert time.monotonic() - started < 8.0
            tls.close()


# ---------------------------------------------------------------------------
# api._end_websocket on a real session
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestEndWebsocketOverTheSessionSocket:
    def test_the_route_sees_the_session_socket_and_end_websocket_forces_eof_through_it(
        self, pki, session_cls, monkeypatch
    ):
        monkeypatch.setattr(api, "WS_READER_JOIN_TIMEOUT", 0.3)
        seen = SimpleNamespace(sock=None, forced=0, thread=None)
        app = Flask("end_ws")
        sock = Sock(app)

        @sock.route("/s")
        def stream(ws):
            seen.sock = ws.sock
            seen.thread = ws.thread
            real_force = ws.sock.force_eof

            def counting():
                seen.forced += 1
                real_force()

            ws.sock.force_eof = counting
            while True:
                data = ws.receive(timeout=10)
                if isinstance(data, str) and json.loads(data).get("done"):
                    break
            ws.send(json.dumps({"status": "ok"}))
            api._end_websocket(ws)
            seen.alive_after = ws.thread.is_alive()

        with Serving(app, pki.gateway, ws_end_timeout=5.0) as s:
            tls, ws = open_stream(s.port, pki.ctx_pinned, "/s")
            ws.send(json.dumps({"done": True}))
            assert json.loads(ws.recv()) == {"status": "ok"}
            started = time.monotonic()
            assert s.wait_slots_free(8.0), s.slot_state()
            # prompt: the 0.3 s join in _end_websocket, not the 5 s of the server's end()
            assert time.monotonic() - started < 4.0
            tls.close()
        assert isinstance(seen.sock, session_cls)
        assert seen.forced == 1
        assert seen.alive_after is False


# ---------------------------------------------------------------------------
# The regression test
# ---------------------------------------------------------------------------


class SslIoSpy:
    """Instruments the SERVER side `ssl.SSLSocket`: every read and write is registered per
    socket and thread, and a close (`_real_close`) or `shutdown` of a socket while ANOTHER
    thread is registered on it is a violation: that is the descriptor being freed (or the TLS
    object dropped) under a thread inside `SSL_read`/`SSL_write`, which is what let the next
    connection's bytes reach a stale reader."""

    IO = ("recv", "recv_into", "read", "send", "sendall", "write")
    TEARDOWN = ("_real_close", "shutdown")

    def __init__(self, monkeypatch):
        self.lock = threading.Lock()
        self.active = {}                    # id(sock) -> {tid: [thread name, [ops]]}
        self.violations = []
        self.reader_io = 0                  # I/O calls made by simple-websocket reader threads
        self.teardowns = {name: 0 for name in self.TEARDOWN}
        for name in self.IO:
            self._wrap_io(monkeypatch, name)
        for name in self.TEARDOWN:
            self._wrap_teardown(monkeypatch, name)

    def _wrap_io(self, monkeypatch, name):
        orig = getattr(ssl.SSLSocket, name)
        spy = self

        def wrapper(sslsock, *args, **kwargs):
            if not sslsock.server_side:
                return orig(sslsock, *args, **kwargs)
            key, tid = id(sslsock), threading.get_ident()
            tname = threading.current_thread().name
            with spy.lock:
                entry = spy.active.setdefault(key, {}).setdefault(tid, [tname, []])
                entry[1].append(name)
                if "simple_websocket" in tname:
                    spy.reader_io += 1
            try:
                return orig(sslsock, *args, **kwargs)
            finally:
                with spy.lock:
                    per_sock = spy.active[key]
                    ops = per_sock[tid][1]
                    ops.pop()
                    if not ops:
                        del per_sock[tid]
                    if not per_sock:
                        del spy.active[key]

        wrapper.__name__ = name
        monkeypatch.setattr(ssl.SSLSocket, name, wrapper)

    def _wrap_teardown(self, monkeypatch, name):
        orig = getattr(ssl.SSLSocket, name)
        spy = self

        def wrapper(sslsock, *args, **kwargs):
            if sslsock.server_side:
                me = threading.get_ident()
                with spy.lock:
                    spy.teardowns[name] += 1
                    others = {tname: list(ops) for tid, (tname, ops)
                              in spy.active.get(id(sslsock), {}).items() if tid != me}
                    if others:
                        spy.violations.append(
                            f"{name} of fd {sslsock.fileno()} while {others} were inside it")
            return orig(sslsock, *args, **kwargs)

        wrapper.__name__ = name
        monkeypatch.setattr(ssl.SSLSocket, name, wrapper)


@pytest.fixture
def ssl_io_spy(monkeypatch):
    return SslIoSpy(monkeypatch)


def run_back_to_back(s, ctx, ending, sessions):
    for _ in range(sessions):
        tls, ws = open_stream(s.port, ctx)
        finish_session(tls, ws)
        if ending == "silent":
            # neither reads the close frame nor closes: the server ends the session alone
            assert s.wait_slots_free(10.0), s.slot_state()
            tls.close()
        elif ending == "close":
            ws.close()
            tls.close()
        else:
            time.sleep(0.02)
            reset(tls)


@pytest.mark.integration
class TestFdNotClosedUnderIo:
    SESSIONS = 8

    @pytest.mark.parametrize("ending", ["silent", "close", "rst"])
    def test_no_connection_is_closed_while_another_thread_is_inside_it(
        self, pki, caplog, ssl_io_spy, ending
    ):
        """Websocket sessions back to back on one server. The server's `SSLSocket` is never
        closed (nor its TLS layer dropped) while the reader thread or the route's thread is
        inside a read or write on it -- so a descriptor number is never reused under a stale
        reader. `silent`: the client takes the ack and goes quiet, so the reader is certainly
        inside `recv` when the route returns (the descriptor was closed under it, every time,
        before the fix); `close` / `rst` are the realistic endings, where it is a race."""
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway, ws_end_timeout=0.3) as s:
                run_back_to_back(s, pki.ctx_pinned, ending, self.SESSIONS)
                assert s.wait_slots_free(10.0), s.slot_state()
        # the instrumentation really saw the sessions
        assert ssl_io_spy.reader_io >= self.SESSIONS, ssl_io_spy.reader_io
        assert ssl_io_spy.teardowns["_real_close"] >= self.SESSIONS
        assert ssl_io_spy.violations == [], "\n".join(ssl_io_spy.violations)
        assert [r for r in caplog.records if "did not leave" in r.getMessage()] == []
        assert errors(caplog) == [], [r.getMessage()[-200:] for r in errors(caplog)]
        # and no reader thread outlives its session
        assert eventually(lambda: not [t for t in threading.enumerate()
                                       if "simple_websocket" in t.name], 5.0, 0.05)

    def test_a_handshake_straight_after_a_session_never_fails(self, pki, ssl_io_spy):
        """The symptom: the connection that gets the freed descriptor number. Each new TLS
        handshake follows a session whose client has just gone silent."""
        with Serving(make_app(), pki.gateway, ws_end_timeout=0.2) as s:
            for _ in range(self.SESSIONS):
                tls, ws = open_stream(s.port, pki.ctx_pinned)
                finish_session(tls, ws)
                other = tls_connect(s.port, pki.ctx_pinned)     # raises if the handshake fails
                other.sendall(b"GET /plain HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                other.settimeout(IO_TIMEOUT)
                assert other.recv(64).startswith(b"HTTP/1.1 200")
                other.close()
                assert s.wait_slots_free(10.0), s.slot_state()
                tls.close()
        assert ssl_io_spy.violations == [], "\n".join(ssl_io_spy.violations)


# ---------------------------------------------------------------------------
# A failed end(): the session takes over the close
# ---------------------------------------------------------------------------


class ClosableTls(FakeTls):
    """`FakeTls` whose `close` is allowed: counts the calls (with the calling thread), can
    raise, and notes whether the session's state lock was free when it ran."""

    def __init__(self, *, close_exc=None, **kw):
        super().__init__(**kw)
        self.close_exc = close_exc
        self.closes = []
        self.session = None
        self.lock_free_at_close = None

    def close(self):
        self.closes.append(threading.current_thread().name)
        if self.session is not None:
            free = self.session._state.acquire(blocking=False)
            if free:
                self.session._state.release()
            self.lock_free_at_close = free
        if self.close_exc is not None:
            raise self.close_exc


def stuck_session(session_cls, tls):
    """A session whose `force_eof` does nothing, so a reader in `recv` stays there."""
    sess = session_cls(tls)
    tls.session = sess
    sess.force_eof = lambda: None
    return sess


@pytest.mark.unit
class TestSessionTakesOverTheClose:
    def test_a_successful_end_does_not_close_and_sets_nothing(self, session_cls):
        tls = ClosableTls()
        sess = session_cls(tls)
        assert sess.end(0.5) is True
        assert sess._close_when_idle is False
        assert tls.closes == []

    def test_a_failed_end_sets_close_when_idle_and_closes_nothing_yet(self, session_cls):
        gate = threading.Event()
        tls = ClosableTls(recv_gate=gate)
        sess = stuck_session(session_cls, tls)
        reader, box = in_thread(sess.recv, 10)
        try:
            assert tls.recv_entered.wait(IO_TIMEOUT)
            assert sess.end(0.1) is False
            assert sess._close_when_idle is True
            assert tls.closes == []                      # the reader is still inside
        finally:
            gate.set()
            reader.join(IO_TIMEOUT)

    def test_the_reader_leaving_closes_the_connection_exactly_once_on_its_own_thread(
        self, session_cls
    ):
        gate = threading.Event()
        tls = ClosableTls(recv_gate=gate)
        sess = stuck_session(session_cls, tls)
        reader, box = in_thread(sess.recv, 10)
        assert tls.recv_entered.wait(IO_TIMEOUT)
        assert sess.end(0.1) is False
        gate.set()
        reader.join(IO_TIMEOUT)
        assert not reader.is_alive() and box.exc is None and box.value == b"data"
        assert len(tls.closes) == 1
        assert tls.closes[0] == reader.name
        assert sess._close_when_idle is False
        assert tls.lock_free_at_close is True            # closed outside the state lock
        # nothing else can reach the connection or close it again
        assert sess.recv(10) == b""
        with pytest.raises(BrokenPipeError):
            sess.sendall(b"x")
        assert sess.end(0.1) is True
        assert len(tls.closes) == 1

    def test_with_two_calls_inside_only_the_last_to_leave_closes(self, session_cls):
        recv_gate = threading.Event()
        tls = ClosableTls(recv_gate=recv_gate, send_delay=0.0)
        sess = stuck_session(session_cls, tls)
        reader, _ = in_thread(sess.recv, 10)
        assert tls.recv_entered.wait(IO_TIMEOUT)

        send_gate = threading.Event()
        tls.send = lambda data, flags=0: (send_gate.wait(IO_TIMEOUT), len(data))[1]   # a stuck write too
        writer, _ = in_thread(sess.sendall, b"w")
        assert eventually(lambda: sess._busy == 2, 5.0, 0.005)
        assert sess.end(0.1) is False
        recv_gate.set()
        reader.join(IO_TIMEOUT)
        assert tls.closes == [], "closed while a write was still inside"
        send_gate.set()
        writer.join(IO_TIMEOUT)
        assert len(tls.closes) == 1 and tls.closes[0] == writer.name

    def test_an_oserror_from_the_close_is_ignored(self, session_cls):
        gate = threading.Event()
        tls = ClosableTls(recv_gate=gate, close_exc=OSError(errno.EBADF, "bad fd"))
        sess = stuck_session(session_cls, tls)
        reader, box = in_thread(sess.recv, 10)
        assert tls.recv_entered.wait(IO_TIMEOUT)
        assert sess.end(0.1) is False
        gate.set()
        reader.join(IO_TIMEOUT)
        assert box.exc is None and box.value == b"data"
        assert len(tls.closes) == 1

    def test_a_late_leaver_after_a_clean_end_never_closes(self, session_cls):
        """`end()` succeeded (the server closes): the session must not close it as well."""
        gate = threading.Event()
        tls = ClosableTls(recv_gate=gate)
        sess = session_cls(tls)
        reader, _ = in_thread(sess.recv, 10)
        assert tls.recv_entered.wait(IO_TIMEOUT)
        threading.Timer(0.1, gate.set).start()
        assert sess.end(5.0) is True
        reader.join(IO_TIMEOUT)
        assert tls.closes == []

    def test_a_real_socket_is_closed_by_the_last_leaver(self, session_cls):
        hold = threading.Event()
        tls, peer = SockTls.pair(hold)
        try:
            sess = session_cls(tls)
            sess.force_eof = lambda: None
            reader, _ = in_thread(sess.recv, 10)
            assert tls.entered.wait(IO_TIMEOUT)
            assert sess.end(0.1) is False
            assert tls.fileno() >= 0
            hold.set()
            reader.join(IO_TIMEOUT)
            assert tls.fileno() == -1
            peer.settimeout(IO_TIMEOUT)
            assert peer.recv(10) == b""                   # the peer sees the connection go
        finally:
            hold.set()
            tls.close()
            peer.close()


class _FakeRequest:
    def __init__(self):
        self.calls = []

    def shutdown(self, how):
        self.calls.append(("shutdown", how))

    def close(self):
        self.calls.append(("close",))


@pytest.mark.integration
class TestShutdownRequestAndHandedOver:
    @pytest.fixture
    def server(self, pki):
        server = workers._api_server_class()(LOOPBACK, 0, Flask("x"), pki.gateway)
        yield server
        server.server_close()

    def test_a_handed_over_connection_is_neither_shut_down_nor_closed(self, server):
        req = _FakeRequest()
        server.connection_state.handed_over = req
        server.shutdown_request(req)
        assert req.calls == []

    def test_any_other_connection_is_shut_down_and_closed_as_before(self, server):
        mine, other = _FakeRequest(), _FakeRequest()
        server.connection_state.handed_over = mine
        server.shutdown_request(other)
        assert other.calls == [("shutdown", socket.SHUT_WR), ("close",)]
        assert mine.calls == []

    def test_with_nothing_handed_over_it_closes(self, server):
        req = _FakeRequest()
        server.shutdown_request(req)                     # the attribute does not even exist yet
        assert req.calls == [("shutdown", socket.SHUT_WR), ("close",)]
        server.connection_state.handed_over = None
        req2 = _FakeRequest()
        server.shutdown_request(req2)
        assert req2.calls == [("shutdown", socket.SHUT_WR), ("close",)]

    def test_the_hand_over_is_per_thread(self, server):
        req = _FakeRequest()
        server.connection_state.handed_over = req
        thread, _ = in_thread(server.shutdown_request, req)    # another thread's local is unset
        thread.join(IO_TIMEOUT)
        assert req.calls == [("shutdown", socket.SHUT_WR), ("close",)]

    def test_handed_over_is_reset_before_and_after_each_request_on_one_thread(self, pki):
        """Three connections served one after the other by THIS thread (`process_request_thread`
        called directly): a stale value is cleared before the request, the second request hands
        its connection over (skipped close, descriptor left open) and the value is cleared
        again afterwards, so the third connection is closed normally."""
        seen = []
        conns = []

        def hook(handler, environ):
            state = s.server.connection_state
            seen.append(state.handed_over)
            conns.append(handler.connection)
            if len(seen) == 2:
                state.handed_over = handler.connection

        s = Serving(make_app(), pki.gateway).spy_handlers(hook)
        server = s.server
        sentinel = object()
        results = []

        def client():
            tls = tls_connect(s.port, pki.ctx_pinned)
            try:
                tls.sendall(b"GET /plain HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                tls.settimeout(IO_TIMEOUT)
                got = b""
                while True:
                    try:
                        data = tls.recv(4096)
                    except (OSError, ssl.SSLError):
                        break
                    if not data:
                        break
                    got += data
                results.append(got)
            finally:
                tls.close()

        try:
            server.connection_state.handed_over = sentinel       # stale, from "a previous request"
            for n in (1, 2, 3):
                thread = threading.Thread(target=client, daemon=True)
                thread.start()
                raw, addr = server.socket.accept()
                assert server._slots.acquire(blocking=False)
                server.process_request_thread(raw, addr)
                thread.join(IO_TIMEOUT)
                assert server.connection_state.handed_over is None, f"not reset after request {n}"
                assert results[-1].startswith(b"HTTP/1.1 200"), results
        finally:
            for conn in conns:
                conn.close()
            server.server_close()
        assert seen == [None, None, None]                        # incl. the stale sentinel, cleared
        assert conns[0].fileno() == -1 and conns[2].fileno() == -1   # normal close happened
        # (the second one was left open by the skip; the test closed it only in the `finally`)


@pytest.mark.integration
class TestStuckReaderEndToEnd:
    def test_the_descriptor_stays_open_until_the_reader_returns_then_closes_exactly_once(
        self, pki, caplog
    ):
        """A reader truly stuck in `SSL_read` (a silent client; `force_eof` stubbed to nothing):
        `end()` fails, the WARNING says the session will close the connection, the server's
        `shutdown_request` leaves the descriptor alone (no `SHUT_WR`, no close), the slot is
        released; when the reader finally returns (the client goes away) the connection is
        closed once, on the reader's thread."""
        box = SimpleNamespace(tls=None, closes=[], shutdowns=[], end=[])

        def hook(handler, environ):
            session = handler.session_socket
            session.force_eof = lambda: None
            real_end = session.end

            def end(timeout):
                result = real_end(timeout)
                box.end.append(result)
                return result

            session.end = end
            tls = handler.connection
            box.tls = tls
            real_close, real_shutdown = tls.close, tls.shutdown
            tls.close = lambda: (box.closes.append(threading.current_thread().name), real_close())[1]
            tls.shutdown = lambda how: (box.shutdowns.append(how), real_shutdown(how))[1]

        client_tls = None
        with caplog.at_level(logging.DEBUG):
            try:
                with Serving(make_app(), pki.gateway, ws_end_timeout=0.2).spy_handlers(hook) as s:
                    client_tls, ws = open_stream(s.port, pki.ctx_pinned)
                    finish_session(client_tls, ws)
                    assert eventually(lambda: any(
                        "it will be closed when the reader returns" in r.getMessage()
                        for r in caplog.records), 10.0), "no hand-over warning"
                    assert box.end == [False]
                    assert s.wait_slots_free(5.0), s.slot_state()
                    time.sleep(0.4)                         # the server has finished with it
                    assert box.tls.fileno() >= 0, "the descriptor was closed under the reader"
                    assert box.closes == [] and box.shutdowns == []
                    assert [t for t in threading.enumerate() if "simple_websocket" in t.name]
                    reset(client_tls)                       # the reader's recv now fails
                    client_tls = None
                    assert eventually(lambda: box.tls.fileno() == -1, 10.0), "never closed"
                    assert len(box.closes) == 1
                    assert "simple_websocket" in box.closes[0]
                    assert box.shutdowns == []
                    assert eventually(lambda: not [t for t in threading.enumerate()
                                                   if "simple_websocket" in t.name], 5.0)
            finally:
                if client_tls is not None:
                    reset(client_tls)
        warnings = [r for r in caplog.records
                    if r.levelno == logging.WARNING and "did not leave" in r.getMessage()]
        assert len(warnings) == 1
        assert warnings[0].getMessage().endswith("it will be closed when the reader returns")
        assert errors(caplog) == []


# ---------------------------------------------------------------------------
# ApiServer(ws_end_timeout=...) validation
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestWsEndTimeoutValidation:
    BAD = [
        pytest.param(0, id="zero"),
        pytest.param(0.0, id="zero-float"),
        pytest.param(-1, id="negative"),
        pytest.param(-0.5, id="negative-float"),
        pytest.param(math.inf, id="inf"),
        pytest.param(-math.inf, id="minus-inf"),
        pytest.param(math.nan, id="nan"),
        pytest.param(None, id="none"),
        pytest.param(True, id="true"),
        pytest.param(False, id="false"),
        pytest.param("2", id="str"),
        pytest.param(b"2", id="bytes"),
        pytest.param([2], id="list"),
        pytest.param(2 + 0j, id="complex"),
        pytest.param(object(), id="object"),
    ]

    @pytest.mark.parametrize("value", BAD)
    def test_a_bad_value_raises_valueerror(self, pki, value):
        with pytest.raises(ValueError, match="ws_end_timeout"):
            workers._api_server_class()(LOOPBACK, 0, Flask("x"), pki.gateway, ws_end_timeout=value)

    @pytest.mark.parametrize("value", BAD)
    def test_the_check_runs_before_the_gateway_check(self, value):
        """A bad timeout is a ValueError even with a gateway that would be a TypeError."""
        with pytest.raises(ValueError, match="ws_end_timeout"):
            workers._api_server_class()(LOOPBACK, 0, Flask("x"), object(), ws_end_timeout=value)

    def test_a_good_timeout_with_a_bad_gateway_is_still_the_gateway_typeerror(self):
        with pytest.raises(TypeError):
            workers._api_server_class()(LOOPBACK, 0, Flask("x"), object(), ws_end_timeout=1.0)

    @pytest.mark.parametrize("value", [1, 0.001, 2.5, 3600, 10 ** 6])
    def test_a_positive_finite_number_is_accepted(self, pki, value):
        server = workers._api_server_class()(LOOPBACK, 0, Flask("x"), pki.gateway,
                                             ws_end_timeout=value)
        try:
            assert server.ws_end_timeout == value
        finally:
            server.server_close()

    def test_a_rejected_server_binds_no_port(self, pki):
        """Nothing was bound: the error is raised before the base class opens the socket."""
        listener = socket.socket()
        listener.bind((LOOPBACK, 0))
        port = listener.getsockname()[1]
        listener.close()
        with pytest.raises(ValueError):
            workers._api_server_class()(LOOPBACK, port, Flask("x"), pki.gateway, ws_end_timeout=0)
        again = socket.socket()
        try:
            again.bind((LOOPBACK, port))                 # still free
        finally:
            again.close()


# ---------------------------------------------------------------------------
# upgraded / discard of any status after the 101
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSessionUpgraded:
    def test_it_starts_false_and_a_read_does_not_set_it(self, session_cls):
        sess = session_cls(FakeTls())
        assert sess.upgraded is False
        sess.recv(10)
        assert sess.upgraded is False

    @pytest.mark.parametrize("write", ["sendall", "send"])
    def test_a_successful_write_sets_it(self, session_cls, write):
        sess = session_cls(FakeTls())
        getattr(sess, write)(b"HTTP/1.1 101 ...")
        assert sess.upgraded is True

    def test_a_failing_write_sets_it_too(self, session_cls):
        """Even a failed write may have put bytes of the 101 on the wire."""
        tls = FakeTls()

        def broken(data, flags=0):
            raise ssl.SSLError(1, "write failed")

        tls.send = broken
        sess = session_cls(tls)
        sess.force_eof = lambda: None                        # the fake is not a socket
        with pytest.raises(ssl.SSLError):
            sess.sendall(b"x")
        assert sess.upgraded is True

    def test_a_write_after_the_session_ended_does_not_set_it(self, session_cls):
        sess = session_cls(FakeTls())
        sess.end(0.1)
        with pytest.raises(BrokenPipeError):
            sess.sendall(b"x")
        assert sess.upgraded is False


def upgrade_request(path):
    return ("GET %s HTTP/1.1\r\nHost: x\r\n%s\r\n" % (path, WS_HEADERS)).encode()


def read_until_closed(tls, deadline=IO_TIMEOUT):
    tls.settimeout(deadline)
    got = b""
    while True:
        try:
            data = tls.recv(65536)
        except (OSError, ssl.SSLError):
            break
        if not data:
            break
        got += data
    return got


def make_failing_app():
    app = Flask("wssession_failing")
    sock = Sock(app)

    @sock.route("/boom")
    def boom(ws):
        ws.send("hello")
        raise RuntimeError("route failed after the 101")

    @sock.route("/gone")
    def gone(ws):
        from flask import abort
        abort(404)

    @sock.route("/bad")
    def bad(ws):
        from flask import abort
        abort(400)

    @app.route("/refuse")
    def refuse():
        return "not a websocket route"

    @sock.route("/guarded")
    def guarded(ws):
        ws.send("never reached")

    @app.before_request
    def deny():
        from flask import abort, request
        if request.path == "/guarded":
            abort(403)                      # a refusal BEFORE the route creates the session

    return app


@pytest.mark.integration
class TestAnyResponseAfterTheUpgradeIsDiscarded:
    @pytest.mark.parametrize("path, status", [("/boom", 500), ("/gone", 404), ("/bad", 400)])
    def test_no_http_bytes_follow_the_101(self, pki, caplog, path, status):
        seen = []
        app = make_failing_app()
        app.config["PROPAGATE_EXCEPTIONS"] = False
        with caplog.at_level(logging.DEBUG):
            with Serving(app, pki.gateway, ws_end_timeout=0.3).spy_handlers(
                lambda h, env: seen.append(h)
            ) as s:
                tls = tls_connect(s.port, pki.ctx_pinned)
                tls.sendall(upgrade_request(path))
                got = read_until_closed(tls)
                tls.close()
                assert s.wait_slots_free(10.0), s.slot_state()
        assert got.startswith(b"HTTP/1.1 101"), got[:100]
        head, _, rest = got.partition(b"\r\n\r\n")
        # after the 101 head: websocket frames only, no second HTTP response
        assert b"HTTP/" not in rest, rest[:200]
        assert b"Internal Server Error" not in rest and b"Not Found" not in rest
        assert b"Bad Request" not in rest
        assert b"Content-Type" not in rest
        (handler,) = seen
        assert handler.session_socket.upgraded is True
        assert handler.discard_response is True
        # the response is logged as usual (werkzeug's access log), just never sent
        assert any(f'" {status} ' in r.getMessage() for r in caplog.records
                   if r.name == "werkzeug"), [r.getMessage() for r in caplog.records]

    def test_the_route_that_raised_is_still_logged_by_flask(self, pki, caplog):
        app = make_failing_app()
        with caplog.at_level(logging.DEBUG):
            with Serving(app, pki.gateway, ws_end_timeout=0.3) as s:
                tls = tls_connect(s.port, pki.ctx_pinned)
                tls.sendall(upgrade_request("/boom"))
                read_until_closed(tls)
                tls.close()
        assert any("route failed after the 101" in (r.getMessage() + (r.exc_text or "")
                   + str(r.exc_info[1] if r.exc_info else ""))
                   for r in caplog.records)

    @pytest.mark.parametrize("path, status", [("/guarded", b"HTTP/1.1 403"),
                                              ("/refuse", b"HTTP/1.1 400"),
                                              ("/missing", b"HTTP/1.1 404")])
    def test_a_refusal_before_the_upgrade_is_still_written(self, pki, path, status):
        """Upgrade headers, but the request is refused before the route creates a session (a
        `before_request` 403, a route that is not a websocket one, no route at all): nothing was
        written to the stream, so the 4xx goes out as an ordinary response."""
        seen = []
        with Serving(make_failing_app(), pki.gateway, ws_end_timeout=0.3).spy_handlers(
            lambda h, env: seen.append(h)
        ) as s:
            tls = tls_connect(s.port, pki.ctx_pinned)
            tls.sendall(upgrade_request(path))
            got = read_until_closed(tls)
            tls.close()
        assert got.startswith(status), got[:120]
        (handler,) = seen
        assert handler.session_socket.upgraded is False
        assert handler.discard_response is False

    def test_a_plain_request_is_never_discarded(self, pki):
        with Serving(make_failing_app(), pki.gateway) as s:
            tls = tls_connect(s.port, pki.ctx_pinned)
            tls.sendall(b"GET /nothing HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            got = read_until_closed(tls)
            tls.close()
        assert got.startswith(b"HTTP/1.1 404")


# ---------------------------------------------------------------------------
# The simple-websocket pin
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSimpleWebsocketPin:
    def test_the_installed_version_satisfies_the_projects_pin(self):
        import importlib.metadata
        import pathlib
        import tomllib
        from packaging.specifiers import SpecifierSet

        pyproject = pathlib.Path(__file__).resolve().parent.parent / "pyproject.toml"
        if not pyproject.exists():
            pytest.skip("no pyproject.toml next to the tests")
        spec = tomllib.loads(pyproject.read_text())["tool"]["poetry"]["dependencies"].get(
            "simple-websocket")
        assert spec is not None, "pyproject.toml does not pin simple-websocket"
        installed = importlib.metadata.version("simple-websocket")
        assert installed in SpecifierSet(spec.replace(" ", "")), (installed, spec)


# ---------------------------------------------------------------------------
# Plan S-2: the write-stall bound (`_write_started`, `force_eof_if_write_stalled`)
# ---------------------------------------------------------------------------


class FailingTls(FakeTls):
    """A connection whose write fails (or blocks first, on `gate`)."""

    def __init__(self, *, exc=None, gate=None, **kw):
        super().__init__(**kw)
        self.exc = exc
        self.gate = gate
        self.entered = threading.Event()

    def send(self, data, flags=0):
        self.entered.set()
        if self.gate is not None:
            assert self.gate.wait(IO_TIMEOUT), "test bug: never released"
        if self.exc is not None:
            raise self.exc
        return super().send(data, flags)


def small_pair(session_cls):
    """A session over a real loopback socket with tiny buffers, and its peer."""
    tls, peer = SockTls.pair()
    tls.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
    peer.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    return session_cls(tls), tls, peer


@pytest.mark.unit
class TestWriteStallCheck:
    def test_the_check_is_offered_to_the_registry(self, session_cls):
        assert callable(session_cls.force_eof_if_write_stalled)
        sess = session_cls(FakeTls())
        assert sess._write_started is None

    def test_a_blocked_sendall_is_released_past_the_limit(self, session_cls):
        sess, tls, peer = small_pair(session_cls)
        try:
            writer, box = in_thread(sess.sendall, b"z" * (64 * 1024 * 1024))   # the peer never reads
            assert eventually(lambda: sess._write_started is not None, 5.0, 0.005)
            time.sleep(0.4)
            assert writer.is_alive(), "the write did not block"
            age = sess.force_eof_if_write_stalled(0.2)
            assert age is not None and age >= 0.4 - 0.05
            writer.join(IO_TIMEOUT)
            assert not writer.is_alive()
            assert isinstance(box.exc, OSError)                 # EPIPE / reset: the write failed
            assert sess._write_started is None and sess._busy == 0
        finally:
            tls.close()
            peer.close()

    def test_below_the_limit_nothing_is_cut_and_the_write_completes(self, session_cls):
        sess, tls, peer = small_pair(session_cls)
        payload = b"z" * (2 * 1024 * 1024)
        try:
            writer, box = in_thread(sess.sendall, payload)
            assert eventually(lambda: sess._write_started is not None, 5.0, 0.005)
            time.sleep(0.2)
            assert sess.force_eof_if_write_stalled(60.0) is None
            assert writer.is_alive(), "a check below the limit must not shut the connection"
            got = _read_exactly(peer, len(payload))             # the peer reads at last
            writer.join(IO_TIMEOUT)
            assert box.exc is None and len(got) == len(payload)
        finally:
            tls.close()
            peer.close()

    def test_idle_means_none_whatever_the_limit(self, session_cls):
        sess = session_cls(FakeTls())
        for limit in (-1.0, 0.0, 1.0):
            assert sess.force_eof_if_write_stalled(limit) is None
        sess.sendall(b"done")
        for limit in (-1.0, 0.0):
            assert sess.force_eof_if_write_stalled(limit) is None
        assert sess._write_started is None

    def test_the_mark_is_a_monotonic_time_set_when_the_write_starts(self, session_cls):
        gate = threading.Event()
        tls = FailingTls(gate=gate)
        sess = session_cls(tls)
        before = time.monotonic()
        writer, _ = in_thread(sess.sendall, b"x")
        try:
            assert tls.entered.wait(IO_TIMEOUT)
            mark = sess._write_started
            assert mark is not None and before <= mark <= time.monotonic()
        finally:
            gate.set()
            writer.join(IO_TIMEOUT)
        assert sess._write_started is None

    def test_a_write_that_finished_and_was_replaced_is_not_cut_by_a_stale_check(
        self, session_cls
    ):
        """The first write started long ago and is over; the second one is young. A check
        whose limit lies between the two ages must find only the young one."""
        gate = threading.Event()
        tls = FailingTls()
        sess = session_cls(tls)
        forced = []
        sess.force_eof = lambda: forced.append(True)
        sess.sendall(b"first")
        time.sleep(0.4)
        tls.gate = gate
        tls.entered.clear()
        writer, _ = in_thread(sess.sendall, b"second")
        try:
            assert tls.entered.wait(IO_TIMEOUT)
            assert sess.force_eof_if_write_stalled(0.3) is None     # 0.4 s ago would exceed it
            assert forced == []
        finally:
            gate.set()
            writer.join(IO_TIMEOUT)
        assert sess.force_eof_if_write_stalled(0.0) is None

    def test_a_cut_only_ever_happens_while_a_write_is_recorded(self, session_cls):
        """Check and shutdown are one step under `_state`: whenever `force_eof` runs, the
        write that justifies it is still in progress. A checker hammering a writer that starts
        and ends thousands of writes never sees the opposite."""
        tls = FakeTls()
        sess = session_cls(tls)
        violations, cuts = [], []

        def force():
            cuts.append(1)
            if sess._write_started is None or not sess._busy:
                violations.append(True)

        sess.force_eof = force
        stop = threading.Event()

        def write_loop():
            while not stop.is_set():
                sess.sendall(b"w")

        writer, _ = in_thread(write_loop)
        try:
            deadline = time.monotonic() + 1.5
            while time.monotonic() < deadline:
                sess.force_eof_if_write_stalled(0.0)
        finally:
            stop.set()
            writer.join(IO_TIMEOUT)
        assert cuts, "the checker never caught a write in progress: the test proves nothing"
        assert violations == []

    def test_the_mark_is_cleared_after_a_failed_write(self, session_cls):
        sess = session_cls(FailingTls(exc=OSError(errno.ECONNRESET, "reset")))
        sess.force_eof = lambda: None                        # the fake is not a socket
        with pytest.raises(OSError):
            sess.sendall(b"x")
        assert sess._write_started is None and sess._busy == 0
        assert sess.force_eof_if_write_stalled(0.0) is None

    def test_a_refused_write_after_end_leaves_no_mark(self, session_cls):
        sess = session_cls(FakeTls())
        sess.end(1.0)
        with pytest.raises(BrokenPipeError):
            sess.sendall(b"x")
        assert sess._write_started is None

    def test_recv_never_sets_the_mark(self, session_cls):
        gate = threading.Event()
        tls = FakeTls(recv_gate=gate)
        sess = session_cls(tls)
        reader, box = in_thread(sess.recv, 10)
        try:
            assert tls.recv_entered.wait(IO_TIMEOUT)
            assert sess._write_started is None
            assert sess.force_eof_if_write_stalled(-1.0) is None     # a silent reader is not a stall
        finally:
            gate.set()
            reader.join(IO_TIMEOUT)
        assert box.value == b"data" and sess._write_started is None

    def test_a_write_queued_behind_a_blocked_one_does_not_count(self, session_cls):
        """Only the write inside the connection is timed: a second writer waiting for the
        write lock has not started."""
        gate = threading.Event()
        tls = FailingTls(gate=gate)
        sess = session_cls(tls)
        first, _ = in_thread(sess.sendall, b"1")
        assert tls.entered.wait(IO_TIMEOUT)
        mark = sess._write_started
        second, _ = in_thread(sess.sendall, b"2")
        time.sleep(0.2)
        try:
            assert sess._write_started == mark
        finally:
            gate.set()
            first.join(IO_TIMEOUT)
            second.join(IO_TIMEOUT)
        assert sess._write_started is None

    def test_it_does_not_wait_for_the_write_lock(self, session_cls):
        """The checker must never queue behind the writer it is trying to release."""
        gate = threading.Event()
        tls = FailingTls(gate=gate)
        sess = session_cls(tls)
        writer, _ = in_thread(sess.sendall, b"1")
        assert tls.entered.wait(IO_TIMEOUT)
        forced = []
        sess.force_eof = lambda: forced.append(True)
        try:
            started = time.monotonic()
            age = sess.force_eof_if_write_stalled(-1.0)
            assert age is not None and time.monotonic() - started < 2.0
            assert forced == [True]
        finally:
            gate.set()
            writer.join(IO_TIMEOUT)

    def test_after_end_the_check_finds_nothing(self, session_cls):
        sess = session_cls(FakeTls())
        sess.end(1.0)
        assert sess.force_eof_if_write_stalled(-1.0) is None

    def test_it_never_raises_on_a_connection_already_closed(self, session_cls):
        gate = threading.Event()
        a, b = tcp_pair()
        sess = session_cls(a)
        tls_gate = FailingTls(gate=gate)
        sess._tls = tls_gate                                  # so the write can be held
        writer, _ = in_thread(sess.sendall, b"1")
        assert tls_gate.entered.wait(IO_TIMEOUT)
        sess._tls = a
        a.close()
        try:
            assert sess.force_eof_if_write_stalled(-1.0) is not None     # EBADF swallowed
        finally:
            gate.set()
            writer.join(IO_TIMEOUT)
            b.close()


# ---------------------------------------------------------------------------
# K-2: one thread inside OpenSSL at a time (`_tls_lock`, non-blocking `_io`)
# ---------------------------------------------------------------------------
#
# OpenSSL does not support a read and a write on one `SSL` object from two threads, and a
# websocket session has exactly that (the reader thread inside `recv` for the whole session,
# the route and the reader's own pongs writing). Ubuntu 24.04 / OpenSSL 3.0.13 showed spurious
# `SSLEOFError`, bad-record-MAC and heap corruption. `_SessionSocket` now runs every TLS call
# under one lock, never held while waiting: the descriptor is non-blocking and a call that
# needs the peer is retried after a `poll`.


def pattern(n, offset=0):
    """`n` bytes of a position-dependent pattern (period 251: a slice taken from the wrong
    offset differs)."""
    base = bytes(range(251))
    reps = (offset + n) // 251 + 2
    start = offset % 251
    return (base * reps)[start:start + n]


@pytest.fixture(scope="module")
def write_chunk(session_cls):
    chunk = inspect.getclosurevars(session_cls.sendall).nonlocals["_WS_WRITE_CHUNK"]
    assert chunk == 64 * 1024
    return chunk


@pytest.mark.unit
class TestNonBlockingIsLazy:
    def test_the_connection_is_blocking_until_the_first_tls_call(self, session_cls):
        tls, peer = SockTls.pair()
        try:
            sess = session_cls(tls)
            assert tls.gettimeout() is None
            sess.fileno()
            sess.close()
            sess.force_eof_if_write_stalled(0.0)
            assert tls.gettimeout() is None
            peer.sendall(b"x")
            assert sess.recv(10) == b"x"
            assert tls.gettimeout() == 0.0
        finally:
            tls.close()
            peer.close()

    def test_a_first_write_makes_it_non_blocking_too(self, session_cls):
        tls, peer = SockTls.pair()
        try:
            sess = session_cls(tls)
            assert tls.gettimeout() is None
            sess.sendall(b"hello")
            assert tls.gettimeout() == 0.0
            assert _read_exactly(peer, 5) == b"hello"
        finally:
            tls.close()
            peer.close()

    def test_it_is_set_once(self, session_cls):
        tls = FakeTls()
        sess = session_cls(tls)
        assert tls.timeouts_set == []
        for _ in range(3):
            sess.recv(10)
            sess.sendall(b"x")
        assert tls.timeouts_set == [0.0]

    def test_a_session_that_never_does_io_never_touches_the_mode(self, session_cls):
        tls = FakeTls(timeout=7.0)
        sess = session_cls(tls)
        assert sess.end(1.0) is True
        assert sess.recv(10) == b""
        with pytest.raises(BrokenPipeError):
            sess.sendall(b"x")
        assert tls.timeouts_set == [] and tls.timeout == 7.0 and tls.calls == []

    def test_the_timeout_of_the_connection_is_read_when_the_session_is_created(
        self, session_cls, script_tls
    ):
        tls = script_tls(timeout=0.3)
        sess = session_cls(tls)
        tls.settimeout(None)                         # later changes to the connection: ignored
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            sess.recv(10)
        assert 0.25 <= time.monotonic() - started < 3.0

    def test_a_plain_socket_without_pending_works(self, session_cls):
        tls, peer = SockTls.pair()                   # no `pending` attribute: a plain socket
        try:
            sess = session_cls(tls)
            reader, box = in_thread(sess.recv, 10)
            time.sleep(0.2)
            assert reader.is_alive()
            peer.sendall(b"late")
            reader.join(IO_TIMEOUT)
            assert box.value == b"late" and box.exc is None
        finally:
            tls.close()
            peer.close()


@pytest.mark.integration
class TestNonBlockingModeOverTheServer:
    def test_a_websocket_connection_is_non_blocking_and_a_refusal_before_it_is_not(self, pki):
        """A refused upgrade never reaches `_SessionSocket`: its 4xx is written to the
        still-blocking connection (it must not get `BlockingIOError`)."""
        seen = []
        with Serving(make_failing_app(), pki.gateway, ws_end_timeout=0.3).spy_handlers(
            lambda h, env: seen.append(h)
        ) as s:
            for path in ("/boom", "/guarded"):
                tls = tls_connect(s.port, pki.ctx_pinned)
                tls.sendall(upgrade_request(path))
                got = read_until_closed(tls)
                tls.close()
                assert got.startswith(b"HTTP/1.1 101" if path == "/boom" else b"HTTP/1.1 403")
                assert s.wait_slots_free(10.0), s.slot_state()
        upgraded, refused = seen[0], seen[1]
        assert upgraded.session_socket.upgraded is True
        assert upgraded.connection.gettimeout() == 0.0
        assert refused.session_socket.upgraded is False
        assert refused.connection.gettimeout() != 0.0


@pytest.mark.unit
class TestRetryProtocol:
    """A call that says it needs the peer is retried, under the lock again, after a wait that
    holds no lock."""

    @pytest.mark.parametrize("want", [WantRead, BlockingIOError])
    def test_a_read_that_wants_data_waits_outside_the_lock_then_retries(
        self, session_cls, script_tls, want
    ):
        tls = script_tls()
        tls.script("recv", want, b"hello")
        sess = bound(session_cls, tls)
        reader, box = in_thread(sess.recv, 100)
        assert eventually(lambda: tls.attempts("recv") == 1, 5.0, 0.005)
        time.sleep(0.15)
        assert reader.is_alive() and tls.attempts("recv") == 1, "retried without waiting"
        assert tls_lock_free(sess), "the lock is held while the reader waits"
        tls.peer.sendall(b"x")                              # the descriptor turns readable
        reader.join(IO_TIMEOUT)
        assert box.value == b"hello" and box.exc is None
        assert tls.attempts("recv") == 2
        assert tls.lock_checks == [("recv", True), ("recv", True)]
        assert tls.max_inside == 1

    def test_a_read_that_wants_to_write_retries_when_the_descriptor_is_writable(
        self, session_cls, script_tls
    ):
        """TLS 1.3 KeyUpdate answers, a session ticket: a read can need the other direction."""
        tls = script_tls()
        tls.script("recv", WantWrite, WantWrite, b"ok")
        sess = bound(session_cls, tls)
        assert sess.recv(10) == b"ok"
        assert tls.attempts("recv") == 3
        assert tls.lock_checks == [("recv", True)] * 3

    def test_a_read_that_wants_to_write_waits_for_writability(self, session_cls, script_tls):
        tls = script_tls()
        tls.fill()
        tls.script("recv", WantWrite, b"ok")
        sess = bound(session_cls, tls)
        reader, box = in_thread(sess.recv, 10)
        assert eventually(lambda: tls.attempts("recv") == 1, 5.0, 0.005)
        time.sleep(0.15)
        assert reader.is_alive() and tls.attempts("recv") == 1
        assert tls_lock_free(sess)
        tls.drain()
        reader.join(IO_TIMEOUT)
        assert box.value == b"ok" and tls.attempts("recv") == 2

    @pytest.mark.parametrize("want", [WantWrite, BlockingIOError])
    def test_a_write_that_wants_to_write_resends_the_same_slice(
        self, session_cls, script_tls, want
    ):
        tls = script_tls()
        tls.fill()
        data = pattern(1000)
        tls.script("send", want, want, lambda d: len(d))
        sess = bound(session_cls, tls)
        writer, box = in_thread(sess.sendall, data)
        assert eventually(lambda: tls.attempts("send") == 1, 5.0, 0.005)
        time.sleep(0.15)
        assert writer.is_alive() and tls.attempts("send") == 1, "retried without waiting"
        assert tls_lock_free(sess), "the lock is held while the writer waits"
        tls.drain()
        writer.join(IO_TIMEOUT)
        assert box.exc is None and not writer.is_alive()
        assert tls.payloads() == [data, data, data]
        assert tls.lock_checks == [("send", True)] * 3
        assert tls.max_inside == 1

    def test_a_write_that_wants_to_read_waits_for_data_then_resends(
        self, session_cls, script_tls
    ):
        tls = script_tls()
        data = pattern(500)
        tls.script("send", WantRead, lambda d: len(d))
        sess = bound(session_cls, tls)
        writer, box = in_thread(sess.sendall, data)
        assert eventually(lambda: tls.attempts("send") == 1, 5.0, 0.005)
        time.sleep(0.15)
        assert writer.is_alive() and tls.attempts("send") == 1
        assert tls_lock_free(sess)
        tls.peer.sendall(b"x")
        writer.join(IO_TIMEOUT)
        assert box.exc is None and tls.payloads() == [data, data]

    def test_a_retried_slice_is_the_identical_slice_at_every_offset(
        self, session_cls, script_tls, write_chunk
    ):
        data = pattern(2 * write_chunk + 777)
        tls = script_tls()
        tls.peer.sendall(b"x")                                  # readable: a want-read wakes at once
        tls.script("send",
                   WantWrite, lambda d: len(d),                 # slice 0: once refused
                   WantRead, WantWrite, lambda d: len(d),       # slice 1: twice
                   lambda d: len(d))                            # slice 2: at once
        sess = bound(session_cls, tls)
        sess.sendall(data)
        c = write_chunk
        assert tls.payloads() == [data[:c]] * 2 + [data[c:2 * c]] * 3 + [data[2 * c:]]

    def test_a_partial_send_continues_after_what_was_taken(
        self, session_cls, script_tls, write_chunk
    ):
        data = pattern(write_chunk + 5000)
        tls = script_tls()
        tls.script("send", lambda d: 1000,                      # took 1000 of the first slice
                   WantWrite,                                   # then the next must wait ...
                   lambda d: len(d), lambda d: len(d))
        sess = bound(session_cls, tls)
        sess.sendall(data)
        got = tls.payloads()
        assert got[0] == data[:write_chunk]
        assert got[1] == got[2] == data[1000:1000 + write_chunk]   # ... and is resent as it was
        assert got[3] == data[1000 + write_chunk:]

    def test_an_error_that_is_not_a_want_propagates_and_is_not_retried(
        self, session_cls, script_tls
    ):
        tls = script_tls()
        tls.script("recv", ssl.SSLError(1, "boom"))
        tls.script("send", OSError(errno.ECONNRESET, "reset"))
        sess = bound(session_cls, tls)
        with pytest.raises(ssl.SSLError):
            sess.recv(10)
        with pytest.raises(ConnectionResetError):
            sess.sendall(b"x")
        assert tls.attempts("recv") == 1 and tls.attempts("send") == 1
        assert sess._busy == 0 and sess._write_started is None

    def test_a_recv_waiting_for_the_peer_does_not_block_a_send_from_another_thread(
        self, session_cls, script_tls
    ):
        tls = script_tls()
        tls.script("recv", WantRead)
        sess = bound(session_cls, tls)
        reader, rbox = in_thread(sess.recv, 10)
        assert eventually(lambda: tls.attempts("recv") == 1, 5.0, 0.005)
        time.sleep(0.1)
        started = time.monotonic()
        bounded(sess.sendall, b"hello")
        assert time.monotonic() - started < 1.0
        assert _read_exactly_n(tls.peer, 5) == b"hello"
        assert reader.is_alive(), "the reader is still waiting for the peer"
        assert tls.max_inside == 1
        tls.peer.sendall(b"reply")
        reader.join(IO_TIMEOUT)
        assert rbox.value == b"reply"

    def test_a_send_waiting_for_the_peer_does_not_block_a_recv_from_another_thread(
        self, session_cls, script_tls
    ):
        tls = script_tls()
        tls.fill()
        sess = bound(session_cls, tls)
        writer, wbox = in_thread(sess.sendall, b"blocked" * 100)
        assert eventually(lambda: tls.attempts("send") >= 1, 5.0, 0.005)
        time.sleep(0.1)
        tls.peer.sendall(b"ping")
        started = time.monotonic()
        assert bounded(sess.recv, 10) == b"ping"
        assert time.monotonic() - started < 1.0
        assert writer.is_alive()
        tls.drain()
        writer.join(IO_TIMEOUT)
        assert wbox.exc is None

    def test_every_attempt_runs_under_the_lock_and_never_two_at_once(
        self, session_cls, script_tls
    ):
        tls = script_tls()
        tls.script("send", *([WantRead, lambda d: 100] * 20))
        tls.script("recv", *([WantRead] * 20))
        sess = bound(session_cls, tls)
        stop = threading.Event()

        def pump():                              # keep the descriptor readable
            while not stop.is_set():
                try:
                    tls.peer.send(b"x" * 64)
                except OSError:
                    return
                time.sleep(0.001)

        feeder = threading.Thread(target=pump, daemon=True)
        feeder.start()
        try:
            reader, rbox = in_thread(lambda: [sess.recv(10) for _ in range(5)])
            writer, wbox = in_thread(sess.sendall, pattern(2000))
            reader.join(IO_TIMEOUT)
            writer.join(IO_TIMEOUT)
        finally:
            stop.set()
            feeder.join(IO_TIMEOUT)
        assert rbox.exc is None and wbox.exc is None
        assert tls.max_inside == 1
        assert tls.lock_checks and all(held for _op, held in tls.lock_checks)


class _Interrupt(BaseException):
    """Not an `Exception`: the write guard must catch `BaseException`."""


@pytest.mark.unit
class TestAFailedWriteBreaksTheWrites:
    """If `sendall` raises part-way, OpenSSL may hold a half-written record, and a later write
    of different data would flush it and splice the stream: so no later write reaches OpenSSL
    (`BrokenPipeError`) and the connection is shut down."""

    EXCEPTIONS = [
        ssl.SSLError(1, "write failed"),
        ConnectionResetError(errno.ECONNRESET, "reset"),
        BrokenPipeError(errno.EPIPE, "pipe"),
        TimeoutError("The operation timed out"),
        _Interrupt(),
    ]

    @pytest.mark.parametrize("exc", EXCEPTIONS, ids=lambda e: type(e).__name__)
    def test_after_any_exception_later_writes_never_reach_the_connection(self, session_cls, exc):
        tls = FailingTls(exc=exc)
        sess = session_cls(tls)
        forced = []
        sess.force_eof = lambda: forced.append(True)
        with pytest.raises(type(exc)) as info:
            sess.sendall(b"first")
        assert info.value is exc                              # the original exception
        assert forced == [True]
        calls = list(tls.calls)
        tls.exc = None                                        # the connection "recovers"
        for write in ("sendall", "send"):
            with pytest.raises(BrokenPipeError) as again:
                getattr(sess, write)(b"second")
            assert again.value.errno == errno.EPIPE
        assert tls.calls == calls and tls.sent == []
        assert forced == [True], "the shutdown is done once, by the failing write"
        assert sess._write_broken is True
        assert sess._busy == 0 and sess._write_started is None
        assert sess.end(1.0) is True                          # nothing is left inside

    def test_a_failure_in_a_later_slice_breaks_it_too(self, session_cls, write_chunk):
        tls = FakeTls()
        real_send = tls.send
        count = []

        def flaky(data, flags=0):
            count.append(1)
            if len(count) == 2:
                raise OSError(errno.ECONNRESET, "reset")
            return real_send(data, flags)

        tls.send = flaky
        sess = session_cls(tls)
        sess.force_eof = lambda: None
        with pytest.raises(ConnectionResetError):
            sess.sendall(pattern(3 * write_chunk))
        assert len(tls.sent) == 1                             # the first slice went
        assert sess._write_broken is True
        with pytest.raises(BrokenPipeError) as again:
            sess.sendall(b"x")
        assert again.value.errno == errno.EPIPE
        assert len(count) == 2                                # never reached the connection again
        assert sess._busy == 0 and sess._write_started is None
        assert sess.end(1.0) is True

    @pytest.mark.parametrize("failure", [OSError(errno.EIO, "io"), RuntimeError("not a socket")],
                             ids=["oserror", "runtimeerror"])
    def test_a_force_eof_that_raises_never_replaces_the_writes_error(self, session_cls, failure):
        tls = FailingTls(exc=ConnectionResetError(errno.ECONNRESET, "reset"))
        sess = session_cls(tls)

        def boom():
            raise failure

        sess.force_eof = boom
        with pytest.raises(ConnectionResetError):
            sess.sendall(b"x")
        assert sess._write_broken is True and sess._busy == 0
        with pytest.raises(BrokenPipeError):
            sess.sendall(b"y")

    def test_a_connection_that_is_not_a_socket_still_reports_the_writes_own_error(
        self, session_cls
    ):
        """The real `force_eof` on a fake that cannot be shut down (`TypeError`)."""
        sess = session_cls(FailingTls(exc=ssl.SSLError(1, "write failed")))
        with pytest.raises(ssl.SSLError):
            sess.sendall(b"x")
        assert sess._write_broken is True

    @pytest.mark.parametrize("data", [object(), "text", 12, None, memoryview(b"abcdef")[::2]],
                             ids=["object", "str", "int", "none", "strided"])
    def test_a_bad_argument_does_not_break_the_session(self, session_cls, data):
        """Nothing reached OpenSSL, so nothing can be half written."""
        tls = FakeTls()
        sess = session_cls(tls)
        forced = []
        sess.force_eof = lambda: forced.append(True)
        with pytest.raises((TypeError, ValueError)):
            sess.sendall(data)
        assert sess._write_broken is False and forced == []
        assert sess._busy == 0 and sess._write_started is None
        sess.sendall(b"fine")
        assert tls.sent == [b"fine"]

    def test_a_refused_write_is_not_counted_as_a_write_in_progress(self, session_cls):
        tls = FailingTls(exc=OSError(errno.ECONNRESET, "reset"))
        sess = session_cls(tls)
        sess.force_eof = lambda: None
        with pytest.raises(OSError):
            sess.sendall(b"a")
        with pytest.raises(BrokenPipeError):
            sess.sendall(b"b")
        assert sess._write_started is None and sess._busy == 0
        assert sess.force_eof_if_write_stalled(-1.0) is None

    def test_reads_are_not_affected(self, session_cls):
        tls = FailingTls(exc=OSError(errno.ECONNRESET, "reset"))
        sess = session_cls(tls)
        sess.force_eof = lambda: None
        with pytest.raises(OSError):
            sess.sendall(b"a")
        assert sess.recv(10) == b"data"                       # the reader sees what is left

    def test_a_failed_read_does_not_break_the_writes(self, session_cls, script_tls):
        tls = script_tls()
        tls.script("recv", ssl.SSLError(1, "read failed"))
        sess = bound(session_cls, tls)
        with pytest.raises(ssl.SSLError):
            sess.recv(10)
        sess.sendall(b"still fine")
        assert _read_exactly(tls.peer, 10) == b"still fine"

    def test_a_timed_out_write_shuts_the_real_connection_down(self, session_cls, script_tls):
        tls = script_tls(timeout=0.3)
        tls.fill()
        sess = session_cls(tls)
        with pytest.raises(TimeoutError):
            sess.sendall(pattern(100000))
        attempts = tls.attempts("send")
        tls.peer.setblocking(False)
        # the peer sees the shutdown: after what was queued, EOF
        deadline = time.monotonic() + IO_TIMEOUT
        eof = False
        while time.monotonic() < deadline and not eof:
            try:
                eof = tls.peer.recv(65536) == b""
            except BlockingIOError:
                time.sleep(0.01)
        assert eof, "the connection was not shut down"
        with pytest.raises(BrokenPipeError):
            sess.sendall(b"later")
        assert tls.attempts("send") == attempts

    def test_a_cut_write_from_the_stall_check_breaks_the_writes(self, session_cls, script_tls):
        tls = script_tls()
        tls.fill()
        sess = session_cls(tls)
        writer, box = in_thread(sess.sendall, pattern(100000))
        assert eventually(lambda: sess._write_started is not None, 5.0, 0.005)
        time.sleep(0.2)
        assert sess.force_eof_if_write_stalled(0.1) is not None
        writer.join(IO_TIMEOUT)
        assert isinstance(box.exc, OSError)
        with pytest.raises(BrokenPipeError):
            sess.sendall(b"later")

    def test_a_successful_write_leaves_it_working(self, session_cls):
        tls = FakeTls()
        sess = session_cls(tls)
        for i in range(3):
            sess.sendall(b"w%d" % i)
        assert tls.sent == [b"w0", b"w1", b"w2"]


def _read_exactly_n(sock, n):
    return _read_exactly(sock, n)


@pytest.mark.unit
class TestPendingShortcut:
    """Plaintext OpenSSL already holds is not announced by the descriptor."""

    def test_buffered_plaintext_is_returned_without_waiting(self, session_cls, script_tls):
        tls = script_tls(timeout=2.0)
        tls.pending_result = True
        tls.script("recv", WantRead, b"buffered")
        sess = session_cls(tls)
        started = time.monotonic()
        assert sess.recv(100) == b"buffered"
        assert time.monotonic() - started < 1.0, "waited for the descriptor despite SSL_pending"
        assert tls.attempts("recv") == 2 and tls.pending_calls == 1

    def test_pending_runs_under_the_tls_lock(self, session_cls, script_tls):
        tls = script_tls(timeout=2.0)
        tls.pending_result = True
        tls.script("recv", WantRead, b"buffered")
        sess = bound(session_cls, tls)
        assert sess.recv(100) == b"buffered"
        assert tls.pending_checks == [True], "pending() must run under the TLS lock"

    def test_nothing_pending_means_wait_for_the_descriptor(self, session_cls, script_tls):
        tls = script_tls()
        tls.script("recv", WantRead, b"x")
        sess = bound(session_cls, tls)
        reader, box = in_thread(sess.recv, 10)
        assert eventually(lambda: tls.attempts("recv") == 1, 5.0, 0.005)
        time.sleep(0.2)
        assert reader.is_alive() and tls.attempts("recv") == 1, "spun without data"
        assert tls.pending_calls == 1
        tls.peer.sendall(b"y")
        reader.join(IO_TIMEOUT)
        assert box.value == b"x"

    def test_a_write_never_consults_it(self, session_cls, script_tls):
        """For a write, pending plaintext would not unblock it: the retry would spin."""
        tls = script_tls()
        tls.pending_result = True
        tls.script("send", WantRead, lambda d: len(d))
        sess = bound(session_cls, tls)
        writer, box = in_thread(sess.sendall, b"data")
        assert eventually(lambda: tls.attempts("send") == 1, 5.0, 0.005)
        time.sleep(0.3)
        assert writer.is_alive() and tls.attempts("send") == 1, "a write that wants data spun"
        assert tls.pending_calls == 0
        tls.peer.sendall(b"x")
        writer.join(IO_TIMEOUT)
        assert box.exc is None and tls.attempts("send") == 2

    def test_a_read_that_wants_to_write_never_consults_it(self, session_cls, script_tls):
        tls = script_tls()
        tls.fill()
        tls.pending_result = True
        tls.script("recv", WantWrite, b"ok")
        sess = bound(session_cls, tls)
        reader, box = in_thread(sess.recv, 10)
        assert eventually(lambda: tls.attempts("recv") == 1, 5.0, 0.005)
        time.sleep(0.3)
        assert reader.is_alive() and tls.attempts("recv") == 1, "a read that wants to write spun"
        assert tls.pending_calls == 0
        tls.drain()
        reader.join(IO_TIMEOUT)
        assert box.value == b"ok"


@pytest.mark.unit
class TestTimeouts:
    def test_a_silent_peer_times_out_a_recv(self, session_cls, script_tls):
        tls = script_tls(timeout=0.3)
        tls.script("recv", WantRead)
        sess = bound(session_cls, tls)
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            sess.recv(10)
        assert 0.25 <= time.monotonic() - started < 3.0
        assert sess._busy == 0
        assert tls_lock_free(sess)

    def test_a_plain_non_blocking_recv_times_out_too(self, session_cls):
        tls, peer = SockTls.pair()
        try:
            tls.settimeout(0.3)
            sess = session_cls(tls)
            started = time.monotonic()
            with pytest.raises(TimeoutError):
                sess.recv(10)
            assert 0.25 <= time.monotonic() - started < 3.0
        finally:
            tls.close()
            peer.close()

    def test_a_stalled_sendall_times_out(self, session_cls, script_tls):
        tls = script_tls(timeout=0.3)
        tls.fill()
        sess = bound(session_cls, tls)
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            sess.sendall(pattern(100000))
        assert 0.25 <= time.monotonic() - started < 3.0
        assert sess._busy == 0 and sess._write_started is None
        assert tls_lock_free(sess)

    def test_the_session_is_usable_after_a_timeout(self, session_cls, script_tls):
        tls = script_tls(timeout=0.3)
        sess = bound(session_cls, tls)
        with pytest.raises(TimeoutError):
            sess.recv(10)
        tls.peer.sendall(b"alive")
        assert sess.recv(10) == b"alive"
        sess.sendall(b"pong")
        assert _read_exactly(tls.peer, 4) == b"pong"

    def test_none_waits_as_long_as_it_takes(self, session_cls, script_tls):
        tls = script_tls(timeout=None)
        sess = bound(session_cls, tls)
        reader, box = in_thread(sess.recv, 10)
        time.sleep(0.8)
        assert reader.is_alive() and box.exc is None
        tls.peer.sendall(b"finally")
        reader.join(IO_TIMEOUT)
        assert box.value == b"finally" and box.exc is None

    def test_the_deadline_is_per_call_not_per_session(self, session_cls, script_tls):
        tls = script_tls(timeout=0.5)
        sess = bound(session_cls, tls)
        for _ in range(2):                                  # 2 x 0.3 s > 0.5 s
            timer = threading.Timer(0.3, tls.peer.sendall, args=(b"a",))
            timer.start()
            assert sess.recv(10) == b"a"
            timer.join()

    def test_the_deadline_is_per_sendall_not_per_slice(
        self, session_cls, script_tls, write_chunk
    ):
        """Slice 0 waits 0.7 s for the peer and then goes; slice 1 stalls. The whole call has
        1.0 s, so it times out ~1.0 s after it started, not 1.0 s after slice 1 did."""
        tls = script_tls(timeout=1.0)
        tls.fill()
        tls.script("send", WantWrite, lambda d: (tls.fill(), len(d))[1], WantWrite)
        sess = bound(session_cls, tls)
        timer = threading.Timer(0.7, tls.drain)
        timer.start()
        try:
            started = time.monotonic()
            with pytest.raises(TimeoutError):
                sess.sendall(pattern(write_chunk + 100))
            elapsed = time.monotonic() - started
        finally:
            timer.cancel()
        assert 0.9 <= elapsed < 1.5, elapsed

    def test_a_timeout_during_a_write_clears_the_stall_mark(self, session_cls, script_tls):
        tls = script_tls(timeout=0.2)
        tls.fill()
        sess = bound(session_cls, tls)
        with pytest.raises(TimeoutError):
            sess.sendall(b"x" * 10)
        assert sess._write_started is None
        assert sess.force_eof_if_write_stalled(0.0) is None


@pytest.mark.unit
class TestForcedEofWakesTheWait:
    def test_force_eof_wakes_a_reader_waiting_in_poll(self, session_cls, script_tls):
        tls = script_tls()
        tls.script("recv", WantRead)
        sess = bound(session_cls, tls)
        reader, box = in_thread(sess.recv, 10)
        assert eventually(lambda: tls.attempts("recv") == 1, 5.0, 0.005)
        time.sleep(0.1)
        assert reader.is_alive()
        sess.force_eof()
        reader.join(IO_TIMEOUT)
        assert not reader.is_alive()
        assert box.value == b"" and box.exc is None             # EOF

    def test_force_eof_wakes_a_writer_waiting_for_writability(self, session_cls, script_tls):
        tls = script_tls()
        tls.fill()
        tls.script("send", WantWrite)
        sess = bound(session_cls, tls)
        writer, box = in_thread(sess.sendall, pattern(100000))
        assert eventually(lambda: tls.attempts("send") == 1, 5.0, 0.005)
        time.sleep(0.1)
        assert writer.is_alive()
        sess.force_eof()
        writer.join(IO_TIMEOUT)
        assert not writer.is_alive()
        assert isinstance(box.exc, OSError)                     # EPIPE / reset

    def test_end_gets_a_waiting_reader_out_without_the_reader_holding_a_lock(
        self, session_cls, script_tls
    ):
        tls = script_tls()
        tls.script("recv", WantRead)
        sess = bound(session_cls, tls)
        reader, box = in_thread(sess.recv, 10)
        assert eventually(lambda: tls.attempts("recv") == 1, 5.0, 0.005)
        time.sleep(0.1)
        started = time.monotonic()
        assert sess.end(0.3) is True
        assert time.monotonic() - started < 5.0
        reader.join(IO_TIMEOUT)
        assert box.value == b"" and box.exc is None

    def test_the_stall_check_cuts_a_write_waiting_for_writability_while_the_reader_works(
        self, session_cls, script_tls
    ):
        tls = script_tls()
        tls.fill()
        sess = session_cls(tls)
        stop = threading.Event()
        reads = []

        def read_loop():
            while True:
                data = sess.recv(100)
                if not data:
                    return
                reads.append(len(data))

        def feed():
            while not stop.is_set():
                try:
                    tls.peer.send(b"r" * 10)
                except OSError:
                    return
                time.sleep(0.005)

        feeder = threading.Thread(target=feed, daemon=True)
        reader, rbox = in_thread(read_loop)
        feeder.start()
        writer, wbox = in_thread(sess.sendall, pattern(300000))
        try:
            assert eventually(lambda: sess._write_started is not None, 5.0, 0.005)
            time.sleep(0.4)
            assert writer.is_alive(), "the write did not stall"
            before = len(reads)
            assert before > 10, "the reader was not working while the write waited"
            age = sess.force_eof_if_write_stalled(0.2)
            assert age is not None and age >= 0.3
            writer.join(IO_TIMEOUT)
            assert not writer.is_alive() and isinstance(wbox.exc, OSError)
        finally:
            stop.set()
            sess.force_eof()
            feeder.join(IO_TIMEOUT)
            reader.join(IO_TIMEOUT)
        assert rbox.exc is None and not reader.is_alive()


class BusyTls(FakeTls):
    """`recv` returns at once, forever, and keeps no record (a reader looping over a busy
    stream)."""

    def recv(self, bufsize, flags=0):
        return b"d"


@pytest.mark.unit
class TestFairness:
    def test_a_reader_looping_on_a_ready_descriptor_does_not_starve_a_writer(self, session_cls):
        tls = BusyTls()
        sess = session_cls(tls)
        stop = threading.Event()

        def spin():
            while not stop.is_set():
                sess.recv(10)

        reader, rbox = in_thread(spin)
        try:
            time.sleep(0.05)
            worst = 0.0
            started = time.monotonic()
            for _ in range(60):
                t0 = time.monotonic()
                sess.sendall(b"w")
                worst = max(worst, time.monotonic() - t0)
            total = time.monotonic() - started
        finally:
            stop.set()
            reader.join(IO_TIMEOUT)
        assert rbox.exc is None
        assert len(tls.sent) == 60
        assert worst < 1.0, f"a write waited {worst:.2f}s behind a reader"
        assert total < 15.0, total

    def test_a_writer_looping_does_not_starve_a_reader(self, session_cls):
        tls = BusyTls()
        sess = session_cls(tls)
        stop = threading.Event()

        def spin():
            while not stop.is_set():
                sess.sendall(b"w")

        writer, wbox = in_thread(spin)
        try:
            time.sleep(0.05)
            worst = 0.0
            for _ in range(60):
                t0 = time.monotonic()
                sess.recv(10)
                worst = max(worst, time.monotonic() - t0)
        finally:
            stop.set()
            writer.join(IO_TIMEOUT)
        assert wbox.exc is None
        assert worst < 1.0, f"a read waited {worst:.2f}s behind a writer"

    def test_a_waiter_is_not_overtaken_by_the_thread_that_just_released_the_lock(
        self, session_cls
    ):
        """The turnstile itself: while B waits for the lock, A releasing and asking again
        queues behind B."""
        lock_cls = type(session_cls(FakeTls())._tls_lock)
        lock = lock_cls()
        order = []
        a_in = threading.Event()
        b_waiting = threading.Event()

        def a():
            with lock:
                a_in.set()
                assert b_waiting.wait(IO_TIMEOUT)
                time.sleep(0.2)                      # B is by now blocked inside `__enter__`
            with lock:                               # asks again at once
                order.append("a")

        def b():
            assert a_in.wait(IO_TIMEOUT)
            b_waiting.set()
            with lock:
                order.append("b")

        ta, _ = in_thread(a)
        tb, _ = in_thread(b)
        ta.join(IO_TIMEOUT)
        tb.join(IO_TIMEOUT)
        assert order == ["b", "a"]


@pytest.mark.unit
class TestPayloads:
    @pytest.mark.parametrize("size", [0, 1, 65535, 65536, 65537, 150000, 3 * 65536])
    def test_slices_are_at_most_the_chunk_and_arrive_in_order(
        self, session_cls, write_chunk, size
    ):
        data = pattern(size)
        tls = FakeTls()
        sess = session_cls(tls)
        sess.sendall(data, 9)
        assert b"".join(tls.sent) == data
        assert all(0 < len(piece) <= write_chunk for piece in tls.sent)
        assert len(tls.sent) == -(-size // write_chunk)
        assert all(call[2] == 9 for call in tls.calls)       # the flags go with every slice

    def test_an_empty_write_makes_no_call_but_still_marks_the_session_upgraded(self, session_cls):
        tls = FakeTls()
        sess = session_cls(tls)
        sess.sendall(b"")
        assert tls.calls == [] and sess.upgraded is True and sess._busy == 0

    @pytest.mark.parametrize("kind", ["bytes", "bytearray", "memoryview", "wide-array", "view-of-view"])
    def test_every_buffer_type_goes_out_byte_for_byte(self, session_cls, write_chunk, kind):
        raw = pattern(2 * write_chunk + 12)                   # divisible by 4
        if kind == "bytes":
            data = raw
        elif kind == "bytearray":
            data = bytearray(raw)
        elif kind == "memoryview":
            data = memoryview(raw)
        elif kind == "wide-array":
            data = memoryview(array("I", array("I", raw).tolist()))
            raw = bytes(data)
        else:
            data = memoryview(raw)[5:-7]
            raw = raw[5:-7]
        tls = FakeTls()
        sess = session_cls(tls)
        assert sess.send(data) == len(raw)
        assert b"".join(tls.sent) == raw
        assert all(len(piece) <= write_chunk for piece in tls.sent)

    def test_the_buffer_is_released_after_the_write(self, session_cls):
        data = bytearray(pattern(100000))
        sess = session_cls(FakeTls())
        sess.sendall(data)
        data.extend(b"more")                                  # BufferError if an export leaked
        mem = memoryview(pattern(70000))
        sess.sendall(mem)
        mem.release()

    def test_a_buffer_that_cannot_be_flattened_is_a_clean_error(self, session_cls):
        tls = FakeTls()
        sess = session_cls(tls)
        with pytest.raises((TypeError, ValueError)):
            sess.sendall(memoryview(b"abcdef")[::2])
        assert tls.calls == [] and sess._busy == 0 and sess._write_started is None

    def test_a_large_write_over_a_real_socket_arrives_whole(self, session_cls):
        tls, peer = SockTls.pair()
        try:
            sess = session_cls(tls)
            data = pattern(3 * 1024 * 1024 + 17)
            reader, box = in_thread(lambda: _read_exactly(peer, len(data)))
            sess.sendall(data)
            reader.join(IO_TIMEOUT * 3)
            assert box.value == data
        finally:
            tls.close()
            peer.close()

    def test_another_writer_gets_in_between_slices_of_a_large_write_only_after_it(
        self, session_cls, write_chunk
    ):
        """`_write_lock` still keeps one `sendall` whole: two big writes never interleave."""
        tls = FakeTls()
        sess = session_cls(tls)
        a, b = b"A" * (3 * write_chunk), b"B" * (3 * write_chunk)
        ta, _ = in_thread(sess.sendall, a)
        tb, _ = in_thread(sess.sendall, b)
        ta.join(IO_TIMEOUT)
        tb.join(IO_TIMEOUT)
        joined = b"".join(tls.sent)
        assert joined in (a + b, b + a)

    def test_a_write_after_end_does_not_touch_the_connection(self, session_cls, script_tls):
        tls = script_tls()
        sess = bound(session_cls, tls)
        assert sess.end(1.0) is True
        with pytest.raises(BrokenPipeError) as info:
            sess.sendall(pattern(200000))
        assert info.value.errno == errno.EPIPE
        with pytest.raises(BrokenPipeError):
            sess.send(b"x")
        assert sess.recv(10) == b""
        assert tls.log == [] and tls.gettimeout() is None

    def test_a_write_cut_by_end_leaves_the_session_consistent(self, session_cls, script_tls):
        tls = script_tls()
        tls.fill()
        sess = bound(session_cls, tls)
        writer, box = in_thread(sess.sendall, pattern(300000))
        assert eventually(lambda: tls.attempts("send") >= 1, 5.0, 0.005)
        time.sleep(0.1)
        assert sess.end(0.2) is True                            # forces EOF, the writer leaves
        writer.join(IO_TIMEOUT)
        assert not writer.is_alive() and isinstance(box.exc, OSError)
        assert sess._busy == 0 and sess._write_started is None


# ---------------------------------------------------------------------------
# K-2: the real thing -- a reader thread and a writer thread over one TLS 1.3 connection
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def loopback_tls(tmp_path_factory):
    base = tmp_path_factory.mktemp("wssession_loopback")
    ident = server_identity(base / "srv.tls")
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    ctx.load_cert_chain(str(ident.cert_path), str(ident.key_path))
    return SimpleNamespace(server_ctx=ctx, client_ctx=insecure_client_context(), ident=ident)


@contextlib.contextmanager
def tls_loopback(loop, *, server_timeout=None, echo=True):
    """-> `(server, peer, peer_errors)`: a TLS 1.3 pair over 127.0.0.1. `server` is the
    server-side `SSLSocket` (what the API worker wraps in a `_SessionSocket`); `peer` is the
    client end, run by a single healthy thread that echoes whatever it reads (when `echo`)."""
    listener = socket.socket()
    listener.bind((LOOPBACK, 0))
    listener.listen(1)
    box = SimpleNamespace(client=None, exc=None)

    def connect():
        try:
            raw = socket.create_connection(listener.getsockname(), timeout=IO_TIMEOUT)
            box.client = loop.client_ctx.wrap_socket(raw, server_hostname=LOOPBACK)
            box.client.settimeout(None)
        except BaseException as exc:                # noqa: BLE001 -- handed to the test
            box.exc = exc

    connector = threading.Thread(target=connect, daemon=True)
    connector.start()
    raw, _ = listener.accept()
    listener.close()
    raw.settimeout(IO_TIMEOUT)
    server = loop.server_ctx.wrap_socket(raw, server_side=True)
    connector.join(IO_TIMEOUT)
    assert box.exc is None, box.exc
    server.settimeout(server_timeout)
    peer = box.client
    peer_errors = []
    echoer = None
    if echo:
        def echo_loop():
            try:
                while True:
                    data = peer.recv(65536)
                    if not data:
                        return
                    peer.sendall(data)
            except OSError as exc:
                peer_errors.append(exc)

        echoer = threading.Thread(target=echo_loop, daemon=True)
        echoer.start()
    try:
        yield server, peer, peer_errors
    finally:
        try:
            socket.socket.shutdown(server, socket.SHUT_RDWR)      # wakes the echo thread
        except OSError:
            pass
        if echoer is not None:
            echoer.join(IO_TIMEOUT)
            assert not echoer.is_alive()
        for sock in (peer, server):
            try:
                sock.close()
            except OSError:
                pass


_ALPHABET = b"abcdefghijklmnopqrstuvwxyz"
_END = b"\x00END\x00"


def _chunk(n, offset):
    return (_ALPHABET * (n // 26 + 2))[offset % 26:offset % 26 + n]


def run_reader_and_writer(sess, seconds, *, size_of=None, watchdog=60.0):
    """One reader thread and one writer thread on `sess` for `seconds`; the writer's last write
    is an end marker the (echoing) peer sends back. -> `(sent, received, errors)`."""
    sent, received, errors = bytearray(), bytearray(), []

    def sizes(i):
        return size_of(i) if size_of else (200000 if i % 50 == 49 else 131)

    def reader():
        try:
            while True:
                data = sess.recv(65536)
                if not data:
                    errors.append(AssertionError("unexpected EOF"))
                    return
                received.extend(data)
                if received.endswith(_END):
                    return
        except BaseException as exc:                # noqa: BLE001
            errors.append(exc)

    def writer():
        try:
            end = time.monotonic() + seconds
            i = 0
            while time.monotonic() < end:
                chunk = _chunk(sizes(i), len(sent))
                sess.sendall(chunk)
                sent.extend(chunk)
                i += 1
            sess.sendall(_END)
            sent.extend(_END)
        except BaseException as exc:                # noqa: BLE001
            errors.append(exc)

    rt = threading.Thread(target=reader, name="race-reader", daemon=True)
    wt = threading.Thread(target=writer, name="race-writer", daemon=True)
    rt.start()
    wt.start()
    wt.join(watchdog)
    rt.join(watchdog)
    if rt.is_alive() or wt.is_alive():
        errors.append(AssertionError("a thread is stuck"))
        sess.force_eof()
        rt.join(10)
        wt.join(10)
    return sent, received, errors


@pytest.mark.integration
class TestRealTlsReaderAndWriter:
    """The regression test of K-2. Without `_tls_lock`, one TLS 1.3 connection with a reader
    thread in `recv` and a writer thread in `send`, against a healthy peer, fails on Linux /
    OpenSSL 3.0 within seconds (`SSLEOFError`, bad record MAC, heap corruption)."""

    SECONDS = 3.0

    @pytest.mark.parametrize("timeout", [None, 30.0], ids=["no-timeout", "timeout-30"])
    def test_a_reader_and_a_writer_on_one_connection_lose_nothing(
        self, session_cls, loopback_tls, timeout
    ):
        with tls_loopback(loopback_tls, server_timeout=timeout) as (server, _peer, peer_errors):
            sess = session_cls(server)
            sent, received, errors = run_reader_and_writer(sess, self.SECONDS)
            assert sess.end(5.0) is True
        assert errors == [], errors
        assert peer_errors == [], peer_errors
        assert len(sent) > 100000, f"the writer made no progress ({len(sent)} bytes)"
        assert bytes(received) == bytes(sent)

    def test_only_large_writes(self, session_cls, loopback_tls, write_chunk):
        """Every write spans several slices, so a reader gets a turn between them."""
        with tls_loopback(loopback_tls) as (server, _peer, peer_errors):
            sess = session_cls(server)
            sent, received, errors = run_reader_and_writer(
                sess, 2.0, size_of=lambda i: 3 * write_chunk + (i % 7) * 1000)
            assert sess.end(5.0) is True
        assert errors == [] and peer_errors == [], (errors, peer_errors)
        assert bytes(received) == bytes(sent)

    def test_a_reader_waiting_does_not_hold_up_a_write(self, session_cls, loopback_tls):
        """The peer is silent, the reader sits in `recv`; the route's writes still go out, and
        the reader still wakes up when the peer speaks."""
        with tls_loopback(loopback_tls, echo=False) as (server, peer, _errors):
            sess = session_cls(server)
            reader, rbox = in_thread(sess.recv, 10)
            time.sleep(0.2)
            assert reader.is_alive()
            payload = pattern(1_000_000)
            got = SimpleNamespace(value=None)

            def peer_reads():
                got.value = b"".join(iter_exact(peer, len(payload)))

            pr = threading.Thread(target=peer_reads, daemon=True)
            pr.start()
            started = time.monotonic()
            bounded(sess.sendall, payload, limit=30.0)
            pr.join(IO_TIMEOUT)
            assert time.monotonic() - started < 8.0
            assert got.value == payload
            assert reader.is_alive(), "the reader returned although the peer sent nothing"
            peer.sendall(b"hello")
            reader.join(IO_TIMEOUT)
            assert rbox.value == b"hello" and rbox.exc is None
            assert sess.end(5.0) is True

    def test_a_writer_waiting_for_a_silent_peer_does_not_hold_up_the_reader(
        self, session_cls, loopback_tls
    ):
        """The peer never reads: the write fills the socket buffers and waits. The reader still
        receives what the peer sends, at once."""
        with tls_loopback(loopback_tls, echo=False) as (server, peer, _errors):
            sess = session_cls(server)
            writer, wbox = in_thread(sess.sendall, pattern(64 * 1024 * 1024))
            assert eventually(lambda: sess._write_started is not None, 5.0, 0.005)
            time.sleep(0.5)
            assert writer.is_alive(), "the write did not block"
            peer.sendall(b"ping")
            started = time.monotonic()
            assert bounded(sess.recv, 100) == b"ping"
            assert time.monotonic() - started < 2.0
            assert writer.is_alive()
            age = sess.force_eof_if_write_stalled(0.2)
            assert age is not None
            writer.join(IO_TIMEOUT)
            assert not writer.is_alive() and isinstance(wbox.exc, OSError)
            assert sess.end(5.0) is True

    def test_an_end_while_the_reader_waits_leaves_nothing_inside(self, session_cls, loopback_tls):
        with tls_loopback(loopback_tls, echo=False) as (server, _peer, _errors):
            sess = session_cls(server)
            reader, rbox = in_thread(sess.recv, 10)
            time.sleep(0.2)
            assert sess.end(0.3) is True
            reader.join(IO_TIMEOUT)
            assert not reader.is_alive()
            # EOF; some OpenSSL builds (4.0 on macOS) report the shut-down descriptor as
            # EPIPE from a read, as the blocking `SSLSocket.recv` always did there
            assert rbox.value == b"" or isinstance(rbox.exc, (BrokenPipeError, ConnectionResetError))

    def test_the_peers_close_is_seen_as_eof_by_the_reader(self, session_cls, loopback_tls):
        with tls_loopback(loopback_tls, echo=False) as (server, peer, _errors):
            sess = session_cls(server)
            reader, rbox = in_thread(sess.recv, 10)
            time.sleep(0.2)
            peer.settimeout(0.3)
            try:
                peer.recv(10)                        # takes the session tickets: a clean FIN
            except (TimeoutError, ssl.SSLError):
                pass
            peer.close()
            reader.join(IO_TIMEOUT)
            assert not reader.is_alive() and rbox.exc is None and rbox.value == b""


def iter_exact(sock, n):
    got = 0
    sock.settimeout(IO_TIMEOUT)
    while got < n:
        data = sock.recv(min(65536, n - got))
        if not data:
            return
        got += len(data)
        yield data
    sock.settimeout(None)


def _openssl_with_keyupdate():
    exe = shutil.which("openssl")
    if exe is None:
        return None
    try:
        version = subprocess.run([exe, "version"], capture_output=True, text=True, timeout=30)
        helped = subprocess.run([exe, "s_server", "-help"], capture_output=True, text=True,
                                timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if not version.stdout.startswith("OpenSSL"):
        return None                                  # LibreSSL has no KeyUpdate command
    if "-tls1_3" not in helped.stdout + helped.stderr:
        return None
    return exe


KEYUPDATE_SENT = ">>> TLS 1.3, Handshake [length 0005], KeyUpdate"


class OpenSslServer:
    """`openssl s_server -tls1_3 -msg` on 127.0.0.1, driven through stdin. Without `-quiet`,
    a stdin line `k` makes it send a KeyUpdate and `K` one that asks the peer to answer
    (`-quiet` turns the commands off and sends `k` / `K` as data). Each command must reach it
    in a read of its own, so `command` / `line` write and flush one at a time. `-msg` prints
    every handshake message, so `captured` shows which KeyUpdates really went each way.
    Stdout is drained by a thread (`keep=False` discards it, for big writes)."""

    def __init__(self, exe, ident, *, keep=True):
        probe = socket.socket()
        probe.bind((LOOPBACK, 0))
        self.port = probe.getsockname()[1]
        probe.close()
        self.keep = keep
        self.captured = bytearray()
        self.process = subprocess.Popen(
            [exe, "s_server", "-accept", f"{LOOPBACK}:{self.port}",
             "-cert", str(ident.cert_path), "-key", str(ident.key_path), "-tls1_3", "-msg"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        )
        self._drainer = threading.Thread(target=self._drain, daemon=True)
        self._drainer.start()

    def _drain(self):
        try:
            while True:
                data = self.process.stdout.read1(65536)
                if not data:
                    return
                if self.keep:
                    self.captured.extend(data)
        except (OSError, ValueError):
            return

    def text(self):
        return bytes(self.captured).decode("latin-1")

    def connect(self, ctx):
        deadline = time.monotonic() + 20
        while True:
            try:
                raw = socket.create_connection((LOOPBACK, self.port), timeout=IO_TIMEOUT)
                return ctx.wrap_socket(raw, server_hostname=LOOPBACK)
            except OSError:
                if time.monotonic() > deadline or self.process.poll() is not None:
                    pytest.skip("openssl s_server did not come up")
                time.sleep(0.1)

    def write(self, data):
        self.process.stdin.write(data)
        self.process.stdin.flush()
        time.sleep(0.01)

    def close(self):
        self.process.kill()
        self.process.wait(20)
        for pipe in (self.process.stdin, self.process.stdout):
            try:
                pipe.close()
            except OSError:
                pass
        self._drainer.join(IO_TIMEOUT)


@contextlib.contextmanager
def openssl_keyupdates(loop, timeout, *, keep=True):
    """-> `(server, tls)`; skips unless this `s_server` really takes `k` as a command (the build
    on some hosts echoes it as data) -- the scenario is meaningless then."""
    exe = _openssl_with_keyupdate()
    if exe is None:
        pytest.skip("no `openssl s_server` with -tls1_3 key updates")
    server = OpenSslServer(exe, loop.ident, keep=True)
    tls = None
    try:
        tls = server.connect(loop.client_ctx)
        server.write(b"k\n")
        if not eventually(lambda: KEYUPDATE_SENT in server.text(), 5.0, 0.05):
            pytest.skip("this `openssl s_server` does not take `k` as a KeyUpdate command "
                        "(stdin is not wired as commands): nothing would be exercised")
        server.keep = keep
        tls.settimeout(timeout)
        yield server, tls
    finally:
        server.close()
        if tls is not None:
            tls.close()


@pytest.mark.integration
class TestRealTlsKeyUpdates:
    """`openssl s_server -tls1_3` rotates its keys (`k`: a KeyUpdate that does not ask for an
    answer) while the session reads and writes from two threads. Skipped where the host's
    `s_server` does not perform them.

    Deliberately NOT here: `K` (a KeyUpdate with update_requested) arriving while a
    non-blocking `SSL_write` is pending. That is a known OpenSSL limitation (`[SSL: BAD_LENGTH]`
    on 3.0; 4.0.3 handles it), so the outcome depends on the host's OpenSSL and the test would
    take seconds of 2 MiB writes. Its scenario is the standalone script
    `keyupdate_pending_write.py` kept outside the suite. The answer path itself (a read that
    has to write) is covered by the scripted `TestRetryProtocol` tests. Here the writes are
    100 bytes and throttled, so no write is ever pending; the test checks that.
    """

    SECONDS = 4.0

    @pytest.mark.parametrize("timeout", [None, 30.0], ids=["no-timeout", "timeout-30"])
    def test_key_updates_while_reading_and_writing(self, session_cls, loopback_tls, timeout):
        with openssl_keyupdates(loopback_tls, timeout) as (server, tls):
            wants = []
            real_send = tls.send

            def counting_send(data, flags=0):
                try:
                    return real_send(data, flags)
                except ssl.SSLWantWriteError:
                    wants.append(1)
                    raise

            tls.send = counting_send
            sess = session_cls(tls)
            fed, errors, stop = [], [], threading.Event()
            received, ending = bytearray(), threading.Event()

            def reader():
                try:
                    while True:
                        data = sess.recv(65536)
                        if not data:
                            if not ending.is_set():
                                errors.append(AssertionError("unexpected EOF"))
                            return
                        received.extend(data)
                except BaseException as exc:        # noqa: BLE001
                    if not ending.is_set():
                        errors.append(exc)

            def feeder():
                n = 0
                while not stop.is_set():
                    line = b"line %d\n" % n
                    try:
                        if n % 3 == 0:
                            server.write(b"k\n")          # never `K`: see the class docstring
                        server.write(line)
                    except (OSError, ValueError):
                        return
                    fed.append(line)
                    n += 1
                    time.sleep(0.01)

            rt = threading.Thread(target=reader, daemon=True)
            ft = threading.Thread(target=feeder, daemon=True)
            rt.start()
            ft.start()
            sent = 0
            end = time.monotonic() + self.SECONDS
            try:
                while time.monotonic() < end:
                    sess.sendall(b"~" * 100)
                    sent += 100
                    time.sleep(0.001)
            except BaseException as exc:            # noqa: BLE001
                errors.append(exc)
            stop.set()
            ft.join(IO_TIMEOUT)
            expected = b"".join(fed)
            assert eventually(lambda: len(received) >= len(expected), 15.0, 0.05), \
                (len(received), len(expected), errors)
            eventually(lambda: server.text().count("~") >= sent, 10.0, 0.05)
            assert errors == [], errors
            assert bytes(received) == expected
            assert server.text().count("~") == sent
            assert server.text().count(KEYUPDATE_SENT) >= 5, "too few KeyUpdates to mean anything"
            assert wants == [], "a write was pending: this scenario is not the one it claims to be"
            ending.set()
            assert sess.end(5.0) is True
            rt.join(IO_TIMEOUT)
            assert not rt.is_alive()

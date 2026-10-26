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

Hygiene: sockets are `127.0.0.1` port 0, every server is shut down in a `finally`, threads are
joined, and a network test finishes by waiting for all connection slots to come back.
"""

import ast
import errno
import inspect
import json
import logging
import math
import re
import selectors
import socket
import ssl
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


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeTls:
    """A stand-in for the `SSLSocket` (not a real socket: `force_eof` is stubbed out on the
    session under test). Records every call; `recv` can block until released; `sendall` can
    take time, and notices overlapping writers."""

    def __init__(self, *, recv_gate=None, send_delay=0.0, recv_data=b"data"):
        self.recv_gate = recv_gate
        self.send_delay = send_delay
        self.recv_data = recv_data
        self.recv_entered = threading.Event()
        self.recv_returned = False
        self.calls = []
        self.sent = []
        self._writers = 0
        self.max_writers = 0
        self._lock = threading.Lock()
        self.closed_called = False

    def recv(self, bufsize, flags=0):
        self.calls.append(("recv", bufsize, flags))
        self.recv_entered.set()
        if self.recv_gate is not None:
            assert self.recv_gate.wait(IO_TIMEOUT), "test bug: recv never released"
        self.recv_returned = True
        return self.recv_data

    def sendall(self, data, flags=0):
        self.calls.append(("sendall", bytes(data), flags))
        with self._lock:
            self._writers += 1
            self.max_writers = max(self.max_writers, self._writers)
        try:
            if self.send_delay:
                time.sleep(self.send_delay)
            self.sent.append(bytes(data))
        finally:
            with self._lock:
                self._writers -= 1

    def send(self, data, flags=0):           # the session must never call this one
        raise AssertionError("the session used send(), not sendall()")

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
        assert tls.calls == [("recv", 10, 3), ("sendall", b"abc", 7)]

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

    def test_a_read_is_not_blocked_by_a_write_in_progress(self, session_cls):
        """Reads are unlocked: the reader thread must be able to receive (a peer's close frame)
        while the route's thread is in the middle of a long write."""
        tls = FakeTls(send_delay=0.6)
        sess = session_cls(tls)
        writer, _ = in_thread(sess.sendall, b"slow")
        assert eventually(lambda: tls._writers == 1, 5.0, 0.005)
        started = time.monotonic()
        assert sess.recv(10) == b"data"
        assert time.monotonic() - started < 0.4
        writer.join(IO_TIMEOUT)

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
        tls = FakeTls()                                      # its send() raises: only sendall
        sess = session_cls(tls)
        n = sess.send(data, 5)
        assert n == memoryview(data).nbytes
        assert tls.sent == [bytes(data)]
        assert tls.calls == [("sendall", bytes(data), 5)]

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
        tls.sendall = lambda data, flags=0: send_gate.wait(IO_TIMEOUT)     # a stuck write too
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

        tls.sendall = broken
        sess = session_cls(tls)
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

    def sendall(self, data, flags=0):
        self.entered.set()
        if self.gate is not None:
            assert self.gate.wait(IO_TIMEOUT), "test bug: never released"
        if self.exc is not None:
            raise self.exc
        super().sendall(data, flags)


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

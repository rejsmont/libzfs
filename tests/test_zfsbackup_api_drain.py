"""The API server's handling of werkzeug's post-response drain of an unread request body.

Background. After the response, `werkzeug.serving.WSGIRequestHandler.run_wsgi` reads and
discards whatever the client still has queued (`rfile.read(10 MB)` in a loop) so it can show
the client a 413 rather than a reset. When the client had closed or reset the TLS connection
by then (every `/stream` websocket ends that way) the read raised `ssl.SSLError` and werkzeug
logged `Error on request:` with a traceback; for a websocket it also raced simple-websocket's
reader thread on the same `SSLSocket`. `workers._api_server_class()` now handles that in
`_DeadlineReader` / `ApiRequestHandler`:

* a websocket request's drain is an immediate EOF and never touches the socket;
* any other request, after the status line, turns a teardown error (`_is_teardown`) into a
  sticky quiet EOF plus exactly one DEBUG line and `close_connection`; any other `OSError` is
  still raised;
* the drain has its own ABSOLUTE deadline (`drain_timeout`, from the first drain read, or the
  request's own deadline when earlier), expiring the same quiet way;
* before the status line nothing changes.

Layout
------

* `TestDrainConstants` -- `API_DRAIN_TIMEOUT`, the `drain_timeout` keyword, the handler defaults.
* `TestDeadlineReaderAfterResponse` / `TestDeadlineReaderBeforeResponse` /
  `TestDrainDeadlineUnit` / `TestPerRequestReset` -- `_DeadlineReader` and the handler on a
  fake raw reader and a fake connection (no network).
* `TestDrainEof` / `TestDrainedBytesPerRequest` -- an EOF (a 0-byte read) after the drain has
  discarded some of the unread body is a teardown (one DEBUG line ending `(EOF)`, sticky,
  `close_connection`): CPython 3.11/3.12 with OpenSSL 3.0 report a client RST during the
  drain as a ragged EOF, not as an exception. An EOF with nothing drained stays a silent
  normal close. `handler.drained_bytes` is reset for every request.
* `TestDrainEofOnTheWire` -- the same on a real connection (an unread body, then a FIN with no
  `close_notify`).
* `TestWebsocketTeardown` -- a real `/stream` websocket (flask-sock) through `ApiServer`.
* `TestUnreadBodyTeardown` -- an unread POST body followed by a reset, pinned and anonymous.
* `TestDrainDeadlineOverTls` -- a peer that goes silent during the drain, and a response that
  streams longer than `drain_timeout`.
* `TestGenuineErrorsStillLogged` -- app errors are not swallowed.
* `TestIsWebsocketRequest` / `TestDiscardedWebsocketResponse` / `TestResponseWriter` -- the
  post-session `200` of a websocket is never written (`discard_response`, `_ResponseWriter`),
  4xx/5xx and non-websocket responses are; `is_websocket_request` follows werkzeug's rule.
* `TestEndWebsocket` / `TestBackupStreamEndsTheWebsocket` -- `api._end_websocket` (close, join
  the reader, forced shutdown of a silent peer, never raises) and its place in `backup_stream`
  (after `gate.end()`, on every exit path).

Hygiene: sockets are `127.0.0.1` port 0, every server is shut down in a `finally`, and every
network test finishes by waiting for all connection slots to come back (a leaked slot fails
it). Clients reset (SO_LINGER 0) only with nothing left to send.
"""

import errno
import http.client
import inspect
import json
import logging
import socket
import ssl
import struct
import threading
import time
from unittest.mock import MagicMock
from types import SimpleNamespace

import pytest
import websocket
from flask import Flask
from flask_sock import ConnectionClosed, Sock

from _tls_support import (
    IO_TIMEOUT,
    LOOPBACK,
    client_identity,
    insecure_client_context,
    make_gateway,
    trusted_pin,
    try_get,
)
from zfsbackup import api, workers

DRAIN_LOGGER = "zfsbackup.workers"
ENDED = "ended after the response"


# ---------------------------------------------------------------------------
# Rig
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def pki(tmp_path_factory):
    """-> a namespace: a gateway over a stub trust with client `alice` pinned, and `ctx_pinned`
    / `ctx_anon` client contexts."""
    base = tmp_path_factory.mktemp("drain_pki")
    gateway, _server, trust = make_gateway(base / "srv.tls")
    alice = client_identity(base / "alice.tls")
    trust.set_pins([trusted_pin(alice, "alice")], generation=2)
    gateway.refresh()
    return SimpleNamespace(
        gateway=gateway,
        alice=alice,
        ctx_pinned=insecure_client_context(alice),
        ctx_anon=insecure_client_context(),
    )


class Serving:
    """An `ApiServer` served from a thread. Shut down on exit, and asserts every slot is back
    (waits for the connection threads, which log before they release their slot)."""

    def __init__(self, app, gateway, **kwargs):
        self.server = workers._api_server_class()(LOOPBACK, 0, app, gateway, **kwargs)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.port = self.server.server_port
        #: raw reads (events) and handler instances, once `spy()` was called
        self.events = []
        self.handlers = []

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

    def spy(self):
        """Record every read the handlers make on their raw socket reader, with whether the
        response had started. Call before the first connection."""
        events, handlers = self.events, self.handlers
        base = self.server.RequestHandlerClass

        class SpyHandler(base):
            def setup(self):
                super().setup()
                handlers.append(self)
                reader = self.rfile.raw
                reader._raw = _SpyRaw(reader._raw, self, events)

        self.server.RequestHandlerClass = SpyHandler
        return self

    def reads_after_response(self):
        return [e for e in self.events if e[0]]


class _SpyRaw:
    """Wraps the handler's raw socket reader: appends `(response_started, nbytes or exc)`."""

    def __init__(self, raw, handler, events):
        self._raw, self._handler, self._events = raw, handler, events

    def readinto(self, buffer):
        started = self._handler.response_started
        try:
            n = self._raw.readinto(buffer)
        except BaseException as exc:
            self._events.append((started, exc))
            raise
        self._events.append((started, n))
        return n

    def __getattr__(self, name):
        return getattr(self._raw, name)


def eventually(predicate, timeout=10.0, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return bool(predicate())


def errors(caplog):
    return [r for r in caplog.records if r.levelno >= logging.ERROR]


def drain_errors(caplog):
    """ERROR records whose traceback is werkzeug's drain read (`self.rfile.read(...)`)."""
    return [r for r in errors(caplog) if "self.rfile.read" in r.getMessage()]


def ended_lines(caplog):
    return [r for r in caplog.records
            if r.name == DRAIN_LOGGER and r.levelno == logging.DEBUG and ENDED in r.getMessage()]


def make_app():
    app = Flask("drain_tests")
    sock = Sock(app)

    @sock.route("/backup/<cid>/<path:ds>/stream")
    def stream(ws, cid, ds):
        while True:
            data = ws.receive(timeout=10)
            if isinstance(data, str) and json.loads(data).get("done"):
                break
        ws.send(json.dumps({"status": "ok"}))

    @app.route("/ignore", methods=["POST"])
    def ignore():
        return "ok"                                   # never reads the body

    @app.route("/plain", methods=["GET", "POST"])
    def plain():
        return "hello"

    @app.route("/boom")
    def boom():
        raise RuntimeError("genuine app failure")

    @app.route("/slow", methods=["POST"])
    def slow():
        def body():
            for _ in range(6):
                time.sleep(0.2)
                yield b"tick"
        return app.response_class(body(), mimetype="text/plain")

    return app


def with_failing_iterator(inner):
    """WSGI wrapper: `/gen` answers 200 + one chunk, then the iterator raises."""
    def wsgi(environ, start_response):
        if environ.get("PATH_INFO") == "/gen":
            start_response("200 OK", [("Content-Type", "text/plain")])

            def body():
                yield b"first"
                raise RuntimeError("genuine failure mid-response")
            return body()
        return inner(environ, start_response)
    return wsgi


def reset(sock):
    """Abortive close: RST, no close_notify. Only with nothing left to send."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    sock.close()


def tls_connect(port, ctx):
    raw = socket.create_connection((LOOPBACK, port), timeout=IO_TIMEOUT)
    return ctx.wrap_socket(raw)


def run_stream(port, ctx, ending):
    """One real `/stream` websocket session, then end it: `rst` (reset after the status ack),
    `garbage` (a junk TLS record, then a reset), or `close` (what remote.py does)."""
    tls = tls_connect(port, ctx)
    ws = websocket.WebSocket()
    ws.connect(f"wss://{LOOPBACK}:{port}/backup/alice/pool/ds/stream?mode=fresh",
               socket=tls, timeout=IO_TIMEOUT)
    for _ in range(4):
        ws.send_binary(b"x" * 65536)
    ws.send(json.dumps({"done": True}))
    assert json.loads(ws.recv()) == {"status": "ok"}
    if ending == "rst":
        reset(tls)
    elif ending == "garbage":
        raw = socket.socket(fileno=tls.detach())
        try:
            raw.sendall(b"\x17\x03\x03\x00\x10" + b"Z" * 16)
        except OSError:
            pass                                      # the server was already done with us
        time.sleep(0.05)
        reset(raw)
    else:
        ws.close()


def post_unread(port, ctx, *, declared=50_000_000, chunks=2, end="rst", linger=0.3,
                path="/ignore", extra=b""):
    """POST whose body the app never reads: send `chunks` x 64 KiB of a `declared`-byte body,
    read the response head, wait `linger` (the server has then read it all and its drain is
    blocked: a reset with data still unsent may be dropped by the kernel), then reset (or
    close). -> the status line."""
    tls = tls_connect(port, ctx)
    tls.sendall(b"POST %s HTTP/1.1\r\nHost: x\r\n%sContent-Length: %d\r\n\r\n"
                % (path.encode(), extra, declared))
    chunk = b"y" * 65536
    try:
        for _ in range(chunks):
            tls.sendall(chunk)
    except OSError:
        pass
    got = b""
    tls.settimeout(IO_TIMEOUT)
    try:
        while b"\r\n\r\n" not in got:
            data = tls.recv(4096)
            if not data:
                break
            got += data
    except OSError:
        pass
    if linger:
        time.sleep(linger)
    if end == "rst":
        reset(tls)
    else:
        tls.close()
    return got.split(b"\r\n", 1)[0]


# ---------------------------------------------------------------------------
# The handler / reader on fakes
# ---------------------------------------------------------------------------


class FakeRaw:
    """Scripted raw reader: an item is bytes (returned) or an exception (raised); empty -> EOF."""

    def __init__(self, *script):
        self.script = list(script)
        self.calls = 0

    def readinto(self, buffer):
        self.calls += 1
        item = self.script.pop(0) if self.script else b""
        if isinstance(item, BaseException):
            raise item
        buffer[:len(item)] = item
        return len(item)

    def close(self):
        pass


class FakeConn:
    """Stands in for `handler.connection`: records `settimeout`, optionally raising."""

    def __init__(self, settimeout_exc=None):
        self.timeouts = []
        self.exc = settimeout_exc

    def settimeout(self, value):
        if self.exc is not None:
            raise self.exc
        self.timeouts.append(value)


@pytest.fixture(scope="module")
def handler_cls(pki):
    server = workers._api_server_class()(LOOPBACK, 0, make_app(), pki.gateway)
    try:
        return server.RequestHandlerClass
    finally:
        server.server_close()


@pytest.fixture
def make_handler(handler_cls):
    """-> `make(...)`: an `ApiRequestHandler` set up on a real socketpair (so `setup()` is the
    real one) whose raw reader is a `FakeRaw` and whose connection is a `FakeConn`."""
    opened = []

    def make(*script, drain_timeout=5.0, idle=None, upgrade=None, connection=None, settimeout_exc=None,
             response_started=True, read_deadline=None):
        a, b = socket.socketpair()
        opened.extend([a, b])
        h = handler_cls.__new__(handler_cls)
        h.request = a
        h.client_address = ("127.0.0.1", 4242)
        h.server = SimpleNamespace(drain_timeout=drain_timeout, request_idle_timeout=idle)
        handler_cls.setup(h)
        h.headers = http.client.HTTPMessage()
        if upgrade is not None:
            h.headers["Upgrade"] = upgrade
            h.headers["Connection"] = "Upgrade" if connection is None else connection
        elif connection is not None:
            h.headers["Connection"] = connection
        h.fake_raw = FakeRaw(*script)
        h.rfile.raw._raw = h.fake_raw
        h.connection = FakeConn(settimeout_exc)
        h.response_started = response_started
        h.read_deadline = read_deadline
        h.peer = b
        return h

    yield make
    for s in opened:
        s.close()


def read(handler):
    return handler.rfile.raw.readinto(bytearray(64))


#: every error class the reader must treat as the peer tearing the connection down
TEARDOWN_ERRORS = [
    pytest.param(lambda: ssl.SSLError(1, "[SSL] record layer failure"), id="SSLError"),
    pytest.param(lambda: ssl.SSLError(), id="bare-SSLError"),
    pytest.param(lambda: ssl.SSLEOFError(8, "EOF occurred in violation of protocol"), id="SSLEOFError"),
    pytest.param(lambda: ssl.SSLZeroReturnError(6, "closed"), id="SSLZeroReturnError"),
    pytest.param(lambda: ssl.SSLCertVerificationError(1, "verify"), id="SSLCertVerificationError"),
    pytest.param(lambda: ConnectionError("conn"), id="ConnectionError"),
    pytest.param(lambda: ConnectionResetError(errno.ECONNRESET, "reset"), id="ConnectionResetError"),
    pytest.param(lambda: BrokenPipeError(errno.EPIPE, "pipe"), id="BrokenPipeError"),
    pytest.param(lambda: ConnectionAbortedError(errno.ECONNABORTED, "abort"), id="ConnectionAbortedError"),
    pytest.param(lambda: TimeoutError("timed out"), id="TimeoutError"),
    pytest.param(lambda: socket.timeout("timed out"), id="socket.timeout"),
    pytest.param(lambda: OSError(errno.EBADF, "Bad file descriptor"), id="EBADF"),
    pytest.param(lambda: OSError(errno.ENOTCONN, "Socket is not connected"), id="ENOTCONN"),
]

#: errors that are NOT a teardown: a real failure, raised as before
GENUINE_ERRORS = [
    pytest.param(lambda: OSError(errno.EIO, "Input/output error"), id="EIO"),
    pytest.param(lambda: OSError(errno.EPERM, "Operation not permitted"), id="EPERM"),
    pytest.param(lambda: OSError("no errno at all"), id="OSError-no-errno"),
    pytest.param(lambda: ValueError("not an OSError"), id="ValueError"),
]


@pytest.mark.unit
class TestDrainConstants:
    def test_the_default_drain_timeout(self):
        assert workers.API_DRAIN_TIMEOUT == 5.0

    def test_the_server_keyword_defaults_to_the_constant_and_is_keyword_only(self):
        param = inspect.signature(workers._api_server_class().__init__).parameters["drain_timeout"]
        assert param.kind is inspect.Parameter.KEYWORD_ONLY
        assert param.default == workers.API_DRAIN_TIMEOUT

    def test_a_server_stores_the_default_and_an_explicit_value(self, pki):
        cls = workers._api_server_class()
        for kwargs, expected in (({}, 5.0), ({"drain_timeout": 0.25}, 0.25), ({"drain_timeout": 0}, 0)):
            server = cls(LOOPBACK, 0, make_app(), pki.gateway, **kwargs)
            try:
                assert server.drain_timeout == expected
            finally:
                server.server_close()

    def test_the_bound_api_server_uses_the_default(self, pki):
        server = workers._bind_api_server(make_app(), LOOPBACK, 0, pki.gateway)
        try:
            assert server.drain_timeout == workers.API_DRAIN_TIMEOUT
        finally:
            server.server_close()

    def test_handler_class_defaults(self, handler_cls):
        assert handler_cls.response_started is False
        assert handler_cls.drain_ended is False
        assert handler_cls.drain_deadline is None


@pytest.mark.unit
class TestDeadlineReaderAfterResponse:
    @pytest.mark.parametrize("make_exc", TEARDOWN_ERRORS)
    def test_a_teardown_is_a_sticky_quiet_eof_with_one_debug_line(self, make_handler, caplog, make_exc):
        exc = make_exc()
        h = make_handler(exc, b"never read", b"never read")
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            assert read(h) == 0
            assert h.drain_ended is True
            assert h.close_connection is True
            # sticky: the dead socket is not read again
            assert read(h) == 0
            assert read(h) == 0
        assert h.fake_raw.calls == 1
        lines = ended_lines(caplog)
        assert len(lines) == 1
        assert lines[0].getMessage() == (
            f"API: connection from ('127.0.0.1', 4242) ended after the response "
            f"({type(exc).__name__}: {exc})"
        )

    @pytest.mark.parametrize("make_exc", GENUINE_ERRORS)
    def test_a_non_teardown_error_is_still_raised(self, make_handler, caplog, make_exc):
        exc = make_exc()
        h = make_handler(exc)
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            with pytest.raises(type(exc)) as info:
                read(h)
        assert info.value is exc
        assert h.drain_ended is False
        assert getattr(h, "close_connection", None) is not True
        assert ended_lines(caplog) == []

    @pytest.mark.parametrize("errno_value", [errno.EBADF, errno.ENOTCONN])
    def test_settimeout_on_a_dead_socket_is_a_quiet_eof(self, make_handler, caplog, errno_value):
        h = make_handler(b"unused", settimeout_exc=OSError(errno_value, "dead"))
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            assert read(h) == 0
            assert read(h) == 0
        assert h.fake_raw.calls == 0                       # never reached the read
        assert h.drain_ended is True and h.close_connection is True
        assert len(ended_lines(caplog)) == 1

    def test_settimeout_failing_otherwise_is_raised(self, make_handler, caplog):
        h = make_handler(b"unused", settimeout_exc=OSError(errno.EIO, "io"))
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            with pytest.raises(OSError) as info:
                read(h)
        assert info.value.errno == errno.EIO
        assert h.drain_ended is False and ended_lines(caplog) == []

    def test_data_is_returned_untouched_until_the_peer_goes(self, make_handler, caplog):
        h = make_handler(b"abc", b"de", ConnectionResetError("rst"))
        buf = bytearray(16)
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            assert h.rfile.raw.readinto(buf) == 3 and bytes(buf[:3]) == b"abc"
            assert h.rfile.raw.readinto(buf) == 2
            assert ended_lines(caplog) == []
            assert h.rfile.raw.readinto(buf) == 0
        assert len(ended_lines(caplog)) == 1


@pytest.mark.unit
class TestDeadlineReaderWebsocket:
    @pytest.mark.parametrize("upgrade", ["websocket", "WebSocket", "WEBSOCKET"])
    def test_a_websocket_request_is_eof_and_the_socket_is_never_read(self, make_handler, caplog, upgrade):
        h = make_handler(b"would block", ConnectionResetError("x"), upgrade=upgrade)
        assert h.is_websocket_request() is True
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            assert read(h) == 0
            assert read(h) == 0
        assert h.fake_raw.calls == 0
        assert h.connection.timeouts == []                 # not even settimeout
        assert ended_lines(caplog) == []                   # a normal end, nothing to log
        assert h.drain_ended is False

    def test_a_non_websocket_upgrade_is_drained(self, make_handler):
        h = make_handler(b"xy", upgrade="h2c")
        assert h.is_websocket_request() is False
        assert read(h) == 2
        assert h.fake_raw.calls == 1

    def test_no_upgrade_header_is_not_a_websocket(self, make_handler):
        assert make_handler().is_websocket_request() is False

    def test_no_headers_at_all_is_not_a_websocket(self, make_handler):
        h = make_handler()
        del h.headers
        assert h.is_websocket_request() is False


@pytest.mark.unit
class TestDeadlineReaderBeforeResponse:
    @pytest.mark.parametrize("make_exc", TEARDOWN_ERRORS)
    def test_every_error_is_raised_unchanged(self, make_handler, caplog, make_exc):
        exc = make_exc()
        h = make_handler(exc, b"unused", response_started=False)
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            with pytest.raises(type(exc)) as info:
                read(h)
        assert info.value is exc
        assert h.drain_ended is False
        assert h.drain_deadline is None
        assert ended_lines(caplog) == []
        # not sticky: the next read goes to the raw reader again
        assert read(h) == len(b"unused")

    def test_an_expired_deadline_is_the_timeout_path_not_a_quiet_eof(self, make_handler, caplog):
        h = make_handler(b"unused", response_started=False,
                         read_deadline=time.monotonic() - 1.0)
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            with pytest.raises(TimeoutError, match="request not received in time"):
                read(h)
        assert h.fake_raw.calls == 0
        assert h.drain_ended is False and ended_lines(caplog) == []

    def test_the_header_deadline_sets_the_socket_timeout(self, make_handler):
        h = make_handler(b"abc", response_started=False, read_deadline=time.monotonic() + 3.0,
                         drain_timeout=0.1)
        assert read(h) == 3
        (timeout,) = h.connection.timeouts
        assert 2.0 < timeout <= 3.0                        # the request deadline, not drain_timeout

    def test_no_deadline_leaves_the_read_untouched(self, make_handler):
        h = make_handler(b"abc", response_started=False)
        assert read(h) == 3
        assert h.connection.timeouts == []

    def test_a_dead_socket_before_the_response_raises(self, make_handler):
        h = make_handler(b"abc", response_started=False, read_deadline=time.monotonic() + 3.0,
                         settimeout_exc=OSError(errno.EBADF, "dead"))
        with pytest.raises(OSError) as info:
            read(h)
        assert info.value.errno == errno.EBADF
        assert h.drain_ended is False


@pytest.mark.unit
class TestDrainDeadlineUnit:
    def test_it_starts_at_the_first_drain_read_and_is_not_extended(self, make_handler):
        h = make_handler(b"a", b"b", b"c", drain_timeout=5.0)
        assert h.drain_deadline is None
        before = time.monotonic()
        assert read(h) == 1
        first = h.drain_deadline
        assert before + 5.0 <= first <= time.monotonic() + 5.0
        time.sleep(0.05)
        assert read(h) == 1 and read(h) == 1
        assert h.drain_deadline == first                   # absolute: later reads do not push it
        assert h.connection.timeouts == sorted(h.connection.timeouts, reverse=True)
        assert all(t <= 5.0 for t in h.connection.timeouts)

    def test_an_expired_drain_deadline_is_a_quiet_eof_without_reading(self, make_handler, caplog):
        h = make_handler(b"unused")
        h.drain_deadline = time.monotonic() - 0.01
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            assert read(h) == 0
            assert read(h) == 0
        assert h.fake_raw.calls == 0
        assert h.drain_ended is True and h.close_connection is True
        (line,) = ended_lines(caplog)
        assert "TimeoutError: drain deadline passed" in line.getMessage()

    def test_the_earlier_of_the_drain_and_the_request_deadline_wins(self, make_handler):
        # request deadline sooner than the drain's
        h = make_handler(b"a", drain_timeout=30.0, read_deadline=time.monotonic() + 1.0)
        read(h)
        assert h.connection.timeouts[0] <= 1.0
        # drain deadline sooner than the request's
        h = make_handler(b"a", drain_timeout=1.0, read_deadline=time.monotonic() + 30.0)
        read(h)
        assert h.connection.timeouts[0] <= 1.0

    def test_an_expired_request_deadline_ends_the_drain_even_with_a_long_drain_timeout(
        self, make_handler, caplog
    ):
        h = make_handler(b"unused", drain_timeout=30.0, read_deadline=time.monotonic() - 0.5)
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            assert read(h) == 0
        assert h.fake_raw.calls == 0
        assert len(ended_lines(caplog)) == 1

    @pytest.mark.parametrize("drain_timeout", [0, None])
    def test_no_drain_timeout_means_no_drain_deadline(self, make_handler, drain_timeout):
        h = make_handler(b"a", b"b", drain_timeout=drain_timeout)
        assert read(h) == 1 and read(h) == 1
        assert h.drain_deadline is None
        assert h.connection.timeouts == []                 # the read is untouched

    def test_start_drain_deadline(self, make_handler):
        assert make_handler(drain_timeout=0).start_drain_deadline() is None
        assert make_handler(drain_timeout=None).start_drain_deadline() is None
        value = make_handler(drain_timeout=2.0).start_drain_deadline()
        assert time.monotonic() < value <= time.monotonic() + 2.0

    def test_the_deadline_applies_to_a_pinned_connection(self, make_handler):
        # a pinned connection has no request deadline once the app starts
        h = make_handler(b"a", drain_timeout=0.5, read_deadline=None)
        read(h)
        assert 0 < h.connection.timeouts[0] <= 0.5


@pytest.mark.unit
class TestPerRequestReset:
    def test_the_drain_state_is_reset_for_each_request(self, make_handler):
        h = make_handler(response_started=True)
        h.drain_ended = True
        h.drain_deadline = 12345.0
        h.read_deadline = 1.0
        h.peer.close()                    # the next request line reads EOF: no request, no response
        h.handle_one_request()
        assert h.response_started is False
        assert h.drain_ended is False
        assert h.drain_deadline is None
        assert h.close_connection is True

    def test_send_response_marks_the_response_started(self, make_handler):
        h = make_handler(response_started=False)
        h.request_version = "HTTP/1.1"
        h.protocol_version = "HTTP/1.1"
        h.requestline = "GET / HTTP/1.1"
        h.command = "GET"
        h.wfile = SimpleNamespace(write=lambda b: len(b), flush=lambda: None)
        h._headers_buffer = []
        assert h.response_started is False
        h.send_response(200)
        assert h.response_started is True
        # and this request's drain is the quiet kind
        h.fake_raw.script = [ConnectionResetError("x")]
        assert read(h) == 0 and h.drain_ended is True


# ---------------------------------------------------------------------------
# Real sockets
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestWebsocketTeardown:
    @pytest.mark.parametrize("ending", ["rst", "garbage", "close"])
    def test_a_stream_ended_by_the_client_logs_no_error_and_the_drain_never_reads(
        self, pki, caplog, ending
    ):
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway).spy() as s:
                for _ in range(5):
                    run_stream(s.port, pki.ctx_pinned, ending)
                assert s.wait_slots_free(10.0), s.slot_state()
                assert len(s.handlers) == 5
                # a real websocket request that reached the point of the drain
                assert all(h.is_websocket_request() for h in s.handlers)
                assert all(h.response_started for h in s.handlers)
                assert s.reads_after_response() == [], s.reads_after_response()
        assert drain_errors(caplog) == [], [r.getMessage() for r in drain_errors(caplog)]
        assert ended_lines(caplog) == []

    def test_a_reset_stream_logs_no_error_at_all(self, pki, caplog):
        """Not only the drain: before the discard of the post-session `200`, werkzeug wrote it
        onto the stream while simple-websocket's reader thread could still be inside `recv` on
        the same `SSLSocket`, and a reset made that WRITE raise a bare `ssl.SSLError` (about 1
        reset stream in 20 here): "Error on request". 100 resets, none may log an ERROR."""
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway) as s:
                for _ in range(100):
                    run_stream(s.port, pki.ctx_pinned, "rst")
                assert s.wait_slots_free(10.0), s.slot_state()
        assert errors(caplog) == [], f"{len(errors(caplog))} ERROR records"

    @pytest.mark.parametrize("ending", ["rst", "fin"])
    def test_the_client_going_while_the_route_is_still_finishing(self, pki, caplog, ending):
        """(Deterministically logged "Error on request" for `rst` before the discard.) The route is still busy after `done` (the real one waits for `zfs receive` to exit)
        when the client resets or closes: the reader thread has closed the `SSLSocket` before
        werkzeug would write flask-sock's `200`. No "Error on request" at all."""
        app = Flask("early_close")
        sock = Sock(app)

        @sock.route("/backup/<cid>/<path:ds>/stream")
        def stream(ws, cid, ds):
            while True:
                data = ws.receive(timeout=10)
                if data is None:
                    return
                if isinstance(data, str) and json.loads(data).get("done"):
                    break
            time.sleep(0.3)
            try:
                ws.send(json.dumps({"status": "ok"}))
            except ConnectionClosed:
                pass

        with caplog.at_level(logging.DEBUG):
            with Serving(app, pki.gateway).spy() as s:
                for _ in range(6):
                    tls = tls_connect(s.port, pki.ctx_pinned)
                    ws = websocket.WebSocket()
                    ws.connect(f"wss://{LOOPBACK}:{s.port}/backup/alice/pool/ds/stream",
                               socket=tls, timeout=IO_TIMEOUT)
                    ws.send_binary(b"x" * 1000)
                    ws.send(json.dumps({"done": True}))
                    time.sleep(0.05)                      # nothing left unsent
                    if ending == "rst":
                        reset(tls)
                    else:
                        tls.close()
                assert s.wait_slots_free(15.0), s.slot_state()
                assert s.reads_after_response() == []
        assert errors(caplog) == [], [r.getMessage()[-300:] for r in errors(caplog)]

    def test_a_session_longer_than_the_drain_timeout_is_unaffected(self, pki, caplog):
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway, drain_timeout=0.2).spy() as s:
                tls = tls_connect(s.port, pki.ctx_pinned)
                ws = websocket.WebSocket()
                ws.connect(f"wss://{LOOPBACK}:{s.port}/backup/alice/pool/ds/stream?mode=fresh",
                           socket=tls, timeout=IO_TIMEOUT)
                started = time.monotonic()
                for _ in range(8):
                    ws.send_binary(b"x" * 1024)
                    time.sleep(0.15)
                assert time.monotonic() - started > 1.0          # five times drain_timeout
                ws.send(json.dumps({"done": True}))
                assert json.loads(ws.recv()) == {"status": "ok"}
                ws.close()
                assert s.wait_slots_free(10.0), s.slot_state()
                assert s.reads_after_response() == []
        assert errors(caplog) == []
        assert ended_lines(caplog) == []


@pytest.mark.integration
class TestUnreadBodyTeardown:
    @pytest.mark.parametrize("who", ["pinned", "anonymous"])
    def test_an_unread_body_then_a_reset_is_one_debug_line_and_no_error(self, pki, caplog, who):
        ctx = pki.ctx_pinned if who == "pinned" else pki.ctx_anon
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway).spy() as s:
                statuses = {post_unread(s.port, ctx) for _ in range(3)}
                assert s.wait_slots_free(10.0), s.slot_state()
                assert statuses == {b"HTTP/1.1 200 OK"}
                assert s.reads_after_response(), "the drain never ran: this test proves nothing"
        assert errors(caplog) == [], [r.getMessage() for r in errors(caplog)]
        assert "Error on request" not in caplog.text
        assert len(ended_lines(caplog)) == 3               # exactly one per connection

    def test_a_clean_close_after_the_response_is_a_normal_end(self, pki, caplog):
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway).spy() as s:
                post_unread(s.port, pki.ctx_pinned, end="close")
                assert s.wait_slots_free(10.0), s.slot_state()
        assert errors(caplog) == []


@pytest.mark.integration
class TestDrainDeadlineOverTls:
    @staticmethod
    def _silent_peer(port, ctx, *, window):
        """POST with a long declared body of which ~100 KiB is sent; read the response; then go
        silent. -> (client socket, seconds the peer took to close within `window`, or None)."""
        tls = tls_connect(port, ctx)
        tls.sendall(b"POST /ignore HTTP/1.1\r\nHost: x\r\nContent-Length: 1000000\r\n\r\n")
        tls.sendall(b"y" * 100_000)
        got = b""
        tls.settimeout(IO_TIMEOUT)
        while b"\r\n\r\n" not in got:
            got += tls.recv(4096)
        assert got.startswith(b"HTTP/1.1 200")
        started = time.monotonic()
        tls.settimeout(window)
        try:
            while True:
                if tls.recv(4096) == b"":
                    return tls, time.monotonic() - started
        except socket.timeout:
            return tls, None
        except OSError:
            return tls, time.monotonic() - started

    def test_a_pinned_peer_that_goes_silent_is_closed_and_its_slot_freed(self, pki, caplog):
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway, drain_timeout=0.5) as s:
                tls, closed_after = self._silent_peer(s.port, pki.ctx_pinned, window=6.0)
                try:
                    assert closed_after is not None, "a silent pinned peer kept its connection"
                    assert 0.3 < closed_after < 4.0
                    assert s.wait_slots_free(5.0), s.slot_state()
                finally:
                    tls.close()
        assert errors(caplog) == []
        (line,) = ended_lines(caplog)
        assert "TimeoutError" in line.getMessage()

    def test_an_anonymous_peer_is_bound_by_the_earlier_request_deadline(self, pki, caplog):
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway, drain_timeout=30.0, request_idle_timeout=0.8) as s:
                tls, closed_after = self._silent_peer(s.port, pki.ctx_anon, window=8.0)
                try:
                    assert closed_after is not None and closed_after < 4.0
                    assert s.wait_slots_free(5.0), s.slot_state()
                finally:
                    tls.close()
        assert errors(caplog) == []
        assert len(ended_lines(caplog)) == 1

    def test_an_anonymous_peer_is_bound_by_the_earlier_drain_deadline(self, pki, caplog):
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway, drain_timeout=0.5, request_idle_timeout=30.0) as s:
                tls, closed_after = self._silent_peer(s.port, pki.ctx_anon, window=8.0)
                try:
                    assert closed_after is not None and closed_after < 4.0
                    assert s.wait_slots_free(5.0), s.slot_state()
                finally:
                    tls.close()
        assert len(ended_lines(caplog)) == 1

    def test_with_drain_timeout_zero_a_silent_pinned_peer_is_not_bounded(self, pki, caplog):
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway, drain_timeout=0) as s:
                tls, closed_after = self._silent_peer(s.port, pki.ctx_pinned, window=1.2)
                try:
                    assert closed_after is None
                    assert not s.slots_free()                  # still held
                    assert s.server._slots._value == s.server.max_connections - 1
                finally:
                    tls.close()                                # the peer goes: the slot comes back
                assert s.wait_slots_free(10.0), s.slot_state()
        assert errors(caplog) == []

    def test_a_response_longer_than_drain_timeout_still_gets_its_drain(self, pki, caplog):
        """The deadline starts at the first drain read, not at the request: this response takes
        ~1.2 s with `drain_timeout=0.5`, and the body the client sent is still drained after."""
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway, drain_timeout=0.5).spy() as s:
                tls = tls_connect(s.port, pki.ctx_pinned)
                tls.sendall(b"POST /slow HTTP/1.1\r\nHost: x\r\nContent-Length: 1000000\r\n\r\n")
                tls.sendall(b"y" * 200_000)
                tls.settimeout(IO_TIMEOUT)
                started = time.monotonic()
                got = b""
                while True:
                    data = tls.recv(4096)
                    if not data:
                        break
                    got += data
                    if got.endswith(b"0\r\n\r\n"):
                        break
                assert time.monotonic() - started > 0.9        # longer than drain_timeout
                assert b"tick" in got
                time.sleep(0.1)                                # under drain_timeout, then go quietly
                tls.close()
                assert s.wait_slots_free(10.0), s.slot_state()
                drained = sum(n for started_, n in s.reads_after_response() if isinstance(n, int))
        assert drained > 0, "the drain was cut off before it read anything"
        assert errors(caplog) == []
        assert not [r for r in ended_lines(caplog) if "TimeoutError" in r.getMessage()]


@pytest.mark.integration
class TestGenuineErrorsStillLogged:
    def test_a_flask_view_exception_is_logged_with_its_traceback(self, pki, caplog):
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway) as s:
                result = try_get(s.port, "/boom", pki.ctx_pinned)
                assert result[0] == 500
        flask_records = [r for r in caplog.records if r.exc_info and r.exc_info[0] is RuntimeError]
        assert flask_records, caplog.text
        assert "genuine app failure" in str(flask_records[0].exc_info[1])
        assert ended_lines(caplog) == []

    def test_a_wsgi_iterator_that_raises_after_its_first_chunk_is_error_on_request(self, pki, caplog):
        with caplog.at_level(logging.DEBUG):
            with Serving(with_failing_iterator(make_app()), pki.gateway) as s:
                try_get(s.port, "/gen", pki.ctx_pinned)        # IncompleteRead or similar
                assert s.wait_slots_free(10.0), s.slot_state()
        found = [r for r in errors(caplog) if "Error on request" in r.getMessage()]
        assert len(found) == 1
        assert "RuntimeError: genuine failure mid-response" in found[0].getMessage()

    def test_the_same_failure_is_logged_for_an_anonymous_connection(self, pki, caplog):
        with caplog.at_level(logging.DEBUG):
            with Serving(with_failing_iterator(make_app()), pki.gateway) as s:
                try_get(s.port, "/gen", pki.ctx_anon)
                assert s.wait_slots_free(10.0), s.slot_state()
        assert len([r for r in errors(caplog) if "Error on request" in r.getMessage()]) == 1


@pytest.mark.integration
class TestBeforeTheStatusLineNothingChanges:
    def test_the_header_deadline_still_gives_request_timed_out(self, pki, caplog):
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway, request_idle_timeout=0.4,
                         drain_timeout=30.0) as s:
                tls = tls_connect(s.port, pki.ctx_anon)
                tls.sendall(b"GET /x HTTP/1.1\r\nHost: x\r\nX-Partial: a")   # never finished
                tls.settimeout(IO_TIMEOUT)
                started = time.monotonic()
                try:
                    assert tls.recv(1) == b""
                except OSError:
                    pass
                finally:
                    tls.close()
                assert time.monotonic() - started < 4.0
                assert s.wait_slots_free(5.0), s.slot_state()
        assert ended_lines(caplog) == []                       # the drain path was never involved
        assert any("closing idle connection" in r.getMessage() for r in caplog.records)

    def test_a_pinned_body_read_inside_the_app_is_not_cut_off_at_drain_timeout(self, pki, caplog):
        """The app reads its whole body, which dribbles for ~1.5 s with `drain_timeout=0.3`."""
        def reading_app(environ, start_response):
            n = len(environ["wsgi.input"].read(int(environ["CONTENT_LENGTH"])))
            body = str(n).encode()
            start_response("200 OK", [("Content-Length", str(len(body)))])
            return [body]

        body = b"0123456789"
        with caplog.at_level(logging.DEBUG):
            with Serving(reading_app, pki.gateway, drain_timeout=0.3).spy() as s:
                tls = tls_connect(s.port, pki.ctx_pinned)
                tls.sendall(b"POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\n")
                for byte in body:
                    tls.sendall(bytes([byte]))
                    time.sleep(0.15)
                tls.settimeout(IO_TIMEOUT)
                got = b""
                while True:
                    data = tls.recv(4096)
                    if not data:
                        break
                    got += data
                tls.close()
                assert s.wait_slots_free(10.0), s.slot_state()
        assert got.startswith(b"HTTP/1.1 200") and got.endswith(b"10")
        assert errors(caplog) == []
        assert ended_lines(caplog) == []


# ===========================================================================
# The post-session response of a websocket is never written
# ===========================================================================

#: (Upgrade, Connection) -> is a websocket request, by werkzeug's own rule
WS_MATRIX = [
    pytest.param("websocket", "Upgrade", True, id="canonical"),
    pytest.param("WebSocket", "upgrade", True, id="case-insensitive"),
    pytest.param("websocket", "keep-alive, Upgrade", True, id="token-in-a-list"),
    pytest.param("websocket", " upgrade ,keep-alive", True, id="token-with-spaces"),
    pytest.param("websocket", None, False, id="Upgrade-only"),
    pytest.param("websocket", "keep-alive", False, id="no-upgrade-token"),
    pytest.param("websocket", "close", False, id="close"),
    pytest.param("websocket", "upgraded", False, id="not-a-token"),
    pytest.param("h2c", "Upgrade", False, id="other-protocol"),
    pytest.param("websocket, h2c", "Upgrade", False, id="not-exactly-websocket"),
    pytest.param(None, "Upgrade", False, id="Connection-only"),
    pytest.param(None, None, False, id="neither"),
]


def _set_headers(handler, upgrade, connection):
    handler.headers = http.client.HTTPMessage()
    if upgrade is not None:
        handler.headers["Upgrade"] = upgrade
    if connection is not None:
        handler.headers["Connection"] = connection


def _prepare_response(h):
    """The attributes `send_response` / `log_request` need on a bare handler."""
    h.request_version = h.protocol_version = "HTTP/1.1"
    h.requestline = "GET /x HTTP/1.1"
    h.command, h.path = "GET", "/x"


def _wire(h, window=0.3):
    """Whatever the handler has written to its socket so far."""
    h.wfile.flush()
    h.peer.settimeout(window)
    try:
        return h.peer.recv(65536)
    except socket.timeout:
        return b""


@pytest.mark.unit
class TestIsWebsocketRequest:
    @pytest.mark.parametrize("upgrade, connection, expected", WS_MATRIX)
    def test_werkzeugs_rule(self, make_handler, upgrade, connection, expected):
        h = make_handler()
        _set_headers(h, upgrade, connection)
        assert h.is_websocket_request() is expected

    def test_repeated_connection_header_lines_are_joined(self, make_handler):
        h = make_handler()
        h.headers = http.client.HTTPMessage()
        h.headers["Upgrade"] = "websocket"
        h.headers["Connection"] = "keep-alive"
        h.headers["Connection"] = "Upgrade"
        assert h.is_websocket_request() is True


@pytest.mark.unit
class TestDiscardedWebsocketResponse:
    def _respond(self, h, code, upgrade, connection):
        _set_headers(h, upgrade, connection)
        _prepare_response(h)
        h.send_response(code)
        h.send_header("Content-Length", "0")
        h.end_headers()

    def test_a_websocket_200_is_marked_and_nothing_is_sent(self, make_handler):
        h = make_handler(response_started=False)
        self._respond(h, 200, "websocket", "Upgrade")
        assert h.response_started is True
        assert h.discard_response is True
        assert _wire(h) == b""

    @pytest.mark.parametrize("code", [400, 401, 403, 404, 500, 503])
    def test_a_refused_upgrade_is_written(self, make_handler, code):
        h = make_handler(response_started=False)
        self._respond(h, code, "websocket", "Upgrade")
        assert h.discard_response is False
        assert _wire(h).startswith(b"HTTP/1.1 %d" % code)

    def test_a_non_websocket_200_is_written(self, make_handler):
        h = make_handler(response_started=False)
        self._respond(h, 200, None, None)
        assert h.discard_response is False
        assert _wire(h).startswith(b"HTTP/1.1 200")

    def test_upgrade_websocket_alone_gets_its_200(self, make_handler):
        h = make_handler(response_started=False)
        self._respond(h, 200, "websocket", None)
        assert h.discard_response is False
        assert _wire(h).startswith(b"HTTP/1.1 200")

    def test_discard_response_is_reset_for_each_request(self, make_handler):
        h = make_handler()
        h.discard_response = True
        h.peer.close()
        h.handle_one_request()
        assert h.discard_response is False

    def test_the_status_line_is_still_logged(self, make_handler, caplog):
        h = make_handler(response_started=False)
        # A 200 access line is DEBUG since C3 (1xx-3xx DEBUG, 4xx INFO, 5xx
        # WARNING), and the werkzeug logger sits at INFO unless
        # `logsetup.configure` pinned it, so lower it explicitly.
        with caplog.at_level(logging.DEBUG, logger='werkzeug'):
            self._respond(h, 200, "websocket", "Upgrade")
        assert any(" 200 " in r.getMessage() for r in caplog.records)

    def test_the_default(self, handler_cls):
        assert handler_cls.discard_response is False


class FakeInner:
    """A stand-in for the socket writer under `_ResponseWriter`."""

    def __init__(self, flush_exc=None):
        self.written, self.flushes, self.closes = [], 0, 0
        self.closed = False
        self.flush_exc = flush_exc

    def write(self, data):
        self.written.append(bytes(data))
        return len(data)

    def flush(self):
        self.flushes += 1
        if self.flush_exc is not None:
            raise self.flush_exc

    def close(self):
        self.closes += 1
        self.closed = True


@pytest.mark.unit
class TestResponseWriter:
    def test_it_passes_writes_and_flushes_through(self, make_handler):
        h = make_handler()
        inner = h.wfile._inner = FakeInner()
        assert h.wfile.write(b"abc") == 3
        h.wfile.flush()
        assert inner.written == [b"abc"] and inner.flushes == 1

    def test_while_discarding_writes_and_flushes_are_dropped_but_reported_written(self, make_handler):
        h = make_handler()
        inner = h.wfile._inner = FakeInner()
        h.discard_response = True
        assert h.wfile.write(b"abcd") == 4
        assert h.wfile.write(memoryview(b"xy")) == 2
        h.wfile.flush()
        assert inner.written == [] and inner.flushes == 0

    def test_close_flushes_then_closes_the_inner_writer(self, make_handler):
        h = make_handler()
        inner = h.wfile._inner = FakeInner()
        h.wfile.close()
        assert inner.flushes == 1 and inner.closes == 1 and h.wfile.closed

    def test_close_while_discarding_does_not_flush_but_still_closes(self, make_handler):
        h = make_handler()
        inner = h.wfile._inner = FakeInner()
        h.discard_response = True
        h.wfile.close()
        assert inner.flushes == 0 and inner.closes == 1

    def test_close_after_the_inner_writer_closed_does_not_flush_it(self, make_handler):
        h = make_handler()
        inner = h.wfile._inner = FakeInner()
        inner.closed = True
        h.wfile.close()
        assert inner.flushes == 0

    def test_a_failing_flush_still_closes_the_inner_writer(self, make_handler):
        h = make_handler()
        inner = h.wfile._inner = FakeInner(flush_exc=OSError(errno.EIO, "boom"))
        with pytest.raises(OSError):
            h.wfile.close()
        assert inner.closes == 1 and inner.closed

    def test_finish_reaches_rfile_close_and_the_fd_is_closed(self, make_handler):
        """`finish()` flushes (a failure is caught there), closes `wfile`, then `rfile`: the
        socket's last io-ref goes and closing it releases the descriptor."""
        h = make_handler()
        real = h.wfile._inner
        calls = {"n": 0}

        class FlakyFlush:
            closed = False

            def write(self, data):
                return real.write(data)

            def flush(self):
                calls["n"] += 1
                if calls["n"] == 1:
                    raise OSError(errno.EPIPE, "gone")      # finish() catches this one

            def close(self):
                self.closed = True
                real.close()

        h.wfile._inner = FlakyFlush()
        h.finish()
        assert h.wfile.closed and h.rfile.closed
        assert h.request._io_refs == 0
        h.request.close()
        assert h.request.fileno() == -1


# ---------------------------------------------------------------------------
# On the wire
# ---------------------------------------------------------------------------


def _upgrade_request(path, key=b"dGhlIHNhbXBsZSBub25jZQ==", extra=b"", connection=b"Upgrade"):
    return (b"GET %s HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: %s\r\n"
            b"Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n%s\r\n"
            % (path.encode(), connection, key, extra))


def _read_all(tls, window=IO_TIMEOUT):
    tls.settimeout(window)
    data = b""
    try:
        while True:
            chunk = tls.recv(65536)
            if not chunk:
                break
            data += chunk
    except OSError:
        pass
    return data


@pytest.mark.integration
class TestWebsocketResponseOnTheWire:
    def test_no_200_follows_the_101(self, pki, caplog):
        """The client closes the session the way remote.py does; everything the server sends
        after the handshake is websocket frames, never an `HTTP/1.1 200`."""
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway) as s:
                tls = tls_connect(s.port, pki.ctx_pinned)
                ws = websocket.WebSocket()
                ws.connect(f"wss://{LOOPBACK}:{s.port}/backup/alice/pool/ds/stream?mode=fresh",
                           socket=tls, timeout=IO_TIMEOUT)
                ws.send(json.dumps({"done": True}))
                assert json.loads(ws.recv()) == {"status": "ok"}
                ws.send_close()                 # the close frame, the socket stays ours
                tail = _read_all(tls)
                tls.close()
                assert s.wait_slots_free(10.0), s.slot_state()
        assert b"HTTP/1." not in tail, tail
        assert errors(caplog) == []

    def test_a_raw_upgrade_gets_a_101_and_then_no_200(self, pki, caplog):
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway) as s:
                tls = tls_connect(s.port, pki.ctx_pinned)
                tls.sendall(_upgrade_request("/backup/alice/pool/ds/stream?mode=fresh"))
                head = b""
                tls.settimeout(IO_TIMEOUT)
                while b"\r\n\r\n" not in head:
                    head += tls.recv(4096)
                assert head.startswith(b"HTTP/1.1 101")
                # a close frame (masked, code 1000): the session ends, the route returns
                tls.sendall(b"\x88\x82\x00\x00\x00\x00\x03\xe8")
                tail = head.split(b"\r\n\r\n", 1)[1] + _read_all(tls)
                tls.close()
                assert s.wait_slots_free(10.0), s.slot_state()
        assert b"HTTP/1." not in tail, tail
        assert errors(caplog) == []

    def test_a_refused_upgrade_is_still_written(self, pki):
        app = make_app()

        @app.before_request
        def refuse():
            from flask import request
            if request.path.endswith("/stream"):
                return ("denied", 401)

        with Serving(app, pki.gateway) as s:
            tls = tls_connect(s.port, pki.ctx_pinned)
            tls.sendall(_upgrade_request("/backup/alice/pool/ds/stream?mode=fresh"))
            data = _read_all(tls)
            tls.close()
        assert data.startswith(b"HTTP/1.1 401"), data[:80]
        assert data.endswith(b"denied")

    def test_upgrade_websocket_alone_is_an_ordinary_request(self, pki, caplog):
        """No `Connection: upgrade`: werkzeug routes it as a plain request, so it gets its
        `200` and the body."""
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway) as s:
                status, body = try_get(s.port, "/plain", pki.ctx_pinned,
                                       headers={"Upgrade": "websocket"})
        assert (status, body) == (200, b"hello")
        assert errors(caplog) == []

    @pytest.mark.parametrize("who", ["pinned", "anonymous"])
    def test_upgrade_websocket_alone_gets_the_normal_drain(self, pki, caplog, who):
        ctx = pki.ctx_pinned if who == "pinned" else pki.ctx_anon
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway).spy() as s:
                line = post_unread(s.port, ctx, path="/plain", extra=b"Upgrade: websocket\r\n")
                assert s.wait_slots_free(10.0), s.slot_state()
                assert line == b"HTTP/1.1 200 OK"
                assert s.reads_after_response(), "the body was not drained"
        assert errors(caplog) == []
        assert len(ended_lines(caplog)) == 1

    def test_both_headers_on_a_non_websocket_route_do_not_hang_the_slot(self, pki):
        with Serving(make_app(), pki.gateway) as s:
            tls = tls_connect(s.port, pki.ctx_pinned)
            tls.sendall(_upgrade_request("/plain"))
            data = _read_all(tls, 5.0)
            tls.close()
        assert data.startswith(b"HTTP/1.1 ")


# ===========================================================================
# api._end_websocket and backup_stream
# ===========================================================================


class FakeWs:
    def __init__(self, thread=None, sock=None, close_exc=None):
        self.thread, self.sock, self.close_exc = thread, sock, close_exc
        self.closes = 0

    def close(self, *a, **k):
        self.closes += 1
        if self.close_exc is not None:
            raise self.close_exc


def tcp_pair():
    listener = socket.socket()
    listener.bind((LOOPBACK, 0))
    listener.listen(1)
    a = socket.create_connection(listener.getsockname())
    b, _ = listener.accept()
    listener.close()
    return a, b


@pytest.mark.unit
class TestEndWebsocket:
    def test_constant(self):
        assert api.WS_READER_JOIN_TIMEOUT == 2.0

    def test_a_normal_client_returns_at_once_without_a_shutdown(self, monkeypatch, caplog):
        a, b = tcp_pair()
        shut = []
        monkeypatch.setattr(api.socket.socket, "shutdown", lambda self, how: shut.append(how))
        try:
            thread = threading.Thread(target=lambda: time.sleep(0.05))
            thread.start()
            ws = FakeWs(thread, a)
            started = time.monotonic()
            with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
                api._end_websocket(ws)
            assert time.monotonic() - started < 1.0
            assert ws.closes == 1 and not thread.is_alive() and shut == []
            assert not [r for r in caplog.records if "silent peer" in r.getMessage()]
        finally:
            a.close()
            b.close()

    def test_a_silent_peer_gets_the_forced_shutdown_and_the_reader_leaves(self, monkeypatch, caplog):
        monkeypatch.setattr(api, "WS_READER_JOIN_TIMEOUT", 0.3)
        a, b = tcp_pair()                    # b never sends and never closes
        try:
            thread = threading.Thread(target=lambda: a.recv(1), daemon=True)
            thread.start()
            time.sleep(0.1)
            ws = FakeWs(thread, a)
            started = time.monotonic()
            with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
                api._end_websocket(ws)
            elapsed = time.monotonic() - started
            assert not thread.is_alive(), "the reader is still blocked"
            assert 0.25 < elapsed < 2 * 0.3 + 1.0
            assert any("silent peer" in r.getMessage() for r in caplog.records)
        finally:
            a.close()
            b.close()

    def test_a_reader_that_never_leaves_is_given_up_on_after_two_joins(self, monkeypatch):
        monkeypatch.setattr(api, "WS_READER_JOIN_TIMEOUT", 0.2)
        gate = threading.Event()
        thread = threading.Thread(target=gate.wait, daemon=True)
        thread.start()
        a, b = tcp_pair()
        try:
            started = time.monotonic()
            api._end_websocket(FakeWs(thread, a))        # does not raise
            assert 0.35 < time.monotonic() - started < 3.0
            assert thread.is_alive()
        finally:
            gate.set()
            thread.join(2)
            a.close()
            b.close()

    def test_it_never_raises(self):
        api._end_websocket(FakeWs(close_exc=RuntimeError("close failed")))
        api._end_websocket(FakeWs(close_exc=ConnectionError("gone")))
        api._end_websocket(FakeWs(threading.Thread(target=lambda: None)))      # never started
        api._end_websocket(FakeWs(None, None))
        api._end_websocket(object())                                           # no close, no thread

    def test_a_shutdown_failure_is_swallowed(self, monkeypatch):
        monkeypatch.setattr(api, "WS_READER_JOIN_TIMEOUT", 0.1)
        gate = threading.Event()
        thread = threading.Thread(target=gate.wait, daemon=True)
        thread.start()
        a, b = tcp_pair()
        try:
            def broken(self, how):
                raise OSError(errno.ENOTCONN, "not connected")
            monkeypatch.setattr(api.socket.socket, "shutdown", broken)
            api._end_websocket(FakeWs(thread, a))
        finally:
            gate.set()
            thread.join(2)
            a.close()
            b.close()

    def test_the_current_thread_is_never_joined(self):
        ws = FakeWs(threading.current_thread(), None)
        api._end_websocket(ws)               # would deadlock / raise if it joined itself
        assert ws.closes == 1

    def test_mocks_are_a_no_op_beyond_close(self):
        ws = MagicMock()
        api._end_websocket(ws)
        ws.close.assert_called_once()
        ws.thread.join.assert_not_called()
        ws.sock.shutdown.assert_not_called()

    def test_the_shutdown_is_the_plain_socket_one_not_the_ssl_sockets(self, monkeypatch):
        """`SSLSocket.shutdown` would unwrap the TLS object from under the reader."""
        monkeypatch.setattr(api, "WS_READER_JOIN_TIMEOUT", 0.2)
        a, b = tcp_pair()
        tls_like = insecure_client_context().wrap_socket(a, do_handshake_on_connect=False)
        gate = threading.Event()
        thread = threading.Thread(target=gate.wait, daemon=True)
        thread.start()
        calls = []
        orig = socket.socket.shutdown
        monkeypatch.setattr(api.socket.socket, "shutdown",
                            lambda self, how: (calls.append(type(self)), orig(self, how)))
        try:
            assert tls_like._sslobj is not None
            api._end_websocket(FakeWs(thread, tls_like))
            assert calls == [ssl.SSLSocket]       # `socket.socket.shutdown` invoked on it
            assert tls_like._sslobj is not None   # the TLS object was not pulled from under the reader
        finally:
            gate.set()
            thread.join(2)
            tls_like.close()
            b.close()


@pytest.mark.integration
class TestEndWebsocketOnTheWire:
    def test_a_real_flask_sock_session_with_a_silent_client_leaves_promptly(self, pki, monkeypatch):
        """Through the real `backup_stream`-shaped route: the client sends `done`, gets the ack,
        and goes silent without closing. The route's end waits for the reader up to a small
        timeout and forces the shutdown, so the connection (and its slot) goes."""
        monkeypatch.setattr(api, "WS_READER_JOIN_TIMEOUT", 0.3)
        app = Flask("end_ws")
        sock = Sock(app)

        @sock.route("/s")
        def stream(ws):
            while True:
                data = ws.receive(timeout=10)
                if isinstance(data, str) and json.loads(data).get("done"):
                    break
            ws.send(json.dumps({"status": "ok"}))
            api._end_websocket(ws)

        with Serving(app, pki.gateway) as s:
            tls = tls_connect(s.port, pki.ctx_pinned)
            ws = websocket.WebSocket()
            ws.connect(f"wss://{LOOPBACK}:{s.port}/s", socket=tls, timeout=IO_TIMEOUT)
            ws.send(json.dumps({"done": True}))
            assert json.loads(ws.recv()) == {"status": "ok"}
            started = time.monotonic()
            assert s.wait_slots_free(8.0), s.slot_state()      # we never closed
            assert time.monotonic() - started < 5.0
            tls.close()


def _remote_config():
    from datetime import timedelta
    from zfsbackup.config import BackupConfig, DatasetConfig, RemoteServerConfig, RetentionRule
    return BackupConfig(
        datasets=[DatasetConfig(name="pool/data", frequency=timedelta(hours=1),
                                retention_rules=[RetentionRule(timedelta(days=1), timedelta(days=30))])],
        snapshot_prefix="autosnap",
        remote_backup=RemoteServerConfig(target_dataset="pool/backups", enabled=True),
    )


@pytest.mark.unit
class TestBackupStreamEndsTheWebsocket:
    """`backup_stream` calls `_end_websocket` once, after `gate.end()`, on every exit path."""

    @pytest.fixture
    def rig(self, mocker):
        from unittest.mock import PropertyMock
        from _api_support import Authed
        from zfsbackup.backup_manager import DatasetManager

        mocker.patch.object(DatasetManager, "datasets", new_callable=PropertyMock, return_value=[])
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.api.zfs.list", return_value=[])
        mocker.patch("zfsbackup.api.zfs.create")
        mocker.patch("zfsbackup.api.zfs.set")
        mocker.patch("zfsbackup.api.zfs.receive.abort")
        r = SimpleNamespace()
        r.authed = Authed(_remote_config())
        r.handler = r.authed.app.view_functions["backup_stream"].__wrapped__
        r.gate = r.authed.app.extensions["zfsbackup.stream_gate"]
        r.writer = MagicMock()
        r.receive = mocker.patch("zfsbackup.api.zfs.receive.filesystem", return_value=r.writer)
        r.seen = []                               # (gate.active, gate.draining) at each call
        mocker.patch("zfsbackup.api._end_websocket",
                     side_effect=lambda ws: r.seen.append((r.gate.active, ws)))
        r.ws = MagicMock()
        r.ws.receive.side_effect = [b"chunk", json.dumps({"done": True})]

        def run(mode="fresh", ws=None):
            ws = ws or r.ws
            qs = {} if mode is None else {"mode": mode}
            who = r.authed.cert_for("client1")
            with r.authed.context("/backup/client1/pool/data/stream", who=who, query_string=qs):
                r.handler(ws, "client1", "pool/data")
        r.run = run
        return r

    def test_after_a_complete_session(self, rig):
        rig.run()
        assert rig.seen == [(0, rig.ws)]

    @pytest.mark.parametrize("mode", [None, "bogus"])
    def test_after_a_refused_mode(self, rig, mode):
        rig.run(mode)
        assert rig.seen == [(0, rig.ws)]

    def test_after_a_draining_refusal(self, rig):
        rig.gate.start_draining()
        rig.run()
        assert rig.seen == [(0, rig.ws)]

    def test_when_remote_backup_is_disabled(self, mocker):
        from unittest.mock import PropertyMock
        from datetime import timedelta
        from _api_support import Authed
        from zfsbackup.backup_manager import DatasetManager
        from zfsbackup.config import BackupConfig, DatasetConfig

        mocker.patch.object(DatasetManager, "datasets", new_callable=PropertyMock, return_value=[])
        authed = Authed(BackupConfig(datasets=[DatasetConfig(name="pool/data",
                                                             frequency=timedelta(hours=1))],
                                     snapshot_prefix="autosnap"))
        end = mocker.patch("zfsbackup.api._end_websocket")
        ws = MagicMock()
        with authed.context("/backup/client1/pool/data/stream",
                            query_string={"mode": "fresh"}):
            authed.app.view_functions["backup_stream"].__wrapped__(ws, "client1", "pool/data")
        end.assert_called_once_with(ws)

    def test_after_a_failure_inside_the_receive(self, rig):
        rig.receive.side_effect = RuntimeError("zfs receive failed to start")
        try:
            rig.run()
        except RuntimeError:
            pass
        assert rig.seen == [(0, rig.ws)]
        assert rig.gate.active == 0

    def test_after_a_dead_socket(self, rig):
        rig.ws.receive.side_effect = ConnectionError("reset")
        rig.ws.send.side_effect = ConnectionError("reset")
        try:
            rig.run()
        except ConnectionError:
            pass
        assert rig.seen == [(0, rig.ws)]
        assert rig.gate.active == 0

    def test_a_failure_of_the_end_itself_does_not_leak_the_gate(self, rig, mocker):
        mocker.patch("zfsbackup.api._end_websocket", side_effect=RuntimeError("never raises"))
        try:
            rig.run()
        except RuntimeError:
            pass
        assert rig.gate.active == 0


# ---------------------------------------------------------------------------
# An EOF during the drain
# ---------------------------------------------------------------------------

EOF_LINE = "API: connection from ('127.0.0.1', 4242) ended after the response (EOF)"


@pytest.mark.unit
class TestDrainEof:
    """A 0-byte read is a teardown only once the drain has discarded body bytes."""

    def test_body_bytes_then_eof_is_one_debug_line_ending_in_eof(self, make_handler, caplog):
        h = make_handler(b"abc", b"de", b"")
        buf = bytearray(16)
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            assert h.rfile.raw.readinto(buf) == 3
            assert h.rfile.raw.readinto(buf) == 2
            assert h.drained_bytes == 5
            assert ended_lines(caplog) == []                 # data alone logs nothing
            assert h.rfile.raw.readinto(buf) == 0
        (line,) = ended_lines(caplog)
        assert line.getMessage() == EOF_LINE
        assert line.getMessage().endswith("(EOF)")
        assert h.drain_ended is True and h.close_connection is True

    def test_the_eof_is_sticky_and_the_dead_socket_is_not_read_again(self, make_handler, caplog):
        # the script holds more data after the EOF: a reader that read on would return it
        h = make_handler(b"abc", b"", b"never read", b"never read")
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            assert read(h) == 3
            assert read(h) == 0
            assert read(h) == 0
            assert read(h) == 0
        assert h.fake_raw.calls == 2
        assert len(ended_lines(caplog)) == 1                  # one line, however often it is read

    def test_the_eof_line_is_the_same_one_line_as_for_an_exception(self, make_handler, caplog):
        """A reset that arrives as an exception and one that arrives as an EOF are logged
        alike: one DEBUG line at the same place, only the reason differs."""
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            reset_exc = ConnectionResetError(errno.ECONNRESET, "reset")
            for script in ((b"abc", reset_exc), (b"abc", b"")):
                h = make_handler(*script)
                assert read(h) == 3 and read(h) == 0 and read(h) == 0
                assert h.drain_ended is True and h.close_connection is True
        lines = [r.getMessage() for r in ended_lines(caplog)]
        assert len(lines) == 2
        assert lines[0] == (
            f"API: connection from ('127.0.0.1', 4242) ended after the response "
            f"(ConnectionResetError: {reset_exc})"
        )
        assert lines[1] == EOF_LINE

    def test_an_eof_with_nothing_drained_is_a_silent_normal_close(self, make_handler, caplog):
        h = make_handler(b"")
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            assert read(h) == 0
        assert ended_lines(caplog) == []
        assert h.drain_ended is False
        assert getattr(h, "close_connection", None) is not True
        assert h.drained_bytes == 0

    def test_an_eof_with_nothing_drained_is_not_sticky(self, make_handler, caplog):
        """No teardown was decided, so the next read still goes to the reader (as before)."""
        h = make_handler(b"", b"late")
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            assert read(h) == 0
            assert read(h) == 4
        assert h.fake_raw.calls == 2
        assert ended_lines(caplog) == []

    def test_a_drain_that_ended_at_the_deadline_is_not_logged_a_second_time(
        self, make_handler, caplog
    ):
        h = make_handler(b"abc", b"", drain_timeout=5.0)
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            assert read(h) == 3
            h.drain_deadline = time.monotonic() - 0.01
            assert read(h) == 0 and read(h) == 0
        (line,) = ended_lines(caplog)
        assert "TimeoutError: drain deadline passed" in line.getMessage()
        assert h.fake_raw.calls == 1                           # the EOF was never even read

    def test_bytes_read_before_the_response_are_not_drained_bytes(self, make_handler, caplog):
        """The request body the app read (before the status line) is not a discarded body: an
        EOF after the response with nothing left to discard is still a normal close."""
        h = make_handler(b"request-body", b"", response_started=False)
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            assert read(h) == len(b"request-body")
            assert h.drained_bytes == 0
            h.response_started = True
            assert read(h) == 0
        assert ended_lines(caplog) == []
        assert h.drain_ended is False

    def test_a_websocket_never_reads_so_it_never_logs_an_eof(self, make_handler, caplog):
        h = make_handler(b"x", b"", upgrade="websocket")
        h.drained_bytes = 7                                   # even with bytes on the books
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            assert read(h) == 0
        assert h.fake_raw.calls == 0
        assert ended_lines(caplog) == []
        assert h.drain_ended is False


@pytest.mark.unit
class TestDrainedBytesPerRequest:
    def test_the_class_default(self, handler_cls):
        assert handler_cls.drained_bytes == 0

    def test_it_counts_every_drained_byte(self, make_handler):
        h = make_handler(b"a" * 10, b"b" * 7, b"c")
        assert h.drained_bytes == 0
        read(h)
        read(h)
        read(h)
        assert h.drained_bytes == 18

    def test_a_new_request_starts_from_zero(self, make_handler, caplog):
        h = make_handler(b"abc")
        assert read(h) == 3
        assert h.drained_bytes == 3
        h.peer.close()                    # the next request line reads EOF (the script is empty)
        h.handle_one_request()
        assert h.drained_bytes == 0
        assert h.response_started is False

    def test_an_eof_in_the_next_request_is_judged_on_its_own_drain(self, make_handler, caplog):
        """The previous request drained bytes; this one drained none. An EOF here is a normal
        close and must not be logged because of the last request's bytes."""
        h = make_handler(b"abc", b"x")
        assert read(h) == 3 and h.drained_bytes == 3
        h.fake_raw.script = []             # EOF for the request line of the next request
        h.handle_one_request()
        assert h.drained_bytes == 0
        h.response_started = True          # (the next request got as far as its response)
        with caplog.at_level(logging.DEBUG, logger=DRAIN_LOGGER):
            assert read(h) == 0
        assert ended_lines(caplog) == []
        assert h.drain_ended is False

@pytest.mark.integration
class TestDrainEofOnTheWire:
    @staticmethod
    def _fin_without_close_notify(port, ctx, *, declared=50_000_000, chunks=2, linger=0.3):
        """POST with an unread body, read the response head, then close the TCP connection
        with a plain FIN and no TLS `close_notify` (a ragged EOF at the server)."""
        tls = tls_connect(port, ctx)
        tls.sendall(b"POST /ignore HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n" % declared)
        for _ in range(chunks):
            tls.sendall(b"y" * 65536)
        got = b""
        tls.settimeout(IO_TIMEOUT)
        while b"\r\n\r\n" not in got:
            data = tls.recv(4096)
            if not data:
                break
            got += data
        time.sleep(linger)                    # the server's drain has read everything, and waits
        raw = socket.socket(fileno=tls.detach())
        raw.close()
        return got.split(b"\r\n", 1)[0]

    @pytest.mark.parametrize("who", ["pinned", "anonymous"])
    def test_a_ragged_eof_during_the_drain_is_one_debug_line_and_no_error(self, pki, caplog, who):
        ctx = pki.ctx_pinned if who == "pinned" else pki.ctx_anon
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway).spy() as s:
                statuses = {self._fin_without_close_notify(s.port, ctx) for _ in range(3)}
                assert s.wait_slots_free(10.0), s.slot_state()
                assert statuses == {b"HTTP/1.1 200 OK"}
                assert s.reads_after_response(), "the drain never ran: this test proves nothing"
        assert errors(caplog) == [], [r.getMessage() for r in errors(caplog)]
        assert "Error on request" not in caplog.text
        assert len(ended_lines(caplog)) == 3                  # exactly one per connection

    def test_a_request_with_no_body_to_discard_logs_nothing(self, pki, caplog):
        """The mirror case: nothing is left unread, so the client's close is a normal end."""
        with caplog.at_level(logging.DEBUG):
            with Serving(make_app(), pki.gateway) as s:
                tls = tls_connect(s.port, pki.ctx_pinned)
                tls.sendall(b"GET /plain HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                tls.settimeout(IO_TIMEOUT)
                got = b""
                while True:
                    data = tls.recv(4096)
                    if not data:
                        break
                    got += data
                tls.close()
                assert got.startswith(b"HTTP/1.1 200")
                assert s.wait_slots_free(10.0), s.slot_state()
        assert errors(caplog) == []
        assert ended_lines(caplog) == []

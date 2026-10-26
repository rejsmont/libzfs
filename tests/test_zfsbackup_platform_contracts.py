"""Contract tests for the third-party internals zfsbackup builds on (C19 / S-9b / Q12).

``zfsbackup.workers._api_server_class`` subclasses werkzeug's ``ThreadedWSGIServer`` and
``WSGIRequestHandler`` and reaches into hooks that are not part of any documented API;
``zfsbackup.identity.pinned_session`` overrides ``requests.adapters.HTTPAdapter`` hooks whose
docstrings say "only exposed for use when subclassing"; ``_SessionSocket`` stands in for the
socket simple-websocket 1.1 hands to its reader thread. A dependency upgrade that changes any of
these does not raise an ``ImportError``: it silently turns a security control (the TLS pin, the
connection-slot accounting, the per-connection handshake) into dead code.

``tests/test_zfsbackup_tls_platform.py`` characterises the *behaviour* the TLS design needs
(S0-S10). This file pins the *surface* we hook into, and each failure names the internal that
moved and what zfsbackup does with it, so the weekly latest-dependencies job (and a Dependabot
bump) produces a message that points at the line to revisit, not a distant symptom.

Two kinds of test:

* **Signature contracts** (``unit``): ``inspect.signature`` of every internal we override or call,
  compared with the shape our code uses. New *optional* trailing parameters are tolerated (they
  cannot break a caller); a rename, reorder or removal is not. Every override in the API server
  classes is also checked against its base class automatically, so a new override is covered
  the day it is added.
* **Behaviour contracts** (``integration``): a plain-HTTP rig built from werkzeug and the standard
  library only (no zfsbackup code), recording the hook order, threads and arguments the API
  server's design depends on. All sockets are ``127.0.0.1`` port 0, no TLS material is needed,
  nothing reaches ``zfs``, every server and client thread is shut down and joined.

Not duplicated here: the end-to-end pinned-TLS behaviour (``test_zfsbackup_tls_platform.py``,
``TestPinnedSession`` in ``test_zfsbackup_identity.py``) and the ``_SessionSocket`` unit tests
(``test_zfsbackup_api_wssession.py``).
"""

import http.client
import http.server
import importlib.metadata
import inspect
import io
import os
import socket
import socketserver
import ssl
import threading
import time
from types import SimpleNamespace
from unittest import mock

import pytest

pytest.importorskip("cryptography")
import requests  # noqa: E402
import urllib3  # noqa: E402
import websocket  # noqa: E402
from flask import Flask  # noqa: E402
from flask_sock import Sock  # noqa: E402
from requests.adapters import BaseAdapter, HTTPAdapter  # noqa: E402
from simple_websocket import ConnectionClosed, Server as WsServer  # noqa: E402
from simple_websocket import ws as simple_ws  # noqa: E402
from werkzeug import serving as wz_serving  # noqa: E402
from werkzeug.serving import (  # noqa: E402
    BaseWSGIServer,
    ThreadedWSGIServer,
    WSGIRequestHandler,
)

from zfsbackup import identity, workers  # noqa: E402

LOOPBACK = "127.0.0.1"
WAIT = 10.0


def _version(dist: str) -> str:
    try:
        return importlib.metadata.version(dist)
    except importlib.metadata.PackageNotFoundError:  # pragma: no cover
        return "?"


def _versions() -> str:
    return ", ".join(
        f"{d} {_version(d)}"
        for d in ("werkzeug", "requests", "urllib3", "simple-websocket", "flask-sock")
    )


# ---------------------------------------------------------------------------
# Signature helper
# ---------------------------------------------------------------------------


def _signature_problem(func, expected, *, keyword_only=()):
    """Why `func`'s parameters are not `expected` (names, in order, `self` included), or None.

    Extra trailing parameters are fine only if they have defaults (or are *args/**kwargs):
    they cannot break a caller that passes `expected`. Every name in `keyword_only` must exist
    and must not be positional-only.
    """
    params = list(inspect.signature(func).parameters.values())
    names = tuple(p.name for p in params)
    if names[: len(expected)] != tuple(expected):
        return f"parameters are {names}, expected them to start with {tuple(expected)}"
    for extra in params[len(expected):]:
        if (extra.default is inspect.Parameter.empty
                and extra.kind in (inspect.Parameter.POSITIONAL_ONLY,
                                   inspect.Parameter.POSITIONAL_OR_KEYWORD,
                                   inspect.Parameter.KEYWORD_ONLY)):
            return f"new required parameter {extra.name!r} after {tuple(expected)}"
    for name in keyword_only:
        if name not in names:
            return f"parameter {name!r} is gone"
    return None


def _assert_signature(func, label, expected, why, **kw):
    problem = _signature_problem(func, expected, **kw)
    assert problem is None, (
        f"CONTRACT BROKEN: {label}: {problem}. zfsbackup relies on it: {why} "
        f"(installed: {_versions()}). Revisit the code named above before widening the pin."
    )


# (callable, label, expected parameter names, why it matters)
WERKZEUG_AND_STDLIB_SIGNATURES = [
    (
        BaseWSGIServer.__init__, "werkzeug.serving.BaseWSGIServer.__init__",
        ("self", "host", "port", "app", "handler", "passthrough_errors", "ssl_context"),
        "workers.ApiServer.__init__ calls super().__init__(host, port, app, ApiRequestHandler, "
        "False, None) positionally",
    ),
    (
        ThreadedWSGIServer.__init__, "werkzeug.serving.ThreadedWSGIServer.__init__",
        ("self", "host", "port", "app", "handler", "passthrough_errors", "ssl_context"),
        "ApiServer's super().__init__ resolves to it (positional host..ssl_context)",
    ),
    (
        socketserver.ThreadingMixIn.process_request,
        "socketserver.ThreadingMixIn.process_request",
        ("self", "request", "client_address"),
        "ApiServer.process_request takes a connection slot, then calls super().process_request"
        "(request, client_address) to start the connection's thread",
    ),
    (
        socketserver.ThreadingMixIn.process_request_thread,
        "socketserver.ThreadingMixIn.process_request_thread",
        ("self", "request", "client_address"),
        "ApiServer.process_request_thread runs the TLS handshake on the connection's own thread "
        "and then calls super().process_request_thread(tls, client_address)",
    ),
    (
        socketserver.BaseServer.shutdown_request, "socketserver.BaseServer.shutdown_request",
        ("self", "request"),
        "ApiServer.shutdown_request skips the close of a connection a websocket session took over",
    ),
    (
        socketserver.TCPServer.server_bind, "socketserver.TCPServer.server_bind", ("self",),
        "ApiServer.server_bind calls it unbound (to skip HTTPServer's reverse DNS lookup)",
    ),
    (
        socketserver.StreamRequestHandler.setup, "socketserver.StreamRequestHandler.setup",
        ("self",),
        "ApiRequestHandler.setup wraps the rfile/wfile it creates",
    ),
    (
        http.server.BaseHTTPRequestHandler.handle_one_request,
        "http.server.BaseHTTPRequestHandler.handle_one_request", ("self",),
        "ApiRequestHandler arms the per-request deadline before delegating",
    ),
    (
        http.server.BaseHTTPRequestHandler.send_response,
        "http.server.BaseHTTPRequestHandler.send_response", ("self", "code", "message"),
        "werkzeug's run_wsgi calls self.send_response(code, msg) positionally; "
        "ApiRequestHandler.send_response marks the start of a response and drops a post-websocket one",
    ),
    (
        http.server.BaseHTTPRequestHandler.log_error,
        "http.server.BaseHTTPRequestHandler.log_error", ("self", "format", "args"),
        "ApiRequestHandler.log_error demotes 'Request timed out' to DEBUG",
    ),
    (
        WSGIRequestHandler.make_environ, "werkzeug.serving.WSGIRequestHandler.make_environ",
        ("self",),
        "ApiRequestHandler.make_environ swaps environ['werkzeug.socket'] for a websocket request",
    ),
    (
        WSGIRequestHandler.run_wsgi, "werkzeug.serving.WSGIRequestHandler.run_wsgi", ("self",),
        "ApiRequestHandler.run_wsgi ends the websocket session before the connection is closed",
    ),
    (
        WSGIRequestHandler.log_request, "werkzeug.serving.WSGIRequestHandler.log_request",
        ("self", "code", "size"),
        "the per-request access line is the one werkzeug record zfsbackup's logging policy "
        "(C3) singles out",
    ),
    (
        wz_serving.get_sockaddr, "werkzeug.serving.get_sockaddr", ("host", "port", "family"),
        "daemon._probe_bind resolves the API address the way werkzeug will",
    ),
    (
        wz_serving.select_address_family, "werkzeug.serving.select_address_family",
        ("host", "port"),
        "daemon._probe_bind picks the address family the way werkzeug will",
    ),
]


# ---------------------------------------------------------------------------
# The API server classes as built by workers._api_server_class()
# ---------------------------------------------------------------------------


def _stub_gateway():
    """Enough gateway to construct the server: a context, no certificate."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    return SimpleNamespace(current_context=lambda: context)


@pytest.fixture(scope="module")
def api_classes():
    server_cls = workers._api_server_class()
    server = server_cls(LOOPBACK, 0, Flask("contract"), _stub_gateway())
    try:
        handler_cls = server.RequestHandlerClass
    finally:
        server.server_close()
    return SimpleNamespace(server=server_cls, handler=handler_cls)


@pytest.fixture(scope="module")
def session_cls(api_classes):
    """`_SessionSocket` (local to `_api_server_class`), found through `make_environ`'s closure."""
    cls = inspect.getclosurevars(api_classes.handler.make_environ).nonlocals.get("_SessionSocket")
    assert cls is not None, (
        "workers.ApiRequestHandler.make_environ no longer refers to _SessionSocket; "
        "update this fixture together with the websocket wiring"
    )
    return cls


@pytest.mark.unit
class TestSignatures:
    @pytest.mark.parametrize(
        "func, label, expected, why", WERKZEUG_AND_STDLIB_SIGNATURES,
        ids=[row[1] for row in WERKZEUG_AND_STDLIB_SIGNATURES],
    )
    def test_internal_signature(self, func, label, expected, why):
        _assert_signature(func, label, expected, why)

    def test_the_install_is_the_pinned_family(self):
        """Not a contract on its own: names the versions in the CI log next to any failure."""
        assert _version("werkzeug").startswith("3."), (
            f"werkzeug {_version('werkzeug')}: the 3.x serving internals are what "
            "workers._api_server_class was written against (pyproject caps it)"
        )
        assert _version("urllib3").startswith("2."), f"urllib3 {_version('urllib3')} (pyproject caps it)"
        assert _version("simple-websocket").startswith("1.1"), (
            f"simple-websocket {_version('simple-websocket')}: workers._SessionSocket was written "
            "against 1.1's socket surface (pyproject pins >=1.1,<1.2)"
        )

    @pytest.mark.parametrize("which", ["server", "handler"])
    def test_every_override_matches_the_base_it_overrides(self, api_classes, which):
        """Whatever the API server classes override, the base's parameters (names, order) still
        match: a rename upstream would otherwise leave the override accepting the old shape
        while the base calls it with the new one. `__init__` is checked explicitly instead."""
        cls = getattr(api_classes, which)
        checked = []
        for name, value in vars(cls).items():
            if name.startswith("__") or not inspect.isfunction(value):
                continue
            base = next((getattr(b, name) for b in cls.__mro__[1:] if name in vars(b)), None)
            if base is None or not inspect.isfunction(base):
                continue
            checked.append(name)
            base_names = tuple(inspect.signature(base).parameters)
            problem = _signature_problem(value, base_names)
            assert problem is None, (
                f"CONTRACT BROKEN: {cls.__name__}.{name} overrides "
                f"{getattr(base, '__qualname__', name)}{inspect.signature(base)} but is "
                f"{inspect.signature(value)} ({problem}); installed: {_versions()}"
            )
        assert checked, f"{cls.__name__} no longer overrides anything: this test lost its subject"

    def test_api_server_init_passes_the_positional_shape_to_werkzeug(self, api_classes):
        source = inspect.getsource(api_classes.server.__init__)
        assert "super().__init__(host, port, app, ApiRequestHandler, False, None)" in source, (
            "ApiServer.__init__ no longer calls werkzeug positionally as "
            "(host, port, app, handler, passthrough_errors, ssl_context): update "
            "WERKZEUG_AND_STDLIB_SIGNATURES with the new call"
        )


@pytest.mark.unit
class TestServerBindContract:
    """`ApiServer.server_bind` replaces `http.server.HTTPServer.server_bind` (which does a reverse
    DNS lookup, ~30 s on a broken resolver) with `TCPServer.server_bind` plus its bookkeeping."""

    def test_werkzeug_does_not_override_server_bind(self):
        assert ThreadedWSGIServer.server_bind is http.server.HTTPServer.server_bind, (
            "werkzeug's ThreadedWSGIServer now defines its own server_bind: "
            "workers.ApiServer.server_bind bypasses HTTPServer's and would skip whatever "
            "werkzeug added; re-derive it"
        )

    def test_httpserver_server_bind_only_adds_name_and_port(self, monkeypatch):
        """If HTTPServer.server_bind starts setting anything else, ApiServer.server_bind (which
        sets `server_name` and `server_port` only) would leave it unset."""
        monkeypatch.setattr(socket, "getfqdn", lambda host="": "stub.invalid")
        srv = http.server.HTTPServer(
            (LOOPBACK, 0), http.server.BaseHTTPRequestHandler, bind_and_activate=False
        )
        try:
            before = set(vars(srv))
            srv.server_bind()
            added = set(vars(srv)) - before
            assert added == {"server_name", "server_port"}, (
                f"http.server.HTTPServer.server_bind now also sets {sorted(added - {'server_name', 'server_port'})}"
                " (or no longer sets server_name/server_port): workers.ApiServer.server_bind "
                "replicates only those two"
            )
            assert srv.server_name == "stub.invalid", "server_name is no longer socket.getfqdn(host)"
            assert srv.server_port == srv.server_address[1] != 0
        finally:
            srv.server_close()

    def test_werkzeug_wsgi_environ_does_not_read_server_name(self):
        """The reason skipping the lookup is safe: werkzeug builds SERVER_NAME from
        `server_address`, not from `server_name`."""
        source = inspect.getsource(WSGIRequestHandler.make_environ)
        assert 'self.server.server_address[0]' in source and "server_name" not in source, (
            "werkzeug's make_environ now reads server_name: ApiServer.server_bind sets it to "
            "the configured host instead of the FQDN; check that is still what the app wants"
        )


# ---------------------------------------------------------------------------
# A plain-HTTP rig made only of werkzeug and the standard library
# ---------------------------------------------------------------------------


class Recorder:
    """A thread-safe, ordered log of (name, thread ident, details)."""

    def __init__(self):
        self.events = []
        self._lock = threading.Lock()
        self.extra = {}

    def add(self, name, **details):
        with self._lock:
            self.events.append((name, threading.get_ident(), details))

    def named(self, name):
        with self._lock:
            return [e for e in self.events if e[0] == name]

    def names(self):
        with self._lock:
            return [e[0] for e in self.events]

    def wait_for(self, name, count=1, timeout=WAIT):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if len(self.named(name)) >= count:
                return True
            time.sleep(0.01)
        return False


class _RecordingRaw(io.RawIOBase):
    """The raw reader of the handler's rfile, as `_DeadlineReader` is: records every read."""

    def __init__(self, raw, rec, *, raise_timeout=False):
        super().__init__()
        self._raw = raw
        self._rec = rec
        self._raise_timeout = raise_timeout

    def readable(self):
        return True

    def readinto(self, buffer):
        if self._raise_timeout:
            raise TimeoutError("contract: request not received in time")
        n = self._raw.readinto(buffer)
        self._rec.add("read", n=n)
        return n

    def close(self):
        try:
            self._raw.close()
        finally:
            super().close()


def make_rig_classes(rec, *, rfile_timeout=False, socket_timeout=None):
    class Handler(WSGIRequestHandler):
        def setup(self):
            rec.add("setup")
            super().setup()
            rec.add(
                "rfile", buffered=isinstance(self.rfile, io.BufferedReader),
                detachable=hasattr(self.rfile, "detach"),
                wfile_api=all(hasattr(self.wfile, a) for a in ("write", "flush", "close", "closed")),
            )
            if socket_timeout is not None:
                self.connection.settimeout(socket_timeout)
            # Exactly what ApiRequestHandler.setup does.
            self.rfile = io.BufferedReader(
                _RecordingRaw(self.rfile.detach(), rec, raise_timeout=rfile_timeout),
                io.DEFAULT_BUFFER_SIZE,
            )

        def handle_one_request(self):
            rec.add("handle_one_request", connection=id(self.connection))
            return super().handle_one_request()

        def make_environ(self):
            environ = super().make_environ()
            rec.add(
                "make_environ",
                socket_is_connection=environ.get("werkzeug.socket") is self.connection,
                scheme=environ["wsgi.url_scheme"],
                upgrade=self.headers.get("Upgrade"),
                headers_get_all=callable(getattr(self.headers, "get_all", None)),
            )
            self._contract_environ_hook(environ)
            return environ

        def _contract_environ_hook(self, environ):
            """Subclasses may swap things in the environ here."""

        def run_wsgi(self):
            rec.add("run_wsgi", close_connection=self.close_connection)
            try:
                return super().run_wsgi()
            finally:
                rec.add("run_wsgi_done")

        def send_response(self, code, message=None):
            rec.add("send_response", code=code, message=message,
                    upgrade=(self.headers.get("Upgrade") if self.headers else None))
            return super().send_response(code, message)

        def log_error(self, format, *args):
            rec.add("log_error", format=format)
            return super().log_error(format, *args)

    class Server(ThreadedWSGIServer):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        def server_bind(self):
            rec.add("server_bind", host=getattr(self, "host", "<unset>"),
                    ssl_context=getattr(self, "ssl_context", "<unset>"))
            return super().server_bind()

        def process_request(self, request, client_address):
            rec.add("process_request", request=id(request))
            return super().process_request(request, client_address)

        def process_request_thread(self, request, client_address):
            rec.add("process_request_thread", request=id(request))
            return super().process_request_thread(request, client_address)

        def shutdown_request(self, request):
            rec.add("shutdown_request", request=id(request))
            return super().shutdown_request(request)

        def log(self, type, message, *args):  # keep the test output quiet
            pass

    return Server, Handler


class Rig:
    """A served rig: `Server(LOOPBACK, 0, app, Handler, False, None)` on a thread."""

    def __init__(self, app, rec=None, handler_factory=None, **handler_kwargs):
        self.rec = rec or Recorder()
        server_cls, handler_cls = make_rig_classes(self.rec, **handler_kwargs)
        if handler_factory is not None:
            handler_cls = handler_factory(handler_cls, self.rec)
        self.server = server_cls(LOOPBACK, 0, app, handler_cls, False, None)
        self.serve_thread = threading.Thread(target=self.server.serve_forever, daemon=True,
                                             name="contract-serve")
        self.port = self.server.server_address[1]

    def __enter__(self):
        self.serve_thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.serve_thread.join(WAIT)
        self.server.server_close()
        assert not self.serve_thread.is_alive()


def _hello_app(seen=None):
    def app(environ, start_response):
        if seen is not None:
            seen.append(environ)
        body = b"ok"
        start_response("200 OK", [("Content-Type", "text/plain"),
                                  ("Content-Length", str(len(body)))])
        return [body]
    return app


def _get(port, path="/", method="GET", body=None):
    conn = http.client.HTTPConnection(LOOPBACK, port, timeout=WAIT)
    try:
        conn.request(method, path, body=body)
        resp = conn.getresponse()
        data = resp.read()
        return resp.status, data
    finally:
        conn.close()


@pytest.mark.integration
class TestWerkzeugServerContract:
    def test_construction_binds_after_host_and_ssl_context_are_set(self):
        """`ApiServer.server_bind` reads `self.host`, and ApiServer.__init__ assigns
        `self.ssl_context` AFTER the base constructor; both depend on the constructor order."""
        rec = Recorder()
        with Rig(_hello_app(), rec) as rig:
            binds = rec.named("server_bind")
            assert len(binds) == 1, (
                "werkzeug's BaseWSGIServer.__init__ no longer binds exactly once in the constructor"
            )
            details = binds[0][2]
            assert details["host"] == LOOPBACK, (
                "BaseWSGIServer.__init__ now calls server_bind() before assigning self.host: "
                "workers.ApiServer.server_bind (self.server_name = str(self.host)) would break"
            )
            assert rig.server.host == LOOPBACK
            assert rig.server.port == rig.server.server_address[1] != 0
            assert rig.server.ssl_context is None, (
                "BaseWSGIServer no longer sets ssl_context = None without one: "
                "ApiServer.__init__ assigns it afterwards for werkzeug's environ"
            )

    def test_bind_failure_is_a_systemexit_not_an_oserror(self, capfd):
        """`_bind_api_server` converts werkzeug's `sys.exit(1)` back into an OSError."""
        holder = socket.socket()
        try:
            holder.bind((LOOPBACK, 0))
            holder.listen(1)
            port = holder.getsockname()[1]
            with pytest.raises(SystemExit) as info:
                ThreadedWSGIServer(LOOPBACK, port, _hello_app(), None, False, None)
            assert info.value.code == 1, "werkzeug's bind failure no longer exits with status 1"
        finally:
            holder.close()
        capfd.readouterr()

    def test_threaded_server_semantics(self):
        assert ThreadedWSGIServer.daemon_threads is True, (
            "ThreadedWSGIServer.daemon_threads is no longer True: stalled connection threads "
            "would then block process exit (API_HANDSHAKE_TIMEOUT)"
        )
        assert ThreadedWSGIServer.multithread is True
        assert issubclass(ThreadedWSGIServer, socketserver.ThreadingMixIn)
        assert ThreadedWSGIServer.allow_reuse_address is True

    def test_hook_order_threads_and_the_connection_identity(self):
        rec = Recorder()
        with Rig(_hello_app(), rec) as rig:
            assert _get(rig.port) == (200, b"ok")
            assert rec.wait_for("shutdown_request"), rec.names()
            order = rec.names()
            for earlier, later in [
                ("process_request", "process_request_thread"),
                ("process_request_thread", "setup"),
                ("setup", "handle_one_request"),
                ("handle_one_request", "run_wsgi"),
                ("run_wsgi", "make_environ"),    # make_environ is called INSIDE run_wsgi:
                ("make_environ", "send_response"),  # the session socket is made there, ended after
                ("send_response", "shutdown_request"),
            ]:
                assert order.index(earlier) < order.index(later), (
                    f"the hook order changed: {earlier!r} no longer precedes {later!r} "
                    f"(saw {order}); workers.ApiServer/ApiRequestHandler assume it "
                    f"(installed: {_versions()})"
                )
            ident = lambda n: rec.named(n)[0][1]  # noqa: E731
            assert ident("process_request") == rig.serve_thread.ident, (
                "ThreadingMixIn.process_request no longer runs on the accept thread: "
                "ApiServer.process_request takes a slot there, non-blocking"
            )
            conn_thread = ident("process_request_thread")
            assert conn_thread != rig.serve_thread.ident
            for name in ("setup", "make_environ", "run_wsgi", "send_response", "shutdown_request"):
                assert ident(name) == conn_thread, (
                    f"{name} no longer runs on the connection's own thread: ApiServer's "
                    "threading.local connection_state (pinned / handed_over) is per thread"
                )
            request_id = rec.named("process_request_thread")[0][2]["request"]
            assert rec.named("handle_one_request")[0][2]["connection"] == request_id, (
                "handler.connection is no longer the object passed to process_request_thread: "
                "the websocket hand-over compares it with shutdown_request's argument"
            )
            assert rec.named("shutdown_request")[0][2]["request"] == request_id, (
                "shutdown_request now receives a different object than process_request_thread "
                "was given: the websocket hand-over (`handed_over`) would never match"
            )

    def test_handler_stream_shapes_and_environ(self):
        rec = Recorder()
        seen = []
        with Rig(_hello_app(seen), rec) as rig:
            assert _get(rig.port)[0] == 200
            assert rec.wait_for("shutdown_request")
            rfile = rec.named("rfile")[0][2]
            assert rfile["buffered"] and rfile["detachable"], (
                "StreamRequestHandler.rfile is no longer a detachable io.BufferedReader: "
                "ApiRequestHandler.setup wraps it with .detach()"
            )
            assert rfile["wfile_api"], "wfile lost write/flush/close/closed (_ResponseWriter wraps it)"
            made = rec.named("make_environ")[0][2]
            assert made["socket_is_connection"], (
                "environ['werkzeug.socket'] is no longer handler.connection: "
                "simple-websocket takes its socket from there (_SessionSocket replaces it)"
            )
            assert made["headers_get_all"], "handler.headers lost get_all (is_websocket_request)"
            assert made["scheme"] == "http"
            send = rec.named("send_response")[0][2]
            assert send["code"] == 200 and isinstance(send["code"], int)
            assert send["message"] == "OK", "run_wsgi no longer passes (code, message)"
            assert seen[0]["werkzeug.socket"] is not None

    def test_environ_scheme_follows_server_ssl_context(self):
        """`ApiServer` assigns `ssl_context` after construction purely so the environ says https."""
        seen = []
        with Rig(_hello_app(seen)) as rig:
            rig.server.ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            assert _get(rig.port)[0] == 200
        assert seen[0]["wsgi.url_scheme"] == "https", (
            "werkzeug's make_environ no longer derives wsgi.url_scheme from server.ssl_context"
        )

    def test_environ_ssl_client_cert_needs_getpeercert_on_the_connection(self):
        """The source of `SSL_CLIENT_CERT` (every authenticated route reads it)."""
        source = inspect.getsource(WSGIRequestHandler.make_environ)
        assert "getpeercert(binary_form=True)" in source and '"SSL_CLIENT_CERT"' in source, (
            "werkzeug's make_environ no longer fills SSL_CLIENT_CERT from "
            "connection.getpeercert(binary_form=True): api.py authentication would see no certificate"
        )

    def test_unread_request_body_is_drained_after_the_response(self):
        """`_DeadlineReader` treats every rfile read after `send_response` as werkzeug's drain."""
        rec = Recorder()
        with Rig(_hello_app(), rec) as rig:
            # Larger than the handler's read buffer (io.DEFAULT_BUFFER_SIZE: 128 KiB on 3.14),
            # so part of the body is certainly still unread when the response goes out.
            assert _get(rig.port, method="POST", body=b"x" * (1 << 20))[0] == 200
            assert rec.wait_for("shutdown_request")
            names = rec.names()
            first_response = names.index("send_response")
            late_reads = [
                e for i, e in enumerate(rec.events)
                if e[0] == "read" and i > first_response and e[2]["n"]
            ]
            assert late_reads, (
                "werkzeug's run_wsgi no longer reads (drains) the unread request body from "
                "handler.rfile after the response: ApiRequestHandler/_DeadlineReader's "
                "API_DRAIN_TIMEOUT would be dead code, and the 413 path would reset"
            )

    @pytest.mark.parametrize("how", ["socket-timeout", "rfile-raises"])
    def test_a_timeout_reading_the_request_is_logged_and_closes(self, how):
        """`ApiRequestHandler.log_error` recognises the 'Request timed out' text, and a
        TimeoutError out of `rfile.readline()` closes the connection."""
        rec = Recorder()
        kwargs = {"socket_timeout": 0.3} if how == "socket-timeout" else {"rfile_timeout": True}
        with Rig(_hello_app(), rec, **kwargs) as rig:
            client = socket.create_connection((LOOPBACK, rig.port), timeout=WAIT)
            try:
                # Nothing is sent: the server's first read of the request line times out (the
                # raw reader raises at once, or the socket timeout expires). Unread client bytes
                # would turn the close into a reset.
                assert rec.wait_for("log_error"), rec.names()
                assert client.recv(10) == b"", "the connection stays open after a request timeout"
            finally:
                client.close()
            fmt = rec.named("log_error")[0][2]["format"]
            assert fmt.startswith("Request timed out"), (
                f"http.server's timeout report is now {fmt!r}: ApiRequestHandler.log_error "
                "matches on 'Request timed out' to keep idle connections out of the error log"
            )
            assert rec.wait_for("shutdown_request")


# ---------------------------------------------------------------------------
# simple-websocket 1.1 / flask-sock on the werkzeug rig
# ---------------------------------------------------------------------------


class SocketProxy:
    """What `_SessionSocket` is to simple-websocket: records every attribute simple-websocket
    touches, and delegates to the real connection. `close()` is a no-op, as `_SessionSocket`'s."""

    def __init__(self, sock, rec):
        self._sock = sock
        self._rec = rec
        self.first_send = None

    def send(self, data, flags=0):
        self._rec.add("sock.send")
        if self.first_send is None:
            self.first_send = bytes(data[:12])
        self._sock.sendall(data)
        return len(data)

    def recv(self, bufsize, flags=0):
        self._rec.add("sock.recv")
        return self._sock.recv(bufsize, flags)

    def fileno(self):
        self._rec.add("sock.fileno")
        return self._sock.fileno()

    def close(self):
        self._rec.add("sock.close")

    def __getattr__(self, name):
        # Anything else simple-websocket touches is a surface _SessionSocket may lack.
        self._rec.add("sock.other", attr=name)
        return getattr(self._sock, name)


def _ws_handler_factory(handler_cls, rec):
    class WsHandler(handler_cls):
        def _contract_environ_hook(self, environ):
            proxy = SocketProxy(self.connection, rec)
            rec.extra["proxy"] = proxy
            environ["werkzeug.socket"] = proxy
    return WsHandler


def _ws_app(rec, **server_options):
    app = Flask("contract-ws")
    app.config["SOCK_SERVER_OPTIONS"] = server_options
    sock = Sock(app)

    @sock.route("/ws")
    def ws_route(ws):
        rec.extra["ws"] = ws
        rec.extra["mode"] = ws.mode
        try:
            ws.send(ws.receive(timeout=WAIT))
            rec.extra["idle_receive"] = ws.receive(timeout=0.05)
            try:
                ws.receive(timeout=WAIT)
            except ConnectionClosed as exc:
                rec.extra["closed_exc"] = exc
            try:
                ws.send("late")
            except ConnectionClosed:
                rec.extra["send_after_close_raised"] = True
            try:
                ws.close()
            except ConnectionClosed:
                rec.extra["close_after_close_raised"] = True
        except Exception as exc:  # pragma: no cover - reported by the test
            rec.extra["route_error"] = repr(exc)

    return app


def _run_ws_session(rig, hold):
    client = websocket.create_connection(f"ws://{LOOPBACK}:{rig.port}/ws", timeout=WAIT)
    try:
        client.send("hello")
        assert client.recv() == "hello"
        time.sleep(hold)
    finally:
        client.close()
    assert rig.rec.wait_for("shutdown_request"), rig.rec.names()


@pytest.mark.integration
class TestSimpleWebsocketSocketSurface:
    """What simple-websocket 1.1 and flask-sock do with the object in environ['werkzeug.socket']."""

    @pytest.fixture
    def session(self):
        rec = Recorder()
        app = _ws_app(rec, ping_interval=0.2)
        with Rig(app, rec, handler_factory=_ws_handler_factory) as rig:
            _run_ws_session(rig, hold=0.7)
            yield rec

    def test_only_the_methods_session_socket_offers_are_used(self, session, session_cls):
        others = sorted({e[2]["attr"] for e in session.named("sock.other")})
        assert not others, (
            f"simple-websocket now touches socket attribute(s) {others}, which workers._SessionSocket "
            f"does not offer (it fails loudly on purpose); installed: {_versions()}"
        )
        used = {e[0].split(".", 1)[1] for e in session.events if e[0].startswith("sock.")}
        used.discard("other")
        assert {"send", "recv", "close", "fileno"} <= used, (
            f"expected simple-websocket to use send, recv, close and (with a ping interval) fileno; "
            f"saw {sorted(used)}: the library's socket usage changed"
        )
        missing = sorted(n for n in used if not callable(getattr(session_cls, n, None)))
        assert not missing, f"workers._SessionSocket lacks {missing}, which simple-websocket calls"

    def test_the_101_is_written_with_send_so_upgraded_is_set(self, session):
        proxy = session.extra["proxy"]
        assert proxy.first_send is not None and proxy.first_send.startswith(b"HTTP/1.1 101"), (
            f"the first bytes simple-websocket sends are {proxy.first_send!r}, not the 101: "
            "_SessionSocket.upgraded (set on the first write) would be wrong, and a "
            "post-upgrade HTTP response could be written into the websocket stream"
        )

    def test_ws_object_surface_used_by_api_and_end_websocket(self, session):
        assert "route_error" not in session.extra, session.extra.get("route_error")
        ws = session.extra["ws"]
        assert session.extra["mode"] == "werkzeug", (
            "simple_websocket.Server no longer reports mode == 'werkzeug' for a werkzeug "
            "environ: flask-sock then returns [] instead of the response werkzeug expects"
        )
        assert ws.sock is session.extra["proxy"], (
            "ws.sock is no longer the object from environ['werkzeug.socket']: "
            "api._end_websocket looks for force_eof() on it"
        )
        assert isinstance(ws.thread, threading.Thread), "ws.thread is gone (api._end_websocket joins it)"
        assert ws.connected is False

    def test_receive_and_send_semantics_api_relies_on(self, session):
        assert session.extra["idle_receive"] is None, (
            "ws.receive(timeout=...) no longer returns None on an idle timeout (/stream's idle check)"
        )
        exc = session.extra.get("closed_exc")
        assert isinstance(exc, ConnectionClosed), (
            "ws.receive() no longer raises ConnectionClosed when the peer closes "
            "(api.stream's end-of-data path)"
        )
        assert session.extra.get("send_after_close_raised"), (
            "ws.send() after the peer closed no longer raises ConnectionClosed"
        )
        assert session.extra.get("close_after_close_raised"), (
            "ws.close() on a closed session no longer raises ConnectionClosed "
            "(api._end_websocket suppresses it; flask-sock's own close is in a bare except)"
        )

    def test_the_only_http_response_after_the_session_is_werkzeugs_200(self, session):
        """`ApiRequestHandler.send_response` drops the 200 flask-sock hands werkzeug once the
        session is over (code == 200 and a websocket request)."""
        responses = session.named("send_response")
        assert [r[2]["code"] for r in responses] == [200], (
            f"werkzeug's response to a finished websocket session is now "
            f"{[r[2]['code'] for r in responses]}, not a single 200: ApiRequestHandler.send_response "
            "discards a 200 on a websocket request after the 101; anything else would be written "
            "into the websocket stream"
        )
        assert responses[0][2]["upgrade"] == "websocket"


@pytest.mark.unit
class TestSimpleWebsocketSignatures:
    def test_server_constructor(self):
        _assert_signature(
            WsServer.__init__, "simple_websocket.Server.__init__",
            ("self", "environ", "subprotocols", "receive_bytes", "ping_interval", "max_message_size"),
            "flask-sock builds Server(request.environ, **SOCK_SERVER_OPTIONS)",
        )

    @pytest.mark.parametrize("func, label, expected", [
        (simple_ws.Base.send, "simple_websocket.Base.send", ("self", "data")),
        (simple_ws.Base.receive, "simple_websocket.Base.receive", ("self", "timeout")),
        (simple_ws.Base.close, "simple_websocket.Base.close", ("self", "reason", "message")),
    ])
    def test_session_methods_api_calls(self, func, label, expected):
        _assert_signature(func, label, expected, "api.stream and api._end_websocket call it")

    def test_connection_closed_carries_reason_and_message(self):
        exc = ConnectionClosed(1000, "bye")
        assert (exc.reason, exc.message) == (1000, "bye")
        assert isinstance(exc, RuntimeError)


# ---------------------------------------------------------------------------
# requests / urllib3 hooks that identity.pinned_session overrides
# ---------------------------------------------------------------------------


class _Stop(Exception):
    """Raised by a spy to stop `HTTPAdapter.send` before any connection is attempted."""


def _prepared(url="https://127.0.0.1:1/x"):
    return requests.Request("GET", url).prepare()


_HAS_POOL_KEY_HOOK = hasattr(HTTPAdapter, "build_connection_pool_key_attributes")


@pytest.mark.unit
class TestRequestsAdapterSignatures:
    @pytest.mark.parametrize("name, expected, why", [
        ("init_poolmanager", ("self", "connections", "maxsize", "block"),
         "_PinnedAdapter.init_poolmanager injects ssl_context and assert_fingerprint here; "
         "HTTPAdapter.__init__ calls (connections, maxsize, block=...)"),
        ("proxy_manager_for", ("self", "proxy"),
         "_PinnedAdapter.proxy_manager_for keeps the pin through a CONNECT tunnel"),
        ("cert_verify", ("self", "conn", "url", "verify", "cert"),
         "_PinnedAdapter.cert_verify forces verify=False, cert=None (send() calls it positionally)"),
        ("send", ("self", "request", "stream", "timeout", "verify", "cert", "proxies"),
         "_PinnedAdapter.send applies the default timeout and forces verify=False, cert=None"),
        ("close", ("self",), "requests.Session.close() calls adapter.close()"),
    ])
    def test_http_adapter_hook(self, name, expected, why):
        _assert_signature(getattr(HTTPAdapter, name), f"requests.adapters.HTTPAdapter.{name}",
                          expected, why)

    @pytest.mark.skipif(not _HAS_POOL_KEY_HOOK, reason="requests < 2.32 has no pool-key hook "
                        "(the override is then never called, which is harmless: cert_verify still pins)")
    def test_pool_key_hook(self):
        _assert_signature(
            HTTPAdapter.build_connection_pool_key_attributes,
            "requests.adapters.HTTPAdapter.build_connection_pool_key_attributes",
            ("self", "request", "verify", "cert"),
            "_PinnedAdapter.build_connection_pool_key_attributes forces verify=False, cert=None; "
            "without it requests >= 2.32 hands urllib3 its own default ssl_context for verify=True "
            "and the pin context is lost",
        )

    def test_base_adapter_interface_for_the_plaintext_refusal(self):
        _assert_signature(
            BaseAdapter.send, "requests.adapters.BaseAdapter.send",
            ("self", "request", "stream", "timeout", "verify", "cert", "proxies"),
            "_RefusePlaintext.send(request, *args, **kwargs) must accept whatever Session.send passes",
        )
        _assert_signature(BaseAdapter.close, "requests.adapters.BaseAdapter.close", ("self",),
                          "Session.close() closes every mounted adapter")

    def test_session_hooks(self):
        _assert_signature(requests.Session.mount, "requests.Session.mount", ("self", "prefix", "adapter"),
                          "pinned_session mounts the pinned adapter on https:// and a refusal on http://")
        _assert_signature(requests.Session.get_adapter, "requests.Session.get_adapter", ("self", "url"),
                          "requests picks the adapter by URL prefix")


@pytest.mark.unit
class TestRequestsAdapterBehaviour:
    """The call shapes and effects of the hooks, observed on a spy subclass of the real adapter
    (no connection is ever attempted: the spies stop `send` first)."""

    def test_init_poolmanager_is_called_by_the_constructor_with_block_keyword(self):
        calls = []

        class Spy(HTTPAdapter):
            def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
                calls.append((connections, maxsize, block, pool_kwargs))
                super().init_poolmanager(connections, maxsize, block, **pool_kwargs)

        Spy()
        assert len(calls) == 1, (
            "HTTPAdapter.__init__ no longer calls init_poolmanager exactly once: the pinned "
            "adapter would be built without its ssl_context/assert_fingerprint"
        )
        connections, maxsize, block, pool_kwargs = calls[0]
        assert (connections, maxsize) == (10, 10) and block is False and pool_kwargs == {}

    def test_pool_kwargs_reach_the_pool_manager_and_the_connection_pool(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        fingerprint = "ab" * 32

        class Pinned(HTTPAdapter):
            def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
                pool_kwargs.update(ssl_context=ctx, assert_fingerprint=fingerprint)
                super().init_poolmanager(connections, maxsize, block, **pool_kwargs)

        adapter = Pinned()
        kw = adapter.poolmanager.connection_pool_kw
        assert kw.get("ssl_context") is ctx and kw.get("assert_fingerprint") == fingerprint, (
            f"requests/urllib3 no longer store init_poolmanager's kwargs on "
            f"poolmanager.connection_pool_kw (saw {sorted(kw)}); installed: {_versions()}"
        )
        pool = adapter.poolmanager.connection_from_host("127.0.0.1", 1, "https")
        assert pool.assert_fingerprint == fingerprint, (
            "urllib3's HTTPSConnectionPool no longer receives assert_fingerprint from the "
            "pool manager's connection_pool_kw: the server pin would not be enforced"
        )
        assert pool.conn_kw.get("ssl_context") is ctx, (
            "urllib3's HTTPSConnectionPool no longer carries the manager's ssl_context into "
            "its connections: the client certificate would not be presented"
        )

    def test_proxy_manager_accepts_the_pin_kwargs(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        manager = urllib3.proxy_from_url("http://127.0.0.1:9", ssl_context=ctx,
                                         assert_fingerprint="ab" * 32)
        assert manager.connection_pool_kw.get("assert_fingerprint") == "ab" * 32

    def test_send_calls_the_hooks_with_the_verify_and_cert_it_was_given(self):
        """The reason the overrides exist: send() routes `verify`/`cert` through exactly these
        two hooks, so overriding them (and send) is what keeps `CERT_REQUIRED` off."""
        calls = []

        class Spy(HTTPAdapter):
            if _HAS_POOL_KEY_HOOK:
                def build_connection_pool_key_attributes(self, request, verify, cert=None):
                    calls.append(("pool_key", verify, cert))
                    return super().build_connection_pool_key_attributes(request, verify, cert)

            def cert_verify(self, conn, url, verify, cert):
                calls.append(("cert_verify", url, verify, cert))
                raise _Stop

        with pytest.raises(_Stop):
            Spy().send(_prepared(), verify=True, cert=("c.pem", "k.pem"))
        by_name = {c[0]: c for c in calls}
        assert by_name["cert_verify"] == ("cert_verify", "https://127.0.0.1:1/x", True, ("c.pem", "k.pem")), (
            "HTTPAdapter.send no longer calls self.cert_verify(conn, url, verify, cert) with the "
            f"caller's verify/cert (saw {by_name['cert_verify']}); installed: {_versions()}"
        )
        if _HAS_POOL_KEY_HOOK:
            assert by_name["pool_key"] == ("pool_key", True, ("c.pem", "k.pem")), (
                "HTTPAdapter.send no longer passes the caller's verify/cert to "
                f"build_connection_pool_key_attributes (saw {by_name['pool_key']})"
            )

    def test_send_asks_proxy_manager_for_with_the_proxy_url_only(self):
        seen = []

        class Spy(HTTPAdapter):
            def proxy_manager_for(self, proxy, **proxy_kwargs):
                seen.append((proxy, proxy_kwargs))
                raise _Stop

        with pytest.raises(_Stop):
            Spy().send(_prepared(), proxies={"https": "http://127.0.0.1:9"})
        assert seen == [("http://127.0.0.1:9", {})], (
            f"HTTPAdapter no longer calls proxy_manager_for(proxy) with just the proxy URL "
            f"(saw {seen}): the pin's proxy_kwargs injection depends on that shape"
        )

    def test_verify_false_means_cert_none_on_the_connection(self):
        conn = SimpleNamespace(cert_reqs="CERT_REQUIRED", ca_certs="/x", ca_cert_dir="/y")
        HTTPAdapter().cert_verify(conn, "https://127.0.0.1:1/", False, None)
        assert (conn.cert_reqs, conn.ca_certs, conn.ca_cert_dir) == ("CERT_NONE", None, None), (
            "HTTPAdapter.cert_verify(verify=False) no longer sets CERT_NONE and clears the CA "
            "settings: with the pinned context's own verify_mode that is what keeps urllib3 from "
            "re-enabling verification (the pin is the only check)"
        )

    def test_verify_false_pool_key_has_cert_none_and_no_default_ssl_context(self):
        if not _HAS_POOL_KEY_HOOK:
            pytest.skip("requests < 2.32")
        host_params, pool_kwargs = HTTPAdapter().build_connection_pool_key_attributes(
            _prepared(), False, None
        )
        assert host_params["scheme"] == "https" and host_params["host"] == "127.0.0.1"
        assert pool_kwargs.get("cert_reqs") == "CERT_NONE" and "ssl_context" not in pool_kwargs, (
            f"build_connection_pool_key_attributes(verify=False) now returns {pool_kwargs}: "
            "requests may be injecting an ssl_context of its own, which would replace the pinned "
            "client context (see identity.pinned_session)"
        )

    def test_the_pinned_session_overrides_are_what_this_file_describes(self):
        """`pinned_session` really does override the hooks named above (so the signature
        contracts are about the right methods)."""
        session = identity.pinned_session(None, "ab" * 32)
        adapter = session.get_adapter("https://127.0.0.1:1/")
        overridden = {n for n in vars(type(adapter)) if not n.startswith("__")}
        expected = {"init_poolmanager", "proxy_manager_for", "cert_verify", "send"}
        if _HAS_POOL_KEY_HOOK:
            expected.add("build_connection_pool_key_attributes")
        assert expected <= overridden, (
            f"identity.pinned_session's adapter overrides {sorted(overridden)}; the contract tests "
            f"in this file cover {sorted(expected)}: add the missing hook(s) here"
        )
        assert overridden <= expected, (
            f"identity.pinned_session's adapter now also overrides {sorted(overridden - expected)}: "
            "add a signature contract for it in TestRequestsAdapterSignatures"
        )
        kw = adapter.poolmanager.connection_pool_kw
        assert kw.get("assert_fingerprint") == "ab" * 32 and isinstance(kw.get("ssl_context"), ssl.SSLContext)
        session.close()

    def test_the_session_ignores_the_environment_when_trust_env_is_off(self, monkeypatch):
        monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
        monkeypatch.setenv("https_proxy", "http://127.0.0.1:9")
        monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/nonexistent/ca.pem")
        monkeypatch.setenv("CURL_CA_BUNDLE", "/nonexistent/ca.pem")
        session = identity.pinned_session(None, "ab" * 32)
        try:
            settings = session.merge_environment_settings("https://127.0.0.1:1/", {}, None, True, None)
        finally:
            session.close()
        assert settings["proxies"] == {} and settings["verify"] is True, (
            f"Session.merge_environment_settings with trust_env=False returned {settings}: "
            "requests now reads the proxy/CA environment despite trust_env, which would re-route "
            "or re-verify the pinned connection"
        )

    def test_plaintext_http_is_refused_without_touching_the_network(self):
        session = identity.pinned_session(None, "ab" * 32)
        try:
            with pytest.raises(requests.exceptions.InvalidURL, match="plaintext http://"):
                session.get("http://127.0.0.1:1/")
        finally:
            session.close()


@pytest.mark.unit
class TestUrllib3PinMessage:
    def test_a_wrong_fingerprint_raises_ssl_error_with_the_text_is_pin_mismatch_matches(self):
        from urllib3.util import ssl_ as u3_ssl

        with pytest.raises(urllib3.exceptions.SSLError) as info:
            u3_ssl.assert_fingerprint(b"certificate bytes", "ab" * 32)
        assert identity._URLLIB3_PIN_TEXT in str(info.value), (
            f"urllib3's fingerprint mismatch message is now {str(info.value)!r}: "
            "identity.is_pin_mismatch looks for 'Fingerprints did not match'; update "
            "_URLLIB3_PIN_TEXT or the pin mismatch would be reported as a generic TLS failure"
        )
        assert identity.is_pin_mismatch(requests.exceptions.SSLError(info.value))

    def test_fingerprint_forms_urllib3_accepts(self):
        from urllib3.util import ssl_ as u3_ssl

        digest = __import__("hashlib").sha256(b"der").hexdigest()
        u3_ssl.assert_fingerprint(b"der", digest)
        u3_ssl.assert_fingerprint(b"der", ":".join(digest[i:i + 2] for i in range(0, 64, 2)).upper())
        _assert_signature(u3_ssl.assert_fingerprint, "urllib3.util.ssl_.assert_fingerprint",
                          ("cert", "fingerprint"), "the pin check run on every pinned connection")


# ---------------------------------------------------------------------------
# importlib.metadata: what the S-8 version-skew guard reads
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestImportlibMetadataContract:
    """`zfsbackup.version._record_path` takes the dist-info directory from the PRIVATE
    `PathDistribution._path` (one directory listing less than `files`, and it works for a
    `RECORD`-less install) and falls back to the public `Distribution.files`. If the private
    attribute moves, the guard silently degrades to the fallback; if `files` changes too, it
    stats `__init__.py` instead and no longer notices a reinstall."""

    @pytest.fixture
    def dist_info(self, tmp_path, monkeypatch):
        site = tmp_path / "site"
        info = site / "zfsbackup-9.9.9.dist-info"
        info.mkdir(parents=True)
        (info / "METADATA").write_text("Metadata-Version: 2.1\nName: zfsbackup\nVersion: 9.9.9\n")
        (info / "RECORD").write_text("zfsbackup-9.9.9.dist-info/RECORD,,\n")
        monkeypatch.syspath_prepend(str(site))
        importlib.invalidate_caches()
        return info

    def test_a_path_distribution_exposes_its_dist_info_directory_as__path(self, dist_info):
        dist = importlib.metadata.distribution("zfsbackup")
        assert type(dist).__name__ == "PathDistribution"
        assert hasattr(dist, "_path"), (
            "importlib.metadata.PathDistribution no longer has `_path`: "
            "zfsbackup.version._record_path falls back to Distribution.files"
        )
        assert os.fspath(dist._path) == str(dist_info)

    def test_the_private_path_and_the_public_files_agree_on_the_record(self, dist_info):
        dist = importlib.metadata.distribution("zfsbackup")
        records = [f for f in dist.files or () if f.name == "RECORD"]
        assert len(records) == 1, "Distribution.files must list the dist-info RECORD"
        assert records[0].parent.name.endswith(".dist-info")
        assert os.fspath(records[0].locate()) == str(dist_info / "RECORD")
        assert os.fspath(dist._path / "RECORD") == str(dist_info / "RECORD")

    def test_files_is_none_without_a_record_and_the_guard_copes(self, dist_info):
        (dist_info / "RECORD").unlink()
        dist = importlib.metadata.distribution("zfsbackup")
        assert dist.files is None
        from zfsbackup import version

        assert version._record_path(dist) == str(dist_info / "RECORD")        # via `_path`
        assert version._record_path(SimpleNamespace(files=dist.files)) is None

    def test_a_dist_info_added_after_the_first_lookup_is_seen_after_invalidate_caches(
        self, tmp_path, monkeypatch
    ):
        site = tmp_path / "site2"
        site.mkdir()
        monkeypatch.syspath_prepend(str(site))
        importlib.invalidate_caches()
        before = importlib.metadata.version("zfsbackup")
        info = site / "zfsbackup-8.8.8.dist-info"
        info.mkdir()
        (info / "METADATA").write_text("Metadata-Version: 2.1\nName: zfsbackup\nVersion: 8.8.8\n")
        importlib.invalidate_caches()
        assert importlib.metadata.version("zfsbackup") == "8.8.8" != before

    def test_the_version_comes_from_the_same_object_as_the_path(self, dist_info):
        """The guard reads `.version` and `_path` from ONE `distribution()` call."""
        dist = importlib.metadata.distribution("zfsbackup")
        assert dist.version == "9.9.9"
        assert dist._path.name == "zfsbackup-9.9.9.dist-info"

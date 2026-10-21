"""Platform spikes for the TLS / mutual-TLS milestone (plan item M2).

These are *characterization* tests of stdlib ``ssl``, werkzeug, flask-sock /
simple-websocket, websocket-client and requests / urllib3 behaviour that
``docs/tls_mtls_plan.md`` depends on.  They stay in the suite as regression
guards: if an upgrade changes one of these behaviours, the design that rests on
it has to be revisited, not the assertion quietly adjusted.

Scope and hygiene:

* every certificate is generated with ``cryptography`` (ECDSA P-256) into a
  ``tmp_path``-based directory; there is no network, no ``zfs`` and no
  subprocess -- servers bind ``127.0.0.1:0`` only, run on daemon threads, and
  are shut down and joined by their fixtures;
* the server under test pins client certificates the way the plan intends
  (``CERT_OPTIONAL`` + ``VERIFY_X509_PARTIAL_CHAIN`` + the exact leaf PEMs as
  trust anchors) and does the TLS handshake on its own per-connection thread
  (D6), not on werkzeug's accept thread.

Index (the plan's spike numbers):

S0 pinning matrix, S1 expired / not-yet-valid, S2 per-connection context swap
(and the ``sni_callback`` fallback), S3 silent peer vs accept loop and
``shutdown()``, S4 TLS 1.2 refused, S5 ``SSL_CLIENT_CERT`` in the WSGI environ,
S6 concurrent read+write on one wss ``SSLSocket``, S7 ``requests`` pinned to
the server fingerprint, S8 websocket-client over a pre-wrapped ``SSLSocket``,
S9 401/403 on a websocket route, S10 the exception a client sees when its
certificate is rejected under TLS 1.3.
"""

import datetime
import hashlib
import http.client
import json
import os
import socket
import ssl
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, Sequence

import pytest

cryptography = pytest.importorskip("cryptography")
from cryptography import x509  # noqa: E402
from cryptography.hazmat.primitives import hashes, serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import ec  # noqa: E402
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID  # noqa: E402

import requests  # noqa: E402
import websocket  # noqa: E402
from flask import Flask, jsonify, request  # noqa: E402
from flask_sock import Sock  # noqa: E402
from requests.adapters import HTTPAdapter  # noqa: E402
from werkzeug.serving import ThreadedWSGIServer, WSGIRequestHandler  # noqa: E402

pytestmark = pytest.mark.integration

LOOPBACK = "127.0.0.1"
#: Generous per-operation timeout: everything here is loopback, so hitting it
#: means a hang (the thing several spikes exist to rule out), not slowness.
IO_TIMEOUT = 10.0


# --------------------------------------------------------------------------
# PKI helpers
# --------------------------------------------------------------------------

_KEY_CACHE: dict = {}
_KEY_LOCK = threading.Lock()


def _key(name: str) -> ec.EllipticCurvePrivateKey:
    """Session-wide ECDSA P-256 key cache: keygen dominates cert creation."""
    with _KEY_LOCK:
        if name not in _KEY_CACHE:
            _KEY_CACHE[name] = ec.generate_private_key(ec.SECP256R1())
        return _KEY_CACHE[name]


@dataclass
class Ident:
    """One certificate + key, written to disk (``load_cert_chain`` wants files)."""
    name: str
    cert: x509.Certificate
    key: ec.EllipticCurvePrivateKey
    cert_path: Path
    key_path: Path

    @property
    def pem(self) -> str:
        return self.cert.public_bytes(serialization.Encoding.PEM).decode()

    @property
    def der(self) -> bytes:
        return self.cert.public_bytes(serialization.Encoding.DER)

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(self.der).hexdigest()


def _build(directory: Path, name: str, *, issuer: Optional[Ident], not_before,
           not_after, eku: Optional[Sequence], ca: bool = False) -> Ident:
    key = _key(name)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    signing_key = issuer.key if issuer is not None else key
    issuer_name = issuer.cert.subject if issuer is not None else subject
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer_name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
                       critical=False)
    )
    if ca:
        builder = builder.add_extension(
            x509.KeyUsage(digital_signature=True, key_cert_sign=True, crl_sign=True,
                          content_commitment=False, key_encipherment=False,
                          data_encipherment=False, key_agreement=False,
                          encipher_only=False, decipher_only=False),
            critical=True)
    else:
        builder = builder.add_extension(
            x509.KeyUsage(digital_signature=True, content_commitment=False,
                          key_encipherment=False, data_encipherment=False,
                          key_agreement=False, key_cert_sign=False, crl_sign=False,
                          encipher_only=False, decipher_only=False),
            critical=True)
    if issuer is not None:
        builder = builder.add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(issuer.key.public_key()),
            critical=False)
    if eku:
        builder = builder.add_extension(x509.ExtendedKeyUsage(list(eku)), critical=False)
    cert = builder.sign(signing_key, hashes.SHA256())
    ident = Ident(name=name, cert=cert, key=key,
                  cert_path=directory / f"{name}.crt", key_path=directory / f"{name}.key")
    ident.cert_path.write_text(ident.pem)
    ident.key_path.write_bytes(key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption()))
    return ident


@dataclass
class Pki:
    ca: Ident
    server: Ident
    #: self-signed, clientAuth
    client_ss: Ident
    client_ss_other: Ident
    #: issued by ``ca``, clientAuth
    client_ca: Ident
    client_ca_other: Ident
    #: self-signed, EKU serverAuth only (no clientAuth)
    client_eku_server: Ident
    client_expired: Ident
    client_future: Ident


@pytest.fixture(scope="module")
def pki(tmp_path_factory) -> Pki:
    d = tmp_path_factory.mktemp("tls_platform_pki")
    now = datetime.datetime.now(datetime.timezone.utc)
    ok_from = now - datetime.timedelta(minutes=5)
    ok_to = now + datetime.timedelta(days=3650)
    client_auth = [ExtendedKeyUsageOID.CLIENT_AUTH]

    def make(name, *, issuer=None, eku=client_auth, nb=ok_from, na=ok_to, ca=False):
        return _build(d, name, issuer=issuer, not_before=nb, not_after=na, eku=eku, ca=ca)

    ca = make("test-ca", eku=None, ca=True)
    return Pki(
        ca=ca,
        server=make("server", eku=[ExtendedKeyUsageOID.SERVER_AUTH]),
        client_ss=make("client-ss"),
        client_ss_other=make("client-ss-other"),
        client_ca=make("client-ca", issuer=ca),
        client_ca_other=make("client-ca-other", issuer=ca),
        client_eku_server=make("client-eku-server", eku=[ExtendedKeyUsageOID.SERVER_AUTH]),
        client_expired=make("client-expired", nb=now - datetime.timedelta(days=2),
                            na=now - datetime.timedelta(days=1)),
        client_future=make("client-future", nb=now + datetime.timedelta(days=1),
                           na=now + datetime.timedelta(days=2)),
    )


def server_context(server: Ident, pinned: Sequence[Ident], *,
                   minimum: ssl.TLSVersion = ssl.TLSVersion.TLSv1_3) -> ssl.SSLContext:
    """The plan's server context: client certs optional, trust = exactly the pins."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = minimum
    ctx.verify_mode = ssl.CERT_OPTIONAL
    ctx.verify_flags |= ssl.VERIFY_X509_PARTIAL_CHAIN
    ctx.load_cert_chain(str(server.cert_path), str(server.key_path))
    if pinned:
        ctx.load_verify_locations(cadata="".join(p.pem for p in pinned))
    return ctx


def client_context(ident: Optional[Ident] = None, *,
                   maximum: Optional[ssl.TLSVersion] = None) -> ssl.SSLContext:
    """A client that trusts nothing (the plan pins the server by fingerprint)."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    if maximum is not None:
        ctx.maximum_version = maximum
    if ident is not None:
        ctx.load_cert_chain(str(ident.cert_path), str(ident.key_path))
    return ctx


def tls_connect(port: int, ctx: ssl.SSLContext, *, timeout: float = IO_TIMEOUT,
                server_hostname: Optional[str] = None) -> ssl.SSLSocket:
    """TCP connect + client handshake, returning the SSLSocket."""
    raw = socket.create_connection((LOOPBACK, port), timeout=timeout)
    try:
        return ctx.wrap_socket(raw, server_hostname=server_hostname)
    except BaseException:
        raw.close()
        raise


# --------------------------------------------------------------------------
# Servers
# --------------------------------------------------------------------------

@dataclass
class Event:
    ok: bool
    peer_der: Optional[bytes] = None
    error: Optional[BaseException] = None


class _Events:
    """Server-side handshake log with a blocking wait (TLS 1.3 clients can see
    the alert before the server thread has recorded the failure)."""

    def __init__(self):
        self._cond = threading.Condition()
        self._items: List[Event] = []

    def add(self, ev: Event) -> None:
        with self._cond:
            self._items.append(ev)
            self._cond.notify_all()

    def wait_for(self, n: int, timeout: float = IO_TIMEOUT) -> List[Event]:
        deadline = time.monotonic() + timeout
        with self._cond:
            while len(self._items) < n:
                left = deadline - time.monotonic()
                if left <= 0:
                    raise AssertionError(f"only {len(self._items)} handshakes logged, wanted {n}")
                self._cond.wait(left)
            return list(self._items)

    def snapshot(self) -> List[Event]:
        with self._cond:
            return list(self._items)


class BareTlsServer:
    """Sequential TLS echo-ish server on bare sockets: handshake, record the
    outcome, send ``b"ok"``, close.  Used for the pure-``ssl`` spikes."""

    def __init__(self, context_provider: Callable[[], ssl.SSLContext],
                 handshake_timeout: float = IO_TIMEOUT):
        self._provider = context_provider
        self._timeout = handshake_timeout
        self.events = _Events()
        self._listener = socket.socket()
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind((LOOPBACK, 0))
        self._listener.listen(8)
        self._listener.settimeout(0.1)
        self.port = self._listener.getsockname()[1]
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="bare-tls-server")
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                raw, _ = self._listener.accept()
            except (socket.timeout, TimeoutError):
                continue
            except OSError:
                return
            try:
                tls = self._provider().wrap_socket(raw, server_side=True,
                                                   do_handshake_on_connect=False)
            except OSError as exc:
                raw.close()
                self.events.add(Event(False, error=exc))
                continue
            try:
                tls.settimeout(self._timeout)
                tls.do_handshake()
                self.events.add(Event(True, peer_der=tls.getpeercert(binary_form=True)))
                tls.sendall(b"ok")
            except (OSError, ssl.SSLError) as exc:
                self.events.add(Event(False, error=exc))
            finally:
                try:
                    tls.close()
                except OSError:
                    pass

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self._listener.close()


@contextmanager
def bare_server(provider):
    srv = BareTlsServer(provider)
    try:
        yield srv
    finally:
        srv.close()


def probe(srv: BareTlsServer, ctx: ssl.SSLContext, n: int = 1):
    """One client connection.  Returns ``(client_exc_or_None, server_event)``.

    A TLS 1.3 client's ``do_handshake`` completes before the server has
    verified the client certificate, so a rejection surfaces on the first read;
    the client therefore always reads the server's ``b"ok"``.
    """
    client_exc: Optional[BaseException] = None
    try:
        with tls_connect(srv.port, ctx) as tls:
            tls.settimeout(IO_TIMEOUT)
            data = tls.recv(2)
            assert data == b"ok" or data == b""
            if data == b"":
                client_exc = ConnectionAbortedError("EOF without ok")
    except (OSError, ssl.SSLError) as exc:
        client_exc = exc
    ev = srv.events.wait_for(n)[n - 1]
    return client_exc, ev


class _Handler(WSGIRequestHandler):
    # Defined on the subclass so werkzeug does not mutate the global default.
    protocol_version = "HTTP/1.1"

    def log_message(self, *args, **kwargs):  # silence stderr
        pass


class PerConnTlsServer(ThreadedWSGIServer):
    """A ``ThreadedWSGIServer`` that does the TLS handshake itself, per
    connection, on the connection's own thread (D6).

    Hook chosen: ``process_request_thread``.  ``socketserver`` calls it on the
    new thread with the raw accepted socket; it is the one place that is (a)
    already off the accept loop, (b) runs *before* ``finish_request`` builds the
    ``rfile``/``wfile`` (so the request handler sees only the ``SSLSocket``),
    and (c) owns the ``shutdown_request`` in its ``finally``.  ``get_request``
    would handshake on the accept thread; ``process_request`` would still be on
    it too.  The listening socket is never wrapped (no ``ssl_context`` passed
    to the constructor); ``self.ssl_context`` is assigned afterwards purely so
    werkzeug reports ``https`` and fills ``SSL_CLIENT_CERT``.
    """

    def __init__(self, app, context_provider: Callable[[], ssl.SSLContext], *,
                 handshake_timeout: float = IO_TIMEOUT):
        super().__init__(LOOPBACK, 0, app, handler=_Handler)
        assert self.socket.__class__ is socket.socket  # not wrapped by werkzeug
        self.context_provider = context_provider
        self.handshake_timeout = handshake_timeout
        self.events = _Events()
        self.ssl_context = context_provider()

    def process_request_thread(self, request, client_address):
        try:
            tls = self.context_provider().wrap_socket(
                request, server_side=True, do_handshake_on_connect=False)
        except OSError as exc:
            self.events.add(Event(False, error=exc))
            self.shutdown_request(request)
            return
        try:
            tls.settimeout(self.handshake_timeout)
            tls.do_handshake()
            tls.settimeout(None)
        except (OSError, ssl.SSLError) as exc:  # TimeoutError is an OSError
            self.events.add(Event(False, error=exc))
            try:
                tls.close()
            except OSError:
                pass
            return
        self.events.add(Event(True, peer_der=tls.getpeercert(binary_form=True)))
        super().process_request_thread(tls, client_address)


class _Running:
    def __init__(self, server: ThreadedWSGIServer, app: Flask):
        self.server = server
        self.app = app
        self.port = server.port
        self.thread = threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.05},
            daemon=True, name="tls-platform-wsgi")
        self.thread.start()

    @property
    def events(self) -> _Events:
        return self.server.events

    def stop(self) -> None:
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.server.server_close()


@contextmanager
def running(server, app):
    handle = _Running(server, app)
    try:
        yield handle
    finally:
        handle.stop()


# --------------------------------------------------------------------------
# The Flask app under test
# --------------------------------------------------------------------------

class Holder:
    """Swappable pinned set -> context, the thing S2 swaps between connections."""

    def __init__(self, pki: Pki, pinned: Sequence[Ident], **kw):
        self.pki = pki
        self._kw = kw
        self.swap(pinned)

    def swap(self, pinned: Sequence[Ident]) -> None:
        self.ctx = server_context(self.pki.server, pinned, **self._kw)

    def __call__(self) -> ssl.SSLContext:
        return self.ctx


STREAM_CHUNK = 64 * 1024


def make_app() -> Flask:
    app = Flask(__name__)
    app.config["PROPAGATE_EXCEPTIONS"] = True
    sock = Sock(app)
    app.request_count = 0
    lock = threading.Lock()

    @app.before_request
    def _count_and_gate():
        with lock:
            app.request_count += 1
        if request.path.startswith("/ws/denied/"):
            code = int(request.path.rsplit("/", 1)[1])
            return f"denied {code}", code
        return None

    @app.route("/cert")
    def cert():
        return jsonify(cert=request.environ.get("SSL_CLIENT_CERT"), scheme=request.scheme)

    @sock.route("/ws/cert")
    def ws_cert(ws):
        ws.send(json.dumps({"cert": ws.environ.get("SSL_CLIENT_CERT"),
                            "scheme": ws.environ.get("wsgi.url_scheme")}))

    @sock.route("/ws/echo")
    def ws_echo(ws):
        while True:
            msg = ws.receive(timeout=IO_TIMEOUT)
            if msg is None:
                return
            ws.send(f"echo:{msg}")

    @sock.route("/ws/denied/<int:code>")
    def ws_denied(ws, code):  # pragma: no cover - before_request always answers
        ws.send("should not get here")

    @sock.route("/ws/stream")
    def ws_stream(ws):
        """Upload sink with a concurrent writer.

        simple_websocket's background thread reads the SSLSocket; this handler
        thread writes to the *same* SSLSocket while that read is in flight:
        a small ack every 16 chunks, a 512 KiB server->client burst half way
        (the control text ``START`` tells us the planned chunk count), then the
        final digests.
        """
        digest = hashlib.sha256()
        total = chunks = 0
        planned = None
        out = hashlib.sha256()
        burst_done = False
        while True:
            msg = ws.receive(timeout=IO_TIMEOUT)
            if msg is None:
                return                                   # client stalled: bail out
            if isinstance(msg, str):
                if msg.startswith("START "):
                    planned = int(msg.split()[1])
                    continue
                break                                    # "DONE"
            digest.update(msg)
            total += len(msg)
            chunks += 1
            if chunks % 16 == 0:
                ws.send(f"ack {chunks}")
            if planned and not burst_done and chunks >= planned // 2:
                burst_done = True
                for i in range(16):
                    piece = bytes([i]) * (32 * 1024)
                    out.update(piece)
                    ws.send(piece)
        ws.send(json.dumps({"final": True, "received_bytes": total, "chunks": chunks,
                            "received_sha256": digest.hexdigest(),
                            "sent_sha256": out.hexdigest()}))

    return app


@pytest.fixture
def app() -> Flask:
    return make_app()


@contextmanager
def serve(app, provider, **kw):
    server = PerConnTlsServer(app, provider, **kw)
    with running(server, app) as handle:
        yield handle


def https_get(port: int, path: str, ctx: ssl.SSLContext, *, host: str = LOOPBACK,
              timeout: float = IO_TIMEOUT):
    conn = http.client.HTTPSConnection(host, port, context=ctx, timeout=timeout)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def try_get(port, path, ctx, **kw):
    """``("ok", status, body)`` or ``("err", exception)``."""
    try:
        status, body = https_get(port, path, ctx, **kw)
        return ("ok", status, body)
    except (OSError, http.client.HTTPException) as exc:
        return ("err", exc)


# ==========================================================================
# S0 -- the pinning matrix (re-assert of the manual spike)
# ==========================================================================

class TestS0PinningMatrix:
    """CERT_OPTIONAL + PARTIAL_CHAIN + exact pinned leaves as trust anchors."""

    @pytest.mark.parametrize("client, pinned, accepted", [
        ("client_ss", ["client_ss"], True),                      # pinned self-signed
        ("client_ca", ["client_ca"], True),                      # pinned CA-issued leaf
        ("client_ss_other", ["client_ss"], False),               # unpinned self-signed
        ("client_ca_other", ["client_ca"], False),               # same CA, not pinned
        ("client_ss", [], False),                                # nothing pinned at all
        ("client_eku_server", ["client_eku_server"], False),     # pinned, EKU lacks clientAuth
    ])
    def test_matrix(self, pki, client, pinned, accepted):
        pins = [getattr(pki, n) for n in pinned]
        ident = getattr(pki, client)
        with bare_server(lambda: server_context(pki.server, pins)) as srv:
            client_exc, ev = probe(srv, client_context(ident))
        if accepted:
            assert ev.ok and client_exc is None
            assert ev.peer_der == ident.der
        else:
            assert not ev.ok
            assert isinstance(ev.error, ssl.SSLError)
            assert client_exc is not None   # the client learns of it (see S10)

    def test_no_client_certificate_completes_the_handshake(self, pki):
        with bare_server(lambda: server_context(pki.server, [pki.client_ss])) as srv:
            client_exc, ev = probe(srv, client_context(None))
        assert client_exc is None
        assert ev.ok
        assert ev.peer_der is None            # the application layer decides (401/403)

    def test_pinned_ca_issued_leaf_does_not_trust_the_ca(self, pki):
        """Pinning one leaf must not make the issuing CA a trust anchor."""
        with bare_server(lambda: server_context(pki.server, [pki.client_ca])) as srv:
            _, ev = probe(srv, client_context(pki.client_ca_other))
        assert not ev.ok


# ==========================================================================
# S1 -- validity window is enforced for pinned certificates
# ==========================================================================

class TestS1Validity:
    @pytest.mark.parametrize("which, needle", [
        ("client_expired", "expired"),
        ("client_future", "not yet valid"),
    ])
    def test_pinned_but_out_of_validity_is_rejected(self, pki, which, needle):
        ident = getattr(pki, which)
        with bare_server(lambda: server_context(pki.server, [ident])) as srv:
            client_exc, ev = probe(srv, client_context(ident))
        assert not ev.ok
        assert isinstance(ev.error, ssl.SSLError)
        assert needle in str(ev.error).lower()
        assert client_exc is not None


# ==========================================================================
# S2 -- per-connection wrapping with a swappable context (D6)
# ==========================================================================

class TestS2ContextSwap:
    def test_swap_between_connections_without_restart(self, pki, app):
        holder = Holder(pki, [pki.client_ss])
        with serve(app, holder) as srv:
            a, b = client_context(pki.client_ss), client_context(pki.client_ca)

            assert try_get(srv.port, "/cert", a)[0] == "ok"
            assert try_get(srv.port, "/cert", b)[0] == "err"           # not pinned yet

            holder.swap([pki.client_ca])                                # revoke ss, pin ca
            assert try_get(srv.port, "/cert", a)[0] == "err"           # revoked
            kind, status, body = try_get(srv.port, "/cert", b)         # newly pinned
            assert (kind, status) == ("ok", 200)
            assert json.loads(body)["cert"] == pki.client_ca.pem

            holder.swap([pki.client_ss, pki.client_ca])                 # and back, same server
            assert try_get(srv.port, "/cert", a)[0] == "ok"
        # 3 accepted + 2 rejected handshakes, all logged by the server
        events = srv.events.snapshot()
        assert [e.ok for e in events] == [True, False, False, True, True]

    def test_environ_reports_https_and_client_cert(self, pki, app):
        with serve(app, Holder(pki, [pki.client_ss])) as srv:
            kind, status, body = try_get(srv.port, "/cert", client_context(pki.client_ss))
        assert (kind, status) == ("ok", 200)
        payload = json.loads(body)
        assert payload["scheme"] == "https"
        assert payload["cert"] == pki.client_ss.pem        # ssl.DER_cert_to_PEM_cert form

    def test_listening_socket_is_not_wrapped(self, pki, app):
        server = PerConnTlsServer(app, Holder(pki, []))
        try:
            assert type(server.socket) is socket.socket
            assert server.ssl_context is not None          # only for werkzeug's environ
        finally:
            server.server_close()

    def test_sni_callback_variant_works_inside_werkzeug(self, pki, app):
        """Fallback (record only): werkzeug wraps the listening socket with a
        *front* context whose ``sni_callback`` swaps ``sslobj.context`` per
        connection.  Handshakes then run on werkzeug's accept thread (see S3b),
        but the swap does take effect, including the verify store."""
        holder = Holder(pki, [pki.client_ss])
        front = server_context(pki.server, [])             # trusts nothing by itself

        def sni(sslobj, server_name, _ctx):
            sslobj.context = holder()

        front.sni_callback = sni
        server = ThreadedWSGIServer(LOOPBACK, 0, app, handler=_Handler, ssl_context=front)
        server.events = _Events()  # unused, keeps _Running happy
        with running(server, app) as srv:
            a, b = client_context(pki.client_ss), client_context(pki.client_ca)
            # an IP literal sends no SNI; use a name that resolves to loopback
            get = lambda ctx: try_get(srv.port, "/cert", ctx, host="localhost")  # noqa: E731
            assert get(a)[0] == "ok"
            assert get(b)[0] == "err"
            holder.swap([pki.client_ca])
            assert get(a)[0] == "err"
            kind, status, body = get(b)
            assert (kind, status) == ("ok", 200)
            assert json.loads(body)["cert"] == pki.client_ca.pem


# ==========================================================================
# S3 -- a silent peer must not stall the accept loop or shutdown()
# ==========================================================================

class TestS3SilentPeer:
    def test_silent_peer_blocks_nobody_and_shutdown_is_prompt(self, pki, app):
        silent = None
        with serve(app, Holder(pki, [pki.client_ss]), handshake_timeout=30) as srv:
            try:
                silent = socket.create_connection((LOOPBACK, srv.port), timeout=IO_TIMEOUT)
                # give the server time to accept it and park in do_handshake()
                time.sleep(0.3)
                t0 = time.monotonic()
                kind, status, _ = try_get(srv.port, "/cert", client_context(pki.client_ss),
                                          timeout=3)
                assert (kind, status) == ("ok", 200)
                assert time.monotonic() - t0 < 2.0

                t0 = time.monotonic()
                srv.server.shutdown()                       # silent peer still connected
                elapsed = time.monotonic() - t0
                assert elapsed < 2.0, f"shutdown() took {elapsed:.2f}s with a silent peer"
            finally:
                if silent is not None:
                    silent.close()

    def test_silent_peer_is_dropped_after_the_handshake_timeout(self, pki, app):
        with serve(app, Holder(pki, [pki.client_ss]), handshake_timeout=0.5) as srv:
            silent = socket.create_connection((LOOPBACK, srv.port), timeout=IO_TIMEOUT)
            try:
                ev = srv.events.wait_for(1, timeout=5)[0]
                assert not ev.ok
                assert isinstance(ev.error, (TimeoutError, socket.timeout))
                silent.settimeout(IO_TIMEOUT)
                assert silent.recv(1) == b""                # server closed its end
            finally:
                silent.close()

    def test_stock_werkzeug_wrapping_stalls_on_a_silent_peer(self, pki, app):
        """Why D6 exists: with werkzeug's own ``ssl_context`` the handshake runs
        inside ``accept()`` on the serving thread, so one silent peer stalls
        every later client."""
        ctx = server_context(pki.server, [pki.client_ss])
        server = ThreadedWSGIServer(LOOPBACK, 0, app, handler=_Handler, ssl_context=ctx)
        server.events = _Events()
        silent = None
        with running(server, app) as srv:
            try:
                silent = socket.create_connection((LOOPBACK, srv.port), timeout=IO_TIMEOUT)
                time.sleep(0.3)
                kind, exc = try_get(srv.port, "/cert", client_context(pki.client_ss),
                                    timeout=1.5)
                assert kind == "err" and isinstance(exc, (TimeoutError, socket.timeout))
            finally:
                if silent is not None:
                    silent.close()                          # frees the accept thread


# ==========================================================================
# S4 -- TLS 1.2 refused when the server's minimum is 1.3
# ==========================================================================

class TestS4MinimumVersion:
    def test_tls12_client_is_refused(self, pki):
        with bare_server(lambda: server_context(pki.server, [pki.client_ss])) as srv:
            with pytest.raises(ssl.SSLError) as info:
                tls_connect(srv.port,
                            client_context(pki.client_ss, maximum=ssl.TLSVersion.TLSv1_2))
            ev = srv.events.wait_for(1)[0]
        assert not ev.ok and isinstance(ev.error, ssl.SSLError)
        text = (str(info.value) + str(ev.error)).lower()
        assert "version" in text or "protocol" in text

    def test_tls13_client_negotiates_tls13(self, pki):
        with bare_server(lambda: server_context(pki.server, [pki.client_ss])) as srv:
            with tls_connect(srv.port, client_context(pki.client_ss)) as tls:
                assert tls.version() == "TLSv1.3"
                tls.settimeout(IO_TIMEOUT)
                assert tls.recv(2) == b"ok"


# ==========================================================================
# S5 -- SSL_CLIENT_CERT in the environ (plain route and websocket route)
# ==========================================================================

class TestS5ClientCertEnviron:
    def test_present_on_plain_route(self, pki, app):
        with serve(app, Holder(pki, [pki.client_ss])) as srv:
            _, status, body = try_get(srv.port, "/cert", client_context(pki.client_ss))
        assert status == 200
        assert json.loads(body)["cert"] == pki.client_ss.pem

    def test_present_on_websocket_route(self, pki, app):
        with serve(app, Holder(pki, [pki.client_ss])) as srv:
            tls = tls_connect(srv.port, client_context(pki.client_ss))
            ws = websocket.WebSocket()
            try:
                ws.connect(f"wss://{LOOPBACK}:{srv.port}/ws/cert", socket=tls)
                payload = json.loads(ws.recv())
            finally:
                ws.close()
        assert payload["cert"] == pki.client_ss.pem
        assert payload["scheme"] == "https"

    def test_absent_without_a_client_certificate(self, pki, app):
        with serve(app, Holder(pki, [pki.client_ss])) as srv:
            ctx = client_context(None)                      # handshake still completes
            kind, status, body = try_get(srv.port, "/cert", ctx)
            tls = tls_connect(srv.port, ctx)
            ws = websocket.WebSocket()
            try:
                ws.connect(f"wss://{LOOPBACK}:{srv.port}/ws/cert", socket=tls)
                ws_payload = json.loads(ws.recv())
            finally:
                ws.close()
        assert (kind, status) == ("ok", 200)
        assert json.loads(body)["cert"] is None
        assert ws_payload["cert"] is None
        assert [e.ok for e in srv.events.snapshot()] == [True, True]
        assert all(e.peer_der is None for e in srv.events.snapshot())


# ==========================================================================
# S6 -- one wss SSLSocket, data in both directions at once
# ==========================================================================

def _run_stream(port: int, ctx: ssl.SSLContext, size: int) -> dict:
    """Upload ``size`` random bytes in 64 KiB binary messages while the server
    writes acks/bursts on the same connection; return the verdict."""
    tls = tls_connect(port, ctx)
    tls.settimeout(IO_TIMEOUT)
    ws = websocket.WebSocket()
    sent = hashlib.sha256()
    chunks = max(1, size // STREAM_CHUNK)
    try:
        ws.connect(f"wss://{LOOPBACK}:{port}/ws/stream", socket=tls)
        ws.settimeout(IO_TIMEOUT)
        ws.send(f"START {chunks}")
        for _ in range(chunks):
            chunk = os.urandom(STREAM_CHUNK)
            sent.update(chunk)
            ws.send_binary(chunk)
        ws.send("DONE")
        burst = hashlib.sha256()
        burst_bytes = acks = 0
        final = None
        while final is None:
            msg = ws.recv()
            if isinstance(msg, bytes):
                burst.update(msg)
                burst_bytes += len(msg)
            elif msg.startswith("ack "):
                acks += 1
            else:
                final = json.loads(msg)
    finally:
        ws.close()
    return {"sent_sha256": sent.hexdigest(), "chunks": chunks, "burst_sha256": burst.hexdigest(),
            "burst_bytes": burst_bytes, "acks": acks, "final": final}


def _check_stream(res: dict) -> None:
    final = res["final"]
    assert final["chunks"] == res["chunks"]
    assert final["received_bytes"] == res["chunks"] * STREAM_CHUNK
    assert final["received_sha256"] == res["sent_sha256"], "upload corrupted"
    assert res["burst_bytes"] == 16 * 32 * 1024, "server->client burst truncated"
    assert final["sent_sha256"] == res["burst_sha256"], "server->client burst corrupted"
    assert res["acks"] == res["chunks"] // 16


class TestS6DuplexWss:
    def _run(self, pki, app, sizes):
        with serve(app, Holder(pki, [pki.client_ss])) as srv:
            ctx = client_context(pki.client_ss)
            for size in sizes:
                _check_stream(_run_stream(srv.port, ctx, size))

    def test_duplex_wss_small(self, pki, app):
        # 4 runs x 4 MiB = 16 MiB through one connection each
        self._run(pki, app, [4 * 1024 * 1024] * 4)

    @pytest.mark.slow
    def test_duplex_wss_64_mib(self, pki, app):
        # the plan's target: 64 MiB total across several runs
        self._run(pki, app, [16 * 1024 * 1024] * 4)


# ==========================================================================
# S7 -- requests pinned to the server fingerprint
# ==========================================================================

class PinnedAdapter(HTTPAdapter):
    """The recipe: a CERT_NONE context carrying the client certificate, plus
    urllib3's ``assert_fingerprint`` (checked right after the handshake, before
    the first HTTP byte).

    ``cert_verify`` is overridden because urllib3 does
    ``context.verify_mode = resolve_cert_reqs(cert_reqs)`` on EVERY connection,
    with ``cert_reqs`` supplied by requests from ``verify``: the context alone
    does not stay CERT_NONE when the session says ``verify=True`` (the default)
    -- see ``test_default_verify_overrides_the_context_without_the_override``.
    """

    def __init__(self, ssl_context, fingerprint, *, force_no_verify=True, **kw):
        self._ssl_context = ssl_context
        self._fingerprint = fingerprint
        self._force_no_verify = force_no_verify
        super().__init__(**kw)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        super().init_poolmanager(connections, maxsize, block,
                                 ssl_context=self._ssl_context,
                                 assert_fingerprint=self._fingerprint, **pool_kwargs)

    def cert_verify(self, conn, url, verify, cert):
        super().cert_verify(conn, url, False if self._force_no_verify else verify, cert)


def pinned_session(ctx, fingerprint, **adapter_kw) -> requests.Session:
    s = requests.Session()
    s.mount("https://", PinnedAdapter(ctx, fingerprint, **adapter_kw))
    return s


class TestS7RequestsPinning:
    def test_right_fingerprint_passes_and_presents_the_client_cert(self, pki, app):
        with serve(app, Holder(pki, [pki.client_ss])) as srv:
            s = pinned_session(client_context(pki.client_ss), pki.server.fingerprint)
            try:
                r = s.get(f"https://{LOOPBACK}:{srv.port}/cert", timeout=IO_TIMEOUT)
            finally:
                s.close()
        assert r.status_code == 200
        assert r.json()["cert"] == pki.client_ss.pem
        assert app.request_count == 1

    def test_fingerprint_accepts_colon_and_upper_case_forms(self, pki, app):
        fp = pki.server.fingerprint.upper()
        colon = ":".join(fp[i:i + 2] for i in range(0, len(fp), 2))
        with serve(app, Holder(pki, [pki.client_ss])) as srv:
            s = pinned_session(client_context(pki.client_ss), colon)
            try:
                assert s.get(f"https://{LOOPBACK}:{srv.port}/cert",
                             timeout=IO_TIMEOUT).status_code == 200
            finally:
                s.close()

    def test_wrong_fingerprint_fails_before_any_http_request(self, pki, app):
        wrong = hashlib.sha256(b"not the server").hexdigest()
        with serve(app, Holder(pki, [pki.client_ss])) as srv:
            s = pinned_session(client_context(pki.client_ss), wrong)
            try:
                with pytest.raises(requests.exceptions.SSLError) as info:
                    s.get(f"https://{LOOPBACK}:{srv.port}/cert", timeout=IO_TIMEOUT)
            finally:
                s.close()
            # the handshake itself succeeded (the client cert was accepted) ...
            assert srv.events.wait_for(1)[0].ok
        assert "fingerprint" in str(info.value).lower()
        # ... and the server never saw a single HTTP request
        assert app.request_count == 0

    @pytest.mark.parametrize("verify", [True, False])
    def test_session_verify_does_not_override_the_pin(self, pki, app, verify):
        """With the ``cert_verify`` override, neither ``verify=True`` (certifi)
        nor ``verify=False`` on the session/request changes the outcome."""
        wrong = hashlib.sha256(b"nope").hexdigest()
        with serve(app, Holder(pki, [pki.client_ss])) as srv:
            url = f"https://{LOOPBACK}:{srv.port}/cert"
            good = pinned_session(client_context(pki.client_ss), pki.server.fingerprint)
            bad = pinned_session(client_context(pki.client_ss), wrong)
            try:
                good.verify = bad.verify = verify
                assert good.get(url, timeout=IO_TIMEOUT).status_code == 200
                before = app.request_count
                with pytest.raises(requests.exceptions.SSLError):
                    bad.get(url, timeout=IO_TIMEOUT)
                assert app.request_count == before           # still no request seen
                # per-request verify wins over the session value in requests;
                # the adapter must neutralise that too
                assert good.get(url, verify=verify, timeout=IO_TIMEOUT).status_code == 200
            finally:
                good.close()
                bad.close()

    def test_certifi_bundle_from_the_environment_does_not_override(self, pki, app, monkeypatch):
        import certifi
        monkeypatch.setenv("REQUESTS_CA_BUNDLE", certifi.where())
        monkeypatch.setenv("CURL_CA_BUNDLE", certifi.where())
        with serve(app, Holder(pki, [pki.client_ss])) as srv:
            s = pinned_session(client_context(pki.client_ss), pki.server.fingerprint)
            try:
                assert s.get(f"https://{LOOPBACK}:{srv.port}/cert",
                             timeout=IO_TIMEOUT).status_code == 200
            finally:
                s.close()

    def test_default_verify_overrides_the_context_without_the_override(self, pki, app):
        """Record the hazard the override exists for: a bare adapter (context +
        fingerprint, no ``cert_verify`` override) under the default
        ``verify=True`` has its CERT_NONE context overwritten by urllib3 with
        CERT_REQUIRED + certifi, so the self-signed server is refused."""
        with serve(app, Holder(pki, [pki.client_ss])) as srv:
            s = pinned_session(client_context(pki.client_ss), pki.server.fingerprint,
                               force_no_verify=False)
            try:
                with pytest.raises(requests.exceptions.SSLError) as info:
                    s.get(f"https://{LOOPBACK}:{srv.port}/cert", timeout=IO_TIMEOUT)
            finally:
                s.close()
        assert "certificate verify failed" in str(info.value).lower()
        assert app.request_count == 0

    def test_bare_adapter_with_verify_false_also_works(self, pki, app):
        """The simpler recipe: no override, but ``session.verify = False``."""
        with serve(app, Holder(pki, [pki.client_ss])) as srv:
            s = pinned_session(client_context(pki.client_ss), pki.server.fingerprint,
                               force_no_verify=False)
            s.verify = False
            try:
                assert s.get(f"https://{LOOPBACK}:{srv.port}/cert",
                             timeout=IO_TIMEOUT).status_code == 200
            finally:
                s.close()


# ==========================================================================
# S8 -- websocket-client over an already wrapped SSLSocket
# ==========================================================================

class TestS8WebsocketPreWrappedSocket:
    def test_socket_is_used_as_is_and_not_wrapped_again(self, pki, app):
        with serve(app, Holder(pki, [pki.client_ss])) as srv:
            tls = tls_connect(srv.port, client_context(pki.client_ss))
            assert tls.getpeercert(binary_form=True) == pki.server.der   # fingerprint check point
            ws = websocket.WebSocket()
            try:
                ws.connect(f"wss://{LOOPBACK}:{srv.port}/ws/echo", socket=tls)
                assert ws.sock is tls                      # same object, no second wrap
                assert isinstance(ws.sock, ssl.SSLSocket)
                assert not isinstance(ws.sock.context, type(None))
                ws.send("hello")
                assert ws.recv() == "echo:hello"
                ws.send("again")
                assert ws.recv() == "echo:again"
            finally:
                ws.close()
            events = srv.events.snapshot()
        assert len(events) == 1 and events[0].ok          # exactly one TLS handshake
        assert events[0].peer_der == pki.client_ss.der

    def test_client_without_ca_trust_and_hostname_mismatch_still_connects(self, pki, app):
        """The URL host is not used for TLS when ``socket=`` is given, so no
        hostname / CA machinery of websocket-client's own sslopt is involved."""
        with serve(app, Holder(pki, [pki.client_ss])) as srv:
            tls = tls_connect(srv.port, client_context(pki.client_ss))
            ws = websocket.WebSocket(sslopt={"cert_reqs": ssl.CERT_REQUIRED,
                                             "check_hostname": True})
            try:
                ws.connect(f"wss://{LOOPBACK}:{srv.port}/ws/echo", socket=tls)
                ws.send("x")
                assert ws.recv() == "echo:x"
            finally:
                ws.close()


# ==========================================================================
# S9 -- before_request 401/403 on a websocket route
# ==========================================================================

class TestS9WebsocketDenied:
    @pytest.mark.parametrize("code", [401, 403])
    def test_before_request_status_reaches_websocket_client(self, pki, app, code):
        with serve(app, Holder(pki, [pki.client_ss])) as srv:
            tls = tls_connect(srv.port, client_context(pki.client_ss))
            ws = websocket.WebSocket()
            try:
                with pytest.raises(websocket.WebSocketBadStatusException) as info:
                    ws.connect(f"wss://{LOOPBACK}:{srv.port}/ws/denied/{code}", socket=tls)
            finally:
                ws.close()
        assert info.value.status_code == code


# ==========================================================================
# S10 -- what a TLS 1.3 client sees when the server rejects its certificate
# ==========================================================================
#
# For M11's error mapping.  Under TLS 1.3 the client's handshake *completes*
# before the server has verified the client certificate, so a rejection is NOT
# a connect-time error: the server's fatal alert arrives on the client's first
# read after the handshake (the server side logs an ``ssl.SSLCertVerificationError``).
# What each client stack raises, observed (OpenSSL 4.0 spelling of the reasons;
# older OpenSSL says ``SSLV3_ALERT_*``/``TLSV1_ALERT_*``, hence the sets):
#
# * raw ``ssl``: ``do_handshake()`` returns normally; the first send/recv raises
#   plain ``ssl.SSLError`` (not ``SSLCertVerificationError``) whose ``reason`` is
#   the alert: unpinned -> unknown_ca, expired -> certificate_expired, EKU
#   lacking clientAuth -> unsupported_certificate;
# * ``requests`` (S7 adapter): ``requests.exceptions.SSLError`` wrapping
#   urllib3's ``MaxRetryError``; the alert text is in ``str(exc)``;
# * websocket-client over a pre-wrapped socket (S8): the raw ``ssl.SSLError``
#   escapes ``connect()`` un-wrapped (it is not a ``WebSocketException``).
#
# So M11 should classify "my client certificate was refused" as: ssl.SSLError
# (any of the above) whose text/reason contains ``ALERT``, and must not rely on
# it appearing at connect time.  No HTTP request reaches the server in any case.

ALERT_REASONS = {
    "unpinned": {"TLSV1_ALERT_UNKNOWN_CA", "TLS_ALERT_UNKNOWN_CA"},
    "expired": {"SSLV3_ALERT_CERTIFICATE_EXPIRED", "TLS_ALERT_CERTIFICATE_EXPIRED"},
    "eku": {"TLS_ALERT_UNSUPPORTED_CERTIFICATE", "SSLV3_ALERT_UNSUPPORTED_CERTIFICATE"},
}


def _rejection_case(pki, kind):
    """``(client identity, pinned set)`` that the server must refuse."""
    return {
        "unpinned": (pki.client_ss_other, [pki.client_ss]),
        "expired": (pki.client_expired, [pki.client_expired]),
        "eku": (pki.client_eku_server, [pki.client_eku_server]),
    }[kind]


@pytest.mark.parametrize("kind", ["unpinned", "expired", "eku"])
class TestS10RejectedCertificateErrors:
    def test_raw_ssl_handshake_succeeds_then_first_io_fails(self, pki, app, kind):
        ident, pinned = _rejection_case(pki, kind)
        with serve(app, Holder(pki, pinned)) as srv:
            tls = tls_connect(srv.port, client_context(ident))   # no error here
            try:
                tls.settimeout(IO_TIMEOUT)
                with pytest.raises(ssl.SSLError) as info:
                    tls.sendall(b"GET /cert HTTP/1.1\r\nHost: x\r\n\r\n")
                    tls.recv(1)
            finally:
                tls.close()
            ev = srv.events.wait_for(1)[0]
        assert type(info.value) is ssl.SSLError
        assert info.value.reason in ALERT_REASONS[kind]
        assert not ev.ok and isinstance(ev.error, ssl.SSLCertVerificationError)
        assert app.request_count == 0

    def test_requests_sees_requests_ssl_error(self, pki, app, kind):
        ident, pinned = _rejection_case(pki, kind)
        with serve(app, Holder(pki, pinned)) as srv:
            s = pinned_session(client_context(ident), pki.server.fingerprint)
            try:
                with pytest.raises(requests.exceptions.SSLError) as info:
                    s.get(f"https://{LOOPBACK}:{srv.port}/cert", timeout=IO_TIMEOUT)
            finally:
                s.close()
        assert "alert" in str(info.value).lower()
        assert any(r in str(info.value) for r in ALERT_REASONS[kind])
        assert app.request_count == 0

    def test_websocket_client_sees_raw_ssl_error(self, pki, app, kind):
        ident, pinned = _rejection_case(pki, kind)
        with serve(app, Holder(pki, pinned)) as srv:
            tls = tls_connect(srv.port, client_context(ident))
            ws = websocket.WebSocket()
            try:
                with pytest.raises(ssl.SSLError) as info:
                    ws.connect(f"wss://{LOOPBACK}:{srv.port}/ws/echo", socket=tls)
            finally:
                ws.close()
        assert type(info.value) is ssl.SSLError
        assert not isinstance(info.value, websocket.WebSocketException)
        assert info.value.reason in ALERT_REASONS[kind]
        assert app.request_count == 0

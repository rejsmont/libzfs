"""Shared helpers for the client-side trust commands: `zfsbackup-config enroll` and
`identity rotate` (TLS/mTLS plan M13 / M14c / M16).

Importable from any test module (`tests/` is on `sys.path`). Everything is `127.0.0.1`
and nothing here runs a real `zfs`.

* `RealServer` -- a REAL `api.create_app` over a REAL `api.TrustGateway` and a real
  config database (the real `zfsbackup.config.store.trust` writers), served by the real
  `workers._api_server_class()` (TLS 1.3, per-connection handshake) from a thread. Every
  request is recorded (`log`), and a path can be scripted (`on`) to answer something the
  real app would not (a 502, a reset right after the handshake, a hang), or to fall
  through to the real app after the first N scripted answers. `zfs.exists` is stubbed in
  memory (`ZfsStub`), so the server never reaches a `zfs` binary.
* `Reply` / `RESET` / `HANG` -- scripted answers.
* `RealServer.edit_body(path, fn)` -- let the REAL app answer, then let `fn(body_dict)` change a 200
  JSON answer before it is sent (e.g. drop `protocol` from a real `/enroll` or `/rotate` answer: the
  server-side change happens, the client sees an answer that lacks the protocol).
* `make_server` -- fixture factory (stops every server at teardown); `peer_limits=` / `server_kwargs=`
  go to `create_app` / the server class (S-3 flood control); `record=False` leaves the app unwrapped
  (no request recorder, scripts or body edits: it would read a body before the app does).
* `closed_port` / `plain_http_server` / `silent_server` -- a port nobody listens on,
  a server that answers plaintext HTTP, a server that accepts and says nothing.
* `reloads` -- fixture: `daemonlock.request_reload` replaced by a recorder.
* `token_file` -- a 0600 file holding a token.

A note on tokens: the store keeps ONE enrollment token; creating a second replaces the
first. `RealServer.pin` (which enrolls a certificate through a fresh token of its own)
therefore has to come BEFORE the token a test wants to use.
"""

import io
import json
import os
import socket
import struct
import threading
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional

import pytest

from _api_support import RealTrust
from _tls_support import LOOPBACK
from _cli_support import (
    PLAIN_YAML,
    db_state,
    make_db,
    seed_client_id,
    store_key,
    stored,
)
from zfsbackup import api, daemonlock, identity, workers
from zfsbackup.config.store import dispose_all, import_yaml, read_config, resolve_config_path

REMOTE_ON = PLAIN_YAML + 'remote_backup:\n  enabled: true\n  target_dataset: "tank/backups"\n'
REMOTE_OFF = PLAIN_YAML + 'remote_backup:\n  enabled: false\n  target_dataset: "tank/backups"\n'


# --------------------------------------------------------------------------
# Scripted answers
# --------------------------------------------------------------------------


class Reply:
    """A scripted HTTP answer. `body` is a dict (JSON) or raw bytes / str."""

    def __init__(self, status: int, body=None, *, content_type: Optional[str] = None,
                 headers: Optional[Dict[str, str]] = None) -> None:
        self.status = status
        self.body = {} if body is None else body
        self.content_type = content_type
        self.headers = headers or {}

    def render(self):
        if isinstance(self.body, (dict, list)):
            data, ctype = json.dumps(self.body).encode(), "application/json"
        elif isinstance(self.body, str):
            data, ctype = self.body.encode(), "text/plain"
        else:
            data, ctype = bytes(self.body), "application/octet-stream"
        return data, self.content_type or ctype


class _Reset:
    """Close the connection with an RST, right after the request was read."""

    def __repr__(self) -> str:
        return "RESET"


class _Hang:
    """Do not answer for `seconds` (the client is expected to give up first)."""

    def __init__(self, seconds: float = 3.0) -> None:
        self.seconds = seconds


RESET = _Reset()
HANG = _Hang()

_STATUS_TEXT = {
    200: "OK", 302: "Found", 400: "Bad Request", 401: "Unauthorized", 403: "Forbidden",
    404: "Not Found", 405: "Method Not Allowed", 409: "Conflict",
    413: "Payload Too Large", 422: "Unprocessable Entity", 500: "Internal Server Error",
    502: "Bad Gateway", 503: "Service Unavailable", 504: "Gateway Timeout",
}


class Seen:
    """One request the server received."""

    def __init__(self, method, path, query, cert, body, headers) -> None:
        self.method = method
        self.path = path
        self.query = query
        self.cert = cert            # did the TLS layer hand the app a client certificate?
        self.body = body            # bytes
        self.headers = headers      # {name: value} of the HTTP_* variables

    @property
    def json(self):
        return json.loads(self.body) if self.body else None

    def __repr__(self) -> str:
        return f"<{self.method} {self.path} cert={self.cert} {len(self.body)}B>"


class ZfsStub:
    """In-memory `zfs.exists` for every server of one test."""

    def __init__(self) -> None:
        self.existing: set = set()
        self.calls: List[str] = []

    def exists(self, dataset, *args, **kwargs) -> bool:
        name = getattr(dataset, "name", None) or str(dataset)
        self.calls.append(name)
        return name in self.existing


class _BodyEditor:
    """Wraps the real app: for a path with an edit registered (`RealServer.edit_body`), a 200 JSON
    object answer is passed through `fn(dict)` (in place) and re-encoded."""

    def __init__(self, server: "RealServer", inner) -> None:
        self.server = server
        self.inner = inner

    def __call__(self, environ, start_response):
        edit = self.server._edits.get(environ["PATH_INFO"])
        if edit is None:
            return self.inner(environ, start_response)
        captured = {}

        def capture(status, headers, exc_info=None):
            captured["status"], captured["headers"] = status, list(headers)
            return lambda data: None

        result = self.inner(environ, capture)
        try:
            body = b"".join(result)
        finally:
            close = getattr(result, "close", None)
            if close is not None:
                close()
        if captured["status"].startswith("200"):
            try:
                data = json.loads(body)
            except ValueError:
                data = None
            if isinstance(data, dict):
                edit(data)
                body = json.dumps(data).encode()
        headers = [(k, v) for k, v in captured["headers"] if k.lower() != "content-length"]
        headers.append(("Content-Length", str(len(body))))
        start_response(captured["status"], headers)
        return [body]


class _Middleware:
    def __init__(self, server: "RealServer", inner) -> None:
        self.server = server
        self.inner = inner

    def __call__(self, environ, start_response):
        length = int(environ.get("CONTENT_LENGTH") or 0)
        body = environ["wsgi.input"].read(length) if length else b""
        environ["wsgi.input"] = io.BytesIO(body)
        method, path = environ["REQUEST_METHOD"], environ["PATH_INFO"]
        entry = Seen(
            method, path, environ.get("QUERY_STRING", ""),
            bool(environ.get("SSL_CLIENT_CERT")), body,
            {k: v for k, v in environ.items() if k.startswith("HTTP_")},
        )
        with self.server._lock:
            self.server.log.append(entry)
            n = sum(1 for e in self.server.log if e.path == path and e.method == method)
        effect = self.server._effect(path, n)
        if effect is None:
            return self.inner(environ, start_response)
        if effect is RESET:
            sock = environ["werkzeug.socket"]
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            # `close()` would not close the descriptor while the handler's file objects still
            # reference the socket: detach it and close the descriptor itself (an RST).
            os.close(sock.detach())
            raise ConnectionAbortedError("scripted reset")
        if isinstance(effect, _Hang):
            time.sleep(effect.seconds)
            effect = Reply(200, {})
        data, ctype = effect.render()
        headers = [("Content-Type", ctype), ("Content-Length", str(len(data)))]
        headers += list(effect.headers.items())
        start_response(f"{effect.status} {_STATUS_TEXT.get(effect.status, 'Status')}", headers)
        return [data]


# --------------------------------------------------------------------------
# A real server
# --------------------------------------------------------------------------


class RealServer:
    """The real API over a real trust database, on a loopback port. Build it with the
    `make_server` fixture."""

    def __init__(self, base: Path, zfs: ZfsStub, *, remote: bool = True,
                 dry_run: bool = False, peer_limits=None, server_kwargs=None,
                 record: bool = True) -> None:
        base.parent.mkdir(parents=True, exist_ok=True)
        source = base.with_name(base.name + ".in.yaml")      # the import creates `base` itself
        source.write_text(REMOTE_ON if remote else REMOTE_OFF)
        self.resolved = resolve_config_path(str(base / "config.db"))
        import_yaml(source, self.resolved)
        dispose_all()
        self.zfs = zfs
        self.trust = RealTrust(self.resolved)
        config = read_config(self.resolved).config
        dispose_all()
        config.dry_run = dry_run
        # S-3: the app's own flood-control state unless a test supplies one
        extra = {} if peer_limits is None else {"peer_limits": peer_limits}
        self.app = api.create_app(config, self.trust.gateway, self.trust.server_fp, **extra)
        self._edits: Dict[str, Callable[[dict], None]] = {}
        if record:      # the recorder reads each body in full BEFORE the app: not for a stalled-body test
            self.app.wsgi_app = _Middleware(self, _BodyEditor(self, self.app.wsgi_app))
        self.log: List[Seen] = []
        self._lock = threading.Lock()
        self._script: Dict[str, tuple] = {}
        cls = workers._api_server_class()
        self._server = cls(LOOPBACK, 0, self.app, self.trust.gateway, **(server_kwargs or {}))
        # a scripted RESET makes the handler thread's next socket call fail; werkzeug would
        # print that traceback to sys.stderr, i.e. into the CLI's captured stderr
        self._server.handle_error = lambda request, client_address: None
        self.port = self._server.server_port
        self._thread = threading.Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True
        )
        self._stopped = False

    # -- lifecycle
    def start(self) -> "RealServer":
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self._server.shutdown()
        self._thread.join(10)
        self._server.server_close()
        assert not self._thread.is_alive()

    # -- what the server is
    @property
    def url(self) -> str:
        return f"https://{LOOPBACK}:{self.port}"

    @property
    def fp(self) -> str:
        return self.trust.server_fp

    # -- scripting
    def on(self, path: str, *effects, then=None) -> "RealServer":
        """The n-th request for `path` gets `effects[n-1]`; later ones get `then` (None =
        the real app). An effect of None passes that one request through."""
        with self._lock:
            self._script[path] = (list(effects), then)
        return self

    def edit_body(self, path: str, fn: Optional[Callable[[dict], None]]) -> "RealServer":
        """Let the real app answer `path`, then change a 200 JSON object answer with `fn(dict)`
        (`None` removes the edit). The server-side effect of the request still happens."""
        with self._lock:
            if fn is None:
                self._edits.pop(path, None)
            else:
                self._edits[path] = fn
        return self

    def _effect(self, path: str, n: int):
        with self._lock:
            entry = self._script.get(path)
        if entry is None:
            return None
        effects, then = entry
        return effects[n - 1] if n <= len(effects) else then

    # -- what it saw
    def seen(self, path: str, method: Optional[str] = None) -> List[Seen]:
        return [e for e in self.log if e.path == path and (method is None or e.method == method)]

    def paths(self) -> List[str]:
        return [e.path for e in self.log]

    def raw_text(self) -> str:
        """Everything the server received that is NOT a request body: paths, queries and
        headers (the token must never be in any of it)."""
        return "\n".join(
            f"{e.method} {e.path}?{e.query}\n" + "\n".join(f"{k}: {v}" for k, v in e.headers.items())
            for e in self.log
        )

    # -- the trust store, through the real API
    def token(self, **kw) -> str:
        return self.trust.token(**kw)

    def token_state(self) -> str:
        return self.trust.token_state()

    def pins(self):
        return self.trust.pins()

    def clients(self):
        return self.trust.clients()

    def pin(self, ident, client_id: str) -> None:
        """Pin `ident`'s certificate under `client_id` (through a token of its own: call
        this BEFORE creating the token a test will use)."""
        self.trust.enroll(ident, client_id)

    def pin_additional(self, ident, client_id: str) -> None:
        """Add `ident`'s certificate to an EXISTING `client_id` (a token scoped to the id,
        no replace), as an earlier successful `enroll --new-key` would have."""
        import time

        from zfsbackup.config.store import trust as store_trust

        token = self.trust.token(client_id=client_id)
        _, secret = identity.parse_token(token)
        outcome = store_trust.enroll_client(
            self.resolved, secret_sha256=identity.secret_hash(secret), server_fingerprint=self.fp,
            client_id=client_id,
            pin=store_trust.NewPin(ident.fingerprint, ident.cert_pem, ident.not_after.timestamp()),
            now=time.time, dataset_exists=False,
        )
        assert outcome is store_trust.EnrollOutcome.ENROLLED, outcome
        self.refresh()

    def refresh(self) -> None:
        """Make the gateway re-read the pins (a revocation made in the database)."""
        assert self.trust.gateway.refresh()


@pytest.fixture
def zfs_stub(monkeypatch) -> ZfsStub:
    stub = ZfsStub()
    monkeypatch.setattr(api.zfs, "exists", stub.exists)
    return stub


@pytest.fixture
def make_server(tmp_path, zfs_stub):
    """`make_server(name=None, remote=True, dry_run=False) -> RealServer`, started. All of
    them are stopped at teardown."""
    made: List[RealServer] = []

    def make(name: Optional[str] = None, **kw) -> RealServer:
        server = RealServer(tmp_path / "servers" / (name or f"srv{len(made) + 1}"), zfs_stub, **kw)
        server.start()
        made.append(server)
        return server

    yield make
    for server in made:
        server.stop()
    dispose_all()


# --------------------------------------------------------------------------
# Ports with something else behind them
# --------------------------------------------------------------------------


def closed_port() -> int:
    """A loopback port nobody listens on."""
    with socket.socket() as s:
        s.bind((LOOPBACK, 0))
        return s.getsockname()[1]


class _RawServer:
    def __init__(self, handler: Callable[[socket.socket], None]) -> None:
        self._handler = handler
        self._sock = socket.socket()
        self._sock.bind((LOOPBACK, 0))
        self._sock.listen(8)
        self._sock.settimeout(0.2)
        self.port = self._sock.getsockname()[1]
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    @property
    def url(self) -> str:
        return f"https://{LOOPBACK}:{self.port}"

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self._one, args=(conn,), daemon=True).start()

    def _one(self, conn: socket.socket) -> None:
        try:
            self._handler(conn)
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def __enter__(self) -> "_RawServer":
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._thread.join(5)
        self._sock.close()


def plain_http_server() -> _RawServer:
    """A server that answers a plaintext HTTP error whatever it is sent (a TLS client gets
    a handshake failure)."""
    def handler(conn):
        conn.settimeout(2)
        try:
            conn.recv(4096)
        except OSError:
            pass
        conn.sendall(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
    return _RawServer(handler)


def silent_server(hold: float = 3.0) -> _RawServer:
    """A server that accepts and never says anything for `hold` seconds."""
    def handler(conn):
        time.sleep(hold)
    return _RawServer(handler)


# --------------------------------------------------------------------------
# The client side
# --------------------------------------------------------------------------


class Reloads(list):
    """Every `daemonlock.request_reload` call: `(skip_if_applied, key the database held at
    that moment)`."""


@pytest.fixture
def reloads(db, monkeypatch) -> Reloads:
    """Replace `request_reload` with a recorder (answer `no-daemon`)."""
    calls = Reloads()

    def recorder(resolved, *, skip_if_applied=None):
        calls.append((skip_if_applied, store_key(db)))
        return daemonlock.ReloadRequest("no-daemon")

    monkeypatch.setattr(daemonlock, "request_reload", recorder)
    return calls


def token_file(directory: Path, token, *, mode: int = 0o600, name: str = "token") -> Path:
    """A file holding `token` (what `trust token create` printed)."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text(str(token) + "\n")
    path.chmod(mode)
    return path


def secret_of(token) -> str:
    return str(token).rsplit(".", 1)[1]


# --------------------------------------------------------------------------
# A client database and the commands around it (shared by the enroll / rotate tests; the
# importing module defines `run` -- a `Cli` -- and imports `db`, `reloads`)
# --------------------------------------------------------------------------

CID = "host-1"


@pytest.fixture
def client(run, db, reloads):
    """A client database with a client id already stored (so a failing command changes
    nothing), no destinations, and `reloads` forgotten."""
    make_db(run, db, PLAIN_YAML)
    seed_client_id(db, CID)
    reloads.clear()
    return db


def client_with(run, db, reloads, destinations, extra=""):
    """A client database (id `CID`) whose configuration already has `destinations`:
    `{name: (url, server_fingerprint | None)}`."""
    text = PLAIN_YAML + ("destinations:\n" if destinations else "")
    for name, (url, fp) in destinations.items():
        text += f'  {name}:\n    url: "{url}"\n'
        if fp:
            text += f'    server_fingerprint: "{fp}"\n'
    make_db(run, db, text + extra)
    seed_client_id(db, CID)
    reloads.clear()
    return db


def tls_of(database: Path) -> Path:
    return Path(str(database) + ".tls")


def do_enroll(run, db, srv, token, *extra, name="offsite", url=True, fname="token"):
    """`enroll NAME [--url <srv>] --token-file F`. `srv` is a RealServer or a URL string."""
    path = token_file(db.parent.parent / "tokens", token, name=fname)
    args = ["enroll", name]
    if url:
        args += ["--url", srv if isinstance(srv, str) else srv.url]
    return run(*args, "--token-file", path, *extra, "-c", db)


def everything(result) -> str:
    return result.stdout + result.stderr


def dest(database: Path, name="offsite"):
    return stored(database).destinations.get(name)


def client_cert(database: Path) -> "identity.Identity":
    return identity.load_identity(
        tls_of(database) / "client.crt", tls_of(database) / "client.key", identity.ROLE_CLIENT
    )


def untouched(database: Path, before, reloads):
    assert db_state(database) == before, "the database changed"
    assert list(reloads) == [], "the daemon was asked to reload"


def make_foreign_token(srv) -> str:
    """A well-formed token for `srv` that the server has never stored."""
    token, _ = identity.make_token(srv.fp)
    return str(token)

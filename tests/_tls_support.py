"""Shared helpers for the tests that exercise the API worker's TLS (plan M9 / M16).

Plain functions and tiny classes, importable from any test module (`tests/` is on
`sys.path`, like `_hermetic` and `_config_handoff`). Nothing here talks to ZFS and
every socket is `127.0.0.1`.

* `server_identity` / `client_identity` / `ensure_server_identity` -- real generated
  identities (`zfsbackup.identity.ensure_identity`).
* `StubTrust` -- the `read_trust_key` / `read_trust` seams of `api.TrustGateway`,
  scripted in memory (no database).
* `make_gateway` -- a `TrustGateway` over a freshly generated server identity.
* `pin_client` / `unpin_client` -- add or revoke a client certificate through the real
  `zfsbackup.config.store.trust` writers on a real config database.
* `https_get` / `try_get` -- one TLS 1.3 HTTPS request against the daemon API (`try_get`
  returns the exception instead of raising), certificate verification off (the server certificate is self-signed and
  pinned by fingerprint in production). `https_get` sends the wire protocol header
  (plan S-5) unless `protocol=False`.
* `raw_connect` -- a plain TCP connection (no TLS): a silent peer or a plaintext request.
"""

import hashlib
import http.client
import itertools
import socket
import ssl
from pathlib import Path
from typing import Iterable, Optional, Sequence

from zfsbackup import identity
from zfsbackup.config.store import trust as store_trust
from zfsbackup.protocol import protocol_headers

LOOPBACK = "127.0.0.1"
#: Generous per-operation timeout: everything is loopback, so hitting it means a hang.
IO_TIMEOUT = 10.0


# ---------------------------------------------------------------------------
# Identities
# ---------------------------------------------------------------------------


def server_identity(directory: Path) -> "identity.Identity":
    """A generated server identity in `directory` (created 0750; the group is that
    of its parent)."""
    return identity.ensure_identity(directory, identity.ROLE_SERVER, group_of=Path(directory).parent)


def client_identity(directory: Path, name: Optional[str] = None) -> "identity.Identity":
    """A generated client identity in `directory`."""
    return identity.ensure_identity(
        directory, identity.ROLE_CLIENT, name=name, group_of=Path(directory).parent
    )


def ensure_server_identity(resolved) -> "identity.Identity":
    """The server identity the daemon would have created at startup for the config
    database `resolved` (`<db>.tls/server.{crt,key}`)."""
    directory = identity.tls_dir(resolved.resolved_path)
    return identity.ensure_identity(
        directory, identity.ROLE_SERVER, group_of=Path(resolved.resolved_path).parent
    )


# ---------------------------------------------------------------------------
# A scripted trust store (the TrustGateway seams)
# ---------------------------------------------------------------------------


class StubTrust:
    """In-memory `read_trust_key` / `read_trust`: `pins` is a list of
    `store.trust.TrustedPin`; `store_id` / `generation` the key. `fail` makes both
    raise it. Counts the calls."""

    def __init__(self, pins: Sequence = (), *, store_id: int = 7, generation: int = 1):
        self.pins = list(pins)
        self.store_id = store_id
        self.generation = generation
        self.fail: Optional[BaseException] = None
        self.key_calls = 0
        self.read_calls = 0

    def read_trust_key(self, resolved):
        self.key_calls += 1
        if self.fail is not None:
            raise self.fail
        return store_trust.TrustKey(self.store_id, self.generation)

    def read_trust(self, resolved):
        self.read_calls += 1
        if self.fail is not None:
            raise self.fail
        return store_trust.TrustSnapshot(self.store_id, self.generation, tuple(self.pins))

    def set_pins(self, pins: Iterable, *, generation: Optional[int] = None,
                 store_id: Optional[int] = None) -> None:
        self.pins = list(pins)
        if generation is not None:
            self.generation = generation
        if store_id is not None:
            self.store_id = store_id


_PIN_COUNTER = itertools.count(1)


def trusted_pin(ident: "identity.Identity", client_id: str = "host-a", *,
                predecessor: Optional[str] = None) -> "store_trust.TrustedPin":
    """The `TrustedPin` the store would hold for `ident`'s certificate."""
    return store_trust.TrustedPin(
        fingerprint=ident.fingerprint,
        client_id=client_id,
        cert_pem=ident.cert_pem,
        not_after=ident.not_after.timestamp(),
        enrolled_at=float(next(_PIN_COUNTER)),
        enrolled_via="token",
        predecessor=predecessor,
    )


def make_gateway(directory: Path, *, pins: Sequence = (), stub: Optional[StubTrust] = None,
                 **kwargs):
    """-> `(gateway, server_identity, stub)`: a real `api.TrustGateway` over a fresh server
    identity in `directory`, reading from `stub` (created if not given). `kwargs` go to the
    gateway (`context_factory`...). `resolved` is a placeholder: the stub ignores it."""
    from zfsbackup.api import TrustGateway

    ident = server_identity(directory)
    stub = stub or StubTrust(pins)
    gateway = TrustGateway(
        ident, object(), read_trust_key=stub.read_trust_key, read_trust=stub.read_trust, **kwargs
    )
    return gateway, ident, stub


# ---------------------------------------------------------------------------
# Real trust writes on a real config database
# ---------------------------------------------------------------------------

_TOKEN_SECRET = hashlib.sha256(b"m9 tests enrollment secret").hexdigest()
_NOW = 1_800_000_000.0


def pin_client(resolved, ident: "identity.Identity", client_id: str, *,
               server_fp: str = "ab" * 32) -> None:
    """Pin `ident`'s certificate under `client_id` through the real store writers (one token,
    one enrollment). The token's server fingerprint is arbitrary: the store only compares it
    with the one passed to `enroll_client`."""
    import time

    now = time.time()
    store_trust.create_token(
        resolved, secret_sha256=_TOKEN_SECRET, server_fingerprint=server_fp,
        created_at=now - 1, expires_at=now + 3600, client_id=None, lock=None,
    )
    outcome = store_trust.enroll_client(
        resolved, secret_sha256=_TOKEN_SECRET, server_fingerprint=server_fp,
        client_id=client_id,
        pin=store_trust.NewPin(ident.fingerprint, ident.cert_pem, ident.not_after.timestamp()),
        now=now, dataset_exists=False,
    )
    assert outcome is store_trust.EnrollOutcome.ENROLLED, outcome


def unpin_client(resolved, ident: "identity.Identity") -> None:
    """Revoke `ident`'s pin (the client stays known)."""
    store_trust.revoke_pin(resolved, ident.fingerprint, lock=None)


# ---------------------------------------------------------------------------
# Talking TLS to the API
# ---------------------------------------------------------------------------


def insecure_client_context(ident: Optional["identity.Identity"] = None,
                            *, maximum_version=None) -> ssl.SSLContext:
    """A TLS client context that verifies nothing and presents `ident`'s certificate if
    given. TLS 1.3 minimum unless `maximum_version` caps it (to test refusal)."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2 if maximum_version else ssl.TLSVersion.TLSv1_3
    if maximum_version is not None:
        ctx.maximum_version = maximum_version
    if ident is not None:
        ctx.load_cert_chain(str(ident.cert_path), str(ident.key_path))
    return ctx


def https_get(port: int, path: str, ctx: Optional[ssl.SSLContext] = None, *,
              host: str = LOOPBACK, timeout: float = IO_TIMEOUT, headers=None,
              protocol: bool = True):
    """-> `(status, body_bytes)`. Raises `ssl.SSLError` / `OSError` if the connection or the
    request fails (a certificate TLS 1.3 rejects shows up on the first read, not at connect).
    The request names this code's wire protocol version (plan S-5) unless `protocol=False`; an
    explicit `headers` entry of the same name wins."""
    conn = http.client.HTTPSConnection(
        host, port, timeout=timeout, context=ctx or insecure_client_context()
    )
    try:
        conn.request("GET", path, headers={**(protocol_headers() if protocol else {}), **(headers or {})})
        response = conn.getresponse()
        return response.status, response.read()
    finally:
        conn.close()


def try_get(port: int, path: str, ctx: Optional[ssl.SSLContext] = None, **kw):
    """`https_get`, or the exception it raised."""
    try:
        return https_get(port, path, ctx, **kw)
    except (OSError, http.client.HTTPException) as exc:     # SSLError is an OSError
        return exc


def raw_connect(port: int, *, host: str = LOOPBACK, timeout: float = IO_TIMEOUT) -> socket.socket:
    """A plain TCP connection (no TLS), e.g. a silent peer or a plaintext request."""
    return socket.create_connection((host, port), timeout=timeout)

"""Shared helpers for the API route tests (TLS/mTLS plan M10 / M14b / M16).

Importable from any test module (`tests/` is on `sys.path`, like `_tls_support`).
Nothing here talks to ZFS and no socket is opened.

* `ClientCert` / `make_client_cert` -- an in-memory client certificate and key, generated
  with `cryptography` (no files); `sign` is the proof-of-possession signature.
* `FakeGateway` -- a stub of everything `api.create_app` asks of a `TrustGateway`:
  `lookup`, `refresh`, `restart_reason`, `current_context` and the three store writers
  (`token_precheck`, `enroll_client`, `rotate_client`, `retire_predecessor`), each scriptable and counted.
* `plain_client` / `buffered_client` -- test clients that send the wire protocol header
  (plan S-5, `protocol=None` for none) and the second of them a certificate.
* `Authed` -- a Flask app built over a gateway, with a test client / request context that
  presents a client certificate in `environ["SSL_CLIENT_CERT"]`, exactly where the TLS
  layer puts it. `client_id` pins one certificate under that id (`Authed.who`).
* `RealTrust` -- a REAL `api.TrustGateway` over a real config database (the real
  `zfsbackup.config.store.trust` writers) plus token / pin helpers.
* `enroll_body` / `rotate_body` -- the request bodies of the wire contract.
"""

import base64
import contextlib
import datetime as dt
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from zfsbackup import identity
from zfsbackup.api import PinInfo, TrustGateway, create_app
from zfsbackup.config.store import trust as store_trust
from zfsbackup.protocol import PROTOCOL_HEADER, PROTOCOL_VERSION

#: The client id of the default pinned certificate: the id the pre-TLS tests used in URLs.
CLIENT_ID = "client1"
#: A well-formed server fingerprint for apps that never present a real identity.
SERVER_FP = "ab" * 32

_DEFAULT = object()
#: The WSGI environ key of the protocol header (plan S-5).
PROTOCOL_ENVIRON = "HTTP_" + PROTOCOL_HEADER.upper().replace("-", "_")
_DAY = dt.timedelta(days=1)


# ---------------------------------------------------------------------------
# Certificates
# ---------------------------------------------------------------------------


@dataclass
class ClientCert:
    """A client certificate with its key. `cert_pem` is what the TLS layer would put in
    `SSL_CLIENT_CERT` and what `/enroll` receives."""

    key: ec.EllipticCurvePrivateKey
    cert: x509.Certificate
    cert_pem: str
    fingerprint: str
    not_after: dt.datetime

    @property
    def pem(self) -> str:
        return self.cert_pem

    def sign(self, payload: bytes) -> bytes:
        return identity.sign_payload(self.key, payload)

    def signature_b64(self, payload: bytes) -> str:
        return base64.b64encode(self.sign(payload)).decode()


def make_client_cert(
    *,
    cn: str = "test-client",
    days: float = 30,
    not_before: Optional[dt.datetime] = None,
    not_after: Optional[dt.datetime] = None,
    eku=(ExtendedKeyUsageOID.CLIENT_AUTH,),
    ca: bool = False,
    key=None,
) -> ClientCert:
    """A self-signed P-256 certificate acceptable to `identity.check_client_cert` by default
    (valid from an hour ago for `days` days, `clientAuth`, `CA:FALSE`, `digitalSignature`).
    `ca` / `eku` / the window make the unacceptable variants."""
    key = key or ec.generate_private_key(ec.SECP256R1())
    now = dt.datetime.now(dt.timezone.utc)
    nb = not_before if not_before is not None else now - dt.timedelta(hours=1)
    na = not_after if not_after is not None else now + dt.timedelta(days=days)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    builder = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(nb)
        .not_valid_after(na)
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, content_commitment=False, key_encipherment=False,
                data_encipherment=False, key_agreement=False, key_cert_sign=ca, crl_sign=False,
                encipher_only=False, decipher_only=False,
            ),
            critical=True,
        )
    )
    if eku:
        builder = builder.add_extension(x509.ExtendedKeyUsage(list(eku)), critical=False)
    cert = builder.sign(key, hashes.SHA256())
    pem = cert.public_bytes(serialization.Encoding.PEM).decode("ascii")
    return ClientCert(
        key=key, cert=cert, cert_pem=pem,
        fingerprint=identity.fingerprint_pem(pem), not_after=cert.not_valid_after_utc,
    )


# ---------------------------------------------------------------------------
# A scriptable stand-in for api.TrustGateway
# ---------------------------------------------------------------------------


def _effect(value, *args, **kwargs):
    """Scripted behaviour: an exception is raised, a callable is called, anything else is the
    result."""
    if isinstance(value, BaseException):
        raise value
    if callable(value):
        return value(*args, **kwargs)
    return value


class FakeGateway:
    """What `api.create_app` asks of a `TrustGateway`, in memory.

    `pins` maps fingerprint -> `PinInfo` (`pin()` adds one). Each store writer returns the
    scripted `*_effect` (a value, an exception to raise, or a callable) and records its keyword
    arguments in `*_calls`. `refresh()` counts and returns `refresh_result`.
    """

    def __init__(self) -> None:
        from zfsbackup.config.store.trust import EnrollOutcome, RotateOutcome

        self.pins: Dict[str, PinInfo] = {}
        self.restart_reason: Optional[str] = None
        self.refresh_result = True
        self.refresh_calls = 0
        #: The `timeout=` of each `refresh()` call (request threads pass `REQUEST_REFRESH_WAIT`).
        self.refresh_timeouts: List[Optional[float]] = []
        self.enroll_effect = EnrollOutcome.ENROLLED
        self.rotate_effect = RotateOutcome.ROTATED
        self.retire_effect = True
        #: `token_precheck`'s answer: True (the default) lets `/enroll` go on to `zfs` and the store.
        self.precheck_effect = True
        self.precheck_calls: List[dict] = []
        self.enroll_calls: List[dict] = []
        self.rotate_calls: List[dict] = []
        self.retire_calls: List[str] = []

    def pin(self, cert: ClientCert, client_id: str, *, not_after: Optional[float] = None,
            predecessor: Optional[str] = None) -> ClientCert:
        self.pins[cert.fingerprint] = PinInfo(
            client_id,
            cert.not_after.timestamp() if not_after is None else not_after,
            predecessor,
        )
        return cert

    # -- the TrustGateway surface create_app uses
    def lookup(self, fingerprint):
        return self.pins.get(fingerprint)

    def refresh(self, *, timeout: Optional[float] = None) -> bool:
        self.refresh_calls += 1
        self.refresh_timeouts.append(timeout)
        return self.refresh_result

    def current_context(self):
        return None

    def token_precheck(self, **kwargs) -> bool:
        self.precheck_calls.append(kwargs)
        return _effect(self.precheck_effect, **kwargs)

    def enroll_client(self, **kwargs):
        self.enroll_calls.append(kwargs)
        return _effect(self.enroll_effect, **kwargs)

    def rotate_client(self, **kwargs):
        self.rotate_calls.append(kwargs)
        return _effect(self.rotate_effect, **kwargs)

    def retire_predecessor(self, fingerprint):
        self.retire_calls.append(fingerprint)
        return _effect(self.retire_effect, fingerprint)


# ---------------------------------------------------------------------------
# An app plus a way to call it as a client certificate
# ---------------------------------------------------------------------------


def protocol_environ(protocol=_DEFAULT) -> Dict[str, str]:
    """The WSGI environ entry carrying the protocol header (plan S-5): this code's own version
    by default, `str(protocol)` for anything else, nothing at all for `None` (a client that
    sends no header)."""
    if protocol is None:
        return {}
    return {PROTOCOL_ENVIRON: str(PROTOCOL_VERSION if protocol is _DEFAULT else protocol)}


def plain_client(app, *, protocol=_DEFAULT):
    """An UNBUFFERED test client (a response is closed only by the test) that sends the
    protocol header (`protocol_environ`) and presents no certificate; set
    `client.environ_base["SSL_CLIENT_CERT"]` for one."""
    client = app.test_client()
    client.environ_base.update(protocol_environ(protocol))
    return client


def buffered_client(app, who=None, *, protocol=_DEFAULT):
    """A test client that presents `who`'s certificate (none if `None`) and BUFFERS every
    response, i.e. closes it before returning. That matters: the API refreshes its trust view
    once a response has been sent (`response.call_on_close`), and an unbuffered test response
    is only closed when it is garbage-collected.

    Every request carries `X-Zfsbackup-Protocol: <this code's version>` (plan S-5) unless
    `protocol` says otherwise (`protocol_environ`); a header passed to one request wins."""
    client = app.test_client()
    if who is not None:
        client.environ_base["SSL_CLIENT_CERT"] = who.cert_pem
    client.environ_base.update(protocol_environ(protocol))
    open_ = client.open
    client.open = lambda *args, **kwargs: open_(*args, **{"buffered": True, **kwargs})
    return client


class Authed:
    """`create_app(config, gateway, server_fp, clock=...)` and ways to call it presenting a
    client certificate.

    With a `FakeGateway` (the default), `cert_for(client_id)` makes a certificate and pins it
    under that id (cached per id); `who` is the one for `client_id` (None for `client_id=None`).
    """

    def __init__(self, config, *, client_id: Optional[str] = CLIENT_ID, gateway=None,
                 server_fp: str = SERVER_FP, clock: Optional[Callable[[], float]] = None):
        self.config = config
        self.gateway = gateway if gateway is not None else FakeGateway()
        self.server_fp = server_fp
        self.certs: Dict[str, ClientCert] = {}
        kwargs = {} if clock is None else {"clock": clock}
        self.app = create_app(config, self.gateway, server_fp, **kwargs)
        self.app.testing = True
        self.who = self.cert_for(client_id) if client_id is not None else None

    @property
    def gate(self):
        return self.app.extensions["zfsbackup.stream_gate"]

    def cert_for(self, client_id: str) -> ClientCert:
        cert = self.certs.get(client_id)
        if cert is None:
            cert = self.certs[client_id] = make_client_cert(cn=client_id)
            self.gateway.pin(cert, client_id)
        return cert

    def client(self, who=_DEFAULT, *, protocol=_DEFAULT):
        """A Flask test client that presents `who`'s certificate (default: `self.who`;
        `None`: no certificate) and the protocol header (`protocol=None`: none, another
        value: that one)."""
        who = self.who if who is _DEFAULT else who
        return buffered_client(self.app, who, protocol=protocol)

    @contextlib.contextmanager
    def context(self, path: str, who=_DEFAULT, *, protocol=_DEFAULT, **kwargs):
        """A request context for `path` as `who`, with the before-request hooks (authentication)
        already run: for calling a view function directly, e.g. the websocket handler. Asserts
        that authentication let the request through."""
        who = self.who if who is _DEFAULT else who
        environ = {} if who is None else {"SSL_CLIENT_CERT": who.cert_pem}
        environ.update(protocol_environ(protocol))
        with self.app.test_request_context(path, environ_base=environ, **kwargs):
            refusal = self.app.preprocess_request()
            assert refusal is None, f"the request was refused: {refusal!r}"
            yield


# ---------------------------------------------------------------------------
# A real gateway over a real config database
# ---------------------------------------------------------------------------


class RealTrust:
    """A real `api.TrustGateway` over the config database `resolved` (which must have its
    server identity: the `tls_config_db` fixture), reading and writing through the real
    `zfsbackup.config.store.trust`. The first `refresh()` has run."""

    def __init__(self, resolved, **gateway_kwargs):
        from _tls_support import ensure_server_identity

        self.resolved = resolved
        self.identity = ensure_server_identity(resolved)
        self.gateway = TrustGateway(self.identity, resolved, **gateway_kwargs)
        self.gateway.refresh()

    @property
    def server_fp(self) -> str:
        return self.identity.fingerprint

    def token(self, *, client_id: Optional[str] = None, replace: bool = False,
              ttl: float = 3600.0, created_at: Optional[float] = None) -> str:
        """Create the (single) enrollment token; -> the whole token string."""
        token, sha = identity.make_token(self.server_fp)
        now = time.time() if created_at is None else created_at
        store_trust.create_token(
            self.resolved, secret_sha256=sha, server_fingerprint=self.server_fp,
            created_at=now - 1, expires_at=now - 1 + ttl, client_id=client_id,
            replace=replace, lock=None,
        )
        return str(token)

    def token_state(self):
        return store_trust.token_status(self.resolved, time.time()).state

    def pins(self) -> Dict[str, "store_trust.TrustedPin"]:
        return {p.fingerprint: p for p in store_trust.read_trust(self.resolved).pins}

    def clients(self):
        return {c.client_id: c for c in store_trust.list_clients(self.resolved)}

    def trust_key(self):
        return tuple(store_trust.read_trust_key(self.resolved))

    def enroll(self, cert: ClientCert, client_id: str, *, scope: Optional[str] = None,
               **token_kwargs) -> None:
        """Pin `cert` under `client_id` through the real store (one fresh token, scoped to
        `scope` if given; `replace=True` makes it a `--replace` token), then refresh the
        gateway."""
        token = self.token(client_id=scope, **token_kwargs)
        _, secret = identity.parse_token(token)
        outcome = store_trust.enroll_client(
            self.resolved, secret_sha256=identity.secret_hash(secret),
            server_fingerprint=self.server_fp, client_id=client_id,
            pin=store_trust.NewPin(cert.fingerprint, cert.cert_pem, cert.not_after.timestamp()),
            now=time.time, dataset_exists=False,
        )
        assert outcome is store_trust.EnrollOutcome.ENROLLED, outcome
        assert self.gateway.refresh()


# ---------------------------------------------------------------------------
# Request bodies of the wire contract
# ---------------------------------------------------------------------------


def enroll_body(server_fp: str, client_id: str, cert: ClientCert, token: str, *,
                label: Optional[str] = None) -> dict:
    """A valid `/enroll` body: the signature is the certificate key's over
    `enrollment_payload(server_fp, client_id, cert_fp, secret_hash(secret))`."""
    _, secret = identity.parse_token(token)
    payload = identity.enrollment_payload(
        server_fp, client_id, cert.fingerprint, identity.secret_hash(secret)
    )
    body = {
        "version": 1, "client_id": client_id, "token_secret": token,
        "cert_pem": cert.cert_pem, "signature": cert.signature_b64(payload),
    }
    if label is not None:
        body["label"] = label
    return body


def rotate_body(server_fp: str, client_id: str, old_fp: str, successor: ClientCert) -> dict:
    """A valid `/rotate` body: the SUCCESSOR key signs
    `rotation_payload(server_fp, client_id, old_fp, new_fp)`."""
    payload = identity.rotation_payload(server_fp, client_id, old_fp, successor.fingerprint)
    return {
        "version": 1, "successor_cert_pem": successor.cert_pem,
        "signature": successor.signature_b64(payload),
    }

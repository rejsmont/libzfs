"""Tests for `zfsbackup.identity` (plan items M3 / M16): TLS identities, fingerprint
pinning, enrollment tokens and proof of possession.

Layout of this file
-------------------

* pure / filesystem unit tests (marker ``unit``): fingerprints, tokens, payloads,
  proof of possession, the ``check_client_cert`` matrix, ``ensure_identity`` /
  ``promote_identity`` / ``load_identity`` file handling (regenerate-versus-refuse,
  modes, orphan-certificate ordering, stale temporaries, the group path with
  ``geteuid``/``fchown`` mocked, the lock-free fast path);
* real sockets and real processes (marker ``integration``): the multi-process
  ``ensure_identity`` race under fork, spawn and forkserver, ``server_context`` +
  ``connect_pinned`` (including IPv6), ``pinned_session``, and the error
  classifiers against a server that really rejects the client certificate.

Hygiene: no ``zfs``, servers bind ``127.0.0.1`` / ``::1`` port 0 only and run on
threads that are joined, every child process is joined (and killed if it is not
finished) in a ``finally``. Keys are cached for the whole session
(``key_of``); certificates are built with ``cryptography`` at fixed instants so the
validity-window arithmetic is exact and clock independent -- only the tests that
do a real TLS handshake use the real clock (OpenSSL checks the real time).

This module deliberately keeps its own helpers: a shared certificate factory is
planned for ``conftest.py`` (M16a) but is not available yet.
"""

import base64
import dataclasses
import errno
import fcntl
import hashlib
import logging
import multiprocessing
import os
import socket
import ssl
import stat
import tempfile
import threading
import time
import types
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, List, Optional

import pytest

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import (
    ec,
    ed448,
    ed25519,
    padding,
    rsa,
)
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from zfsbackup import identity
from zfsbackup.config import normalize_fingerprint as config_normalize_fingerprint

LOOPBACK = "127.0.0.1"
#: Generous per-operation timeout: everything is loopback, so hitting it means a hang.
IO_TIMEOUT = 10.0

#: The instant the exact-arithmetic certificate tests are pinned to.
FIXED_NOW = datetime(2026, 10, 20, 12, 0, 0, tzinfo=timezone.utc)
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)

CLIENT_AUTH = ExtendedKeyUsageOID.CLIENT_AUTH
SERVER_AUTH = ExtendedKeyUsageOID.SERVER_AUTH

FP_A = "ab" * 32
FP_B = "cd" * 32
FP_C = "ef" * 32
FP_D = "01" * 32


# ==========================================================================
# Helpers: keys and certificates
# ==========================================================================

_KEY_FACTORIES = {
    "ec256": lambda: ec.generate_private_key(ec.SECP256R1()),
    "ec384": lambda: ec.generate_private_key(ec.SECP384R1()),
    "ec521": lambda: ec.generate_private_key(ec.SECP521R1()),
    "k1": lambda: ec.generate_private_key(ec.SECP256K1()),
    "p224": lambda: ec.generate_private_key(ec.SECP224R1()),
    "bp256": lambda: ec.generate_private_key(ec.BrainpoolP256R1()),
    "rsa2048": lambda: rsa.generate_private_key(65537, 2048),
    "rsa1024": lambda: rsa.generate_private_key(65537, 1024),
    "ed25519": lambda: ed25519.Ed25519PrivateKey.generate(),
    "ed448": lambda: ed448.Ed448PrivateKey.generate(),
}
_KEY_CACHE: dict = {}
_KEY_LOCK = threading.Lock()


def key_of(name: str):
    """Session-wide key cache. ``"ec256:foo"`` and ``"ec256:bar"`` are two distinct
    P-256 keys; the part before the colon picks the algorithm."""
    with _KEY_LOCK:
        if name not in _KEY_CACHE:
            _KEY_CACHE[name] = _KEY_FACTORIES[name.split(":")[0]]()
        return _KEY_CACHE[name]


def key_pem(key) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def ku_ext(**on) -> x509.KeyUsage:
    flags = dict(
        digital_signature=False, content_commitment=False, key_encipherment=False,
        data_encipherment=False, key_agreement=False, key_cert_sign=False,
        crl_sign=False, encipher_only=False, decipher_only=False,
    )
    flags.update(on)
    return x509.KeyUsage(**flags)


SIG = ku_ext(digital_signature=True)


def make_cert(key, *, cn="test", signer=None, not_before=None, not_after=None,
              eku=(CLIENT_AUTH,), ca=False, ku=SIG, basic=True, extra=()):
    """A certificate for ``key`` (self-signed unless ``signer`` is given).

    ``eku=None`` / ``ku=None`` / ``basic=False`` omit that extension; ``extra`` is
    a list of ``(extension, critical)``. The default window brackets FIXED_NOW.
    """
    nb = FIXED_NOW - HOUR if not_before is None else not_before
    na = FIXED_NOW + DAY if not_after is None else not_after
    signer = key if signer is None else signer
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    builder = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(nb)
        .not_valid_after(na)
    )
    if basic:
        builder = builder.add_extension(x509.BasicConstraints(ca=ca, path_length=None),
                                        critical=True)
    if ku is not None:
        builder = builder.add_extension(ku, critical=True)
    if eku:
        builder = builder.add_extension(x509.ExtendedKeyUsage(list(eku)), critical=False)
    for ext, critical in extra:
        builder = builder.add_extension(ext, critical=critical)
    algorithm = None if isinstance(signer, (ed25519.Ed25519PrivateKey,
                                            ed448.Ed448PrivateKey)) else hashes.SHA256()
    return builder.sign(signer, algorithm)


def pem_of(cert: x509.Certificate) -> str:
    return cert.public_bytes(serialization.Encoding.PEM).decode("ascii")


def der_of(cert: x509.Certificate) -> bytes:
    return cert.public_bytes(serialization.Encoding.DER)


def fp_of(cert: x509.Certificate) -> str:
    return hashlib.sha256(der_of(cert)).hexdigest()


def _der_header(buf: bytes, pos: int):
    length = buf[pos + 1]
    if length < 0x80:
        return pos + 2, length
    count = length & 0x7F
    return pos + 2 + count, int.from_bytes(buf[pos + 2:pos + 2 + count], "big")


def _der_encode(tag: int, body: bytes) -> bytes:
    n = len(body)
    if n < 0x80:
        return bytes([tag, n]) + body
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big")
    return bytes([tag, 0x80 | len(raw)]) + raw + body


def with_version(cert: x509.Certificate, version: Optional[bytes]) -> str:
    """PEM of ``cert`` with its version field rewritten: ``None`` drops it (X.509 v1),
    ``b"\\x02\\x01\\x01"`` makes it v2. The signature no longer matches, which is
    irrelevant to a parser."""
    der = der_of(cert)
    start, size = _der_header(der, 0)
    body = der[start:start + size]
    tbs_start, tbs_size = _der_header(body, 0)
    tbs = body[tbs_start:tbs_start + tbs_size]
    rest = body[tbs_start + tbs_size:]
    assert tbs[:5] == b"\xa0\x03\x02\x01\x02"            # explicit [0] version = v3
    new_tbs = (b"\xa0\x03" + version if version else b"") + tbs[5:]
    new = _der_encode(0x30, _der_encode(0x30, new_tbs) + rest)
    b64 = base64.encodebytes(new).decode("ascii")
    return f"-----BEGIN CERTIFICATE-----\n{b64}-----END CERTIFICATE-----\n"


def real_window(*, before=timedelta(minutes=5), after=timedelta(days=30)):
    now = datetime.now(timezone.utc)
    return now - before, now + after


def write_pair(directory: Path, name: str, key, cert: x509.Certificate,
               key_mode=0o600, cert_mode=0o640):
    """Write ``<name>.key`` / ``<name>.crt`` (what ``ensure_identity`` would)."""
    cert_path, key_path = identity.identity_paths(directory, name)
    key_path.write_bytes(key_pem(key))
    key_path.chmod(key_mode)
    cert_path.write_text(pem_of(cert))
    cert_path.chmod(cert_mode)
    return cert_path, key_path


def raw_identity(cert_path: Path, key_path: Path, role: str) -> "identity.Identity":
    """An ``Identity`` built without ``load_identity``'s checks, for presenting
    certificates that the loader would (rightly) refuse -- expired, wrong EKU --
    to a server that must reject them."""
    pem = Path(cert_path).read_text()
    cert = x509.load_pem_x509_certificate(pem.encode())
    return identity.Identity(
        cert_path=Path(cert_path), key_path=Path(key_path), cert_pem=pem,
        fingerprint=fp_of(cert), not_after=cert.not_valid_after_utc, role=role,
    )


def snapshot(directory: Path) -> dict:
    """name -> (bytes, mode, inode) of every file, to prove 'left untouched'."""
    out = {}
    for p in sorted(directory.iterdir()):
        st = p.lstat()
        out[p.name] = (
            p.read_bytes() if stat.S_ISREG(st.st_mode) else None,
            stat.S_IMODE(st.st_mode), st.st_ino,
        )
    return out


def mode_of(path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


def make_tlsdir(tmp_path: Path) -> Path:
    """``<tmp>/config.db.tls`` next to an (empty) ``<tmp>/config.db``."""
    (tmp_path / "config.db").touch()
    return tmp_path / "config.db.tls"


# ==========================================================================
# Helpers: a small TLS server
# ==========================================================================

@dataclasses.dataclass
class Conn:
    ok: bool = False
    peer_der: Optional[bytes] = None
    received: bytes = b""
    error: Optional[BaseException] = None


def _ipv6_loopback_available() -> bool:
    if not socket.has_ipv6:
        return False
    try:
        with socket.socket(socket.AF_INET6) as s:
            s.bind(("::1", 0))
        return True
    except OSError:
        return False


IPV6 = pytest.mark.skipif(not _ipv6_loopback_available(), reason="no IPv6 loopback (::1)")


class TlsServer:
    """Threaded TLS server on loopback, port 0. One thread per connection.

    ``mode``: ``"echo"`` (read one chunk, answer ``b"echo:" + chunk``), ``"http"``
    (read one request, answer ``responder(request)``) or ``"silent"`` (handshake,
    then read until EOF without ever answering). Each finished connection is
    recorded as a ``Conn`` -- including what application bytes it received, which
    is how "the server saw nothing" is asserted.
    """

    def __init__(self, ctx: ssl.SSLContext, *, mode="http",
                 responder: Optional[Callable[[bytes], bytes]] = None,
                 host=LOOPBACK, family=socket.AF_INET, handshake_timeout=5.0):
        self.ctx = ctx
        self.mode = mode
        self.responder = responder or self._ok
        self.handshake_timeout = handshake_timeout
        self.accepted = 0
        self.sni: List[Optional[str]] = []
        if hasattr(ctx, "sni_callback"):
            ctx.sni_callback = self._on_sni
        self._cond = threading.Condition()
        self._conns: List[Conn] = []
        self._threads: List[threading.Thread] = []
        self._stop = threading.Event()
        self._listener = socket.socket(family)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind((host, 0))
        self._listener.listen(16)
        self._listener.settimeout(0.1)
        self.port = self._listener.getsockname()[1]
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True,
                                               name="identity-test-server")
        self._accept_thread.start()

    @staticmethod
    def _ok(_request: bytes) -> bytes:
        return b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"

    def _on_sni(self, _sock, name, _ctx):
        self.sni.append(name)
        return None

    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                raw, _ = self._listener.accept()
            except (socket.timeout, TimeoutError):
                continue
            except OSError:
                return
            self.accepted += 1
            t = threading.Thread(target=self._serve, args=(raw,), daemon=True)
            self._threads.append(t)
            t.start()

    def _serve(self, raw):
        conn = Conn()
        tls = None
        try:
            tls = self.ctx.wrap_socket(raw, server_side=True, do_handshake_on_connect=False)
            tls.settimeout(self.handshake_timeout)
            tls.do_handshake()
            conn.ok = True
            conn.peer_der = tls.getpeercert(binary_form=True)
            self._application(tls, conn)
        except (OSError, ssl.SSLError) as exc:
            conn.error = exc
        finally:
            try:
                (tls or raw).close()
            except OSError:
                pass
            with self._cond:
                self._conns.append(conn)
                self._cond.notify_all()

    def _application(self, tls, conn: Conn):
        if self.mode == "echo":
            data = tls.recv(4096)
            conn.received += data
            if data:
                tls.sendall(b"echo:" + data)
        elif self.mode == "http":
            while b"\r\n\r\n" not in conn.received:
                data = tls.recv(65536)
                if not data:
                    return
                conn.received += data
            tls.sendall(self.responder(conn.received))
        else:  # silent
            while True:
                data = tls.recv(65536)
                if not data:
                    return
                conn.received += data

    def wait_for(self, n: int, timeout: float = IO_TIMEOUT) -> List[Conn]:
        deadline = time.monotonic() + timeout
        with self._cond:
            while len(self._conns) < n:
                left = deadline - time.monotonic()
                if left <= 0:
                    raise AssertionError(f"only {len(self._conns)} connections finished, wanted {n}")
                self._cond.wait(left)
            return list(self._conns)

    def close(self):
        self._stop.set()
        self._accept_thread.join(timeout=5)
        for t in list(self._threads):
            t.join(timeout=10)
        self._listener.close()


@contextmanager
def serving(ctx, **kw):
    srv = TlsServer(ctx, **kw)
    try:
        yield srv
    finally:
        srv.close()


# ==========================================================================
# Session fixtures: generated identities (the real ensure_identity)
# ==========================================================================

@pytest.fixture(scope="session")
def pki_dir(tmp_path_factory) -> Path:
    base = tmp_path_factory.mktemp("identity_pki")
    return make_tlsdir(base)


@pytest.fixture(scope="session")
def server_id(pki_dir) -> "identity.Identity":
    return identity.ensure_identity(pki_dir, "server")


@pytest.fixture(scope="session")
def client_a(pki_dir) -> "identity.Identity":
    return identity.ensure_identity(pki_dir, "client")


@pytest.fixture(scope="session")
def client_b(pki_dir) -> "identity.Identity":
    return identity.ensure_identity(pki_dir, "client", name="client.next")


@pytest.fixture(scope="session")
def crafted(tmp_path_factory):
    """Client identities that ``load_identity`` would refuse, to be presented to a
    server that has them pinned: ``expired`` and ``eku`` (serverAuth only)."""
    d = tmp_path_factory.mktemp("identity_crafted")
    expired_key, eku_key = key_of("ec256:expired"), key_of("ec256:eku")
    nb, na = real_window(before=timedelta(days=3), after=timedelta(days=-2))
    exp_crt, exp_key = write_pair(
        d, "client", expired_key, make_cert(expired_key, cn="expired", not_before=nb, not_after=na))
    nb, na = real_window()
    eku_crt, eku_keyp = write_pair(
        d, "client.next", eku_key,
        make_cert(eku_key, cn="eku", not_before=nb, not_after=na, eku=(SERVER_AUTH,)))
    return types.SimpleNamespace(
        expired=raw_identity(exp_crt, exp_key, "client"),
        eku=raw_identity(eku_crt, eku_keyp, "client"),
    )


# ==========================================================================
# Layout, re-exports, fingerprints
# ==========================================================================

@pytest.mark.unit
class TestLayoutAndConstants:
    def test_tls_dir_is_db_path_plus_suffix(self):
        assert identity.tls_dir("/var/lib/zfsbackup/config.db") == Path(
            "/var/lib/zfsbackup/config.db.tls")
        assert identity.tls_dir(Path("rel/c.db")) == Path("rel/c.db.tls")

    def test_tls_dir_touches_nothing(self, tmp_path):
        target = identity.tls_dir(tmp_path / "config.db")
        assert target == tmp_path / "config.db.tls"
        assert not target.exists()

    @pytest.mark.parametrize("name,crt,key", [
        ("server", "server.crt", "server.key"),
        ("client", "client.crt", "client.key"),
        ("client.next", "client.next.crt", "client.next.key"),
    ])
    def test_identity_paths_returns_cert_then_key(self, tmp_path, name, crt, key):
        assert identity.identity_paths(tmp_path, name) == (tmp_path / crt, tmp_path / key)

    @pytest.mark.parametrize("name", ["", "other", "client.old", "../client", "SERVER"])
    def test_identity_paths_rejects_unknown_names(self, tmp_path, name):
        with pytest.raises(ValueError):
            identity.identity_paths(tmp_path, name)

    def test_layout_constants(self):
        assert (identity.SERVER_KEY, identity.SERVER_CRT) == ("server.key", "server.crt")
        assert (identity.CLIENT_KEY, identity.CLIENT_CRT) == ("client.key", "client.crt")
        assert (identity.CLIENT_NEXT_KEY, identity.CLIENT_NEXT_CRT) == (
            "client.next.key", "client.next.crt")
        assert identity.IDENTITY_LOCK == "identity.lock"
        assert identity.TLS_DIR_MODE == 0o750
        assert identity.KEY_MODE == 0o600
        assert identity.CERT_MODE == 0o640
        assert identity.TOKEN_PREFIX == "zfsb1"
        assert identity.MAX_CLIENT_CERT_PEM == 16 * 1024

    def test_normalize_fingerprint_is_the_config_implementation(self):
        assert identity.normalize_fingerprint is config_normalize_fingerprint

    def test_public_names_are_all_importable(self):
        missing = [n for n in identity.__all__ if not hasattr(identity, n)]
        assert missing == []


@pytest.mark.unit
class TestFingerprints:
    def test_der_is_sha256_lowercase_hex(self):
        assert identity.fingerprint_der(b"abc") == hashlib.sha256(b"abc").hexdigest()
        fp = identity.fingerprint_der(b"")
        assert len(fp) == 64 and fp == fp.lower()

    @pytest.mark.parametrize("wrap", [bytes, bytearray, memoryview])
    def test_der_accepts_bytes_like(self, wrap):
        assert identity.fingerprint_der(wrap(b"abc")) == hashlib.sha256(b"abc").hexdigest()

    @pytest.mark.parametrize("bad", ["abc", None, 5])
    def test_der_rejects_non_bytes(self, bad):
        with pytest.raises(TypeError):
            identity.fingerprint_der(bad)

    def test_pem_matches_der_of_the_certificate(self):
        cert = make_cert(key_of("ec256"))
        expected = hashlib.sha256(der_of(cert)).hexdigest()
        assert identity.fingerprint_pem(pem_of(cert)) == expected
        assert identity.fingerprint_pem(pem_of(cert).encode()) == expected

    def test_pem_of_a_chain_is_the_first_certificate(self):
        a, b = make_cert(key_of("ec256")), make_cert(key_of("ec384"))
        assert identity.fingerprint_pem(pem_of(a) + pem_of(b)) == fp_of(a)

    @pytest.mark.parametrize("bad", ["", "not a pem", "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"])
    def test_pem_rejects_garbage(self, bad):
        with pytest.raises(ValueError):
            identity.fingerprint_pem(bad)

    def test_pem_rejects_a_private_key(self):
        with pytest.raises(ValueError):
            identity.fingerprint_pem(key_pem(key_of("ec256")))

    def test_different_certificates_have_different_fingerprints(self):
        a = make_cert(key_of("ec256"))
        b = make_cert(key_of("ec256"))              # same key, new serial
        assert identity.fingerprint_pem(pem_of(a)) != identity.fingerprint_pem(pem_of(b))

    @pytest.mark.parametrize("variant", [
        lambda fp: fp.upper(),
        lambda fp: ":".join(fp[i:i + 2] for i in range(0, 64, 2)),
        lambda fp: ":".join(fp[i:i + 2] for i in range(0, 64, 2)).upper(),
        lambda fp: "  " + fp + "\n",
    ])
    def test_reexported_normalize_accepts_the_usual_spellings(self, variant):
        fp = hashlib.sha256(b"x").hexdigest()
        assert identity.normalize_fingerprint(variant(fp)) == fp

    @pytest.mark.parametrize("bad", ["", "abc", "g" * 64, "a" * 63, "a" * 65, None, 5])
    def test_reexported_normalize_rejects_the_rest(self, bad):
        with pytest.raises(ValueError):
            identity.normalize_fingerprint(bad)


# ==========================================================================
# Tokens
# ==========================================================================

_B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


@pytest.mark.unit
class TestTokens:
    SECRET = bytes(range(32))

    def good(self, fp=FP_A, secret=None):
        return f"zfsb1.{fp}.{_b64url(self.SECRET if secret is None else secret)}"

    def test_make_token_shape(self):
        token, digest = identity.make_token(FP_A)
        prefix, fp, secret = str(token).split(".")
        assert prefix == "zfsb1" and fp == FP_A
        assert len(secret) == 43 and set(secret) <= set(_B64)
        assert len(digest) == 64 and digest == digest.lower()

    def test_round_trip_and_hash_binding(self):
        token, digest = identity.make_token(FP_B)
        fp, secret = identity.parse_token(token)
        assert fp == FP_B
        assert isinstance(secret, bytes) and len(secret) == 32
        assert identity.secret_hash(secret) == digest == hashlib.sha256(bytes(secret)).hexdigest()

    def test_tokens_are_unique(self):
        seen = {str(identity.make_token(FP_A)[0]) for _ in range(20)}
        assert len(seen) == 20

    def test_make_token_normalises_the_server_fingerprint(self):
        colon = ":".join(FP_A[i:i + 2] for i in range(0, 64, 2)).upper()
        token, _ = identity.make_token(colon)
        assert str(token).split(".")[1] == FP_A

    @pytest.mark.parametrize("bad", ["", "xyz", "a" * 63, None])
    def test_make_token_rejects_a_bad_server_fingerprint(self, bad):
        with pytest.raises(ValueError):
            identity.make_token(bad)

    def test_the_token_value_prints_but_its_repr_is_redacted(self):
        token, _ = identity.make_token(FP_A)
        secret_part = str(token).split(".")[2]
        assert isinstance(token, str)
        assert secret_part in str(token) and secret_part in f"{token}"
        assert repr(token) == "<redacted>"
        assert secret_part not in f"{token!r}"
        assert secret_part not in repr([token]) and secret_part not in repr({"t": token})

    def test_the_parsed_secret_never_shows_in_repr_or_str(self):
        _, secret = identity.parse_token(self.good())
        raw_hex = bytes(secret).hex()
        for rendering in (repr(secret), str(secret), f"{secret}", f"{secret!r}", repr([secret])):
            assert raw_hex not in rendering and _b64url(bytes(secret)) not in rendering
        assert "32" in repr(secret)
        assert bytes(secret) == self.SECRET

    def test_surrounding_whitespace_is_ignored(self):
        for text in (self.good() + "\n", "  " + self.good() + "\r\n", "\t" + self.good()):
            assert identity.parse_token(text)[0] == FP_A

    def test_a_secret_with_padding_bits_in_every_alphabet_position_round_trips(self):
        for raw in (b"\x00" * 32, b"\xff" * 32, bytes(range(32)), bytes(range(224, 256))):
            assert bytes(identity.parse_token(self.good(secret=raw))[1]) == raw

    def _bad_tokens(self):
        good = self.good()
        secret = _b64url(self.SECRET)
        canon_last = secret[-1]
        # the last character carries only 4 payload bits: setting a spare bit is
        # a different spelling of the same secret
        alt_last = _B64[_B64.index(canon_last) | 1]
        assert alt_last != canon_last
        return {
            "empty": "",
            "blank": "   ",
            "not a token": "hello world",
            "other prefix": good.replace("zfsb1", "zfsb2", 1),
            "no prefix": good[len("zfsb1."):],
            "missing secret": f"zfsb1.{FP_A}",
            "missing fingerprint": f"zfsb1.{secret}",
            "extra part": good + ".extra",
            "uppercase fingerprint": f"zfsb1.{FP_A.upper()}.{secret}",
            "colon fingerprint": f"zfsb1.{':'.join(FP_A[i:i + 2] for i in range(0, 64, 2))}.{secret}",
            "short fingerprint": f"zfsb1.{FP_A[:-1]}.{secret}",
            "long fingerprint": f"zfsb1.{FP_A}a.{secret}",
            "non-hex fingerprint": f"zfsb1.{'g' * 64}.{secret}",
            "short secret": f"zfsb1.{FP_A}.{secret[:-1]}",
            "long secret": f"zfsb1.{FP_A}.{secret}A",
            "padded secret": f"zfsb1.{FP_A}.{secret}=",
            "standard alphabet": f"zfsb1.{FP_A}.{secret[:-2]}+/",
            "non-canonical spelling": f"zfsb1.{FP_A}.{secret[:-1]}{alt_last}",
            "inner whitespace": f"zfsb1.{FP_A}.{secret[:20]} {secret[21:]}",
            "inner newline": f"zfsb1.{FP_A}\n.{secret}",
            "unicode digit": f"zfsb1.{FP_A[:-1]}٣.{secret}",
            "non-ascii secret": f"zfsb1.{FP_A}.{secret[:-1]}é",
        }

    def test_malformed_tokens_are_rejected(self):
        for label, text in self._bad_tokens().items():
            with pytest.raises(ValueError):
                identity.parse_token(text)
            assert label  # keeps the case name in the traceback locals

    @pytest.mark.parametrize("bad", [None, 5, b"zfsb1.x.y", ["zfsb1"]])
    def test_non_text_is_rejected(self, bad):
        with pytest.raises(ValueError):
            identity.parse_token(bad)

    def test_rejection_never_echoes_the_secret_or_any_part_of_the_token(self):
        secret = _b64url(self.SECRET)
        for label, text in self._bad_tokens().items():
            with pytest.raises(ValueError) as info:
                identity.parse_token(text)
            exc = info.value
            rendered = " ".join([str(exc), repr(exc), *map(str, exc.args)])
            assert secret not in rendered, label
            assert secret[:12] not in rendered and secret[-12:] not in rendered, label
            assert FP_A not in rendered, label
            assert text.strip() not in rendered or not text.strip(), label
            # no chained traceback context that could carry the input
            assert exc.__cause__ is None, label
            assert exc.__suppress_context__ or exc.__context__ is None, label

    def test_a_wrong_prefix_is_told_apart_from_a_damaged_token(self):
        with pytest.raises(ValueError, match="not a zfsbackup enrollment token"):
            identity.parse_token("hello")
        with pytest.raises(ValueError, match="malformed"):
            identity.parse_token(f"zfsb1.{FP_A}.short")

    def test_secret_hash_known_value_and_types(self):
        assert identity.secret_hash(b"abc") == hashlib.sha256(b"abc").hexdigest()
        assert identity.secret_hash(bytearray(b"abc")) == hashlib.sha256(b"abc").hexdigest()
        with pytest.raises(TypeError):
            identity.secret_hash("abc")

    def test_parse_returns_the_hash_the_server_stores(self):
        """The server keeps only sha256(secret); the client derives the same value."""
        token, stored = identity.make_token(FP_C)
        assert identity.secret_hash(identity.parse_token(token)[1]) == stored


# ==========================================================================
# Payloads and proof of possession
# ==========================================================================

@pytest.mark.unit
class TestPayloads:
    def test_enrollment_payload_exact_bytes(self):
        assert identity.enrollment_payload(FP_A, "alice", FP_B, FP_C) == (
            f"zfsbackup-enroll-v1\nserver={FP_A}\nclient_id=alice\nclient={FP_B}\ntoken={FP_C}\n"
        ).encode()

    def test_rotation_payload_exact_bytes(self):
        assert identity.rotation_payload(FP_A, "alice", FP_B, FP_C) == (
            f"zfsbackup-rotate-v1\nserver={FP_A}\nclient_id=alice\nold={FP_B}\nnew={FP_C}\n"
        ).encode()

    def test_payloads_are_deterministic_ascii_bytes(self):
        p = identity.enrollment_payload(FP_A, "a.b_c-1", FP_B, FP_C)
        assert isinstance(p, bytes) and p == identity.enrollment_payload(FP_A, "a.b_c-1", FP_B, FP_C)
        p.decode("ascii")

    def test_the_two_payload_kinds_never_coincide(self):
        e = identity.enrollment_payload(FP_A, "x", FP_B, FP_C)
        r = identity.rotation_payload(FP_A, "x", FP_B, FP_C)
        assert e != r and e.split(b"\n")[0] != r.split(b"\n")[0]

    BAD_FPS = [
        "", "AB" * 32, "ab" * 31, "ab" * 33, "zz" * 32, "ab" * 32 + "\n", "\n" + "ab" * 32,
        ":".join(["ab"] * 32), " " + "ab" * 32, None, 5, b"ab" * 32,
    ]
    BAD_IDS = [
        "", ".hidden", "a b", "a\nb", "alice\nserver=" + FP_B, "alice\r", "a/b", "a:b",
        "é", "x" * 65, "alice ", None, 5, b"alice",
    ]

    @pytest.mark.parametrize("slot", [0, 2, 3])
    @pytest.mark.parametrize("bad", BAD_FPS, ids=repr)
    def test_enrollment_rejects_a_non_canonical_fingerprint_anywhere(self, slot, bad):
        args = [FP_A, "alice", FP_B, FP_C]
        args[slot] = bad
        with pytest.raises(ValueError):
            identity.enrollment_payload(*args)

    @pytest.mark.parametrize("slot", [0, 2, 3])
    @pytest.mark.parametrize("bad", BAD_FPS, ids=repr)
    def test_rotation_rejects_a_non_canonical_fingerprint_anywhere(self, slot, bad):
        args = [FP_A, "alice", FP_B, FP_C]
        args[slot] = bad
        with pytest.raises(ValueError):
            identity.rotation_payload(*args)

    @pytest.mark.parametrize("bad", BAD_IDS, ids=repr)
    def test_client_id_grammar_and_newline_injection(self, bad):
        with pytest.raises(ValueError):
            identity.enrollment_payload(FP_A, bad, FP_B, FP_C)
        with pytest.raises(ValueError):
            identity.rotation_payload(FP_A, bad, FP_B, FP_C)

    def test_the_longest_valid_client_id_is_accepted(self):
        identity.enrollment_payload(FP_A, "x" * 64, FP_B, FP_C)
        identity.rotation_payload(FP_A, "A.b_c-9" + "z" * 57, FP_B, FP_C)


PRIVATE_KINDS = ["ec256", "ec384", "ec521", "rsa2048", "ed25519"]


@pytest.mark.unit
class TestProofOfPossession:
    """The client signs a payload with its new key; the server verifies it against
    the certificate it is being asked to pin."""

    def material(self, kind, client_id="alice"):
        key = key_of(kind)
        cert = make_cert(key)
        pem = pem_of(cert)
        client_fp = fp_of(cert)
        _, token_hash = identity.make_token(FP_A)
        payload = identity.enrollment_payload(FP_A, client_id, client_fp, token_hash)
        return key, pem, client_fp, token_hash, payload

    @pytest.mark.parametrize("kind", PRIVATE_KINDS)
    def test_sign_and_verify_round_trip(self, kind):
        key, pem, _, _, payload = self.material(kind)
        sig = identity.sign_payload(key, payload)
        assert isinstance(sig, bytes) and sig
        assert identity.verify_payload(pem, payload, sig) is True
        assert identity.verify_payload(pem.encode(), payload, sig) is True

    @pytest.mark.parametrize("kind", PRIVATE_KINDS)
    def test_signing_from_a_key_file_matches_the_key_object(self, kind, tmp_path):
        key, pem, _, _, payload = self.material(kind)
        path = tmp_path / "k.pem"
        path.write_bytes(key_pem(key))
        path.chmod(0o600)
        for ref in (path, str(path)):
            assert identity.verify_payload(pem, payload, identity.sign_payload(ref, payload))

    def test_algorithms_follow_the_key_type(self):
        payload = b"payload"
        ec_sig = identity.sign_payload(key_of("ec256"), payload)
        key_of("ec256").public_key().verify(ec_sig, payload, ec.ECDSA(hashes.SHA256()))
        rsa_sig = identity.sign_payload(key_of("rsa2048"), payload)
        key_of("rsa2048").public_key().verify(rsa_sig, payload, padding.PKCS1v15(), hashes.SHA256())
        ed_sig = identity.sign_payload(key_of("ed25519"), payload)
        assert len(ed_sig) == 64
        key_of("ed25519").public_key().verify(ed_sig, payload)

    @pytest.mark.parametrize("kind", PRIVATE_KINDS)
    def test_a_relay_to_another_server_fails(self, kind):
        """The signature binds the server: a signature collected by server X is of
        no use when X replays it at server Y."""
        key, pem, client_fp, token_hash, payload = self.material(kind)
        sig = identity.sign_payload(key, payload)
        as_seen_by_y = identity.enrollment_payload(FP_D, "alice", client_fp, token_hash)
        assert identity.verify_payload(pem, as_seen_by_y, sig) is False

    @pytest.mark.parametrize("kind", PRIVATE_KINDS)
    def test_a_rewritten_client_id_fails(self, kind):
        key, pem, client_fp, token_hash, payload = self.material(kind)
        sig = identity.sign_payload(key, payload)
        rewritten = identity.enrollment_payload(FP_A, "mallory", client_fp, token_hash)
        assert identity.verify_payload(pem, rewritten, sig) is False

    @pytest.mark.parametrize("kind", PRIVATE_KINDS)
    def test_a_swapped_certificate_fails(self, kind):
        key, pem, client_fp, token_hash, payload = self.material(kind)
        sig = identity.sign_payload(key, payload)
        other_pem = pem_of(make_cert(key_of("ec256:other")))
        other_fp = identity.fingerprint_pem(other_pem)
        # the verifier derives the payload from the certificate it was handed
        swapped_payload = identity.enrollment_payload(FP_A, "alice", other_fp, token_hash)
        assert identity.verify_payload(other_pem, swapped_payload, sig) is False
        # and even the original payload does not verify under the other key
        assert identity.verify_payload(other_pem, payload, sig) is False

    @pytest.mark.parametrize("kind", PRIVATE_KINDS)
    def test_a_token_hash_mismatch_fails(self, kind):
        key, pem, client_fp, token_hash, payload = self.material(kind)
        sig = identity.sign_payload(key, payload)
        _, other_hash = identity.make_token(FP_A)
        assert other_hash != token_hash
        assert identity.verify_payload(
            pem, identity.enrollment_payload(FP_A, "alice", client_fp, other_hash), sig) is False

    @pytest.mark.parametrize("kind", PRIVATE_KINDS)
    def test_a_rotation_payload_cannot_stand_in_for_an_enrollment_one(self, kind):
        key, pem, client_fp, token_hash, payload = self.material(kind)
        rotation = identity.rotation_payload(FP_A, "alice", client_fp, FP_C)
        rot_sig = identity.sign_payload(key, rotation)
        assert identity.verify_payload(pem, rotation, rot_sig) is True
        assert identity.verify_payload(pem, payload, rot_sig) is False
        enr_sig = identity.sign_payload(key, payload)
        assert identity.verify_payload(pem, rotation, enr_sig) is False
        # same field values in both: only the version/purpose line differs
        same_fields = identity.enrollment_payload(FP_A, "alice", client_fp, FP_C)
        assert identity.verify_payload(pem, same_fields, rot_sig) is False

    @pytest.mark.parametrize("kind", PRIVATE_KINDS)
    def test_garbage_signatures_fail_without_raising(self, kind):
        key, pem, _, _, payload = self.material(kind)
        good = identity.sign_payload(key, payload)
        flipped = bytes([good[0] ^ 1]) + good[1:]
        tail_flipped = good[:-1] + bytes([good[-1] ^ 1])
        for bad in (b"", b"x", b"\x00" * 64, b"\xff" * 72, b"A" * 4096, good[:-1], good + b"\x00",
                    flipped, tail_flipped, good[::-1]):
            assert identity.verify_payload(pem, payload, bad) is False

    @pytest.mark.parametrize("kind", PRIVATE_KINDS)
    def test_a_modified_payload_fails(self, kind):
        key, pem, _, _, payload = self.material(kind)
        sig = identity.sign_payload(key, payload)
        for tampered in (payload + b"\n", payload[:-1], payload.replace(b"alice", b"alicf"), b"", payload * 2):
            assert identity.verify_payload(pem, tampered, sig) is False

    def test_a_signature_from_a_different_key_type_fails(self):
        payload = b"payload"
        sigs = {k: identity.sign_payload(key_of(k), payload) for k in PRIVATE_KINDS}
        pems = {k: pem_of(make_cert(key_of(k))) for k in PRIVATE_KINDS}
        for signer in PRIVATE_KINDS:
            for verifier in PRIVATE_KINDS:
                ok = identity.verify_payload(pems[verifier], payload, sigs[signer])
                assert ok is (signer == verifier), (signer, verifier)

    def test_wrong_hash_and_wrong_rsa_padding_fail(self):
        payload = b"payload"
        pem = pem_of(make_cert(key_of("ec256")))
        sha384 = key_of("ec256").sign(payload, ec.ECDSA(hashes.SHA384()))
        assert identity.verify_payload(pem, payload, sha384) is False
        rsa_key = key_of("rsa2048")
        rsa_pem = pem_of(make_cert(rsa_key))
        pss = rsa_key.sign(payload, padding.PSS(padding.MGF1(hashes.SHA256()), 32), hashes.SHA256())
        assert identity.verify_payload(rsa_pem, payload, pss) is False

    @pytest.mark.parametrize("bad_cert", [
        "", b"", "not a pem", "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        "éè", None, 5, [],
    ], ids=repr)
    def test_a_hostile_certificate_argument_gives_false(self, bad_cert):
        assert identity.verify_payload(bad_cert, b"payload", b"sig") is False

    def test_a_private_key_pem_is_not_a_certificate(self):
        assert identity.verify_payload(key_pem(key_of("ec256")), b"p", b"s") is False

    @pytest.mark.parametrize("bad", [None, "text", 5, [b"x"]], ids=repr)
    def test_a_hostile_signature_or_payload_type_gives_false(self, bad):
        pem = pem_of(make_cert(key_of("ec256")))
        assert identity.verify_payload(pem, b"payload", bad) is False
        assert identity.verify_payload(pem, bad, b"sig") is False

    def test_an_unsupported_certificate_key_type_gives_false(self):
        ed448_key = key_of("ed448")
        pem = pem_of(make_cert(ed448_key))
        sig = ed448_key.sign(b"payload")
        assert identity.verify_payload(pem, b"payload", sig) is False

    def test_signing_with_an_unsupported_key_is_a_value_error(self):
        with pytest.raises(ValueError):
            identity.sign_payload(key_of("ed448"), b"payload")

    def test_signing_from_a_key_file_applies_the_permission_checks(self, tmp_path):
        path = tmp_path / "k.pem"
        path.write_bytes(key_pem(key_of("ec256")))
        path.chmod(0o644)
        with pytest.raises(identity.IdentityError):
            identity.sign_payload(path, b"payload")
        path.chmod(0o600)
        assert identity.sign_payload(path, b"payload")
        with pytest.raises(identity.IdentityError):
            identity.sign_payload(tmp_path / "missing.pem", b"payload")


# ==========================================================================
# check_client_cert: the acceptance policy before a certificate may be pinned
# ==========================================================================

def accept(cert_or_pem, now=FIXED_NOW):
    pem = cert_or_pem if isinstance(cert_or_pem, (str, bytes)) else pem_of(cert_or_pem)
    return identity.check_client_cert(pem, now=now)


def reject(cert_or_pem, match, now=FIXED_NOW):
    pem = cert_or_pem if isinstance(cert_or_pem, (str, bytes)) else pem_of(cert_or_pem)
    with pytest.raises(ValueError, match=match):
        identity.check_client_cert(pem, now=now)


@pytest.mark.unit
class TestCheckClientCertAccepts:
    @pytest.mark.parametrize("kind", ["ec256", "ec384", "ec521", "rsa2048", "ed25519"])
    def test_supported_key_types(self, kind):
        cert = make_cert(key_of(kind), cn="alice")
        info = accept(cert)
        assert info.fingerprint == fp_of(cert)
        assert info.not_after == FIXED_NOW + DAY
        assert info.subject == "CN=alice"

    def test_result_is_a_certinfo_and_clientcertinfo_is_the_same_type(self):
        info = accept(make_cert(key_of("ec256")))
        assert isinstance(info, identity.CertInfo) and identity.ClientCertInfo is identity.CertInfo
        assert info.not_after.tzinfo is not None

    def test_bytes_and_text_input_agree(self):
        cert = make_cert(key_of("ec256"))
        assert accept(pem_of(cert)) == accept(pem_of(cert).encode())

    def test_fingerprint_matches_fingerprint_pem(self):
        cert = make_cert(key_of("ec256"))
        assert accept(cert).fingerprint == identity.fingerprint_pem(pem_of(cert))

    @pytest.mark.parametrize("eku", [None, (CLIENT_AUTH,), (CLIENT_AUTH, SERVER_AUTH),
                                     (SERVER_AUTH, CLIENT_AUTH, ExtendedKeyUsageOID.EMAIL_PROTECTION)])
    def test_eku_absent_or_containing_client_auth(self, eku):
        accept(make_cert(key_of("ec256"), eku=eku))

    def test_no_key_usage_and_no_basic_constraints_at_all(self):
        accept(make_cert(key_of("ec256"), ku=None, basic=False))

    def test_key_usage_with_digital_signature_plus_others(self):
        accept(make_cert(key_of("ec256"), ku=ku_ext(digital_signature=True, key_agreement=True)))

    def test_a_recognised_critical_extension_is_fine(self):
        san = x509.SubjectAlternativeName([x509.DNSName("client.example")])
        accept(make_cert(key_of("ec256"), extra=[(san, True)]))

    def test_an_unrecognised_non_critical_extension_is_fine(self):
        odd = x509.UnrecognizedExtension(x509.ObjectIdentifier("1.3.6.1.4.1.99999.1"), b"\x05\x00")
        accept(make_cert(key_of("ec256"), extra=[(odd, False)]))

    def test_surrounding_whitespace_and_crlf_line_endings(self):
        pem = pem_of(make_cert(key_of("ec256")))
        accept("\n\n" + pem + "\n  \n")
        accept(pem.replace("\n", "\r\n"))

    # --- validity window -------------------------------------------------

    def test_not_before_up_to_five_minutes_ahead_is_tolerated(self):
        accept(make_cert(key_of("ec256"), not_before=FIXED_NOW + timedelta(minutes=4, seconds=59),
                         not_after=FIXED_NOW + DAY))
        accept(make_cert(key_of("ec256"), not_before=FIXED_NOW + timedelta(minutes=5),
                         not_after=FIXED_NOW + DAY))

    def test_valid_until_the_last_second(self):
        cert = make_cert(key_of("ec256"), not_before=FIXED_NOW - DAY, not_after=FIXED_NOW)
        accept(cert, now=FIXED_NOW)

    def test_naive_now_is_taken_as_utc(self):
        cert = make_cert(key_of("ec256"))
        accept(cert, now=FIXED_NOW.replace(tzinfo=None))
        with pytest.raises(ValueError, match="expired"):
            identity.check_client_cert(pem_of(cert), now=(FIXED_NOW + 2 * DAY).replace(tzinfo=None))

    def test_now_defaults_to_the_real_clock(self):
        nb, na = real_window()
        info = identity.check_client_cert(pem_of(make_cert(key_of("ec256"), not_before=nb, not_after=na)))
        assert info.not_after == na.replace(microsecond=0)

    def test_size_limit_is_inclusive(self):
        pem = pem_of(make_cert(key_of("ec256")))
        padded = pem + " " * (identity.MAX_CLIENT_CERT_PEM - len(pem))
        assert len(padded) == identity.MAX_CLIENT_CERT_PEM
        accept(padded)


@pytest.mark.unit
class TestCheckClientCertRejects:
    @pytest.mark.parametrize("junk", ["", "   \n", "hello", "-----BEGIN CERTIFICATE-----",
                                      "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n"],
                             ids=repr)
    def test_not_a_certificate(self, junk):
        with pytest.raises(ValueError):
            identity.check_client_cert(junk, now=FIXED_NOW)

    def test_corrupted_body(self):
        pem = pem_of(make_cert(key_of("ec256")))
        lines = pem.splitlines()
        del lines[2]                      # a base64 line goes missing: the DER is truncated
        reject("\n".join(lines) + "\n", "cannot parse")

    def test_der_bytes_are_not_pem(self):
        with pytest.raises(ValueError):
            identity.check_client_cert(der_of(make_cert(key_of("ec256"))), now=FIXED_NOW)

    def test_two_certificates(self):
        a, b = make_cert(key_of("ec256")), make_cert(key_of("ec384"))
        reject(pem_of(a) + pem_of(b), "exactly one PEM certificate")

    def test_a_certificate_followed_by_its_private_key(self):
        """The classic mistake: pasting a combined cert+key file. The key must never
        be accepted into the database."""
        pem = pem_of(make_cert(key_of("ec256"))) + key_pem(key_of("ec256")).decode()
        reject(pem, "exactly one PEM certificate")

    def test_a_private_key_alone(self):
        reject(key_pem(key_of("ec256")).decode(), "exactly one PEM certificate")

    def test_text_around_the_block(self):
        pem = pem_of(make_cert(key_of("ec256")))
        reject("Subject: x\n" + pem, "exactly one PEM certificate")
        reject(pem + "trailing junk\n", "exactly one PEM certificate")

    @pytest.mark.parametrize("label", ["TRUSTED CERTIFICATE", "X509 CERTIFICATE", "CERTIFICATE REQUEST"])
    def test_other_pem_labels(self, label):
        pem = pem_of(make_cert(key_of("ec256"))).replace("CERTIFICATE", label)
        with pytest.raises(ValueError):
            identity.check_client_cert(pem, now=FIXED_NOW)

    def test_oversized_input_even_if_it_is_a_valid_certificate_plus_blanks(self):
        pem = pem_of(make_cert(key_of("ec256")))
        reject(pem + " " * (identity.MAX_CLIENT_CERT_PEM - len(pem) + 1), "limit is")
        reject("A" * 100_000, "limit is")

    def test_non_ascii_text(self):
        reject(pem_of(make_cert(key_of("ec256"))) + "é", "ASCII")

    @pytest.mark.parametrize("bad", [None, 5, ["x"], {"a": 1}], ids=repr)
    def test_not_text_or_bytes(self, bad):
        with pytest.raises(ValueError, match="must be text"):
            identity.check_client_cert(bad, now=FIXED_NOW)

    @pytest.mark.parametrize("version", [None, b"\x02\x01\x01"], ids=["v1", "v2"])
    def test_only_x509_v3(self, version):
        reject(with_version(make_cert(key_of("ec256"), ku=None, basic=False, eku=None), version),
               "version 3")

    # --- validity window -------------------------------------------------

    def test_not_yet_valid_beyond_the_slack(self):
        reject(make_cert(key_of("ec256"), not_before=FIXED_NOW + timedelta(minutes=5, seconds=1),
                         not_after=FIXED_NOW + DAY), "not valid until")
        reject(make_cert(key_of("ec256"), not_before=FIXED_NOW + DAY,
                         not_after=FIXED_NOW + 2 * DAY), "not valid until")

    def test_expired_even_by_one_second(self):
        reject(make_cert(key_of("ec256"), not_before=FIXED_NOW - DAY,
                         not_after=FIXED_NOW - timedelta(seconds=1)), "expired")

    # --- extended key usage / CA / key usage ------------------------------

    @pytest.mark.parametrize("eku", [(SERVER_AUTH,), (ExtendedKeyUsageOID.EMAIL_PROTECTION,),
                                     (ExtendedKeyUsageOID.CODE_SIGNING, SERVER_AUTH)])
    def test_eku_without_client_auth(self, eku):
        reject(make_cert(key_of("ec256"), eku=eku), "clientAuth")

    def test_basic_constraints_ca_true(self):
        reject(make_cert(key_of("ec256"), ca=True, ku=ku_ext(digital_signature=True, key_cert_sign=True)),
               "CA certificate")

    def test_a_ca_without_any_key_usage_extension(self):
        reject(make_cert(key_of("ec256"), ca=True, ku=None), "CA certificate")

    def test_key_cert_sign_makes_it_a_ca_even_with_ca_false(self):
        """OpenSSL treats keyCertSign as a CA too; a pinned 'leaf' with it would
        let everything it signs pass the handshake."""
        reject(make_cert(key_of("ec256"), ca=False,
                         ku=ku_ext(digital_signature=True, key_cert_sign=True)), "keyCertSign")

    def test_ca_certificate_signed_by_itself_with_all_client_credentials_otherwise_fine(self):
        reject(make_cert(key_of("ed25519"), ca=True, ku=ku_ext(key_cert_sign=True, crl_sign=True),
                         eku=None), "CA certificate")

    @pytest.mark.parametrize("ku", [ku_ext(key_encipherment=True), ku_ext(key_agreement=True),
                                    ku_ext(content_commitment=True), ku_ext(crl_sign=True)])
    def test_key_usage_present_without_digital_signature(self, ku):
        reject(make_cert(key_of("ec256"), ku=ku), "digitalSignature")

    def test_unrecognised_critical_extension(self):
        odd = x509.UnrecognizedExtension(x509.ObjectIdentifier("1.3.6.1.4.1.99999.1"), b"\x05\x00")
        reject(make_cert(key_of("ec256"), extra=[(odd, True)]),
               r"unsupported critical extension 1\.3\.6\.1\.4\.1\.99999\.1")

    # --- key types ---------------------------------------------------------

    def test_rsa_below_2048(self):
        reject(make_cert(key_of("rsa1024")), "1024 bits")

    @pytest.mark.parametrize("kind", ["k1", "p224", "bp256"])
    def test_ec_curves_outside_p256_p384_p521(self, kind):
        reject(make_cert(key_of(kind)), "only P-256")

    def test_ed448_and_other_key_types(self):
        reject(make_cert(key_of("ed448")), "not accepted")


# ==========================================================================
# load_identity / peek_identity
# ==========================================================================

NOW = dict(now=FIXED_NOW)


def loadable(tmp_path, *, kind="ec256", role="client", **cert_kw):
    """A valid pair (window around FIXED_NOW) on disk; -> (cert_path, key_path, cert)."""
    key = key_of(kind)
    eku = (CLIENT_AUTH,) if role == "client" else (SERVER_AUTH,)
    cert_kw.setdefault("eku", eku)
    cert = make_cert(key, **cert_kw)
    cert_path, key_path = write_pair(tmp_path, "client" if role == "client" else "server", key, cert)
    return cert_path, key_path, cert


@pytest.mark.unit
class TestLoadIdentity:
    @pytest.mark.parametrize("kind", ["ec256", "ec384", "rsa2048", "ed25519"])
    @pytest.mark.parametrize("role", ["client", "server"])
    def test_valid_pairs_for_every_supported_key_type(self, tmp_path, kind, role):
        cert_path, key_path, cert = loadable(tmp_path, kind=kind, role=role)
        ident = identity.load_identity(cert_path, key_path, role, **NOW)
        assert ident.fingerprint == fp_of(cert)
        assert ident.role == role
        assert ident.cert_path == cert_path and ident.key_path == key_path
        assert ident.cert_pem == pem_of(cert)
        assert ident.not_after == FIXED_NOW + DAY and ident.not_after.tzinfo is not None

    def test_string_paths_are_accepted(self, tmp_path):
        cert_path, key_path, _ = loadable(tmp_path)
        ident = identity.load_identity(str(cert_path), str(key_path), "client", **NOW)
        assert isinstance(ident.cert_path, Path) and isinstance(ident.key_path, Path)

    def test_the_identity_is_frozen_and_its_repr_omits_the_certificate(self, tmp_path):
        cert_path, key_path, cert = loadable(tmp_path)
        ident = identity.load_identity(cert_path, key_path, "client", **NOW)
        with pytest.raises(dataclasses.FrozenInstanceError):
            ident.fingerprint = "x"
        assert "BEGIN CERTIFICATE" not in repr(ident)
        assert ident.fingerprint in repr(ident)

    @pytest.mark.parametrize("role", ["", "Server", "both", None])
    def test_unknown_role(self, tmp_path, role):
        cert_path, key_path, _ = loadable(tmp_path)
        with pytest.raises(ValueError):
            identity.load_identity(cert_path, key_path, role, **NOW)

    # --- the key file ------------------------------------------------------

    @pytest.mark.parametrize("mode", [0o600, 0o400])
    def test_private_key_modes_that_are_fine(self, tmp_path, mode, caplog):
        cert_path, key_path, _ = loadable(tmp_path)
        key_path.chmod(mode)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.identity"):
            identity.load_identity(cert_path, key_path, "client", **NOW)
        assert caplog.records == []

    @pytest.mark.parametrize("mode", [0o640, 0o440])
    def test_a_group_readable_key_loads_with_a_warning(self, tmp_path, mode, caplog):
        cert_path, key_path, _ = loadable(tmp_path)
        key_path.chmod(mode)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.identity"):
            identity.load_identity(cert_path, key_path, "client", **NOW)
        assert any("readable by its group" in r.getMessage() for r in caplog.records)

    @pytest.mark.parametrize("mode", [0o660, 0o620, 0o602, 0o604, 0o644, 0o666, 0o606, 0o664])
    def test_a_key_others_can_write_or_the_world_can_read_is_refused(self, tmp_path, mode):
        cert_path, key_path, _ = loadable(tmp_path)
        key_path.chmod(mode)
        with pytest.raises(identity.IdentityError, match="chmod 600") as info:
            identity.load_identity(cert_path, key_path, "client", **NOW)
        assert info.value.path == key_path

    def test_a_symlinked_key_is_followed_and_the_target_is_checked(self, tmp_path):
        cert_path, key_path, _ = loadable(tmp_path)
        real = tmp_path / "real.key"
        key_path.rename(real)
        key_path.symlink_to(real)
        identity.load_identity(cert_path, key_path, "client", **NOW)
        real.chmod(0o644)
        with pytest.raises(identity.IdentityError, match="readable by everyone"):
            identity.load_identity(cert_path, key_path, "client", **NOW)

    def test_a_symlinked_certificate_is_followed(self, tmp_path):
        cert_path, key_path, cert = loadable(tmp_path)
        real = tmp_path / "real.crt"
        cert_path.rename(real)
        cert_path.symlink_to(real)
        assert identity.load_identity(cert_path, key_path, "client", **NOW).fingerprint == fp_of(cert)

    def test_the_key_must_be_a_regular_file(self, tmp_path):
        cert_path, key_path, _ = loadable(tmp_path)
        key_path.unlink()
        key_path.mkdir()
        with pytest.raises(identity.IdentityError, match="not a regular file"):
            identity.load_identity(cert_path, key_path, "client", **NOW)

    def test_a_dangling_key_symlink_is_a_missing_key(self, tmp_path):
        cert_path, key_path, _ = loadable(tmp_path)
        key_path.unlink()
        key_path.symlink_to(tmp_path / "nowhere")
        with pytest.raises(identity.IdentityError, match="does not exist"):
            identity.load_identity(cert_path, key_path, "client", **NOW)

    def test_missing_files(self, tmp_path):
        cert_path, key_path, _ = loadable(tmp_path)
        key_path.unlink()
        with pytest.raises(identity.IdentityError, match="private key .* does not exist"):
            identity.load_identity(cert_path, key_path, "client", **NOW)
        cert_path2, key_path2, _ = loadable(tmp_path)
        cert_path2.unlink()
        with pytest.raises(identity.IdentityError, match="certificate .* does not exist"):
            identity.load_identity(cert_path2, key_path2, "client", **NOW)

    @pytest.mark.skipif(os.geteuid() == 0, reason="root reads everything")
    def test_an_unreadable_key(self, tmp_path):
        cert_path, key_path, _ = loadable(tmp_path)
        key_path.chmod(0)
        try:
            with pytest.raises(identity.IdentityError, match="permission denied"):
                identity.load_identity(cert_path, key_path, "client", **NOW)
        finally:
            key_path.chmod(0o600)

    def test_garbage_files(self, tmp_path):
        cert_path, key_path, _ = loadable(tmp_path)
        key_path.write_text("garbage")
        with pytest.raises(identity.IdentityError, match="PEM private key"):
            identity.load_identity(cert_path, key_path, "client", **NOW)
        cert_path2, key_path2, _ = loadable(tmp_path)
        cert_path2.write_text("garbage")
        with pytest.raises(identity.IdentityError, match="PEM certificate"):
            identity.load_identity(cert_path2, key_path2, "client", **NOW)

    def test_an_encrypted_key(self, tmp_path):
        cert_path, key_path, _ = loadable(tmp_path)
        key_path.write_bytes(key_of("ec256").private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
            serialization.BestAvailableEncryption(b"pw")))
        with pytest.raises(identity.IdentityError, match="encrypted"):
            identity.load_identity(cert_path, key_path, "client", **NOW)

    def test_an_unsupported_key_type(self, tmp_path):
        cert_path, key_path, _ = loadable(tmp_path, kind="ed448")
        with pytest.raises(identity.IdentityError, match="only EC, RSA and Ed25519"):
            identity.load_identity(cert_path, key_path, "client", **NOW)

    def test_a_file_too_large_to_be_a_key(self, tmp_path):
        cert_path, key_path, _ = loadable(tmp_path)
        with open(key_path, "ab") as fh:
            fh.write(b"#" * (1024 * 1024 + 1))
        with pytest.raises(identity.IdentityError, match="not a key or certificate"):
            identity.load_identity(cert_path, key_path, "client", **NOW)

    # --- the certificate ----------------------------------------------------

    def test_key_and_certificate_must_match(self, tmp_path):
        cert_path, key_path, _ = loadable(tmp_path)
        cert_path.write_text(pem_of(make_cert(key_of("ec256:other"))))
        with pytest.raises(identity.IdentityError, match="does not match private key"):
            identity.load_identity(cert_path, key_path, "client", **NOW)

    def test_a_chain_file_uses_the_first_certificate(self, tmp_path):
        cert_path, key_path, cert = loadable(tmp_path)
        cert_path.write_text(pem_of(cert) + pem_of(make_cert(key_of("ec384"))))
        assert identity.load_identity(cert_path, key_path, "client", **NOW).fingerprint == fp_of(cert)

    @pytest.mark.parametrize("role,eku,ok", [
        ("client", (CLIENT_AUTH,), True),
        ("client", (SERVER_AUTH, CLIENT_AUTH), True),
        ("client", None, True),
        ("client", (SERVER_AUTH,), False),
        ("client", (ExtendedKeyUsageOID.CODE_SIGNING,), False),
        ("server", (SERVER_AUTH,), True),
        ("server", (SERVER_AUTH, CLIENT_AUTH), True),
        ("server", None, True),
        ("server", (CLIENT_AUTH,), False),
    ])
    def test_extended_key_usage_must_fit_the_role(self, tmp_path, role, eku, ok):
        cert_path, key_path, _ = loadable(tmp_path, role=role, eku=eku)
        if ok:
            identity.load_identity(cert_path, key_path, role, **NOW)
        else:
            want = "clientAuth" if role == "client" else "serverAuth"
            with pytest.raises(identity.IdentityError, match=want):
                identity.load_identity(cert_path, key_path, role, **NOW)

    def test_expiry_is_refused_unless_allowed(self, tmp_path, caplog):
        cert_path, key_path, _ = loadable(tmp_path)
        later = FIXED_NOW + 2 * DAY
        with pytest.raises(identity.IdentityError, match="expired"):
            identity.load_identity(cert_path, key_path, "client", now=later)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.identity"):
            ident = identity.load_identity(cert_path, key_path, "client", now=later, allow_expired=True)
        assert ident.fingerprint
        assert any("expired" in r.getMessage() for r in caplog.records)

    def test_the_last_instant_of_validity_is_still_valid(self, tmp_path):
        cert_path, key_path, _ = loadable(tmp_path)
        identity.load_identity(cert_path, key_path, "client", now=FIXED_NOW + DAY)
        with pytest.raises(identity.IdentityError, match="expired"):
            identity.load_identity(cert_path, key_path, "client",
                                   now=FIXED_NOW + DAY + timedelta(seconds=1))

    def test_a_certificate_not_valid_yet_is_refused_beyond_five_minutes(self, tmp_path):
        cert_path, key_path, _ = loadable(tmp_path)           # not_before = FIXED_NOW - 1h
        identity.load_identity(cert_path, key_path, "client",
                               now=FIXED_NOW - HOUR - timedelta(minutes=5))
        with pytest.raises(identity.IdentityError, match="not valid until"):
            identity.load_identity(cert_path, key_path, "client",
                                   now=FIXED_NOW - HOUR - timedelta(minutes=5, seconds=1))

    def test_naive_now_is_taken_as_utc(self, tmp_path):
        cert_path, key_path, _ = loadable(tmp_path)
        identity.load_identity(cert_path, key_path, "client", now=FIXED_NOW.replace(tzinfo=None))

    def test_now_defaults_to_the_real_clock(self, tmp_path):
        nb, na = real_window()
        cert_path, key_path, _ = loadable(tmp_path, not_before=nb, not_after=na)
        identity.load_identity(cert_path, key_path, "client")
        cert_path2, key_path2, _ = loadable(tmp_path, not_before=nb - 2 * DAY, not_after=nb - DAY)
        with pytest.raises(identity.IdentityError, match="expired"):
            identity.load_identity(cert_path2, key_path2, "client")


@pytest.mark.unit
class TestPeekIdentity:
    def test_missing_certificate_is_none(self, tmp_path):
        assert identity.peek_identity(tmp_path, "client") is None
        assert identity.peek_identity(tmp_path / "no-such-dir", "server") is None

    def test_describes_the_certificate_without_a_key(self, tmp_path):
        cert = make_cert(key_of("ec256"), cn="peeked")
        (tmp_path / "server.crt").write_text(pem_of(cert))          # no server.key at all
        info = identity.peek_identity(tmp_path, "server")
        assert isinstance(info, identity.CertInfo)
        assert info.fingerprint == fp_of(cert)
        assert info.not_after == FIXED_NOW + DAY
        assert info.subject == "CN=peeked"

    def test_does_not_validate_expiry_or_eku(self, tmp_path):
        cert = make_cert(key_of("ec256"), not_before=FIXED_NOW - 3 * DAY, not_after=FIXED_NOW - 2 * DAY,
                         eku=(SERVER_AUTH,))
        (tmp_path / "client.crt").write_text(pem_of(cert))
        assert identity.peek_identity(tmp_path, "client").fingerprint == fp_of(cert)

    def test_unparseable_certificate_is_an_error_not_none(self, tmp_path):
        (tmp_path / "client.crt").write_text("garbage")
        with pytest.raises(identity.IdentityError):
            identity.peek_identity(tmp_path, "client")

    def test_creates_nothing(self, tmp_path):
        d = tmp_path / "config.db.tls"
        assert identity.peek_identity(d, "client") is None
        assert not d.exists()

    def test_unknown_name(self, tmp_path):
        with pytest.raises(ValueError):
            identity.peek_identity(tmp_path, "nope")

    def test_agrees_with_a_generated_identity(self, tmp_path):
        d = make_tlsdir(tmp_path)
        ident = identity.ensure_identity(d, "server")
        info = identity.peek_identity(d, "server")
        assert info.fingerprint == ident.fingerprint
        assert info.not_after == ident.not_after


@pytest.mark.unit
class TestClientCertExpiry:
    """S-12 (2): `client_cert_not_after` reads an (even expired) client certificate without
    its key; `client_cert_expiry_problem` is the pure `(level, message)` decision shared by
    the daemon's start/reload check and the remote worker's daily one."""

    WARN = timedelta(days=identity.CLIENT_CERT_EXPIRY_WARN_DAYS)
    PATH = "/var/lib/zfsbackup/config.db.tls/client.crt"

    def _problem(self, left, now=FIXED_NOW, path=PATH):
        return identity.client_cert_expiry_problem(path, now + left, now)

    # -- constants and exports ----------------------------------------------------

    def test_the_window_is_thirty_days_and_exported(self):
        assert identity.CLIENT_CERT_EXPIRY_WARN_DAYS == 30
        for name in ("CLIENT_CERT_EXPIRY_WARN_DAYS", "client_cert_not_after",
                     "client_cert_expiry_problem"):
            assert name in identity.__all__

    def test_the_daemon_shares_the_one_definition(self):
        from zfsbackup import daemon

        assert daemon.CLIENT_CERT_EXPIRY_WARN_DAYS is identity.CLIENT_CERT_EXPIRY_WARN_DAYS

    # -- the decision: boundaries ------------------------------------------------------

    def test_exactly_thirty_days_left_is_silent(self):
        assert self._problem(self.WARN) is None

    def test_one_second_inside_the_window_is_a_warning(self):
        level, message = self._problem(self.WARN - timedelta(seconds=1))
        assert level == logging.WARNING
        assert "(in 29 day(s))" in message

    def test_far_from_expiry_is_silent(self):
        assert self._problem(timedelta(days=365)) is None
        assert self._problem(self.WARN + timedelta(seconds=1)) is None

    def test_expired_one_second_ago_is_an_error(self):
        level, message = self._problem(-timedelta(seconds=1))
        assert level == logging.ERROR

    def test_exactly_now_is_not_yet_expired(self):
        level, message = self._problem(timedelta(0))
        assert level == logging.WARNING and "(in 0 day(s))" in message

    def test_long_expired_is_an_error(self):
        level, message = self._problem(-timedelta(days=400))
        assert level == logging.ERROR

    @pytest.mark.parametrize("left, days", [
        (timedelta(hours=1), 0), (timedelta(hours=23, minutes=59), 0),
        (timedelta(days=1), 1), (timedelta(days=1, hours=23), 1),
        (timedelta(days=12, hours=5), 12), (timedelta(days=29, hours=23), 29),
    ])
    def test_the_days_left_are_whole_days_clamped_at_zero(self, left, days):
        level, message = self._problem(left)
        assert level == logging.WARNING and f"(in {days} day(s))" in message

    def test_the_days_never_go_negative(self):
        for left in (timedelta(0), timedelta(seconds=1), timedelta(minutes=5)):
            assert "(in 0 day(s))" in self._problem(left)[1]

    # -- the decision: wording ------------------------------------------------------------

    def test_the_warning_wording(self):
        not_after = datetime(2026, 11, 3, 4, 5, 59, tzinfo=timezone.utc)
        level, message = identity.client_cert_expiry_problem(
            self.PATH, not_after, datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc))
        assert level == logging.WARNING
        assert message == (
            f"the client certificate {self.PATH} expires on 2026-11-03 04:05 UTC (in 13 "
            "day(s)); rotate it: `zfsbackup-config identity rotate`"
        )

    def test_the_error_wording(self):
        not_after = datetime(2026, 10, 1, 8, 30, tzinfo=timezone.utc)
        level, message = identity.client_cert_expiry_problem(self.PATH, not_after, FIXED_NOW)
        assert level == logging.ERROR
        assert message == (
            f"the client certificate {self.PATH} expired on 2026-10-01 08:30 UTC; remote "
            "backups fail until it is replaced (`identity rotate` needs a valid certificate: "
            "enroll again with a new token, `zfsbackup-config enroll DESTINATION ...`)"
        )

    def test_the_path_is_named_as_given(self):
        for path in ("relative/client.crt", Path("/x/y/client.crt")):
            assert str(path) in self._problem(timedelta(days=2), path=path)[1]

    # -- the decision: `now` -------------------------------------------------------------------

    def test_a_naive_now_is_taken_as_utc(self):
        naive = FIXED_NOW.replace(tzinfo=None)
        assert identity.client_cert_expiry_problem(
            self.PATH, FIXED_NOW + timedelta(days=3), naive
        ) == identity.client_cert_expiry_problem(self.PATH, FIXED_NOW + timedelta(days=3), FIXED_NOW)
        level, _ = identity.client_cert_expiry_problem(self.PATH, FIXED_NOW - HOUR, naive)
        assert level == logging.ERROR

    def test_an_aware_now_in_another_zone_is_the_same_instant(self):
        plus_five = timezone(timedelta(hours=5))
        now = FIXED_NOW.astimezone(plus_five)
        not_after = FIXED_NOW + timedelta(days=3)
        assert identity.client_cert_expiry_problem(self.PATH, not_after, now) == \
            identity.client_cert_expiry_problem(self.PATH, not_after, FIXED_NOW)

    def test_without_now_it_uses_the_current_time(self):
        soon = datetime.now(timezone.utc) + timedelta(days=3, hours=1)
        level, message = identity.client_cert_expiry_problem(self.PATH, soon)
        assert level == logging.WARNING and "(in 3 day(s))" in message
        assert identity.client_cert_expiry_problem(
            self.PATH, datetime.now(timezone.utc) - HOUR)[0] == logging.ERROR
        assert identity.client_cert_expiry_problem(
            self.PATH, datetime.now(timezone.utc) + timedelta(days=90)) is None

    def test_it_is_pure(self, caplog):
        """Nothing is read (the path does not exist) and nothing is logged."""
        with caplog.at_level(logging.DEBUG):
            self._problem(-DAY, path="/no/such/dir/client.crt")
            self._problem(DAY, path="/no/such/dir/client.crt")
        assert caplog.records == []

    # -- client_cert_not_after ---------------------------------------------------------------------

    def test_not_after_of_a_valid_certificate(self, tmp_path):
        cert = make_cert(key_of("ec256"))
        (tmp_path / "client.crt").write_text(pem_of(cert))              # and no key at all
        result = identity.client_cert_not_after(tmp_path / "client.crt")
        assert result == FIXED_NOW + DAY
        assert result.tzinfo is not None and result.utcoffset() == timedelta(0)

    def test_not_after_of_an_expired_certificate_which_load_identity_refuses(self, tmp_path):
        cert = make_cert(key_of("ec256"), not_before=FIXED_NOW - 9 * DAY, not_after=FIXED_NOW - 2 * DAY)
        d = make_tlsdir(tmp_path)
        d.mkdir(exist_ok=True)
        cert_path, key_path = identity.identity_paths(d, "client")
        cert_path.write_text(pem_of(cert))
        key_path.write_bytes(key_pem(key_of("ec256")))
        key_path.chmod(0o600)
        with pytest.raises(identity.IdentityError):
            identity.load_identity(cert_path, key_path, identity.ROLE_CLIENT)
        assert identity.client_cert_not_after(cert_path) == FIXED_NOW - 2 * DAY

    def test_not_after_takes_a_string_path_too(self, tmp_path):
        (tmp_path / "client.crt").write_text(pem_of(make_cert(key_of("ec256"))))
        assert identity.client_cert_not_after(str(tmp_path / "client.crt")) == FIXED_NOW + DAY

    def test_not_after_does_not_check_the_purpose(self, tmp_path):
        (tmp_path / "x.crt").write_text(pem_of(make_cert(key_of("ec256"), eku=(SERVER_AUTH,))))
        assert identity.client_cert_not_after(tmp_path / "x.crt") == FIXED_NOW + DAY

    def test_not_after_is_the_first_certificate_of_a_bundle(self, tmp_path):
        first = make_cert(key_of("ec256"), not_after=FIXED_NOW + 3 * DAY)
        second = make_cert(key_of("ec256:other"), not_after=FIXED_NOW + 30 * DAY)
        (tmp_path / "client.crt").write_text(pem_of(first) + pem_of(second))
        assert identity.client_cert_not_after(tmp_path / "client.crt") == FIXED_NOW + 3 * DAY

    def test_not_after_of_a_missing_file_is_an_identity_error(self, tmp_path):
        with pytest.raises(identity.IdentityError):
            identity.client_cert_not_after(tmp_path / "client.crt")
        with pytest.raises(identity.IdentityError):
            identity.client_cert_not_after(tmp_path / "no-such-dir" / "client.crt")

    @pytest.mark.parametrize("content", [
        "", "garbage\n", "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n",
    ], ids=["empty", "text", "corrupt-armour", "a-key"])
    def test_not_after_of_a_non_pem_file_is_an_identity_error(self, tmp_path, content):
        (tmp_path / "client.crt").write_text(content)
        with pytest.raises(identity.IdentityError):
            identity.client_cert_not_after(tmp_path / "client.crt")

    def test_not_after_of_a_directory_is_an_identity_error(self, tmp_path):
        (tmp_path / "client.crt").mkdir()
        with pytest.raises(identity.IdentityError):
            identity.client_cert_not_after(tmp_path / "client.crt")

    @pytest.mark.skipif(os.geteuid() == 0, reason="root reads everything")
    def test_not_after_of_an_unreadable_file_is_an_identity_error(self, tmp_path):
        path = tmp_path / "client.crt"
        path.write_text(pem_of(make_cert(key_of("ec256"))))
        path.chmod(0)
        try:
            with pytest.raises(identity.IdentityError):
                identity.client_cert_not_after(path)
        finally:
            path.chmod(0o600)

    def test_not_after_and_the_decision_compose(self, tmp_path):
        """The way the callers use them."""
        cert = make_cert(key_of("ec256"), not_before=FIXED_NOW - DAY, not_after=FIXED_NOW + 5 * DAY)
        path = tmp_path / "client.crt"
        path.write_text(pem_of(cert))
        level, message = identity.client_cert_expiry_problem(
            path, identity.client_cert_not_after(path), FIXED_NOW)
        assert level == logging.WARNING and str(path) in message and "(in 5 day(s))" in message


# ==========================================================================
# ensure_identity
# ==========================================================================

def _client_files(d: Path):
    return identity.identity_paths(d, "client")


def _replace_cert(d: Path, **kw):
    """Rewrite client.crt for the key already on disk, keeping everything else."""
    cert_path, key_path = _client_files(d)
    key = serialization.load_pem_private_key(key_path.read_bytes(), None)
    nb, na = real_window()
    kw.setdefault("not_before", nb)
    kw.setdefault("not_after", na)
    cert_path.write_text(pem_of(make_cert(key, **kw)))


@pytest.mark.unit
class TestEnsureIdentityGeneration:
    def test_creates_directory_and_a_matching_pair(self, tmp_path):
        d = make_tlsdir(tmp_path)
        ident = identity.ensure_identity(d, "client")
        cert_path, key_path = _client_files(d)
        assert (ident.cert_path, ident.key_path, ident.role) == (cert_path, key_path, "client")
        again = identity.load_identity(cert_path, key_path, "client")
        assert again.fingerprint == ident.fingerprint == identity.fingerprint_pem(cert_path.read_text())
        assert sorted(p.name for p in d.iterdir()) == ["client.crt", "client.key", "identity.lock"]

    @pytest.mark.parametrize("umask", [0o000, 0o077, 0o027, 0o022])
    def test_modes_are_exact_whatever_the_umask(self, tmp_path, umask):
        d = make_tlsdir(tmp_path)
        old = os.umask(umask)
        try:
            identity.ensure_identity(d, "client")
        finally:
            os.umask(old)
        assert mode_of(d) == 0o750
        assert mode_of(d / "client.key") == 0o600
        assert mode_of(d / "client.crt") == 0o640

    @pytest.mark.parametrize("umask", [0o000, 0o077, 0o027, 0o022])
    def test_lock_file_is_group_readable_whatever_the_umask(self, tmp_path, umask):
        d = make_tlsdir(tmp_path)
        old = os.umask(umask)
        try:
            identity.ensure_identity(d, "client")
        finally:
            os.umask(old)
        assert mode_of(d / "identity.lock") == 0o640

    def test_server_and_client_and_next_identities_coexist(self, tmp_path):
        d = make_tlsdir(tmp_path)
        s = identity.ensure_identity(d, "server")
        c = identity.ensure_identity(d, "client")
        n = identity.ensure_identity(d, "client", name="client.next")
        assert len({s.fingerprint, c.fingerprint, n.fingerprint}) == 3
        assert (d / "client.next.key").exists() and (d / "client.next.crt").exists()
        assert n.role == "client" and s.role == "server"

    def test_the_generated_certificate_follows_the_recipe(self, tmp_path):
        d = make_tlsdir(tmp_path)
        before = datetime.now(timezone.utc)
        for role in ("server", "client"):
            identity.ensure_identity(d, role)
            cert = x509.load_pem_x509_certificate((d / f"{role}.crt").read_bytes())
            key = serialization.load_pem_private_key((d / f"{role}.key").read_bytes(), None)
            assert isinstance(key, ec.EllipticCurvePrivateKey) and key.curve.name == "secp256r1"
            assert isinstance(cert.signature_hash_algorithm, hashes.SHA256)
            assert cert.issuer == cert.subject
            assert abs((cert.not_valid_before_utc - (before - timedelta(minutes=5))).total_seconds()) < 60
            assert abs((cert.not_valid_after_utc - (before + timedelta(days=3650))).total_seconds()) < 60
            bc = cert.extensions.get_extension_for_class(x509.BasicConstraints)
            assert bc.critical and bc.value.ca is False
            ku = cert.extensions.get_extension_for_class(x509.KeyUsage)
            assert ku.critical and ku.value.digital_signature and not ku.value.key_cert_sign
            eku = cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
            assert list(eku) == [SERVER_AUTH if role == "server" else CLIENT_AUTH]
            with pytest.raises(x509.ExtensionNotFound):
                cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
            assert cert.version == x509.Version.v3
            cn = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
            assert cn.startswith(f"zfsbackup {role} ") and len(cn) <= 64
            # the private key PEM is unencrypted PKCS#8
            assert (d / f"{role}.key").read_bytes().startswith(b"-----BEGIN PRIVATE KEY-----")

    def test_each_generation_has_a_new_key(self, tmp_path):
        (tmp_path / "a").mkdir()
        (tmp_path / "b").mkdir()
        a = identity.ensure_identity(make_tlsdir(tmp_path / "a"), "client")
        b = identity.ensure_identity(make_tlsdir(tmp_path / "b"), "client")
        assert a.fingerprint != b.fingerprint
        assert (tmp_path / "a" / "config.db.tls" / "client.key").read_bytes() != (
            tmp_path / "b" / "config.db.tls" / "client.key").read_bytes()

    def test_a_valid_pair_is_returned_unchanged(self, tmp_path):
        d = make_tlsdir(tmp_path)
        first = identity.ensure_identity(d, "client")
        before = snapshot(d)
        second = identity.ensure_identity(d, "client")
        assert second == first
        assert snapshot(d) == before

    def test_a_valid_pair_is_returned_without_taking_the_lock(self, tmp_path, monkeypatch):
        d = make_tlsdir(tmp_path)
        first = identity.ensure_identity(d, "client")

        def no_lock(*a, **kw):
            raise AssertionError("the lock must not be taken for a valid pair")

        monkeypatch.setattr(identity, "_acquire_lock", no_lock)
        assert identity.ensure_identity(d, "client").fingerprint == first.fingerprint

    def test_the_repr_of_the_result_omits_the_certificate_pem(self, tmp_path):
        ident = identity.ensure_identity(make_tlsdir(tmp_path), "server")
        assert "BEGIN CERTIFICATE" not in repr(ident) and ident.cert_pem.startswith("-----BEGIN CERT")

    def test_role_and_name_must_agree(self, tmp_path):
        d = make_tlsdir(tmp_path)
        with pytest.raises(ValueError):
            identity.ensure_identity(d, "server", name="client")
        with pytest.raises(ValueError):
            identity.ensure_identity(d, "client", name="server")
        with pytest.raises(ValueError):
            identity.ensure_identity(d, "client", name="bogus")
        with pytest.raises(ValueError):
            identity.ensure_identity(d, "root")
        assert not d.exists()

    def test_common_name_is_bounded_and_sanitised(self, tmp_path, monkeypatch):
        for host in ("h" * 300, "hôst ☃ name", ""):
            monkeypatch.setattr(identity.socket, "gethostname", lambda host=host: host)
            d = tmp_path / host[:3].encode().hex() / "config.db.tls"
            d.parent.mkdir(parents=True, exist_ok=True)
            identity.ensure_identity(d, "client")
            cert = x509.load_pem_x509_certificate((d / "client.crt").read_bytes())
            cn = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
            assert len(cn) <= 64 and cn.isascii() and cn.startswith("zfsbackup client ")

    def test_common_name_survives_a_hostname_error(self, tmp_path, monkeypatch):
        def broken():
            raise OSError("no hostname")

        monkeypatch.setattr(identity.socket, "gethostname", broken)
        identity.ensure_identity(make_tlsdir(tmp_path), "client")


@pytest.mark.unit
class TestEnsureIdentityDebrisIsRegenerated:
    DEBRIS = {
        "nothing": lambda c, k: (c.unlink(), k.unlink()),
        "orphan_key": lambda c, k: c.unlink(),
        "orphan_certificate": lambda c, k: k.unlink(),
        "empty_certificate": lambda c, k: c.write_bytes(b""),
        "empty_key": lambda c, k: k.write_bytes(b""),
        "garbage_certificate": lambda c, k: c.write_text("garbage\n"),
        "garbage_key": lambda c, k: k.write_text("garbage\n"),
        "truncated_certificate": lambda c, k: c.write_text(c.read_text()[:100]),
        "truncated_key": lambda c, k: k.write_text(k.read_text()[:60]),
        "both_garbage": lambda c, k: (c.write_text("x"), k.write_text("y")),
    }

    @pytest.mark.parametrize("with_stale_tmp", [False, True], ids=["clean", "stale-tmp"])
    @pytest.mark.parametrize("case", list(DEBRIS))
    def test_regenerates_a_working_pair(self, tmp_path, case, with_stale_tmp, caplog):
        d = make_tlsdir(tmp_path)
        old = identity.ensure_identity(d, "client")
        cert_path, key_path = _client_files(d)
        self.DEBRIS[case](cert_path, key_path)
        if with_stale_tmp:
            for name in (".client.key.tmp", ".client.crt.tmp"):
                (d / name).write_text("stale")
                (d / name).chmod(0o644)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.identity"):
            new = identity.ensure_identity(d, "client")
        assert new.fingerprint != old.fingerprint
        checked = identity.load_identity(cert_path, key_path, "client")
        assert checked.fingerprint == new.fingerprint
        assert sorted(p.name for p in d.iterdir()) == ["client.crt", "client.key", "identity.lock"]
        assert mode_of(key_path) == 0o600 and mode_of(cert_path) == 0o640
        if case != "nothing":
            assert any("incomplete" in r.getMessage() for r in caplog.records)
        if with_stale_tmp:
            assert any("stale temporary" in r.getMessage() for r in caplog.records)

    def test_stale_temporaries_alone_are_removed_and_a_pair_made(self, tmp_path):
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        (d / ".client.key.tmp").write_text("stale key")
        (d / ".client.crt.tmp").write_text("stale cert")
        ident = identity.ensure_identity(d, "client")
        assert sorted(p.name for p in d.iterdir()) == ["client.crt", "client.key", "identity.lock"]
        assert ident.fingerprint

    def test_a_stale_temporary_symlink_is_removed_not_followed(self, tmp_path):
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        victim = tmp_path / "victim.txt"
        victim.write_text("precious")
        (d / ".client.key.tmp").symlink_to(victim)
        (d / ".client.crt.tmp").symlink_to(tmp_path / "does-not-exist")
        identity.ensure_identity(d, "client")
        assert victim.read_text() == "precious"
        assert sorted(p.name for p in d.iterdir()) == ["client.crt", "client.key", "identity.lock"]

    def test_only_this_identitys_temporaries_are_removed(self, tmp_path):
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        (d / ".server.key.tmp").write_text("someone else's work in progress")
        identity.ensure_identity(d, "client")
        assert (d / ".server.key.tmp").read_text() == "someone else's work in progress"

    def test_a_regenerated_key_is_a_new_key(self, tmp_path):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        cert_path, key_path = _client_files(d)
        old_key = key_path.read_bytes()
        cert_path.unlink()
        identity.ensure_identity(d, "client")
        assert key_path.read_bytes() != old_key


@pytest.mark.unit
class TestEnsureIdentityUnacceptablePairsAreRefused:
    """A complete pair that parses but is not acceptable is the operator's call:
    replacing it would change a fingerprint that peers have pinned."""

    @staticmethod
    def _swap_key(d, key, **cert_kw):
        cert_path, key_path = _client_files(d)
        key_path.write_bytes(key_pem(key))
        key_path.chmod(0o600)
        nb, na = real_window()
        cert_kw.setdefault("not_before", nb)
        cert_kw.setdefault("not_after", na)
        cert_path.write_text(pem_of(make_cert(key, **cert_kw)))

    CASES = {
        "key_world_readable": lambda d: _client_files(d)[1].chmod(0o644),
        "key_group_writable": lambda d: _client_files(d)[1].chmod(0o660),
        "key_world_writable": lambda d: _client_files(d)[1].chmod(0o666),
        "certificate_for_another_key": lambda d: _replace_cert_with_other_key(d),
        "wrong_eku": lambda d: _replace_cert(d, eku=(SERVER_AUTH,)),
        "expired_client_certificate": lambda d: _replace_cert(
            d, not_before=datetime.now(timezone.utc) - 3 * DAY,
            not_after=datetime.now(timezone.utc) - DAY),
        "not_yet_valid": lambda d: _replace_cert(
            d, not_before=datetime.now(timezone.utc) + DAY,
            not_after=datetime.now(timezone.utc) + 2 * DAY),
        "encrypted_key": lambda d: _client_files(d)[1].write_bytes(
            serialization.load_pem_private_key(_client_files(d)[1].read_bytes(), None).private_bytes(
                serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                serialization.BestAvailableEncryption(b"pw"))),
        "unsupported_key_type": lambda d: TestEnsureIdentityUnacceptablePairsAreRefused._swap_key(
            d, key_of("ed448")),
    }

    @pytest.mark.parametrize("case", list(CASES))
    def test_raises_and_changes_nothing(self, tmp_path, case):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        self.CASES[case](d)
        before = snapshot(d)
        with pytest.raises(identity.IdentityError, match="left alone") as info:
            identity.ensure_identity(d, "client")
        assert snapshot(d) == before
        assert info.value.path is not None

    def test_a_key_that_is_a_directory_is_not_replaced_either(self, tmp_path):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        _, key_path = _client_files(d)
        key_path.unlink()
        key_path.mkdir()
        before = snapshot(d)
        with pytest.raises(identity.IdentityError, match="not a regular file"):
            identity.ensure_identity(d, "client")
        assert snapshot(d) == before

    def test_the_message_says_how_to_start_over(self, tmp_path):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        _client_files(d)[1].chmod(0o644)
        with pytest.raises(identity.IdentityError) as info:
            identity.ensure_identity(d, "client")
        assert "client.crt" in str(info.value) and "client.key" in str(info.value)
        assert "re-enroll" in str(info.value)

    def test_an_expired_server_certificate_is_accepted_with_a_warning(self, tmp_path, caplog):
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        key = key_of("ec256:srv-expired")
        nb, na = real_window(before=timedelta(days=3), after=timedelta(days=-1))
        cert = make_cert(key, eku=(SERVER_AUTH,), not_before=nb, not_after=na)
        write_pair(d, "server", key, cert)
        before = snapshot(d)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.identity"):
            ident = identity.ensure_identity(d, "server")
        assert ident.fingerprint == fp_of(cert)
        assert any("expired" in r.getMessage() for r in caplog.records)
        assert snapshot(d) == before

    def test_an_expired_client_certificate_is_not_given_the_server_exemption(self, tmp_path):
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        key = key_of("ec256:cli-expired")
        nb, na = real_window(before=timedelta(days=3), after=timedelta(days=-1))
        write_pair(d, "client", key, make_cert(key, not_before=nb, not_after=na))
        with pytest.raises(identity.IdentityError, match="expired"):
            identity.ensure_identity(d, "client")


def _replace_cert_with_other_key(d: Path):
    cert_path, _ = _client_files(d)
    nb, na = real_window()
    cert_path.write_text(pem_of(make_cert(key_of("ec256:mismatch"), not_before=nb, not_after=na)))



@pytest.mark.unit
class TestEnsureIdentityDirectory:
    def test_symlinked_directory_is_refused(self, tmp_path):
        (tmp_path / "config.db").touch()
        real = tmp_path / "real"
        real.mkdir(mode=0o750)
        link = tmp_path / "config.db.tls"
        link.symlink_to(real)
        with pytest.raises(identity.IdentityError, match="symlink or not a directory"):
            identity.ensure_identity(link, "client")
        assert list(real.iterdir()) == []

    def test_non_directory_is_refused(self, tmp_path):
        d = make_tlsdir(tmp_path)
        d.write_text("i am a file")
        with pytest.raises(identity.IdentityError, match="symlink or not a directory"):
            identity.ensure_identity(d, "client")
        assert d.read_text() == "i am a file"

    def test_world_writable_directory_is_refused_and_left_alone(self, tmp_path):
        d = make_tlsdir(tmp_path)
        d.mkdir()
        d.chmod(0o777)
        with pytest.raises(identity.IdentityError, match="writable by everyone"):
            identity.ensure_identity(d, "client")
        assert list(d.iterdir()) == [] and mode_of(d) == 0o777

    def test_an_existing_directory_keeps_its_mode(self, tmp_path):
        d = make_tlsdir(tmp_path)
        d.mkdir()
        d.chmod(0o700)
        identity.ensure_identity(d, "client")
        assert mode_of(d) == 0o700

    def test_missing_parent_is_an_identity_error(self, tmp_path):
        with pytest.raises(identity.IdentityError, match="does not exist"):
            identity.ensure_identity(tmp_path / "nope" / "config.db.tls", "client")

    @pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions")
    def test_unwritable_parent_for_a_new_directory(self, tmp_path):
        parent = tmp_path / "ro"
        parent.mkdir()
        parent.chmod(0o500)
        try:
            with pytest.raises(identity.IdentityError, match="permission denied"):
                identity.ensure_identity(parent / "config.db.tls", "client")
            assert list(parent.iterdir()) == []
        finally:
            parent.chmod(0o700)


@pytest.mark.unit
class TestEnsureIdentityLockedAndReadOnly:
    @pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions")
    def test_a_caller_who_cannot_write_the_directory_still_gets_a_valid_identity(self, tmp_path):
        """The group-operator case: read/execute on the directory, no write, hence no
        lock file to create -- and none is needed for a valid pair."""
        d = make_tlsdir(tmp_path)
        first = identity.ensure_identity(d, "client")
        (d / "identity.lock").unlink()
        d.chmod(0o500)
        try:
            again = identity.ensure_identity(d, "client")
            assert again.fingerprint == first.fingerprint
            assert not (d / "identity.lock").exists()
        finally:
            d.chmod(0o750)

    @pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions")
    def test_a_caller_who_cannot_write_gets_an_error_when_there_is_nothing_to_return(self, tmp_path):
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        d.chmod(0o500)
        try:
            with pytest.raises(identity.IdentityError):
                identity.ensure_identity(d, "client")
            assert list(d.iterdir()) == []
        finally:
            d.chmod(0o750)

    def test_lock_timeout_leaves_everything_untouched(self, tmp_path):
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        fd = os.open(d / "identity.lock", os.O_RDWR | os.O_CREAT, 0o640)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            started = time.monotonic()
            with pytest.raises(identity.IdentityError, match="timed out"):
                identity.ensure_identity(d, "client", lock_timeout=0.3)
            assert 0.25 <= time.monotonic() - started < 5
            assert [p.name for p in d.iterdir()] == ["identity.lock"]
        finally:
            os.close(fd)
        identity.ensure_identity(d, "client", lock_timeout=5)      # free again

    def test_the_lock_is_released_after_success_and_after_failure(self, tmp_path, monkeypatch):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        (d / "client.crt").unlink()

        def boom(*a, **kw):
            raise OSError(errno.ENOSPC, "No space left on device")

        monkeypatch.setattr(identity, "_write_tmp", boom)
        with pytest.raises(identity.IdentityError, match="cannot write"):
            identity.ensure_identity(d, "client")
        monkeypatch.undo()
        for _ in range(2):
            fd = os.open(d / "identity.lock", os.O_RDONLY)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)      # raises if still held
            finally:
                os.close(fd)
            identity.ensure_identity(d, "client")                    # also fine after success

    def test_a_waiting_caller_returns_what_the_lock_holder_generated(self, tmp_path):
        """Deterministic version of the race: while we hold the lock a second caller
        waits; we put a valid pair in place and release; the waiter must return that
        pair and not generate its own."""
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        (tmp_path / "donor-base").mkdir()
        donor = make_tlsdir(tmp_path / "donor-base")
        donor_ident = identity.ensure_identity(donor, "client")
        fd = os.open(d / "identity.lock", os.O_RDWR | os.O_CREAT, 0o640)
        result = {}

        def waiter():
            try:
                result["ident"] = identity.ensure_identity(d, "client", lock_timeout=20)
            except BaseException as exc:                              # pragma: no cover
                result["error"] = exc

        t = threading.Thread(target=waiter)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            t.start()
            time.sleep(0.3)
            assert t.is_alive() and "ident" not in result
            for name, mode in (("client.key", 0o600), ("client.crt", 0o640)):
                (d / name).write_bytes((donor / name).read_bytes())
                (d / name).chmod(mode)
        finally:
            os.close(fd)                                              # releases the flock
            t.join(timeout=20)
        assert not t.is_alive() and "error" not in result
        assert result["ident"].fingerprint == donor_ident.fingerprint
        assert (d / "client.key").read_bytes() == (donor / "client.key").read_bytes()


@pytest.mark.unit
class TestEnsureIdentityCrashSafety:
    def test_an_orphan_certificate_is_unlinked_before_the_new_key_exists(self, tmp_path, monkeypatch):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        cert_path, key_path = _client_files(d)
        old_cert = cert_path.read_bytes()
        key_path.unlink()                                   # orphan certificate
        seen = {}
        real_write = identity._write_tmp

        def spy(directory, final, data, mode, gid):
            seen.setdefault("old_cert_present", cert_path.exists())
            seen.setdefault("key_present", key_path.exists())
            return real_write(directory, final, data, mode, gid)

        monkeypatch.setattr(identity, "_write_tmp", spy)
        identity.ensure_identity(d, "client")
        assert seen == {"old_cert_present": False, "key_present": False}
        assert cert_path.read_bytes() != old_cert

    @pytest.mark.parametrize("crash_at", [1, 2], ids=["before-key-rename", "between-the-renames"])
    def test_a_crash_never_leaves_a_complete_mismatched_pair(self, tmp_path, monkeypatch, crash_at):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        cert_path, key_path = _client_files(d)
        key_path.unlink()                                   # orphan old certificate
        old_cert = cert_path.read_bytes()
        real_rename = os.rename
        calls = []

        def flaky(src, dst, *a, **kw):
            calls.append((Path(src).name, Path(dst).name))
            if len(calls) == crash_at:
                raise OSError(errno.EIO, "simulated crash")
            return real_rename(src, dst, *a, **kw)

        monkeypatch.setattr(os, "rename", flaky)
        with pytest.raises(identity.IdentityError, match="cannot write"):
            identity.ensure_identity(d, "client")
        monkeypatch.undo()

        # the key goes first and the old certificate is already gone: there is no
        # state in which the old certificate sits next to the new key
        assert calls[0] == (".client.key.tmp", "client.key")
        assert not cert_path.exists() or cert_path.read_bytes() != old_cert
        assert not (cert_path.exists() and key_path.exists())
        assert (d / ".client.crt.tmp").exists()

        recovered = identity.ensure_identity(d, "client")
        assert identity.load_identity(cert_path, key_path, "client").fingerprint == recovered.fingerprint
        assert sorted(p.name for p in d.iterdir()) == ["client.crt", "client.key", "identity.lock"]

    def test_a_write_failure_cleans_up_its_temporary(self, tmp_path, monkeypatch):
        d = make_tlsdir(tmp_path)

        def full_disk(fd):
            raise OSError(errno.ENOSPC, "No space left on device")

        monkeypatch.setattr(os, "fsync", full_disk)
        with pytest.raises(identity.IdentityError, match="cannot write"):
            identity.ensure_identity(d, "client")
        monkeypatch.undo()
        assert [p.name for p in d.iterdir()] == ["identity.lock"]
        assert identity.ensure_identity(d, "client").fingerprint

    def test_permission_error_while_writing_is_reported(self, tmp_path, monkeypatch):
        d = make_tlsdir(tmp_path)

        def denied(*a, **kw):
            raise PermissionError(errno.EACCES, "Permission denied")

        monkeypatch.setattr(identity, "_write_tmp", denied)
        with pytest.raises(identity.IdentityError, match="permission denied"):
            identity.ensure_identity(d, "client")


@pytest.mark.unit
class TestEnsureIdentityGroup:
    """The root / group path, with `geteuid`, `getgroups` and `fchown` mocked: the new
    directory, `identity.lock` and the key/certificate temporaries get the DB file's group."""

    DB_GID = 4242

    @pytest.fixture
    def chown_calls(self, tmp_path, monkeypatch):
        db = tmp_path / "config.db"
        db.touch()
        real_stat = os.stat

        def fake_stat(path, *a, **kw):
            if os.fspath(path) == str(db):
                return types.SimpleNamespace(st_gid=self.DB_GID)
            return real_stat(path, *a, **kw)

        calls = []
        monkeypatch.setattr(os, "stat", fake_stat)
        monkeypatch.setattr(os, "fchown", lambda fd, uid, gid: calls.append((uid, gid)))
        monkeypatch.setattr(os, "getegid", lambda: 99999)
        monkeypatch.setattr(os, "getgroups", lambda: [])
        return calls

    def test_root_chowns_the_directory_the_lock_and_both_files_to_the_db_group(self, tmp_path, monkeypatch, chown_calls):
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        identity.ensure_identity(tmp_path / "config.db.tls", "client")
        assert chown_calls == [(-1, self.DB_GID)] * 4      # directory, identity.lock, key tmp, cert tmp

    def test_a_non_root_member_of_the_group_does_too(self, tmp_path, monkeypatch, chown_calls):
        monkeypatch.setattr(os, "getgroups", lambda: [7, self.DB_GID])
        identity.ensure_identity(tmp_path / "config.db.tls", "client")
        assert chown_calls == [(-1, self.DB_GID)] * 4      # directory, lock, key tmp, cert tmp

    def test_the_effective_group_counts_as_membership(self, tmp_path, monkeypatch, chown_calls):
        monkeypatch.setattr(os, "getegid", lambda: self.DB_GID)
        identity.ensure_identity(tmp_path / "config.db.tls", "client")
        assert chown_calls == [(-1, self.DB_GID)] * 4      # directory, lock, key tmp, cert tmp

    def test_a_non_member_never_attempts_a_chown(self, tmp_path, chown_calls):
        identity.ensure_identity(tmp_path / "config.db.tls", "client")
        assert chown_calls == []

    def test_group_of_overrides_the_default_reference_file(self, tmp_path, monkeypatch, chown_calls):
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        other = tmp_path / "other"
        other.touch()
        d = tmp_path / "custom-tls-dir"                             # no ".tls" suffix -> no default
        identity.ensure_identity(d, "client")
        assert chown_calls == []
        real_stat = os.stat

        def second_stat(path, *a, **kw):
            if os.fspath(path) == str(other):
                return types.SimpleNamespace(st_gid=555)
            return real_stat(path, *a, **kw)

        monkeypatch.setattr(os, "stat", second_stat)
        d2 = tmp_path / "custom2"
        identity.ensure_identity(d2, "client", group_of=other)
        assert chown_calls == [(-1, 555)] * 4

    def test_a_missing_reference_file_means_no_group_change(self, tmp_path, monkeypatch, chown_calls):
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        identity.ensure_identity(tmp_path / "x.db.tls", "client")   # x.db does not exist
        assert chown_calls == []

    def test_a_failing_chown_is_a_warning_not_an_error(self, tmp_path, monkeypatch, chown_calls, caplog):
        monkeypatch.setattr(os, "geteuid", lambda: 0)

        def eperm(fd, uid, gid):
            raise PermissionError(errno.EPERM, "Operation not permitted")

        monkeypatch.setattr(os, "fchown", eperm)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.identity"):
            ident = identity.ensure_identity(tmp_path / "config.db.tls", "client")
        assert ident.fingerprint
        assert sum("could not give" in r.getMessage() for r in caplog.records) == 4


# ==========================================================================
# promote_identity
# ==========================================================================

def two_identities(tmp_path):
    d = make_tlsdir(tmp_path)
    current = identity.ensure_identity(d, "client")
    successor = identity.ensure_identity(d, "client", name="client.next")
    return d, current, successor


@pytest.mark.unit
class TestPromoteIdentity:
    def test_moves_client_next_over_client(self, tmp_path):
        d, current, successor = two_identities(tmp_path)
        promoted = identity.promote_identity(d)
        assert promoted.fingerprint == successor.fingerprint != current.fingerprint
        assert promoted.cert_path == d / "client.crt" and promoted.key_path == d / "client.key"
        assert identity.load_identity(d / "client.crt", d / "client.key", "client").fingerprint == \
            successor.fingerprint
        assert sorted(p.name for p in d.iterdir()) == ["client.crt", "client.key", "identity.lock"]
        assert mode_of(d / "client.key") == 0o600 and mode_of(d / "client.crt") == 0o640

    def test_works_when_there_is_no_current_identity(self, tmp_path):
        d = make_tlsdir(tmp_path)
        successor = identity.ensure_identity(d, "client", name="client.next")
        assert identity.promote_identity(d).fingerprint == successor.fingerprint
        assert not (d / "client.next.key").exists()

    def test_nothing_to_promote(self, tmp_path):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        before = snapshot(d)
        with pytest.raises(identity.IdentityError, match="no client.next identity"):
            identity.promote_identity(d)
        assert snapshot(d) == before

    def test_promoting_twice_second_time_has_nothing_to_do(self, tmp_path):
        d, _, successor = two_identities(tmp_path)
        identity.promote_identity(d)
        before = snapshot(d)
        with pytest.raises(identity.IdentityError, match="no client.next identity"):
            identity.promote_identity(d)
        assert snapshot(d) == before
        assert identity.peek_identity(d, "client").fingerprint == successor.fingerprint

    def test_the_source_must_be_valid_and_nothing_moves_otherwise(self, tmp_path):
        d, current, _ = two_identities(tmp_path)
        (d / "client.next.key").chmod(0o644)
        before = snapshot(d)
        with pytest.raises(identity.IdentityError):
            identity.promote_identity(d)
        assert snapshot(d) == before

    def test_an_expired_source_is_refused(self, tmp_path):
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        key = key_of("ec256:next-expired")
        nb, na = real_window(before=timedelta(days=3), after=timedelta(days=-1))
        write_pair(d, "client.next", key, make_cert(key, not_before=nb, not_after=na))
        with pytest.raises(identity.IdentityError, match="expired"):
            identity.promote_identity(d)
        assert (d / "client.next.key").exists() and not (d / "client.key").exists()

    def test_a_source_certificate_for_another_key_is_refused(self, tmp_path):
        d, current, _ = two_identities(tmp_path)
        nb, na = real_window()
        (d / "client.next.crt").write_text(
            pem_of(make_cert(key_of("ec256:mismatch"), not_before=nb, not_after=na)))
        before = snapshot(d)
        with pytest.raises(identity.IdentityError, match="does not match"):
            identity.promote_identity(d)
        assert snapshot(d) == before

    @pytest.mark.parametrize("source,target", [
        ("server", "client"), ("client.next", "server"), ("client", "client"),
        ("client.next", "client.next"), ("nope", "client"), ("client.next", "nope"),
    ])
    def test_invalid_pairs(self, tmp_path, source, target):
        d, _, _ = two_identities(tmp_path)
        before = snapshot(d)
        with pytest.raises(ValueError):
            identity.promote_identity(d, source, target)
        assert snapshot(d) == before

    def test_explicit_names(self, tmp_path):
        d, current, successor = two_identities(tmp_path)
        assert identity.promote_identity(d, "client.next", "client").fingerprint == successor.fingerprint
        # and the other direction is a legal pair too (a rollback)
        identity.ensure_identity(d, "client", name="client.next")
        identity.promote_identity(d, source="client", target="client.next")
        assert identity.peek_identity(d, "client.next").fingerprint == successor.fingerprint

    def test_recovery_after_a_crash_between_the_two_renames(self, tmp_path, monkeypatch):
        """Crash for real: the key rename succeeds, the certificate rename fails."""
        d, current, successor = two_identities(tmp_path)
        real_rename = os.rename
        calls = []

        def flaky(src, dst, *a, **kw):
            calls.append((Path(src).name, Path(dst).name))
            if len(calls) == 2:
                raise OSError(errno.EIO, "simulated crash")
            return real_rename(src, dst, *a, **kw)

        monkeypatch.setattr(os, "rename", flaky)
        with pytest.raises(identity.IdentityError, match="cannot promote"):
            identity.promote_identity(d)
        monkeypatch.undo()
        assert calls == [("client.next.key", "client.key"), ("client.next.crt", "client.crt")]
        # the half-done state: new key, old certificate, successor certificate still there
        assert (d / "client.crt").read_text() == current.cert_pem
        assert (d / "client.next.crt").exists() and not (d / "client.next.key").exists()
        with pytest.raises(identity.IdentityError):
            identity.load_identity(d / "client.crt", d / "client.key", "client")

        promoted = identity.promote_identity(d)                       # the re-run completes it
        assert promoted.fingerprint == successor.fingerprint
        assert identity.load_identity(d / "client.crt", d / "client.key", "client").fingerprint == \
            successor.fingerprint
        assert sorted(p.name for p in d.iterdir()) == ["client.crt", "client.key", "identity.lock"]

    def test_recovery_from_a_staged_half_done_state(self, tmp_path):
        d, current, successor = two_identities(tmp_path)
        os.rename(d / "client.next.key", d / "client.key")            # the first rename only
        assert identity.promote_identity(d).fingerprint == successor.fingerprint
        assert not (d / "client.next.crt").exists()

    def test_recovery_refuses_a_source_certificate_that_does_not_fit_the_moved_key(self, tmp_path):
        d, current, successor = two_identities(tmp_path)
        os.rename(d / "client.next.key", d / "client.key")
        nb, na = real_window()
        (d / "client.next.crt").write_text(
            pem_of(make_cert(key_of("ec256:mismatch"), not_before=nb, not_after=na)))
        before = snapshot(d)
        with pytest.raises(identity.IdentityError, match="does not match"):
            identity.promote_identity(d)
        assert snapshot(d) == before

    def test_the_identity_lock_is_taken(self, tmp_path):
        d, _, successor = two_identities(tmp_path)
        fd = os.open(d / "identity.lock", os.O_RDWR | os.O_CREAT, 0o640)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            before = snapshot(d)
            with pytest.raises(identity.IdentityError, match="timed out"):
                identity.promote_identity(d, lock_timeout=0.2)
            assert snapshot(d) == before
        finally:
            os.close(fd)
        assert identity.promote_identity(d, lock_timeout=5).fingerprint == successor.fingerprint


# ==========================================================================
# Interrupted promotions, locking, durability (identity.py review fixes)
# ==========================================================================

needs_fork = pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(), reason="needs the fork start method")
needs_non_root = pytest.mark.skipif(
    os.geteuid() == 0, reason="root ignores directory and file permissions")


def stage_half_promotion(tmp_path, with_current=True):
    """Leave exactly what a process killed between the two renames of
    ``promote_identity`` leaves, without a process: the key has moved, the
    successor's certificate has not. -> ``(dir, current|None, successor)``."""
    d = make_tlsdir(tmp_path)
    cur = identity.ensure_identity(d, "client") if with_current else None
    nxt = identity.ensure_identity(d, "client", name="client.next")
    os.rename(d / "client.next.key", d / "client.key")
    return d, cur, nxt


@contextmanager
def read_only(d: Path):
    """The group-operator view: the directory can be read but not written."""
    d.chmod(0o500)
    try:
        yield
    finally:
        d.chmod(0o750)


def names_in(d: Path) -> List[str]:
    return sorted(p.name for p in d.iterdir())


def plain(exc: BaseException, d: Path) -> str:
    """The message with the (test-name derived) directory path blanked out."""
    return str(exc).replace(str(d), "<dir>")


def _die_after_rename(directory, op, dst_name):
    """Child body: run ``op`` and SIGKILL ourselves right after a rename onto ``dst_name``."""
    import signal
    real = os.rename

    def renamed(src, dst, *a, **kw):
        real(src, dst, *a, **kw)
        if Path(dst).name == dst_name:
            os.kill(os.getpid(), signal.SIGKILL)

    os.rename = renamed
    if op == "promote":
        identity.promote_identity(directory)
    else:
        identity.ensure_identity(directory, "client")
    os._exit(0)                                    # pragma: no cover - not reached


def kill_after_rename(directory, op, dst_name):
    """Really kill a writer at that instant; the child is always joined."""
    import signal
    ctx = multiprocessing.get_context("fork")
    p = ctx.Process(target=_die_after_rename, args=(str(directory), op, dst_name))
    p.start()
    try:
        p.join(60)
    finally:
        if p.is_alive():                           # pragma: no cover - only on a hang
            p.kill()
            p.join(10)
    assert p.exitcode == -signal.SIGKILL, p.exitcode


def _hold_window(directory, ready, go):
    """Child body: a live promoter -- LOCK_EX held between its two renames."""
    d = Path(directory)
    fd = os.open(d / "identity.lock", os.O_RDWR)
    fcntl.flock(fd, fcntl.LOCK_EX)
    os.rename(d / "client.next.key", d / "client.key")
    ready.set()
    go.wait(60)
    os.rename(d / "client.next.crt", d / "client.crt")
    os.close(fd)


@contextmanager
def live_window(d: Path):
    """Another process sits between the renames holding LOCK_EX until ``go`` is set."""
    ctx = multiprocessing.get_context("fork")
    ready, go = ctx.Event(), ctx.Event()
    p = ctx.Process(target=_hold_window, args=(str(d), ready, go))
    p.start()
    try:
        assert ready.wait(30), "the promoter never reached the window"
        yield go
    finally:
        go.set()
        p.join(30)
        if p.is_alive():                           # pragma: no cover - only on a hang
            p.kill()
            p.join(10)
    assert p.exitcode == 0


def run_in_thread(out: dict, label: str, fn) -> threading.Thread:
    def body():
        try:
            out[label] = fn()
        except BaseException as exc:               # reported through ``out``
            out[label] = exc

    t = threading.Thread(target=body, name=f"identity-test-{label}", daemon=True)
    t.start()
    return t


@pytest.mark.integration
@needs_fork
class TestPromoteKilledBetweenTheRenames:
    """A real SIGKILL between the two renames of `promote_identity`, then recovery
    through every public path. The successor's key (K1) must always survive and
    be the client's key afterwards."""

    @pytest.fixture(params=[True, False], ids=["with-current-client", "no-current-client"])
    def killed(self, request, tmp_path):
        d = make_tlsdir(tmp_path)
        cur = identity.ensure_identity(d, "client") if request.param else None
        nxt = identity.ensure_identity(d, "client", name="client.next")
        k1 = (d / "client.next.key").read_bytes()
        kill_after_rename(d, "promote", "client.key")
        return types.SimpleNamespace(d=d, cur=cur, nxt=nxt, k1=k1)

    @staticmethod
    def finished(k):
        """The one acceptable end state of the swap."""
        d = k.d
        assert (d / "client.key").read_bytes() == k.k1
        got = identity.load_identity(d / "client.crt", d / "client.key", "client")
        assert got.fingerprint == k.nxt.fingerprint
        assert k.cur is None or got.fingerprint != k.cur.fingerprint
        assert mode_of(d / "client.key") == 0o600 and mode_of(d / "client.crt") == 0o640
        return got

    def test_the_kill_leaves_the_half_done_state(self, killed):
        d = killed.d
        expected = ["client.key", "client.next.crt", "identity.lock"]
        if killed.cur is not None:
            expected.insert(0, "client.crt")
            assert (d / "client.crt").read_text() == killed.cur.cert_pem     # the old certificate
        assert names_in(d) == expected
        assert (d / "client.key").read_bytes() == killed.k1                 # K1 is already the key

    def test_load_identity_reports_an_interruption_and_changes_nothing(self, killed):
        before = snapshot(killed.d)
        with pytest.raises(identity.IdentityError) as info:
            identity.load_identity(killed.d / "client.crt", killed.d / "client.key", "client")
        msg = plain(info.value, killed.d)
        assert "interrupted" in msg and "Do not delete" in msg and "remove" not in msg
        assert snapshot(killed.d) == before

    def test_ensure_identity_of_the_client_rolls_forward(self, killed, caplog):
        with caplog.at_level(logging.INFO, logger="zfsbackup.identity"):
            got = identity.ensure_identity(killed.d, "client")
        assert got.fingerprint == killed.nxt.fingerprint
        assert names_in(killed.d) == ["client.crt", "client.key", "identity.lock"]
        self.finished(killed)
        messages = [r.getMessage() for r in caplog.records]
        assert any("finished an interrupted promotion" in m for m in messages)
        assert not any(m.startswith("generated a new") for m in messages)    # nothing was generated
        again = snapshot(killed.d)
        assert identity.ensure_identity(killed.d, "client").fingerprint == killed.nxt.fingerprint
        assert snapshot(killed.d) == again

    def test_ensure_identity_of_the_successor_finishes_the_swap_then_makes_a_new_successor(
            self, killed, caplog):
        """`ensure_identity(client.next)` finishes the interrupted swap first -- which
        consumes the old successor -- and then, because `client.next` no longer exists,
        generates a fresh one. The old successor is the client now, not lost."""
        d = killed.d
        with caplog.at_level(logging.INFO, logger="zfsbackup.identity"):
            fresh = identity.ensure_identity(d, "client", name="client.next")
        assert fresh.fingerprint != killed.nxt.fingerprint
        assert killed.cur is None or fresh.fingerprint != killed.cur.fingerprint
        assert sum(r.getMessage().startswith("generated a new") for r in caplog.records) == 1
        assert any("finished an interrupted promotion" in r.getMessage() for r in caplog.records)
        assert names_in(d) == ["client.crt", "client.key", "client.next.crt",
                               "client.next.key", "identity.lock"]
        self.finished(killed)
        assert (d / "client.next.key").read_bytes() != killed.k1
        assert identity.peek_identity(d, "client.next").fingerprint == fresh.fingerprint

    def test_complete_promotion_finishes_it(self, killed):
        got = identity.complete_promotion(killed.d)
        assert got is not None and got.fingerprint == killed.nxt.fingerprint
        assert names_in(killed.d) == ["client.crt", "client.key", "identity.lock"]
        self.finished(killed)
        before = snapshot(killed.d)
        assert identity.complete_promotion(killed.d) is None             # nothing left to do
        assert snapshot(killed.d) == before

    def test_rerunning_promote_identity_finishes_it(self, killed, caplog):
        with caplog.at_level(logging.WARNING, logger="zfsbackup.identity"):
            got = identity.promote_identity(killed.d)
        assert any("finished an interrupted promotion" in r.getMessage() for r in caplog.records)
        assert got.fingerprint == killed.nxt.fingerprint
        assert names_in(killed.d) == ["client.crt", "client.key", "identity.lock"]
        self.finished(killed)

    def test_concurrent_recoveries_agree(self, killed):
        out = {}
        calls = {
            "ensure-a": lambda: identity.ensure_identity(killed.d, "client", lock_timeout=20).fingerprint,
            "ensure-b": lambda: identity.ensure_identity(killed.d, "client", lock_timeout=20).fingerprint,
            "complete": lambda: identity.complete_promotion(killed.d, lock_timeout=20),
        }
        threads = [run_in_thread(out, label, fn) for label, fn in calls.items()]
        for t in threads:
            t.join(30)
        assert not any(t.is_alive() for t in threads)
        assert out["ensure-a"] == out["ensure-b"] == killed.nxt.fingerprint
        assert out["complete"] is None or out["complete"].fingerprint == killed.nxt.fingerprint
        self.finished(killed)
        assert names_in(killed.d) == ["client.crt", "client.key", "identity.lock"]


@pytest.mark.integration
@needs_fork
class TestEnsureKilledAfterTheKeyRename:
    """Finding 2: a valid certificate next to an empty/corrupt/missing key. The old
    certificate must be gone *before* the new key is renamed in, so a kill right after
    that rename never leaves a complete (new key, old certificate) pair."""

    @pytest.mark.parametrize("key_state", ["empty", "garbage", "missing"])
    def test_the_old_certificate_is_already_gone_and_the_next_call_regenerates(self, tmp_path, key_state):
        d = make_tlsdir(tmp_path)
        old = identity.ensure_identity(d, "client")
        key_path = d / "client.key"
        if key_state == "missing":
            key_path.unlink()
        else:
            key_path.write_bytes(b"" if key_state == "empty" else b"this is not a key\n")

        kill_after_rename(d, "ensure", "client.key")

        assert not (d / "client.crt").exists()                       # nothing to pair with the new key
        killed_key = key_path.read_bytes()
        serialization.load_pem_private_key(killed_key, None)         # the new key landed, whole
        assert names_in(d) == [".client.crt.tmp", "client.key", "identity.lock"]
        with pytest.raises(identity.IdentityError, match="does not exist"):
            identity.load_identity(d / "client.crt", key_path, "client")

        new = identity.ensure_identity(d, "client")
        assert new.fingerprint != old.fingerprint
        assert identity.load_identity(d / "client.crt", key_path, "client").fingerprint == new.fingerprint
        assert key_path.read_bytes() != killed_key                   # an orphan key is replaced
        assert names_in(d) == ["client.crt", "client.key", "identity.lock"]


@pytest.mark.unit
class TestPromotionDetection:
    """`_half_promoted` must not mistake other states for an interrupted promotion."""

    @pytest.mark.parametrize("keep_successor_key", [False, True],
                             ids=["successor-key-gone", "successor-key-present"])
    def test_an_orphan_client_key_with_someone_elses_certificate_is_regenerated(
            self, tmp_path, keep_successor_key):
        d = make_tlsdir(tmp_path)
        nxt = identity.ensure_identity(d, "client", name="client.next")
        if not keep_successor_key:
            (d / "client.next.key").unlink()
        (d / "client.key").write_bytes(key_pem(key_of("ec256:orphan")))
        (d / "client.key").chmod(0o600)
        next_cert, next_key = (d / "client.next.crt").read_bytes(), (d / "client.next.key")
        next_key = next_key.read_bytes() if next_key.exists() else None

        got = identity.ensure_identity(d, "client")

        assert got.fingerprint != nxt.fingerprint                      # not rolled forward
        assert (d / "client.next.crt").read_bytes() == next_cert        # and the successor untouched
        assert ((d / "client.next.key").read_bytes() if keep_successor_key else None) == next_key

    def test_a_rollback_promotion_is_recognised_too(self, tmp_path):
        """`promote_identity(client -> client.next)` interrupted: ensure of the *target*
        name finishes it, and the old client becomes client.next."""
        d, cur, nxt = two_identities(tmp_path)
        os.rename(d / "client.key", d / "client.next.key")              # the first rename only
        got = identity.ensure_identity(d, "client", name="client.next")
        assert got.fingerprint == cur.fingerprint
        assert names_in(d) == ["client.next.crt", "client.next.key", "identity.lock"]

    def test_complete_promotion_touches_nothing_when_there_is_nothing_to_finish(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        before = snapshot(d)
        assert identity.complete_promotion(d) is None
        assert snapshot(d) == before

    def test_complete_promotion_creates_no_lock_file_or_directory(self, tmp_path):
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        key = key_of("ec256:nolock")
        nb, na = real_window()
        write_pair(d, "client", key, make_cert(key, not_before=nb, not_after=na))
        assert identity.complete_promotion(d) is None
        assert names_in(d) == ["client.crt", "client.key"]               # no identity.lock appeared
        assert identity.complete_promotion(tmp_path / "nowhere.db.tls") is None
        assert not (tmp_path / "nowhere.db.tls").exists()


@pytest.mark.unit
@needs_non_root
class TestCrashedPromotionSeenByACallerThatCannotWrite:
    """The group-operator view of an interrupted promotion: an explanation that says
    what happened and how to finish it, never advice to delete, and no change."""

    CALLS = {
        "load_identity": lambda d: identity.load_identity(d / "client.crt", d / "client.key", "client"),
        "load_identity_of_the_successor": lambda d: identity.load_identity(
            d / "client.next.crt", d / "client.next.key", "client"),
        "ensure_identity": lambda d: identity.ensure_identity(d, "client"),
        "ensure_identity_of_the_successor": lambda d: identity.ensure_identity(
            d, "client", name="client.next"),
        "complete_promotion": lambda d: identity.complete_promotion(d),
    }

    @pytest.mark.parametrize("lock_present", [True, False], ids=["lock-file", "no-lock-file"])
    @pytest.mark.parametrize("with_current", [True, False], ids=["with-current", "no-current"])
    @pytest.mark.parametrize("call", list(CALLS))
    def test_it_says_interrupted_and_never_advises_removal(self, tmp_path, call, with_current, lock_present):
        d, cur, nxt = stage_half_promotion(tmp_path, with_current)
        if not lock_present:
            (d / "identity.lock").unlink()
        before = snapshot(d)
        with read_only(d):
            with pytest.raises(identity.IdentityError) as info:
                self.CALLS[call](d)
        msg = plain(info.value, d)
        assert "interrupted" in msg, msg
        assert "Do not delete" in msg, msg
        assert "remove" not in msg.lower(), msg
        assert "finish" in msg and "identity rotate" in msg
        assert info.value.path == d / "client.next.crt"
        assert snapshot(d) == before
        assert ((d / "identity.lock").exists()) == lock_present          # and no lock file was made

    def test_promote_identity_is_refused_without_changing_anything(self, tmp_path):
        d, _, _ = stage_half_promotion(tmp_path)
        before = snapshot(d)
        with read_only(d):
            with pytest.raises(identity.IdentityError, match="permission denied"):
                identity.promote_identity(d)
        assert snapshot(d) == before

    def test_the_directory_owner_finishes_it_afterwards(self, tmp_path):
        d, cur, nxt = stage_half_promotion(tmp_path)
        with read_only(d):
            with pytest.raises(identity.IdentityError):
                identity.ensure_identity(d, "client")
        assert identity.ensure_identity(d, "client").fingerprint == nxt.fingerprint


@pytest.mark.integration
@needs_fork
class TestReadersDuringALiveWindow:
    """Another process holds LOCK_EX between the renames: readers wait for it and
    then see the finished pair, never a damage report."""

    def test_load_identity_and_ensure_identity_wait_then_return_the_finished_pair(self, tmp_path):
        d, cur, nxt = two_identities(tmp_path)
        out = {}
        with live_window(d) as go:
            threads = [
                run_in_thread(out, "load", lambda: identity.load_identity(
                    d / "client.crt", d / "client.key", "client", lock_timeout=20).fingerprint),
                run_in_thread(out, "ensure", lambda: identity.ensure_identity(
                    d, "client", lock_timeout=20).fingerprint),
            ]
            time.sleep(0.4)
            assert all(t.is_alive() for t in threads) and out == {}      # waiting, not failing
            go.set()
            for t in threads:
                t.join(20)
        assert not any(t.is_alive() for t in threads)
        assert out == {"load": nxt.fingerprint, "ensure": nxt.fingerprint}
        assert names_in(d) == ["client.crt", "client.key", "identity.lock"]

    def test_a_reader_that_runs_out_of_patience_gets_a_timeout_not_a_damage_report(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        with live_window(d):
            with pytest.raises(identity.IdentityError, match="timed out"):
                identity.load_identity(d / "client.crt", d / "client.key", "client", lock_timeout=0.3)
            with pytest.raises(identity.IdentityError, match="timed out"):
                identity.ensure_identity(d, "client", lock_timeout=0.3)

    def test_complete_promotion_waits_and_finds_nothing_left_to_do(self, tmp_path):
        d, cur, nxt = two_identities(tmp_path)
        out = {}
        with live_window(d) as go:
            t = run_in_thread(out, "complete", lambda: identity.complete_promotion(d, lock_timeout=20))
            time.sleep(0.4)
            assert t.is_alive()
            go.set()
            t.join(20)
        assert not t.is_alive() and out == {"complete": None}            # the live promoter finished it
        assert identity.peek_identity(d, "client").fingerprint == nxt.fingerprint


@pytest.mark.unit
class TestLockModes:
    def test_a_writer_takes_it_exclusively_and_blocks_other_takers(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        fd, exclusive = identity._acquire_lock(d, 1)
        try:
            assert fd is not None and exclusive is True
            with pytest.raises(identity.IdentityError, match="timed out"):
                identity._acquire_lock(d, 0.2)
        finally:
            os.close(fd)
        fd, _ = identity._acquire_lock(d, 1)                              # released on close
        os.close(fd)

    def test_the_lock_file_is_created_exclusively_and_an_existing_one_is_not_touched(self, tmp_path):
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        (d / "identity.lock").write_bytes(b"")
        (d / "identity.lock").chmod(0o600)                                # an operator's own choice
        ino = (d / "identity.lock").stat().st_ino
        identity.ensure_identity(d, "client")
        assert mode_of(d / "identity.lock") == 0o600
        assert (d / "identity.lock").stat().st_ino == ino

    @needs_non_root
    def test_two_read_only_callers_both_get_a_shared_lock(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        with read_only(d):
            a = identity._acquire_lock(d, 1)
            b = identity._acquire_lock(d, 1)                               # does not block on a
            probe = os.open(d / "identity.lock", os.O_RDONLY)
            try:
                assert a[0] is not None and b[0] is not None
                assert a[1] is False and b[1] is False
                with pytest.raises(BlockingIOError):
                    fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)     # they really hold it
                fcntl.flock(probe, fcntl.LOCK_SH | fcntl.LOCK_NB)         # another shared one is fine
            finally:
                for fd in (a[0], b[0], probe):
                    os.close(fd)

    @needs_non_root
    def test_no_lock_file_and_no_write_access_means_no_lock(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        (d / "identity.lock").unlink()
        with read_only(d):
            assert identity._acquire_lock(d, 1) == (None, False)
        assert not (d / "identity.lock").exists()

    @needs_non_root
    def test_a_reader_is_not_blocked_by_other_readers_and_leaves_a_half_done_state_alone(self, tmp_path):
        d, _, _ = stage_half_promotion(tmp_path)
        before = snapshot(d)
        out = {}
        with read_only(d):
            holder = identity._acquire_lock(d, 1)                          # a reader mid-check
            try:
                started = time.monotonic()
                threads = [run_in_thread(out, f"r{i}", lambda: identity.ensure_identity(d, "client", lock_timeout=2))
                           for i in range(2)]
                for t in threads:
                    t.join(10)
                elapsed = time.monotonic() - started
            finally:
                os.close(holder[0])
        assert not any(t.is_alive() for t in threads)
        assert elapsed < 1.5                                              # nobody waited out a timeout
        for label in ("r0", "r1"):
            assert isinstance(out[label], identity.IdentityError), out
            assert "interrupted" in str(out[label]) and "timed out" not in str(out[label])
        assert snapshot(d) == before

    def test_a_shared_holder_holds_a_writer_up_and_nothing_changes(self, tmp_path):
        d, _, _ = stage_half_promotion(tmp_path)
        before = snapshot(d)
        fd = os.open(d / "identity.lock", os.O_RDONLY)
        try:
            fcntl.flock(fd, fcntl.LOCK_SH)
            with pytest.raises(identity.IdentityError, match="timed out"):
                identity.ensure_identity(d, "client", lock_timeout=0.2)
        finally:
            os.close(fd)
        assert snapshot(d) == before


@pytest.mark.unit
class TestRefusalsAreLeftAlone:
    """Every `_inspect` refusal says the identity was left alone; a permission-denied
    one is not a defect of the identity and never suggests removing it."""

    @needs_non_root
    @pytest.mark.parametrize("which", ["private key", "certificate"])
    def test_permission_denied_is_reported_without_removal_advice(self, tmp_path, which):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        target = d / ("client.key" if which == "private key" else "client.crt")
        before = snapshot(d)
        target.chmod(0o000)
        try:
            with pytest.raises(identity._AccessDenied) as info:
                identity.ensure_identity(d, "client")
        finally:
            target.chmod(0o600 if which == "private key" else 0o640)
        assert snapshot(d) == before
        msg = plain(info.value, d)
        assert "permission denied" in msg and "left alone" in msg
        assert "remove" not in msg.lower() and "re-enroll" not in msg
        assert info.value.path == target

    def test_a_key_that_is_a_directory_is_wrapped_as_left_alone(self, tmp_path):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        (d / "client.key").unlink()
        (d / "client.key").mkdir()
        with pytest.raises(identity.IdentityError) as info:
            identity.ensure_identity(d, "client")
        assert "not a regular file" in str(info.value) and "left alone" in str(info.value)
        assert "re-enroll" in str(info.value)                              # a real defect: the usual advice

    def test_a_refusal_that_is_not_a_permission_problem_keeps_the_start_over_advice(self, tmp_path):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        (d / "client.key").chmod(0o666)
        with pytest.raises(identity.IdentityError) as info:
            identity.ensure_identity(d, "client")
        assert not isinstance(info.value, identity._AccessDenied)
        assert "left alone" in str(info.value) and "remove client.crt and client.key" in str(info.value)


@pytest.mark.unit
class TestUnlinkQuiet:
    def test_removes_an_existing_file_and_ignores_a_missing_one(self, tmp_path):
        f = tmp_path / "x"
        f.write_text("x")
        assert identity._unlink_quiet(f) is True and not f.exists()
        assert identity._unlink_quiet(f) is False

    def test_permission_error_becomes_an_identity_error(self, tmp_path, monkeypatch):
        f = tmp_path / "x"
        f.write_text("x")

        def denied(path, *a, **kw):
            raise PermissionError(errno.EACCES, "Permission denied")

        monkeypatch.setattr(os, "unlink", denied)
        with pytest.raises(identity.IdentityError, match="permission denied") as info:
            identity._unlink_quiet(f)
        assert info.value.path == f
        assert not isinstance(info.value, PermissionError)

    def test_any_other_os_error_becomes_an_identity_error_too(self, tmp_path, monkeypatch):
        def broken(path, *a, **kw):
            raise OSError(errno.EIO, "Input/output error")

        monkeypatch.setattr(os, "unlink", broken)
        with pytest.raises(identity.IdentityError, match="cannot remove"):
            identity._unlink_quiet(tmp_path / "x")

    def test_a_failing_unlink_of_the_old_certificate_stops_ensure_identity_cleanly(self, tmp_path, monkeypatch):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        (d / "client.key").unlink()                                       # orphan certificate
        real_unlink = os.unlink

        def selective(path, *a, **kw):
            if Path(path).name == "client.crt":
                raise PermissionError(errno.EACCES, "Permission denied")
            return real_unlink(path, *a, **kw)

        monkeypatch.setattr(os, "unlink", selective)
        before = snapshot(d)
        with pytest.raises(identity.IdentityError, match="cannot remove"):
            identity.ensure_identity(d, "client")
        assert snapshot(d) == before                                      # no key appeared next to it


@pytest.fixture
def fs_events(monkeypatch):
    """Record renames, unlinks and fsyncs (of files and directories) in call order."""
    log = []
    real_fsync, real_rename, real_unlink = os.fsync, os.rename, os.unlink

    def fsync(fd):
        try:
            st = os.fstat(fd)
            what = ("dir", st.st_dev, st.st_ino) if stat.S_ISDIR(st.st_mode) else ("file",)
        except OSError:
            what = ("?",)
        log.append(("fsync",) + what)
        return real_fsync(fd)

    def rename(src, dst, *a, **kw):
        log.append(("rename", Path(src).name, Path(dst).name))
        return real_rename(src, dst, *a, **kw)

    def unlink(path, *a, **kw):
        log.append(("unlink", Path(path).name))
        return real_unlink(path, *a, **kw)

    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(os, "rename", rename)
    monkeypatch.setattr(os, "unlink", unlink)

    def durable(tls: Path):
        """The log with directory fsyncs named, file fsyncs and tmp unlinks dropped."""
        ids = {}
        for label, path in (("parent", tls.parent), ("tls", tls)):
            if path.exists():
                st = os.stat(path)
                ids[("dir", st.st_dev, st.st_ino)] = label
        out = []
        for ev in log:
            if ev[0] == "fsync":
                if ev[1] != "file":
                    out.append(("fsync", ids.get(ev[1:], "other-dir")))
                else:
                    out.append(("fsync", "file"))
            elif ev[0] == "unlink" and ev[1].startswith("."):
                continue
            else:
                out.append(ev)
        return out

    durable.raw = log
    return durable


def without_file_fsyncs(events):
    return [e for e in events if e != ("fsync", "file")]


@pytest.mark.unit
class TestDurabilityOrdering:
    def test_the_parent_is_fsynced_after_creating_the_tls_directory_and_the_dir_after_each_rename(
            self, tmp_path, fs_events):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        events = fs_events(d)
        assert without_file_fsyncs(events) == [
            ("fsync", "parent"),
            ("rename", ".client.key.tmp", "client.key"), ("fsync", "tls"),
            ("rename", ".client.crt.tmp", "client.crt"), ("fsync", "tls"),
        ]
        # and the data of both files is on disk before either is renamed in
        assert events.index(("fsync", "file")) < events.index(("rename", ".client.key.tmp", "client.key"))
        assert events[:events.index(("rename", ".client.key.tmp", "client.key"))].count(("fsync", "file")) == 2

    def test_an_existing_directory_is_not_fsynced_in_its_parent(self, tmp_path, fs_events):
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        identity.ensure_identity(d, "client")
        assert ("fsync", "parent") not in fs_events(d)

    @pytest.mark.parametrize("key_state", ["missing", "empty", "garbage"])
    def test_an_old_certificate_is_unlinked_and_the_unlink_made_durable_before_the_new_key_is_written(
            self, tmp_path, fs_events, key_state):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        key_path = d / "client.key"
        if key_state == "missing":
            key_path.unlink()
        else:
            key_path.write_bytes(b"" if key_state == "empty" else b"junk\n")
        fs_events.raw.clear()
        identity.ensure_identity(d, "client")
        events = fs_events(d)
        assert events[:2] == [("unlink", "client.crt"), ("fsync", "tls")]
        assert without_file_fsyncs(events) == [
            ("unlink", "client.crt"), ("fsync", "tls"),
            ("rename", ".client.key.tmp", "client.key"), ("fsync", "tls"),
            ("rename", ".client.crt.tmp", "client.crt"), ("fsync", "tls"),
        ]

    def test_promote_identity_fsyncs_the_directory_between_its_renames_and_after_each(self, tmp_path, fs_events):
        d, _, _ = two_identities(tmp_path)
        fs_events.raw.clear()
        identity.promote_identity(d)
        assert without_file_fsyncs(fs_events(d)) == [
            ("rename", "client.next.key", "client.key"), ("fsync", "tls"),
            ("rename", "client.next.crt", "client.crt"), ("fsync", "tls"),
        ]

    def test_finishing_an_interrupted_promotion_fsyncs_after_the_renaming(self, tmp_path, fs_events):
        d, _, _ = stage_half_promotion(tmp_path)
        fs_events.raw.clear()
        assert identity.complete_promotion(d) is not None
        assert without_file_fsyncs(fs_events(d)) == [
            ("rename", "client.next.crt", "client.crt"), ("fsync", "tls"),
        ]

    def _fsync_failing_for(self, monkeypatch, directory: Path, exc: OSError):
        st = os.stat(directory)
        real_fsync = os.fsync

        def fsync(fd):
            now = os.fstat(fd)
            if stat.S_ISDIR(now.st_mode) and (now.st_dev, now.st_ino) == (st.st_dev, st.st_ino):
                raise exc
            return real_fsync(fd)

        monkeypatch.setattr(os, "fsync", fsync)

    def test_a_parent_fsync_failure_is_only_a_warning(self, tmp_path, monkeypatch, caplog):
        d = make_tlsdir(tmp_path)
        self._fsync_failing_for(monkeypatch, tmp_path, OSError(errno.EIO, "Input/output error"))
        with caplog.at_level(logging.WARNING, logger="zfsbackup.identity"):
            assert identity.ensure_identity(d, "client").fingerprint
        assert any("cannot fsync directory" in r.getMessage() and str(tmp_path) in r.getMessage()
                   for r in caplog.records)

    def test_a_filesystem_that_refuses_directory_fsync_is_tolerated_silently(self, tmp_path, monkeypatch, caplog):
        d = make_tlsdir(tmp_path)
        self._fsync_failing_for(monkeypatch, tmp_path, OSError(errno.EINVAL, "Invalid argument"))
        with caplog.at_level(logging.WARNING, logger="zfsbackup.identity"):
            assert identity.ensure_identity(d, "client").fingerprint
        assert not any("fsync" in r.getMessage() for r in caplog.records)

    def test_a_failing_fsync_of_the_tls_directory_after_a_rename_is_an_error(self, tmp_path, monkeypatch):
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        self._fsync_failing_for(monkeypatch, d, OSError(errno.EIO, "Input/output error"))
        with pytest.raises(identity.IdentityError, match="cannot fsync directory"):
            identity.ensure_identity(d, "client")


# ==========================================================================
# The multi-process race (real processes; fork, spawn and forkserver)
# ==========================================================================

class _GenerationCounter(logging.Handler):
    """Counts how many times *this process* generated an identity."""

    def __init__(self):
        super().__init__(logging.INFO)
        self.count = 0

    def emit(self, record):
        if record.getMessage().startswith("generated a new"):
            self.count += 1


def _race_child(directory, role, barrier, out):
    """Child body: line up on a barrier, call ensure_identity, report. Module level
    so that spawn/forkserver can import it by reference."""
    counter = _GenerationCounter()
    log = logging.getLogger("zfsbackup.identity")
    log.setLevel(logging.INFO)
    log.addHandler(counter)
    try:
        barrier.wait(60)
        ident = identity.ensure_identity(directory, role, lock_timeout=60)
        out.put(("ok", ident.fingerprint, counter.count))
    except BaseException as exc:                               # reported, never swallowed
        out.put(("error", repr(exc), counter.count))


START_METHODS = [m for m in ("fork", "spawn", "forkserver")
                 if m in multiprocessing.get_all_start_methods()]


def run_race(method: str, directory: Path, role: str, workers: int = 4):
    """Start ``workers`` processes that call ensure_identity at the same moment.
    Every process is joined, and killed if it does not finish."""
    mp = multiprocessing.get_context(method)
    barrier = mp.Barrier(workers)
    out = mp.Queue()
    procs = [mp.Process(target=_race_child, args=(str(directory), role, barrier, out), daemon=True)
             for _ in range(workers)]
    results = []
    try:
        for p in procs:
            p.start()
        for _ in procs:
            results.append(out.get(timeout=120))
        for p in procs:
            p.join(timeout=60)
    finally:
        for p in procs:
            if p.is_alive():
                p.kill()
            p.join(timeout=10)
        out.close()
        out.join_thread()
    return results, [p.exitcode for p in procs]


@pytest.mark.integration
class TestEnsureIdentityRace:
    @pytest.mark.parametrize("scenario", ["fresh", "orphan_certificate"])
    @pytest.mark.parametrize("method", START_METHODS)
    def test_concurrent_callers_end_up_with_one_matching_pair(self, tmp_path, method, scenario):
        role = "client"
        d = make_tlsdir(tmp_path)
        old_cert = None
        if scenario == "orphan_certificate":
            identity.ensure_identity(d, role)
            old_cert = (d / f"{role}.crt").read_bytes()
            (d / f"{role}.key").unlink()

        results, exitcodes = run_race(method, d, role)

        assert exitcodes == [0] * len(results)
        assert [r[0] for r in results] == ["ok"] * len(results), results
        fingerprints = {r[1] for r in results}
        assert len(fingerprints) == 1, f"processes disagree: {results}"
        # exactly one process generated; everybody else adopted its result
        assert sum(r[2] for r in results) == 1, results
        final = identity.load_identity(d / f"{role}.crt", d / f"{role}.key", role)
        assert {final.fingerprint} == fingerprints
        if old_cert is not None:
            assert (d / f"{role}.crt").read_bytes() != old_cert
        assert sorted(p.name for p in d.iterdir()) == [f"{role}.crt", f"{role}.key", "identity.lock"]
        assert mode_of(d / f"{role}.key") == 0o600 and mode_of(d / f"{role}.crt") == 0o640
        assert mode_of(d) == 0o750

    def test_both_roles_at_once_do_not_interfere(self, tmp_path):
        """Server and client identities are separate files behind one lock."""
        d = make_tlsdir(tmp_path)
        mp = multiprocessing.get_context(START_METHODS[0])
        barrier = mp.Barrier(4)
        out = mp.Queue()
        roles = ["server", "client", "server", "client"]
        procs = [mp.Process(target=_race_child, args=(str(d), r, barrier, out), daemon=True)
                 for r in roles]
        try:
            for p in procs:
                p.start()
            results = [out.get(timeout=120) for _ in procs]
            for p in procs:
                p.join(timeout=60)
        finally:
            for p in procs:
                if p.is_alive():
                    p.kill()
                p.join(timeout=10)
            out.close()
            out.join_thread()
        assert [r[0] for r in results] == ["ok"] * 4, results
        assert sum(r[2] for r in results) == 2
        assert len({r[1] for r in results}) == 2
        for role in ("server", "client"):
            identity.load_identity(d / f"{role}.crt", d / f"{role}.key", role)


# ==========================================================================
# TLS contexts
# ==========================================================================

def server_ctx(server_id, *clients):
    return identity.server_context(server_id, [c.cert_pem for c in clients])


@pytest.mark.integration
class TestContexts:
    def test_server_context_settings(self, server_id, client_a):
        ctx = server_ctx(server_id, client_a)
        assert ctx.num_tickets == 0
        assert ctx.minimum_version == ssl.TLSVersion.TLSv1_3
        assert ctx.verify_mode == ssl.CERT_OPTIONAL
        assert ctx.verify_flags & ssl.VERIFY_X509_PARTIAL_CHAIN
        assert ctx.cert_store_stats()["x509"] == 1

    def test_the_pins_are_the_only_trust_anchors(self, server_id, client_a, client_b):
        assert server_ctx(server_id).cert_store_stats()["x509"] == 0           # nothing loaded at all
        assert server_ctx(server_id, client_a, client_b).cert_store_stats()["x509"] == 2

    def test_pins_without_a_trailing_newline_and_generators_are_accepted(self, server_id, client_a, client_b):
        pems = (c.cert_pem.rstrip("\n") for c in (client_a, client_b))
        assert identity.server_context(server_id, pems).cert_store_stats()["x509"] == 2

    @pytest.mark.parametrize("bad", ["not a certificate", "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----"])
    def test_an_unusable_pin_is_a_value_error(self, server_id, bad):
        with pytest.raises(ValueError):
            identity.server_context(server_id, [bad])

    def test_a_vanished_identity_is_an_identity_error(self, server_id, tmp_path):
        gone = dataclasses.replace(server_id, cert_path=tmp_path / "gone.crt", key_path=tmp_path / "gone.key")
        with pytest.raises(identity.IdentityError):
            identity.server_context(gone, [])
        with pytest.raises(identity.IdentityError):
            identity.client_context(dataclasses.replace(gone, role="client"))

    def test_no_session_tickets_are_issued_so_every_connection_is_verified_afresh(self, server_id, client_a):
        """With tickets a resumed session would skip checking the client certificate
        against the *current* pin set -- the premise of swapping contexts per connection."""
        outcomes = {}
        for label, tickets in (("default", 2), ("ours", None)):
            ctx = server_ctx(server_id, client_a)
            if tickets is not None:
                ctx.num_tickets = tickets                   # control: a server that does issue them
            cctx = identity.client_context(client_a)
            with serving(ctx, mode="echo") as srv:
                first = identity.connect_pinned(LOOPBACK, srv.port, cctx, server_id.fingerprint, IO_TIMEOUT)
                first.sendall(b"x")
                assert first.recv(100) == b"echo:x"
                session = first.session
                first.close()
                raw = socket.create_connection((LOOPBACK, srv.port), timeout=IO_TIMEOUT)
                second = cctx.wrap_socket(raw, server_hostname=None, session=session)
                outcomes[label] = (session is not None, second.session_reused)
                second.close()
        assert outcomes["default"] == (True, True), "control failed: this platform never resumes"
        assert outcomes["ours"][1] is False

    def test_client_context_trusts_nothing_and_demands_tls13(self):
        ctx = identity.client_context()
        assert ctx.check_hostname is False
        assert ctx.verify_mode == ssl.CERT_NONE
        assert ctx.minimum_version == ssl.TLSVersion.TLSv1_3

    def test_a_client_without_an_identity_presents_no_certificate(self, server_id, client_a):
        with serving(server_ctx(server_id, client_a), mode="echo") as srv:
            sock = identity.connect_pinned(LOOPBACK, srv.port, identity.client_context(),
                                           server_id.fingerprint, IO_TIMEOUT)
            sock.sendall(b"hi")
            assert sock.recv(100) == b"echo:hi"
            sock.close()
            conn = srv.wait_for(1)[0]
        assert conn.ok and conn.peer_der is None            # CERT_OPTIONAL: handshake still completes

    def test_a_client_with_an_identity_presents_it(self, server_id, client_a):
        with serving(server_ctx(server_id, client_a), mode="echo") as srv:
            sock = identity.connect_pinned(LOOPBACK, srv.port, identity.client_context(client_a),
                                           server_id.fingerprint, IO_TIMEOUT)
            sock.sendall(b"hi")
            assert sock.recv(100) == b"echo:hi"
            sock.close()
            conn = srv.wait_for(1)[0]
        assert identity.fingerprint_der(conn.peer_der) == client_a.fingerprint

    def test_a_tls12_only_client_is_refused(self, server_id, client_a):
        legacy = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        legacy.check_hostname = False
        legacy.verify_mode = ssl.CERT_NONE
        legacy.maximum_version = ssl.TLSVersion.TLSv1_2
        with serving(server_ctx(server_id, client_a), mode="echo") as srv:
            with pytest.raises(ssl.SSLError):
                identity.connect_pinned(LOOPBACK, srv.port, legacy, server_id.fingerprint, IO_TIMEOUT)

    def test_the_client_refuses_a_tls12_only_server(self, server_id):
        old = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        old.maximum_version = ssl.TLSVersion.TLSv1_2
        old.load_cert_chain(str(server_id.cert_path), str(server_id.key_path))
        with serving(old, mode="echo") as srv:
            with pytest.raises(ssl.SSLError):
                identity.connect_pinned(LOOPBACK, srv.port, identity.client_context(),
                                        server_id.fingerprint, IO_TIMEOUT)


# ==========================================================================
# Contexts present exactly the validated certificate (`_load_chain`, `_blob_paths`)
# ==========================================================================

def open_fds() -> int:
    return len(os.listdir("/dev/fd"))


_HAS_MEMFD = (hasattr(os, "memfd_create") and os.path.isdir("/proc/self/fd"))
_HAS_DEV_FD = os.path.isdir("/dev/fd")


@pytest.fixture(params=["memfd", "pipe", "tempdir"])
def blob_branch(request, monkeypatch):
    """Force one `_blob_paths` branch (skipping those this platform cannot run) and
    record which branches actually exposed data: ``.used``, ``.tmpdirs``."""
    branch = request.param
    if branch == "memfd" and not _HAS_MEMFD:
        pytest.skip("memfd_create and /proc/self/fd are Linux only")
    if branch == "pipe" and not _HAS_DEV_FD:
        pytest.skip("no /dev/fd on this platform")
    rec = types.SimpleNamespace(branch=branch, used=[], tmpdirs=[])
    real_memfd, real_pipe, real_mkdtemp = identity._memfd_paths, identity._pipe_paths, tempfile.mkdtemp

    def memfd(blobs, fds):
        if branch != "memfd":
            return None
        paths = real_memfd(blobs, fds)
        if paths is not None:
            rec.used.append("memfd")
        return paths

    def pipe(blobs, fds):
        if branch != "pipe":
            return None
        paths = real_pipe(blobs, fds)
        if paths is not None:
            rec.used.append("pipe")
        return paths

    def mkdtemp(*a, **kw):
        path = real_mkdtemp(*a, **kw)
        rec.tmpdirs.append(path)
        rec.used.append("tempdir")
        return path

    monkeypatch.setattr(identity, "_memfd_paths", memfd)
    monkeypatch.setattr(identity, "_pipe_paths", pipe)
    monkeypatch.setattr(tempfile, "mkdtemp", mkdtemp)
    return rec


def assert_clean(rec, fds_before):
    assert not any(os.path.exists(p) for p in rec.tmpdirs), "temporary directory left behind"
    assert open_fds() == fds_before, "file descriptors leaked"


@pytest.mark.unit
class TestBlobPaths:
    BLOBS = (b"first blob\n" * 20, b"second blob\n")

    def test_every_blob_is_readable_inside_and_everything_is_gone_after(self, blob_branch):
        before = open_fds()
        with identity._blob_paths(*self.BLOBS) as paths:
            assert len(paths) == 2
            assert [Path(p).read_bytes() if blob_branch.branch == "tempdir"
                    else open(p, "rb").read() for p in paths] == list(self.BLOBS)
            if blob_branch.branch == "tempdir":
                tmp = Path(paths[0]).parent
                assert mode_of(tmp) == 0o700
                assert all(mode_of(p) == 0o600 for p in paths)
        assert blob_branch.used == [blob_branch.branch]
        assert_clean(blob_branch, before)

    def test_cleanup_also_happens_when_the_body_raises(self, blob_branch):
        before = open_fds()
        with pytest.raises(RuntimeError, match="boom"):
            with identity._blob_paths(*self.BLOBS):
                raise RuntimeError("boom")
        assert_clean(blob_branch, before)

    @pytest.mark.skipif(not _HAS_DEV_FD, reason="no /dev/fd on this platform")
    def test_data_that_does_not_fit_a_pipe_falls_back_to_a_private_temporary_directory(
            self, monkeypatch):
        monkeypatch.setattr(identity, "_memfd_paths", lambda blobs, fds: None)
        made = []
        real_mkdtemp = tempfile.mkdtemp
        monkeypatch.setattr(tempfile, "mkdtemp", lambda *a, **kw: made.append(real_mkdtemp(*a, **kw)) or made[-1])
        big = b"x" * (4 * 1024 * 1024)               # far beyond any pipe buffer
        before = open_fds()
        with identity._blob_paths(big, b"small") as paths:
            assert Path(paths[0]).read_bytes() == big and Path(paths[1]).read_bytes() == b"small"
            assert made and mode_of(made[0]) == 0o700
        assert len(made) == 1 and not os.path.exists(made[0])
        assert open_fds() == before

    @pytest.mark.skipif(not _HAS_DEV_FD, reason="no /dev/fd on this platform")
    def test_a_partly_filled_pipe_does_not_leak_descriptors_when_it_gives_up(self, monkeypatch):
        monkeypatch.setattr(identity, "_memfd_paths", lambda blobs, fds: None)
        before = open_fds()
        fds = []
        assert identity._pipe_paths((b"ok", b"y" * (4 * 1024 * 1024)), fds) is None
        assert fds == [] and open_fds() == before


def forge(tmp_path, role):
    """A loaded identity in a directory of its own, and a different one to swap in."""
    mine, theirs = tmp_path / "mine", tmp_path / "theirs"
    mine.mkdir()
    theirs.mkdir()
    return (identity.ensure_identity(make_tlsdir(mine), role),
            identity.ensure_identity(make_tlsdir(theirs), role))


SWAPS = {
    "cert_and_key": lambda me, other: (me.cert_path.write_bytes(other.cert_path.read_bytes()),
                                       me.key_path.write_bytes(other.key_path.read_bytes())),
    "cert_only": lambda me, other: me.cert_path.write_bytes(other.cert_path.read_bytes()),
    "key_only": lambda me, other: me.key_path.write_bytes(other.key_path.read_bytes()),
    "key_made_world_readable": lambda me, other: me.key_path.chmod(0o644),
    "cert_deleted": lambda me, other: me.cert_path.unlink(),
    "cert_corrupted": lambda me, other: me.cert_path.write_text("not a certificate\n"),
}
SWAP_MESSAGES = {
    "cert_and_key": "changed since the identity was loaded",
    "cert_only": "changed since the identity was loaded",
    "key_only": "no longer matches",
    "key_made_world_readable": "readable by everyone",
    "cert_deleted": "does not exist",
    "cert_corrupted": "does not contain a PEM certificate",
}


def make_context(role, ident):
    return identity.server_context(ident, []) if role == "server" else identity.client_context(ident)


@pytest.mark.integration
class TestContextsPresentExactlyTheValidatedCertificate:
    @pytest.mark.parametrize("role", ["server", "client"])
    @pytest.mark.parametrize("swap", list(SWAPS))
    def test_files_that_changed_after_loading_are_refused_before_any_data_is_exposed(
            self, tmp_path, blob_branch, role, swap):
        me, other = forge(tmp_path, role)
        SWAPS[swap](me, other)
        before = open_fds()
        with pytest.raises(identity.IdentityError, match=SWAP_MESSAGES[swap]):
            make_context(role, me)
        assert blob_branch.used == []                    # refused before any blob was exposed
        assert_clean(blob_branch, before)

    @pytest.mark.parametrize("role", ["server", "client"])
    def test_putting_the_original_files_back_makes_the_identity_usable_again(self, tmp_path, role):
        me, other = forge(tmp_path, role)
        cert, key = me.cert_path.read_bytes(), me.key_path.read_bytes()
        SWAPS["cert_and_key"](me, other)
        with pytest.raises(identity.IdentityError):
            make_context(role, me)
        me.cert_path.write_bytes(cert)
        me.key_path.write_bytes(key)
        assert isinstance(make_context(role, me), ssl.SSLContext)

    def test_the_client_context_presents_the_certificate_through_every_branch(
            self, blob_branch, server_id, client_a):
        before = open_fds()
        with serving(server_ctx(server_id, client_a), mode="echo") as srv:
            sock = identity.connect_pinned(LOOPBACK, srv.port, identity.client_context(client_a),
                                           server_id.fingerprint, IO_TIMEOUT)
            sock.sendall(b"hi")
            assert sock.recv(100) == b"echo:hi"
            sock.close()
            conn = srv.wait_for(1)[0]
        assert identity.fingerprint_der(conn.peer_der) == client_a.fingerprint
        assert blob_branch.used == [blob_branch.branch] * 2         # the server's and the client's
        assert_clean(blob_branch, before)

    def test_the_server_context_presents_the_certificate_through_every_branch(
            self, blob_branch, server_id, client_a):
        before = open_fds()
        ctx = server_ctx(server_id, client_a)
        with serving(ctx, mode="echo") as srv:
            sock = identity.connect_pinned(LOOPBACK, srv.port, identity.client_context(client_a),
                                           server_id.fingerprint, IO_TIMEOUT)    # the pin is the check
            sock.sendall(b"hi")
            assert sock.recv(100) == b"echo:hi"
            sock.close()
        assert blob_branch.used.count(blob_branch.branch) == 2      # server context + client context
        assert_clean(blob_branch, before)

    @pytest.mark.parametrize("role", ["server", "client"])
    def test_a_load_failure_still_cleans_up(self, tmp_path, monkeypatch, blob_branch, role):
        me, _ = forge(tmp_path, role)

        def refuse(self, certfile, keyfile=None, password=None):
            raise ssl.SSLError("simulated")

        monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", refuse)
        before = open_fds()
        with pytest.raises(identity.IdentityError, match="cannot load TLS identity"):
            make_context(role, me)
        assert blob_branch.used == [blob_branch.branch]              # the blobs were exposed ...
        assert_clean(blob_branch, before)                            # ... and are gone again

    def test_the_context_uses_the_bytes_it_validated_not_the_files_on_disk_now(
            self, tmp_path, monkeypatch, server_id):
        """Swap the files after `_load_chain` has checked them and before OpenSSL reads
        the blobs: the context must still present the checked certificate."""
        me, other = forge(tmp_path, "client")
        real_blob_paths = identity._blob_paths

        @contextmanager
        def swapping(*blobs):
            SWAPS["cert_and_key"](me, other)
            with real_blob_paths(*blobs) as paths:
                yield paths

        monkeypatch.setattr(identity, "_blob_paths", swapping)
        ctx = identity.client_context(me)
        monkeypatch.undo()
        assert me.cert_path.read_bytes() == other.cert_path.read_bytes()    # the swap did happen
        with serving(server_ctx(server_id, me), mode="echo") as srv:         # only `me` is pinned
            sock = identity.connect_pinned(LOOPBACK, srv.port, ctx, server_id.fingerprint, IO_TIMEOUT)
            sock.sendall(b"hi")
            assert sock.recv(100) == b"echo:hi"
            sock.close()
            conn = srv.wait_for(1)[0]
        assert identity.fingerprint_der(conn.peer_der) == me.fingerprint

    def test_a_context_waits_for_a_writer_in_the_middle_of_a_move(self, tmp_path):
        d, cur, nxt = two_identities(tmp_path)
        out = {}
        fd = os.open(d / "identity.lock", os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            t = run_in_thread(out, "ctx", lambda: identity.client_context(cur))
            time.sleep(0.4)
            assert t.is_alive()
        finally:
            os.close(fd)
        t.join(20)
        assert not t.is_alive() and isinstance(out["ctx"], ssl.SSLContext)

    def test_a_context_for_an_identity_outside_a_tls_directory_needs_no_lock(self, tmp_path):
        key = key_of("ec256:plain")
        nb, na = real_window()
        write_pair(tmp_path, "client", key, make_cert(key, not_before=nb, not_after=na))
        ident = identity.load_identity(tmp_path / "client.crt", tmp_path / "client.key", "client")
        assert isinstance(identity.client_context(ident), ssl.SSLContext)
        assert names_in(tmp_path) == ["client.crt", "client.key"]


# ==========================================================================
# connect_pinned
# ==========================================================================

@pytest.mark.integration
class TestConnectPinned:
    def test_the_right_pin_returns_a_usable_tls13_socket(self, server_id, client_a):
        with serving(server_ctx(server_id, client_a), mode="echo") as srv:
            sock = identity.connect_pinned(LOOPBACK, srv.port, identity.client_context(client_a),
                                           server_id.fingerprint, 7.5)
            try:
                assert isinstance(sock, ssl.SSLSocket)
                assert sock.version() == "TLSv1.3"
                assert sock.gettimeout() == 7.5
                assert identity.fingerprint_der(sock.getpeercert(binary_form=True)) == server_id.fingerprint
                sock.sendall(b"ping")
                assert sock.recv(100) == b"echo:ping"
            finally:
                sock.close()

    @pytest.mark.parametrize("spelling", [
        lambda fp: fp.upper(),
        lambda fp: ":".join(fp[i:i + 2] for i in range(0, 64, 2)),
        lambda fp: f"  {fp}\n",
    ], ids=["upper", "colon", "padded"])
    def test_any_spelling_of_the_fingerprint_is_accepted(self, server_id, client_a, spelling):
        with serving(server_ctx(server_id, client_a), mode="echo") as srv:
            sock = identity.connect_pinned(LOOPBACK, srv.port, identity.client_context(client_a),
                                           spelling(server_id.fingerprint), IO_TIMEOUT)
            sock.close()

    def test_the_timeout_may_be_none(self, server_id, client_a):
        with serving(server_ctx(server_id, client_a), mode="echo") as srv:
            sock = identity.connect_pinned(LOOPBACK, srv.port, identity.client_context(client_a),
                                           server_id.fingerprint, None)
            assert sock.gettimeout() is None
            sock.close()

    def test_a_wrong_pin_raises_before_any_application_byte_is_sent(self, server_id, client_a):
        wrong = hashlib.sha256(b"someone else").hexdigest()
        with serving(server_ctx(server_id, client_a), mode="echo") as srv:
            with pytest.raises(identity.PinMismatch) as info:
                identity.connect_pinned(LOOPBACK, srv.port, identity.client_context(client_a),
                                        wrong, IO_TIMEOUT)
            conn = srv.wait_for(1)[0]
        exc = info.value
        assert (exc.expected, exc.actual) == (wrong, server_id.fingerprint)
        assert wrong in str(exc) and server_id.fingerprint in str(exc)
        # the handshake itself completed (the pin is checked after it) ...
        assert conn.ok and identity.fingerprint_der(conn.peer_der) == client_a.fingerprint
        # ... but not one application byte ever reached the server
        assert conn.received == b""

    def test_pin_mismatch_is_not_an_oserror(self):
        """OSError means 'unreachable, retry later'; an impostor must not look like that."""
        exc = identity.PinMismatch("a" * 64, "b" * 64)
        assert not isinstance(exc, OSError)
        assert isinstance(exc, Exception)

    def test_pin_mismatch_without_a_peer_certificate(self):
        exc = identity.PinMismatch("a" * 64, None)
        assert exc.actual is None and "no certificate" in str(exc)

    def test_the_mismatching_socket_is_closed(self, server_id, client_a, monkeypatch):
        closed = []
        real_wrap = ssl.SSLContext.wrap_socket

        class Spy:
            def __init__(self, sock):
                self.sock = sock

            def __getattr__(self, name):
                return getattr(self.sock, name)

            def close(self):
                closed.append(True)
                self.sock.close()

        cctx = identity.client_context(client_a)

        class SpyCtx:
            def __getattr__(self, name):
                return getattr(cctx, name)

            def wrap_socket(self, raw, **kw):
                return Spy(cctx.wrap_socket(raw, **kw))

        with serving(server_ctx(server_id, client_a), mode="echo") as srv:
            with pytest.raises(identity.PinMismatch):
                identity.connect_pinned(LOOPBACK, srv.port, SpyCtx(), "0" * 64, IO_TIMEOUT)
        assert closed == [True]

    @pytest.mark.parametrize("bad", ["", "xyz", "a" * 63, None])
    def test_a_malformed_expected_fingerprint_fails_before_connecting(self, server_id, client_a, bad):
        with serving(server_ctx(server_id, client_a), mode="echo") as srv:
            with pytest.raises(ValueError):
                identity.connect_pinned(LOOPBACK, srv.port, identity.client_context(client_a), bad, 1)
            assert srv.accepted == 0

    def test_sni_is_sent_for_names_but_not_for_ip_literals(self, server_id, client_a):
        with serving(server_ctx(server_id, client_a), mode="echo") as srv:
            identity.connect_pinned("localhost", srv.port, identity.client_context(client_a),
                                    server_id.fingerprint, IO_TIMEOUT).close()
            by_name = list(srv.sni)
            srv.sni.clear()
            identity.connect_pinned(LOOPBACK, srv.port, identity.client_context(client_a),
                                    server_id.fingerprint, IO_TIMEOUT).close()
            by_ip = list(srv.sni)
        assert by_name == ["localhost"]
        assert all(name is None for name in by_ip)           # no server_name extension at all

    @IPV6
    @pytest.mark.parametrize("host", ["::1", "[::1]"])
    def test_ipv6_loopback_with_and_without_brackets(self, server_id, client_a, host):
        with serving(server_ctx(server_id, client_a), mode="echo", host="::1",
                     family=socket.AF_INET6) as srv:
            sock = identity.connect_pinned(host, srv.port, identity.client_context(client_a),
                                           server_id.fingerprint, IO_TIMEOUT)
            try:
                sock.sendall(b"v6")
                assert sock.recv(100) == b"echo:v6"
            finally:
                sock.close()
            assert all(name is None for name in srv.sni)

    @IPV6
    def test_ipv6_wrong_pin(self, server_id, client_a):
        with serving(server_ctx(server_id, client_a), mode="echo", host="::1",
                     family=socket.AF_INET6) as srv:
            with pytest.raises(identity.PinMismatch):
                identity.connect_pinned("[::1]", srv.port, identity.client_context(client_a),
                                        "1" * 64, IO_TIMEOUT)
            assert srv.wait_for(1)[0].received == b""

    def test_a_refused_connection_is_an_oserror_not_a_pin_mismatch(self):
        with socket.socket() as s:
            s.bind((LOOPBACK, 0))
            port = s.getsockname()[1]                       # closed again on exit: nobody listens
        with pytest.raises(OSError) as info:
            identity.connect_pinned(LOOPBACK, port, identity.client_context(), "a" * 64, 2)
        assert not isinstance(info.value, identity.PinMismatch)

    def test_a_server_that_never_handshakes_times_out(self):
        listener = socket.socket()
        listener.bind((LOOPBACK, 0))
        listener.listen(1)                                  # connects succeed; nobody answers TLS
        try:
            started = time.monotonic()
            with pytest.raises(OSError):                    # TimeoutError
                identity.connect_pinned(LOOPBACK, listener.getsockname()[1],
                                        identity.client_context(), "a" * 64, 0.4)
            assert time.monotonic() - started < 5
        finally:
            listener.close()


# ==========================================================================
# pinned_session
# ==========================================================================

requests = pytest.importorskip("requests")


@pytest.mark.integration
class TestPinnedSession:
    def url(self, srv, path="/"):
        return f"https://{LOOPBACK}:{srv.port}{path}"

    def get(self, session, url, **kw):
        try:
            return session.get(url, **kw)
        finally:
            session.close()

    def test_the_right_pin_works_and_presents_the_client_certificate(self, server_id, client_a):
        with serving(server_ctx(server_id, client_a)) as srv:
            r = self.get(identity.pinned_session(client_a, server_id.fingerprint, timeout=IO_TIMEOUT),
                         self.url(srv))
            conn = srv.wait_for(1)[0]
        assert r.status_code == 200 and r.text == "ok"
        assert identity.fingerprint_der(conn.peer_der) == client_a.fingerprint
        assert conn.received.startswith(b"GET / HTTP/1.1")

    def test_without_an_identity_no_client_certificate_is_sent(self, server_id, client_a):
        with serving(server_ctx(server_id, client_a)) as srv:
            r = self.get(identity.pinned_session(None, server_id.fingerprint, timeout=IO_TIMEOUT),
                         self.url(srv))
            conn = srv.wait_for(1)[0]
        assert r.status_code == 200 and conn.peer_der is None

    def test_any_spelling_of_the_fingerprint(self, server_id, client_a):
        colon = ":".join(server_id.fingerprint[i:i + 2] for i in range(0, 64, 2)).upper()
        with serving(server_ctx(server_id, client_a)) as srv:
            assert self.get(identity.pinned_session(client_a, colon, timeout=IO_TIMEOUT),
                            self.url(srv)).status_code == 200

    @pytest.mark.parametrize("bad", ["", "nope", "a" * 63, None])
    def test_a_malformed_fingerprint_is_a_value_error(self, client_a, bad):
        with pytest.raises(ValueError):
            identity.pinned_session(client_a, bad)

    def test_a_wrong_pin_fails_before_the_server_sees_a_request(self, server_id, client_a):
        wrong = hashlib.sha256(b"impostor").hexdigest()
        with serving(server_ctx(server_id, client_a)) as srv:
            with pytest.raises(requests.exceptions.SSLError) as info:
                self.get(identity.pinned_session(client_a, wrong, timeout=IO_TIMEOUT), self.url(srv))
            conn = srv.wait_for(1)[0]
        assert identity.is_pin_mismatch(info.value)
        assert not identity.is_tls_alert_rejection(info.value)
        assert conn.received == b""

    @pytest.mark.parametrize("verify", [True, False, "/nonexistent/ca.pem"], ids=["True", "False", "bogus-path"])
    def test_session_verify_cannot_re_enable_certificate_verification(self, server_id, client_a, verify):
        with serving(server_ctx(server_id, client_a)) as srv:
            session = identity.pinned_session(client_a, server_id.fingerprint, timeout=IO_TIMEOUT)
            session.verify = verify
            assert self.get(session, self.url(srv)).status_code == 200

    @pytest.mark.parametrize("verify", [True, "/nonexistent/ca.pem"], ids=["True", "bogus-path"])
    def test_per_request_verify_cannot_either(self, server_id, client_a, verify):
        with serving(server_ctx(server_id, client_a)) as srv:
            session = identity.pinned_session(client_a, server_id.fingerprint, timeout=IO_TIMEOUT)
            assert self.get(session, self.url(srv), verify=verify).status_code == 200

    def test_ca_bundle_environment_variables_are_ignored(self, server_id, client_a, monkeypatch):
        monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/nonexistent/bundle.pem")
        monkeypatch.setenv("CURL_CA_BUNDLE", "/nonexistent/bundle.pem")
        with serving(server_ctx(server_id, client_a)) as srv:
            session = identity.pinned_session(client_a, server_id.fingerprint, timeout=IO_TIMEOUT)
            assert self.get(session, self.url(srv)).status_code == 200

    def test_verify_true_would_otherwise_reject_the_self_signed_server(self, server_id, client_a):
        """Control for the two tests above: a stock adapter with the same context and
        fingerprint, but without the overrides, is turned back to CERT_REQUIRED."""
        from requests.adapters import HTTPAdapter
        ctx = identity.client_context(client_a)

        class Stock(HTTPAdapter):
            def init_poolmanager(self, connections, maxsize, block=False, **kw):
                kw.update(ssl_context=ctx, assert_fingerprint=server_id.fingerprint)
                super().init_poolmanager(connections, maxsize, block, **kw)

        with serving(server_ctx(server_id, client_a)) as srv:
            session = requests.Session()
            session.trust_env = False
            session.mount("https://", Stock())
            with pytest.raises(requests.exceptions.SSLError, match="certificate verify failed"):
                self.get(session, self.url(srv), timeout=IO_TIMEOUT)

    def test_session_cert_cannot_replace_the_identity(self, server_id, client_a, client_b):
        with serving(server_ctx(server_id, client_a, client_b)) as srv:
            session = identity.pinned_session(client_a, server_id.fingerprint, timeout=IO_TIMEOUT)
            session.cert = ("/nonexistent/other.crt", "/nonexistent/other.key")
            r = self.get(session, self.url(srv))
            conn = srv.wait_for(1)[0]
        assert r.status_code == 200
        assert identity.fingerprint_der(conn.peer_der) == client_a.fingerprint

    def test_per_request_cert_is_ignored_too(self, server_id, client_a):
        with serving(server_ctx(server_id, client_a)) as srv:
            session = identity.pinned_session(client_a, server_id.fingerprint, timeout=IO_TIMEOUT)
            r = self.get(session, self.url(srv), cert=("/nonexistent/c.crt", "/nonexistent/c.key"))
            assert r.status_code == 200
            assert identity.fingerprint_der(srv.wait_for(1)[0].peer_der) == client_a.fingerprint

    def test_plaintext_http_is_refused_without_connecting(self, server_id, client_a):
        with serving(server_ctx(server_id, client_a)) as srv:
            session = identity.pinned_session(client_a, server_id.fingerprint, timeout=IO_TIMEOUT)
            with pytest.raises(requests.exceptions.InvalidURL):
                self.get(session, f"http://{LOOPBACK}:{srv.port}/")
            assert srv.accepted == 0

    def test_a_redirect_to_plaintext_is_refused(self, server_id, client_a):
        def to_http(_request):
            return (b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/x\r\n"
                    b"Content-Length: 0\r\nConnection: close\r\n\r\n")

        with serving(server_ctx(server_id, client_a), responder=to_http) as srv:
            session = identity.pinned_session(client_a, server_id.fingerprint, timeout=IO_TIMEOUT)
            with pytest.raises(requests.exceptions.InvalidURL):
                self.get(session, self.url(srv), allow_redirects=True)
            assert srv.accepted == 1                    # the https request only

    def test_environment_proxies_are_not_used(self, server_id, client_a, monkeypatch):
        for var in ("HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"):
            monkeypatch.setenv(var, "http://127.0.0.1:1")           # a proxy nobody listens on
        monkeypatch.setenv("NO_PROXY", "")
        with serving(server_ctx(server_id, client_a)) as srv:
            session = identity.pinned_session(client_a, server_id.fingerprint, timeout=IO_TIMEOUT)
            assert session.trust_env is False
            assert self.get(session, self.url(srv)).status_code == 200

    def test_ipv6_literal_url(self, server_id, client_a):
        if not _ipv6_loopback_available():
            pytest.skip("no IPv6 loopback (::1)")
        with serving(server_ctx(server_id, client_a), host="::1", family=socket.AF_INET6) as srv:
            session = identity.pinned_session(client_a, server_id.fingerprint, timeout=IO_TIMEOUT)
            assert self.get(session, f"https://[::1]:{srv.port}/").status_code == 200

    # --- timeouts ---------------------------------------------------------------

    def test_the_default_timeout_applies_to_requests_that_pass_none(self, server_id, client_a):
        with serving(server_ctx(server_id, client_a), mode="silent") as srv:
            session = identity.pinned_session(client_a, server_id.fingerprint, timeout=0.4)
            started = time.monotonic()
            with pytest.raises(requests.exceptions.ReadTimeout):
                self.get(session, self.url(srv))
            assert time.monotonic() - started < 8

    def test_an_explicit_request_timeout_wins(self, server_id, client_a):
        with serving(server_ctx(server_id, client_a), mode="silent") as srv:
            session = identity.pinned_session(client_a, server_id.fingerprint, timeout=60)
            started = time.monotonic()
            with pytest.raises(requests.exceptions.ReadTimeout):
                self.get(session, self.url(srv), timeout=0.4)
            assert time.monotonic() - started < 8

    # --- structure (no sockets) -----------------------------------------------------

    def test_the_adapter_forces_verify_off_and_no_cert_and_supplies_the_default_timeout(self, monkeypatch, client_a):
        from requests.adapters import HTTPAdapter
        seen = []

        def fake_send(self, request, **kw):
            seen.append(kw)
            return requests.Response()

        monkeypatch.setattr(HTTPAdapter, "send", fake_send)
        session = identity.pinned_session(client_a, "ab" * 32, timeout=12.5)
        try:
            session.get("https://127.0.0.1:1/a")
            session.get("https://127.0.0.1:1/b", timeout=3, verify=True, cert=("c", "k"))
            session.verify, session.cert = "/some/bundle.pem", ("x", "y")
            session.get("https://127.0.0.1:1/c", verify=True)
        finally:
            session.close()
        assert [k["timeout"] for k in seen] == [12.5, 3, 12.5]
        assert [k["verify"] for k in seen] == [False, False, False]
        assert [k["cert"] for k in seen] == [None, None, None]

    def test_no_default_timeout_when_none_was_given(self, monkeypatch):
        from requests.adapters import HTTPAdapter
        seen = []
        monkeypatch.setattr(HTTPAdapter, "send",
                            lambda self, request, **kw: (seen.append(kw), requests.Response())[1])
        session = identity.pinned_session(None, "ab" * 32)
        session.get("https://127.0.0.1:1/")
        session.close()
        assert seen[0]["timeout"] is None

    def test_pool_key_attributes_never_carry_a_ca_or_a_client_cert(self):
        session = identity.pinned_session(None, "ab" * 32)
        adapter = session.get_adapter("https://127.0.0.1:1/")
        prepared = requests.Request("GET", "https://127.0.0.1:9/").prepare()
        host_params, pool_kwargs = adapter.build_connection_pool_key_attributes(
            prepared, True, ("client.crt", "client.key"))
        assert host_params == {"scheme": "https", "host": "127.0.0.1", "port": 9}
        assert pool_kwargs == {"cert_reqs": "CERT_NONE"}
        session.close()

    def test_cert_verify_cannot_switch_verification_or_a_client_cert_back_on(self):
        """`HTTPAdapter.cert_verify` is the hook older requests/urllib3 combinations apply
        `verify=`/`cert=` through; with S7's recipe it must see `False`/`None` whatever
        the caller passes."""
        session = identity.pinned_session(None, "ab" * 32)
        adapter = session.get_adapter("https://example.invalid/")
        conn = types.SimpleNamespace()
        adapter.cert_verify(conn, "https://example.invalid/", True, ("/nonexistent/c.crt", "/nonexistent/c.key"))
        assert conn.cert_reqs == "CERT_NONE"
        assert conn.ca_certs is None and conn.ca_cert_dir is None
        assert getattr(conn, "cert_file", None) is None
        conn2 = types.SimpleNamespace()
        adapter.cert_verify(conn2, "https://example.invalid/", "/nonexistent/ca.pem", None)
        assert conn2.cert_reqs == "CERT_NONE" and conn2.ca_certs is None
        session.close()

    def test_the_pin_is_part_of_the_pool_manager_and_survives_a_proxy_manager(self, client_a):
        fp = "ab" * 32
        session = identity.pinned_session(client_a, ":".join(fp[i:i + 2] for i in range(0, 64, 2)).upper())
        adapter = session.get_adapter("https://example.invalid/")
        assert adapter.poolmanager.connection_pool_kw["assert_fingerprint"] == fp
        ctx = adapter.poolmanager.connection_pool_kw["ssl_context"]
        assert ctx.verify_mode == ssl.CERT_NONE and ctx.check_hostname is False
        proxied = adapter.proxy_manager_for("http://127.0.0.1:3128")
        assert proxied.connection_pool_kw["assert_fingerprint"] == fp
        assert proxied.connection_pool_kw["ssl_context"] is ctx
        session.close()

    def test_mounts_and_trust_env(self):
        session = identity.pinned_session(None, "ab" * 32)
        assert session.trust_env is False
        https_adapter = session.get_adapter("https://example.invalid/")
        plain_adapter = session.get_adapter("http://example.invalid/")
        assert https_adapter is not plain_adapter
        with pytest.raises(requests.exceptions.InvalidURL, match="plaintext"):
            plain_adapter.send(requests.Request("GET", "http://example.invalid/").prepare())
        plain_adapter.close()
        session.close()


# ==========================================================================
# Error classification against a server that really rejects the certificate
# ==========================================================================

def rejection_case(kind, client_a, client_b, crafted):
    """-> (identity to present, identities the server pins) for a certificate the
    server must refuse."""
    return {
        "unpinned": (client_b, [client_a]),
        "expired": (crafted.expired, [crafted.expired]),
        "eku": (crafted.eku, [crafted.eku]),
    }[kind]


@pytest.mark.integration
class TestErrorClassification:
    @pytest.mark.parametrize("kind", ["unpinned", "expired", "eku"])
    def test_raw_socket_rejection_arrives_on_first_io_and_is_an_alert(
            self, server_id, client_a, client_b, crafted, kind):
        ident, pinned = rejection_case(kind, client_a, client_b, crafted)
        with serving(server_ctx(server_id, *pinned), mode="echo") as srv:
            sock = identity.connect_pinned(LOOPBACK, srv.port, identity.client_context(ident),
                                           server_id.fingerprint, IO_TIMEOUT)      # no error yet (TLS 1.3)
            try:
                # Let the server finish rejecting us first, so that its alert is already
                # queued: writing first races the alert against the RST that the server's
                # close produces for unread data (platform behaviour, see the next tests).
                conn = srv.wait_for(1)[0]
                with pytest.raises(ssl.SSLError) as info:
                    sock.recv(1)
            finally:
                sock.close()
        assert identity.is_tls_alert_rejection(info.value)
        assert not identity.is_pin_mismatch(info.value)
        assert not conn.ok and conn.received == b""

    @pytest.mark.parametrize("kind", ["unpinned", "expired", "eku"])
    def test_requests_rejection_is_an_alert_not_a_pin_mismatch(
            self, server_id, client_a, client_b, crafted, kind):
        ident, pinned = rejection_case(kind, client_a, client_b, crafted)
        resets = 0
        error = None
        with serving(server_ctx(server_id, *pinned)) as srv:
            # requests writes its request the moment the handshake completes. Rarely
            # (about 1 in 30 here) the server's close then turns into an RST that eats
            # the alert, and the client sees ConnectionError(BrokenPipe/reset) instead of
            # an SSLError -- a platform race, not something this module can classify
            # (see test_a_reset_instead_of_the_alert_is_not_classified). Retry until the
            # alert wins.
            for _ in range(10):
                session = identity.pinned_session(ident, server_id.fingerprint, timeout=IO_TIMEOUT)
                try:
                    session.get(f"https://{LOOPBACK}:{srv.port}/")
                    pytest.fail("the server accepted a certificate it must reject")
                except requests.exceptions.SSLError as exc:
                    error = exc
                    break
                except requests.exceptions.ConnectionError:
                    resets += 1
                finally:
                    session.close()
            conns = srv.wait_for(resets + (1 if error else 0))
        assert error is not None, "the alert never won in 10 attempts"
        assert identity.is_tls_alert_rejection(error)
        assert not identity.is_pin_mismatch(error)
        assert all(not c.ok and c.received == b"" for c in conns)

    def test_a_pinned_client_is_not_a_rejection(self, server_id, client_a):
        with serving(server_ctx(server_id, client_a), mode="echo") as srv:
            sock = identity.connect_pinned(LOOPBACK, srv.port, identity.client_context(client_a),
                                           server_id.fingerprint, IO_TIMEOUT)
            sock.sendall(b"ok")
            assert sock.recv(100) == b"echo:ok"
            sock.close()

    def test_a_reset_instead_of_the_alert_is_not_classified(self):
        """Known limit (reported for M11): when the TLS 1.3 alert is lost to a reset, the
        client sees a plain connection error that cannot be told from a network blip."""
        for exc in (requests.exceptions.ConnectionError(BrokenPipeError(32, "Broken pipe")),
                    requests.exceptions.ConnectionError(ConnectionResetError(54, "reset"))):
            assert not identity.is_tls_alert_rejection(exc)
            assert not identity.is_pin_mismatch(exc)

    def test_a_wrong_server_pin_is_a_pin_mismatch_and_not_an_alert(self, server_id, client_a):
        with serving(server_ctx(server_id, client_a), mode="echo") as srv:
            with pytest.raises(identity.PinMismatch) as info:
                identity.connect_pinned(LOOPBACK, srv.port, identity.client_context(client_a),
                                        "2" * 64, IO_TIMEOUT)
        assert identity.is_pin_mismatch(info.value)
        assert not identity.is_tls_alert_rejection(info.value)

    # --- synthetic exceptions: wrapping, prefixes, negatives --------------------------

    @staticmethod
    def alert(reason):
        exc = ssl.SSLError(1, f"[SSL: {reason}] alert")
        exc.reason = reason
        return exc

    @pytest.mark.parametrize("prefix", ["TLS", "TLSV1", "SSLV3"])
    @pytest.mark.parametrize("alert", ["UNKNOWN_CA", "CERTIFICATE_EXPIRED",
                                       "UNSUPPORTED_CERTIFICATE", "BAD_CERTIFICATE"])
    def test_every_openssl_spelling_of_the_alerts_is_recognised(self, prefix, alert):
        assert identity.is_tls_alert_rejection(self.alert(f"{prefix}_ALERT_{alert}"))

    @pytest.mark.parametrize("reason", [
        "TLS_ALERT_HANDSHAKE_FAILURE", "TLSV1_ALERT_PROTOCOL_VERSION", "SSLV3_ALERT_CERTIFICATE_REVOKED",
        "TLS_ALERT_INTERNAL_ERROR", "WRONG_VERSION_NUMBER", "", None, "XTLS_ALERT_UNKNOWN_CA",
        "TLS_ALERT_UNKNOWN_CA_EXTRA",
    ], ids=repr)
    def test_other_ssl_errors_are_not_rejections(self, reason):
        exc = ssl.SSLError(1, "x")
        exc.reason = reason
        assert not identity.is_tls_alert_rejection(exc)

    def test_a_local_certificate_verification_failure_is_not_a_rejection(self):
        exc = ssl.SSLCertVerificationError(1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed")
        exc.reason = "TLS_ALERT_UNKNOWN_CA"          # even if it carried the reason
        assert not identity.is_tls_alert_rejection(exc)

    @pytest.mark.parametrize("exc", [
        ConnectionResetError(), BrokenPipeError(), TimeoutError(), OSError(1, "x"), ValueError("x"),
        KeyError("k"), identity.PinMismatch("a" * 64, "b" * 64),
    ], ids=lambda e: type(e).__name__)
    def test_unrelated_exceptions_are_not_rejections(self, exc):
        assert not identity.is_tls_alert_rejection(exc)

    def test_a_rejection_wrapped_by_cause_context_or_arg_is_found(self):
        inner = self.alert("TLS_ALERT_UNKNOWN_CA")
        try:
            try:
                raise inner
            except ssl.SSLError as e:
                raise RuntimeError("wrapped") from e
        except RuntimeError as caused:
            assert identity.is_tls_alert_rejection(caused)
        try:
            try:
                raise inner
            except ssl.SSLError:
                raise RuntimeError("implicit context")
        except RuntimeError as ctx_exc:
            assert identity.is_tls_alert_rejection(ctx_exc)
        assert identity.is_tls_alert_rejection(OSError(inner))
        assert identity.is_tls_alert_rejection(
            requests.exceptions.SSLError(inner))

    def test_a_requests_error_that_only_carries_the_alert_text(self):
        assert identity.is_tls_alert_rejection(requests.exceptions.SSLError(
            "HTTPSConnectionPool: Max retries exceeded (Caused by SSLError(SSLError(1, "
            "'[SSL] tlsv1 alert unknown ca (_ssl.c:1)')) TLSV1_ALERT_UNKNOWN_CA"))
        assert not identity.is_tls_alert_rejection(requests.exceptions.SSLError("certificate verify failed"))

    def test_pin_mismatch_is_recognised_wrapped_and_through_urllib3_text(self):
        pm = identity.PinMismatch("a" * 64, "b" * 64)
        assert identity.is_pin_mismatch(pm)
        try:
            try:
                raise pm
            except identity.PinMismatch as e:
                raise RuntimeError("outer") from e
        except RuntimeError as outer:
            assert identity.is_pin_mismatch(outer)
        urllib3_text = requests.exceptions.SSLError(
            'Fingerprints did not match. Expected "aa", got "bb"')
        assert identity.is_pin_mismatch(urllib3_text)
        import urllib3
        assert identity.is_pin_mismatch(requests.exceptions.SSLError(
            urllib3.exceptions.MaxRetryError(None, "/", urllib3.exceptions.SSLError(
                'Fingerprints did not match. Expected "aa", got "bb"'))))

    @pytest.mark.parametrize("exc", [
        ValueError("x"), OSError("x"), requests.exceptions.SSLError("certificate verify failed"),
        ConnectionResetError(), TimeoutError(),
    ], ids=lambda e: type(e).__name__)
    def test_unrelated_exceptions_are_not_pin_mismatches(self, exc):
        assert not identity.is_pin_mismatch(exc)

    def test_cyclic_exception_chains_terminate(self):
        a, b = RuntimeError("a"), RuntimeError("b")
        a.__cause__, b.__cause__ = b, a
        assert not identity.is_pin_mismatch(a)
        assert not identity.is_tls_alert_rejection(a)
        b.__cause__ = identity.PinMismatch("a" * 64, None)
        assert identity.is_pin_mismatch(a)


# ==========================================================================
# Special files (FIFO, directory, socket) are refused, never waited on
# ==========================================================================
#
# `_read_file` opens `O_NONBLOCK` and `fstat`s the descriptor it opened, so a
# FIFO at a key or certificate path (planted, or a mistyped operator path) is
# refused at once. Without that, `open(O_RDONLY)` blocks until a writer
# appears -- and the daemon supervisor reads these files. Every call here runs
# on a thread with a short join so a regression fails the test instead of
# hanging the suite.

FIFO_GUARD = 5.0
needs_mkfifo = pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="os.mkfifo is unavailable")


def _release_fifo(path: Path) -> None:
    """Unblock a thread stuck in ``open(path, O_RDONLY)``: ``O_RDWR`` on a FIFO
    never blocks, and counts as a writer for the pending reader."""
    try:
        os.close(os.open(str(path), os.O_RDWR | os.O_NONBLOCK))
    except OSError:
        pass


def guarded(fn, *, fifo: Optional[Path] = None, timeout: float = FIFO_GUARD):
    """Run ``fn()`` on a daemon thread and return its result (or re-raise its
    exception). If it has not finished within ``timeout`` the test fails -- after
    releasing the blocked thread -- instead of hanging."""
    out: dict = {}
    t = run_in_thread(out, "call", fn)
    t.join(timeout)
    if t.is_alive():
        if fifo is not None:
            _release_fifo(fifo)
        t.join(IO_TIMEOUT)
        pytest.fail(f"call blocked for more than {timeout}s on a special file (open() waiting for a writer?)")
    result = out["call"]
    if isinstance(result, BaseException):
        raise result
    return result


def refused(fn, *, fifo: Optional[Path] = None, match: str = "not a regular file"):
    """``fn()`` must raise ``IdentityError`` (matching ``match``) without blocking."""
    with pytest.raises(identity.IdentityError, match=match) as info:
        guarded(fn, fifo=fifo)
    return info.value


def make_fifo(path: Path) -> Path:
    os.mkfifo(str(path), 0o600)
    return path


def fifo_listing(d: Path) -> List[str]:
    return sorted(p.name for p in d.iterdir() if stat.S_ISFIFO(p.lstat().st_mode))


@pytest.mark.unit
@needs_mkfifo
class TestReadFileSpecialFiles:
    def test_a_fifo_is_refused_without_waiting_for_a_writer(self, tmp_path):
        fifo = make_fifo(tmp_path / "pipe")
        err = refused(lambda: identity._read_file(fifo, "certificate"), fifo=fifo, match="named pipe")
        text = str(err)
        assert "not a regular file" in text and "named pipe" in text
        assert str(fifo) in text and "certificate" in text
        assert "point it at the PEM file itself" in text
        assert err.path == fifo

    def test_the_what_label_is_used(self, tmp_path):
        fifo = make_fifo(tmp_path / "pipe")
        refused(lambda: identity._read_file(fifo, "private key"), fifo=fifo,
                match=r"private key .* is not a regular file \(it is a named pipe\)")

    def test_a_fifo_with_a_writer_attached_is_still_refused(self, tmp_path):
        """The one case that needs the write end: a reader opened while a writer
        is attached succeeds even without ``O_NONBLOCK`` -- and, with PEM-looking
        data queued, would then read it and block waiting for EOF."""
        fifo = make_fifo(tmp_path / "pipe")
        keeper = os.open(str(fifo), os.O_RDONLY | os.O_NONBLOCK)      # keeps the queued data alive
        writer = os.open(str(fifo), os.O_WRONLY | os.O_NONBLOCK)
        try:
            os.write(writer, pem_of(make_cert(key_of("ec256"))).encode())
            err = refused(lambda: identity._read_file(fifo, "certificate"), fifo=fifo, match="named pipe")
            assert "not a regular file" in str(err)
            # nothing was consumed from the pipe
            assert os.read(keeper, 100).startswith(b"-----BEGIN CERTIFICATE-----")
        finally:
            os.close(writer)
            os.close(keeper)

    def test_a_directory_is_refused(self, tmp_path):
        d = tmp_path / "adir"
        d.mkdir()
        err = refused(lambda: identity._read_file(d, "certificate"), match="directory")
        assert "not a regular file" in str(err)

    def test_a_unix_socket_is_refused(self, tmp_path, monkeypatch):
        # AF_UNIX paths are short (104 bytes on macOS): bind a relative name.
        monkeypatch.chdir(tmp_path)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.bind("s")
            path = tmp_path / "s"
            assert stat.S_ISSOCK(path.lstat().st_mode)
            # Depending on the platform, open() itself fails (ENXIO / EOPNOTSUPP) or
            # the fstat check names the socket; either way it is a refusal.
            refused(lambda: identity._read_file(path, "certificate"),
                    match="not a regular file|cannot open certificate")
        finally:
            sock.close()

    def test_a_regular_file_is_read_and_left_blocking(self, tmp_path):
        """The ``O_NONBLOCK`` used for the open must not leak into the read."""
        p = tmp_path / "f.pem"
        p.write_bytes(b"x" * 100_000)                 # more than one 64 KiB chunk
        data, st = guarded(lambda: identity._read_file(p, "certificate"))
        assert data == b"x" * 100_000
        assert stat.S_ISREG(st.st_mode) and st.st_size == 100_000

    def test_a_symlink_to_a_regular_file_is_followed(self, tmp_path):
        """Pins the policy: the check is on the file actually opened, and
        operator-supplied paths may be symlinks."""
        real = tmp_path / "real.pem"
        real.write_bytes(b"payload")
        link = tmp_path / "link.pem"
        link.symlink_to(real)
        data, st = guarded(lambda: identity._read_file(link, "certificate"))
        assert data == b"payload" and stat.S_ISREG(st.st_mode)

    def test_a_symlink_to_a_fifo_is_refused(self, tmp_path):
        fifo = make_fifo(tmp_path / "pipe")
        link = tmp_path / "link"
        link.symlink_to(fifo)
        refused(lambda: identity._read_file(link, "certificate"), fifo=fifo, match="named pipe")

    def test_a_missing_file_is_still_reported_as_missing(self, tmp_path):
        refused(lambda: identity._read_file(tmp_path / "nope", "certificate"), match="does not exist")


@pytest.mark.unit
@needs_mkfifo
class TestFileKindNames:
    @pytest.mark.parametrize("mode,name", [
        (stat.S_IFIFO, "named pipe"), (stat.S_IFDIR, "directory"), (stat.S_IFSOCK, "socket"),
        (stat.S_IFCHR, "character device"), (stat.S_IFBLK, "block device"),
        (stat.S_IFREG, "special file"),
    ])
    def test_names(self, mode, name):
        assert identity._file_kind(mode | 0o600) == name


@pytest.mark.unit
@needs_mkfifo
class TestFifoAtIdentityPaths:
    """Each reader of a certificate or key path, with a FIFO at that path."""

    def test_peek_identity_raises_it_does_not_return_none(self, tmp_path):
        fifo = make_fifo(tmp_path / "client.crt")
        # "no certificate" is None; a FIFO is something there that cannot be read.
        err = refused(lambda: identity.peek_identity(tmp_path, "client"), fifo=fifo, match="named pipe")
        assert "not a regular file" in str(err)

    @pytest.mark.parametrize("layout", ["generated", "free_form"])
    def test_load_identity_with_a_fifo_as_the_certificate(self, tmp_path, layout):
        cert_path, key_path, _ = loadable(tmp_path)
        if layout == "free_form":
            cert_path = cert_path.rename(tmp_path / "operator-chosen.pem")
            key_path = key_path.rename(tmp_path / "operator-chosen-key.pem")
        cert_path.unlink()
        make_fifo(cert_path)
        err = refused(lambda: identity.load_identity(cert_path, key_path, "client", **NOW),
                      fifo=cert_path, match="certificate .* is not a regular file")
        assert "named pipe" in str(err)

    @pytest.mark.parametrize("layout", ["generated", "free_form"])
    def test_load_identity_with_a_fifo_as_the_key(self, tmp_path, layout):
        cert_path, key_path, _ = loadable(tmp_path)
        if layout == "free_form":
            cert_path = cert_path.rename(tmp_path / "operator-chosen.pem")
            key_path = key_path.rename(tmp_path / "operator-chosen-key.pem")
        key_path.unlink()
        make_fifo(key_path)
        err = refused(lambda: identity.load_identity(cert_path, key_path, "client", **NOW),
                      fifo=key_path, match="private key .* is not a regular file")
        assert "named pipe" in str(err)

    def test_load_identity_with_the_fifo_as_both(self, tmp_path):
        cert_path, key_path = identity.identity_paths(tmp_path, "client")
        make_fifo(cert_path)
        make_fifo(key_path)
        refused(lambda: identity.load_identity(cert_path, key_path, "client", **NOW),
                fifo=key_path, match="named pipe")

    def test_load_identity_with_a_fifo_that_has_a_writer(self, tmp_path):
        cert_path, key_path, _ = loadable(tmp_path)
        key_path.unlink()
        make_fifo(key_path)
        keeper = os.open(str(key_path), os.O_RDONLY | os.O_NONBLOCK)
        writer = os.open(str(key_path), os.O_WRONLY | os.O_NONBLOCK)
        try:
            os.write(writer, key_pem(key_of("ec256")))
            refused(lambda: identity.load_identity(cert_path, key_path, "client", **NOW),
                    fifo=key_path, match="named pipe")
        finally:
            os.close(writer)
            os.close(keeper)

    @pytest.mark.parametrize("half", ["cert", "key"])
    def test_load_identity_still_accepts_a_symlink_to_a_regular_file(self, tmp_path, half):
        cert_path, key_path, cert = loadable(tmp_path)
        target = cert_path if half == "cert" else key_path
        moved = target.rename(tmp_path / f"{target.name}.real")
        target.symlink_to(moved)
        ident = guarded(lambda: identity.load_identity(cert_path, key_path, "client", **NOW))
        assert ident.fingerprint == fp_of(cert)

    @pytest.mark.parametrize("which", ["certificate", "private key"])
    def test_load_identity_refuses_a_directory(self, tmp_path, which):
        cert_path, key_path, _ = loadable(tmp_path)
        victim = cert_path if which == "certificate" else key_path
        victim.unlink()
        victim.mkdir()
        refused(lambda: identity.load_identity(cert_path, key_path, "client", **NOW),
                match=f"{which} .* is not a regular file \\(it is a directory\\)")

    @pytest.mark.parametrize("which", ["certificate", "private key"])
    def test_load_identity_refuses_a_unix_socket(self, tmp_path, monkeypatch, which):
        cert_path, key_path, _ = loadable(tmp_path)
        victim = cert_path if which == "certificate" else key_path
        victim.unlink()
        monkeypatch.chdir(tmp_path)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.bind(victim.name)
            assert stat.S_ISSOCK(victim.lstat().st_mode)
            refused(lambda: identity.load_identity(cert_path, key_path, "client", **NOW),
                    match=f"{which} .* (is not a regular file|cannot open)|cannot open {which}")
        finally:
            sock.close()


@pytest.mark.unit
@needs_mkfifo
class TestEnsureIdentityFifoHalf:
    """`ensure_identity` / `_inspect` on a pair where one half is a FIFO: refused,
    and nothing is regenerated over it (replacing the pair would change a
    fingerprint that peers have pinned -- and a FIFO is not crash debris)."""

    @staticmethod
    def _pair_with_fifo(tmp_path, half):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        cert_path, key_path = _client_files(d)
        victim = cert_path if half == "cert" else key_path
        victim.unlink()
        make_fifo(victim)
        return d, victim

    @pytest.mark.parametrize("half", ["cert", "key"])
    def test_ensure_identity_refuses_and_changes_nothing(self, tmp_path, half):
        d, victim = self._pair_with_fifo(tmp_path, half)
        before = snapshot(d)
        err = refused(lambda: identity.ensure_identity(d, "client"), fifo=victim, match="left alone")
        text = str(err)
        assert "not a regular file" in text and "named pipe" in text
        assert snapshot(d) == before
        assert fifo_listing(d) == [victim.name]
        assert err.path == victim

    @pytest.mark.parametrize("half", ["cert", "key"])
    def test_inspect_refuses(self, tmp_path, half):
        d, victim = self._pair_with_fifo(tmp_path, half)
        err = refused(lambda: identity._inspect(d, "client", "client"), fifo=victim, match="left alone")
        assert "not a regular file" in str(err) and "named pipe" in str(err)

    @pytest.mark.parametrize("half", ["cert", "key"])
    def test_a_second_attempt_is_refused_the_same_way(self, tmp_path, half):
        d, victim = self._pair_with_fifo(tmp_path, half)
        for _ in range(2):
            refused(lambda: identity.ensure_identity(d, "client"), fifo=victim, match="named pipe")
        assert fifo_listing(d) == [victim.name]

    @pytest.mark.parametrize("half", ["cert", "key"])
    def test_a_fifo_with_a_writer_is_refused_and_untouched(self, tmp_path, half):
        d, victim = self._pair_with_fifo(tmp_path, half)
        keeper = os.open(str(victim), os.O_RDONLY | os.O_NONBLOCK)
        writer = os.open(str(victim), os.O_WRONLY | os.O_NONBLOCK)
        try:
            os.write(writer, b"-----BEGIN PRIVATE KEY-----\n")
            before = snapshot(d)
            refused(lambda: identity.ensure_identity(d, "client"), fifo=victim, match="named pipe")
            assert snapshot(d) == before
        finally:
            os.close(writer)
            os.close(keeper)

    def test_a_directory_in_place_of_the_certificate_is_not_replaced(self, tmp_path):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        cert_path, _ = _client_files(d)
        cert_path.unlink()
        cert_path.mkdir()
        before = snapshot(d)
        refused(lambda: identity.ensure_identity(d, "client"), match="directory")
        assert snapshot(d) == before

    def test_a_symlink_to_a_regular_certificate_is_accepted(self, tmp_path):
        d = make_tlsdir(tmp_path)
        ident = identity.ensure_identity(d, "client")
        cert_path, _ = _client_files(d)
        real = cert_path.rename(d / "client.crt.real")
        cert_path.symlink_to(real)
        again = guarded(lambda: identity.ensure_identity(d, "client"))
        assert again.fingerprint == ident.fingerprint
        assert cert_path.is_symlink() and real.exists()


@pytest.mark.unit
@needs_mkfifo
class TestFifoInHalfPromotionAndContexts:
    @staticmethod
    def _moved_key_state(d: Path):
        """client.crt certifies a key that now sits at client.next.key; client.key is gone."""
        key = key_of("ec256")
        cert_path, key_path = write_pair(d, "client", key, make_cert(key, eku=(CLIENT_AUTH,)))
        key_path.rename(d / "client.next.key")

    def test_the_state_without_a_fifo_is_a_half_promotion(self, tmp_path):
        """Control for the cases below."""
        self._moved_key_state(tmp_path)
        assert identity._find_half_promotion(tmp_path, "client") == ("client", "client.next")

    def test_a_fifo_as_the_moved_key_is_not_a_half_promotion(self, tmp_path):
        self._moved_key_state(tmp_path)
        nxt = tmp_path / "client.next.key"
        nxt.unlink()
        make_fifo(nxt)
        assert guarded(lambda: identity._find_half_promotion(tmp_path, "client"), fifo=nxt) is None
        assert guarded(lambda: identity._half_promoted(tmp_path, "client", "client.next"), fifo=nxt) is False

    def test_a_fifo_as_the_source_certificate_is_not_a_half_promotion(self, tmp_path):
        self._moved_key_state(tmp_path)
        crt = tmp_path / "client.crt"
        crt.unlink()
        make_fifo(crt)
        assert guarded(lambda: identity._find_half_promotion(tmp_path, "client"), fifo=crt) is None

    def test_a_fifo_as_the_target_certificate_does_not_block(self, tmp_path):
        """An unreadable target certificate counts as 'stopped between the renames'
        (`_half_promoted` documents that); the point here is that it answers."""
        self._moved_key_state(tmp_path)
        tgt = make_fifo(tmp_path / "client.next.crt")
        assert guarded(lambda: identity._half_promoted(tmp_path, "client", "client.next"), fifo=tgt) is True

    @pytest.mark.parametrize("half", ["cert", "key"])
    def test_a_context_is_never_built_from_a_swapped_in_fifo(self, tmp_path, half):
        """`_load_chain` re-reads both files after `load_identity` validated them."""
        cert_path, key_path, _ = loadable(tmp_path)
        ident = identity.load_identity(cert_path, key_path, "client", **NOW)
        victim = cert_path if half == "cert" else key_path
        victim.unlink()
        make_fifo(victim)
        err = refused(lambda: identity.client_context(ident), fifo=victim, match="named pipe")
        assert "not a regular file" in str(err)
        refused(lambda: identity.server_context(ident, []), fifo=victim, match="named pipe")


# ==========================================================================
# IdentityChanged, lock_timeout, the identity lock's own file kind, and the
# "no lock for operator paths" rule of `_load_chain`
# ==========================================================================

@contextmanager
def lock_held(directory: Path, op: int = fcntl.LOCK_EX):
    """Hold `<directory>/identity.lock` (flock) from a separate open file description, so
    it conflicts with whatever the code under test opens, in this process too."""
    path = Path(directory) / identity.IDENTITY_LOCK
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o640)
    try:
        fcntl.flock(fd, op)
        yield path
    finally:
        os.close(fd)


exact = type          # the exact class: `isinstance` would accept the IdentityChanged subclass


@pytest.mark.integration
class TestIdentityChanged:
    """`IdentityChanged` is raised by `server_context` / `client_context` / `pinned_session`
    for exactly two findings (another certificate, a key that no longer matches); everything
    else they raise is a plain `IdentityError`."""

    def test_it_is_an_identity_error_carrying_expected_and_actual(self):
        exc = identity.IdentityChanged("m", Path("/x"), expected="a" * 64, actual=None)
        assert isinstance(exc, identity.IdentityError)
        assert (exc.expected, exc.actual, exc.path) == ("a" * 64, None, Path("/x"))
        assert str(exc) == "m"

    def test_expected_and_actual_are_keyword_only(self):
        with pytest.raises(TypeError):
            identity.IdentityChanged("m", Path("/x"), "a" * 64, None)

    def test_it_is_exported(self):
        assert "IdentityChanged" in identity.__all__

    @pytest.mark.parametrize("role", ["server", "client"])
    @pytest.mark.parametrize("swap", ["cert_and_key", "cert_only"])
    def test_another_certificate_reports_both_fingerprints(self, tmp_path, role, swap):
        me, other = forge(tmp_path, role)
        SWAPS[swap](me, other)
        with pytest.raises(identity.IdentityChanged) as info:
            make_context(role, me)
        assert info.value.expected == me.fingerprint
        assert info.value.actual == other.fingerprint
        assert info.value.path == me.cert_path
        assert "changed since the identity was loaded" in str(info.value)

    @pytest.mark.parametrize("role", ["server", "client"])
    def test_a_key_that_no_longer_matches_has_no_actual_fingerprint(self, tmp_path, role):
        me, other = forge(tmp_path, role)
        SWAPS["key_only"](me, other)
        with pytest.raises(identity.IdentityChanged) as info:
            make_context(role, me)
        assert info.value.expected == me.fingerprint and info.value.actual is None
        assert info.value.path == me.key_path
        assert "no longer matches" in str(info.value)

    @pytest.mark.parametrize("role", ["server", "client"])
    @pytest.mark.parametrize("swap", ["key_made_world_readable", "cert_deleted", "cert_corrupted"])
    def test_an_unreadable_or_unsafe_file_is_a_plain_identity_error(self, tmp_path, role, swap):
        me, other = forge(tmp_path, role)
        SWAPS[swap](me, other)
        with pytest.raises(identity.IdentityError) as info:
            make_context(role, me)
        assert exact(info.value) is identity.IdentityError

    @pytest.mark.parametrize("role", ["server", "client"])
    def test_an_unparseable_key_is_a_plain_identity_error(self, tmp_path, role):
        me, _ = forge(tmp_path, role)
        me.key_path.write_text("this is not a key\n")
        me.key_path.chmod(0o600)
        with pytest.raises(identity.IdentityError) as info:
            make_context(role, me)
        assert exact(info.value) is identity.IdentityError

    @pytest.mark.parametrize("role", ["server", "client"])
    def test_a_lock_timeout_is_a_plain_identity_error(self, tmp_path, role):
        me, _ = forge(tmp_path, role)
        with lock_held(me.cert_path.parent):
            with pytest.raises(identity.IdentityError, match="timed out") as info:
                (identity.server_context(me, [], lock_timeout=0.2) if role == "server"
                 else identity.client_context(me, lock_timeout=0.2))
        assert exact(info.value) is identity.IdentityError

    @pytest.mark.parametrize("role", ["server", "client"])
    def test_ssl_refusing_the_pair_is_a_plain_identity_error(self, tmp_path, role, monkeypatch):
        me, _ = forge(tmp_path, role)

        def refuse(self, *args, **kwargs):
            raise ssl.SSLError("injected: key values mismatch")

        monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", refuse)
        with pytest.raises(identity.IdentityError, match="cannot load TLS identity") as info:
            make_context(role, me)
        assert exact(info.value) is identity.IdentityError

    @pytest.mark.parametrize("role", ["server", "client"])
    def test_an_unchanged_identity_raises_nothing(self, tmp_path, role):
        me, _ = forge(tmp_path, role)
        assert make_context(role, me) is not None

    def test_pinned_session_passes_it_through(self, tmp_path):
        me, other = forge(tmp_path, "client")
        SWAPS["cert_only"](me, other)
        with pytest.raises(identity.IdentityChanged) as info:
            identity.pinned_session(me, "ab" * 32)
        assert info.value.actual == other.fingerprint

    def test_pinned_session_keeps_a_lock_timeout_plain(self, tmp_path):
        me, _ = forge(tmp_path, "client")
        with lock_held(me.cert_path.parent):
            with pytest.raises(identity.IdentityError, match="timed out") as info:
                identity.pinned_session(me, "ab" * 32, lock_timeout=0.2)
        assert exact(info.value) is identity.IdentityError

    def test_the_load_helpers_never_raise_it(self, tmp_path):
        """`IdentityChanged` is the contexts' finding about a LOADED identity; loading itself
        reports plain errors."""
        me, other = forge(tmp_path, "client")
        SWAPS["cert_only"](me, other)
        with pytest.raises(identity.IdentityError) as info:
            identity.load_identity(me.cert_path, me.key_path, "client")
        assert not isinstance(info.value, identity.IdentityChanged)


@pytest.mark.integration
class TestContextLockTimeout:
    @pytest.mark.parametrize("fn", [identity.server_context, identity.client_context,
                                    identity.pinned_session])
    def test_lock_timeout_is_keyword_only_and_defaults_to_the_module_default(self, fn):
        import inspect

        param = inspect.signature(fn).parameters["lock_timeout"]
        assert param.kind is inspect.Parameter.KEYWORD_ONLY
        assert param.default == identity._DEFAULT_LOCK_TIMEOUT

    @pytest.mark.parametrize("role", ["server", "client"])
    def test_a_positional_timeout_is_refused(self, tmp_path, role):
        me, _ = forge(tmp_path, role)
        with pytest.raises(TypeError):
            if role == "server":
                identity.server_context(me, [], 0.1)
            else:
                identity.client_context(me, 0.1)

    @pytest.mark.parametrize("role", ["server", "client"])
    @pytest.mark.parametrize("timeout, shown", [(0.3, "0.3s"), (0.5, "0.5s"), (1, "1s"), (1.5, "1.5s")])
    def test_a_writer_holding_the_lock_times_the_context_out_in_the_stated_time(
            self, tmp_path, role, timeout, shown):
        """The 30 s default would make this take half a minute: it proves the argument is
        what bounds the wait. The message prints it with `{timeout:g}s`."""
        me, _ = forge(tmp_path, role)
        with lock_held(me.cert_path.parent):
            started = time.monotonic()
            with pytest.raises(identity.IdentityError) as info:
                (identity.server_context(me, [], lock_timeout=timeout) if role == "server"
                 else identity.client_context(me, lock_timeout=timeout))
            elapsed = time.monotonic() - started
        assert f"timed out after {shown} waiting for " in str(info.value)
        assert str(me.cert_path.parent / identity.IDENTITY_LOCK) in str(info.value)
        assert timeout * 0.8 <= elapsed < timeout + 5

    def test_pinned_session_hands_it_to_the_client_context(self, tmp_path):
        me, _ = forge(tmp_path, "client")
        with lock_held(me.cert_path.parent):
            started = time.monotonic()
            with pytest.raises(identity.IdentityError, match=r"timed out after 0\.25s"):
                identity.pinned_session(me, "ab" * 32, lock_timeout=0.25)
            assert time.monotonic() - started < 5

    def test_the_context_is_built_once_the_writer_lets_go(self, tmp_path):
        me, _ = forge(tmp_path, "server")
        with lock_held(me.cert_path.parent):
            with pytest.raises(identity.IdentityError, match="timed out"):
                identity.server_context(me, [], lock_timeout=0.1)
        assert identity.server_context(me, [], lock_timeout=0.1) is not None

    @pytest.mark.parametrize("role", ["server", "client"])
    def test_a_shared_holder_does_not_delay_a_reader(self, tmp_path, role):
        me, _ = forge(tmp_path, role)
        with lock_held(me.cert_path.parent, fcntl.LOCK_SH):
            started = time.monotonic()
            assert make_context(role, me) is not None
            assert time.monotonic() - started < 2

    def test_a_writer_that_finishes_inside_the_timeout_is_waited_for(self, tmp_path):
        me, _ = forge(tmp_path, "client")
        path = me.cert_path.parent / identity.IDENTITY_LOCK
        fd = os.open(str(path), os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX)
        threading.Timer(0.3, os.close, args=(fd,)).start()
        assert identity.client_context(me, lock_timeout=10) is not None

    def test_flock_formats_the_timeout_with_g(self, tmp_path):
        path = tmp_path / "l"
        path.write_bytes(b"")
        holder = os.open(str(path), os.O_RDWR)
        probe = os.open(str(path), os.O_RDWR)
        try:
            fcntl.flock(holder, fcntl.LOCK_EX)
            for timeout, shown in ((0.05, "0.05s"), (2.0, "2s"), (0.1 + 0.2, "0.3s")):
                with pytest.raises(identity.IdentityError) as info:
                    identity._flock(probe, path, fcntl.LOCK_SH, timeout)
                assert f"timed out after {shown} waiting" in str(info.value)
                assert "0.3000000" not in str(info.value)
        finally:
            os.close(holder)
            os.close(probe)


def operator_identity(tmp_path: Path, role: str = "client", name: str = "operator"):
    """A loaded identity whose paths are NOT the generated layout (`<dir>/<name>.crt` with a
    name `ensure_identity` never uses), in a directory of its own."""
    d = tmp_path / f"operator-{role}-{name}"
    d.mkdir()
    key = key_of("ec256")
    cert = make_cert(key, eku=(CLIENT_AUTH,) if role == "client" else (SERVER_AUTH,))
    cert_path, key_path = d / f"{name}.crt", d / f"{name}.key"   # `write_pair` only knows layout names
    key_path.write_bytes(key_pem(key))
    key_path.chmod(0o600)
    cert_path.write_text(pem_of(cert))
    return d, identity.load_identity(cert_path, key_path, role, **NOW)


@pytest.fixture
def lock_spy(monkeypatch):
    """Record every `_shared_lock(directory, timeout)` entry (the real one still runs)."""
    calls = []
    real = identity._shared_lock

    @contextmanager
    def spy(directory, timeout):
        calls.append((Path(directory), timeout))
        with real(directory, timeout) as held:
            yield held

    monkeypatch.setattr(identity, "_shared_lock", spy)
    return calls


@pytest.mark.integration
class TestLoadChainLocksOnlyTheGeneratedLayout:
    @pytest.mark.parametrize("role", ["server", "client"])
    def test_a_generated_layout_is_read_under_the_shared_lock_with_the_given_timeout(
            self, tmp_path, lock_spy, role):
        me, _ = forge(tmp_path, role)
        lock_spy.clear()
        ctx = (identity.server_context(me, [], lock_timeout=7.5) if role == "server"
               else identity.client_context(me, lock_timeout=7.5))
        assert ctx is not None
        assert lock_spy == [(me.cert_path.parent, 7.5)]

    @pytest.mark.parametrize("role", ["server", "client"])
    def test_operator_paths_are_read_without_taking_any_lock(self, tmp_path, lock_spy, role):
        d, ident = operator_identity(tmp_path, role)
        assert identity._layout_name(ident.cert_path, ident.key_path) is None
        assert make_context(role, ident) is not None
        assert lock_spy == []

    def test_operator_paths_do_not_wait_for_a_lock_in_their_directory(self, tmp_path):
        d, ident = operator_identity(tmp_path)
        with lock_held(d):
            started = time.monotonic()
            assert identity.client_context(ident, lock_timeout=0.2) is not None
            assert time.monotonic() - started < 2

    def test_the_same_lock_does_stop_a_generated_layout(self, tmp_path):
        """Control for the test above."""
        me, _ = forge(tmp_path, "client")
        with lock_held(me.cert_path.parent):
            with pytest.raises(identity.IdentityError, match="timed out"):
                identity.client_context(me, lock_timeout=0.2)

    def test_layout_names_with_a_cert_and_key_that_do_not_pair_up_take_no_lock(
            self, tmp_path, lock_spy):
        """`client.crt` with `server.key` is not a generated pair."""
        d = make_tlsdir(tmp_path)
        d.mkdir()
        key = key_of("ec256")
        cert = make_cert(key, eku=(CLIENT_AUTH, SERVER_AUTH))
        write_pair(d, "client", key, cert)
        write_pair(d, "server", key, cert)
        ident = identity.load_identity(d / "client.crt", d / "server.key", "client", **NOW)
        assert identity._layout_name(ident.cert_path, ident.key_path) is None
        identity.client_context(ident)
        assert lock_spy == []

    def test_cert_and_key_in_different_directories_take_no_lock(self, tmp_path, lock_spy):
        d1, d2 = tmp_path / "a", tmp_path / "b"
        d1.mkdir()
        d2.mkdir()
        key = key_of("ec256")
        cert = make_cert(key)
        cert_path, _ = write_pair(d1, "client", key, cert)
        _, key_path = write_pair(d2, "client", key, cert)
        ident = identity.load_identity(cert_path, key_path, "client", **NOW)
        identity.client_context(ident)
        assert lock_spy == []

    @pytest.mark.parametrize("name", ["client", "server", "client.next"])
    def test_every_generated_name_is_locked(self, tmp_path, lock_spy, name):
        d = make_tlsdir(tmp_path)
        role = "server" if name == "server" else "client"
        ident = identity.ensure_identity(d, role, name=name) if name != role else \
            identity.ensure_identity(d, role)
        lock_spy.clear()
        make_context(role, ident)
        assert lock_spy == [(d, identity._DEFAULT_LOCK_TIMEOUT)]

    def test_the_shared_lock_is_released_afterwards(self, tmp_path):
        me, _ = forge(tmp_path, "client")
        identity.client_context(me)
        fd = os.open(str(me.cert_path.parent / identity.IDENTITY_LOCK), os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)         # would raise if still held
        finally:
            os.close(fd)


@pytest.mark.unit
@needs_mkfifo
class TestIdentityLockIsARegularFile:
    """A FIFO planted as `identity.lock` is refused at once, whichever way the lock is opened
    (`O_RDWR` for a writer, `O_RDONLY` for a reader): an `O_RDONLY` open of a FIFO would block
    for ever waiting for a writer, ignoring `lock_timeout` and SIGTERM."""

    @staticmethod
    def planted(tmp_path) -> Path:
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        make_fifo(d / identity.IDENTITY_LOCK)
        return d

    def test_the_read_only_open_is_refused_without_blocking(self, tmp_path):
        d = self.planted(tmp_path)
        err = refused(lambda: identity._open_lock(d, None, False),
                      fifo=d / identity.IDENTITY_LOCK, match="not a regular file")
        assert "named pipe" in str(err) and err.path == d / identity.IDENTITY_LOCK

    def test_the_read_write_open_is_refused_too(self, tmp_path):
        d = self.planted(tmp_path)
        err = refused(lambda: identity._open_lock(d, None, True),
                      fifo=d / identity.IDENTITY_LOCK, match="not a regular file")
        assert "named pipe" in str(err)

    @pytest.mark.parametrize("flags", [os.O_RDONLY, os.O_RDWR])
    def test_open_existing_lock_checks_the_kind_for_both_flag_sets(self, tmp_path, flags):
        fifo = make_fifo(tmp_path / "pipe")
        refused(lambda: identity._open_existing_lock(fifo, flags), fifo=fifo,
                match="not a regular file")

    def test_a_regular_lock_file_is_opened_and_left_blocking(self, tmp_path):
        path = tmp_path / "lock"
        path.write_bytes(b"")
        fd = identity._open_existing_lock(path, os.O_RDONLY)
        try:
            assert os.get_blocking(fd) is True
        finally:
            os.close(fd)

    def test_a_refusal_leaks_no_descriptor_and_leaves_the_fifo(self, tmp_path):
        d = self.planted(tmp_path)
        before = open_fds()
        for write in (False, True):
            refused(lambda: identity._open_lock(d, None, write),
                    fifo=d / identity.IDENTITY_LOCK)
        assert open_fds() == before
        assert fifo_listing(d) == [identity.IDENTITY_LOCK]

    def test_a_directory_as_the_lock_is_an_identity_error(self, tmp_path):
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        (d / identity.IDENTITY_LOCK).mkdir()
        for write in (False, True):
            with pytest.raises(identity.IdentityError, match="identity lock"):
                guarded(lambda: identity._open_lock(d, None, write))

    def test_acquire_lock_refuses_it_for_a_writer(self, tmp_path):
        d = self.planted(tmp_path)
        refused(lambda: identity._acquire_lock(d, 1), fifo=d / identity.IDENTITY_LOCK)

    @needs_non_root
    def test_acquire_lock_refuses_it_for_a_read_only_caller(self, tmp_path):
        d = self.planted(tmp_path)
        with read_only(d):
            refused(lambda: identity._acquire_lock(d, 1), fifo=d / identity.IDENTITY_LOCK)

    def test_ensure_identity_refuses_and_creates_nothing(self, tmp_path):
        d = self.planted(tmp_path)
        before = snapshot(d)
        refused(lambda: identity.ensure_identity(d, "client"),
                fifo=d / identity.IDENTITY_LOCK, match="identity lock")
        assert snapshot(d) == before

    def test_promote_refuses_it(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        (d / identity.IDENTITY_LOCK).unlink()
        make_fifo(d / identity.IDENTITY_LOCK)
        before = snapshot(d)
        refused(lambda: identity.promote_identity(d), fifo=d / identity.IDENTITY_LOCK)
        assert snapshot(d) == before

    def test_complete_promotion_refuses_it_when_there_is_something_to_finish(self, tmp_path):
        # (with nothing to finish it returns None before touching the lock, by contract)
        sub = tmp_path / "half"
        sub.mkdir()
        d, _, _ = stage_half_promotion(sub)
        if (d / identity.IDENTITY_LOCK).exists():
            (d / identity.IDENTITY_LOCK).unlink()
        make_fifo(d / identity.IDENTITY_LOCK)
        before = snapshot(d)
        refused(lambda: identity.complete_promotion(d), fifo=d / identity.IDENTITY_LOCK)
        assert snapshot(d) == before

    def test_the_shared_reader_lock_falls_back_to_an_unlocked_read(self, tmp_path):
        d = self.planted(tmp_path)
        def enter():
            with identity._shared_lock(d, 1) as held:
                return held
        assert guarded(enter, fifo=d / identity.IDENTITY_LOCK) is False

    def test_a_context_is_still_built_next_to_the_fifo_and_does_not_hang(self, tmp_path):
        me, _ = forge(tmp_path, "client")
        lock = me.cert_path.parent / identity.IDENTITY_LOCK
        lock.unlink()
        make_fifo(lock)
        ctx = guarded(lambda: identity.client_context(me), fifo=lock)
        assert ctx is not None

    def test_load_identity_recheck_does_not_hang_on_the_fifo(self, tmp_path):
        """A failed validation in a generated layout re-checks under the shared lock; with a
        FIFO as the lock that falls back to the first error, promptly."""
        me, other = forge(tmp_path, "client")
        lock = me.cert_path.parent / identity.IDENTITY_LOCK
        lock.unlink()
        make_fifo(lock)
        me.cert_path.write_text("junk\n")
        refused(lambda: identity.load_identity(me.cert_path, me.key_path, "client"),
                fifo=lock, match="PEM")


# --------------------------------------------------------------------------
# nsCertType (Netscape certificate type) acceptance matrix
# --------------------------------------------------------------------------

NS_OID = x509.ObjectIdentifier("2.16.840.1.113730.1.1")
NS_CLIENT, NS_SERVER, NS_EMAIL, NS_OBJSIGN = 0x80, 0x40, 0x20, 0x10
NS_SSL_CA, NS_SMIME_CA, NS_OBJSIGN_CA = 0x04, 0x02, 0x01


def ns_der(bits: int) -> bytes:
    """DER BIT STRING for an nsCertType whose first octet is `bits` (named-bit-string rules:
    trailing zero bits are dropped, so `unused` counts them)."""
    if bits == 0:
        return b"\x03\x01\x00"
    unused = (bits & -bits).bit_length() - 1
    return bytes([0x03, 0x02, unused, bits])


def ns_cert(value: bytes, *, basic=True, ku=SIG, critical=False, ca=False, kind="ec256"):
    ext = x509.UnrecognizedExtension(NS_OID, value)
    return make_cert(key_of(kind), basic=basic, ca=ca, ku=ku, extra=[(ext, critical)])


NS_ACCEPTED = {
    "client": NS_CLIENT,
    "client_email": NS_CLIENT | NS_EMAIL,                 # old `openssl ca` / Easy-RSA 2 profile
    "client_server": NS_CLIENT | NS_SERVER,
    "server": NS_SERVER,
    "email": NS_EMAIL,
    "objsign": NS_OBJSIGN,
    "all_non_ca": NS_CLIENT | NS_SERVER | NS_EMAIL | NS_OBJSIGN,
    "reserved_bit_3": 0x08,
    "no_bits": 0x00,
}
NS_CA = {
    "ssl_ca": NS_SSL_CA,
    "smime_ca": NS_SMIME_CA,
    "objsign_ca": NS_OBJSIGN_CA,
    "client_and_ssl_ca": NS_CLIENT | NS_SSL_CA,
    "client_and_smime_ca": NS_CLIENT | NS_SMIME_CA,
    "client_and_objsign_ca": NS_CLIENT | NS_OBJSIGN_CA,
    "ssl_and_smime_ca": NS_SSL_CA | NS_SMIME_CA,
    "all_three_ca": NS_SSL_CA | NS_SMIME_CA | NS_OBJSIGN_CA,
    "everything": 0xFF,
}
NS_MALFORMED = {
    "empty": b"",
    "one_byte": b"\x03",
    "wrong_tag_integer": b"\x02\x01\x00",
    "wrong_tag_octet_string": b"\x04\x02\x07\x80",
    "length_too_long": b"\x03\x05\x00\x80",
    "length_too_short": b"\x03\x01\x07\x80",
    "trailing_garbage": b"\x03\x02\x07\x80\x00",
    "unused_bits_over_7": b"\x03\x02\x08\x80",
    "unused_bits_with_no_data": b"\x03\x01\x05",
    "length_zero": b"\x03\x00",
    "truncated": b"\x03\x02\x07",
    "indefinite_length": b"\x03\x80\x00\x00",
    "long_form_count_zero": b"\x03\x80\x07\x80",
    "long_form_count_5": b"\x03\x85\x00\x00\x00\x00\x02\x07\x80",
    "long_form_truncated": b"\x03\x82\x00",
    "an_octet_string_of_text": b"client",
}


@pytest.mark.unit
class TestNsCertTypeBits:
    """`_ns_cert_type_bits`: the first content octet of a DER BIT STRING, `0` when it has no
    bits, `None` when it is not a well-formed BIT STRING."""

    @pytest.mark.parametrize("bits", [0x80, 0xA0, 0x04, 0x02, 0x01, 0xFF, 0x08, 0x84])
    def test_the_first_octet_is_returned(self, bits):
        assert identity._ns_cert_type_bits(ns_der(bits)) == bits

    def test_a_bit_string_with_no_bits_is_zero(self):
        assert identity._ns_cert_type_bits(b"\x03\x01\x00") == 0

    def test_only_the_first_octet_counts(self):
        assert identity._ns_cert_type_bits(b"\x03\x03\x07\x80\x80") == 0x80
        assert identity._ns_cert_type_bits(b"\x03\x03\x07\x04\xff") == 0x04

    def test_a_long_form_length_is_read(self):
        assert identity._ns_cert_type_bits(b"\x03\x81\x02\x07\x80") == 0x80
        assert identity._ns_cert_type_bits(b"\x03\x82\x00\x02\x07\x04") == 0x04

    @pytest.mark.parametrize("value", list(NS_MALFORMED.values()), ids=list(NS_MALFORMED))
    def test_anything_else_is_none(self, value):
        assert identity._ns_cert_type_bits(value) is None

    def test_the_ca_mask_is_the_three_low_bits(self):
        assert identity._NS_ANY_CA == 0x07


@pytest.mark.unit
class TestCheckClientCertNsCertType:
    @pytest.mark.parametrize("basic", [True, False], ids=["with_basic_constraints", "no_basic_constraints"])
    @pytest.mark.parametrize("name", list(NS_ACCEPTED))
    def test_without_ca_bits_it_is_accepted(self, name, basic):
        cert = ns_cert(ns_der(NS_ACCEPTED[name]), basic=basic)
        assert accept(cert).fingerprint == fp_of(cert)

    @pytest.mark.parametrize("basic", [True, False], ids=["ca_false", "no_basic_constraints"])
    @pytest.mark.parametrize("name", list(NS_CA))
    def test_any_ca_bit_is_refused_even_alongside_ca_false(self, name, basic):
        reject(ns_cert(ns_der(NS_CA[name]), basic=basic), "Netscape certificate type .*CA")

    @pytest.mark.parametrize("name", list(NS_CA))
    def test_a_ca_bit_is_refused_with_or_without_key_usage(self, name):
        reject(ns_cert(ns_der(NS_CA[name]), ku=None), "marks it as a CA")
        reject(ns_cert(ns_der(NS_CA[name]), ku=SIG), "marks it as a CA")

    @pytest.mark.parametrize("basic", [True, False], ids=["ca_false", "no_basic_constraints"])
    @pytest.mark.parametrize("name", list(NS_MALFORMED))
    def test_a_malformed_value_is_refused(self, name, basic):
        reject(ns_cert(NS_MALFORMED[name], basic=basic), "malformed Netscape certificate type")

    def test_a_malformed_value_without_key_usage_is_refused_too(self):
        reject(ns_cert(NS_MALFORMED["wrong_tag_integer"], ku=None, basic=False),
               "malformed Netscape certificate type")

    @pytest.mark.parametrize("kind", ["ec256", "rsa2048", "ed25519"])
    def test_the_key_type_does_not_matter(self, kind):
        accept(ns_cert(ns_der(NS_CLIENT | NS_EMAIL), kind=kind))
        reject(ns_cert(ns_der(NS_SSL_CA), kind=kind), "marks it as a CA")

    def test_the_ca_message_points_at_the_leaf_certificate(self):
        reject(ns_cert(ns_der(NS_SSL_CA)), "pin the client's own leaf certificate")

    def test_a_real_ca_with_a_client_only_nscerttype_is_still_refused_by_basic_constraints(self):
        reject(ns_cert(ns_der(NS_CLIENT), basic=True, ca=True), "CA certificate")

    def test_no_nscerttype_at_all_is_unchanged(self):
        accept(make_cert(key_of("ec256")))
        accept(make_cert(key_of("ec256"), basic=False, ku=None))

    def test_a_critical_nscerttype_is_still_an_unsupported_critical_extension(self):
        """Documents current behaviour: the extension is not in the recognised set, so a
        critical one is refused before its bits are looked at."""
        reject(ns_cert(ns_der(NS_CLIENT), critical=True), "unsupported critical extension")


# ==========================================================================
# discard_identity
# ==========================================================================

NEXT = "client.next"


def next_files(d: Path):
    return d / "client.next.crt", d / "client.next.key"


def without_lock(snap: dict) -> dict:
    """`snapshot` minus the lock file (the one thing `discard_identity` may create)."""
    return {k: v for k, v in snap.items() if k != identity.IDENTITY_LOCK}


def _hold_exclusive(directory, ready, go):
    """Child body: another process holding the identity lock exclusively."""
    fd = os.open(os.path.join(directory, identity.IDENTITY_LOCK), os.O_RDWR | os.O_CREAT, 0o640)
    fcntl.flock(fd, fcntl.LOCK_EX)
    ready.set()
    go.wait(60)
    os.close(fd)


@contextmanager
def other_process_holds_lock(d: Path):
    ctx = multiprocessing.get_context("fork")
    ready, go = ctx.Event(), ctx.Event()
    p = ctx.Process(target=_hold_exclusive, args=(str(d), ready, go))
    p.start()
    try:
        assert ready.wait(30), "the lock holder never started"
        yield
    finally:
        go.set()
        p.join(30)
        if p.is_alive():                           # pragma: no cover - only on a hang
            p.kill()
            p.join(10)


@pytest.mark.unit
class TestDiscardIdentity:
    def test_it_is_exported(self):
        assert "discard_identity" in identity.__all__
        assert identity.discard_identity.__name__ == "discard_identity"

    def test_lock_timeout_and_expected_fingerprint_are_keyword_only(self, tmp_path):
        import inspect

        params = inspect.signature(identity.discard_identity).parameters
        assert params["expected_fingerprint"].kind is inspect.Parameter.KEYWORD_ONLY
        assert params["lock_timeout"].kind is inspect.Parameter.KEYWORD_ONLY
        assert params["expected_fingerprint"].default is None
        assert params["lock_timeout"].default == identity._DEFAULT_LOCK_TIMEOUT
        assert identity._DISCARDABLE == frozenset({"client.next"})

    # --- what is removed --------------------------------------------------------------------

    def test_it_removes_the_pair_and_leaves_the_live_identity_alone(self, tmp_path):
        d, current, successor = two_identities(tmp_path)
        live = {p.name: p.read_bytes() for p in (d / "client.crt", d / "client.key")}
        assert identity.discard_identity(d, NEXT) is True
        assert names_in(d) == ["client.crt", "client.key", "identity.lock"]
        assert {p.name: p.read_bytes() for p in (d / "client.crt", d / "client.key")} == live
        assert identity.load_identity(d / "client.crt", d / "client.key", "client").fingerprint \
            == current.fingerprint

    def test_it_can_be_generated_again_afterwards(self, tmp_path):
        d, _, successor = two_identities(tmp_path)
        identity.discard_identity(d, NEXT)
        again = identity.ensure_identity(d, "client", name=NEXT)
        assert again.fingerprint != successor.fingerprint

    def test_a_second_discard_has_nothing_to_remove(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        assert identity.discard_identity(d, NEXT) is True
        before = snapshot(d)
        assert identity.discard_identity(d, NEXT) is False
        assert snapshot(d) == before

    def test_an_orphan_key_is_removed(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        next_files(d)[0].unlink()
        assert identity.discard_identity(d, NEXT) is True
        assert not next_files(d)[1].exists()

    def test_a_certificate_without_a_key_is_removed_when_it_is_not_promotion_state(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        next_files(d)[1].unlink()                     # client.key does NOT certify client.next.crt
        assert identity.discard_identity(d, NEXT) is True
        assert not next_files(d)[0].exists()

    def test_the_leftover_temporaries_of_that_name_go_and_others_stay(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        mine = [d / ".client.next.crt.tmp", d / ".client.next.key.tmp"]
        theirs = [d / ".client.crt.tmp", d / ".client.key.tmp", d / ".server.crt.tmp"]
        for p in mine + theirs:
            p.write_text("partial")
        assert identity.discard_identity(d, NEXT) is True
        assert not any(p.exists() for p in mine)
        assert all(p.exists() for p in theirs)

    def test_only_temporaries_count_as_something_removed(self, tmp_path):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        (d / ".client.next.key.tmp").write_text("partial")
        assert identity.discard_identity(d, NEXT) is True
        assert not (d / ".client.next.key.tmp").exists()

    def test_an_empty_directory_has_nothing_to_remove(self, tmp_path):
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        assert identity.discard_identity(d, NEXT) is False
        assert names_in(d) == [identity.IDENTITY_LOCK]       # only the lock file may appear

    def test_a_missing_directory_is_false_and_nothing_is_created(self, tmp_path):
        d = make_tlsdir(tmp_path)
        assert not d.exists()
        assert identity.discard_identity(d, NEXT) is False
        assert not d.exists()
        assert identity.discard_identity(tmp_path / "no" / "such" / "dir", NEXT) is False
        assert not (tmp_path / "no").exists()

    def test_a_str_directory_is_accepted(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        assert identity.discard_identity(str(d), NEXT) is True

    # --- the allow-list ---------------------------------------------------------------------

    @pytest.mark.parametrize("name", [
        "client", "server", "bogus", "", "client.next.crt", "client.next ", "CLIENT.NEXT",
        "../client.next", "client.next/", "server.next", None,
    ], ids=repr)
    def test_only_client_next_may_be_discarded(self, tmp_path, name):
        d, _, _ = two_identities(tmp_path)
        identity.ensure_identity(d, "server")
        before = snapshot(d)
        with pytest.raises(identity.IdentityError, match="refusing to discard"):
            identity.discard_identity(d, name)
        assert snapshot(d) == before

    def test_a_refused_name_does_no_io_at_all(self, tmp_path, monkeypatch):
        d = make_tlsdir(tmp_path)                    # does not exist
        calls = []
        for fn in ("mkdir", "open", "unlink", "rename", "stat", "lstat"):
            real = getattr(os, fn)
            monkeypatch.setattr(os, fn, lambda *a, _r=real, _f=fn, **k: calls.append(_f) or _r(*a, **k))
        for name in ("client", "server", "bogus"):
            with pytest.raises(identity.IdentityError):
                identity.discard_identity(d, name)
        assert calls == []
        monkeypatch.undo()
        assert not d.exists()

    def test_the_live_identities_are_refused_before_the_fingerprint_is_looked_at(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        with pytest.raises(identity.IdentityError, match="refusing to discard"):
            identity.discard_identity(d, "client", expected_fingerprint="not a fingerprint")

    # --- expected_fingerprint -----------------------------------------------------------------

    @pytest.mark.parametrize("spelling", [
        lambda fp: fp, lambda fp: fp.upper(),
        lambda fp: ":".join(fp[i:i + 2] for i in range(0, 64, 2)),
        lambda fp: ":".join(fp[i:i + 2].upper() for i in range(0, 64, 2)),
    ], ids=["lower", "upper", "colons", "upper_colons"])
    def test_a_matching_fingerprint_in_any_spelling_removes_the_pair(self, tmp_path, spelling):
        d, _, successor = two_identities(tmp_path)
        assert identity.discard_identity(d, NEXT, expected_fingerprint=spelling(successor.fingerprint)) is True
        assert not any(p.exists() for p in next_files(d))

    def test_a_different_fingerprint_changes_nothing(self, tmp_path):
        d, current, _ = two_identities(tmp_path)
        before = snapshot(d)
        assert identity.discard_identity(d, NEXT, expected_fingerprint=current.fingerprint) is False
        assert snapshot(d) == before

    def test_a_pair_regenerated_since_the_caller_looked_survives(self, tmp_path):
        d, _, successor = two_identities(tmp_path)
        identity.discard_identity(d, NEXT)
        fresh = identity.ensure_identity(d, "client", name=NEXT)
        before = snapshot(d)
        assert identity.discard_identity(d, NEXT, expected_fingerprint=successor.fingerprint) is False
        assert snapshot(d) == before
        assert identity.discard_identity(d, NEXT, expected_fingerprint=fresh.fingerprint) is True

    def test_a_missing_certificate_with_a_fingerprint_changes_nothing(self, tmp_path):
        d, _, successor = two_identities(tmp_path)
        next_files(d)[0].unlink()
        before = snapshot(d)
        assert identity.discard_identity(d, NEXT, expected_fingerprint=successor.fingerprint) is False
        assert snapshot(d) == before                  # the orphan key stays

    @pytest.mark.parametrize("junk", ["", "garbage\n", "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"])
    def test_an_unreadable_certificate_with_a_fingerprint_changes_nothing(self, tmp_path, junk):
        d, _, successor = two_identities(tmp_path)
        next_files(d)[0].write_text(junk)
        before = snapshot(d)
        assert identity.discard_identity(d, NEXT, expected_fingerprint=successor.fingerprint) is False
        assert snapshot(d) == before

    @needs_mkfifo
    def test_a_fifo_certificate_with_a_fingerprint_is_false_without_blocking(self, tmp_path):
        d, _, successor = two_identities(tmp_path)
        crt = next_files(d)[0]
        crt.unlink()
        make_fifo(crt)
        assert guarded(lambda: identity.discard_identity(d, NEXT, expected_fingerprint=successor.fingerprint),
                       fifo=crt) is False
        assert fifo_listing(d) == ["client.next.crt"]

    @pytest.mark.parametrize("bad", ["", "abc", "zz" * 32, "ab" * 31, 5, b"ab" * 32])
    def test_a_malformed_fingerprint_is_a_value_error_and_changes_nothing(self, tmp_path, bad):
        d, _, _ = two_identities(tmp_path)
        before = snapshot(d)
        with pytest.raises(ValueError):
            identity.discard_identity(d, NEXT, expected_fingerprint=bad)
        assert snapshot(d) == before

    def test_a_malformed_fingerprint_is_refused_before_the_directory_is_looked_at(self, tmp_path):
        d = make_tlsdir(tmp_path)
        with pytest.raises(ValueError):
            identity.discard_identity(d, NEXT, expected_fingerprint="abc")

    # --- half promotion ---------------------------------------------------------------------------

    @pytest.mark.parametrize("with_current", [True, False])
    def test_an_interrupted_promotion_is_refused_and_nothing_is_removed(self, tmp_path, with_current):
        d, _, successor = stage_half_promotion(tmp_path, with_current)
        before = snapshot(d)
        with pytest.raises(identity.IdentityError, match="complete_promotion") as info:
            identity.discard_identity(d, NEXT)
        assert "client.next" in str(info.value) and info.value.path == d
        assert snapshot(d) == before
        with pytest.raises(identity.IdentityError, match="complete_promotion"):
            identity.discard_identity(d, NEXT, expected_fingerprint=successor.fingerprint)
        assert snapshot(d) == before

    def test_after_completing_the_promotion_there_is_nothing_left_to_discard(self, tmp_path):
        d, _, successor = stage_half_promotion(tmp_path)
        with pytest.raises(identity.IdentityError):
            identity.discard_identity(d, NEXT)
        done = identity.complete_promotion(d)
        assert done.fingerprint == successor.fingerprint
        assert identity.discard_identity(d, NEXT) is False
        assert identity.load_identity(d / "client.crt", d / "client.key", "client").fingerprint \
            == successor.fingerprint

    # --- locking ------------------------------------------------------------------------------------

    @needs_fork
    def test_another_process_holding_the_lock_makes_it_time_out_and_changes_nothing(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        before = snapshot(d)
        with other_process_holds_lock(d):
            started = time.monotonic()
            with pytest.raises(identity.IdentityError, match=r"timed out after 0\.3s") as info:
                identity.discard_identity(d, NEXT, lock_timeout=0.3)
            assert 0.25 <= time.monotonic() - started < 10
            assert exact(info.value) is identity.IdentityError
            assert snapshot(d) == before
        assert identity.discard_identity(d, NEXT, lock_timeout=5) is True

    @needs_fork
    def test_it_does_not_unlink_while_promote_identity_holds_the_lock(self, tmp_path):
        d, _, successor = stage_half_promotion(tmp_path)
        os.rename(d / "client.key", d / "client.next.key")        # back to a whole successor pair
        with live_window(d) as go:                                 # a live promoter between the renames
            during = snapshot(d)
            with pytest.raises(identity.IdentityError, match="timed out"):
                identity.discard_identity(d, NEXT, lock_timeout=0.3)
            assert snapshot(d) == during
        # the promoter finished: client is the successor and client.next is gone
        assert identity.discard_identity(d, NEXT) is False
        assert identity.load_identity(d / "client.crt", d / "client.key", "client").fingerprint \
            == successor.fingerprint

    def test_a_shared_holder_also_blocks_it(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        before = snapshot(d)
        with lock_held(d, fcntl.LOCK_SH):
            with pytest.raises(identity.IdentityError, match="timed out"):
                identity.discard_identity(d, NEXT, lock_timeout=0.2)
        assert snapshot(d) == before

    def test_the_lock_is_released_afterwards_on_every_path(self, tmp_path):
        d, _, successor = two_identities(tmp_path)
        identity.discard_identity(d, NEXT, expected_fingerprint="ab" * 32)       # False path
        identity.discard_identity(d, NEXT)                                        # True path
        with pytest.raises(identity.IdentityError):
            identity.discard_identity(d, "client")                                # refusal
        with lock_held(d):                                                        # would time out otherwise
            pass
        fd = os.open(str(d / identity.IDENTITY_LOCK), os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(fd)

    def test_concurrent_ensure_and_discard_never_leave_a_certificate_without_its_key(self, tmp_path):
        for round_ in range(12):
            (tmp_path / f"r{round_}").mkdir()
            d = make_tlsdir(tmp_path / f"r{round_}")
            identity.ensure_identity(d, "client")
            identity.ensure_identity(d, "client", name=NEXT)
            results = run_threads_pair(
                lambda: identity.discard_identity(d, NEXT),
                lambda: identity.ensure_identity(d, "client", name=NEXT).fingerprint,
            )
            assert not any(isinstance(r, BaseException) for r in results), results
            crt, key = next_files(d)
            assert crt.exists() == key.exists(), (round_, names_in(d))
            if crt.exists():
                assert identity.load_identity(crt, key, "client").fingerprint
            assert identity.load_identity(d / "client.crt", d / "client.key", "client")
            assert not [n for n in names_in(d) if n.endswith(".tmp")]

    def test_two_concurrent_discards_remove_it_once(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        results = run_threads_pair(lambda: identity.discard_identity(d, NEXT),
                                   lambda: identity.discard_identity(d, NEXT))
        assert sorted(results) == [False, True]

    # --- the lock file's own kind ---------------------------------------------------------------

    @needs_mkfifo
    def test_a_fifo_as_the_lock_is_refused_without_blocking(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        lock = d / identity.IDENTITY_LOCK
        lock.unlink()
        make_fifo(lock)
        before = snapshot(d)
        refused(lambda: identity.discard_identity(d, NEXT), fifo=lock, match="identity lock")
        assert snapshot(d) == before

    # --- crash and failure injection ------------------------------------------------------------

    def test_a_failure_removing_the_key_leaves_an_orphan_key_that_ensure_regenerates(
            self, tmp_path, monkeypatch):
        d, _, successor = two_identities(tmp_path)
        crt, key = next_files(d)
        real = identity._unlink_quiet

        def failing(path):
            if Path(path) == key:
                raise identity.IdentityError(f"cannot remove {path}: injected", path)
            return real(path)

        monkeypatch.setattr(identity, "_unlink_quiet", failing)
        with pytest.raises(identity.IdentityError, match="injected"):
            identity.discard_identity(d, NEXT)
        monkeypatch.undo()
        assert not crt.exists() and key.exists()                   # the certificate went first
        fresh = identity.ensure_identity(d, "client", name=NEXT)   # the orphan is debris
        assert fresh.fingerprint != successor.fingerprint
        assert crt.exists() and key.exists()

    def test_a_permission_error_on_the_key_is_an_identity_error_with_the_certificate_gone(
            self, tmp_path, monkeypatch):
        d, _, _ = two_identities(tmp_path)
        crt, key = next_files(d)
        real = os.unlink

        def unlink(path, *a, **kw):
            if Path(path) == key:
                raise PermissionError(errno.EACCES, "Permission denied")
            return real(path, *a, **kw)

        monkeypatch.setattr(os, "unlink", unlink)
        with pytest.raises(identity.IdentityError, match="permission denied") as info:
            identity.discard_identity(d, NEXT)
        monkeypatch.undo()
        assert info.value.path == key
        assert not crt.exists() and key.exists()
        assert identity.discard_identity(d, NEXT) is True          # a later run cleans up
        assert not key.exists()

    def test_an_os_error_on_the_certificate_removes_nothing(self, tmp_path, monkeypatch):
        d, _, _ = two_identities(tmp_path)
        before = snapshot(d)
        real = os.unlink

        def unlink(path, *a, **kw):
            if Path(path).name == "client.next.crt":
                raise OSError(errno.EIO, "Input/output error")
            return real(path, *a, **kw)

        monkeypatch.setattr(os, "unlink", unlink)
        with pytest.raises(identity.IdentityError, match="cannot remove"):
            identity.discard_identity(d, NEXT)
        monkeypatch.undo()
        assert snapshot(d) == before

    @needs_fork
    def test_a_process_killed_between_the_two_unlinks_leaves_an_orphan_key(self, tmp_path):
        import signal

        d, _, successor = two_identities(tmp_path)

        def child():
            real = identity._unlink_quiet

            def wrapped(path):
                result = real(path)
                if Path(path).name == "client.next.crt":
                    os.kill(os.getpid(), signal.SIGKILL)
                return result

            identity._unlink_quiet = wrapped
            identity.discard_identity(d, NEXT)
            os._exit(0)                                            # pragma: no cover

        ctx = multiprocessing.get_context("fork")
        p = ctx.Process(target=child)
        p.start()
        try:
            p.join(60)
        finally:
            if p.is_alive():                                       # pragma: no cover
                p.kill()
                p.join(10)
        assert p.exitcode == -signal.SIGKILL
        crt, key = next_files(d)
        assert not crt.exists() and key.exists()
        assert identity._find_half_promotion(d, NEXT) is None      # never mistaken for promotion state
        fresh = identity.ensure_identity(d, "client", name=NEXT, lock_timeout=10)
        assert fresh.fingerprint != successor.fingerprint
        assert identity.discard_identity(d, NEXT, lock_timeout=10) is True

    # --- durability order ---------------------------------------------------------------------------

    def test_the_order_is_certificate_fsync_key_temporaries_fsync(self, tmp_path, fs_events):
        d, _, _ = two_identities(tmp_path)
        (d / ".client.next.crt.tmp").write_text("x")
        (d / ".client.next.key.tmp").write_text("x")
        fs_events.raw.clear()
        identity.discard_identity(d, NEXT)
        steps = [e for e in fs_events.raw if e != ("fsync", "file")]
        names = [e[1] if e[0] == "unlink" else "fsync" for e in steps]
        assert names == ["client.next.crt", "fsync", "client.next.key",
                         ".client.next.key.tmp", ".client.next.crt.tmp", "fsync"]
        assert all(e[0] in ("unlink", "fsync") for e in steps)
        tls = ("dir", os.stat(d).st_dev, os.stat(d).st_ino)
        assert [e[1:] for e in steps if e[0] == "fsync"] == [tls, tls]    # the TLS directory, twice

    def test_nothing_removed_means_no_fsync(self, tmp_path, fs_events):
        d = make_tlsdir(tmp_path)
        d.mkdir(mode=0o750)
        fs_events.raw.clear()
        assert identity.discard_identity(d, NEXT) is False
        assert [e for e in fs_events.raw if e[0] == "fsync"] == []

    # --- who may call it ---------------------------------------------------------------------------

    @needs_non_root
    def test_a_caller_who_cannot_write_the_directory_is_refused(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        before = snapshot(d)
        with read_only(d):
            with pytest.raises(identity.IdentityError, match="permission denied") as info:
                identity.discard_identity(d, NEXT)
        assert exact(info.value) is identity.IdentityError
        assert "cannot discard the client.next identity" in str(info.value)   # not a failed unlink
        assert snapshot(d) == before

    @needs_non_root
    def test_a_non_writer_is_refused_even_when_there_is_nothing_to_remove(self, tmp_path):
        d = make_tlsdir(tmp_path)
        identity.ensure_identity(d, "client")
        with read_only(d):
            with pytest.raises(identity.IdentityError, match="cannot discard the client.next identity"):
                identity.discard_identity(d, NEXT)

    @needs_non_root
    def test_a_read_only_caller_without_a_lock_file_is_refused_too(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        (d / identity.IDENTITY_LOCK).unlink()
        before = snapshot(d)
        with read_only(d):
            with pytest.raises(identity.IdentityError, match="permission denied"):
                identity.discard_identity(d, NEXT)
        assert snapshot(d) == before

    def test_a_symlinked_directory_is_refused(self, tmp_path):
        real_dir, _, _ = two_identities(tmp_path)
        link = tmp_path / "link.tls"
        link.symlink_to(real_dir)
        before = snapshot(real_dir)
        with pytest.raises(identity.IdentityError, match="symlink or not a directory"):
            identity.discard_identity(link, NEXT)
        assert snapshot(real_dir) == before

    def test_a_dangling_symlink_is_refused_not_treated_as_missing(self, tmp_path):
        link = tmp_path / "dangling.tls"
        link.symlink_to(tmp_path / "nowhere")
        with pytest.raises(identity.IdentityError):
            identity.discard_identity(link, NEXT)
        assert not (tmp_path / "nowhere").exists()

    def test_a_regular_file_in_place_of_the_directory_is_refused(self, tmp_path):
        d = make_tlsdir(tmp_path)
        d.write_text("not a directory")
        with pytest.raises(identity.IdentityError, match="not a directory"):
            identity.discard_identity(d, NEXT)
        assert d.read_text() == "not a directory"

    def test_a_world_writable_directory_is_refused(self, tmp_path):
        d, _, _ = two_identities(tmp_path)
        before = snapshot(d)
        d.chmod(0o777)
        try:
            with pytest.raises(identity.IdentityError, match="writable by everyone"):
                identity.discard_identity(d, NEXT)
        finally:
            d.chmod(0o750)
        assert snapshot(d) == before


def run_threads_pair(first, second):
    """Run two callables on threads released together; -> their results (exceptions as values)."""
    barrier = threading.Barrier(2)
    out = [None, None]

    def body(index, fn):
        try:
            barrier.wait(30)
            out[index] = fn()
        except BaseException as exc:  # noqa: BLE001 - reported through the result
            out[index] = exc

    threads = [threading.Thread(target=body, args=(i, fn), daemon=True)
               for i, fn in enumerate((first, second))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert not any(t.is_alive() for t in threads), "a thread hung"
    return out

"""TLS identities, fingerprint pinning, enrollment tokens and proof of possession.

This is the one implementation of the cryptographic plumbing that the daemon
(API server, remote client), the config store and the `zfsbackup-config` CLI
share (plan item M3 in `docs/tls_mtls_plan.md`). It knows nothing about the
config database, workers or HTTP routes.

**Import rule.** Only the stdlib, `cryptography` and `zfsbackup.config` are
imported at module level. The CLI imports this module, so it must never import
the daemon, workers, `remote` or `api`. `requests`/`urllib3` (for
`pinned_session`) and `websocket` are imported lazily inside the functions that
need them, so the CLI's import cost stays low.

Trust model (A1, D7):

* **The client pins only the server's certificate fingerprint** (SHA-256 of the
  DER certificate, 64 lowercase hex). It connects with `CERT_NONE` and no
  hostname check, and compares the fingerprint right after the handshake,
  before a single application byte is sent: `connect_pinned` for raw/websocket
  connections, `pinned_session` (urllib3's `assert_fingerprint`) for HTTP. The
  server certificate's expiry is deliberately ignored by clients.
* **The server pins client certificates as exact trust anchors**:
  `CERT_OPTIONAL` + `VERIFY_X509_PARTIAL_CHAIN` with the pinned leaf PEMs as
  the only CA data (`server_context`). A pinned certificate must still be
  within its validity window and carry a usable EKU; an unpinned one fails the
  handshake; no certificate at all completes the handshake, and the
  application layer answers 401/403. `check_client_cert` is the acceptance
  policy applied *before* a certificate may be pinned -- most importantly it
  refuses CA certificates, because a pinned CA would let everything it signs
  pass the handshake.

Files (A2, A4). Generated identities live in `<db>.tls/` (`tls_dir`), mode
0750, created with the DB file's group when running as root. Keys are 0600,
certificates 0640, `identity.lock` 0640. Creating or moving anything in that
directory happens under an exclusive `flock` on `<db>.tls/identity.lock`;
readers that hit an inconsistent pair re-check under a shared `flock` (a
caller that cannot write the directory only ever takes the shared one).
`ensure_identity` writes each file to an `O_EXCL` 0600 temporary, fsyncs it,
removes any old certificate, renames the key into place before the
certificate, and fsyncs the directory after every step; a crash at any point
leaves either the old state, an incomplete pair (regenerated on the next
call), or a complete new pair -- never a complete *mismatched* pair produced by
us. `promote_identity` cannot avoid a mismatched window between its two
renames (new key, old certificate); that state is recognised -- the source
certificate is still there, without its key, and certifies the moved key --
and rolled forward by the next writer (`ensure_identity`, `promote_identity`,
`complete_promotion`), never reported as damage to be deleted. Keys are never
stored in the database.

Secrets. An enrollment token (`zfsb1.<server fp>.<base64url secret>`) is a
bearer secret until consumed. Nothing here puts a token or its secret into an
exception message or a log line, and the values `make_token`/`parse_token`
return have redacted `repr()`s (`SecretStr`/`SecretBytes`), so an accidental
`%r`, f-string `!r` or traceback-locals dump does not leak them. `str()` of a
`SecretStr` is still the token -- the CLI has to print it once.

Errors. `IdentityError` carries an operator-facing message (what is wrong with
which file, and what to do). Its subclass `IdentityChanged` is the one finding
that means "the files now hold another identity" (`server_context` /
`client_context`); every other `IdentityError` there is worth retrying.
`PinMismatch` is raised by `connect_pinned` when
the server presents a different certificate; it is deliberately *not* an
`OSError`, so code that treats `OSError` as "unreachable, retry later" does not
mistake an impostor for a network blip. Format/policy failures of tokens,
payload components and client certificates raise `ValueError`.
"""

from __future__ import annotations

import base64
import binascii
import contextlib
import errno
import fcntl
import hashlib
import hmac
import ipaddress
import logging
import os
import re
import secrets
import shutil
import socket
import ssl
import stat as stat_module
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    TYPE_CHECKING, Iterable, Iterator, List, Literal, Optional, Tuple, Union,
)

from cryptography import x509
from cryptography.exceptions import InvalidSignature, UnsupportedAlgorithm
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519, padding, rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from zfsbackup.config import client_id_problem, normalize_fingerprint

if TYPE_CHECKING:  # pragma: no cover - typing only; imported lazily at runtime
    import requests

logger = logging.getLogger(__name__)

__all__ = [
    # roles and file layout
    "ROLE_SERVER",
    "ROLE_CLIENT",
    "Role",
    "SERVER_NAME",
    "CLIENT_NAME",
    "CLIENT_NEXT_NAME",
    "SERVER_KEY",
    "SERVER_CRT",
    "CLIENT_KEY",
    "CLIENT_CRT",
    "CLIENT_NEXT_KEY",
    "CLIENT_NEXT_CRT",
    "IDENTITY_LOCK",
    "TLS_DIR_MODE",
    "KEY_MODE",
    "CERT_MODE",
    "tls_dir",
    "identity_paths",
    # fingerprints
    "fingerprint_der",
    "fingerprint_pem",
    "normalize_fingerprint",
    # identities
    "Identity",
    "IdentityError",
    "IdentityChanged",
    "CLIENT_CERT_EXPIRY_WARN_DAYS",
    "client_cert_not_after",
    "client_cert_expiry_problem",
    "CertInfo",
    "ClientCertInfo",
    "ensure_identity",
    "load_identity",
    "peek_identity",
    "promote_identity",
    "complete_promotion",
    "discard_identity",
    "copy_tls_dir",
    "KeyUnreadable",
    # TLS
    "PinMismatch",
    "server_context",
    "client_context",
    "connect_pinned",
    "pinned_session",
    "is_pin_mismatch",
    "is_tls_alert_rejection",
    # tokens
    "SecretStr",
    "SecretBytes",
    "TOKEN_PREFIX",
    "make_token",
    "parse_token",
    "secret_hash",
    # proof of possession
    "enrollment_payload",
    "rotation_payload",
    "sign_payload",
    "verify_payload",
    # acceptance policy
    "check_client_cert",
    "MAX_CLIENT_CERT_PEM",
]


# --------------------------------------------------------------------------
# Roles and file layout
# --------------------------------------------------------------------------

ROLE_SERVER = "server"
ROLE_CLIENT = "client"
Role = Literal["server", "client"]
_ROLES = (ROLE_SERVER, ROLE_CLIENT)

#: Base names of the identities kept in the TLS directory. `client.next` is the
#: successor identity `identity rotate` (M14c) creates before it swaps it in.
SERVER_NAME = "server"
CLIENT_NAME = "client"
CLIENT_NEXT_NAME = "client.next"

#: Which role each base name holds (its EKU and validation rules).
_NAME_ROLES = {
    SERVER_NAME: ROLE_SERVER,
    CLIENT_NAME: ROLE_CLIENT,
    CLIENT_NEXT_NAME: ROLE_CLIENT,
}

SERVER_KEY = "server.key"
SERVER_CRT = "server.crt"
CLIENT_KEY = "client.key"
CLIENT_CRT = "client.crt"
CLIENT_NEXT_KEY = "client.next.key"
CLIENT_NEXT_CRT = "client.next.crt"

#: The identity-creation lock inside the TLS directory (A4).
IDENTITY_LOCK = "identity.lock"

#: Suffix of the TLS directory next to the config DB: `<db>.tls`.
_TLS_DIR_SUFFIX = ".tls"

TLS_DIR_MODE = 0o750
KEY_MODE = 0o600
CERT_MODE = 0o640
#: The lock file is group-readable so a group operator can `flock` it
#: (`flock` needs only a read-only descriptor). It holds no data.
_LOCK_MODE = 0o640

#: Generated certificates (A3).
_CERT_VALIDITY = timedelta(days=3650)
_CERT_BACKDATE = timedelta(minutes=5)
#: Upper bound on any key/certificate file we read; real ones are a few KiB.
_MAX_FILE_BYTES = 1024 * 1024
#: How long a caller waits for another process holding the identity lock.
_DEFAULT_LOCK_TIMEOUT = 30.0
_LOCK_POLL = 0.05

_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_O_NONBLOCK = getattr(os, "O_NONBLOCK", 0)


def tls_dir(db_path: Union[str, Path]) -> Path:
    """Return the TLS directory for a config database: `<db>.tls`.

    Pure path arithmetic; nothing is created or checked.
    """
    db = Path(db_path)
    return db.with_name(db.name + _TLS_DIR_SUFFIX)


def identity_paths(directory: Union[str, Path], name: str) -> Tuple[Path, Path]:
    """Return `(cert_path, key_path)` of identity `name` in `directory`.

    `name` is one of `SERVER_NAME`, `CLIENT_NAME`, `CLIENT_NEXT_NAME`.
    """
    _role_of(name)
    d = Path(directory)
    return d / f"{name}.crt", d / f"{name}.key"


def _role_of(name: str) -> str:
    try:
        return _NAME_ROLES[name]
    except KeyError:
        raise ValueError(
            f"unknown identity name {name!r}; expected one of {sorted(_NAME_ROLES)}"
        ) from None


def _check_role(role: str) -> str:
    if role not in _ROLES:
        raise ValueError(f"role must be 'server' or 'client', got {role!r}")
    return role


# --------------------------------------------------------------------------
# Fingerprints
# --------------------------------------------------------------------------

def fingerprint_der(der: bytes) -> str:
    """SHA-256 of a DER certificate, as 64 lowercase hex digits."""
    if not isinstance(der, (bytes, bytearray, memoryview)):
        raise TypeError(f"DER certificate must be bytes, got {type(der).__name__}")
    return hashlib.sha256(bytes(der)).hexdigest()


def fingerprint_pem(pem: Union[str, bytes]) -> str:
    """SHA-256 of the first certificate in `pem` (its DER form), 64 lowercase hex.

    Raises `ValueError` if `pem` holds no parseable certificate.
    """
    return fingerprint_der(_load_cert(pem).public_bytes(serialization.Encoding.DER))


def _load_cert(pem: Union[str, bytes]) -> x509.Certificate:
    data = pem.encode("ascii", "replace") if isinstance(pem, str) else bytes(pem)
    try:
        return x509.load_pem_x509_certificate(data)
    except (ValueError, x509.InvalidVersion) as exc:
        raise ValueError(f"not a PEM certificate ({exc})") from None


def _cert_fingerprint(cert: x509.Certificate) -> str:
    return fingerprint_der(cert.public_bytes(serialization.Encoding.DER))


def _cert_pem(cert: x509.Certificate) -> str:
    return cert.public_bytes(serialization.Encoding.PEM).decode("ascii")


def _is_canonical_fingerprint(value: object) -> bool:
    return isinstance(value, str) and _HEX64_RE.fullmatch(value) is not None


_HEX64_RE = re.compile(r"[0-9a-f]{64}")


# --------------------------------------------------------------------------
# Errors and value objects
# --------------------------------------------------------------------------

class IdentityError(Exception):
    """A TLS identity is missing, unreadable, unsafe or unusable.

    The message is meant for the operator: it names the file and, where there
    is one, the fix. `path` is the offending file or directory, if any.
    """

    def __init__(self, message: str, path: Optional[Union[str, Path]] = None):
        super().__init__(message)
        self.path = Path(path) if path is not None else None


class _AccessDenied(IdentityError):
    """This process may not read the file: not a defect of the identity."""


class IdentityChanged(IdentityError):
    """The files of a loaded `Identity` now hold a different identity.

    Raised by `server_context` / `client_context` (`_load_chain`) only, and
    only for two findings: the certificate file's fingerprint is no longer
    `expected` (`actual` is the one found), or the key file no longer matches
    the certificate (`actual` is `None`). Every other `IdentityError` from
    those functions -- the identity lock timing out, a file that cannot be
    opened (EMFILE, EACCES), unparseable PEM -- says nothing about *which*
    identity is on disk and is worth retrying; this one means "load the
    identity again". A caller that restarts on it should still confirm the
    change (`peek_identity`) before acting: the files may be mid-write by a
    process that does not take the identity lock.
    """

    def __init__(self, message: str, path: Optional[Union[str, Path]] = None, *,
                 expected: str, actual: Optional[str]):
        super().__init__(message, path)
        self.expected = expected
        self.actual = actual


class PinMismatch(Exception):
    """The server presented a certificate whose fingerprint is not the pinned one.

    Raised by `connect_pinned` after the handshake and before any application
    byte is sent; the socket is already closed. Not an `OSError` on purpose:
    this is an impostor (or a re-keyed server), not a transient failure.
    """

    def __init__(self, expected: str, actual: Optional[str]):
        self.expected = expected
        self.actual = actual
        got = actual if actual is not None else "no certificate"
        super().__init__(
            f"server certificate fingerprint {got} does not match the pinned "
            f"fingerprint {expected}; refusing to talk to it (if the server was "
            "re-keyed, re-enroll this destination)"
        )


@dataclass(frozen=True)
class Identity:
    """A loaded, validated key + certificate pair.

    No key material is held: `server_context`/`client_context` and the signing
    helpers re-read `key_path` (and `cert_path`) when needed, and the contexts
    refuse the files if their certificate is no longer `fingerprint`.
    `not_after` is timezone-aware UTC.
    """

    cert_path: Path
    key_path: Path
    cert_pem: str = field(repr=False)
    fingerprint: str
    not_after: datetime
    role: str


@dataclass(frozen=True)
class CertInfo:
    """What a certificate is, without its key: fingerprint, expiry, subject."""

    fingerprint: str
    not_after: datetime
    subject: str


#: `check_client_cert`'s result type (the plan's name for it).
ClientCertInfo = CertInfo


# --------------------------------------------------------------------------
# Reading keys and certificates
# --------------------------------------------------------------------------

PrivateKey = Union[
    ec.EllipticCurvePrivateKey, rsa.RSAPrivateKey, ed25519.Ed25519PrivateKey
]


def _file_kind(mode: int) -> str:
    for test, name in (
        (stat_module.S_ISFIFO, "named pipe"), (stat_module.S_ISDIR, "directory"),
        (stat_module.S_ISSOCK, "socket"), (stat_module.S_ISCHR, "character device"),
        (stat_module.S_ISBLK, "block device"),
    ):
        if test(mode):
            return name
    return "special file"


def _read_file(path: Path, what: str) -> Tuple[bytes, os.stat_result]:
    """Read a small regular file; -> (content, fstat of the opened file).

    Symlinks are followed (operator-supplied paths may legitimately be
    symlinks); the checks apply to the file actually opened. The open is
    `O_NONBLOCK`, so a FIFO (a planted file, or a mistyped operator path)
    opens at once and is refused by the `fstat` below rather than blocking
    the caller -- the daemon supervisor among them -- until a writer
    appears. A regular file ignores `O_NONBLOCK`; it is cleared anyway
    before reading.
    """
    try:
        fd = os.open(str(path), os.O_RDONLY | _O_CLOEXEC | _O_NONBLOCK)
    except FileNotFoundError:
        raise IdentityError(f"{what} {path} does not exist", path) from None
    except PermissionError:
        raise _AccessDenied(
            f"cannot read {what} {path}: permission denied (run as its owner or root)",
            path,
        ) from None
    except OSError as exc:
        raise IdentityError(f"cannot open {what} {path}: {exc.strerror}", path) from None
    try:
        st = os.fstat(fd)
        if not stat_module.S_ISREG(st.st_mode):
            raise IdentityError(
                f"{what} {path} is not a regular file (it is a "
                f"{_file_kind(st.st_mode)}); point it at the PEM file itself",
                path,
            )
        if _O_NONBLOCK:
            os.set_blocking(fd, True)
        if st.st_size > _MAX_FILE_BYTES:
            raise IdentityError(
                f"{what} {path} is {st.st_size} bytes; that is not a key or certificate",
                path,
            )
        chunks = []
        while True:
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks), st
    except OSError as exc:
        raise IdentityError(f"cannot read {what} {path}: {exc.strerror}", path) from None
    finally:
        os.close(fd)


def _check_key_mode(path: Path, st: os.stat_result, warn: bool = True) -> None:
    """Refuse a key others can write or the world can read; warn on group-read."""
    mode = stat_module.S_IMODE(st.st_mode)
    if mode & 0o022:
        raise IdentityError(
            f"private key {path} is writable by its group or by others (mode "
            f"{mode:04o}); refusing to use it. Run: chmod 600 {path}",
            path,
        )
    if mode & 0o004:
        raise IdentityError(
            f"private key {path} is readable by everyone (mode {mode:04o}); "
            f"refusing to use it. Run: chmod 600 {path} -- and consider the key "
            "compromised if this host has untrusted local users",
            path,
        )
    if warn and mode & 0o040:
        logger.warning(
            "private key %s is readable by its group (mode %04o); 0600 is recommended",
            path, mode,
        )


def _parse_private_key(data: bytes, path: Path) -> PrivateKey:
    try:
        key = serialization.load_pem_private_key(data, password=None)
    except TypeError:
        raise IdentityError(
            f"private key {path} is encrypted; zfsbackup needs an unencrypted key "
            "(protect it with file permissions instead)",
            path,
        ) from None
    except (ValueError, UnsupportedAlgorithm):
        raise IdentityError(f"{path} does not contain a PEM private key", path) from None
    if not isinstance(key, (ec.EllipticCurvePrivateKey, rsa.RSAPrivateKey,
                            ed25519.Ed25519PrivateKey)):
        raise IdentityError(
            f"private key {path} is a {type(key).__name__}; only EC, RSA and "
            "Ed25519 keys are supported",
            path,
        )
    return key


def _load_private_key(path: Path) -> PrivateKey:
    data, st = _read_file(path, "private key")
    _check_key_mode(path, st)
    return _parse_private_key(data, path)


def _load_cert_file(path: Path) -> x509.Certificate:
    data, _ = _read_file(path, "certificate")
    try:
        return x509.load_pem_x509_certificate(data)
    except ValueError:
        raise IdentityError(f"{path} does not contain a PEM certificate", path) from None


def _spki(public_key) -> bytes:
    return public_key.public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    )


def _eku(cert: x509.Certificate) -> Optional[x509.ExtendedKeyUsage]:
    try:
        return cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
    except x509.ExtensionNotFound:
        return None


_ROLE_EKU = {
    ROLE_SERVER: ExtendedKeyUsageOID.SERVER_AUTH,
    ROLE_CLIENT: ExtendedKeyUsageOID.CLIENT_AUTH,
}


def _now(now: Optional[datetime]) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now


def load_identity(
    cert_path: Union[str, Path],
    key_path: Union[str, Path],
    role: str,
    *,
    allow_expired: bool = False,
    now: Optional[datetime] = None,
    lock_timeout: float = _DEFAULT_LOCK_TIMEOUT,
) -> Identity:
    """Load and validate a key + certificate pair for `role`.

    Refuses (raises `IdentityError`) when:

    * the key is not a regular file, is writable by its group or others, or is
      readable by everyone (a group-readable key only logs a warning);
    * the key is encrypted or of an unsupported type (EC, RSA, Ed25519 only);
    * either file is unreadable or not PEM;
    * the certificate's public key does not match the key;
    * the certificate has expired (unless `allow_expired`, for the server role
      whose expiry clients ignore -- it is then logged as a warning) or is not
      valid yet;
    * the certificate has an EKU that does not include `serverAuth`/`clientAuth`
      for `role` (no EKU at all is allowed).

    `cert_path` may hold a chain; the first certificate is the leaf.

    When the paths are a generated identity (`<dir>/<name>.crt` and
    `<dir>/<name>.key`) and the pair does not validate, it is checked once
    more under a *shared* `flock` on `<dir>/identity.lock` (waiting up to
    `lock_timeout` for a writer), so a reader that lands in the middle of a
    `promote_identity` gets the finished pair instead of an error. If an
    interrupted promotion is still there, the error says so and how to finish
    it. Nothing is ever created or changed.
    """
    _check_role(role)
    cert_path, key_path = Path(cert_path), Path(key_path)
    try:
        return _validate_pair(cert_path, key_path, role,
                              allow_expired=allow_expired, now=now)
    except _AccessDenied:
        raise
    except IdentityError:
        name = _layout_name(cert_path, key_path)
        if name is None:
            raise
    directory = cert_path.parent
    with _shared_lock(directory, lock_timeout):
        try:
            return _validate_pair(cert_path, key_path, role,
                                  allow_expired=allow_expired, now=now)
        except _AccessDenied:
            raise
        except IdentityError as exc:
            pending = _find_half_promotion(directory, name)
            if pending is None:
                raise
            raise _half_promotion_error(exc, directory, *pending) from None


def _layout_name(cert_path: Path, key_path: Path) -> Optional[str]:
    """The identity name if `(cert_path, key_path)` is a generated pair's layout."""
    if cert_path.parent != key_path.parent:
        return None
    for name in _NAME_ROLES:
        if identity_paths(cert_path.parent, name) == (cert_path, key_path):
            return name
    return None


def _validate_pair(
    cert_path: Path,
    key_path: Path,
    role: str,
    *,
    allow_expired: bool = False,
    now: Optional[datetime] = None,
) -> Identity:
    """`load_identity` without the locked re-check: what this module uses
    internally, including while it holds the identity lock itself."""
    key = _load_private_key(key_path)
    cert = _load_cert_file(cert_path)

    if _spki(key.public_key()) != _spki(cert.public_key()):
        raise IdentityError(
            f"certificate {cert_path} does not match private key {key_path}",
            cert_path,
        )

    moment = _now(now)
    not_after = cert.not_valid_after_utc
    if moment > not_after:
        if not allow_expired:
            raise IdentityError(
                f"certificate {cert_path} expired on {not_after.isoformat()}",
                cert_path,
            )
        logger.warning(
            "%s certificate %s expired on %s (clients ignore the server certificate's "
            "expiry, so it keeps working)", role, cert_path, not_after.isoformat(),
        )
    if cert.not_valid_before_utc > moment + _CERT_BACKDATE:
        raise IdentityError(
            f"certificate {cert_path} is not valid until "
            f"{cert.not_valid_before_utc.isoformat()} (check the system clock)",
            cert_path,
        )

    eku = _eku(cert)
    if eku is not None and _ROLE_EKU[role] not in eku:
        wanted = "serverAuth" if role == ROLE_SERVER else "clientAuth"
        raise IdentityError(
            f"certificate {cert_path} has an extended key usage without {wanted}, "
            f"so it cannot be used as a TLS {role} certificate",
            cert_path,
        )

    return Identity(
        cert_path=cert_path,
        key_path=key_path,
        cert_pem=_cert_pem(cert),
        fingerprint=_cert_fingerprint(cert),
        not_after=not_after,
        role=role,
    )


def peek_identity(directory: Union[str, Path], name: str) -> Optional[CertInfo]:
    """Describe the certificate of identity `name` without touching its key.

    For readers that need only the fingerprint (e.g. `trust token create` run by
    a group operator who cannot read the root-owned `server.key`, or
    `identity show`). Returns `None` when the certificate does not exist;
    raises `IdentityError` when it exists but cannot be read or parsed. Never
    creates or takes a lock. Does *not* validate the pair -- use
    `load_identity` for that.
    """
    cert_path, _ = identity_paths(directory, name)
    try:
        cert = _load_cert_file(cert_path)
    except IdentityError:
        if not os.path.lexists(cert_path):
            return None
        raise
    return CertInfo(
        fingerprint=_cert_fingerprint(cert),
        not_after=cert.not_valid_after_utc,
        subject=cert.subject.rfc4514_string(),
    )


#: A client certificate that expires within this many days is reported by
#: `client_cert_expiry_problem` (the daemon's start/reload check and the
#: remote worker's daily check).
CLIENT_CERT_EXPIRY_WARN_DAYS = 30


def client_cert_not_after(cert_path: Union[str, Path]) -> datetime:
    """The `not_after` (timezone-aware UTC) of the first certificate in
    `cert_path`, without touching any key, so an expired certificate -- which
    `load_identity` refuses -- can still be described. Raises `IdentityError`
    when the file is missing, unreadable or not PEM."""
    return _load_cert_file(Path(cert_path)).not_valid_after_utc


def client_cert_expiry_problem(
    cert_path: Union[str, Path], not_after: datetime, now: Optional[datetime] = None,
) -> Optional[Tuple[int, str]]:
    """`(logging level, message)` when the client certificate `cert_path`
    has expired (`logging.ERROR`) or expires within
    `CLIENT_CERT_EXPIRY_WARN_DAYS` (`logging.WARNING`), else `None`. Pure:
    nothing is read or logged. `now` defaults to the current time (naive
    values are taken as UTC)."""
    left = not_after - _now(now)
    when = f"{not_after:%Y-%m-%d %H:%M} UTC"
    if left < timedelta(0):
        return logging.ERROR, (
            f"the client certificate {cert_path} expired on {when}; remote backups "
            "fail until it is replaced (`identity rotate` needs a valid certificate: "
            "enroll again with a new token, `zfsbackup-config enroll DESTINATION ...`)"
        )
    if left < timedelta(days=CLIENT_CERT_EXPIRY_WARN_DAYS):
        return logging.WARNING, (
            f"the client certificate {cert_path} expires on {when} (in "
            f"{max(left.days, 0)} day(s)); rotate it: `zfsbackup-config identity rotate`"
        )
    return None


# --------------------------------------------------------------------------
# Generating identities
# --------------------------------------------------------------------------

def _common_name(role: str) -> str:
    """`zfsbackup <role> <host>`, at most 64 characters, no secrets."""
    try:
        host = socket.gethostname()
    except OSError:
        host = ""
    host = re.sub(r"[^A-Za-z0-9._-]", "-", host or "") or "host"
    prefix = f"zfsbackup {role} "
    return prefix + host[: 64 - len(prefix)]


def _generate(role: str) -> Tuple[bytes, bytes]:
    """A fresh ECDSA P-256 key and self-signed certificate (A3); -> (key PEM, cert PEM).

    No subjectAltName: nobody verifies a hostname against these certificates
    (peers pin fingerprints), so a SAN would only leak names.
    """
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.now(timezone.utc)
    name = x509.Name([
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "zfsbackup"),
        x509.NameAttribute(NameOID.COMMON_NAME, _common_name(role)),
    ])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - _CERT_BACKDATE)
        .not_valid_after(now + _CERT_VALIDITY)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, content_commitment=False,
                key_encipherment=False, data_encipherment=False, key_agreement=False,
                key_cert_sign=False, crl_sign=False, encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage([_ROLE_EKU[role]]), critical=False)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False
        )
        .sign(key, hashes.SHA256())
    )
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return key_pem, cert.public_bytes(serialization.Encoding.PEM)


def _fsync_dir(dir_fd: int, directory: Path) -> None:
    try:
        os.fsync(dir_fd)
    except OSError as exc:  # some filesystems refuse fsync on a directory
        if exc.errno not in (errno.EINVAL, errno.ENOTSUP, errno.EBADF):
            raise IdentityError(
                f"cannot fsync directory {directory}: {exc.strerror}", directory
            ) from None


def _target_gid(directory: Path, group_of: Optional[Path]) -> Optional[int]:
    """The gid to give new TLS files: that of the DB file (or `group_of`).

    Root can chgrp to anything. A non-root owner can only chgrp to a group it
    belongs to, so this returns `None` (leave the default) otherwise.
    """
    ref = group_of
    if ref is None and directory.name.endswith(_TLS_DIR_SUFFIX):
        ref = directory.with_name(directory.name[: -len(_TLS_DIR_SUFFIX)])
    if ref is None:
        return None
    try:
        gid = os.stat(ref).st_gid
    except OSError:
        return None
    if os.geteuid() == 0 or gid == os.getegid() or gid in os.getgroups():
        return gid
    return None


def _fchown_group(fd: int, gid: Optional[int], path: Path) -> None:
    if gid is None:
        return
    try:
        if os.fstat(fd).st_gid != gid:
            os.fchown(fd, -1, gid)
    except OSError as exc:
        logger.warning("could not give %s group id %d: %s", path, gid, exc.strerror)


def _fsync_parent(directory: Path) -> None:
    """Make a new entry in `directory`'s parent durable (best effort: warns)."""
    parent = directory.parent
    try:
        fd = os.open(str(parent), os.O_RDONLY | _O_CLOEXEC | _O_DIRECTORY)
    except OSError as exc:
        logger.warning("cannot open %s to fsync it: %s", parent, exc.strerror)
        return
    try:
        _fsync_dir(fd, parent)
    except IdentityError as exc:
        logger.warning("%s", exc)
    finally:
        os.close(fd)


def _open_tls_dir(directory: Path, gid: Optional[int]) -> int:
    """Create `directory` (0750, DB group) if needed; -> an O_DIRECTORY fd.

    The parent must exist (it is the config DB's directory); after a mkdir it
    is fsynced, so the directory entry is as durable as the files written into
    it. A symlink, a non-directory or a world-writable directory is refused:
    anyone who can write it could swap the key and certificate.
    """
    created = False
    try:
        os.mkdir(str(directory), TLS_DIR_MODE)
        created = True
    except FileExistsError:
        pass
    except FileNotFoundError:
        raise IdentityError(
            f"cannot create TLS directory {directory}: {directory.parent} does not exist",
            directory,
        ) from None
    except PermissionError:
        raise IdentityError(
            f"cannot create TLS directory {directory}: permission denied on "
            f"{directory.parent} (run as root or as the config DB's owner)",
            directory,
        ) from None
    except OSError as exc:
        raise IdentityError(
            f"cannot create TLS directory {directory}: {exc.strerror}", directory
        ) from None
    try:
        fd = os.open(str(directory), os.O_RDONLY | _O_CLOEXEC | _O_NOFOLLOW | _O_DIRECTORY)
    except OSError as exc:
        what = (
            "is a symlink or not a directory"
            if exc.errno in (errno.ELOOP, errno.ENOTDIR) else exc.strerror
        )
        raise IdentityError(f"TLS directory {directory}: {what}", directory) from None
    try:
        st = os.fstat(fd)
        if not stat_module.S_ISDIR(st.st_mode):
            raise IdentityError(f"TLS directory {directory} is not a directory", directory)
        if created:
            _fchown_group(fd, gid, directory)
            # mkdir's mode is filtered by the umask; set it exactly.
            os.fchmod(fd, TLS_DIR_MODE)
            _fsync_parent(directory)
            logger.info("created TLS directory %s", directory)
        elif stat_module.S_IMODE(st.st_mode) & 0o002:
            raise IdentityError(
                f"TLS directory {directory} is writable by everyone (mode "
                f"{stat_module.S_IMODE(st.st_mode):04o}); refusing to use it. "
                f"Run: chmod {TLS_DIR_MODE:o} {directory}",
                directory,
            )
        return fd
    except BaseException:
        os.close(fd)
        raise


def _can_write_dir(directory: Path) -> bool:
    """Whether this process may create and rename files in `directory`."""
    kwargs = {"effective_ids": True} if os.access in os.supports_effective_ids else {}
    try:
        return os.access(str(directory), os.W_OK | os.X_OK, **kwargs)
    except OSError:
        return False


def _open_lock(directory: Path, gid: Optional[int], write: bool) -> Tuple[Optional[int], bool]:
    """Open `identity.lock`; -> `(fd, writable)`.

    With `write`, the file is created if absent -- `O_EXCL`, so this process
    knows it made it, and then gets exactly 0640 and the DB group whatever the
    umask: a group operator must be able to open it to take the shared lock.
    Otherwise (or when it cannot be opened for writing) an existing lock is
    opened read-only, which is all `flock` needs. `fd` is `None` when the file
    does not exist and is not to be (or cannot be) created.

    An existing lock file is opened `O_NONBLOCK` and must be a regular file
    (`_open_existing_lock`), exactly as `_read_file` does: a FIFO planted
    as `identity.lock` would otherwise block the open for ever, ignoring
    `lock_timeout` (which only bounds the `flock`) and SIGTERM. It is
    refused with `IdentityError`.
    """
    path = directory / IDENTITY_LOCK
    base = _O_CLOEXEC | _O_NOFOLLOW
    try:
        if write:
            try:
                fd = os.open(str(path), os.O_RDWR | os.O_CREAT | os.O_EXCL | base, _LOCK_MODE)
            except (FileExistsError, PermissionError):
                pass
            else:
                try:
                    _fchown_group(fd, gid, path)
                    os.fchmod(fd, _LOCK_MODE)
                except BaseException:
                    os.close(fd)
                    raise
                return fd, True
            try:
                return _open_existing_lock(path, os.O_RDWR | base), True
            except (PermissionError, FileNotFoundError):
                pass
        try:
            return _open_existing_lock(path, os.O_RDONLY | base), False
        except FileNotFoundError:
            return None, False
    except PermissionError:
        raise _AccessDenied(
            f"cannot open identity lock {path}: permission denied (run as root, as "
            f"the owner of {directory} or as a member of its group)",
            path,
        ) from None
    except OSError as exc:
        raise IdentityError(f"cannot open identity lock {path}: {exc.strerror}", path) from None


def _open_existing_lock(path: Path, flags: int) -> int:
    """`os.open(path, flags)` for an existing lock file, which must be a
    regular file. Opened `O_NONBLOCK` (a FIFO opens at once instead of
    waiting for a peer), checked with `fstat`, then switched back to
    blocking. `OSError` from the open propagates; a non-regular file is
    `IdentityError`."""
    fd = os.open(str(path), flags | _O_NONBLOCK)
    try:
        st = os.fstat(fd)
        if not stat_module.S_ISREG(st.st_mode):
            raise IdentityError(
                f"identity lock {path} is not a regular file (it is a "
                f"{_file_kind(st.st_mode)}); remove it, a regular file is created "
                "in its place",
                path,
            )
        if _O_NONBLOCK:
            os.set_blocking(fd, True)
    except BaseException:
        os.close(fd)
        raise
    return fd


def _flock(fd: int, path: Path, op: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            fcntl.flock(fd, op | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            pass
        except OSError as exc:
            if exc.errno not in (errno.EAGAIN, errno.EWOULDBLOCK, errno.EINTR):
                raise IdentityError(f"cannot lock {path}: {exc.strerror}", path) from None
        if time.monotonic() >= deadline:
            raise IdentityError(
                f"timed out after {timeout:g}s waiting for {path}; another "
                "process is creating or moving a TLS identity -- retry",
                path,
            )
        time.sleep(_LOCK_POLL)


def _acquire_lock(directory: Path, timeout: float,
                  gid: Optional[int] = None) -> Tuple[Optional[int], bool]:
    """Take the identity `flock`; -> `(fd, exclusive)` (close `fd` to release).

    A caller that can write `directory` takes it exclusively, creating the
    lock file if needed. Anyone else -- a DB-group operator, who can only read
    the directory -- takes it *shared* from a read-only descriptor: that waits
    out a writer, holds a writer up only for the caller's own re-check, and
    never lets a reader stall root's startup. `fd` is `None` (nothing locked)
    when the lock file does not exist and this caller cannot create it: no
    writer has ever run there.
    """
    write = _can_write_dir(directory)
    fd, writable = _open_lock(directory, gid, write)
    if fd is None:
        return None, False
    exclusive = write and writable
    try:
        _flock(fd, directory / IDENTITY_LOCK,
               fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH, timeout)
    except BaseException:
        os.close(fd)
        raise
    return fd, exclusive


@contextlib.contextmanager
def _shared_lock(directory: Path, timeout: float) -> Iterator[bool]:
    """Hold `<directory>/identity.lock` shared while reading, if it exists.

    Never creates anything. Yields whether a lock is held: a missing or
    unopenable lock file (an operator's own directory, a caller without
    access) just means an unlocked read.
    """
    try:
        fd, _ = _open_lock(directory, None, write=False)
    except IdentityError as exc:
        logger.debug("reading %s without the identity lock: %s", directory, exc)
        fd = None
    if fd is None:
        yield False
        return
    try:
        _flock(fd, directory / IDENTITY_LOCK, fcntl.LOCK_SH, timeout)
        yield True
    finally:
        os.close(fd)


def _tmp_name(final: str) -> str:
    return f".{final}.tmp"


def _unlink_quiet(path: Path) -> bool:
    """Unlink `path` if it exists; -> whether it did. Any other failure is an
    `IdentityError` with an operator message."""
    try:
        os.unlink(str(path))
        return True
    except FileNotFoundError:
        return False
    except PermissionError:
        raise IdentityError(
            f"cannot remove {path}: permission denied (run as root or as the owner "
            f"of {path.parent})",
            path,
        ) from None
    except OSError as exc:
        raise IdentityError(f"cannot remove {path}: {exc.strerror}", path) from None


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]


def _write_tmp(directory: Path, final: str, data: bytes, mode: int,
               gid: Optional[int]) -> Path:
    """Write `data` to `.<final>.tmp`: `O_EXCL` 0600, fsynced, then `mode`."""
    tmp = directory / _tmp_name(final)
    fd = os.open(
        str(tmp),
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_CLOEXEC | _O_NOFOLLOW,
        KEY_MODE,
    )
    try:
        os.fchmod(fd, KEY_MODE)  # the umask cannot loosen it, but be exact
        _fchown_group(fd, gid, tmp)
        _write_all(fd, data)
        os.fsync(fd)
        if mode != KEY_MODE:
            # Final mode set on the descriptor, before the rename, so the
            # visible file never has a wrong mode and no path-based chmod
            # can be redirected.
            os.fchmod(fd, mode)
    except BaseException:
        os.close(fd)
        with contextlib.suppress(IdentityError):
            _unlink_quiet(tmp)
        raise
    os.close(fd)
    return tmp


_INCOMPLETE = "incomplete"


def _left_alone(exc: IdentityError, directory: Path, name: str) -> IdentityError:
    """Wrap a refusal of an existing identity: we did not touch it, and why."""
    if isinstance(exc, _AccessDenied):
        # Not a defect of the identity: no advice to remove anything.
        return _AccessDenied(
            f"{exc} -- the {name} identity in {directory} was left alone", exc.path
        )
    cert_path, key_path = identity_paths(directory, name)
    return IdentityError(
        f"{exc} -- the existing {name} identity in {directory} is not usable and "
        "was left alone, because replacing it changes its fingerprint. Fix it, "
        f"or remove {cert_path.name} and {key_path.name} to generate a new one "
        "(peers will then need to re-enroll / re-pin)",
        exc.path,
    )


def _inspect(directory: Path, name: str, role: str) -> Union[Identity, str]:
    """-> a valid `Identity`, or `_INCOMPLETE` (missing/half/unparseable pair).

    A complete pair that parses but is unacceptable (unsafe key mode, mismatch,
    expired client certificate, wrong EKU), or a file that is not a regular
    file or cannot be read, raises `IdentityError`: replacing it would silently
    change a fingerprint that peers have pinned, so that is the operator's
    decision, not ours. Callers that hold the lock check for an interrupted
    promotion (`_find_half_promotion`) first: that state is mismatched too,
    but recoverable, and must never get the "remove both files" advice.
    """
    cert_path, key_path = identity_paths(directory, name)
    have_cert = os.path.lexists(cert_path)
    have_key = os.path.lexists(key_path)
    if not (have_cert and have_key):
        return _INCOMPLETE
    # Parse both first: an empty or garbage file is crash debris, not an identity.
    try:
        cert_data, _ = _read_file(cert_path, "certificate")
        x509.load_pem_x509_certificate(cert_data)
        key_data, _ = _read_file(key_path, "private key")
    except ValueError:
        return _INCOMPLETE
    except IdentityError as exc:
        # Vanished between lexists() and open(): a concurrent generator
        # unlinked an orphan certificate. Re-checked under the lock.
        if not (os.path.lexists(cert_path) and os.path.lexists(key_path)):
            return _INCOMPLETE
        raise _left_alone(exc, directory, name) from None
    try:
        serialization.load_pem_private_key(key_data, password=None)
    except TypeError:
        pass  # encrypted: a real key; _validate_pair explains the refusal
    except (ValueError, UnsupportedAlgorithm):
        return _INCOMPLETE
    try:
        return _validate_pair(cert_path, key_path, role,
                              allow_expired=(role == ROLE_SERVER))
    except IdentityError as exc:
        raise _left_alone(exc, directory, name) from None


# -- interrupted promotions -------------------------------------------------

def _promotion_pairs() -> List[Tuple[str, str]]:
    """Every `(source, target)` that `promote_identity` accepts."""
    return [(s, t) for s in _NAME_ROLES for t in _NAME_ROLES
            if s != t and _NAME_ROLES[s] == _NAME_ROLES[t]]


def _half_promoted(directory: Path, source: str, target: str) -> bool:
    """Whether a `promote_identity(source -> target)` stopped between its renames.

    The signature: the source key is gone, the target key is there, the source
    certificate is still there and certifies the target key, and the target
    certificate is missing, unreadable or for another key. Nothing else
    produces it: our generator never leaves a certificate without its key next
    to a key it certifies. Reads files only; changes nothing.
    """
    src_cert, src_key = identity_paths(directory, source)
    dst_cert, dst_key = identity_paths(directory, target)
    if (os.path.lexists(src_key) or not os.path.lexists(src_cert)
            or not os.path.lexists(dst_key)):
        return False
    try:
        key_data, _ = _read_file(dst_key, "private key")
        moved = _spki(_parse_private_key(key_data, dst_key).public_key())
        if _spki(_load_cert_file(src_cert).public_key()) != moved:
            return False
    except IdentityError:
        return False
    try:
        return _spki(_load_cert_file(dst_cert).public_key()) != moved
    except IdentityError:
        return True  # missing or unreadable target certificate


def _find_half_promotion(directory: Path, name: str) -> Optional[Tuple[str, str]]:
    """An interrupted promotion into or out of `name`: -> `(source, target)`."""
    for source, target in _promotion_pairs():
        if name in (source, target) and _half_promoted(directory, source, target):
            return source, target
    return None


def _half_promotion_error(cause: object, directory: Path, source: str,
                          target: str) -> IdentityError:
    src_cert, _ = identity_paths(directory, source)
    _, dst_key = identity_paths(directory, target)
    return IdentityError(
        f"{cause} -- a promotion of the {source} identity to {target} in "
        f"{directory} was interrupted between its two renames: {dst_key.name} is "
        f"already the key of {src_cert.name}, which peers may have pinned. Nothing "
        "is lost: run `zfsbackup-config identity rotate` again (or anything that "
        f"ensures the {target} identity, as root or as the owner of {directory}) "
        f"to finish it. Do not delete {dst_key.name} or {src_cert.name}.",
        src_cert,
    )


def _finish_promotion(directory: Path, dir_fd: int, source: str, target: str) -> None:
    """Roll an interrupted promotion forward: the certificate follows its key.

    The caller holds the identity lock exclusively and has just seen
    `_half_promoted(directory, source, target)`.
    """
    src_cert, _ = identity_paths(directory, source)
    dst_cert, _ = identity_paths(directory, target)
    try:
        os.rename(str(src_cert), str(dst_cert))
    except OSError as exc:
        raise _half_promotion_error(
            f"cannot rename {src_cert} to {dst_cert}: {exc.strerror}",
            directory, source, target,
        ) from None
    _fsync_dir(dir_fd, directory)
    logger.warning(
        "finished an interrupted promotion of the %s identity to %s in %s",
        source, target, directory,
    )


# -- the public writers ------------------------------------------------------

def ensure_identity(
    directory: Union[str, Path],
    role: str,
    *,
    name: Optional[str] = None,
    group_of: Optional[Union[str, Path]] = None,
    lock_timeout: float = _DEFAULT_LOCK_TIMEOUT,
) -> Identity:
    """Return the identity for `role` in `directory`, generating it if needed.

    `name` defaults to the role (`server`/`client`); pass `CLIENT_NEXT_NAME`
    for the rotation successor. `group_of` is the file whose group the
    directory and files get (default: the DB file `<directory minus .tls>`);
    root always applies it, a non-root caller only when it is a member.

    * The directory is created 0750 if absent (its parent must exist, and is
      fsynced).
    * A valid existing pair is returned without taking the lock. Validating it
      reads the private key (0600), so this needs root or the key's owner: a
      DB-group operator gets an `IdentityError` (permission denied) here for
      every role. For the fingerprint alone, use `peek_identity`, which never
      reads the key.
    * Anything else is re-checked under the `identity.lock` flock. A caller
      that can write the directory takes it exclusively and then: a promotion
      into or out of `name` that was interrupted between its two renames is
      rolled forward (see `promote_identity`); stale temporaries are removed;
      the state is re-checked (another process may have just made it); any
      existing certificate is unlinked and the directory fsynced before a new
      key can appear; a fresh pair is written to `O_EXCL` 0600 temporaries,
      fsynced, and renamed key first, then certificate (0640), with a
      directory fsync after each. An orphan key or certificate or an
      unparseable file is replaced. Two concurrent callers therefore end up
      with one matching pair.
    * A caller that cannot write the directory only takes the lock *shared*:
      it waits out a writer and re-checks, but never generates or repairs
      anything (`IdentityError` instead).
    * A complete pair that is unacceptable is NOT replaced: `IdentityError`.
      An expired *server* certificate is accepted with a warning (clients
      ignore its expiry; replacing it would orphan every pin).
    """
    _check_role(role)
    name = role if name is None else name
    if _role_of(name) != role:
        raise ValueError(f"identity {name!r} is a {_role_of(name)} identity, not {role}")
    directory = Path(directory)
    gid = _target_gid(directory, Path(group_of) if group_of is not None else None)

    dir_fd = _open_tls_dir(directory, gid)
    try:
        try:
            found = _inspect(directory, name, role)
        except IdentityError:
            # Possibly a writer in the middle of a move: only the locked
            # re-check below may call the pair broken.
            found = _INCOMPLETE
        if isinstance(found, Identity):
            return found

        lock_fd, exclusive = _acquire_lock(directory, lock_timeout, gid)
        try:
            return _ensure_locked(directory, dir_fd, name, role, gid, exclusive)
        finally:
            if lock_fd is not None:
                os.close(lock_fd)  # releases the flock
    finally:
        os.close(dir_fd)


def _ensure_locked(directory: Path, dir_fd: int, name: str, role: str,
                   gid: Optional[int], exclusive: bool) -> Identity:
    cert_path, key_path = identity_paths(directory, name)
    pending = _find_half_promotion(directory, name)
    if pending is not None:
        if not exclusive:
            raise _half_promotion_error(
                f"cannot finish it from this process: it cannot write {directory}",
                directory, *pending,
            )
        _finish_promotion(directory, dir_fd, *pending)

    if exclusive:
        # Only exclusive lock holders write here, so any temporary is a crash
        # leftover.
        for final in (key_path.name, cert_path.name):
            if _unlink_quiet(directory / _tmp_name(final)):
                logger.warning("removed stale temporary %s", directory / _tmp_name(final))

    found = _inspect(directory, name, role)
    if isinstance(found, Identity):
        return found  # valid, or someone else generated it while we waited
    if not exclusive:
        raise IdentityError(
            f"there is no usable {name} identity in {directory}, and this process "
            f"cannot create one: it cannot write {directory} (run as root or as "
            f"the owner of {directory})",
            directory,
        )

    if os.path.lexists(cert_path) or os.path.lexists(key_path):
        logger.warning(
            "the %s identity in %s is incomplete (crash leftover?); generating "
            "a new one", name, directory,
        )
    if os.path.lexists(cert_path):
        # Whatever state the key is in (missing, empty, garbage): remove the
        # old certificate, durably, before a new key can appear, so a crash
        # between the two renames below can never leave new key + old
        # certificate (a complete but mismatched pair).
        _unlink_quiet(cert_path)
        _fsync_dir(dir_fd, directory)

    key_pem, cert_pem = _generate(role)
    try:
        key_tmp = _write_tmp(directory, key_path.name, key_pem, KEY_MODE, gid)
        cert_tmp = _write_tmp(directory, cert_path.name, cert_pem, CERT_MODE, gid)
        os.rename(str(key_tmp), str(key_path))
        _fsync_dir(dir_fd, directory)
        os.rename(str(cert_tmp), str(cert_path))
    except PermissionError:
        raise IdentityError(
            f"cannot write the {name} identity in {directory}: permission "
            f"denied (run as root or as the owner of {directory})",
            directory,
        ) from None
    except OSError as exc:
        raise IdentityError(
            f"cannot write the {name} identity in {directory}: {exc.strerror}",
            directory,
        ) from None
    _fsync_dir(dir_fd, directory)

    ident = _validate_pair(cert_path, key_path, role)
    logger.info(
        "generated a new %s TLS identity %s (fingerprint %s)",
        role, cert_path, ident.fingerprint,
    )
    return ident


def promote_identity(
    directory: Union[str, Path],
    source: str = CLIENT_NEXT_NAME,
    target: str = CLIENT_NAME,
    *,
    lock_timeout: float = _DEFAULT_LOCK_TIMEOUT,
) -> Identity:
    """Move identity `source` over `target` (M14c: `client.next` -> `client`).

    Under the exclusive identity lock: the source pair is validated, the key
    is renamed over the target key and the directory fsynced, the source
    certificate is checked against the moved key, renamed over the target
    certificate, and the directory fsynced again.

    Two files cannot be swapped atomically: between the renames the target is
    new key + old certificate. That window is never reported as damage:
    readers that land in it re-check under the shared lock (`load_identity`,
    `ensure_identity`) and so get the finished pair. If the process dies
    there, the state is recognised -- source certificate present without its
    key, certifying the target key -- and rolled forward by the next
    `ensure_identity` (of either name), `complete_promotion`, or a re-run of
    this function. Returns the promoted identity. Needs write access to the
    directory.
    """
    role = _role_of(source)
    if _role_of(target) != role or source == target:
        raise ValueError(f"cannot promote {source!r} to {target!r}")
    directory = Path(directory)
    src_cert, src_key = identity_paths(directory, source)
    dst_cert, dst_key = identity_paths(directory, target)
    gid = _target_gid(directory, None)

    dir_fd = _open_tls_dir(directory, gid)
    try:
        lock_fd, exclusive = _acquire_lock(directory, lock_timeout, gid)
        try:
            if not exclusive:
                raise IdentityError(
                    f"cannot promote {source} to {target} in {directory}: permission "
                    f"denied (run as root or as the owner of {directory})",
                    directory,
                )
            if _half_promoted(directory, source, target):
                _finish_promotion(directory, dir_fd, source, target)
            else:
                if os.path.lexists(src_key):
                    _validate_pair(src_cert, src_key, role)
                    os.rename(str(src_key), str(dst_key))
                    _fsync_dir(dir_fd, directory)
                elif not os.path.lexists(src_cert):
                    raise IdentityError(
                        f"there is no {source} identity in {directory} to promote",
                        directory,
                    )
                # The key is in place: the source certificate must match it
                # before it may follow.
                _validate_pair(src_cert, dst_key, role)
                os.rename(str(src_cert), str(dst_cert))
                _fsync_dir(dir_fd, directory)
            ident = _validate_pair(dst_cert, dst_key, role)
            logger.info(
                "promoted the %s identity to %s (fingerprint %s)",
                source, target, ident.fingerprint,
            )
            return ident
        except OSError as exc:
            raise IdentityError(
                f"cannot promote {source} to {target} in {directory}: {exc.strerror}",
                directory,
            ) from None
        finally:
            if lock_fd is not None:
                os.close(lock_fd)
    finally:
        os.close(dir_fd)


def complete_promotion(
    directory: Union[str, Path],
    *,
    lock_timeout: float = _DEFAULT_LOCK_TIMEOUT,
) -> Optional[Identity]:
    """Finish a `promote_identity` that was interrupted between its two renames.

    Returns the promoted (target) identity when there was one to finish, and
    `None` when there was nothing to do -- then nothing was created or changed,
    not even the directory or its lock file. Meant for a re-run of `identity
    rotate` (M14c), before it decides whether to reuse `client.next`: a
    returned identity means the earlier run's swap is now complete. Raises
    `IdentityError` if there is something to finish and this process cannot
    write the directory.
    """
    directory = Path(directory)
    pairs = _promotion_pairs()
    if not os.path.lexists(directory) or not any(
            _half_promoted(directory, s, t) for s, t in pairs):
        return None
    gid = _target_gid(directory, None)
    dir_fd = _open_tls_dir(directory, gid)
    try:
        lock_fd, exclusive = _acquire_lock(directory, lock_timeout, gid)
        try:
            for source, target in pairs:
                if not _half_promoted(directory, source, target):
                    continue
                if not exclusive:
                    raise _half_promotion_error(
                        f"cannot finish it from this process: it cannot write {directory}",
                        directory, source, target,
                    )
                _finish_promotion(directory, dir_fd, source, target)
                cert, key = identity_paths(directory, target)
                role = _NAME_ROLES[target]
                return _validate_pair(cert, key, role, allow_expired=(role == ROLE_SERVER))
            return None  # a live promoter finished it while we waited
        finally:
            if lock_fd is not None:
                os.close(lock_fd)
    finally:
        os.close(dir_fd)


#: The identities `discard_identity` may remove. The live `client` and `server`
#: identities are deliberately absent: they are what peers have pinned, and
#: replacing them is `identity rotate` / an explicit operator decision, never a
#: side effect of a helper meant for the unpublished rotation successor.
_DISCARDABLE = frozenset({CLIENT_NEXT_NAME})


def discard_identity(
    directory: Union[str, Path],
    name: str,
    *,
    expected_fingerprint: Optional[str] = None,
    lock_timeout: float = _DEFAULT_LOCK_TIMEOUT,
) -> bool:
    """Remove the identity `name` (only `client.next`) from `directory`, under the lock.

    Takes the exclusive `identity.lock` like `ensure_identity` and
    `promote_identity`, so it cannot race either of them (or another
    `discard_identity`): the caller must be able to write `directory`
    (`IdentityError` otherwise). Nothing is created -- a missing directory is
    "nothing to discard" -- apart from the lock file itself.

    * `name` must be in the allow-list `{CLIENT_NEXT_NAME}`; every other name
      -- notably the live `client` and `server` -- raises `IdentityError`,
      whether or not it exists. The check happens before any I/O.
    * An interrupted promotion into or out of `name` (e.g. `client.next.crt`
      without `client.next.key` while `client.key` is its key) is promotion
      state, not debris: it raises `IdentityError` and removes nothing. This
      helper does not finish it itself, because rolling forward changes the
      live identity, which a "discard" caller never asked for, and it cannot
      call `complete_promotion` anyway (that takes the same lock on another
      descriptor). The caller decides: run `complete_promotion(directory)`
      first, then discard again if `client.next` still exists.
    * With `expected_fingerprint` (any form `normalize_fingerprint` accepts;
      `ValueError` if malformed), the on-disk certificate must still have
      that fingerprint; if it differs, is missing or unreadable, nothing at
      all is changed and the result is `False` -- another process may have
      regenerated the pair since the caller looked.
    * Removal order: the certificate, directory fsync, the key, then the
      matching `.<name>.crt.tmp` / `.<name>.key.tmp` leftovers, directory
      fsync. A crash after the certificate is gone leaves an orphan key, which
      `ensure_identity` already replaces as debris (it is never mistaken for
      promotion state: that needs the source *certificate* present).

    Returns whether anything was removed. Not removing anything (no files, or
    a fingerprint mismatch) is not an error.
    """
    if name not in _DISCARDABLE:
        raise IdentityError(
            f"refusing to discard the {name!r} identity: only "
            f"{sorted(_DISCARDABLE)} may be discarded (the live identities are "
            "replaced by `identity rotate`, never deleted by this helper)",
            Path(directory),
        )
    expected = None if expected_fingerprint is None else normalize_fingerprint(
        expected_fingerprint)
    directory = Path(directory)
    if not os.path.lexists(directory):
        return False
    cert_path, key_path = identity_paths(directory, name)
    gid = _target_gid(directory, None)

    dir_fd = _open_tls_dir(directory, gid)
    try:
        lock_fd, exclusive = _acquire_lock(directory, lock_timeout, gid)
        try:
            if not exclusive:
                raise IdentityError(
                    f"cannot discard the {name} identity in {directory}: permission "
                    f"denied (run as root or as the owner of {directory})",
                    directory,
                )
            pending = _find_half_promotion(directory, name)
            if pending is not None:
                source, target = pending
                raise IdentityError(
                    f"refusing to discard the {name} identity in {directory}: a "
                    f"promotion of {source} to {target} was interrupted between its "
                    f"two renames, and what is left is that promotion's state, not "
                    f"debris. Finish it first (zfsbackup.identity.complete_promotion, "
                    f"or `zfsbackup-config identity rotate`), then discard again.",
                    directory,
                )
            if expected is not None:
                try:
                    current = _cert_fingerprint(_load_cert_file(cert_path))
                except IdentityError:
                    return False  # missing or unreadable: not the pair we meant
                if current != expected:
                    return False

            removed = False
            # Certificate first, durably: a crash then leaves an orphan key.
            if _unlink_quiet(cert_path):
                removed = True
                _fsync_dir(dir_fd, directory)
            removed |= _unlink_quiet(key_path)
            for final in (key_path.name, cert_path.name):
                removed |= _unlink_quiet(directory / _tmp_name(final))
            if removed:
                _fsync_dir(dir_fd, directory)
                logger.info("discarded the %s identity in %s", name, directory)
            return removed
        finally:
            if lock_fd is not None:
                os.close(lock_fd)
    finally:
        os.close(dir_fd)


# --------------------------------------------------------------------------
# A consistent copy of the TLS directory (backups)
# --------------------------------------------------------------------------

#: What `copy_tls_dir` copies, key files marked. An explicit list, not a glob:
#: `identity.lock` and the writers' `.<name>.tmp` files are not identities.
_COPIED_FILES = (
    (SERVER_CRT, False), (SERVER_KEY, True),
    (CLIENT_CRT, False), (CLIENT_KEY, True),
    (CLIENT_NEXT_CRT, False), (CLIENT_NEXT_KEY, True),
)


class KeyUnreadable(PermissionError):
    """`copy_tls_dir` may not read a private key (they are 0600 and owned by
    the daemon's user, so a group operator cannot). `filename` is the key.
    Nothing has been created at the destination when this is raised; copy
    the certificates only with `include_keys=False`."""

    def __init__(self, path: Union[str, Path]):
        super().__init__(errno.EACCES, "permission denied reading a TLS private key", str(path))


def copy_tls_dir(
    src: Union[str, Path],
    dst: Union[str, Path],
    *,
    include_keys: bool = True,
    lock_timeout: float = _DEFAULT_LOCK_TIMEOUT,
) -> List[str]:
    """Copy the identities in the TLS directory `src` (`server.*`, `client.*`,
    `client.next.*`) to `dst`, as one consistent snapshot; -> the names copied.

    The files are read while holding `src/identity.lock` shared (no writer runs
    meanwhile), so a certificate and its key are never read from either side
    of a rotation or promotion. The lock is released before anything is
    written. The lock is opened read-only (never created) and taken shared, so it
    does not hold up the daemon's own readers. The identity lock file itself is
    not copied; if `src` has none, the files are read unlocked and read again
    under the lock if one has appeared meanwhile.

    `dst` is created (0700; its parent must exist) or may be an existing
    directory (set to 0700); an existing file is never overwritten
    (`IdentityError`). Keys are written 0600, certificates 0640. On failure
    whatever this call created is removed again.

    `include_keys=False` copies certificates only and never opens a key. A key
    this process may not read raises `KeyUnreadable` (a `PermissionError`)
    before anything is created. A missing `src` or any other failure is
    `IdentityError`. Only the stdlib and `cryptography` are involved; the
    contents are copied verbatim and not parsed.
    """
    src_dir, dst_dir = Path(src), Path(dst)
    try:
        src_fd = os.open(str(src_dir), os.O_RDONLY | _O_CLOEXEC | _O_NOFOLLOW | _O_DIRECTORY)
    except FileNotFoundError:
        raise IdentityError(f"TLS directory {src_dir} does not exist", src_dir) from None
    except OSError as exc:
        what = (
            "is a symlink or not a directory"
            if exc.errno in (errno.ELOOP, errno.ENOTDIR) else exc.strerror
        )
        raise IdentityError(f"cannot read TLS directory {src_dir}: {what}", src_dir) from None
    try:
        def read_all() -> List[Tuple[str, bytes, bool]]:
            found: List[Tuple[str, bytes, bool]] = []
            for name, is_key in _COPIED_FILES:
                if is_key and not include_keys:
                    continue
                path = src_dir / name
                if not os.path.lexists(path):
                    continue
                try:
                    data, _ = _read_file(path, "private key" if is_key else "certificate")
                except _AccessDenied as exc:
                    if is_key:
                        raise KeyUnreadable(path) from None
                    raise IdentityError(str(exc), exc.path) from None
                found.append((name, data, is_key))
            return found

        lock_path = src_dir / IDENTITY_LOCK

        def read_locked() -> Optional[List[Tuple[str, bytes, bool]]]:
            """The files read under a SHARED lock (a writer holds it
            exclusively while it moves anything, so this excludes every
            writer; it never blocks the daemon's own shared readers). The
            lock is opened read-only, so it is never created here. `None` if
            there is no lock file."""
            lock_fd, _ = _open_lock(src_dir, None, write=False)
            if lock_fd is None:
                return None
            try:
                _flock(lock_fd, lock_path, fcntl.LOCK_SH, lock_timeout)
                return read_all()
            finally:
                os.close(lock_fd)

        blobs = read_locked()
        if blobs is None:
            # No lock file yet: no writer has ever run here, or one is about
            # to. Every writer creates the lock BEFORE it touches a file, so
            # a lock still absent after an unlocked read proves that no
            # writer overlapped it; one that appeared meanwhile may have, and
            # the read is redone under that lock.
            blobs = read_all()
            if os.path.lexists(lock_path):
                blobs = read_locked()
                if blobs is None:  # removed again in between: nothing to lock
                    blobs = read_all()
    finally:
        os.close(src_fd)

    created_dir = False
    try:
        os.mkdir(str(dst_dir), 0o700)
        created_dir = True
    except FileExistsError:
        pass
    except OSError as exc:
        raise IdentityError(
            f"cannot create {dst_dir}: {exc.strerror}", dst_dir
        ) from None
    try:
        dst_fd = os.open(str(dst_dir), os.O_RDONLY | _O_CLOEXEC | _O_NOFOLLOW | _O_DIRECTORY)
    except OSError as exc:
        if created_dir:
            with contextlib.suppress(OSError):
                os.rmdir(str(dst_dir))
        raise IdentityError(
            f"cannot use {dst_dir} as the destination: {exc.strerror}", dst_dir
        ) from None
    written: List[str] = []
    try:
        os.fchmod(dst_fd, 0o700)  # exactly, whatever the umask
        for name, data, is_key in blobs:
            mode = KEY_MODE if is_key else CERT_MODE
            try:
                fd = os.open(
                    name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_CLOEXEC | _O_NOFOLLOW,
                    mode, dir_fd=dst_fd,
                )
            except FileExistsError:
                raise IdentityError(
                    f"{dst_dir / name} already exists; not overwriting it", dst_dir / name
                ) from None
            except OSError as exc:
                raise IdentityError(
                    f"cannot create {dst_dir / name}: {exc.strerror}", dst_dir / name
                ) from None
            written.append(name)
            try:
                os.fchmod(fd, mode)  # exactly, whatever the umask
                _write_all(fd, data)
                os.fsync(fd)
            except OSError as exc:
                raise IdentityError(
                    f"cannot write {dst_dir / name}: {exc.strerror}", dst_dir / name
                ) from None
            finally:
                os.close(fd)
        _fsync_dir(dst_fd, dst_dir)
    except BaseException:
        for name in written:
            with contextlib.suppress(OSError):
                os.unlink(name, dir_fd=dst_fd)
        if created_dir:
            with contextlib.suppress(OSError):
                os.rmdir(str(dst_dir))
        raise
    finally:
        os.close(dst_fd)
    return [name for name, _, _ in blobs]


# --------------------------------------------------------------------------
# TLS contexts and pinned connections
# --------------------------------------------------------------------------

def _load_chain(ctx: ssl.SSLContext, identity: Identity,
                lock_timeout: float = _DEFAULT_LOCK_TIMEOUT) -> None:
    """Give `ctx` exactly the certificate that was validated as `identity`.

    The files are read once, the leaf's fingerprint must still be
    `identity.fingerprint` and the key must still match it, and *those
    bytes* are what `load_cert_chain` reads -- through `_blob_paths`, not by
    opening the paths again. So a context can never present a certificate
    other than the one its `Identity` names.

    When the paths are a generated identity's layout (`<dir>/<name>.crt` and
    `.key`, as for `load_identity`) the read happens under the shared
    identity lock, waiting up to `lock_timeout` seconds for a writer, so no
    writer is mid-move; operator-supplied paths elsewhere are read without
    locking their directory.

    Raises `IdentityChanged` when the files hold another identity (a
    different certificate, or a key that no longer matches it), and plain
    `IdentityError` for everything else (lock timeout, unreadable file,
    unparseable PEM, `ssl` refusing the pair).
    """
    cert_path, key_path = identity.cert_path, identity.key_path
    if _layout_name(cert_path, key_path) is not None:
        lock = _shared_lock(cert_path.parent, lock_timeout)
    else:
        lock = contextlib.nullcontext(False)
    with lock:
        cert_data, _ = _read_file(cert_path, "certificate")
        key_data, key_st = _read_file(key_path, "private key")
    _check_key_mode(key_path, key_st, warn=False)
    key = _parse_private_key(key_data, key_path)
    try:
        cert = x509.load_pem_x509_certificate(cert_data)
    except ValueError:
        raise IdentityError(f"{cert_path} does not contain a PEM certificate", cert_path) from None
    actual = _cert_fingerprint(cert)
    if actual != identity.fingerprint:
        raise IdentityChanged(
            f"certificate {cert_path} changed since the identity was loaded "
            f"(fingerprint {actual}, expected {identity.fingerprint}); load the "
            "identity again",
            cert_path,
            expected=identity.fingerprint,
            actual=actual,
        )
    if _spki(key.public_key()) != _spki(cert.public_key()):
        raise IdentityChanged(
            f"private key {key_path} no longer matches certificate {cert_path} "
            "(did it change since it was validated?)",
            key_path,
            expected=identity.fingerprint,
            actual=None,
        )
    try:
        with _blob_paths(cert_data, key_data) as (cert_file, key_file):
            ctx.load_cert_chain(cert_file, key_file)
    except (ssl.SSLError, OSError) as exc:
        raise IdentityError(
            f"cannot load TLS identity {cert_path} / {key_path}: {exc}", cert_path
        ) from None


@contextlib.contextmanager
def _blob_paths(*blobs: bytes) -> Iterator[List[str]]:
    """Expose in-memory `blobs` as paths, for `load_cert_chain` (paths only).

    No private key is written to a filesystem if it can be helped: an
    anonymous memory file (Linux `memfd_create`, opened via `/proc/self/fd`),
    else a pipe opened via `/dev/fd` (macOS/BSD, when the data fits the pipe
    buffer), else -- last resort -- 0600 files in a private 0700 temporary
    directory, removed on exit. Each path is good for one read, inside the
    `with` block only.
    """
    fds: List[int] = []
    tmpdir = None
    try:
        paths = _memfd_paths(blobs, fds)
        if paths is None:
            paths = _pipe_paths(blobs, fds)
        if paths is None:
            tmpdir = tempfile.mkdtemp(prefix="zfsbackup-tls-")
            paths = []
            for i, blob in enumerate(blobs):
                path = os.path.join(tmpdir, f"{i}.pem")
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_CLOEXEC
                             | _O_NOFOLLOW, KEY_MODE)
                try:
                    _write_all(fd, blob)
                finally:
                    os.close(fd)
                paths.append(path)
        yield paths
    finally:
        _close_fds(fds, 0)
        if tmpdir is not None:
            shutil.rmtree(tmpdir, ignore_errors=True)


def _close_fds(fds: List[int], start: int) -> None:
    for fd in fds[start:]:
        with contextlib.suppress(OSError):
            os.close(fd)
    del fds[start:]


def _memfd_paths(blobs: Tuple[bytes, ...], fds: List[int]) -> Optional[List[str]]:
    create = getattr(os, "memfd_create", None)
    if create is None or not os.path.isdir("/proc/self/fd"):
        return None
    start = len(fds)
    try:
        for blob in blobs:
            fd = create("zfsbackup-tls")  # MFD_CLOEXEC by default
            fds.append(fd)
            _write_all(fd, blob)
    except OSError:
        _close_fds(fds, start)
        return None
    return [f"/proc/self/fd/{fd}" for fd in fds[start:]]


def _pipe_paths(blobs: Tuple[bytes, ...], fds: List[int]) -> Optional[List[str]]:
    if not os.path.isdir("/dev/fd"):
        return None
    start = len(fds)
    try:
        for blob in blobs:
            r, w = os.pipe()
            fds.append(r)
            try:
                os.set_blocking(w, False)
                _write_all(w, blob)  # BlockingIOError: larger than the buffer
            finally:
                os.close(w)  # the reader sees EOF after the data
    except OSError:
        _close_fds(fds, start)
        return None
    return [f"/dev/fd/{fd}" for fd in fds[start:]]


def server_context(identity: Identity, pinned_pems: Iterable[str], *,
                   lock_timeout: float = _DEFAULT_LOCK_TIMEOUT) -> ssl.SSLContext:
    """The API server's context: TLS 1.3, client certificates optional, pins only.

    `CERT_OPTIONAL` + `VERIFY_X509_PARTIAL_CHAIN` with exactly the pinned leaf
    certificates as trust anchors: a pinned certificate passes (if within its
    validity window and EKU-compatible), an unpinned one fails the handshake,
    and a client with no certificate completes it (the app answers 401).
    `load_verify_locations` is called only when there is at least one pin.

    No TLS 1.3 session tickets are issued (`num_tickets = 0`): a resumed
    session would skip certificate verification against the *current* pin
    set, which the per-connection context swap (D6) relies on.

    The certificate presented is exactly the one validated as `identity`: the
    files are re-read once and must still hold `identity.fingerprint` (see
    `_load_chain`). Raises `IdentityChanged` if its files now hold another
    identity (load it again), `IdentityError` if they cannot be read right
    now -- including the shared identity lock not being granted within
    `lock_timeout` seconds -- and `ValueError` if a pinned PEM is unusable.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    ctx.verify_mode = ssl.CERT_OPTIONAL
    ctx.verify_flags |= ssl.VERIFY_X509_PARTIAL_CHAIN
    ctx.num_tickets = 0
    pems = [p if p.endswith("\n") else p + "\n" for p in pinned_pems]
    if pems:
        try:
            ctx.load_verify_locations(cadata="".join(pems))
        except (ssl.SSLError, ValueError) as exc:
            raise ValueError(f"cannot load the pinned client certificates: {exc}") from None
    _load_chain(ctx, identity, lock_timeout)
    return ctx


def client_context(identity: Optional[Identity] = None, *,
                   lock_timeout: float = _DEFAULT_LOCK_TIMEOUT) -> ssl.SSLContext:
    """A client context that trusts nothing: the server is pinned by fingerprint.

    TLS 1.3 minimum, `check_hostname=False`, `CERT_NONE`; presents `identity`
    as the client certificate when given -- exactly the certificate validated
    as `identity`, or `IdentityChanged` if its files changed since and
    `IdentityError` if they cannot be read now (as for `server_context`;
    `lock_timeout` likewise). Use only with `connect_pinned` or `pinned_session`,
    which do the fingerprint check -- on its own this context authenticates
    nobody.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    if identity is not None:
        _load_chain(ctx, identity, lock_timeout)
    return ctx


def _strip_brackets(host: str) -> str:
    if host.startswith("[") and host.endswith("]"):
        return host[1:-1]
    return host


def _is_ip_literal(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


def connect_pinned(
    host: str,
    port: int,
    ctx: ssl.SSLContext,
    expected_fp: str,
    timeout: Optional[float],
) -> ssl.SSLSocket:
    """TCP connect, TLS handshake, then check the server's fingerprint.

    Returns the connected `SSLSocket` (its timeout is `timeout`), ready to hand
    to `websocket.WebSocket().connect(url, socket=...)`. If the server's
    certificate is not `expected_fp`, the socket is closed and `PinMismatch` is
    raised before any application byte is sent. SNI is sent only for host
    names, not IP literals; `[v6]` brackets are accepted. Connection and
    handshake failures propagate as `OSError`/`ssl.SSLError`.

    Under TLS 1.3 a *client certificate* rejection is not seen here: the
    handshake completes and the server's alert arrives on the first read or
    write (see `is_tls_alert_rejection`).
    """
    expected = normalize_fingerprint(expected_fp)
    host = _strip_brackets(host)
    sni = None if _is_ip_literal(host) else host
    raw = socket.create_connection((host, port), timeout=timeout)
    try:
        tls = ctx.wrap_socket(raw, server_hostname=sni)
    except BaseException:
        raw.close()
        raise
    try:
        der = tls.getpeercert(binary_form=True)
        actual = fingerprint_der(der) if der else None
    except BaseException:
        tls.close()
        raise
    if actual is None or not hmac.compare_digest(actual, expected):
        tls.close()
        raise PinMismatch(expected, actual)
    return tls


def pinned_session(
    identity: Optional[Identity],
    expected_fp: str,
    timeout: Optional[float] = None,
    *,
    lock_timeout: float = _DEFAULT_LOCK_TIMEOUT,
) -> "requests.Session":
    """A `requests.Session` whose `https://` connections are pinned to `expected_fp`.

    The S7 recipe: an `HTTPAdapter` hands urllib3 our `client_context(identity)`
    and `assert_fingerprint=expected_fp`, and forces `verify=False`/`cert=None`
    on every request, so neither `session.verify`, a per-request `verify=`,
    `REQUESTS_CA_BUNDLE` nor certifi can turn `CERT_REQUIRED` back on (urllib3
    re-applies `cert_reqs` from `verify` on every connection). A wrong
    fingerprint raises `requests.exceptions.SSLError` before any HTTP request
    is sent (`is_pin_mismatch` recognises it).

    `trust_env` is off: no proxy, `.netrc` credentials or CA bundle is picked
    up silently from the environment. `http://` is refused outright (a
    redirect must not downgrade to plaintext). `timeout` is the default for
    requests that pass none. `lock_timeout` is `client_context`'s.
    """
    import requests
    from requests.adapters import BaseAdapter, HTTPAdapter

    fingerprint = normalize_fingerprint(expected_fp)
    ctx = client_context(identity, lock_timeout=lock_timeout)
    default_timeout = timeout

    class _PinnedAdapter(HTTPAdapter):
        def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
            pool_kwargs.update(ssl_context=ctx, assert_fingerprint=fingerprint)
            super().init_poolmanager(connections, maxsize, block, **pool_kwargs)

        def proxy_manager_for(self, proxy, **proxy_kwargs):
            # Only reachable if a caller sets `session.proxies` explicitly; keep
            # the pin through a CONNECT tunnel too.
            proxy_kwargs.update(ssl_context=ctx, assert_fingerprint=fingerprint)
            return super().proxy_manager_for(proxy, **proxy_kwargs)

        def build_connection_pool_key_attributes(self, request, verify, cert=None):
            return super().build_connection_pool_key_attributes(request, False, None)

        def cert_verify(self, conn, url, verify, cert):
            super().cert_verify(conn, url, False, None)

        def send(self, request, stream=False, timeout=None, verify=True, cert=None,
                 proxies=None):
            if timeout is None:
                timeout = default_timeout
            return super().send(request, stream=stream, timeout=timeout, verify=False,
                                cert=None, proxies=proxies)

    class _RefusePlaintext(BaseAdapter):
        def send(self, request, *args, **kwargs):
            raise requests.exceptions.InvalidURL(
                "plaintext http:// is not allowed on a pinned zfsbackup session",
                request=request,
            )

        def close(self):
            pass

    session = requests.Session()
    # No environment proxies / CA bundles / .netrc: a proxy picked up from
    # HTTPS_PROXY would silently re-route the pinned connection.
    session.trust_env = False
    session.mount("https://", _PinnedAdapter())
    session.mount("http://", _RefusePlaintext())
    return session


def _exception_chain(exc: BaseException) -> Iterator[BaseException]:
    """`exc` and everything it wraps: causes, contexts, `.reason`, exception args."""
    seen = set()
    stack = [exc]
    while stack:
        e = stack.pop()
        if e is None or id(e) in seen:
            continue
        seen.add(id(e))
        yield e
        stack.append(e.__cause__)
        stack.append(e.__context__)
        reason = getattr(e, "reason", None)
        if isinstance(reason, BaseException):
            stack.append(reason)
        stack.extend(a for a in getattr(e, "args", ()) if isinstance(a, BaseException))


_URLLIB3_PIN_TEXT = "Fingerprints did not match"


def is_pin_mismatch(exc: BaseException) -> bool:
    """Whether `exc` means "the server is not the one we pinned".

    True for `PinMismatch` and for `requests`/urllib3 errors raised by
    `pinned_session`'s fingerprint assertion, wherever they are wrapped.
    """
    for e in _exception_chain(exc):
        if isinstance(e, PinMismatch):
            return True
        if _URLLIB3_PIN_TEXT in str(e):
            return True
    return False


_ALERT_RE = re.compile(
    r"\b(?:TLS|TLSV1|SSLV3)_ALERT_"
    r"(?:UNKNOWN_CA|CERTIFICATE_EXPIRED|UNSUPPORTED_CERTIFICATE|BAD_CERTIFICATE)\b"
)


def is_tls_alert_rejection(exc: BaseException) -> bool:
    """Whether `exc` is the server rejecting *our client certificate* (S10).

    Under TLS 1.3 this arrives after the handshake, on the first read/write:
    a plain `ssl.SSLError` whose `reason` is `*_ALERT_UNKNOWN_CA` (not pinned),
    `*_ALERT_CERTIFICATE_EXPIRED`, `*_ALERT_UNSUPPORTED_CERTIFICATE` (EKU) or
    `*_ALERT_BAD_CERTIFICATE`, with a `TLS_`, `TLSV1_` or `SSLV3_` prefix
    depending on the OpenSSL version -- or a `requests` SSLError wrapping one.
    A local verification failure (`ssl.SSLCertVerificationError`) is not one.
    """
    for e in _exception_chain(exc):
        if isinstance(e, ssl.SSLCertVerificationError):
            continue
        if isinstance(e, ssl.SSLError):
            if _ALERT_RE.fullmatch(str(getattr(e, "reason", "") or "")):
                return True
        elif type(e).__module__.split(".", 1)[0] in ("requests", "urllib3"):
            # The underlying ssl.SSLError is sometimes only present as text.
            if _ALERT_RE.search(str(e)):
                return True
    return False


# --------------------------------------------------------------------------
# Enrollment tokens
# --------------------------------------------------------------------------

class SecretStr(str):
    """A `str` whose `repr()` is redacted. `str()` and formatting with `{}` /
    `%s` still give the value: the one place a token is meant to be printed."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<redacted>"


class SecretBytes(bytes):
    """`bytes` whose `repr()`/`str()` are redacted."""

    __slots__ = ()

    def __repr__(self) -> str:
        return f"<redacted {len(self)} bytes>"

    __str__ = __repr__


TOKEN_PREFIX = "zfsb1"
_SECRET_BYTES = 32
_SECRET_B64_LEN = 43  # base64url of 32 bytes, no padding
_TOKEN_RE = re.compile(
    rf"{TOKEN_PREFIX}\.([0-9a-f]{{64}})\.([A-Za-z0-9_-]{{{_SECRET_B64_LEN}}})"
)
_TOKEN_SHAPE = f"{TOKEN_PREFIX}.<64 lowercase hex>.<{_SECRET_B64_LEN} base64url characters>"


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def secret_hash(secret: bytes) -> str:
    """SHA-256 of a token secret, 64 lowercase hex (what the server stores)."""
    if not isinstance(secret, (bytes, bytearray)):
        raise TypeError("token secret must be bytes")
    return hashlib.sha256(bytes(secret)).hexdigest()


def make_token(server_fp: str) -> Tuple[SecretStr, str]:
    """A new enrollment token for the server `server_fp`; -> `(token, secret_sha256)`.

    `token` is `zfsb1.<server fp>.<base64url(32 random bytes), no padding>`, a
    `SecretStr`. Only the hash is stored server-side.
    """
    fp = normalize_fingerprint(server_fp)
    secret = secrets.token_bytes(_SECRET_BYTES)
    return SecretStr(f"{TOKEN_PREFIX}.{fp}.{_b64url(secret)}"), secret_hash(secret)


def parse_token(token: str) -> Tuple[str, SecretBytes]:
    """Split a token into `(server_fp, secret)`; strict.

    Leading/trailing whitespace (a newline from `--token-file`) is ignored;
    anything else that is not exactly `zfsb1.<64 lowercase hex>.<43 base64url>`
    decoding to 32 bytes raises `ValueError`. The message never contains the
    token or any part of the secret.
    """
    if not isinstance(token, str):
        raise ValueError("enrollment token must be text")
    text = token.strip()
    m = _TOKEN_RE.fullmatch(text)
    if m is None:
        if text and not text.startswith(TOKEN_PREFIX + "."):
            raise ValueError(
                f"this is not a zfsbackup enrollment token (expected {_TOKEN_SHAPE})"
            )
        raise ValueError(f"malformed enrollment token (expected {_TOKEN_SHAPE})")
    fp, encoded = m.group(1), m.group(2)
    try:
        secret = base64.urlsafe_b64decode(encoded + "=")
    except (binascii.Error, ValueError):
        raise ValueError(f"malformed enrollment token (expected {_TOKEN_SHAPE})") from None
    # Non-canonical encodings (stray low bits in the last character) are refused,
    # so one secret has exactly one spelling.
    if len(secret) != _SECRET_BYTES or _b64url(secret) != encoded:
        raise ValueError(f"malformed enrollment token (expected {_TOKEN_SHAPE})")
    return fp, SecretBytes(secret)


# --------------------------------------------------------------------------
# Proof of possession
# --------------------------------------------------------------------------

def _require_fp(label: str, value: object) -> str:
    if not _is_canonical_fingerprint(value):
        raise ValueError(f"{label} must be a canonical fingerprint (64 lowercase hex)")
    return value  # type: ignore[return-value]


def _require_client_id(value: object) -> str:
    problem = client_id_problem(value)
    if problem is not None:
        raise ValueError(problem)
    return value  # type: ignore[return-value]


def enrollment_payload(server_fp: str, client_id: str, client_fp: str,
                       secret_sha256: str) -> bytes:
    """The bytes a client signs with its new key to enroll (proof of possession).

    Binds the server (no relay to another server), the claimed client id, the
    certificate and the token. Every component is validated first, so none can
    inject a newline.
    """
    _require_fp("server fingerprint", server_fp)
    _require_client_id(client_id)
    _require_fp("client fingerprint", client_fp)
    _require_fp("token hash", secret_sha256)
    return (
        f"zfsbackup-enroll-v1\nserver={server_fp}\nclient_id={client_id}\n"
        f"client={client_fp}\ntoken={secret_sha256}\n"
    ).encode("ascii")


def rotation_payload(server_fp: str, client_id: str, old_fp: str, new_fp: str) -> bytes:
    """The bytes the *successor* key signs for `/rotate` (D4). Validated like
    `enrollment_payload`."""
    _require_fp("server fingerprint", server_fp)
    _require_client_id(client_id)
    _require_fp("old fingerprint", old_fp)
    _require_fp("new fingerprint", new_fp)
    return (
        f"zfsbackup-rotate-v1\nserver={server_fp}\nclient_id={client_id}\n"
        f"old={old_fp}\nnew={new_fp}\n"
    ).encode("ascii")


def sign_payload(key: Union[str, Path, PrivateKey], payload: bytes) -> bytes:
    """Sign `payload`: ECDSA-SHA256 (DER) for EC, PKCS#1 v1.5-SHA256 for RSA,
    raw Ed25519. `key` is a private key object or a key file path (loaded with
    the same permission checks as `load_identity`). Other key types raise
    `ValueError`."""
    if isinstance(key, (str, Path)):
        key = _load_private_key(Path(key))
    if isinstance(key, ec.EllipticCurvePrivateKey):
        return key.sign(payload, ec.ECDSA(hashes.SHA256()))
    if isinstance(key, rsa.RSAPrivateKey):
        return key.sign(payload, padding.PKCS1v15(), hashes.SHA256())
    if isinstance(key, ed25519.Ed25519PrivateKey):
        return key.sign(payload)
    raise ValueError(f"unsupported key type {type(key).__name__}")


def verify_payload(cert_pem: Union[str, bytes], payload: bytes, signature: bytes) -> bool:
    """Whether `signature` over `payload` was made by the key of `cert_pem`.

    False for a bad signature, an unparseable certificate or an unsupported key
    type -- never raises for hostile input.
    """
    try:
        public_key = _load_cert(cert_pem).public_key()
        if isinstance(public_key, ec.EllipticCurvePublicKey):
            public_key.verify(signature, payload, ec.ECDSA(hashes.SHA256()))
        elif isinstance(public_key, rsa.RSAPublicKey):
            public_key.verify(signature, payload, padding.PKCS1v15(), hashes.SHA256())
        elif isinstance(public_key, ed25519.Ed25519PublicKey):
            public_key.verify(signature, payload)
        else:
            return False
    except (InvalidSignature, ValueError, TypeError, UnsupportedAlgorithm,
            x509.InvalidVersion):
        return False
    return True


# --------------------------------------------------------------------------
# Client certificate acceptance policy
# --------------------------------------------------------------------------

#: Largest PEM `check_client_cert` accepts (a P-256 leaf is well under 1 KiB).
MAX_CLIENT_CERT_PEM = 16 * 1024
_CERT_SLACK = timedelta(minutes=5)
_PEM_CERT_RE = re.compile(
    r"\s*-----BEGIN CERTIFICATE-----\r?\n[A-Za-z0-9+/=\r\n]+-----END CERTIFICATE-----\s*"
)
_EC_CURVES = (ec.SECP256R1, ec.SECP384R1, ec.SECP521R1)
#: Netscape certificate type (`nsCertType`), a BIT STRING. OpenSSL reads its
#: first content octet; bit 0 is the MSB (0x80 SSL client ... 0x04 SSL CA,
#: 0x02 S/MIME CA, 0x01 object-signing CA -- OpenSSL's `NS_ANY_CA`).
_NS_CERT_TYPE_OID = x509.ObjectIdentifier("2.16.840.1.113730.1.1")
_NS_ANY_CA = 0x04 | 0x02 | 0x01


def _ns_cert_type_bits(der: bytes) -> Optional[int]:
    """The first octet of an `nsCertType` BIT STRING (DER), `0` when it has
    no bits, or `None` when `der` is not a well-formed BIT STRING."""
    if len(der) < 2 or der[0] != 0x03:
        return None
    length, offset = der[1], 2
    if length & 0x80:
        count = length & 0x7F
        if count == 0 or count > 4 or len(der) < 2 + count:
            return None
        length = int.from_bytes(der[2:2 + count], "big")
        offset = 2 + count
    if length < 1 or len(der) != offset + length:
        return None
    unused = der[offset]
    if unused > 7 or (length == 1 and unused != 0):
        return None
    return der[offset + 1] if length > 1 else 0


def check_client_cert(pem: Union[str, bytes], now: Optional[datetime] = None) -> CertInfo:
    """Decide whether a client certificate may be pinned; -> `CertInfo`.

    Accepts only a single PEM certificate (nothing else in the text -- in
    particular no private key), at most `MAX_CLIENT_CERT_PEM` bytes, X.509 v3,
    that:

    * is within its validity window (`not_before` may be up to 5 minutes in the
      future, for clock skew; `not_after` must not have passed);
    * has no EKU, or an EKU containing `clientAuth`;
    * is not a CA -- a pinned CA would let everything it signs pass the
      handshake. Every way OpenSSL's `check_ca()` can call a v3 certificate a
      CA is refused: basicConstraints `CA:TRUE`; keyUsage `keyCertSign`; and
      a Netscape certificate type (`nsCertType`, OID 2.16.840.1.113730.1.1)
      with any of its CA bits (SSL CA, S/MIME CA, object-signing CA), which
      OpenSSL honours when there is neither basicConstraints nor keyUsage. An
      `nsCertType` with CA bits is refused even alongside `CA:FALSE` (where
      OpenSSL would ignore it): such a certificate is contradictory, and a
      client leaf never needs it. An `nsCertType` without CA bits (old
      `openssl ca` / Easy-RSA 2 client profiles set `client, email`) is
      accepted; a malformed one is refused;
    * has, if keyUsage is present, `digitalSignature` (TLS 1.3 needs it);
    * has no critical extension this stack does not understand (OpenSSL would
      refuse it at the handshake);
    * has an RSA key of at least 2048 bits, an EC key on P-256/384/521, or an
      Ed25519 key.

    Raises `ValueError` with the reason. `now` defaults to the current time
    (naive values are taken as UTC).
    """
    if isinstance(pem, str):
        try:
            data = pem.encode("ascii")
        except UnicodeEncodeError:
            raise ValueError("certificate PEM must be ASCII") from None
    elif isinstance(pem, (bytes, bytearray)):
        data = bytes(pem)
    else:
        raise ValueError("certificate PEM must be text")
    if len(data) > MAX_CLIENT_CERT_PEM:
        raise ValueError(
            f"certificate PEM is {len(data)} bytes; the limit is {MAX_CLIENT_CERT_PEM}"
        )
    text = data.decode("ascii", "replace")
    if text.count("-----BEGIN ") != 1 or not _PEM_CERT_RE.fullmatch(text):
        raise ValueError("expected exactly one PEM certificate and nothing else")
    try:
        cert = x509.load_pem_x509_certificate(data)
    except x509.InvalidVersion:
        raise ValueError("certificate must be X.509 version 3") from None
    except ValueError as exc:
        raise ValueError(f"cannot parse the certificate ({exc})") from None
    if cert.version != x509.Version.v3:
        raise ValueError("certificate must be X.509 version 3")

    moment = _now(now)
    if cert.not_valid_before_utc > moment + _CERT_SLACK:
        raise ValueError(
            f"certificate is not valid until {cert.not_valid_before_utc.isoformat()}"
        )
    if moment > cert.not_valid_after_utc:
        raise ValueError(f"certificate expired on {cert.not_valid_after_utc.isoformat()}")

    try:
        extensions = cert.extensions
    except (ValueError, x509.DuplicateExtension, x509.UnsupportedGeneralNameType) as exc:
        raise ValueError(f"certificate has malformed extensions ({exc})") from None
    for ext in extensions:
        if ext.critical and isinstance(ext.value, x509.UnrecognizedExtension):
            raise ValueError(
                f"certificate has an unsupported critical extension {ext.oid.dotted_string}"
            )

    eku = _eku(cert)
    if eku is not None and ExtendedKeyUsageOID.CLIENT_AUTH not in eku:
        raise ValueError("certificate's extended key usage does not include clientAuth")
    try:
        bc = extensions.get_extension_for_class(x509.BasicConstraints).value
    except x509.ExtensionNotFound:
        bc = None
    if bc is not None and bc.ca:
        raise ValueError(
            "certificate is a CA certificate (basicConstraints CA:TRUE); pin the "
            "client's own leaf certificate instead"
        )
    try:
        ku = extensions.get_extension_for_class(x509.KeyUsage).value
    except x509.ExtensionNotFound:
        ku = None
    if ku is not None:
        if ku.key_cert_sign:
            raise ValueError(
                "certificate's key usage includes keyCertSign (a CA key); pin the "
                "client's own leaf certificate instead"
            )
        if not ku.digital_signature:
            raise ValueError("certificate's key usage does not include digitalSignature")
    try:
        ns_cert_type = extensions.get_extension_for_oid(_NS_CERT_TYPE_OID).value
    except x509.ExtensionNotFound:
        ns_cert_type = None
    if ns_cert_type is not None:
        bits = _ns_cert_type_bits(getattr(ns_cert_type, "value", b""))
        if bits is None:
            raise ValueError("certificate has a malformed Netscape certificate type (nsCertType)")
        if bits & _NS_ANY_CA:
            raise ValueError(
                "certificate's Netscape certificate type (nsCertType) marks it as a "
                "CA; pin the client's own leaf certificate instead"
            )

    try:
        public_key = cert.public_key()
    except (ValueError, UnsupportedAlgorithm):
        raise ValueError("certificate's public key type is not supported") from None
    if isinstance(public_key, rsa.RSAPublicKey):
        if public_key.key_size < 2048:
            raise ValueError(
                f"certificate's RSA key is {public_key.key_size} bits; at least 2048 required"
            )
    elif isinstance(public_key, ec.EllipticCurvePublicKey):
        if not isinstance(public_key.curve, _EC_CURVES):
            raise ValueError(
                f"certificate's EC key is on {public_key.curve.name}; only P-256, "
                "P-384 and P-521 are accepted"
            )
    elif not isinstance(public_key, ed25519.Ed25519PublicKey):
        raise ValueError(
            f"certificate's key type {type(public_key).__name__} is not accepted "
            "(RSA >= 2048, EC P-256/384/521 or Ed25519)"
        )

    return CertInfo(
        fingerprint=_cert_fingerprint(cert),
        not_after=cert.not_valid_after_utc,
        subject=cert.subject.rfc4514_string(),
    )

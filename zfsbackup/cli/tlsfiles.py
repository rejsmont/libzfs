"""Where this host's TLS files are, and what a certificate file says.

Shared by `identity show` (`trust.py`), `enroll` (plan M13) and `identity
rotate` (plan M14c), so every command resolves the client identity the same
way:

- `client_identity_files(config, resolved)` -> the client certificate and key
  paths: the configured `client_cert_file` / `client_key_file` (absolute paths,
  both or neither -- the model enforces that) when set, otherwise
  `<db>.tls/client.crt` and `<db>.tls/client.key` (`identity.tls_dir`).
  `ClientFiles.configured` says which, and `ClientFiles.generated_in` is the
  directory `identity.ensure_identity` may generate a client identity into --
  `None` when the paths are configured (the operator's files are never
  generated or overwritten by this tool; a missing one is the operator's to
  provide).
- `server_identity_files(resolved)` -> `(server.crt, server.key)` in
  `<db>.tls/`.
- `describe_certificate(path)` -> a `CertSummary`: fingerprint and expiry of
  one certificate file, WITHOUT reading any key and without creating anything.
  It never raises for a missing or bad file; the outcome is in the summary
  (`exists`, `error`), so a read-only command can show it.

This module reads certificates only. It imports `zfsbackup.identity` (which
imports the stdlib, `cryptography` and `zfsbackup.config` only) and nothing of
the daemon.
"""

import os
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

from cryptography import x509
from cryptography.hazmat.primitives import serialization

from zfsbackup import identity
from zfsbackup.config import BackupConfig
from zfsbackup.config.store import ResolvedConfigPath

__all__ = [
    "ClientFiles",
    "CertSummary",
    "client_identity_files",
    "describe_certificate",
    "server_identity_files",
    "tls_directory",
]

#: A certificate file larger than this is not a certificate (the same cap as
#: `identity.py`'s file reads).
_MAX_CERT_BYTES = 1024 * 1024


@dataclass(frozen=True)
class ClientFiles:
    """The client identity's files. `configured` is True when they come from
    `client_cert_file` / `client_key_file`; `generated_in` is the directory a
    missing identity may be generated into (`None` for configured paths)."""

    cert: Path
    key: Path
    configured: bool
    generated_in: Optional[Path]


@dataclass(frozen=True)
class CertSummary:
    """What a certificate file holds. `exists` is False for a missing file;
    `error` is set (with `exists` True) for one that cannot be read or parsed.
    `not_after` is timezone-aware UTC. Never carries key material."""

    path: Path
    exists: bool
    fingerprint: Optional[str] = None
    not_after: Optional[datetime] = None
    subject: Optional[str] = None
    error: Optional[str] = None

    @property
    def expired(self) -> bool:
        return self.not_after is not None and self.not_after <= datetime.now(timezone.utc)


def tls_directory(resolved: ResolvedConfigPath) -> Path:
    """`<db>.tls`, the directory of the generated identities. Pure path
    arithmetic: nothing is created or checked."""
    return identity.tls_dir(resolved.resolved_path)


def server_identity_files(resolved: ResolvedConfigPath) -> Tuple[Path, Path]:
    """`(certificate, key)` of this host's server identity."""
    return identity.identity_paths(tls_directory(resolved), identity.SERVER_NAME)


def client_identity_files(config: BackupConfig, resolved: ResolvedConfigPath) -> ClientFiles:
    """The client identity's certificate and key paths (see the module
    docstring). Both configured paths or neither: the model refuses one
    without the other, so a half-set pair cannot reach here from a validated
    configuration; if it does, it is treated as not configured."""
    cert = getattr(config, "client_cert_file", None)
    key = getattr(config, "client_key_file", None)
    if cert is not None and key is not None:
        return ClientFiles(Path(cert), Path(key), configured=True, generated_in=None)
    directory = tls_directory(resolved)
    default_cert, default_key = identity.identity_paths(directory, identity.CLIENT_NAME)
    return ClientFiles(default_cert, default_key, configured=False, generated_in=directory)


def describe_certificate(path: Path) -> CertSummary:
    """Fingerprint and expiry of the first certificate in `path`. Reads the
    certificate file only (never a key), takes no lock, creates nothing."""
    path = Path(path)
    try:
        # O_NONBLOCK: opening a FIFO must not wait for a writer.
        fd = os.open(
            str(path),
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0),
        )
    except FileNotFoundError:
        return CertSummary(path, exists=False)
    except PermissionError:
        return CertSummary(path, exists=True, error="permission denied")
    except OSError as exc:
        return CertSummary(path, exists=True, error=exc.strerror or str(exc))
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            return CertSummary(path, exists=True, error="not a regular file")
        data = b""
        while len(data) <= _MAX_CERT_BYTES:
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            data += chunk
    except OSError as exc:
        return CertSummary(path, exists=True, error=exc.strerror or str(exc))
    finally:
        os.close(fd)
    if len(data) > _MAX_CERT_BYTES:
        return CertSummary(path, exists=True, error="too large to be a certificate")
    try:
        cert = x509.load_pem_x509_certificate(data)
    except (ValueError, x509.InvalidVersion) as exc:
        return CertSummary(path, exists=True, error=f"not a PEM certificate ({exc})")
    return CertSummary(
        path,
        exists=True,
        fingerprint=identity.fingerprint_der(cert.public_bytes(serialization.Encoding.DER)),
        not_after=cert.not_valid_after_utc,
        subject=cert.subject.rfc4514_string(),
    )

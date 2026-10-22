"""Shared helpers for the tests of `zfsbackup.remote` (plan M11 / M16).

Importable from any test module (`tests/` is on `sys.path`, like `_tls_support`). Nothing here
talks to ZFS and no socket is opened.

* `make_transport` -- a `remote._Transport` over a plain `requests.Session` (so `requests_mock`
  or a patched `session.get` answers) and a stand-in SSL context: what the unit tests hand to
  `_negotiate`, `_fetch_resume_token`, `_try_resume`, `_start_fresh`, `_push_step` and `_transfer`.
* `snapshot_presence` -- the `zfs get guid <snapshot>` the client runs after every acknowledged step
  (`RemoteBackupManager._snapshot_present`), answered from the listing; every remote test file
  turns it on with an autouse fixture so no push reaches even the stub `zfs`.
* `FakeConnect` / `fake_connect_pinned` -- replaces `zfsbackup.remote.connect_pinned` (the TLS
  connect plus pin check of the websocket) with a recorder that hands out `MagicMock` sockets.
* `client_tls_dir` -- a real, loadable client identity in `<tmp>/client.tls`, copied from one
  identity generated once per session (key generation is the slow part). Pair it with
  `RemoteBackupManager(..., tls_dir=client_tls_dir)` and a destination that has a
  `server_fingerprint` to take the real `backup_dataset` path (identity, pinned session).
* `whoami_json` -- the `/whoami` answer of a server that binds this client to `client_id` (and
  speaks the current wire protocol); `protocol_refusal_json` -- the server's 426 body.
* `snapshot_info` -- a managed `SnapshotInfo` of `pool/data` (timestamp parsed from its name).
* `write_identity_files` -- lay an in-memory `_api_support.ClientCert` out as `client.{crt,key}`
  (to build identities `ensure_identity` would never generate: expired, not yet valid).
"""

import os
import shutil
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from libzfseasy.types import Filesystem, Snapshot as ZFSSnapshot
from zfsbackup import identity as ident
from zfsbackup.backup_manager import SnapshotInfo
from zfsbackup.protocol import PROTOCOL_VERSION
from zfsbackup.remote import _endpoint, _Transport

#: A pinned server fingerprint and a client fingerprint that are not any real certificate's.
SERVER_FP = "ab" * 32
OTHER_SERVER_FP = "cd" * 32
CLIENT_FP = "ef" * 32

HTTPS_BASE = "https://backup.example.com"


def make_transport(base=HTTPS_BASE, *, session=None, server_fp=SERVER_FP, identity_fp=CLIENT_FP,
                   ctx=None) -> _Transport:
    """A transport for the unit tests: `session` defaults to a plain `requests.Session()` (the
    `requests_mock` fixture intercepts every session), `ctx` to a `MagicMock`."""
    return _Transport(
        _endpoint(base), server_fp, identity_fp,
        session if session is not None else requests.Session(),
        ctx if ctx is not None else MagicMock(name="ssl_ctx"),
    )


def whoami_json(client_id="client1", fingerprint=CLIENT_FP, protocol=PROTOCOL_VERSION):
    """The body of `GET /whoami` for a client the server binds to `client_id`, from a server
    speaking `protocol` (plan S-5; `None` leaves the key out, as an older server does)."""
    body = {"client_id": client_id, "fingerprint": fingerprint}
    if protocol is not None:
        body["protocol"] = protocol
    return body


def protocol_refusal_json(protocol=PROTOCOL_VERSION):
    """The body of the server's 426 (`api._protocol_refusal`)."""
    return {
        "error": "unsupported zfsbackup protocol", "reason": "protocol",
        "protocol": protocol, "supported": [protocol],
    }


class FakeConnect:
    """Stand-in for `identity.connect_pinned` as imported by `zfsbackup.remote`.

    Records `(host, port, ctx, expected_fp, timeout)` in `calls` and returns a fresh `MagicMock`
    socket (kept in `socks`). Set `side_effect` to an exception (or a callable) to fail the
    connect, e.g. `PinMismatch`."""

    def __init__(self):
        self.calls = []
        self.socks = []
        self.side_effect = None

    def __call__(self, host, port, ctx, expected_fp, timeout):
        self.calls.append((host, port, ctx, expected_fp, timeout))
        effect = self.side_effect
        if isinstance(effect, BaseException):
            raise effect
        if effect is not None:
            return effect(host, port, ctx, expected_fp, timeout)
        sock = MagicMock(name="tls_sock")
        self.socks.append(sock)
        return sock


@pytest.fixture
def fake_connect_pinned(mocker):
    fake = FakeConnect()
    mocker.patch("zfsbackup.remote.connect_pinned", fake)
    return fake


@pytest.fixture(scope="session")
def _client_tls_template(tmp_path_factory):
    root = tmp_path_factory.mktemp("remote-client-tls")
    directory = root / "client.tls"
    ident.ensure_identity(directory, ident.ROLE_CLIENT, group_of=root)
    return directory


@pytest.fixture
def client_tls_dir(tmp_path, _client_tls_template):
    """A fresh copy (own inodes and mtimes, same modes) of one generated client identity."""
    directory = tmp_path / "client.tls"
    shutil.copytree(_client_tls_template, directory)
    return directory


def client_fingerprint(directory) -> str:
    """The fingerprint of the client identity in `directory`."""
    info = ident.peek_identity(Path(directory), ident.CLIENT_NAME)
    assert info is not None
    return info.fingerprint


def new_client_identity(directory, name=None) -> "ident.Identity":
    """Generate a client identity in `directory` (default name `client`)."""
    directory = Path(directory)
    return ident.ensure_identity(
        directory, ident.ROLE_CLIENT, name=name, group_of=directory.parent
    )


class SnapshotPresence:
    """Stands in for `zfs.get.value(<snapshot>, 'guid')` as `RemoteBackupManager._snapshot_present`
    asks it. By default a snapshot is present with the guid it was listed with. `gone` is a set of
    short names for which zfs says the dataset does not exist; `unknown` a set for which zfs fails
    for another reason (the answer is then uncertain); `replaced` maps a short name to a different
    guid (a snapshot destroyed and recreated under the same name). `calls` records the asks."""

    def __init__(self):
        self.gone = set()
        self.unknown = set()
        self.replaced = {}
        self.calls = []

    def __call__(self, snapshot, prop, *args, **kwargs):
        name = str(snapshot)
        short = name.split("@", 1)[1] if "@" in name else name
        self.calls.append((name, prop))
        if short in self.gone:
            raise Exception(f"cannot open '{name}': dataset does not exist")
        if short in self.unknown:
            raise Exception("cannot open pool: pool I/O is currently suspended")
        if short in self.replaced:
            return str(self.replaced[short])
        try:
            return str(snapshot[prop].value)
        except Exception:
            return None


@pytest.fixture
def snapshot_presence(mocker):
    """`zfs.get.value` as seen by `zfsbackup.remote`: every listed snapshot is still there."""
    presence = SnapshotPresence()
    mocker.patch("zfsbackup.remote.zfs.get.value", side_effect=presence)
    return presence


def snapshot_info(name, dataset="pool/data", **props) -> SnapshotInfo:
    """A managed snapshot (`autosnap_<YYYYMMDDHHMMSS>`) of `dataset`, as the manager's one listing
    delivers it (S-1): a `createtxg` ordered like the name and a `guid`; see
    `_snapshot_support.listed_info` for `txg=` / `guid=` overrides."""
    from _snapshot_support import listed_info

    return listed_info(name, "autosnap", dataset, **props)


def write_identity_files(directory, cert, name="client") -> None:
    """Write `cert` (an `_api_support.ClientCert`) as `<name>.crt` (0640) and `<name>.key` (0600)
    in `directory`, replacing what is there atomically (a new inode each)."""
    from cryptography.hazmat.primitives import serialization

    directory = Path(directory)
    directory.mkdir(mode=0o750, exist_ok=True)
    key_pem = cert.key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    for suffix, data, mode in (("key", key_pem, 0o600), ("crt", cert.cert_pem.encode(), 0o640)):
        tmp = directory / f".{name}.{suffix}.tmp"
        tmp.write_bytes(data)
        os.chmod(tmp, mode)
        os.replace(tmp, directory / f"{name}.{suffix}")

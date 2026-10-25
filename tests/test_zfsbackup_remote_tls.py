"""The client half of the TLS / mutual-TLS transport (plan M11 / M16): `RemoteBackupManager`.

What this file pins down, none of which the transport-level tests in `test_zfsbackup_remote.py`
and `test_zfsbackup_remote_timeouts.py` (they hand the methods a fake `_Transport`) can:

* `_endpoint` -- how a destination URL is read (scheme, IPv6, default port, user info).
* **Every check runs before any send.** A missing client id, an un-enrolled destination, an unusable
  client TLS identity, a server that does not trust us or is not the pinned one, a client id that does
  not match the server's binding: each is decided BEFORE `zfs send` is spawned, a pin is written or a
  byte of the stream is read -- and logged once (ERROR), then at DEBUG, with an INFO line on recovery.
  The error-mapping table (`TestRefusalMapping` and friends) is the spec.
* The `/whoami` heuristics: a reset right after the handshake is retried once and then reported as a
  possible rejection; other failures, the server's retryable 503 and a bad answer each have their own
  line.
* The client identity is reloaded when its files change (device, inode, size or mtime), with no new
  manager; a failed load is logged once per distinct message and retried.
* `begin_cycle`: one `/whoami` per destination per cycle, a retry of a failed identity load, and what a
  manager used without it does.
* `TestClientCertExpiryCheck` (S-12): `begin_cycle` reports a client certificate that expires within 30
  days (WARNING) or has expired (ERROR) on a manager's first cycle and then every 24 h, from the
  certificate alone (so also when the identity cannot load); silent for a missing file, DEBUG otherwise.
* Real TLS, end to end (`TestRealTls`, `integration`): the manager against a real `ApiWorker` with real
  handshakes -- a successful pinned-mTLS backup, an impostor server, a revoked client, a client id that
  does not match, an identity rotated under a running manager. Server-side zfs is replaced in memory
  and every subprocess runs in the hermetic environment; no real `zfs` is ever reached.
"""

import contextlib
import errno
import http.client
import json
import logging
import multiprocessing
import os
import re
import shutil
import socket
import ssl
import subprocess
import sys
import threading
import time
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
import websocket

from _remote_support import (  # noqa: F401  (fixtures: _client_tls_template, client_tls_dir, ...)
    CLIENT_FP, HTTPS_BASE, OTHER_SERVER_FP, SERVER_FP, _client_tls_template, client_tls_dir,
    fake_connect_pinned, new_client_identity, protocol_refusal_json, snapshot_info, snapshot_presence,
    whoami_json, write_identity_files,
)
from libzfseasy.types import Filesystem
from libzfseasy.zfs import StreamHandle
from zfsbackup import identity as ident
from zfsbackup import remote
from zfsbackup.backup_manager import DatasetManager
from zfsbackup.config import (
    BackupConfig, DatasetConfig, Destination, RemoteDatasetConfig, RemoteServerConfig,
)
from zfsbackup.identity import PinMismatch
from zfsbackup.protocol import protocol_headers
from zfsbackup.remote import (
    HTTP_TIMEOUT, NO_CLIENT_ID_MESSAGE, RemoteBackupManager, _alert_reason,
    _endpoint, _exc_chain, _is_reset, _json_body, _presented_fingerprint,
)

BASE = HTTPS_BASE
WHOAMI = f"{BASE}/whoami"
NEGOTIATE = re.compile(r".*/negotiate$")
RESUME_TOKEN = re.compile(r".*/resume_token$")
OLD = "autosnap_20240101120000"
NEW = "autosnap_20240103120000"
ACTUAL_FP = "12" * 32            # what an impostor presents
ENROLL = "`zfsbackup-config enroll offsite --token-file …`"
#: Appended to the re-enroll advice for a TLS-alert or "client not trusted" refusal.
ENROLL_HINT = (" (`--new-key` if this certificate was revoked there; plain `enroll` if this host "
               "just changed key)")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _snapshots_are_present(snapshot_presence):
    """After an acknowledged step the client asks `zfs get guid` whether the snapshot it sent still
    exists; every listed snapshot does, and no `zfs` runs. (The real-TLS tests below use real
    `SnapshotInfo`s for the same reason.)"""
    return snapshot_presence


@pytest.fixture(autouse=True)
def _no_local_zfs_property_writes(mocker):
    """The anchor / in-flight pin properties are `zfs get|set|inherit` calls on the LOCAL dataset:
    harmless defaults, so no test here reaches even the stub `zfs`. Tests that assert on them patch
    the instance (which wins over these class patches)."""
    mocker.patch.object(DatasetManager, "get_anchor", return_value=None)
    mocker.patch.object(DatasetManager, "set_anchor", return_value=True)
    mocker.patch.object(DatasetManager, "get_inflight", return_value=None)
    mocker.patch.object(DatasetManager, "set_inflight", return_value=True)
    mocker.patch.object(DatasetManager, "clear_inflight", return_value=True)


def own(caplog):
    """`[(level, message)]` of the records `zfsbackup.remote` logged."""
    return [(r.levelname, r.getMessage()) for r in caplog.records if r.name == "zfsbackup.remote"]


def errors(caplog):
    return [m for lv, m in own(caplog) if lv == "ERROR"]


def levels(caplog, text):
    """The levels of the own records whose message contains `text`."""
    return [lv for lv, m in own(caplog) if text in m]


class Stream:
    """A `zfs send` stand-in: `chunks`, then end of data. Counts closes."""

    def __init__(self, chunks=(b"data",)):
        self._chunks = list(chunks)
        self.closes = 0

    def read(self, n):
        return self._chunks.pop(0) if self._chunks else b""

    def close(self):
        self.closes += 1


def reset_error(kind="reset"):
    """What `requests` raises for a connection reset / broken pipe / hang-up after the handshake."""
    if kind == "reset":
        inner = ConnectionResetError(errno.ECONNRESET, "Connection reset by peer")
    elif kind == "pipe":
        inner = BrokenPipeError(errno.EPIPE, "Broken pipe")
    else:
        inner = http.client.RemoteDisconnected("Remote end closed connection without response")
    return requests.exceptions.ConnectionError(
        requests.packages.urllib3.exceptions.ProtocolError("Connection aborted.", inner)
    )


def urllib3_pin_error(expected=SERVER_FP, actual=ACTUAL_FP):
    """The `requests` error `pinned_session` produces for a wrong server fingerprint."""
    return requests.exceptions.SSLError(
        f'HTTPSConnectionPool(host=\'backup.example.com\', port=443): Max retries exceeded with url: '
        f'/whoami (Caused by SSLError(SSLError(\'Fingerprints did not match. Expected "{expected}", '
        f'got "{actual}"\')))'
    )


def tls_alert(reason="TLSV1_ALERT_UNKNOWN_CA", *, raw=True):
    """A rejected client certificate (S10): the raw `ssl.SSLError` a socket raises, or the
    `requests` error wrapping its text."""
    text = f"[SSL: {reason}] tlsv1 alert unknown ca (_ssl.c:2648)"
    if raw:
        exc = ssl.SSLError(1, text)
        exc.reason = reason
        return exc
    return requests.exceptions.SSLError(f"HTTPSConnectionPool(host='x', port=443): {text}")


def bad_status(status, error=None, **fields):
    """The `WebSocketBadStatusException` of a refused upgrade (S9)."""
    body = json.dumps({"error": error, **fields}).encode() if error is not None else b""
    return websocket.WebSocketBadStatusException(
        f"Handshake status {status}", status, "refused", {}, body
    )


# ---------------------------------------------------------------------------
# The rig: a real manager, real client identity, real pinned sessions (answered by requests_mock)
# ---------------------------------------------------------------------------


class Rig:
    """A real `RemoteBackupManager` over a real client identity (`tls_dir`), real pinned `requests`
    sessions (answered by the `requests_mock` fixture) and spies on everything that must NOT happen
    before the checks pass: the `zfs send` spawn, the websocket, the pinned TLS connect, the
    negotiate / resume-token requests, the in-flight pin and the local snapshot listing.

    The server is healthy by default (`/whoami` binds this client to its id, `negotiate` finds no
    common snapshot, no resume token); tests make it unhealthy by registering other answers."""

    def __init__(self, tmp_path, requests_mock, mocker, caplog, tls_dir, fake_connect, *,
                 client_id="client1", destinations=None, datasets=("pool/data",), **config_kw):
        self.rm = requests_mock
        self.fake_connect = fake_connect
        self.tls_dir = tls_dir
        info = ident.peek_identity(tls_dir, ident.CLIENT_NAME)
        self.client_fp = info.fingerprint if info is not None else CLIENT_FP     # (a deliberately absent identity)
        caplog.set_level(logging.DEBUG, logger="zfsbackup.remote")
        self.caplog = caplog
        self.destinations = destinations or {
            "offsite": Destination(url=BASE, server_fingerprint=SERVER_FP)
        }
        self.config = BackupConfig(
            datasets=[
                DatasetConfig(
                    name=name,
                    remote=[RemoteDatasetConfig(destination=d) for d in self.destinations],
                )
                for name in datasets
            ],
            destinations=self.destinations,
            **config_kw,
        )
        self.manager = DatasetManager(self.config)
        self.mgr = RemoteBackupManager(
            self.config, self.manager, client_id=client_id, tls_dir=tls_dir
        )
        self.snaps = [snapshot_info(NEW), snapshot_info(OLD)]
        self.list_snapshots = mocker.patch.object(
            self.manager, "list_snapshots", side_effect=lambda dsi: list(self.snaps)
        )
        self.set_inflight = mocker.patch.object(self.manager, "set_inflight", return_value=True)
        self.clear_inflight = mocker.patch.object(self.manager, "clear_inflight", return_value=True)
        self.set_anchor = mocker.patch.object(self.manager, "set_anchor", return_value=True)
        self.sends = []          # (label, args) of every `zfs send` spawned
        self.streams = []

        def spawn(label):
            def start(*args, **kwargs):
                stream = Stream()
                self.sends.append((label, args, kwargs))
                self.streams.append(stream)
                return stream
            return start

        self.send_snapshot = mocker.patch("zfsbackup.remote.zfs.send.snapshot", side_effect=spawn("send"))
        self.send_resume = mocker.patch("zfsbackup.remote.zfs.send.resume", side_effect=spawn("send-t"))
        self.ws = MagicMock(name="ws")
        self.ws.recv.return_value = json.dumps({"status": "ok"})
        self.ws_cls = mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=self.ws)
        for dest in self.destinations.values():
            with contextlib.suppress(ValueError):                  # a non-https URL has no routes
                self.route_whoami(client_id, _endpoint(dest.url).base_url)
        self.rm.post(NEGOTIATE, json={"common_snapshot": None})
        self.rm.get(RESUME_TOKEN, json={"resume_token": None})

    # -- the server's answers ------------------------------------------------------------------
    def route_whoami(self, client_id="client1", base=BASE, **response):
        response = response or {"json": whoami_json(client_id, self.client_fp)}
        return self.rm.get(f"{base}/whoami", **response)

    def refuse(self, stages=("whoami", "negotiate", "resume_token"), **response):
        """The server answers every HTTP stage in `stages` with `response`."""
        if "whoami" in stages:
            self.rm.get(WHOAMI, **response)
        if "negotiate" in stages:
            self.rm.post(NEGOTIATE, **response)
        if "resume_token" in stages:
            self.rm.get(RESUME_TOKEN, **response)

    def heal(self):
        self.route_whoami(self.mgr.client_id)
        self.rm.post(NEGOTIATE, json={"common_snapshot": None})
        self.rm.get(RESUME_TOKEN, json={"resume_token": None})

    # -- act ----------------------------------------------------------------------------------
    def rcfg(self, dest="offsite", **kw):
        return RemoteDatasetConfig(destination=dest, **kw)

    def run(self, index=0, dest="offsite", **kw):
        """`backup_dataset` for dataset `index` -> `dest`, within the current cycle."""
        dsi = self.manager.datasets[index]
        return self.mgr.backup_dataset(dsi, self.rcfg(dest), **kw)

    def cycle(self, index=0, dest="offsite", **kw):
        self.mgr.begin_cycle()
        return self.run(index, dest, **kw)

    # -- look ---------------------------------------------------------------------------------
    def requests(self, suffix):
        return [r for r in self.rm.request_history if r.path.endswith(suffix)]

    @property
    def http_count(self):
        return len(self.rm.request_history)

    def assert_nothing_sent(self, *, local_listing=False):
        """No stream, no pin, no negotiation: the destination was decided before any of it."""
        assert self.sends == []
        self.ws_cls.assert_not_called()
        assert self.fake_connect.calls == []
        assert self.requests("/negotiate") == [] and self.requests("/resume_token") == []
        self.set_inflight.assert_not_called()
        self.set_anchor.assert_not_called()
        if not local_listing:
            self.list_snapshots.assert_not_called()

    @property
    def transport(self):
        return self.mgr._transports["offsite"]


@pytest.fixture
def make_rig(tmp_path, requests_mock, mocker, caplog, client_tls_dir, fake_connect_pinned):
    def build(**kw):
        return Rig(tmp_path, requests_mock, mocker, caplog, kw.pop("tls_dir", client_tls_dir),
                   fake_connect_pinned, **kw)
    return build


@pytest.fixture
def rig(make_rig):
    return make_rig()


# ---------------------------------------------------------------------------
# _endpoint
# ---------------------------------------------------------------------------


class TestEndpoint:
    @pytest.mark.parametrize("url, base, ws_base, host, port", [
        ("https://backup.example.com", "https://backup.example.com", "wss://backup.example.com",
         "backup.example.com", 443),
        ("https://backup.example.com:443", "https://backup.example.com", "wss://backup.example.com",
         "backup.example.com", 443),
        ("https://backup.example.com:8443", "https://backup.example.com:8443",
         "wss://backup.example.com:8443", "backup.example.com", 8443),
        ("https://backup.example.com/", "https://backup.example.com", "wss://backup.example.com",
         "backup.example.com", 443),
        ("https://backup.example.com/zb/", "https://backup.example.com/zb",
         "wss://backup.example.com/zb", "backup.example.com", 443),
        ("https://backup.example.com:9000/a/b///", "https://backup.example.com:9000/a/b",
         "wss://backup.example.com:9000/a/b", "backup.example.com", 9000),
        ("https://127.0.0.1:1234", "https://127.0.0.1:1234", "wss://127.0.0.1:1234", "127.0.0.1", 1234),
    ], ids=["plain", "explicit-443", "port", "trailing-slash", "path", "path-and-port", "ipv4"])
    def test_https_destinations(self, url, base, ws_base, host, port):
        ep = _endpoint(url)
        assert (ep.base_url, ep.ws_base, ep.host, ep.port) == (base, ws_base, host, port)

    @pytest.mark.parametrize("url, base, host, port", [
        ("https://[::1]", "https://[::1]", "::1", 443),
        ("https://[::1]:443", "https://[::1]", "::1", 443),
        ("https://[::1]:9000/zb/", "https://[::1]:9000/zb", "::1", 9000),
        ("https://[2001:db8::10]:8443", "https://[2001:db8::10]:8443", "2001:db8::10", 8443),
    ], ids=["default-port", "explicit-443", "port-and-path", "global"])
    def test_ipv6_keeps_its_brackets_in_the_urls_but_not_in_the_host(self, url, base, host, port):
        """URLs need the brackets; the socket layer needs the bare address."""
        ep = _endpoint(url)
        assert (ep.base_url, ep.host, ep.port) == (base, host, port)
        assert ep.ws_base == base.replace("https://", "wss://")

    @pytest.mark.parametrize("url", [
        "HTTPS://backup.example.com", "Https://backup.example.com:8443/", "hTTpS://[::1]:9",
    ])
    def test_an_upper_case_scheme_is_normalised(self, url):
        ep = _endpoint(url)
        assert ep.base_url.startswith("https://") and ep.ws_base.startswith("wss://")

    def test_the_host_is_lower_cased(self):
        assert _endpoint("https://Backup.EXAMPLE.com").host == "backup.example.com"

    @pytest.mark.parametrize("url", [
        "https://user@backup.example.com", "https://user:secret@backup.example.com:8443/zb",
        "https://:secret@backup.example.com", "https://us%40er:p%3Aw@backup.example.com",
    ])
    def test_user_info_is_dropped_never_logged_or_sent(self, url):
        ep = _endpoint(url)
        assert "@" not in ep.base_url and "@" not in ep.ws_base
        assert "secret" not in ep.base_url + ep.ws_base and "user" not in ep.base_url + ep.ws_base
        assert ep.host == "backup.example.com"

    @pytest.mark.parametrize("url, base", [
        ("https://backup.example.com/zb?token=abc", "https://backup.example.com/zb"),
        ("https://backup.example.com/zb#frag", "https://backup.example.com/zb"),
        ("https://backup.example.com?x=1#y", "https://backup.example.com"),
    ])
    def test_query_and_fragment_are_dropped(self, url, base):
        ep = _endpoint(url)
        assert ep.base_url == base and ep.ws_base == base.replace("https://", "wss://")

    def test_surrounding_whitespace_is_ignored(self):
        assert _endpoint("  https://backup.example.com:8443/zb  \n").base_url == (
            "https://backup.example.com:8443/zb"
        )

    @pytest.mark.parametrize("url", [
        "http://backup.example.com", "HTTP://backup.example.com", "ftp://backup.example.com",
        "ws://backup.example.com", "wss://backup.example.com", "backup.example.com",
        "//backup.example.com", "https://", "https:///zb", "", "   ",
        "https://backup.example.com:abc", "https://backup.example.com:70000", "https://[::1",
    ])
    def test_anything_but_an_https_url_with_a_host_is_a_value_error(self, url):
        with pytest.raises(ValueError):
            _endpoint(url)

    @pytest.mark.parametrize("url", [
        "https://backup.example.com:0", "https://backup.example.com:0/zb", "https://backup.example.com:00",
        "https://backup.example.com:", "https://backup.example.com:/zb", "https://user@backup.example.com:",
        "https://[::1]:0", "https://[::1]:", "https://[::1]:/zb",
    ], ids=["port-0", "port-0-path", "port-00", "empty-port", "empty-port-path", "empty-port-userinfo",
            "ipv6-port-0", "ipv6-empty-port", "ipv6-empty-port-path"])
    def test_port_zero_and_an_empty_port_are_refused_never_taken_for_443(self, url):
        with pytest.raises(ValueError, match="port"):
            _endpoint(url)

    @pytest.mark.parametrize("url, port", [
        ("https://backup.example.com:1", 1), ("https://backup.example.com:65535", 65535),
        ("https://[::1]:1", 1),
    ])
    def test_the_ports_at_the_edges_of_the_range_are_accepted(self, url, port):
        assert _endpoint(url).port == port

    def test_the_result_is_a_named_tuple(self):
        ep = _endpoint("https://h:1/p")
        assert ep == ("https://h:1/p", "wss://h:1/p", "h", 1)


# ---------------------------------------------------------------------------
# The small helpers
# ---------------------------------------------------------------------------


class TestErrorHelpers:
    def test_exc_chain_walks_causes_contexts_reasons_and_args(self):
        inner = ConnectionResetError("reset")
        proto = requests.packages.urllib3.exceptions.ProtocolError("aborted", inner)
        outer = requests.exceptions.ConnectionError(proto)
        chain = list(_exc_chain(outer))
        assert outer in chain and proto in chain and inner in chain

    def test_exc_chain_follows_dunder_cause_and_context(self):
        try:
            try:
                raise BrokenPipeError("pipe")
            except BrokenPipeError as bp:
                raise RuntimeError("wrapper") from bp
        except RuntimeError as exc:
            assert any(isinstance(e, BrokenPipeError) for e in _exc_chain(exc))

    def test_exc_chain_survives_a_cycle(self):
        a, b = RuntimeError("a"), RuntimeError("b")
        a.__cause__, b.__cause__ = b, a
        assert len(list(_exc_chain(a))) == 2

    def test_exc_chain_follows_a_reason_that_is_an_exception(self):
        class Wrapped(Exception):
            reason = ConnectionResetError("x")
        assert any(isinstance(e, ConnectionResetError) for e in _exc_chain(Wrapped("w")))

    @pytest.mark.parametrize("make", [
        lambda: ConnectionResetError("r"), lambda: BrokenPipeError("p"),
        lambda: http.client.RemoteDisconnected("closed"),
        lambda: reset_error("reset"), lambda: reset_error("pipe"), lambda: reset_error("hangup"),
    ], ids=["reset", "pipe", "remote-disconnected", "wrapped-reset", "wrapped-pipe", "wrapped-hangup"])
    def test_resets_are_recognised_wherever_they_are_wrapped(self, make):
        assert _is_reset(make())

    @pytest.mark.parametrize("make", [
        lambda: ConnectionRefusedError("refused"), lambda: TimeoutError("t"),
        lambda: requests.exceptions.Timeout("slow"), lambda: tls_alert(), lambda: ValueError("v"),
        lambda: requests.exceptions.ConnectionError(ConnectionRefusedError("refused")),
    ], ids=["refused", "timeout", "requests-timeout", "alert", "value", "wrapped-refused"])
    def test_other_errors_are_not_resets(self, make):
        assert not _is_reset(make())

    def test_the_presented_fingerprint_comes_from_a_pin_mismatch(self):
        assert _presented_fingerprint(PinMismatch(SERVER_FP, ACTUAL_FP)) == ACTUAL_FP

    def test_the_presented_fingerprint_comes_from_a_wrapped_pin_mismatch(self):
        wrapped = requests.exceptions.ConnectionError(PinMismatch(SERVER_FP, ACTUAL_FP))
        assert _presented_fingerprint(wrapped) == ACTUAL_FP

    def test_the_presented_fingerprint_comes_from_urllib3s_text(self):
        assert _presented_fingerprint(urllib3_pin_error()) == ACTUAL_FP

    def test_urllib3s_colon_separated_upper_case_form_is_normalised(self):
        pairs = ":".join(ACTUAL_FP[i:i + 2].upper() for i in range(0, 64, 2))
        exc = requests.exceptions.SSLError(f'Fingerprints did not match. Expected "x", got "{pairs}"')
        assert _presented_fingerprint(exc) == ACTUAL_FP

    def test_an_unknown_presented_fingerprint_is_none(self):
        assert _presented_fingerprint(PinMismatch(SERVER_FP, None)) is None
        assert _presented_fingerprint(ValueError("nothing")) is None

    @pytest.mark.parametrize("prefix", ["TLS_ALERT_", "TLSV1_ALERT_", "SSLV3_ALERT_"])
    def test_the_alert_name_comes_from_the_reason_for_every_openssl_spelling(self, prefix):
        assert _alert_reason(tls_alert(f"{prefix}UNKNOWN_CA")) == f"{prefix}UNKNOWN_CA"

    def test_the_alert_name_comes_from_a_wrapped_requests_text(self):
        assert _alert_reason(tls_alert("TLSV1_ALERT_CERTIFICATE_EXPIRED", raw=False)) == (
            "TLSV1_ALERT_CERTIFICATE_EXPIRED"
        )

    def test_with_no_alert_in_it_the_whole_text_is_used(self):
        assert _alert_reason(RuntimeError("something else")) == "something else"

    def test_json_body_is_an_object_or_empty(self):
        good, listed, junk = MagicMock(), MagicMock(), MagicMock()
        good.json.return_value = {"error": "x"}
        listed.json.return_value = ["x"]
        junk.json.side_effect = ValueError("no json")
        assert _json_body(good) == {"error": "x"}
        assert _json_body(listed) == {} and _json_body(junk) == {}


# ---------------------------------------------------------------------------
# Constructor, contract, removals
# ---------------------------------------------------------------------------


class TestContract:
    def test_client_id_is_required_and_keyword_only(self, tmp_path):
        config = BackupConfig(datasets=[], destinations={})
        manager = DatasetManager(config)
        with pytest.raises(TypeError):
            RemoteBackupManager(config, manager)
        with pytest.raises(TypeError):
            RemoteBackupManager(config, manager, "client1")
        assert RemoteBackupManager(config, manager, client_id=None).client_id is None

    def test_tls_dir_is_optional_and_a_path_when_given(self, tmp_path):
        config = BackupConfig(datasets=[], destinations={})
        manager = DatasetManager(config)
        assert RemoteBackupManager(config, manager, client_id="c").tls_dir is None
        assert RemoteBackupManager(config, manager, client_id="c", tls_dir=str(tmp_path)).tls_dir == tmp_path

    @pytest.mark.parametrize("name", ["ClientIdentity", "_read_id_file", "_ID_FILE_MAX"])
    def test_the_file_based_client_id_is_gone(self, name):
        assert not hasattr(remote, name)

    def test_register_is_gone_and_the_manager_has_no_identity_attribute(self, rig):
        assert not hasattr(remote.RemoteBackupManager, "_register")
        assert not hasattr(rig.mgr, "identity")

    def test_the_constants(self):
        assert (HTTP_TIMEOUT, remote.RESUME_TOKEN_TIMEOUT) == (30, 10)
        assert NO_CLIENT_ID_MESSAGE == "no client id; run `zfsbackup-config enroll`"

    def test_the_remote_module_has_no_plaintext_downgrade(self):
        text = Path(remote.__file__).read_text()
        assert "ws://" not in text.replace("wss://", "")
        assert ".replace('http'" not in text and '.replace("http"' not in text
        assert "requests.post(" not in text and "requests.get(" not in text    # only the pinned sessions

    def test_the_worker_starts_each_cycle_with_begin_cycle(self, mocker):
        """`RemoteBackupWorker._get_datasets` is called once at the top of every cycle."""
        from zfsbackup.stopflag import StopFlag
        from zfsbackup.workers import RemoteBackupWorker

        worker = RemoteBackupWorker(BackupConfig(datasets=[], destinations={}), StopFlag(), client_id="c")
        worker._remote_manager = MagicMock()
        manager = MagicMock(datasets=[types.SimpleNamespace(name="pool/a"), types.SimpleNamespace(name="pool/b")])
        assert [d.name for d in worker._get_datasets(manager)] == ["pool/a", "pool/b"]
        worker._remote_manager.begin_cycle.assert_called_once_with()
        worker._get_datasets(manager)
        assert worker._remote_manager.begin_cycle.call_count == 2

    def test_the_worker_builds_the_manager_with_the_handed_over_id_and_tls_dir(self, mocker, tmp_path):
        from zfsbackup.stopflag import StopFlag
        from zfsbackup.workers import RemoteBackupWorker

        config = BackupConfig(datasets=[], destinations={})
        worker = RemoteBackupWorker(config, StopFlag(), client_id="host-1", tls_dir=tmp_path)
        cls = mocker.patch("zfsbackup.remote.RemoteBackupManager")
        manager = MagicMock()
        worker._before_loop(config, manager)
        cls.assert_called_once_with(config, manager, client_id="host-1", tls_dir=tmp_path)


# ---------------------------------------------------------------------------
# The healthy path
# ---------------------------------------------------------------------------


class TestHealthyPath:
    def test_whoami_then_negotiate_then_resume_token_then_the_pinned_websocket(self, rig, caplog):
        assert rig.cycle() is True
        order = [(r.method, r.path.rsplit("/", 1)[-1]) for r in rig.rm.request_history]
        assert order == [("GET", "whoami"), ("POST", "negotiate"), ("GET", "resume_token")]
        ((host, port, ctx, fp, timeout),) = rig.fake_connect.calls
        assert (host, port, fp, timeout) == ("backup.example.com", 443, SERVER_FP, remote.WS_CONNECT_TIMEOUT)
        assert rig.ws.connect.call_args.kwargs["socket"] is rig.fake_connect.socks[0]
        assert rig.ws.connect.call_args.args[0] == (
            "wss://backup.example.com/backup/client1/pool/data/stream?mode=fresh"
        )
        assert [lv for lv, _ in own(caplog) if lv in ("ERROR", "WARNING")] == []

    def test_the_urls_keep_the_client_id_in_the_path(self, rig):
        rig.cycle()
        assert {r.path for r in rig.rm.request_history if "/backup/" in r.path} == {
            "/backup/client1/pool/data/negotiate", "/backup/client1/pool/data/resume_token",
        }

    def test_the_session_and_context_are_pinned_to_the_destinations_fingerprint(self, rig):
        rig.cycle()
        assert rig.transport.server_fp == SERVER_FP
        assert rig.transport.identity_fp == rig.client_fp
        assert rig.transport.key == (rig.client_fp, SERVER_FP)
        assert rig.transport.ctx.verify_mode == ssl.CERT_NONE
        assert rig.transport.ctx.minimum_version == ssl.TLSVersion.TLSv1_3

    def test_the_identity_is_logged_once_at_info(self, rig, caplog):
        rig.cycle()
        rig.cycle()
        loaded = [m for lv, m in own(caplog) if "client TLS identity" in m]
        assert len(loaded) == 1 and rig.client_fp in loaded[0]
        assert levels(caplog, "client TLS identity") == ["INFO"]

    def test_a_dry_run_still_checks_the_destination_but_sends_nothing(self, make_rig):
        rig = make_rig(dry_run=True)
        assert rig.cycle() is True
        assert len(rig.requests("/whoami")) == 1 and len(rig.requests("/negotiate")) == 1
        assert rig.sends == [] and rig.fake_connect.calls == []

    def test_no_snapshots_is_true_after_the_checks(self, rig):
        rig.snaps.clear()
        assert rig.cycle() is True
        assert len(rig.requests("/whoami")) == 1 and rig.requests("/negotiate") == []

    def test_an_unknown_destination_is_false_before_anything(self, rig):
        dsi = rig.manager.datasets[0]
        assert rig.mgr.backup_dataset(dsi, rig.rcfg("nowhere")) is False
        assert rig.http_count == 0

    def test_a_destination_under_a_sub_path_and_port(self, make_rig):
        rig = make_rig(destinations={
            "offsite": Destination(url="https://backup.example.com:8443/zb/", server_fingerprint=SERVER_FP)
        })
        base = "https://backup.example.com:8443/zb"
        rig.route_whoami("client1", base)
        rig.rm.post(re.compile(r".*/negotiate$"), json={"common_snapshot": None})
        assert rig.cycle() is True
        assert rig.requests("/whoami")[0].url.startswith(base + "/whoami")
        assert rig.fake_connect.calls[0][:2] == ("backup.example.com", 8443)
        assert rig.ws.connect.call_args.args[0].startswith(
            "wss://backup.example.com:8443/zb/backup/client1/pool/data/stream"
        )

    def test_an_ipv6_destination(self, make_rig):
        rig = make_rig(destinations={
            "offsite": Destination(url="https://[::1]:8443", server_fingerprint=SERVER_FP)
        })
        rig.route_whoami("client1", "https://[::1]:8443")
        assert rig.cycle() is True
        assert rig.fake_connect.calls[0][:2] == ("::1", 8443)
        assert rig.ws.connect.call_args.args[0].startswith("wss://[::1]:8443/backup/client1/")

    def test_an_upper_case_scheme_destination_works(self, make_rig):
        rig = make_rig(destinations={
            "offsite": Destination(url="HTTPS://backup.example.com/", server_fingerprint=SERVER_FP)
        })
        assert rig.cycle() is True
        assert rig.ws.connect.call_args.args[0].startswith("wss://backup.example.com/backup/")


# ---------------------------------------------------------------------------
# Every check runs before any send
# ---------------------------------------------------------------------------


class TestNoClientId:
    """Row 1: `client_id is None` -> ERROR once per manager, every destination skipped."""

    def test_one_error_for_the_manager_then_debug_and_nothing_else_happens(self, make_rig, caplog):
        rig = make_rig(client_id=None, datasets=("pool/a", "pool/b"), destinations={
            "offsite": Destination(url=BASE, server_fingerprint=SERVER_FP),
            "dc2": Destination(url="https://dc2.example.com", server_fingerprint=SERVER_FP),
        })
        for _ in range(2):                                           # two cycles
            rig.mgr.begin_cycle()
            for index in (0, 1):
                for dest in ("offsite", "dc2"):
                    assert rig.run(index, dest) is False
        message = "remote backup skipped: no client id; run `zfsbackup-config enroll`"
        assert [lv for lv, m in own(caplog) if m == message] == ["ERROR"] + ["DEBUG"] * 7
        assert errors(caplog) == [message]
        assert rig.http_count == 0
        rig.assert_nothing_sent()

    def test_it_is_checked_first_nothing_is_loaded_or_touched(self, make_rig, mocker, caplog, tmp_path):
        load = mocker.spy(remote, "load_identity")
        rig = make_rig(client_id=None, tls_dir=tmp_path / "does-not-exist.tls", destinations={
            "offsite": Destination(url="http://plain.example.com"),         # also not enrolled
        })
        assert rig.cycle() is False
        assert errors(caplog) == [f"remote backup skipped: {NO_CLIENT_ID_MESSAGE}"]
        load.assert_not_called()
        assert not (tmp_path / "does-not-exist.tls").exists()               # the daemon creates nothing

    def test_a_manager_that_gets_an_id_later_is_a_new_manager(self, make_rig, caplog):
        """The id comes from the supervisor's snapshot: a changed id is a reload (a respawned
        worker), never a mutation of a running manager. A fresh manager logs its own ERROR."""
        rig = make_rig(client_id=None)
        rig.cycle()
        rig.route_whoami("client1")
        fresh = RemoteBackupManager(rig.config, rig.manager, client_id="client1", tls_dir=rig.tls_dir)
        fresh.begin_cycle()
        assert fresh.backup_dataset(rig.manager.datasets[0], rig.rcfg()) is True


class TestNotEnrolled:
    """Row 2: a URL that is not https, or no `server_fingerprint`."""

    @pytest.mark.parametrize("dest, reason, url_hint", [
        (Destination(url=BASE), "no server fingerprint is pinned", BASE),
        (Destination(url="http://backup.example.com", server_fingerprint=SERVER_FP),
         "plaintext http is not supported", "https://…"),
        (Destination(url="ftp://backup.example.com", server_fingerprint=SERVER_FP), None, "https://…"),
        (Destination(url="https://", server_fingerprint=SERVER_FP), None, "https://…"),
        (Destination(url="https://backup.example.com:0", server_fingerprint=SERVER_FP),
         "port 0", "https://…"),
        (Destination(url="https://backup.example.com:", server_fingerprint=SERVER_FP),
         "empty port", "https://…"),
    ], ids=["no-pin", "http", "ftp", "no-host", "port-0", "empty-port"])
    def test_one_error_then_debug_and_the_destination_is_skipped(self, make_rig, caplog, dest, reason, url_hint):
        rig = make_rig(destinations={"offsite": dest})
        for _ in range(3):
            assert rig.cycle() is False
        got = errors(caplog)
        assert len(got) == 1
        assert got[0].startswith(
            f"destination offsite is not enrolled: run `zfsbackup-config enroll offsite --url {url_hint} "
            "--token-file …` ("
        )
        if reason:
            assert reason in got[0]
        assert levels(caplog, "is not enrolled") == ["ERROR", "DEBUG", "DEBUG"]
        assert rig.http_count == 0
        rig.assert_nothing_sent()

    def test_it_is_decided_before_the_identity_is_even_loaded(self, make_rig, mocker, caplog, tmp_path):
        load = mocker.spy(remote, "load_identity")
        rig = make_rig(tls_dir=tmp_path / "missing.tls",
                       destinations={"offsite": Destination(url=BASE)})
        rig.cycle()
        assert [m for m in errors(caplog) if "is not enrolled" in m]
        assert not [m for m in errors(caplog) if "client TLS identity" in m]
        load.assert_not_called()

    def test_each_destination_is_judged_on_its_own(self, make_rig, caplog):
        rig = make_rig(destinations={
            "offsite": Destination(url=BASE, server_fingerprint=SERVER_FP),
            "dc2": Destination(url="https://dc2.example.com"),                # not enrolled
        })
        rig.route_whoami("client1", "https://dc2.example.com")
        assert rig.cycle(dest="dc2") is False
        assert rig.run(dest="offsite") is True                                # the enrolled one is fine
        assert len(errors(caplog)) == 1 and "destination dc2 is not enrolled" in errors(caplog)[0]

    def test_enrolling_it_is_picked_up_at_the_next_cycle(self, make_rig, caplog):
        rig = make_rig(destinations={"offsite": Destination(url=BASE)})
        assert rig.cycle() is False
        rig.config.destinations["offsite"] = Destination(url=BASE, server_fingerprint=SERVER_FP)
        assert rig.cycle() is True
        # the not-enrolled refusal was cleared by the good /whoami, so a later one is an ERROR again
        assert levels(caplog, "accepts this client again") == ["INFO"]
        rig.config.destinations["offsite"] = Destination(url=BASE)
        assert rig.cycle() is False
        assert levels(caplog, "is not enrolled") == ["ERROR", "ERROR"]

    def test_a_changed_reason_is_an_error_again(self, make_rig, caplog):
        rig = make_rig(destinations={"offsite": Destination(url=BASE)})
        rig.cycle()
        rig.config.destinations["offsite"] = Destination(url="http://backup.example.com")
        rig.cycle()
        rig.cycle()
        assert levels(caplog, "is not enrolled") == ["ERROR", "ERROR", "DEBUG"]


class TestIdentityLockTimeout:
    """The worker's main loop waits at most `remote.IDENTITY_LOCK_TIMEOUT` (5 s) for the
    shared identity lock -- not the library's 30 s default, twice over -- in all three places
    it touches the identity."""

    def test_the_constant_is_five_seconds(self):
        assert remote.IDENTITY_LOCK_TIMEOUT == 5.0

    def test_load_identity_client_context_and_pinned_session_all_get_it(self, make_rig, mocker):
        load = mocker.spy(remote, "load_identity")
        context = mocker.spy(remote, "client_context")
        session = mocker.spy(remote, "pinned_session")
        rig = make_rig()
        assert rig.cycle() is True
        for spy in (load, context, session):
            assert spy.call_count >= 1, spy
            assert all(c.kwargs.get("lock_timeout") == remote.IDENTITY_LOCK_TIMEOUT
                       for c in spy.call_args_list), (spy, spy.call_args_list)

    def test_a_changed_constant_is_what_the_calls_use(self, make_rig, mocker, monkeypatch):
        monkeypatch.setattr(remote, "IDENTITY_LOCK_TIMEOUT", 1.25)
        spies = [mocker.spy(remote, n) for n in ("load_identity", "client_context", "pinned_session")]
        rig = make_rig()
        rig.cycle()
        assert all(c.kwargs["lock_timeout"] == 1.25 for spy in spies for c in spy.call_args_list)

    def test_a_held_identity_lock_fails_the_cycle_within_the_bound_not_the_default(
        self, make_rig, tmp_path, monkeypatch, caplog
    ):
        import fcntl
        monkeypatch.setattr(remote, "IDENTITY_LOCK_TIMEOUT", 0.3)
        rig = make_rig()
        lock = os.open(str(Path(rig.tls_dir) / ident.IDENTITY_LOCK), os.O_RDWR | os.O_CREAT, 0o640)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX)
            started = time.monotonic()
            assert rig.cycle() is False
            assert time.monotonic() - started < 10
        finally:
            os.close(lock)
        assert any("timed out" in m for m in errors(caplog)), errors(caplog)
        assert rig.sends == []


class TestIdentityErrors:
    """Row 3: an unusable client TLS identity -> ERROR once per distinct message, skipped, retried."""

    PREFIX = "remote backup skipped: cannot use the client TLS identity: "

    def break_key_mode(self, d):
        os.chmod(d / "client.key", 0o644)

    def break_mismatch(self, d, tmp_path):
        other = new_client_identity(tmp_path / "other.tls")
        shutil.copyfile(other.key_path, d / "client.key")
        os.chmod(d / "client.key", 0o600)

    def break_missing(self, d):
        os.unlink(d / "client.crt")

    def break_garbage(self, d):
        (d / "client.crt").write_text("garbage\n")

    def break_expired(self, d):
        import datetime as dt
        from _api_support import make_client_cert
        now = dt.datetime.now(dt.timezone.utc)
        write_identity_files(d, make_client_cert(
            not_before=now - dt.timedelta(days=30), not_after=now - dt.timedelta(days=1)))

    def break_not_yet_valid(self, d):
        import datetime as dt
        from _api_support import make_client_cert
        now = dt.datetime.now(dt.timezone.utc)
        write_identity_files(d, make_client_cert(
            not_before=now + dt.timedelta(days=1), not_after=now + dt.timedelta(days=30)))

    @pytest.mark.parametrize("how, text", [
        ("break_key_mode", "readable by everyone"),
        ("break_mismatch", "does not match private key"),
        ("break_missing", "does not exist"),
        ("break_garbage", "does not contain a PEM certificate"),
        ("break_expired", "expired on"),
        ("break_not_yet_valid", "is not valid until"),
    ])
    def test_each_reason_is_one_error_then_debug_and_skips_every_destination_before_any_send(
        self, make_rig, caplog, tmp_path, how, text
    ):
        rig = make_rig(destinations={
            "offsite": Destination(url=BASE, server_fingerprint=SERVER_FP),
            "dc2": Destination(url="https://dc2.example.com", server_fingerprint=SERVER_FP),
        }, datasets=("pool/a", "pool/b"))
        method = getattr(self, how)
        method(rig.tls_dir, tmp_path) if how == "break_mismatch" else method(rig.tls_dir)
        for _ in range(2):
            rig.mgr.begin_cycle()
            for index in (0, 1):
                for dest in ("offsite", "dc2"):
                    assert rig.run(index, dest) is False
        got = errors(caplog)
        if how == "break_expired":
            # S-12: the first cycle logs the expiry reminder AND the identity-load error, the
            # expiry first (it runs at the start of the cycle). The second cycle, moments later,
            # repeats neither as an ERROR: the load error drops to DEBUG and the expiry reminder
            # waits for its daily slot (`TestClientCertExpiryCheck`).
            assert len(got) == 2
            expiry, load = got
            assert expiry.startswith("the client certificate ") and "expired on" in expiry
            assert load.startswith(self.PREFIX) and text in load
            assert [lv for lv, m in own(caplog) if m == expiry] == ["ERROR"]
        else:
            assert len(got) == 1 and got[0].startswith(self.PREFIX) and text in got[0]
            load = got[0]
        # logged by the first failed load; not again for the other datasets and destinations of the
        # cycle (the failure is remembered), once more at DEBUG when the next cycle retries
        assert [lv for lv, m in own(caplog) if m == load] == ["ERROR", "DEBUG"]
        assert rig.http_count == 0
        rig.assert_nothing_sent()

    def test_no_tls_dir_and_no_configured_paths(self, make_rig, caplog):
        rig = make_rig()
        rig.mgr.tls_dir = None
        assert rig.cycle() is False
        assert errors(caplog) == [
            self.PREFIX + "no client certificate is configured and the TLS directory is unknown"
        ]
        rig.assert_nothing_sent()

    def test_a_missing_directory_is_reported_and_never_created(self, make_rig, caplog, tmp_path):
        rig = make_rig(tls_dir=tmp_path / "absent.tls")
        assert rig.cycle() is False
        assert len(errors(caplog)) == 1 and errors(caplog)[0].startswith(self.PREFIX)
        assert not (tmp_path / "absent.tls").exists()

    def test_a_distinct_message_is_an_error_again(self, make_rig, caplog, tmp_path):
        rig = make_rig()
        self.break_garbage(rig.tls_dir)
        rig.cycle()
        rig.cycle()
        self.break_key_mode(rig.tls_dir)          # a different failure (key mode now)
        (rig.tls_dir / "client.crt").unlink()
        rig.cycle()
        rig.cycle()
        assert levels(caplog, self.PREFIX) == ["ERROR", "DEBUG", "ERROR", "DEBUG"]

    def test_fixing_the_files_recovers_without_a_new_manager(self, make_rig, caplog, client_tls_dir):
        rig = make_rig()
        good = (client_tls_dir / "client.crt").read_bytes()
        self.break_garbage(rig.tls_dir)
        assert rig.cycle() is False
        (rig.tls_dir / "client.crt").write_bytes(good)
        assert rig.cycle() is True
        assert levels(caplog, "client TLS identity") == ["ERROR", "INFO"]

    def test_a_failed_load_is_not_retried_while_the_files_are_unchanged_within_a_cycle(self, make_rig, mocker):
        load = mocker.spy(remote, "load_identity")
        rig = make_rig(datasets=("pool/a", "pool/b", "pool/c"))
        self.break_garbage(rig.tls_dir)
        rig.mgr.begin_cycle()
        for index in range(3):
            assert rig.run(index) is False
        assert load.call_count == 1

    def test_a_new_cycle_retries_a_failed_load_even_when_the_stat_signature_is_unchanged(
        self, make_rig, mocker, caplog
    ):
        """`chmod` changes neither device, inode, size nor mtime: only `begin_cycle()` makes the
        manager look again."""
        load = mocker.spy(remote, "load_identity")
        rig = make_rig()
        self.break_key_mode(rig.tls_dir)
        assert rig.cycle() is False
        assert rig.run() is False and load.call_count == 1               # same cycle: not retried
        os.chmod(rig.tls_dir / "client.key", 0o600)
        assert rig.run() is False and load.call_count == 1               # still the same cycle
        assert rig.cycle() is True                                       # a new cycle: retried, works
        assert load.call_count == 2

    def test_without_begin_cycle_every_call_retries(self, make_rig, mocker):
        load = mocker.spy(remote, "load_identity")
        rig = make_rig()
        self.break_key_mode(rig.tls_dir)
        assert rig.run() is False
        os.chmod(rig.tls_dir / "client.key", 0o600)
        assert rig.run() is True                                          # no begin_cycle needed
        assert load.call_count == 2

    def test_an_interrupted_promotion_fails_with_its_own_explanation_and_recovers_when_finished(
        self, make_rig, caplog, tmp_path
    ):
        """`identity rotate` renames the key, then the certificate. Between the two the pair does not
        match: the manager refuses to use it and picks the new pair up once the CLI finishes."""
        rig = make_rig()
        nxt = new_client_identity(rig.tls_dir, name=ident.CLIENT_NEXT_NAME)
        os.replace(rig.tls_dir / "client.next.key", rig.tls_dir / "client.key")      # half promoted
        assert rig.cycle() is False
        assert "does not match private key" in errors(caplog)[0]
        os.replace(rig.tls_dir / "client.next.crt", rig.tls_dir / "client.crt")      # promotion done
        rig.route_whoami("client1")
        assert rig.cycle() is True
        assert rig.transport.identity_fp == nxt.fingerprint

    def test_ident_helper_from_the_cli_promotes_a_pair_the_manager_then_uses(self, make_rig, caplog):
        rig = make_rig()
        rig.cycle()
        before = rig.transport.identity_fp
        nxt = new_client_identity(rig.tls_dir, name=ident.CLIENT_NEXT_NAME)
        promoted = ident.promote_identity(rig.tls_dir)
        assert promoted.fingerprint == nxt.fingerprint != before
        assert rig.cycle() is True
        assert rig.transport.identity_fp == nxt.fingerprint


class TestConfiguredIdentityPaths:
    """`client_cert_file` / `client_key_file` name the identity (both or neither)."""

    def test_the_configured_files_are_used_instead_of_the_tls_dir(self, make_rig, tmp_path, caplog):
        custom = new_client_identity(tmp_path / "custom.tls")
        rig = make_rig(client_cert_file=custom.cert_path, client_key_file=custom.key_path)
        shutil.rmtree(rig.tls_dir)                                          # the default location is gone
        rig.client_fp = custom.fingerprint
        rig.route_whoami("client1")
        assert rig.cycle() is True
        assert rig.transport.identity_fp == custom.fingerprint

    def test_only_one_of_the_two_falls_back_to_the_tls_dir(self, make_rig):
        rig = make_rig(client_cert_file=Path("/nonexistent/client.crt"))
        assert rig.cycle() is True                          # the TLS dir's client.{crt,key}

    def test_a_missing_configured_file_is_an_identity_error(self, make_rig, caplog, tmp_path):
        rig = make_rig(client_cert_file=tmp_path / "no.crt", client_key_file=tmp_path / "no.key")
        assert rig.cycle() is False
        assert "cannot use the client TLS identity" in errors(caplog)[0]
        assert str(tmp_path / "no.key") in errors(caplog)[0] or str(tmp_path / "no.crt") in errors(caplog)[0]
        rig.assert_nothing_sent()


class TestOddIdentityFiles:
    """What the file-based client id tests guarded, for the client TLS identity files: an odd file is
    reported (never read for ever, never overwritten, never replaced by a generated one) and the
    manager recovers when it is fixed."""

    def within(self, seconds, fn):
        box = {}

        def run():
            try:
                box["result"] = fn()
            except BaseException as e:  # noqa: BLE001
                box["error"] = e

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join(seconds)
        assert not thread.is_alive(), f"blocked for more than {seconds}s"
        if "error" in box:
            raise box["error"]
        return box["result"]

    @pytest.fixture
    def custom(self, tmp_path):
        """A valid identity whose certificate path the test can swap for something odd."""
        ident_ = new_client_identity(tmp_path / "custom.tls")
        return ident_

    def test_a_fifo_certificate_is_refused_without_blocking_and_left_alone(
        self, make_rig, custom, tmp_path, caplog
    ):
        fifo = tmp_path / "cert.fifo"
        os.mkfifo(fifo)
        rig = make_rig(client_cert_file=fifo, client_key_file=custom.key_path)
        assert self.within(10, rig.cycle) is False
        assert "not a regular file" in errors(caplog)[0]
        import stat
        assert stat.S_ISFIFO(os.stat(fifo).st_mode)
        rig.assert_nothing_sent()

    def test_a_symlink_to_a_fifo_is_refused_too(self, make_rig, custom, tmp_path, caplog):
        fifo = tmp_path / "cert.fifo"
        os.mkfifo(fifo)
        link = tmp_path / "cert.link"
        link.symlink_to(fifo)
        rig = make_rig(client_cert_file=link, client_key_file=custom.key_path)
        assert self.within(10, rig.cycle) is False
        assert "not a regular file" in errors(caplog)[0]

    def test_a_directory_is_refused_and_left_alone(self, make_rig, custom, tmp_path, caplog):
        d = tmp_path / "cert.d"
        d.mkdir()
        rig = make_rig(client_cert_file=d, client_key_file=custom.key_path)
        assert rig.cycle() is False
        assert "not a regular file" in errors(caplog)[0]
        assert d.is_dir() and list(d.iterdir()) == []

    def test_an_oversized_file_is_refused_and_never_overwritten(self, make_rig, custom, tmp_path, caplog):
        big = tmp_path / "cert.big"
        big.write_bytes(b"important " * 300_000)
        before = big.read_bytes()
        rig = make_rig(client_cert_file=big, client_key_file=custom.key_path)
        assert rig.cycle() is False
        assert "that is not a key or certificate" in errors(caplog)[0]
        assert big.read_bytes() == before

    def test_a_dangling_symlink_is_missing(self, make_rig, custom, tmp_path, caplog):
        link = tmp_path / "cert.dangling"
        link.symlink_to(tmp_path / "nowhere")
        rig = make_rig(client_cert_file=link, client_key_file=custom.key_path)
        assert rig.cycle() is False
        assert "does not exist" in errors(caplog)[0]
        assert not (tmp_path / "nowhere").exists()                  # nothing was written through it

    def test_a_symlink_to_a_valid_certificate_is_followed(self, make_rig, custom, tmp_path, caplog):
        link = tmp_path / "cert.link"
        link.symlink_to(custom.cert_path)
        rig = make_rig(client_cert_file=link, client_key_file=custom.key_path)
        rig.client_fp = custom.fingerprint
        rig.route_whoami("client1")
        assert rig.cycle() is True

    def test_an_empty_certificate_file_is_an_error_not_a_trigger_to_generate_one(
        self, make_rig, custom, tmp_path, caplog
    ):
        empty = tmp_path / "cert.empty"
        empty.write_text("")
        rig = make_rig(client_cert_file=empty, client_key_file=custom.key_path)
        assert rig.cycle() is False
        assert len(errors(caplog)) == 1
        assert empty.read_text() == ""                              # never rewritten

    def test_the_daemon_side_never_generates_a_tls_identity(self, make_rig, tmp_path):
        absent = tmp_path / "gen.tls"
        rig = make_rig(tls_dir=absent)
        rig.cycle()
        rig.cycle()
        assert not absent.exists()


# ---------------------------------------------------------------------------
# The error-mapping table
# ---------------------------------------------------------------------------

WHERE = "server https://backup.example.com (destination offsite)"


def expect_pin(rig):
    return [
        f"{WHERE} presented certificate {ACTUAL_FP}, but the destination is pinned to {SERVER_FP}",
        "possible MITM or the server was re-keyed", f"re-enroll ({ENROLL})",
    ]


def expect_alert(rig, reason="TLSV1_ALERT_UNKNOWN_CA"):
    return [f"{WHERE} does not trust this client {rig.client_fp} (TLS alert {reason})",
            f"re-enroll ({ENROLL}){ENROLL_HINT}"]


def expect_not_trusted(rig):
    return [f"{WHERE} does not trust this client {rig.client_fp} (revoked, expired or never enrolled)",
            f"re-enroll ({ENROLL}){ENROLL_HINT}"]


def mismatch_message(rig, bound):
    """The 403 `client_id mismatch` text: this certificate stays bound to the old id on the server, so
    the way forward is the old id back, or a revoke of that binding plus a NEW key."""
    return (
        f"{WHERE} binds this client's certificate {rig.client_fp} to client id '{bound}', but this "
        f"host's client id is 'client1'. Either restore the old id "
        f"(`zfsbackup-config identity set-client-id {bound}`), or have the server operator revoke the "
        f"old binding (`zfsbackup-config trust revoke --client {bound}`) and then re-enroll with a "
        "fresh key (`zfsbackup-config enroll offsite --new-key --token-file …`)"
    )


def expect_mismatch(rig):
    return [mismatch_message(rig, "other-id")]


def expect_401(rig):
    return [f"{WHERE} did not receive this client's certificate {rig.client_fp} (HTTP 401): "
            "client certificate required"]


def expect_other_403(rig):
    return [f"{WHERE} refused this client (HTTP 403): nope"]


#: kind -> (HTTP response kwargs for requests_mock, websocket failure, expected fragments)
def _http(kind):
    return {
        "pin": dict(exc=urllib3_pin_error()),
        "alert": dict(exc=tls_alert(raw=False)),
        "not-trusted": dict(status_code=403, json={"error": "client not trusted"}),
        "mismatch": dict(status_code=403, json={"error": "client_id mismatch", "bound": "other-id"}),
        "401": dict(status_code=401, json={"error": "client certificate required"}),
        "other-403": dict(status_code=403, json={"error": "nope"}),
    }[kind]


def _websocket_failure(kind, rig):
    """-> `(connect_pinned side effect | None, ws.connect side effect | None)`."""
    return {
        "pin": (PinMismatch(SERVER_FP, ACTUAL_FP), None),
        "alert": (None, tls_alert()),
        "not-trusted": (None, bad_status(403, "client not trusted")),
        "mismatch": (None, bad_status(403, "client_id mismatch", bound="other-id")),
        "401": (None, bad_status(401, "client certificate required")),
        "other-403": (None, bad_status(403, "nope")),
    }[kind]


EXPECT = {
    "pin": expect_pin, "alert": expect_alert, "not-trusted": expect_not_trusted,
    "mismatch": expect_mismatch, "401": expect_401, "other-403": expect_other_403,
}
KINDS = list(EXPECT)
HTTP_STAGES = ["whoami", "negotiate", "resume_token"]


class TestRefusalMapping:
    """Rows 4-8: PinMismatch, TLS alert, 403 not trusted, 403 id mismatch, 401 / other 403 -- at each
    place they can arrive. Each is ERROR once, the destination is skipped for the rest of the cycle,
    and nothing is sent unless the stream had already started (then it is reaped)."""

    @pytest.mark.parametrize("stage", HTTP_STAGES)
    @pytest.mark.parametrize("kind", KINDS)
    def test_http_stages(self, make_rig, caplog, kind, stage):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        rig.refuse(stages=(stage,), **_http(kind))
        assert rig.cycle(0) is False
        (message,) = errors(caplog)
        for fragment in EXPECT[kind](rig):
            assert fragment in message
        # skipped for the rest of the cycle: the next dataset makes no request and logs nothing
        count, logged = rig.http_count, len(own(caplog))
        assert rig.run(1) is False
        assert rig.http_count == count and len(own(caplog)) == logged
        # never a send, never a pin, never a websocket
        assert rig.sends == [] and rig.fake_connect.calls == []
        rig.ws_cls.assert_not_called()
        rig.set_inflight.assert_not_called()
        rig.set_anchor.assert_not_called()

    @pytest.mark.parametrize("kind", KINDS)
    def test_only_the_untrusted_refusals_end_with_the_enroll_hint(self, make_rig, caplog, kind):
        """The hint tells a revoked certificate (`--new-key`) from a re-keyed host (plain
        `enroll`); it belongs to the TLS-alert and "client not trusted" refusals only."""
        rig = make_rig()
        rig.refuse(stages=("whoami",), **_http(kind))
        rig.cycle(0)
        (message,) = errors(caplog)
        assert message.endswith(ENROLL_HINT) == (kind in ("alert", "not-trusted"))
        if kind in ("alert", "not-trusted"):
            assert message.count("--new-key") == 1 and "if it was revoked" not in message

    @pytest.mark.parametrize("kind", KINDS)
    def test_websocket_stage_reaps_the_started_send(self, make_rig, caplog, kind):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        connect_effect, ws_effect = _websocket_failure(kind, rig)
        rig.fake_connect.side_effect = connect_effect
        rig.ws.connect.side_effect = ws_effect
        assert rig.cycle(0) is False
        (message,) = errors(caplog)
        for fragment in EXPECT[kind](rig):
            assert fragment in message
        # the send WAS started (the pin is written first) and is reaped: closed exactly once
        assert len(rig.sends) == 1 and rig.streams[0].closes == 1
        rig.set_inflight.assert_called_once()
        rig.clear_inflight.assert_not_called()
        rig.set_anchor.assert_not_called()
        rig.ws.send_binary.assert_not_called()
        # destination skipped for the rest of the cycle: no further whoami/negotiate/send
        count, calls = rig.http_count, len(rig.send_snapshot.call_args_list)
        assert rig.run(1) is False
        assert rig.http_count == count and len(rig.send_snapshot.call_args_list) == calls
        assert [lv for lv, _ in own(caplog) if lv == "ERROR"] == ["ERROR"]

    @pytest.mark.parametrize("kind", ["not-trusted", "mismatch", "401", "other-403", "alert"])
    def test_a_websocket_refusal_is_not_probed_for_a_server_message(self, make_rig, kind):
        """An auth refusal is decided from the failed upgrade itself: the 2 s `recv` probe for a
        refusal left in the socket is for BrokenPipe and friends."""
        rig = make_rig()
        _, ws_effect = _websocket_failure(kind, rig)
        rig.ws.connect.side_effect = ws_effect
        rig.cycle()
        rig.ws.recv.assert_not_called()

    def test_a_pin_mismatch_in_the_websocket_connect_never_reaches_the_websocket(self, rig):
        rig.fake_connect.side_effect = PinMismatch(SERVER_FP, ACTUAL_FP)
        rig.cycle()
        rig.ws.connect.assert_not_called()          # the upgrade is never attempted on the wrong server
        rig.ws.send_binary.assert_not_called()

    def test_the_websocket_upgrade_body_is_read_for_the_reason(self, make_rig, caplog):
        """403 without a JSON body is "refused this client (HTTP 403): forbidden"."""
        rig = make_rig()
        rig.ws.connect.side_effect = websocket.WebSocketBadStatusException(
            "Handshake status 403", 403, "Forbidden", {}, b"<html>forbidden</html>")
        rig.cycle()
        assert errors(caplog) == [f"{WHERE} refused this client (HTTP 403): forbidden"]

    def test_a_non_auth_upgrade_failure_is_a_transfer_error_not_a_destination_refusal(self, make_rig, caplog):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        rig.ws.connect.side_effect = bad_status(500, "boom")
        assert rig.cycle(0) is False
        assert any(m.startswith("Transfer failed for pool/a -> https://backup.example.com") for m in errors(caplog))
        assert not [m for m in errors(caplog) if "refused this client" in m]
        # the destination is NOT skipped: the next dataset is attempted (a second send is started)
        assert rig.run(1) is False
        assert len(rig.sends) == 2

    def test_a_pin_mismatch_named_in_a_requests_connection_error(self, make_rig, caplog):
        rig = make_rig()
        rig.refuse(stages=("whoami",), exc=requests.exceptions.ConnectionError(PinMismatch(SERVER_FP, ACTUAL_FP)))
        rig.cycle()
        assert f"presented certificate {ACTUAL_FP}" in errors(caplog)[0]

    def test_an_unknown_presented_fingerprint_is_reported_as_unknown(self, make_rig, caplog):
        rig = make_rig()
        rig.refuse(stages=("whoami",), exc=requests.exceptions.SSLError("Fingerprints did not match"))
        rig.cycle()
        assert "presented certificate unknown, but the destination is pinned to" in errors(caplog)[0]

    @pytest.mark.parametrize("prefix", ["TLS_ALERT_", "TLSV1_ALERT_", "SSLV3_ALERT_"])
    @pytest.mark.parametrize("name", ["UNKNOWN_CA", "CERTIFICATE_EXPIRED", "BAD_CERTIFICATE",
                                      "UNSUPPORTED_CERTIFICATE"])
    @pytest.mark.parametrize("raw", [True, False], ids=["raw-ssl", "requests"])
    def test_every_rejection_alert_in_every_openssl_spelling_is_a_trust_refusal(
        self, make_rig, caplog, prefix, name, raw
    ):
        rig = make_rig()
        rig.refuse(stages=("whoami",), exc=tls_alert(prefix + name, raw=raw))
        assert rig.cycle() is False
        assert len(errors(caplog)) == 1
        assert f"(TLS alert {prefix}{name})" in errors(caplog)[0]
        assert "does not trust this client" in errors(caplog)[0]

    def test_a_local_certificate_verification_failure_is_not_a_trust_refusal(self, make_rig, caplog):
        """Only the server's alert about OUR certificate is one: a local verification error is an
        ordinary unreachable-destination failure."""
        rig = make_rig()
        exc = ssl.SSLCertVerificationError(1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed")
        exc.reason = "TLSV1_ALERT_UNKNOWN_CA"
        rig.refuse(stages=("whoami",), exc=requests.exceptions.SSLError(exc))
        rig.cycle()
        assert "does not trust this client" not in " ".join(errors(caplog))
        assert "cannot reach https://backup.example.com (destination offsite)" in errors(caplog)[0]


class TestDedupeAndRecovery:
    """`_refusal_logged[(None, destination)]`: ERROR on the first occurrence, DEBUG on repeats, INFO when a
    good `/whoami` shows the server accepts this client again."""

    @pytest.mark.parametrize("kind", KINDS)
    def test_a_standing_refusal_is_error_once_then_debug(self, make_rig, caplog, kind):
        rig = make_rig()
        rig.refuse(**_http(kind))
        for _ in range(4):
            assert rig.cycle() is False
        assert len(errors(caplog)) == 1
        message = errors(caplog)[0]
        assert [lv for lv, m in own(caplog) if m == message] == ["ERROR", "DEBUG", "DEBUG", "DEBUG"]
        assert rig.requests("/negotiate") == [] and rig.sends == []

    @pytest.mark.parametrize("kind", KINDS)
    def test_a_good_whoami_logs_info_and_clears_it_so_the_next_refusal_is_an_error_again(
        self, make_rig, caplog, kind
    ):
        rig = make_rig()
        rig.refuse(**_http(kind))
        rig.cycle()
        rig.heal()
        assert rig.cycle() is True
        assert levels(caplog, "accepts this client again as 'client1'") == ["INFO"]
        assert rig.mgr._refusal_logged == {}
        rig.refuse(**_http(kind))
        rig.cycle()
        assert len(errors(caplog)) == 2 and errors(caplog)[0] == errors(caplog)[1]

    def test_no_recovery_line_without_a_prior_refusal(self, rig, caplog):
        rig.cycle()
        rig.cycle()
        assert levels(caplog, "accepts this client again") == []

    def test_the_recovery_line_names_the_destination_and_the_bound_id(self, rig, caplog):
        rig.refuse(**_http("not-trusted"))
        rig.cycle()
        rig.heal()
        rig.cycle()
        assert (
            "INFO",
            "server https://backup.example.com (destination offsite) accepts this client again as 'client1'",
        ) in own(caplog)

    def test_a_different_refusal_message_is_an_error_again(self, rig, caplog):
        rig.refuse(**_http("not-trusted"))
        rig.cycle()
        rig.cycle()
        rig.refuse(**_http("mismatch"))
        rig.cycle()
        rig.cycle()
        assert [lv for lv, m in own(caplog) if "does not trust" in m or "binds this" in m] == [
            "ERROR", "DEBUG", "ERROR", "DEBUG"]

    def test_destinations_are_deduplicated_independently(self, make_rig, caplog):
        rig = make_rig(destinations={
            "offsite": Destination(url=BASE, server_fingerprint=SERVER_FP),
            "dc2": Destination(url="https://dc2.example.com", server_fingerprint=SERVER_FP),
        })
        rig.rm.get(WHOAMI, status_code=403, json={"error": "client not trusted"})
        rig.rm.get("https://dc2.example.com/whoami", status_code=403, json={"error": "client not trusted"})
        rig.mgr.begin_cycle()
        rig.run(dest="offsite")
        rig.run(dest="dc2")
        rig.mgr.begin_cycle()
        rig.run(dest="offsite")
        rig.run(dest="dc2")
        assert len(errors(caplog)) == 2           # one per destination, each once
        assert {"destination offsite" in m for m in errors(caplog)} == {True, False}
        # one destination recovering clears only its own entry
        rig.route_whoami("client1")
        rig.cycle(dest="offsite")
        assert levels(caplog, "accepts this client again") == ["INFO"]
        assert (None, "dc2") in rig.mgr._refusal_logged and (None, "offsite") not in rig.mgr._refusal_logged

    def test_the_dataset_level_refusal_and_the_destination_level_one_do_not_share_a_key(
        self, rig, caplog
    ):
        """D7 (`server refused <dataset>`) is keyed by (dataset, destination); the destination-level
        ones by (None, destination)."""
        rig.rm.post(NEGOTIATE, status_code=409, json={"error": "held elsewhere"})
        rig.cycle()
        rig.cycle()
        assert (rig.manager.datasets[0].name, "offsite") in rig.mgr._refusal_logged
        assert (None, "offsite") not in rig.mgr._refusal_logged
        assert levels(caplog, "server refused pool/data: held elsewhere") == ["ERROR", "DEBUG"]


class TestWhoamiAnswers:
    """The `/whoami` answer itself: which id the server binds, and answers that are not an answer."""

    def test_a_different_bound_id_is_the_mismatch_refusal(self, make_rig, caplog):
        rig = make_rig()
        rig.route_whoami(json=whoami_json("B-host", rig.client_fp))
        assert rig.cycle() is False
        assert errors(caplog) == [mismatch_message(rig, "B-host")]
        assert rig.requests("/negotiate") == [] and rig.sends == []

    def test_the_mismatch_is_deduplicated(self, make_rig, caplog):
        rig = make_rig()
        rig.route_whoami(json=whoami_json("B-host", rig.client_fp))
        for _ in range(3):
            rig.cycle()
        assert levels(caplog, "binds this client's certificate") == ["ERROR", "DEBUG", "DEBUG"]

    def test_the_http_403_mismatch_and_the_whoami_mismatch_read_alike(self, make_rig, caplog):
        a = make_rig()
        a.route_whoami(json=whoami_json("other-id", a.client_fp))
        a.cycle()
        text_from_whoami = errors(caplog)[0]
        caplog.clear()
        b = make_rig()
        b.refuse(stages=("whoami",), **_http("mismatch"))
        b.cycle()
        assert errors(caplog)[0] == text_from_whoami

    @pytest.mark.parametrize("answer", [
        dict(status_code=200, text="not json"),
        dict(status_code=200, json=["a list"]),
        dict(status_code=200, json={"fingerprint": CLIENT_FP}),
        dict(status_code=200, json={"client_id": 7}),
        dict(status_code=500, json={"error": "boom"}),
        dict(status_code=404, json={}),
        dict(status_code=502, text="bad gateway"),
        dict(status_code=503, json={"error": "remote_backup disabled"}),
        dict(status_code=503, json={"error": "x", "retry": False}),
    ], ids=["not-json", "list", "no-id", "int-id", "500", "404", "502", "503-no-retry", "503-retry-false"])
    def test_an_unusable_answer_is_an_error_naming_the_status_and_the_destination_is_skipped(
        self, make_rig, caplog, answer
    ):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        rig.route_whoami(**answer)
        assert rig.cycle(0) is False
        (message,) = errors(caplog)
        assert message.startswith("unexpected /whoami answer from https://backup.example.com (destination offsite): HTTP")
        assert message.endswith("; skipping it this cycle")
        count = rig.http_count
        assert rig.run(1) is False and rig.http_count == count          # skipped for the cycle
        assert rig.sends == [] and rig.requests("/negotiate") == []

    def test_an_unusable_answer_is_not_deduplicated(self, rig, caplog):
        rig.route_whoami(status_code=500)
        rig.cycle()
        rig.cycle()
        assert len(errors(caplog)) == 2

    def test_the_fingerprint_in_the_answer_is_informational(self, rig):
        rig.route_whoami(json=whoami_json("client1", "00" * 32))
        assert rig.cycle() is True

    def test_a_retryable_503_is_an_info_not_an_error_and_skips_the_destination_for_the_cycle(
        self, make_rig, caplog
    ):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        rig.route_whoami(status_code=503, json={"error": "server is reloading; retry later", "retry": True})
        assert rig.cycle(0) is False
        assert own(caplog) and [lv for lv, m in own(caplog) if "reloading" in m] == ["INFO"]
        assert (
            "INFO", "server https://backup.example.com is reloading; destination offsite skipped this cycle"
        ) in own(caplog)
        assert errors(caplog) == []
        count = rig.http_count
        assert rig.run(1) is False and rig.http_count == count
        rig.assert_nothing_sent()

    def test_the_reload_503_leaves_nothing_to_deduplicate_and_the_next_cycle_works(self, rig, caplog):
        rig.route_whoami(status_code=503, json={"retry": True})
        rig.cycle()
        assert rig.mgr._refusal_logged == {}
        rig.heal()
        assert rig.cycle() is True
        assert levels(caplog, "accepts this client again") == []

    def test_whoami_is_one_get_with_the_http_timeout_through_the_pinned_session(self, rig):
        rig.cycle()
        (request,) = rig.requests("/whoami")
        assert request.method == "GET" and request.timeout == HTTP_TIMEOUT
        assert request.url == "https://backup.example.com/whoami"


class TestProtocolVersion:
    """Plan S-5, client side. Every request names the wire protocol (`X-Zfsbackup-Protocol`): the
    session's default header for HTTP and `header=` on the websocket upgrade. A server that answers
    426, or whose `/whoami` lacks a supported `protocol`, is a destination-level refusal like an
    auth one: ONE deduplicated ERROR, the destination is skipped for the cycle, no `zfs send` is
    started (the websocket stage reaps the send it had already begun), and a good `/whoami`
    later logs the recovery."""

    HEADER = {"X-Zfsbackup-Protocol": "1"}
    UPGRADE = ("Upgrade the client and the server to the same zfsbackup release; "
               "the destination is skipped until then")

    @staticmethod
    def refused_text(theirs):
        return (f"{WHERE} refuses this client's zfsbackup protocol 1 (HTTP 426); it speaks {theirs}. "
                f"{TestProtocolVersion.UPGRADE}")

    @staticmethod
    def whoami_text(theirs):
        return (f"{WHERE} speaks {theirs}; this client speaks zfsbackup protocol 1. "
                f"{TestProtocolVersion.UPGRADE}")

    NO_VERSION = "no protocol version (an older zfsbackup release)"

    # -- the header is sent ---------------------------------------------------------------------
    def test_every_http_request_names_the_protocol(self, rig):
        assert rig.cycle() is True
        assert [r.path for r in rig.rm.request_history] == [
            "/whoami", "/backup/client1/pool/data/negotiate", "/backup/client1/pool/data/resume_token"]
        for request in rig.rm.request_history:
            assert request.headers.get("X-Zfsbackup-Protocol") == "1", request.path

    def test_every_destination_session_sends_it(self, make_rig):
        rig = make_rig(destinations={
            "offsite": Destination(url=BASE, server_fingerprint=SERVER_FP),
            "dc2": Destination(url="https://dc2.example.com", server_fingerprint=SERVER_FP),
        })
        rig.mgr.begin_cycle()
        assert rig.run(dest="offsite") is True and rig.run(dest="dc2") is True
        hosts = {r.hostname for r in rig.rm.request_history}
        assert hosts == {"backup.example.com", "dc2.example.com"}
        assert all(r.headers.get("X-Zfsbackup-Protocol") == "1" for r in rig.rm.request_history)

    def test_the_websocket_upgrade_names_the_protocol(self, rig):
        assert rig.cycle() is True
        assert rig.ws.connect.call_args.kwargs["header"] == self.HEADER

    def test_a_rebuilt_transport_sends_it_too(self, rig):
        rig.cycle()
        rig.mgr.close()
        rig.rm.reset_mock()
        assert rig.cycle() is True
        assert rig.rm.request_history
        assert all(r.headers.get("X-Zfsbackup-Protocol") == "1" for r in rig.rm.request_history)

    def test_the_session_header_is_the_protocol_headers_dict(self, rig):
        rig.cycle()
        assert protocol_headers() == self.HEADER
        assert rig.transport.session.headers["X-Zfsbackup-Protocol"] == "1"

    # -- a 426 ----------------------------------------------------------------------------------
    @pytest.mark.parametrize("stage", HTTP_STAGES)
    @pytest.mark.parametrize("body, theirs", [
        (protocol_refusal_json(2), "protocol 2"),
        (protocol_refusal_json(7), "protocol 7"),
        ({"error": "x", "reason": "protocol"}, NO_VERSION),
        ({"error": "x", "reason": "protocol", "protocol": "2"}, NO_VERSION),
        ({"error": "x", "reason": "protocol", "protocol": True}, NO_VERSION),
        ({"error": "x", "reason": "protocol", "protocol": 2.0}, NO_VERSION),
        ({"error": "x", "reason": "protocol", "protocol": None}, NO_VERSION),
        ({}, NO_VERSION),
        ({"error": "something else", "reason": "other", "protocol": 3}, "protocol 3"),
        ("<html>426 Upgrade Required</html>", NO_VERSION),
        (b"", NO_VERSION),
    ], ids=["v2", "v7", "no-protocol", "string", "bool", "float", "null", "empty-json",
            "other-reason", "html", "empty-body"])
    def test_a_426_is_one_deduplicated_error_and_nothing_is_sent(
        self, make_rig, caplog, stage, body, theirs
    ):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        response = {"content": body} if isinstance(body, bytes) else (
            {"text": body} if isinstance(body, str) else {"json": body})
        rig.refuse(stages=(stage,), status_code=426, **response)
        assert rig.cycle(0) is False
        assert errors(caplog) == [self.refused_text(theirs)]
        # skipped for the rest of the cycle: the next dataset makes no request and logs nothing
        count, logged = rig.http_count, len(own(caplog))
        assert rig.run(1) is False
        assert rig.http_count == count and len(own(caplog)) == logged
        assert rig.sends == [] and rig.fake_connect.calls == []
        rig.ws_cls.assert_not_called()
        rig.set_inflight.assert_not_called()
        rig.set_anchor.assert_not_called()
        if stage == "whoami":
            assert rig.requests("/negotiate") == [] and rig.requests("/resume_token") == []

    def test_a_standing_426_is_error_once_then_debug(self, make_rig, caplog):
        """A server that speaks another protocol answers every endpoint with 426: the `/whoami`
        refusal is the one logged, and it keeps the later cycles quiet."""
        rig = make_rig()
        rig.refuse(status_code=426, json=protocol_refusal_json(2))
        for _ in range(4):
            assert rig.cycle() is False
        assert levels(caplog, "refuses this client's zfsbackup protocol") == ["ERROR", "DEBUG", "DEBUG", "DEBUG"]
        assert rig.requests("/negotiate") == [] and rig.sends == []

    def test_the_message_names_both_versions_and_the_remedy(self, make_rig, caplog):
        rig = make_rig()
        rig.refuse(stages=("whoami",), status_code=426, json=protocol_refusal_json(2))
        rig.cycle()
        (message,) = errors(caplog)
        assert "protocol 1" in message and "protocol 2" in message and "HTTP 426" in message
        assert "Upgrade the client and the server" in message and "skipped until then" in message

    def test_a_426_is_not_an_auth_refusal_message(self, make_rig, caplog):
        rig = make_rig()
        rig.refuse(stages=("whoami",), status_code=426, json=protocol_refusal_json(2))
        rig.cycle()
        (message,) = errors(caplog)
        assert "re-enroll" not in message and "does not trust" not in message

    def test_a_426_after_a_good_whoami_still_sends_nothing(self, make_rig, caplog):
        """A server that answered `/whoami` and then refuses the next request (it was downgraded in
        between): the same refusal at that stage, no resume-token request, no send."""
        rig = make_rig()
        rig.rm.post(NEGOTIATE, status_code=426, json=protocol_refusal_json(2))
        assert rig.cycle() is False
        assert len(rig.requests("/whoami")) == 1 and rig.requests("/resume_token") == []
        assert len(errors(caplog)) == 1 and rig.sends == []

    # -- a 426 on the websocket upgrade -----------------------------------------------------------
    @pytest.mark.parametrize("body, theirs", [
        ({"error": "x", "reason": "protocol", "protocol": 2, "supported": [2]}, "protocol 2"),
        ({"error": "x", "reason": "protocol"}, NO_VERSION),
        (None, NO_VERSION),
    ], ids=["v2", "no-protocol", "no-body"])
    def test_a_426_on_the_upgrade_reaps_the_started_send_and_skips_the_destination(
        self, make_rig, caplog, body, theirs
    ):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        if body is None:
            rig.ws.connect.side_effect = websocket.WebSocketBadStatusException(
                "Handshake status 426", 426, "Upgrade Required", {}, b"")
        else:
            rig.ws.connect.side_effect = bad_status(426, **body)
        assert rig.cycle(0) is False
        assert errors(caplog) == [self.refused_text(theirs)]
        assert len(rig.sends) == 1 and rig.streams[0].closes == 1          # started, then reaped
        rig.ws.send_binary.assert_not_called()
        rig.set_anchor.assert_not_called()
        rig.clear_inflight.assert_not_called()
        # the destination is skipped for the rest of the cycle: no second whoami / send
        count, sends = rig.http_count, len(rig.send_snapshot.call_args_list)
        assert rig.run(1) is False
        assert rig.http_count == count and len(rig.send_snapshot.call_args_list) == sends
        assert [lv for lv, _ in own(caplog) if lv == "ERROR"] == ["ERROR"]

    def test_a_426_on_the_upgrade_is_not_a_transfer_failure(self, rig, caplog):
        rig.ws.connect.side_effect = bad_status(426, "x", reason="protocol", protocol=2, supported=[2])
        rig.cycle()
        assert not [m for m in errors(caplog) if m.startswith("Transfer failed")]
        assert (None, "offsite") in rig.mgr._refusal_logged

    # -- /whoami without a supported protocol -------------------------------------------------------
    @pytest.mark.parametrize("protocol, theirs", [
        (None, NO_VERSION),               # the key is absent: a server that predates the check
        (2, "protocol 2"), (0, "protocol 0"), (-1, "protocol -1"), (99, "protocol 99"),
        ("1", NO_VERSION), (True, NO_VERSION), (False, NO_VERSION), (1.0, NO_VERSION),
        ([1], NO_VERSION), ({"v": 1}, NO_VERSION),
    ], ids=["missing", "two", "zero", "minus-one", "ninety-nine", "string-one", "true", "false",
            "float-one", "list", "dict"])
    def test_a_whoami_without_a_supported_protocol_is_refused(self, make_rig, caplog, protocol, theirs):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        body = whoami_json("client1", rig.client_fp, protocol=None)
        if protocol is not None:
            body["protocol"] = protocol
        rig.route_whoami(json=body)
        assert rig.cycle(0) is False
        assert errors(caplog) == [self.whoami_text(theirs)]
        count = rig.http_count
        assert rig.run(1) is False and rig.http_count == count          # skipped for the cycle
        assert rig.sends == [] and rig.requests("/negotiate") == []
        rig.ws_cls.assert_not_called()

    def test_a_json_null_protocol_is_refused_like_a_missing_one(self, rig, caplog):
        body = whoami_json("client1", rig.client_fp, protocol=None)
        body["protocol"] = None
        rig.route_whoami(json=body)
        assert rig.cycle() is False
        assert errors(caplog) == [self.whoami_text(self.NO_VERSION)]

    def test_a_whoami_refusal_is_deduplicated(self, rig, caplog):
        rig.route_whoami(json=whoami_json("client1", rig.client_fp, protocol=2))
        for _ in range(3):
            rig.cycle()
        assert levels(caplog, "this client speaks zfsbackup protocol") == ["ERROR", "DEBUG", "DEBUG"]

    def test_the_protocol_is_judged_before_the_bound_id(self, rig, caplog):
        """An unsupported server is not asked who it thinks we are: that answer cannot be trusted."""
        rig.route_whoami(json=whoami_json("B-host", rig.client_fp, protocol=2))
        rig.cycle()
        assert errors(caplog) == [self.whoami_text("protocol 2")]

    def test_an_answer_without_a_client_id_is_still_the_unexpected_answer(self, rig, caplog):
        rig.route_whoami(json={"fingerprint": rig.client_fp, "protocol": 2})
        rig.cycle()
        (message,) = errors(caplog)
        assert message.startswith("unexpected /whoami answer from")
        assert "protocol" not in message

    def test_a_supported_protocol_with_an_ignored_fingerprint_passes(self, rig):
        rig.route_whoami(json=whoami_json("client1", "00" * 32, protocol=1))
        assert rig.cycle() is True

    def test_a_refusal_is_an_error_even_on_the_very_first_cycle_of_a_new_manager(self, make_rig, caplog):
        for _ in range(2):
            rig = make_rig()
            rig.route_whoami(json=whoami_json("client1", rig.client_fp, protocol=2))
            rig.cycle()
        assert len(errors(caplog)) == 2

    # -- recovery -------------------------------------------------------------------------------
    @pytest.mark.parametrize("how", ["426-whoami", "426-negotiate", "whoami-v2", "whoami-missing"])
    def test_an_upgraded_server_logs_the_recovery_and_re_arms_the_error(self, make_rig, caplog, how):
        rig = make_rig()

        def break_it():
            if how == "426-whoami":
                rig.refuse(stages=("whoami",), status_code=426, json=protocol_refusal_json(2))
            elif how == "426-negotiate":
                rig.refuse(stages=("negotiate",), status_code=426, json=protocol_refusal_json(2))
            elif how == "whoami-v2":
                rig.route_whoami(json=whoami_json("client1", rig.client_fp, protocol=2))
            else:
                rig.route_whoami(json=whoami_json("client1", rig.client_fp, protocol=None))

        break_it()
        assert rig.cycle() is False
        rig.heal()                                  # the server is upgraded
        assert rig.cycle() is True
        assert levels(caplog, "accepts this client again as 'client1'") == ["INFO"]
        assert rig.mgr._refusal_logged == {}
        assert len(rig.sends) == 1                  # and it sends again
        break_it()
        assert rig.cycle() is False
        assert len(errors(caplog)) == 2 and errors(caplog)[0] == errors(caplog)[1]

    def test_a_server_that_changes_its_version_is_a_new_error(self, rig, caplog):
        rig.refuse(stages=("whoami",), status_code=426, json=protocol_refusal_json(2))
        rig.cycle()
        rig.cycle()
        rig.refuse(stages=("whoami",), status_code=426, json=protocol_refusal_json(3))
        rig.cycle()
        assert [lv for lv, m in own(caplog) if "refuses this client's zfsbackup protocol" in m] == [
            "ERROR", "DEBUG", "ERROR"]

    def test_a_protocol_refusal_then_an_auth_refusal_are_separate_errors(self, rig, caplog):
        rig.refuse(stages=("whoami",), status_code=426, json=protocol_refusal_json(2))
        rig.cycle()
        rig.refuse(stages=("whoami",), status_code=403, json={"error": "client not trusted"})
        rig.cycle()
        assert len(errors(caplog)) == 2
        assert "protocol" in errors(caplog)[0] and "does not trust" in errors(caplog)[1]

    def test_destinations_refuse_independently(self, make_rig, caplog):
        rig = make_rig(destinations={
            "offsite": Destination(url=BASE, server_fingerprint=SERVER_FP),
            "dc2": Destination(url="https://dc2.example.com", server_fingerprint=SERVER_FP),
        })
        rig.rm.get(WHOAMI, status_code=426, json=protocol_refusal_json(2))
        rig.mgr.begin_cycle()
        assert rig.run(dest="offsite") is False
        assert rig.run(dest="dc2") is True                      # the other destination is unaffected
        assert len(errors(caplog)) == 1 and "destination offsite" in errors(caplog)[0]
        assert len(rig.sends) == 1


class TestResetHeuristics:
    """Row 9/10: under TLS 1.3 a server that rejects our certificate sometimes shows up as a reset or
    broken pipe after the handshake instead of an alert: retry `/whoami` once, then report it."""

    RESET = (
        "server https://backup.example.com (destination offsite): connection reset after TLS handshake "
        "-- the server may have rejected this client's certificate {fp}"
    )

    @pytest.mark.parametrize("kind", ["reset", "pipe", "hangup"])
    def test_two_in_a_row_is_an_error_after_exactly_one_retry(self, make_rig, caplog, kind):
        rig = make_rig()
        rig.route_whoami(exc=reset_error(kind))
        assert rig.cycle() is False
        assert len(rig.requests("/whoami")) == 2
        assert errors(caplog) == [self.RESET.format(fp=rig.client_fp)]
        rig.assert_nothing_sent()

    def test_the_first_attempt_is_only_a_debug_line(self, make_rig, caplog):
        rig = make_rig()
        rig.route_whoami(exc=reset_error("reset"))
        rig.cycle()
        attempts = [(lv, m) for lv, m in own(caplog) if "/whoami at https://backup.example.com" in m]
        assert [lv for lv, _ in attempts] == ["DEBUG", "DEBUG"]
        assert "attempt 1" in attempts[0][1] and "attempt 2" in attempts[1][1]

    def test_a_reset_that_goes_away_on_the_retry_is_a_success_with_no_error(self, make_rig, caplog):
        rig = make_rig()
        rig.rm.get(WHOAMI, [{"exc": reset_error("reset")}, {"json": whoami_json("client1", rig.client_fp)}])
        assert rig.cycle() is True
        assert len(rig.requests("/whoami")) == 2
        assert errors(caplog) == [] and levels(caplog, "accepts this client again") == []

    def test_a_reset_then_a_broken_pipe_is_one_finding(self, make_rig, caplog):
        rig = make_rig()
        rig.rm.get(WHOAMI, [{"exc": reset_error("reset")}, {"exc": reset_error("pipe")}])
        rig.cycle()
        assert errors(caplog) == [self.RESET.format(fp=rig.client_fp)]

    def test_a_reset_then_another_failure_is_cannot_reach(self, make_rig, caplog):
        rig = make_rig()
        rig.rm.get(WHOAMI, [{"exc": reset_error("reset")},
                            {"exc": requests.exceptions.ConnectionError(ConnectionRefusedError("refused"))}])
        assert rig.cycle() is False
        assert len(rig.requests("/whoami")) == 2
        assert len(errors(caplog)) == 1 and errors(caplog)[0].startswith(
            "cannot reach https://backup.example.com (destination offsite): ")
        assert "connection reset after TLS handshake" not in errors(caplog)[0]

    def test_a_reset_then_an_alert_is_the_trust_refusal(self, make_rig, caplog):
        rig = make_rig()
        rig.rm.get(WHOAMI, [{"exc": reset_error("reset")}, {"exc": tls_alert(raw=False)}])
        rig.cycle()
        assert len(errors(caplog)) == 1 and "does not trust this client" in errors(caplog)[0]

    def test_the_error_is_deduplicated_across_cycles_and_cleared_by_a_good_whoami(self, make_rig, caplog):
        rig = make_rig()
        rig.route_whoami(exc=reset_error("reset"))
        for _ in range(3):
            rig.cycle()
        text = self.RESET.format(fp=rig.client_fp)
        assert [lv for lv, m in own(caplog) if m == text] == ["ERROR", "DEBUG", "DEBUG"]
        rig.heal()
        assert rig.cycle() is True
        assert levels(caplog, "accepts this client again") == ["INFO"]
        rig.route_whoami(exc=reset_error("pipe"))
        rig.cycle()
        assert [lv for lv, m in own(caplog) if m == text] == ["ERROR", "DEBUG", "DEBUG", "ERROR"]

    def test_skipped_for_the_cycle(self, make_rig):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        rig.route_whoami(exc=reset_error("reset"))
        rig.cycle(0)
        count = rig.http_count
        assert rig.run(1) is False and rig.http_count == count

    @pytest.mark.parametrize("exc", [
        tls_alert(raw=False), urllib3_pin_error(), requests.exceptions.ConnectionError(ConnectionRefusedError()),
        requests.exceptions.Timeout("slow"), requests.exceptions.SSLError("handshake failure"),
        ValueError("anything"),
    ], ids=["alert", "pin", "refused", "timeout", "ssl", "other"])
    def test_anything_else_is_never_retried(self, make_rig, exc):
        rig = make_rig()
        rig.route_whoami(exc=exc)
        rig.cycle()
        assert len(rig.requests("/whoami")) == 1

    def test_a_reset_on_negotiate_is_a_negotiation_failure_not_a_whoami_retry(self, make_rig, caplog):
        """The retry is for `/whoami` only: negotiate/resume_token/stream failures keep their own handling."""
        rig = make_rig()
        rig.rm.post(NEGOTIATE, exc=reset_error("reset"))
        assert rig.cycle() is False
        assert len(rig.requests("/negotiate")) == 1
        assert any(m.startswith("Negotiation failed for pool/data at https://backup.example.com") for m in errors(caplog))
        assert rig.sends == []


class TestOtherWhoamiFailures:
    """Row 11: any other `/whoami` failure -> "cannot reach", NOT deduplicated, skipped for the cycle."""

    @pytest.mark.parametrize("exc", [
        requests.exceptions.ConnectionError(ConnectionRefusedError(errno.ECONNREFUSED, "refused")),
        requests.exceptions.Timeout("slow"), requests.exceptions.ConnectTimeout("no route"),
        requests.exceptions.SSLError("[SSL: WRONG_VERSION_NUMBER] wrong version number"),
        requests.exceptions.ChunkedEncodingError("x"), requests.exceptions.InvalidURL("bad"),
    ], ids=["refused", "timeout", "connect-timeout", "ssl", "chunked", "invalid-url"])
    def test_cannot_reach_every_cycle(self, make_rig, caplog, exc):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        rig.route_whoami(exc=exc)
        for _ in range(3):
            assert rig.cycle(0) is False
        got = errors(caplog)
        assert len(got) == 3                                           # not deduplicated
        for message in got:
            assert message.startswith("cannot reach https://backup.example.com (destination offsite): ")
            assert message.endswith("; skipping it this cycle")
        assert len(rig.requests("/whoami")) == 3                       # one attempt per cycle, no retry
        count = rig.http_count
        assert rig.run(1) is False and rig.http_count == count          # skipped within the cycle
        rig.assert_nothing_sent()

    def test_it_does_not_arm_the_deduplication_of_the_refusals(self, rig, caplog):
        rig.route_whoami(exc=requests.exceptions.Timeout("slow"))
        rig.cycle()
        assert rig.mgr._refusal_logged == {}
        rig.heal()
        rig.cycle()
        assert levels(caplog, "accepts this client again") == []


class TestReloadingAndBusyStaysPerDataset:
    """Row 12: a retryable 503 on negotiate / resume_token / the stream is unchanged -- it skips THIS
    dataset only; the destination is still usable for the next one."""

    RELOADING = {"error": "server is reloading; retry later", "retry": True}

    def test_negotiate_503_skips_the_dataset_only(self, make_rig, caplog):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        rig.rm.post(NEGOTIATE, [{"status_code": 503, "json": self.RELOADING},
                                {"json": {"common_snapshot": None}}])
        assert rig.cycle(0) is False
        assert ("INFO", "server https://backup.example.com is reloading; skipping pool/a this cycle") in own(caplog)
        assert errors(caplog) == []
        assert rig.run(1) is True                                       # the destination is still usable
        assert len(rig.requests("/negotiate")) == 2 and len(rig.requests("/whoami")) == 1

    def test_resume_token_503_skips_the_dataset_only(self, make_rig, caplog):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        rig.rm.get(RESUME_TOKEN, [{"status_code": 503, "json": self.RELOADING},
                                  {"json": {"resume_token": None}}])
        assert rig.cycle(0) is False
        assert ("INFO", "server https://backup.example.com is reloading; skipping pool/a this cycle") in own(caplog)
        assert errors(caplog) == [] and rig.sends == []
        assert rig.run(1) is True
        assert len(rig.sends) == 1

    def test_busy_negotiate_is_the_same(self, make_rig, caplog):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        rig.rm.post(NEGOTIATE, [{"status_code": 503, "json": {"error": "busy", "retry": True}},
                                {"json": {"common_snapshot": None}}])
        assert rig.cycle(0) is False
        assert errors(caplog) == []
        assert rig.run(1) is True

    def test_a_reloading_refusal_instead_of_the_final_ack_skips_the_dataset_only(self, make_rig, caplog):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        rig.ws.recv.side_effect = [json.dumps(self.RELOADING), json.dumps({"status": "ok"})]
        assert rig.cycle(0) is False
        assert ("INFO", "server https://backup.example.com is reloading; skipping pool/a this cycle") in own(caplog)
        assert errors(caplog) == []
        rig.set_anchor.assert_not_called()
        assert rig.run(1) is True


# ---------------------------------------------------------------------------
# Nothing is sent before the checks pass
# ---------------------------------------------------------------------------


class TestChecksRunBeforeAnySend:
    """One spy on the send spawn: whatever refuses the destination, `zfs send` is never started, no
    pin is written and no snapshot listing is read."""

    def setup_scenario(self, rig, name, tmp_path):
        if name == "no-client-id":
            rig.mgr.client_id = None
        elif name == "not-enrolled":
            rig.config.destinations["offsite"] = Destination(url=BASE)
        elif name == "http-url":
            rig.config.destinations["offsite"] = Destination(url="http://backup.example.com",
                                                             server_fingerprint=SERVER_FP)
        elif name == "bad-identity":
            (rig.tls_dir / "client.crt").write_text("garbage")
        elif name == "whoami-pin":
            rig.refuse(stages=("whoami",), **_http("pin"))
        elif name == "whoami-alert":
            rig.refuse(stages=("whoami",), **_http("alert"))
        elif name == "whoami-not-trusted":
            rig.refuse(stages=("whoami",), **_http("not-trusted"))
        elif name == "whoami-mismatch":
            rig.refuse(stages=("whoami",), **_http("mismatch"))
        elif name == "whoami-bound-elsewhere":
            rig.route_whoami(json=whoami_json("B", rig.client_fp))
        elif name == "whoami-401":
            rig.refuse(stages=("whoami",), **_http("401"))
        elif name == "whoami-reset":
            rig.route_whoami(exc=reset_error("reset"))
        elif name == "whoami-down":
            rig.route_whoami(exc=requests.exceptions.ConnectionError(ConnectionRefusedError()))
        elif name == "whoami-reloading":
            rig.route_whoami(status_code=503, json={"retry": True})
        else:
            raise AssertionError(name)

    SCENARIOS = [
        "no-client-id", "not-enrolled", "http-url", "bad-identity", "whoami-pin", "whoami-alert",
        "whoami-not-trusted", "whoami-mismatch", "whoami-bound-elsewhere", "whoami-401",
        "whoami-reset", "whoami-down", "whoami-reloading",
    ]

    @pytest.mark.parametrize("scenario", SCENARIOS)
    def test_zfs_send_is_never_spawned(self, make_rig, tmp_path, scenario):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        self.setup_scenario(rig, scenario, tmp_path)
        for _ in range(2):
            rig.mgr.begin_cycle()
            assert rig.run(0) is False and rig.run(1) is False
        rig.assert_nothing_sent()

    def test_the_stop_callback_is_not_even_consulted(self, make_rig, tmp_path):
        rig = make_rig()
        rig.mgr.client_id = None
        stop = MagicMock(return_value=False)
        assert rig.cycle(should_stop=stop) is False
        stop.assert_not_called()

    def test_check_order_client_id_then_enrollment_then_identity_then_whoami(self, make_rig, mocker, caplog):
        """Each earlier failure hides every later check: later work is never even attempted."""
        load = mocker.spy(remote, "load_identity")
        rig = make_rig(destinations={"offsite": Destination(url="http://backup.example.com")})
        (rig.tls_dir / "client.crt").write_text("garbage")
        rig.route_whoami(exc=ValueError("must not be called"))
        rig.mgr.client_id = None
        rig.cycle()
        assert [m for m in errors(caplog)] == [f"remote backup skipped: {NO_CLIENT_ID_MESSAGE}"]
        rig.mgr.client_id = "client1"
        caplog.clear()
        rig.cycle()
        assert len(errors(caplog)) == 1 and "is not enrolled" in errors(caplog)[0]
        load.assert_not_called()
        rig.config.destinations["offsite"] = Destination(url=BASE, server_fingerprint=SERVER_FP)
        caplog.clear()
        rig.cycle()
        assert len(errors(caplog)) == 1 and "cannot use the client TLS identity" in errors(caplog)[0]
        assert rig.http_count == 0

    def test_after_a_whoami_success_the_checks_do_not_repeat_within_a_cycle(self, make_rig):
        rig = make_rig(datasets=("pool/a", "pool/b", "pool/c"))
        rig.mgr.begin_cycle()
        for index in range(3):
            assert rig.run(index) is True
        assert len(rig.requests("/whoami")) == 1
        assert len(rig.requests("/negotiate")) == 3 and len(rig.sends) == 3


# ---------------------------------------------------------------------------
# Identity reload
# ---------------------------------------------------------------------------


class TestIdentityReload:
    """The client identity is reloaded when either file's (device, inode, size, mtime) changes, so a
    re-enroll or `identity rotate` takes effect without a restart or a new manager."""

    def other(self, tmp_path):
        return new_client_identity(tmp_path / "next.tls")

    def put(self, rig, ident_):
        """Swap `ident_`'s files into the manager's TLS dir (atomic renames: new inodes)."""
        for name, mode in (("client.key", 0o600), ("client.crt", 0o640)):
            tmp = rig.tls_dir / f".{name}.swap"
            shutil.copyfile(Path(ident_.cert_path).parent / name, tmp)
            os.chmod(tmp, mode)
            os.replace(tmp, rig.tls_dir / name)

    def test_unchanged_files_are_not_reloaded(self, rig, mocker):
        load = mocker.spy(remote, "load_identity")
        for _ in range(4):
            rig.cycle()
        assert load.call_count == 1

    def test_a_replaced_pair_is_loaded_and_the_session_and_whoami_follow(self, make_rig, mocker, caplog, tmp_path):
        load = mocker.spy(remote, "load_identity")
        rig = make_rig()
        rig.cycle()
        old_transport = rig.transport
        old_close = mocker.patch.object(old_transport.session, "close", wraps=old_transport.session.close)
        new = self.other(tmp_path)
        self.put(rig, new)
        rig.client_fp = new.fingerprint
        rig.route_whoami("client1")
        assert rig.cycle() is True
        assert load.call_count == 2
        assert rig.transport is not old_transport
        assert rig.transport.identity_fp == new.fingerprint
        old_close.assert_called()                                # the old pinned session was closed
        assert len(rig.requests("/whoami")) == 2                 # the new identity is checked afresh
        assert [m for lv, m in own(caplog) if "client TLS identity" in m and lv == "INFO"][-1].endswith(
            f"(fingerprint {new.fingerprint})")

    def test_a_replaced_pair_is_picked_up_in_the_same_cycle_too(self, make_rig, tmp_path, caplog):
        """The `/whoami` cache is keyed by (identity fingerprint, server pin): a new identity is never
        trusted on the old identity's answer, even mid-cycle."""
        rig = make_rig(datasets=("pool/a", "pool/b"))
        rig.cycle(0)
        new = self.other(tmp_path)
        self.put(rig, new)
        rig.client_fp = new.fingerprint
        rig.route_whoami("client1")
        assert rig.run(1) is True
        assert len(rig.requests("/whoami")) == 2

    def test_a_new_identity_the_server_does_not_trust_is_refused_before_any_send(self, make_rig, tmp_path, caplog):
        rig = make_rig()
        rig.cycle()
        sends = len(rig.sends)
        self.put(rig, self.other(tmp_path))
        rig.refuse(stages=("whoami",), **_http("not-trusted"))
        assert rig.cycle() is False
        assert len(rig.sends) == sends
        assert "does not trust this client" in errors(caplog)[0]

    @pytest.mark.parametrize("change", ["mtime", "size", "inode"])
    @pytest.mark.parametrize("which", ["client.crt", "client.key"])
    def test_each_stat_field_of_either_file_triggers_a_reload(self, make_rig, mocker, which, change):
        """Exactly one field differs each time (the others are restored), so each is shown to matter."""
        load = mocker.spy(remote, "load_identity")
        rig = make_rig()
        rig.cycle()
        path = rig.tls_dir / which
        before = os.stat(path)
        if change == "mtime":                                   # same inode, same size, new mtime
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns + 5_000_000_000))
        elif change == "size":                                  # same inode, new size, same mtime
            with open(path, "ab") as fh:
                fh.write(b"\n")
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        else:                                                   # new inode, same size, same mtime
            tmp = rig.tls_dir / "swap.tmp"
            shutil.copy2(path, tmp)
            os.replace(tmp, path)
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        after = os.stat(path)
        assert {
            "mtime": (after.st_ino == before.st_ino, after.st_size == before.st_size, after.st_mtime_ns != before.st_mtime_ns),
            "size": (after.st_ino == before.st_ino, after.st_size != before.st_size, after.st_mtime_ns == before.st_mtime_ns),
            "inode": (after.st_ino != before.st_ino, after.st_size == before.st_size, after.st_mtime_ns == before.st_mtime_ns),
        }[change] == (True, True, True)
        rig.cycle()
        assert load.call_count == 2

    def test_a_reload_of_the_same_certificate_keeps_the_session_and_the_whoami_cache(self, rig, mocker):
        rig.cycle()
        transport = rig.transport
        path = rig.tls_dir / "client.key"
        st = os.stat(path)
        os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 7_000_000_000))
        assert rig.run() is True                                 # same cycle, files touched
        assert rig.transport is transport
        assert len(rig.requests("/whoami")) == 1

    def test_the_old_session_is_not_closed_when_the_fingerprint_is_unchanged(self, rig, mocker):
        rig.cycle()
        close = mocker.patch.object(rig.transport.session, "close", wraps=rig.transport.session.close)
        path = rig.tls_dir / "client.crt"
        st = os.stat(path)
        os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 7_000_000_000))
        rig.cycle()
        close.assert_not_called()

    def test_a_valid_identity_that_becomes_unusable_drops_the_sessions_and_recovers(self, rig, caplog):
        rig.cycle()
        good = (rig.tls_dir / "client.key").read_bytes()
        (rig.tls_dir / "client.key").write_text("garbage")
        os.chmod(rig.tls_dir / "client.key", 0o600)
        assert rig.cycle() is False
        assert rig.mgr._transports == {}                          # nothing stale to reuse
        assert len(errors(caplog)) == 1
        (rig.tls_dir / "client.key").write_bytes(good)
        os.chmod(rig.tls_dir / "client.key", 0o600)
        assert rig.cycle() is True

    def test_the_configured_paths_are_watched_not_the_default_ones(self, make_rig, tmp_path, mocker):
        custom = new_client_identity(tmp_path / "custom.tls")
        load = mocker.spy(remote, "load_identity")
        rig = make_rig(client_cert_file=custom.cert_path, client_key_file=custom.key_path)
        rig.client_fp = custom.fingerprint
        rig.route_whoami("client1")
        rig.cycle()
        # touching the default location does nothing
        st = os.stat(rig.tls_dir / "client.crt")
        os.utime(rig.tls_dir / "client.crt", ns=(st.st_atime_ns, st.st_mtime_ns + 9_000_000_000))
        rig.cycle()
        assert load.call_count == 1
        st = os.stat(custom.cert_path)
        os.utime(custom.cert_path, ns=(st.st_atime_ns, st.st_mtime_ns + 9_000_000_000))
        rig.cycle()
        assert load.call_count == 2

    def test_a_changed_server_pin_rebuilds_the_transport(self, rig, mocker):
        rig.cycle()
        first = rig.transport
        close = mocker.patch.object(first.session, "close", wraps=first.session.close)
        rig.config.destinations["offsite"] = Destination(url=BASE, server_fingerprint=OTHER_SERVER_FP)
        assert rig.run() is True                                  # the cache key (identity, pin) changed
        assert rig.transport is not first and rig.transport.server_fp == OTHER_SERVER_FP
        assert len(rig.requests("/whoami")) == 2
        close.assert_called()
        assert rig.fake_connect.calls[-1][3] == OTHER_SERVER_FP

    def test_a_changed_url_rebuilds_the_transport(self, rig):
        rig.cycle()
        first = rig.transport
        rig.config.destinations["offsite"] = Destination(url="https://moved.example.com:8443",
                                                         server_fingerprint=SERVER_FP)
        rig.route_whoami("client1", "https://moved.example.com:8443")
        assert rig.cycle() is True
        assert rig.transport is not first
        assert rig.transport.base_url == "https://moved.example.com:8443"


# ---------------------------------------------------------------------------
# begin_cycle
# ---------------------------------------------------------------------------


class TestBeginCycle:
    def test_one_whoami_per_destination_per_cycle_however_many_datasets(self, make_rig):
        rig = make_rig(datasets=("pool/a", "pool/b", "pool/c", "pool/d"), destinations={
            "offsite": Destination(url=BASE, server_fingerprint=SERVER_FP),
            "dc2": Destination(url="https://dc2.example.com", server_fingerprint=SERVER_FP),
        })
        rig.rm.post(NEGOTIATE, json={"common_snapshot": None})
        rig.mgr.begin_cycle()
        for index in range(4):
            for dest in ("offsite", "dc2"):
                assert rig.run(index, dest) is True
        assert len(rig.requests("/whoami")) == 2
        assert len(rig.requests("/negotiate")) == 8

    def test_the_next_cycle_asks_again(self, rig):
        for _ in range(3):
            rig.cycle()
        assert len(rig.requests("/whoami")) == 3

    def test_without_begin_cycle_every_call_asks(self, make_rig):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        rig.run(0)
        rig.run(1)
        rig.run(0)
        assert len(rig.requests("/whoami")) == 3

    def test_a_refused_destination_stays_refused_for_the_cycle_and_is_retried_next_cycle(self, make_rig):
        rig = make_rig(datasets=("pool/a", "pool/b"))
        rig.route_whoami(status_code=500)
        rig.mgr.begin_cycle()
        assert rig.run(0) is False and rig.run(1) is False
        assert len(rig.requests("/whoami")) == 1
        rig.heal()
        assert rig.run(0) is False                                # still the same cycle
        assert len(rig.requests("/whoami")) == 1
        rig.mgr.begin_cycle()
        assert rig.run(0) is True

    def test_a_destination_that_recovers_mid_run_without_begin_cycle(self, make_rig):
        rig = make_rig()
        rig.route_whoami(status_code=500)
        assert rig.run() is False
        rig.heal()
        assert rig.run() is True

    def test_begin_cycle_does_not_forget_the_refusal_log_state(self, rig, caplog):
        rig.refuse(**_http("not-trusted"))
        rig.cycle()
        rig.mgr.begin_cycle()
        assert (None, "offsite") in rig.mgr._refusal_logged

    def test_begin_cycle_keeps_the_transports(self, rig):
        rig.cycle()
        transport = rig.transport
        rig.mgr.begin_cycle()
        assert rig.mgr._transports["offsite"] is transport

    def test_the_cache_is_per_destination(self, make_rig):
        rig = make_rig(destinations={
            "offsite": Destination(url=BASE, server_fingerprint=SERVER_FP),
            "dc2": Destination(url="https://dc2.example.com", server_fingerprint=SERVER_FP),
        })
        rig.rm.get("https://dc2.example.com/whoami", status_code=403, json={"error": "client not trusted"})
        rig.mgr.begin_cycle()
        assert rig.run(dest="dc2") is False
        assert rig.run(dest="offsite") is True               # one destination's refusal skips only itself

    def test_close_closes_every_session_and_is_idempotent(self, make_rig, mocker):
        rig = make_rig(destinations={
            "offsite": Destination(url=BASE, server_fingerprint=SERVER_FP),
            "dc2": Destination(url="https://dc2.example.com", server_fingerprint=SERVER_FP),
        })
        rig.rm.get("https://dc2.example.com/whoami", json=whoami_json("client1", rig.client_fp))
        rig.mgr.begin_cycle()
        rig.run(dest="offsite")
        rig.run(dest="dc2")
        sessions = [t.session for t in rig.mgr._transports.values()]
        assert len(sessions) == 2
        closes = [mocker.patch.object(s, "close", wraps=s.close) for s in sessions]
        rig.mgr.close()
        assert all(c.call_count == 1 for c in closes)
        assert rig.mgr._transports == {} and rig.mgr._checked == {}
        rig.mgr.close()
        assert all(c.call_count == 1 for c in closes)

    def test_a_failing_session_close_does_not_stop_the_others(self, rig, mocker):
        rig.cycle()
        mocker.patch.object(rig.transport.session, "close", side_effect=OSError("boom"))
        rig.mgr.close()                                          # must not raise
        assert rig.mgr._transports == {}

    def test_a_closed_manager_works_again(self, rig):
        rig.cycle()
        rig.mgr.close()
        assert rig.cycle() is True
        assert "offsite" in rig.mgr._transports


class TestClientCertExpiryCheck:
    """S-12 (2): `begin_cycle` -> `_check_client_cert_expiry`. On the first cycle of a manager and
    then at most every `CLIENT_CERT_EXPIRY_CHECK_EVERY` (24 h, monotonic): a WARNING when the client
    certificate expires within `CLIENT_CERT_EXPIRY_WARN_DAYS`, an ERROR once it has expired, nothing
    for a certificate that is far from expiry or for a file that does not exist, DEBUG for any other
    failure. It reads the certificate only, so it runs when the identity cannot be loaded too."""

    @pytest.fixture
    def clock(self, mocker):
        from _clock_support import FakeTime

        fake = FakeTime()
        mocker.patch.object(remote, "time", fake)
        return fake

    @staticmethod
    def cert(client_tls_dir, *, expires_in=None, expired_ago=None):
        """Replace the client certificate (and key) in `client_tls_dir`; -> the `ClientCert`."""
        import datetime as dt
        from _api_support import make_client_cert

        now = dt.datetime.now(dt.timezone.utc)
        if expired_ago is not None:
            made = make_client_cert(not_before=now - expired_ago - dt.timedelta(days=30),
                                    not_after=now - expired_ago)
        else:
            made = make_client_cert(not_before=now - dt.timedelta(hours=1),
                                    not_after=now + expires_in)
        write_identity_files(client_tls_dir, made)
        return made

    @staticmethod
    def reminders(caplog):
        """`[(level, message)]` of the expiry reminders (WARNING/ERROR) the manager logged."""
        return [(lv, m) for lv, m in own(caplog)
                if lv in ("WARNING", "ERROR") and m.startswith("the client certificate ")]

    @staticmethod
    def expected(path, made):
        return ident.client_cert_expiry_problem(path, made.not_after)

    def test_the_constants(self):
        assert remote.CLIENT_CERT_EXPIRY_CHECK_EVERY == 24 * 3600.0
        assert ident.CLIENT_CERT_EXPIRY_WARN_DAYS == 30

    # -- within the warning window ---------------------------------------------------------

    def test_within_thirty_days_the_first_cycle_warns_once(self, make_rig, caplog, client_tls_dir, clock):
        import datetime as dt
        made = self.cert(client_tls_dir, expires_in=dt.timedelta(days=10))
        rig = make_rig()
        assert rig.cycle() is True                                    # the backup itself goes ahead
        level, message = self.expected(client_tls_dir / "client.crt", made)
        assert level == logging.WARNING
        assert self.reminders(caplog) == [("WARNING", message)]
        assert str(client_tls_dir / "client.crt") in message and "(in 9 day(s))" in message
        assert "zfsbackup-config identity rotate" in message

    def test_and_nothing_on_the_later_cycles(self, make_rig, caplog, client_tls_dir, clock):
        import datetime as dt
        self.cert(client_tls_dir, expires_in=dt.timedelta(days=10))
        rig = make_rig()
        for _ in range(6):
            rig.cycle()
        assert len(self.reminders(caplog)) == 1

    def test_again_after_twenty_four_hours(self, make_rig, caplog, client_tls_dir, clock):
        import datetime as dt
        self.cert(client_tls_dir, expires_in=dt.timedelta(days=10))
        rig = make_rig()
        rig.cycle()
        clock.advance(24 * 3600 - 120)
        rig.cycle()
        assert len(self.reminders(caplog)) == 1                       # two minutes short of a day
        clock.advance(180)
        rig.cycle()
        assert len(self.reminders(caplog)) == 2
        rig.cycle()
        assert len(self.reminders(caplog)) == 2                       # and the next day begins
        clock.advance(24 * 3600)
        rig.cycle()
        assert len(self.reminders(caplog)) == 3

    def test_the_certificate_is_read_once_per_check_not_per_cycle(
        self, make_rig, client_tls_dir, clock, mocker
    ):
        import datetime as dt
        self.cert(client_tls_dir, expires_in=dt.timedelta(days=10))
        rig = make_rig()
        read = mocker.spy(remote.ident, "client_cert_not_after")
        for _ in range(4):
            rig.cycle()
        assert read.call_count == 1
        clock.advance(24 * 3600 + 1)
        rig.cycle()
        assert read.call_count == 2

    def test_the_day_count_follows_the_certificate(self, make_rig, caplog, client_tls_dir, clock):
        import datetime as dt
        self.cert(client_tls_dir, expires_in=dt.timedelta(days=2, hours=3))
        make_rig().cycle()
        assert "(in 2 day(s))" in self.reminders(caplog)[0][1]

    # -- expired ----------------------------------------------------------------------------------

    def test_an_expired_certificate_is_an_error_on_the_first_cycle_and_then_daily(
        self, make_rig, caplog, client_tls_dir, clock
    ):
        import datetime as dt
        made = self.cert(client_tls_dir, expired_ago=dt.timedelta(days=2))
        rig = make_rig()
        rig.cycle()
        level, message = self.expected(client_tls_dir / "client.crt", made)
        assert level == logging.ERROR and "expired on" in message
        assert self.reminders(caplog) == [("ERROR", message)]
        rig.cycle()
        rig.cycle()
        assert len(self.reminders(caplog)) == 1
        clock.advance(24 * 3600 + 1)
        rig.cycle()
        assert self.reminders(caplog) == [("ERROR", message)] * 2
        clock.advance(24 * 3600 + 1)
        rig.cycle()
        assert self.reminders(caplog) == [("ERROR", message)] * 3

    def test_the_identity_load_error_is_logged_once_while_the_expiry_error_repeats_daily(
        self, make_rig, caplog, client_tls_dir, clock
    ):
        """The decided contract: day one has both ERRORs (expiry, then the load failure); the
        load failure then drops to DEBUG on every cycle, the expiry ERROR comes back each day."""
        import datetime as dt
        self.cert(client_tls_dir, expired_ago=dt.timedelta(days=2))
        rig = make_rig()
        prefix = TestIdentityErrors.PREFIX
        for day in range(3):
            for _ in range(2):
                rig.cycle()
            clock.advance(24 * 3600 + 1)
        load_levels = [lv for lv, m in own(caplog) if m.startswith(prefix)]
        assert load_levels.count("ERROR") == 1 and load_levels[0] == "ERROR"
        assert load_levels.count("DEBUG") == 5
        assert [lv for lv, m in self.reminders(caplog)] == ["ERROR"] * 3
        first_error_order = [m for lv, m in own(caplog) if lv == "ERROR"]
        assert first_error_order[0].startswith("the client certificate ")
        assert first_error_order[1].startswith(prefix)

    def test_an_expired_certificate_still_skips_every_send(self, make_rig, client_tls_dir, clock):
        import datetime as dt
        self.cert(client_tls_dir, expired_ago=dt.timedelta(days=2))
        rig = make_rig()
        assert rig.cycle() is False
        rig.assert_nothing_sent()

    # -- silence ------------------------------------------------------------------------------------

    def test_a_certificate_far_from_expiry_is_silent(self, make_rig, caplog, clock):
        rig = make_rig()                                              # generated: ten years
        for _ in range(3):
            clock.advance(24 * 3600 + 1)
            assert rig.cycle() is True
        assert self.reminders(caplog) == []
        assert not [m for lv, m in own(caplog) if "expiry check" in m]

    def test_thirty_one_days_is_silent(self, make_rig, caplog, client_tls_dir, clock):
        import datetime as dt
        self.cert(client_tls_dir, expires_in=dt.timedelta(days=31))
        make_rig().cycle()
        assert self.reminders(caplog) == []

    def test_a_missing_certificate_file_is_silent_even_at_debug(self, make_rig, caplog, client_tls_dir, clock):
        os.unlink(client_tls_dir / "client.crt")
        rig = make_rig()
        assert rig.cycle() is False                                    # (the identity load says so)
        assert self.reminders(caplog) == []
        assert not [m for lv, m in own(caplog) if "expiry check" in m]
        # the load failure is still reported as before
        assert any(m.startswith(TestIdentityErrors.PREFIX) for lv, m in own(caplog) if lv == "ERROR")

    def test_a_missing_tls_directory_is_silent(self, make_rig, caplog, tmp_path, clock):
        rig = make_rig(tls_dir=tmp_path / "absent.tls")
        rig.mgr.begin_cycle()
        assert self.reminders(caplog) == []
        assert not (tmp_path / "absent.tls").exists()

    def test_no_tls_dir_and_no_configured_paths_never_logs_a_reminder(self, make_rig, caplog, clock):
        rig = make_rig()
        rig.mgr.tls_dir = None
        rig.mgr.begin_cycle()                                          # must not raise
        assert self.reminders(caplog) == []
        assert [lv for lv, m in own(caplog) if "expiry check" in m] == ["DEBUG"]

    @pytest.mark.parametrize("damage", ["garbage", "armour", "directory"])
    def test_an_unreadable_or_unparseable_certificate_is_debug_only(
        self, make_rig, caplog, client_tls_dir, clock, damage
    ):
        rig = make_rig()
        crt = client_tls_dir / "client.crt"
        if damage == "garbage":
            crt.write_text("garbage\n")
        elif damage == "armour":
            crt.write_text("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n")
        else:
            crt.unlink()
            crt.mkdir()
        rig.mgr.begin_cycle()                                          # must not raise
        assert self.reminders(caplog) == []
        assert [lv for lv, m in own(caplog) if "client certificate expiry check failed" in m] == ["DEBUG"]

    @pytest.mark.skipif(os.geteuid() == 0, reason="root reads everything")
    def test_a_certificate_the_process_cannot_read_is_debug_only(self, make_rig, caplog, client_tls_dir, clock):
        rig = make_rig()
        crt = client_tls_dir / "client.crt"
        os.chmod(crt, 0)
        try:
            rig.mgr.begin_cycle()
        finally:
            os.chmod(crt, 0o640)
        assert self.reminders(caplog) == []
        assert [lv for lv, m in own(caplog) if "client certificate expiry check failed" in m] == ["DEBUG"]

    def test_a_failed_check_is_not_retried_before_the_next_day(
        self, make_rig, client_tls_dir, clock, mocker
    ):
        rig = make_rig()
        (client_tls_dir / "client.crt").write_text("garbage\n")
        read = mocker.spy(remote.ident, "client_cert_not_after")
        for _ in range(3):
            rig.mgr.begin_cycle()
        assert read.call_count == 1

    def test_an_unexpected_error_in_the_decision_is_swallowed(
        self, make_rig, caplog, mocker, clock
    ):
        mocker.patch.object(remote.ident, "client_cert_expiry_problem", side_effect=RuntimeError("boom"))
        rig = make_rig()
        rig.mgr.begin_cycle()                                          # must not raise
        assert rig.cycle() is True                                     # and the cycle works
        assert self.reminders(caplog) == []
        assert any("expiry check failed: boom" in m for lv, m in own(caplog) if lv == "DEBUG")

    # -- independence from the identity load ---------------------------------------------------------

    def test_it_runs_when_the_identity_cannot_load(self, make_rig, caplog, client_tls_dir, clock):
        """A near-expiry certificate whose private key is unusable (mode 0644): the load fails,
        the reminder is made all the same."""
        import datetime as dt
        made = self.cert(client_tls_dir, expires_in=dt.timedelta(days=5))
        os.chmod(client_tls_dir / "client.key", 0o644)
        rig = make_rig()
        assert rig.cycle() is False
        assert self.reminders(caplog) == [("WARNING", self.expected(client_tls_dir / "client.crt", made)[1])]
        assert any("readable by everyone" in m for lv, m in own(caplog) if lv == "ERROR")

    def test_it_runs_when_the_key_is_missing(self, make_rig, caplog, client_tls_dir, clock):
        import datetime as dt
        self.cert(client_tls_dir, expires_in=dt.timedelta(days=5))
        os.unlink(client_tls_dir / "client.key")
        rig = make_rig()
        rig.mgr.begin_cycle()
        assert len(self.reminders(caplog)) == 1

    def test_it_runs_without_the_identity_ever_being_requested(self, make_rig, caplog, client_tls_dir, clock):
        """`begin_cycle` alone (no dataset processed yet) is enough."""
        import datetime as dt
        self.cert(client_tls_dir, expires_in=dt.timedelta(days=5))
        rig = make_rig()
        rig.mgr.begin_cycle()
        assert len(self.reminders(caplog)) == 1
        rig.assert_nothing_sent()

    def test_a_manager_used_without_begin_cycle_never_checks(self, make_rig, caplog, client_tls_dir, clock):
        import datetime as dt
        self.cert(client_tls_dir, expires_in=dt.timedelta(days=5))
        rig = make_rig()
        rig.run()
        rig.run()
        assert self.reminders(caplog) == []

    def test_each_manager_has_its_own_first_check(self, make_rig, caplog, client_tls_dir, clock):
        import datetime as dt
        self.cert(client_tls_dir, expires_in=dt.timedelta(days=5))
        make_rig().mgr.begin_cycle()
        make_rig().mgr.begin_cycle()
        assert len(self.reminders(caplog)) == 2

    def test_it_checks_the_configured_certificate_not_the_tls_dir_one(
        self, make_rig, caplog, tmp_path, client_tls_dir, clock
    ):
        import datetime as dt
        from _api_support import make_client_cert

        now = dt.datetime.now(dt.timezone.utc)
        near = make_client_cert(not_before=now - dt.timedelta(hours=1), not_after=now + dt.timedelta(days=4))
        write_identity_files(tmp_path / "custom.tls", near)
        rig = make_rig(client_cert_file=tmp_path / "custom.tls" / "client.crt",
                       client_key_file=tmp_path / "custom.tls" / "client.key")
        rig.mgr.begin_cycle()
        (_, message), = self.reminders(caplog)
        assert str(tmp_path / "custom.tls" / "client.crt") in message
        assert str(client_tls_dir) not in message

    def test_the_reminder_goes_to_the_remote_logger(self, make_rig, caplog, client_tls_dir, clock):
        import datetime as dt
        self.cert(client_tls_dir, expires_in=dt.timedelta(days=5))
        make_rig().mgr.begin_cycle()
        records = [r for r in caplog.records if r.getMessage().startswith("the client certificate ")]
        assert [r.name for r in records] == ["zfsbackup.remote"]


# ---------------------------------------------------------------------------
# Real TLS, end to end
# ---------------------------------------------------------------------------

START_METHOD = "fork"
RECV_NAME = "received.txt"


class _Writer:
    """The server's fake `zfs receive`: counts the bytes and appends the total to a file at close
    (the API worker is another process). `landed` runs once the receive has completed: the
    snapshot it carried now exists on the server."""

    def __init__(self, path, landed=None):
        self.path, self.n, self.landed = path, 0, landed

    def write(self, data):
        self.n += len(data)

    def close(self):
        with open(self.path, "a") as fh:
            fh.write(f"{self.n}\n")
        if self.landed is not None:
            self.landed()


_NS = types.SimpleNamespace      # (`_list` has a parameter called `types`)


class FakeServerZfs(types.SimpleNamespace):
    """Everything `zfsbackup.api` asks of libzfseasy for a receive, in memory, and stateful like
    the real thing: negotiate creates the dataset and stores the config (`set`), a stream reads it
    back (`get.value`) and is refused without it, and the receive's `-o` lands in the same store.
    The state lives in the (forked) API worker's copy; the properties the receive was given are
    appended to the `received_props` file next to the received bytes, for the client side to read."""

    def __init__(self, received_path, snapshots=None, incoming=None):
        self.store = {}                    # dataset name -> {property: value}
        self.received_props_path = received_path + ".props"
        #: dataset name -> the short snapshot names it already holds (what `negotiate` lists).
        #: Seed it BEFORE the API worker starts: the forked worker has its own copy.
        self.snapshots = {ds: list(names) for ds, names in (snapshots or {}).items()}
        #: dataset name -> the short names of the snapshots its next completed receives bring, in
        #: order: the server's listing grows with them (the receive's `-F` decision and the next
        #: negotiate read it), as a real pool's does.
        self.incoming = {ds: list(names) for ds, names in (incoming or {}).items()}
        #: dataset name -> `zfs get type` (default: a filesystem), for the datasets that exist
        self.dataset_types = {}
        # The datasets the server already holds were made by an earlier receive: they carry the LOCAL
        # client id (B-2c), the one in their path `<target>/<id>/...`.
        for name in self.snapshots:
            self.store.setdefault(name, {}).setdefault(
                "org.zfsbackup:client_id", name.split("/")[2] if name.count("/") >= 2 else None)
        super().__init__(
            exists=lambda ds: str(ds) in self.store,
            create=self._create,
            set=self._set,
            list=self._list,
            get=types.SimpleNamespace(value=self._value),
            receive=types.SimpleNamespace(
                filesystem=lambda ds, props=None, **k: self._receive(received_path, ds, props, k.get("force")),
                abort=lambda *a, **k: None,
            ),
        )

    def _list(self, roots=None, types=None, properties=None, **k):
        if types == "snapshot":
            # with the `createtxg` and `guid` the listing is asked for (the receive decides `-F`
            # from them): derived from the name, so they agree with the client's own listing
            from _snapshot_support import listed_snapshot

            return [listed_snapshot(n, str(roots)) for n in self.snapshots.get(str(roots), [])]
        return []

    def _create(self, ds, *a, properties=None, **k):
        self.store.setdefault(str(ds), {}).update(properties or {})

    def _set(self, ds, props, *a, **k):
        self.store.setdefault(str(ds), {}).update(props)

    def _value(self, ds, prop, sources=None, raw=False):
        if prop == "type":
            return self.dataset_types.get(str(ds), "filesystem") if str(ds) in self.store else None
        return self.store.get(str(ds), {}).get(prop)

    def _receive(self, received_path, ds, props, force=None):
        self.store.setdefault(str(ds), {}).update(props or {})
        with open(self.received_props_path, "a") as fh:
            fh.write(json.dumps({"dataset": str(ds), "props": props, "force": force}) + "\n")
        return _Writer(received_path, lambda: self._landed(str(ds), bool(force)))

    def _landed(self, ds, force):
        """The receive completed: the next incoming snapshot exists. An `incoming` entry may be
        `(name, [names])`: a receive with `-F` also destroys those (what `zfs receive -F` of an
        incremental does to the snapshots newer than its base). The listing is written to a file
        next to the received bytes for the client side to read."""
        if self.incoming.get(ds):
            item = self.incoming[ds].pop(0)
            name, destroyed = (item, []) if isinstance(item, str) else item
            held = self.snapshots.setdefault(ds, [])
            if force:
                held[:] = [n for n in held if n not in destroyed]
            held.append(name)
            with open(self.received_props_path + ".snaps", "w") as fh:
                json.dump(held, fh)

    def properties_of(self, name):
        """What `zfsbackup.api.read_zfsbackup_properties` shows for `name`."""
        return dict(self.store.get(name, {}))


def fake_server_zfs(received_path, snapshots=None, incoming=None):
    return FakeServerZfs(received_path, snapshots, incoming)


def _snap(name):
    """A managed local snapshot of testpool/data as the manager's listing delivers it."""
    from _snapshot_support import listed_info

    return listed_info(name, "autosnap", "testpool/data")


class _LocalManager:
    """The client's local dataset manager as far as `RemoteBackupManager` uses it."""

    def __init__(self):
        self.anchors, self.pins = {}, []
        self.snaps = [_snap("autosnap_20261020120000"), _snap("autosnap_20261020110000")]

    def list_snapshots(self, dsi):
        return list(self.snaps)

    def get_anchor(self, dsi, dest):
        return self.anchors.get(dest)

    def get_local_anchor(self, dsi, dest):
        return self.anchors.get(dest)

    def set_anchor(self, dsi, dest, name):
        self.anchors[dest] = name
        return True

    def clear_anchor(self, dsi, dest):
        self.anchors.pop(dest, None)

    def set_inflight(self, dsi, dest, from_name, to_name):
        self.pins.append((dest, from_name, to_name))
        return True

    def clear_inflight(self, *a):
        return True

    def get_inflight(self, *a):
        return None


class Impostor:
    """A TLS server with its own (unpinned) certificate: completes the handshake, records the
    application bytes it is sent (there must be none)."""

    def __init__(self, directory):
        sid = ident.ensure_identity(directory, ident.ROLE_SERVER, group_of=directory.parent)
        self.fp = sid.fingerprint
        self.ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.ctx.load_cert_chain(sid.cert_path, sid.key_path)
        self.sock = socket.create_server(("127.0.0.1", 0))
        self.port = self.sock.getsockname()[1]
        self.handshakes = 0
        self.app_bytes = 0
        self._stop = False
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        self.sock.settimeout(0.2)
        while not self._stop:
            try:
                raw, _ = self.sock.accept()
            except (socket.timeout, OSError):
                continue
            try:
                raw.settimeout(3)
                tls = self.ctx.wrap_socket(raw, server_side=True)
                self.handshakes += 1
                try:
                    while True:
                        data = tls.recv(65536)
                        if not data:
                            break
                        self.app_bytes += len(data)
                except (OSError, ssl.SSLError):
                    pass
                tls.close()
            except (OSError, ssl.SSLError):
                pass
            finally:
                raw.close()

    def close(self):
        self._stop = True
        self.thread.join(5)
        self.sock.close()


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _eventually(predicate, timeout=30.0, interval=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return predicate()


class World:
    """A real `ApiWorker` (HTTPS, mTLS, trust from a real config database) on 127.0.0.1 with its
    zfs calls replaced in memory, a client TLS identity, and everything to run a real
    `RemoteBackupManager` against it. Cleans up after itself: the worker, the impostor and every
    fake `zfs send` child."""

    def __init__(self, tls_config_db, tmp_path, mocker, monkeypatch, caplog, server_snapshots=None,
                 server_incoming=None):
        from _reload_workers import SelectableApiWorker
        from zfsbackup.stopflag import StopFlag

        home = tmp_path / "home"
        home.mkdir()
        monkeypatch.setenv("HOME", str(home))
        self.tmp = tmp_path
        self.resolved = tls_config_db
        self.received_path = tmp_path / RECV_NAME
        self.server_fp = ident.peek_identity(ident.tls_dir(tls_config_db.resolved_path), "server").fingerprint
        self.port = _free_port()
        self.sends = []
        self.send_calls = []          # (snapshot, since, flags) of every fake `zfs send`
        self.impostor_ = None
        self.endless = False
        caplog.set_level(logging.DEBUG, logger="zfsbackup.remote")
        self.caplog = caplog
        # the server's zfs, in memory: the forked worker inherits the patched module
        self.server_zfs = fake_server_zfs(str(self.received_path), server_snapshots, server_incoming)
        mocker.patch("zfsbackup.api.zfs", self.server_zfs)
        mocker.patch(
            "zfsbackup.api.read_zfsbackup_properties", lambda ds: self.server_zfs.properties_of(ds.name)
        )
        config = BackupConfig(
            datasets=[DatasetConfig(name="testpool/srv")], api_host="127.0.0.1", api_port=self.port,
            remote_backup=RemoteServerConfig(target_dataset="testpool/backups", enabled=True),
        )
        self.stop = StopFlag()
        self.worker = SelectableApiWorker(config, self.stop, config_db=tls_config_db)
        self.worker.start_method = START_METHOD
        self.worker.start()
        from _tls_support import try_get
        self._try_get = try_get

        def up():
            if not self.worker.is_alive():
                pytest.fail(f"the API worker exited early ({self.worker.exitcode})")
            r = try_get(self.port, "/health")
            return isinstance(r, tuple) and r[0] == 200

        assert _eventually(up, 60, 0.2), "the API worker never answered /health"

        # the client side
        self.ctls = tmp_path / "client.db.tls"
        self.client = ident.ensure_identity(self.ctls, ident.ROLE_CLIENT, group_of=tmp_path)
        self.local = _LocalManager()
        self.base = f"https://127.0.0.1:{self.port}"
        mocker.patch.object(remote.zfs.send, "snapshot", side_effect=self._fake_send)

    # -- the fake `zfs send`: a python byte writer in a hermetic subprocess --------------------------
    def _fake_send(self, snapshot, since=None, **flags):
        """The stream starts with a real `DRR_BEGIN` record (the server reads it before it spawns
        `zfs receive`): a full stream's, or an incremental one whose source guid is `since`'s.
        Its 312 bytes replace the first 312 of the first 64 KiB chunk, so a finite stream is
        still 64 * 65536 bytes."""
        from _hermetic import hermetic_env
        from _snapshot_support import default_guid
        from _stream_support import HEADER, incremental_header

        self.send_calls.append((snapshot, since, flags))
        if since is None:
            header = HEADER
        else:
            guid = since["guid"].value if hasattr(since, "__getitem__") else default_guid(str(since))
            header = incremental_header(int(guid))
        body = "while True:\n    w(b)\n" if self.endless else "for _ in range(63):\n    w(b)\n"
        code = (
            "import sys\nh = bytes.fromhex(sys.argv[1])\nb = b'z' * 65536\nw = sys.stdout.buffer.write\n"
            "w(h + b[len(h):])\n" + body
        )
        proc = subprocess.Popen(
            [sys.executable, "-c", code, header.hex()], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env=hermetic_env(),
        )
        self.sends.append(proc)
        return StreamHandle(proc, proc.stdout)

    # -- enrollment and trust -------------------------------------------------------------------
    def token(self):
        from zfsbackup.config.store import trust

        token, sha = ident.make_token(self.server_fp)
        now = time.time()
        trust.create_token(self.resolved, secret_sha256=sha, server_fingerprint=self.server_fp,
                           created_at=now, expires_at=now + 600, lock=None)
        return str(token)

    def enroll(self, client_id="client-a", who=None):
        """Enroll `who`'s certificate (default: this client's) through the real `/enroll`."""
        import base64

        who = who or self.client
        token = self.token()
        _, secret = ident.parse_token(token)
        payload = ident.enrollment_payload(self.server_fp, client_id, who.fingerprint, ident.secret_hash(secret))
        body = {
            "version": 1, "client_id": client_id, "token_secret": token, "cert_pem": who.cert_pem,
            "signature": base64.b64encode(ident.sign_payload(who.key_path, payload)).decode(),
        }
        session = ident.pinned_session(None, self.server_fp, timeout=10)
        session.headers.update(protocol_headers())
        try:
            response = session.post(f"{self.base}/enroll", json=body)
        finally:
            session.close()
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "enrolled"
        # The API worker installs the new pin right after it has sent the 200 (and its trust poll
        # does within a couple of seconds): a client that connects the instant it reads the answer
        # can still be turned away. Wait until the TLS layer lets the certificate in.
        assert _eventually(lambda: self.server_accepts(who), 30), "the new pin never took effect"
        return response.json()

    def revoke(self, client_id="client-a"):
        from zfsbackup.config.store import dispose_all, trust

        trust.revoke_client(self.resolved, client_id, lock=None)
        dispose_all()

    def pinned(self):
        from zfsbackup.config.store import dispose_all, trust

        try:
            return {p.fingerprint for p in trust.read_trust(self.resolved).pins}
        finally:
            dispose_all()

    def server_accepts(self, who):
        """Does the server's TLS layer let `who` through? (a bare authenticated GET)."""
        from _tls_support import insecure_client_context

        result = self._try_get(self.port, "/whoami", insecure_client_context(who))
        return isinstance(result, tuple) and result[0] == 200

    # -- the client ----------------------------------------------------------------------------------
    def config(self, **destinations):
        dests = {"offsite": Destination(url=self.base, server_fingerprint=self.server_fp)}
        dests.update(destinations)
        return BackupConfig(
            datasets=[DatasetConfig(
                name="testpool/data",
                remote=[RemoteDatasetConfig(destination=d) for d in dests],
            )],
            destinations=dests,
        )

    def manager(self, client_id="client-a", config=None):
        config = config or self.config()
        mgr = RemoteBackupManager(config, self.local, client_id=client_id, tls_dir=self.ctls)
        self._managers.append(mgr)
        dsi = types.SimpleNamespace(name="testpool/data", config=config.datasets[0],
                                    dataset=Filesystem("testpool/data"))
        return mgr, dsi

    _managers = None

    def cycle(self, mgr, dsi, dest="offsite"):
        mgr.begin_cycle()
        return mgr.backup_dataset(dsi, RemoteDatasetConfig(destination=dest))

    def impostor(self):
        if self.impostor_ is None:
            self.impostor_ = Impostor(self.tmp / "impostor.tls")
        return self.impostor_

    def received(self):
        if not self.received_path.exists():
            return []
        return [int(x) for x in self.received_path.read_text().split()]

    def server_snapshot_names(self):
        """The short names the fake server holds after its last completed receive (None before the
        first)."""
        path = Path(str(self.received_path) + ".props.snaps")
        return json.loads(path.read_text()) if path.exists() else None

    def received_props(self):
        """The `props` (`-o`) each fake `zfs receive` was started with, in order."""
        path = Path(str(self.received_path) + ".props")
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines()]

    def new_sends(self, since):
        return self.sends[since:]

    def errors_since(self, mark):
        return [m for lv, m in own(self.caplog)[mark:] if lv == "ERROR"]

    def close(self):
        for mgr in self._managers:
            mgr.close()
        if self.impostor_ is not None:
            self.impostor_.close()
        self.stop.set()
        self.worker.join(30)
        if self.worker.is_alive():
            self.worker.terminate()
            self.worker.join(10)
        if self.worker.is_alive():
            self.worker.kill()
            self.worker.join(10)
        for proc in self.sends:
            if proc.poll() is None:
                proc.kill()
                proc.wait(10)
            with contextlib.suppress(Exception):
                proc.stdout.close()


@pytest.fixture
def world(tls_config_db, tmp_path, mocker, monkeypatch, caplog, request):
    # `param`: the server's snapshots (`{dataset: [short names]}`), or `(snapshots, incoming)` where
    # `incoming` are the snapshots its next completed receives bring (`FakeServerZfs`)
    param = getattr(request, "param", None)
    snapshots, incoming = param if isinstance(param, tuple) else (param, None)
    w = World(tls_config_db, tmp_path, mocker, monkeypatch, caplog,
              server_snapshots=snapshots, server_incoming=incoming)
    w._managers = []
    try:
        yield w
    finally:
        w.close()


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.skipif(START_METHOD not in multiprocessing.get_all_start_methods(), reason="needs fork")
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestRealTls:
    """The manager against a real `ApiWorker` over real TLS 1.3 with real certificates. No real zfs:
    the server's zfs calls are replaced in memory and the client's `zfs send` is a hermetic byte
    writer."""

    def test_1_a_pinned_mtls_backup_end_to_end(self, world, caplog):
        world.enroll()
        mgr, dsi = world.manager()
        mark = len(own(caplog))
        assert world.cycle(mgr, dsi) is True
        assert len(world.sends) == 1 and world.sends[0].returncode is not None      # reaped
        assert _eventually(lambda: world.received() == [64 * 65536], 10)           # every byte arrived
        assert world.local.anchors["offsite"] == "autosnap_20261020120000"
        assert world.errors_since(mark) == []
        assert world.local.pins == [("offsite", None, "autosnap_20261020120000")]
        assert any("client TLS identity" in m for _, m in own(caplog)[mark:])
        # negotiate stored the config; the receive put it in effect with `-o`, with the BOUND id
        (received,) = world.received_props()
        assert received["dataset"] == "testpool/backups/client-a/data"
        assert received["props"]["org.zfsbackup:client_id"] == "client-a"
        assert received["props"]["org.zfsbackup:source_dataset"] == "testpool/data"
        assert received["props"]["org.zfsbackup:config"]

    def test_1b_the_second_cycle_is_incremental_or_nothing_to_send(self, world):
        world.enroll()
        mgr, dsi = world.manager()
        assert world.cycle(mgr, dsi) is True
        # the fake server holds no snapshot (list -> []), so it is a full send again, but the
        # whole path -- whoami, negotiate, resume_token, wss -- works a second time on the live sessions
        assert world.cycle(mgr, dsi) is True
        assert _eventually(lambda: len(world.received()) == 2, 10)

    def test_2a_an_impostor_destination_gets_no_http_byte_and_no_send(self, world, caplog):
        world.enroll()
        imp = world.impostor()
        config = world.config(wrongpin=Destination(
            url=f"https://127.0.0.1:{imp.port}", server_fingerprint=world.server_fp))
        mgr, dsi = world.manager(config=config)
        mark = len(own(caplog))
        assert world.cycle(mgr, dsi, "wrongpin") is False
        assert world.cycle(mgr, dsi, "wrongpin") is False
        assert world.sends == []
        assert imp.handshakes >= 1 and imp.app_bytes == 0           # the pin is checked before any HTTP byte
        got = world.errors_since(mark)
        assert len(got) == 1
        assert f"presented certificate {imp.fp}, but the destination is pinned to {world.server_fp}" in got[0]
        assert "possible MITM or the server was re-keyed" in got[0]
        assert [lv for lv, m in own(caplog)[mark:] if "presented certificate" in m] == ["ERROR", "DEBUG"]

    def test_2b_a_pin_mismatch_on_the_websocket_reaps_the_started_send(self, world, caplog):
        world.enroll()
        imp = world.impostor()
        mgr, dsi = world.manager()
        real_connect = remote.connect_pinned
        world.endless = True

        def to_impostor(host, port, ctx, fp, timeout):
            return real_connect("127.0.0.1", imp.port, ctx, fp, timeout)

        mark = len(own(caplog))
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(remote, "connect_pinned", to_impostor)
            assert world.cycle(mgr, dsi) is False
        world.endless = False
        assert len(world.sends) == 1
        assert world.sends[0].returncode is not None, "the zfs send stand-in is still running"
        assert imp.app_bytes == 0
        got = world.errors_since(mark)
        assert len(got) == 1 and f"presented certificate {imp.fp}" in got[0]
        assert world.local.anchors == {}                              # nothing was recorded as sent
        # the destination is skipped for the rest of the cycle: no second send
        before = len(world.sends)
        assert mgr.backup_dataset(dsi, RemoteDatasetConfig(destination="offsite")) is False
        assert len(world.sends) == before

    def test_2c_a_url_id_the_server_does_not_bind_is_refused_at_each_stage(self, world, caplog):
        world.enroll()
        mgr, dsi = world.manager()
        assert world.cycle(mgr, dsi) is True                       # whoami cached, transport built
        transport = mgr._transports["offsite"]
        latest = world.local.snaps[0]
        anchor = dict(world.local.anchors)
        before = len(world.sends)
        mark = len(own(caplog))
        # 1. /resume_token under another client's id: 403 client_id mismatch, no send
        assert mgr._transfer(transport, "someone-else", dsi, latest, None, "offsite",
                             snapshots=world.local.snaps) is False
        assert len(world.sends) == before
        got = world.errors_since(mark)
        assert len(got) == 1 and "binds this client's certificate" in got[0] and "'client-a'" in got[0]
        # 2. the websocket upgrade under that id: 403, the started send is reaped
        world.endless = True
        mgr._fetch_resume_token = lambda *a, **k: ("none", None)
        try:
            assert mgr._transfer(transport, "someone-else", dsi, latest, None, "offsite",
                                 snapshots=world.local.snaps) is False
        finally:
            world.endless = False
            del mgr._fetch_resume_token
        new = world.sends[before:]
        assert len(new) == 1 and new[0].returncode is not None
        assert world.local.anchors == anchor
        assert len(world.errors_since(mark)) == 1                  # deduplicated: still one ERROR

    def test_3_a_revoked_client_is_refused_once_then_quietly_and_never_sent_for(self, world, caplog):
        world.enroll()
        mgr, dsi = world.manager()
        assert world.cycle(mgr, dsi) is True
        before = len(world.sends)
        world.revoke()
        assert _eventually(lambda: not world.server_accepts(world.client), 30), \
            "the server never stopped accepting the revoked certificate"
        mark = len(own(caplog))
        assert world.cycle(mgr, dsi) is False
        assert world.cycle(mgr, dsi) is False
        assert len(world.sends) == before                           # no stream for a refused client
        got = world.errors_since(mark)
        assert len(got) == 1
        # normally the TLS alert; rarely the reset that stands in for it (plan, "M11 rejection detection")
        assert ("does not trust this client" in got[0]) or ("connection reset after TLS handshake" in got[0])
        assert world.client.fingerprint in got[0]
        assert len([lv for lv, m in own(caplog)[mark:] if m == got[0]]) == 2        # ERROR, then DEBUG

    def test_4_a_client_id_the_server_has_not_bound_to_this_certificate(self, world, caplog):
        world.enroll("client-a")
        mgr, dsi = world.manager(client_id="other-host")
        mark = len(own(caplog))
        assert world.cycle(mgr, dsi) is False
        assert world.cycle(mgr, dsi) is False
        assert world.sends == []
        got = world.errors_since(mark)
        assert len(got) == 1
        assert (f"binds this client's certificate {world.client.fingerprint} to client id 'client-a', "
                "but this host's client id is 'other-host'") in got[0]
        assert "(`zfsbackup-config identity set-client-id client-a`)" in got[0]
        assert "(`zfsbackup-config trust revoke --client client-a`)" in got[0]
        assert "(`zfsbackup-config enroll offsite --new-key --token-file …`)" in got[0]

    def test_5_unenrolled_and_idless_are_skipped_without_touching_the_server(self, world, caplog):
        world.enroll()
        config = world.config(
            noprint=Destination(url=world.base),
            plain=Destination(url=f"http://127.0.0.1:{world.port}", server_fingerprint=world.server_fp),
        )
        mgr, dsi = world.manager(config=config)
        mark = len(own(caplog))
        results = [world.cycle(mgr, dsi, d) for d in ("noprint", "noprint", "plain", "plain")]
        nocid, dsi2 = world.manager(client_id=None, config=config)
        results += [world.cycle(nocid, dsi2, "offsite"), world.cycle(nocid, dsi2, "offsite")]
        assert results == [False] * 6
        assert world.sends == []
        got = world.errors_since(mark)
        assert len(got) == 3
        assert "destination noprint is not enrolled" in got[0] and "no server fingerprint is pinned" in got[0]
        assert "destination plain is not enrolled" in got[1] and "plaintext http is not supported" in got[1]
        assert got[2] == f"remote backup skipped: {NO_CLIENT_ID_MESSAGE}"

    def test_6_a_rotated_identity_is_picked_up_by_the_running_manager(self, world, caplog):
        import base64

        world.enroll()
        mgr, dsi = world.manager()
        assert world.cycle(mgr, dsi) is True
        old_fp = world.client.fingerprint
        shutil.copytree(world.ctls, world.tmp / "old.tls")          # the files are replaced below
        old = ident.load_identity(*ident.identity_paths(world.tmp / "old.tls", "client"), ident.ROLE_CLIENT)
        assert world.server_accepts(old)
        # `identity rotate`, by hand: a successor next to the current pair, /rotate, then promote
        nxt = ident.ensure_identity(world.ctls, ident.ROLE_CLIENT, name=ident.CLIENT_NEXT_NAME,
                                    group_of=world.tmp)
        payload = ident.rotation_payload(world.server_fp, "client-a", old_fp, nxt.fingerprint)
        body = {"version": 1, "successor_cert_pem": nxt.cert_pem,
                "signature": base64.b64encode(ident.sign_payload(nxt.key_path, payload)).decode()}
        session = ident.pinned_session(world.client, world.server_fp, timeout=10)
        session.headers.update(protocol_headers())
        try:
            response = session.post(f"{world.base}/rotate", json=body)
        finally:
            session.close()
        assert response.status_code == 200 and response.json()["status"] == "rotated"
        # as after /enroll, the successor's pin is installed just after the answer is sent: wait for it
        # (this first authenticated request is also the one that retires the old pin)
        assert _eventually(lambda: world.server_accepts(nxt), 30), "the successor's pin never took effect"
        assert ident.promote_identity(world.ctls).fingerprint == nxt.fingerprint
        world.local.snaps.insert(0, _snap("autosnap_20261020130000"))
        before, mark = len(world.sends), len(own(caplog))
        received = len(world.received())
        assert world.cycle(mgr, dsi) is True                         # same manager, new certificate
        assert len(world.sends) == before + 1
        assert _eventually(lambda: len(world.received()) == received + 1, 10)
        assert world.local.anchors["offsite"] == "autosnap_20261020130000"
        loaded = [m for lv, m in own(caplog)[mark:] if "client TLS identity" in m]
        assert len(loaded) == 1 and nxt.fingerprint in loaded[0]
        assert mgr._transports["offsite"].identity_fp == nxt.fingerprint
        assert world.errors_since(mark) == []
        # the successor's first request retired the old pin
        assert _eventually(lambda: world.pinned() == {nxt.fingerprint}, 20)
        assert _eventually(lambda: not world.server_accepts(old), 20)    # the old certificate no longer works
        assert world.server_accepts(world.client)                       # (the files now hold the successor)


CHAIN_SNAPS = [f"autosnap_202610201{h}0000" for h in ("20", "10", "00")] + ["autosnap_20261020090000"]
CHAIN_SERVER = {"testpool/backups/client-a/data": ["autosnap_20261020090000"]}
#: the three snapshots the chain brings, oldest first: after each receive the server's listing has it
CHAIN_INCOMING = {"testpool/backups/client-a/data": CHAIN_SNAPS[2::-1]}
CHAIN_WORLD = (CHAIN_SERVER, CHAIN_INCOMING)


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.skipif(START_METHOD not in multiprocessing.get_all_start_methods(), reason="needs fork")
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestRealTlsChain:
    """B5-b over a real socket: the server already holds the oldest snapshot, the client holds three
    newer ones, and one cycle sends all three as back-to-back `-i` streams. The server's per-dataset
    claim is released before it answers each stream, so the next one never meets `busy`."""

    @pytest.mark.parametrize("world", [CHAIN_WORLD], indirect=True)
    def test_a_three_step_chain_goes_through_back_to_back(self, world, caplog):
        world.enroll()
        world.local.snaps = [_snap(n) for n in CHAIN_SNAPS]
        mgr, dsi = world.manager()
        mark = len(own(caplog))
        assert world.cycle(mgr, dsi) is True
        s3, s2, s1, base = CHAIN_SNAPS
        assert len(world.sends) == 3 and all(p.returncode is not None for p in world.sends)   # all reaped
        assert _eventually(lambda: world.received() == [64 * 65536] * 3, 20)      # every byte of all three arrived
        # the chain, oldest first, each step incremental from the one before, with the frozen plain flags
        assert [(since.short, snap.short, flags) for snap, since, flags in world.send_calls] == [
            (base, s1, {"large_blocks": True, "compressed": True}),
            (s1, s2, {"large_blocks": True, "compressed": True}),
            (s2, s3, {"large_blocks": True, "compressed": True}),
        ]
        assert world.local.pins == [("offsite", base, s1), ("offsite", s1, s2), ("offsite", s2, s3)]
        assert world.local.anchors["offsite"] == s3
        assert world.errors_since(mark) == []
        assert not [m for _, m in own(caplog)[mark:] if "busy" in m.lower()]
        # every receive was started with the server's own properties, and the bound id
        props = world.received_props()
        assert len(props) == 3
        assert {p["dataset"] for p in props} == {"testpool/backups/client-a/data"}
        assert {p["props"]["org.zfsbackup:client_id"] for p in props} == {"client-a"}

    @pytest.mark.parametrize("world", [(CHAIN_SERVER, {
        "testpool/backups/client-a/data": CHAIN_SNAPS[2::-1] + ["autosnap_20261020130000"]
    })], indirect=True)
    def test_a_second_cycle_continues_the_chain_unblocked(self, world):
        """The server's listing grows with the receives: the second cycle negotiates the newest
        snapshot of the first, and sends the one new snapshot incrementally from it. Nothing the
        first cycle left behind blocks it."""
        world.enroll()
        world.local.snaps = [_snap(n) for n in CHAIN_SNAPS]
        mgr, dsi = world.manager()
        assert world.cycle(mgr, dsi) is True
        newest = "autosnap_20261020130000"
        world.local.snaps = [_snap(newest)] + world.local.snaps
        assert world.cycle(mgr, dsi) is True
        assert _eventually(lambda: len(world.received()) == 4, 20)
        assert (world.send_calls[-1][1].short, world.send_calls[-1][0].short) == (CHAIN_SNAPS[0], newest)
        assert world.local.anchors["offsite"] == newest


R1, R2, R3, R4, R5 = (f"autosnap_2026102010{m}00" for m in ("00", "10", "20", "30", "40"))
GUARD_DS = "testpool/backups/client-a/data"


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.skipif(START_METHOD not in multiprocessing.get_all_start_methods(), reason="needs fork")
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestRealTlsRollbackGuard:
    """B-2b over a real socket: the client's `discard` and the server's decision (`decide_receive`),
    both real, with the server's zfs in memory (`FakeServerZfs`: a forced receive drops what its
    `incoming` entry says it destroys). The server holds R1, R2 (and R3 where stated)."""

    def lost(self, world, snapshot_presence, caplog):
        """Cycle 1: R3 goes over and is destroyed locally while it is being sent; the backstop puts
        the anchor back on R2. Returns the manager and its dataset."""
        world.enroll()
        world.local.snaps = [_snap(n) for n in (R3, R2, R1)]
        world.local.anchors["offsite"] = R2
        mgr, dsi = world.manager()
        snapshot_presence.gone.add(R3)
        mark = len(own(caplog))
        world.cycle(mgr, dsi)
        assert _eventually(lambda: world.received() == [64 * 65536], 20)
        assert world.local.anchors["offsite"] == R2                        # back on the base
        assert world.server_snapshot_names() == [R1, R2, R3]               # the server has R3
        assert any("was destroyed while it was being sent" in m for m in world.errors_since(mark))
        snapshot_presence.gone.clear()
        return mgr, dsi

    @pytest.mark.parametrize("world", [(
        {GUARD_DS: [R1, R2]}, {GUARD_DS: [R3, (R4, [R3]), R5]},
    )], indirect=True)
    def test_the_backstop_is_recovered_by_the_next_cycle_and_only_that_snapshot_goes(
        self, world, snapshot_presence, caplog
    ):
        mgr, dsi = self.lost(world, snapshot_presence, caplog)
        world.local.snaps = [_snap(n) for n in (R5, R4, R2, R1)]          # R3 is gone here, two new
        mark = len(own(caplog))
        assert world.cycle(mgr, dsi) is True
        assert _eventually(lambda: len(world.received()) == 3, 20)
        assert world.errors_since(mark) == []
        assert [(since.short, snap.short) for snap, since, _ in world.send_calls[1:]] == [(R2, R4), (R4, R5)]
        assert world.local.anchors["offsite"] == R5
        # the server dropped exactly the snapshot we lost, and nothing else
        assert world.server_snapshot_names() == [R1, R2, R4, R5]
        assert all(p["force"] is True for p in world.received_props())

    @pytest.mark.parametrize("world", [({GUARD_DS: [R1, R2, R3]}, {GUARD_DS: [R4]})], indirect=True)
    def test_a_rolled_back_client_is_refused_before_anything_is_sent(self, world, caplog):
        """The host rolled back to R2 (`zfs rollback -r` destroyed R3, keeping the local anchor on
        it) and made R4. Nothing is sent, nothing on the server changes, the operator is told."""
        world.enroll()
        world.local.snaps = [_snap(n) for n in (R4, R2, R1)]
        world.local.anchors["offsite"] = R3
        mgr, dsi = world.manager()
        mark = len(own(caplog))
        assert world.cycle(mgr, dsi) is False
        assert world.sends == [] and world.send_calls == []
        assert world.received() == [] and world.received_props() == []
        assert world.server_snapshot_names() is None                       # no receive ever completed
        assert world.local.anchors["offsite"] == R3 and world.local.pins == []
        (error,) = world.errors_since(mark)
        assert f"has 1 snapshot(s) newer than {R2}" in error and R3 in error and "zfs rename" in error

    @pytest.mark.parametrize("world", [({GUARD_DS: [R1, R2, R3]}, {GUARD_DS: [R4]})], indirect=True)
    @pytest.mark.parametrize("how", ["no-discard", "wrong-guid"])
    def test_the_server_refuses_on_its_own_when_the_client_does_not_ask(self, world, caplog, how, mocker):
        """A client that skips its check (a race, an older release): the server compares the base
        with what it holds and refuses before it reads a byte of the stream."""
        world.enroll()
        world.local.snaps = [_snap(n) for n in (R4, R2, R1)]
        mgr, dsi = world.manager()
        guids = () if how == "no-discard" else ("123456789",)
        mocker.patch.object(mgr, "_rollback_permit", return_value=guids)
        mark = len(own(caplog))
        assert world.cycle(mgr, dsi) is False
        assert world.server_snapshot_names() is None and world.received_props() == []
        (error,) = world.errors_since(mark)
        assert "refused pool" not in error and "testpool/data -> offsite" in error
        assert f"{GUARD_DS} holds 1 snapshot(s) newer than {R2}" in error and R3 in error
        assert world.local.anchors.get("offsite") is None

    @pytest.mark.parametrize("world", [({GUARD_DS: [R1, R2, R3]}, {})], indirect=True)
    def test_a_full_stream_onto_a_copy_with_snapshots_is_refused_by_the_server(self, world, caplog, mocker):
        """The client's own check is bypassed (no common snapshot reported): the server's `no_common`."""
        world.enroll()
        world.local.snaps = [_snap(n) for n in (R5, R4)]
        mgr, dsi = world.manager()
        mocker.patch.object(mgr, "_rollback_permit", return_value=())
        mark = len(own(caplog))
        assert world.cycle(mgr, dsi) is False
        assert world.server_snapshot_names() is None and world.received_props() == []
        (error,) = world.errors_since(mark)
        assert "already holds snapshots and a full stream cannot replace them" in error

"""The wire protocol version, server side (production readiness plan S-5).

Every request names the protocol it speaks in `X-Zfsbackup-Protocol` (`zfsbackup.protocol`). The
server answers **426** `{"error", "reason": "protocol", "protocol": 1, "supported": [1]}` -- with
the header on the response -- for a missing, malformed or unsupported value, on every endpoint
except `/health`. Where the check sits in the request is part of the contract:

* `/enroll` (unauthenticated) checks it FIRST, before its body is read or the token store is
  touched;
* every other endpoint checks it after 401/403 (an unauthenticated caller learns nothing but
  401/403) and BEFORE the client-id mismatch check (403), the predecessor retirement (a store
  write), the gate (503) and the route.

Layout
------

* `TestParseProtocolHeader` / `TestProtocolHeaders` -- `zfsbackup.protocol` itself.
* `TestRefusalMatrix`         -- the 426 for each bad value on each endpoint, and its body.
* `TestAcceptedValues`        -- the values that ARE the supported version.
* `TestHealthIsExempt`        -- `/health` needs no header.
* `TestEnrollChecksFirst`     -- `/enroll` 426s before its body is read.
* `TestOrder`                 -- 401/403 before 426; 426 before the mismatch 403, the retirement,
  the gate and the route.
* `TestLogging`               -- one ERROR per peer per hour through the shared S-3 log throttle (kind
  `protocol`, logger `zfsbackup.api.peer`), at most 120 an hour over all peers, the peer table
  bounded and evicting the oldest.
* `TestWebsocketUpgrade`      -- a real websocket upgrade over real TLS without the header.
"""

import json
import logging
import time
from datetime import timedelta
from unittest.mock import MagicMock

import pytest
import websocket

from _clock_support import FakeTime
from _api_support import (
    CLIENT_ID, PROTOCOL_ENVIRON, Authed, enroll_body, make_client_cert, plain_client, rotate_body,
)
from _enroll_support import make_server, zfs_stub  # noqa: F401  (fixtures)
from _tls_support import client_identity
from zfsbackup import identity
from zfsbackup import api
from zfsbackup.api import PeerLimits, _PerPeerThrottle
from zfsbackup.config import BackupConfig, DatasetConfig, RemoteServerConfig, RetentionRule
from zfsbackup.logsetup import PEER_LOGGER
from zfsbackup.protocol import (
    PROTOCOL_HEADER, PROTOCOL_REFUSAL_REASON, PROTOCOL_VERSION, SUPPORTED_PROTOCOLS,
    parse_protocol_header, protocol_headers,
)

GOOD_CONFIG = DatasetConfig(name="pool/data").to_property()
NEGOTIATE = f"/backup/{CLIENT_ID}/pool/data/negotiate"
RESUME_TOKEN = f"/backup/{CLIENT_ID}/pool/data/resume_token"
STREAM = f"/backup/{CLIENT_ID}/pool/data/stream"

#: The values the server refuses: `None` is "no header at all".
BAD_VALUES = [
    pytest.param(None, id="missing"),
    pytest.param("", id="empty"),
    pytest.param("0", id="zero"),
    pytest.param("2", id="two"),
    pytest.param("99", id="ninety-nine"),
    pytest.param("abc", id="abc"),
    pytest.param("-1", id="minus-one"),
    pytest.param("+1", id="plus-one"),
    pytest.param("1.0", id="fraction"),
    pytest.param("1,1", id="list"),
    pytest.param("1 1", id="two-numbers"),
    pytest.param("0x1", id="hex"),
    pytest.param("1e0", id="exponent"),
    pytest.param("1" * 7, id="seven-digits"),
]

#: Every endpoint with the protocol check: (id, method, url, request kwargs).
ENDPOINTS = [
    pytest.param("get", "/whoami", {}, id="whoami"),
    pytest.param("post", NEGOTIATE, {"json": {"snapshots": [], "config": GOOD_CONFIG}}, id="negotiate"),
    pytest.param("get", RESUME_TOKEN, {}, id="resume_token"),
    pytest.param("get", STREAM, {"query_string": {"mode": "resume"}}, id="stream"),
    pytest.param("post", "/rotate", {"json": {}}, id="rotate"),
    pytest.param("post", "/enroll", {"json": {}}, id="enroll"),
]


def _config(**kwargs) -> BackupConfig:
    return BackupConfig(
        datasets=[DatasetConfig(name="pool/data", retention_rules=[
            RetentionRule(timedelta(days=1), timedelta(days=30))])],
        snapshot_prefix="autosnap",
        remote_backup=RemoteServerConfig(target_dataset="pool/backups", enabled=True),
        **kwargs,
    )


@pytest.fixture(autouse=True)
def zfs(monkeypatch):
    """No test here reaches `zfs`: the calls a request that got past the protocol check would make
    are mocks, so `calls` can say none was made.

    Patched with `monkeypatch`, not `mocker`: `TestWebsocketUpgrade` also uses the `zfs_stub`
    fixture (`_enroll_support`), which `monkeypatch.setattr`s `api.zfs.exists` over this mock. Two
    different undo stacks (`mocker` here, `monkeypatch` there) unwind in the order the fixtures
    were first requested, and when `monkeypatch` was requested before `mocker` its undo ran LAST
    and put THIS mock back, leaving a `MagicMock` as `libzfseasy.exists` for every later test in
    the process (it broke `test_zfsbackup_cli_rename` in a serial run). One stack unwinds in
    exactly the reverse order of the patches."""
    mocks = {}
    for name in ("exists", "list", "create", "set"):
        mocks[name] = MagicMock(name=name, return_value=True if name == "exists" else [])
        monkeypatch.setattr(f"zfsbackup.api.zfs.{name}", mocks[name])
    mocks["props"] = MagicMock(name="props", return_value={})
    monkeypatch.setattr("zfsbackup.api.read_zfsbackup_properties", mocks["props"])
    return mocks


@pytest.fixture
def authed():
    return Authed(_config())


def _untouched(zfs):
    for name, mock in zfs.items():
        assert not mock.called, f"zfs access {name} before the protocol check"


def _api_records(caplog):
    return [r for r in caplog.records if r.name == PEER_LOGGER and "refusing" in r.getMessage()]


# ---------------------------------------------------------------------------
# zfsbackup.protocol
# ---------------------------------------------------------------------------


class TestParseProtocolHeader:
    @pytest.mark.parametrize("value, expected", [
        ("1", 1), ("2", 2), ("0", 0), ("123456", 123456), (" 1", 1), ("1 ", 1), ("\t1\t", 1),
        ("007", 7),
    ])
    def test_plain_decimals(self, value, expected):
        assert parse_protocol_header(value) == expected

    @pytest.mark.parametrize("value", [
        None, "", " ", "abc", "-1", "+1", "1.0", "1,1", "1 1", "0x1", "1e0", "1_0", "١",
        "1234567", "1\n", "\n1", "１",
    ])
    def test_everything_else_is_none(self, value):
        assert parse_protocol_header(value) is None

    def test_a_non_string_is_none(self):
        for value in (1, 1.0, True, b"1", ["1"]):
            assert parse_protocol_header(value) is None


class TestProtocolHeaders:
    def test_the_constants(self):
        assert PROTOCOL_VERSION == 1
        assert SUPPORTED_PROTOCOLS == frozenset({1})
        assert PROTOCOL_HEADER == "X-Zfsbackup-Protocol"
        assert PROTOCOL_REFUSAL_REASON == "protocol"

    def test_the_request_header(self):
        assert protocol_headers() == {"X-Zfsbackup-Protocol": "1"}

    def test_each_call_is_a_fresh_dict(self):
        protocol_headers()["x"] = "y"
        assert protocol_headers() == {"X-Zfsbackup-Protocol": "1"}

    def test_the_wsgi_key_the_helpers_use(self):
        assert PROTOCOL_ENVIRON == "HTTP_X_ZFSBACKUP_PROTOCOL"


# ---------------------------------------------------------------------------
# The 426 itself
# ---------------------------------------------------------------------------


class TestRefusalMatrix:
    @pytest.mark.parametrize("value", BAD_VALUES)
    @pytest.mark.parametrize("method, url, kwargs", ENDPOINTS)
    def test_every_bad_value_is_426_on_every_endpoint(self, authed, zfs, method, url, kwargs, value):
        client = authed.client(None if url == "/enroll" else authed.who, protocol=value)
        resp = getattr(client, method)(url, **kwargs)
        assert resp.status_code == 426, resp.get_data(as_text=True)
        body = resp.get_json()
        assert set(body) == {"error", "reason", "protocol", "supported"}
        assert body["reason"] == PROTOCOL_REFUSAL_REASON == "protocol"
        assert body["protocol"] == 1 and type(body["protocol"]) is int
        assert body["supported"] == [1]
        assert isinstance(body["error"], str) and "Upgrade the client and the server" in body["error"]
        assert resp.headers[PROTOCOL_HEADER] == "1"
        assert "Upgrade" not in resp.headers           # the protocol is not an HTTP upgrade
        _untouched(zfs)
        assert authed.gate.active == 0

    def test_the_error_says_what_was_sent_and_what_is_spoken(self, authed):
        resp = authed.client(protocol="2").get("/whoami")
        assert "protocol version '2'" in resp.get_json()["error"]
        assert "speaks protocol 1" in resp.get_json()["error"]
        resp = authed.client(protocol=None).get("/whoami")
        assert "no protocol version" in resp.get_json()["error"]

    def test_a_huge_value_is_cut_in_the_echo(self, authed):
        resp = authed.client(protocol="9" * 5000).get("/whoami")
        assert resp.status_code == 426
        assert len(resp.get_data()) < 1000

    def test_the_refusal_is_not_gated(self, authed, zfs):
        """426 is answered while draining (it is not a 503), and the gate stays balanced."""
        authed.gate.start_draining()
        resp = authed.client(protocol=None).post(NEGOTIATE, json={"snapshots": [], "config": GOOD_CONFIG})
        assert resp.status_code == 426
        assert authed.gate.active == 0
        _untouched(zfs)

    def test_a_removed_route_is_still_426_before_404(self, authed):
        """Routing errors are answered only after authentication and the protocol check."""
        assert authed.client(protocol="2").get("/config").status_code == 426

    def test_unknown_routes_are_401_without_a_certificate_whatever_the_header(self, authed):
        assert authed.client(None, protocol=None).get("/config").status_code == 401


class TestAcceptedValues:
    @pytest.mark.parametrize("value", ["1", " 1 ", "\t1"])
    def test_the_supported_version_gets_through(self, authed, value):
        resp = authed.client(protocol=value).get("/whoami")
        assert resp.status_code == 200
        assert resp.get_json()["protocol"] == 1

    def test_whoami_reports_the_servers_version(self, authed):
        assert authed.client().get("/whoami").get_json()["protocol"] == PROTOCOL_VERSION

    def test_the_default_test_client_sends_the_header(self, authed):
        """The helpers send it by default: a regression there would 426 the whole suite."""
        assert authed.client().get("/whoami").status_code == 200

    def test_a_response_header_on_a_good_request_is_not_set(self, authed):
        assert PROTOCOL_HEADER not in authed.client().get("/whoami").headers

    def test_a_widened_support_set_accepts_the_new_version(self, authed, monkeypatch):
        from zfsbackup import api
        monkeypatch.setattr(api, "SUPPORTED_PROTOCOLS", frozenset({1, 2}))
        assert authed.client(protocol="2").get("/whoami").status_code == 200
        assert authed.client(protocol="3").get("/whoami").status_code == 426
        body = authed.client(protocol="3").get("/whoami").get_json()
        assert body["supported"] == [1, 2]


class TestHealthIsExempt:
    @pytest.mark.parametrize("value", [None, "", "2", "abc", "1"])
    def test_health_needs_no_header(self, authed, value):
        resp = authed.client(None, protocol=value).get("/health")
        assert resp.status_code == 200
        assert resp.get_json() == {"status": "ok"}

    def test_health_with_a_certificate_and_no_header(self, authed):
        assert authed.client(protocol=None).get("/health").status_code == 200

    def test_a_refused_health_probe_is_not_logged(self, authed, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            authed.client(None, protocol=None).get("/health")
        assert _api_records(caplog) == []

    def test_health_is_the_only_exempt_endpoint(self, authed):
        from zfsbackup.api import PROTOCOL_EXEMPT_ENDPOINTS
        assert PROTOCOL_EXEMPT_ENDPOINTS == {"health"}


# ---------------------------------------------------------------------------
# /enroll: first, before the body
# ---------------------------------------------------------------------------


class TestEnrollChecksFirst:
    @pytest.fixture
    def untouched(self, authed):
        def check():
            gateway = authed.gateway
            assert gateway.precheck_calls == [] and gateway.enroll_calls == []
        return check

    def test_an_oversized_body_is_426_not_413(self, authed, untouched):
        """413 is decided when the body is read: seeing 426 proves it was not read."""
        big = b'{"pad": "' + b"x" * (2 * 1024 * 1024) + b'"}'
        client = authed.client(None, protocol=None)
        resp = client.post("/enroll", data=big, content_type="application/json")
        assert resp.status_code == 426
        untouched()
        # the control: the same body WITH the header is the 413
        resp = authed.client(None).post("/enroll", data=big, content_type="application/json")
        assert resp.status_code == 413

    def test_a_garbage_body_is_426_not_400(self, authed, untouched):
        resp = authed.client(None, protocol=None).post(
            "/enroll", data=b"\xff not json", content_type="application/json")
        assert resp.status_code == 426
        untouched()
        assert authed.client(None).post(
            "/enroll", data=b"\xff not json", content_type="application/json").status_code == 400

    @pytest.mark.parametrize("value", ["2", "abc", "-1", "1,1", ""])
    def test_a_good_body_with_a_bad_version_enrolls_nobody(self, authed, untouched, value):
        cert = make_client_cert()
        token = str(identity.make_token(authed.server_fp)[0])
        body = enroll_body(authed.server_fp, "host-z", cert, token)
        resp = authed.client(None, protocol=value).post("/enroll", json=body)
        assert resp.status_code == 426
        untouched()
        assert authed.gateway.refresh_calls == 0

    def test_the_same_good_body_enrolls_with_the_header(self, authed):
        cert = make_client_cert()
        token = str(identity.make_token(authed.server_fp)[0])
        resp = authed.client(None).post("/enroll", json=enroll_body(authed.server_fp, "host-z", cert, token))
        assert resp.status_code == 200 and authed.gateway.enroll_calls

    def test_it_is_426_while_draining_not_503(self, authed, untouched):
        authed.gate.start_draining()
        assert authed.client(None, protocol=None).post("/enroll", json={}).status_code == 426
        assert authed.gate.active == 0
        untouched()

    def test_the_enrolling_certificate_is_not_asked_for(self, authed, untouched):
        """No certificate is needed (nor looked at) for the 426 of /enroll."""
        resp = authed.client(make_client_cert(), protocol="9").post("/enroll", json={})
        assert resp.status_code == 426


# ---------------------------------------------------------------------------
# Order against the other checks
# ---------------------------------------------------------------------------


class TestOrder:
    @pytest.mark.parametrize("method, url, kwargs", [e.values[:3] for e in ENDPOINTS if e.id != "enroll"],
                             ids=[e.id for e in ENDPOINTS if e.id != "enroll"])
    def test_no_certificate_is_401_not_426(self, authed, zfs, method, url, kwargs):
        resp = getattr(authed.client(None, protocol=None), method)(url, **kwargs)
        assert resp.status_code == 401
        assert resp.get_json() == {"error": "client certificate required"}
        _untouched(zfs)

    @pytest.mark.parametrize("method, url, kwargs", [e.values[:3] for e in ENDPOINTS if e.id != "enroll"],
                             ids=[e.id for e in ENDPOINTS if e.id != "enroll"])
    def test_an_unpinned_certificate_is_403_not_426(self, authed, zfs, method, url, kwargs):
        resp = getattr(authed.client(make_client_cert(), protocol=None), method)(url, **kwargs)
        assert resp.status_code == 403
        assert resp.get_json() == {"error": "client not trusted"}
        _untouched(zfs)

    @pytest.mark.parametrize("value", [None, "2"])
    def test_an_expired_pin_is_403_not_426(self, authed, value):
        cert = make_client_cert()
        authed.gateway.pin(cert, "old-host", not_after=time.time() - 60)
        assert authed.client(cert, protocol=value).get("/whoami").status_code == 403

    def test_an_unauthenticated_caller_never_sees_the_426_body(self, authed):
        """A probe without a pinned certificate learns neither the version nor the support set."""
        for who in (None, make_client_cert()):
            body = authed.client(who, protocol=None).get("/whoami").get_data(as_text=True)
            assert "supported" not in body and "protocol" not in body

    def test_a_426_comes_before_the_client_id_mismatch(self, authed, zfs):
        resp = authed.client(protocol=None).post(
            "/backup/client2/pool/data/negotiate", json={"snapshots": [], "config": GOOD_CONFIG})
        assert resp.status_code == 426
        # the control: with the header the same URL is the 403 mismatch
        resp = authed.client().post(
            "/backup/client2/pool/data/negotiate", json={"snapshots": [], "config": GOOD_CONFIG})
        assert resp.status_code == 403 and resp.get_json()["error"] == "client_id mismatch"
        _untouched(zfs)

    @pytest.fixture
    def pair(self, authed):
        """`authed` with a successor pinned over `authed.who` (the predecessor)."""
        authed.successor = authed.gateway.pin(
            make_client_cert(), CLIENT_ID, predecessor=authed.who.fingerprint)
        return authed

    @pytest.mark.parametrize("value", [None, "2", "abc"])
    def test_a_426_does_not_retire_the_predecessor(self, pair, value):
        resp = pair.client(pair.successor, protocol=value).get("/whoami")
        assert resp.status_code == 426
        assert pair.gateway.retire_calls == []
        assert pair.gateway.refresh_calls == 0

    def test_the_same_request_with_the_header_does_retire_it(self, pair):
        """The control for the test above: the successor's first USE is what retires the pin."""
        assert pair.client(pair.successor, protocol=None).get("/whoami").status_code == 426
        assert pair.gateway.retire_calls == []
        assert pair.client(pair.successor).get("/whoami").status_code == 200
        assert pair.gateway.retire_calls == [pair.successor.fingerprint]

    def test_a_426_on_rotate_rotates_nothing(self, authed):
        successor = make_client_cert()
        body = rotate_body(authed.server_fp, CLIENT_ID, authed.who.fingerprint, successor)
        resp = authed.client(protocol="3").post("/rotate", json=body)
        assert resp.status_code == 426
        assert authed.gateway.rotate_calls == []


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


class TestLogging:
    """The 426 ERROR line goes through the shared per-peer throttle (S-3,
    `api._PerPeerThrottle`, kind `protocol`): ONE line per peer per hour (the key is the peer --
    the address, or `client@address` once authenticated --, never the header value or the
    endpoint), at most 120 lines an hour over all peers, the rest DEBUG. It is logged on
    `logsetup.PEER_LOGGER`."""

    def _levels(self, caplog):
        return [r.levelname for r in _api_records(caplog)]

    @staticmethod
    def _small(clock=None, **kwargs):
        """-> `(limits, clock)`: a `PeerLimits` whose log throttle has the given sizes and a
        frozen fake clock (`clock.frozen += n` moves it)."""
        clock = clock or FakeTime()
        clock.frozen = 1000.0
        kwargs.setdefault("global_max", 10 ** 9)
        return PeerLimits(log=_PerPeerThrottle(clock=clock.monotonic, **kwargs),
                          clock=clock.monotonic), clock

    def test_error_once_then_debug(self, authed, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for _ in range(4):
                assert authed.client(protocol="2").get("/whoami").status_code == 426
        assert self._levels(caplog) == ["ERROR", "DEBUG", "DEBUG", "DEBUG"]

    def test_it_is_logged_on_the_peer_logger(self, authed, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            authed.client(protocol="2").get("/whoami")
        (record,) = [r for r in caplog.records if "refusing" in r.getMessage()]
        assert record.name == PEER_LOGGER == "zfsbackup.api.peer"

    def test_the_error_names_the_client_the_version_and_the_remedy(self, authed, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            authed.client(protocol="2").get("/whoami")
        (record,) = _api_records(caplog)
        text = record.getMessage()
        assert f"client {CLIENT_ID}" in text and "protocol version '2'" in text
        assert "speaks protocol 1" in text and "426" in text
        assert "upgrade the client and the server together" in text

    def test_the_header_value_is_not_part_of_the_key(self, authed, caplog):
        """A missing header, then versions 2, 3 and garbage from the same client: one ERROR
        (S-12 told it once per version, up to three)."""
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for value in (None, "2", "3", "abc", "2", None):
                authed.client(protocol=value).get("/whoami")
        assert self._levels(caplog) == ["ERROR"] + ["DEBUG"] * 5
        assert "no protocol version" in _api_records(caplog)[0].getMessage()

    def test_each_client_gets_its_own_error(self, authed, caplog):
        other = authed.cert_for("client2")
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            authed.client(protocol="2").get("/whoami")
            authed.client(other, protocol="2").get("/whoami")
            authed.client(protocol="2").get("/whoami")
            authed.client(other, protocol="2").get("/whoami")
        assert self._levels(caplog) == ["ERROR", "ERROR", "DEBUG", "DEBUG"]
        assert "client client2" in _api_records(caplog)[1].getMessage()

    def test_the_endpoint_is_not_part_of_the_key(self, authed, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            authed.client(protocol="2").get("/whoami")
            authed.client(protocol="2").post(NEGOTIATE, json={})
            authed.client(protocol="2").get(RESUME_TOKEN)
        assert self._levels(caplog) == ["ERROR", "DEBUG", "DEBUG"]

    def test_enroll_is_logged_once_per_address(self, authed, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for _ in range(3):
                authed.client(None, protocol="2").post("/enroll", json={})
            authed.client(None, protocol="3").post("/enroll", json={})
            authed.client(None, protocol="3").post(
                "/enroll", json={}, environ_overrides=self._addr(5))
        assert self._levels(caplog) == ["ERROR", "DEBUG", "DEBUG", "DEBUG", "ERROR"]
        assert "POST /enroll from 127.0.0.1" in _api_records(caplog)[0].getMessage()
        assert "from 10.0.0.5" in _api_records(caplog)[-1].getMessage()

    def test_the_dedupe_is_per_app(self, caplog):
        a, b = Authed(_config()), Authed(_config())
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            a.client(protocol="2").get("/whoami")
            b.client(protocol="2").get("/whoami")
        assert self._levels(caplog) == ["ERROR", "ERROR"]

    def test_a_huge_header_is_cut_in_the_log(self, authed, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            authed.client(protocol="9" * 5000).get("/whoami")
        (record,) = _api_records(caplog)
        assert len(record.getMessage()) < 500

    def test_the_same_peer_is_told_again_after_an_hour(self, caplog):
        limits, clock = self._small()
        authed = Authed(_config(), peer_limits=limits)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            authed.client(protocol="2").get("/whoami")
            clock.frozen += api.PEER_LOG_EVERY - 1
            authed.client(protocol="2").get("/whoami")
            assert self._levels(caplog) == ["ERROR", "DEBUG"]
            clock.frozen += 1                                    # exactly an hour after the line
            authed.client(protocol="2").get("/whoami")
            authed.client(protocol="2").get("/whoami")
        assert self._levels(caplog) == ["ERROR", "DEBUG", "ERROR", "DEBUG"]

    def test_the_suppressed_lines_are_counted_for_the_summary(self, caplog):
        limits, clock = self._small()
        authed = Authed(_config(), peer_limits=limits)
        for _ in range(5):
            authed.client(protocol="2").get("/whoami")
        clock.frozen += api.PEER_LOG_EVERY
        summary = limits.log.take_summary()
        assert summary is not None and "protocol 4" in summary

    # -- the bound: per peer, and in the number of peers remembered --------------------------------

    @staticmethod
    def _addr(n):
        """The n-th distinct peer address (`REMOTE_ADDR`)."""
        return {"REMOTE_ADDR": f"10.{(n >> 16) & 255}.{(n >> 8) & 255}.{n & 255}"}

    def _enroll(self, authed, n, version):
        return authed.client(None, protocol=version).post(
            "/enroll", json={}, environ_overrides=self._addr(n))

    def test_the_limits_are_one_line_per_peer_and_hour_and_120_an_hour(self):
        assert api.PEER_LOG_EVERY == 3600.0
        assert api.PEER_LOG_GLOBAL_MAX == 120
        assert api.PEER_LOG_KEYS == 4096
        assert not hasattr(api, "PROTOCOL_ERROR_LINES_PER_PEER")
        assert not hasattr(api, "PROTOCOL_LOGGED_PEERS")

    def test_one_address_varying_the_header_gets_one_error_line(self, authed, caplog):
        """`/enroll` is unauthenticated: the peer chooses the header value freely, so the value
        cannot be part of the key."""
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for i in range(500):
                assert self._enroll(authed, 0, str(1000 + i)).status_code == 426
        levels = self._levels(caplog)
        assert len(levels) == 500
        assert levels.count("ERROR") == 1
        assert levels[0] == "ERROR" and set(levels[1:]) == {"DEBUG"}

    def test_an_authenticated_client_is_capped_the_same_way(self, authed, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for i in range(200):
                resp = authed.client(protocol=str(2000 + i)).get("/whoami")
                assert resp.status_code == 426
        assert self._levels(caplog).count("ERROR") == 1

    def test_the_authenticated_key_is_client_and_address(self, authed, caplog):
        other = authed.cert_for("client2")
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for i in range(20):
                authed.client(protocol=str(2000 + i)).get("/whoami")
                authed.client(other, protocol=str(2000 + i)).get("/whoami")
                authed.client(protocol=str(2000 + i)).get(
                    "/whoami", environ_overrides=self._addr(7))
        text = [r.getMessage() for r in _api_records(caplog) if r.levelname == "ERROR"]
        assert len(text) == 3                                             # 3 keys x 1 line
        assert sum(f"client {CLIENT_ID} (127.0.0.1)" in t for t in text) == 1
        assert sum(f"client {CLIENT_ID} (10.0.0.7)" in t for t in text) == 1
        assert sum("client client2 (127.0.0.1)" in t for t in text) == 1

    def test_a_legitimate_client_is_still_logged_in_the_middle_of_a_flood(self, authed, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for i in range(300):
                self._enroll(authed, 0, str(1000 + i))                    # the attacker's address
            caplog.clear()
            assert authed.client(protocol="2").get("/whoami").status_code == 426   # the real client
        (record,) = _api_records(caplog)
        assert record.levelname == "ERROR"
        assert f"client {CLIENT_ID}" in record.getMessage()

    def test_a_flood_of_addresses_cannot_write_more_than_120_error_lines_an_hour(self, authed, caplog):
        """The global cap: 300 distinct addresses are 300 keys, but only the first 120 lines are
        ERROR, and the 121st line of a real client is DEBUG too (the price of the cap)."""
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for n in range(300):
                assert self._enroll(authed, n, "2").status_code == 426
        levels = self._levels(caplog)
        assert len(levels) == 300
        assert levels.count("ERROR") == api.PEER_LOG_GLOBAL_MAX == 120
        assert levels[:120] == ["ERROR"] * 120 and set(levels[120:]) == {"DEBUG"}

    def test_the_cap_is_per_hour_the_next_hour_logs_again(self, caplog):
        limits, clock = self._small(anon_max=3)
        authed = Authed(_config(), peer_limits=limits)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for n in range(6):
                self._enroll(authed, n, "2")
            assert self._levels(caplog) == ["ERROR"] * 3 + ["DEBUG"] * 3
            clock.frozen += api.PEER_LOG_EVERY
            self._enroll(authed, 100, "2")
        assert self._levels(caplog)[-1] == "ERROR"

    def test_the_peer_table_evicts_one_at_a_time(self, caplog):
        """With room for 4 peers, a 5th evicts only the oldest (its next line is logged
        early); the other three stay throttled. The server does not start over for everybody
        when it is flooded with new addresses."""
        limits, _ = self._small(max_keys=4)
        authed = Authed(_config(), peer_limits=limits)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for n in range(5):                                            # peers 0 .. 4
                self._enroll(authed, n, "2")
            assert self._levels(caplog) == ["ERROR"] * 5
            caplog.clear()
            for n in (1, 4, 2):                                           # remembered
                self._enroll(authed, n, "2")
            assert self._levels(caplog) == ["DEBUG"] * 3
            self._enroll(authed, 0, "2")                                  # evicted: the oldest
        assert self._levels(caplog) == ["DEBUG"] * 3 + ["ERROR"]
        assert "from 10.0.0.0" in _api_records(caplog)[-1].getMessage()

    def test_the_table_never_exceeds_the_bound(self):
        limits, _ = self._small(max_keys=8)
        authed = Authed(_config(), peer_limits=limits)
        for n in range(40):
            self._enroll(authed, n, "2")
            assert len(limits.log._last) <= 8
        assert len(limits.log._last) == 8

    def test_no_traceback_and_no_header_value_beyond_the_cut(self, authed, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            authed.client(protocol="ab" + "c" * 100).get("/whoami")
        (record,) = _api_records(caplog)
        assert record.exc_info is None
        assert "c" * 40 not in record.getMessage()


# ---------------------------------------------------------------------------
# A real websocket upgrade over real TLS
# ---------------------------------------------------------------------------


class TestWebsocketUpgrade:
    """The `/stream` upgrade is refused by the same `before_request`, with the real server stack in
    front (TLS, the per-connection handshake, werkzeug): a 426 where the client expects a 101."""

    @pytest.fixture
    def live(self, make_server, tmp_path):
        srv = make_server(dry_run=True)          # a dry run never reaches `zfs create` / `receive`
        client = client_identity(tmp_path / "ws-client.tls")
        srv.pin(client, CLIENT_ID)
        return srv, client

    def _connect(self, srv, client, header):
        sock = identity.connect_pinned(
            "127.0.0.1", srv.port, identity.client_context(client), srv.fp, 10)
        ws = websocket.WebSocket()
        try:
            ws.connect(f"wss://127.0.0.1:{srv.port}{STREAM}?mode=resume", socket=sock, timeout=10,
                       header=header)
        except BaseException:
            ws.close()
            raise
        return ws

    @pytest.mark.parametrize("header", [None, {PROTOCOL_HEADER: "2"}, {PROTOCOL_HEADER: "abc"}],
                             ids=["missing", "unsupported", "malformed"])
    def test_the_upgrade_is_a_426_with_the_refusal_body(self, live, header):
        srv, client = live
        with pytest.raises(websocket.WebSocketBadStatusException) as excinfo:
            self._connect(srv, client, header)
        exc = excinfo.value
        assert exc.status_code == 426
        body = json.loads(exc.resp_body)
        assert body["reason"] == "protocol" and body["protocol"] == 1 and body["supported"] == [1]
        assert srv.seen(STREAM)[0].headers.get("HTTP_UPGRADE", "").lower() == "websocket"

    def test_the_upgrade_succeeds_with_the_header(self, live):
        srv, client = live
        ws = self._connect(srv, client, protocol_headers())
        try:
            assert ws.status == 101
            assert json.loads(ws.recv()) == {"status": "ok", "dry_run": True}
        finally:
            ws.close()
        assert srv.seen(STREAM)[0].headers["HTTP_X_ZFSBACKUP_PROTOCOL"] == "1"

    def test_a_plain_http_client_without_the_header_gets_the_same_426(self, live):
        """No websocket upgrade headers: the same answer (the check is not about the upgrade)."""
        srv, client = live
        session = identity.pinned_session(client, srv.fp, 10)
        try:
            resp = session.get(f"{srv.url}/whoami")
            assert resp.status_code == 426
            session.headers.update(protocol_headers())
            assert session.get(f"{srv.url}/whoami").status_code == 200
        finally:
            session.close()


class TestPlainClientSendsTheHeader:
    """The unbuffered helper the auth tests use sends the header too (and can leave it out)."""

    def test_plain_client(self, authed):
        client = plain_client(authed.app)
        client.environ_base["SSL_CLIENT_CERT"] = authed.who.cert_pem
        resp = client.get("/whoami")
        assert resp.status_code == 200
        resp.close()
        client = plain_client(authed.app, protocol=None)
        client.environ_base["SSL_CLIENT_CERT"] = authed.who.cert_pem
        resp = client.get("/whoami")
        assert resp.status_code == 426
        resp.close()


class TestSuccessBodiesNameTheProtocol:
    """`/whoami`, `/enroll` and `/rotate` 200 bodies carry `protocol == PROTOCOL_VERSION`, so the
    CLI can tell, from the answer that committed something, whether the server speaks its protocol."""

    @pytest.mark.parametrize("outcome, status", [("ENROLLED", "enrolled"), ("ALREADY_TRUSTED", "already_trusted")])
    def test_enroll(self, authed, outcome, status):
        from zfsbackup.config.store.trust import EnrollOutcome
        authed.gateway.enroll_effect = getattr(EnrollOutcome, outcome)
        cert = make_client_cert()
        token = str(identity.make_token(authed.server_fp)[0])
        resp = authed.client(None).post("/enroll", json=enroll_body(authed.server_fp, "host-z", cert, token))
        assert resp.status_code == 200, resp.get_data(as_text=True)
        body = resp.get_json()
        assert body["status"] == status
        assert body["protocol"] == PROTOCOL_VERSION and type(body["protocol"]) is int
        assert set(body) == {"status", "client_id", "fingerprint", "protocol"}

    @pytest.mark.parametrize("outcome", ["ROTATED", "ALREADY"])
    def test_rotate(self, authed, outcome):
        from zfsbackup.config.store.trust import RotateOutcome
        authed.gateway.rotate_effect = getattr(RotateOutcome, outcome)
        successor = make_client_cert()
        resp = authed.client().post(
            "/rotate", json=rotate_body(authed.server_fp, CLIENT_ID, authed.who.fingerprint, successor))
        assert resp.status_code == 200, resp.get_data(as_text=True)
        body = resp.get_json()
        assert body["protocol"] == PROTOCOL_VERSION and type(body["protocol"]) is int
        assert set(body) == {"status", "client_id", "fingerprint", "protocol"}

    def test_whoami(self, authed):
        assert authed.client().get("/whoami").get_json()["protocol"] == PROTOCOL_VERSION

    def test_the_error_bodies_do_not_pretend_to_(self, authed):
        """A refusal that is not the protocol one carries no `protocol` key."""
        resp = authed.client(None).post("/enroll", json={})
        assert resp.status_code == 400 and "protocol" not in resp.get_json()

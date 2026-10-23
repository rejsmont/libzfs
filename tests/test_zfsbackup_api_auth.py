"""Authentication, authorization and the trust endpoints of the API (TLS/mTLS plan M10, M14b).

The wire contract these tests pin is the "M10/M14b wire contract" section of
`docs/tls_mtls_plan.md` (D3, D5, D7).

Layout
------

* `TestAuthentication`        401 / 403 / expired, `/health` and `/enroll` open, every other
                              route closed, unknown URLs.
* `TestClientIdMismatch`      the URL's `<client_id>` against the binding (D5), websocket too.
* `TestAuthorization`         certificate B cannot touch A's data: mismatch 403, a server dataset
                              recorded for another id 409 (the other id is never named), bad
                              dataset names 400.
* `TestWhoami`                `/whoami`.
* `TestEnroll*`               the `/enroll` matrix against a REAL trust store (tokens are
                              really spent or not), the check order, every store outcome, the
                              store's failures, and that the token is never logged.
* `TestRotate*`               `/rotate` against the real store, the check order, the outcomes.
* `TestPredecessorRetirement` the successor's first authenticated request retires the old pin.
* `TestGateway*`              `TrustGateway`'s failed rebuilds, rollbacks, polling against a
                              running refresh, and the serialised store calls.
* `TestCreateApp`             what `create_app` demands and publishes.

Fixtures: `authed` is an app over a stub gateway (`_api_support.FakeGateway`) with one
certificate pinned under `client1`; `real` is an app over a REAL `TrustGateway`, a real config
database and a real server identity (`tls_config_db`), so token redemption, pins, rotation and
retirement are the store's own. Requests carry the certificate in `environ["SSL_CLIENT_CERT"]`,
where the TLS layer puts it. Nothing here touches ZFS (`zfsbackup.api.zfs` is patched) or opens
a network socket.
"""

import base64
import datetime as dt
import json
import logging
import re
import threading
import time
import types
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock

import pytest

from _api_support import (
    plain_client,
    CLIENT_ID,
    SERVER_FP,
    Authed,
    ClientCert,
    FakeGateway,
    RealTrust,
    enroll_body,
    make_client_cert,
    rotate_body,
)
from _tls_support import StubTrust, make_gateway
from cryptography.x509.oid import ExtendedKeyUsageOID
from zfsbackup import api as api_module
from zfsbackup import identity
from zfsbackup.api import (
    CERT_REQUIRED_ERROR,
    ENROLL_REFUSED_ERROR,
    MAX_REQUEST_BYTES,
    MAX_TRUST_REQUEST_BYTES,
    NOT_TRUSTED_ERROR,
    RELOADING_ERROR,
    ROTATE_REFUSED_ERROR,
    STORE_CALL_WAIT,
    TRUST_BUSY_ERROR,
    TRUST_UNAVAILABLE_ERROR,
    UNAUTHENTICATED_ENDPOINTS,
    PinInfo,
    TrustGateway,
    canonical_dataset_config,
    create_app,
)
from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_SOURCE_DATASET, DatasetManager
from zfsbackup.config import BackupConfig, DatasetConfig, RemoteServerConfig
from zfsbackup.config.store import trust as store_trust
from zfsbackup.config.store.errors import ConfigBusy
from zfsbackup.config.store.trust import EnrollOutcome, RotateOutcome, TokenState
from zfsbackup.protocol import PROTOCOL_HEADER, PROTOCOL_VERSION

TARGET = "pool/backups"
HOUR = 3600.0
DAY = 86400.0


def _config(*, remote=True, **kwargs):
    return BackupConfig(
        datasets=[DatasetConfig(name="pool/data")],
        remote_backup=RemoteServerConfig(target_dataset=TARGET, enabled=True) if remote else None,
        **kwargs,
    )


def _authed(mocker, config=None, **kwargs):
    mocker.patch.object(DatasetManager, "datasets", new_callable=PropertyMock, return_value=[])
    return Authed(_config() if config is None else config, **kwargs)


@pytest.fixture
def authed(mocker):
    return _authed(mocker)


@pytest.fixture
def zfs(mocker):
    """Every `zfs` entry point `api` uses, patched: nothing runs, everything is recorded."""
    return types.SimpleNamespace(
        exists=mocker.patch("zfsbackup.api.zfs.exists", return_value=False),
        list=mocker.patch("zfsbackup.api.zfs.list", return_value=[]),
        create=mocker.patch("zfsbackup.api.zfs.create"),
        set=mocker.patch("zfsbackup.api.zfs.set"),
        abort=mocker.patch("zfsbackup.api.zfs.receive.abort"),
        receive=mocker.patch("zfsbackup.api.zfs.receive.filesystem"),
        props=mocker.patch("zfsbackup.api.read_zfsbackup_properties", return_value={}),
        # the local `org.zfsbackup:config` negotiate stored (plan B-1b); a stream is refused without
        value=mocker.patch(
            "zfsbackup.api.zfs.get.value",
            return_value=canonical_dataset_config(DatasetConfig(name="pool/data").to_property()),
        ),
    )


def _untouched(zfs):
    for name in ("exists", "list", "create", "set", "abort", "receive", "props", "value"):
        assert not getattr(zfs, name).called, f"zfs.{name} was called"


@pytest.fixture
def real(mocker, tls_config_db):
    """An app over a REAL gateway, config database and server identity. No certificate is
    pinned and no client is bound (`client_id=None`): use `authed.client(None)` for the
    unauthenticated `/enroll` and `authed.client(cert)` for a pinned certificate.
    `authed.trust` is the `RealTrust`."""
    trust = RealTrust(tls_config_db)
    app = _authed(
        mocker, _config(), gateway=trust.gateway, server_fp=trust.server_fp, client_id=None
    )
    app.trust = trust
    return app


def _anonymous(authed):
    return authed.client(None)


def _enroll(authed, cid, cert, token, *, label=None, **overrides):
    """POST /enroll for `cid` with a valid body; `overrides` replace fields afterwards (so they
    may be invalid, which `enroll_body` would not let us build)."""
    body = {**enroll_body(authed.server_fp, cid, cert, token, label=label), **overrides}
    return _anonymous(authed).post("/enroll", json=body)


def _rotate(authed, current, successor, *, client_id=None, **overrides):
    body = rotate_body(authed.server_fp, client_id or CLIENT_ID, current.fingerprint, successor)
    return authed.client(current).post("/rotate", json={**body, **overrides})


def _raw_post(client, url, body, content_type="application/json"):
    return client.post(url, data=body, content_type=content_type)


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

#: Every route that is not `/health` or `/enroll`, as `(method, url)` with a client id of `client1`.
CLOSED_ROUTES = [
    ("get", "/whoami"),
    ("get", "/config"),
    ("get", "/datasets"),
    ("get", "/datasets/pool/data/snapshots"),
    ("post", "/rotate"),
    ("post", "/backup/client1/pool/data/negotiate"),
    ("get", "/backup/client1/pool/data/resume_token"),
    ("get", "/backup/client1/pool/data/stream"),
]


class TestAuthentication:
    @pytest.mark.parametrize("method, url", CLOSED_ROUTES)
    def test_no_certificate_is_401_and_nothing_runs(self, authed, zfs, method, url):
        resp = getattr(authed.client(None), method)(url)
        assert resp.status_code == 401
        assert resp.get_json() == CERT_REQUIRED_ERROR == {"error": "client certificate required"}
        _untouched(zfs)
        assert authed.gate.active == 0

    @pytest.mark.parametrize("method, url", CLOSED_ROUTES)
    def test_an_unpinned_certificate_is_403_and_nothing_runs(self, authed, zfs, method, url):
        stranger = make_client_cert(cn="stranger")
        resp = getattr(authed.client(stranger), method)(url)
        assert resp.status_code == 403
        assert resp.get_json() == NOT_TRUSTED_ERROR == {"error": "client not trusted"}
        _untouched(zfs)

    @pytest.mark.parametrize("method, url", CLOSED_ROUTES)
    def test_a_pinned_certificate_gets_past_authentication(self, authed, zfs, method, url):
        resp = getattr(authed.client(), method)(url)
        assert resp.status_code not in (401, 403), resp.get_data(as_text=True)

    def test_every_route_but_health_and_enroll_is_closed(self, authed):
        """No route can be added without authentication: walk the URL map."""
        assert UNAUTHENTICATED_ENDPOINTS == {"health", "enroll"}
        client = authed.client(None)
        seen = set()
        for rule in authed.app.url_map.iter_rules():
            if rule.endpoint == "static" or rule.endpoint in UNAUTHENTICATED_ENDPOINTS:
                continue
            url = rule.rule
            for argument in rule.arguments:
                url = url.replace(f"<path:{argument}>", "x/y").replace(f"<{argument}>", "x")
            for method in sorted(rule.methods - {"HEAD", "OPTIONS"}):
                resp = client.open(url, method=method)
                assert resp.status_code == 401, (rule.endpoint, method, url, resp.status_code)
                seen.add(rule.endpoint)
        assert seen == {
            "whoami", "rotate", "backup_negotiate", "backup_stream", "backup_resume_token",
        }, "a route was added or removed: authenticated, the cross-client info routes stay gone (S-4)"

    def test_health_is_open_without_a_certificate_and_with_an_unpinned_one(self, authed):
        assert authed.client(None).get("/health").get_json() == {"status": "ok"}
        assert authed.client(make_client_cert()).get("/health").status_code == 200
        assert authed.gateway.refresh_calls == 0

    def test_health_is_open_even_with_a_garbage_certificate_variable(self, authed):
        client = plain_client(authed.app)
        client.environ_base["SSL_CLIENT_CERT"] = "not a certificate"
        assert client.get("/health").status_code == 200

    def test_enroll_is_open_without_a_certificate(self, authed):
        """Not 401: the answer is the endpoint's own (an empty body is a 400)."""
        resp = authed.client(None).post("/enroll", json={})
        assert resp.status_code == 400
        resp = authed.client(make_client_cert()).post("/enroll", json={})
        assert resp.status_code == 400

    def test_an_empty_certificate_variable_is_no_certificate(self, authed):
        client = plain_client(authed.app)
        client.environ_base["SSL_CLIENT_CERT"] = ""
        assert client.get("/whoami").status_code == 401

    @pytest.mark.parametrize("pem", [
        "not a certificate",
        "-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----\n",
        "-----BEGIN CERTIFICATE-----\n",
    ])
    def test_an_unreadable_certificate_is_not_trusted(self, authed, pem):
        client = plain_client(authed.app)
        client.environ_base["SSL_CLIENT_CERT"] = pem
        resp = client.get("/whoami")
        assert resp.status_code == 403
        assert resp.get_json() == NOT_TRUSTED_ERROR

    def test_the_binding_comes_from_the_pin_not_from_the_certificate(self, authed):
        """A certificate whose subject says `client2` but which is pinned to `client1`."""
        liar = make_client_cert(cn="client2")
        authed.gateway.pin(liar, "client9")
        resp = authed.client(liar).get("/whoami")
        assert resp.get_json() == {"client_id": "client9", "fingerprint": liar.fingerprint, "protocol": PROTOCOL_VERSION}

    def test_the_fingerprint_is_the_sha256_of_the_der_not_of_the_text(self, authed):
        """The same certificate with different PEM framing is the same pin."""
        who = authed.who
        framed = who.cert_pem.replace("\n", "\r\n")
        client = plain_client(authed.app)
        client.environ_base["SSL_CLIENT_CERT"] = framed
        assert client.get("/whoami").get_json()["fingerprint"] == who.fingerprint

    def test_several_certificates_of_one_id_and_several_ids_each_work(self, authed):
        second = authed.gateway.pin(make_client_cert(), CLIENT_ID)
        other = authed.cert_for("client2")
        for cert, expected in ((authed.who, CLIENT_ID), (second, CLIENT_ID), (other, "client2")):
            assert authed.client(cert).get("/whoami").get_json()["client_id"] == expected

    def test_the_lookup_is_by_fingerprint_so_a_revoked_pin_stops_working(self, authed):
        assert authed.client().get("/whoami").status_code == 200
        del authed.gateway.pins[authed.who.fingerprint]
        assert authed.client().get("/whoami").status_code == 403

    def test_a_refusal_is_logged_with_the_fingerprint_and_never_the_certificate(self, authed, caplog):
        stranger = make_client_cert(cn="stranger")
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            authed.client(stranger).get("/whoami")
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        text = warnings[0].getMessage()
        assert stranger.fingerprint in text and "not pinned" in text
        assert "BEGIN CERTIFICATE" not in caplog.text

    def test_a_missing_certificate_is_logged_at_debug_only(self, authed, caplog):
        """A scanner without a certificate must not flood the log."""
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            authed.client(None).get("/whoami")
        assert [r.levelno for r in caplog.records] == [logging.DEBUG]


class TestExpiry:
    def _app(self, mocker, now):
        clock = types.SimpleNamespace(now=now)
        authed = _authed(mocker, clock=lambda: clock.now)
        authed.clock = clock
        return authed

    def test_a_pin_is_valid_up_to_and_including_its_not_after(self, mocker):
        pin_until = 2_000_000_000.0
        authed = self._app(mocker, pin_until - 1)
        authed.gateway.pin(authed.who, CLIENT_ID, not_after=pin_until)
        client = authed.client()
        assert client.get("/whoami").status_code == 200
        authed.clock.now = pin_until
        assert client.get("/whoami").status_code == 200
        authed.clock.now = pin_until + 0.5
        resp = client.get("/whoami")
        assert resp.status_code == 403 and resp.get_json() == NOT_TRUSTED_ERROR

    def test_the_expiry_is_that_of_the_pin_not_of_the_certificate_in_the_request(self, mocker):
        authed = self._app(mocker, 1_000.0)
        authed.gateway.pin(authed.who, CLIENT_ID, not_after=500.0)
        assert authed.client().get("/whoami").status_code == 403

    def test_an_expired_pin_is_logged_as_expired(self, mocker, caplog):
        authed = self._app(mocker, 1_000.0)
        authed.gateway.pin(authed.who, CLIENT_ID, not_after=500.0)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            authed.client().get("/whoami")
        assert "expired" in caplog.text and authed.who.fingerprint in caplog.text

    def test_health_and_enroll_do_not_care_about_the_clock(self, mocker):
        authed = self._app(mocker, 1e12)
        assert authed.client(None).get("/health").status_code == 200

    def test_expiry_is_checked_before_the_binding(self, mocker, zfs):
        """An expired certificate asking for someone else's URL learns nothing about its binding."""
        authed = self._app(mocker, 1_000.0)
        authed.gateway.pin(authed.who, CLIENT_ID, not_after=500.0)
        resp = authed.client().get("/backup/client2/pool/data/resume_token")
        assert resp.status_code == 403 and resp.get_json() == NOT_TRUSTED_ERROR
        _untouched(zfs)


class TestUnknownUrls:
    def test_without_a_certificate_an_unknown_url_is_401(self, authed):
        """Routing errors are answered only to authenticated clients."""
        assert authed.client(None).get("/no/such/route").status_code == 401

    def test_with_a_pinned_certificate_it_is_404(self, authed):
        assert authed.client().get("/no/such/route").status_code == 404

    def test_with_an_unpinned_certificate_it_is_403(self, authed):
        assert authed.client(make_client_cert()).get("/no/such/route").status_code == 403

    @pytest.mark.parametrize("method", ["get", "post"])
    def test_register_is_gone(self, authed, zfs, method):
        assert getattr(authed.client(None), method)("/backup/register").status_code == 401
        resp = getattr(authed.client(), method)("/backup/register", json={"client_id": "client1"})
        assert resp.status_code == 404
        _untouched(zfs)

    def test_a_wrong_method_is_405_for_a_pinned_client(self, authed):
        assert authed.client().post("/whoami").status_code == 405


# ---------------------------------------------------------------------------
# The URL's client id against the binding (D5)
# ---------------------------------------------------------------------------

#: Routes that carry a `<client_id>` (the URL names `X`).
ID_ROUTES = [
    ("post", "/backup/{}/pool/data/negotiate"),
    ("get", "/backup/{}/pool/data/resume_token"),
    ("get", "/backup/{}/pool/data/stream"),
]

#: werkzeug only matches the websocket rule for a websocket upgrade request; a plain GET to the
#: stream URL is a routing 400 (after authentication).
WS_HEADERS = {
    "Upgrade": "websocket", "Connection": "Upgrade",
    "Sec-WebSocket-Key": base64.b64encode(b"0123456789abcdef").decode(),
    "Sec-WebSocket-Version": "13",
}


def _call_id_route(client, method, url):
    headers = WS_HEADERS if url.endswith("/stream") else {}
    return getattr(client, method)(url, headers=headers)


class TestClientIdMismatch:
    @pytest.mark.parametrize("method, template", ID_ROUTES)
    def test_a_url_naming_another_client_is_403_with_the_bound_id(self, authed, zfs, method, template):
        resp = _call_id_route(authed.client(), method, template.format("client2"))
        assert resp.status_code == 403
        assert resp.get_json() == {"error": "client_id mismatch", "bound": CLIENT_ID}
        _untouched(zfs)
        assert authed.gate.active == 0

    @pytest.mark.parametrize("method, template", ID_ROUTES)
    def test_the_bound_id_is_not_a_prefix_match_or_case_insensitive(self, authed, zfs, method, template):
        for wrong in ("client", "client11", "Client1", "client1.", "client1 ", "client1\\"):
            resp = _call_id_route(authed.client(), method, template.format(wrong))
            assert resp.status_code == 403, wrong
        _untouched(zfs)

    def test_the_websocket_upgrade_is_refused_before_the_handler(self, authed, zfs):
        """The same check runs on the websocket route's upgrade request."""
        resp = authed.client().get("/backup/client2/pool/data/stream?mode=fresh", headers=WS_HEADERS)
        assert resp.status_code == 403
        assert resp.get_json()["error"] == "client_id mismatch"
        _untouched(zfs)

    def test_the_websocket_route_without_a_certificate_is_401(self, authed, zfs):
        resp = authed.client(None).get("/backup/client1/pool/data/stream?mode=fresh", headers=WS_HEADERS)
        assert resp.status_code == 401
        _untouched(zfs)

    def test_the_matching_id_is_not_refused(self, authed, zfs):
        resp = authed.client().get("/backup/client1/pool/data/resume_token")
        assert resp.status_code == 200

    def test_a_mismatch_is_logged_with_both_ids(self, authed, zfs, caplog):
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            authed.client().post("/backup/client2/pool/data/negotiate", json={})
        assert "client2" in caplog.text and CLIENT_ID in caplog.text and authed.who.fingerprint in caplog.text

    def test_routes_without_a_client_id_are_not_subject_to_the_check(self, authed):
        rules = [r for r in authed.app.url_map.iter_rules()
                 if r.endpoint not in UNAUTHENTICATED_ENDPOINTS and r.endpoint != "static"
                 and "client_id" not in r.arguments and "GET" in r.methods]
        assert [r.rule for r in rules] == ["/whoami"]
        for rule in rules:
            assert authed.client().get(rule.rule).status_code == 200

    def test_the_mismatch_does_not_run_a_predecessor_retirement(self, authed, zfs):
        """A request that is refused has not 'used' the successor."""
        old = authed.cert_for("old")
        succ = authed.gateway.pin(make_client_cert(), CLIENT_ID, predecessor=old.fingerprint)
        resp = authed.client(succ).get("/backup/client2/pool/data/resume_token")
        assert resp.status_code == 403
        assert authed.gateway.retire_calls == []


# ---------------------------------------------------------------------------
# Authorization: certificate B cannot touch A's data
# ---------------------------------------------------------------------------


def _stream(authed, cert, client_id, *, mode="resume", dataset="pool/data", ws=None):
    """Drive the websocket handler authenticated as `cert`, with a URL naming `client_id`."""
    ws = ws or MagicMock()
    ws.receive.side_effect = [json.dumps({"done": True})]
    handler = authed.app.view_functions["backup_stream"].__wrapped__
    with authed.context(f"/backup/{client_id}/{dataset}/stream", who=cert, query_string={"mode": mode}):
        handler(ws, client_id, dataset)
    return ws


class TestAuthorization:
    """Server datasets are `<target>/<bound id>/...`. Certificate B is bound to `host-b`; host
    A's data is at `pool/backups/host-a/data`."""

    A_DATA = "pool/backups/host-a/data"
    B_DATA = "pool/backups/host-b/data"

    @pytest.fixture
    def two(self, authed):
        authed.a = authed.cert_for("host-a")
        authed.b = authed.cert_for("host-b")
        return authed

    @pytest.mark.parametrize("method, template", ID_ROUTES)
    def test_b_naming_a_in_the_url_is_403_and_a_is_never_looked_at(self, two, zfs, method, template):
        resp = _call_id_route(two.client(two.b), method, template.format("host-a"))
        assert resp.status_code == 403
        assert resp.get_json() == {"error": "client_id mismatch", "bound": "host-b"}
        _untouched(zfs)

    def test_a_dataset_recorded_for_another_id_is_409_in_negotiate_and_names_nobody(self, two, zfs):
        """B's own path holds a dataset stamped `host-a` (an operator rename, say)."""
        zfs.exists.return_value = True
        zfs.props.return_value = {PROP_CLIENT_ID: "host-a", PROP_SOURCE_DATASET: "pool/data"}
        resp = two.client(two.b).post(
            "/backup/host-b/pool/data/negotiate",
            json={"snapshots": [], "config": DatasetConfig(name="pool/data").to_property()},
        )
        assert resp.status_code == 409
        assert "host-a" not in resp.get_data(as_text=True)
        assert PROP_CLIENT_ID in resp.get_json()["error"]
        zfs.set.assert_not_called()          # the recorded owner is not overwritten
        zfs.create.assert_not_called()
        zfs.list.assert_not_called()
        assert two.gate.active == 0

    def test_the_same_in_resume_token(self, two, zfs):
        zfs.exists.return_value = True
        zfs.props.return_value = {PROP_CLIENT_ID: "host-a"}
        resp = two.client(two.b).get("/backup/host-b/pool/data/resume_token")
        assert resp.status_code == 409
        assert "host-a" not in resp.get_data(as_text=True)
        zfs.list.assert_not_called()

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_the_same_in_stream(self, two, zfs, mode):
        zfs.exists.return_value = True
        zfs.props.return_value = {PROP_CLIENT_ID: "host-a"}
        ws = _stream(two, two.b, "host-b", mode=mode)
        sent = [json.loads(c[0][0]) for c in ws.send.call_args_list]
        assert len(sent) == 1 and PROP_CLIENT_ID in sent[0]["error"]
        assert "host-a" not in sent[0]["error"]
        for name in ("create", "set", "abort", "receive"):
            assert not getattr(zfs, name).called, name
        ws.receive.assert_not_called()
        assert two.gate.active == 0

    def test_stream_refuses_in_a_dry_run_too(self, mocker, zfs):
        authed = _authed(mocker, _config(dry_run=True))
        zfs.exists.return_value = True
        zfs.props.return_value = {PROP_CLIENT_ID: "someone-else"}
        ws = _stream(authed, authed.who, CLIENT_ID)
        sent = json.loads(ws.send.call_args[0][0])
        assert "dry_run" not in sent and PROP_CLIENT_ID in sent["error"]

    def test_the_message_says_what_to_do_without_naming_the_other_id(self, two, zfs):
        zfs.exists.return_value = True
        zfs.props.return_value = {PROP_CLIENT_ID: "secret-other-host"}
        resp = two.client(two.b).get("/backup/host-b/pool/data/resume_token")
        error = resp.get_json()["error"]
        assert "secret-other-host" not in error
        assert "host-b" in error and "server operator" in error

    def test_a_dataset_recorded_for_the_bound_id_or_for_nobody_is_fine(self, two, zfs):
        zfs.exists.return_value = True
        for recorded in ({PROP_CLIENT_ID: "host-b"}, {}):
            zfs.props.return_value = recorded
            assert two.client(two.b).get("/backup/host-b/pool/data/resume_token").status_code == 200

    def test_a_missing_dataset_is_never_a_conflict(self, two, zfs):
        zfs.exists.return_value = False
        zfs.props.side_effect = AssertionError("properties of a missing dataset were read")
        assert two.client(two.b).get("/backup/host-b/pool/data/resume_token").status_code == 200

    def test_a_property_read_failure_refuses_rather_than_assumes(self, two, zfs):
        zfs.exists.return_value = True
        zfs.props.side_effect = RuntimeError("zfs get failed")
        resp = two.client(two.b).post(
            "/backup/host-b/pool/data/negotiate",
            json={"snapshots": [], "config": DatasetConfig(name="pool/data").to_property()},
        )
        assert resp.status_code == 500
        zfs.set.assert_not_called()

    def test_each_client_streams_into_its_own_tree(self, two, zfs):
        zfs.exists.return_value = True
        for cert, owner in ((two.a, "host-a"), (two.b, "host-b")):
            _stream(two, cert, owner)
        zfs.create.assert_not_called()          # R0: the handler creates and stamps nothing
        zfs.set.assert_not_called()
        # received with `-o` (plan B-1b), always with the bound id
        received = [(str(c.args[0]), c.kwargs["props"][PROP_CLIENT_ID]) for c in zfs.receive.call_args_list]
        assert received == [(self.A_DATA, "host-a"), (self.B_DATA, "host-b")]
        assert [str(c.args[0]) for c in zfs.value.call_args_list] == [self.A_DATA, self.B_DATA]

    def test_each_client_creates_its_new_dataset_in_its_own_tree(self, two, zfs):
        """The datasets do not exist: the receive creates them (no `-F`), still under the bound id."""
        zfs.exists.return_value = False
        for cert, owner in ((two.a, "host-a"), (two.b, "host-b")):
            resp = two.client(cert).post(
                f"/backup/{owner}/pool/data/negotiate",
                json={"snapshots": [], "config": DatasetConfig(name="pool/data").to_property()},
            )
            assert resp.status_code == 200
            _stream(two, cert, owner)
        assert [str(c.args[0]) for c in zfs.receive.call_args_list] == [self.A_DATA, self.B_DATA]
        assert [c.kwargs["force"] for c in zfs.receive.call_args_list] == [False, False]
        assert [c.kwargs["props"][PROP_CLIENT_ID] for c in zfs.receive.call_args_list] == ["host-a", "host-b"]
        zfs.set.assert_not_called()
        # only the parents were made, each under its own id
        assert [str(c.args[0]) for c in zfs.create.call_args_list] == [
            "pool/backups/host-a", "pool/backups/host-b"]

    def test_a_pending_config_of_a_is_never_used_by_b(self, two, zfs):
        """A negotiated first and has not streamed; B streams the same dataset name. B's server
        dataset is its own (`host-b`), so A's pending entry is not found: B is told to negotiate."""
        zfs.exists.return_value = False
        resp = two.client(two.a).post(
            "/backup/host-a/pool/data/negotiate",
            json={"snapshots": [], "config": DatasetConfig(name="pool/data").to_property()},
        )
        assert resp.status_code == 200
        ws = _stream(two, two.b, "host-b")
        sent = [json.loads(c[0][0]) for c in ws.send.call_args_list]
        assert len(sent) == 1 and "negotiate again" in sent[0]["error"]
        zfs.receive.assert_not_called()

    def test_b_cannot_reach_a_through_the_handler_argument_either(self, two, zfs):
        """The websocket view is handed the URL's id, but uses the binding."""
        zfs.exists.side_effect = [True, True, False]
        ws = MagicMock()
        ws.receive.side_effect = [json.dumps({"done": True})]
        handler = two.app.view_functions["backup_stream"].__wrapped__
        with two.context("/backup/host-b/pool/data/stream", who=two.b, query_string={"mode": "resume"}):
            handler(ws, "host-a", "pool/data")
        assert str(zfs.receive.call_args.args[0]) == self.B_DATA
        assert str(zfs.value.call_args.args[0]) == self.B_DATA            # the config is B's, not A's
        assert zfs.receive.call_args.kwargs["props"][PROP_CLIENT_ID] == "host-b"
        zfs.create.assert_not_called()


class TestBadDatasetNames:
    """A client dataset that makes `<target>/<bound id>/<rest>` an invalid name is a 400."""

    @pytest.mark.parametrize("encoded", ["pool/a%25b", "pool/a%40b", "pool/a%23b", "pool/.hidden"])
    def test_negotiate_and_resume_token_say_400_and_run_nothing(self, authed, zfs, encoded):
        resp = authed.client().post(f"/backup/client1/{encoded}/negotiate", json={"config": "x"})
        assert resp.status_code == 400
        assert resp.get_json()["error"].startswith("invalid dataset name ")
        resp = authed.client().get(f"/backup/client1/{encoded}/resume_token")
        assert resp.status_code == 400
        _untouched(zfs)
        assert authed.gate.active == 0

    def test_the_websocket_gets_an_error_message_and_no_claim(self, authed, zfs):
        ws = MagicMock()
        handler = authed.app.view_functions["backup_stream"].__wrapped__
        with authed.context("/backup/client1/pool/data/stream", query_string={"mode": "fresh"}):
            handler(ws, "client1", "pool/a%b")
        sent = json.loads(ws.send.call_args[0][0])
        assert sent["error"].startswith("invalid dataset name ")
        _untouched(zfs)
        ws.receive.assert_not_called()
        assert authed.gate.active == 0

    def test_a_name_valid_with_the_bound_id_is_served(self, authed, zfs):
        assert authed.client().get("/backup/client1/pool/data/resume_token").status_code == 200


# ---------------------------------------------------------------------------
# /whoami
# ---------------------------------------------------------------------------


class TestWhoami:
    def test_it_reports_the_bound_id_and_the_certificates_fingerprint(self, authed):
        resp = authed.client().get("/whoami")
        assert resp.status_code == 200
        assert resp.get_json() == {
            "client_id": CLIENT_ID, "fingerprint": authed.who.fingerprint, "protocol": PROTOCOL_VERSION,
        }

    def test_the_fingerprint_is_64_lowercase_hex_of_the_der(self, authed):
        fp = authed.client().get("/whoami").get_json()["fingerprint"]
        assert len(fp) == 64 and fp == fp.lower() and int(fp, 16) >= 0
        assert fp == identity.fingerprint_pem(authed.who.cert_pem)

    def test_each_certificate_is_told_its_own_id_and_fingerprint(self, authed):
        other = authed.cert_for("client2")
        assert authed.client(other).get("/whoami").get_json() == {
            "client_id": "client2", "fingerprint": other.fingerprint, "protocol": PROTOCOL_VERSION,
        }

    def test_requires_a_pinned_certificate(self, authed):
        assert authed.client(None).get("/whoami").status_code == 401
        assert authed.client(make_client_cert()).get("/whoami").status_code == 403

    def test_it_is_not_gated_and_answers_while_draining(self, authed):
        authed.gate.start_draining()
        assert authed.client().get("/whoami").status_code == 200

    def test_it_answers_when_remote_backup_is_disabled_and_in_a_dry_run(self, mocker):
        for config in (_config(remote=False), _config(dry_run=True)):
            authed = _authed(mocker, config)
            assert authed.client().get("/whoami").get_json()["client_id"] == CLIENT_ID

    def test_it_runs_no_zfs(self, authed, zfs):
        authed.client().get("/whoami")
        _untouched(zfs)

    def test_it_takes_no_arguments_and_accepts_only_get(self, authed):
        assert authed.client().post("/whoami", json={}).status_code == 405
        assert authed.client().get("/whoami?client_id=other").get_json()["client_id"] == CLIENT_ID

    def test_after_a_rotation_it_reports_the_certificate_in_use(self, authed):
        successor = authed.gateway.pin(make_client_cert(), CLIENT_ID)
        assert authed.client(successor).get("/whoami").get_json()["fingerprint"] == successor.fingerprint
        assert authed.client().get("/whoami").get_json()["fingerprint"] == authed.who.fingerprint


# ---------------------------------------------------------------------------
# /enroll against the real store
# ---------------------------------------------------------------------------


def _refused_body():
    return dict(ENROLL_REFUSED_ERROR)


@pytest.mark.integration
class TestEnroll:
    def test_enrolling_pins_the_certificate_and_spends_the_token(self, real, zfs):
        cert = make_client_cert()
        token = real.trust.token()
        resp = _enroll(real, "host-a", cert, token, label="box A")
        assert resp.status_code == 200
        assert resp.get_json() == {
            "status": "enrolled", "client_id": "host-a", "fingerprint": cert.fingerprint,
            "protocol": PROTOCOL_VERSION,
        }
        assert real.trust.token_state() is TokenState.CONSUMED
        pin = real.trust.pins()[cert.fingerprint]
        assert pin.client_id == "host-a" and pin.predecessor is None
        assert real.trust.clients()["host-a"].label == "box A"

    def test_the_certificate_works_on_the_very_next_request(self, real, zfs):
        cert = make_client_cert()
        assert real.client(cert).get("/whoami").status_code == 403
        assert _enroll(real, "host-a", cert, real.trust.token()).status_code == 200
        assert real.client(cert).get("/whoami").get_json() == {
            "client_id": "host-a", "fingerprint": cert.fingerprint,
            "protocol": PROTOCOL_VERSION,
        }

    def test_the_trust_view_is_refreshed_before_the_response_is_sent(self, real, zfs):
        """The bounded refresh runs inline, so the 200 is the cue: the certificate is already in the
        view when the client sees it, whether or not the response has been closed."""
        cert = make_client_cert()
        body = enroll_body(real.server_fp, "host-a", cert, real.trust.token())
        client = plain_client(real.app)
        resp = client.post("/enroll", json=body)              # unbuffered: not closed
        assert resp.status_code == 200
        assert real.gateway.lookup(cert.fingerprint) is not None
        resp.close()
        assert real.gateway.lookup(cert.fingerprint) is not None

    def test_a_refresh_that_cannot_run_in_time_is_retried_once_the_response_has_been_sent(
        self, real, zfs, monkeypatch
    ):
        """Another refresh holds the gateway for the whole inline budget: the 200 goes out
        with the view stale, and the retry at response close (not the next poll) applies it."""
        monkeypatch.setattr(api_module, "REQUEST_REFRESH_WAIT", 0.05)
        cert = make_client_cert()
        body = enroll_body(real.server_fp, "host-a", cert, real.trust.token())
        client = plain_client(real.app)
        real.gateway._lock.acquire()
        try:
            resp = client.post("/enroll", json=body)
            assert resp.status_code == 200 and real.trust.pins()   # committed
            assert real.gateway.lookup(cert.fingerprint) is None   # inline refresh skipped
        finally:
            real.gateway._lock.release()
        resp.close()                                               # the after-send retry
        assert real.gateway.lookup(cert.fingerprint) is not None

    def test_it_needs_no_certificate_and_no_content_type(self, real, zfs):
        cert = make_client_cert()
        body = enroll_body(real.server_fp, "host-a", cert, real.trust.token())
        resp = _raw_post(_anonymous(real), "/enroll", json.dumps(body), content_type="text/plain")
        assert resp.status_code == 200

    def test_a_client_id_dataset_is_looked_up_under_the_target(self, real, zfs):
        _enroll(real, "host-a", make_client_cert(), real.trust.token())
        assert [str(c[0][0]) for c in zfs.exists.call_args_list] == [f"{TARGET}/host-a"]

    def test_the_label_is_optional_and_empty_means_none(self, real, zfs):
        _enroll(real, "host-a", make_client_cert(), real.trust.token())
        _enroll(real, "host-b", make_client_cert(), real.trust.token(), label="")
        clients = real.trust.clients()
        assert clients["host-a"].label is None and clients["host-b"].label is None

    def test_a_label_of_128_characters_and_non_ascii_text_is_accepted(self, real, zfs):
        label = "ü" * 127 + "x"
        assert _enroll(real, "host-a", make_client_cert(), real.trust.token(), label=label).status_code == 200
        assert real.trust.clients()["host-a"].label == label

    def test_the_gateway_is_refreshed_once_after_an_enrollment(self, mocker, real, zfs):
        refresh = mocker.spy(real.gateway, "refresh")
        _enroll(real, "host-a", make_client_cert(), real.trust.token())
        assert refresh.call_count == 1

    def test_an_id_at_the_grammar_limits_is_accepted(self, real, zfs):
        for client_id in ("a" * 64, "A.b_c-d", "9", "x-"):
            resp = _enroll(real, client_id, make_client_cert(), real.trust.token())
            assert resp.status_code == 200, (client_id, resp.get_json())

    @pytest.mark.parametrize("client_id", ["-x", ".x", "_x"])
    def test_an_id_with_a_leading_symbol_is_refused_by_the_grammar_and_the_token_is_kept(
        self, real, zfs, client_id
    ):
        """A7 starts an id with a letter or digit: `<target>/-x` could never be a dataset name,
        and the grammar says so before `zfs` is asked anything."""
        token = real.trust.token()
        resp = _enroll(real, "host-a", make_client_cert(), token, client_id=client_id)
        assert resp.status_code == 400
        error = resp.get_json()["error"]
        assert error.startswith("client_id:") and "must start with an ASCII letter or digit" in error
        assert "cannot name a dataset" not in error
        assert real.trust.token_state() is TokenState.ACTIVE
        zfs.exists.assert_not_called()

    def test_an_id_the_grammar_allows_but_the_target_makes_too_long_is_400_and_the_token_is_kept(
        self, mocker, zfs, tls_config_db
    ):
        """The one way left for `<target>/<client id>` to be no dataset name: it passes the
        grammar (64 characters) but is longer than ZFS allows under this target."""
        trust = RealTrust(tls_config_db)
        long_target = "tank/" + "t" * 200
        app = _authed(
            mocker,
            BackupConfig(
                datasets=[DatasetConfig(name="pool/data")],
                remote_backup=RemoteServerConfig(target_dataset=long_target, enabled=True),
            ),
            gateway=trust.gateway, server_fp=trust.server_fp, client_id=None,
        )
        token = trust.token()
        resp = _enroll(app, "x" * 64, make_client_cert(), token)
        assert resp.status_code == 400 and "cannot name a dataset" in resp.get_json()["error"]
        assert trust.token_state() is TokenState.ACTIVE
        zfs.exists.assert_not_called()


@pytest.mark.integration
class TestEnrollRefusals:
    """The 403 for every token problem is one and the same body."""

    @pytest.fixture
    def tokens(self, real, zfs):
        """-> `{case: token}`: each token is unusable for a different reason."""
        out = {}
        out["none"] = str(identity.make_token(real.trust.server_fp)[0])        # no token row at all
        other_server = str(identity.make_token("cd" * 32)[0])
        out["other server"] = other_server
        # an expired token, then (replacing it) a spent one, then a wrong secret and a scoped one
        out["expired"] = real.trust.token(ttl=10.0, created_at=time.time() - 100)
        spent = real.trust.token()
        assert _enroll(real, "spender", make_client_cert(), spent).status_code == 200
        out["spent"] = spent
        live = real.trust.token()
        out["wrong secret"] = str(identity.make_token(real.trust.server_fp)[0])
        scoped = real.trust.token(client_id="spender")
        out["scoped to another id"] = scoped
        return out

    @pytest.mark.parametrize("case", [
        "none", "other server", "expired", "spent", "wrong secret", "scoped to another id",
    ])
    def test_the_refusal_is_identical_for_every_token_problem(self, real, tokens, case):
        resp = _enroll(real, "host-z", make_client_cert(), tokens[case])
        assert resp.status_code == 403
        assert resp.get_json() == ENROLL_REFUSED_ERROR == {
            "error": "enrollment refused", "reason": "refused",
        }

    def test_the_bytes_are_identical_too(self, real, tokens):
        bodies = {
            _enroll(real, "host-z", make_client_cert(), tokens[case]).get_data()
            for case in tokens
        }
        assert len(bodies) == 1

    def test_a_refused_enrollment_pins_nothing(self, real, tokens):
        before = real.trust.trust_key()
        for case in tokens:
            _enroll(real, "host-z", make_client_cert(), tokens[case])
        assert real.trust.trust_key() == before
        assert "host-z" not in real.trust.clients()

    def test_a_scoped_token_cannot_enroll_another_id(self, real, zfs):
        _enroll(real, "host-q", make_client_cert(), real.trust.token())
        scoped = real.trust.token(client_id="host-q")
        assert _enroll(real, "host-z", make_client_cert(), scoped).status_code == 403
        assert real.trust.token_state() is TokenState.ACTIVE

    def test_a_token_for_another_server_is_refused_before_the_certificate_is_looked_at(self, real, zfs):
        """So a bad certificate cannot be used to tell a foreign token from a valid one."""
        token = str(identity.make_token("cd" * 32)[0])
        expired = make_client_cert(not_after=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1),
                                   not_before=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=9))
        resp = _enroll(real, "host-z", expired, token)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR


@pytest.mark.integration
class TestEnrollCheckOrder:
    """shape 400 -> grammar 400 -> label 400 -> token parse 400 -> other server 403 ->
    certificate 422 -> signature 403 -> token pre-check 403 -> `zfs.exists` 503 -> store."""

    @pytest.fixture
    def token(self, real):
        return real.trust.token()

    def test_a_bad_certificate_with_a_bad_signature_is_422_not_403(self, real, zfs, token):
        expired = make_client_cert(
            not_before=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=9),
            not_after=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1),
        )
        resp = _enroll(real, "host-z", expired, token, signature=base64.b64encode(b"junk").decode())
        assert resp.status_code == 422
        assert resp.get_json()["reason"] == "bad_certificate"
        assert real.trust.token_state() is TokenState.ACTIVE

    def test_a_bad_signature_is_checked_before_zfs_is_asked_anything(self, real, zfs, token):
        zfs.exists.return_value = True
        resp = _enroll(real, "host-z", make_client_cert(), token,
                       signature=base64.b64encode(b"junk").decode())
        assert resp.status_code == 403 and resp.get_json()["reason"] == "bad_signature"
        zfs.exists.assert_not_called()

    def test_the_token_is_prechecked_before_zfs_is_asked_anything(self, real, zfs, token):
        """A wrong secret with a valid signature is the store's 403 refusal and costs no `zfs`
        call; with a healthy token the same request does reach `zfs`."""
        wrong = str(identity.make_token(real.trust.server_fp)[0])
        zfs.exists.side_effect = RuntimeError("zfs hung")           # would be a 503 if it were asked
        resp = _enroll(real, "host-z", make_client_cert(), wrong)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR
        zfs.exists.assert_not_called()
        assert _enroll(real, "host-z", make_client_cert(), token).status_code == 503
        zfs.exists.assert_called_once()

    def test_a_well_formed_token_for_this_server_with_no_token_row_is_403_and_runs_no_zfs(
        self, real, zfs
    ):
        assert real.trust.token_state() is TokenState.NONE
        fresh = str(identity.make_token(real.trust.server_fp)[0])
        resp = _enroll(real, "host-z", make_client_cert(), fresh)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR
        zfs.exists.assert_not_called()
        assert real.trust.pins() == {}

    def test_a_spent_token_runs_no_zfs_either(self, real, zfs):
        spent = real.trust.token()
        assert _enroll(real, "host-a", make_client_cert(), spent).status_code == 200
        zfs.exists.reset_mock()
        assert _enroll(real, "host-b", make_client_cert(), spent).status_code == 403
        zfs.exists.assert_not_called()

    def test_an_expired_token_with_the_right_secret_still_reaches_the_store(self, real, zfs):
        """The pre-check lets it through on purpose, so the store can retire it durably."""
        expired = real.trust.token(ttl=10.0, created_at=time.time() - 100)
        resp = _enroll(real, "host-z", make_client_cert(), expired)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR
        zfs.exists.assert_called_once()
        assert real.trust.token_state() is TokenState.EXPIRED

    def test_the_bad_signature_is_still_checked_before_the_precheck(self, mocker, real, zfs, token):
        spy = mocker.spy(real.gateway, "token_precheck")
        _enroll(real, "host-z", make_client_cert(), token, signature=base64.b64encode(b"junk").decode())
        spy.assert_not_called()

    def test_the_grammar_is_checked_before_the_label_and_the_token(self, real, zfs):
        token = real.trust.token()
        resp = _enroll(real, "host-z", make_client_cert(), token, label="x" * 500,
                       client_id=".bad", token_secret="garbage")
        assert resp.status_code == 400
        assert "client_id" in resp.get_json()["error"]

    def test_the_label_is_checked_before_the_token_is_parsed(self, real, zfs):
        token = real.trust.token()
        resp = _enroll(real, "host-z", make_client_cert(), token, label="x" * 500,
                       token_secret="garbage")
        assert resp.status_code == 400 and "label" in resp.get_json()["error"]

    def test_missing_fields_are_reported_before_the_token_is_parsed(self, real, zfs, token):
        body = enroll_body(real.server_fp, "host-z", make_client_cert(), token)
        del body["cert_pem"]
        resp = _anonymous(real).post("/enroll", json=body)
        assert resp.status_code == 400 and "cert_pem" in resp.get_json()["error"]

    def test_the_certificate_is_checked_before_the_signature(self, real, zfs, token):
        ca = make_client_cert(ca=True)
        resp = _enroll(real, "host-z", ca, token)
        assert resp.status_code == 422

    def test_nothing_reaches_the_store_before_the_signature_is_proven(self, mocker, real, zfs, token):
        spy = mocker.spy(real.gateway, "enroll_client")
        _enroll(real, "host-z", make_client_cert(), token, signature=base64.b64encode(b"junk").decode())
        spy.assert_not_called()


@pytest.mark.integration
class TestEnrollRequestShape:
    def test_a_body_over_64_kib_is_413_with_the_limit_named(self, real, zfs):
        body = b'{"version":1,"pad":"' + b"x" * (MAX_TRUST_REQUEST_BYTES + 10) + b'"}'
        resp = _raw_post(_anonymous(real), "/enroll", body)
        assert resp.status_code == 413
        assert str(MAX_TRUST_REQUEST_BYTES) in resp.get_json()["error"]
        assert real.gate.active == 0

    def test_the_limit_is_65536_bytes_exactly(self, real, zfs):
        assert MAX_TRUST_REQUEST_BYTES == 65536
        head, tail = b'{"version":1,"pad":"', b'"}'
        at_limit = head + b"x" * (65536 - len(head) - len(tail)) + tail
        assert len(at_limit) == 65536
        assert _raw_post(_anonymous(real), "/enroll", at_limit).status_code == 400     # parsed, incomplete
        assert _raw_post(_anonymous(real), "/enroll", at_limit + b" ").status_code == 413

    def test_the_global_limit_stays_one_mebibyte_for_the_other_routes(self, real):
        assert real.app.config["MAX_CONTENT_LENGTH"] == MAX_REQUEST_BYTES == 1024 * 1024

    @pytest.mark.parametrize("raw", [b"", b"not json", b"[1, 2]", b"null", b'"text"', b"{", b"\xff\xfe"])
    def test_a_body_that_is_not_a_json_object_is_400(self, real, zfs, raw):
        resp = _raw_post(_anonymous(real), "/enroll", raw)
        assert resp.status_code == 400
        assert resp.get_json()["error"] == "request body must be a JSON object"

    @pytest.mark.parametrize("version", [None, 0, 2, "1", 1.0, True, [1], {"v": 1}])
    def test_the_version_must_be_the_integer_1(self, real, zfs, version):
        cert = make_client_cert()
        body = enroll_body(real.server_fp, "host-z", cert, real.trust.token())
        if version is None:
            del body["version"]
        else:
            body["version"] = version
        resp = _anonymous(real).post("/enroll", json=body)
        assert resp.status_code == 400
        assert "version" in resp.get_json()["error"]
        assert real.trust.token_state() is TokenState.ACTIVE

    def test_deeply_nested_json_is_a_400_not_a_crash(self, real):
        resp = _raw_post(_anonymous(real), "/enroll", b"[" * 30000)          # under the 64 KiB limit
        assert resp.status_code == 400

    @pytest.mark.parametrize("client_id", [
        None, 7, "", ".hidden", "..", "a/b", "a b", "a\nb", "x" * 65, "ü", "a@b", "a:b",
    ])
    def test_a_client_id_outside_the_grammar_is_400(self, real, zfs, client_id):
        body = enroll_body(real.server_fp, "host-z", make_client_cert(), real.trust.token())
        body["client_id"] = client_id
        resp = _anonymous(real).post("/enroll", json=body)
        assert resp.status_code == 400
        assert resp.get_json()["error"].startswith("client_id")
        zfs.exists.assert_not_called()
        assert real.trust.token_state() is TokenState.ACTIVE

    @pytest.mark.parametrize("label", [
        7, ["x"], "x" * 129, "bad\x00nul", "line\nbreak", "tab\there", "del\x7f", "zero​width",
        "bidi‮", "\ud800",
    ])
    def test_a_bad_label_is_400(self, real, zfs, label):
        try:
            payload = json.dumps(
                {**enroll_body(real.server_fp, "host-z", make_client_cert(), real.trust.token()),
                 "label": label}
            ).encode("ascii")
        except (UnicodeEncodeError, ValueError):  # pragma: no cover
            pytest.skip("cannot encode")
        resp = _raw_post(_anonymous(real), "/enroll", payload)
        assert resp.status_code == 400
        assert "label" in resp.get_json()["error"]
        assert real.trust.token_state() is TokenState.ACTIVE

    @pytest.mark.parametrize("field", ["token_secret", "cert_pem", "signature"])
    @pytest.mark.parametrize("value", [None, "", 5, ["x"]])
    def test_a_missing_or_non_string_field_is_400_and_named(self, real, zfs, field, value):
        body = enroll_body(real.server_fp, "host-z", make_client_cert(), real.trust.token())
        body[field] = value
        resp = _anonymous(real).post("/enroll", json=body)
        assert resp.status_code == 400
        assert field in resp.get_json()["error"]

    @pytest.mark.parametrize("signature", [
        "not base64!", "####", "AB", "QQ", "Zm9v YmFy", "Zm9v\nYmFy", "Zm9vYg",
    ])
    def test_a_signature_that_is_not_standard_padded_base64_is_400(self, real, zfs, signature):
        body = enroll_body(real.server_fp, "host-z", make_client_cert(), real.trust.token())
        body["signature"] = signature
        resp = _anonymous(real).post("/enroll", json=body)
        assert resp.status_code == 400
        assert "signature" in resp.get_json()["error"]
        zfs.exists.assert_not_called()

    def test_a_url_safe_base64_signature_is_not_accepted(self, real, zfs):
        cert = make_client_cert()
        body = enroll_body(real.server_fp, "host-z", cert, real.trust.token())
        raw = base64.b64decode(body["signature"])
        # find a signature whose standard encoding uses '+' or '/', else the test says nothing
        for _ in range(200):
            payload = identity.enrollment_payload(
                real.server_fp, "host-z", cert.fingerprint, identity.secret_hash(
                    identity.parse_token(body["token_secret"])[1])
            )
            raw = cert.sign(payload)
            std = base64.b64encode(raw).decode()
            if "+" in std or "/" in std:
                break
        else:                                                   # pragma: no cover
            pytest.skip("no signature with + or / in 200 tries")
        body["signature"] = base64.urlsafe_b64encode(raw).decode()
        assert _anonymous(real).post("/enroll", json=body).status_code == 400

    @pytest.mark.parametrize("token", [
        "garbage", "zfsb1.", "zfsb1.abc.def", "ZFSB1." + "ab" * 32 + "." + "A" * 43,
        "zfsb1." + "AB" * 32 + "." + "A" * 43, "zfsb1." + "ab" * 32 + "." + "A" * 42,
        "zfsb1." + "ab" * 32 + "." + "A" * 44, "zfsb1." + "ab" * 32 + ".!" + "A" * 42,
    ])
    def test_a_malformed_token_is_400_and_is_not_echoed(self, real, zfs, token, caplog):
        body = enroll_body(real.server_fp, "host-z", make_client_cert(), real.trust.token())
        body["token_secret"] = token
        with caplog.at_level(logging.DEBUG):
            resp = _anonymous(real).post("/enroll", json=body)
        assert resp.status_code == 400
        assert re.fullmatch(
            r"(malformed enrollment token|this is not a zfsbackup enrollment token) "
            r"\(expected zfsb1\.<64 lowercase hex>\.<43 base64url characters>\)",
            resp.get_json()["error"],
        )                                                       # the stock text, nothing of the input
        assert token not in caplog.text
        zfs.exists.assert_not_called()

    def test_a_token_with_surrounding_whitespace_is_accepted(self, real, zfs):
        cert = make_client_cert()
        token = real.trust.token()
        body = enroll_body(real.server_fp, "host-a", cert, token)
        body["token_secret"] = f"  {token}\n"
        assert _anonymous(real).post("/enroll", json=body).status_code == 200


@pytest.mark.integration
class TestEnrollBadCertificates:
    def _bad(self):
        now = dt.datetime.now(dt.timezone.utc)
        return {
            "expired": make_client_cert(not_before=now - dt.timedelta(days=9),
                                        not_after=now - dt.timedelta(days=1)),
            "not yet valid": make_client_cert(not_before=now + dt.timedelta(hours=2),
                                              not_after=now + dt.timedelta(days=9)),
            "a CA": make_client_cert(ca=True),
            "server auth only": make_client_cert(eku=(ExtendedKeyUsageOID.SERVER_AUTH,)),
        }

    @pytest.mark.parametrize("case", ["expired", "not yet valid", "a CA", "server auth only"])
    def test_an_unacceptable_certificate_is_422_with_a_reason_and_the_token_is_kept(
        self, real, zfs, case
    ):
        cert = self._bad()[case]
        token = real.trust.token()
        resp = _enroll(real, "host-z", cert, token)
        assert resp.status_code == 422
        body = resp.get_json()
        assert body["reason"] == "bad_certificate"
        assert body["error"].startswith("certificate not acceptable: ")
        assert real.trust.token_state() is TokenState.ACTIVE
        assert real.trust.pins() == {}
        zfs.exists.assert_not_called()

    def test_text_that_is_not_a_certificate_is_422(self, real, zfs):
        token = real.trust.token()
        body = enroll_body(real.server_fp, "host-z", make_client_cert(), token)
        for junk in ("garbage", "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"):
            body["cert_pem"] = junk
            resp = _anonymous(real).post("/enroll", json=body)
            assert resp.status_code == 422 and resp.get_json()["reason"] == "bad_certificate"

    def test_two_certificates_or_a_certificate_and_a_key_are_422(self, real, zfs):
        from cryptography.hazmat.primitives import serialization

        token = real.trust.token()
        a, b = make_client_cert(), make_client_cert()
        key_pem = a.key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode()
        body = enroll_body(real.server_fp, "host-z", a, token)
        for pem in (a.cert_pem + b.cert_pem, a.cert_pem + key_pem):
            body["cert_pem"] = pem
            resp = _anonymous(real).post("/enroll", json=body)
            assert resp.status_code == 422, pem[:40]
            assert "PRIVATE KEY" not in resp.get_data(as_text=True)

    def test_a_pem_over_16_kib_is_422(self, real, zfs):
        body = enroll_body(real.server_fp, "host-z", make_client_cert(), real.trust.token())
        body["cert_pem"] = make_client_cert().cert_pem + "\n" * (16 * 1024)
        resp = _anonymous(real).post("/enroll", json=body)
        assert resp.status_code == 422

    def test_a_certificate_that_is_acceptable_only_by_the_servers_clock_is_judged_by_it(self, mocker):
        """The server's `clock` decides validity (the same one the pins' expiry uses)."""
        authed = _authed(mocker, clock=lambda: time.time() + 60 * DAY)
        cert = make_client_cert(days=30)                    # expired, 60 days from now
        resp = authed.client(None).post(
            "/enroll", json=enroll_body(authed.server_fp, "host-z", cert,
                                        str(identity.make_token(authed.server_fp)[0]))
        )
        assert resp.status_code == 422


@pytest.mark.integration
class TestEnrollSignature:
    """The signature proves the caller holds the key and binds the token, the id, the certificate
    and THIS server together."""

    def _forged(self, real, signer, client_id="host-z", **payload_overrides):
        cert = make_client_cert()
        token = real.trust.token()
        _, secret = identity.parse_token(token)
        fields = dict(
            server_fp=real.server_fp, client_id=client_id, client_fp=cert.fingerprint,
            secret_sha256=identity.secret_hash(secret),
        )
        fields.update(payload_overrides)
        body = enroll_body(real.server_fp, "host-z", cert, token)
        body["signature"] = signer(cert).signature_b64(identity.enrollment_payload(**fields))
        return cert, token, body

    def _post(self, real, body):
        resp = _anonymous(real).post("/enroll", json=body)
        assert resp.status_code == 403, resp.get_json()
        assert resp.get_json()["reason"] == "bad_signature"
        assert real.trust.token_state() is TokenState.ACTIVE
        assert real.trust.pins() == {}

    def test_a_signature_by_a_key_that_is_not_the_certificates_is_403(self, real, zfs):
        thief = make_client_cert()
        _, _, body = self._forged(real, lambda cert: thief)
        self._post(real, body)

    def test_a_signature_over_another_client_id_is_403(self, real, zfs):
        _, _, body = self._forged(real, lambda cert: cert, client_id="host-other")
        self._post(real, body)

    def test_a_signature_for_another_server_is_403_a_relayed_enrollment(self, real, zfs):
        _, _, body = self._forged(real, lambda cert: cert, server_fp="cd" * 32)
        self._post(real, body)

    def test_a_signature_over_another_certificate_is_403(self, real, zfs):
        _, _, body = self._forged(real, lambda cert: cert, client_fp=make_client_cert().fingerprint)
        self._post(real, body)

    def test_a_signature_over_another_token_is_403(self, real, zfs):
        _, _, body = self._forged(real, lambda cert: cert, secret_sha256="12" * 32)
        self._post(real, body)

    def test_a_signature_of_a_rotation_payload_is_not_an_enrollment(self, real, zfs):
        cert = make_client_cert()
        token = real.trust.token()
        body = enroll_body(real.server_fp, "host-z", cert, token)
        body["signature"] = cert.signature_b64(identity.rotation_payload(
            real.server_fp, "host-z", "11" * 32, cert.fingerprint))
        self._post(real, body)

    def test_garbage_bytes_that_are_valid_base64_are_403(self, real, zfs):
        body = enroll_body(real.server_fp, "host-z", make_client_cert(), real.trust.token())
        body["signature"] = base64.b64encode(b"\x30\x06\x02\x01\x01\x02\x01\x01").decode()
        self._post(real, body)

    def test_a_good_signature_still_enrolls(self, real, zfs):
        cert, token, body = self._forged(real, lambda cert: cert, client_id="host-z")
        assert _anonymous(real).post("/enroll", json=body).status_code == 200


@pytest.mark.integration
class TestEnrollOutcomesAgainstTheStore:
    def test_a_squat_on_an_existing_target_dataset_is_409_and_the_token_is_kept(self, real, zfs):
        zfs.exists.return_value = True                       # pool/backups/host-a exists, no client row
        token = real.trust.token()
        resp = _enroll(real, "host-a", make_client_cert(), token)
        assert resp.status_code == 409
        body = resp.get_json()
        assert body["reason"] == "client_id_taken" and body["client_id"] == "host-a"
        assert "trust token create --client-id host-a" in body["hint"]
        assert "--replace" in body["hint"]
        assert real.trust.token_state() is TokenState.ACTIVE
        assert real.trust.pins() == {} and "host-a" not in real.trust.clients()
        # ...and the same token still enrolls a name nobody holds
        zfs.exists.return_value = False
        assert _enroll(real, "host-b", make_client_cert(), token).status_code == 200

    def test_an_existing_client_id_is_409_and_the_token_is_kept(self, real, zfs):
        assert _enroll(real, "host-a", make_client_cert(), real.trust.token()).status_code == 200
        token = real.trust.token()
        resp = _enroll(real, "host-a", make_client_cert(), token)
        assert resp.status_code == 409 and resp.get_json()["reason"] == "client_id_taken"
        assert real.trust.token_state() is TokenState.ACTIVE
        assert len(real.trust.pins()) == 1

    def test_the_409_does_not_reveal_whose_id_it_is_beyond_the_requested_one(self, real, zfs):
        _enroll(real, "host-a", make_client_cert(), real.trust.token(), label="Secret Label")
        resp = _enroll(real, "host-a", make_client_cert(), real.trust.token())
        assert "Secret Label" not in resp.get_data(as_text=True)

    def test_a_revoked_clients_id_stays_taken_for_an_unscoped_token(self, real, zfs):
        assert _enroll(real, "host-a", make_client_cert(), real.trust.token()).status_code == 200
        store_trust.revoke_client(real.trust.resolved, "host-a", lock=None)
        assert real.trust.gateway.refresh()
        resp = _enroll(real, "host-a", make_client_cert(), real.trust.token())
        assert resp.status_code == 409 and resp.get_json()["reason"] == "client_id_taken"

    def test_a_scoped_token_reclaims_a_revoked_id(self, real, zfs):
        old = make_client_cert()
        assert _enroll(real, "host-a", old, real.trust.token()).status_code == 200
        store_trust.revoke_client(real.trust.resolved, "host-a", lock=None)
        assert real.trust.gateway.refresh()
        new = make_client_cert()
        resp = _enroll(real, "host-a", new, real.trust.token(client_id="host-a"))
        assert resp.status_code == 200
        assert real.client(new).get("/whoami").get_json()["client_id"] == "host-a"
        assert real.client(old).get("/whoami").status_code == 403

    def test_a_scoped_token_adds_a_second_certificate_to_the_id(self, real, zfs):
        first, second = make_client_cert(), make_client_cert()
        _enroll(real, "host-a", first, real.trust.token())
        resp = _enroll(real, "host-a", second, real.trust.token(client_id="host-a"))
        assert resp.status_code == 200 and resp.get_json()["status"] == "enrolled"
        assert set(real.trust.pins()) == {first.fingerprint, second.fingerprint}
        for cert in (first, second):
            assert real.client(cert).get("/whoami").get_json()["client_id"] == "host-a"

    def test_replace_drops_the_other_certificates_of_the_id(self, real, zfs):
        first, second = make_client_cert(), make_client_cert()
        _enroll(real, "host-a", first, real.trust.token())
        resp = _enroll(real, "host-a", second, real.trust.token(client_id="host-a", replace=True))
        assert resp.status_code == 200
        assert set(real.trust.pins()) == {second.fingerprint}
        assert real.client(first).get("/whoami").status_code == 403       # gone from the next request on
        assert real.client(second).get("/whoami").status_code == 200

    def test_replace_for_an_already_pinned_certificate_is_enrolled_and_drops_the_other_pins(
        self, real, zfs
    ):
        """A `--replace` token scoped to the id, presented with a certificate that id already
        pins: 200 `enrolled` (not `already_trusted`), the token is spent, the OTHER pins go."""
        keep, other = make_client_cert(), make_client_cert()
        _enroll(real, "host-a", keep, real.trust.token())
        _enroll(real, "host-a", other, real.trust.token(client_id="host-a"))
        assert set(real.trust.pins()) == {keep.fingerprint, other.fingerprint}
        token = real.trust.token(client_id="host-a", replace=True)
        resp = _enroll(real, "host-a", keep, token)
        assert resp.status_code == 200
        assert resp.get_json() == {
            "status": "enrolled", "client_id": "host-a", "fingerprint": keep.fingerprint,
            "protocol": PROTOCOL_VERSION,
        }
        assert real.trust.token_state() is TokenState.CONSUMED
        assert set(real.trust.pins()) == {keep.fingerprint}
        assert real.client(keep).get("/whoami").status_code == 200
        assert real.client(other).get("/whoami").status_code == 403        # refresh applied it

    def test_a_scoped_token_without_replace_for_a_pinned_certificate_is_still_already_trusted(
        self, real, zfs
    ):
        keep = make_client_cert()
        _enroll(real, "host-a", keep, real.trust.token())
        resp = _enroll(real, "host-a", keep, real.trust.token(client_id="host-a"))
        assert resp.status_code == 200 and resp.get_json()["status"] == "already_trusted"
        assert real.trust.token_state() is TokenState.ACTIVE

    def test_a_scoped_token_works_though_the_target_dataset_exists(self, real, zfs):
        """An operator who scoped a token knows the id: the existing data is the point."""
        _enroll(real, "host-a", make_client_cert(), real.trust.token())
        zfs.exists.return_value = True
        resp = _enroll(real, "host-a", make_client_cert(), real.trust.token(client_id="host-a"))
        assert resp.status_code == 200

    def test_a_certificate_pinned_to_another_id_is_409_cert_bound_elsewhere(self, real, zfs):
        cert = make_client_cert()
        _enroll(real, "host-a", cert, real.trust.token())
        token = real.trust.token()
        resp = _enroll(real, "host-b", cert, token)
        assert resp.status_code == 409
        assert resp.get_json()["reason"] == "cert_bound_elsewhere"
        assert "host-a" not in resp.get_data(as_text=True)
        assert real.trust.token_state() is TokenState.ACTIVE
        assert real.trust.pins()[cert.fingerprint].client_id == "host-a"

    def test_already_trusted_is_200_and_does_not_spend_the_token(self, real, zfs):
        cert = make_client_cert()
        _enroll(real, "host-a", cert, real.trust.token())
        before = real.trust.trust_key()
        spare = real.trust.token()
        resp = _enroll(real, "host-a", cert, spare)
        assert resp.status_code == 200
        assert resp.get_json() == {
            "status": "already_trusted", "client_id": "host-a", "fingerprint": cert.fingerprint,
            "protocol": PROTOCOL_VERSION,
        }
        assert real.trust.token_state() is TokenState.ACTIVE
        assert real.trust.trust_key() == before            # nothing was written
        assert len(real.trust.pins()) == 1

    def test_already_trusted_needs_a_live_token_though(self, real, zfs):
        """Idempotency for a client that is already enrolled comes from /whoami, not /enroll."""
        cert = make_client_cert()
        spent = real.trust.token()
        assert _enroll(real, "host-a", cert, spent).status_code == 200
        resp = _enroll(real, "host-a", cert, spent)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR
        expired = real.trust.token(ttl=10.0, created_at=time.time() - 100)
        assert _enroll(real, "host-a", cert, expired).status_code == 403
        wrong = str(identity.make_token(real.trust.server_fp)[0])
        real.trust.token()
        assert _enroll(real, "host-a", cert, wrong).status_code == 403

    def test_already_trusted_does_not_refresh_the_gateway(self, mocker, real, zfs):
        cert = make_client_cert()
        _enroll(real, "host-a", cert, real.trust.token())
        refresh = mocker.spy(real.gateway, "refresh")
        _enroll(real, "host-a", cert, real.trust.token())
        refresh.assert_not_called()

    def test_two_concurrent_enrollments_with_one_token_have_exactly_one_winner(self, real, zfs):
        token = real.trust.token()
        barrier = threading.Barrier(6)
        statuses = []

        def go(i):
            barrier.wait()
            statuses.append(_enroll(real, f"host-{i}", make_client_cert(), token).status_code)

        threads = [threading.Thread(target=go, args=(i,)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        assert sorted(statuses) == [200, 403, 403, 403, 403, 403]
        assert len(real.trust.pins()) == 1


@pytest.mark.integration
class TestEnrollRefusesRevokedCertificates:
    """A certificate this server revoked (a revoked pin or client, a pin dropped by `--replace`,
    a retired rotation predecessor) is never pinned again, whatever the token: the token
    pre-check sees the certificate's fingerprint and refuses BEFORE `zfs.exists` (one SQLite
    read, no subprocess), and the token stays active."""

    def _revoked(self, real, how):
        """-> a certificate the store now holds as revoked, after `how`."""
        cert = make_client_cert()
        trust = real.trust
        if how == "pin":
            trust.enroll(cert, "host-a")
            store_trust.revoke_pin(trust.resolved, cert.fingerprint, lock=None)
        elif how == "client":
            trust.enroll(cert, "host-a")
            store_trust.revoke_client(trust.resolved, "host-a", lock=None)
        elif how == "replaced":
            trust.enroll(cert, "host-a")
            trust.enroll(make_client_cert(), "host-a", scope="host-a", replace=True)
        elif how == "retired":
            trust.enroll(cert, "host-a")
            successor = make_client_cert()
            assert _rotate(real, cert, successor, client_id="host-a").status_code == 200
            assert real.client(successor).get("/whoami").status_code == 200   # retires `cert`
        else:  # pragma: no cover
            raise AssertionError(how)
        assert cert.fingerprint not in trust.pins()
        assert {r.fingerprint for r in store_trust.list_revoked(trust.resolved)} >= {cert.fingerprint}
        assert trust.gateway.refresh()
        return cert

    @pytest.mark.parametrize("how", ["pin", "client", "replaced", "retired"])
    @pytest.mark.parametrize("scoped", [False, True])
    def test_it_is_the_one_403_and_zfs_is_never_asked(self, real, zfs, how, scoped):
        cert = self._revoked(real, how)
        zfs.exists.reset_mock()
        token = real.trust.token(client_id="host-a" if scoped else None)
        resp = _enroll(real, "host-a" if scoped else "host-new", cert, token)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR
        zfs.exists.assert_not_called()
        assert real.trust.token_state() is TokenState.ACTIVE            # not spent
        assert cert.fingerprint not in real.trust.pins()

    def test_the_same_token_still_enrolls_a_fresh_certificate(self, real, zfs):
        cert = self._revoked(real, "pin")
        token = real.trust.token(client_id="host-a")
        assert _enroll(real, "host-a", cert, token).status_code == 403
        fresh = make_client_cert()
        assert _enroll(real, "host-a", fresh, token).status_code == 200
        assert real.client(fresh).get("/whoami").get_json()["client_id"] == "host-a"

    def test_a_revoked_certificate_is_told_nothing_about_why(self, real, zfs):
        cert = self._revoked(real, "client")
        resp = _enroll(real, "host-a", cert, real.trust.token(client_id="host-a"))
        text = resp.get_data(as_text=True)
        assert "revoked" not in text and "host-a" not in text


class TestEnrollOutcomeMapping:
    """Every `enroll_client` result and failure, with a stub gateway (the store's behaviour is
    above)."""

    @pytest.fixture
    def authed(self, mocker, zfs):
        return _authed(mocker, client_id=None)

    def _enroll(self, authed, **overrides):
        token = str(identity.make_token(authed.server_fp)[0])
        cert = make_client_cert()
        resp = _enroll(authed, "host-z", cert, token, label="lbl", **overrides)
        return resp, cert, token

    def test_enrolled_refreshes_the_gateway_once_and_is_200(self, authed):
        resp, cert, _ = self._enroll(authed)
        assert resp.status_code == 200
        assert resp.get_json() == {
            "status": "enrolled", "client_id": "host-z", "fingerprint": cert.fingerprint,
            "protocol": PROTOCOL_VERSION,
        }
        assert authed.gateway.refresh_calls == 1

    def test_the_store_gets_the_verified_values(self, authed, zfs):
        zfs.exists.return_value = True
        resp, cert, token = self._enroll(authed)
        (call,) = authed.gateway.enroll_calls
        secret = identity.parse_token(token)[1]
        assert call["secret_sha256"] == identity.secret_hash(secret)
        assert call["server_fingerprint"] == authed.server_fp
        assert call["client_id"] == "host-z" and call["label"] == "lbl"
        assert call["dataset_exists"] is True
        pin = call["pin"]
        assert (pin.fingerprint, pin.cert_pem) == (cert.fingerprint, cert.cert_pem)
        assert pin.not_after == cert.not_after.timestamp()
        assert call["now"] is time.time                      # the default clock, evaluated by the store
        assert token not in repr({k: v for k, v in call.items() if k != "secret_sha256"})

    def test_now_is_the_apps_clock(self, mocker, zfs):
        def clock():
            return time.time()

        authed = _authed(mocker, client_id=None, clock=clock)
        _enroll(authed, "host-z", make_client_cert(), str(identity.make_token(authed.server_fp)[0]))
        assert authed.gateway.enroll_calls[0]["now"] is clock

    def test_already_trusted_is_200_without_a_refresh(self, authed):
        authed.gateway.enroll_effect = EnrollOutcome.ALREADY_TRUSTED
        resp, cert, _ = self._enroll(authed)
        assert resp.status_code == 200 and resp.get_json()["status"] == "already_trusted"
        assert authed.gateway.refresh_calls == 0

    def test_refused_is_the_one_403(self, authed):
        authed.gateway.enroll_effect = EnrollOutcome.REFUSED
        resp, _, _ = self._enroll(authed)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR

    def test_client_id_taken_is_409(self, authed):
        authed.gateway.enroll_effect = EnrollOutcome.CLIENT_ID_TAKEN
        resp, _, _ = self._enroll(authed)
        assert resp.status_code == 409
        assert resp.get_json()["reason"] == "client_id_taken"
        assert authed.gateway.refresh_calls == 0

    def test_cert_bound_elsewhere_is_409(self, authed):
        authed.gateway.enroll_effect = EnrollOutcome.CERT_BOUND_ELSEWHERE
        resp, _, _ = self._enroll(authed)
        assert resp.status_code == 409 and resp.get_json()["reason"] == "cert_bound_elsewhere"

    def test_an_unknown_outcome_is_the_one_403(self, authed):
        authed.gateway.enroll_effect = "something new"
        resp, _, _ = self._enroll(authed)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR

    def test_a_busy_database_is_503_with_retry_and_logs_no_error(self, authed, caplog):
        authed.gateway.enroll_effect = ConfigBusy("database is locked")
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp, _, _ = self._enroll(authed)
        assert resp.status_code == 503
        assert resp.get_json() == TRUST_BUSY_ERROR
        assert resp.get_json()["retry"] is True and resp.get_json()["reason"] == "busy"
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert authed.gate.active == 0

    @pytest.mark.parametrize("error", [
        RuntimeError("no such table: trusted_clients"), OSError("disk I/O error"), ValueError("x"),
    ])
    def test_any_other_store_failure_is_503_without_retry_and_leaks_nothing(self, authed, caplog, error):
        authed.gateway.enroll_effect = error
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp, _, token = self._enroll(authed)
        assert resp.status_code == 503
        assert resp.get_json() == TRUST_UNAVAILABLE_ERROR
        assert "retry" not in resp.get_json() and resp.get_json()["reason"] == "unavailable"
        assert str(error) not in resp.get_data(as_text=True)
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1 and str(error) in errors[0].getMessage()
        assert token not in caplog.text

    def test_zfs_failing_is_503_with_retry_and_an_error_log(self, authed, zfs, caplog):
        zfs.exists.side_effect = RuntimeError("cannot open 'pool': pool I/O is currently suspended")
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp, _, _ = self._enroll(authed)
        assert resp.status_code == 503
        body = resp.get_json()
        assert body["reason"] == "zfs" and body["retry"] is True
        assert "suspended" not in resp.get_data(as_text=True)
        assert authed.gateway.enroll_calls == []
        assert any(r.levelno == logging.ERROR and "suspended" in r.getMessage() for r in caplog.records)

    def test_a_failed_refresh_after_enrolling_is_still_200_with_one_warning(self, authed, caplog):
        """Inline attempt, then the after-send retry; only the retry's failure warns."""
        authed.gateway.refresh_result = False
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp, _, _ = self._enroll(authed)
        assert resp.status_code == 200 and resp.get_json()["status"] == "enrolled"
        assert authed.gateway.refresh_calls == 2
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1 and "could not be refreshed" in warnings[0]
        assert "enrollment of client host-z" in warnings[0]

    def test_a_refresh_is_tried_before_the_response_and_only_a_failure_waits_for_its_close(
        self, authed, caplog
    ):
        authed.gateway.refresh_result = False
        client = plain_client(authed.app)                 # unbuffered: closed by hand
        cert = make_client_cert()
        token = str(identity.make_token(authed.server_fp)[0])
        body = enroll_body(authed.server_fp, "host-z", cert, token)
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp = client.post("/enroll", json=body)
            assert resp.status_code == 200
            assert authed.gateway.refresh_calls == 1      # inline, before the response
            assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
            resp.close()
            assert authed.gateway.refresh_calls == 2      # the after-send retry
        assert authed.gateway.refresh_timeouts == [api_module.REQUEST_REFRESH_WAIT] * 2
        assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1

    def test_an_inline_refresh_that_succeeds_is_not_repeated_or_warned_about(self, authed, caplog):
        client = plain_client(authed.app)
        token = str(identity.make_token(authed.server_fp)[0])
        body = enroll_body(authed.server_fp, "host-z", make_client_cert(), token)
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp = client.post("/enroll", json=body)
            resp.close()
        assert authed.gateway.refresh_calls == 1
        assert not [r for r in caplog.records if "could not be refreshed" in r.getMessage()]

    def test_a_refresh_that_fails_inline_but_works_after_the_send_does_not_warn(self, authed, caplog):
        results = iter([False, True])
        authed.gateway.refresh = lambda **kw: next(results)
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp, _, _ = self._enroll(authed)
        assert resp.status_code == 200
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    def test_the_precheck_gets_the_hash_the_server_and_the_clock_and_no_secret(self, mocker, zfs):
        def clock():
            return time.time()

        authed = _authed(mocker, client_id=None, clock=clock)
        token = str(identity.make_token(authed.server_fp)[0])
        cert = make_client_cert()
        _enroll(authed, "host-z", cert, token)
        (call,) = authed.gateway.precheck_calls
        assert call == {
            "secret_sha256": identity.secret_hash(identity.parse_token(token)[1]),
            "server_fingerprint": authed.server_fp,
            "now": clock,
            "fingerprint": cert.fingerprint,
        }
        assert token.split(".")[-1] not in repr(call)

    def test_the_precheck_is_told_which_certificate_is_presented(self, authed, zfs):
        """A precheck that refuses only a revoked fingerprint refuses exactly that certificate."""
        revoked = make_client_cert()
        authed.gateway.precheck_effect = lambda **k: k["fingerprint"] != revoked.fingerprint
        token = str(identity.make_token(authed.server_fp)[0])
        resp = _enroll(authed, "host-z", revoked, token)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR
        zfs.exists.assert_not_called()
        assert authed.gateway.enroll_calls == [] and authed.gateway.refresh_calls == 0
        assert [c["fingerprint"] for c in authed.gateway.precheck_calls] == [revoked.fingerprint]
        resp = _enroll(authed, "host-z", make_client_cert(), token)
        assert resp.status_code == 200 and len(authed.gateway.enroll_calls) == 1

    def test_a_negative_precheck_is_the_one_403_and_nothing_else_runs(self, authed, zfs):
        authed.gateway.precheck_effect = False
        resp, _, _ = self._enroll(authed)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR
        zfs.exists.assert_not_called()
        assert authed.gateway.enroll_calls == [] and authed.gateway.refresh_calls == 0

    def test_a_negative_precheck_is_logged_as_a_token_or_revoked_certificate_refusal(
        self, authed, zfs, caplog
    ):
        authed.gateway.precheck_effect = False
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp, _, token = self._enroll(authed)
        assert resp.get_json() == ENROLL_REFUSED_ERROR           # the client learns nothing more
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1 and warnings[0].endswith("refused: token or revoked certificate")
        assert "revoked" not in resp.get_data(as_text=True)
        assert token not in caplog.text and token.split(".")[-1] not in caplog.text

    def test_a_busy_precheck_is_503_with_retry_and_runs_no_zfs(self, authed, zfs, caplog):
        authed.gateway.precheck_effect = ConfigBusy("database is locked")
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp, _, _ = self._enroll(authed)
        assert resp.status_code == 503 and resp.get_json() == TRUST_BUSY_ERROR
        zfs.exists.assert_not_called()
        assert authed.gateway.enroll_calls == []
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert authed.gate.active == 0

    @pytest.mark.parametrize("error", [RuntimeError("no such table: enrollment_tokens"), OSError("disk I/O error")])
    def test_any_other_precheck_failure_is_503_unavailable_and_leaks_nothing(
        self, authed, zfs, caplog, error
    ):
        authed.gateway.precheck_effect = error
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp, _, token = self._enroll(authed)
        assert resp.status_code == 503 and resp.get_json() == TRUST_UNAVAILABLE_ERROR
        assert str(error) not in resp.get_data(as_text=True)
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1 and str(error) in errors[0].getMessage()
        assert token not in caplog.text and token.split(".")[-1] not in caplog.text
        zfs.exists.assert_not_called()
        assert authed.gateway.enroll_calls == []

    def test_the_precheck_runs_before_zfs_and_the_store(self, authed, zfs):
        order = []
        authed.gateway.precheck_effect = lambda **k: order.append("precheck") or True
        zfs.exists.side_effect = lambda *a: order.append("zfs") or False
        authed.gateway.enroll_effect = lambda **k: order.append("store") or EnrollOutcome.ENROLLED
        self._enroll(authed)
        assert order == ["precheck", "zfs", "store"]

    def test_the_exists_check_names_the_client_root_only(self, authed, zfs):
        self._enroll(authed)
        assert [str(c[0][0]) for c in zfs.exists.call_args_list] == [f"{TARGET}/host-z"]


class TestEnrollGating:
    def test_dry_run_is_503_without_retry_and_nothing_is_read(self, mocker, zfs):
        authed = _authed(mocker, _config(dry_run=True), client_id=None)
        for body in (None, {"version": 1}):             # not even the body is looked at
            resp = authed.client(None).post("/enroll", json=body)
            assert resp.status_code == 503
            assert resp.get_json()["reason"] == "dry_run" and "retry" not in resp.get_json()
        assert authed.gateway.enroll_calls == []
        _untouched(zfs)

    def test_a_disabled_server_is_503_without_retry(self, mocker, zfs):
        authed = _authed(mocker, _config(remote=False), client_id=None)
        resp = authed.client(None).post("/enroll", json={})
        assert resp.status_code == 503 and resp.get_json() == {"error": "remote_backup disabled"}
        _untouched(zfs)

    def test_a_draining_server_is_503_with_retry(self, mocker, zfs):
        authed = _authed(mocker, client_id=None)
        authed.gate.start_draining()
        resp = authed.client(None).post("/enroll", json={})
        assert resp.status_code == 503 and resp.get_json() == RELOADING_ERROR

    def test_the_dry_run_check_comes_after_the_disabled_and_draining_checks(self, mocker):
        authed = _authed(mocker, _config(dry_run=True, remote=False), client_id=None)
        assert authed.client(None).post("/enroll", json={}).get_json() == {
            "error": "remote_backup disabled"}


@pytest.mark.integration
class TestTokenIsNeverLogged:
    def _everything(self, real, zfs):
        """Drive /enroll down every path; -> the tokens used."""
        tokens = []

        def token():
            t = real.trust.token()
            tokens.append(t)
            return t

        good = make_client_cert()
        _enroll(real, "host-a", good, token())                                      # enrolled
        _enroll(real, "host-a", good, token())                                      # already_trusted
        _enroll(real, "host-a", make_client_cert(), token())                        # client_id_taken
        _enroll(real, "host-b", good, token())                                      # cert_bound_elsewhere
        t = token()
        _enroll(real, "host-c", make_client_cert(), t, signature="QUJD")             # bad signature
        _enroll(real, "host-c", make_client_cert(ca=True), t)                       # bad certificate
        _enroll(real, "host-c", make_client_cert(), t, client_id=".bad")             # bad id
        _enroll(real, "host-c", make_client_cert(), t, label="x" * 200)             # bad label
        wrong = str(identity.make_token(real.trust.server_fp)[0])
        tokens.append(wrong)
        _enroll(real, "host-d", make_client_cert(), wrong)                           # refused
        tokens.append(str(identity.make_token("cd" * 32)[0]))
        _enroll(real, "host-e", make_client_cert(), tokens[-1])                      # other server
        zfs.exists.side_effect = RuntimeError("zfs down")
        _enroll(real, "host-f", make_client_cert(), t)                               # zfs failure
        zfs.exists.side_effect = None
        _enroll(real, "host-g", make_client_cert(), t)                               # enrolled (spent)
        body = enroll_body(real.server_fp, "host-h", make_client_cert(), t)
        body["token_secret"] = "zfsb1." + "ab" * 32 + ".secret-looking-text"
        tokens.append(body["token_secret"])
        _anonymous(real).post("/enroll", json=body)                                  # malformed token
        return tokens

    def test_no_log_record_contains_a_token_or_its_secret_or_its_hash(self, real, zfs, caplog):
        with caplog.at_level(logging.DEBUG):
            tokens = self._everything(real, zfs)
        assert len(tokens) >= 8
        records = "\n".join(
            f"{r.getMessage()}\n{r.exc_text or ''}\n{r.args!r}" for r in caplog.records
        )
        for token in tokens:
            secret = token.split(".")[-1]
            assert token not in records and secret not in records, token
            if token.startswith("zfsb1.") and len(secret) == 43:
                digest = identity.secret_hash(identity.parse_token(token)[1])
                assert digest not in records
        assert "enrolled client host-a" in records                   # and the log is not simply empty

    def test_no_response_contains_the_token(self, real, zfs):
        token = real.trust.token()
        secret = token.split(".")[-1]
        wrong = dict(enroll_body(real.server_fp, "host-a", make_client_cert(), token),
                     signature="QUJD")
        for body in (wrong, {"version": 1, "token_secret": token}, {"token_secret": token}):
            resp = _anonymous(real).post("/enroll", json=body)
            assert secret not in resp.get_data(as_text=True)

    def test_a_refused_enrollment_is_one_warning_with_the_category_only(self, real, zfs, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            _enroll(real, "host-z", make_client_cert(), str(identity.make_token(real.trust.server_fp)[0]))
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert warnings[0].startswith("enrollment from ") and "refused" in warnings[0]


# ---------------------------------------------------------------------------
# /rotate
# ---------------------------------------------------------------------------


@pytest.fixture
def enrolled(real, zfs):
    """`real` with certificate `old` enrolled under `host-a`."""
    real.old = make_client_cert()
    real.trust.enroll(real.old, "host-a")
    return real


@pytest.mark.integration
class TestRotate:
    def test_it_pins_the_successor_next_to_the_current_certificate(self, enrolled):
        successor = make_client_cert()
        resp = _rotate(enrolled, enrolled.old, successor, client_id="host-a")
        assert resp.status_code == 200
        assert resp.get_json() == {
            "status": "rotated", "client_id": "host-a", "fingerprint": successor.fingerprint,
            "protocol": PROTOCOL_VERSION,
        }
        pins = enrolled.trust.pins()
        assert set(pins) == {enrolled.old.fingerprint, successor.fingerprint}
        assert pins[successor.fingerprint].predecessor == enrolled.old.fingerprint
        assert pins[successor.fingerprint].client_id == "host-a"

    def test_both_certificates_work_until_the_successors_first_use(self, enrolled):
        successor = make_client_cert()
        _rotate(enrolled, enrolled.old, successor, client_id="host-a")
        for cert in (enrolled.old, successor):
            # (a `/health` request is not an authenticated use; whoami by the OLD one is fine)
            if cert is successor:
                break
            assert enrolled.client(cert).get("/whoami").status_code == 200
        pin = enrolled.gateway.lookup(successor.fingerprint)
        assert pin == PinInfo("host-a", successor.not_after.timestamp(), enrolled.old.fingerprint)

    def test_the_gateway_is_refreshed_so_the_successor_works_at_once(self, mocker, enrolled):
        successor = make_client_cert()
        assert enrolled.client(successor).get("/whoami").status_code == 403
        refresh = mocker.spy(enrolled.gateway, "refresh")
        _rotate(enrolled, enrolled.old, successor, client_id="host-a")
        assert refresh.call_count == 1
        assert enrolled.gateway.lookup(successor.fingerprint) is not None

    def test_a_second_identical_rotation_is_already(self, enrolled):
        successor = make_client_cert()
        _rotate(enrolled, enrolled.old, successor, client_id="host-a")
        before = enrolled.trust.trust_key()
        resp = _rotate(enrolled, enrolled.old, successor, client_id="host-a")
        assert resp.status_code == 200
        assert resp.get_json() == {
            "status": "already", "client_id": "host-a", "fingerprint": successor.fingerprint,
            "protocol": PROTOCOL_VERSION,
        }
        assert enrolled.trust.trust_key() == before

    def test_only_one_successor_per_predecessor(self, enrolled):
        _rotate(enrolled, enrolled.old, make_client_cert(), client_id="host-a")
        resp = _rotate(enrolled, enrolled.old, make_client_cert(), client_id="host-a")
        assert resp.status_code == 403
        assert resp.get_json() == ROTATE_REFUSED_ERROR == {
            "error": "rotation refused", "reason": "refused",
        }
        assert len(enrolled.trust.pins()) == 2

    def test_a_successor_pinned_to_another_client_is_refused(self, enrolled):
        other = make_client_cert()
        enrolled.trust.enroll(other, "host-b")
        resp = _rotate(enrolled, enrolled.old, other, client_id="host-a")
        assert resp.status_code == 403 and resp.get_json() == ROTATE_REFUSED_ERROR
        assert enrolled.trust.pins()[other.fingerprint].client_id == "host-b"

    def test_rotating_to_the_current_certificate_is_refused(self, enrolled):
        resp = _rotate(enrolled, enrolled.old, enrolled.old, client_id="host-a")
        assert resp.status_code == 403 and resp.get_json() == ROTATE_REFUSED_ERROR

    def test_a_successor_whose_pin_was_revoked_is_refused(self, enrolled):
        gone = make_client_cert()
        enrolled.trust.enroll(gone, "host-b")
        store_trust.revoke_pin(enrolled.trust.resolved, gone.fingerprint, lock=None)
        assert enrolled.gateway.refresh()
        resp = _rotate(enrolled, enrolled.old, gone, client_id="host-a")
        assert resp.status_code == 403 and resp.get_json() == ROTATE_REFUSED_ERROR
        assert set(enrolled.trust.pins()) == {enrolled.old.fingerprint}

    def test_a_retired_predecessor_cannot_come_back_as_a_successor(self, enrolled):
        old, a2 = enrolled.old, make_client_cert()
        _rotate(enrolled, old, a2, client_id="host-a")
        assert enrolled.client(a2).get("/whoami").status_code == 200        # retires `old`
        resp = _rotate(enrolled, a2, old, client_id="host-a")
        assert resp.status_code == 403 and resp.get_json() == ROTATE_REFUSED_ERROR
        assert set(enrolled.trust.pins()) == {a2.fingerprint}
        assert enrolled.client(old).get("/whoami").status_code == 403

    def test_a_pin_dropped_by_replace_cannot_come_back_as_a_successor(self, enrolled):
        newer = make_client_cert()
        enrolled.trust.enroll(newer, "host-a", scope="host-a", replace=True)   # drops `old`
        resp = _rotate(enrolled, newer, enrolled.old, client_id="host-a")
        assert resp.status_code == 403 and resp.get_json() == ROTATE_REFUSED_ERROR
        assert set(enrolled.trust.pins()) == {newer.fingerprint}

    def test_the_successor_keeps_the_clients_id_whatever_the_body_says(self, enrolled):
        successor = make_client_cert()
        body = rotate_body(enrolled.server_fp, "host-a", enrolled.old.fingerprint, successor)
        body["client_id"] = "host-b"
        resp = enrolled.client(enrolled.old).post("/rotate", json=body)
        assert resp.status_code == 200 and resp.get_json()["client_id"] == "host-a"
        assert enrolled.trust.pins()[successor.fingerprint].client_id == "host-a"

    def test_it_needs_the_current_certificate(self, enrolled):
        successor = make_client_cert()
        body = rotate_body(enrolled.server_fp, "host-a", enrolled.old.fingerprint, successor)
        assert enrolled.client(None).post("/rotate", json=body).status_code == 401
        assert enrolled.client(make_client_cert()).post("/rotate", json=body).status_code == 403
        assert len(enrolled.trust.pins()) == 1

    def test_it_is_refused_with_401_before_the_dry_run_check(self, mocker, tls_config_db):
        authed = _authed(mocker, _config(dry_run=True))
        assert authed.client(None).post("/rotate", json={}).status_code == 401

    def test_a_pending_rotation_does_not_let_the_successor_rotate_again_until_retired(self, enrolled):
        a2, a3 = make_client_cert(), make_client_cert()
        _rotate(enrolled, enrolled.old, a2, client_id="host-a")
        # a2's first request retires the old pin; only then can a2 rotate on
        assert enrolled.client(a2).get("/whoami").status_code == 200
        assert enrolled.client(enrolled.old).get("/whoami").status_code == 403
        resp = _rotate(enrolled, a2, a3, client_id="host-a")
        assert resp.status_code == 200 and resp.get_json()["status"] == "rotated"


@pytest.mark.integration
class TestRotateCheckOrder:
    """body 400 -> fields 400 -> certificate 422 -> signature 403 -> store."""

    def test_a_bad_successor_with_a_bad_signature_is_422(self, enrolled):
        expired = make_client_cert(
            not_before=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=9),
            not_after=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1),
        )
        resp = _rotate(enrolled, enrolled.old, expired, client_id="host-a", signature="QUJD")
        assert resp.status_code == 422 and resp.get_json()["reason"] == "bad_certificate"

    @pytest.mark.parametrize("case", ["a CA", "server auth only", "garbage"])
    def test_unacceptable_successors_are_422(self, enrolled, case):
        successor = {
            "a CA": make_client_cert(ca=True),
            "server auth only": make_client_cert(eku=(ExtendedKeyUsageOID.SERVER_AUTH,)),
            "garbage": make_client_cert(),
        }[case]
        overrides = {"successor_cert_pem": "garbage"} if case == "garbage" else {}
        resp = _rotate(enrolled, enrolled.old, successor, client_id="host-a", **overrides)
        assert resp.status_code == 422 and resp.get_json()["reason"] == "bad_certificate"
        assert len(enrolled.trust.pins()) == 1

    def test_the_successor_key_must_sign_not_the_current_one(self, enrolled):
        successor = make_client_cert()
        payload = identity.rotation_payload(
            enrolled.server_fp, "host-a", enrolled.old.fingerprint, successor.fingerprint)
        resp = _rotate(enrolled, enrolled.old, successor, client_id="host-a",
                       signature=enrolled.old.signature_b64(payload))
        assert resp.status_code == 403
        assert resp.get_json()["reason"] == "bad_signature"
        assert len(enrolled.trust.pins()) == 1

    @pytest.mark.parametrize("what", [
        "another server", "another client id", "another old fingerprint", "another new fingerprint",
    ])
    def test_a_signature_over_other_values_is_403(self, enrolled, what):
        successor = make_client_cert()
        args = dict(server_fp=enrolled.server_fp, client_id="host-a",
                    old_fp=enrolled.old.fingerprint, new_fp=successor.fingerprint)
        args.update({
            "another server": {"server_fp": "cd" * 32},
            "another client id": {"client_id": "host-b"},
            "another old fingerprint": {"old_fp": make_client_cert().fingerprint},
            "another new fingerprint": {"new_fp": make_client_cert().fingerprint},
        }[what])
        signature = successor.signature_b64(identity.rotation_payload(**args))
        resp = _rotate(enrolled, enrolled.old, successor, client_id="host-a", signature=signature)
        assert resp.status_code == 403 and resp.get_json()["reason"] == "bad_signature"
        assert len(enrolled.trust.pins()) == 1

    def test_an_enrollment_signature_is_not_a_rotation_signature(self, enrolled):
        successor = make_client_cert()
        payload = identity.enrollment_payload(
            enrolled.server_fp, "host-a", successor.fingerprint, "12" * 32)
        resp = _rotate(enrolled, enrolled.old, successor, client_id="host-a",
                       signature=successor.signature_b64(payload))
        assert resp.status_code == 403

    @pytest.mark.parametrize("signature", [None, "", 5, "not base64!", "AB"])
    def test_a_missing_or_malformed_signature_is_400(self, enrolled, signature):
        resp = _rotate(enrolled, enrolled.old, make_client_cert(), client_id="host-a",
                       signature=signature)
        assert resp.status_code == 400 and "signature" in resp.get_json()["error"]

    @pytest.mark.parametrize("value", [None, "", 5])
    def test_a_missing_successor_is_400_and_named(self, enrolled, value):
        resp = _rotate(enrolled, enrolled.old, make_client_cert(), client_id="host-a",
                       successor_cert_pem=value)
        assert resp.status_code == 400 and "successor_cert_pem" in resp.get_json()["error"]

    @pytest.mark.parametrize("version", [None, 0, 2, "1", True])
    def test_the_version_must_be_1(self, enrolled, version):
        resp = _rotate(enrolled, enrolled.old, make_client_cert(), client_id="host-a",
                       version=version)
        assert resp.status_code == 400 and "version" in resp.get_json()["error"]

    def test_a_body_that_is_not_a_json_object_is_400(self, enrolled):
        for raw in (b"", b"[]", b"nope"):
            resp = _raw_post(enrolled.client(enrolled.old), "/rotate", raw)
            assert resp.status_code == 400

    def test_a_body_over_64_kib_is_413(self, enrolled):
        body = b'{"version":1,"pad":"' + b"x" * (MAX_TRUST_REQUEST_BYTES + 10) + b'"}'
        resp = _raw_post(enrolled.client(enrolled.old), "/rotate", body)
        assert resp.status_code == 413 and str(MAX_TRUST_REQUEST_BYTES) in resp.get_json()["error"]


class TestRotateOutcomeMapping:
    @pytest.fixture
    def authed(self, mocker, zfs):
        return _authed(mocker)

    def _rotate(self, authed, **kwargs):
        successor = make_client_cert()
        return _rotate(authed, authed.who, successor, client_id=CLIENT_ID, **kwargs), successor

    def test_the_store_gets_the_verified_values(self, authed):
        resp, successor = self._rotate(authed)
        assert resp.status_code == 200
        (call,) = authed.gateway.rotate_calls
        assert call["old_fp"] == authed.who.fingerprint
        assert call["new_pin"].fingerprint == successor.fingerprint
        assert call["new_pin"].cert_pem == successor.cert_pem
        assert call["new_pin"].not_after == successor.not_after.timestamp()
        assert callable(call["now"])

    def test_rotated_and_already_are_200_with_a_refresh(self, authed):
        for outcome in (RotateOutcome.ROTATED, RotateOutcome.ALREADY):
            authed.gateway.rotate_effect = outcome
            before = authed.gateway.refresh_calls
            resp, successor = self._rotate(authed)
            assert resp.status_code == 200
            assert resp.get_json() == {
                "status": str(outcome), "client_id": CLIENT_ID, "fingerprint": successor.fingerprint,
                "protocol": PROTOCOL_VERSION,
            }
            assert authed.gateway.refresh_calls == before + 1

    def test_refused_is_403(self, authed):
        authed.gateway.rotate_effect = RotateOutcome.REFUSED
        resp, _ = self._rotate(authed)
        assert resp.status_code == 403 and resp.get_json() == ROTATE_REFUSED_ERROR
        assert authed.gateway.refresh_calls == 0

    def test_busy_is_503_with_retry(self, authed):
        authed.gateway.rotate_effect = ConfigBusy("database is locked")
        resp, _ = self._rotate(authed)
        assert resp.status_code == 503 and resp.get_json() == TRUST_BUSY_ERROR

    def test_any_other_failure_is_503_without_retry_and_logged_once_as_an_error(self, authed, caplog):
        authed.gateway.rotate_effect = RuntimeError("schema is old")
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp, _ = self._rotate(authed)
        assert resp.status_code == 503 and resp.get_json() == TRUST_UNAVAILABLE_ERROR
        assert "schema is old" not in resp.get_data(as_text=True)
        assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 1

    def test_a_failed_refresh_after_rotating_is_still_200_with_one_warning(self, authed, caplog):
        authed.gateway.refresh_result = False
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp, _ = self._rotate(authed)
        assert resp.status_code == 200
        assert authed.gateway.refresh_calls == 2                      # inline + after-send retry
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1 and "could not be refreshed" in warnings[0]
        assert "rotation for client" in warnings[0]

    def test_dry_run_is_503_without_retry(self, mocker, zfs):
        authed = _authed(mocker, _config(dry_run=True))
        resp, _ = self._rotate(authed)
        assert resp.status_code == 503 and resp.get_json()["reason"] == "dry_run"
        assert "retry" not in resp.get_json() and authed.gateway.rotate_calls == []

    def test_disabled_and_draining(self, mocker, zfs):
        authed = _authed(mocker, _config(remote=False))
        resp, _ = self._rotate(authed)
        assert resp.status_code == 503 and resp.get_json() == {"error": "remote_backup disabled"}
        authed = _authed(mocker)
        authed.gate.start_draining()
        resp, _ = self._rotate(authed)
        assert resp.status_code == 503 and resp.get_json() == RELOADING_ERROR

    def test_the_signature_is_over_the_bound_id_not_the_url_or_body(self, authed):
        """`/rotate` has no client id in its URL; the payload names the BOUND id."""
        successor = make_client_cert()
        body = rotate_body(authed.server_fp, "someone-else", authed.who.fingerprint, successor)
        resp = authed.client().post("/rotate", json=body)
        assert resp.status_code == 403 and resp.get_json()["reason"] == "bad_signature"


# ---------------------------------------------------------------------------
# The successor's first authenticated request retires the predecessor
# ---------------------------------------------------------------------------


class TestPredecessorRetirement:
    @pytest.fixture
    def pair(self, authed):
        """`authed` with a successor pinned over `authed.who` (the predecessor)."""
        authed.successor = authed.gateway.pin(
            make_client_cert(), CLIENT_ID, predecessor=authed.who.fingerprint
        )
        return authed

    def test_the_successors_first_request_retires_the_old_pin(self, pair):
        resp = pair.client(pair.successor).get("/whoami")
        assert resp.status_code == 200
        assert pair.gateway.retire_calls == [pair.successor.fingerprint]
        assert pair.gateway.refresh_calls == 1

    def test_the_predecessors_own_requests_retire_nothing(self, pair):
        assert pair.client().get("/whoami").status_code == 200
        assert pair.gateway.retire_calls == [] and pair.gateway.refresh_calls == 0

    @pytest.mark.parametrize("url", ["/whoami", "/backup/client1/pool/data/resume_token"])
    def test_it_happens_on_any_authenticated_route(self, pair, zfs, url):
        assert pair.client(pair.successor).get(url).status_code == 200
        assert pair.gateway.retire_calls == [pair.successor.fingerprint]

    def test_the_store_call_precedes_the_refresh_and_the_refresh_precedes_the_view(self, pair):
        """Inline: retire -> bounded refresh -> the route; no after-send call when it worked."""
        order = []
        pair.gateway.retire_effect = lambda fp: order.append("retire") or True
        pair.gateway.refresh = lambda **kw: order.append("refresh") or True
        authed_view = pair.app.view_functions["whoami"]
        pair.app.view_functions["whoami"] = lambda: order.append("view") or authed_view()
        pair.client(pair.successor).get("/whoami")
        assert order == ["retire", "refresh", "view"]

    def test_the_refresh_runs_inline_with_the_request_budget_and_is_not_repeated(self, pair):
        client = plain_client(pair.app)
        client.environ_base["SSL_CLIENT_CERT"] = pair.successor.cert_pem
        resp = client.get("/whoami")                         # not buffered: not closed yet
        assert resp.status_code == 200 and pair.gateway.retire_calls == [pair.successor.fingerprint]
        assert pair.gateway.refresh_calls == 1               # before the response, not at its close
        assert pair.gateway.refresh_timeouts == [api_module.REQUEST_REFRESH_WAIT]
        resp.close()
        assert pair.gateway.refresh_calls == 1

    def test_a_refresh_that_fails_inline_is_retried_once_the_response_is_closed(self, pair, caplog):
        pair.gateway.refresh_result = False
        client = plain_client(pair.app)
        client.environ_base["SSL_CLIENT_CERT"] = pair.successor.cert_pem
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp = client.get("/whoami")
            assert resp.status_code == 200 and pair.gateway.refresh_calls == 1
            assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
            resp.close()
        assert pair.gateway.refresh_calls == 2
        assert pair.gateway.refresh_timeouts == [api_module.REQUEST_REFRESH_WAIT] * 2
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1 and f"retirement of pin {pair.who.fingerprint}" in warnings[0]

    def test_a_settled_pair_is_not_retried_on_later_requests(self, pair):
        client = pair.client(pair.successor)
        for _ in range(5):
            assert client.get("/whoami").status_code == 200
        assert pair.gateway.retire_calls == [pair.successor.fingerprint]

    def test_a_pair_the_store_says_is_stale_is_settled_without_a_refresh(self, pair):
        """`retire_predecessor` returns False when the field was only stale: no pin was
        deleted, so there is nothing to refresh, and the pair is remembered all the same."""
        pair.gateway.retire_effect = False
        client = pair.client(pair.successor)
        for _ in range(3):
            assert client.get("/whoami").status_code == 200
        assert pair.gateway.retire_calls == [pair.successor.fingerprint]
        assert pair.gateway.refresh_calls == 0

    def test_a_busy_database_does_not_fail_the_request_and_is_retried_later(self, pair, caplog):
        pair.gateway.retire_effect = ConfigBusy("locked")
        client = pair.client(pair.successor)
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            assert client.get("/whoami").status_code == 200
            assert client.get("/whoami").status_code == 200
        assert len(pair.gateway.retire_calls) == 2           # not remembered: tried again
        assert pair.gateway.refresh_calls == 0
        busy = [r for r in caplog.records if "database is busy" in r.getMessage()]
        assert len(busy) == 2 and all(r.levelno == logging.INFO for r in busy)
        pair.gateway.retire_effect = True                    # the database is free again
        assert client.get("/whoami").status_code == 200
        assert len(pair.gateway.retire_calls) == 3 and pair.gateway.refresh_calls == 1
        assert client.get("/whoami").status_code == 200
        assert len(pair.gateway.retire_calls) == 3           # settled now

    def test_any_other_failure_does_not_fail_the_request_and_warns(self, pair, caplog):
        pair.gateway.retire_effect = RuntimeError("schema is old")
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            assert pair.client(pair.successor).get("/whoami").status_code == 200
        assert any(r.levelno == logging.WARNING and "schema is old" in r.getMessage()
                   for r in caplog.records)
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_a_failed_refresh_after_retiring_does_not_fail_the_request(self, pair, caplog):
        pair.gateway.refresh_result = False
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            assert pair.client(pair.successor).get("/whoami").status_code == 200
        assert pair.gateway.refresh_calls == 2
        warnings = [r for r in caplog.records if "could not be refreshed" in r.getMessage()]
        assert len(warnings) == 1 and warnings[0].levelno == logging.WARNING

    def test_a_dry_run_retires_nothing(self, mocker, caplog):
        authed = _authed(mocker, _config(dry_run=True))
        succ = authed.gateway.pin(make_client_cert(), CLIENT_ID, predecessor=authed.who.fingerprint)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for _ in range(2):
                assert authed.client(succ).get("/whoami").status_code == 200
        assert authed.gateway.retire_calls == [] and authed.gateway.refresh_calls == 0
        assert any(r.levelno == logging.DEBUG and "dry run" in r.getMessage() for r in caplog.records)

    def test_the_retirement_is_logged_with_both_fingerprints(self, pair, caplog):
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            pair.client(pair.successor).get("/whoami")
        text = caplog.text
        assert "rotation complete" in text
        assert pair.who.fingerprint in text and pair.successor.fingerprint in text

    def test_an_expired_successor_does_not_retire_anything(self, mocker):
        authed = _authed(mocker, clock=lambda: 1_000.0)
        succ = authed.gateway.pin(make_client_cert(), CLIENT_ID, not_after=10.0,
                                  predecessor=authed.who.fingerprint)
        assert authed.client(succ).get("/whoami").status_code == 403
        assert authed.gateway.retire_calls == []

    def test_a_refused_unbound_request_does_not_retire(self, pair):
        resp = pair.client(pair.successor).post("/backup/client2/pool/data/negotiate", json={})
        assert resp.status_code == 403 and pair.gateway.retire_calls == []


@pytest.mark.integration
class TestRotationEndToEnd:
    """rotate -> first use of the successor -> the old certificate is gone, all in the real store."""

    def test_the_whole_flow(self, enrolled, caplog):
        old, successor = enrolled.old, make_client_cert()
        assert _rotate(enrolled, old, successor, client_id="host-a").status_code == 200
        assert enrolled.client(old).get("/whoami").status_code == 200          # still pinned
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp = enrolled.client(successor).get("/whoami")                    # first use
        assert resp.get_json() == {
            "client_id": "host-a", "fingerprint": successor.fingerprint, "protocol": PROTOCOL_VERSION,
        }
        pins = enrolled.trust.pins()
        assert set(pins) == {successor.fingerprint} and pins[successor.fingerprint].predecessor is None
        assert enrolled.client(old).get("/whoami").status_code == 403          # gone, at once
        assert enrolled.client(successor).get("/whoami").status_code == 200
        assert "rotation complete" in caplog.text

    def test_a_busy_database_postpones_the_retirement(self, mocker, enrolled):
        old, successor = enrolled.old, make_client_cert()
        _rotate(enrolled, old, successor, client_id="host-a")
        real_retire = store_trust.retire_predecessor
        failing = {"on": True}

        def retire(resolved, fingerprint):
            if failing["on"]:
                raise ConfigBusy("database is locked")
            return real_retire(resolved, fingerprint)

        mocker.patch.object(enrolled.trust.gateway, "_retire_predecessor", retire)
        client = enrolled.client(successor)
        assert client.get("/whoami").status_code == 200                         # request succeeds
        assert old.fingerprint in enrolled.trust.pins()                         # nothing retired yet
        failing["on"] = False
        assert client.get("/whoami").status_code == 200                         # retried on a later request
        assert old.fingerprint not in enrolled.trust.pins()
        assert enrolled.client(old).get("/whoami").status_code == 403

    def test_concurrent_first_requests_retire_exactly_once(self, enrolled):
        old, successor = enrolled.old, make_client_cert()
        _rotate(enrolled, old, successor, client_id="host-a")
        barrier = threading.Barrier(6)
        statuses = []

        def go():
            client = enrolled.client(successor)
            barrier.wait()
            statuses.append(client.get("/whoami").status_code)

        threads = [threading.Thread(target=go) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        assert statuses == [200] * 6
        assert set(enrolled.trust.pins()) == {successor.fingerprint}

    def test_a_dry_run_server_retires_nothing_and_enrolls_nothing(self, mocker, enrolled):
        """The same database, served by a dry-run app: the pending rotation stays pending."""
        old, successor = enrolled.old, make_client_cert()
        _rotate(enrolled, old, successor, client_id="host-a")
        dry = _authed(mocker, _config(dry_run=True), gateway=enrolled.trust.gateway,
                      server_fp=enrolled.server_fp, client_id=None)
        assert dry.client(successor).get("/whoami").status_code == 200
        assert old.fingerprint in enrolled.trust.pins()
        resp = dry.client(None).post("/enroll", json={})
        assert resp.status_code == 503 and resp.get_json()["reason"] == "dry_run"


# ---------------------------------------------------------------------------
# TrustGateway: failed rebuilds, rollbacks, polling, serialised store calls
# ---------------------------------------------------------------------------


class Factory:
    """A `context_factory` that records what it was asked to build and can be told to fail."""

    def __init__(self):
        self.fail = None
        self.built = []

    def __call__(self, ident, pems):
        if self.fail is not None:
            raise self.fail
        ctx = types.SimpleNamespace(pems=list(pems))
        self.built.append(ctx)
        return ctx


def pin_of(cert: ClientCert, client_id: str = "host-a", *, predecessor=None, enrolled_at=1.0):
    return store_trust.TrustedPin(
        fingerprint=cert.fingerprint, client_id=client_id, cert_pem=cert.cert_pem,
        not_after=cert.not_after.timestamp(), enrolled_at=enrolled_at, enrolled_via="token",
        predecessor=predecessor,
    )


@pytest.fixture
def gw(tmp_path):
    """-> `(gateway, stub, factory)`: a real `TrustGateway` over scripted reads and a scripted
    context factory."""
    factory = Factory()
    stub = StubTrust(store_id=7, generation=1)
    gateway, ident, stub = make_gateway(tmp_path / "srv.tls", stub=stub, context_factory=factory)
    gateway.test_identity = ident
    return gateway, stub, factory


class TestGatewayFailedRebuild:
    def test_a_failed_rebuild_keeps_the_old_pins_minus_the_revoked_ones(self, gw, caplog):
        gateway, stub, factory = gw
        a, b, c = make_client_cert(), make_client_cert(), make_client_cert()
        stub.set_pins([pin_of(a, "host-a"), pin_of(b, "host-b")], generation=1)
        assert gateway.refresh() is True
        context = gateway.current_context()
        assert gateway.lookup(a.fingerprint) and gateway.lookup(b.fingerprint)

        # generation 2: b revoked, c enrolled -- and the context cannot be rebuilt
        factory.fail = identity.IdentityError("server key changed on disk")
        stub.set_pins([pin_of(a, "host-a"), pin_of(c, "host-c")], generation=2)
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            assert gateway.refresh() is False
        assert gateway.lookup(a.fingerprint) is not None          # kept
        assert gateway.lookup(b.fingerprint) is None              # revoked: refused by the app
        assert gateway.lookup(c.fingerprint) is None              # new pins wait for the rebuild
        assert gateway.current_context() is context               # the old context stays
        assert gateway.view.key == (7, 2)                         # the view carries the NEW key
        assert any("1 revoked certificate(s) refused" in r.getMessage() for r in caplog.records)

    @staticmethod
    def _replace_server_identity(gateway):
        """Swap the server identity files on disk for a new one; -> the new fingerprint."""
        ident = gateway.test_identity
        directory = Path(ident.cert_path).parent
        Path(ident.cert_path).unlink()
        Path(ident.key_path).unlink()
        return identity.ensure_identity(
            directory, identity.ROLE_SERVER, group_of=directory.parent
        ).fingerprint

    def test_a_plain_identity_error_degrades_without_asking_for_a_restart(self, gw, caplog):
        """A lock timeout, an unreadable file...: say nothing about WHICH identity is on disk."""
        gateway, stub, factory = gw
        factory.fail = identity.IdentityError("identity lock not granted")
        stub.set_pins([pin_of(make_client_cert())], generation=2)
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            assert gateway.refresh() is False
        assert gateway.restart_reason is None
        assert "new pins wait; retrying every" in caplog.text

    def test_identity_changed_with_the_same_certificate_on_disk_is_no_reason_to_restart(self, gw):
        gateway, stub, factory = gw
        factory.fail = identity.IdentityChanged(
            "key no longer matches", expected=gateway.server_fingerprint, actual=None
        )
        stub.set_pins([pin_of(make_client_cert())], generation=2)
        assert gateway.refresh() is False
        assert gateway.restart_reason is None

    def test_a_different_server_certificate_on_disk_asks_for_a_restart(self, gw):
        gateway, stub, factory = gw
        new_fp = self._replace_server_identity(gateway)
        factory.fail = identity.IdentityChanged(
            "certificate changed", expected=gateway.server_fingerprint, actual=new_fp
        )
        stub.set_pins([pin_of(make_client_cert())], generation=2)
        assert gateway.refresh() is False
        reason = gateway.restart_reason
        assert reason is not None
        assert new_fp in reason and gateway.server_fingerprint in reason
        assert "no longer matches" in reason and "certificate changed" in reason

    def test_the_restart_reason_is_set_once(self, gw):
        gateway, stub, factory = gw
        new_fp = self._replace_server_identity(gateway)
        factory.fail = identity.IdentityChanged(
            "first", expected=gateway.server_fingerprint, actual=new_fp)
        stub.set_pins([pin_of(make_client_cert())], generation=2)
        gateway.refresh()
        first = gateway.restart_reason
        assert first is not None and "first" in first
        factory.fail = identity.IdentityChanged(
            "second", expected=gateway.server_fingerprint, actual=new_fp)
        stub.set_pins([pin_of(make_client_cert())], generation=3)
        gateway.refresh()
        assert gateway.restart_reason == first

    def test_the_pins_kept_take_the_new_reads_binding_expiry_and_predecessor(self, gw):
        gateway, stub, factory = gw
        a = make_client_cert()
        stub.set_pins([pin_of(a, "host-a")], generation=1)
        gateway.refresh()
        factory.fail = identity.IdentityError("cannot rebuild")
        newer = store_trust.TrustedPin(
            fingerprint=a.fingerprint, client_id="host-renamed", cert_pem=a.cert_pem,
            not_after=123.0, enrolled_at=1.0, enrolled_via="rotate", predecessor="ef" * 32,
        )
        stub.set_pins([newer], generation=2)
        gateway.refresh()
        assert gateway.lookup(a.fingerprint) == PinInfo("host-renamed", 123.0, "ef" * 32)

    @pytest.mark.parametrize("error", [ValueError("one pin is bad"), RuntimeError("ssl said no")])
    def test_other_rebuild_failures_degrade_without_a_restart(self, gw, error):
        gateway, stub, factory = gw
        a = make_client_cert()
        stub.set_pins([pin_of(a)], generation=1)
        gateway.refresh()
        factory.fail = error
        # ValueError makes the gateway look for the bad pin (all loadable here) and rebuild again
        stub.set_pins([pin_of(a), pin_of(make_client_cert())], generation=2)
        assert gateway.refresh() is False
        assert gateway.restart_reason is None
        assert gateway.lookup(a.fingerprint) is not None
        assert gateway.view.key == (7, 2)

    def test_poll_keeps_retrying_a_degraded_view_though_the_key_is_unchanged(self, gw):
        gateway, stub, factory = gw
        old, new = make_client_cert(), make_client_cert()
        stub.set_pins([pin_of(old)], generation=1)
        gateway.refresh()
        factory.fail = identity.IdentityError("on disk changed")
        stub.set_pins([pin_of(old), pin_of(new)], generation=2)
        assert gateway.refresh() is False
        reads = stub.read_calls
        assert gateway.poll() is False                       # still failing
        assert stub.read_calls == reads + 1                  # ...but it tried: the view is degraded
        factory.fail = None                                  # the identity is loadable again
        assert gateway.poll() is True
        assert gateway.lookup(new.fingerprint) is not None   # the waiting pin now works
        reads = stub.read_calls
        assert gateway.poll() is False                       # healthy and unchanged: no read
        assert stub.read_calls == reads

    def test_a_degraded_refresh_with_an_unchanged_key_still_retries(self, gw):
        gateway, stub, factory = gw
        stub.set_pins([pin_of(make_client_cert())], generation=1)
        factory.fail = identity.IdentityError("boom")
        assert gateway.refresh() is False
        built = len(factory.built)
        factory.fail = None
        assert gateway.refresh() is True
        assert len(factory.built) == built + 1

    def test_a_healthy_unchanged_refresh_builds_nothing(self, gw):
        gateway, stub, factory = gw
        stub.set_pins([pin_of(make_client_cert())], generation=1)
        assert gateway.refresh() is True
        built = len(factory.built)
        assert gateway.refresh() is True
        assert len(factory.built) == built

    def test_the_degraded_failure_is_logged_once_per_distinct_error(self, gw, caplog):
        gateway, stub, factory = gw
        factory.fail = identity.IdentityError("same")
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            for generation in (1, 2, 3):
                stub.set_pins([pin_of(make_client_cert())], generation=generation)
                gateway.refresh()
        rebuilds = [r for r in caplog.records if "revocations still apply" in r.getMessage()]
        assert len(rebuilds) == 1

    def test_a_revoked_pin_stays_refused_through_repeated_failures(self, gw):
        gateway, stub, factory = gw
        a, b = make_client_cert(), make_client_cert()
        stub.set_pins([pin_of(a), pin_of(b)], generation=1)
        gateway.refresh()
        factory.fail = identity.IdentityError("x")
        for generation in (2, 3, 4):
            stub.set_pins([pin_of(a)], generation=generation)
            gateway.refresh()
            assert gateway.lookup(b.fingerprint) is None and gateway.lookup(a.fingerprint)

    def test_a_failed_read_keeps_the_view_and_does_not_ask_for_a_restart(self, gw):
        gateway, stub, factory = gw
        a = make_client_cert()
        stub.set_pins([pin_of(a)], generation=1)
        gateway.refresh()
        stub.fail = ConfigBusy("locked")
        assert gateway.refresh() is False
        assert gateway.lookup(a.fingerprint) is not None and gateway.restart_reason is None


class TestGatewayRollback:
    def test_a_lower_generation_in_the_same_store_is_installed_with_a_warning(self, gw, caplog):
        gateway, stub, _ = gw
        a, b = make_client_cert(), make_client_cert()
        stub.set_pins([pin_of(a), pin_of(b)], generation=5)
        gateway.refresh()
        stub.set_pins([pin_of(a)], generation=3)             # restored from a backup: b is gone
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            assert gateway.refresh() is True
        assert gateway.view.key == (7, 3)
        assert gateway.lookup(b.fingerprint) is None and gateway.lookup(a.fingerprint)
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "went back from 5 to 3" in warnings[0] and "restored from a backup" in warnings[0]

    def test_poll_installs_a_rollback_too(self, gw):
        gateway, stub, _ = gw
        a = make_client_cert()
        stub.set_pins([pin_of(a)], generation=5)
        gateway.refresh()
        stub.set_pins([], generation=2)
        assert gateway.poll() is True
        assert gateway.view.key == (7, 2) and gateway.lookup(a.fingerprint) is None

    def test_a_different_store_at_a_lower_generation_is_installed_without_the_warning(self, gw, caplog):
        gateway, stub, _ = gw
        stub.set_pins([pin_of(make_client_cert())], generation=5, store_id=7)
        gateway.refresh()
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            stub.set_pins([], generation=1, store_id=99)     # the database FILE was replaced
            assert gateway.refresh() is True
        assert gateway.view.key == (99, 1)
        assert not [r for r in caplog.records if r.levelno == logging.WARNING]

    def test_a_higher_generation_is_installed(self, gw):
        gateway, stub, _ = gw
        a = make_client_cert()
        stub.set_pins([], generation=1)
        gateway.refresh()
        stub.set_pins([pin_of(a)], generation=2)
        assert gateway.refresh() is True and gateway.lookup(a.fingerprint)

    def test_an_equal_key_is_current_and_builds_nothing(self, gw):
        gateway, stub, factory = gw
        stub.set_pins([pin_of(make_client_cert())], generation=4)
        gateway.refresh()
        built = len(factory.built)
        assert gateway.refresh() is True and len(factory.built) == built

    def test_the_pin_info_carries_the_predecessor_and_expiry(self, gw):
        gateway, stub, _ = gw
        old, new = make_client_cert(), make_client_cert()
        stub.set_pins([pin_of(old), pin_of(new, predecessor=old.fingerprint)], generation=2)
        gateway.refresh()
        assert gateway.lookup(new.fingerprint) == PinInfo(
            "host-a", new.not_after.timestamp(), old.fingerprint)
        assert gateway.lookup(old.fingerprint).predecessor is None


class TestGatewayPollAndRefresh:
    def test_poll_skips_while_a_refresh_is_running(self, gw):
        gateway, stub, _ = gw
        stub.set_pins([pin_of(make_client_cert())], generation=1)
        entered, release = threading.Event(), threading.Event()
        real_read = stub.read_trust

        def slow_read(resolved):
            entered.set()
            assert release.wait(30)
            return real_read(resolved)

        gateway._read_trust = slow_read
        result = {}
        thread = threading.Thread(target=lambda: result.update(refreshed=gateway.refresh()), daemon=True)
        thread.start()
        try:
            assert entered.wait(30)
            keys = stub.key_calls
            started = time.monotonic()
            assert gateway.poll() is False                     # skipped, not waited for
            assert time.monotonic() - started < 2
            assert stub.key_calls == keys                      # it did not even read the key
        finally:
            release.set()
            thread.join(30)
        assert result["refreshed"] is True
        assert gateway.view.key == (7, 1)

    def test_a_refresh_waits_for_a_running_refresh_and_reads_after_it(self, gw):
        gateway, stub, _ = gw
        stub.set_pins([], generation=1)
        entered, release = threading.Event(), threading.Event()
        real_read = stub.read_trust
        calls = []

        def slow_read(resolved):
            calls.append(1)
            if len(calls) == 1:
                entered.set()
                assert release.wait(30)
            return real_read(resolved)

        gateway._read_trust = slow_read
        first = threading.Thread(target=gateway.refresh, daemon=True)
        first.start()
        assert entered.wait(30)
        second_done = threading.Event()
        second = threading.Thread(target=lambda: (gateway.refresh(), second_done.set()), daemon=True)
        second.start()
        assert not second_done.wait(0.3)                       # waiting for the first
        stub.set_pins([pin_of(make_client_cert())], generation=2)
        release.set()
        first.join(30)
        second.join(30)
        assert len(calls) == 2
        assert gateway.view.key == (7, 2)                      # the later read is the installed one

    def test_a_request_threads_refresh_gives_up_after_its_budget(self, gw):
        """`refresh(timeout=...)` neither waits for a running refresh beyond the budget nor reads;
        `False` leaves the change to the trust poll."""
        gateway, stub, _ = gw
        stub.set_pins([pin_of(make_client_cert())], generation=1)
        with gateway._lock:                                     # a refresh (or poll) is running
            started = time.monotonic()
            assert gateway.refresh(timeout=0.2) is False
            assert 0.15 <= time.monotonic() - started < 3
            assert gateway.refresh(timeout=0) is False
        assert stub.read_calls == 0 and gateway.view.key is None
        assert gateway.refresh(timeout=0.2) is True              # free again
        assert gateway.view.key == (7, 1)

    def test_a_request_threads_refresh_does_not_wait_for_the_store_lock_either(self, gw):
        gateway, stub, _ = gw
        stub.set_pins([pin_of(make_client_cert())], generation=1)
        api_module._STORE_LOCK.acquire()
        try:
            started = time.monotonic()
            assert gateway.refresh(timeout=0.2) is False
            assert time.monotonic() - started < 3
        finally:
            api_module._STORE_LOCK.release()
        assert stub.read_calls == 0
        assert gateway.poll() is True and gateway.view.key == (7, 1)   # the poll applies it

    def test_poll_reads_the_pins_only_when_the_key_changed(self, gw):
        gateway, stub, _ = gw
        stub.set_pins([], generation=1)
        assert gateway.poll() is True and stub.read_calls == 1       # first key: install
        assert gateway.poll() is False and stub.read_calls == 1      # unchanged
        stub.set_pins([pin_of(make_client_cert())], generation=2)
        assert gateway.poll() is True and stub.read_calls == 2

    def test_a_failing_key_read_is_swallowed_and_logged_once(self, gw, caplog):
        gateway, stub, _ = gw
        stub.fail = ConfigBusy("locked")
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            assert gateway.poll() is False and gateway.poll() is False
        assert len([r for r in caplog.records if "cannot read" in r.getMessage()]) == 1

    def test_before_the_first_read_nobody_is_trusted(self, gw):
        gateway, _, _ = gw
        assert gateway.view.key is None and gateway.lookup(make_client_cert().fingerprint) is None


class TestGatewayStoreCalls:
    @pytest.fixture
    def seams(self):
        calls = []
        return calls, dict(
            enroll_client=lambda resolved, **kw: calls.append(("enroll", kw)) or EnrollOutcome.ENROLLED,
            rotate_client=lambda resolved, **kw: calls.append(("rotate", kw)) or RotateOutcome.ROTATED,
            retire_predecessor=lambda resolved, fp: calls.append(("retire", fp)) or True,
            token_precheck=lambda resolved, **kw: calls.append(("precheck", kw)) or True,
        )

    @pytest.fixture
    def stored(self, tmp_path, seams):
        calls, kwargs = seams
        stub = StubTrust()
        gateway, _, _ = make_gateway(tmp_path / "srv.tls", stub=stub, **kwargs)
        return gateway, calls

    def test_the_three_writers_call_the_store_seams_with_the_database(self, stored):
        gateway, calls = stored
        assert gateway.enroll_client(client_id="x") is EnrollOutcome.ENROLLED
        assert gateway.rotate_client(old_fp="y") is RotateOutcome.ROTATED
        assert gateway.retire_predecessor("z") is True
        assert gateway.token_precheck(secret_sha256="h") is True
        assert calls == [("enroll", {"client_id": "x"}), ("rotate", {"old_fp": "y"}),
                         ("retire", "z"), ("precheck", {"secret_sha256": "h"})]

    def test_a_request_thread_gives_up_after_the_wait_with_config_busy(self, stored, monkeypatch):
        gateway, calls = stored
        monkeypatch.setattr(api_module, "STORE_CALL_WAIT", 0.2)
        assert STORE_CALL_WAIT == 15.0                       # the production value
        api_module._STORE_LOCK.acquire()                     # another thread's store call
        try:
            for call in (
                lambda: gateway.enroll_client(client_id="x"),
                lambda: gateway.rotate_client(old_fp="y"),
                lambda: gateway.retire_predecessor("z"),
                lambda: gateway.token_precheck(secret_sha256="h"),
            ):
                started = time.monotonic()
                with pytest.raises(ConfigBusy, match="another trust operation"):
                    call()
                assert 0.15 <= time.monotonic() - started < 3
        finally:
            api_module._STORE_LOCK.release()
        assert calls == []                                    # nothing was written
        assert gateway.retire_predecessor("z") is True        # and the lock is usable again

    def test_the_timeout_is_a_503_retry_for_enroll_and_rotate_and_a_later_retry_for_retirement(
        self, mocker, tmp_path, monkeypatch, zfs
    ):
        monkeypatch.setattr(api_module, "STORE_CALL_WAIT", 0.1)
        stub = StubTrust()
        gateway, _, _ = make_gateway(
            tmp_path / "srv.tls", stub=stub,
            enroll_client=lambda *a, **k: EnrollOutcome.ENROLLED,
            rotate_client=lambda *a, **k: RotateOutcome.ROTATED,
            retire_predecessor=lambda *a: True,
            token_precheck=lambda *a, **k: True,
        )
        authed = _authed(mocker, gateway=gateway, server_fp=gateway.server_fingerprint, client_id=None)
        who = make_client_cert()
        stub.set_pins([pin_of(who, CLIENT_ID)], generation=1)
        gateway.refresh()
        succ = make_client_cert()
        stub.set_pins([pin_of(who, CLIENT_ID), pin_of(succ, CLIENT_ID, predecessor=who.fingerprint)],
                      generation=2)
        gateway.refresh()
        api_module._STORE_LOCK.acquire()
        try:
            resp = _enroll(authed, "host-z", make_client_cert(),
                           str(identity.make_token(gateway.server_fingerprint)[0]))
            assert resp.status_code == 503 and resp.get_json() == TRUST_BUSY_ERROR
            resp = _rotate(authed, who, make_client_cert(), client_id=CLIENT_ID)
            assert resp.status_code == 503 and resp.get_json() == TRUST_BUSY_ERROR
            assert authed.client(succ).get("/whoami").status_code == 200   # retirement postponed
        finally:
            api_module._STORE_LOCK.release()

    def test_the_poll_thread_waits_indefinitely(self, gw, monkeypatch):
        gateway, stub, _ = gw
        monkeypatch.setattr(api_module, "STORE_CALL_WAIT", 0.05)
        stub.set_pins([pin_of(make_client_cert())], generation=1)
        api_module._STORE_LOCK.acquire()
        done = threading.Event()
        result = {}
        thread = threading.Thread(
            target=lambda: (result.update(polled=gateway.poll()), done.set()), daemon=True
        )
        thread.start()
        try:
            assert not done.wait(0.5)                         # still waiting: far past 0.05 s
        finally:
            api_module._STORE_LOCK.release()
        assert done.wait(30)
        thread.join(30)
        assert result["polled"] is True and gateway.view.key == (7, 1)

    def test_store_calls_never_overlap(self, gw):
        gateway, stub, _ = gw
        running = {"now": 0, "max": 0}
        guard = threading.Lock()

        def slow(resolved, *args, **kwargs):
            with guard:
                running["now"] += 1
                running["max"] = max(running["max"], running["now"])
            time.sleep(0.02)
            with guard:
                running["now"] -= 1
            return True

        gateway._retire_predecessor = slow
        threads = [threading.Thread(target=gateway.retire_predecessor, args=("z",)) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        assert running["max"] == 1

    def test_the_lock_is_released_when_the_call_raises(self):
        with pytest.raises(RuntimeError):
            api_module._store_call(lambda: (_ for _ in ()).throw(RuntimeError("x")))
        assert api_module._STORE_LOCK.acquire(timeout=1)
        api_module._STORE_LOCK.release()

    def test_a_new_lock_replaces_the_inherited_one_in_a_forked_child(self, monkeypatch):
        held = threading.Lock()
        held.acquire()
        monkeypatch.setattr(api_module, "_STORE_LOCK", held)
        api_module._reinit_store_lock_after_fork()
        fresh = api_module._STORE_LOCK
        assert fresh is not held and fresh.acquire(timeout=1)
        fresh.release()

    def test_the_default_seams_are_the_stores_writers(self, tmp_path):
        ident = make_gateway(tmp_path / "srv.tls")[1]
        gateway = TrustGateway(ident, object())
        assert gateway._enroll_client is store_trust.enroll_client
        assert gateway._rotate_client is store_trust.rotate_client
        assert gateway._retire_predecessor is store_trust.retire_predecessor
        assert gateway._token_precheck is store_trust.token_precheck
        assert gateway._read_trust is store_trust.read_trust
        assert gateway._read_trust_key is store_trust.read_trust_key


# ---------------------------------------------------------------------------
# create_app
# ---------------------------------------------------------------------------


class TestApiUsesOnlyThePublicStoreApi:
    """`api.py` reaches the config store through the `zfsbackup.config.store` package (its
    `__all__`), never through a submodule (`...store.trust`, `...store.errors`)."""

    @staticmethod
    def _tree():
        import ast
        return ast.parse(Path(api_module.__file__).read_text())

    def test_no_import_names_a_store_submodule(self):
        import ast
        offenders = []
        for node in ast.walk(self._tree()):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module.startswith("zfsbackup.config.store."):
                    offenders.append(module)
            elif isinstance(node, ast.Import):
                offenders += [a.name for a in node.names if a.name.startswith("zfsbackup.config.store.")]
        assert offenders == []

    def test_every_store_name_it_uses_is_exported_by_the_package(self):
        import ast
        from zfsbackup.config import store
        used = set()
        for node in ast.walk(self._tree()):
            if isinstance(node, ast.ImportFrom) and node.module == "zfsbackup.config.store":
                used |= {a.name for a in node.names}
            elif (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
                  and node.value.id == "_store"):
                used.add(node.attr)
        assert {"ConfigBusy", "EnrollOutcome", "NewPin", "RotateOutcome", "token_precheck",
                "read_trust_key", "read_trust", "enroll_client", "rotate_client",
                "retire_predecessor"} <= used
        assert used <= set(store.__all__), used - set(store.__all__)


class TestCreateApp:
    def test_the_gateway_is_required_there_is_no_unauthenticated_mode(self, mocker):
        mocker.patch.object(DatasetManager, "datasets", new_callable=PropertyMock, return_value=[])
        with pytest.raises(TypeError):
            create_app(_config())
        with pytest.raises(TypeError, match="no unauthenticated mode"):
            create_app(_config(), None, SERVER_FP)

    def test_the_server_fingerprint_is_required_and_validated(self, mocker):
        mocker.patch.object(DatasetManager, "datasets", new_callable=PropertyMock, return_value=[])
        with pytest.raises(TypeError):
            create_app(_config(), FakeGateway())
        with pytest.raises(ValueError):
            create_app(_config(), FakeGateway(), "not a fingerprint")

    def test_the_gateway_and_the_normalised_fingerprint_are_published(self, mocker):
        mocker.patch.object(DatasetManager, "datasets", new_callable=PropertyMock, return_value=[])
        gateway = FakeGateway()
        app = create_app(_config(), gateway, ("AB" * 32))
        assert app.extensions["zfsbackup.trust"] is gateway
        assert app.extensions["zfsbackup.server_fingerprint"] == "ab" * 32

    def test_the_clock_is_keyword_only_and_defaults_to_time_time(self, mocker):
        mocker.patch.object(DatasetManager, "datasets", new_callable=PropertyMock, return_value=[])
        with pytest.raises(TypeError):
            create_app(_config(), FakeGateway(), SERVER_FP, lambda: 0.0)

    def test_the_default_clock_is_the_real_one(self, authed):
        """A pin valid for the next hour authenticates; the same pin an hour ago does not."""
        authed.gateway.pins[authed.who.fingerprint] = PinInfo(CLIENT_ID, time.time() + HOUR, None)
        assert authed.client().get("/whoami").status_code == 200
        authed.gateway.pins[authed.who.fingerprint] = PinInfo(CLIENT_ID, time.time() - HOUR, None)
        assert authed.client().get("/whoami").status_code == 403

    def test_the_request_limits(self):
        assert MAX_REQUEST_BYTES == 1024 * 1024
        assert MAX_TRUST_REQUEST_BYTES == 64 * 1024

    def test_a_too_large_request_to_any_route_is_a_json_413(self, authed):
        resp = authed.client().post(
            "/backup/client1/pool/data/negotiate", data=b"x" * (MAX_REQUEST_BYTES + 1),
            content_type="application/json",
        )
        assert resp.status_code == 413 and resp.is_json

"""A local destination that shares the server's backup target (docs/local_destination_plan.md,
"Addendum -- a local target shared with the server").

One backup dataset `T` holds the remote clients' copies AND this host's own local copies
(`T/<own client id>/<pool>/<path>`). Four product pieces carry the feature, and this file covers the ones
no other file owns (the validation matrix is in `test_zfsbackup_store_local_dest.py`, the CLI in
`test_zfsbackup_cli_local_dest.py`, the pruner in `test_zfsbackup_prune_local_copies.py`, real ZFS in
`test_zfsbackup_real_local.py`):

* ``TestSharedReceiveTarget`` -- `config.shared_receive_target`, the one definition of "shared";
* ``TestReservedClientIdRoutes`` -- `create_app(reserved_client_id=)`: `negotiate`, `resume_token` and the
  `/stream` upgrade answer 409 `reserved_client_id` to a client BOUND to the reserved id, after
  authentication and the protocol check, before any `zfs` call, claim, pending entry or spawn;
* ``TestReservedClientIdEnroll`` -- `/enroll` refuses the id after the token pre-check, with the token unspent
  and no lockout count; ``TestReservedClientIdEnrollTokenLiveness`` / ``...ConfirmWiring`` -- the branch asks
  `confirm_token_live`: an EXPIRED matching token (True from `token_precheck`) is the uniform 403, latched
  durably and counted towards the lockout, a live one is still the 409 and stays unspent (real store, then a
  scripted gateway for the call order, arguments and 503 mapping);
* ``TestReservedClientIdLog`` -- one WARNING per hour naming `trust revoke --client`;
* ``TestApiWorkerPassesTheReservation`` -- `ApiWorker._serve` passes `reserved_client_id` only when shared;
  ``TestDaemonHandsOverTheClientId`` -- `BackupDaemon._new_worker` hands the id to the API worker only when
  shared (a real child under fork/spawn/forkserver is in ``TestApiWorkerChildren``).

Nothing here reaches a real `zfs`: every `zfs` entry point `api` uses is patched and asserted untouched.

Mutation counts (scratch copy of the tree, one revert at a time; failing tests in this file): the route check
in `_authenticate` disabled 17; `create_app` ignoring `reserved_client_id` 23; the `/enroll` check removed 6;
the `/enroll` refusal counted as a failed attempt (`failed(...)`) 1; `ApiWorker._serve` passing the id
unconditionally 4; `_new_worker` passing it unconditionally 3; `shared_receive_target` ignoring
`remote_backup.enabled` 2.
"""

import base64
import logging
import multiprocessing
import os
import socket
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from flask import request

from _api_support import (
    CLIENT_ID, Authed, FakeGateway, RealTrust, enroll_body, make_client_cert, permissive_limits,
)
from _config_handoff import RecordingApiWorker, read_records
from test_zfsbackup_api_auth import (  # noqa: F401  (fixtures and helpers)
    WS_HEADERS, _anonymous, _authed, _enroll, _real_app, _untouched, real, zfs,
)
from zfsbackup import api as api_module
from zfsbackup import identity
from zfsbackup import workers as workers_module
from zfsbackup.api import (
    ENROLL_REFUSED_ERROR, RESERVED_CLIENT_ENDPOINTS, RESERVED_CLIENT_REASON, TRUST_BUSY_ERROR,
    TRUST_UNAVAILABLE_ERROR,
)
from zfsbackup.config import (
    BackupConfig, DatasetConfig, Destination, RemoteServerConfig, shared_receive_target,
)
from zfsbackup.config import model as model_module
from zfsbackup.config.store import trust as store_trust
from zfsbackup.config.store.errors import ConfigBusy
from zfsbackup.config.store.trust import EnrollOutcome, TokenState
from zfsbackup.daemon import BackupDaemon
from zfsbackup.logsetup import PEER_LOGGER
from zfsbackup.protocol import PROTOCOL_HEADER
from zfsbackup.workers import ApiWorker

TARGET = "pool/backups"
HOST = "host-1"              # this host's own client id (the reserved one)
OTHER = "client2"


def shared_config(*, target=TARGET, local=None, remote_enabled=True, **kwargs) -> BackupConfig:
    """A config whose local destination `usb` targets `local` (default: the remote target itself)."""
    return BackupConfig(
        datasets=[DatasetConfig(name="pool/data")],
        destinations={"usb": Destination(url=f"zfs:{TARGET if local is None else local}")},
        remote_backup=RemoteServerConfig(target_dataset=target, enabled=remote_enabled),
        **kwargs,
    )


# ---------------------------------------------------------------------------
# shared_receive_target
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSharedReceiveTarget:
    def test_it_is_exported_from_the_package(self):
        import zfsbackup.config as package

        assert package.shared_receive_target is model_module.shared_receive_target
        assert "shared_receive_target" in package.__all__

    def test_equal_targets_are_shared_and_the_remote_target_is_returned(self):
        assert shared_receive_target(shared_config()) == TARGET

    def test_a_pool_root_target_is_shared_too(self):
        assert shared_receive_target(shared_config(target="bk", local="bk")) == "bk"

    @pytest.mark.parametrize("remote, local", [
        (TARGET + "/", TARGET), (TARGET, TARGET + "/"), (TARGET + "//", TARGET + "/"),
    ])
    def test_a_trailing_slash_is_ignored_and_the_result_has_none(self, remote, local):
        assert shared_receive_target(shared_config(target=remote, local=local)) == TARGET

    def test_it_compares_components_not_prefixes(self):
        assert shared_receive_target(shared_config(target="pool/b", local="pool/bk")) is None
        assert shared_receive_target(shared_config(target="pool/bk", local="pool/b")) is None

    @pytest.mark.parametrize("local", [TARGET + "/inner", "pool", TARGET + "x", "other/backups"])
    def test_nested_above_and_unrelated_targets_are_not_shared(self, local):
        assert shared_receive_target(shared_config(local=local)) is None

    def test_a_disabled_remote_backup_shares_nothing(self):
        assert shared_receive_target(shared_config(remote_enabled=False)) is None

    def test_no_remote_backup_shares_nothing(self):
        config = shared_config()
        config.remote_backup = None
        assert shared_receive_target(config) is None

    def test_a_declared_destination_counts_whether_or_not_a_dataset_uses_it(self):
        """The check is on the declaration: `datasets[].remote` does not name `usb`."""
        config = shared_config()
        assert all(not ds.remote for ds in config.datasets)
        assert shared_receive_target(config) == TARGET

    def test_it_does_not_depend_on_the_dataset_being_enabled(self):
        config = shared_config()
        config.datasets[0].enabled = False
        assert shared_receive_target(config) == TARGET

    def test_https_destinations_are_ignored(self):
        config = shared_config(local="other/backups")
        config.destinations["offsite"] = Destination(url="https://pool.example:8443")
        assert shared_receive_target(config) is None

    def test_any_one_of_several_local_destinations_is_enough(self):
        config = shared_config(local="other/backups")
        config.destinations["second"] = Destination(url=f"zfs:{TARGET}")
        assert shared_receive_target(config) == TARGET

    def test_no_destinations_at_all(self):
        config = shared_config()
        config.destinations = {}
        assert shared_receive_target(config) is None

    @pytest.mark.parametrize("config", [
        pytest.param(SimpleNamespace(), id="nothing"),
        pytest.param(SimpleNamespace(remote_backup=None), id="no-remote"),
        pytest.param(SimpleNamespace(remote_backup=SimpleNamespace()), id="remote-without-enabled"),
        pytest.param(SimpleNamespace(remote_backup=SimpleNamespace(enabled=True)), id="no-target"),
        pytest.param(SimpleNamespace(
            remote_backup=SimpleNamespace(enabled=True, target_dataset=None),
            destinations={"u": Destination(url="zfs:bk")}), id="target-none"),
        pytest.param(SimpleNamespace(
            remote_backup=SimpleNamespace(enabled=True, target_dataset=5),
            destinations={"u": Destination(url="zfs:bk")}), id="target-int"),
        pytest.param(SimpleNamespace(
            remote_backup=SimpleNamespace(enabled=True, target_dataset="  "),
            destinations={"u": Destination(url="zfs:bk")}), id="target-blank"),
        pytest.param(SimpleNamespace(
            remote_backup=SimpleNamespace(enabled=True, target_dataset="/"),
            destinations={"u": Destination(url="zfs:bk")}), id="target-slash"),
        pytest.param(SimpleNamespace(
            remote_backup=SimpleNamespace(enabled=True, target_dataset="bk"),
            destinations=None), id="destinations-none"),
        pytest.param(SimpleNamespace(
            remote_backup=SimpleNamespace(enabled=True, target_dataset="bk"),
            destinations={"u": SimpleNamespace()}), id="destination-without-fields"),
        pytest.param(SimpleNamespace(
            remote_backup=SimpleNamespace(enabled=True, target_dataset="bk"),
            destinations={"u": SimpleNamespace(is_local=True, target_dataset=None)}), id="local-no-target"),
        pytest.param(SimpleNamespace(
            remote_backup=SimpleNamespace(enabled=True, target_dataset="bk"),
            destinations={"u": SimpleNamespace(is_local=True, target_dataset=7)}), id="local-int-target"),
        pytest.param(SimpleNamespace(
            remote_backup=SimpleNamespace(enabled=True, target_dataset="bk"),
            destinations={"u": SimpleNamespace(is_local=True, target_dataset="")}), id="local-empty-target"),
    ])
    def test_a_config_built_in_code_with_missing_fields_is_not_shared_and_never_raises(self, config):
        assert shared_receive_target(config) is None

    def test_a_duck_typed_config_that_is_shared(self):
        config = SimpleNamespace(
            remote_backup=SimpleNamespace(enabled=True, target_dataset="bk/"),
            destinations={"u": SimpleNamespace(is_local=True, target_dataset="bk")},
        )
        assert shared_receive_target(config) == "bk"


# ---------------------------------------------------------------------------
# The routes
# ---------------------------------------------------------------------------

NEGOTIATE = "/backup/{}/pool/data/negotiate"
RESUME = "/backup/{}/pool/data/resume_token"
STREAM = "/backup/{}/pool/data/stream?mode=fresh"
HTTP_ROUTES = [
    pytest.param("post", NEGOTIATE, id="negotiate"),
    pytest.param("get", RESUME, id="resume_token"),
]
ROUTES = [*HTTP_ROUTES, pytest.param("get", STREAM, id="stream")]
STREAM_PATH = "/backup/{}/pool/data/stream"


def call(client, method, template, client_id):
    url = template.format(client_id)
    kwargs = {"headers": WS_HEADERS} if template is STREAM else {}
    if method == "post":
        kwargs["json"] = {"snapshots": [], "config": "e30="}
    return getattr(client, method)(url, **kwargs)


def api_module_protocol_environ():
    from _api_support import PROTOCOL_ENVIRON

    return PROTOCOL_ENVIRON


def reserved_app(mocker, *, reserved=HOST, bound=HOST, **kwargs):
    """An app that reserves `reserved`, with a certificate pinned under `bound`."""
    return _authed(mocker, _shared_server_config(), client_id=bound, reserved_client_id=reserved, **kwargs)


def _shared_server_config():
    return BackupConfig(
        datasets=[DatasetConfig(name="pool/data")],
        remote_backup=RemoteServerConfig(target_dataset=TARGET, enabled=True),
        destinations={"usb": Destination(url=f"zfs:{TARGET}")},
    )


def assert_refused_as_reserved(resp, authed):
    assert resp.status_code == 409, resp.get_data(as_text=True)
    body = resp.get_json()
    assert body["reason"] == RESERVED_CLIENT_REASON == "reserved_client_id"
    assert isinstance(body["error"], str) and body["error"]       # the wording is not pinned
    assert "retry" not in body
    assert authed.gate.active == 0


def assert_no_server_state(authed):
    from _api_support import pending_configs, streams_in_progress

    assert dict(pending_configs(authed.app)) == {}
    assert streams_in_progress(authed.app) == set()


@pytest.mark.integration
class TestReservedClientIdRoutes:
    @pytest.mark.parametrize("method, template", ROUTES)
    def test_the_reserved_id_gets_409_and_nothing_runs(self, mocker, zfs, method, template):
        authed = reserved_app(mocker)
        resp = call(authed.client(), method, template, HOST)
        assert_refused_as_reserved(resp, authed)
        _untouched(zfs)
        assert_no_server_state(authed)

    def test_the_endpoint_set_is_exactly_the_three_client_routes(self):
        assert RESERVED_CLIENT_ENDPOINTS == {"backup_negotiate", "backup_stream", "backup_resume_token"}

    def test_every_client_route_is_covered(self, mocker):
        authed = reserved_app(mocker)
        client_routes = {r.endpoint for r in authed.app.url_map.iter_rules() if "client_id" in r.arguments}
        assert client_routes == RESERVED_CLIENT_ENDPOINTS

    @pytest.mark.parametrize("method, template", ROUTES)
    def test_a_stream_upgrade_and_the_others_are_refused_as_plain_http(self, mocker, zfs, method, template):
        authed = reserved_app(mocker)
        resp = call(authed.client(), method, template, HOST)
        assert resp.status_code == 409 and resp.mimetype == "application/json"

    @pytest.mark.parametrize("method, template", ROUTES)
    def test_the_negotiate_body_is_not_read_for_a_reserved_client(self, mocker, zfs, method, template):
        """A malformed body would be 400 from negotiate; the reserved refusal comes first."""
        authed = reserved_app(mocker)
        client = authed.client()
        if method == "post":
            resp = client.post(template.format(HOST), data=b"{not json", content_type="application/json")
            assert resp.status_code == 409
        else:
            assert call(client, method, template, HOST).status_code == 409

    @pytest.mark.parametrize("method, template", HTTP_ROUTES)
    def test_other_ids_are_unaffected(self, mocker, zfs, method, template):
        authed = reserved_app(mocker, bound=OTHER)
        resp = call(authed.client(), method, template, OTHER)
        assert resp.status_code != 409 or resp.get_json().get("reason") != RESERVED_CLIENT_REASON
        assert resp.status_code in (200, 400, 503)       # the route ran (or its own shape check did)

    @pytest.mark.parametrize("reserved", ["host-", "Host-1", "host-10", "host-1.", "ost-1"])
    def test_the_match_is_exact(self, mocker, zfs, reserved):
        authed = reserved_app(mocker, reserved=reserved, bound=HOST)
        resp = authed.client().get(RESUME.format(HOST))
        assert resp.status_code == 200
        assert resp.get_json() == {"resume_token": None} or "resume_token" in resp.get_json()

    @pytest.mark.parametrize("method, template", HTTP_ROUTES)
    def test_nothing_is_reserved_without_a_reserved_client_id(self, mocker, zfs, method, template):
        authed = _authed(mocker, _shared_server_config(), client_id=HOST)       # reserved_client_id=None
        resp = call(authed.client(), method, template, HOST)
        assert not (resp.status_code == 409 and resp.get_json().get("reason") == RESERVED_CLIENT_REASON)
        assert resp.status_code in (200, 400, 503)

    # The flask-sock route cannot run under the Flask test client (no socket in the WSGI environ), so
    # the stream's "not refused" cases run the `before_request` hooks only, as `Authed.context` does.

    def test_the_stream_of_another_id_passes_authentication(self, mocker, zfs):
        authed = reserved_app(mocker, bound=OTHER)
        with authed.context(STREAM_PATH.format(OTHER), query_string={"mode": "fresh"}, headers=WS_HEADERS):
            pass                                  # `context` asserts the request was not refused

    def test_the_stream_of_the_reserved_id_is_refused_by_the_hooks(self, mocker, zfs):
        authed = reserved_app(mocker)
        environ = {"SSL_CLIENT_CERT": authed.who.cert_pem, api_module_protocol_environ(): "1"}
        with authed.app.test_request_context(
            STREAM_PATH.format(HOST), query_string={"mode": "fresh"}, environ_base=environ,
            headers=WS_HEADERS,
        ):
            assert request.endpoint == "backup_stream"
            refusal = authed.app.preprocess_request()
        response, status = refusal
        assert status == 409 and response.get_json()["reason"] == RESERVED_CLIENT_REASON
        _untouched(zfs)

    def test_the_stream_without_a_reservation_passes_authentication(self, mocker, zfs):
        authed = _authed(mocker, _shared_server_config(), client_id=HOST)
        with authed.context(STREAM_PATH.format(HOST), query_string={"mode": "fresh"}, headers=WS_HEADERS):
            pass

    def test_a_server_that_does_not_share_never_reserves_even_for_its_own_id(self, mocker, zfs):
        """Loopback without a shared target keeps working: the same id is served."""
        authed = _authed(mocker, _config_with_remote_only(), client_id=HOST)
        resp = authed.client().get(RESUME.format(HOST))
        assert resp.status_code == 200

    # -- precedence: authentication and the protocol check come first --------------------------------

    @pytest.mark.parametrize("method, template", ROUTES)
    def test_no_certificate_is_401_not_409(self, mocker, zfs, method, template):
        authed = reserved_app(mocker)
        resp = call(authed.client(None), method, template, HOST)
        assert resp.status_code == 401
        _untouched(zfs)

    @pytest.mark.parametrize("method, template", ROUTES)
    def test_an_unknown_certificate_is_403_not_409(self, mocker, zfs, method, template):
        authed = reserved_app(mocker)
        stranger = make_client_cert(cn=HOST)            # never pinned
        resp = call(authed.client(stranger), method, template, HOST)
        assert resp.status_code == 403
        assert resp.get_json().get("reason") != RESERVED_CLIENT_REASON
        _untouched(zfs)

    @pytest.mark.parametrize("protocol", [None, 2, "abc"])
    @pytest.mark.parametrize("method, template", ROUTES)
    def test_a_missing_or_unsupported_protocol_is_426_not_409(self, mocker, zfs, method, template, protocol):
        authed = reserved_app(mocker)
        resp = call(authed.client(protocol=protocol), method, template, HOST)
        assert resp.status_code == 426
        assert resp.get_json()["reason"] == "protocol"
        _untouched(zfs)

    @pytest.mark.parametrize("method, template", ROUTES)
    def test_a_url_naming_another_id_is_403_mismatch_not_409(self, mocker, zfs, method, template):
        """The bound id is reserved but the URL names someone else: the existing 403 comes first."""
        authed = reserved_app(mocker)
        resp = call(authed.client(), method, template, OTHER)
        assert resp.status_code == 403
        assert resp.get_json() == {"error": "client_id mismatch", "bound": HOST}
        _untouched(zfs)

    @pytest.mark.parametrize("method, template", ROUTES)
    def test_an_expired_pin_is_403_not_409(self, mocker, zfs, method, template):
        authed = reserved_app(mocker)
        for pin in authed.gateway.pins.values():
            authed.gateway.pins[authed.who.fingerprint] = api_module.PinInfo(
                pin.client_id, time.time() - 10, None)
        resp = call(authed.client(), method, template, HOST)
        assert resp.status_code == 403
        assert resp.get_json().get("reason") != RESERVED_CLIENT_REASON
        _untouched(zfs)

    # -- endpoints that are not touched --------------------------------------------------------------

    def test_whoami_answers_the_reserved_client_as_before(self, mocker, zfs):
        authed = reserved_app(mocker)
        resp = authed.client().get("/whoami")
        assert resp.status_code == 200
        assert resp.get_json()["client_id"] == HOST

    def test_health_is_unchanged(self, mocker, zfs):
        authed = reserved_app(mocker)
        resp = authed.client(None).get("/health")
        assert resp.status_code == 200

    def test_rotate_is_unchanged_for_the_reserved_client(self, mocker, tls_config_db, zfs):
        """A reserved client may still rotate its certificate (it is not a receive)."""
        app = _real_app(mocker, tls_config_db, reserved_client_id=HOST)
        current = make_client_cert(cn=HOST)
        app.trust.enroll(current, HOST)
        successor = make_client_cert(cn=HOST)
        from _api_support import rotate_body

        body = rotate_body(app.server_fp, HOST, current.fingerprint, successor)
        resp = app.client(current).post("/rotate", json=body)
        assert resp.status_code == 200, resp.get_data(as_text=True)
        assert resp.get_json()["status"] == "rotated"
        _untouched(zfs)

    def test_the_predecessor_retirement_still_runs_before_the_refusal(self, mocker, zfs):
        """Authentication is complete (including retiring a superseded pin) before the reserved check."""
        authed = reserved_app(mocker)
        old = make_client_cert(cn=HOST)
        authed.gateway.pin(old, HOST)
        authed.gateway.pin(authed.who, HOST, predecessor=old.fingerprint)
        resp = authed.client().get(RESUME.format(HOST))
        assert resp.status_code == 409
        assert authed.gateway.retire_calls == [authed.who.fingerprint]   # keyed by the successor


def _config_with_remote_only():
    return BackupConfig(
        datasets=[DatasetConfig(name="pool/data")],
        remote_backup=RemoteServerConfig(target_dataset=TARGET, enabled=True),
    )


# ---------------------------------------------------------------------------
# /enroll
# ---------------------------------------------------------------------------


@pytest.fixture
def reserved_real(mocker, tls_config_db):
    """An app over a REAL gateway and database that reserves `HOST`; no certificate is pinned."""
    return _real_app(mocker, tls_config_db, reserved_client_id=HOST)


@pytest.mark.integration
class TestReservedClientIdEnroll:
    def test_the_reserved_id_is_refused_with_409_and_the_reason(self, reserved_real, zfs):
        token = reserved_real.trust.token()
        resp = _enroll(reserved_real, HOST, make_client_cert(), token)
        assert resp.status_code == 409
        body = resp.get_json()
        assert body["reason"] == RESERVED_CLIENT_REASON
        assert "retry" not in body

    def test_the_token_is_not_consumed_and_a_later_enroll_with_another_id_works(self, reserved_real, zfs):
        token = reserved_real.trust.token()
        assert _enroll(reserved_real, HOST, make_client_cert(), token).status_code == 409
        assert reserved_real.trust.token_state() is TokenState.ACTIVE
        assert reserved_real.trust.pins() == {} and reserved_real.trust.clients() == {}
        cert = make_client_cert()
        resp = _enroll(reserved_real, OTHER, cert, token)           # the very same token
        assert resp.status_code == 200 and resp.get_json()["client_id"] == OTHER
        assert reserved_real.trust.token_state() is TokenState.CONSUMED
        assert cert.fingerprint in reserved_real.trust.pins()

    def test_no_zfs_call_and_no_store_write(self, mocker, reserved_real, zfs):
        spy = mocker.spy(reserved_real.gateway, "enroll_client")
        _enroll(reserved_real, HOST, make_client_cert(), reserved_real.trust.token())
        spy.assert_not_called()
        _untouched(zfs)

    def test_it_comes_after_the_token_pre_check(self, mocker, reserved_real, zfs):
        spy = mocker.spy(reserved_real.gateway, "token_precheck")
        _enroll(reserved_real, HOST, make_client_cert(), reserved_real.trust.token())
        spy.assert_called_once()

    def test_a_caller_without_a_valid_token_still_gets_the_uniform_403(self, reserved_real, zfs):
        """The reservation is not an oracle: only a holder of a valid token learns the id is reserved."""
        wrong = str(identity.make_token(reserved_real.trust.server_fp)[0])
        resp = _enroll(reserved_real, HOST, make_client_cert(), wrong)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR
        assert RESERVED_CLIENT_REASON not in resp.get_data(as_text=True)

    def test_no_token_at_all_is_the_uniform_403(self, reserved_real, zfs):
        fresh = str(identity.make_token(reserved_real.trust.server_fp)[0])
        assert reserved_real.trust.token_state() is TokenState.NONE
        resp = _enroll(reserved_real, HOST, make_client_cert(), fresh)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR

    def test_a_spent_token_is_the_uniform_403(self, reserved_real, zfs):
        spent = reserved_real.trust.token()
        assert _enroll(reserved_real, OTHER, make_client_cert(), spent).status_code == 200
        assert _enroll(reserved_real, HOST, make_client_cert(), spent).status_code == 403

    def test_a_bad_signature_is_still_checked_first(self, reserved_real, zfs):
        resp = _enroll(reserved_real, HOST, make_client_cert(), reserved_real.trust.token(),
                       signature=base64.b64encode(b"junk").decode())
        assert resp.status_code == 403 and resp.get_json()["reason"] == "bad_signature"

    def test_other_ids_are_unaffected(self, reserved_real, zfs):
        cert = make_client_cert()
        assert _enroll(reserved_real, OTHER, cert, reserved_real.trust.token()).status_code == 200
        assert reserved_real.trust.pins()[cert.fingerprint].client_id == OTHER

    def test_without_a_reservation_the_same_id_enrolls(self, mocker, tls_config_db, zfs):
        app = _real_app(mocker, tls_config_db)
        cert = make_client_cert()
        assert _enroll(app, HOST, cert, app.trust.token()).status_code == 200

    def test_trying_the_reserved_id_does_not_count_towards_the_lockout(self, mocker, tls_config_db, zfs):
        """The default limits lock an address out after 5 failures; 10 attempts of the reserved id
        (each with a valid token) must not."""
        app = _real_app(mocker, tls_config_db, reserved_client_id=HOST)
        token = app.trust.token()
        for _ in range(10):
            assert _enroll(app, HOST, make_client_cert(), token).status_code == 409
        resp = _enroll(app, OTHER, make_client_cert(), token)
        assert resp.status_code == 200, resp.get_data(as_text=True)

    def test_a_lockout_from_real_failures_is_unchanged(self, mocker, tls_config_db, zfs):
        """The reservation did not change what counts: five wrong tokens still lock the address."""
        app = _real_app(mocker, tls_config_db, reserved_client_id=HOST)
        wrong = str(identity.make_token(app.trust.server_fp)[0])
        statuses = [_enroll(app, OTHER, make_client_cert(), wrong).status_code for _ in range(7)]
        assert 429 in statuses

    def test_the_in_flight_slot_is_released(self, mocker, tls_config_db, zfs):
        app = _real_app(mocker, tls_config_db, reserved_client_id=HOST)
        token = app.trust.token()
        for _ in range(4):                       # more than ENROLL_MAX_IN_FLIGHT would allow if leaked
            assert _enroll(app, HOST, make_client_cert(), token).status_code == 409
        assert _enroll(app, OTHER, make_client_cert(), token).status_code == 200


class _Clock:
    """A settable clock for `create_app(clock=...)`."""

    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


@pytest.fixture
def timed_real(mocker, tls_config_db):
    """`reserved_real` with a settable app clock (`app.clock`) and a token created at `app.t0`
    that lives 100 s: `app.token_text` (created at t0 - 1, expires at t0 + 99)."""
    clock = _Clock(time.time())
    app = _real_app(mocker, tls_config_db, reserved_client_id=HOST, clock=clock)
    app.clock, app.t0 = clock, clock.now
    app.token_text = app.trust.token(created_at=app.t0, ttl=100)
    return app


def token_state_at(app, when):
    return store_trust.token_status(app.trust.resolved, when).state


@pytest.mark.integration
class TestReservedClientIdEnrollTokenLiveness:
    """The reserved-id branch of `/enroll` asks `confirm_token_live`: `token_precheck` is True for an
    EXPIRED token whose credentials match (so that `enroll_client` can retire it), and the branch
    returns before `enroll_client`, so without the confirmation an expired token earned the 409 and
    was never latched (a clock stepping back revived it)."""

    def test_an_expired_matching_token_is_the_uniform_403_not_the_reservation(self, timed_real, zfs):
        app = timed_real
        app.clock.now = app.t0 + 500
        resp = _enroll(app, HOST, make_client_cert(), app.token_text)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR
        assert RESERVED_CLIENT_REASON not in resp.get_data(as_text=True)

    def test_the_expired_token_is_retired_durably(self, timed_real, zfs):
        app = timed_real
        assert token_state_at(app, app.t0 + 10) is TokenState.ACTIVE
        app.clock.now = app.t0 + 500
        assert _enroll(app, HOST, make_client_cert(), app.token_text).status_code == 403
        assert token_state_at(app, app.t0 + 10) is not TokenState.ACTIVE      # latched
        # The clock steps back into the window; another id, the same token: still dead.
        app.clock.now = app.t0 + 10
        cert = make_client_cert()
        resp = _enroll(app, OTHER, cert, app.token_text)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR
        assert app.trust.pins() == {} and app.trust.clients() == {}

    def test_it_reaches_neither_zfs_nor_the_enrolling_writer_nor_a_trust_row(self, mocker, timed_real, zfs):
        app = timed_real
        spy = mocker.spy(app.gateway, "enroll_client")
        key = app.trust.trust_key()
        app.clock.now = app.t0 + 500
        _enroll(app, HOST, make_client_cert(), app.token_text)
        spy.assert_not_called()
        _untouched(zfs)
        assert app.trust.pins() == {} and app.trust.clients() == {}
        assert app.trust.trust_key() == key          # the latch is not a trust change

    def test_an_expired_token_counts_towards_the_lockout_like_any_dead_token(self, timed_real, zfs):
        app = timed_real
        app.clock.now = app.t0 + 500
        statuses = [_enroll(app, HOST, make_client_cert(), app.token_text).status_code
                    for _ in range(7)]
        assert statuses[:5] == [403] * 5 and 429 in statuses

    def test_a_live_token_is_still_the_409_and_stays_unspent(self, mocker, timed_real, zfs):
        app = timed_real
        spy = mocker.spy(app.gateway, "enroll_client")
        key = app.trust.trust_key()
        app.clock.now = app.t0 + 10
        resp = _enroll(app, HOST, make_client_cert(), app.token_text)
        assert resp.status_code == 409 and resp.get_json()["reason"] == RESERVED_CLIENT_REASON
        assert token_state_at(app, app.t0 + 10) is TokenState.ACTIVE
        assert app.trust.token_state() is TokenState.ACTIVE
        spy.assert_not_called()
        _untouched(zfs)
        assert app.trust.pins() == {} and app.trust.clients() == {}
        assert app.trust.trust_key() == key
        assert app.limits.enroll.retry_after("127.0.0.1") == 0

    def test_a_live_token_is_never_a_lockout_count_and_still_enrolls_another_id(self, timed_real, zfs):
        app = timed_real
        for _ in range(10):
            assert _enroll(app, HOST, make_client_cert(), app.token_text).status_code == 409
        cert = make_client_cert()
        resp = _enroll(app, OTHER, cert, app.token_text)
        assert resp.status_code == 200, resp.get_data(as_text=True)
        assert cert.fingerprint in app.trust.pins()

    def test_the_confirmation_is_asked_once_after_the_pre_check(self, mocker, timed_real, zfs):
        app = timed_real
        order = []
        pre, conf = app.gateway.token_precheck, app.gateway.confirm_token_live
        mocker.patch.object(app.gateway, "token_precheck",
                            side_effect=lambda **k: order.append("precheck") or pre(**k))
        mocker.patch.object(app.gateway, "confirm_token_live",
                            side_effect=lambda **k: order.append("confirm") or conf(**k))
        _enroll(app, HOST, make_client_cert(), app.token_text)
        assert order == ["precheck", "confirm"]

    def test_a_revoked_certificate_is_the_uniform_403_and_the_token_is_untouched(self, mocker, timed_real, zfs):
        app = timed_real
        cert = make_client_cert()
        app.trust.enroll(cert, OTHER)
        store_trust.revoke_pin(app.trust.resolved, cert.fingerprint, lock=None)
        token = app.trust.token(created_at=app.t0, ttl=100)
        conf = mocker.spy(app.gateway, "confirm_token_live")
        resp = _enroll(app, HOST, cert, token)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR
        conf.assert_not_called()                           # the pre-check already said no
        assert token_state_at(app, app.t0 + 10) is TokenState.ACTIVE
        _untouched(zfs)

    def test_a_revocation_the_pre_check_missed_is_still_403_and_does_not_latch(self, mocker, timed_real, zfs):
        """The confirmation re-checks the certificate under the write lock."""
        app = timed_real
        cert = make_client_cert()
        app.trust.enroll(cert, OTHER)
        store_trust.revoke_pin(app.trust.resolved, cert.fingerprint, lock=None)
        token = app.trust.token(created_at=app.t0, ttl=100)
        mocker.patch.object(app.gateway, "token_precheck", return_value=True)
        conf = mocker.spy(app.gateway, "confirm_token_live")
        resp = _enroll(app, HOST, cert, token)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR
        conf.assert_called_once()
        assert token_state_at(app, app.t0 + 10) is TokenState.ACTIVE
        _untouched(zfs)

    def test_a_live_token_scoped_to_another_id_is_the_uniform_403_and_stays_unspent(self, timed_real, zfs):
        app = timed_real
        app.trust.enroll(make_client_cert(), OTHER)
        token = app.trust.token(client_id=OTHER, created_at=app.t0, ttl=100)
        key = app.trust.trust_key()
        resp = _enroll(app, HOST, make_client_cert(), token)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR
        assert RESERVED_CLIENT_REASON not in resp.get_data(as_text=True)
        assert token_state_at(app, app.t0 + 10) is TokenState.ACTIVE      # a scope refusal never latches
        assert app.trust.trust_key() == key
        _untouched(zfs)

    def test_a_live_token_scoped_to_the_reserved_id_is_the_409(self, timed_real, zfs):
        app = timed_real
        app.trust.enroll(make_client_cert(), HOST)
        token = app.trust.token(client_id=HOST, created_at=app.t0, ttl=100)
        resp = _enroll(app, HOST, make_client_cert(), token)
        assert resp.status_code == 409 and resp.get_json()["reason"] == RESERVED_CLIENT_REASON
        assert token_state_at(app, app.t0 + 10) is TokenState.ACTIVE
        _untouched(zfs)

    def test_another_ids_expired_token_is_refused_by_enroll_client_as_before(self, mocker, timed_real, zfs):
        app = timed_real
        conf = mocker.spy(app.gateway, "confirm_token_live")
        app.clock.now = app.t0 + 500
        resp = _enroll(app, OTHER, make_client_cert(), app.token_text)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR
        conf.assert_not_called()
        assert token_state_at(app, app.t0 + 10) is not TokenState.ACTIVE

    def test_without_a_reservation_the_host_id_and_an_expired_token_never_ask(self, mocker, tls_config_db, zfs):
        clock = _Clock(time.time())
        app = _real_app(mocker, tls_config_db, clock=clock)
        token = app.trust.token(created_at=clock.now, ttl=100)
        conf = mocker.spy(app.gateway, "confirm_token_live")
        clock.now += 500
        assert _enroll(app, HOST, make_client_cert(), token).status_code == 403
        conf.assert_not_called()


@pytest.mark.integration
class TestReservedClientIdEnrollConfirmWiring:
    """`/enroll`'s call of `trust.confirm_token_live`, with a scripted gateway."""

    @pytest.fixture
    def authed(self, mocker, zfs):
        self.clock = _Clock(time.time())
        return _authed(mocker, client_id=None, reserved_client_id=HOST, clock=self.clock)

    @staticmethod
    def enroll(authed, cid=HOST):
        token = str(identity.make_token(authed.server_fp)[0])
        cert = make_client_cert()
        return _enroll(authed, cid, cert, token), cert, token

    def test_a_live_token_is_the_409_with_no_zfs_and_no_enroll_call(self, authed, zfs):
        resp, _, _ = self.enroll(authed)
        assert resp.status_code == 409 and resp.get_json()["reason"] == RESERVED_CLIENT_REASON
        assert len(authed.gateway.confirm_calls) == 1
        assert authed.gateway.enroll_calls == []
        _untouched(zfs)

    def test_a_dead_token_is_the_uniform_403(self, authed, zfs):
        authed.gateway.confirm_effect = False
        resp, _, _ = self.enroll(authed)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR
        assert RESERVED_CLIENT_REASON not in resp.get_data(as_text=True)
        assert authed.gateway.enroll_calls == []
        _untouched(zfs)

    def test_a_dead_token_counts_towards_the_lockout(self, authed):
        authed.gateway.confirm_effect = False
        statuses = [self.enroll(authed)[0].status_code for _ in range(6)]
        assert statuses == [403] * 5 + [429]

    def test_four_dead_tokens_are_not_yet_a_lockout(self, authed):
        authed.gateway.confirm_effect = False
        assert [self.enroll(authed)[0].status_code for _ in range(4)] == [403] * 4
        assert authed.limits.enroll.retry_after("127.0.0.1") == 0

    def test_a_dead_token_is_logged_as_a_token_or_revoked_certificate_refusal(self, authed, caplog):
        authed.gateway.confirm_effect = False
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp, _, token = self.enroll(authed)
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1 and warnings[0].endswith("refused: token or revoked certificate")
        assert token not in caplog.text and token.split(".")[-1] not in caplog.text

    def test_a_live_token_is_not_a_lockout_count(self, authed):
        for _ in range(10):
            assert self.enroll(authed)[0].status_code == 409
        assert authed.limits.enroll.retry_after("127.0.0.1") == 0

    def test_the_order_is_precheck_then_confirm_then_the_409_and_no_zfs(self, authed, zfs):
        order = []
        authed.gateway.precheck_effect = lambda **k: order.append("precheck") or True
        authed.gateway.confirm_effect = lambda **k: order.append("confirm") or True
        zfs.exists.side_effect = lambda *a: order.append("zfs") or False
        authed.gateway.enroll_effect = lambda **k: order.append("store") or EnrollOutcome.ENROLLED
        assert self.enroll(authed)[0].status_code == 409
        assert order == ["precheck", "confirm"]

    def test_a_refusing_pre_check_means_confirm_is_never_asked(self, authed):
        authed.gateway.precheck_effect = False
        assert self.enroll(authed)[0].status_code == 403
        assert authed.gateway.confirm_calls == []

    def test_it_is_called_with_the_hash_the_server_the_id_the_clock_and_the_certificate(self, authed):
        resp, cert, token = self.enroll(authed)
        (call,) = authed.gateway.confirm_calls
        assert call == {
            "secret_sha256": identity.secret_hash(identity.parse_token(token)[1]),
            "server_fingerprint": authed.server_fp,
            "client_id": HOST,
            "now": self.clock,
            "fingerprint": cert.fingerprint,
        }
        assert token.split(".")[-1] not in repr(call)
        (pre,) = authed.gateway.precheck_calls
        assert pre == {k: v for k, v in call.items() if k != "client_id"}   # the same question, scoped

    def test_a_busy_confirmation_is_503_with_retry_and_not_a_failure(self, authed, zfs, caplog):
        authed.gateway.confirm_effect = ConfigBusy("database is locked")
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            for _ in range(10):
                resp, _, _ = self.enroll(authed)
                assert resp.status_code == 503 and resp.get_json() == TRUST_BUSY_ERROR
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert authed.limits.enroll.retry_after("127.0.0.1") == 0
        assert authed.gateway.enroll_calls == [] and authed.gate.active == 0
        _untouched(zfs)

    @pytest.mark.parametrize("error", [RuntimeError("no such table: enrollment_tokens"), OSError("disk I/O error")])
    def test_any_other_confirmation_failure_is_503_unavailable_and_leaks_nothing(
        self, authed, zfs, caplog, error
    ):
        authed.gateway.confirm_effect = error
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp, _, token = self.enroll(authed)
        assert resp.status_code == 503 and resp.get_json() == TRUST_UNAVAILABLE_ERROR
        assert str(error) not in resp.get_data(as_text=True)
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1 and str(error) in errors[0].getMessage()
        assert token not in caplog.text and token.split(".")[-1] not in caplog.text
        assert authed.limits.enroll.retry_after("127.0.0.1") == 0
        assert authed.gateway.enroll_calls == []
        _untouched(zfs)

    def test_the_in_flight_slot_is_released_after_every_answer(self, authed):
        for effect in (True, ConfigBusy("busy"), RuntimeError("down")) * 2:
            authed.gateway.confirm_effect = effect
            assert self.enroll(authed)[0].status_code in (409, 503)
        authed.gateway.confirm_effect = True
        assert self.enroll(authed, OTHER)[0].status_code == 200

    def test_a_non_reserved_id_never_asks(self, authed, zfs):
        resp, _, _ = self.enroll(authed, OTHER)
        assert resp.status_code == 200
        assert authed.gateway.confirm_calls == []
        assert len(authed.gateway.precheck_calls) == 1 and len(authed.gateway.enroll_calls) == 1

    @pytest.mark.parametrize("reserved", [None, "host-2"])
    def test_without_a_matching_reservation_the_id_never_asks(self, mocker, zfs, reserved):
        authed = _authed(mocker, client_id=None, reserved_client_id=reserved)
        resp, _, _ = self.enroll(authed, HOST)
        assert resp.status_code == 200
        assert authed.gateway.confirm_calls == []

    def test_a_dead_token_for_a_non_reserved_id_is_decided_by_enroll_client(self, authed):
        authed.gateway.confirm_effect = False          # must not matter
        authed.gateway.enroll_effect = EnrollOutcome.REFUSED
        resp, _, _ = self.enroll(authed, OTHER)
        assert resp.status_code == 403 and resp.get_json() == ENROLL_REFUSED_ERROR
        assert authed.gateway.confirm_calls == []


# ---------------------------------------------------------------------------
# The log line
# ---------------------------------------------------------------------------


def reserved_records(caplog):
    return [r for r in caplog.records if r.name == PEER_LOGGER and "reserves client id" in r.getMessage()]


@pytest.mark.integration
class TestReservedClientIdLog:
    def test_one_warning_per_episode_then_debug_and_it_names_the_command(self, mocker, zfs, caplog):
        authed = reserved_app(mocker)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for template in (NEGOTIATE, RESUME, NEGOTIATE, RESUME):
                method = "post" if template is NEGOTIATE else "get"
                call(authed.client(), method, template, HOST)
        levels = [r.levelname for r in reserved_records(caplog)]
        assert levels == ["WARNING", "DEBUG", "DEBUG", "DEBUG"]
        (first,) = [r for r in reserved_records(caplog) if r.levelname == "WARNING"]
        text = first.getMessage()
        assert f"zfsbackup-config trust revoke --client {HOST}" in text
        assert TARGET in text and HOST in text

    def test_it_is_the_peer_logger(self, mocker, zfs, caplog):
        authed = reserved_app(mocker)
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            call(authed.client(), "get", RESUME, HOST)
        assert reserved_records(caplog)

    def test_a_new_hour_warns_again(self, mocker, zfs, caplog):
        now = [1000.0]
        from zfsbackup.api import PeerLimits

        authed = _authed(
            mocker, _shared_server_config(), client_id=HOST, reserved_client_id=HOST,
            peer_limits=PeerLimits(clock=lambda: now[0]),
        )
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            call(authed.client(), "get", RESUME, HOST)
            now[0] += 3601
            call(authed.client(), "get", RESUME, HOST)
        assert [r.levelname for r in reserved_records(caplog)] == ["WARNING", "WARNING"]

    def test_the_enroll_refusal_shares_the_episode_and_names_the_command(self, reserved_real, zfs, caplog):
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            _enroll(reserved_real, HOST, make_client_cert(), reserved_real.trust.token())
        (rec,) = reserved_records(caplog)
        assert rec.levelname == "WARNING"
        assert f"trust revoke --client {HOST}" in rec.getMessage()

    def test_no_token_or_secret_is_logged(self, reserved_real, zfs, caplog):
        token = reserved_real.trust.token()
        with caplog.at_level(logging.DEBUG):
            _enroll(reserved_real, HOST, make_client_cert(), token)
        assert token not in caplog.text and token.rsplit(".", 1)[1] not in caplog.text


# ---------------------------------------------------------------------------
# ApiWorker and the daemon
# ---------------------------------------------------------------------------


@pytest.fixture
def served(mocker):
    """Run `ApiWorker._serve` as far as `create_app` and stop there: -> `serve(config, client_id)`,
    with the `create_app` mock as `serve.create`."""
    create = mocker.patch("zfsbackup.api.create_app", side_effect=RuntimeError("stop after create_app"))

    def serve(config, client_id):
        worker = ApiWorker(config, multiprocessing.Event(), client_id=client_id)
        worker._trust = MagicMock(name="gateway")
        server_identity = SimpleNamespace(fingerprint="ab" * 32)
        with pytest.raises(RuntimeError, match="stop after create_app"):
            worker._serve(config, logging.getLogger("test"), create, server_identity)
        return worker

    serve.create = create
    return serve


@pytest.mark.integration
class TestApiWorkerPassesTheReservation:
    def test_the_id_is_passed_when_the_target_is_shared(self, served):
        config = shared_config()
        worker = served(config, HOST)
        args, kwargs = served.create.call_args
        assert args[0] is config and args[1] is worker._trust
        assert kwargs == {"reserved_client_id": HOST}

    def test_none_is_passed_when_nothing_is_shared(self, served):
        served(shared_config(local="other/backups"), HOST)
        assert served.create.call_args.kwargs == {"reserved_client_id": None}

    def test_none_is_passed_for_a_disabled_remote_backup(self, served):
        served(shared_config(remote_enabled=False), HOST)
        assert served.create.call_args.kwargs == {"reserved_client_id": None}

    def test_none_is_passed_without_a_local_destination(self, served):
        config = shared_config()
        config.destinations = {}
        served(config, HOST)
        assert served.create.call_args.kwargs == {"reserved_client_id": None}

    def test_a_shared_target_without_an_id_reserves_nothing(self, served):
        """No client id yet: there is nothing to reserve."""
        served(shared_config(), None)
        assert served.create.call_args.kwargs == {"reserved_client_id": None}


def _daemon(sqlite_config_db, config, client_id):
    return BackupDaemon(config, sqlite_config_db, client_id=client_id)


@pytest.mark.integration
class TestDaemonHandsOverTheClientId:
    def test_the_api_worker_gets_the_id_when_shared(self, mocker, sqlite_config_db):
        factory = mocker.patch("zfsbackup.daemon.ApiWorker")
        _daemon(sqlite_config_db, shared_config(), HOST)._new_worker("api")
        assert factory.call_args.kwargs["client_id"] == HOST
        assert factory.call_args.kwargs["config_db"] is not None

    def test_the_api_worker_gets_no_id_when_not_shared(self, mocker, sqlite_config_db):
        factory = mocker.patch("zfsbackup.daemon.ApiWorker")
        _daemon(sqlite_config_db, shared_config(local="other/backups"), HOST)._new_worker("api")
        assert "client_id" not in factory.call_args.kwargs

    def test_the_api_worker_gets_no_id_without_a_local_destination(self, mocker, sqlite_config_db):
        factory = mocker.patch("zfsbackup.daemon.ApiWorker")
        config = shared_config()
        config.destinations = {}
        _daemon(sqlite_config_db, config, HOST)._new_worker("api")
        assert "client_id" not in factory.call_args.kwargs

    def test_the_handoffs_id_not_the_constructors_is_used(self, mocker, sqlite_config_db):
        import dataclasses

        factory = mocker.patch("zfsbackup.daemon.ApiWorker")
        daemon = _daemon(sqlite_config_db, shared_config(), HOST)
        daemon._handoff = dataclasses.replace(daemon._handoff, client_id="host-2")
        daemon._new_worker("api")
        assert factory.call_args.kwargs["client_id"] == "host-2"

    def test_the_other_workers_are_unchanged(self, mocker, sqlite_config_db):
        snap = mocker.patch("zfsbackup.daemon.SnapshotWorker")
        remote = mocker.patch("zfsbackup.daemon.RemoteBackupWorker")
        prune = mocker.patch("zfsbackup.daemon.PruningWorker")
        daemon = _daemon(sqlite_config_db, shared_config(), HOST)
        for name in ("snapshot", "remote", "pruning"):
            daemon._new_worker(name)
        assert "client_id" not in snap.call_args.kwargs
        assert remote.call_args.kwargs["client_id"] == HOST
        assert prune.call_args.kwargs["client_id"] == HOST

    def test_a_shared_config_still_builds_a_real_api_worker_with_the_id(self, sqlite_config_db):
        worker = _daemon(sqlite_config_db, shared_config(), HOST)._new_worker("api")
        assert isinstance(worker, ApiWorker) and worker.client_id == HOST

    def test_an_unshared_config_builds_a_real_api_worker_without_one(self, sqlite_config_db):
        worker = _daemon(sqlite_config_db, shared_config(local="other/backups"), HOST)._new_worker("api")
        assert isinstance(worker, ApiWorker) and worker.client_id is None


_START_METHODS = [m for m in ("fork", "spawn", "forkserver") if m in multiprocessing.get_all_start_methods()]


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestApiWorkerChildren:
    """The handed-over id crosses a real process boundary (pickled under spawn/forkserver)."""

    @pytest.mark.parametrize("method", _START_METHODS)
    def test_a_real_child_is_born_with_the_reserved_id(self, sqlite_config_db, tmp_path, mocker, method):
        from test_zfsbackup_daemon import _bind_recording

        records = tmp_path / "rec"
        records.mkdir()
        mocker.patch(
            "zfsbackup.daemon.ApiWorker", side_effect=_bind_recording(RecordingApiWorker, method, records)
        )
        daemon = _daemon(sqlite_config_db, shared_config(), HOST)
        worker = daemon._spawn("api")
        worker.join(120)
        assert worker.exitcode == 0, f"{method} child failed"
        (record,) = read_records(records)
        assert record["client_id"] == HOST


# ---------------------------------------------------------------------------
# A real ApiWorker process, over real TLS
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestRealApiWorkerRefusesTheReservedId:
    """`ApiWorker(client_id=)` -> `_serve` -> `create_app(reserved_client_id=)` in a real child (built
    directly, as `_new_worker` would; the hand-over itself is `TestDaemonHandsOverTheClientId`). The `zfs`
    calls are patched in the child only; the client is spoken to over TLS with real certificates pinned
    through the real trust store."""

    def rig(self, resolved, method, tmp_path, *, shared):
        from _session_support import ProcessRig, SessionApiWorker

        rig = ProcessRig(resolved, method, tmp_path)
        config = shared_config(local=None if shared else "other/backups")
        config.api_host, config.api_port = "127.0.0.1", rig.port
        rig.worker = SessionApiWorker(config, rig.stop, config_db=resolved, client_id=HOST)
        rig.worker.start_method = method
        rig.worker.record_path = str(rig.record)
        return rig

    def statuses(self, rig, host_ident, other_ident):
        from _tls_support import insecure_client_context, try_get

        def get(path, ident):
            result = try_get(rig.port, path, insecure_client_context(ident))
            assert isinstance(result, tuple), result
            return result

        return {
            "host": get(f"/backup/{HOST}/pool/data/resume_token", host_ident),
            "other": get(f"/backup/{OTHER}/pool/data/resume_token", other_ident),
            "whoami": get("/whoami", host_ident),
        }

    @pytest.mark.parametrize("method", _START_METHODS)
    def test_a_shared_target_refuses_the_hosts_own_id_and_serves_the_others(
        self, tls_config_db, tmp_path, method
    ):
        import json

        from _tls_support import client_identity, pin_client

        host_ident = client_identity(tmp_path / "host.tls")
        other_ident = client_identity(tmp_path / "other.tls")
        pin_client(tls_config_db, host_ident, HOST)
        pin_client(tls_config_db, other_ident, OTHER)
        with self.rig(tls_config_db, method, tmp_path, shared=True) as rig:
            rig.ready()
            got = self.statuses(rig, host_ident, other_ident)
        status, body = got["host"]
        assert status == 409 and json.loads(body)["reason"] == RESERVED_CLIENT_REASON
        assert got["other"][0] != 409
        assert got["whoami"][0] == 200 and json.loads(got["whoami"][1])["client_id"] == HOST

    def test_without_a_shared_target_the_same_id_is_served(self, tls_config_db, tmp_path):
        import json

        from _tls_support import client_identity, pin_client

        host_ident = client_identity(tmp_path / "host.tls")
        other_ident = client_identity(tmp_path / "other.tls")
        pin_client(tls_config_db, host_ident, HOST)
        pin_client(tls_config_db, other_ident, OTHER)
        with self.rig(tls_config_db, _START_METHODS[0], tmp_path, shared=False) as rig:
            rig.ready()
            got = self.statuses(rig, host_ident, other_ident)
        assert got["host"][0] != 409
        assert got["whoami"][0] == 200

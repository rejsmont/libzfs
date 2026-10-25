"""Milestone D, plan B-2c: the server never adopts a dataset it did not make.

Every dataset a negotiate or a receive of this server creates carries a LOCAL
`org.zfsbackup:client_id` from the start (`zfs create -o` for the containers, `zfs receive -o` for
the leaf). A dataset on the server's backup path that has none -- an operator's own `<target>/<id>`,
a dataset moved in, a received-only copy of the property -- was not made for a backup: a full `-F`
receive would overwrite it and the pruner would take over its snapshots, so negotiate, `/stream`
and `/resume_token` answer 409 with the one command that adopts it on purpose
(`zfs set org.zfsbackup:client_id=<id> <dataset>`), one WARNING per dataset. Another client's id is
a 409 too, naming nobody.

Negotiate creates the missing ancestors outermost first, ONE level at a time, with
`zfs create -u -o canmount=noauto -o org.zfsbackup:client_id=<id>` (no `-p`); `<target>` itself must
exist and is never created or even asked about.

Layout: `TestUnownedIsRefused` (negotiate), `TestOwnerReadIsLocal`, `TestUnownedWarning`,
`TestStreamAndResumeToken`, `TestAncestorCreation` (the real argv, against the M8 captures),
`TestCreateFailure`.
"""

import logging

import pytest

from _api_support import Authed, _find_in_closures, pending_configs, streams_in_progress
from _openzfs_fixtures import VERSIONS, load_section
from _stream_support import HEADER
from test_zfsbackup_api import Stream, source_props, stored_config  # noqa: F401  (Stream: harness; rest: fixtures)
from test_zfsbackup_api_negotiate import (  # noqa: F401  (fixtures and helpers)
    CLIENT_ID, CREATE_KWARGS, PARENT_DS, REAL_GET_VALUE, SERVER_DS, ancestor_negotiate, ancestors, authed,
    client, existing, negotiate, zfs, _config, assert_nothing_written, enc, good, GOOD,
)
from zfsbackup import api as _api
from zfsbackup.backup_manager import PROP_CLIENT_ID

#: The real `zfs.create`, captured before any test patches it (`CREATE` puts it back as a mock's
#: `side_effect`, so the real argv is built and only `Popen` is mocked).
REAL_CREATE = _api.zfs.create
HINT = f"zfs set {PROP_CLIENT_ID}={CLIENT_ID} "
LEAF_HINT = HINT + SERVER_DS


def hint_for(dataset, client_id=CLIENT_ID):
    import shlex
    return f"zfs set {PROP_CLIENT_ID}={client_id} {shlex.quote(dataset)}"


def warnings_of(caplog):
    return [r.getMessage() for r in caplog.records
            if r.name == "zfsbackup.api" and r.levelno == logging.WARNING]


# ---------------------------------------------------------------------------
# Negotiate: the 409 matrix
# ---------------------------------------------------------------------------


class TestUnownedIsRefused:
    def refused(self, resp, dataset, *, client_id=CLIENT_ID):
        assert resp.status_code == 409, resp.get_data(as_text=True)
        error = resp.get_json()["error"]
        assert hint_for(dataset, client_id) in error
        assert f"server dataset {dataset} already exists" in error and "refusing to adopt it" in error
        return error

    def test_an_existing_leaf_without_a_local_client_id(self, client, authed, zfs, ancestors):
        existing(zfs, SERVER_DS)
        ancestors.owner[SERVER_DS] = None
        error = self.refused(negotiate(client), SERVER_DS)
        assert LEAF_HINT in error
        assert_nothing_written(zfs)                           # no set (the owner is not stamped), no create
        assert dict(pending_configs(authed.app)) == {}
        assert streams_in_progress(authed.app) == set() and authed.gate.active == 0

    def test_an_unowned_id_directory_blocks_the_new_leaf_below_it(self, client, authed, zfs, ancestors):
        existing(zfs, PARENT_DS)
        ancestors.owner[PARENT_DS] = None
        self.refused(ancestor_negotiate(client, "pool/a"), PARENT_DS)
        assert_nothing_written(zfs)
        assert dict(pending_configs(authed.app)) == {}
        assert streams_in_progress(authed.app) == set()

    def test_an_unowned_intermediate_dataset(self, client, zfs, ancestors):
        existing(zfs, PARENT_DS, PARENT_DS + "/a")
        ancestors.owner[PARENT_DS + "/a"] = None
        self.refused(ancestor_negotiate(client, "pool/a/b"), PARENT_DS + "/a")
        assert_nothing_written(zfs)

    def test_the_nearest_unowned_ancestor_is_the_one_named(self, client, zfs, ancestors):
        existing(zfs, PARENT_DS, PARENT_DS + "/a", PARENT_DS + "/a/b")
        ancestors.owner[PARENT_DS] = None
        ancestors.owner[PARENT_DS + "/a/b"] = None
        self.refused(ancestor_negotiate(client, "pool/a/b/c"), PARENT_DS + "/a/b")

    def test_an_unowned_ancestor_above_missing_ones_creates_nothing(self, client, zfs, ancestors):
        """`<target>/<id>` is missing but `a` exists unowned: nothing is built around it."""
        existing(zfs, PARENT_DS + "/a")
        ancestors.owner[PARENT_DS + "/a"] = None
        self.refused(ancestor_negotiate(client, "pool/a/b/c"), PARENT_DS + "/a")
        zfs["create"].assert_not_called()

    def test_another_clients_ancestor_is_a_409_that_names_nobody(self, client, zfs, ancestors):
        existing(zfs, PARENT_DS)
        ancestors.owner[PARENT_DS] = "bob-7"
        resp = ancestor_negotiate(client, "pool/a")
        assert resp.status_code == 409
        text = resp.get_data(as_text=True)
        assert "another client's" in text and PROP_CLIENT_ID in text
        assert "bob-7" not in text and "zfs set" not in text
        assert_nothing_written(zfs)

    def test_another_clients_leaf_is_a_409_that_names_nobody(self, client, zfs, ancestors):
        existing(zfs, SERVER_DS)
        ancestors.owner[SERVER_DS] = "bob-7"
        resp = negotiate(client)
        assert resp.status_code == 409 and "bob-7" not in resp.get_data(as_text=True)
        assert_nothing_written(zfs)

    def test_owned_containers_pass(self, client, zfs, ancestors):
        existing(zfs, PARENT_DS, PARENT_DS + "/a")             # both carry the bound id
        assert ancestor_negotiate(client, "pool/a/b").status_code == 200
        zfs["create"].assert_not_called()

    def test_an_owned_leaf_passes_and_is_stamped_as_before(self, client, zfs, ancestors):
        existing(zfs, SERVER_DS)
        assert negotiate(client).status_code == 200
        (call,) = zfs["set"].call_args_list
        assert call.args[1][PROP_CLIENT_ID] == CLIENT_ID

    def test_a_missing_dataset_is_not_asked_for_an_owner(self, client, zfs, ancestors):
        existing(zfs)
        assert negotiate(client).status_code == 200
        assert ancestors.reads == []

    def test_the_owner_is_checked_before_the_recorded_source_dataset(self, client, zfs, ancestors):
        """An unowned dataset that also records another source is refused as unowned (the same 409)."""
        existing(zfs, SERVER_DS)
        ancestors.owner[SERVER_DS] = None
        zfs["props"].return_value = {"org.zfsbackup:source_dataset": "tank/other"}
        self.refused(negotiate(client), SERVER_DS)

    def test_a_dry_run_refuses_an_unowned_existing_dataset_too(self, zfs, ancestors):
        dry = Authed(_config(dry_run=True))
        existing(zfs, SERVER_DS)
        ancestors.owner[SERVER_DS] = None
        self.refused(negotiate(dry.client()), SERVER_DS)
        assert_nothing_written(zfs)

    def test_a_conflict_is_reported_before_a_partial_receive_state_would_block(self, client, zfs, ancestors):
        """An unowned ancestor with a cut first receive: 409 (not the retryable 503), because a
        child must never be built under it whatever its state."""
        existing(zfs, PARENT_DS, PARENT_DS + "/a")
        ancestors.owner[PARENT_DS + "/a"] = None
        ancestors.token[PARENT_DS + "/a"] = "1-token"
        zfs["list"].return_value = []
        resp = ancestor_negotiate(client, "pool/a/b")
        assert resp.status_code == 409 and "retry" not in resp.get_json()

    def test_the_nearest_ancestor_decides_between_unowned_and_unsettled(self, client, authed, zfs, ancestors):
        existing(zfs, PARENT_DS, PARENT_DS + "/a")
        ancestors.owner[PARENT_DS + "/a"] = None
        streams_in_progress(authed.app).add(PARENT_DS)             # a farther ancestor is claimed
        assert ancestor_negotiate(client, "pool/a/b").status_code == 409
        existing(zfs, PARENT_DS + "/a")
        ancestors.owner[PARENT_DS + "/a"] = CLIENT_ID
        streams_in_progress(authed.app).discard(PARENT_DS)
        streams_in_progress(authed.app).add(PARENT_DS + "/a")      # the nearest is claimed
        assert ancestor_negotiate(client, "pool/a/b").status_code == 503

    def test_the_409_is_not_retryable(self, client, zfs, ancestors):
        existing(zfs, SERVER_DS)
        ancestors.owner[SERVER_DS] = None
        assert set(negotiate(client).get_json()) == {"error"}


class TestOwnerReadIsLocal:
    """The owner is read with `zfs get -s local`: a value inherited from a parent or only RECEIVED
    (a stream's own copy of the property) is not the server's record."""

    def test_the_leaf_is_read_locally(self, client, zfs, ancestors):
        existing(zfs, SERVER_DS)
        assert negotiate(client).status_code == 200
        calls = [c for c in ancestors.mock.call_args_list if c.args[1] == PROP_CLIENT_ID]
        assert [(str(c.args[0]), c.kwargs) for c in calls] == [(SERVER_DS, {"sources": "local"})]

    def test_every_ancestor_is_read_locally(self, client, zfs, ancestors):
        existing(zfs, PARENT_DS, PARENT_DS + "/a")
        assert ancestor_negotiate(client, "pool/a/b").status_code == 200
        calls = [c for c in ancestors.mock.call_args_list if c.args[1] == PROP_CLIENT_ID]
        assert [str(c.args[0]) for c in calls] == [PARENT_DS + "/a", PARENT_DS]
        assert all(c.kwargs == {"sources": "local"} for c in calls)

    def test_a_received_only_client_id_is_unowned(self, client, zfs, ancestors):
        """`read_zfsbackup_properties` (local AND received) shows the id, the local read does not."""
        existing(zfs, SERVER_DS)
        ancestors.owner[SERVER_DS] = None
        zfs["props"].return_value = {PROP_CLIENT_ID: CLIENT_ID}
        resp = negotiate(client)
        assert resp.status_code == 409 and LEAF_HINT in resp.get_json()["error"]
        assert_nothing_written(zfs)

    def test_a_received_only_client_id_of_another_client_is_refused_as_theirs(self, client, zfs, ancestors):
        existing(zfs, SERVER_DS)
        ancestors.owner[SERVER_DS] = None
        zfs["props"].return_value = {PROP_CLIENT_ID: "bob-7"}
        resp = negotiate(client)
        assert resp.status_code == 409 and "bob-7" not in resp.get_data(as_text=True)

    def test_through_the_real_get_command_the_argv_asks_for_the_local_source(self, client, zfs, mock_subprocess):
        existing(zfs, SERVER_DS)
        zfs["get_value"].side_effect = REAL_GET_VALUE
        mock_subprocess.setup_multi((["-\n"],))                  # `-`: not set locally
        resp = negotiate(client)
        assert resp.status_code == 409
        argv = mock_subprocess.call_args_list[0][0][0][1:]
        assert argv[:3] == ["get", "-H", "-o"] and "local" in argv and PROP_CLIENT_ID in argv
        assert argv[-1] == SERVER_DS

    def test_a_failing_owner_read_is_a_500_that_writes_nothing(self, client, zfs, ancestors):
        existing(zfs, SERVER_DS)
        ancestors.mock.side_effect = RuntimeError("zfs get exploded for tank/secret")
        resp = negotiate(client)
        assert resp.status_code == 500                         # refused rather than assumed owned
        assert_nothing_written(zfs)

    def test_a_failing_ancestor_owner_read_is_the_parents_500(self, client, authed, zfs, ancestors):
        existing(zfs, PARENT_DS)
        ancestors.mock.side_effect = RuntimeError("zfs get exploded for tank/secret")
        resp = ancestor_negotiate(client, "pool/a")
        assert resp.status_code == 500
        assert resp.get_json() == {"error": f"Failed to check the parents of {PARENT_DS}/a"}
        assert_nothing_written(zfs)
        assert streams_in_progress(authed.app) == set()


# ---------------------------------------------------------------------------
# One WARNING per dataset
# ---------------------------------------------------------------------------


class TestUnownedWarning:
    def test_the_first_refusal_warns_with_the_hint_the_next_ones_do_not(self, client, zfs, ancestors, caplog):
        existing(zfs, SERVER_DS)
        ancestors.owner[SERVER_DS] = None
        with caplog.at_level(logging.DEBUG, logger="zfsbackup.api"):
            for _ in range(3):
                assert negotiate(client).status_code == 409
        (warning,) = warnings_of(caplog)
        assert SERVER_DS in warning and PROP_CLIENT_ID in warning and LEAF_HINT in warning
        assert "refusing to use" in warning

    def test_each_dataset_warns_once(self, client, zfs, ancestors, caplog):
        existing(zfs, PARENT_DS)
        ancestors.owner[PARENT_DS] = None
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            ancestor_negotiate(client, "pool/a")
            existing(zfs, PARENT_DS, PARENT_DS + "/a")
            ancestors.owner[PARENT_DS] = CLIENT_ID
            ancestors.owner[PARENT_DS + "/a"] = None
            ancestor_negotiate(client, "pool/a/b")
            ancestor_negotiate(client, "pool/a/b")
        warnings = warnings_of(caplog)
        assert len(warnings) == 2
        assert hint_for(PARENT_DS) in warnings[0] and hint_for(PARENT_DS + "/a") in warnings[1]

    def test_adopting_the_dataset_rearms_the_warning(self, client, zfs, ancestors, caplog):
        existing(zfs, SERVER_DS)
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            ancestors.owner[SERVER_DS] = None
            negotiate(client)
            ancestors.owner[SERVER_DS] = CLIENT_ID           # the operator ran the hint
            assert negotiate(client).status_code == 200
            ancestors.owner[SERVER_DS] = None                # a later episode
            negotiate(client)
        assert len(warnings_of(caplog)) == 2

    def test_a_name_with_a_space_is_quoted_in_the_hint_and_the_log(self, client, zfs, ancestors, caplog):
        name = "pool/backups/client1/my data"
        existing(zfs, name)
        ancestors.owner[name] = None
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            resp = negotiate(client, dataset="pool/my data")
        assert hint_for(name) in resp.get_json()["error"]
        assert f"'{name}'" in resp.get_json()["error"]
        assert f"'{name}'" in warnings_of(caplog)[0]

    def test_the_warning_set_is_bounded(self, client, authed, zfs, ancestors, caplog):
        noted = _find_in_closures(authed.app, "_unowned_noted")
        zfs["exists"].return_value = True
        ancestors.owner.clear()
        zfs["props"].return_value = {}
        zfs["exists"].side_effect = None
        for i in range(1100):
            name = f"pool/backups/client1/d{i}"
            ancestors.owner[name] = None
            assert negotiate(client, dataset=f"pool/d{i}").status_code == 409
        assert len(noted) <= 1025

    def test_another_clients_dataset_is_not_a_warning_of_this_kind(self, client, zfs, ancestors, caplog):
        existing(zfs, SERVER_DS)
        ancestors.owner[SERVER_DS] = "bob-7"
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            negotiate(client)
        assert not [w for w in warnings_of(caplog) if "zfs set" in w]


# ---------------------------------------------------------------------------
# /stream and /resume_token
# ---------------------------------------------------------------------------


class TestStreamAndResumeToken:
    @pytest.fixture
    def stream(self, mocker):
        return Stream(mocker)

    def test_a_stream_into_an_unowned_dataset_is_refused_before_anything_runs(self, stream):
        stream.owner = None
        stream.run("resume")
        (reply,) = stream.sent
        assert set(reply) == {"error"} and LEAF_HINT in reply["error"]
        for m in (stream.receive, stream.abort, stream.create, stream.set):
            m.assert_not_called()
        stream.ws.receive.assert_not_called()                 # no byte of the stream was read
        assert stream.config_reads == 0
        assert stream.gate.active == 0 and stream.claimed == set()

    def test_a_fresh_stream_discards_nothing_of_an_unowned_dataset(self, stream):
        stream.owner = None
        stream.token("1-token")
        stream.run("fresh")
        stream.abort.assert_not_called()
        assert stream.order == []

    def test_a_stream_into_another_clients_dataset_is_refused_naming_nobody(self, stream):
        stream.owner = "bob-7"
        stream.run("resume")
        (reply,) = stream.sent
        assert "bob-7" not in reply["error"] and "another client's" in reply["error"]
        stream.receive.assert_not_called()

    def test_the_claim_is_released_once_before_the_refusal(self, stream):
        from test_zfsbackup_api import _spy_releases
        stream.owner = None
        with _spy_releases(stream) as releases:
            stream.run("resume")
        assert releases == [(SERVER_DS, 0)]

    def test_the_owner_is_read_locally_for_a_stream(self, stream):
        stream.run("resume")
        assert (PROP_CLIENT_ID, "local") in stream.reads

    def test_an_unowned_dataset_is_one_warning_for_the_stream_and_the_negotiate_together(
        self, stream, caplog
    ):
        stream.owner = None
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            stream.run("resume")
            stream.run("resume")
            stream.exists.return_value = True
            assert stream.authed.client().post(
                "/backup/client1/pool/data/negotiate", json={"snapshots": [], "config": enc(GOOD)}
            ).status_code == 409
        assert len(warnings_of(caplog)) == 1                  # one dataset: one notice for every path

    def test_resume_token_is_409_for_an_unowned_dataset(self, stream):
        stream.owner = None
        resp = stream.authed.client().get("/backup/client1/pool/data/resume_token")
        assert resp.status_code == 409
        assert LEAF_HINT in resp.get_json()["error"]
        stream.list.assert_not_called()

    def test_resume_token_is_409_for_another_clients_dataset(self, stream):
        stream.owner = "bob-7"
        resp = stream.authed.client().get("/backup/client1/pool/data/resume_token")
        assert resp.status_code == 409 and "bob-7" not in resp.get_data(as_text=True)

    def test_resume_token_answers_for_an_owned_dataset(self, stream):
        stream.token("1-token")
        resp = stream.authed.client().get("/backup/client1/pool/data/resume_token")
        assert resp.status_code == 200 and resp.get_json() == {"resume_token": "1-token"}

    def test_an_owned_stream_is_received_as_before(self, stream):
        stream.run("resume")
        assert stream.sent == [{"status": "ok"}]
        assert stream.written[0] == HEADER


# ---------------------------------------------------------------------------
# Creating the containers
# ---------------------------------------------------------------------------


def create_argvs(mock_subprocess):
    return [c[0][0][1:] for c in mock_subprocess.call_args_list if c[0][0][1] == "create"]


class TestAncestorCreation:
    """Through the real `zfs create` (only `Popen` is mocked)."""

    @pytest.fixture
    def real_create(self, zfs, mock_subprocess):
        zfs["create"].side_effect = REAL_CREATE
        mock_subprocess.setup_multi(*[("",)] * 5)
        return mock_subprocess

    def test_the_argv_of_one_missing_ancestor(self, client, zfs, real_create):
        assert negotiate(client).status_code == 200
        assert create_argvs(real_create) == [
            ["create", "-u", "-o", "canmount=noauto", "-o", f"{PROP_CLIENT_ID}={CLIENT_ID}", PARENT_DS]]

    def test_no_dash_p_ever(self, client, zfs, real_create):
        assert ancestor_negotiate(client, "pool/a/b/c").status_code == 200
        for argv in create_argvs(real_create):
            assert "-p" not in argv and "-V" not in argv and argv[:2] == ["create", "-u"]

    def test_several_levels_are_created_outermost_first_one_command_each(self, client, zfs, real_create):
        existing(zfs)
        assert ancestor_negotiate(client, "pool/a/b/c").status_code == 200
        assert [a[-1] for a in create_argvs(real_create)] == [
            PARENT_DS, PARENT_DS + "/a", PARENT_DS + "/a/b"]
        assert all(a[:-1] == ["create", "-u", "-o", "canmount=noauto", "-o", f"{PROP_CLIENT_ID}={CLIENT_ID}"]
                   for a in create_argvs(real_create))

    def test_only_the_missing_levels_are_created(self, client, zfs, real_create):
        existing(zfs, PARENT_DS)
        assert ancestor_negotiate(client, "pool/a/b/c").status_code == 200
        assert [a[-1] for a in create_argvs(real_create)] == [PARENT_DS + "/a", PARENT_DS + "/a/b"]

    def test_the_target_is_neither_created_nor_asked_about(self, client, zfs, ancestors):
        existing(zfs)
        assert ancestor_negotiate(client, "pool/a/b").status_code == 200
        asked = {str(c.args[0]) for c in zfs["exists"].call_args_list}
        created = {str(c.args[0]) for c in zfs["create"].call_args_list}
        assert "pool/backups" not in asked | created and "pool" not in asked | created

    def test_a_top_level_client_dataset_creates_nothing_at_all(self, client, zfs, real_create):
        assert negotiate(client, dataset="pool").status_code == 200
        assert create_argvs(real_create) == []

    @pytest.mark.parametrize("version", VERSIONS)
    def test_the_command_is_the_one_captured_on_every_openzfs_version(self, client, zfs, real_create, version):
        """`M8/C9.a1`: `zfs create -u -o canmount=noauto -o org.zfsbackup:client_id=x <ds>` worked
        (rc 0) on 2.1.5, 2.2.2 and 2.4.1; ours has the same words around a different value."""
        record = load_section(version, "M8")["M8/C9.a1_unencrypted_parent"]
        assert record.rc == 0
        captured = record.cmd.split()[1:]
        assert negotiate(client).status_code == 200
        (argv,) = create_argvs(real_create)
        assert argv[:-1] == captured[:-1][:5] + [f"{PROP_CLIENT_ID}={CLIENT_ID}"]
        assert argv[:5] == captured[:5]

    def test_the_created_container_is_unmounted_and_stamped_in_one_command(self, client, zfs, real_create):
        negotiate(client)
        (argv,) = create_argvs(real_create)
        assert "-u" in argv and f"{PROP_CLIENT_ID}={CLIENT_ID}" in argv and "canmount=noauto" in argv


class TestCreateFailure:
    def test_a_failed_first_create_unclaims_the_pending_entry(self, client, authed, zfs):
        zfs["create"].side_effect = RuntimeError("cannot create 'pool/backups/client1': no such pool")
        resp = negotiate(client)
        assert resp.status_code == 500
        assert resp.get_json() == {"error": f"Failed to record the configuration of {SERVER_DS}"}
        assert dict(pending_configs(authed.app)) == {}
        assert streams_in_progress(authed.app) == set() and authed.gate.active == 0

    def test_a_failed_second_create_unclaims_it_too_and_the_first_one_stays(self, client, authed, zfs):
        existing(zfs)
        made = []

        def create(ds, **kwargs):
            if str(ds) == PARENT_DS + "/a":
                raise RuntimeError("cannot create: dataset already exists")     # it appeared meanwhile
            made.append(str(ds))

        zfs["create"].side_effect = create
        resp = ancestor_negotiate(client, "pool/a/b")
        assert resp.status_code == 500
        assert made == [PARENT_DS]                              # created before the failure
        assert dict(pending_configs(authed.app)) == {}
        assert streams_in_progress(authed.app) == set()
        # the retry finds the first level in place and builds the rest
        zfs["create"].side_effect = lambda ds, **k: made.append(str(ds))
        existing(zfs, PARENT_DS)
        assert ancestor_negotiate(client, "pool/a/b").status_code == 200
        assert made == [PARENT_DS, PARENT_DS + "/a"]
        assert list(pending_configs(authed.app)) == [PARENT_DS + "/a/b"]

    def test_a_dataset_that_appeared_between_the_check_and_the_create_is_a_failure_not_an_adoption(
        self, client, zfs
    ):
        """No `-p`: `zfs create` of a name that now exists fails, so a dataset made in between by
        anyone else is never silently stamped over."""
        zfs["create"].side_effect = RuntimeError("cannot create 'pool/backups/client1': dataset already exists")
        assert negotiate(client).status_code == 500
        zfs["set"].assert_not_called()

    def test_a_value_error_from_the_create_is_the_bad_property_400(self, client, authed, zfs):
        zfs["create"].side_effect = ValueError("value contains a newline")
        resp = negotiate(client)
        assert resp.status_code == 400
        assert dict(pending_configs(authed.app)) == {}
        assert streams_in_progress(authed.app) == set()

"""The receive side of `/stream`: a client's stream cannot set the server's ownership and config
properties (production readiness plan B-1b), mocked.

On real pools (OpenZFS 2.1.5, 2.2.2, 2.4.1) `zfs receive -x <property>` does NOT protect a local
value on a full `-F` receive of a `zfs send -p` stream: the stream's copy replaces it (source
`received`). So the server puts its own values in effect with `-o` ON the receive:

* before anything is destroyed (before the fresh-mode `receive -A`) it reads the dataset's LOCAL
  `org.zfsbackup:config` (stored by negotiate, canonicalised) -- or, for a dataset that does not
  exist yet (R0), the pending entry negotiate kept in memory for THIS client dataset. Neither: the
  stream is REFUSED ("negotiate again before streaming");
* `props = {config, client_id (the bound id), source_dataset (the URL)}` goes to
  `zfs.receive.filesystem(..., force=<the dataset exists>, save=True, mount=False)` for `fresh` and
  `resume` alike, with no `reset` (`-x` and `-o` of one name are mutually exclusive). A full stream
  into a dataset that does not exist creates it (zfs refuses `-F` with a raw stream onto an existing
  dataset), so no `-F` there;
* there is no `zfs create` or `zfs set` anywhere on the stream path: not after the receive, and not
  after a fresh-mode abort that destroyed a new dataset whole (Z3), which the full stream recreates.

Layout: `TestReceiveFlags` (the call), `TestReceiveArgv` (the real argv), `TestStreamOwnsTheServerProperties`
(what is read, from where, and whose names), `TestStreamNeedsANegotiatedConfig` (the refusals),
`TestPendingConfigStream` (R0: the config of a dataset that does not exist yet),
`TestForceFollowsExistence`, `TestPlaceholderOverwriteMessage`, `TestNoStampAfterTheReceive`,
`TestNoStampAfterAFailedReceive`.

B-2b (`-F` only when it destroys nothing the client did not allow): `TestParseStreamBase`,
`TestDecideReceive` (every row of the table, pure), `TestServerSnapshots`,
`TestMalformedStreamParameters`, `TestTheDecisionThroughTheStream` (refusals, the abort not run,
force recomputed after it, createtxg order, fail-closed listing, claim release).
"""

import base64
import json
import logging
from unittest.mock import MagicMock

import pytest
from simple_websocket import ConnectionClosed

from test_zfsbackup_api import (  # noqa: F401  (Stream: a harness, not a test; the rest: fixtures)
    GOOD_CONFIG, NOT_NEGOTIATED, SERVER_DS, STREAM_PATH, TOKEN, Stream, _remote_config,
    source_props, stored_config,
)
from _stream_support import (
    FILESYSTEM_FORCED, HEADER, expected_receive_argv, get_value_answers, incremental_header, stream_script,
)
from _api_support import Authed, pending_configs
from libzfseasy.types import Dataset
from zfsbackup.api import SERVER_OWNED_PROPERTIES, canonical_dataset_config
from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_CONFIG, PROP_SOURCE_DATASET

OWNED = [PROP_CONFIG, PROP_CLIENT_ID, PROP_SOURCE_DATASET]
STORED = canonical_dataset_config(GOOD_CONFIG)
EXPECTED_PROPS = {
    PROP_CONFIG: STORED,
    PROP_CLIENT_ID: "client1",
    PROP_SOURCE_DATASET: "pool/data",
}
#: What the receive is given as `props` for a filesystem stream: the owned values, then the
#: forced ones (B-2a; the forced set itself is tested in `test_zfsbackup_api_stream_header.py`).
RECEIVE_PROPS = {**EXPECTED_PROPS, **FILESYSTEM_FORCED}


@pytest.fixture
def stream(mocker):
    return Stream(mocker)


def negotiate(stream, *, client="client1", dataset="pool/data", config=None):
    """A real negotiate against `stream`'s app (the zfs calls are the harness's mocks). With
    `stream.exists` False it makes the pending entry; the parent `create` is not counted."""
    who = stream.authed.cert_for(client)
    resp = stream.authed.client(who).post(
        f"/backup/{client}/{dataset}/negotiate",
        json={"snapshots": [], "config": GOOD_CONFIG if config is None else config},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    stream.create.reset_mock()
    stream.set.reset_mock()
    stream.order.clear()
    return resp


def owned_in(argv):
    """The `name=value` pairs named by `-o` in a `zfs receive` argv, in order."""
    return [argv[i + 1] for i, a in enumerate(argv[:-1]) if a == "-o"]


def non_canonical_config():
    """A config a client could store that is valid but not what `to_property()` writes: extra keys
    and layout. Decodes, but re-encoding changes the bytes."""
    text = json.dumps({
        "name": "pool/data", "recursive": False, "frequency": 3600.0, "enabled": True,
        "retention_rules": [], "remote": [], "junk": "x" * 10,
    }, indent=2)
    value = base64.b64encode(text.encode()).decode()
    assert value != STORED and canonical_dataset_config(value) != value
    return value


# ---------------------------------------------------------------------------
# What is passed to `zfs receive`
# ---------------------------------------------------------------------------


class TestReceiveFlags:
    def test_the_constant_names_the_three_properties(self):
        assert list(SERVER_OWNED_PROPERTIES) == OWNED
        assert OWNED == ["org.zfsbackup:config", "org.zfsbackup:client_id",
                         "org.zfsbackup:source_dataset"]

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_receive_is_called_with_the_exact_owned_props(self, stream, mode):
        stream.run(mode)
        (call,) = stream.receive.call_args_list
        assert str(call.args[0]) == SERVER_DS
        assert call.kwargs["props"] == RECEIVE_PROPS
        assert (call.kwargs["force"], call.kwargs["save"], call.kwargs["mount"]) == (True, True, False)

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_no_reset_is_passed_for_the_owned_names(self, stream, mode):
        """`-x` and `-o` of one name are mutually exclusive ("specified multiple times")."""
        stream.run(mode)
        kwargs = stream.receive.call_args.kwargs
        assert not kwargs.get("reset")
        assert "reset" not in kwargs

    def test_fresh_and_resume_receive_identically(self, mocker):
        calls = {}
        for mode in ("fresh", "resume"):
            s = Stream(mocker)
            s.run(mode)
            (call,) = s.receive.call_args_list
            calls[mode] = (str(call.args[0]), dict(call.kwargs))
        assert calls["fresh"] == calls["resume"]

    def test_a_fresh_receive_after_an_abort_has_the_same_props(self, stream):
        stream.token(TOKEN)
        stream.exists.side_effect = [True, True, True, True]
        stream.run("fresh")
        (call,) = stream.receive.call_args_list
        assert call.kwargs["props"] == RECEIVE_PROPS
        stream.abort.assert_called_once()

    def test_the_props_are_a_fresh_dict_each_time(self, mocker):
        """A callee that mutates its argument cannot weaken the next call."""
        for _ in range(2):
            s = Stream(mocker)
            s.run("resume")
            props = s.receive.call_args.kwargs["props"]
            assert props == RECEIVE_PROPS
            props.clear()
        assert list(SERVER_OWNED_PROPERTIES) == OWNED


class TestReceiveArgv:
    """The real `zfs receive` argv (only `Popen` is mocked)."""

    def run(self, mocker, mock_subprocess, mode, *, token=None):
        mocker.patch("zfsbackup.api.read_zfsbackup_properties", return_value={})
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.api.zfs.get.value", side_effect=get_value_answers({
            PROP_CONFIG: STORED, PROP_CLIENT_ID: "client1", "type": "filesystem"}))
        authed = Authed(_remote_config())
        handler = authed.app.view_functions["backup_stream"].__wrapped__
        first = len(mock_subprocess.call_args_list)          # (a test may run several streams)
        # every stream first lists the server's snapshots (the `-F` decision: an incremental from
        # the guid below, which the server holds as its newest snapshot)
        snapshots = ([f"{SERVER_DS}@autosnap_20260101000000\tsnapshot\t100\t5000\n"],)
        if mode == "resume":
            mock_subprocess.setup_multi(snapshots, ("",))                       # snapshots, receive
        elif token is None:
            mock_subprocess.setup_multi(
                snapshots, ([f"{SERVER_DS}\tfilesystem\t-\n"],), ("",))         # snapshots, token, receive
        else:
            mock_subprocess.setup_multi(
                snapshots, ([f"{SERVER_DS}\tfilesystem\t{token}\n"],), ("",), ("",))   # + abort
        ws = MagicMock()
        ws.receive.side_effect = stream_script(b"chunk", header=incremental_header(5000))
        with authed.context(STREAM_PATH, query_string={"mode": mode, "from_guid": "5000"}):
            handler(ws, "client1", "pool/data")
        assert json.loads(ws.send.call_args[0][0]) == {"status": "ok"}
        return [c[0][0][1:] for c in mock_subprocess.call_args_list[first:]]

    def receive_argv(self, argvs):
        (argv,) = [a for a in argvs if a[0] == "receive" and a[1] != "-A"]
        return argv

    def test_every_receive_carries_o_for_all_three_in_order(self, mocker, mock_subprocess):
        for mode, token in (("resume", None), ("fresh", None), ("fresh", TOKEN)):
            argv = self.receive_argv(self.run(mocker, mock_subprocess, mode, token=token))
            assert owned_in(argv)[:3] == [
                f"{PROP_CONFIG}={STORED}",
                f"{PROP_CLIENT_ID}=client1",
                f"{PROP_SOURCE_DATASET}=pool/data",
            ], (mode, token, argv)
            assert argv == expected_receive_argv(EXPECTED_PROPS, "filesystem", SERVER_DS, force=True)
            assert argv[-1] == SERVER_DS
            for flag in ("-F", "-h", "-s", "-u"):
                assert flag in argv

    def test_there_is_no_x_at_all(self, mocker, mock_subprocess):
        for mode in ("resume", "fresh"):
            argv = self.receive_argv(self.run(mocker, mock_subprocess, mode))
            assert "-x" not in argv
            assert argv.count("-o") == 3 + len(FILESYSTEM_FORCED)       # the owned and the forced values

    def test_fresh_and_resume_argv_are_identical(self, mocker, mock_subprocess):
        fresh = self.receive_argv(self.run(mocker, mock_subprocess, "fresh"))
        resumed = self.receive_argv(self.run(mocker, mock_subprocess, "resume"))
        assert fresh == resumed

    def test_the_receive_is_the_last_zfs_command_of_a_resume(self, mocker, mock_subprocess):
        argvs = self.run(mocker, mock_subprocess, "resume")
        # the one listing is the `-F` decision (snapshots, not the token); no `zfs set` after it
        assert [a[0] for a in argvs] == ["list", "receive"]
        assert "snapshot" in argvs[0] and "receive_resume_token" not in ",".join(argvs[0])

    def test_a_fresh_stream_runs_both_listings_abort_receive_and_nothing_after(self, mocker, mock_subprocess):
        argvs = self.run(mocker, mock_subprocess, "fresh", token=TOKEN)
        assert [a[0] for a in argvs] == ["list", "list", "receive", "receive"]
        assert "snapshot" in argvs[0]                        # the `-F` decision, before the abort
        assert "receive_resume_token" in ",".join(argvs[1])
        assert argvs[2] == ["receive", "-A", SERVER_DS]


# ---------------------------------------------------------------------------
# Where the values come from
# ---------------------------------------------------------------------------


class TestStreamOwnsTheServerProperties:
    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_the_config_is_read_locally_from_the_server_dataset(self, stream, mode):
        stream.run(mode)
        (call,) = [c for c in stream.get_value.call_args_list if c.args[1] == PROP_CONFIG]
        assert isinstance(call.args[0], Dataset) and str(call.args[0]) == SERVER_DS
        assert call.kwargs == {"sources": "local"}          # never a received or inherited copy
        # ...and so is the owner (B-2c); the type is the one read with no source restriction
        assert (PROP_CLIENT_ID, "local") in stream.reads
        assert ("type", None) in stream.reads

    def test_the_stored_value_is_canonicalised(self, stream):
        stream.stored = non_canonical_config()
        stream.run("resume")
        props = stream.receive.call_args.kwargs["props"]
        assert props[PROP_CONFIG] == canonical_dataset_config(non_canonical_config())
        assert props[PROP_CONFIG] != non_canonical_config()
        assert props[PROP_CONFIG] == STORED

    def test_the_config_is_read_before_the_fresh_mode_abort(self, stream):
        stream.token(TOKEN)
        stream.run("fresh")
        assert stream.order == ["get", "abort", "receive"]

    def test_the_value_read_before_the_abort_is_the_one_received_with(self, stream):
        """A second read after the abort (or the receive) would find a different value, or none."""
        stream.token(TOKEN)
        stream.config_script[:] = [STORED, None, "junk"]
        stream.run("fresh")
        assert stream.receive.call_args.kwargs["props"][PROP_CONFIG] == STORED
        assert stream.config_reads == 1

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_the_url_dataset_and_the_bound_id_are_stamped(self, stream, mode):
        stream.run(mode, client="client1", dataset="pool/data")
        props = stream.receive.call_args.kwargs["props"]
        assert props[PROP_CLIENT_ID] == "client1"
        assert props[PROP_SOURCE_DATASET] == "pool/data"

    def test_it_uses_the_bound_id_and_the_url_dataset(self, stream):
        stream.run("resume", client="other", dataset="tank/vol")
        (call,) = stream.receive.call_args_list
        assert str(call.args[0]) == "pool/backups/other/vol"
        assert call.kwargs["props"][PROP_CLIENT_ID] == "other"
        assert call.kwargs["props"][PROP_SOURCE_DATASET] == "tank/vol"

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_the_view_argument_does_not_choose_the_stamped_id(self, stream, mode):
        """The id a request acts as is the certificate's binding, never the URL's."""
        with stream.authed.context(STREAM_PATH, query_string={"mode": mode}):
            stream.handler(stream.ws, "someone-else", "pool/data")
        (call,) = stream.receive.call_args_list
        assert str(call.args[0]) == SERVER_DS
        assert call.kwargs["props"][PROP_CLIENT_ID] == "client1"
        assert "someone-else" not in json.dumps(call.kwargs["props"])
        assert "someone-else" not in " ".join(str(c) for c in stream.get_value.call_args_list)

    def test_a_dry_run_reads_nothing_and_receives_nothing(self, mocker):
        s = Stream(mocker, dry_run=True)
        s.run("fresh")
        assert s.sent == [{"status": "ok", "dry_run": True}]
        assert s.reads == [(PROP_CLIENT_ID, "local")]       # only the (read-only) ownership check
        s.receive.assert_not_called()

    def test_an_ownership_conflict_is_answered_before_the_config_is_read(self, stream, source_props):
        source_props.return_value = {PROP_SOURCE_DATASET: "tank/other"}
        stream.run("fresh")
        assert "refusing to mix in" in stream.sent[0]["error"]
        assert stream.config_reads == 0
        stream.receive.assert_not_called()


class TestStreamNeedsANegotiatedConfig:
    """A stream into a dataset without a usable local config is refused before anything is
    aborted, created, received or even listed: the server has no config of its own to put in
    effect, and without one the stream's copy could become the one in effect."""

    def assert_refused(self, stream):
        assert stream.sent == [NOT_NEGOTIATED]
        for m in (stream.abort, stream.create, stream.set, stream.receive):
            m.assert_not_called()
        stream.list.assert_not_called()
        stream.ws.receive.assert_not_called()
        assert stream.gate.active == 0
        assert stream.claimed == set()

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_a_missing_dataset(self, stream, mode):
        stream.exists.return_value = False
        stream.token(TOKEN)
        stream.run(mode)
        self.assert_refused(stream)
        stream.get_value.assert_not_called()          # nothing to read from

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    @pytest.mark.parametrize("value", [None, ""], ids=["unset", "empty"])
    def test_a_dataset_without_a_local_config(self, stream, mode, value):
        stream.stored = value
        stream.token(TOKEN)
        stream.run(mode)
        self.assert_refused(stream)

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    @pytest.mark.parametrize("value", [
        "not base64 at all!",
        "bm90IGpzb24=",                                             # base64 of "not json"
        base64.b64encode(b'{"name": "x"}').decode(),                # no frequency
        base64.b64encode(b'[]').decode(),
        "e30=\n",                                                    # trailing control character
        base64.b64encode(json.dumps({
            "name": "pool/data", "frequency": 0, "retention_rules": [], "remote": [],
        }).encode()).decode(),                                       # decodes, but zero frequency
    ], ids=["garbage", "not-json", "no-frequency", "list", "newline", "zero-frequency"])
    def test_an_undecodable_or_unusable_config(self, stream, mode, value):
        stream.stored = value
        stream.token(TOKEN)
        stream.run(mode)
        self.assert_refused(stream)

    def test_the_refusal_is_logged_without_the_stored_value(self, stream, caplog):
        stream.stored = "SECRET-NOT-BASE64!"
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            stream.run("resume")
        assert any("negotiate first" in r.getMessage() for r in caplog.records)
        assert "SECRET-NOT-BASE64" not in caplog.text
        assert "SECRET-NOT-BASE64" not in json.dumps(stream.sent)

    def test_a_failing_read_is_its_own_error_and_nothing_runs(self, stream, caplog):
        stream.fail_config_read(RuntimeError("cannot open: permission denied for tank/secret"))
        with caplog.at_level(logging.ERROR, logger="zfsbackup.api"):
            stream.run("fresh")
        assert stream.sent == [{"error": f"Failed to read the configuration of {SERVER_DS}"}]
        assert "tank/secret" not in json.dumps(stream.sent)          # detail stays in the log
        assert "tank/secret" in caplog.text
        for m in (stream.abort, stream.create, stream.set, stream.receive):
            m.assert_not_called()
        stream.list.assert_not_called()
        assert stream.gate.active == 0 and stream.claimed == set()

    def test_the_next_stream_is_served_after_a_refusal(self, stream):
        stream.config_script[:] = [None, STORED]
        stream.run("resume")
        stream.ws.receive.side_effect = stream_script()
        stream.run("resume")
        assert stream.sent == [NOT_NEGOTIATED, {"status": "ok"}]

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_the_refusal_is_retryable_so_the_client_negotiates_again(self, stream, mode):
        """The refusal carries `retry: true`: nothing was wrong with the stream, the server only forgot."""
        stream.exists.return_value = False
        stream.run(mode)
        assert stream.sent[0]["retry"] is True
        assert "negotiate again" in stream.sent[0]["error"]

    def test_a_negotiated_config_is_accepted(self, stream):
        stream.run("resume")
        assert stream.sent == [{"status": "ok"}]


# ---------------------------------------------------------------------------
# Nothing is stamped after the receive
# ---------------------------------------------------------------------------


class TestNoStampAfterTheReceive:
    def record(self, stream):
        stream.writer.write.side_effect = lambda d: stream.order.append("write")
        stream.writer.close.side_effect = lambda: stream.order.append("close")
        stream.ws.send.side_effect = lambda m: stream.order.append(("send", json.loads(m)))

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_ok_follows_the_close_directly(self, stream, mode):
        self.record(stream)
        stream.run(mode)
        assert stream.order == ["get", "receive", "write", "write", "close", ("send", {"status": "ok"})]
        stream.set.assert_not_called()

    def test_a_second_stream_does_not_stamp_either(self, stream):
        stream.run("resume")
        stream.ws.receive.side_effect = stream_script()
        stream.run("resume")
        stream.set.assert_not_called()
        assert stream.receive.call_count == 2

    def test_a_failing_set_is_never_reached(self, stream):
        """The old post-receive failure ('received, but cannot record the ownership') is gone."""
        stream.set.side_effect = RuntimeError("x")
        stream.run("resume")
        assert stream.sent == [{"status": "ok"}]

    def test_a_dry_run_does_not_stamp(self, mocker):
        s = Stream(mocker, dry_run=True)
        s.run("resume")
        s.set.assert_not_called()
        assert s.sent == [{"status": "ok", "dry_run": True}]

    def test_the_close_failure_is_still_the_close_error(self, stream):
        stream.writer.close.side_effect = RuntimeError("cannot receive: bad stream")
        stream.run("resume")
        assert stream.sent == [{"error": "cannot receive: bad stream"}]


class TestNoStampAfterAFailedReceive:
    def test_a_close_that_raises(self, stream):
        stream.writer.close.side_effect = RuntimeError("cannot receive: checksum mismatch")
        stream.run("resume")
        stream.set.assert_not_called()

    def test_a_write_error(self, stream):
        stream.writer.write.side_effect = BrokenPipeError(32, "Broken pipe")
        stream.run("resume")
        stream.set.assert_not_called()
        assert "error" in stream.sent[-1]

    def test_a_client_that_disconnects(self, stream):
        stream.ws.receive.side_effect = [HEADER, ConnectionClosed(None, None)]
        stream.run("resume")
        stream.set.assert_not_called()

    def test_a_timeout(self, stream):
        stream.ws.receive.side_effect = [HEADER, None]
        stream.run("resume")
        stream.set.assert_not_called()
        assert "error" in stream.sent[-1]

    def test_a_receive_that_cannot_start(self, stream):
        stream.receive.side_effect = RuntimeError("cannot receive: dataset is busy")
        stream.run("resume")
        stream.set.assert_not_called()
        assert stream.sent == [{"error": "Failed to start zfs receive: cannot receive: dataset is busy"}]

    def test_a_refused_stream_stamps_nothing(self, stream, source_props):
        source_props.return_value = {PROP_SOURCE_DATASET: "tank/other"}
        stream.run("resume")
        stream.set.assert_not_called()
        stream.receive.assert_not_called()


# ---------------------------------------------------------------------------
# R0: the config of a dataset that does not exist yet
# ---------------------------------------------------------------------------


class TestPendingConfigStream:
    """A dataset that negotiate accepted and nothing has received is not on disk (zfs refuses a raw
    full onto a pre-created dataset): the stream takes its config from the pending entry that THIS
    client dataset's negotiate made, and the receive creates the dataset."""

    @pytest.fixture
    def fresh(self, stream):
        stream.exists.return_value = False
        negotiate(stream)
        return stream

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_the_values_come_from_the_pending_entry(self, fresh, mode):
        fresh.run(mode)
        assert fresh.sent == [{"status": "ok"}]
        (call,) = fresh.receive.call_args_list
        assert str(call.args[0]) == SERVER_DS
        assert call.kwargs["props"] == RECEIVE_PROPS
        fresh.get_value.assert_not_called()                 # no dataset to read a local property from

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_the_receive_has_no_force_and_nothing_is_created_or_set(self, fresh, mode):
        fresh.run(mode)
        kwargs = fresh.receive.call_args.kwargs
        assert (kwargs["force"], kwargs["save"], kwargs["mount"]) == (False, True, False)
        fresh.create.assert_not_called()
        fresh.set.assert_not_called()
        assert fresh.order == ["receive"]

    def test_the_value_is_canonical_whatever_the_client_sent(self, stream):
        stream.exists.return_value = False
        negotiate(stream, config=non_canonical_config())
        stream.run("fresh")
        assert stream.receive.call_args.kwargs["props"][PROP_CONFIG] == STORED

    def test_the_entry_is_dropped_after_a_successful_receive(self, fresh):
        fresh.run("fresh")
        assert dict(pending_configs(fresh.app)) == {}

    def test_a_second_stream_is_refused_without_a_negotiate(self, fresh):
        """After the first receive the dataset exists (and holds its own config); a stream that
        finds neither is told to negotiate again, and receives nothing."""
        fresh.run("fresh")
        fresh.ws.receive.side_effect = stream_script(b"chunk")
        fresh.run("fresh")
        assert fresh.sent == [{"status": "ok"}, NOT_NEGOTIATED]
        assert fresh.receive.call_count == 1
        assert fresh.claimed == set() and fresh.gate.active == 0

    def test_a_failed_receive_keeps_the_entry(self, fresh):
        """The retry of an interrupted first receive needs no negotiate while nothing was created."""
        fresh.writer.close.side_effect = RuntimeError("cannot receive: checksum mismatch")
        fresh.run("fresh")
        assert fresh.sent == [{"error": "cannot receive: checksum mismatch"}]
        assert list(pending_configs(fresh.app)) == [SERVER_DS]
        fresh.writer.close.side_effect = None
        fresh.ws.receive.side_effect = stream_script(b"chunk")
        fresh.run("fresh")
        assert fresh.sent[-1] == {"status": "ok"}
        assert dict(pending_configs(fresh.app)) == {}

    def test_a_refused_stream_keeps_the_entry(self, fresh, source_props):
        source_props.return_value = {PROP_SOURCE_DATASET: "tank/other"}
        # (the dataset "exists" for the ownership check only)
        fresh.exists.side_effect = [True]
        fresh.run("fresh")
        assert "refusing to mix in" in fresh.sent[0]["error"]
        assert list(pending_configs(fresh.app)) == [SERVER_DS]

    def test_nothing_negotiated_is_refused_and_receives_nothing(self, stream):
        stream.exists.return_value = False
        stream.run("fresh")
        assert stream.sent == [NOT_NEGOTIATED]
        stream.receive.assert_not_called()
        stream.list.assert_not_called()

    def test_a_restart_forgets_the_entry(self, fresh, mocker):
        restarted = Stream(mocker)
        restarted.exists.return_value = False
        restarted.run("fresh")
        assert restarted.sent == [NOT_NEGOTIATED]
        assert "negotiate again" in restarted.sent[0]["error"]
        restarted.receive.assert_not_called()

    def test_another_client_datasets_entry_is_never_used(self, stream):
        """`tank/data` negotiated first; `pool/data` maps to the same server dataset."""
        stream.exists.return_value = False
        negotiate(stream, dataset="tank/data")
        stream.run("fresh", dataset="pool/data")
        assert stream.sent == [NOT_NEGOTIATED]
        stream.receive.assert_not_called()
        assert list(pending_configs(stream.app)) == [SERVER_DS]       # theirs is untouched
        stream.ws.receive.side_effect = stream_script(b"chunk")
        stream.run("fresh", dataset="tank/data")
        assert stream.sent[-1] == {"status": "ok"}
        props = stream.receive.call_args.kwargs["props"]
        assert props[PROP_SOURCE_DATASET] == "tank/data"

    def test_another_clients_entry_is_never_used(self, stream):
        stream.exists.return_value = False
        negotiate(stream, client="client2")
        stream.run("fresh", client="client1")
        assert stream.sent == [NOT_NEGOTIATED]
        stream.receive.assert_not_called()
        assert list(pending_configs(stream.app)) == ["pool/backups/client2/data"]

    def test_an_existing_dataset_uses_its_local_config_and_keeps_force(self, stream):
        """The dataset exists by stream time: the local property wins, the stale entry is dropped."""
        from datetime import timedelta
        from zfsbackup.config import DatasetConfig
        stale = DatasetConfig(name="pool/data", frequency=timedelta(minutes=5)).to_property()
        assert canonical_dataset_config(stale) != STORED
        stream.exists.return_value = False
        negotiate(stream, config=stale)
        stream.exists.return_value = True
        stream.run("fresh")
        (call,) = stream.receive.call_args_list
        assert call.kwargs["force"] is True
        assert call.kwargs["props"][PROP_CONFIG] == STORED          # the dataset's, not the entry's
        assert dict(pending_configs(stream.app)) == {}
        stream.create.assert_not_called()
        stream.set.assert_not_called()

    @pytest.mark.parametrize("damaged", ["", "not base64 at all!", "bm90IGpzb24="],
                             ids=["empty", "garbage", "not-json"])
    def test_a_damaged_local_config_is_refused_and_the_entry_is_not_used(self, fresh, damaged):
        fresh.exists.return_value = True
        fresh.stored = damaged
        fresh.run("fresh")
        assert fresh.sent == [NOT_NEGOTIATED]
        fresh.receive.assert_not_called()
        fresh.abort.assert_not_called()
        fresh.list.assert_not_called()

    def test_an_unreadable_local_config_is_its_own_error_and_the_entry_is_not_used(self, fresh):
        fresh.exists.return_value = True
        fresh.fail_config_read(RuntimeError("cannot open: boom"))
        fresh.run("fresh")
        assert fresh.sent == [{"error": f"Failed to read the configuration of {SERVER_DS}"}]
        fresh.receive.assert_not_called()

    def test_the_bound_id_names_the_stamped_values_not_the_entry(self, fresh):
        """The entry holds the config and the client dataset only; the id is the request's."""
        entry = pending_configs(fresh.app)[SERVER_DS]
        assert entry == (STORED, "pool/data")
        fresh.run("resume")
        assert fresh.receive.call_args.kwargs["props"][PROP_CLIENT_ID] == "client1"

    def test_a_dry_run_does_not_consume_the_entry(self, mocker):
        s = Stream(mocker, dry_run=True)
        s.exists.return_value = False
        s.run("fresh")
        assert s.sent == [{"status": "ok", "dry_run": True}]
        s.receive.assert_not_called()

    def test_the_real_argv_has_no_dash_f_and_all_three_o_values(self, mocker, mock_subprocess):
        """Only `Popen` is mocked. A new dataset: `zfs receive -s -u -o ... <dataset>`, no `-F`."""
        mocker.patch("zfsbackup.api.read_zfsbackup_properties", return_value={})
        mocker.patch("zfsbackup.api.zfs.exists", return_value=False)
        mocker.patch("zfsbackup.api.zfs.create")
        mocker.patch("zfsbackup.api.zfs.set")           # negotiate's own writes are not under test here
        authed = Authed(_remote_config())
        resp = authed.client().post(
            "/backup/client1/pool/data/negotiate", json={"snapshots": [], "config": GOOD_CONFIG})
        assert resp.status_code == 200
        mock_subprocess.setup_multi(("",))
        handler = authed.app.view_functions["backup_stream"].__wrapped__
        ws = MagicMock()
        ws.receive.side_effect = stream_script(b"chunk")
        with authed.context(STREAM_PATH, query_string={"mode": "fresh"}):
            handler(ws, "client1", "pool/data")
        assert json.loads(ws.send.call_args[0][0]) == {"status": "ok"}
        (argv,) = [c[0][0][1:] for c in mock_subprocess.call_args_list]
        assert argv[0] == "receive" and argv[-1] == SERVER_DS
        assert "-F" not in argv and "-x" not in argv
        for flag in ("-s", "-u"):
            assert flag in argv
        assert owned_in(argv)[:3] == [
            f"{PROP_CONFIG}={STORED}", f"{PROP_CLIENT_ID}=client1", f"{PROP_SOURCE_DATASET}=pool/data"]
        assert argv == expected_receive_argv(
            EXPECTED_PROPS, "filesystem", SERVER_DS, force=False)


class TestForceFollowsExistence:
    """`-F` is for a dataset that exists (an incremental, a resume, a full over an existing one);
    a full into a dataset that does not exist creates it without."""

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_an_existing_dataset_is_received_with_force(self, stream, mode):
        stream.run(mode)
        assert stream.receive.call_args.kwargs["force"] is True

    def test_the_decision_is_made_after_the_abort(self, stream):
        """A new dataset's cut first receive: the abort destroys it whole (Z3), so the full stream
        that follows must not use `-F`; the owned values are the ones read before the abort."""
        stream.token(TOKEN)
        stream.exists.side_effect = [True, True, True, False]
        stream.run("fresh")
        assert stream.order == ["get", "abort", "receive"]
        assert stream.receive.call_args.kwargs["force"] is False
        assert stream.receive.call_args.kwargs["props"] == RECEIVE_PROPS

    def test_an_abort_that_leaves_the_dataset_keeps_force(self, stream):
        """An incremental's `%recv` state is discarded; the dataset and its snapshots stay (Z3)."""
        stream.token(TOKEN)
        stream.exists.side_effect = [True, True, True, True]
        stream.run("fresh")
        assert stream.order == ["get", "abort", "receive"]
        assert stream.receive.call_args.kwargs["force"] is True

    def test_the_values_read_before_the_abort_are_used_after_it(self, stream):
        stream.token(TOKEN)
        stream.exists.side_effect = [True, True, True, False]
        stream.config_script[:] = [STORED, None, "junk"]
        stream.run("fresh")
        assert stream.receive.call_args.kwargs["props"][PROP_CONFIG] == STORED
        assert stream.config_reads == 1

    def test_the_existence_is_asked_once_for_the_receive_and_by_name(self, stream):
        stream.run("resume")
        # the ownership check, the config read, and the receive's `force`
        assert [str(c.args[0]) for c in stream.exists.call_args_list] == [SERVER_DS] * 3

    def test_a_failing_existence_check_refuses_the_stream(self, stream):
        stream.exists.side_effect = [True, True, RuntimeError("exists failed")]
        stream.run("resume")
        assert stream.sent == [{"error": "Failed to start zfs receive: exists failed"}]
        stream.receive.assert_not_called()
        assert stream.gate.active == 0 and stream.claimed == set()


# ---------------------------------------------------------------------------
# The "overwrite an unencrypted one with an encrypted one" refusal
# ---------------------------------------------------------------------------

OVERWRITE = (
    "cannot receive new filesystem stream: zfs receive -F cannot be used to destroy an encrypted "
    "filesystem or overwrite an unencrypted one with an encrypted one"
)


class TestPlaceholderOverwriteMessage:
    """zfs refuses an encrypted full over an existing dataset. Two causes the operator can act on: a
    parent created as a placeholder (a child was backed up before it), or a plain/raw copy mismatch."""

    def run_close_raises(self, stream, text=OVERWRITE):
        stream.writer.close.side_effect = RuntimeError(text)
        stream.run("fresh")
        return stream.sent[-1]["error"]

    def test_the_client_gets_a_clear_message_with_the_original_text(self, stream):
        error = self.run_close_raises(stream)
        assert len(stream.sent) == 1
        assert error.startswith(f"{SERVER_DS} already exists on the server and cannot receive a full encrypted stream")
        assert "placeholder" in error
        assert "back the parent up before its children" in error
        assert "zfs rename" in error and "plain vs raw" in error
        assert "children" in error                       # a rename moves their server copies too
        assert error.endswith(OVERWRITE)               # zfs's own words are kept

    def test_other_errors_are_sent_verbatim(self, stream):
        assert self.run_close_raises(stream, "cannot receive: checksum mismatch") == (
            "cannot receive: checksum mismatch")
        assert "placeholder" not in stream.sent[-1]["error"]

    def test_it_is_logged_with_the_original_text(self, stream, caplog):
        with caplog.at_level(logging.ERROR, logger="zfsbackup.api"):
            self.run_close_raises(stream)
        records = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert len(records) == 1 and SERVER_DS in records[0] and OVERWRITE in records[0]

    def test_it_applies_on_the_write_error_path_too(self, stream):
        """`zfs receive` died on the first chunk: the diagnostic `close()` raises is the one sent."""
        stream.writer.write.side_effect = BrokenPipeError(32, "Broken pipe")
        stream.writer.close.side_effect = RuntimeError(OVERWRITE)
        stream.run("fresh")
        assert "placeholder" in stream.sent[-1]["error"] and OVERWRITE in stream.sent[-1]["error"]
        stream.writer.close.assert_called_once()

    def test_the_claim_is_released_once_before_the_reply(self, stream):
        from test_zfsbackup_api import _spy_releases
        with _spy_releases(stream) as releases:
            self.run_close_raises(stream)
        assert releases == [(SERVER_DS, 0)]              # once, with no reply sent yet
        assert stream.claimed == set() and stream.gate.active == 0

    def test_the_next_stream_is_served(self, stream):
        self.run_close_raises(stream)
        stream.writer.close.side_effect = None
        stream.ws.receive.side_effect = stream_script()
        stream.run("resume")
        assert stream.sent[-1] == {"status": "ok"}

    def test_a_disconnected_client_gets_nothing(self, stream):
        stream.writer.close.side_effect = RuntimeError(OVERWRITE)
        stream.ws.send.side_effect = ConnectionClosed(None, None)
        stream.run("fresh")                               # must not raise
        assert stream.claimed == set()


# ---------------------------------------------------------------------------
# B-2b: `-F` only when it destroys nothing the client did not allow
# ---------------------------------------------------------------------------
#
# `zfs receive -F` of an incremental stream destroys every snapshot of the destination newer than
# the stream's base (observed on OpenZFS 2.4.1; captured in tests/fixtures/openzfs/*/M7.txt). A
# client that rolled back or restored an older snapshot would make the server destroy history it
# holds, so `decide_receive` passes `-F` only when nothing newer than the base exists, or when the
# client named exactly those snapshots (`discard`, by guid). "Newer" is by `createtxg`.

from zfsbackup.api import (  # noqa: E402
    DISCARD_MAX, SERVER_NEWER_MAX, ReceiveDecision, ServerSnapshot, StreamBase, decide_receive,
    parse_stream_base, server_snapshots,
)

#: Three snapshots of the server copy, oldest first (names ascend with txg).
A, B, C = "autosnap_20260101000000", "autosnap_20260102000000", "autosnap_20260103000000"
GA, GB, GC = 1001, 1002, 1003
ROWS = [ServerSnapshot(A, 100, GA), ServerSnapshot(B, 200, GB), ServerSnapshot(C, 300, GC)]
DS = "pool/backups/client1/data"


def rows_for_stream():
    """The same three snapshots as `Stream.server_has` takes them."""
    return [(A, 100, GA), (B, 200, GB), (C, 300, GC)]


class TestParseStreamBase:
    def test_nothing_is_a_full_stream(self):
        assert parse_stream_base({}) == StreamBase(None, None, frozenset())

    def test_the_fields(self):
        base = parse_stream_base({"from": A, "from_guid": "1001", "discard": "5,6"})
        assert base == StreamBase(A, 1001, frozenset({5, 6}))
        assert isinstance(base.guid, int) and all(isinstance(g, int) for g in base.discard)

    def test_from_alone_is_the_name_fallback_of_an_older_client(self):
        assert parse_stream_base({"from": A}) == StreamBase(A, None, frozenset())

    def test_an_empty_from_is_no_base(self):
        assert parse_stream_base({"from": ""}).name is None

    def test_an_empty_discard_is_none(self):
        assert parse_stream_base({"discard": ""}).discard == frozenset()

    def test_duplicates_collapse(self):
        assert parse_stream_base({"discard": "7,7,8"}).discard == frozenset({7, 8})

    def test_the_caps_are_what_the_plan_says(self):
        assert DISCARD_MAX == 16 and SERVER_NEWER_MAX == 64

    def test_discard_at_the_cap_is_accepted_and_one_over_is_not(self):
        ok = ",".join(str(i) for i in range(DISCARD_MAX))
        assert len(parse_stream_base({"discard": ok}).discard) == DISCARD_MAX
        with pytest.raises(ValueError, match="more than 16"):
            parse_stream_base({"discard": ok + f",{DISCARD_MAX}"})

    def test_the_cap_counts_entries_not_distinct_guids(self):
        """Seventeen copies of one guid are still seventeen entries: a request cannot dodge the cap."""
        with pytest.raises(ValueError):
            parse_stream_base({"discard": ",".join(["1"] * (DISCARD_MAX + 1))})

    @pytest.mark.parametrize("value", [
        "", "abc", "-1", "+1", "1e3", "0x10", " 12", "12 ", "12\n", "1.5", "1,2", "١٢",
        "9" * 21,
    ], ids=["empty", "alpha", "negative", "plus", "exp", "hex", "lead-space", "trail-space",
            "newline", "float", "list", "arabic-indic", "21-digits"])
    def test_a_malformed_from_guid_is_refused(self, value):
        with pytest.raises(ValueError, match="from_guid"):
            parse_stream_base({"from_guid": value})

    def test_twenty_digits_is_the_longest_guid(self):
        assert parse_stream_base({"from_guid": "9" * 20}).guid == int("9" * 20)
        assert parse_stream_base({"from_guid": "18446744073709551615"}).guid == 2 ** 64 - 1

    @pytest.mark.parametrize("value", [
        "1,", ",1", "1,,2", "x", "1,x", "-1", "1, 2", " 1", "1\n", "0x1", "1;2", "9" * 21, ",",
    ])
    def test_a_malformed_discard_is_refused(self, value):
        with pytest.raises(ValueError, match="discard"):
            parse_stream_base({"discard": value})

    def test_the_error_text_does_not_echo_the_value(self):
        with pytest.raises(ValueError) as exc:
            parse_stream_base({"discard": "SECRET,1"})
        assert "SECRET" not in str(exc.value)


class TestDecideReceive:
    """Every row of the plan's table, on the pure function."""

    def decide(self, base, rows=ROWS):
        return decide_receive(DS, base, list(rows))

    # -- dataset with no snapshots ---------------------------------------------------

    @pytest.mark.parametrize("base", [
        StreamBase(), StreamBase(A, GA), StreamBase(A), StreamBase(None, None, frozenset({1})),
    ], ids=["full", "guid", "name", "discard-only"])
    def test_no_snapshots_is_force(self, base):
        """A placeholder, or a first receive that was cut: `-F` replaces it."""
        assert self.decide(base, rows=[]) == ReceiveDecision(True)

    # -- the base is the newest -----------------------------------------------------

    def test_the_base_by_guid_being_the_newest_is_force_and_destroys_nothing(self):
        decision = self.decide(StreamBase(C, GC))
        assert decision == ReceiveDecision(True) and decision.rolled_back == ()

    def test_the_base_by_name_being_the_newest_is_force(self):
        assert self.decide(StreamBase(C, None)) == ReceiveDecision(True)

    def test_a_discard_list_does_not_matter_when_nothing_is_newer(self):
        decision = self.decide(StreamBase(C, GC, frozenset({GA, 99})))
        assert decision == ReceiveDecision(True) and decision.rolled_back == ()

    # -- the base is not on the server ------------------------------------------------

    def test_a_guid_the_server_does_not_hold_is_base_missing(self):
        decision = self.decide(StreamBase(C, 424242))
        assert decision.force is False and decision.reason == "base_missing"
        assert "424242" in decision.message and "zfs rename" in decision.message
        assert decision.rolled_back == ()

    def test_a_name_the_server_does_not_hold_is_base_missing(self):
        decision = self.decide(StreamBase("autosnap_29990101000000", None))
        assert decision.reason == "base_missing" and "autosnap_29990101000000" in decision.message

    def test_the_guid_is_preferred_to_the_name(self):
        """`from` names the newest (C), `from_guid` is A's: the base is A, so B and C are newer."""
        decision = self.decide(StreamBase(C, GA))
        assert decision.reason == "server_newer"
        assert B in decision.message and C in decision.message

    def test_a_guid_the_server_lacks_is_not_rescued_by_a_matching_name(self):
        """No fallback to the name once a guid is given: the name could be a recreated snapshot."""
        assert self.decide(StreamBase(C, 424242)).reason == "base_missing"

    def test_the_wrong_name_with_the_right_guid_is_the_right_base(self):
        assert self.decide(StreamBase("not-the-name", GC)) == ReceiveDecision(True)

    # -- newer snapshots -------------------------------------------------------------

    def test_newer_snapshots_all_in_discard_are_rolled_back_oldest_first(self):
        decision = self.decide(StreamBase(A, GA, frozenset({GC, GB})))
        assert decision.force is True and decision.reason is None
        assert decision.rolled_back == (B, C)

    def test_one_newer_snapshot_in_discard(self):
        decision = self.decide(StreamBase(B, GB, frozenset({GC})))
        assert decision == ReceiveDecision(True, rolled_back=(C,))

    def test_newer_snapshots_not_all_in_discard_are_refused(self):
        decision = self.decide(StreamBase(A, GA, frozenset({GB})))
        assert decision.force is False and decision.reason == "server_newer"
        assert decision.rolled_back == ()

    def test_newer_snapshots_with_no_discard_are_refused(self):
        assert self.decide(StreamBase(A, GA)).reason == "server_newer"
        assert self.decide(StreamBase(A, None)).reason == "server_newer"

    def test_the_refusal_names_the_range_and_both_remedies(self):
        message = self.decide(StreamBase(A, GA)).message
        assert DS in message and f"{B}…{C}" in message and "2 snapshot(s)" in message
        assert f"zfs rename {DS}" in message and "outside the backup target" in message
        assert f"zfs destroy {DS}@{B}%{C}" in message

    def test_a_single_newer_snapshot_is_destroyed_by_plain_name(self):
        message = self.decide(StreamBase(B, GB)).message
        assert f"zfs destroy {DS}@{C}`" in message and "%" not in message
        assert "1 snapshot(s)" in message

    def test_a_discard_of_the_wrong_guids_does_not_cover(self):
        assert self.decide(StreamBase(A, GA, frozenset({1, 2, 3}))).reason == "server_newer"

    def test_a_discard_of_older_or_base_guids_does_not_cover_newer_ones(self):
        assert self.decide(StreamBase(B, GB, frozenset({GA, GB}))).reason == "server_newer"

    def test_a_newer_snapshot_without_a_guid_cannot_be_covered(self):
        rows = [ServerSnapshot(A, 100, GA), ServerSnapshot(B, 200, None)]
        decision = self.decide(StreamBase(A, GA, frozenset({0})), rows=rows)
        assert decision.reason == "server_newer"

    # -- createtxg, not the name ------------------------------------------------------

    def test_newer_is_by_createtxg_when_the_names_disagree(self):
        """The name order is the opposite of the creation order: B's name is older, its txg newer."""
        rows = [ServerSnapshot(B, 300, GB), ServerSnapshot(C, 100, GC)]    # C was created first
        decision = decide_receive(DS, StreamBase(C, GC), rows)
        assert decision.reason == "server_newer" and B in decision.message
        # ...and in the other direction nothing is newer than B
        assert decide_receive(DS, StreamBase(B, GB), rows) == ReceiveDecision(True)

    def test_the_rolled_back_order_is_createtxg_order(self):
        rows = [ServerSnapshot(A, 100, GA), ServerSnapshot(C, 300, GC), ServerSnapshot(B, 200, GB)]
        decision = decide_receive(DS, StreamBase(A, GA, frozenset({GB, GC})), rows)
        assert decision.rolled_back == (B, C)

    def test_a_snapshot_without_a_createtxg_fails_closed(self):
        rows = [ServerSnapshot(A, 100, GA), ServerSnapshot(B, None, GB)]
        for base in (StreamBase(), StreamBase(A, GA), StreamBase(A, GA, frozenset({GB}))):
            with pytest.raises(ValueError, match="createtxg"):
                decide_receive(DS, base, rows)

    # -- a full stream onto a copy with snapshots -----------------------------------------

    def test_a_full_stream_onto_a_copy_with_snapshots_is_no_common(self):
        decision = self.decide(StreamBase())
        assert decision.force is False and decision.reason == "no_common"
        assert f"zfs rename {DS}" in decision.message
        assert "full stream" in decision.message

    def test_a_discard_does_not_turn_a_full_stream_into_a_rollback(self):
        assert self.decide(StreamBase(None, None, frozenset({GA, GB, GC}))).reason == "no_common"

    def test_the_decision_never_depends_on_row_order(self):
        for rows in (ROWS, ROWS[::-1], [ROWS[1], ROWS[2], ROWS[0]]):
            assert decide_receive(DS, StreamBase(A, GA, frozenset({GB, GC})), rows).rolled_back == (B, C)


class TestServerSnapshots:
    def test_one_listing_with_createtxg_and_guid(self, mocker):
        from _snapshot_support import listed_snapshot
        listing = mocker.patch("zfsbackup.api.zfs.list", return_value=[
            listed_snapshot(A, DS, txg=100, guid=GA), listed_snapshot(B, DS, txg=200, guid=GB),
        ])
        assert server_snapshots(Dataset(DS)) == [ServerSnapshot(A, 100, GA), ServerSnapshot(B, 200, GB)]
        listing.assert_called_once()
        assert listing.call_args.kwargs["types"] == "snapshot"
        assert listing.call_args.kwargs["properties"] == ["createtxg", "guid"]
        assert str(listing.call_args.kwargs["roots"]) == DS

    def test_a_missing_column_is_none(self, mocker):
        from _snapshot_support import ABSENT, listed_snapshot
        mocker.patch("zfsbackup.api.zfs.list", return_value=[
            listed_snapshot(A, DS, txg=ABSENT, guid=ABSENT)])
        assert server_snapshots(Dataset(DS)) == [ServerSnapshot(A, None, None)]

    def test_a_failing_listing_raises(self, mocker):
        mocker.patch("zfsbackup.api.zfs.list", side_effect=RuntimeError("pool suspended"))
        with pytest.raises(RuntimeError):
            server_snapshots(Dataset(DS))


class TestMalformedStreamParameters:
    """`from_guid` and `discard` are parsed after the mode check and before the gate: a request
    with garbage in them is answered with the protocol error and nothing else runs."""

    @pytest.mark.parametrize("query", [
        {"from_guid": "abc"}, {"from_guid": ""}, {"from_guid": "-5"}, {"discard": "1,,2"},
        {"discard": "x"}, {"discard": ",".join(str(i) for i in range(DISCARD_MAX + 1))},
    ])
    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_refused_before_anything_runs(self, stream, mode, query):
        stream.token(TOKEN)
        stream.run(mode, **query)
        (reply,) = stream.sent
        assert set(reply) == {"error"} and "stream parameter" in reply["error"]
        stream.untouched()
        assert stream.gate.active == 0 and stream.claimed == set()

    def test_it_is_answered_even_while_draining(self, stream):
        """Like a bad mode, a bad request is the client's to fix, not 'reloading, retry'."""
        stream.gate.start_draining()
        stream.run("fresh", from_guid="abc")
        assert "stream parameter from_guid" in stream.sent[0]["error"] and "retry" not in stream.sent[0]
        stream.untouched()

    def test_the_mode_error_comes_first(self, stream):
        stream.run(None, from_guid="abc")
        assert "stream mode required" in stream.sent[0]["error"]

    def test_the_refusal_survives_a_dead_socket(self, stream):
        stream.ws.send.side_effect = Exception("closed")
        stream.run("fresh", from_guid="abc")                 # must not raise
        stream.untouched()


class TestTheDecisionThroughTheStream:
    """`/stream` with a server copy that holds A, B, C (txg 100/200/300)."""

    @pytest.fixture
    def held(self, stream):
        stream.server_has(*rows_for_stream())
        return stream

    def refused(self, stream, reason):
        (reply,) = stream.sent
        assert reply["reason"] == reason and "retry" not in reply
        assert set(reply) == {"error", "reason"}
        for m in (stream.receive, stream.abort, stream.create, stream.set):
            m.assert_not_called()
        assert stream.ws.receive.call_count == 1       # the header was read first; nothing after it
        assert stream.gate.active == 0 and stream.claimed == set()
        return reply

    # -- the table, by guid ------------------------------------------------------------

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_the_base_is_the_newest_gets_force(self, held, mode):
        held.run(mode, **{"from": C, "from_guid": str(GC)})
        assert held.sent == [{"status": "ok"}]
        assert held.receive.call_args.kwargs["force"] is True

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_newer_snapshots_without_a_discard_are_refused(self, held, mode):
        held.run(mode, **{"from": A, "from_guid": str(GA)})
        reply = self.refused(held, "server_newer")
        assert f"{B}…{C}" in reply["error"] and "zfs rename" in reply["error"]

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_newer_snapshots_all_in_discard_get_force(self, held, mode):
        held.run(mode, **{"from": A, "from_guid": str(GA), "discard": f"{GB},{GC}"})
        assert held.sent == [{"status": "ok"}]
        assert held.receive.call_args.kwargs["force"] is True

    def test_newer_snapshots_partly_in_discard_are_refused(self, held):
        held.run("fresh", **{"from": A, "from_guid": str(GA), "discard": str(GC)})
        self.refused(held, "server_newer")

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_a_base_the_server_lacks_is_base_missing(self, held, mode):
        held.run(mode, **{"from": C, "from_guid": "424242"})
        reply = self.refused(held, "base_missing")
        assert "424242" in reply["error"]

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_a_full_stream_onto_a_copy_with_snapshots_is_no_common(self, held, mode):
        held.run(mode)
        reply = self.refused(held, "no_common")
        assert "zfs rename" in reply["error"]

    def test_no_snapshots_gets_force_for_any_base(self, stream):
        stream.server_has()
        stream.run("fresh", **{"from": A, "from_guid": str(GA)})
        assert stream.sent == [{"status": "ok"}]
        assert stream.receive.call_args.kwargs["force"] is True

    def test_no_snapshots_and_a_full_stream_gets_force(self, stream):
        stream.run("fresh")
        assert stream.receive.call_args.kwargs["force"] is True

    # -- the name fallback of a client without from_guid ------------------------------------

    def test_from_alone_is_matched_by_name(self, held):
        held.run("resume", **{"from": C})
        assert held.sent == [{"status": "ok"}]
        assert held.receive.call_args.kwargs["force"] is True

    def test_from_alone_with_newer_snapshots_is_refused(self, held):
        held.run("resume", **{"from": B})
        self.refused(held, "server_newer")

    def test_from_alone_that_the_server_lacks_is_base_missing(self, held):
        held.run("resume", **{"from": "autosnap_29990101000000"})
        self.refused(held, "base_missing")

    def test_a_discard_without_a_guid_works_with_the_name(self, held):
        held.run("resume", **{"from": B, "discard": str(GC)})
        assert held.sent == [{"status": "ok"}]

    # -- createtxg beats the name ------------------------------------------------------------

    def test_the_order_is_createtxg_when_the_names_disagree(self, stream):
        """Names ascend A < B < C but the txgs say C was created first: from C, the newer snapshots
        are A and B -- not nothing."""
        stream.server_has((A, 300, GA), (B, 200, GB), (C, 100, GC))
        stream.run("resume", **{"from": C, "from_guid": str(GC)})
        reply = self.refused(stream, "server_newer")
        assert f"{B}…{A}" in reply["error"]               # oldest txg first
        # and from A (the newest by txg) there is nothing newer
        stream.ws.receive.side_effect = stream_script(header=incremental_header(GA))
        stream.run("resume", **{"from": A, "from_guid": str(GA)})
        assert stream.sent[-1] == {"status": "ok"}

    # -- abort and force ------------------------------------------------------------------------

    def test_a_refusal_never_runs_the_abort_nor_even_looks_for_a_token(self, held):
        """The partial receive state is kept: a refused fresh stream must not discard it."""
        held.token(TOKEN)
        held.run("fresh", **{"from": A, "from_guid": str(GA)})
        self.refused(held, "server_newer")
        kinds = [c.kwargs.get("types") for c in held.list.call_args_list]
        assert kinds == ["snapshot"]                            # the decision's listing only

    def test_the_decision_is_made_before_the_abort_and_the_receive_follows_it(self, held):
        held.token(TOKEN)
        held.run("fresh", **{"from": C, "from_guid": str(GC)})
        assert held.order == ["get", "abort", "receive"]
        assert held.receive.call_args.kwargs["force"] is True

    def test_force_is_recomputed_after_the_abort(self, held):
        """The abort of a new dataset's cut first receive destroys it whole: no `-F` on the full
        stream that creates it again, even though the decision (made before) said force."""
        held.token(TOKEN)
        held.exists.side_effect = [True, True, True, False]      # ownership, config, token, after abort
        held.run("fresh", **{"from": C, "from_guid": str(GC)})
        assert held.order == ["get", "abort", "receive"]
        assert held.receive.call_args.kwargs["force"] is False

    def test_force_survives_an_abort_that_leaves_the_dataset(self, held):
        held.token(TOKEN)
        held.exists.side_effect = [True, True, True, True]
        held.run("fresh", **{"from": C, "from_guid": str(GC)})
        assert held.receive.call_args.kwargs["force"] is True

    def test_a_refusal_decision_is_not_overturned_by_the_recompute(self, held):
        held.exists.side_effect = [True, True, True, True]
        held.run("resume", **{"from": A, "from_guid": str(GA)})
        held.receive.assert_not_called()

    def test_a_dataset_that_does_not_exist_is_not_listed_and_is_received_without_force(self, mocker):
        """R0: the first receive creates the dataset; there is nothing to decide."""
        s = Stream(mocker)
        s.exists.return_value = False
        negotiate(s)
        s.run("fresh")
        assert s.sent == [{"status": "ok"}]
        s.list.assert_not_called()
        assert s.receive.call_args.kwargs["force"] is False

    # -- fresh and resume stay identical ---------------------------------------------------------

    @pytest.mark.parametrize("query", [
        {"from": C, "from_guid": str(GC)},
        {"from": A, "from_guid": str(GA), "discard": f"{GB},{GC}"},
        {},
    ], ids=["newest", "rollback", "full-onto-nothing"])
    def test_fresh_and_resume_call_receive_identically(self, mocker, query):
        calls = {}
        for mode in ("fresh", "resume"):
            s = Stream(mocker)
            if query:
                s.server_has(*rows_for_stream())
            s.run(mode, **query)
            assert s.sent == [{"status": "ok"}]
            (call,) = s.receive.call_args_list
            calls[mode] = (str(call.args[0]), dict(call.kwargs))
        assert calls["fresh"] == calls["resume"]

    def test_the_argv_of_a_permitted_rollback_is_the_ordinary_forced_receive(self, mocker, mock_subprocess):
        """Through the real command: the ordinary forced receive, nothing else added."""
        mocker.patch("zfsbackup.api.read_zfsbackup_properties", return_value={})
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.api.zfs.get.value", side_effect=get_value_answers({
            PROP_CONFIG: STORED, PROP_CLIENT_ID: "client1", "type": "filesystem"}))
        authed = Authed(_remote_config())
        handler = authed.app.view_functions["backup_stream"].__wrapped__
        mock_subprocess.setup_multi(
            ([f"{SERVER_DS}@{A}\tsnapshot\t100\t{GA}\n", f"{SERVER_DS}@{B}\tsnapshot\t200\t{GB}\n"],),
            ("",),
        )
        ws = MagicMock()
        ws.receive.side_effect = stream_script(b"chunk", header=incremental_header(GA))
        with authed.context(STREAM_PATH, query_string={
            "mode": "resume", "from": A, "from_guid": str(GA), "discard": str(GB),
        }):
            handler(ws, "client1", "pool/data")
        assert json.loads(ws.send.call_args[0][0]) == {"status": "ok"}
        argvs = [c[0][0][1:] for c in mock_subprocess.call_args_list]
        assert [a[0] for a in argvs] == ["list", "receive"]
        argv = argvs[1]
        assert "-x" not in argv
        assert argv == expected_receive_argv(EXPECTED_PROPS, "filesystem", SERVER_DS, force=True)

    # -- listing failures fail closed -------------------------------------------------------------

    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_a_failing_listing_refuses_the_stream(self, held, caplog, mode):
        held.fail_snapshot_listing(RuntimeError("pool I/O is currently suspended"))
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            held.run(mode, **{"from": C, "from_guid": str(GC)})
        (reply,) = held.sent
        assert reply["reason"] == "listing" and "retry" not in reply
        assert reply["error"].startswith(f"Failed to list the snapshots of {SERVER_DS}: ")
        assert "pool I/O is currently suspended" in reply["error"]
        for m in (held.receive, held.abort, held.create, held.set):
            m.assert_not_called()
        assert held.ws.receive.call_count == 1                 # the header, read before the listing
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1 and SERVER_DS in errors[0]
        assert held.gate.active == 0 and held.claimed == set()

    def test_a_listing_that_cannot_be_trusted_refuses_the_stream(self, held, caplog):
        """libzfseasy's strict listing raises when it had to skip a row: a snapshot the decision never saw
        could be destroyed by `-F`."""
        held.fail_snapshot_listing(Exception(
            "zfs list output could not be trusted: 1 row skipped, the first: expected 4 columns, got 2"))
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            held.run("resume", **{"from": C, "from_guid": str(GC)})
        (reply,) = held.sent
        assert reply["reason"] == "listing" and "could not be trusted" in reply["error"]
        held.receive.assert_not_called()

    def test_the_listing_is_asked_for_strictly(self, held):
        held.run("resume", **{"from": C, "from_guid": str(GC)})
        (call,) = held.list.call_args_list
        assert call.kwargs.get("strict") is True

    def test_a_listing_failure_never_runs_the_abort_either(self, held):
        held.token(TOKEN)
        held.fail_snapshot_listing(RuntimeError("x"))
        held.run("fresh", **{"from": C, "from_guid": str(GC)})
        held.abort.assert_not_called()

    def test_a_snapshot_without_createtxg_refuses_the_stream_with_the_listing_reason(self, stream, caplog):
        from _snapshot_support import ABSENT, listed_snapshot
        stream.snapshots = [listed_snapshot(A, SERVER_DS, txg=ABSENT, guid=GA)]
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            stream.run("resume", **{"from": A, "from_guid": str(GA)})
        (reply,) = stream.sent
        assert reply["reason"] == "listing" and "createtxg" in reply["error"] and "retry" not in reply
        stream.receive.assert_not_called()
        assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 1
        assert stream.gate.active == 0 and stream.claimed == set()

    def test_the_claim_is_released_once_before_the_refusal_reply(self, held):
        from test_zfsbackup_api import _spy_releases
        with _spy_releases(held) as releases:
            held.run("fresh", **{"from": A, "from_guid": str(GA)})
        assert releases == [(SERVER_DS, 0)]
        assert held.claimed == set()

    def test_the_claim_is_released_once_before_the_listing_failure_reply(self, held):
        from test_zfsbackup_api import _spy_releases
        held.fail_snapshot_listing(RuntimeError("x"))
        with _spy_releases(held) as releases:
            held.run("resume", **{"from": C, "from_guid": str(GC)})
        assert releases == [(SERVER_DS, 0)]

    def test_the_claim_is_released_once_before_the_createtxg_refusal_reply(self, stream):
        from _snapshot_support import ABSENT, listed_snapshot
        from test_zfsbackup_api import _spy_releases
        stream.snapshots = [listed_snapshot(A, SERVER_DS, txg=ABSENT, guid=GA)]
        with _spy_releases(stream) as releases:
            stream.run("resume", **{"from": A, "from_guid": str(GA)})
        assert releases == [(SERVER_DS, 0)]

    def test_the_next_stream_is_served_after_a_refusal(self, held):
        held.run("resume", **{"from": A, "from_guid": str(GA)})
        held.ws.receive.side_effect = stream_script(header=incremental_header(GC))
        held.run("resume", **{"from": C, "from_guid": str(GC)})
        assert held.sent[-1] == {"status": "ok"} and "reason" in held.sent[0]

    # -- logging --------------------------------------------------------------------------------

    def test_a_refusal_is_one_server_warning_with_the_reason(self, held, caplog):
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            held.run("resume", **{"from": A, "from_guid": str(GA)})
        warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1
        assert "refused (server_newer)" in warnings[0] and B in warnings[0] and "client1" in warnings[0]

    def test_a_permitted_rollback_is_one_warning_naming_what_is_destroyed(self, held, caplog):
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            held.run("resume", **{"from": A, "from_guid": str(GA), "discard": f"{GB},{GC}"})
        warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1
        assert "2 snapshot(s)" in warnings[0] and f"{B}, {C}" in warnings[0] and SERVER_DS in warnings[0]

    def test_an_ordinary_forced_receive_logs_nothing(self, held, caplog):
        with caplog.at_level(logging.INFO, logger="zfsbackup.api"):
            held.run("resume", **{"from": C, "from_guid": str(GC)})
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    def test_a_dry_run_decides_nothing(self, mocker):
        s = Stream(mocker, dry_run=True)
        s.server_has(*rows_for_stream())
        s.run("fresh", **{"from": A, "from_guid": str(GA)})
        assert s.sent == [{"status": "ok", "dry_run": True}]
        s.list.assert_not_called()

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
        assert call.kwargs["props"] == EXPECTED_PROPS
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
        assert call.kwargs["props"] == EXPECTED_PROPS
        stream.abort.assert_called_once()

    def test_the_props_are_a_fresh_dict_each_time(self, mocker):
        """A callee that mutates its argument cannot weaken the next call."""
        for _ in range(2):
            s = Stream(mocker)
            s.run("resume")
            props = s.receive.call_args.kwargs["props"]
            assert props == EXPECTED_PROPS
            props.clear()
        assert list(SERVER_OWNED_PROPERTIES) == OWNED


class TestReceiveArgv:
    """The real `zfs receive` argv (only `Popen` is mocked)."""

    def run(self, mocker, mock_subprocess, mode, *, token=None):
        mocker.patch("zfsbackup.api.read_zfsbackup_properties", return_value={})
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.api.zfs.get.value", return_value=STORED)
        authed = Authed(_remote_config())
        handler = authed.app.view_functions["backup_stream"].__wrapped__
        first = len(mock_subprocess.call_args_list)          # (a test may run several streams)
        if mode == "resume":
            mock_subprocess.setup_multi(("",))                          # receive
        elif token is None:
            mock_subprocess.setup_multi(([f"{SERVER_DS}\tfilesystem\t-\n"],), ("",))   # list, receive
        else:
            mock_subprocess.setup_multi(
                ([f"{SERVER_DS}\tfilesystem\t{token}\n"],), ("",), ("",))   # list, abort, receive
        ws = MagicMock()
        ws.receive.side_effect = [b"chunk", json.dumps({"done": True})]
        with authed.context(STREAM_PATH, query_string={"mode": mode}):
            handler(ws, "client1", "pool/data")
        assert json.loads(ws.send.call_args[0][0]) == {"status": "ok"}
        return [c[0][0][1:] for c in mock_subprocess.call_args_list[first:]]

    def receive_argv(self, argvs):
        (argv,) = [a for a in argvs if a[0] == "receive" and a[1] != "-A"]
        return argv

    def test_every_receive_carries_o_for_all_three_in_order(self, mocker, mock_subprocess):
        for mode, token in (("resume", None), ("fresh", None), ("fresh", TOKEN)):
            argv = self.receive_argv(self.run(mocker, mock_subprocess, mode, token=token))
            assert owned_in(argv) == [
                f"{PROP_CONFIG}={STORED}",
                f"{PROP_CLIENT_ID}=client1",
                f"{PROP_SOURCE_DATASET}=pool/data",
            ], (mode, token, argv)
            assert argv[-1] == SERVER_DS
            for flag in ("-F", "-s", "-u"):
                assert flag in argv

    def test_there_is_no_x_at_all(self, mocker, mock_subprocess):
        for mode in ("resume", "fresh"):
            argv = self.receive_argv(self.run(mocker, mock_subprocess, mode))
            assert "-x" not in argv
            assert argv.count("-o") == 3

    def test_fresh_and_resume_argv_are_identical(self, mocker, mock_subprocess):
        fresh = self.receive_argv(self.run(mocker, mock_subprocess, "fresh"))
        resumed = self.receive_argv(self.run(mocker, mock_subprocess, "resume"))
        assert fresh == resumed

    def test_the_receive_is_the_only_zfs_command_of_a_resume(self, mocker, mock_subprocess):
        argvs = self.run(mocker, mock_subprocess, "resume")
        assert [a[0] for a in argvs] == ["receive"]      # no `zfs set` after it

    def test_a_fresh_stream_runs_list_abort_receive_and_nothing_after(self, mocker, mock_subprocess):
        argvs = self.run(mocker, mock_subprocess, "fresh", token=TOKEN)
        assert [a[0] for a in argvs] == ["list", "receive", "receive"]
        assert argvs[1] == ["receive", "-A", SERVER_DS]


# ---------------------------------------------------------------------------
# Where the values come from
# ---------------------------------------------------------------------------


class TestStreamOwnsTheServerProperties:
    @pytest.mark.parametrize("mode", ["fresh", "resume"])
    def test_the_config_is_read_locally_from_the_server_dataset(self, stream, mode):
        stream.run(mode)
        stream.get_value.assert_called_once()
        call = stream.get_value.call_args
        assert isinstance(call.args[0], Dataset) and str(call.args[0]) == SERVER_DS
        assert call.args[1] == PROP_CONFIG
        assert call.kwargs == {"sources": "local"}          # never a received or inherited copy

    def test_the_stored_value_is_canonicalised(self, stream):
        stream.get_value.side_effect = None
        stream.get_value.return_value = non_canonical_config()
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
        stream.get_value.side_effect = [STORED, None, "junk"]
        stream.run("fresh")
        assert stream.receive.call_args.kwargs["props"][PROP_CONFIG] == STORED
        assert stream.get_value.call_count == 1

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
        s.get_value.assert_not_called()
        s.receive.assert_not_called()

    def test_an_ownership_conflict_is_answered_before_the_config_is_read(self, stream, source_props):
        source_props.return_value = {PROP_SOURCE_DATASET: "tank/other"}
        stream.run("fresh")
        assert "refusing to mix in" in stream.sent[0]["error"]
        stream.get_value.assert_not_called()
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
        stream.get_value.side_effect = None
        stream.get_value.return_value = value
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
        stream.get_value.side_effect = None
        stream.get_value.return_value = value
        stream.token(TOKEN)
        stream.run(mode)
        self.assert_refused(stream)

    def test_the_refusal_is_logged_without_the_stored_value(self, stream, caplog):
        stream.get_value.side_effect = None
        stream.get_value.return_value = "SECRET-NOT-BASE64!"
        with caplog.at_level(logging.WARNING, logger="zfsbackup.api"):
            stream.run("resume")
        assert any("negotiate first" in r.getMessage() for r in caplog.records)
        assert "SECRET-NOT-BASE64" not in caplog.text
        assert "SECRET-NOT-BASE64" not in json.dumps(stream.sent)

    def test_a_failing_read_is_its_own_error_and_nothing_runs(self, stream, caplog):
        stream.get_value.side_effect = RuntimeError("cannot open: permission denied for tank/secret")
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
        stream.get_value.side_effect = [None, STORED]
        stream.run("resume")
        stream.ws.receive.side_effect = [json.dumps({"done": True})]
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
        assert stream.order == ["get", "receive", "write", "close", ("send", {"status": "ok"})]
        stream.set.assert_not_called()

    def test_a_second_stream_does_not_stamp_either(self, stream):
        stream.run("resume")
        stream.ws.receive.side_effect = [json.dumps({"done": True})]
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
        stream.ws.receive.side_effect = ConnectionClosed(None, None)
        stream.run("resume")
        stream.set.assert_not_called()

    def test_a_timeout(self, stream):
        stream.ws.receive.side_effect = [b"chunk", None]
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
        assert call.kwargs["props"] == EXPECTED_PROPS
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
        fresh.ws.receive.side_effect = [b"chunk", json.dumps({"done": True})]
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
        fresh.ws.receive.side_effect = [b"chunk", json.dumps({"done": True})]
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
        stream.ws.receive.side_effect = [b"chunk", json.dumps({"done": True})]
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
        fresh.get_value.side_effect = None
        fresh.get_value.return_value = damaged
        fresh.run("fresh")
        assert fresh.sent == [NOT_NEGOTIATED]
        fresh.receive.assert_not_called()
        fresh.abort.assert_not_called()
        fresh.list.assert_not_called()

    def test_an_unreadable_local_config_is_its_own_error_and_the_entry_is_not_used(self, fresh):
        fresh.exists.return_value = True
        fresh.get_value.side_effect = RuntimeError("cannot open: boom")
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
        ws.receive.side_effect = [b"chunk", json.dumps({"done": True})]
        with authed.context(STREAM_PATH, query_string={"mode": "fresh"}):
            handler(ws, "client1", "pool/data")
        assert json.loads(ws.send.call_args[0][0]) == {"status": "ok"}
        (argv,) = [c[0][0][1:] for c in mock_subprocess.call_args_list]
        assert argv[0] == "receive" and argv[-1] == SERVER_DS
        assert "-F" not in argv and "-x" not in argv
        for flag in ("-s", "-u"):
            assert flag in argv
        assert owned_in(argv) == [
            f"{PROP_CONFIG}={STORED}", f"{PROP_CLIENT_ID}=client1", f"{PROP_SOURCE_DATASET}=pool/data"]


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
        assert stream.receive.call_args.kwargs["props"] == EXPECTED_PROPS

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
        stream.get_value.side_effect = [STORED, None, "junk"]
        stream.run("fresh")
        assert stream.receive.call_args.kwargs["props"][PROP_CONFIG] == STORED
        assert stream.get_value.call_count == 1

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
        stream.ws.receive.side_effect = [json.dumps({"done": True})]
        stream.run("resume")
        assert stream.sent[-1] == {"status": "ok"}

    def test_a_disconnected_client_gets_nothing(self, stream):
        stream.writer.close.side_effect = RuntimeError(OVERWRITE)
        stream.ws.send.side_effect = ConnectionClosed(None, None)
        stream.run("fresh")                               # must not raise
        assert stream.claimed == set()

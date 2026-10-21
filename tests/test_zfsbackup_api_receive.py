"""The receive side of `/stream`: a client's stream cannot set the server's ownership and config
properties (production readiness plan B-1b), mocked.

On real pools (OpenZFS 2.1.5, 2.2.2, 2.4.1) `zfs receive -x <property>` does NOT protect a local
value on a full `-F` receive of a `zfs send -p` stream: the stream's copy replaces it (source
`received`). So the server puts its own values in effect with `-o` ON the receive:

* before anything is destroyed (before the fresh-mode `receive -A`) it reads the dataset's LOCAL
  `org.zfsbackup:config` (stored by negotiate, canonicalised) -- a dataset that does not exist, or
  has no usable config, is REFUSED ("negotiate before streaming");
* `props = {config, client_id (the bound id), source_dataset (the URL)}` goes to
  `zfs.receive.filesystem(..., force=True, save=True, mount=False)` for `fresh` and `resume` alike,
  with no `reset` (`-x` and `-o` of one name are mutually exclusive);
* there is no `zfs set` after the receive; `zfs set` of all three happens only in the recreate
  branch (the abort destroyed the dataset a full receive had created).

Layout: `TestReceiveFlags` (the call), `TestReceiveArgv` (the real argv), `TestStreamOwnsTheServerProperties`
(what is read, from where, and whose names), `TestStreamNeedsANegotiatedConfig` (the refusals),
`TestNoStampAfterTheReceive`, `TestNoStampAfterAFailedReceive`.
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
from _api_support import Authed
from libzfseasy.types import Dataset
from zfsbackup.api import RECEIVE_EXCLUDED_PROPERTIES, canonical_dataset_config
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
        assert list(RECEIVE_EXCLUDED_PROPERTIES) == OWNED
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
        assert list(RECEIVE_EXCLUDED_PROPERTIES) == OWNED


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

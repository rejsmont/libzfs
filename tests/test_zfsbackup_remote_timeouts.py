"""Phase 3, item 18.4 (client half): every blocking websocket operation in
`RemoteBackupManager._transfer` is bounded, and a failed connect never leaves a
`zfs send` running. (The server's `receive(timeout=...)` is in
`test_zfsbackup_stream_gate.py`.)

A live reload defers a worker switch for as long as a transfer is in flight, so
a peer that hangs forever would turn "deferred" into "never".
"""

import ast
import json
import socket
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import websocket

from libzfseasy.zfs import StreamHandle
from zfsbackup import remote
from zfsbackup.backup_manager import DatasetManager
from zfsbackup.config import BackupConfig, DatasetConfig, Destination, RemoteDatasetConfig
from zfsbackup.remote import (
    WS_CONNECT_TIMEOUT,
    WS_FINAL_ACK_TIMEOUT,
    WS_IDLE_TIMEOUT,
    RemoteBackupManager,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
URL = "http://backup.example.com"


class FakeStream:
    """A `StreamHandle` look-alike that counts its closes."""

    def __init__(self, chunks=(b"data",)):
        self._chunks = list(chunks)
        self.closes = 0

    def read(self, n):
        return self._chunks.pop(0) if self._chunks else b""

    def close(self):
        self.closes += 1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


@pytest.fixture
def rbm(tmp_path):
    config = BackupConfig(
        datasets=[DatasetConfig(name="pool/data", remote=[RemoteDatasetConfig(destination="offsite")])],
        destinations={"offsite": Destination(url=URL)},
        client_id_file=tmp_path / "client_id",
    )
    manager = DatasetManager(config)
    return RemoteBackupManager(config, manager), manager.datasets[0]


def snapshot(name="autosnap_20240101120000"):
    snap = MagicMock()
    snap.name = name
    return snap


def transfer(rbm_dsi, mocker, stream, ws):
    manager, dsi = rbm_dsi
    mocker.patch.object(manager, "_get_resume_stream", return_value=stream)
    mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=ws)
    set_anchor = mocker.patch.object(manager.local_manager, "set_anchor")
    result = manager._transfer(URL, "client1", dsi, snapshot(), None, "offsite")
    return result, set_anchor


def ok_ws():
    ws = MagicMock()
    ws.recv.return_value = json.dumps({"status": "ok"})
    return ws


class TestTimeouts:
    def test_the_constants(self):
        assert (WS_CONNECT_TIMEOUT, WS_IDLE_TIMEOUT, WS_FINAL_ACK_TIMEOUT) == (30, 600, 3600)
        assert WS_CONNECT_TIMEOUT < WS_IDLE_TIMEOUT < WS_FINAL_ACK_TIMEOUT

    def test_connect_then_idle_then_final_ack_in_that_order(self, rbm, mocker):
        ws = ok_ws()
        result, _ = transfer(rbm, mocker, FakeStream([b"a", b"b"]), ws)
        assert result is True
        names = [c[0] for c in ws.method_calls if c[0] in ("connect", "settimeout", "send_binary", "send", "recv")]
        assert names == ["connect", "settimeout", "send_binary", "send_binary", "send", "settimeout", "recv"]
        assert ws.connect.call_args.kwargs == {"timeout": WS_CONNECT_TIMEOUT}
        timeouts = [c.args[0] for c in ws.settimeout.call_args_list]
        assert timeouts == [WS_IDLE_TIMEOUT, WS_FINAL_ACK_TIMEOUT]

    def test_the_final_ack_timeout_is_set_after_done_and_before_the_recv(self, rbm, mocker):
        ws = ok_ws()
        transfer(rbm, mocker, FakeStream(), ws)
        calls = [c for c in ws.method_calls if c[0] in ("send", "settimeout", "recv")]
        assert [c[0] for c in calls] == ["settimeout", "send", "settimeout", "recv"]
        assert json.loads(calls[1].args[0]) == {"done": True}
        assert calls[2].args == (WS_FINAL_ACK_TIMEOUT,)

    def test_the_connect_url_is_unchanged(self, rbm, mocker):
        ws = ok_ws()
        transfer(rbm, mocker, FakeStream(), ws)
        assert ws.connect.call_args.args == ("ws://backup.example.com/backup/client1/pool/data/stream",)


class TestSendTimeout:
    @pytest.mark.parametrize(
        "exc", [websocket.WebSocketTimeoutException("timed out"), socket.timeout("timed out"), TimeoutError("t")]
    )
    def test_a_send_binary_timeout_is_false_with_the_anchor_unchanged_and_the_stream_closed(self, rbm, mocker, exc):
        stream = FakeStream([b"a", b"b"])
        ws = ok_ws()
        ws.send_binary.side_effect = exc
        result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        set_anchor.assert_not_called()
        assert stream.closes == 1, "the zfs send handle must be closed (EPIPE, then reaped) exactly once"
        ws.close.assert_called()
        ws.send.assert_not_called()         # never says 'done' for a transfer that did not finish
        ws.recv.assert_not_called()
        # a timeout is a silent peer: no 2 s refusal probe (that is for BrokenPipe and friends)
        assert [c.args[0] for c in ws.settimeout.call_args_list] == [WS_IDLE_TIMEOUT, 5]

    def test_a_timeout_waiting_for_the_ack_is_false_and_leaves_the_anchor(self, rbm, mocker):
        stream = FakeStream()
        ws = ok_ws()
        ws.recv.side_effect = websocket.WebSocketTimeoutException("no ack")
        result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        set_anchor.assert_not_called()
        assert stream.closes == 1

    def test_the_failure_is_logged(self, rbm, mocker, caplog):
        ws = ok_ws()
        ws.send_binary.side_effect = websocket.WebSocketTimeoutException("timed out")
        with caplog.at_level("ERROR"):
            transfer(rbm, mocker, FakeStream(), ws)
        assert "Transfer failed for pool/data" in caplog.text and "timed out" in caplog.text


class TestConnectFailure:
    @pytest.mark.parametrize(
        "exc", [ConnectionRefusedError("refused"), websocket.WebSocketTimeoutException("t"), OSError("net down")]
    )
    def test_connect_raising_is_false_and_the_stream_is_closed_exactly_once(self, rbm, mocker, exc):
        stream = FakeStream()
        ws = ok_ws()
        ws.connect.side_effect = exc
        result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        assert stream.closes == 1
        ws.close.assert_called()
        set_anchor.assert_not_called()
        ws.send_binary.assert_not_called()

    def test_a_failing_stream_close_after_a_failed_connect_is_swallowed(self, rbm, mocker):
        stream = FakeStream()
        stream.close = MagicMock(side_effect=Exception("zfs send exited 1"))
        ws = ok_ws()
        ws.connect.side_effect = ConnectionRefusedError("refused")
        result, _ = transfer(rbm, mocker, stream, ws)
        assert result is False
        stream.close.assert_called_once()

    def test_a_failing_ws_close_is_swallowed(self, rbm, mocker):
        stream = FakeStream()
        ws = ok_ws()
        ws.connect.side_effect = ConnectionRefusedError("refused")
        ws.close.side_effect = OSError("already closed")
        result, _ = transfer(rbm, mocker, stream, ws)
        assert result is False
        assert stream.closes == 1

    def test_a_failing_ws_close_after_success_does_not_fail_the_transfer(self, rbm, mocker):
        ws = ok_ws()
        ws.close.side_effect = OSError("already closed")
        result, set_anchor = transfer(rbm, mocker, FakeStream(), ws)
        assert result is True
        set_anchor.assert_called_once()

    def test_a_settimeout_failure_after_connect_closes_the_stream_once(self, rbm, mocker):
        stream = FakeStream()
        ws = ok_ws()
        ws.settimeout.side_effect = OSError("bad socket")
        result, _ = transfer(rbm, mocker, stream, ws)
        assert result is False
        assert stream.closes == 1, "settimeout is before `with stream:`, so the finally closes it"


class TestNormalPath:
    def test_success_closes_the_stream_once_via_the_with_block(self, rbm, mocker):
        stream = FakeStream([b"a", b"b"])
        result, set_anchor = transfer(rbm, mocker, stream, ok_ws())
        assert result is True
        assert stream.closes == 1, "no double close on the normal path"
        set_anchor.assert_called_once()

    def test_a_server_error_closes_once_and_leaves_the_anchor(self, rbm, mocker):
        stream = FakeStream()
        ws = ok_ws()
        ws.recv.return_value = json.dumps({"status": "error", "error": "disk full"})
        result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        assert stream.closes == 1
        set_anchor.assert_not_called()

    def test_every_chunk_is_forwarded(self, rbm, mocker):
        ws = ok_ws()
        transfer(rbm, mocker, FakeStream([b"1", b"2", b"3"]), ws)
        assert [c.args[0] for c in ws.send_binary.call_args_list] == [b"1", b"2", b"3"]

    def test_dry_run_opens_no_websocket(self, rbm, mocker):
        manager, dsi = rbm
        manager.config.dry_run = True
        ws_cls = mocker.patch("zfsbackup.remote.websocket.WebSocket")
        assert manager._transfer(URL, "c", dsi, snapshot(), None, "offsite") is True
        ws_cls.assert_not_called()


class TestRealStreamHandle:
    """A real child process behind a real `StreamHandle`: closing the handle
    EPIPEs and reaps an endless writer."""

    def _endless(self):
        code = "import sys\nb = b'x' * 65536\nwhile True:\n    sys.stdout.buffer.write(b); sys.stdout.buffer.flush()\n"
        proc = subprocess.Popen(
            [sys.executable, "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=str(REPO_ROOT)
        )
        return proc, StreamHandle(proc, proc.stdout)

    def test_a_send_timeout_reaps_the_endless_writer(self, rbm, mocker):
        proc, handle = self._endless()
        ws = ok_ws()
        ws.send_binary.side_effect = websocket.WebSocketTimeoutException("timed out")
        try:
            result, set_anchor = transfer(rbm, mocker, handle, ws)
            assert result is False
            assert proc.poll() is not None, "the zfs send stand-in is still running"
            set_anchor.assert_not_called()
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(10)
            for pipe in (proc.stdout, proc.stderr):
                try:
                    pipe.close()
                except Exception:
                    pass

    def test_a_failed_connect_reaps_the_endless_writer(self, rbm, mocker):
        proc, handle = self._endless()
        ws = ok_ws()
        ws.connect.side_effect = ConnectionRefusedError("refused")
        try:
            result, _ = transfer(rbm, mocker, handle, ws)
            assert result is False
            assert proc.poll() is not None
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(10)
            for pipe in (proc.stdout, proc.stderr):
                try:
                    pipe.close()
                except Exception:
                    pass


class TestLayering:
    def test_remote_does_not_import_api(self):
        tree = ast.parse(Path(remote.__file__).read_text())
        modules = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules += [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                modules.append(node.module or "")
        assert not [m for m in modules if m == "zfsbackup.api" or m.endswith(".api")], modules

    def test_importing_remote_loads_no_api_module(self):
        script = (
            "import sys\nimport zfsbackup.remote\n"
            "assert 'zfsbackup.api' not in sys.modules, 'remote imported api'\n"
            "assert 'flask' not in sys.modules\nprint('OK')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=60, cwd=str(REPO_ROOT)
        )
        assert result.returncode == 0, result.stderr
        assert "OK" in result.stdout

    def test_api_takes_its_timeout_from_remote(self):
        from zfsbackup import api

        assert api.WS_IDLE_TIMEOUT is remote.WS_IDLE_TIMEOUT


def sequence(ws):
    """The (method, args) calls on `ws` that matter, in order."""
    return [
        (c[0], c.args[0] if c.args else None)
        for c in ws.method_calls
        if c[0] in ("connect", "settimeout", "send_binary", "send", "recv", "close")
    ]


class TestCloseHandshakeAndStreamClose:
    def test_the_failed_path_shortens_the_socket_timeout_before_closing(self, rbm, mocker):
        ws = ok_ws()
        ws.send_binary.side_effect = websocket.WebSocketTimeoutException("timed out")
        transfer(rbm, mocker, FakeStream(), ws)
        seq = sequence(ws)
        assert seq[-2:] == [("settimeout", 5), ("close", None)]
        assert [a for n, a in seq if n == "settimeout"] == [WS_IDLE_TIMEOUT, 5]

    def test_the_non_ok_path_does_too(self, rbm, mocker):
        ws = ok_ws()
        ws.recv.return_value = json.dumps({"status": "error", "error": "disk full"})
        result, _ = transfer(rbm, mocker, FakeStream(), ws)
        assert result is False
        seq = sequence(ws)
        assert seq[-2:] == [("settimeout", 5), ("close", None)]
        assert [a for n, a in seq if n == "settimeout"] == [WS_IDLE_TIMEOUT, WS_FINAL_ACK_TIMEOUT, 5]

    def test_a_connect_failure_does_too(self, rbm, mocker):
        ws = ok_ws()
        ws.connect.side_effect = ConnectionRefusedError("refused")
        transfer(rbm, mocker, FakeStream(), ws)
        assert sequence(ws)[-2:] == [("settimeout", 5), ("close", None)]

    def test_success_never_shortens_the_timeout(self, rbm, mocker):
        ws = ok_ws()
        result, _ = transfer(rbm, mocker, FakeStream([b"a"]), ws)
        assert result is True
        assert 5 not in [a for n, a in sequence(ws) if n == "settimeout"]
        assert [n for n, _ in sequence(ws)] == [
            "connect", "settimeout", "send_binary", "send", "settimeout", "recv", "close",
        ]

    def test_the_other_settimeout_order_is_kept(self, rbm, mocker):
        ws = ok_ws()
        transfer(rbm, mocker, FakeStream(), ws)
        names = [n for n, _ in sequence(ws)]
        assert names.index("connect") < names.index("settimeout") < names.index("send") < names.index("recv")
        assert [a for n, a in sequence(ws) if n == "settimeout"] == [WS_IDLE_TIMEOUT, WS_FINAL_ACK_TIMEOUT]

    def test_a_failing_settimeout_in_the_close_path_does_not_raise(self, rbm, mocker):
        ws = ok_ws()
        ws.send_binary.side_effect = websocket.WebSocketTimeoutException("timed out")
        calls = []

        def settimeout(value):
            calls.append(value)
            if value == 5:
                raise OSError("socket already closed")

        ws.settimeout.side_effect = settimeout
        result, _ = transfer(rbm, mocker, FakeStream(), ws)
        assert result is False and calls == [WS_IDLE_TIMEOUT, 5]
        ws.close.assert_called_once()            # a failing settimeout must not skip close()

    def test_the_logged_error_says_timed_out_even_when_the_stream_close_raises(self, rbm, mocker, caplog):
        """A killed `zfs send` makes `close()` raise its own empty failure; the
        ORIGINAL error is what must be reported."""
        stream = FakeStream()
        stream.close = MagicMock(side_effect=Exception(""))
        ws = ok_ws()
        ws.send_binary.side_effect = websocket.WebSocketTimeoutException("timed out")
        with caplog.at_level("ERROR"):
            result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        assert "timed out" in caplog.text
        stream.close.assert_called_once()
        set_anchor.assert_not_called()

    @pytest.mark.parametrize("scenario", ["success", "send-timeout", "connect-failure", "server-error"])
    def test_the_stream_is_closed_exactly_once(self, rbm, mocker, scenario):
        stream = FakeStream([b"a", b"b"])
        ws = ok_ws()
        if scenario == "send-timeout":
            ws.send_binary.side_effect = websocket.WebSocketTimeoutException("timed out")
        elif scenario == "connect-failure":
            ws.connect.side_effect = ConnectionRefusedError("refused")
        elif scenario == "server-error":
            ws.recv.return_value = json.dumps({"status": "error", "error": "no"})
        result, _ = transfer(rbm, mocker, stream, ws)
        assert result is (scenario == "success")
        assert stream.closes == 1

    def test_the_stream_is_closed_before_done_is_sent(self, rbm, mocker):
        order = []
        stream = FakeStream([b"a"])
        real_close = stream.close
        stream.close = lambda: (order.append("close"), real_close())[1]
        ws = ok_ws()
        ws.send.side_effect = lambda *a: order.append("done")
        transfer(rbm, mocker, stream, ws)
        assert order == ["close", "done"]

    def test_a_raising_close_on_a_normal_end_is_a_failed_transfer(self, rbm, mocker, caplog):
        """`zfs send` exited non-zero after the last chunk: do not tell the
        server 'done', do not move the anchor."""
        stream = FakeStream([b"a"])
        stream.close = MagicMock(side_effect=Exception("cannot send: dataset busy"))
        ws = ok_ws()
        with caplog.at_level("ERROR"):
            result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        assert "Transfer failed" in caplog.text and "dataset busy" in caplog.text
        ws.send.assert_not_called()
        set_anchor.assert_not_called()
        assert sequence(ws)[-2:] == [("settimeout", 5), ("close", None)]

    def test_a_keyboard_interrupt_in_the_send_loop_closes_the_stream_and_reraises(self, rbm, mocker):
        manager, dsi = rbm
        stream = FakeStream()
        stream.read = MagicMock(side_effect=KeyboardInterrupt)
        ws = ok_ws()
        mocker.patch.object(manager, "_get_resume_stream", return_value=stream)
        mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=ws)
        set_anchor = mocker.patch.object(manager.local_manager, "set_anchor")
        with pytest.raises(KeyboardInterrupt):
            manager._transfer(URL, "client1", dsi, snapshot(), None, "offsite")
        assert stream.closes == 1
        ws.close.assert_called()
        set_anchor.assert_not_called()
        ws.send.assert_not_called()

    def test_a_close_that_raises_during_a_keyboard_interrupt_does_not_mask_it(self, rbm, mocker):
        manager, dsi = rbm
        stream = FakeStream()
        stream.read = MagicMock(side_effect=KeyboardInterrupt)
        stream.close = MagicMock(side_effect=Exception("killed by SIGPIPE"))
        mocker.patch.object(manager, "_get_resume_stream", return_value=stream)
        mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=ok_ws())
        with pytest.raises(KeyboardInterrupt):
            manager._transfer(URL, "client1", dsi, snapshot(), None, "offsite")

    def test_a_failing_ws_close_in_the_failed_path_still_closes_nothing_twice(self, rbm, mocker):
        stream = FakeStream()
        ws = ok_ws()
        ws.connect.side_effect = ConnectionRefusedError("refused")
        ws.close.side_effect = OSError("closed")
        result, _ = transfer(rbm, mocker, stream, ws)
        assert result is False and stream.closes == 1


class TestCleanupNeverEscapesOrSkips:
    def test_a_failing_settimeout_on_a_failed_transfer_still_closes_the_websocket(self, rbm, mocker):
        stream = FakeStream()
        ws = ok_ws()
        ws.send_binary.side_effect = websocket.WebSocketTimeoutException("timed out")
        ws.settimeout.side_effect = lambda v: (_ for _ in ()).throw(OSError("closed")) if v == 5 else None
        result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        ws.close.assert_called_once()
        assert stream.closes == 1
        set_anchor.assert_not_called()

    def test_every_settimeout_failing_on_a_connect_failure_still_closes_both(self, rbm, mocker):
        stream = FakeStream()
        ws = ok_ws()
        ws.connect.side_effect = ConnectionRefusedError("refused")
        ws.settimeout.side_effect = OSError("closed")
        result, _ = transfer(rbm, mocker, stream, ws)
        assert result is False
        ws.close.assert_called_once()
        assert stream.closes == 1

    def test_a_raising_ws_close_escapes_nothing_and_the_stream_cleanup_still_runs(self, rbm, mocker):
        """connect fails (so the stream was never entered): `ws.close()` raising must
        not stop `stream.close()` -- the `zfs send` would block forever."""
        stream = FakeStream()
        ws = ok_ws()
        ws.connect.side_effect = ConnectionRefusedError("refused")
        ws.close.side_effect = RuntimeError("close exploded")
        result, _ = transfer(rbm, mocker, stream, ws)
        assert result is False
        assert stream.closes == 1

    def test_a_raising_ws_close_after_success_does_not_undo_the_transfer(self, rbm, mocker):
        ws = ok_ws()
        ws.close.side_effect = RuntimeError("close exploded")
        stream = FakeStream([b"a"])
        result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is True
        set_anchor.assert_called_once()
        assert stream.closes == 1

    def test_a_raising_ws_close_and_a_raising_stream_close_still_escape_nothing(self, rbm, mocker):
        stream = FakeStream()
        stream.close = MagicMock(side_effect=Exception("zfs send exited 1"))
        ws = ok_ws()
        ws.connect.side_effect = ConnectionRefusedError("refused")
        ws.close.side_effect = RuntimeError("close exploded")
        result, _ = transfer(rbm, mocker, stream, ws)
        assert result is False
        stream.close.assert_called_once()


# ---------------------------------------------------------------------------
# A1: the server's retryable "reloading" refusal is INFO, not an ERROR.
#
# While draining for a live reload the API worker sends
# `{"error": "server is reloading; retry later", "retry": true}` as a text
# message and returns without reading. The refusal can arrive where the final
# ack is expected, or -- when the client's first `send_binary` already hit the
# closed socket -- only be discoverable by one bounded read after the failure.
# ---------------------------------------------------------------------------

import logging  # noqa: E402

from zfsbackup.remote import _is_reloading_refusal, _peer_sent_reloading_refusal  # noqa: E402

REFUSAL = {"error": "server is reloading; retry later", "retry": True}
REFUSAL_JSON = json.dumps(REFUSAL)
RELOADING_MSG = "is reloading; skipping pool/data this cycle"


def levels(caplog, text=None):
    """Levels of the captured records (optionally only those containing `text`)."""
    return [r.levelname for r in caplog.records if text is None or text in r.getMessage()]


def settimeouts(ws):
    return [c.args[0] for c in ws.settimeout.call_args_list]


class TestIsReloadingRefusal:
    @pytest.mark.parametrize(
        "result, expected",
        [
            (REFUSAL, True),
            ({"status": "error", "error": "reloading", "retry": True}, True),
            ({"retry": True}, True),
            ({"error": "x", "retry": "yes"}, True),            # truthy, not only `True`
            ({"status": "error", "error": "disk full"}, False),
            ({"status": "error", "error": "x", "retry": False}, False),
            ({"error": "x", "retry": 0}, False),
            ({"error": "x", "retry": None}, False),
            ({"status": "ok"}, False),
            ({"status": "ok", "retry": True}, False),          # an ack is never a refusal
            ({}, False),
            ([], False),
            (None, False),
            ("retry", False),
            (["retry", True], False),
            (1, False),
        ],
    )
    def test_the_table(self, result, expected):
        assert _is_reloading_refusal(result) is expected


class TestPeerSentReloadingRefusal:
    def test_a_refusal_is_recognised_after_a_two_second_timeout(self):
        ws = MagicMock()
        ws.recv.return_value = REFUSAL_JSON
        assert _peer_sent_reloading_refusal(ws) is True
        assert [c[0] for c in ws.method_calls] == ["settimeout", "recv"]
        ws.settimeout.assert_called_once_with(2)
        ws.recv.assert_called_once_with()

    @pytest.mark.parametrize(
        "reply",
        [
            json.dumps({"status": "ok"}),
            json.dumps({"status": "error", "error": "disk full"}),
            json.dumps([REFUSAL]),
            "not json",
            "",
            b"",
            None,
        ],
    )
    def test_anything_else_is_not_a_refusal(self, reply):
        ws = MagicMock()
        ws.recv.return_value = reply
        assert _peer_sent_reloading_refusal(ws) is False

    @pytest.mark.parametrize(
        "exc",
        [
            websocket.WebSocketTimeoutException("t"),
            websocket.WebSocketConnectionClosedException("closed"),
            OSError("reset"),
            ValueError("x"),
            RuntimeError("y"),
        ],
    )
    def test_a_recv_error_is_swallowed(self, exc):
        ws = MagicMock()
        ws.recv.side_effect = exc
        assert _peer_sent_reloading_refusal(ws) is False

    def test_a_failing_settimeout_is_swallowed_and_recv_is_not_attempted(self):
        ws = MagicMock()
        ws.settimeout.side_effect = OSError("socket closed")
        assert _peer_sent_reloading_refusal(ws) is False
        ws.recv.assert_not_called()


class TestRefusalAsTheFinalAck:
    def test_a_retryable_refusal_is_info_not_error_and_leaves_the_anchor(self, rbm, mocker, caplog):
        stream = FakeStream()
        ws = ok_ws()
        ws.recv.return_value = REFUSAL_JSON
        with caplog.at_level(logging.DEBUG):
            result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        set_anchor.assert_not_called()
        assert "ERROR" not in levels(caplog)
        assert "WARNING" not in levels(caplog)
        assert levels(caplog, RELOADING_MSG) == ["INFO"]
        assert f"server {URL} {RELOADING_MSG}" in caplog.text
        assert "Backup complete" not in caplog.text
        assert stream.closes == 1
        ws.recv.assert_called_once()                  # no second (probe) read
        assert settimeouts(ws) == [WS_IDLE_TIMEOUT, WS_FINAL_ACK_TIMEOUT, 5]
        assert sequence(ws)[-2:] == [("settimeout", 5), ("close", None)]

    def test_a_refusal_with_an_explicit_error_status_is_still_a_refusal(self, rbm, mocker, caplog):
        ws = ok_ws()
        ws.recv.return_value = json.dumps({"status": "error", "error": "reloading", "retry": True})
        with caplog.at_level(logging.DEBUG):
            result, set_anchor = transfer(rbm, mocker, FakeStream(), ws)
        assert result is False
        set_anchor.assert_not_called()
        assert "ERROR" not in levels(caplog)

    @pytest.mark.parametrize(
        "reply",
        [
            {"status": "error", "error": "x"},
            {"error": "x"},
            {"status": "error", "error": "x", "retry": False},
            {},
        ],
    )
    def test_a_non_retryable_error_is_an_error(self, rbm, mocker, caplog, reply):
        stream = FakeStream()
        ws = ok_ws()
        ws.recv.return_value = json.dumps(reply)
        with caplog.at_level(logging.DEBUG):
            result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        set_anchor.assert_not_called()
        assert levels(caplog, "Transfer failed for pool/data") == ["ERROR"]
        assert reply.get("error", "unknown error") in caplog.text
        assert RELOADING_MSG not in caplog.text
        assert stream.closes == 1

    def test_an_ok_ack_is_unchanged(self, rbm, mocker, caplog):
        with caplog.at_level(logging.DEBUG):
            result, set_anchor = transfer(rbm, mocker, FakeStream(), ok_ws())
        assert result is True
        set_anchor.assert_called_once()
        assert "ERROR" not in levels(caplog)
        assert RELOADING_MSG not in caplog.text


class TestRefusalDiscoveredAfterASendFailure:
    @pytest.mark.parametrize(
        "exc",
        [
            BrokenPipeError("broken pipe"),
            ConnectionResetError("reset"),
            websocket.WebSocketConnectionClosedException("Connection is already closed."),
            OSError("bad file descriptor"),
        ],
    )
    def test_a_refusal_behind_a_failed_send_is_info(self, rbm, mocker, caplog, exc):
        stream = FakeStream([b"a", b"b"])
        ws = ok_ws()
        ws.send_binary.side_effect = exc
        ws.recv.return_value = REFUSAL_JSON
        with caplog.at_level(logging.DEBUG):
            result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        set_anchor.assert_not_called()
        assert "ERROR" not in levels(caplog)
        assert levels(caplog, RELOADING_MSG) == ["INFO"]
        assert "Transfer failed" not in caplog.text
        assert stream.closes == 1
        ws.close.assert_called()
        ws.send.assert_not_called()                   # never says 'done'
        ws.recv.assert_called_once()

    def test_the_probe_is_a_bounded_read_between_the_failed_send_and_the_close(self, rbm, mocker):
        ws = ok_ws()
        ws.send_binary.side_effect = BrokenPipeError("broken pipe")
        ws.recv.return_value = REFUSAL_JSON
        transfer(rbm, mocker, FakeStream(), ws)
        assert sequence(ws) == [
            ("connect", ws.connect.call_args.args[0]),
            ("settimeout", WS_IDLE_TIMEOUT),
            ("send_binary", b"data"),
            ("settimeout", 2),
            ("recv", None),
            ("settimeout", 5),
            ("close", None),
        ]

    @pytest.mark.parametrize(
        "probe",
        [
            {"side_effect": websocket.WebSocketTimeoutException("probe timed out")},
            {"side_effect": websocket.WebSocketConnectionClosedException("closed")},
            {"side_effect": OSError("reset")},
            {"return_value": ""},
            {"return_value": "not json"},
            {"return_value": json.dumps({"status": "ok"})},
            {"return_value": json.dumps({"status": "error", "error": "disk full"})},
            {"return_value": json.dumps({"error": "x", "retry": False})},
        ],
        ids=["probe-timeout", "probe-closed", "probe-oserror", "empty", "non-json", "ok-json", "error-json",
             "retry-false"],
    )
    def test_no_refusal_behind_the_failure_keeps_the_original_error(self, rbm, mocker, caplog, probe):
        stream = FakeStream()
        ws = ok_ws()
        ws.send_binary.side_effect = BrokenPipeError("pipe went away")
        ws.recv.configure_mock(**probe)
        with caplog.at_level(logging.DEBUG):
            result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        set_anchor.assert_not_called()
        assert levels(caplog, "Transfer failed for pool/data") == ["ERROR"]
        assert "pipe went away" in caplog.text
        assert RELOADING_MSG not in caplog.text
        assert stream.closes == 1
        ws.recv.assert_called_once()
        assert settimeouts(ws) == [WS_IDLE_TIMEOUT, 2, 5]
        assert sequence(ws)[-2:] == [("settimeout", 5), ("close", None)]

    def test_a_failing_probe_settimeout_keeps_the_original_error_and_still_closes(self, rbm, mocker, caplog):
        ws = ok_ws()
        ws.send_binary.side_effect = BrokenPipeError("pipe went away")
        ws.recv.return_value = REFUSAL_JSON
        ws.settimeout.side_effect = lambda v: (_ for _ in ()).throw(OSError("closed")) if v != WS_IDLE_TIMEOUT else None
        with caplog.at_level(logging.DEBUG):
            result, _ = transfer(rbm, mocker, FakeStream(), ws)
        assert result is False
        assert "pipe went away" in caplog.text and RELOADING_MSG not in caplog.text
        ws.recv.assert_not_called()
        ws.close.assert_called_once()

    def test_a_refusal_behind_a_failed_done_send_is_info(self, rbm, mocker, caplog):
        """A quiet dataset fits in the kernel buffer, so the server's refusal
        first surfaces as a failure of the `done` send."""
        stream = FakeStream()
        ws = ok_ws()
        ws.send.side_effect = BrokenPipeError("broken pipe")
        ws.recv.return_value = REFUSAL_JSON
        with caplog.at_level(logging.DEBUG):
            result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        set_anchor.assert_not_called()
        assert "ERROR" not in levels(caplog)
        assert levels(caplog, RELOADING_MSG) == ["INFO"]
        assert "Transfer failed" not in caplog.text
        assert stream.closes == 1
        ws.recv.assert_called_once()
        assert settimeouts(ws) == [WS_IDLE_TIMEOUT, 2, 5]

    @pytest.mark.parametrize(
        "probe",
        [
            {"side_effect": websocket.WebSocketTimeoutException("probe timed out")},
            {"side_effect": OSError("reset")},
            {"return_value": ""},
            {"return_value": "not json"},
            {"return_value": json.dumps({"status": "error", "error": "disk full"})},
        ],
        ids=["probe-timeout", "probe-oserror", "empty", "non-json", "error-json"],
    )
    def test_a_failed_done_send_without_a_refusal_is_an_error(self, rbm, mocker, caplog, probe):
        ws = ok_ws()
        ws.send.side_effect = BrokenPipeError("done went nowhere")
        ws.recv.configure_mock(**probe)
        with caplog.at_level(logging.DEBUG):
            result, set_anchor = transfer(rbm, mocker, FakeStream(), ws)
        assert result is False
        set_anchor.assert_not_called()
        assert levels(caplog, "Transfer failed for pool/data") == ["ERROR"]
        assert "done went nowhere" in caplog.text and RELOADING_MSG not in caplog.text
        ws.recv.assert_called_once()

    def test_a_done_send_timeout_is_an_error_without_a_probe(self, rbm, mocker, caplog):
        ws = ok_ws()
        ws.send.side_effect = websocket.WebSocketTimeoutException("timed out")
        ws.recv.return_value = REFUSAL_JSON
        with caplog.at_level(logging.DEBUG):
            result, _ = transfer(rbm, mocker, FakeStream(), ws)
        assert result is False
        assert levels(caplog, "Transfer failed for pool/data") == ["ERROR"]
        ws.recv.assert_not_called()

    def test_a_stream_close_that_raises_does_not_hide_the_refusal(self, rbm, mocker, caplog):
        stream = FakeStream()
        stream.close = MagicMock(side_effect=Exception(""))   # zfs send killed by SIGPIPE
        ws = ok_ws()
        ws.send_binary.side_effect = BrokenPipeError("broken pipe")
        ws.recv.return_value = REFUSAL_JSON
        with caplog.at_level(logging.DEBUG):
            result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        set_anchor.assert_not_called()
        assert "ERROR" not in levels(caplog)
        assert levels(caplog, RELOADING_MSG) == ["INFO"]
        stream.close.assert_called_once()

    def test_a_real_zfs_send_is_reaped_when_a_refusal_follows(self, rbm, mocker):
        """The refusal path still closes the endless writer (EPIPE, then reap)."""
        proc, handle = TestRealStreamHandle()._endless()
        ws = ok_ws()
        ws.send_binary.side_effect = BrokenPipeError("broken pipe")
        ws.recv.return_value = REFUSAL_JSON
        try:
            result, set_anchor = transfer(rbm, mocker, handle, ws)
            assert result is False
            assert proc.poll() is not None
            set_anchor.assert_not_called()
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(10)
            for pipe in (proc.stdout, proc.stderr):
                try:
                    pipe.close()
                except Exception:
                    pass


class TestNoProbeWhenThePeerCannotHaveLeftARefusal:
    """In each of these `recv` would answer with a refusal if asked; the
    transfer must be a plain ERROR because the probe must not run."""

    @pytest.mark.parametrize(
        "exc",
        [websocket.WebSocketTimeoutException("timed out"), socket.timeout("timed out"), TimeoutError("t")],
        ids=["ws-timeout", "socket-timeout", "timeout-error"],
    )
    def test_a_send_timeout_is_an_error_without_a_probe(self, rbm, mocker, caplog, exc):
        stream = FakeStream()
        ws = ok_ws()
        ws.send_binary.side_effect = exc
        ws.recv.return_value = REFUSAL_JSON
        with caplog.at_level(logging.DEBUG):
            result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        set_anchor.assert_not_called()
        assert levels(caplog, "Transfer failed for pool/data") == ["ERROR"]
        assert RELOADING_MSG not in caplog.text
        ws.recv.assert_not_called()
        assert settimeouts(ws) == [WS_IDLE_TIMEOUT, 5]
        assert stream.closes == 1

    def test_a_final_ack_timeout_is_an_error_without_a_second_recv(self, rbm, mocker, caplog):
        stream = FakeStream()
        ws = ok_ws()
        ws.recv.side_effect = [websocket.WebSocketTimeoutException("no ack"), REFUSAL_JSON]
        with caplog.at_level(logging.DEBUG):
            result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        set_anchor.assert_not_called()
        assert levels(caplog, "Transfer failed for pool/data") == ["ERROR"]
        assert "no ack" in caplog.text and RELOADING_MSG not in caplog.text
        ws.recv.assert_called_once()
        assert settimeouts(ws) == [WS_IDLE_TIMEOUT, WS_FINAL_ACK_TIMEOUT, 5]
        assert stream.closes == 1

    def test_a_non_zero_zfs_send_at_the_end_is_an_error_without_a_probe(self, rbm, mocker, caplog):
        stream = FakeStream([b"a"])
        stream.close = MagicMock(side_effect=Exception("cannot send: dataset busy"))
        ws = ok_ws()
        ws.recv.return_value = REFUSAL_JSON
        with caplog.at_level(logging.DEBUG):
            result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        set_anchor.assert_not_called()
        assert levels(caplog, "Transfer failed for pool/data") == ["ERROR"]
        assert "dataset busy" in caplog.text and RELOADING_MSG not in caplog.text
        ws.recv.assert_not_called()
        ws.send.assert_not_called()
        assert settimeouts(ws) == [WS_IDLE_TIMEOUT, 5]

    # (The `done` message is part of the send phase -- see TestRefusalDiscoveredAfterASendFailure.)

    @pytest.mark.parametrize("failing", ["connect", "settimeout"])
    def test_a_failure_before_the_send_loop_is_an_error_without_a_probe(self, rbm, mocker, caplog, failing):
        ws = ok_ws()
        ws.recv.return_value = REFUSAL_JSON
        if failing == "connect":
            ws.connect.side_effect = ConnectionRefusedError("refused")
        else:
            ws.settimeout.side_effect = OSError("bad socket")
        with caplog.at_level(logging.DEBUG):
            result, _ = transfer(rbm, mocker, FakeStream(), ws)
        assert result is False
        assert levels(caplog, "Transfer failed for pool/data") == ["ERROR"]
        assert RELOADING_MSG not in caplog.text
        ws.recv.assert_not_called()

    def test_a_keyboard_interrupt_in_the_send_loop_is_not_probed(self, rbm, mocker):
        manager, dsi = rbm
        stream = FakeStream()
        stream.read = MagicMock(side_effect=KeyboardInterrupt)
        ws = ok_ws()
        ws.recv.return_value = REFUSAL_JSON
        mocker.patch.object(manager, "_get_resume_stream", return_value=stream)
        mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=ws)
        with pytest.raises(KeyboardInterrupt):
            manager._transfer(URL, "client1", dsi, snapshot(), None, "offsite")
        ws.recv.assert_not_called()

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

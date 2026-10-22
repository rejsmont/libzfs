"""Phase 3, item 18.4 (client half): every blocking websocket operation in
`RemoteBackupManager._transfer` is bounded, and a failed connect never leaves a
`zfs send` running. (The server's `receive(timeout=...)` is in
`test_zfsbackup_stream_gate.py`.)

A live reload defers a worker switch for as long as a transfer is in flight, so
a peer that hangs forever would turn "deferred" into "never".
"""

import ast
import json
from datetime import datetime, timezone
import socket
import ssl
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import websocket

from _remote_support import (  # noqa: F401  (fixtures)
    OTHER_SERVER_FP, SERVER_FP, fake_connect_pinned, make_transport, snapshot_presence,
)
from libzfseasy.zfs import StreamHandle
from _snapshot_support import listed_info
from zfsbackup import remote
from zfsbackup.backup_manager import DatasetManager
from zfsbackup.config import BackupConfig, DatasetConfig, Destination, RemoteDatasetConfig
from zfsbackup.identity import PinMismatch
from zfsbackup.protocol import protocol_headers
from zfsbackup.remote import (
    WS_CONNECT_TIMEOUT,
    WS_FINAL_ACK_TIMEOUT,
    WS_IDLE_TIMEOUT,
    HTTP_TIMEOUT,
    RESUME_TOKEN_TIMEOUT,
    RemoteBackupManager,
    _SendStream,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
URL = "https://backup.example.com"


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


@pytest.fixture(autouse=True)
def _tls_connect_is_faked(fake_connect_pinned):
    """No test here opens a socket: the websocket's pinned TLS connect is a recorder
    (`fake_connect_pinned`) that hands out `MagicMock` sockets."""
    return fake_connect_pinned


@pytest.fixture(autouse=True)
def _snapshots_are_present(snapshot_presence):
    """After every acknowledged step the client checks (`zfs get guid`) that the snapshot it sent
    still exists; here it always does, and no `zfs` runs."""
    return snapshot_presence


@pytest.fixture
def rbm(tmp_path):
    config = BackupConfig(
        datasets=[DatasetConfig(name="pool/data", remote=[RemoteDatasetConfig(destination="offsite")])],
        destinations={"offsite": Destination(url=URL, server_fingerprint=SERVER_FP)},
    )
    manager = DatasetManager(config)
    return RemoteBackupManager(config, manager, client_id="client1"), manager.datasets[0]


def tr():
    """The fake transport (see `_remote_support.make_transport`) for `URL`."""
    return make_transport(URL)


def snapshot(name="autosnap_20240101120000", guid=None):
    """A managed snapshot as the manager's one listing delivers it (S-1): `createtxg` ordered like
    the name, and `guid` (the resume tests' 111/222/333 for the 20240101/02/03 names)."""
    if guid is None:
        guid = {"autosnap_20240101120000": 111, "autosnap_20240102120000": 222,
                "autosnap_20240103120000": 333}.get(name, "auto")
    if guid == "auto":
        return listed_info(name, "autosnap")
    return listed_info(name, "autosnap", guid=guid)


class Outcome(tuple):
    """`(result, set_anchor)` -- what most tests unpack -- plus the other
    mocked `local_manager` calls."""

    def __new__(cls, result, set_anchor, **extra):
        self = super().__new__(cls, (result, set_anchor))
        self.__dict__.update(extra)
        return self


def started(stream, mode="fresh", inflight=False, sent=None, base=None):
    """What `_try_resume` (mode `resume`) or `_start_fresh` returns after starting `stream`."""
    return _SendStream(stream, mode, inflight, sent, base)


def stub_start(mocker, manager, stream, mode="fresh", inflight=False, sent=None, base=None):
    """Patch the two places a stream is started so `_transfer` is handed `stream`: a `resume`
    comes out of `_try_resume` (step 0), anything else out of `_start_fresh`, which sends the
    step it is asked for (`sent`, `base` override that); `stream=None` is a `zfs send` that
    could not be started. `_push_step` itself is the real one."""
    if stream is None:
        mocker.patch.object(manager, "_try_resume", return_value=("fresh", None))
        mocker.patch.object(manager, "_start_fresh", return_value=None)
    elif mode == "resume":
        mocker.patch.object(manager, "_try_resume", return_value=("resume", started(stream, mode, inflight, sent, base)))
    else:
        mocker.patch.object(manager, "_try_resume", return_value=("fresh", None))
        mocker.patch.object(
            manager, "_start_fresh",
            side_effect=lambda dsi, dest, prev, step, m: started(
                stream, "fresh", inflight, sent or step, base if base is not None else prev),
        )


def transfer(rbm_dsi, mocker, stream, ws, mode="fresh", inflight=False, sent=None, base=None, latest=None,
             common=None, **kwargs):
    """`_transfer` with the send already started (`stub_start`: step 0 and the fresh start are
    exercised on their own in test_zfsbackup_remote.py and in TestRealResumeStreamUnderTimeouts
    below). A `resume` of a snapshot other than `latest` is followed by a stop request, so the
    chain that would continue after it is not run here."""
    manager, dsi = rbm_dsi
    latest = latest or snapshot()
    stub_start(mocker, manager, stream, mode, inflight, sent, base)
    if mode == "resume" and "should_stop" not in kwargs:
        answers = iter([False, True])
        kwargs["should_stop"] = lambda: next(answers, True)
    mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=ws)
    lm = manager.local_manager
    set_anchor = mocker.patch.object(lm, "set_anchor", return_value=True)
    get_anchor = mocker.patch.object(lm, "get_anchor", return_value=None)
    clear_inflight = mocker.patch.object(lm, "clear_inflight", return_value=True)
    set_inflight = mocker.patch.object(lm, "set_inflight", return_value=True)
    # a failed step re-lists to see whether its snapshot vanished: it has not
    mocker.patch.object(lm, "list_snapshots", return_value=[latest])
    result = manager._transfer(tr(), "client1", dsi, latest, common, "offsite", snapshots=[latest], **kwargs)
    return Outcome(result, set_anchor, get_anchor=get_anchor, clear_inflight=clear_inflight,
                   set_inflight=set_inflight)


def ok_ws():
    ws = MagicMock()
    ws.recv.return_value = json.dumps({"status": "ok"})
    return ws


class TestTimeouts:
    def test_the_constants(self):
        assert (WS_CONNECT_TIMEOUT, WS_IDLE_TIMEOUT, WS_FINAL_ACK_TIMEOUT) == (30, 600, 3600)
        assert WS_CONNECT_TIMEOUT < WS_IDLE_TIMEOUT < WS_FINAL_ACK_TIMEOUT
        assert (HTTP_TIMEOUT, RESUME_TOKEN_TIMEOUT) == (30, 10)

    def test_the_pinned_tls_connect_is_bounded_by_the_websocket_connect_timeout(
        self, rbm, mocker, fake_connect_pinned
    ):
        """TCP connect + TLS handshake + fingerprint check run under `WS_CONNECT_TIMEOUT`, with the
        destination's host and port, the transport's SSL context and the PINNED server fingerprint."""
        transfer(rbm, mocker, FakeStream(), ok_ws())
        ((host, port, ctx, fp, timeout),) = fake_connect_pinned.calls
        assert (host, port, fp, timeout) == ("backup.example.com", 443, SERVER_FP, WS_CONNECT_TIMEOUT)
        assert ctx is not None

    def test_the_tls_connect_happens_before_the_websocket_connect_and_hands_it_the_socket(
        self, rbm, mocker, fake_connect_pinned
    ):
        ws = ok_ws()
        order = []
        fake_connect_pinned.side_effect = lambda *a: (order.append("tls"), MagicMock(name="sock"))[1]
        ws.connect.side_effect = lambda *a, **k: order.append("ws")
        transfer(rbm, mocker, FakeStream(), ws)
        assert order == ["tls", "ws"]
        assert ws.connect.call_args.kwargs["socket"] is not None

    def test_the_http_calls_are_bounded(self, rbm):
        """`/whoami` and `/negotiate` use `HTTP_TIMEOUT`, `/resume_token` the shorter
        `RESUME_TOKEN_TIMEOUT`: the supervisor defers a worker switch for as long as a call blocks."""
        manager, dsi = rbm
        session = MagicMock()
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {"client_id": "client1", "resume_token": None}
        session.get.return_value.ok = True
        session.post.return_value.status_code = 200
        session.post.return_value.json.return_value = {"common_snapshot": None}
        transport = make_transport(URL, session=session)
        manager._whoami("offsite", transport)
        assert session.get.call_args.kwargs["timeout"] == HTTP_TIMEOUT
        manager._negotiate(transport, "client1", dsi, [], "offsite")
        assert session.post.call_args.kwargs["timeout"] == HTTP_TIMEOUT
        manager._fetch_resume_token(transport, "client1", dsi, "offsite")
        assert session.get.call_args.kwargs["timeout"] == RESUME_TOKEN_TIMEOUT

    def test_connect_then_idle_then_final_ack_in_that_order(self, rbm, mocker, fake_connect_pinned):
        ws = ok_ws()
        result, _ = transfer(rbm, mocker, FakeStream([b"a", b"b"]), ws)
        assert result is True
        names = [c[0] for c in ws.method_calls if c[0] in ("connect", "settimeout", "send_binary", "send", "recv")]
        assert names == ["connect", "settimeout", "send_binary", "send_binary", "send", "settimeout", "recv"]
        assert ws.connect.call_args.kwargs == {
            "socket": fake_connect_pinned.socks[0], "timeout": WS_CONNECT_TIMEOUT,
            "header": protocol_headers(),             # the wire protocol version (plan S-5)
        }
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
        assert ws.connect.call_args.args == ("wss://backup.example.com/backup/client1/pool/data/stream?mode=fresh",)


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


class TestPinnedConnectFailure:
    """The pinned TLS connect (`connect_pinned`) runs after `zfs send` has started and before
    `entered`: any failure of it reaps the send, exactly like a failed websocket connect."""

    @pytest.mark.parametrize(
        "exc",
        [
            ConnectionRefusedError("refused"), TimeoutError("timed out"), socket.timeout("timed out"),
            ssl.SSLError("handshake failure"), OSError("net down"),
            PinMismatch(SERVER_FP, OTHER_SERVER_FP),
        ],
        ids=["refused", "timeout", "socket-timeout", "ssl", "oserror", "pin-mismatch"],
    )
    def test_a_failed_tls_connect_is_false_with_the_stream_closed_exactly_once(
        self, rbm, mocker, fake_connect_pinned, exc
    ):
        stream = FakeStream()
        ws = ok_ws()
        fake_connect_pinned.side_effect = exc
        result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        assert stream.closes == 1
        ws.connect.assert_not_called()               # no websocket byte without a pinned socket
        ws.send_binary.assert_not_called()
        ws.send.assert_not_called()
        ws.close.assert_called()
        set_anchor.assert_not_called()

    def test_a_failed_tls_connect_keeps_the_pin_and_is_never_probed_for_a_refusal(
        self, rbm, mocker, fake_connect_pinned
    ):
        ws = ok_ws()
        ws.recv.return_value = REFUSAL_JSON
        fake_connect_pinned.side_effect = ConnectionRefusedError("refused")
        out = transfer(rbm, mocker, FakeStream(), ws, inflight=True)
        assert out[0] is False
        out.clear_inflight.assert_not_called()
        ws.recv.assert_not_called()

    def test_a_failing_stream_close_after_a_failed_tls_connect_is_swallowed(
        self, rbm, mocker, fake_connect_pinned
    ):
        stream = FakeStream()
        stream.close = MagicMock(side_effect=Exception("zfs send exited 1"))
        fake_connect_pinned.side_effect = ConnectionRefusedError("refused")
        result, _ = transfer(rbm, mocker, stream, ok_ws())
        assert result is False
        stream.close.assert_called_once()

    def test_the_tls_socket_is_closed_after_a_success(self, rbm, mocker, fake_connect_pinned):
        result, _ = transfer(rbm, mocker, FakeStream(), ok_ws())
        assert result is True
        fake_connect_pinned.socks[0].close.assert_called()

    @pytest.mark.parametrize("scenario", ["connect", "send-timeout", "server-error", "settimeout"])
    def test_the_tls_socket_is_closed_after_every_failure_past_the_tls_connect(
        self, rbm, mocker, fake_connect_pinned, scenario
    ):
        """A failed websocket upgrade can leave the raw TLS socket open: `finally` closes it."""
        ws = ok_ws()
        if scenario == "connect":
            ws.connect.side_effect = websocket.WebSocketBadStatusException("Handshake status %d", 500)
        elif scenario == "send-timeout":
            ws.send_binary.side_effect = websocket.WebSocketTimeoutException("t")
        elif scenario == "server-error":
            ws.recv.return_value = json.dumps({"status": "error", "error": "no"})
        else:
            ws.settimeout.side_effect = OSError("bad socket")
        result, _ = transfer(rbm, mocker, FakeStream(), ws)
        assert result is False
        fake_connect_pinned.socks[0].close.assert_called()

    def test_a_failing_tls_socket_close_is_swallowed(self, rbm, mocker, fake_connect_pinned):
        stream = FakeStream()
        ws = ok_ws()
        ws.connect.side_effect = OSError("upgrade failed")

        def connect(*args):
            sock = MagicMock(name="sock")
            sock.close.side_effect = OSError("already closed")
            return sock

        fake_connect_pinned.side_effect = connect
        result, _ = transfer(rbm, mocker, stream, ws)
        assert result is False
        assert stream.closes == 1


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
        assert manager._transfer(tr(), "c", dsi, snapshot(), None, "offsite", snapshots=[snapshot()]) is True
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

    @pytest.mark.parametrize(
        "exc", [ConnectionRefusedError("refused"), PinMismatch(SERVER_FP, OTHER_SERVER_FP), ssl.SSLError("alert")],
        ids=["refused", "pin-mismatch", "ssl-alert"],
    )
    def test_a_failed_pinned_tls_connect_reaps_the_endless_writer(self, rbm, mocker, fake_connect_pinned, exc):
        """The TLS connect is where an impostor server or a rejected client certificate shows up:
        the already started `zfs send` must not be left blocked on a full pipe."""
        proc, handle = self._endless()
        fake_connect_pinned.side_effect = exc
        try:
            result, set_anchor = transfer(rbm, mocker, handle, ok_ws())
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
        stub_start(mocker, manager, stream)
        mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=ws)
        set_anchor = mocker.patch.object(manager.local_manager, "set_anchor")
        with pytest.raises(KeyboardInterrupt):
            manager._transfer(tr(), "client1", dsi, snapshot(), None, "offsite", snapshots=[snapshot()])
        assert stream.closes == 1
        ws.close.assert_called()
        set_anchor.assert_not_called()
        ws.send.assert_not_called()

    def test_a_close_that_raises_during_a_keyboard_interrupt_does_not_mask_it(self, rbm, mocker):
        manager, dsi = rbm
        stream = FakeStream()
        stream.read = MagicMock(side_effect=KeyboardInterrupt)
        stream.close = MagicMock(side_effect=Exception("killed by SIGPIPE"))
        stub_start(mocker, manager, stream)
        mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=ok_ws())
        with pytest.raises(KeyboardInterrupt):
            manager._transfer(tr(), "client1", dsi, snapshot(), None, "offsite", snapshots=[snapshot()])

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

from zfsbackup.remote import (  # noqa: E402
    _is_reloading_refusal, _log_refusal, _peer_refusal, _peer_sent_reloading_refusal,
)

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


class TestPeerRefusal:
    """`_peer_refusal(ws)`: the refusal the server left in the socket, any JSON object that
    is not an ok status and carries an `error` or `retry`; otherwise None."""

    @staticmethod
    def probe(reply=None, **attrs):
        ws = MagicMock()
        if reply is not None:
            ws.recv.return_value = reply if isinstance(reply, str) else json.dumps(reply)
        for k, v in attrs.items():
            getattr(ws, k).side_effect = v
        return ws

    @pytest.mark.parametrize("reply", [
        {"error": "server is reloading; retry later", "retry": True},
        {"error": "busy", "retry": True},
        {"error": "pool/backups/c1/data is held for pool/other"},                # D7, no status
        {"status": "error", "error": "disk full"},
        {"status": "error", "error": "unknown mode", "retry": False},
        {"retry": True},
        {"error": "x", "extra": 1},
    ])
    def test_a_refusal_is_returned_whole(self, reply):
        ws = self.probe(reply)
        assert _peer_refusal(ws) == reply
        assert [c[0] for c in ws.method_calls] == ["settimeout", "recv"]
        ws.settimeout.assert_called_once_with(2)

    @pytest.mark.parametrize("reply", [
        {"status": "ok"}, {"status": "ok", "error": "x"}, {"status": "ok", "retry": True},
        {}, {"status": "error"}, {"error": ""}, {"error": None, "retry": False}, {"retry": 0},
        [], ["error"], "error", 7, None,
    ])
    def test_anything_else_is_none(self, reply):
        assert _peer_refusal(self.probe(json.dumps(reply))) is None

    @pytest.mark.parametrize("raw", ["", "not json", "{", b"", None])
    def test_unreadable_replies_are_none(self, raw):
        ws = MagicMock()
        ws.recv.return_value = raw
        assert _peer_refusal(ws) is None

    @pytest.mark.parametrize("exc", [
        websocket.WebSocketTimeoutException("t"), websocket.WebSocketConnectionClosedException("c"),
        OSError("reset"), RuntimeError("x"), ValueError("y"),
    ])
    def test_a_recv_error_is_swallowed(self, exc):
        assert _peer_refusal(self.probe(recv=exc)) is None

    def test_a_failing_settimeout_is_swallowed_and_recv_is_not_attempted(self):
        ws = self.probe({"error": "x"}, settimeout=OSError("closed"))
        assert _peer_refusal(ws) is None
        ws.recv.assert_not_called()

    def test_the_boolean_wrapper_is_true_only_for_retryable_refusals(self):
        assert _peer_sent_reloading_refusal(self.probe({"error": "busy", "retry": True})) is True
        assert _peer_sent_reloading_refusal(self.probe({"error": "x", "retry": True})) is True
        assert _peer_sent_reloading_refusal(self.probe({"error": "held elsewhere"})) is False
        assert _peer_sent_reloading_refusal(self.probe({"status": "ok"})) is False
        assert _peer_sent_reloading_refusal(self.probe("garbage")) is False


class TestLogRefusal:
    URL_ = "https://backup.example.com"

    def logged(self, caplog, result):
        with caplog.at_level(logging.DEBUG):
            _log_refusal(self.URL_, "pool/data", result)
        return [(r.levelname, r.getMessage()) for r in caplog.records]

    def test_busy_is_info_with_its_own_wording(self, caplog):
        assert self.logged(caplog, {"error": "busy", "retry": True}) == [
            ("INFO", f"server {self.URL_} is busy with another stream of pool/data; skipping this cycle")]

    def test_the_reload_refusal_is_info(self, caplog):
        assert self.logged(caplog, {"error": "server is reloading; retry later", "retry": True}) == [
            ("INFO", f"server {self.URL_} is reloading; skipping pool/data this cycle")]

    @pytest.mark.parametrize("result, shown", [
        ({"retry": True}, "unknown"),
        ({"error": "something else", "retry": True}, "something else"),
        ({"status": "error", "error": "x", "retry": True}, "x"),
        ({"error": "no configuration is recorded for pool/b/c1/data; negotiate again", "retry": True},
         "no configuration is recorded for pool/b/c1/data; negotiate again"),
    ])
    def test_any_other_retryable_reply_asks_to_retry_with_the_servers_text(self, caplog, result, shown):
        expected = shown if result.get("error") else ""
        got = self.logged(caplog, result)
        assert [lv for lv, _ in got] == ["INFO"]
        msg = got[0][1]
        assert msg.startswith(f"server {self.URL_} asks to retry pool/data (") and msg.endswith("); skipping this cycle")
        assert expected in msg
        assert "is reloading" not in msg and "is busy" not in msg

    def test_the_real_busy_and_reloading_refusals_keep_their_own_wording(self, caplog):
        from zfsbackup import api
        assert self.logged(caplog, api.BUSY_ERROR) == [
            ("INFO", f"server {self.URL_} is busy with another stream of pool/data; skipping this cycle")]
        caplog.clear()
        assert self.logged(caplog, api.RELOADING_ERROR) == [
            ("INFO", f"server {self.URL_} is reloading; skipping pool/data this cycle")]

    def test_a_non_retryable_refusal_is_an_error_with_the_servers_text(self, caplog):
        assert self.logged(caplog, {"error": "pool/backups/c1/data is held for pool/other"}) == [
            ("ERROR", "server refused pool/data: pool/backups/c1/data is held for pool/other")]

    def test_busy_without_retry_is_not_retryable(self, caplog):
        assert self.logged(caplog, {"error": "busy"}) == [("ERROR", "server refused pool/data: busy")]

    @pytest.mark.parametrize("result", [{"status": "error"}, {"error": ""}, {"error": None}, {"retry": False}])
    def test_no_text_is_unknown_error(self, caplog, result):
        assert self.logged(caplog, result) == [("ERROR", "server refused pool/data: unknown error")]


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

    def test_a_busy_final_ack_is_info_busy(self, rbm, mocker, caplog):
        ws = ok_ws()
        ws.recv.return_value = json.dumps({"error": "busy", "retry": True})
        with caplog.at_level(logging.DEBUG):
            result, set_anchor = transfer(rbm, mocker, FakeStream(), ws)
        assert result is False
        set_anchor.assert_not_called()
        assert "ERROR" not in levels(caplog)
        assert f"server {URL} is busy with another stream of pool/data; skipping this cycle" in caplog.text

    def test_a_non_retryable_final_ack_keeps_the_transfer_failed_wording(self, rbm, mocker, caplog):
        ws = ok_ws()
        ws.recv.return_value = json.dumps({"status": "error", "error": "disk full"})
        with caplog.at_level(logging.DEBUG):
            transfer(rbm, mocker, FakeStream(), ws)
        assert levels(caplog, "pool/data") == ["ERROR"]
        assert "Transfer failed for pool/data: disk full" in caplog.text and "server refused" not in caplog.text

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
            {"return_value": json.dumps({"status": "ok", "error": "ignored"})},
            {"return_value": json.dumps({"status": "error"})},          # neither `error` nor `retry`
            {"return_value": json.dumps({"error": ""})},
            {"return_value": json.dumps(["error"])},
        ],
        ids=["probe-timeout", "probe-closed", "probe-oserror", "empty", "non-json", "ok-json", "ok-with-error",
             "status-only", "empty-error", "list"],
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
            {"return_value": json.dumps({"status": "error"})},
        ],
        ids=["probe-timeout", "probe-oserror", "empty", "non-json", "status-only"],
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

    def test_busy_behind_a_failed_send_is_info_busy(self, rbm, mocker, caplog):
        stream = FakeStream()
        ws = ok_ws()
        ws.send_binary.side_effect = BrokenPipeError("Broken pipe")
        ws.recv.return_value = json.dumps({"error": "busy", "retry": True})
        with caplog.at_level(logging.DEBUG):
            result, set_anchor = transfer(rbm, mocker, stream, ws)
        assert result is False
        set_anchor.assert_not_called()
        assert levels(caplog, "pool/data") == ["INFO"]
        assert f"server {URL} is busy with another stream of pool/data; skipping this cycle" in caplog.text
        assert "Transfer failed" not in caplog.text and "Broken pipe" not in caplog.text
        assert stream.closes == 1
        ws.recv.assert_called_once()

    def test_a_server_error_behind_a_failed_send_is_the_servers_text_not_the_pipe(self, rbm, mocker, caplog):
        """D7 and friends: the server explains itself and closes; the client's
        BrokenPipe is only the symptom."""
        stream = FakeStream()
        ws = ok_ws()
        ws.send_binary.side_effect = BrokenPipeError("Broken pipe")
        ws.recv.return_value = json.dumps({"error": "pool/backups/c1/data is held for pool/other"})
        with caplog.at_level(logging.DEBUG):
            result, set_anchor = transfer(rbm, mocker, stream, ws, inflight=True)
        assert result is False
        set_anchor.assert_not_called()
        assert levels(caplog, "pool/data") == ["ERROR"]
        assert "server refused pool/data: pool/backups/c1/data is held for pool/other" in caplog.text
        assert "Broken pipe" not in caplog.text and "Transfer failed" not in caplog.text
        assert stream.closes == 1
        assert settimeouts(ws) == [WS_IDLE_TIMEOUT, 2, 5]

    def test_a_status_error_body_behind_a_failed_send_is_the_servers_text(self, rbm, mocker, caplog):
        ws = ok_ws()
        ws.send_binary.side_effect = ConnectionResetError("reset")
        ws.recv.return_value = json.dumps({"status": "error", "error": "unknown mode"})
        with caplog.at_level(logging.DEBUG):
            result, _ = transfer(rbm, mocker, FakeStream(), ws)
        assert result is False
        assert [(r.levelname, r.getMessage()) for r in caplog.records if r.name == "zfsbackup.remote"] == [
            ("ERROR", "server refused pool/data: unknown mode")]

    @pytest.mark.parametrize("body", [
        "garbage", "", json.dumps({"status": "ok"}), json.dumps({}), json.dumps({"status": "error"}),
    ], ids=["garbage", "empty", "ok", "empty-object", "status-only"])
    def test_an_unhelpful_reply_keeps_the_original_exception(self, rbm, mocker, caplog, body):
        ws = ok_ws()
        ws.send_binary.side_effect = BrokenPipeError("Broken pipe")
        ws.recv.return_value = body
        with caplog.at_level(logging.DEBUG):
            result, _ = transfer(rbm, mocker, FakeStream(), ws)
        assert result is False
        assert levels(caplog, "Transfer failed for pool/data") == ["ERROR"]
        assert "Broken pipe" in caplog.text and "server refused" not in caplog.text

    def test_a_server_error_behind_a_failed_done_send_is_the_servers_text(self, rbm, mocker, caplog):
        ws = ok_ws()
        ws.send.side_effect = BrokenPipeError("Broken pipe")
        ws.recv.return_value = json.dumps({"error": "held elsewhere"})
        with caplog.at_level(logging.DEBUG):
            result, _ = transfer(rbm, mocker, FakeStream(), ws)
        assert result is False
        assert "server refused pool/data: held elsewhere" in caplog.text and "Broken pipe" not in caplog.text

    def test_a_timeout_never_probes_so_a_waiting_error_is_not_read(self, rbm, mocker, caplog):
        ws = ok_ws()
        ws.send_binary.side_effect = websocket.WebSocketTimeoutException("timed out")
        ws.recv.return_value = json.dumps({"error": "held elsewhere"})
        with caplog.at_level(logging.DEBUG):
            transfer(rbm, mocker, FakeStream(), ws)
        ws.recv.assert_not_called()
        assert "Transfer failed for pool/data" in caplog.text and "server refused" not in caplog.text

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
        stub_start(mocker, manager, stream)
        mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=ws)
        with pytest.raises(KeyboardInterrupt):
            manager._transfer(tr(), "client1", dsi, snapshot(), None, "offsite", snapshots=[snapshot()])
        ws.recv.assert_not_called()


# ---------------------------------------------------------------------------
# Resume protocol, transfer level: `/stream?mode=fresh|resume[&from=...]`, the
# in-flight pin and the anchor. (The decision table for WHICH stream starts is
# in test_zfsbackup_remote.py; here the started stream is given.)
# ---------------------------------------------------------------------------

from urllib.parse import parse_qs, urlsplit  # noqa: E402

from libzfseasy.zfs import ResumeToken  # noqa: E402

OLD = "autosnap_20240101120000"
MID = "autosnap_20240102120000"
NEW = "autosnap_20240103120000"


def connect_url(ws):
    return ws.connect.call_args.args[0]


def query(ws):
    return parse_qs(urlsplit(connect_url(ws)).query)


class TestStreamUrlModes:
    def test_a_full_fresh_send_says_mode_fresh_and_has_no_from(self, rbm, mocker):
        ws = ok_ws()
        transfer(rbm, mocker, FakeStream(), ws)
        assert connect_url(ws) == "wss://backup.example.com/backup/client1/pool/data/stream?mode=fresh"
        assert query(ws) == {"mode": ["fresh"]}

    def test_an_incremental_fresh_send_carries_from(self, rbm, mocker):
        ws = ok_ws()
        transfer(rbm, mocker, FakeStream(), ws, base=snapshot(OLD), sent=snapshot(NEW), latest=snapshot(NEW))
        assert connect_url(ws).endswith(f"/stream?mode=fresh&from={OLD}")

    def test_a_resume_says_mode_resume_and_carries_from(self, rbm, mocker):
        ws = ok_ws()
        transfer(rbm, mocker, FakeStream(), ws, mode="resume", base=snapshot(OLD), sent=snapshot(MID),
                 latest=snapshot(NEW))
        assert query(ws) == {"mode": ["resume"], "from": [OLD]}

    def test_a_resumed_full_stream_has_no_from(self, rbm, mocker):
        ws = ok_ws()
        transfer(rbm, mocker, FakeStream(), ws, mode="resume", sent=snapshot(OLD), latest=snapshot(OLD))
        assert query(ws) == {"mode": ["resume"]}

    def test_from_is_the_base_not_the_common_snapshot_argument(self, rbm, mocker):
        """A resume's base comes from the token's GUIDs; `common` (negotiated for a
        different, newer target) must not leak into the URL."""
        ws = ok_ws()
        transfer(rbm, mocker, FakeStream(), ws, mode="resume", base=snapshot(OLD), sent=snapshot(MID),
                 latest=snapshot(NEW), common=snapshot(MID))
        assert query(ws)["from"] == [OLD]

    def test_the_mode_is_always_present(self, rbm, mocker):
        for mode in ("fresh", "resume"):
            ws = ok_ws()
            transfer(rbm, mocker, FakeStream(), ws, mode=mode, sent=snapshot(NEW), latest=snapshot(NEW))
            assert query(ws)["mode"] == [mode]

    def test_from_is_percent_encoded(self, rbm, mocker):
        ws = ok_ws()
        transfer(rbm, mocker, FakeStream(), ws, base=snapshot("autosnap_20240101120000_a b:c"),
                 sent=snapshot(NEW), latest=snapshot(NEW))
        assert connect_url(ws).endswith("&from=autosnap_20240101120000_a%20b%3Ac")


class TestPinAndAnchorAfterTheTransfer:
    def test_a_fresh_success_anchors_the_sent_snapshot_and_clears_the_pin(self, rbm, mocker, caplog):
        with caplog.at_level(logging.INFO):
            out = transfer(rbm, mocker, FakeStream(), ok_ws(), inflight=True, sent=snapshot(NEW),
                           latest=snapshot(NEW), base=snapshot(OLD))
        assert out[0] is True
        out[1].assert_called_once()
        assert out[1].call_args.args[1:] == ("offsite", NEW)
        out.clear_inflight.assert_called_once()
        assert out.clear_inflight.call_args.args[1] == "offsite"
        assert f"Backup complete: pool/data@{NEW} (incremental from {OLD}) -> {URL}" in caplog.text
        assert "Resumed" not in caplog.text

    def test_a_full_fresh_success_says_full(self, rbm, mocker, caplog):
        with caplog.at_level(logging.INFO):
            transfer(rbm, mocker, FakeStream(), ok_ws(), sent=snapshot(NEW), latest=snapshot(NEW))
        assert f"Backup complete: pool/data@{NEW} (full) -> {URL}" in caplog.text

    def test_the_pin_is_cleared_whenever_the_anchor_write_succeeded_even_if_this_pin_was_not_written(self, rbm, mocker):
        """S3: an older stale pin goes too; `inflight_set` only says THIS attempt's write worked."""
        out = transfer(rbm, mocker, FakeStream(), ok_ws(), inflight=False)
        assert out[0] is True
        out.clear_inflight.assert_called_once()

    def test_a_failed_anchor_write_keeps_the_pin(self, rbm, mocker):
        manager, _ = rbm
        stream, ws = FakeStream(), ok_ws()
        stub_start(mocker, manager, stream, inflight=True)
        mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=ws)
        mocker.patch.object(manager.local_manager, "get_anchor", return_value=None)
        mocker.patch.object(manager.local_manager, "set_anchor", return_value=False)
        clear = mocker.patch.object(manager.local_manager, "clear_inflight")
        dsi = rbm[1]
        latest = snapshot(NEW)
        assert manager._transfer(tr(), "client1", dsi, latest, None, "offsite", snapshots=[latest]) is True
        clear.assert_not_called()

    @pytest.mark.parametrize("scenario", ["send-timeout", "server-error", "refusal", "connect", "done-fails"])
    def test_a_failed_transfer_neither_anchors_nor_clears_the_pin(self, rbm, mocker, scenario):
        ws = ok_ws()
        if scenario == "send-timeout":
            ws.send_binary.side_effect = websocket.WebSocketTimeoutException("t")
        elif scenario == "server-error":
            ws.recv.return_value = json.dumps({"status": "error", "error": "no"})
        elif scenario == "refusal":
            ws.recv.return_value = REFUSAL_JSON
        elif scenario == "connect":
            ws.connect.side_effect = ConnectionRefusedError("refused")
        else:
            ws.send.side_effect = BrokenPipeError("x")
        out = transfer(rbm, mocker, FakeStream(), ws, inflight=True)
        assert out[0] is False
        out[1].assert_not_called()
        out.clear_inflight.assert_not_called()


class TestResumeAnchorRules:
    def anchored(self, rbm, mocker, existing, sent, caplog=None, **kw):
        ws = ok_ws()
        manager, dsi = rbm
        stream = FakeStream()
        stub_start(mocker, manager, stream, "resume", True, snapshot(sent), snapshot(OLD))
        mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=ws)
        lm = manager.local_manager
        set_anchor = mocker.patch.object(lm, "set_anchor", return_value=True)
        mocker.patch.object(lm, "get_anchor", return_value=existing)
        clear = mocker.patch.object(lm, "clear_inflight", return_value=True)
        snaps = [snapshot(NEW), snapshot(MID), snapshot(OLD)]
        # a stop request right after the resume: the chain that would go on to NEW is not run here
        answers = iter([False, True])
        ok = manager._transfer(tr(), "client1", dsi, snaps[0], None, "offsite", snapshots=snaps,
                               should_stop=lambda: next(answers, True), **kw)
        return ok, set_anchor, clear

    def test_the_resumed_snapshot_becomes_the_anchor(self, rbm, mocker, caplog):
        with caplog.at_level(logging.INFO):
            ok, set_anchor, clear = self.anchored(rbm, mocker, existing=OLD, sent=MID)
        assert ok is True
        assert set_anchor.call_args.args[1:] == ("offsite", MID)
        clear.assert_called_once()
        assert f"Resumed pool/data@{MID} (from {OLD}) to offsite" in caplog.text
        assert "Backup complete" not in caplog.text

    def test_no_anchor_yet_gets_the_resumed_snapshot(self, rbm, mocker):
        ok, set_anchor, clear = self.anchored(rbm, mocker, existing=None, sent=MID)
        assert set_anchor.call_args.args[1:] == ("offsite", MID)
        clear.assert_called_once()

    def test_a_newer_anchor_is_never_moved_backwards(self, rbm, mocker, caplog):
        """S2: nothing is written. (`anchored` stops right after the resume, which settles the pin
        itself; the step's own handling of the pin is `TestPushStepClearPin`.)"""
        with caplog.at_level(logging.DEBUG):
            ok, set_anchor, clear = self.anchored(rbm, mocker, existing=NEW, sent=MID)
        assert ok is True
        set_anchor.assert_not_called()
        clear.assert_called_once()                          # the clean stop's clear, nothing else
        assert f"Keeping anchor {NEW}" in caplog.text

    def test_the_same_anchor_is_rewritten_not_skipped(self, rbm, mocker):
        ok, set_anchor, clear = self.anchored(rbm, mocker, existing=MID, sent=MID)
        set_anchor.assert_called_once()
        clear.assert_called_once()

    def test_an_anchor_that_is_not_a_local_managed_snapshot_is_replaced(self, rbm, mocker):
        ok, set_anchor, clear = self.anchored(rbm, mocker, existing="autosnap_20990101000000", sent=MID)
        assert set_anchor.call_args.args[1:] == ("offsite", MID)

    def test_a_fresh_step_older_than_the_anchor_leaves_it_too_and_keeps_the_pin(self, rbm, mocker):
        """The never-backwards rule holds for every send, not only a resume: a chain step after
        a resume (or a concurrent cycle) can be older than the anchor."""
        manager, dsi = rbm
        ws = ok_ws()
        stub_start(mocker, manager, FakeStream(), "fresh", True, snapshot(MID), None)
        mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=ws)
        set_anchor = mocker.patch.object(manager.local_manager, "set_anchor", return_value=True)
        mocker.patch.object(manager.local_manager, "get_anchor", return_value=NEW)
        clear = mocker.patch.object(manager.local_manager, "clear_inflight")
        snaps = [snapshot(NEW), snapshot(MID)]
        manager._transfer(tr(), "client1", dsi, snaps[1], None, "offsite", snapshots=snaps)
        set_anchor.assert_not_called()
        clear.assert_not_called()

    def test_a_fresh_step_newer_than_the_anchor_moves_it(self, rbm, mocker):
        manager, dsi = rbm
        stub_start(mocker, manager, FakeStream(), "fresh", True, snapshot(NEW), None)
        mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=ok_ws())
        set_anchor = mocker.patch.object(manager.local_manager, "set_anchor", return_value=True)
        mocker.patch.object(manager.local_manager, "get_anchor", return_value=MID)
        clear = mocker.patch.object(manager.local_manager, "clear_inflight")
        snaps = [snapshot(NEW), snapshot(MID)]
        manager._transfer(tr(), "client1", dsi, snaps[0], None, "offsite", snapshots=snaps)
        assert set_anchor.call_args.args[1:] == ("offsite", NEW)
        clear.assert_called_once()

    def test_a_failed_anchor_write_keeps_the_pin_on_a_resume(self, rbm, mocker):
        manager, dsi = rbm
        stub_start(mocker, manager, FakeStream(), "resume", True, snapshot(MID), snapshot(OLD))
        mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=ok_ws())
        mocker.patch.object(manager.local_manager, "get_anchor", return_value=OLD)
        mocker.patch.object(manager.local_manager, "set_anchor", return_value=False)
        clear = mocker.patch.object(manager.local_manager, "clear_inflight")
        snaps = [snapshot(NEW), snapshot(MID), snapshot(OLD)]
        assert manager._transfer(tr(), "client1", dsi, snaps[0], None, "offsite", snapshots=snaps) is False
        clear.assert_not_called()


class TestPushStepClearPin:
    """`_push_step(..., clear_pin=)`: the pin written for a step is released after its anchor only
    when the caller says so (the last step of a chain); a step in the middle leaves it for the
    next step's pin to overwrite."""

    def push(self, rbm, mocker, clear_pin, existing=None, sent=MID, **kw):
        from zfsbackup.remote import _OK
        manager, dsi = rbm
        mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=ok_ws())
        lm = manager.local_manager
        set_anchor = mocker.patch.object(lm, "set_anchor", return_value=True)
        mocker.patch.object(lm, "get_anchor", return_value=existing)
        clear = mocker.patch.object(lm, "clear_inflight", return_value=True)
        snaps = [snapshot(NEW), snapshot(MID), snapshot(OLD)]
        step = next(s for s in snaps if s.name == sent)
        started_ = started(FakeStream(), "fresh", True, step, snapshot(OLD))
        outcome = manager._push_step(tr(), "client1", dsi, "offsite", snaps, started_, clear_pin=clear_pin, **kw)
        return outcome, set_anchor, clear

    def test_clear_pin_true_releases_the_pin_after_the_anchor(self, rbm, mocker):
        outcome, set_anchor, clear = self.push(rbm, mocker, True)
        assert outcome == "ok"
        set_anchor.assert_called_once()
        clear.assert_called_once()

    def test_clear_pin_false_keeps_it(self, rbm, mocker):
        outcome, set_anchor, clear = self.push(rbm, mocker, False)
        assert outcome == "ok"
        set_anchor.assert_called_once()                      # the anchor is written either way
        clear.assert_not_called()

    def test_a_kept_newer_anchor_keeps_the_pin_whatever_clear_pin_says(self, rbm, mocker):
        outcome, set_anchor, clear = self.push(rbm, mocker, True, existing=NEW)
        assert outcome == "ok"
        set_anchor.assert_not_called()
        clear.assert_not_called()

    def test_the_default_releases_it(self, rbm, mocker):
        """A caller that does not say (a single stream) gets the old behaviour."""
        manager, dsi = rbm
        mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=ok_ws())
        lm = manager.local_manager
        mocker.patch.object(lm, "set_anchor", return_value=True)
        mocker.patch.object(lm, "get_anchor", return_value=None)
        clear = mocker.patch.object(lm, "clear_inflight", return_value=True)
        snaps = [snapshot(NEW)]
        manager._push_step(tr(), "client1", dsi, "offsite", snaps, started(FakeStream(), "fresh", True, snaps[0], None))
        clear.assert_called_once()


class TestShouldStop:
    def test_a_pending_stop_starts_no_send_at_all(self, rbm, mocker, caplog):
        manager, dsi = rbm
        get_stream = mocker.patch.object(manager, "_try_resume")
        start = mocker.patch.object(manager, "_start_fresh")
        ws_cls = mocker.patch("zfsbackup.remote.websocket.WebSocket")
        latest = snapshot(NEW)
        with caplog.at_level(logging.INFO):
            assert manager._transfer(tr(), "c", dsi, latest, None, "offsite", snapshots=[latest],
                                     should_stop=lambda: True) is False
        get_stream.assert_not_called()
        start.assert_not_called()
        ws_cls.assert_not_called()
        assert "stop requested; not starting a send of pool/data" in caplog.text

    def test_no_pending_stop_sends(self, rbm, mocker):
        out = transfer(rbm, mocker, FakeStream(), ok_ws(), should_stop=lambda: False)
        assert out[0] is True

    def test_dry_run_is_decided_before_the_stop_check_and_the_send(self, rbm, mocker):
        manager, dsi = rbm
        manager.config.dry_run = True
        stop = MagicMock(return_value=True)
        get_stream = mocker.patch.object(manager, "_try_resume")
        start = mocker.patch.object(manager, "_start_fresh")
        get_info = mocker.patch("zfsbackup.remote.zfs.send.resume_info")
        get_resume = mocker.patch("zfsbackup.remote.zfs.send.resume")
        session = MagicMock()
        latest = snapshot(NEW)
        assert manager._transfer(make_transport(URL, session=session), "c", dsi, latest, None, "offsite",
                                 snapshots=[latest], should_stop=stop) is True
        stop.assert_not_called()
        for m in (get_stream, start, get_info, get_resume, session.get, session.post):
            m.assert_not_called()


# ---------------------------------------------------------------------------
# `_try_resume` and `_start_fresh` NOT patched out: the real resume decision feeding the
# real transfer under websocket failures. Only the edges are mocked: the
# resume-token GET, the token decoder, `zfs send`, the
# local property writes and the websocket.
# ---------------------------------------------------------------------------

TOK = "1-a292306e5-b8-789c636064000310a500c4ec50360710e72765a52697303030419460caa7a515a7968064cae0f26c48f2499525a9c520f1d00aacfa4bf2d34b33531818f238834cbe7b33276a20c97382e5f312735319188a4a0af48b53f352f4131d8a0da16e0100ed3915db"


class TestRealResumeStreamUnderTimeouts:
    def build(self, rbm, mocker, token=None, ws=None):
        manager, dsi = rbm
        self.order = []
        resp = MagicMock()
        resp.ok = True
        resp.status_code = 200
        resp.json.return_value = {"resume_token": token}
        self.session = MagicMock()
        self.session.get.return_value = resp
        self.transport = make_transport(URL, session=self.session)
        self.snaps = [snapshot(NEW), snapshot(MID), snapshot(OLD)]
        self.stream = FakeStream([b"x", b"y"])
        self.send_snapshot = mocker.patch(
            "zfsbackup.remote.zfs.send.snapshot",
            side_effect=lambda *a, **k: (self.order.append("send"), self.stream)[1],
        )
        self.send_resume = mocker.patch(
            "zfsbackup.remote.zfs.send.resume",
            side_effect=lambda *a, **k: (self.order.append("send-t"), self.stream)[1],
        )
        mocker.patch(
            "zfsbackup.remote.zfs.send.resume_info",
            return_value=ResumeToken(toname=f"pool/data@{MID}", toguid=222, fromguid=111, flags=frozenset()),
        )
        lm = manager.local_manager
        self.set_inflight = mocker.patch.object(
            lm, "set_inflight", side_effect=lambda *a, **k: (self.order.append("pin"), True)[1]
        )
        self.clear_inflight = mocker.patch.object(lm, "clear_inflight", return_value=True)
        self.set_anchor = mocker.patch.object(lm, "set_anchor", return_value=True)
        mocker.patch.object(lm, "get_anchor", return_value=None)
        self.ws = ws or ok_ws()
        mocker.patch("zfsbackup.remote.websocket.WebSocket", return_value=self.ws)

    def run(self, rbm, common=None, **kw):
        manager, dsi = rbm
        return manager._transfer(self.transport, "client1", dsi, self.snaps[0], common, "offsite",
                                 snapshots=self.snaps, **kw)

    def test_a_fresh_send_that_times_out_leaves_the_pin_and_the_anchor(self, rbm, mocker):
        ws = ok_ws()
        ws.send_binary.side_effect = websocket.WebSocketTimeoutException("timed out")
        self.build(rbm, mocker, token=None, ws=ws)
        assert self.run(rbm) is False
        assert self.order == ["pin", "send"]               # pinned BEFORE zfs send started
        self.set_inflight.assert_called_once()
        self.clear_inflight.assert_not_called()
        self.set_anchor.assert_not_called()
        assert self.stream.closes == 1
        assert query(ws) == {"mode": ["fresh"]}

    def test_a_resumed_send_that_times_out_leaves_the_pin_and_the_anchor(self, rbm, mocker):
        ws = ok_ws()
        ws.send_binary.side_effect = websocket.WebSocketTimeoutException("timed out")
        self.build(rbm, mocker, token=TOK, ws=ws)
        assert self.run(rbm) is False
        assert self.order == ["pin", "send-t"]
        pin = self.set_inflight.call_args.args
        assert pin[1:] == ("offsite", OLD, MID)            # from the GUID match, not the token's name
        self.clear_inflight.assert_not_called()
        self.set_anchor.assert_not_called()
        assert self.stream.closes == 1
        assert query(ws) == {"mode": ["resume"], "from": [OLD]}
        self.send_snapshot.assert_not_called()

    def test_a_connect_failure_of_a_resume_closes_the_zfs_send_and_keeps_the_pin(self, rbm, mocker):
        ws = ok_ws()
        ws.connect.side_effect = ConnectionRefusedError("refused")
        self.build(rbm, mocker, token=TOK, ws=ws)
        assert self.run(rbm) is False
        assert self.stream.closes == 1
        self.clear_inflight.assert_not_called()
        self.set_anchor.assert_not_called()
        ws.send_binary.assert_not_called()

    def test_a_refusal_after_a_resume_is_info_and_changes_nothing(self, rbm, mocker, caplog):
        ws = ok_ws()
        ws.recv.return_value = REFUSAL_JSON
        self.build(rbm, mocker, token=TOK, ws=ws)
        with caplog.at_level(logging.INFO):
            assert self.run(rbm) is False
        assert "ERROR" not in levels(caplog)
        assert "is reloading; skipping pool/data this cycle" in caplog.text
        self.set_anchor.assert_not_called()
        self.clear_inflight.assert_not_called()

    def test_a_resume_that_completes_anchors_and_clears_without_a_follow_up_at_latest(self, rbm, mocker, caplog):
        self.build(rbm, mocker, token=TOK)
        # the token's target IS the latest snapshot: nothing left to follow up
        mocker.patch("zfsbackup.remote.zfs.send.resume_info",
                     return_value=ResumeToken(toguid=333, fromguid=222, flags=frozenset()))
        with caplog.at_level(logging.INFO):
            assert self.run(rbm) is True
        assert self.set_anchor.call_args.args[1:] == ("offsite", NEW)
        self.clear_inflight.assert_called_once()
        assert f"Resumed pool/data@{NEW} (from {MID}) to offsite" in caplog.text
        assert self.order == ["pin", "send-t"]

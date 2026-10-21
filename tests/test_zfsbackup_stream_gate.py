"""Phase 3, item 18.3 (and the server half of 18.4): the API worker drains
in-flight receives before it leaves for a reload.

* `TestStreamGate`            the counter itself (threads, draining, wait_idle).
* `TestGateInTheApp`          `app.extensions['zfsbackup.stream_gate']`, `negotiate`
                              and `stream` while draining, release on every path.
* `TestServerReceiveTimeout`  `ws.receive(timeout=WS_IDLE_TIMEOUT)` returning `None`.
* `TestApiWorkerDrain`        `ApiWorker.run` against the `_bind_api_server` seam with
                              a fake server and the REAL app and gate.
* `TestGatedEndpoints` /      the gated endpoints (`negotiate`, `resume_token`, and the trust
  `TestGatedTrustEndpoints`   writers `/enroll` and `/rotate`) and the drain that waits for them.

The app is authenticated (plan M10): the route tests use `_api_support.Authed` (a stub trust
gateway pinning a certificate under `client1`); the `ApiWorker` tests run a REAL `ApiWorker`
over a real config database and server identity (`tls_config_db`), with that certificate pinned
through the real trust store. Nothing here touches ZFS or opens a network socket.
"""

import errno
import json
import logging
import multiprocessing
import threading
import time
from unittest.mock import MagicMock, PropertyMock

import pytest
from simple_websocket import ConnectionClosed

from _api_support import Authed, CLIENT_ID, buffered_client, enroll_body, make_client_cert, rotate_body
from _tls_support import pin_client
from test_zfsbackup_daemon_closeout import SUPERVISOR, Parent
from zfsbackup import api as api_module
from zfsbackup import workers
from zfsbackup.api import RELOADING_ERROR, StreamGate
from zfsbackup.backup_manager import DatasetManager
from zfsbackup.config import BackupConfig, DatasetConfig, RemoteServerConfig
from zfsbackup.remote import WS_IDLE_TIMEOUT
from zfsbackup.stopflag import ReloadEpoch, StopFlag
from zfsbackup.workers import API_STOP_DRAIN, ApiWorker


def wait_until(predicate, timeout=20.0, step=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(step)
    return predicate()


# ---------------------------------------------------------------------------
# StreamGate
# ---------------------------------------------------------------------------


class TestStreamGate:
    def test_starts_idle_and_open(self):
        gate = StreamGate()
        assert (gate.active, gate.draining) == (0, False)

    def test_begin_and_end_count(self):
        gate = StreamGate()
        assert gate.begin() is True and gate.begin() is True
        assert gate.active == 2
        gate.end()
        assert gate.active == 1
        gate.end()
        assert gate.active == 0

    def test_accounting_under_threads_balances_to_zero(self):
        gate = StreamGate()
        peak = []
        barrier = threading.Barrier(16)

        def hammer():
            barrier.wait()
            for _ in range(500):
                assert gate.begin()
                peak.append(gate.active)
                gate.end()

        threads = [threading.Thread(target=hammer) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        assert not any(t.is_alive() for t in threads)
        assert gate.active == 0
        assert 1 <= max(peak) <= 16

    def test_concurrent_begins_are_all_counted(self):
        gate = StreamGate()
        ready = threading.Barrier(9)
        release = threading.Event()

        def hold():
            gate.begin()
            ready.wait()
            release.wait(30)
            gate.end()

        threads = [threading.Thread(target=hold) for _ in range(8)]
        for t in threads:
            t.start()
        ready.wait(30)
        assert gate.active == 8
        release.set()
        for t in threads:
            t.join(30)
        assert gate.active == 0

    def test_begin_is_false_while_draining_and_registers_nothing(self):
        gate = StreamGate()
        gate.start_draining()
        assert gate.draining is True
        assert gate.begin() is False
        assert gate.begin() is False
        assert gate.active == 0

    def test_draining_does_not_disturb_streams_already_running(self):
        gate = StreamGate()
        assert gate.begin()
        gate.start_draining()
        assert gate.active == 1
        gate.end()
        assert gate.active == 0

    def test_start_draining_is_idempotent_and_never_reopens(self):
        gate = StreamGate()
        gate.start_draining()
        gate.start_draining()
        assert gate.draining is True

    def test_end_never_goes_negative(self):
        gate = StreamGate()
        gate.end()
        gate.end()
        assert gate.active == 0
        assert gate.begin() and gate.active == 1

    def test_wait_idle_is_true_at_once_when_idle(self):
        gate = StreamGate()
        started = time.monotonic()
        assert gate.wait_idle(5) is True
        assert gate.wait_idle(None) is True
        assert gate.wait_idle(0) is True
        assert time.monotonic() - started < 1

    def test_wait_idle_times_out_false(self):
        gate = StreamGate()
        gate.begin()
        started = time.monotonic()
        assert gate.wait_idle(0.2) is False
        assert 0.15 <= time.monotonic() - started < 3

    def test_wait_idle_zero_with_an_active_stream_is_false_without_sleeping(self):
        gate = StreamGate()
        gate.begin()
        started = time.monotonic()
        assert gate.wait_idle(0) is False
        assert time.monotonic() - started < 0.4

    def test_wait_idle_returns_true_promptly_when_the_stream_ends(self):
        gate = StreamGate()
        gate.begin()
        timer = threading.Timer(0.2, gate.end)
        timer.start()
        started = time.monotonic()
        try:
            assert gate.wait_idle(30) is True
        finally:
            timer.cancel()
        assert time.monotonic() - started < 2     # notified, not polled out

    def test_wait_idle_none_waits_for_the_stream(self):
        gate = StreamGate()
        gate.begin()
        timer = threading.Timer(0.3, gate.end)
        timer.start()
        try:
            assert gate.wait_idle(None) is True
        finally:
            timer.cancel()

    def test_wait_idle_false_on_abort(self):
        gate = StreamGate()
        gate.begin()
        calls = []

        def abort():
            calls.append(1)
            return len(calls) >= 2

        started = time.monotonic()
        assert gate.wait_idle(None, abort=abort) is False
        assert len(calls) == 2
        assert time.monotonic() - started < 5

    def test_abort_is_not_consulted_once_idle(self):
        gate = StreamGate()
        assert gate.wait_idle(None, abort=lambda: pytest.fail("consulted")) is True

    def test_abort_is_called_without_the_gates_lock_held(self):
        gate = StreamGate()
        gate.begin()
        owned = []

        def abort():
            owned.append(gate._cond._is_owned())
            # and a second thread can take the lock at the same moment
            result = []
            t = threading.Thread(target=lambda: result.append(gate.active))
            t.start()
            t.join(2)
            owned.append(not t.is_alive())
            return True

        assert gate.wait_idle(None, abort=abort) is False
        assert owned == [False, True]

    def test_sleep_slices_are_at_most_half_a_second(self):
        gate = StreamGate()
        gate.begin()
        waits = []
        real = gate._cond.wait

        def wait(timeout=None):
            waits.append(timeout)
            return real(0.01)

        gate._cond.wait = wait
        n = []
        gate.wait_idle(None, abort=lambda: n.append(1) or len(n) > 3)
        gate.wait_idle(3600, abort=lambda: n.append(1) or len(n) > 8)
        assert waits and all(w is not None and 0 < w <= 0.5 for w in waits)

    def test_a_short_deadline_shortens_the_slice(self):
        gate = StreamGate()
        gate.begin()
        waits = []
        real = gate._cond.wait
        gate._cond.wait = lambda timeout=None: waits.append(timeout) or real(timeout)
        assert gate.wait_idle(0.15) is False
        assert all(w <= 0.15 + 1e-6 for w in waits)

    def test_a_dead_end_call_after_abort_does_not_hang(self):
        gate = StreamGate()
        gate.begin()
        gate.wait_idle(None, abort=lambda: True)
        gate.end()
        assert gate.wait_idle(1) is True


# ---------------------------------------------------------------------------
# The gate inside the app
# ---------------------------------------------------------------------------


def _config(remote=True, **kw):
    return BackupConfig(
        datasets=[DatasetConfig(name="pool/data")],
        remote_backup=RemoteServerConfig(target_dataset="pool/backups", enabled=True) if remote else None,
        **kw,
    )


@pytest.fixture
def authed(mocker):
    """An authenticated app: `authed.client()` presents the certificate pinned under `client1`."""
    mocker.patch.object(DatasetManager, "datasets", new_callable=PropertyMock, return_value=[])
    return Authed(_config())


@pytest.fixture
def app(authed):
    return authed.app


def _authed(mocker, config, **kwargs):
    mocker.patch.object(DatasetManager, "datasets", new_callable=PropertyMock, return_value=[])
    return Authed(config, **kwargs)


STREAM_PATH = f"/backup/{CLIENT_ID}/pool/data/stream"


@pytest.fixture(autouse=True)
def source_props(mocker):
    """No recorded `org.zfsbackup:source_dataset` anywhere, and never a real `zfs get`."""
    return mocker.patch("zfsbackup.api.read_zfsbackup_properties", return_value={})


class TestGateInTheApp:
    def test_the_gate_is_published_on_the_app(self, authed):
        gate = authed.gate
        assert isinstance(gate, StreamGate)
        assert gate.active == 0 and gate.draining is False

    def test_each_app_has_its_own_gate(self, mocker):
        a, b = _authed(mocker, _config()), _authed(mocker, _config())
        assert a.gate is not b.gate

    def test_the_gate_exists_even_without_remote_backup(self, mocker):
        assert isinstance(_authed(mocker, _config(remote=False)).gate, StreamGate)

    def test_the_reloading_error_is_retryable(self):
        assert RELOADING_ERROR == {"error": "server is reloading; retry later", "retry": True}

    def test_negotiate_is_503_retry_true_while_draining_and_calls_no_zfs(self, authed, mocker):
        zfs = {n: mocker.patch(f"zfsbackup.api.zfs.{n}") for n in ("exists", "list", "create", "set")}
        authed.gate.start_draining()
        resp = authed.client().post(
            "/backup/client1/pool/data/negotiate", json={"snapshots": ["a"], "config": "x"}
        )
        assert resp.status_code == 503
        assert resp.get_json() == RELOADING_ERROR
        assert resp.get_json()["retry"] is True
        for name, mock in zfs.items():
            assert not mock.called, f"zfs.{name} was called while draining"

    def test_negotiate_works_before_draining(self, authed, mocker):
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.api.zfs.list", return_value=[])
        resp = authed.client().post("/backup/client1/pool/data/negotiate", json={"snapshots": []})
        assert resp.status_code == 200

    def test_read_only_endpoints_keep_serving_while_draining(self, authed):
        authed.gate.start_draining()
        client = authed.client()
        assert client.get("/health").status_code == 200
        assert client.get("/config").status_code == 200
        assert client.get("/datasets").status_code == 200

    def test_a_disabled_server_still_answers_disabled_not_reloading(self, mocker):
        authed = _authed(mocker, _config(remote=False))
        authed.gate.start_draining()
        resp = authed.client().post("/backup/client1/pool/data/negotiate", json={})
        assert resp.status_code == 503
        assert resp.get_json() == {"error": "remote_backup disabled"}

    def _handler(self, authed):
        return authed.app.view_functions["backup_stream"].__wrapped__

    def test_stream_refuses_while_draining_and_never_starts_receive(self, authed, mocker):
        exists = mocker.patch("zfsbackup.api.zfs.exists")
        create = mocker.patch("zfsbackup.api.zfs.create")
        receive = mocker.patch("zfsbackup.api.zfs.receive.filesystem")
        gate = authed.gate
        gate.start_draining()
        ws = MagicMock()
        ws.receive.side_effect = ConnectionClosed(None, None)       # a regression must fail, not spin
        with authed.context(STREAM_PATH, query_string={"mode": "resume"}):
            self._handler(authed)(ws, "client1", "pool/data")
        assert json.loads(ws.send.call_args[0][0]) == RELOADING_ERROR
        ws.receive.assert_not_called()
        receive.assert_not_called()
        exists.assert_not_called()
        create.assert_not_called()
        assert gate.active == 0

    def test_a_refused_stream_survives_a_dead_socket(self, authed):
        authed.gate.start_draining()
        ws = MagicMock()
        ws.send.side_effect = Exception("closed")
        ws.receive.side_effect = ConnectionClosed(None, None)
        with authed.context(STREAM_PATH, query_string={"mode": "resume"}):
            self._handler(authed)(ws, "client1", "pool/data")       # must not raise

    def test_a_stream_is_counted_while_it_runs(self, authed, mocker):
        gate = authed.gate
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        writer = MagicMock()
        mocker.patch("zfsbackup.api.zfs.receive.filesystem", return_value=writer)
        seen = []
        ws = MagicMock()

        def receive(timeout=None):
            seen.append(gate.active)
            return json.dumps({"done": True})

        ws.receive.side_effect = receive
        with authed.context(STREAM_PATH, query_string={"mode": "resume"}):
            self._handler(authed)(ws, "client1", "pool/data")
        assert seen == [1]
        assert gate.active == 0

    def test_a_stream_begun_before_draining_runs_to_completion(self, authed, mocker):
        gate = authed.gate
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        writer = MagicMock()
        mocker.patch("zfsbackup.api.zfs.receive.filesystem", return_value=writer)
        ws = MagicMock()

        def receive(timeout=None):
            gate.start_draining()             # the worker decided to leave mid-transfer
            return json.dumps({"done": True})

        ws.receive.side_effect = receive
        with authed.context(STREAM_PATH, query_string={"mode": "resume"}):
            self._handler(authed)(ws, "client1", "pool/data")
        writer.close.assert_called_once()
        assert json.loads(ws.send.call_args[0][0]) == {"status": "ok"}

    @pytest.mark.parametrize(
        "arrange",
        ["exists-raises", "create-fails", "set-fails", "receive-raises", "send-raises",
         "connection-closed", "write-error", "ok"],
    )
    def test_the_gate_is_released_on_every_path(self, authed, mocker, source_props, arrange):
        gate = authed.gate
        ws = MagicMock()
        ws.receive.side_effect = ConnectionClosed(None, None)    # a regression must fail, not spin
        writer = MagicMock()
        exists = mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.api.zfs.receive.filesystem", return_value=writer)
        expect_raise = False
        if arrange == "exists-raises":
            # The D7 source check reads `exists` first and reports a failure to the client.
            exists.side_effect = RuntimeError("zfs exploded")
        elif arrange == "create-fails":
            exists.return_value = False
            mocker.patch("zfsbackup.api.zfs.create", side_effect=RuntimeError("cannot create"))
        elif arrange == "set-fails":
            exists.return_value = False
            mocker.patch("zfsbackup.api.zfs.create")
            mocker.patch("zfsbackup.api.zfs.set", side_effect=RuntimeError("cannot set"))
        elif arrange == "send-raises":
            # A refusal that cannot be sent escapes the handler; the gate still releases.
            source_props.return_value = {"org.zfsbackup:source_dataset": "tank/data"}
            ws.send.side_effect = RuntimeError("socket gone")
            expect_raise = True
        elif arrange == "receive-raises":
            mocker.patch("zfsbackup.api.zfs.receive.filesystem", side_effect=RuntimeError("no"))
        elif arrange == "connection-closed":
            ws.receive.side_effect = ConnectionClosed(None, None)
        elif arrange == "write-error":
            writer.write.side_effect = IOError("disk full")
            ws.receive.side_effect = [b"chunk"]
        else:
            ws.receive.side_effect = [b"x", json.dumps({"done": True})]
        with authed.context(STREAM_PATH, query_string={"mode": "resume"}):
            if expect_raise:
                with pytest.raises(RuntimeError):
                    self._handler(authed)(ws, "client1", "pool/data")
            else:
                self._handler(authed)(ws, "client1", "pool/data")
        assert gate.active == 0, f"gate leaked on path {arrange}"

    def test_the_gate_is_released_in_dry_run(self, mocker):
        authed = _authed(mocker, _config(dry_run=True))
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        ws = MagicMock()
        with authed.context(STREAM_PATH, query_string={"mode": "resume"}):
            authed.app.view_functions["backup_stream"].__wrapped__(ws, "client1", "pool/data")
        assert authed.gate.active == 0
        assert json.loads(ws.send.call_args[0][0])["dry_run"] is True


class TestGateOnProtocolRefusals:
    """Every way the stream/negotiate protocol refuses a request leaves the
    gate balanced (a leaked registration would hang a reload's drain)."""

    SERVER_DS = "pool/backups/client1/data"

    def _run(self, authed, ws, mode):
        qs = {} if mode is None else {"mode": mode}
        with authed.context(STREAM_PATH, query_string=qs):
            authed.app.view_functions["backup_stream"].__wrapped__(ws, "client1", "pool/data")

    @pytest.mark.parametrize(
        "arrange, mode, expect",
        [
            ("none", None, "stream mode required"),
            ("none", "bogus", "unknown stream mode 'bogus'"),
            ("mismatch", "fresh", "refusing to mix in"),
            ("mismatch", "resume", "refusing to mix in"),
            ("source-read-fails", "fresh", "Failed to check pool/backups/client1/data: "),
            ("token-lookup-fails", "fresh", "for partial receive state"),
            ("abort-fails", "fresh", "Failed to discard the partial receive state"),
            ("receive-start-fails", "resume", "Failed to start zfs receive"),
        ],
    )
    def test_the_gate_is_balanced_and_nothing_is_received(
        self, authed, mocker, source_props, arrange, mode, expect
    ):
        gate = authed.gate
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.api.zfs.create")
        abort = mocker.patch("zfsbackup.api.zfs.receive.abort")
        writer = MagicMock()
        receive = mocker.patch("zfsbackup.api.zfs.receive.filesystem", return_value=writer)
        row = MagicMock()
        row.__getitem__ = MagicMock(return_value="1-token")
        lister = mocker.patch("zfsbackup.api.zfs.list", return_value=[row])
        if arrange == "mismatch":
            source_props.return_value = {"org.zfsbackup:source_dataset": "tank/data"}
        elif arrange == "source-read-fails":
            source_props.side_effect = RuntimeError("zfs get failed")
        elif arrange == "token-lookup-fails":
            lister.side_effect = RuntimeError("zfs list failed")
        elif arrange == "abort-fails":
            abort.side_effect = Exception("pool I/O is currently suspended")
        elif arrange == "receive-start-fails":
            receive.side_effect = Exception("cannot start")
        ws = MagicMock()
        ws.receive.side_effect = ConnectionClosed(None, None)     # a regression must fail, not spin
        seen = []
        real_send = ws.send
        ws.send.side_effect = lambda m: seen.append(gate.active)
        self._run(authed, ws, mode)
        sent = json.loads(real_send.call_args[0][0])
        assert expect in sent["error"]
        assert gate.active == 0
        if arrange not in ("receive-start-fails",):
            receive.assert_not_called()
            ws.receive.assert_not_called()
        # Mode errors are refused before the gate; every later refusal holds it while answering.
        assert seen == ([0] if arrange == "none" else [1])

    def test_an_invalid_server_dataset_name_is_refused_and_balances_the_gate(self, authed, mocker):
        gate = authed.gate
        exists = mocker.patch("zfsbackup.api.zfs.exists")
        receive = mocker.patch("zfsbackup.api.zfs.receive.filesystem")
        ws = MagicMock()
        ws.receive.side_effect = ConnectionClosed(None, None)
        with authed.context(STREAM_PATH, query_string={"mode": "fresh"}):
            authed.app.view_functions["backup_stream"].__wrapped__(ws, "client1", "pool//x")
        assert "invalid dataset name" in json.loads(ws.send.call_args[0][0])["error"]
        exists.assert_not_called()
        receive.assert_not_called()
        ws.receive.assert_not_called()
        assert gate.active == 0

    def test_a_dry_run_reply_leaves_the_gate_balanced(self, mocker):
        authed = _authed(mocker, _config(dry_run=True))
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        ws = MagicMock()
        self._run(authed, ws, "fresh")
        assert json.loads(ws.send.call_args[0][0]) == {"status": "ok", "dry_run": True}
        assert authed.gate.active == 0

    @pytest.mark.parametrize("arrange, status", [("mismatch", 409), ("read-fails", 500)])
    def test_negotiate_refusals_balance_the_gate(self, authed, mocker, source_props, arrange, status):
        gate = authed.gate
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        mock_set = mocker.patch("zfsbackup.api.zfs.set")
        if arrange == "mismatch":
            source_props.return_value = {"org.zfsbackup:source_dataset": "tank/data"}
        else:
            source_props.side_effect = RuntimeError("zfs get failed")
        resp = authed.client().post(
            "/backup/client1/pool/data/negotiate", json={"snapshots": [], "config": "cfg"}
        )
        assert resp.status_code == status
        mock_set.assert_not_called()
        assert gate.active == 0


class TestServerReceiveTimeout:
    """18.4: a client that stops sending must not hold the receive (and, through
    the gate, the reload) forever."""

    def _handler(self, authed):
        return authed.app.view_functions["backup_stream"].__wrapped__

    def test_receive_is_called_with_the_idle_timeout(self, authed, mocker):
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.api.zfs.receive.filesystem", return_value=MagicMock())
        ws = MagicMock()
        ws.receive.side_effect = [b"a", json.dumps({"done": True})]
        with authed.context(STREAM_PATH, query_string={"mode": "resume"}):
            self._handler(authed)(ws, "client1", "pool/data")
        assert ws.receive.call_count == 2
        for call in ws.receive.call_args_list:
            assert call.kwargs == {"timeout": WS_IDLE_TIMEOUT}

    def test_none_closes_the_writer_sends_a_timeout_error_and_releases_the_gate(self, authed, mocker):
        gate = authed.gate
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        writer = MagicMock()
        mocker.patch("zfsbackup.api.zfs.receive.filesystem", return_value=writer)
        ws = MagicMock()
        ws.receive.side_effect = [b"chunk", None]
        with authed.context(STREAM_PATH, query_string={"mode": "resume"}):
            self._handler(authed)(ws, "client1", "pool/data")
        writer.write.assert_called_once_with(b"chunk")
        writer.close.assert_called_once()
        sent = json.loads(ws.send.call_args[0][0])
        assert "no data from the client" in sent["error"]
        assert str(WS_IDLE_TIMEOUT) in sent["error"]
        assert "status" not in sent
        assert gate.active == 0

    def test_a_timeout_on_the_first_receive_also_closes_the_writer(self, authed, mocker):
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        writer = MagicMock()
        mocker.patch("zfsbackup.api.zfs.receive.filesystem", return_value=writer)
        ws = MagicMock()
        ws.receive.return_value = None
        with authed.context(STREAM_PATH, query_string={"mode": "resume"}):
            self._handler(authed)(ws, "client1", "pool/data")
        writer.write.assert_not_called()
        writer.close.assert_called_once()

    def test_a_writer_close_failure_during_a_timeout_is_suppressed_and_still_reported(self, authed, mocker):
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        writer = MagicMock()
        writer.close.side_effect = Exception("receive exited 1")
        mocker.patch("zfsbackup.api.zfs.receive.filesystem", return_value=writer)
        ws = MagicMock()
        ws.receive.return_value = None
        with authed.context(STREAM_PATH, query_string={"mode": "resume"}):
            self._handler(authed)(ws, "client1", "pool/data")       # must not raise
        assert "no data from the client" in json.loads(ws.send.call_args[0][0])["error"]


# ---------------------------------------------------------------------------
# ApiWorker.run: drain, then leave
# ---------------------------------------------------------------------------


class FakeServer:
    """What `_bind_api_server` returns: `serve_forever` blocks until `shutdown`,
    and every call that matters is appended to a shared event list."""

    def __init__(self, events, *, serve_error=None, shutdown_error=None, close_error=None):
        self.events = events
        self._halt = threading.Event()
        self.serve_error = serve_error
        self.shutdown_error = shutdown_error
        self.close_error = close_error

    def serve_forever(self):
        if self.serve_error is not None:
            raise self.serve_error
        self._halt.wait(120)

    def shutdown(self):
        self.events.append("shutdown")
        self._halt.set()
        if self.shutdown_error is not None:
            raise self.shutdown_error

    def server_close(self):
        self.events.append("server_close")
        if self.close_error is not None:
            raise self.close_error


class ApiEnv:
    def __init__(self):
        self.events = []
        self.app = None
        self.server = None
        self.listening = threading.Event()
        self.stop = StopFlag()
        self.epoch = ReloadEpoch()
        self.worker = None
        self.log = []
        #: The client certificate pinned under `client1` in the worker's config database.
        self.cert = None

    @property
    def gate(self):
        return self.app.extensions["zfsbackup.stream_gate"]

    def client(self):
        """A test client of the worker's real app, presenting the pinned certificate."""
        return buffered_client(self.app, self.cert)

    def wait_listening(self):
        assert self.listening.wait(30), "the API worker never started listening"
        time.sleep(0.15)             # past `thread.start()` into the wait loop


@pytest.fixture
def env(mocker, monkeypatch, tls_config_db):
    """A REAL `ApiWorker` over a real config database and server identity (HTTPS only: it
    exits 1 without them), with the app real too and only the listening server faked. The
    certificate `env.cert` is pinned under `client1` through the real trust store, so the
    worker's gateway authenticates `env.client()`."""
    env = ApiEnv()
    env.config = _config()
    env.cert = make_client_cert(cn="client1")
    pin_client(tls_config_db, env.cert, CLIENT_ID)
    mocker.patch.object(DatasetManager, "datasets", new_callable=PropertyMock, return_value=[])
    real_create = api_module.create_app

    def create(config, trust, server_fingerprint, **kwargs):
        env.gateway = trust
        env.app = real_create(config, trust, server_fingerprint, **kwargs)
        env.app.testing = True
        return env.app

    mocker.patch("zfsbackup.api.create_app", side_effect=create)
    env.server_kwargs = {}

    def bind(app, host, port, gateway):
        assert gateway is env.gateway, "the connections' TLS contexts come from the app's gateway"
        env.server = FakeServer(env.events, **env.server_kwargs)
        env.listening_after_thread = True
        return env.server

    mocker.patch("zfsbackup.workers._bind_api_server", side_effect=bind)
    real_start = threading.Thread.start

    def start(thread):
        real_start(thread)
        if getattr(thread, "_target", None) is workers._serve:
            env.listening.set()

    monkeypatch.setattr(threading.Thread, "start", start)
    env.worker = ApiWorker(
        env.config, env.stop, reload_epoch=env.epoch, born_epoch=0, config_db=tls_config_db
    )
    return env


def run_worker(env, driver, *, patience=40):
    """Run the worker in this (main) thread -- `_init_child_process` installs
    signal handlers -- with `driver(env)` running alongside in another thread.
    -> (outcome, exit code, elapsed seconds); the worker stops itself if the
    driver wedges (so a failure is an assertion, not a hang)."""
    errors = []

    def drive():
        try:
            driver(env)
        except BaseException as exc:      # noqa: BLE001 -- reported below
            errors.append(exc)
            env.stop.set()

    thread = threading.Thread(target=drive, daemon=True)
    def bail():
        # A worker that never leaves (the bug these tests catch) must fail an
        # assertion, not hang the run: raise the stop flag AND let any stream out.
        env.stop.set()
        if env.app is not None:
            for _ in range(env.gate.active):
                env.gate.end()

    timer = threading.Timer(patience, bail)
    timer.daemon = True
    timer.start()
    thread.start()
    started = time.monotonic()
    outcome, code = "returned", None
    try:
        env.worker.run()
    except SystemExit as exc:
        outcome, code = "exit", exc.code
    finally:
        timer.cancel()
        env.elapsed = time.monotonic() - started
        env.finished_at = time.monotonic()
        env.stop.set()                    # release a driver still waiting
    thread.join(30)
    if errors:
        raise errors[0]
    return outcome, code


class TestApiWorkerDrain:
    def test_a_reload_with_nothing_in_flight_exits_3_after_shutdown_then_close(self, env, caplog):
        env.epoch.set(1)
        with caplog.at_level(logging.INFO):
            outcome, code = run_worker(env, lambda e: None)
        assert (outcome, code) == ("exit", 3)
        assert env.events == ["shutdown", "server_close"]
        assert "waiting for" not in caplog.text
        assert "API worker finished its current work; exiting to switch configuration" in caplog.text

    def test_a_reload_waits_for_an_in_flight_stream_then_exits_3(self, env, caplog):
        seen = {}

        def driver(e):
            e.wait_listening()
            assert e.gate.begin()                      # an inbound receive
            e.epoch.set(1)
            assert wait_until(lambda: e.gate.draining)
            time.sleep(1.5)                            # three poll slices later:
            seen["events_while_waiting"] = list(e.events)
            seen["refused"] = e.gate.begin()
            client = e.client()
            seen["negotiate"] = client.post("/backup/client1/pool/data/negotiate", json={"snapshots": []})
            seen["health"] = client.get("/health").status_code
            seen["ended_at"] = time.monotonic()
            e.gate.end()

        with caplog.at_level(logging.INFO):
            outcome, code = run_worker(env, driver)
        assert (outcome, code) == ("exit", 3)
        assert seen["events_while_waiting"] == [], "the server was shut down under a running receive"
        assert seen["refused"] is False, "a new stream was accepted while draining"
        assert seen["negotiate"].status_code == 503 and seen["negotiate"].get_json()["retry"] is True
        assert seen["health"] == 200
        assert env.finished_at >= seen["ended_at"]
        assert env.events == ["shutdown", "server_close"]
        assert "waiting for 1 in-flight receive(s) to finish before reloading" in caplog.text

    def test_a_reload_wait_is_not_bounded_by_the_stop_drain(self, env, monkeypatch):
        monkeypatch.setattr(workers, "API_STOP_DRAIN", 0.3)
        seen = {}

        def driver(e):
            e.wait_listening()
            e.gate.begin()
            e.epoch.set(1)
            time.sleep(1.5)                            # five times API_STOP_DRAIN
            seen["alive"] = list(e.events)
            e.gate.end()

        outcome, code = run_worker(env, driver)
        assert (outcome, code) == ("exit", 3)
        assert seen["alive"] == []

    def test_a_stop_during_a_reload_drain_is_bounded_and_exits_0(self, env, monkeypatch, caplog):
        monkeypatch.setattr(workers, "API_STOP_DRAIN", 1.0)
        marks = {}

        def driver(e):
            e.wait_listening()
            e.gate.begin()                              # never ended
            e.epoch.set(1)
            assert wait_until(lambda: e.gate.draining)
            time.sleep(0.6)
            marks["stopped_at"] = time.monotonic()
            e.stop.set()

        with caplog.at_level(logging.INFO):
            outcome, code = run_worker(env, driver)
        assert (outcome, code) == ("returned", None)           # exit status 0
        assert env.finished_at - marks["stopped_at"] < 1.0 + 4.0
        assert "abandoning 1 in-flight receive(s)" in caplog.text
        assert env.events == ["shutdown", "server_close"]
        assert "exiting to switch configuration" not in caplog.text

    def test_a_stop_during_a_reload_drain_that_ends_in_time_does_not_abandon(self, env, monkeypatch, caplog):
        monkeypatch.setattr(workers, "API_STOP_DRAIN", 5.0)

        def driver(e):
            e.wait_listening()
            e.gate.begin()
            e.epoch.set(1)
            assert wait_until(lambda: e.gate.draining)
            e.stop.set()
            time.sleep(0.8)
            e.gate.end()

        with caplog.at_level(logging.INFO):
            outcome, code = run_worker(env, driver)
        assert (outcome, code) == ("returned", None)
        assert "abandoning" not in caplog.text

    def test_a_stop_past_the_drain_limit_abandons_with_a_warning_and_exits_0(self, env, monkeypatch, caplog):
        monkeypatch.setattr(workers, "API_STOP_DRAIN", 0.8)

        def driver(e):
            e.wait_listening()
            e.gate.begin()
            e.stop.set()

        with caplog.at_level(logging.INFO):
            outcome, code = run_worker(env, driver)
        assert (outcome, code) == ("returned", None)
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "abandoning" in r.getMessage()]
        assert len(warnings) == 1
        assert "zfs receive -s" in warnings[0].getMessage()
        assert env.events == ["shutdown", "server_close"]
        assert env.elapsed < 15

    def test_a_stop_with_nothing_in_flight_neither_waits_nor_warns(self, env, caplog):
        def driver(e):
            e.wait_listening()
            e.stop.set()

        with caplog.at_level(logging.INFO):
            outcome, code = run_worker(env, driver)
        assert (outcome, code) == ("returned", None)
        assert "abandoning" not in caplog.text and "waiting for" not in caplog.text
        assert env.events == ["shutdown", "server_close"]
        assert env.gate.draining is True            # a stopping server refuses new streams too

    def test_a_gone_supervisor_drains_for_the_bounded_time_too(self, env, monkeypatch, caplog):
        world = Parent(monkeypatch)
        env.worker.supervisor_pid = SUPERVISOR
        env.worker.supervisor_start = None
        monkeypatch.setattr(workers.multiprocessing, "get_start_method", lambda allow_none=False: "spawn")
        monkeypatch.setattr(workers, "API_STOP_DRAIN", 0.8)

        def driver(e):
            e.wait_listening()
            e.gate.begin()
            world.parent = 1

        with caplog.at_level(logging.INFO):
            outcome, code = run_worker(env, driver)
        assert (outcome, code) == ("returned", None)
        assert "supervisor 4242 is gone" in caplog.text
        assert "abandoning 1 in-flight receive(s)" in caplog.text
        assert env.events == ["shutdown", "server_close"]

    def test_a_gone_supervisor_aborts_an_unbounded_reload_wait(self, env, monkeypatch, caplog):
        world = Parent(monkeypatch)
        env.worker.supervisor_pid = SUPERVISOR
        env.worker.supervisor_start = None
        monkeypatch.setattr(workers.multiprocessing, "get_start_method", lambda allow_none=False: "spawn")
        monkeypatch.setattr(workers, "API_STOP_DRAIN", 0.5)

        def driver(e):
            e.wait_listening()
            e.gate.begin()
            e.epoch.set(1)
            assert wait_until(lambda: e.gate.draining)
            time.sleep(0.6)
            world.parent = 1

        outcome, code = run_worker(env, driver)
        assert (outcome, code) == ("returned", None)      # became a stop, not a reload

    @pytest.mark.parametrize("how", ["stop", "reload"])
    def test_shutdown_then_server_close_on_every_clean_exit(self, env, how):
        def driver(e):
            e.wait_listening()
            if how == "stop":
                e.stop.set()
            else:
                e.epoch.set(1)

        outcome, code = run_worker(env, driver)
        assert (outcome, code) == (("returned", None) if how == "stop" else ("exit", 3))
        assert env.events == ["shutdown", "server_close"]

    def test_the_server_thread_is_reaped_before_exit(self, env):
        def driver(e):
            e.wait_listening()
            e.stop.set()

        before = threading.active_count()
        run_worker(env, driver)
        assert wait_until(lambda: threading.active_count() <= before)

    def test_a_failing_shutdown_still_closes_the_socket(self, env):
        env.server_kwargs = {"shutdown_error": RuntimeError("shutdown failed")}

        def driver(e):
            e.wait_listening()
            e.epoch.set(1)

        outcome, code = run_worker(env, driver)
        assert (outcome, code) == ("exit", 3)
        assert env.events == ["shutdown", "server_close"]

    def test_a_failing_server_close_does_not_change_the_exit(self, env):
        env.server_kwargs = {"close_error": OSError(errno.EBADF, "bad fd")}

        def driver(e):
            e.wait_listening()
            e.epoch.set(1)

        assert run_worker(env, driver) == ("exit", 3)
        assert env.events == ["shutdown", "server_close"]

    def test_a_dead_server_thread_exits_1_and_does_not_shut_down_a_dead_thread(self, env, caplog):
        env.server_kwargs = {"serve_error": RuntimeError("serve failed")}
        with caplog.at_level(logging.INFO):
            outcome, code = run_worker(env, lambda e: None)
        assert (outcome, code) == ("exit", 1)
        assert env.events == ["server_close"]               # no shutdown() on an already-dead thread
        assert "API server thread exited unexpectedly" in caplog.text

    def test_a_dead_thread_with_a_pending_reload_is_still_a_crash(self, env):
        env.server_kwargs = {"serve_error": RuntimeError("serve failed")}
        env.epoch.set(1)
        outcome, code = run_worker(env, lambda e: None)
        # The reload reason is seen first on the loop's first pass -- and then the
        # exit is the reload exit; what matters is the socket is closed either way.
        assert code in (1, 3)
        assert "server_close" in env.events

    def test_a_bind_failure_exits_1_with_no_shutdown_and_no_close(self, env, mocker, caplog):
        mocker.patch("zfsbackup.workers._bind_api_server", side_effect=OSError(errno.EADDRINUSE, "Address already in use"))
        with caplog.at_level(logging.INFO):
            outcome, code = run_worker(env, lambda e: None)
        assert (outcome, code) == ("exit", 1)
        assert env.events == []
        assert "cannot listen" in caplog.text
        assert "API listening" not in caplog.text

    def test_the_gate_handed_to_the_drain_is_the_apps_own(self, env):
        def driver(e):
            e.wait_listening()
            e.stop.set()

        run_worker(env, driver)
        assert env.app.extensions["zfsbackup.stream_gate"].draining is True

    def test_the_stop_drain_constant(self):
        assert API_STOP_DRAIN == 20


# ---------------------------------------------------------------------------
# negotiate / resume_token (and the trust writers) are gated too (`_gated`)
# ---------------------------------------------------------------------------

GATED = [
    ("negotiate", "post", "/backup/client1/pool/data/negotiate", {"snapshots": []}),
    ("resume_token", "get", "/backup/client1/pool/data/resume_token", None),
]


def _call(client, method, url, body):
    return getattr(client, method)(url, json=body) if body is not None else getattr(client, method)(url)


class TestGatedEndpoints:
    @pytest.fixture
    def zfs(self, mocker):
        return {n: mocker.patch(f"zfsbackup.api.zfs.{n}", return_value=True if n == "exists" else []) for n in
                ("exists", "list", "create", "set")}

    def test_endpoint_names_are_preserved(self, authed):
        for name in ("backup_negotiate", "backup_resume_token", "backup_stream", "enroll", "rotate"):
            assert name in authed.app.view_functions
            assert authed.app.view_functions[name].__name__ == name
        rules = {r.rule for r in authed.app.url_map.iter_rules()}
        assert {"/enroll", "/rotate", "/backup/<client_id>/<path:dataset>/negotiate",
                "/backup/<client_id>/<path:dataset>/resume_token"} <= rules
        assert "backup_register" not in authed.app.view_functions
        assert "/backup/register" not in rules

    @pytest.mark.parametrize("name, method, url, body", GATED, ids=[g[0] for g in GATED])
    def test_while_draining_it_is_503_reloading_and_calls_no_zfs(self, authed, zfs, name, method, url, body):
        gate = authed.gate
        gate.start_draining()
        resp = _call(authed.client(), method, url, body)
        assert resp.status_code == 503
        assert resp.get_json() == RELOADING_ERROR
        for fn, mock in zfs.items():
            assert not mock.called, f"zfs.{fn} called while draining"
        assert gate.active == 0

    @pytest.mark.parametrize("name, method, url, body", GATED, ids=[g[0] for g in GATED])
    def test_a_disabled_server_says_disabled_not_reloading(self, mocker, name, method, url, body):
        authed = _authed(mocker, _config(remote=False))
        authed.gate.start_draining()
        resp = _call(authed.client(), method, url, body)
        assert resp.status_code == 503
        assert resp.get_json() == {"error": "remote_backup disabled"}
        assert authed.gate.active == 0

    @pytest.mark.parametrize("name, method, url, body", GATED, ids=[g[0] for g in GATED])
    def test_before_draining_it_works(self, authed, zfs, name, method, url, body):
        resp = _call(authed.client(), method, url, body)
        assert resp.status_code == 200
        assert resp.get_json() != RELOADING_ERROR

    @pytest.mark.parametrize("name, method, url, body", GATED, ids=[g[0] for g in GATED])
    def test_the_gate_is_released_after_a_normal_answer(self, authed, zfs, name, method, url, body):
        _call(authed.client(), method, url, body)
        assert authed.gate.active == 0

    def test_a_400_answer_releases_the_gate(self, authed, zfs):
        resp = authed.client().post("/backup/client1/pool/a%25b/negotiate", json={})
        assert resp.status_code == 400
        assert authed.gate.active == 0

    @pytest.mark.parametrize("name, method, url, body", GATED, ids=[g[0] for g in GATED])
    def test_an_exception_releases_the_gate(self, authed, mocker, name, method, url, body):
        mocker.patch("zfsbackup.api.zfs.exists", side_effect=RuntimeError("zfs exploded"))
        if name == "resume_token":
            # The token lookup reports any failure as a 500 rather than raising.
            resp = _call(authed.client(), method, url, body)
            assert resp.status_code == 500 and resp.get_json() == {"error": "zfs exploded"}
        else:
            with pytest.raises(RuntimeError):
                _call(authed.client(), method, url, body)
        assert authed.gate.active == 0

    @pytest.mark.parametrize("name, method, url, body", GATED, ids=[g[0] for g in GATED])
    def test_a_running_call_is_counted_and_finishes_after_draining_begins(self, authed, mocker, name, method, url, body):
        gate = authed.gate
        entered, release = threading.Event(), threading.Event()

        def blocking_exists(*a, **kw):
            entered.set()
            release.wait(30)
            return True

        mocker.patch("zfsbackup.api.zfs.exists", side_effect=blocking_exists)
        mocker.patch("zfsbackup.api.zfs.list", return_value=[])
        mocker.patch("zfsbackup.api.zfs.create")
        result = {}
        thread = threading.Thread(
            target=lambda: result.update(resp=_call(authed.client(), method, url, body)), daemon=True
        )
        thread.start()
        try:
            assert entered.wait(30)
            assert gate.active == 1, "an in-flight call must be counted"
            gate.start_draining()
            assert gate.wait_idle(0.2) is False, "a drain must wait for it"
            late = _call(authed.client(), method, url, body)
            assert late.status_code == 503             # new ones are refused meanwhile
        finally:
            release.set()
            thread.join(30)
        assert result["resp"].status_code == 200, "the call begun before the drain completes"
        assert gate.wait_idle(5) is True and gate.active == 0

    def test_read_only_endpoints_are_not_counted(self, authed, mocker):
        gate = authed.gate
        seen = []
        mocker.patch.object(DatasetManager, "datasets", new_callable=PropertyMock,
                            side_effect=lambda: seen.append(gate.active) or [])
        authed.client().get("/datasets")
        assert seen and set(seen) == {0}


class TestApiWorkerDrainsGatedCalls:
    def test_a_reload_waits_for_a_running_negotiate_then_exits_3(self, env, mocker):
        entered, release = threading.Event(), threading.Event()

        def blocking_exists(*a, **kw):
            entered.set()
            release.wait(30)
            return True

        mocker.patch("zfsbackup.api.zfs.exists", side_effect=blocking_exists)
        mocker.patch("zfsbackup.api.zfs.list", return_value=[])
        seen = {}

        def driver(e):
            e.wait_listening()
            result = {}
            t = threading.Thread(
                target=lambda: result.update(
                    resp=e.client().post("/backup/client1/pool/data/negotiate", json={"snapshots": []})
                ),
                daemon=True,
            )
            t.start()
            assert entered.wait(30)
            e.epoch.set(1)
            assert wait_until(lambda: e.gate.draining)
            time.sleep(1.2)
            seen["events"] = list(e.events)
            seen["active"] = e.gate.active
            seen["ended"] = time.monotonic()
            release.set()
            t.join(30)
            seen["status"] = result["resp"].status_code

        outcome, code = run_worker(env, driver)
        assert (outcome, code) == ("exit", 3)
        assert seen["active"] == 1 and seen["events"] == []
        assert seen["status"] == 200, "the negotiate begun before the reload was not cut off"
        assert env.finished_at >= seen["ended"]
        assert env.events == ["shutdown", "server_close"]


# ---------------------------------------------------------------------------
# /enroll and /rotate write trust, so they are gated too; /whoami and /health are not
# ---------------------------------------------------------------------------


def _enroll_request(authed, *, client_id="host-z"):
    """-> `(url, body)` of a well-formed `/enroll` for `authed` (token for its server)."""
    from zfsbackup import identity

    token, _ = identity.make_token(authed.server_fp)
    return "/enroll", enroll_body(authed.server_fp, client_id, make_client_cert(), str(token))


def _rotate_request(authed):
    return "/rotate", rotate_body(
        authed.server_fp, CLIENT_ID, authed.who.fingerprint, make_client_cert()
    )


TRUST_REQUESTS = {"enroll": _enroll_request, "rotate": _rotate_request}


class TestGatedTrustEndpoints:
    @pytest.fixture(autouse=True)
    def _no_zfs(self, mocker):
        mocker.patch("zfsbackup.api.zfs.exists", return_value=False)

    @staticmethod
    def _post(authed, name):
        url, body = TRUST_REQUESTS[name](authed)
        client = authed.client(None) if name == "enroll" else authed.client()
        return client.post(url, json=body)

    @staticmethod
    def _writer_calls(authed, name):
        gw = authed.gateway
        return gw.enroll_calls if name == "enroll" else gw.rotate_calls

    @pytest.mark.parametrize("name", sorted(TRUST_REQUESTS))
    def test_while_draining_it_is_503_reloading_and_writes_nothing(self, authed, name):
        authed.gate.start_draining()
        resp = self._post(authed, name)
        assert resp.status_code == 503
        assert resp.get_json() == RELOADING_ERROR
        assert self._writer_calls(authed, name) == []
        assert authed.gateway.refresh_calls == 0
        assert authed.gate.active == 0

    @pytest.mark.parametrize("name", sorted(TRUST_REQUESTS))
    def test_a_disabled_server_says_disabled_not_reloading(self, mocker, name):
        authed = _authed(mocker, _config(remote=False))
        authed.gate.start_draining()
        resp = self._post(authed, name)
        assert resp.status_code == 503
        assert resp.get_json() == {"error": "remote_backup disabled"}
        assert self._writer_calls(authed, name) == []

    @pytest.mark.parametrize("name", sorted(TRUST_REQUESTS))
    def test_before_draining_it_works(self, authed, name):
        resp = self._post(authed, name)
        assert resp.status_code == 200, resp.get_json()
        assert len(self._writer_calls(authed, name)) == 1
        assert authed.gate.active == 0

    @pytest.mark.parametrize("name", sorted(TRUST_REQUESTS))
    @pytest.mark.parametrize("answer", ["bad-json", "bad-version", "store-raises"])
    def test_every_way_out_releases_the_gate(self, authed, name, answer):
        url, body = TRUST_REQUESTS[name](authed)
        client = authed.client(None) if name == "enroll" else authed.client()
        if answer == "bad-json":
            resp = client.post(url, data=b"[1", content_type="application/json")
            assert resp.status_code == 400
        elif answer == "bad-version":
            resp = client.post(url, json={**body, "version": 2})
            assert resp.status_code == 400
        else:
            authed.gateway.enroll_effect = RuntimeError("database exploded")
            authed.gateway.rotate_effect = RuntimeError("database exploded")
            resp = client.post(url, json=body)
            assert resp.status_code == 503
        assert authed.gate.active == 0

    @pytest.mark.parametrize("name", sorted(TRUST_REQUESTS))
    def test_a_running_call_is_counted_and_finishes_after_draining_begins(self, authed, name):
        entered, release = threading.Event(), threading.Event()

        def block(**kwargs):
            entered.set()
            assert release.wait(30)
            from zfsbackup.config.store.trust import EnrollOutcome, RotateOutcome

            return EnrollOutcome.ENROLLED if name == "enroll" else RotateOutcome.ROTATED

        authed.gateway.enroll_effect = block
        authed.gateway.rotate_effect = block
        result = {}
        thread = threading.Thread(
            target=lambda: result.update(resp=self._post(authed, name)), daemon=True
        )
        thread.start()
        try:
            assert entered.wait(30)
            assert authed.gate.active == 1, "an in-flight trust write must be counted"
            authed.gate.start_draining()
            assert authed.gate.wait_idle(0.2) is False, "a drain must wait for it"
            assert self._post(authed, name).status_code == 503      # new ones are refused meanwhile
        finally:
            release.set()
            thread.join(30)
        assert result["resp"].status_code == 200
        assert authed.gate.wait_idle(5) is True

    def test_whoami_and_health_are_not_counted_and_answer_while_draining(self, authed):
        seen = []
        real_lookup = authed.gateway.lookup

        def lookup(fp):
            seen.append(authed.gate.active)
            return real_lookup(fp)

        authed.gateway.lookup = lookup
        authed.gate.start_draining()
        client = authed.client()
        assert client.get("/whoami").get_json()["client_id"] == CLIENT_ID
        assert client.get("/health").status_code == 200
        assert seen == [0]
        assert authed.gate.active == 0

    def test_whoami_answers_when_remote_backup_is_disabled(self, mocker):
        authed = _authed(mocker, _config(remote=False))
        assert authed.client().get("/whoami").status_code == 200


class TestApiWorkerDrainsTrustWrites:
    def test_a_reload_waits_for_a_running_enroll_then_exits_3(self, env, mocker):
        from zfsbackup import identity

        entered, release = threading.Event(), threading.Event()
        mocker.patch("zfsbackup.api.zfs.exists", return_value=False)

        def blocking_enroll(self, **kwargs):
            entered.set()
            release.wait(30)
            from zfsbackup.config.store.trust import EnrollOutcome

            return EnrollOutcome.ENROLLED

        mocker.patch.object(api_module.TrustGateway, "enroll_client", blocking_enroll)
        mocker.patch.object(api_module.TrustGateway, "token_precheck", lambda self, **kw: True)
        seen = {}

        def driver(e):
            e.wait_listening()
            token, _ = identity.make_token(e.gateway.server_fingerprint)
            body = enroll_body(e.gateway.server_fingerprint, "host-z", make_client_cert(), str(token))
            result = {}
            t = threading.Thread(
                target=lambda: result.update(
                    resp=buffered_client(e.app).post("/enroll", json=body)
                ),
                daemon=True,
            )
            t.start()
            assert entered.wait(30)
            e.epoch.set(1)
            assert wait_until(lambda: e.gate.draining)
            time.sleep(1.2)
            seen["events"] = list(e.events)
            seen["active"] = e.gate.active
            seen["ended"] = time.monotonic()
            release.set()
            t.join(30)
            seen["status"] = result["resp"].status_code

        outcome, code = run_worker(env, driver)
        assert (outcome, code) == ("exit", 3)
        assert seen["active"] == 1 and seen["events"] == []
        assert seen["status"] == 200, "the enrollment begun before the reload was not cut off"
        assert env.finished_at >= seen["ended"]
        assert env.events == ["shutdown", "server_close"]

"""Shared rig for the S-2 tests (mid-stream revocation and the write-stall bound).

Importable from any test module (`tests/` is on `sys.path`). Nothing here talks to ZFS (the
`zfs` calls of the stream route are patched) and every socket is `127.0.0.1`.

* `ChildWriter` -- the stand-in for `zfs receive`'s stdin: a REAL child process that reads its
  stdin until EOF, so "the receive was reaped" is a fact about a pid, not about a mock call.
* `StreamRig` -- the real `/stream` route (`api.create_app`) behind the real per-connection TLS
  server (`workers._api_server_class`), over a real `TrustGateway` on a real config database
  (the real `store.trust` writers), with the real trust-poll thread
  (`ApiWorker._start_trust_poll(logger, sessions)`) at a short interval. Only the `zfs` calls the
  route makes are patched; `zfs.receive.filesystem` hands out a `ChildWriter`.
* `ping_flood` -- a client that keeps pinging and never reads, so the server's pongs back up.
* `SessionApiWorker` / `ProcessRig` -- a real `ApiWorker` in a child process (fork, spawn or
  forkserver) whose `run` first patches the same `zfs` calls and records what the receive
  writer saw in a file, for the "under every start method" tests.
"""

import contextlib
import json
import logging
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import List, Optional

import websocket

from _api_support import RealTrust, streams_in_progress
from _reload_workers import SelectableApiWorker
from _stream_support import HEADER, get_value_answers
from _tls_support import IO_TIMEOUT, LOOPBACK, client_identity, insecure_client_context, pin_client, try_get
from zfsbackup import api, workers
from zfsbackup.backup_manager import PROP_CLIENT_ID, PROP_CONFIG
from zfsbackup.config import BackupConfig, DatasetConfig, RemoteServerConfig
from zfsbackup.protocol import protocol_headers
from zfsbackup.stopflag import StopFlag

#: The client id and dataset every rig stream uses.
CLIENT = "alice"
DATASET = "pool/data"
SERVER_DATASET = f"pool/backups/{CLIENT}/data"


def eventually(predicate, timeout=20.0, interval=0.02):
    """The truthy value of `predicate()` once it has one, else its last value."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return predicate()


def remote_config() -> BackupConfig:
    return BackupConfig(
        datasets=[DatasetConfig(name=DATASET)],
        remote_backup=RemoteServerConfig(target_dataset="pool/backups", enabled=True),
    )


def zfs_answers(owner: Optional[str] = None):
    """The `zfs.get.value` answers the stream route needs for an existing server dataset: the
    stored config, the LOCAL client id and the type. The client id is `owner`, else the one in
    the dataset name (`<target>/<client id>/...`), so every client owns its own datasets."""
    return get_value_answers({
        PROP_CONFIG: api.canonical_dataset_config(DatasetConfig(name=DATASET).to_property()),
        PROP_CLIENT_ID: lambda ds, prop, sources: owner or str(ds.name).split("/")[2],
        "type": "filesystem",               # B-2a: compared with the stream's type
    })


# ---------------------------------------------------------------------------
# The receive child
# ---------------------------------------------------------------------------

#: What a finished stream ends with: `zfs receive` of a truncated stream exits non-zero, so the
#: child exits 0 only when its input ended with this marker (`finish`).
END_OF_STREAM = b"END-OF-STREAM"

_READER = (
    "import sys\n"
    "tail = b''\n"
    "while True:\n"
    "    block = sys.stdin.buffer.read(65536)\n"
    "    if not block:\n"
    "        break\n"
    "    tail = (tail + block)[-64:]\n"
    f"sys.exit(0 if tail.endswith({END_OF_STREAM!r}) else 1)\n"
)


class ChildWriter:
    """A `StreamHandle` stand-in over a real child process. It reads stdin until EOF and exits 0
    if the stream ended with `END_OF_STREAM`, else 1; `close()` waits for it and, like the real
    handle, raises when the exit status is not 0."""

    def __init__(self):
        self.proc = subprocess.Popen(
            [sys.executable, "-I", "-c", _READER],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        self.writes: List[int] = []
        self.closes = 0
        self._lock = threading.Lock()

    def write(self, data) -> None:
        self.proc.stdin.write(data)
        self.proc.stdin.flush()
        with self._lock:
            self.writes.append(len(data))

    def close(self) -> None:
        with self._lock:
            self.closes += 1
        with contextlib.suppress(OSError):
            self.proc.stdin.close()
        status = self.proc.wait(timeout=60)
        if status != 0:
            raise RuntimeError(f"zfs receive exited {status}")

    @property
    def reaped(self) -> bool:
        return self.proc.poll() is not None

    def kill(self) -> None:
        with contextlib.suppress(Exception):
            self.proc.kill()
        with contextlib.suppress(Exception):
            self.proc.wait(timeout=10)
        with contextlib.suppress(Exception):
            self.proc.stdin.close()


# ---------------------------------------------------------------------------
# The in-process rig
# ---------------------------------------------------------------------------


class StreamRig:
    """See the module docstring. Build with `StreamRig(resolved, tmp_path, mocker, ...)`;
    `close()` (a fixture's finalizer) stops everything and kills every receive child.

    `poll` is `api.TRUST_POLL_SECONDS` (the tick of the trust-poll thread), `stall_limit`
    overrides the registry's write-stall limit, `small_buffers` gives the server's connections
    a tiny send buffer (so a client that does not read blocks a write quickly)."""

    def __init__(self, resolved, tmp_path: Path, mocker, *, poll: float = 0.1,
                 stall_limit: Optional[float] = None, small_buffers: bool = False,
                 ws_end_timeout: float = 5.0):
        self.resolved = resolved
        self.tmp_path = tmp_path
        self.small_buffers = small_buffers
        self.writers: List[ChildWriter] = []
        self.clients: list = []
        self.list_revoked_calls = 0
        self._closed = False

        mocker.patch.object(api, "TRUST_POLL_SECONDS", poll)
        mocker.patch("zfsbackup.api.read_zfsbackup_properties", return_value={})
        mocker.patch("zfsbackup.api.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.api.zfs.get.value", side_effect=zfs_answers())
        mocker.patch("zfsbackup.api.zfs.list", return_value=[])
        mocker.patch("zfsbackup.api.zfs.receive.filesystem", side_effect=self._new_writer)

        from zfsbackup.config import store

        def counting(resolved_path):
            self.list_revoked_calls += 1
            return store.list_revoked(resolved_path)

        self.trust = RealTrust(resolved, list_revoked=counting)
        self.gateway = self.trust.gateway
        self.config = remote_config()
        self.app = api.create_app(self.config, self.gateway, self.trust.server_fp)
        self.gate = self.app.extensions["zfsbackup.stream_gate"]
        self.sessions = self.app.extensions["zfsbackup.sessions"]
        if stall_limit is not None:
            self.sessions._stall_limit = stall_limit

        self.server = workers._api_server_class()(
            LOOPBACK, 0, self.app, self.gateway, ws_end_timeout=ws_end_timeout
        )
        if small_buffers:
            base = self.server.RequestHandlerClass

            class SmallBuffers(base):
                def make_environ(handler):
                    environ = super().make_environ()
                    with contextlib.suppress(OSError):
                        handler.connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
                    return environ

            self.server.RequestHandlerClass = SmallBuffers
        self.port = self.server.server_port
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

        # The real trust-poll thread of the real ApiWorker, wired as `ApiWorker._serve` wires it.
        self.worker = workers.ApiWorker(self.config, StopFlag())
        self.worker._trust = self.gateway
        self.poll_stopped = self.worker._start_trust_poll(
            logging.getLogger("zfsbackup.workers"), self.sessions
        )

    # -- identities
    def new_client(self, name: str, client_id: str = CLIENT, *, pin: bool = True):
        """A generated client identity; `pin` enrolls it under `client_id` through the real
        store (a first enrollment of that id) and refreshes the gateway."""
        ident = client_identity(self.tmp_path / f"{name}.tls")
        if pin:
            pin_client(self.resolved, ident, client_id)
            assert self.gateway.refresh()
        return ident

    # -- receive child
    def _new_writer(self, *args, **kwargs) -> ChildWriter:
        writer = ChildWriter()
        self.writers.append(writer)
        return writer

    # -- clients
    def connect(self, ident, *, client_id: str = CLIENT, dataset: str = DATASET,
                rcvbuf: Optional[int] = None, timeout: float = IO_TIMEOUT, mode: str = "resume"):
        """A websocket to the `/stream` route of `dataset`, authenticated by `ident`."""
        ctx = insecure_client_context(ident)
        raw = socket.socket()
        if rcvbuf is not None:
            raw.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        raw.settimeout(timeout)
        raw.connect((LOOPBACK, self.port))
        tls = ctx.wrap_socket(raw)
        ws = websocket.WebSocket()
        ws.connect(
            f"wss://{LOOPBACK}:{self.port}/backup/{client_id}/{dataset}/stream?mode={mode}",
            socket=tls, timeout=timeout, header=protocol_headers(),
        )
        self.clients.append(ws)
        return ws

    def start_stream(self, ws, *, chunks: int = 2) -> ChildWriter:
        """Send the header and `chunks` chunks; -> the receive child once it has taken them."""
        ws.send_binary(HEADER)
        for _ in range(chunks):
            ws.send_binary(b"x" * 4096)
        assert eventually(lambda: self.writers and len(self.writers[0].writes) >= 1, 30), (
            "the server never started the receive"
        )
        return self.writers[0]

    def only_session(self):
        found = eventually(lambda: self.registered() or None, 30)
        assert found and len(found) == 1, f"expected one registered session, got {found}"
        return found[0]

    def registered(self) -> list:
        with self.sessions._lock:
            return list(self.sessions._sessions)

    def claimed(self) -> set:
        return set(streams_in_progress(self.app))

    # -- teardown
    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.poll_stopped.set()
        for ws in self.clients:
            with contextlib.suppress(Exception):
                ws.shutdown()
        for writer in self.writers:
            writer.kill()
        with contextlib.suppress(Exception):
            self.server.shutdown()
        self.thread.join(15)
        with contextlib.suppress(Exception):
            self.server.server_close()
        for thread in threading.enumerate():
            if thread.name == "trust-poll":
                thread.join(5)


def finish(ws) -> dict:
    """End a rig stream properly: the end-of-stream marker, `done`, and the server's reply."""
    ws.send_binary(END_OF_STREAM)
    ws.send(json.dumps({"done": True}))
    return json.loads(ws.recv())


def ping_flood(ws, *, stop: threading.Event, limit: int = 400_000, payload: bytes = b"p" * 125,
               timeout: float = 2.0) -> SimpleNamespace:
    """Ping from a thread and never read: the server's pongs fill its send buffer. Ends at
    `stop`, after `limit` pings, or when the client's own send blocks for `timeout` seconds (the
    server is no longer reading). -> a namespace with `sent`, `exc` and `thread`."""
    box = SimpleNamespace(sent=0, exc=None, thread=None)
    ws.settimeout(timeout)

    def run():
        try:
            while box.sent < limit and not stop.is_set():
                ws.ping(payload)
                box.sent += 1
        except BaseException as exc:    # noqa: BLE001 -- the test looks at it
            box.exc = exc

    box.thread = threading.Thread(target=run, daemon=True)
    box.thread.start()
    return box


# ---------------------------------------------------------------------------
# A real ApiWorker in a child process
# ---------------------------------------------------------------------------


class _FileWriter:
    """The receive writer inside the child process: records what it saw in a file."""

    def __init__(self, path: str):
        self.path = path
        self._log("spawn")

    def _log(self, line: str) -> None:
        with open(self.path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    def write(self, data) -> None:
        self._log(f"write {len(data)}")

    def close(self) -> None:
        self._log("close")


class SessionApiWorker(SelectableApiWorker):
    """`ApiWorker.run`, unmodified, in a child started with `start_method`, after the `zfs`
    calls of the stream route were patched (in the child only) and the trust poll made fast."""

    record_path: str = ""
    poll_seconds: float = 0.1

    def run(self) -> None:
        from unittest import mock

        from zfsbackup import api as child_api

        child_api.TRUST_POLL_SECONDS = self.poll_seconds
        path = self.record_path
        mock.patch("zfsbackup.api.read_zfsbackup_properties", return_value={}).start()
        mock.patch("zfsbackup.api.zfs.exists", return_value=True).start()
        mock.patch("zfsbackup.api.zfs.get.value", side_effect=zfs_answers()).start()
        mock.patch("zfsbackup.api.zfs.list", return_value=[]).start()
        mock.patch("zfsbackup.api.zfs.receive.filesystem",
                   side_effect=lambda *a, **k: _FileWriter(path)).start()
        super().run()


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((LOOPBACK, 0))
        return s.getsockname()[1]


class ProcessRig:
    """A `SessionApiWorker` child on a free port; `events()` is what its receive writer
    recorded."""

    def __init__(self, resolved, method: str, tmp_path: Path):
        self.resolved = resolved
        self.tmp_path = tmp_path
        self.port = free_port()
        self.record = tmp_path / "receive.log"
        config = BackupConfig(
            datasets=[DatasetConfig(name=DATASET)], api_host=LOOPBACK, api_port=self.port,
            remote_backup=RemoteServerConfig(target_dataset="pool/backups", enabled=True),
        )
        self.stop = StopFlag()
        self.worker = SessionApiWorker(config, self.stop, config_db=resolved)
        self.worker.start_method = method
        self.worker.record_path = str(self.record)
        self.clients: list = []

    def __enter__(self):
        self.worker.start()
        return self

    def __exit__(self, *exc):
        self.halt()

    def ready(self, timeout: float = 90.0) -> None:
        def up():
            if not self.worker.is_alive():
                raise AssertionError(f"the API worker exited early (code {self.worker.exitcode})")
            result = try_get(self.port, "/health")
            return isinstance(result, tuple) and result[0] == 200

        assert eventually(up, timeout, 0.2), "the API worker never answered /health"

    def events(self) -> List[str]:
        try:
            return self.record.read_text(encoding="utf-8").splitlines()
        except FileNotFoundError:
            return []

    def connect(self, ident, *, timeout: float = IO_TIMEOUT):
        raw = socket.create_connection((LOOPBACK, self.port), timeout=timeout)
        tls = insecure_client_context(ident).wrap_socket(raw)
        ws = websocket.WebSocket()
        ws.connect(
            f"wss://{LOOPBACK}:{self.port}/backup/{CLIENT}/{DATASET}/stream?mode=resume",
            socket=tls, timeout=timeout, header=protocol_headers(),
        )
        self.clients.append(ws)
        return ws

    def halt(self) -> None:
        for ws in self.clients:
            with contextlib.suppress(Exception):
                ws.shutdown()
        self.stop.set()
        self.worker.join(45)
        if self.worker.is_alive():
            self.worker.terminate()
            self.worker.join(10)
        if self.worker.is_alive():
            self.worker.kill()
            self.worker.join(10)

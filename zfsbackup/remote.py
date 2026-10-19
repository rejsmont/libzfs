"""Client-side remote backup: identity management and ZFS stream transfer."""

import contextlib
import json
import logging
import re
import socket
import uuid
from pathlib import Path
from typing import Optional, Set, Tuple
from urllib.parse import quote

import requests
import websocket

import libzfseasy as zfs
from zfsbackup.backup_manager import DatasetInfo, DatasetManager
from zfsbackup.config import BackupConfig, Destination, RemoteDatasetConfig, _collapse_retention_rules


logger = logging.getLogger(__name__)

#: Websocket timeouts (seconds). Defined here and imported by `zfsbackup.api`
#: (never the other way round). Live config reload defers a worker switch for as
#: long as a transfer is in flight, so a peer that hangs forever would turn
#: "deferred" into "never"; these bound every blocking socket operation.
#:
#: TCP connect plus the websocket handshake.
WS_CONNECT_TIMEOUT = 30
#: No progress on one socket operation (a `send_binary`, or the server's
#: `receive` waiting for the next chunk).
WS_IDLE_TIMEOUT = 600
#: The client waiting for the server's status after `done`, while the
#: server's `zfs receive` may still be committing a large stream.
WS_FINAL_ACK_TIMEOUT = 3600


class NegotiationFailed(Exception):
    """`_negotiate` could not get an answer from the server. Distinct from a
    successful negotiation that found no common snapshot (which returns
    `None` and rightly leads to an initial full send): a failure must never
    fall through to a send. `reloading` is True for the server's retryable
    503 ("server is reloading; retry later")."""

    def __init__(self, message: str, reloading: bool = False):
        super().__init__(message)
        self.reloading = reloading


def _is_reloading(resp) -> bool:
    """Is `resp` the API's retryable "reloading" 503?"""
    if resp.status_code != 503:
        return False
    try:
        return bool(resp.json().get('retry'))
    except Exception:
        return False


def _is_reloading_refusal(result) -> bool:
    """Is `result` (a decoded websocket text message) the server's retryable
    "reloading" refusal (`api.RELOADING_ERROR`, `retry: True`)? A real error
    status carries no `retry`."""
    return (
        isinstance(result, dict)
        and result.get('status') != 'ok'
        and bool(result.get('retry'))
    )


def _peer_sent_reloading_refusal(ws) -> bool:
    """After a failed send: did the server leave its "reloading" refusal in
    the socket before closing? One bounded read (2 s), every error swallowed.

    While draining, the API worker sends the refusal as a text message and
    returns without reading, so the client's first `send_binary` may hit the
    closed socket (BrokenPipe / connection closed) and never see the reason."""
    try:
        ws.settimeout(2)
        return _is_reloading_refusal(json.loads(ws.recv()))
    except Exception:
        return False


def _path(component: str) -> str:
    """Percent-encode a client id / dataset name for use in a URL path
    (the API's routes decode it; `/` and `:` stay literal)."""
    return quote(component, safe='/:')


class ClientIdentity:
    """Persistent client identity in the form <hostname>-<uuid8>."""

    def __init__(self, id_file: Path):
        self.id_file = id_file
        self._client_id: Optional[str] = None

    @property
    def client_id(self) -> str:
        if self._client_id is None:
            self._client_id = self._load_or_generate()
        return self._client_id

    def _load_or_generate(self) -> str:
        if self.id_file.exists():
            value = self.id_file.read_text().strip()
            if value:
                return value
        client_id = self._generate()
        self.id_file.parent.mkdir(parents=True, exist_ok=True)
        self.id_file.write_text(client_id)
        logger.info(f"Generated new client ID: {client_id} (saved to {self.id_file})")
        return client_id

    @staticmethod
    def _generate() -> str:
        hostname = socket.gethostname()
        sanitized = re.sub(r'[^a-zA-Z0-9._-]', '-', hostname)
        return f"{sanitized}-{uuid.uuid4().hex[:8]}"


class RemoteBackupManager:
    """Negotiates and transfers ZFS snapshots to configured remote destinations."""

    def __init__(self, config: BackupConfig, local_manager: DatasetManager):
        self.config = config
        self.local_manager = local_manager
        self.identity = ClientIdentity(config.effective_client_id_file)
        # (dataset name, destination) pairs whose effective per-destination
        # retention-rule collisions have already been warned about, for this
        # manager's lifetime. `to_property(destination)` ships the resolved
        # ruleset unresolved (correct -- resolution is a read-path concern),
        # and the ONLY collision warning used to appear in the *receiving*
        # admin's journal, naming a server-side path the client operator does
        # not administer. The client must warn about its own collisions too.
        self._retention_warned: Set[Tuple[str, str]] = set()

    def backup_dataset(self, dsi: DatasetInfo, remote_cfg: RemoteDatasetConfig) -> bool:
        """Send a backup of dsi to the configured destination. Returns True on success."""
        dest = self.config.destinations.get(remote_cfg.destination)
        if dest is None:
            logger.error(f"Unknown destination '{remote_cfg.destination}' for {dsi.name}")
            return False

        self._warn_retention_collisions(dsi, remote_cfg.destination)

        base_url = dest.url.rstrip('/')
        client_id = self.identity.client_id

        if not self._register(base_url, client_id):
            return False

        snapshots = self.local_manager.list_snapshots(dsi)
        if not snapshots:
            logger.debug(f"No snapshots available to send for {dsi.name}")
            return True

        latest = snapshots[0]
        try:
            common_snap_name = self._negotiate(
                base_url, client_id, dsi, snapshots, remote_cfg.destination
            )
        except NegotiationFailed as e:
            # Never fall through to a send: with no answer we would treat
            # "unknown" as "no common snapshot" and start a full send.
            if e.reloading:
                logger.info(
                    f"server {base_url} is reloading; skipping {dsi.name} this cycle"
                )
            return False

        # Resolve the common snapshot object for incremental send
        common_snap = None
        if common_snap_name:
            common_snap = next((s for s in snapshots if s.name == common_snap_name), None)

        return self._transfer(base_url, client_id, dsi, latest, common_snap, remote_cfg.destination)

    def _warn_retention_collisions(self, dsi: DatasetInfo, destination: str) -> None:
        """Warn locally about retention-rule collisions in the ruleset that
        will actually be sent for `destination` (dataset-level, or the
        destination's own override if it declared one -- see
        `DatasetConfig.effective_retention_rules`). Once per (dataset,
        destination) for this manager's lifetime; the client's own config is
        loaded once at process start, so unlike the received-side warning in
        `DatasetManager._retention_plan`, the ruleset here cannot change
        without a restart (which creates a new manager and resets this set).
        """
        key = (dsi.name, destination)
        if key in self._retention_warned:
            return
        rules = dsi.config.effective_retention_rules(destination)
        _, collisions = _collapse_retention_rules(rules)
        if collisions:
            self._retention_warned.add(key)
            for msg in collisions:
                logger.warning(f"Dataset {dsi.name} -> {destination}: {msg}")

    def _register(self, base_url: str, client_id: str) -> bool:
        try:
            resp = requests.post(
                f"{base_url}/backup/register",
                json={'client_id': client_id, 'hostname': socket.gethostname()},
                timeout=30,
            )
            if _is_reloading(resp):
                logger.info(f"server {base_url} is reloading; registration skipped this cycle")
                return False
            resp.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Registration failed at {base_url}: {e}")
            return False

    def _negotiate(
        self, base_url: str, client_id: str, dsi: DatasetInfo, snapshots, destination: str
    ) -> Optional[str]:
        """The newest snapshot present on both sides, or `None` when the
        server answered that there is none. Raises `NegotiationFailed` when
        there was no usable answer (logged at ERROR, except the server's
        retryable "reloading" 503, which the caller logs at INFO)."""
        try:
            resp = requests.post(
                f"{base_url}/backup/{_path(client_id)}/{_path(dsi.name)}/negotiate",
                json={
                    'snapshots': [s.name for s in snapshots],
                    # Effective config for this destination -- a per-destination
                    # retention override (if any) replaces the dataset-level set
                    # entirely (see DatasetConfig.to_property and item 3b).
                    'config': dsi.config.to_property(destination),
                },
                timeout=30,
            )
            if _is_reloading(resp):
                raise NegotiationFailed("server is reloading", reloading=True)
            resp.raise_for_status()
            return resp.json().get('common_snapshot')
        except NegotiationFailed:
            raise
        except Exception as e:
            logger.error(f"Negotiation failed for {dsi.name} at {base_url}: {e}")
            raise NegotiationFailed(str(e)) from e

    def _transfer(
        self,
        base_url: str,
        client_id: str,
        dsi: DatasetInfo,
        latest,
        common_snap,
        destination: str,
    ) -> bool:
        # Check dry-run *before* starting a real `zfs send` subprocess: a
        # StreamHandle spawned here would otherwise be left unclosed (no
        # wait()/reap, no error surfaced) since we'd return before ever
        # reading or closing it.
        if self.config.dry_run:
            logger.info(
                f"[DRY RUN] Would send {dsi.name}@{latest.name}"
                + (f" (incremental from {common_snap.name})" if common_snap else " (full)")
                + f" -> {base_url}"
            )
            return True

        stream = self._get_resume_stream(base_url, client_id, dsi, latest, common_snap)
        if stream is None:
            return False

        ws_base = base_url.replace('https://', 'wss://', 1).replace('http://', 'ws://', 1)
        ws_url = f"{ws_base}/backup/{_path(client_id)}/{_path(dsi.name)}/stream"
        if common_snap:
            ws_url += f"?from={quote(common_snap.name, safe='')}"

        ws = websocket.WebSocket()
        entered = False
        failed = False
        sending = False
        try:
            ws.connect(ws_url, timeout=WS_CONNECT_TIMEOUT)
            ws.settimeout(WS_IDLE_TIMEOUT)
            chunk_size = 256 * 1024
            # `stream` is a StreamHandle wrapping the `zfs send` process's
            # stdout. Its close() waits on the subprocess and raises
            # if it exited non-zero, so drain it fully (the while loop below)
            # *before* closing it -- closing early could either
            # deadlock on a full pipe or reap the process before all data has
            # been read. Closing on an error (a socket timeout in
            # `send_binary`) is also safe: `close()` closes the read end of
            # the pipe FIRST, so `zfs send` gets EPIPE/SIGPIPE on its next
            # write and exits, and only then waits for it.
            entered = True
            sending = True
            try:
                while True:
                    chunk = stream.read(chunk_size)
                    if not chunk:
                        break
                    ws.send_binary(chunk)
            except BaseException:
                # Close with a non-zero exit of `zfs send` (which closing its
                # pipe just killed with SIGPIPE) suppressed, so the ORIGINAL
                # error -- "timed out" -- is what is re-raised and logged, not
                # the child's empty failure message that a `with` block's
                # close() would raise in its place.
                with contextlib.suppress(Exception):
                    stream.close()
                raise
            # Normal end: closing waits for `zfs send` and raises if it
            # exited non-zero, exactly as leaving a `with stream:` did.
            sending = False
            stream.close()
            # The `done` message is still part of the send phase: on a quiet
            # dataset the whole stream fits in the kernel buffer, and a server
            # refusal (its close/RST) first surfaces as a failure of this
            # send. A failing `stream.close()` above stays probe-free.
            sending = True
            ws.send(json.dumps({'done': True}))
            sending = False
            ws.settimeout(WS_FINAL_ACK_TIMEOUT)
            result = json.loads(ws.recv())
            if result.get('status') != 'ok':
                failed = True
                if _is_reloading_refusal(result):
                    # Draining for a reload: the refusal arrives where the
                    # final ack is expected. Not an error; the anchor is not
                    # advanced and the next cycle retries.
                    logger.info(
                        f"server {base_url} is reloading; skipping {dsi.name} this cycle"
                    )
                    return False
                logger.error(
                    f"Transfer failed for {dsi.name}: {result.get('error', 'unknown error')}"
                )
                return False
        except Exception as e:
            failed = True
            # A timeout means a silent peer, which cannot have left a refusal
            # behind (a refusal closes the socket): do not wait on it again.
            timed_out = isinstance(e, (TimeoutError, websocket.WebSocketTimeoutException))
            if sending and not timed_out and _peer_sent_reloading_refusal(ws):
                logger.info(
                    f"server {base_url} is reloading; skipping {dsi.name} this cycle"
                )
            else:
                logger.error(f"Transfer failed for {dsi.name} -> {base_url}: {e}")
            return False
        finally:
            if failed:
                # A dead or hung peer: do not let the close handshake
                # wait out the idle timeout (default 3s inside close()
                # only covers the read; the socket timeout covers sends).
                # Its own block: a failing `settimeout` must not skip close().
                with contextlib.suppress(Exception):
                    ws.settimeout(5)
            with contextlib.suppress(Exception):
                ws.close()
            if not entered:
                # connect() failed or timed out before `with stream:`: the
                # `zfs send` is already running and would block forever on a
                # full pipe. Closing the handle EPIPEs it and reaps it.
                try:
                    stream.close()
                except Exception:
                    pass

        old_anchor = self.local_manager.get_anchor(dsi, destination)
        self.local_manager.set_anchor(dsi, destination, latest.name)
        if old_anchor and old_anchor != latest.name:
            logger.debug(f"Previous anchor {old_anchor} superseded by {latest.name}")

        logger.info(
            f"Backup complete: {dsi.name}@{latest.name}"
            + (f" (incremental from {common_snap.name})" if common_snap else " (full)")
            + f" -> {base_url}"
        )
        return True

    def _get_resume_stream(self, base_url, client_id, dsi, latest, common_snap):
        """Try to resume an interrupted transfer; fall back to a fresh send."""
        try:
            resp = requests.get(
                f"{base_url}/backup/{_path(client_id)}/{_path(dsi.name)}/resume_token",
                timeout=10,
            )
            if _is_reloading(resp):
                logger.info(
                    f"server {base_url} is reloading; skipping {dsi.name} this cycle"
                )
                return None
            if resp.ok:
                token = resp.json().get('resume_token')
                if token:
                    logger.info(f"Resuming interrupted transfer for {dsi.name}")
                    return zfs.send.resume(token)
        except Exception:
            pass  # resume check is best-effort

        try:
            if common_snap:
                logger.info(
                    f"Sending incremental {dsi.name}: {common_snap.name} -> {latest.name}"
                )
                return zfs.send.snapshot(latest.snapshot, since=common_snap.snapshot)
            else:
                logger.info(f"Sending full snapshot {dsi.name}@{latest.name}")
                return zfs.send.snapshot(latest.snapshot)
        except Exception as e:
            logger.error(f"Failed to start zfs send for {dsi.name}: {e}")
            return None

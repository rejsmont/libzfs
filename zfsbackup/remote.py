"""Client-side remote backup: identity management and ZFS stream transfer."""

import contextlib
import errno
import json
import locale
import logging
import os
import re
import socket
import stat
import uuid
from pathlib import Path
from typing import Callable, Dict, List, NamedTuple, Optional, Set, Tuple
from urllib.parse import quote

import requests
import websocket

import libzfseasy as zfs
from libzfseasy.zfs import ResumeTokenError
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


def _peer_refusal(ws) -> Optional[dict]:
    """After a failed send: the refusal the server left in the socket before
    closing it, as a dict, or `None`. One bounded read (2 s), every error
    swallowed. Any JSON object that is not an `ok` status and carries an
    `error` or `retry` counts: "reloading" and "busy" (both `retry: true`)
    and real refusals (D7 mismatch, unknown mode, abort failure, ...).

    The API worker sends a refusal as a text message and returns without
    reading, so the client's first `send_binary` may hit the closed socket
    (BrokenPipe / connection closed) and never see the reason."""
    try:
        ws.settimeout(2)
        result = json.loads(ws.recv())
    except Exception:
        return None
    if (
        isinstance(result, dict) and result.get('status') != 'ok'
        and (result.get('error') or result.get('retry'))
    ):
        return result
    return None


def _peer_sent_reloading_refusal(ws) -> bool:
    """After a failed send: did the server leave its retryable refusal
    ("reloading" or "busy") in the socket? See `_peer_refusal`."""
    return _is_reloading_refusal(_peer_refusal(ws))


def _log_refusal(base_url: str, ds_name: str, result: dict) -> None:
    """Log a server refusal that arrived instead of the final ack, or in
    place of a broken pipe: a retryable one (`retry: true`) at INFO with its
    own wording ("busy" vs "reloading"), anything else at ERROR with the
    server's text."""
    if _is_reloading_refusal(result):
        if result.get('error') == 'busy':
            logger.info(
                f"server {base_url} is busy with another stream of {ds_name}; "
                "skipping this cycle"
            )
        else:
            logger.info(f"server {base_url} is reloading; skipping {ds_name} this cycle")
    else:
        logger.error(f"server refused {ds_name}: {result.get('error') or 'unknown error'}")


def _path(component: str) -> str:
    """Percent-encode a client id / dataset name for use in a URL path
    (the API's routes decode it; `/` and `:` stay literal)."""
    return quote(component, safe='/:')


#: A client id is ~40 bytes; anything bigger is not one. Kept in step with
#: `daemon._ID_FILE_MAX`.
_ID_FILE_MAX = 4096


def _read_id_file(path) -> Optional[str]:
    """The text of the client id file, `None` if it does not exist; raises
    `OSError` for anything that is not a plain, small file. Opened
    `O_NONBLOCK` and `fstat`ed before any read, so a FIFO, a device
    (`/dev/zero`) or a directory is refused instead of hanging or filling
    memory, and the read is capped at `_ID_FILE_MAX`. Symlinks are followed
    (as always); a loop or dangling link counts as missing. Decoded with the
    locale encoding, as `Path.read_text()` was. The twin of
    `daemon._read_id_file`."""
    # `stat` (following links) BEFORE `open`: opening some devices has side
    # effects (`/dev/watchdog` arms the watchdog, a rewinding tape rewinds on
    # close). The `fstat` below re-checks what was actually opened, for a path
    # swapped in between.
    try:
        if not stat.S_ISREG(os.stat(path).st_mode):
            raise OSError(errno.EINVAL, f"{path} is not a regular file")
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, 'O_CLOEXEC', 0))
    except (FileNotFoundError, NotADirectoryError):
        return None
    except OSError as e:
        if e.errno == errno.ELOOP:
            return None
        raise
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError(errno.EINVAL, f"{path} is not a regular file")
        data = b''
        while len(data) <= _ID_FILE_MAX:
            chunk = os.read(fd, _ID_FILE_MAX + 1 - len(data))
            if not chunk:
                break
            data += chunk
        if len(data) > _ID_FILE_MAX:
            raise OSError(errno.EFBIG, f"{path} is larger than {_ID_FILE_MAX} bytes")
    finally:
        os.close(fd)
    return data.decode(locale.getpreferredencoding(False))


#: The `*ok` stream-feature flags a resume token carries when it was made by
#: a send like ours. `Send.snapshot()` is called without `-c`/`-L`/`-e`/`-w`,
#: and a plain send records NONE (VM-verified), so a token with any flag was
#: made by something else and is not resumed.
_EXPECTED_RESUME_FLAGS = frozenset()

#: Substrings (lower case) of `resume_info(...).error` that mean the token is
#: DEFINITIVELY dead -- the snapshots it needs are gone or changed -- so a
#: fresh send is right. VM-verified: "used in the initial send no longer
#: exists", "...is no longer the same snapshot used in the initial send",
#: "incremental source (...) no longer exists", and "Invalid cross-device link"
#: (the incremental source is gone). Any other zfs error is NOT known to be
#: final: the cycle is skipped instead (D6), because a fresh send makes the
#: server discard resumable progress.
_STALE_TOKEN_ERRORS = (
    'used in the initial send no longer exists',
    'no longer the same snapshot',
    'invalid cross-device link',
)


def _token_error_is_definitive(error: str) -> bool:
    text = error.lower()
    return any(m in text for m in _STALE_TOKEN_ERRORS) or (
        'incremental source' in text and 'no longer exists' in text
    )


class _SendStream(NamedTuple):
    """What `_get_resume_stream` started: the `zfs send` handle, the `mode`
    the server must be told (`resume` when it took the resume-token path,
    `fresh` otherwise -- see `api.STREAM_MODES`), whether the in-flight pin
    (`inflight.<dest>`) was written before the send began, and which local
    snapshots the stream carries (`sent`, a `SnapshotInfo`; `base`, the
    incremental source or `None` for a full stream). For a resume these come
    from the token's GUIDs, never from its (possibly stale) `toname`."""
    stream: object
    mode: str
    inflight_set: bool = False
    sent: object = None
    base: object = None


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
        text = _read_id_file(self.id_file)
        if text is not None:
            value = text.strip()
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
        # (dataset name, destination) -> the last server refusal logged for
        # it (D7: the server holds this dataset for another source), so a
        # standing refusal is an ERROR once and a DEBUG line afterwards.
        self._refusal_logged: Dict[Tuple[str, str], str] = {}

    def backup_dataset(
        self,
        dsi: DatasetInfo,
        remote_cfg: RemoteDatasetConfig,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> bool:
        """Send a backup of dsi to the configured destination. Returns True on success.

        `should_stop` (the worker passes "an exit reason is pending") is
        checked before any send starts and between a resumed send and its
        follow-up, so a stop or reload never begins a second stream."""
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

        return self._transfer(
            base_url, client_id, dsi, latest, common_snap, remote_cfg.destination,
            snapshots=snapshots, should_stop=should_stop,
        )

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
            if resp.status_code == 409:
                # The server refused to mix this dataset into one it already
                # holds for another source (D7): its message says which.
                try:
                    reason = resp.json().get('error') or resp.text
                except Exception:
                    reason = resp.text
                key = (dsi.name, destination)
                message = f"server refused {dsi.name}: {reason}"
                if self._refusal_logged.get(key) != message:
                    self._refusal_logged[key] = message
                    logger.error(message)
                else:
                    logger.debug(message)
                raise NegotiationFailed(str(reason))
            resp.raise_for_status()
            self._refusal_logged.pop((dsi.name, destination), None)
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
        snapshots: Optional[list] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        followup: bool = True,
    ) -> bool:
        # Nothing to send: the newest managed snapshot is already on the
        # server. Checked first, before the resume-token fetch, any pin, and
        # (so a dry run does not log a send of nothing) the dry-run notice --
        # `zfs send -i X X` is refused by zfs, and the pin would never be
        # cleared. Skipping the token is safe: a server that holds `latest`
        # as a completed snapshot cannot have partial state for anything
        # newer (nothing newer exists locally), and a stale older token is
        # discarded by the next fresh send (`mode=fresh` aborts it).
        if common_snap is not None and common_snap.name == latest.name:
            self._settle_up_to_date(dsi, latest, destination, snapshots)
            return True

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

        if should_stop is not None and should_stop():
            logger.info(f"stop requested; not starting a send of {dsi.name}")
            return False

        if snapshots is None:
            snapshots = self.local_manager.list_snapshots(dsi)
        started = self._get_resume_stream(
            base_url, client_id, dsi, latest, common_snap, destination, snapshots,
            should_stop=should_stop,
        )
        if started is None:
            return False
        stream, mode, inflight_set, sent, base = started
        sent = sent or latest

        ws_base = base_url.replace('https://', 'wss://', 1).replace('http://', 'ws://', 1)
        # `mode` is required by the server: `resume` only when this stream
        # continues the server's resume token, `fresh` for a new stream (the
        # server then discards any partial receive state). `from` is
        # informational.
        ws_url = f"{ws_base}/backup/{_path(client_id)}/{_path(dsi.name)}/stream?mode={mode}"
        if base is not None:
            ws_url += f"&from={quote(base.name, safe='')}"

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
                    # Draining for a reload (or busy with another stream of
                    # this dataset): the refusal arrives where the final ack
                    # is expected. Not an error and never a fresh fallback;
                    # the anchor is not advanced and the next cycle retries.
                    _log_refusal(base_url, dsi.name, result)
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
            refusal = _peer_refusal(ws) if sending and not timed_out else None
            if refusal is not None:
                _log_refusal(base_url, dsi.name, refusal)
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
        keep_anchor = False
        if mode == 'resume' and old_anchor and old_anchor != sent.name:
            # Never move an anchor backwards: a resumed snapshot older than
            # the current anchor leaves it where it is.
            current = next((s for s in snapshots if s.name == old_anchor), None)
            keep_anchor = bool(
                current is not None and current.timestamp is not None
                and sent.timestamp is not None and current.timestamp > sent.timestamp
            )
        if keep_anchor:
            anchored = True
            logger.debug(
                f"Keeping anchor {old_anchor}: newer than the resumed {sent.name}"
            )
        else:
            anchored = self.local_manager.set_anchor(dsi, destination, sent.name)
            if old_anchor and old_anchor != sent.name:
                logger.debug(f"Previous anchor {old_anchor} superseded by {sent.name}")
        # The in-flight pin has done its job once the anchor protects the new
        # base: cleared whenever the anchor write succeeded, even if THIS
        # attempt's pin could not be written (an older stale pin goes too).
        # If the anchor write failed the pin stays (it still protects both
        # ends; the next send overwrites it). When a NEWER anchor was kept
        # (resume of an older snapshot) the pin stays as well: nothing was
        # written, and the follow-up writes its own pin (or the next
        # successful send replaces it).
        if anchored and not keep_anchor:
            self.local_manager.clear_inflight(dsi, destination)

        if mode == 'resume':
            logger.info(
                f"Resumed {dsi.name}@{sent.name} "
                f"(from {base.name if base is not None else 'full'}) to {destination}"
            )
        else:
            logger.info(
                f"Backup complete: {dsi.name}@{sent.name}"
                + (f" (incremental from {base.name})" if base is not None else " (full)")
                + f" -> {base_url}"
            )

        # D4: a resumed send finishes the OLD snapshot; the dataset has moved
        # on. Continue ONCE in this cycle: the resumed snapshot is now common,
        # so the follow-up is a normal incremental to `latest`.
        if mode == 'resume' and followup and sent.name != latest.name:
            if should_stop is not None and should_stop():
                logger.info(
                    f"stop requested; {dsi.name} -> {destination} continues to "
                    f"{latest.name} next cycle"
                )
                return True
            return self._continue_after_resume(
                base_url, client_id, dsi, snapshots, destination, should_stop
            )
        return True

    def _settle_up_to_date(self, dsi, latest, destination, snapshots) -> None:
        """`latest` is already on the server: make the bookkeeping say so.
        The anchor is written if it is missing or points elsewhere (never
        backwards: an anchor on a newer managed snapshot is left), and a stale
        `inflight.<dest>` pin is cleared once the anchor is right. Writes
        nothing in a dry run."""
        logger.debug(
            f"Nothing to send for {dsi.name} -> {destination}: "
            f"{latest.name} is already on the server"
        )
        if self.config.dry_run:
            return
        anchor = self.local_manager.get_anchor(dsi, destination)
        anchored = anchor == latest.name
        if not anchored:
            newer = False
            if anchor and snapshots:
                current = next((s for s in snapshots if s.name == anchor), None)
                newer = bool(
                    current is not None and current.timestamp is not None
                    and latest.timestamp is not None and current.timestamp > latest.timestamp
                )
            anchored = newer or self.local_manager.set_anchor(dsi, destination, latest.name)
        if anchored and self.local_manager.get_inflight(dsi, destination) is not None:
            self.local_manager.clear_inflight(dsi, destination)

    def _continue_after_resume(
        self, base_url, client_id, dsi, snapshots, destination, should_stop
    ) -> bool:
        """The one follow-up send after a resume (D4): re-negotiate (the
        resumed snapshot is now on the server) and send a normal fresh or
        incremental stream to the newest snapshot, with its own pin and
        anchor handling. `followup=False`: never a second continuation."""
        snapshots = self.local_manager.list_snapshots(dsi) or snapshots
        if not snapshots:
            return True
        latest = snapshots[0]
        try:
            common_name = self._negotiate(base_url, client_id, dsi, snapshots, destination)
        except NegotiationFailed as e:
            if e.reloading:
                logger.info(
                    f"server {base_url} is reloading; {dsi.name} continues next cycle"
                )
            return False
        common_snap = None
        if common_name:
            common_snap = next((s for s in snapshots if s.name == common_name), None)
        return self._transfer(
            base_url, client_id, dsi, latest, common_snap, destination,
            snapshots=snapshots, should_stop=should_stop, followup=False,
        )

    def _snapshot_guids(self, dsi: DatasetInfo) -> Dict[int, str]:
        """`{guid: snapshot short name}` for every snapshot of `dsi` (one
        `zfs list -o guid`). Raises on failure."""
        result: Dict[int, str] = {}
        for snap in zfs.list(roots=dsi.dataset, types='snapshot', properties=['guid']):
            raw = snap['guid']
            if raw is not None:
                result[int(str(raw), 0)] = snap.short
        return result

    def _fetch_resume_token(self, base_url, client_id, dsi) -> Tuple[str, Optional[str]]:
        """Ask the server for its resume token -> `('token', token)`,
        `('none', None)`, or `('skip', None)`: no usable answer, so the cycle
        is skipped (D6). Falling through to a fresh send on an unreadable
        answer would make the server discard its partial progress."""
        try:
            resp = requests.get(
                f"{base_url}/backup/{_path(client_id)}/{_path(dsi.name)}/resume_token",
                timeout=10,
            )
            if _is_reloading(resp):
                logger.info(
                    f"server {base_url} is reloading; skipping {dsi.name} this cycle"
                )
                return 'skip', None
            if not resp.ok:
                logger.warning(
                    f"cannot ask {base_url} for the resume token of {dsi.name} "
                    f"(HTTP {resp.status_code}); skipping this cycle rather than "
                    "risk discarding its partial progress with a fresh send"
                )
                return 'skip', None
            body = resp.json()
            if not isinstance(body, dict) or body.get('error'):
                raise ValueError(
                    (body.get('error') if isinstance(body, dict) else None)
                    or "unexpected reply"
                )
            token = body.get('resume_token')
            return ('token', token) if token else ('none', None)
        except Exception as e:
            logger.warning(
                f"cannot ask {base_url} for the resume token of {dsi.name}: {e}; "
                "skipping this cycle rather than risk discarding its partial "
                "progress with a fresh send"
            )
            return 'skip', None

    def _resume_target(self, token: str, dsi: DatasetInfo, snapshots) -> object:
        """Decide whether `token` can be resumed: -> `(to_snap, from_snap)`
        (`SnapshotInfo`s, `from_snap` `None` for a full stream), or a `str`
        giving the reason it must not be. Matching is by GUID, never by the
        token's `toname`, which goes stale when the sender renames. Raises (the
        caller skips the cycle) when the token cannot be decoded for any reason
        other than being corrupt, when zfs reports an error that is not a
        definitive "snapshots gone", or when the local snapshots' GUIDs cannot
        be read."""
        try:
            info = zfs.send.resume_info(token)
        except ResumeTokenError as e:
            return f"the resume token is corrupt or unusable: {e}"
        # Any other failure to decode PROPAGATES: not knowing is not a reason
        # to make the server discard its progress (the caller skips the cycle).
        if info.error:
            if _token_error_is_definitive(info.error):
                return f"zfs says the token cannot be resumed: {info.error}"
            raise RuntimeError(
                f"zfs reports a problem with the token but not a definitive one: {info.error}"
            )
        if info.flags != _EXPECTED_RESUME_FLAGS:
            return (
                "the token records stream features this daemon does not send "
                f"({', '.join(sorted(info.flags - _EXPECTED_RESUME_FLAGS)) or 'none'} "
                f"expected {', '.join(sorted(_EXPECTED_RESUME_FLAGS)) or 'none'})"
            )
        if info.toguid is None:
            return "the token does not name its target snapshot"
        guids = self._snapshot_guids(dsi)
        managed = {s.name: s for s in snapshots}
        to_snap = managed.get(guids.get(info.toguid, ''))
        if to_snap is None:
            return (
                f"its target snapshot (guid {info.toguid}, {info.toname}) is not a "
                "managed local snapshot of this dataset"
            )
        from_snap = None
        if info.fromguid is not None:
            from_snap = managed.get(guids.get(info.fromguid, ''))
            if from_snap is None:
                return (
                    f"its incremental source (guid {info.fromguid}) is not a "
                    "managed local snapshot of this dataset"
                )
            if (
                from_snap.timestamp is None or to_snap.timestamp is None
                or not from_snap.timestamp < to_snap.timestamp
            ):
                return "its incremental source is not older than its target"
        return to_snap, from_snap

    def _get_resume_stream(
        self, base_url, client_id, dsi, latest, common_snap, destination, snapshots=None,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> Optional[_SendStream]:
        """Start the stream for this cycle -> a `_SendStream`, or `None` when
        nothing was started (the caller skips the dataset this cycle).

        The server's resume token is read first (`_fetch_resume_token`; no
        usable answer skips the cycle). A token is resumed only if
        `_resume_target` accepts it -- no zfs error, no stream features we do
        not send, its target (and incremental source) are managed local
        snapshots by GUID -- with `mode=resume`. Anything else (stale,
        corrupt, foreign, or `zfs send -t` failing to start) is logged at
        WARNING and falls through to a FRESH send (`mode=fresh`; the server
        discards the stale state). Each send first pins both of its snapshots
        (`inflight.<dest>`, `PROP_INFLIGHT_PREFIX`), written BEFORE `zfs send`
        starts; a failed write is logged and the send proceeds unpinned. A
        failed send leaves the pin in place."""
        if snapshots is None:
            snapshots = self.local_manager.list_snapshots(dsi)
        status, token = self._fetch_resume_token(base_url, client_id, dsi)
        if status == 'skip':
            return None
        if token:
            try:
                target = self._resume_target(token, dsi, snapshots)
            except Exception as e:
                logger.warning(
                    f"cannot check the resume token of {dsi.name}: {e}; skipping this "
                    "cycle rather than risk discarding the server's partial progress "
                    "with a fresh send"
                )
                return None
            if isinstance(target, str):
                logger.warning(
                    f"Not resuming {dsi.name}: {target}; sending fresh (the server "
                    "discards its partial state)"
                )
            else:
                to_snap, from_snap = target
                if should_stop is not None and should_stop():
                    logger.info(f"stop requested; not resuming {dsi.name}")
                    return None
                pinned = self.local_manager.set_inflight(
                    dsi, destination, from_snap.name if from_snap else None, to_snap.name
                )
                try:
                    logger.info(
                        f"Resuming interrupted transfer for {dsi.name}@{to_snap.name}"
                    )
                    return _SendStream(
                        zfs.send.resume(token), 'resume', pinned, to_snap, from_snap
                    )
                except Exception as e:
                    logger.warning(
                        f"zfs send -t failed to start for {dsi.name}: {e}; sending "
                        "fresh (the server discards its partial state)"
                    )

        # Last look before `zfs send` is spawned (never during the stream).
        if should_stop is not None and should_stop():
            logger.info(f"stop requested; not starting a send of {dsi.name}")
            return None
        inflight_set = self.local_manager.set_inflight(
            dsi, destination, common_snap.name if common_snap else None, latest.name
        )
        try:
            if common_snap:
                logger.info(
                    f"Sending incremental {dsi.name}: {common_snap.name} -> {latest.name}"
                )
                return _SendStream(
                    zfs.send.snapshot(latest.snapshot, since=common_snap.snapshot),
                    'fresh', inflight_set, latest, common_snap,
                )
            else:
                logger.info(f"Sending full snapshot {dsi.name}@{latest.name}")
                return _SendStream(
                    zfs.send.snapshot(latest.snapshot), 'fresh', inflight_set, latest, None
                )
        except Exception as e:
            logger.error(f"Failed to start zfs send for {dsi.name}: {e}")
            return None

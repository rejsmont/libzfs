"""Client-side remote backup: pinned mutual-TLS transport and ZFS stream transfer.

Every connection to a destination is TLS 1.3 with this host's client
certificate, and the server is authenticated by the SHA-256 fingerprint
pinned at enrollment (`Destination.server_fingerprint`, plan A1): HTTP through
`identity.pinned_session`, the websocket through `identity.connect_pinned`
handed to `websocket.WebSocket().connect(..., socket=)`. There is no plaintext
path. The client id is the one the supervisor read from the config database
(`daemon.Handoff.client_id`); the server binds it to our certificate and
`/whoami` reports the binding.

Every request (HTTP and the websocket upgrade) carries the wire protocol
version (`zfsbackup.protocol`, plan S-5). A server that answers 426, or whose
`/whoami` names no supported version, is refused like an auth problem: one
ERROR, then the destination is skipped before any send.
"""

import contextlib
import json
import logging
import os
import re
import ssl
import time
from pathlib import Path
from typing import Callable, Dict, Iterator, NamedTuple, Optional, Set, Tuple
from urllib.parse import quote, urlsplit

import requests
import websocket

import libzfseasy as zfs
from libzfseasy.zfs import ResumeTokenError
from zfsbackup import identity as ident
from zfsbackup.backup_manager import (
    DatasetInfo, DatasetManager, LocalAnchorUnreadable, parse_snapshot_timestamp,
    snapshot_newer,
)
from zfsbackup.config import (
    BackupConfig, Destination, RemoteDatasetConfig, _collapse_retention_rules,
    destination_url_problem,
)
from zfsbackup.identity import (
    CLIENT_NAME, Identity, IdentityError, PinMismatch, client_context, connect_pinned,
    is_pin_mismatch, is_tls_alert_rejection, load_identity, pinned_session,
)
from zfsbackup.protocol import (
    PROTOCOL_VERSION, SUPPORTED_PROTOCOLS, protocol_headers,
)


logger = logging.getLogger(__name__)

#: Websocket timeouts (seconds). Defined here and imported by `zfsbackup.api`
#: (never the other way round). Live config reload defers a worker switch for as
#: long as a transfer is in flight, so a peer that hangs forever would turn
#: "deferred" into "never"; these bound every blocking socket operation.
#:
#: TCP connect plus the websocket handshake.
WS_CONNECT_TIMEOUT = 30

#: Waits (seconds) before the retries of a stream's CONNECT right after a
#: successful step of the same cycle: a server that drained and exited for a
#: reload refuses or resets connections for a few seconds. Three retries, about
#: 7 s in all. Never used for the first stream of a cycle, and only before a
#: single stream byte is sent.
CONNECT_RETRY_DELAYS = (1.0, 2.0, 4.0)

#: A retry wait is taken in slices of this many seconds, so a stop is noticed.
CONNECT_RETRY_SLICE = 0.25
#: No progress on one socket operation (a `send_binary`, or the server's
#: `receive` waiting for the next chunk).
WS_IDLE_TIMEOUT = 600
#: The client waiting for the server's status after `done`, while the
#: server's `zfs receive` may still be committing a large stream.
WS_FINAL_ACK_TIMEOUT = 3600

#: `/whoami`, `/negotiate` (seconds; connect + TLS handshake + answer).
HTTP_TIMEOUT = 30
#: `/resume_token`.
RESUME_TOKEN_TIMEOUT = 10

#: How long the worker's main loop waits for the shared client identity lock
#: (`identity.lock`; the CLI's `enroll` / `identity rotate` hold it
#: exclusively) when it loads or uses the identity. Short: a stopped or slow
#: CLI must not stall the loop for the library's 30 s default (twice over);
#: the failure is an `IdentityError`, handled as `_identity_failed` and
#: retried at the next cycle.
IDENTITY_LOCK_TIMEOUT = 5.0

#: Logged once (ERROR) while the supervisor handed over no client id.
NO_CLIENT_ID_MESSAGE = "no client id; run `zfsbackup-config enroll`"

#: Appended to the "re-enroll" advice for a client the server does not trust:
#: a revoked certificate stays revoked, so a plain re-enroll with the same
#: key would be refused there.
_ENROLL_HINT = (
    " (`--new-key` if this certificate was revoked there; plain `enroll` if "
    "this host just changed key)"
)


class NegotiationFailed(Exception):
    """`_negotiate` could not get an answer from the server. Distinct from a
    successful negotiation that found no common snapshot (which returns
    `None` and rightly leads to an initial full send): a failure must never
    fall through to a send. `reloading` is True for the server's retryable
    503 ("server is reloading; retry later")."""

    def __init__(self, message: str, reloading: bool = False):
        super().__init__(message)
        self.reloading = reloading


#: The server's `api.BUSY_ERROR['error']` (the API imports this module, so the
#: text is repeated here; a test compares the two).
_BUSY_TEXT = 'busy'


def _retry_word(e: 'NegotiationFailed') -> str:
    """How to say a retryable negotiate refusal in a log line: `reloading`,
    `busy`, or `busy: <the server's reason>`."""
    text = str(e)
    if 'reloading' in text:
        return "reloading"
    reason = text[len("server is busy"):].lstrip(': ')
    return f"busy: {reason}" if reason else "busy"


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
        error = str(result.get('error') or '')
        if error == _BUSY_TEXT:
            logger.info(
                f"server {base_url} is busy with another stream of {ds_name}; "
                "skipping this cycle"
            )
        elif 'reloading' in error:
            logger.info(f"server {base_url} is reloading; skipping {ds_name} this cycle")
        else:
            logger.info(f"server {base_url} asks to retry {ds_name} ({error}); skipping this cycle")
    else:
        logger.error(f"server refused {ds_name}: {result.get('error') or 'unknown error'}")


#: The `reason` of the server's refusal to receive a stream because it would
#: destroy, or cannot replace, snapshots it holds (`api.decide_receive`), or
#: because it cannot list them reliably (`listing`): logged once per episode
#: (`RemoteBackupManager._note_refusal`).
_STREAM_REFUSAL_REASONS = ('server_newer', 'base_missing', 'no_common', 'listing')


def _protocol_mismatch(where: str, server_protocol, *, refused: bool) -> str:
    """The (deduplicated) ERROR for a server that speaks another wire
    protocol: it refused ours with 426 (`refused`), or its `/whoami` named an
    unsupported version or none."""
    ours = PROTOCOL_VERSION
    if type(server_protocol) is int:
        theirs = f"protocol {server_protocol}"
    else:
        theirs = "no protocol version (an older zfsbackup release)"
    if refused:
        what = f"refuses this client's zfsbackup protocol {ours} (HTTP 426); it speaks {theirs}"
    else:
        what = f"speaks {theirs}; this client speaks zfsbackup protocol {ours}"
    return (
        f"{where} {what}. Upgrade the client and the server to the same zfsbackup "
        "release; the destination is skipped until then"
    )


def _whoami_protocol_problem(where: str, body: dict) -> Optional[str]:
    """`None` if `/whoami`'s `protocol` is one this client supports, else the
    refusal message."""
    version = body.get('protocol')
    if type(version) is int and version in SUPPORTED_PROTOCOLS:
        return None
    return _protocol_mismatch(where, version, refused=False)


def _path(component: str) -> str:
    """Percent-encode a client id / dataset name for use in a URL path
    (the API's routes decode it; `/` and `:` stay literal)."""
    return quote(component, safe='/:')


class _Endpoint(NamedTuple):
    """Where a destination URL points: `base_url` (`https://host[:port][/path]`,
    scheme lower-cased, default port and any user info, query or fragment
    dropped), the matching `ws_base` (`wss://...`) and the `host`/`port` the
    websocket's TLS socket connects to (`host` without IPv6 brackets)."""
    base_url: str
    ws_base: str
    host: str
    port: int


def _endpoint(url: str) -> _Endpoint:
    """Parse an https destination URL (already accepted by
    `destination_url_problem`). Raises `ValueError` for anything that rule
    refuses -- including port 0 and an empty port, which are never taken
    for 443. The scheme is normalised: the model keeps `HTTPS://` as
    written."""
    problem = destination_url_problem(url)
    if problem is not None:
        raise ValueError(problem)
    parts = urlsplit(url.strip())
    host = parts.hostname
    port = parts.port if parts.port is not None else 443
    shown = f"[{host}]" if ':' in host else host
    authority = shown if port == 443 else f"{shown}:{port}"
    path = parts.path.rstrip('/')
    return _Endpoint(f"https://{authority}{path}", f"wss://{authority}{path}", host, port)


class _Transport(NamedTuple):
    """One destination's pinned connection state, built from one client
    identity and one server pin and rebuilt when either changes: the
    `requests` session (HTTP) and the SSL context the websocket's
    `connect_pinned` uses. `key` identifies the pair."""
    endpoint: _Endpoint
    server_fp: str
    identity_fp: str
    session: requests.Session
    ctx: ssl.SSLContext

    @property
    def base_url(self) -> str:
        return self.endpoint.base_url

    @property
    def key(self) -> Tuple[str, str]:
        return (self.identity_fp, self.server_fp)


def _exc_chain(exc: BaseException) -> Iterator[BaseException]:
    """`exc` and everything it wraps (causes, contexts, `.reason`, args)."""
    seen = set()
    stack = [exc]
    while stack:
        e = stack.pop()
        if e is None or id(e) in seen:
            continue
        seen.add(id(e))
        yield e
        stack.append(e.__cause__)
        stack.append(e.__context__)
        reason = getattr(e, 'reason', None)
        if isinstance(reason, BaseException):
            stack.append(reason)
        stack.extend(a for a in getattr(e, 'args', ()) if isinstance(a, BaseException))


def _is_reset(exc: BaseException) -> bool:
    """A connection reset or broken pipe (`RemoteDisconnected` is a
    `ConnectionResetError`) anywhere in `exc`. Under TLS 1.3 a server that
    rejects our certificate sometimes shows up this way instead of as an
    alert (plan, "M11 rejection detection")."""
    return any(isinstance(e, (ConnectionResetError, BrokenPipeError)) for e in _exc_chain(exc))


_URLLIB3_GOT_RE = re.compile(r'got "([0-9a-fA-F:]+)"')


def _presented_fingerprint(exc: BaseException) -> Optional[str]:
    """The fingerprint the server presented, from a `PinMismatch` or the
    text of urllib3's fingerprint assertion; `None` if unknown."""
    for e in _exc_chain(exc):
        if isinstance(e, PinMismatch):
            return e.actual
    for e in _exc_chain(exc):
        m = _URLLIB3_GOT_RE.search(str(e))
        if m:
            return m.group(1).replace(':', '').lower()
    return None


_ALERT_TEXT_RE = re.compile(r'\b(?:TLS|TLSV1|SSLV3)_ALERT_[A-Z_]+')


def _alert_reason(exc: BaseException) -> str:
    """The TLS alert name in `exc` (`TLSV1_ALERT_UNKNOWN_CA`, ...), for the
    log; the whole exception text if none is found."""
    for e in _exc_chain(exc):
        m = _ALERT_TEXT_RE.search(str(getattr(e, 'reason', '') or '')) or _ALERT_TEXT_RE.search(str(e))
        if m:
            return m.group(0)
    return str(exc)


def _json_body(resp) -> dict:
    """`resp.json()` if it is an object, else `{}` (never raises)."""
    try:
        body = resp.json()
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}


class _SendMode(NamedTuple):
    """One send mode. `options` are the `zfs.send.snapshot` keyword flags of
    a fresh send. `required` and `allowed` bound the `*ok` flags a resume
    token may carry for this mode to be resumed: every required flag present
    and nothing outside `allowed` (a token made by a different mode, or by
    something else, is not resumed; `embedok` and `savedok` are never
    allowed). The recorded set depends on dataset state, not only on the flags
    sent (`largeblockok` appears only when the dataset holds blocks over
    128K), and a plain send of an unencrypted dataset records no `rawok`;
    OpenZFS 2.1.5, 2.2.2 and 2.4.1 agree (captured). The empty set (tokens of
    the flagless sends of earlier versions) stays acceptable in plain mode."""
    options: dict
    required: frozenset
    allowed: frozenset


#: The frozen send-flag sets (plan B1, irreversible per server dataset):
#: plain `-L -c`; raw `-w` (libzfs adds `-L -e -c` itself). A resumed send is
#: `zfs send -t <token>` with no flags: the token carries its own.
_SEND_MODES = {
    'plain': _SendMode(
        {'large_blocks': True, 'compressed': True},
        frozenset(),
        frozenset({'compressok', 'largeblockok'}),
    ),
    'raw': _SendMode(
        {'raw': True},
        frozenset({'rawok'}),
        frozenset({'rawok', 'compressok', 'largeblockok'}),
    ),
}

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


#: Outcomes of `RemoteBackupManager._push_step`.
_OK = 'ok'
_FAILED = 'failed'
_STOP = 'stop'
_ANCHOR_FAILED = 'anchor-failed'


class _SendStream(NamedTuple):
    """What `_try_resume` or `_start_fresh` started: the `zfs send` handle, the `mode`
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


class RemoteBackupManager:
    """Negotiates and transfers ZFS snapshots to configured remote destinations.

    `client_id` is this host's id from the supervisor's config snapshot
    (`daemon.Handoff.client_id`; `None` until `zfsbackup-config enroll`
    generates one). `tls_dir` is `<db>.tls`, where the default client
    identity `client.{crt,key}` lives unless `client_cert_file` /
    `client_key_file` name other files.

    A worker calls `begin_cycle()` once per pass over its datasets: each
    destination's `/whoami` check then runs once per cycle (not per
    dataset), and an identity that failed to load is retried. A manager
    used without `begin_cycle()` checks on every `backup_dataset` call."""

    def __init__(
        self,
        config: BackupConfig,
        local_manager: DatasetManager,
        *,
        client_id: Optional[str],
        tls_dir=None,
    ):
        self.config = config
        self.local_manager = local_manager
        self.client_id = client_id
        self.tls_dir = Path(tls_dir) if tls_dir is not None else None
        # (dataset name, destination) pairs whose effective per-destination
        # retention-rule collisions have already been warned about, for this
        # manager's lifetime. `to_property(destination)` ships the resolved
        # ruleset unresolved (correct -- resolution is a read-path concern),
        # and the ONLY collision warning used to appear in the *receiving*
        # admin's journal, naming a server-side path the client operator does
        # not administer. The client must warn about its own collisions too.
        self._retention_warned: Set[Tuple[str, str]] = set()
        # Key -> the last refusal logged for it, so a standing refusal is an
        # ERROR once and a DEBUG line afterwards. `(dataset, destination)`:
        # a dataset-level refusal (D7: the server holds this dataset for
        # another source). `(None, destination)`: a destination-level one
        # (not enrolled, client not trusted, client id mismatch, pin
        # mismatch, reset after the handshake), cleared by a good `/whoami`.
        self._refusal_logged: Dict[Tuple[Optional[str], str], str] = {}
        # `(dataset, destination) -> (server_copy, server_dataset)` from the
        # last successful negotiate (`server_copy` is `None` for a server that
        # does not report it).
        self._server_info: Dict[Tuple[str, str], Tuple[Optional[str], Optional[str]]] = {}
        # `(dataset name, destination) -> (server_newer_count, server_newer)`
        # from the last successful negotiate: the server snapshots newer than
        # the common one, `[{"name", "guid"}, ...]` oldest first. Empty for a
        # server that does not report them (an older release).
        self._server_newer: Dict[Tuple[str, str], Tuple[int, list]] = {}
        # `(dataset name, destination) -> 'known' | 'unknown' | 'absent'`:
        # whether the last negotiate reported `server_newer` (`known`, also
        # when the list is empty), reported `server_newer_unknown` (a common
        # snapshot whose newer snapshots the server could not tell) or
        # reported neither (`absent`: a server that predates B-2b).
        self._server_newer_status: Dict[Tuple[str, str], str] = {}
        self._no_client_id_logged = False
        # The loaded client identity and the stat signature of its files.
        self._identity: Optional[Identity] = None
        self._identity_sig: Optional[tuple] = None
        # The signature at the last failed load (no retry until the files
        # change or a new cycle starts) and the error logged for it.
        self._identity_failed_sig: Optional[tuple] = None
        self._identity_error: Optional[str] = None
        # destination name -> its pinned transport.
        self._transports: Dict[str, _Transport] = {}
        # destination name -> (transport key, usable) for this cycle.
        self._checked: Dict[str, Tuple[Tuple[str, str], bool]] = {}
        self._per_cycle = False

    def begin_cycle(self) -> None:
        """Start a new pass over the datasets: forget this cycle's
        `/whoami` results, and retry a client identity that failed to load."""
        self._per_cycle = True
        self._checked.clear()
        self._identity_failed_sig = None

    def close(self) -> None:
        """Close every pinned session (idempotent)."""
        for transport in self._transports.values():
            with contextlib.suppress(Exception):
                transport.session.close()
        self._transports.clear()
        self._checked.clear()

    def backup_dataset(
        self,
        dsi: DatasetInfo,
        remote_cfg: RemoteDatasetConfig,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> bool:
        """Send a backup of dsi to the configured destination. Returns True on success.

        Before anything is negotiated or sent: the client id, the
        destination's enrollment (https URL and pinned server fingerprint),
        the client TLS identity and the server's `/whoami` binding must all
        be in order; otherwise the destination is skipped (logged once).

        `should_stop` (the worker passes "an exit reason is pending") is
        checked before every stream of the chain (between steps, never during
        one), so a stop or reload never begins another stream."""
        destination = remote_cfg.destination
        dest = self.config.destinations.get(destination)
        if dest is None:
            logger.error(f"Unknown destination '{destination}' for {dsi.name}")
            return False

        self._warn_retention_collisions(dsi, destination)

        if not self._per_cycle:
            self._checked.clear()
            self._identity_failed_sig = None
        transport = self._ready_transport(destination, dest)
        if transport is None:
            return False
        client_id = self.client_id

        snapshots = self.local_manager.list_snapshots(dsi)
        if not snapshots:
            logger.debug(f"No snapshots available to send for {dsi.name}")
            return True

        latest = snapshots[0]
        # The send mode is decided once, from the newest snapshot, and holds
        # for the whole chain.
        mode = self._send_mode(dsi, remote_cfg, latest)
        if mode is None:
            return False
        try:
            common_snap_name = self._negotiate(
                transport, client_id, dsi, snapshots, destination
            )
        except NegotiationFailed as e:
            # Never fall through to a send: with no answer we would treat
            # "unknown" as "no common snapshot" and start a full send.
            if e.reloading:
                logger.info(
                    f"server {transport.base_url} is {_retry_word(e)}; skipping {dsi.name} this cycle"
                )
            return False
        if not self._server_copy_matches(dsi, destination, mode):
            return False

        # Resolve the common snapshot object for incremental send. A named
        # common snapshot that is not among ours is never a reason for a full
        # send: skip the cycle.
        common_snap = None
        if common_snap_name:
            common_snap = next((s for s in snapshots if s.name == common_snap_name), None)
            if common_snap is None:
                logger.error(
                    f"server {transport.base_url} reports {common_snap_name!r} as the common "
                    f"snapshot of {dsi.name}, which is not a managed local snapshot; "
                    "skipping this cycle rather than sending a full stream"
                )
                return False

        # Before any local anchor or pin is written, or anything is sent
        # (negotiate has already written on the server by now) -- in
        # particular before `_settle_up_to_date`, which would otherwise record
        # `latest` as the anchor of a rolled-back dataset and make it look
        # like a backstop.
        discard = self._rollback_permit(dsi, destination, common_snap, snapshots)
        if discard is None:
            return False

        return self._transfer(
            transport, client_id, dsi, latest, common_snap, destination,
            snapshots=snapshots, should_stop=should_stop, mode=mode, discard=discard,
        )

    def _send_mode(self, dsi: DatasetInfo, remote_cfg: RemoteDatasetConfig, newest) -> Optional[str]:
        """`'raw'` or `'plain'` for this dataset and destination, or `None`
        (ERROR once, nothing is sent). Decided from the newest snapshot's
        listed `encryption`/`keystatus` (no extra zfs call) and the
        destination's `raw` setting: `None` (auto) is raw if and only if the
        dataset is encrypted; `True` is raw and refuses an unencrypted dataset
        (`-w` on one records no `rawok`, a third kind of stream); `False` is
        plain, which needs the key loaded (`zfs send -nv` succeeds without it,
        so it is no preflight)."""
        key = (f"{dsi.name} [send mode]", remote_cfg.destination)
        encrypted = bool(newest.encrypted)
        setting = remote_cfg.raw
        problem = None
        if setting is True and not encrypted:
            problem = (
                f"{dsi.name} -> {remote_cfg.destination}: raw is true but the dataset is not "
                "encrypted; nothing is sent. Set raw to auto (or false) for this destination"
            )
        mode = 'raw' if (setting is True or (setting is None and encrypted)) else 'plain'
        if problem is None and mode == 'plain' and encrypted and newest.key_loaded is False:
            problem = (
                f"{dsi.name} -> {remote_cfg.destination}: raw is false but the dataset's key is "
                "not loaded, so it cannot be sent in plain form; nothing is sent. Load the key "
                "(zfs load-key), or set raw to auto to send it encrypted"
            )
        if problem is not None:
            self._log_refusal_once(key, problem)
            return None
        self._refusal_logged.pop(key, None)
        return mode

    def _server_copy_matches(self, dsi: DatasetInfo, destination: str, mode: str) -> bool:
        """False (ERROR once, nothing is sent) when the server holds a copy of
        the other kind. A server copy cannot be switched between plain and
        raw by an incremental stream; the client never destroys or renames
        server data, so the operator moves the old copy aside and the next
        cycle sends a full stream. `none`, or no report (an older server),
        never blocks."""
        key = (f"{dsi.name} [server copy]", destination)
        copy, server_ds = self._server_info.get((dsi.name, destination), (None, None))
        if copy not in ('plain', 'raw') or copy == mode:
            self._refusal_logged.pop(key, None)
            return True
        name = server_ds or dsi.name
        self._log_refusal_once(key, (
            f"{dsi.name} -> {destination}: the server copy {name} is a {copy} copy but this "
            f"dataset is now sent {mode}; nothing is sent. On the server, move the old copy "
            f"aside with `zfs rename {name} <a name outside the backup target>` (see README "
            f"'Renaming a server copy aside'); the next cycle sends a full {mode} stream"
        ))
        return False

    def _discard_names(self, dsi: DatasetInfo, destination: str, discard: Tuple[str, ...]) -> str:
        """The names (else the guids) of the server snapshots `discard` lets
        the server destroy, for log lines."""
        entries = self._server_newer.get((dsi.name, destination), (0, []))[1]
        by_guid = {e.get('guid'): e.get('name') for e in entries if isinstance(e, dict)}
        return ', '.join(str(by_guid.get(g) or f"guid {g}") for g in discard)

    def _rollback_permit(
        self, dsi: DatasetInfo, destination: str, common_snap, snapshots: list
    ) -> Optional[Tuple[str, ...]]:
        """Whether this cycle may send, and which server snapshots it lets the
        server destroy: `()` (send, destroy nothing), a tuple of snapshot guids
        (the `discard` of every stream of the cycle) or `None` (refused: ERROR
        once per episode, nothing is sent and nothing is written).

        The server receives into an existing dataset with `-F` only when that
        destroys nothing or only the snapshots named in `discard`
        (`api.decide_receive`), because `-F` destroys every server snapshot
        newer than the incremental base. Naming one is only right for a
        snapshot of ours that was sent and then destroyed here while it was
        being sent (the backstop of `_push_step` moved the anchor back to the
        base): the server has it, we do not, and that is the one snapshot we
        may let it discard. A host that rolled back or restored an older
        snapshot looks the same from the negotiate answer, so the evidence is
        local and all of it must hold:

        1. the dataset's OWN (local, never inherited) anchor for this
           destination names a snapshot that is in our managed listing and is
           not older (`createtxg`) than the common one. After the backstop the
           anchor is the base, and the server's pruner may since have removed
           the base from the server, making an older snapshot the common one;
           the anchor then still names a live local snapshot at or past the
           common one, so equality with the common snapshot would refuse that
           recovery for ever. A rollback or an in-place restore leaves the
           anchor naming a snapshot that no longer exists here, and a restore
           under a new name has no local anchor, so both are refused;
        2. the server names exactly one newer snapshot (and counts one);
        3. that snapshot's name is a managed one that is not a local snapshot
           name, and its guid equals no local snapshot's guid (a name can be
           reused for a different snapshot; a guid cannot);
        4. its guid is a readable decimal number.

        No newer snapshots, or a server that does not report them, needs no
        permit.

        With no common snapshot but a server copy that holds snapshots, a full
        stream cannot replace it (zfs refuses): refused here, once, rather
        than started every cycle."""
        key = (f"{dsi.name} [server newer]", destination)
        none_key = (f"{dsi.name} [no common]", destination)
        unknown_key = (f"{dsi.name} [server newer unknown]", destination)
        copy, server_ds = self._server_info.get((dsi.name, destination), (None, None))
        server_ds = server_ds or dsi.name
        if common_snap is None:
            self._refusal_logged.pop(key, None)
            self._refusal_logged.pop(unknown_key, None)
            if copy not in ('plain', 'raw'):
                self._refusal_logged.pop(none_key, None)
                return ()
            self._log_refusal_once(none_key, (
                f"{dsi.name} -> {destination}: the server copy {server_ds} holds snapshots but "
                "none of them is a snapshot this host still has, and a full stream cannot "
                "replace it, so nothing is sent. To keep the server copy, on the server: "
                f"zfs rename {server_ds} <a name outside the backup target> (the next cycle "
                "sends a full stream; see README 'Renaming a server copy aside'). "
                "See README 'Rejoining'"
            ))
            return None
        self._refusal_logged.pop(none_key, None)
        if self._server_newer_status.get((dsi.name, destination)) == 'unknown':
            # The server has a common snapshot but could not tell what is newer
            # than it. That is not "nothing newer": writing an anchor now
            # could launder a rollback into the backstop later. Nothing is
            # written or sent (the stream would be refused with `listing`).
            self._refusal_logged.pop(key, None)
            self._log_warning_once(unknown_key, (
                f"{dsi.name} -> {destination}: the server copy {server_ds} could not list "
                f"which of its snapshots are newer than {common_snap.name}, so nothing is sent "
                "and no anchor is written this cycle; the next cycle retries"
            ))
            return None
        self._refusal_logged.pop(unknown_key, None)
        count, entries = self._server_newer.get((dsi.name, destination), (0, []))
        if not entries:
            self._refusal_logged.pop(key, None)
            return ()
        guid = None
        unreadable_key = (f"{dsi.name} [local anchor]", destination)
        if len(entries) == 1 and count == 1 and isinstance(entries[0], dict):
            name, text = entries[0].get('name'), entries[0].get('guid')
            try:
                if (
                    isinstance(name, str) and isinstance(text, str)
                    and re.fullmatch(r'[0-9]{1,20}', text) is not None
                    and parse_snapshot_timestamp(self.config.snapshot_prefix, name) is not None
                    and name not in {s.name for s in snapshots}
                    and int(text) not in {s.guid for s in snapshots if s.guid is not None}
                    and self._anchor_at_or_past(dsi, destination, common_snap, snapshots)
                ):
                    guid = text
            except LocalAnchorUnreadable as e:
                # Not evidence of a rollback: the read failed. Say so instead
                # of the rollback ERROR, and send nothing this cycle.
                self._log_warning_once(unreadable_key, (
                    f"{dsi.name} -> {destination}: cannot read the local anchor property "
                    f"org.zfsbackup:anchor.{destination} ({e}), which decides whether the "
                    f"server may discard its newer snapshot; nothing is sent this cycle"
                ))
                return None
        self._refusal_logged.pop(unreadable_key, None)
        if guid is not None:
            self._refusal_logged.pop(key, None)
            return (guid,)
        names = [str(e.get('name')) for e in entries if isinstance(e, dict)] or ['?']
        first, last = names[0], names[-1]
        truncated = count > len(entries)
        if len(names) == 1 and not truncated:
            shown, destroy = first, first
        elif truncated:
            # `first%` (a blank last) ranges to the newest (zfs-destroy(8)).
            shown = (
                f"{first}\u2026{last}; the list is truncated, the oldest {len(names)} of "
                f"{count} are named"
            )
            destroy = f"{first}%"
        else:
            shown, destroy = f"{first}\u2026{last}", f"{first}%{last}"
        self._log_refusal_once(key, (
            f"{dsi.name} -> {destination}: the server copy {server_ds} has {count} "
            f"snapshot(s) newer than {common_snap.name} that this host does not have "
            f"({shown}), for example after a rollback or an in-place restore of this dataset, "
            "or snapshots taken on the server. Sending would make the server destroy them, "
            "so nothing is sent. To keep them, on the server: "
            f"zfs rename {server_ds} <a name outside the backup target> (the next cycle "
            "sends a full stream; see README 'Renaming a server copy aside'). If they are "
            f"expendable: zfs destroy {server_ds}@{destroy} "
            "(the next cycle continues). See README 'Rejoining'"
        ))
        return None

    def _anchor_at_or_past(self, dsi: DatasetInfo, destination: str, common_snap, snapshots: list) -> bool:
        """Rule 1 of `_rollback_permit`: this dataset's local anchor for the
        destination names a snapshot in `snapshots` whose `createtxg` is at
        least the common snapshot's (both known)."""
        name = self.local_manager.get_local_anchor(dsi, destination)
        if not name:
            return False
        anchor = next((s for s in snapshots if s.name == name), None)
        return (
            anchor is not None
            and anchor.createtxg is not None and common_snap.createtxg is not None
            and anchor.createtxg >= common_snap.createtxg
        )

    # ------------------------------------------------- identity and transport

    def _ready_transport(self, destination: str, dest: Destination) -> Optional[_Transport]:
        """The destination's pinned transport if this client may use it now:
        a client id, an enrolled destination, a loadable client identity and
        a `/whoami` (once per cycle) that binds our certificate to our id.
        `None` (logged) means skip the destination -- before any send."""
        if self.client_id is None:
            if not self._no_client_id_logged:
                self._no_client_id_logged = True
                logger.error(f"remote backup skipped: {NO_CLIENT_ID_MESSAGE}")
            else:
                logger.debug(f"remote backup skipped: {NO_CLIENT_ID_MESSAGE}")
            return None
        endpoint = self._enrolled_endpoint(destination, dest)
        if endpoint is None:
            return None
        identity = self._client_identity()
        if identity is None:
            return None
        transport = self._transport(destination, endpoint, dest.server_fingerprint, identity)
        if transport is None:
            return None
        checked = self._checked.get(destination)
        if checked is not None and checked[0] == transport.key:
            return transport if checked[1] else None
        ok = self._whoami(destination, transport)
        self._checked[destination] = (transport.key, ok)
        return transport if ok else None

    def _enrolled_endpoint(self, destination: str, dest: Destination) -> Optional[_Endpoint]:
        """The destination's endpoint, or `None` (one ERROR) when it is not
        enrolled: not an https URL, or no pinned server fingerprint."""
        problem = destination_url_problem(dest.url)
        endpoint = None
        if problem is None:
            try:
                endpoint = _endpoint(dest.url)
            except ValueError as e:
                problem = str(e)
        if problem is None and not dest.server_fingerprint:
            problem = "no server fingerprint is pinned"
        if problem is None:
            return endpoint
        url_hint = dest.url if endpoint is not None else 'https://…'
        self._log_refusal_once(
            (None, destination),
            f"destination {destination} is not enrolled: run `zfsbackup-config enroll "
            f"{destination} --url {url_hint} --token-file …` ({problem})",
        )
        return None

    def _identity_paths(self) -> Tuple[Path, Path]:
        cert, key = self.config.client_cert_file, self.config.client_key_file
        if cert is not None and key is not None:
            return Path(cert), Path(key)
        if self.tls_dir is None:
            raise IdentityError(
                "no client certificate is configured and the TLS directory is unknown"
            )
        return ident.identity_paths(self.tls_dir, CLIENT_NAME)

    @staticmethod
    def _file_sig(path: Path) -> Optional[tuple]:
        try:
            st = os.stat(path)
        except OSError:
            return None
        return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)

    def _client_identity(self) -> Optional[Identity]:
        """The client TLS identity, (re)loaded when its files' (device,
        inode, size, mtime) change -- so a re-enroll or `identity rotate`
        takes effect without a restart. `None` when it cannot be loaded: the
        error is logged once per distinct message, and remote backup (only)
        is skipped. A failed load is retried when the files change or at the
        next cycle; an interrupted promotion keeps failing (with its own
        explanation) until the CLI finishes it."""
        try:
            cert_path, key_path = self._identity_paths()
        except IdentityError as e:
            return self._identity_failed(None, e)
        sig = (str(cert_path), str(key_path), self._file_sig(cert_path), self._file_sig(key_path))
        if self._identity is not None and sig == self._identity_sig:
            return self._identity
        if self._identity is None and sig == self._identity_failed_sig:
            return None
        try:
            loaded = load_identity(
                cert_path, key_path, ident.ROLE_CLIENT, lock_timeout=IDENTITY_LOCK_TIMEOUT
            )
        except IdentityError as e:
            return self._identity_failed(sig, e)
        # Signature of what was actually loaded: a change between the stat
        # and the load is seen as a change next time and reloaded then.
        previous = self._identity
        self._identity, self._identity_sig = loaded, sig
        self._identity_failed_sig = None
        self._identity_error = None
        if previous is None or previous.fingerprint != loaded.fingerprint:
            if previous is not None:
                self.close()
            logger.info(
                f"client TLS identity {loaded.cert_path} loaded (fingerprint "
                f"{loaded.fingerprint})"
            )
        return loaded

    def _identity_failed(self, sig, e: IdentityError) -> None:
        self._identity = None
        self._identity_sig = None
        self._identity_failed_sig = sig
        self.close()
        message = f"remote backup skipped: cannot use the client TLS identity: {e}"
        if message != self._identity_error:
            self._identity_error = message
            logger.error(message)
        else:
            logger.debug(message)
        return None

    def _transport(
        self, destination: str, endpoint: _Endpoint, server_fp: str, identity: Identity
    ) -> Optional[_Transport]:
        """The destination's pinned session and websocket context, rebuilt
        when the identity, the server pin or the URL changed."""
        current = self._transports.get(destination)
        if (
            current is not None and current.endpoint == endpoint
            and current.key == (identity.fingerprint, server_fp)
        ):
            return current
        if current is not None:
            with contextlib.suppress(Exception):
                current.session.close()
            del self._transports[destination]
        try:
            ctx = client_context(identity, lock_timeout=IDENTITY_LOCK_TIMEOUT)
            session = pinned_session(
                identity, server_fp, timeout=HTTP_TIMEOUT,
                lock_timeout=IDENTITY_LOCK_TIMEOUT,
            )
        except IdentityError as e:
            # The files changed since the identity was loaded: reload next time.
            self._identity_failed(None, e)
            return None
        # Every HTTP request on this session names our protocol version.
        session.headers.update(protocol_headers())
        transport = _Transport(endpoint, server_fp, identity.fingerprint, session, ctx)
        self._transports[destination] = transport
        return transport

    # ------------------------------------------------------ refusals and whoami

    def _log_warning_once(self, key, message: str) -> None:
        """`_log_refusal_once` at WARNING (a condition that heals by itself)."""
        if self._refusal_logged.get(key) != message:
            self._refusal_logged[key] = message
            logger.warning(message)
        else:
            logger.debug(message)

    def _log_refusal_once(self, key, message: str) -> None:
        if self._refusal_logged.get(key) != message:
            self._refusal_logged[key] = message
            logger.error(message)
        else:
            logger.debug(message)

    def _note_refusal(self, base_url: str, ds_name: str, destination: str, result: dict) -> None:
        """Log a server refusal that arrived instead of the final ack, or in
        place of a broken pipe (`_log_refusal`), except that the refusals to
        destroy or replace server snapshots, or to list them
        (`_STREAM_REFUSAL_REASONS`; a
        well-behaved client is stopped before it gets one, so this is a race or
        an older client's view) are ERROR once per episode, then DEBUG."""
        if result.get('reason') in _STREAM_REFUSAL_REASONS and not result.get('retry'):
            self._log_refusal_once(
                (f"{ds_name} [server refusal]", destination),
                f"server {base_url} refused {ds_name} -> {destination}: "
                f"{result.get('error') or 'unknown error'}",
            )
        else:
            _log_refusal(base_url, ds_name, result)

    def _refuse_destination(self, destination: str, transport: _Transport, message: str) -> None:
        """A destination-level refusal: logged once, and the destination is
        skipped for the rest of this cycle."""
        self._log_refusal_once((None, destination), message)
        self._checked[destination] = (transport.key, False)

    def _auth_refusal(
        self, destination: str, transport: _Transport, *, resp=None, exc=None
    ) -> Optional[str]:
        """If `resp` (an HTTP response) or `exc` (a failure, incl. a websocket
        upgrade refused with 401/403) means the server will not talk to this
        client, or is not the pinned server: the message to log, else
        `None`. Never raises."""
        where = f"server {transport.base_url} (destination {destination})"
        enroll = f"`zfsbackup-config enroll {destination} --token-file …`"
        status, body = None, {}
        if resp is not None:
            status, body = resp.status_code, _json_body(resp)
        elif exc is not None:
            if is_pin_mismatch(exc):
                actual = _presented_fingerprint(exc) or 'unknown'
                return (
                    f"{where} presented certificate {actual}, but the destination is "
                    f"pinned to {transport.server_fp}: possible MITM or the server was "
                    f"re-keyed; re-enroll ({enroll})"
                )
            if is_tls_alert_rejection(exc):
                return (
                    f"{where} does not trust this client {transport.identity_fp} "
                    f"(TLS alert {_alert_reason(exc)}); re-enroll ({enroll})"
                    + _ENROLL_HINT
                )
            if isinstance(exc, websocket.WebSocketBadStatusException):
                status = getattr(exc, 'status_code', None)
                raw = getattr(exc, 'resp_body', None)
                try:
                    parsed = json.loads(raw) if raw else {}
                except Exception:
                    parsed = {}
                body = parsed if isinstance(parsed, dict) else {}
        return self._status_refusal(destination, transport, status, body)

    def _status_refusal(
        self, destination: str, transport: _Transport, status, body: dict
    ) -> Optional[str]:
        """The message for an HTTP 401/403 auth refusal or a 426 protocol
        refusal, else `None`."""
        where = f"server {transport.base_url} (destination {destination})"
        enroll = f"`zfsbackup-config enroll {destination} --token-file …`"
        if status == 426:
            return _protocol_mismatch(where, body.get('protocol'), refused=True)
        if status == 401:
            return (
                f"{where} did not receive this client's certificate "
                f"{transport.identity_fp} (HTTP 401): {body.get('error') or 'refused'}"
            )
        if status == 403:
            error = body.get('error')
            if error == 'client_id mismatch':
                # This certificate stays bound to the old id on the server, so
                # a plain re-enroll with it would only get
                # `cert_bound_elsewhere`: the way forward is the old id back,
                # or a revoke of the old binding plus a NEW key.
                bound = body.get('bound')
                return (
                    f"{where} binds this client's certificate {transport.identity_fp} "
                    f"to client id {bound!r}, but this host's client id is "
                    f"{self.client_id!r}. Either restore the old id "
                    f"(`zfsbackup-config identity set-client-id {bound}`), or have "
                    f"the server operator revoke the old binding (`zfsbackup-config "
                    f"trust revoke --client {bound}`) and then re-enroll with a fresh "
                    f"key (`zfsbackup-config enroll {destination} --new-key "
                    f"--token-file …`)"
                )
            if error == 'client not trusted':
                return (
                    f"{where} does not trust this client {transport.identity_fp} "
                    f"(revoked, expired or never enrolled); re-enroll ({enroll})"
                    + _ENROLL_HINT
                )
            return f"{where} refused this client (HTTP 403): {error or 'forbidden'}"
        return None

    def _whoami(self, destination: str, transport: _Transport) -> bool:
        """Ask the server who it takes this client for (plan A6); -> whether
        the destination may be used this cycle. Every "no" is logged: auth
        refusals, a pin mismatch and a protocol mismatch (a 426, or an answer
        without a supported `protocol`) once (ERROR, deduplicated), a transient
        failure at ERROR each cycle, the server's retryable 503 at INFO. A
        reset or broken pipe is retried once: under TLS 1.3 it can be the
        server rejecting our certificate after the handshake."""
        url = f"{transport.base_url}/whoami"
        resp = None
        for attempt in (1, 2):
            try:
                resp = transport.session.get(url, timeout=HTTP_TIMEOUT)
                break
            except Exception as e:
                refusal = self._auth_refusal(destination, transport, exc=e)
                if refusal is not None:
                    self._refuse_destination(destination, transport, refusal)
                    return False
                if _is_reset(e):
                    logger.debug(f"/whoami at {transport.base_url} (attempt {attempt}): {e}")
                    if attempt == 1:
                        continue
                    # No exception text in the message: it is deduplicated,
                    # and a reset and a broken pipe are the same finding.
                    self._refuse_destination(
                        destination, transport,
                        f"server {transport.base_url} (destination {destination}): "
                        "connection reset after TLS handshake -- the server may have "
                        f"rejected this client's certificate {transport.identity_fp}",
                    )
                    return False
                logger.error(
                    f"cannot reach {transport.base_url} (destination {destination}): {e}; "
                    "skipping it this cycle"
                )
                return False
        if _is_reloading(resp):
            logger.info(
                f"server {transport.base_url} is reloading; destination {destination} "
                "skipped this cycle"
            )
            return False
        refusal = self._auth_refusal(destination, transport, resp=resp)
        if refusal is not None:
            self._refuse_destination(destination, transport, refusal)
            return False
        body = _json_body(resp)
        if not resp.ok or not isinstance(body.get('client_id'), str):
            logger.error(
                f"unexpected /whoami answer from {transport.base_url} (destination "
                f"{destination}): HTTP {resp.status_code}; skipping it this cycle"
            )
            return False
        problem = _whoami_protocol_problem(
            f"server {transport.base_url} (destination {destination})", body
        )
        if problem is not None:
            self._refuse_destination(destination, transport, problem)
            return False
        bound = body['client_id']
        if bound != self.client_id:
            self._refuse_destination(
                destination, transport,
                self._status_refusal(
                    destination, transport, 403, {'error': 'client_id mismatch', 'bound': bound}
                ),
            )
            return False
        if self._refusal_logged.pop((None, destination), None) is not None:
            logger.info(
                f"server {transport.base_url} (destination {destination}) accepts this "
                f"client again as {bound!r}"
            )
        return True

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

    def _negotiate(
        self, transport: _Transport, client_id: str, dsi: DatasetInfo, snapshots,
        destination: str,
    ) -> Optional[str]:
        """The newest snapshot present on both sides, or `None` when the
        server answered that there is none. Raises `NegotiationFailed` when
        there was no usable answer (logged at ERROR, except the server's
        retryable "reloading" 503, which the caller logs at INFO). An auth
        refusal or pin mismatch is a destination-level refusal (logged once;
        the destination is skipped for the rest of the cycle)."""
        base_url = transport.base_url
        try:
            resp = transport.session.post(
                f"{base_url}/backup/{_path(client_id)}/{_path(dsi.name)}/negotiate",
                json={
                    'snapshots': [s.name for s in snapshots],
                    # Effective config for this destination -- a per-destination
                    # retention override (if any) replaces the dataset-level set
                    # entirely (see DatasetConfig.to_property and item 3b).
                    'config': dsi.config.to_property(destination),
                },
                timeout=HTTP_TIMEOUT,
            )
            if _is_reloading(resp):
                # The one retryable 503 covers a reload and a busy/unsettled
                # dataset; the server's own text tells which.
                body = _json_body(resp)
                text = str(body.get('error') or '')
                if body.get('reason') == 'unsettled_ancestor':
                    # A parent has not completed its first backup, so this
                    # child waits for it: visible at WARNING, once per
                    # episode (re-armed by the next successful negotiate).
                    self._log_warning_once(
                        (f"{dsi.name} [unsettled]", destination),
                        f"{dsi.name} -> {destination}: waiting for {body.get('ancestor')!r}, "
                        "whose first backup has not completed on the server; "
                        f"{dsi.name} is not sent until it has",
                    )
                if 'reloading' in text:
                    raise NegotiationFailed("server is reloading", reloading=True)
                raise NegotiationFailed(
                    "server is busy" if text in ('', _BUSY_TEXT) else f"server is busy: {text}",
                    reloading=True,
                )
            refusal = self._auth_refusal(destination, transport, resp=resp)
            if refusal is not None:
                self._refuse_destination(destination, transport, refusal)
                raise NegotiationFailed(refusal)
            if resp.status_code == 409:
                # The server refused to mix this dataset into one it already
                # holds for another source (D7): its message says which.
                try:
                    reason = resp.json().get('error') or resp.text
                except Exception:
                    reason = resp.text
                self._log_refusal_once(
                    (dsi.name, destination), f"server refused {dsi.name}: {reason}"
                )
                raise NegotiationFailed(str(reason))
            if resp.status_code >= 400:
                # The server's own explanation (a 400 says what is wrong with
                # the request) instead of a bare "400 Client Error".
                reason = _json_body(resp).get('error')
                if reason:
                    logger.error(
                        f"Negotiation failed for {dsi.name} at {base_url}: "
                        f"HTTP {resp.status_code}: {reason}"
                    )
                    raise NegotiationFailed(f"HTTP {resp.status_code}: {reason}")
            resp.raise_for_status()
            self._refusal_logged.pop((dsi.name, destination), None)
            self._refusal_logged.pop((f"{dsi.name} [unsettled]", destination), None)
            body = resp.json()
            self._server_info[(dsi.name, destination)] = (
                body.get('server_copy'), body.get('server_dataset')
            )
            newer, newer_count = body.get('server_newer'), body.get('server_newer_count')
            self._server_newer[(dsi.name, destination)] = (
                newer_count if type(newer_count) is int else 0,
                newer if isinstance(newer, list) else [],
            )
            if body.get('server_newer_unknown') is True:
                status = 'unknown'
            elif 'server_newer' in body or 'server_newer_count' in body:
                status = 'known'
            else:
                status = 'absent'
            self._server_newer_status[(dsi.name, destination)] = status
            return body.get('common_snapshot')
        except NegotiationFailed:
            raise
        except Exception as e:
            refusal = self._auth_refusal(destination, transport, exc=e)
            if refusal is not None:
                self._refuse_destination(destination, transport, refusal)
            else:
                logger.error(f"Negotiation failed for {dsi.name} at {base_url}: {e}")
            raise NegotiationFailed(str(e)) from e

    def _transfer(
        self,
        transport: _Transport,
        client_id: str,
        dsi: DatasetInfo,
        latest,
        common_snap,
        destination: str,
        snapshots: Optional[list] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        mode: str = 'plain',
        discard: Tuple[str, ...] = (),
    ) -> bool:
        """Bring the server up to `latest`, one `zfs send -i` per snapshot.

        After one negotiate, the chain is the managed snapshots newer (by
        `createtxg`) than `common_snap`, oldest first, so every intermediate
        snapshot reaches the server; with no common snapshot only `latest` is
        sent in full (history before the first backup is not backfilled).
        A resumable token of the server runs first, as step 0; then the common
        snapshot is negotiated again and the chain continues from it. Each step:
        `should_stop` (a safe point), the in-flight pin, a `mode=fresh` stream
        and its ack, the anchor, the pin cleared. Any failure stops the chain
        (the next cycle continues from the anchor), except that a step whose
        snapshot has vanished is rebuilt around, once per missing member.
        `mode` is the send mode (`_SEND_MODES`), fixed for the whole chain.
        `discard` (`_rollback_permit`) is the guids of the server snapshots
        every stream of this cycle lets the server destroy.
        A stop mid-chain returns True: the work continues next cycle."""
        base_url = transport.base_url
        if snapshots is None:
            snapshots = self.local_manager.list_snapshots(dsi)
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
            chain = self._plan_chain(snapshots, common_snap)
            logger.info(
                f"[DRY RUN] Would send {dsi.name}: "
                + (f"{common_snap.name} -> " if common_snap else "full ")
                + ' -> '.join(s.name for s in chain)
                + f" ({len(chain)} step(s)) -> {base_url}"
                + (
                    f"; the server would be allowed to discard {self._discard_names(dsi, destination, discard)} "
                    "(a snapshot it holds that this host does not)"
                    if discard else ""
                )
            )
            return True

        if should_stop is not None and should_stop():
            logger.info(f"stop requested; not starting a send of {dsi.name}")
            return False

        # Step 0: the server's partial receive, if it can be resumed.
        status, resumed = self._try_resume(
            transport, client_id, dsi, destination, snapshots, mode, should_stop
        )
        if status == 'skip':
            return False
        if resumed is not None:
            finished = resumed.sent.name == latest.name
            outcome = self._push_step(
                transport, client_id, dsi, destination, snapshots, resumed,
                clear_pin=finished, discard=discard,
            )
            if outcome != _OK:
                return outcome == _ANCHOR_FAILED and finished
            if finished:
                return True
            if should_stop is not None and should_stop():
                logger.info(
                    f"stop requested; {dsi.name} -> {destination} continues to "
                    f"{latest.name} next cycle"
                )
                # Cleanly settled on the anchor: the kept pin is not needed.
                self.local_manager.clear_inflight(dsi, destination)
                return True
            # The resumed snapshot is now on the server: negotiate again and
            # carry on from the new common snapshot.
            snapshots = self.local_manager.list_snapshots(dsi) or snapshots
            try:
                common_name = self._negotiate(transport, client_id, dsi, snapshots, destination)
            except NegotiationFailed as e:
                if e.reloading:
                    logger.info(
                        f"server {transport.base_url} is {_retry_word(e)}; {dsi.name} continues next cycle"
                    )
                return False
            latest = snapshots[0]
            common_snap = None
            if common_name:
                common_snap = next((s for s in snapshots if s.name == common_name), None)
                if common_snap is None:
                    logger.error(
                        f"server {base_url} reports {common_name!r} as the common snapshot of "
                        f"{dsi.name}, which is not a managed local snapshot; skipping the rest "
                        "of this cycle rather than sending a full stream"
                    )
                    return False
            discard = self._rollback_permit(dsi, destination, common_snap, snapshots)
            if discard is None:
                return False
            if common_snap is not None and common_snap.name == latest.name:
                self._settle_up_to_date(dsi, latest, destination, snapshots)
                return True

        # The chain of fresh steps.
        sent_any = resumed is not None
        rebuilds = 0
        while True:
            chain = self._plan_chain(snapshots, common_snap)
            if not chain:
                if sent_any:
                    self.local_manager.clear_inflight(dsi, destination)
                return True
            if len(chain) > 1:
                logger.info(
                    f"Sending {len(chain)} snapshots of {dsi.name} to {destination}: "
                    f"{common_snap.name if common_snap else 'full'} -> "
                    + ' -> '.join(s.name for s in chain)
                )
            prev = common_snap
            rebuilt = False
            for step in chain:
                # One poll per step, the last look before `zfs send` is spawned
                # (never during a stream). After at least one stream a stop is
                # a clean pause; before the first send nothing was done.
                if should_stop is not None and should_stop():
                    if sent_any:
                        logger.info(
                            f"stop requested; {dsi.name} -> {destination} continues to "
                            f"{chain[-1].name} next cycle"
                        )
                        # Settled on the anchor of the step just sent; the pin
                        # kept between steps is not needed any more.
                        self.local_manager.clear_inflight(dsi, destination)
                        return True
                    logger.info(f"stop requested; not starting a send of {dsi.name}")
                    return False
                started = self._start_fresh(dsi, destination, prev, step, mode)
                outcome = _FAILED if started is None else self._push_step(
                    transport, client_id, dsi, destination, snapshots, started,
                    clear_pin=step is chain[-1], after_success=sent_any,
                    should_stop=should_stop, discard=discard,
                )
                if outcome == _OK:
                    sent_any = True
                    prev = step
                    continue
                if outcome == _ANCHOR_FAILED:
                    return step.name == chain[-1].name
                if outcome == _FAILED and rebuilds < len(chain):
                    # B7: a member deleted between steps is not an error.
                    fresh = self.local_manager.list_snapshots(dsi)
                    names = {s.name for s in fresh}
                    if (
                        fresh and step.name not in names
                        and (prev is None or prev.name in names)
                    ):
                        rebuilds += 1
                        logger.warning(
                            f"{dsi.name}@{step.name} no longer exists; "
                            "rebuilding the rest of the chain without it"
                        )
                        snapshots = fresh
                        common_snap = (
                            next(s for s in fresh if s.name == prev.name) if prev else None
                        )
                        rebuilt = True
                        break
                return False
            if not rebuilt:
                return True

    @staticmethod
    def _plan_chain(snapshots: list, common_snap) -> list:
        """The snapshots to send, oldest first. With a common snapshot: every
        managed snapshot newer than it by `createtxg`. Without: just the
        newest, in full."""
        if common_snap is None:
            return [snapshots[0]] if snapshots else []
        return [s for s in reversed(snapshots) if snapshot_newer(s, common_snap)]

    def _start_fresh(self, dsi, destination, prev, step, mode: str) -> Optional[_SendStream]:
        """Pin `prev`..`step` (`inflight.<dest>`, written BEFORE `zfs send`
        starts; a failed write is logged and the send proceeds unpinned) and
        start `zfs send [-i prev] step` with the flags of `mode`. `None` (logged)
        when the send cannot be started."""
        inflight_set = self.local_manager.set_inflight(
            dsi, destination, prev.name if prev else None, step.name
        )
        options = _SEND_MODES[mode].options
        try:
            if prev:
                logger.info(f"Sending incremental {dsi.name}: {prev.name} -> {step.name}")
                return _SendStream(
                    zfs.send.snapshot(step.snapshot, since=prev.snapshot, **options),
                    'fresh', inflight_set, step, prev,
                )
            logger.info(f"Sending full snapshot {dsi.name}@{step.name}")
            return _SendStream(
                zfs.send.snapshot(step.snapshot, **options), 'fresh', inflight_set, step, None
            )
        except Exception as e:
            logger.error(f"Failed to start zfs send for {dsi.name}: {e}")
            return None

    def _push_step(
        self,
        transport: _Transport,
        client_id: str,
        dsi: DatasetInfo,
        destination: str,
        snapshots: list,
        started: _SendStream,
        clear_pin: bool = True,
        after_success: bool = False,
        should_stop: Optional[Callable[[], bool]] = None,
        discard: Tuple[str, ...] = (),
    ) -> str:
        """Send ONE stream (`started`, from `_try_resume` or `_start_fresh`) over
        the pinned websocket, wait for the server's ack, then record the anchor
        and clear the in-flight pin (unless `clear_pin` is False: between chain
        steps the pin `(prev, sent)` stays, so `prev` is still protected if
        `sent` vanishes unnoticed; the next step's pin overwrites it and
        `_transfer` clears it after the last step or on a clean stop).
        -> `_OK`; `_ANCHOR_FAILED` (the server has
        the snapshot but the anchor could not be written: the pin stays and
        the caller stops); `_STOP` (the server is reloading or busy, or this
        destination was refused: nothing more this cycle, no diagnosis); or
        `_FAILED` (the transfer did not complete).

        `after_success` is True when an earlier stream of this cycle has
        already succeeded. Then a failure to CONNECT (before one byte of the
        stream is sent: an `OSError`, a TLS reset, a failed websocket
        handshake, but not an authentication refusal or a timeout) is retried
        for the connect only, `CONNECT_RETRY_DELAYS`, while the started
        `zfs send` waits on its pipe: a server that has just drained for a
        reload is back in a few seconds. `should_stop` is checked before each
        retry. A stop, or exhausted retries, reap the send exactly once (the
        `finally`), change neither the anchor nor the pin (the previous
        step's stays in place) and return `_STOP` (INFO, no re-list). The
        first stream of a cycle fails as always.

        `discard` is the guids of the server snapshots the server may destroy
        for this stream (`_rollback_permit`), sent as `&discard=` on every
        stream of the cycle."""
        base_url = transport.base_url
        stream, mode, inflight_set, sent, base = started

        ws = None
        tls_sock = None
        entered = False
        failed = False
        sending = False
        try:
            # Everything below, including building the URL and the websocket,
            # is inside this `try`: `stream` (a started `zfs send`) is reaped
            # by the `finally` whatever fails first (`entered` is still False).
            #
            # `mode` is required by the server: `resume` only when this stream
            # continues the server's resume token, `fresh` for a new stream
            # (the server then discards any partial receive state). `from`
            # (the base's name) and `from_guid` (its guid, preferred when
            # present) tell the server which of its snapshots the stream is
            # incremental from: it passes `-F` only if that destroys nothing,
            # or only the snapshots listed in `discard` (guids; the cycle's
            # `_rollback_permit`). A server that predates them ignores all
            # three.
            ws_url = (
                f"{transport.endpoint.ws_base}/backup/{_path(client_id)}/{_path(dsi.name)}"
                f"/stream?mode={mode}"
            )
            if base is not None:
                ws_url += f"&from={quote(base.name, safe='')}"
                if base.guid is not None:
                    ws_url += f"&from_guid={base.guid}"
            if discard:
                ws_url += f"&discard={','.join(discard)}"
            ws = websocket.WebSocket()
            # TCP + TLS handshake + pin check BEFORE any websocket byte; a
            # wrong server raises `PinMismatch` (not an OSError) and, like any
            # connect failure, reaps the already started `zfs send` below
            # (`entered` is still False). The websocket then runs over that
            # very socket (S8: the URL's host and `sslopt` are not used).
            delays = CONNECT_RETRY_DELAYS if after_success else ()
            attempt = 0
            while True:
                try:
                    tls_sock = connect_pinned(
                        transport.endpoint.host, transport.endpoint.port, transport.ctx,
                        transport.server_fp, WS_CONNECT_TIMEOUT,
                    )
                    ws.connect(
                        ws_url, socket=tls_sock, timeout=WS_CONNECT_TIMEOUT,
                        header=protocol_headers(),
                    )
                    break
                except Exception as connect_error:
                    transient = (
                        after_success
                        and isinstance(connect_error, (OSError, websocket.WebSocketException))
                        and not isinstance(
                            connect_error, (TimeoutError, websocket.WebSocketTimeoutException)
                        )
                        and self._auth_refusal(destination, transport, exc=connect_error) is None
                    )
                    if not transient:
                        raise
                    if attempt >= len(delays):
                        logger.info(
                            f"server {base_url} is not accepting connections right after a "
                            f"successful step (probably reloading); {dsi.name} -> "
                            f"{destination} continues next cycle"
                        )
                        return _STOP
                    # Start the next attempt clean; the started `zfs send`
                    # keeps waiting on its pipe.
                    with contextlib.suppress(Exception):
                        ws.close()
                    if tls_sock is not None:
                        with contextlib.suppress(Exception):
                            tls_sock.close()
                        tls_sock = None
                    ws = websocket.WebSocket()
                    for _ in range(max(1, round(delays[attempt] / CONNECT_RETRY_SLICE))):
                        if should_stop is not None and should_stop():
                            logger.info(
                                f"stop requested; {dsi.name} -> {destination} continues next cycle"
                            )
                            return _STOP
                        time.sleep(CONNECT_RETRY_SLICE)
                    if should_stop is not None and should_stop():
                        logger.info(
                            f"stop requested; {dsi.name} -> {destination} continues next cycle"
                        )
                        return _STOP
                    attempt += 1
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
                # the child's failure (libzfseasy reports "Command killed by
                # signal SIGPIPE") that a `with` block's close() would raise in
                # its place.
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
                    return _STOP
                if result.get('reason') in _STREAM_REFUSAL_REASONS:
                    self._note_refusal(base_url, dsi.name, destination, result)
                    return _FAILED
                logger.error(
                    f"Transfer failed for {dsi.name}: {result.get('error', 'unknown error')}"
                )
                return _FAILED
        except Exception as e:
            failed = True
            # Not trusted / id mismatch on the upgrade (401/403), a TLS alert
            # (S10: a raw `ssl.SSLError` escapes `connect()`, not a
            # WebSocketException) or a pin mismatch: a destination-level
            # refusal, logged once, and the destination is skipped for the
            # rest of this cycle.
            auth = self._auth_refusal(destination, transport, exc=e)
            if auth is not None:
                self._refuse_destination(destination, transport, auth)
                return _STOP
            # A timeout means a silent peer, which cannot have left a refusal
            # behind (a refusal closes the socket): do not wait on it again.
            timed_out = isinstance(e, (TimeoutError, websocket.WebSocketTimeoutException))
            refusal = _peer_refusal(ws) if sending and not timed_out else None
            if refusal is not None:
                self._note_refusal(base_url, dsi.name, destination, refusal)
                if _is_reloading_refusal(refusal):
                    # Reloading or busy, seen as a failed send: the same
                    # retryable refusal as at the ack, so the chain stops
                    # without a diagnosis.
                    return _STOP
            else:
                logger.error(f"Transfer failed for {dsi.name} -> {base_url}: {e}")
            return _FAILED
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
            if tls_sock is not None:
                # Normally closed by `ws.close()`; this covers a failed
                # upgrade that left it open. Closing twice is harmless.
                with contextlib.suppress(Exception):
                    tls_sock.close()
            if not entered:
                # connect() failed or timed out before `with stream:`: the
                # `zfs send` is already running and would block forever on a
                # full pipe. Closing the handle EPIPEs it and reaps it.
                try:
                    stream.close()
                except Exception:
                    pass

        self._refusal_logged.pop((f"{dsi.name} [server refusal]", destination), None)
        old_anchor = self.local_manager.get_anchor(dsi, destination)
        keep_anchor = False
        if old_anchor and old_anchor != sent.name:
            # Never move an anchor backwards (by createtxg): a snapshot older
            # than the current anchor -- a resumed one, or a chain step after
            # one -- leaves it where it is.
            current = next((s for s in snapshots if s.name == old_anchor), None)
            keep_anchor = snapshot_newer(current, sent)
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
        # the pin stays as well: nothing was written, and the next step writes
        # its own pin (or the next successful send replaces it).
        if anchored and not keep_anchor:
            # Backstop for the pruner: a destroy that read the anchors before
            # this step pinned its pair could still have taken `sent` while it
            # was being sent. Check, before the pin is released, that it is
            # still there. If not, put the anchor back on the server's
            # previous common snapshot (never backwards past it), keep the
            # pin and stop. (A `zfs hold` on the in-flight pair would be the
            # airtight alternative; deferred, since holds left by a crash
            # would need cleaning up.)
            present = self._snapshot_present(sent)
            if present is not True:
                restore = base.name if base is not None else old_anchor
                if present is False:
                    logger.error(
                        f"{dsi.name}@{sent.name} was destroyed while it was being sent to "
                        f"{destination}; the server has it but this host no longer does. "
                        + (f"Putting the anchor back on {restore}. " if restore else "")
                        + "Stopping; the next cycle negotiates again"
                    )
                    if restore:
                        self.local_manager.set_anchor(dsi, destination, restore)
                    else:
                        self.local_manager.clear_anchor(dsi, destination)
                else:
                    logger.warning(
                        f"cannot confirm that {dsi.name}@{sent.name} still exists; keeping "
                        f"its in-flight pin and stopping"
                    )
                return _ANCHOR_FAILED
            if clear_pin:
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
        return _OK if anchored else _ANCHOR_FAILED

    @staticmethod
    def _snapshot_present(sent) -> Optional[bool]:
        """Does the snapshot `sent` still exist, and is it still the same one
        (GUID)? One `zfs get` of that snapshot. `None` when zfs fails for any
        other reason."""
        try:
            guid = zfs.get.value(sent.snapshot, 'guid')
        except Exception as e:
            if 'does not exist' in str(e).lower():
                return False
            return None
        if guid is None or sent.guid is None:
            return True if guid is not None else None
        try:
            return int(guid, 0) == sent.guid
        except ValueError:
            return None

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
        wrote = False
        if not anchored:
            newer = False
            current = None
            if anchor and snapshots:
                current = next((s for s in snapshots if s.name == anchor), None)
                newer = snapshot_newer(current, latest)
            if (
                anchor and snapshots and current is None
                and self._server_newer_status.get((dsi.name, destination), 'absent') != 'absent'
            ):
                # The anchor names a snapshot this host no longer has (a
                # rollback or an in-place restore, or the pruner after a
                # backstop). A server that reports B-2b state may hold that
                # snapshot, so moving the anchor to `latest` would let a later
                # cycle mistake the rollback for the backstop and send
                # `discard`. Left as it is; the pin stays too.
                logger.debug(
                    f"{dsi.name} -> {destination}: the anchor {anchor!r} names a snapshot this "
                    f"host does not have; left unchanged although {latest.name} is on the server"
                )
                return
            wrote = not newer
            anchored = newer or self.local_manager.set_anchor(dsi, destination, latest.name)
        if anchored and self.local_manager.get_inflight(dsi, destination) is not None:
            # As after a step: release the pin only if the snapshot is really
            # still here (the pruner may have taken it meanwhile). If it is
            # gone, put the anchor back where it was and keep the pin; if that
            # cannot be told, keep the pin.
            present = self._snapshot_present(latest)
            if present is not True:
                if present is False:
                    logger.error(
                        f"{dsi.name}@{latest.name} no longer exists although {destination} "
                        "holds it; keeping its in-flight pin"
                    )
                    if wrote:
                        if anchor:
                            self.local_manager.set_anchor(dsi, destination, anchor)
                        else:
                            self.local_manager.clear_anchor(dsi, destination)
                else:
                    logger.warning(
                        f"cannot confirm that {dsi.name}@{latest.name} still exists; "
                        "keeping its in-flight pin"
                    )
                return
            self.local_manager.clear_inflight(dsi, destination)

    def _fetch_resume_token(
        self, transport, client_id, dsi, destination
    ) -> Tuple[str, Optional[str]]:
        """Ask the server for its resume token -> `('token', token)`,
        `('none', None)`, or `('skip', None)`: no usable answer, so the cycle
        is skipped (D6). Falling through to a fresh send on an unreadable
        answer would make the server discard its partial progress. An auth
        refusal or pin mismatch is also a destination-level refusal (logged
        once)."""
        base_url = transport.base_url
        try:
            resp = transport.session.get(
                f"{base_url}/backup/{_path(client_id)}/{_path(dsi.name)}/resume_token",
                timeout=RESUME_TOKEN_TIMEOUT,
            )
            if _is_reloading(resp):
                logger.info(
                    f"server {base_url} is reloading; skipping {dsi.name} this cycle"
                )
                return 'skip', None
            auth = self._auth_refusal(destination, transport, resp=resp)
            if auth is not None:
                self._refuse_destination(destination, transport, auth)
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
            auth = self._auth_refusal(destination, transport, exc=e)
            if auth is not None:
                self._refuse_destination(destination, transport, auth)
                return 'skip', None
            logger.warning(
                f"cannot ask {base_url} for the resume token of {dsi.name}: {e}; "
                "skipping this cycle rather than risk discarding its partial "
                "progress with a fresh send"
            )
            return 'skip', None

    def _resume_target(self, token: str, dsi: DatasetInfo, snapshots, mode: str = 'plain') -> object:
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
        send_mode = _SEND_MODES[mode]
        if not (send_mode.required <= info.flags <= send_mode.allowed):
            return (
                "the token records stream features this daemon does not send in "
                f"{mode} mode (unexpected: "
                f"{', '.join(sorted(info.flags - send_mode.allowed)) or 'none'}; "
                f"missing: {', '.join(sorted(send_mode.required - info.flags)) or 'none'})"
            )
        if info.toguid is None:
            return "the token does not name its target snapshot"
        # GUIDs come from the one managed listing (`SnapshotInfo.guid`); only a
        # managed snapshot can match, as before. A managed snapshot whose
        # GUID was not listed makes "no match" uncertain, not definitive.
        by_guid = {s.guid: s for s in snapshots if s.guid is not None}
        unknown = [s.name for s in snapshots if s.guid is None]

        def _lookup(guid):
            found = by_guid.get(guid)
            if found is None and unknown:
                raise RuntimeError(
                    f"cannot read the GUID of local snapshot(s) {', '.join(unknown[:3])}"
                )
            return found

        to_snap = _lookup(info.toguid)
        if to_snap is None:
            return (
                f"its target snapshot (guid {info.toguid}, {info.toname}) is not a "
                "managed local snapshot of this dataset"
            )
        from_snap = None
        if info.fromguid is not None:
            from_snap = _lookup(info.fromguid)
            if from_snap is None:
                return (
                    f"its incremental source (guid {info.fromguid}) is not a "
                    "managed local snapshot of this dataset"
                )
            if not snapshot_newer(to_snap, from_snap):
                return "its incremental source is not older than its target"
        return to_snap, from_snap

    def _try_resume(
        self, transport, client_id, dsi, destination, snapshots, mode: str,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> Tuple[str, Optional[_SendStream]]:
        """Step 0: the server's resume token -> `('skip', None)` (no usable
        answer: skip the cycle), `('fresh', None)` (nothing to resume, or the
        token cannot be: carry on with a fresh send), or `('resume',
        stream)` with `zfs send -t <token>` started (`mode=resume`).

        The token is read first (`_fetch_resume_token`; no usable answer skips
        the cycle). It is resumed only if `_resume_target` accepts it -- no
        zfs error, only the stream features of `mode` (`_SEND_MODES`), its
        target (and incremental source) are managed local snapshots by GUID.
        Anything else (stale, corrupt, foreign, or `zfs send -t` failing to
        start) is logged at WARNING and falls through to a FRESH send (the
        server discards the stale state). A resume pins both of its snapshots
        (`inflight.<dest>`) BEFORE `zfs send` starts; a failed write is logged
        and the send proceeds unpinned. A failed send leaves the pin in place."""
        status, token = self._fetch_resume_token(transport, client_id, dsi, destination)
        if status == 'skip':
            return 'skip', None
        if not token:
            return 'fresh', None
        try:
            target = self._resume_target(token, dsi, snapshots, mode)
        except Exception as e:
            logger.warning(
                f"cannot check the resume token of {dsi.name}: {e}; skipping this "
                "cycle rather than risk discarding the server's partial progress "
                "with a fresh send"
            )
            return 'skip', None
        if isinstance(target, str):
            logger.warning(
                f"Not resuming {dsi.name}: {target}; sending fresh (the server "
                "discards its partial state)"
            )
            return 'fresh', None
        to_snap, from_snap = target
        if should_stop is not None and should_stop():
            logger.info(f"stop requested; not resuming {dsi.name}")
            return 'skip', None
        pinned = self.local_manager.set_inflight(
            dsi, destination, from_snap.name if from_snap else None, to_snap.name
        )
        try:
            logger.info(f"Resuming interrupted transfer for {dsi.name}@{to_snap.name}")
            return 'resume', _SendStream(
                zfs.send.resume(token), 'resume', pinned, to_snap, from_snap
            )
        except Exception as e:
            logger.warning(
                f"zfs send -t failed to start for {dsi.name}: {e}; sending "
                "fresh (the server discards its partial state)"
            )
            return 'fresh', None

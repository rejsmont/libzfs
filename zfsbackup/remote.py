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

import libzfseasy as zfs  # noqa: F401 -- tests patch `zfsbackup.remote.zfs.*` (the shared libzfseasy module)
from zfsbackup import identity as ident
from zfsbackup import receiver
from zfsbackup.backup_manager import DatasetInfo, DatasetManager
from zfsbackup.chain import (  # noqa: F401 -- re-exported: the names tests and callers import from here
    ChainSender, NegotiationFailed, RESUME_TOKEN_SKIP_ERROR_AFTER, TOKEN_SKIP_NOTE,
    _ANCHOR_FAILED, _FAILED, _OK, _SEND_MODES, _STALE_TOKEN_ERRORS, _STOP, _SendMode,
    _SendStream, _retry_word, _token_error_is_definitive,
)
from zfsbackup.config import BackupConfig, Destination, destination_url_problem
from zfsbackup.identity import (
    CLIENT_NAME, Identity, IdentityError, PinMismatch, client_context, connect_pinned,
    is_pin_mismatch, is_tls_alert_rejection, load_identity, pinned_session,
)
from zfsbackup.protocol import (
    NAME_TOO_LONG_REASON, PROTOCOL_VERSION, SUPPORTED_PROTOCOLS, protocol_headers,
)
from zfsbackup.sendmemory import (
    IDENTITY_ERROR, IDENTITY_LOADED, NEGOTIATE_ZFS_ERROR, NEGOTIATE_ZFS_SKIPS, NO_CLIENT_ID,
    REFUSALS, UNREACHABLE, WHOAMI_UNEXPECTED, SendMemory,
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

#: S-12: the client certificate's expiry is checked at most this often
#: (seconds) per `SendMemory` (one per worker), at the start of a cycle.
CLIENT_CERT_EXPIRY_CHECK_EVERY = 24 * 3600.0

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
#: `/resume_token`. Not shorter than `HTTP_TIMEOUT`: the server may take its
#: whole `api.ZFS_REQUEST_BUDGET` (plus the kill of a timed-out `zfs`) to
#: answer, and the client must still be listening for the `zfs_timeout` 503
#: (`api.py` asserts the inequality). A client that gave up first would leave
#: the server's hung `zfs` running and start another one with its next request.
RESUME_TOKEN_TIMEOUT = 30

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


#: The server's `api.BUSY_ERROR['error']` (the API imports this module, so the
#: text is repeated here; a test compares the two).
_BUSY_TEXT = 'busy'


def _connect_failure_retryable(e: BaseException) -> bool:
    """Whether a failure to open the stream's websocket (before one byte of the
    stream was sent) is worth retrying soon: a failure of the CONNECTION (an
    `OSError` incl. timeouts, resets and TLS errors, a closed or timed-out
    websocket) or the server's retryable 503 on the upgrade. A refused upgrade
    with any other status (409, 400, 500, ...) is the server's answer and is
    not."""
    if isinstance(e, websocket.WebSocketBadStatusException):
        return getattr(e, 'status_code', None) == 503
    return isinstance(e, (
        OSError, TimeoutError, ConnectionError,
        websocket.WebSocketConnectionClosedException, websocket.WebSocketTimeoutException,
    ))


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
#: destroy, or cannot replace, snapshots it holds (`api.decide_receive`),
#: because it cannot list them reliably (`listing`), or because the stream
#: itself is not what the client declared or the server accepts
#: (`stream_header`: not a plain single-snapshot stream; `base_mismatch`: its
#: incremental source is not the declared one; `type_mismatch`: the server
#: holds a filesystem where this is a volume, or the reverse): logged once per
#: episode (`RemoteBackupManager._note_refusal`), re-armed by the next success.
_STREAM_REFUSAL_REASONS = (
    'server_newer', 'base_missing', 'no_common', 'listing',
    'stream_header', 'base_mismatch', 'type_mismatch',
)

#: What the operator should do about the refusals that need more than the
#: server's own text: appended to the logged message.
_STREAM_REFUSAL_HINTS = {
    'stream_header': (
        " (the server does not accept what this client sent: a client/server version skew "
        "or a bug in one of them; compare `zfsbackupd --version` on both hosts and report "
        "it if they match)"
    ),
    'base_mismatch': (
        " (the stream is not the one this client declared: a client/server version skew "
        "or a bug in one of them; compare `zfsbackupd --version` on both hosts and report "
        "it if they match)"
    ),
    'type_mismatch': (
        " (the server holds a dataset of the other kind under this name: on the server, "
        "`zfs rename` it to a name outside the backup target; the next cycle sends a full "
        "stream)"
    ),
}


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
    if parts.scheme.lower() != 'https' or host is None:
        # A local `zfs:<dataset>` destination passes `destination_url_problem`
        # but has no endpoint: never reached for one, refused if it is.
        raise ValueError(f"url {url!r} is not an https destination")
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


class RemoteBackupManager(ChainSender):
    """Negotiates and transfers ZFS snapshots to configured remote destinations.

    The send chain itself (`backup_dataset`, `_transfer`, resume, anchors) is
    `chain.ChainSender`'s; this class is its HTTPS / websocket transport:
    enrollment, the pinned client identity, `/whoami`, `_negotiate`,
    `_fetch_resume_token` and `_stream_step`.

    `client_id` is this host's id from the supervisor's config snapshot
    (`daemon.Handoff.client_id`; `None` until `zfsbackup-config enroll`
    generates one). `tls_dir` is `<db>.tls`, where the default client
    identity `client.{crt,key}` lives unless `client_cert_file` /
    `client_key_file` name other files.

    The parallel remote worker builds a FRESH manager for every send (one
    pair, one thread; `sendloop.SenderKit`) and never calls `begin_cycle()`:
    the manager checks the destination's `/whoami` on every `backup_dataset`
    call, and everything that must outlive a send (log latches, the
    skip counters, the "copy confirmed" set) lives in the shared `memory`.
    `begin_cycle()` remains for a manager used over a pass of several
    datasets (it caches each destination's `/whoami` result for the pass and
    retries an identity that failed to load); a manager used without it
    checks on every `backup_dataset` call."""

    _logger = logger

    def __init__(
        self,
        config: BackupConfig,
        local_manager: DatasetManager,
        *,
        client_id: Optional[str],
        tls_dir=None,
        memory: Optional[SendMemory] = None,
    ):
        super().__init__(config, local_manager, client_id=client_id, memory=memory)
        self.tls_dir = Path(tls_dir) if tls_dir is not None else None
        # The loaded client identity and the stat signature of its files. Per
        # manager (a manager built per send loads its own; `close()` of this one
        # must never cut another's sessions).
        self._identity: Optional[Identity] = None
        self._identity_sig: Optional[tuple] = None
        # The signature at the last failed load (no retry until the files
        # change or a new cycle starts).
        self._identity_failed_sig: Optional[tuple] = None
        # destination name -> its pinned transport.
        self._transports: Dict[str, _Transport] = {}
        # destination name -> (transport key, usable) for this cycle.
        self._checked: Dict[str, Tuple[Tuple[str, str], bool]] = {}
        # destination name -> whether its failed check this cycle is worth
        # retrying soon (the server reloading or unreachable; `last_retryable`).
        self._checked_retryable: Dict[str, bool] = {}
        # destination name -> the transport key of the cached `/whoami` failure
        # that was retryable (`_checked`'s retryability, so a cached failure
        # reports it again); cleared with `_checked`.
        self._cached_retryable: Dict[str, Tuple[str, str]] = {}
        # Set by `_whoami` for the check it just ran.
        self._whoami_retryable = False
        self._per_cycle = False

    # In the shared memory (`SendMemory`), under the names they always had:
    # the "no client id" ERROR is logged once per memory, `_identity_error` is
    # the last client-identity error logged, `_expiry_checked_at` the
    # `time.monotonic()` of the last client certificate expiry check (`None`:
    # not yet, see `_check_client_cert_expiry`), and, as the resume-token skip
    # counts (`ChainSender._token_skips`), the negotiate answered `zfs_timeout`
    # (the server's zfs did not answer in time): consecutive skipped cycles,
    # ERROR once at `RESUME_TOKEN_SKIP_ERROR_AFTER`, re-armed by a good
    # negotiate. The properties are the live containers, for tests.
    @property
    def _expiry_checked_at(self) -> Optional[float]:
        return self._memory.expiry_checked_at

    @property
    def _negotiate_zfs_skips(self) -> Dict[Tuple[str, str], int]:
        return self._memory.counter_view(NEGOTIATE_ZFS_SKIPS)

    @property
    def _negotiate_zfs_error_logged(self) -> Set[Tuple[str, str]]:
        return self._memory.latch_view(NEGOTIATE_ZFS_ERROR)

    def _transport_retryable(self, destination: str) -> bool:
        return self._checked_retryable.get(destination, False)

    def _before_destination_check(self) -> None:
        """Outside a worker cycle (`begin_cycle` never called) every
        `backup_dataset` call checks the destination afresh: no cached
        `/whoami` result, and a failed identity load is retried."""
        if not self._per_cycle:
            self._checked.clear()
            self._checked_retryable.clear()
            self._cached_retryable.clear()
            self._identity_failed_sig = None

    def begin_cycle(self) -> None:
        """Start a new pass over the datasets: forget this cycle's
        `/whoami` results, retry a client identity that failed to load, and
        (at most once every `CLIENT_CERT_EXPIRY_CHECK_EVERY`) report a client
        certificate that is expired or about to expire."""
        self._per_cycle = True
        self._checked.clear()
        self._checked_retryable.clear()
        self._cached_retryable.clear()
        self._identity_failed_sig = None
        self._check_client_cert_expiry()

    def _has_https_destination(self) -> bool:
        """Whether the configuration has a destination a client certificate
        is used for (a leftover certificate means nothing without one)."""
        return not all(
            getattr(d, 'is_local', False) is True
            for d in self.config.destinations.values()
        )

    def _check_client_cert_expiry(self) -> None:
        """S-12: a WARNING within `CLIENT_CERT_EXPIRY_WARN_DAYS` of the client
        certificate's expiry, an ERROR once it has expired (the identity load
        then fails and is logged once; this repeats the reminder daily). Reads
        the certificate only, never the key, and never raises. The first call
        of a worker's memory checks (`SendMemory.claim_expiry_check`, atomic: of
        several managers exactly one claims it); later ones at most once per
        `CLIENT_CERT_EXPIRY_CHECK_EVERY` seconds, whether or not it found
        anything. Not run at all when the configuration has no https
        destination (a leftover certificate means nothing then).

        The parallel remote worker's scheduler does not come through here
        (it never calls `begin_cycle`): it claims the memory's check itself
        and calls `report_client_cert_expiry` on a throwaway manager."""
        if not self._has_https_destination():
            return
        if not self._memory.claim_expiry_check(CLIENT_CERT_EXPIRY_CHECK_EVERY, time.monotonic()):
            return
        self.report_client_cert_expiry()

    def report_client_cert_expiry(self) -> None:
        """The expiry reminder itself, with no claim (the caller decided it is
        due): see `_check_client_cert_expiry`. Never raises."""
        try:
            cert_path, _ = self._identity_paths()
            if not os.path.lexists(cert_path):
                return  # not enrolled yet: `_client_identity` says so
            problem = ident.client_cert_expiry_problem(
                cert_path, ident.client_cert_not_after(cert_path)
            )
        except Exception as e:  # advisory only
            logger.debug(f"client certificate expiry check failed: {e}")
            return
        if problem is not None:
            level, message = problem
            logger.log(level, message)

    def _negotiate_zfs_skipped(self, dsi: DatasetInfo, destination: str, base_url: str) -> None:
        """Negotiate was answered `zfs_timeout` (S-14): WARNING once per
        episode, and the `RESUME_TOKEN_SKIP_ERROR_AFTER`th consecutive skipped
        cycle logs an ERROR, once until `_negotiate_zfs_ok`, so a server whose
        pool stays hung is not an eternal INFO."""
        key = (dsi.name, destination)
        self._call_note[key] = f"negotiate skipped: {base_url}'s zfs did not answer in time"
        self._log_warning_once(
            (f"{dsi.name} [negotiate zfs_timeout]", destination),
            f"server {base_url} could not answer the negotiation of {dsi.name} in time "
            "(its zfs did not answer); skipping this cycle",
        )
        count = self._memory.incr(NEGOTIATE_ZFS_SKIPS, key)
        if count >= RESUME_TOKEN_SKIP_ERROR_AFTER and self._memory.note(NEGOTIATE_ZFS_ERROR, key):
            logger.error(
                f"{dsi.name} -> {destination}: the server's zfs has not answered the "
                f"negotiation for the last {count} cycles in a row; nothing is sent for "
                "this dataset until it does, so the destination is falling behind. Check "
                "the server's pool (a hung pool leaves `zfs` processes in uninterruptible "
                "sleep)"
            )

    def _negotiate_zfs_ok(self, dsi: DatasetInfo, destination: str) -> None:
        key = (dsi.name, destination)
        self._memory.reset(NEGOTIATE_ZFS_SKIPS, key)
        self._forget_refusal((f"{dsi.name} [negotiate zfs_timeout]", destination))
        if self._memory.clear(NEGOTIATE_ZFS_ERROR, key):
            logger.info(f"{dsi.name} -> {destination}: the server answers negotiations again")

    def close(self) -> None:
        """Close every pinned session (idempotent)."""
        for transport in self._transports.values():
            with contextlib.suppress(Exception):
                transport.session.close()
        self._transports.clear()
        self._checked.clear()
        self._checked_retryable.clear()
        self._cached_retryable.clear()

    # ------------------------------------------------- identity and transport

    def _ready_transport(self, destination: str, dest: Destination) -> Optional[_Transport]:
        """The destination's pinned transport if this client may use it now:
        a client id, an enrolled destination, a loadable client identity and
        a `/whoami` (once per cycle) that binds our certificate to our id.
        `None` (logged) means skip the destination -- before any send.

        `_transport_retryable` reports on THIS call only: the destination's
        entry is dropped first and set only from this call's `/whoami` (or its
        cached result), so a `None` from anywhere before it -- no client id, no
        endpoint, an identity or transport failure -- is never retryable."""
        self._checked_retryable.pop(destination, None)
        if self.client_id is None:
            if self._memory.note(NO_CLIENT_ID, 'remote'):
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
            if not checked[1]:
                # A cached failure (per-cycle checks) keeps the retryability its
                # `/whoami` had.
                self._checked_retryable[destination] = (
                    self._cached_retryable.get(destination) == transport.key
                )
            return transport if checked[1] else None
        ok = self._whoami(destination, transport)
        self._checked[destination] = (transport.key, ok)
        retryable = (not ok) and self._whoami_retryable
        self._checked_retryable[destination] = retryable
        if retryable:
            self._cached_retryable[destination] = transport.key
        else:
            self._cached_retryable.pop(destination, None)
        return transport if ok else None

    def _enrolled_endpoint(self, destination: str, dest: Destination) -> Optional[_Endpoint]:
        """The destination's endpoint, or `None` (one ERROR) when it is not
        enrolled: not an https URL, or no pinned server fingerprint. A local
        (`zfs:<dataset>`) destination is refused outright (defence in depth:
        the worker never hands one to this manager)."""
        if getattr(dest, 'is_local', False) is True:
            self._log_refusal_once(
                (None, destination),
                f"destination {destination} is a local destination ({dest.url}); "
                "the remote backup manager does not send to it",
            )
            return None
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
        self._memory.pop(IDENTITY_ERROR, 'remote')
        if previous is not None and previous.fingerprint != loaded.fingerprint:
            self.close()
        # Announced once per fingerprint in the shared memory, not once per
        # manager: a manager built per send loads its own identity every time.
        if self._memory.note_value(IDENTITY_LOADED, 'remote', loaded.fingerprint):
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
        # The next successful load is news again (the same fingerprint included).
        self._memory.pop(IDENTITY_LOADED, 'remote')
        message = f"remote backup skipped: cannot use the client TLS identity: {e}"
        if self._memory.note_value(IDENTITY_ERROR, 'remote', message):
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
                f"{result.get('error') or 'unknown error'}"
                f"{_STREAM_REFUSAL_HINTS.get(result.get('reason'), '')}",
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
        failure (cannot reach the server) at ERROR once per episode (`UNREACHABLE`
        in the shared memory, INFO when it answers again), an unexpected
        answer likewise (`WHOAMI_UNEXPECTED`), the server's
        retryable 503 at INFO. A
        reset or broken pipe is retried once: under TLS 1.3 it can be the
        server rejecting our certificate after the handshake."""
        url = f"{transport.base_url}/whoami"
        resp = None
        self._whoami_retryable = False
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
                message = (
                    f"cannot reach {transport.base_url} (destination {destination}): {e}; "
                    "not sending to it for now"
                )
                # ERROR once per episode in the shared memory (a manager built
                # per send would otherwise log it for every pair), DEBUG while
                # it lasts; the next answer from the server ends the episode.
                if self._memory.note(UNREACHABLE, destination):
                    logger.error(message)
                else:
                    logger.debug(message)
                # Nothing was sent, and the server may simply be down for a while.
                self._whoami_retryable = True
                return False
        if self._memory.clear(UNREACHABLE, destination):
            logger.info(f"{transport.base_url} (destination {destination}) is reachable again")
        if _is_reloading(resp):
            self._whoami_retryable = True
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
            message = (
                f"unexpected /whoami answer from {transport.base_url} (destination "
                f"{destination}): HTTP {resp.status_code}; not sending to it for now"
            )
            # ERROR once per episode in the shared memory (every send builds its
            # own manager, so the old once-per-cycle dedup is gone), DEBUG while
            # it lasts; the next well-formed answer ends the episode.
            if self._memory.note(WHOAMI_UNEXPECTED, destination):
                logger.error(message)
            else:
                logger.debug(message)
            return False
        if self._memory.clear(WHOAMI_UNEXPECTED, destination):
            logger.info(
                f"{transport.base_url} (destination {destination}) answers /whoami normally again"
            )
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
        if self._forget_refusal((None, destination)) is not None:
            logger.info(
                f"server {transport.base_url} (destination {destination}) accepts this "
                f"client again as {bound!r}"
            )
        return True

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
                # The one retryable 503 covers a reload, a busy/unsettled
                # dataset and (S-14) a server `zfs` that did not answer in
                # time; the server's own `reason`/text tells which.
                body = _json_body(resp)
                text = str(body.get('error') or '')
                if body.get('reason') == 'zfs_timeout':
                    self._negotiate_zfs_skipped(dsi, destination, base_url)
                    raise NegotiationFailed(
                        f"server is busy: {text or 'zfs timed out; retry later'}", reloading=True,
                        zfs_timeout=True,
                    )
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
                body = _json_body(resp)
                reason = body.get('error')
                if reason and body.get('reason') == NAME_TOO_LONG_REASON:
                    # Decided by the server before it wrote anything: the
                    # same every cycle, so ERROR once per episode.
                    self._log_refusal_once(
                        (dsi.name, destination),
                        f"server refused {dsi.name}: {reason}; nothing is sent",
                    )
                    raise NegotiationFailed(f"HTTP {resp.status_code}: {reason}")
                if reason:
                    logger.error(
                        f"Negotiation failed for {dsi.name} at {base_url}: "
                        f"HTTP {resp.status_code}: {reason}"
                    )
                    raise NegotiationFailed(f"HTTP {resp.status_code}: {reason}")
            resp.raise_for_status()
            self._negotiate_zfs_ok(dsi, destination)
            body = resp.json()
            # An old server has no name-length check: the copy's name is
            # known from its answer, so a snapshot that could never exist
            # there is refused before the first stream (same ERROR-once
            # handling as the server's own 400). A server that names no copy
            # cannot be checked.
            copy = body.get('server_dataset')
            too_long = (
                receiver.copy_snapshot_name_problem(copy, [s.name for s in snapshots])
                if isinstance(copy, str) and copy else None
            )
            if too_long is not None:
                self._log_refusal_once(
                    (dsi.name, destination),
                    f"{dsi.name} -> {destination}: {too_long}; nothing is sent",
                )
                raise NegotiationFailed(too_long)
            # Only now is the episode over (the check above keeps its entry, so
            # a standing refusal is logged once, not every cycle).
            self._forget_refusal((dsi.name, destination))
            self._forget_refusal((f"{dsi.name} [unsettled]", destination))
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
                if isinstance(e, (
                    requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                    ConnectionError, TimeoutError,
                )):
                    # Nothing was answered (the server is down or unreachable),
                    # as `/whoami`'s "cannot reach": worth retrying soon. An
                    # HTTP error or an unreadable answer is not (`HTTPError` is
                    # an `OSError`, so the test is not `OSError`).
                    self._mark_retryable(dsi.name, destination)
            raise NegotiationFailed(str(e)) from e

    def _stream_step(
        self,
        transport: _Transport,
        client_id: str,
        dsi: DatasetInfo,
        destination: str,
        started: _SendStream,
        after_success: bool = False,
        should_stop: Optional[Callable[[], bool]] = None,
        discard: Tuple[str, ...] = (),
    ) -> str:
        """Send ONE stream (`started`, from `_try_resume` or `_start_fresh`) over
        the pinned websocket and wait for the server's ack (`_push_step` then
        records the anchor and the pin). -> `_OK`; `_STOP` (the server is
        reloading or busy, or this destination was refused: nothing more this
        cycle, no diagnosis); or `_FAILED` (the transfer did not complete).
        The started `zfs send` is reaped on every path.

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
                        # Nothing of the stream was sent: retry soon.
                        self._mark_retryable(dsi.name, destination)
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
                            self._mark_retryable(dsi.name, destination)
                            logger.info(
                                f"stop requested; {dsi.name} -> {destination} continues next cycle"
                            )
                            return _STOP
                        time.sleep(CONNECT_RETRY_SLICE)
                    if should_stop is not None and should_stop():
                        self._mark_retryable(dsi.name, destination)
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
                    self._mark_retryable(dsi.name, destination)
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
                    self._mark_retryable(dsi.name, destination)
                    return _STOP
            else:
                if not entered and _connect_failure_retryable(e):
                    # The connect failed before one byte of the stream was sent
                    # and the failure is the connection's (a server that is
                    # down, or draining for a reload). Any other refusal of the
                    # upgrade (409, 400, 500, ...) is the server's answer, not
                    # a transient: the same request fails the same way.
                    self._mark_retryable(dsi.name, destination)
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

        return _OK

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
                if _json_body(resp).get('reason') == 'zfs_timeout':
                    # S-14: the server's zfs did not answer in time. Not a
                    # reload: the cycle is skipped like any unreadable token
                    # check (counted, so a server that stays stuck is
                    # escalated), never a fresh send.
                    self._log_warning_once(
                        (f"{dsi.name} [zfs_timeout]", destination),
                        f"server {base_url} could not read the resume state of {dsi.name} "
                        "in time (its zfs did not answer); skipping this cycle",
                    )
                    self._token_skipped(dsi, destination, f"{base_url}'s zfs did not answer in time")
                    return 'skip', None
                # Reloading, draining or busy (the retryable 503): not a token
                # problem, so not counted by `_token_skipped`, and worth retrying soon.
                self._mark_retryable(dsi.name, destination)
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
                self._token_skipped(
                    dsi, destination, f"{base_url} answered the resume-token request with HTTP {resp.status_code}"
                )
                return 'skip', None
            body = resp.json()
            if not isinstance(body, dict) or body.get('error'):
                raise ValueError(
                    (body.get('error') if isinstance(body, dict) else None)
                    or "unexpected reply"
                )
            self._forget_refusal((f"{dsi.name} [zfs_timeout]", destination))
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
            self._token_skipped(dsi, destination, f"cannot ask {base_url} for it: {e}")
            return 'skip', None

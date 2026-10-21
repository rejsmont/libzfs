"""Client-side remote backup: pinned mutual-TLS transport and ZFS stream transfer.

Every connection to a destination is TLS 1.3 with this host's client
certificate, and the server is authenticated by the SHA-256 fingerprint
pinned at enrollment (`Destination.server_fingerprint`, plan A1): HTTP through
`identity.pinned_session`, the websocket through `identity.connect_pinned`
handed to `websocket.WebSocket().connect(..., socket=)`. There is no plaintext
path. The client id is the one the supervisor read from the config database
(`daemon.Handoff.client_id`); the server binds it to our certificate and
`/whoami` reports the binding.
"""

import contextlib
import json
import logging
import os
import re
import ssl
from pathlib import Path
from typing import Callable, Dict, Iterator, NamedTuple, Optional, Set, Tuple
from urllib.parse import quote, urlsplit

import requests
import websocket

import libzfseasy as zfs
from libzfseasy.zfs import ResumeTokenError
from zfsbackup import identity as ident
from zfsbackup.backup_manager import DatasetInfo, DatasetManager
from zfsbackup.config import (
    BackupConfig, Destination, RemoteDatasetConfig, _collapse_retention_rules,
    destination_url_problem,
)
from zfsbackup.identity import (
    CLIENT_NAME, Identity, IdentityError, PinMismatch, client_context, connect_pinned,
    is_pin_mismatch, is_tls_alert_rejection, load_identity, pinned_session,
)


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
        checked before any send starts and between a resumed send and its
        follow-up, so a stop or reload never begins a second stream."""
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
        try:
            common_snap_name = self._negotiate(
                transport, client_id, dsi, snapshots, destination
            )
        except NegotiationFailed as e:
            # Never fall through to a send: with no answer we would treat
            # "unknown" as "no common snapshot" and start a full send.
            if e.reloading:
                logger.info(
                    f"server {transport.base_url} is reloading; skipping {dsi.name} this cycle"
                )
            return False

        # Resolve the common snapshot object for incremental send
        common_snap = None
        if common_snap_name:
            common_snap = next((s for s in snapshots if s.name == common_snap_name), None)

        return self._transfer(
            transport, client_id, dsi, latest, common_snap, destination,
            snapshots=snapshots, should_stop=should_stop,
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
        transport = _Transport(endpoint, server_fp, identity.fingerprint, session, ctx)
        self._transports[destination] = transport
        return transport

    # ------------------------------------------------------ refusals and whoami

    def _log_refusal_once(self, key, message: str) -> None:
        if self._refusal_logged.get(key) != message:
            self._refusal_logged[key] = message
            logger.error(message)
        else:
            logger.debug(message)

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
        """The message for an HTTP 401/403 auth refusal, else `None`."""
        where = f"server {transport.base_url} (destination {destination})"
        enroll = f"`zfsbackup-config enroll {destination} --token-file …`"
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
        refusals and a pin mismatch once (ERROR, deduplicated), a transient
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
                raise NegotiationFailed("server is reloading", reloading=True)
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
            resp.raise_for_status()
            self._refusal_logged.pop((dsi.name, destination), None)
            return resp.json().get('common_snapshot')
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
        followup: bool = True,
    ) -> bool:
        base_url = transport.base_url
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
            transport, client_id, dsi, latest, common_snap, destination, snapshots,
            should_stop=should_stop,
        )
        if started is None:
            return False
        stream, mode, inflight_set, sent, base = started
        sent = sent or latest

        # `mode` is required by the server: `resume` only when this stream
        # continues the server's resume token, `fresh` for a new stream (the
        # server then discards any partial receive state). `from` is
        # informational.
        ws_url = (
            f"{transport.endpoint.ws_base}/backup/{_path(client_id)}/{_path(dsi.name)}"
            f"/stream?mode={mode}"
        )
        if base is not None:
            ws_url += f"&from={quote(base.name, safe='')}"

        ws = websocket.WebSocket()
        tls_sock = None
        entered = False
        failed = False
        sending = False
        try:
            # TCP + TLS handshake + pin check BEFORE any websocket byte; a
            # wrong server raises `PinMismatch` (not an OSError) and, like any
            # connect failure, reaps the already started `zfs send` below
            # (`entered` is still False). The websocket then runs over that
            # very socket (S8: the URL's host and `sslopt` are not used).
            tls_sock = connect_pinned(
                transport.endpoint.host, transport.endpoint.port, transport.ctx,
                transport.server_fp, WS_CONNECT_TIMEOUT,
            )
            ws.connect(ws_url, socket=tls_sock, timeout=WS_CONNECT_TIMEOUT)
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
            # Not trusted / id mismatch on the upgrade (401/403), a TLS alert
            # (S10: a raw `ssl.SSLError` escapes `connect()`, not a
            # WebSocketException) or a pin mismatch: a destination-level
            # refusal, logged once, and the destination is skipped for the
            # rest of this cycle.
            auth = self._auth_refusal(destination, transport, exc=e)
            if auth is not None:
                self._refuse_destination(destination, transport, auth)
                return False
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
                transport, client_id, dsi, snapshots, destination, should_stop
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
        self, transport, client_id, dsi, snapshots, destination, should_stop
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
            common_name = self._negotiate(transport, client_id, dsi, snapshots, destination)
        except NegotiationFailed as e:
            if e.reloading:
                logger.info(
                    f"server {transport.base_url} is reloading; {dsi.name} continues next cycle"
                )
            return False
        common_snap = None
        if common_name:
            common_snap = next((s for s in snapshots if s.name == common_name), None)
        return self._transfer(
            transport, client_id, dsi, latest, common_snap, destination,
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
        self, transport, client_id, dsi, latest, common_snap, destination, snapshots=None,
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
        status, token = self._fetch_resume_token(transport, client_id, dsi, destination)
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

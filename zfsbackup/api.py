"""Flask API for inspecting daemon configuration and dataset snapshots.

Served over HTTPS only (TLS/mTLS plan M9): `workers.ApiWorker` runs the app
behind `workers._api_server_class()`, which does every TLS handshake on the
connection's own thread with the context `TrustGateway.current_context()`
returns at that moment. `TrustGateway` is this module's view of the pinned
client certificates (`zfsbackup.config.store.trust`): the TLS layer uses its
context, and route authentication uses its pin map.

**Authentication (plan M10, D3, D5, D7).** Every endpoint except `/health`
and `/enroll` needs a client certificate pinned in the gateway. The client id
a request acts as is the one that certificate is *bound* to (`PinInfo.
client_id`, `flask.g.client_id`) and nothing else: the `<client_id>` in a
`/backup/<client_id>/...` URL is compared with the binding (403 on a
mismatch), never used. See `create_app`.
"""

import base64
import binascii
import contextlib
import functools
import json
import logging
import os
import ssl
import threading
import time
import types
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping, NamedTuple, Optional

from flask import Flask, after_this_request, g, jsonify, request
from flask_sock import Sock
from simple_websocket import ConnectionClosed
from werkzeug.exceptions import RequestEntityTooLarge

import libzfseasy as zfs
from libzfseasy.types import Dataset, Filesystem

from zfsbackup.backup_manager import (
    DatasetManager,
    PROP_CONFIG, PROP_CLIENT_ID, PROP_SOURCE_DATASET,
    read_zfsbackup_properties,
)
from zfsbackup.config import BackupConfig, client_id_problem, normalize_fingerprint
from zfsbackup.identity import (
    IdentityChanged,
    IdentityError,
    check_client_cert,
    enrollment_payload,
    fingerprint_der,
    parse_token,
    rotation_payload,
    secret_hash,
    verify_payload,
)
from zfsbackup.remote import WS_IDLE_TIMEOUT

logger = logging.getLogger(__name__)

#: Longest `StreamGate.wait_idle` sleeps before re-checking its abort callback.
_GATE_POLL = 0.5

#: The refusal body for `negotiate`/`stream` while the API worker is draining
#: for a configuration reload (HTTP 503). `retry` tells a client this is
#: transient: the replacement worker is up within moments of the drain ending.
RELOADING_ERROR = {'error': 'server is reloading; retry later', 'retry': True}

#: The values of `/stream`'s required `?mode=` query parameter. `fresh`: the
#: client is sending a new stream and any partial receive state on the server
#: dataset is discarded first. `resume`: the client is continuing the transfer
#: the server's resume token describes; nothing is discarded.
STREAM_MODES = ('fresh', 'resume')

#: The refusal body for `stream` while another stream (or its abort) for the
#: SAME server dataset is running in this API worker. `retry` marks it as a
#: transient skip for the client, exactly like `RELOADING_ERROR`: never a
#: reason to fall back to a fresh send.
BUSY_ERROR = {'error': 'busy', 'retry': True}

#: Substrings of `zfs receive -A`'s stderr that mean "nothing to abort": rc 1
#: "'<ds>' does not have any resumable receive state to abort"; rc 2 "cannot
#: open '<ds>': dataset does not exist". In this flow the server dataset
#: always exists first (negotiate or `_receive_stream` creates it), so `-A`
#: destroys only the saved `<ds>/%recv` state and rc 1 is the normal "already
#: clean". rc 2 only arises for a dataset that a FULL receive created
#: outside this flow (a legacy or manual one), which the abort itself
#: destroys.
_ABORT_ALREADY_CLEAN = (
    'does not have any resumable receive state',
    'dataset does not exist',
)

#: The endpoints that need no pinned client certificate (plan D7): the
#: liveness probe, and enrollment, which is how a certificate gets pinned.
#: `/enroll` is reached WITHOUT a client certificate: the TLS layer refuses
#: an unpinned one at the handshake (`identity.server_context`).
UNAUTHENTICATED_ENDPOINTS = frozenset({'health', 'enroll'})

#: 401: the connection presented no client certificate.
CERT_REQUIRED_ERROR = {'error': 'client certificate required'}

#: 403: the certificate is not pinned (any more), or its pin has expired.
NOT_TRUSTED_ERROR = {'error': 'client not trusted'}

#: 403 for every `/enroll` refusal the token decides (none, spent, expired,
#: for another server, wrong secret, scoped to another id): identical on
#: purpose, so a caller learns nothing about which check failed.
ENROLL_REFUSED_ERROR = {'error': 'enrollment refused', 'reason': 'refused'}

#: 403 for every `/rotate` refusal the store decides.
ROTATE_REFUSED_ERROR = {'error': 'rotation refused', 'reason': 'refused'}

#: 503 + retry: a trust write could not get the config database's write lock
#: in time (`ConfigBusy`); nothing was written.
TRUST_BUSY_ERROR = {
    'error': 'the server is busy; nothing was recorded, retry later',
    'reason': 'busy',
    'retry': True,
}

#: 503, no retry: the trust tables cannot be written (wrong schema, an
#: unusable database). The server operator has to act; details are in the
#: server's log, never in the response.
TRUST_UNAVAILABLE_ERROR = {
    'error': 'the server cannot record client trust right now; '
             'the server operator must check its log',
    'reason': 'unavailable',
}

#: The only `version` `/enroll` and `/rotate` accept.
TRUST_WIRE_VERSION = 1

#: `app.config['MAX_CONTENT_LENGTH']`: the cap on every request body.
MAX_REQUEST_BYTES = 1024 * 1024

#: The tighter cap on `/enroll` and `/rotate` bodies (a certificate of at most
#: 16 KiB, a signature, a token and a label).
MAX_TRUST_REQUEST_BYTES = 64 * 1024

#: Longest `/enroll` `label` accepted (characters).
LABEL_MAX_CHARS = 128


class _BadDatasetName(ValueError):
    """The server dataset derived from a request is not a valid filesystem
    name (`create_app._server_dataset`); answered with 400."""


class StreamGate:
    """Counts in-flight inbound streams and refuses new ones while draining.

    The API worker hosts the server side of `zfs receive -s`; cycling it in the
    middle of a receive would tear an inbound stream. On a reload the worker
    calls `start_draining()` (new streams are refused with a retryable 503,
    read-only endpoints keep serving) and then `wait_idle()`s for the streams
    already running to finish before it exits.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._active = 0
        self._draining = False

    def begin(self) -> bool:
        """Register a stream; `False` (nothing registered) while draining."""
        with self._cond:
            if self._draining:
                return False
            self._active += 1
            return True

    def end(self) -> None:
        with self._cond:
            if self._active > 0:
                self._active -= 1
            self._cond.notify_all()

    def start_draining(self) -> None:
        with self._cond:
            self._draining = True
            self._cond.notify_all()

    @property
    def draining(self) -> bool:
        with self._cond:
            return self._draining

    @property
    def active(self) -> int:
        with self._cond:
            return self._active

    def wait_idle(
        self, timeout: Optional[float], abort: Optional[Callable[[], bool]] = None
    ) -> bool:
        """Wait until no stream is active -> `True`; `False` on `timeout`
        (`None` waits indefinitely) or when `abort()` (optional) turns true. Sleeps in
        slices of at most 0.5s so `abort` is re-evaluated promptly (it is
        called WITHOUT the gate's lock held)."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._cond:
                if self._active == 0:
                    return True
                remaining = _GATE_POLL if deadline is None else min(
                    _GATE_POLL, deadline - time.monotonic()
                )
                if remaining > 0:
                    self._cond.wait(timeout=remaining)
                    if self._active == 0:
                        return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            if abort is not None and abort():
                return False


#: How often `ApiWorker` calls `TrustGateway.poll()` (seconds): the bound on
#: how long a revoked or newly pinned client certificate takes to reach new
#: connections without a restart.
TRUST_POLL_SECONDS = 2.0

#: How long a request thread waits for another thread's `store.trust` call
#: before giving up with `ConfigBusy` (503 + retry; a retirement retries on a
#: later request). Store calls take milliseconds, or at most the store's own
#: busy timeouts (5 s each) when the database is locked.
STORE_CALL_WAIT = 15.0

#: How long a request thread's trust refresh (`TrustGateway.refresh(timeout=
#: ...)`) waits for a running refresh and for `_STORE_LOCK`, and is also the
#: deadline of the inline context rebuild's identity-lock wait. Tried before
#: the response, and once more right after it is sent; anything left the trust
#: poll applies (a failed attempt marks the gateway dirty for it).
REQUEST_REFRESH_WAIT = 2.0

#: How long a context rebuild waits for the shared identity lock
#: (`identity.server_context(lock_timeout=...)`). Short: the lock is shared
#: with the CLI (`enroll`, `identity rotate` hold it exclusively), and a
#: rebuild that times out is simply retried by the next poll.
GATEWAY_LOCK_TIMEOUT = 5.0

#: Serialises every `store.trust` call this process makes (`_store_call`).
_STORE_LOCK = threading.Lock()


def _reinit_store_lock_after_fork() -> None:
    """A fork while some thread holds `_STORE_LOCK` would leave the child a
    lock nobody can release."""
    global _STORE_LOCK
    _STORE_LOCK = threading.Lock()


if hasattr(os, "register_at_fork"):  # POSIX only
    os.register_at_fork(after_in_child=_reinit_store_lock_after_fork)


def _store_call(fn, *args, wait: Optional[float] = None, **kwargs):
    """Call the `store.trust` function `fn` holding `_STORE_LOCK`.

    One store call at a time per process: the store's preflight
    (`check_config_db`) opens and closes the database file, and closing ANY
    descriptor of a file releases every POSIX (`fcntl`) lock the process
    holds on it -- including the SQLite SHARED/RESERVED lock of another
    thread's open transaction, which SQLite would then believe it still
    holds. The API worker is the only worker that opens the database, and
    every one of its opens goes through here (`TrustGateway`).

    `wait=None` waits indefinitely (the trust-poll thread); otherwise a wait
    longer than `wait` seconds raises `ConfigBusy`."""
    lock = _acquire_store_lock(wait)
    if lock is None:
        from zfsbackup.config.store import ConfigBusy
        raise ConfigBusy(
            "another trust operation of this API worker is still using the "
            "config database; nothing was written. Retry in a moment."
        )
    try:
        return fn(*args, **kwargs)
    finally:
        lock.release()


def _acquire_store_lock(wait: Optional[float]) -> Optional[threading.Lock]:
    """Take `_STORE_LOCK`; -> the lock taken (release THAT one: a fork may
    replace the global), or `None` if it was not free within `wait` seconds
    (`None`: wait indefinitely; `<= 0`: do not wait)."""
    lock = _STORE_LOCK
    if wait is None:
        lock.acquire()
        return lock
    if wait <= 0:
        return lock if lock.acquire(blocking=False) else None
    return lock if lock.acquire(timeout=wait) else None


class PinInfo(NamedTuple):
    """What the server knows about one pinned client certificate."""

    client_id: str
    #: The certificate's expiry, POSIX seconds.
    not_after: float
    #: The fingerprint this pin succeeds by rotation while that one is still
    #: pinned (M14b retires it on this pin's first use), else `None`.
    predecessor: Optional[str]


@dataclass(frozen=True)
class TrustView:
    """One immutable state of the server's trust: the pins read at `key`
    and the TLS context built from exactly those pins.

    `key` is `None` before the first successful read: then `pins` is empty
    and `context` trusts no client certificate (fail closed). A view is never
    mutated; `TrustGateway` replaces it whole, by one attribute assignment,
    so a reader that took a view sees a consistent key, pin map and context.
    """

    key: Optional[tuple]
    pins: Mapping[str, PinInfo]
    context: ssl.SSLContext


class TrustGateway:
    """The API worker's view of the pinned client certificates (plan D1, D6).

    Holds one `TrustView` and swaps it atomically:

    - `current_context()` is what the API server wraps each new connection
      with (`workers._api_server_class`), on that connection's own thread. A
      connection already established keeps the context it was accepted with;
      revocation reaches new connections only (streams in flight finish).
    - `poll()` is called by `ApiWorker`'s trust-poll thread every
      `TRUST_POLL_SECONDS`. It reads the cheap trust key
      (`store.trust.read_trust_key`) and only when that changed reads the
      pins (`read_trust`) and rebuilds the context. It never waits for a
      running `refresh()`.
    - `refresh()` reads the pins unconditionally and installs them (see its
      docstring). Request threads call it -- bounded, `timeout=
      REQUEST_REFRESH_WAIT` -- after an enrollment, a rotation or a
      predecessor retirement committed: before the response when it can
      run in time, else right after the response is sent
      (`create_app._refresh_after_commit`), so the change normally works on
      the client's next connection.
    - `lookup(fingerprint)` is the pin map for route authentication (M10).
    - `enroll_client()`, `rotate_client()` and `retire_predecessor()` are the
      store's three API trust writers (`store.trust`) bound to this
      gateway's database. They only write; the caller `refresh()`es after a
      write that changed the pins.
    - `token_precheck()` is the store's read-only enrollment filter
      (`store.trust.token_precheck`): `/enroll` asks it before any `zfs`
      call. It never writes; `enroll_client` stays the authority.

    **Fails closed.** Until the first successful read nobody is trusted: the
    context has no pins (a client certificate fails the handshake; a client
    without one completes it and is refused by the app). A failed read --
    the database locked, replaced by one with an older schema, unreadable --
    keeps the last good view and logs once per distinct error (WARNING; INFO
    when it clears). A pin whose PEM cannot be loaded is left out of both the
    context and the pin map (ERROR, once per fingerprint), never allowed to
    make the whole set unusable.

    Every store call builds its own engine (`store.trust`: unpooled, never
    cached), so `poll()` and `refresh()` are safe from any thread. All of
    them -- reads and writes, from the poll thread and request threads --
    are also serialised by one process-wide lock (`_store_call`): the
    store's preflight opens and closes the database file, and closing ANY
    descriptor of a file drops every POSIX lock this process holds on it,
    including another thread's live SQLite lock. `_lock` serialises the
    read-and-swap, so reads are installed in the order they were made (an
    older read can never overwrite a newer view and re-trust a just-revoked
    pin). The server identity is the one loaded at worker start: if its
    files change on disk, a rebuild fails (`server_context` raises
    `IdentityChanged`), revocations still apply and -- once `peek_identity`
    confirms that the certificate on disk really has another fingerprint --
    `restart_reason` asks the worker to restart. Any other rebuild failure
    (the identity lock held by the CLI past `GATEWAY_LOCK_TIMEOUT`, EMFILE,
    an unreadable file) only degrades the view; the poll retries it.
    `current_context()` and `lookup()` take no lock.
    """

    def __init__(
        self,
        identity,
        resolved,
        *,
        read_trust_key: Optional[Callable] = None,
        read_trust: Optional[Callable] = None,
        context_factory: Optional[Callable] = None,
        enroll_client: Optional[Callable] = None,
        rotate_client: Optional[Callable] = None,
        retire_predecessor: Optional[Callable] = None,
        token_precheck: Optional[Callable] = None,
    ) -> None:
        """`identity` is the loaded server `identity.Identity`; `resolved`
        the `ResolvedConfigPath` of the config database. The callables are
        seams for tests; the defaults are `store.trust.read_trust_key`,
        `store.trust.read_trust`, `identity.server_context` and
        `store.trust.enroll_client` / `rotate_client` / `retire_predecessor` /
        `token_precheck` (all through the `zfsbackup.config.store` package).
        With the default factory, rebuilds wait at most
        `GATEWAY_LOCK_TIMEOUT` for the identity lock; the initial context
        waits the identity module's default. A `context_factory` is called
        as `(identity, pems)`.
        Building the initial (empty) context raises `IdentityError` if the
        server identity cannot be loaded into `ssl` -- the caller
        (`ApiWorker`) then exits."""
        store = (
            read_trust_key, read_trust, enroll_client, rotate_client,
            retire_predecessor, token_precheck,
        )
        if any(f is None for f in store):
            from zfsbackup.config import store as _store
            read_trust_key = read_trust_key or _store.read_trust_key
            read_trust = read_trust or _store.read_trust
            enroll_client = enroll_client or _store.enroll_client
            rotate_client = rotate_client or _store.rotate_client
            retire_predecessor = retire_predecessor or _store.retire_predecessor
            token_precheck = token_precheck or _store.token_precheck
        server_context_fn = None
        if context_factory is None:
            from zfsbackup.identity import server_context
            initial_factory = server_context
            server_context_fn = server_context
            context_factory = self._default_factory
        else:
            initial_factory = context_factory
        #: `identity.server_context` when the default factory is in use (its
        #: lock wait is then budgeted per build, see `_build`), else `None`.
        self._server_context: Optional[Callable] = server_context_fn
        #: The next default-factory call's `lock_timeout` (`None`: the time
        #: left until `_factory_end`) and the end of the whole build's budget.
        self._factory_first: Optional[float] = GATEWAY_LOCK_TIMEOUT
        self._factory_end = 0.0
        self._identity = identity
        self._resolved = resolved
        self._read_trust_key = read_trust_key
        self._read_trust = read_trust
        self._context_factory = context_factory
        self._enroll_client = enroll_client
        self._rotate_client = rotate_client
        self._retire_predecessor = retire_predecessor
        self._token_precheck = token_precheck
        self._lock = threading.Lock()
        #: Message of the read failure currently in effect (logged once).
        self._problem: Optional[str] = None
        #: Fingerprints already reported as unloadable (logged once each).
        self._bad_pins: set = set()
        #: The installed view kept the old context after a failed rebuild.
        self._degraded = False
        #: A bounded (request-thread) refresh failed or did not run, or a read
        #: failed: `poll()` re-reads whatever the trust key says, so a view
        #: installed by a read that predates a commit cannot stay stale.
        self._dirty = False
        #: Set once when the server identity on disk changed (`refresh`).
        self._restart_reason: Optional[str] = None
        self._view = TrustView(
            key=None,
            pins=types.MappingProxyType({}),
            context=initial_factory(identity, []),
        )

    @property
    def view(self) -> TrustView:
        return self._view

    @property
    def server_fingerprint(self) -> str:
        return self._identity.fingerprint

    def current_context(self) -> ssl.SSLContext:
        """The context for the next connection. Lock-free: one attribute read."""
        return self._view.context

    def lookup(self, fingerprint: str) -> Optional[PinInfo]:
        """The pin for a client certificate's fingerprint, or `None`."""
        return self._view.pins.get(fingerprint)

    # The API trust writers (`store.trust`), on this gateway's database. Each
    # opens its own unpooled engine and runs under `_store_call`'s lock, so
    # they are safe from any request thread; none is called while the caller
    # holds a store session. They raise what the store raises (`ConfigBusy`,
    # `SchemaError`, `ConfigPathError`, ...): the routes map those to
    # responses. Waiting longer than `STORE_CALL_WAIT` for another thread's
    # store call is `ConfigBusy`.

    def enroll_client(self, **kwargs):
        """`store.trust.enroll_client(resolved, **kwargs)` -> `EnrollOutcome`."""
        return _store_call(self._enroll_client, self._resolved, wait=STORE_CALL_WAIT, **kwargs)

    def rotate_client(self, **kwargs):
        """`store.trust.rotate_client(resolved, **kwargs)` -> `RotateOutcome`."""
        return _store_call(self._rotate_client, self._resolved, wait=STORE_CALL_WAIT, **kwargs)

    def retire_predecessor(self, fingerprint: str) -> bool:
        """`store.trust.retire_predecessor(resolved, fingerprint)`."""
        return _store_call(
            self._retire_predecessor, self._resolved, fingerprint, wait=STORE_CALL_WAIT
        )

    def token_precheck(self, **kwargs) -> bool:
        """`store.trust.token_precheck(resolved, **kwargs)` -> `bool`: the
        read-only filter `/enroll` runs before it spends a `zfs` call. Takes
        no write lock and writes nothing."""
        return _store_call(self._token_precheck, self._resolved, wait=STORE_CALL_WAIT, **kwargs)

    def poll(self) -> bool:
        """Re-read the pins if the trust key changed (or the installed view
        is degraded, see `refresh`, or marked dirty by a request-thread
        refresh that did not complete or a failed read); -> `False` when nothing was read or the
        read failed, else `refresh()`'s result. Never raises (a failure is
        logged, the view kept).

        Called by `ApiWorker`'s trust-poll thread every `TRUST_POLL_SECONDS`,
        never by the main loop. **Never waits for a refresh:** if one is
        running (a request thread after an enrollment), this poll is skipped
        -- that refresh reads the newest state anyway. The polled key only
        decides whether to read; the view gets the key of the read itself."""
        if not self._lock.acquire(blocking=False):
            return False
        try:
            try:
                key = tuple(_store_call(self._read_trust_key, self._resolved))
            except Exception as exc:
                self._note_problem(exc)
                return False
            if key == self._view.key and not self._degraded and not self._dirty:
                self._note_problem(None)
                return False
            return self._refresh_locked()
        finally:
            self._lock.release()

    def refresh(self, *, timeout: Optional[float] = None) -> bool:
        """Read the pins now and install them; -> `True` when the installed
        view is this read's (installed now, or already current), `False` on a
        failure or when it did not run. Never raises.

        `timeout=None` (worker start) waits as long as it takes for a running
        refresh or poll and for the store lock (`_STORE_LOCK`). Request
        threads pass `timeout` (`REQUEST_REFRESH_WAIT`): both waits share
        that budget, and if either is not granted in time nothing is read,
        nothing is logged above DEBUG and `False` is returned -- the trust
        poll applies the change (see below). The read itself is bounded by
        the store's own busy timeout (5 s, not by `timeout`). The inline
        context rebuild gets only what is left of `timeout` as its identity
        lock wait (`lock_timeout`, at most `GATEWAY_LOCK_TIMEOUT`; `0`: one
        try), and the whole rebuild -- the fallback that drops an unusable
        PEM included -- never waits longer than that, so a request thread
        overruns `timeout` only by the store read. `timeout=None` (worker
        start, poll) rebuilds with up to `GATEWAY_LOCK_TIMEOUT` in total.

        **A request-thread refresh that fails or does not run marks the
        gateway dirty:** the next `poll()` then re-reads unconditionally,
        even when the trust key it polls equals the installed view's.
        The mark is cleared when a read starts (and set again if that read
        fails), so a mark set meanwhile by another thread is never lost.

        The read *and* the swap happen under `_lock`, so reads are installed
        in the order they were made and the installed view is always the
        most recent read. A read whose key differs from the installed one is
        installed, whatever the direction: a different `store_id` means the
        database file was replaced; a LOWER generation in the same store
        means the database was rolled back (restored from a backup) -- it is
        the truth now, so it is installed and a WARNING logged. (Refusing it
        would hide every later revocation until the generation caught up.)

        **A failed rebuild fails closed for removals.** When the context
        cannot be rebuilt -- an `IdentityError` (the server identity's files
        changed, cannot be read, or the identity lock was not granted in
        `GATEWAY_LOCK_TIMEOUT`), or `ssl` rejecting the pin set as a whole --
        the old context stays (new pins wait) but a view is still installed
        with the new key and the old pins MINUS every pin the read no longer
        has; the pins kept take their `client_id`, `not_after` and
        `predecessor` from the new read. The TLS layer may then still
        accept a just-revoked certificate, but route authentication
        (`lookup`) refuses it. The view is marked degraded, so `poll()`
        keeps retrying the rebuild; each distinct error is logged once.
        Only `IdentityChanged`, confirmed by `identity.peek_identity` (the
        certificate on disk has a fingerprint other than the loaded one),
        sets `restart_reason`: `ApiWorker` drains and exits 1 so that the
        supervisor restarts it with the identity now on disk. A lock timeout
        or an EMFILE never does -- a restart would not help, and each one
        counts toward the supervisor's restart limit."""
        if timeout is None:
            with self._lock:
                return self._refresh_locked()
        deadline = time.monotonic() + max(0.0, timeout)
        acquired = (
            self._lock.acquire(timeout=timeout) if timeout > 0
            else self._lock.acquire(blocking=False)
        )
        if not acquired:
            self._dirty = True
            logger.debug("trust refresh skipped: another refresh is running; "
                         "the trust poll applies the change")
            return False
        try:
            ok = self._refresh_locked(deadline=deadline)
        finally:
            self._lock.release()
        if not ok:
            self._dirty = True
        return ok

    @property
    def restart_reason(self) -> Optional[str]:
        """Why this worker should be restarted (the server identity on disk
        changed under it), or `None`."""
        return self._restart_reason

    def _refresh_locked(self, deadline: Optional[float] = None) -> bool:
        """`refresh()` holding `_lock`. `deadline` (`time.monotonic()` based;
        `None`: unbounded) bounds the wait for `_STORE_LOCK` and the identity
        lock wait of the rebuild; not getting the store lock is no problem to
        report, just a skipped refresh."""
        wait = None if deadline is None else max(0.0, deadline - time.monotonic())
        store_lock = _acquire_store_lock(wait)
        if store_lock is None:
            logger.debug("trust refresh skipped: the config database is in use by "
                         "another trust call; the trust poll applies the change")
            return False
        # Cleared before the read: a mark another thread sets while it runs
        # (its commit may postdate this read) survives it.
        self._dirty = False
        try:
            snapshot = self._read_trust(self._resolved)
        except Exception as exc:
            self._dirty = True
            self._note_problem(exc)
            return False
        finally:
            store_lock.release()
        old = self._view
        key = tuple(snapshot.key)
        if key == old.key and not self._degraded:
            self._note_problem(None)
            return True
        if old.key is not None and key[0] == old.key[0] and key[1] < old.key[1]:
            logger.warning(
                "client trust generation went back from %s to %s in the same "
                "database (restored from a backup?); applying it",
                old.key[1], key[1],
            )
        try:
            view = self._build(snapshot, deadline)
        except Exception as exc:
            # Fingerprints present in both the old view and this read keep
            # their place (the old context still trusts them), but take what
            # the read says about them: the binding, expiry and predecessor
            # of the NEW snapshot. Nothing new is trusted at the TLS layer.
            live = {
                p.fingerprint: PinInfo(p.client_id, float(p.not_after), p.predecessor)
                for p in snapshot.pins
            }
            self._view = TrustView(
                key=key,
                pins=types.MappingProxyType(
                    {fp: live[fp] for fp in old.pins if fp in live}
                ),
                context=old.context,
            )
            self._degraded = True
            changed_to = (
                self._changed_on_disk(exc) if isinstance(exc, IdentityChanged) else None
            )
            if changed_to is not None:
                if self._restart_reason is None:
                    self._restart_reason = (
                        f"the server TLS identity on disk ({changed_to}) no longer "
                        f"matches the one this API worker started with "
                        f"({self._identity.fingerprint}): {exc}"
                    )
                exc = IdentityError(
                    f"{exc}; revocations still apply, new pins wait until the API "
                    "worker restarts (it restarts once its in-flight receives finish)"
                )
            elif isinstance(exc, IdentityError):
                exc = IdentityError(
                    f"{exc}; revocations still apply, new pins wait; retrying every "
                    f"{TRUST_POLL_SECONDS:g} s"
                )
            self._note_problem(exc, action="apply")
            removed = set(old.pins) - set(live)
            if removed:
                logger.warning(
                    "client trust generation %s: %d revoked certificate(s) refused "
                    "by the API, though the TLS context could not be rebuilt",
                    key[1], len(removed),
                )
            return False
        self._view = view
        self._degraded = False
        self._note_problem(None)
        if old.key != view.key or set(old.pins) != set(view.pins):
            logger.info(
                "client trust generation %s: %d pinned certificate(s) for %d client(s)",
                view.key[1], len(view.pins),
                len({p.client_id for p in view.pins.values()}),
            )
        return True

    def _changed_on_disk(self, exc: IdentityChanged) -> Optional[str]:
        """The fingerprint of the server certificate now on disk if it really
        differs from the identity this worker loaded, else `None` (the same
        certificate -- only its key no longer matches --, no certificate, or
        it cannot be read right now: none of which a restart would cure).

        A generated identity (`<db>.tls/server.crt`, always the case for
        `workers._server_identity`) is re-read with `identity.peek_identity`;
        for any other path the fingerprint `_load_chain` itself found
        (`exc.actual`) is the evidence."""
        from zfsbackup import identity as ident
        loaded = self._identity
        cert = Path(loaded.cert_path)
        if cert == ident.identity_paths(cert.parent, ident.SERVER_NAME)[0]:
            try:
                info = ident.peek_identity(cert.parent, ident.SERVER_NAME)
            except (IdentityError, OSError) as e:
                logger.debug("cannot re-read the server certificate %s: %s", cert, e)
                return None
            actual = info.fingerprint if info is not None else None
        else:
            actual = exc.actual
        if actual is None or actual == loaded.fingerprint:
            return None
        return actual

    def _default_factory(self, identity, pems) -> ssl.SSLContext:
        """The default context factory: `identity.server_context` with the
        identity-lock wait `_build` budgeted -- the full budget for the first
        call, what is left of it for the fallback call -- so one build never
        waits twice."""
        first, self._factory_first = self._factory_first, None
        timeout = (
            first if first is not None
            else max(0.0, self._factory_end - time.monotonic())
        )
        return self._server_context(identity, pems, lock_timeout=timeout)

    def _build(self, snapshot, deadline: Optional[float] = None) -> TrustView:
        """A `TrustView` for a `store.trust.TrustSnapshot`. The key is the
        snapshot's own (`snapshot.key`), never the polled one: a write
        between the poll and the read is part of what is installed.

        With the default factory the identity lock wait of the whole build
        is `GATEWAY_LOCK_TIMEOUT`, or what is left until `deadline`
        (`time.monotonic()` based) if less. A custom `context_factory` is
        called as is."""
        pins = list(snapshot.pins)
        started = time.monotonic()
        budget = GATEWAY_LOCK_TIMEOUT
        if deadline is not None:
            budget = min(budget, max(0.0, deadline - started))
        self._factory_first = budget
        self._factory_end = started + budget
        try:
            context = self._context_factory(self._identity, [p.cert_pem for p in pins])
        except ValueError:
            # One unusable PEM must not leave every client locked out (or
            # the old set in force forever): find it, drop it, report it.
            pins = [p for p in pins if self._loadable(p)]
            context = self._context_factory(self._identity, [p.cert_pem for p in pins])
        return TrustView(
            key=tuple(snapshot.key),
            pins=types.MappingProxyType({
                p.fingerprint: PinInfo(p.client_id, float(p.not_after), p.predecessor)
                for p in pins
            }),
            context=context,
        )

    def _loadable(self, pin) -> bool:
        try:
            ssl.create_default_context(cadata=pin.cert_pem)
            return True
        except (ssl.SSLError, ValueError) as exc:
            if pin.fingerprint not in self._bad_pins:
                self._bad_pins.add(pin.fingerprint)
                logger.error(
                    "pinned certificate %s of client %s cannot be loaded (%s); "
                    "it is not trusted. Revoke it and re-enroll the client",
                    pin.fingerprint, pin.client_id, exc,
                )
            return False

    def _note_problem(self, exc: Optional[Exception], action: str = "read") -> None:
        if exc is None:
            if self._problem is not None:
                self._problem = None
                logger.info("client trust can be read again")
            return
        message = f"cannot {action} the trusted client certificates: {type(exc).__name__}: {exc}"
        if message != self._problem:
            self._problem = message
            state = (
                "nobody is trusted yet" if self._view.key is None
                else f"keeping trust generation {self._view.key[1]}"
            )
            logger.warning(f"{message} ({state})")
        else:
            logger.debug("trust read still failing: %s", message)


class _RepeatFilter(logging.Filter):
    """Drop a WARNING-or-lower record whose text was already emitted within
    `interval` seconds. The config store's preflight warns on every open
    (e.g. a sidecar with the wrong group), and the trust poll opens the
    database every `TRUST_POLL_SECONDS`; without this the same line would
    repeat every 2 s. ERRORs always pass."""

    def __init__(self, interval: float = 3600.0) -> None:
        super().__init__()
        self._interval = interval
        self._seen: dict = {}
        self._guard = threading.Lock()

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.ERROR:
            return True
        try:
            text = record.getMessage()
        except Exception:
            return True
        now = time.monotonic()
        with self._guard:
            last = self._seen.get(text)
            if last is not None and now - last < self._interval:
                return False
            if len(self._seen) > 256:
                self._seen.clear()
            self._seen[text] = now
        return True


#: Loggers of the config store that may warn on every open.
_STORE_LOGGERS = ('zfsbackup.config.store.paths', 'zfsbackup.config.store.db')


def dedupe_store_warnings() -> None:
    """Install `_RepeatFilter` on the config store's loggers, once per
    process. Called by `ApiWorker.run` (the only worker that opens the
    database, for the trust poll)."""
    for name in _STORE_LOGGERS:
        lg = logging.getLogger(name)
        if not any(isinstance(f, _RepeatFilter) for f in lg.filters):
            lg.addFilter(_RepeatFilter())


def create_app(
    config: BackupConfig,
    trust: TrustGateway,
    server_fingerprint: str,
    *,
    clock: Callable[[], float] = time.time,
) -> Flask:
    """The API's Flask app. There is no unauthenticated mode.

    `trust` is the API worker's `TrustGateway` (`app.extensions
    ['zfsbackup.trust']`): its pin map authenticates requests and its store
    writers record enrollments, rotations and retirements.
    `server_fingerprint` is the fingerprint of the server identity the worker
    loaded at start (`app.extensions['zfsbackup.server_fingerprint']`, 64
    lowercase hex); enrollment tokens and the enrollment/rotation payloads
    are bound to it. It is taken from that `Identity` once, never re-read
    from disk, so it cannot drift from the certificate the gateway's contexts
    present. `clock` (POSIX seconds) decides pin and certificate expiry and is
    passed to the store as `now` (read again under its write lock).

    **Authentication** (`before_request`; it also runs on the websocket
    upgrade of `/stream`, so it is checked when a request or a stream
    STARTS -- a stream already running finishes even if its pin is revoked):

    1. `/health` and `/enroll` are exempt (`UNAUTHENTICATED_ENDPOINTS`).
    2. No client certificate (`SSL_CLIENT_CERT` absent) -> 401
       `{"error": "client certificate required"}`. Also for an unknown URL:
       routing errors are answered only to authenticated clients.
    3. The certificate's SHA-256 (DER) fingerprint is not pinned, or the pin's
       `not_after` has passed -> 403 `{"error": "client not trusted"}`.
    4. Otherwise `g.client_id` is the id the pin is BOUND to and
       `g.fingerprint` the certificate's fingerprint. Routes take the client
       id from `g` only.
    5. A `<client_id>` in the URL that differs from the binding -> 403
       `{"error": "client_id mismatch", "bound": <bound id>}` (plan D5).
    6. A pin that succeeds another by rotation retires its predecessor on
       its first authenticated request (`retire_predecessor`, then a bounded
       refresh of the trust view, see `_refresh_after_commit`); a failure is
       logged and retried on a later
       request, never fails the request. Not in a dry run (no persistent
       change).

    **The binding owns the server namespace.** Server datasets are
    `<target>/<bound id>/...`; `org.zfsbackup:client_id` is written from the
    binding, and an existing server dataset that records a different id is
    refused with 409.

    The trust endpoints' wire contract is in the docstrings of `enroll`,
    `rotate` and `whoami` below."""
    if trust is None:
        raise TypeError(
            "create_app needs the API worker's TrustGateway: the API has no "
            "unauthenticated mode"
        )
    own_fp = normalize_fingerprint(server_fingerprint)
    app = Flask(__name__)
    gate = StreamGate()
    app.extensions['zfsbackup.stream_gate'] = gate
    app.extensions['zfsbackup.trust'] = trust
    app.extensions['zfsbackup.server_fingerprint'] = own_fp
    app.config['PROPAGATE_EXCEPTIONS'] = True
    app.config['MAX_CONTENT_LENGTH'] = MAX_REQUEST_BYTES
    sock = Sock(app)
    manager = DatasetManager(config)
    #: The server datasets a `/stream` is working on right now, claimed
    #: non-blocking around its whole check -> abort -> create -> receive
    #: sequence: two streams for the same dataset (a client retry after a
    #: timeout, say) must not race one client's abort against the other's
    #: receive. A name is in the set exactly while its stream runs, so the
    #: set never grows beyond the number of concurrent streams.
    streams_in_progress: set = set()
    streams_guard = threading.Lock()

    def _claim_dataset(name: str) -> bool:
        with streams_guard:
            if name in streams_in_progress:
                return False
            streams_in_progress.add(name)
            return True

    def _release_dataset(name: str) -> None:
        with streams_guard:
            streams_in_progress.discard(name)

    remote_backup = config.remote_backup
    enabled = bool(remote_backup and remote_backup.enabled)
    target = remote_backup.target_dataset if remote_backup else None

    def _gated(view):
        """Count `view` as an in-flight remote operation. While the worker
        drains for a reload, new ones are refused with the retryable 503, and
        a RELOAD drain waits for the ones running -- so negotiate/
        resume_token (which run `zfs` and create datasets) and enroll/rotate
        (which write trust) are not cut off mid-call, and a refused negotiate
        cannot be mistaken by the client for "no common snapshot" (-> a
        needless full send)."""
        @functools.wraps(view)
        def wrapper(*args, **kwargs):
            disabled = _require_enabled()  # a disabled server says so, always
            if disabled is not None:
                return disabled
            if not gate.begin():
                return jsonify(RELOADING_ERROR), 503
            try:
                return view(*args, **kwargs)
            finally:
                gate.end()
        return wrapper

    def _require_enabled():
        if enabled and target:
            return None
        return jsonify({'error': 'remote_backup disabled'}), 503

    def _server_dataset(client_id: str, client_dataset: str) -> str:
        """Derive the server-side dataset path from the client's dataset
        name: `<target>/<client_id>/<client dataset without its pool>`.
        `client_id` is always the BOUND id (`g.client_id`). Raises
        `_BadDatasetName` (the routes answer 400) when the result is not a
        valid filesystem name, so no `zfs` command ever sees it."""
        parts = client_dataset.split('/', 1)
        relative = parts[1] if len(parts) > 1 else ''
        name = f"{target}/{client_id}/{relative}" if relative else f"{target}/{client_id}"
        try:
            Filesystem(name)
        except Exception as e:
            raise _BadDatasetName(f"invalid dataset name {name!r}: {e}") from None
        return name

    def _resume_token(server_dataset: str) -> Optional[str]:
        """The `receive_resume_token` of the server dataset -- a filesystem
        or a volume, found by name alone -- or `None` (no dataset, or no
        token). Raises on any other failure: callers must not guess."""
        ds = Dataset(server_dataset)
        if not zfs.exists(ds):
            return None
        results = zfs.list(roots=ds, properties=['receive_resume_token'])
        if not results:
            return None
        prop = results[0]['receive_resume_token']
        if prop is None:
            return None
        val = str(prop)
        return val if val != 'none' else None

    def _ownership_conflict(
        server_dataset: str, dataset: str, client_id: str
    ) -> Optional[str]:
        """An error message (the routes answer 409) when `server_dataset`
        exists and is not this client dataset's, else `None`:

        - it records a different client id (`org.zfsbackup:client_id`) than
          the bound `client_id` -- the server namespace is the binding's, and
          a dataset stamped for another id (an operator rename, a changed
          host id) is never written through;
        - it records a different client dataset as its source.
          `_server_dataset` drops the pool, so `tank/data` and `backup/data`
          map to the same server dataset and would otherwise silently share
          (and overwrite) it.

        A dataset without a property (never negotiated) is not a conflict for
        that property. Raises if the dataset's properties cannot be read."""
        ds = Dataset(server_dataset)
        if not zfs.exists(ds):
            return None
        props = read_zfsbackup_properties(ds)
        recorded_id = props.get(PROP_CLIENT_ID)
        if recorded_id and recorded_id != client_id:
            # The other id is not named: it is not this client's business.
            return (
                f"server dataset {server_dataset} is recorded as another "
                f"client's ({PROP_CLIENT_ID} differs from your bound client id "
                f"{client_id!r}); refusing to use it. The server operator must "
                "resolve this"
            )
        recorded = props.get(PROP_SOURCE_DATASET)
        if recorded and recorded != dataset:
            return (
                f"server dataset {server_dataset} already holds backups of "
                f"{recorded!r}; refusing to mix in {dataset!r}"
            )
        return None

    # ---------------------------------------------------------------- auth

    #: `(fingerprint, predecessor)` pairs whose retirement already ran to
    #: completion (`retire_predecessor` returned without raising). Only a
    #: stale predecessor field -- the store clears it without bumping the
    #: trust generation, so the view keeps showing it -- returns False and
    #: would otherwise cost a read transaction on every request.
    retire_settled: set = set()
    retire_guard = threading.Lock()

    def _retire_predecessor(fingerprint: str, predecessor: str) -> None:
        """Plan D4: the successor's first authenticated request retires the
        pin it succeeds. Never raises and never fails the request: a busy or
        unusable database is logged and the next request tries again (the
        store's read-only pre-check makes the retry cheap, and racing
        retirements retire once)."""
        key = (fingerprint, predecessor)
        with retire_guard:
            if key in retire_settled:
                return
        if config.dry_run:
            logger.debug(
                "dry run: not retiring pin %s (superseded by %s)", predecessor, fingerprint
            )
            return
        from zfsbackup.config.store import ConfigBusy
        try:
            retired = trust.retire_predecessor(fingerprint)
        except ConfigBusy:
            logger.info(
                "cannot retire pin %s (superseded by %s) yet: the config database "
                "is busy; retrying on a later request", predecessor, fingerprint,
            )
            return
        except Exception as e:
            logger.warning(
                "cannot retire pin %s (superseded by %s): %s: %s; retrying on a "
                "later request", predecessor, fingerprint, type(e).__name__, e,
            )
            return
        with retire_guard:
            if len(retire_settled) > 1024:
                retire_settled.clear()
            retire_settled.add(key)
        if retired:
            logger.info(
                "client %s: retired pin %s, superseded by %s (rotation complete)",
                g.client_id, predecessor, fingerprint,
            )
            _refresh_after_commit(f"retirement of pin {predecessor}")

    @app.before_request
    def _authenticate():
        """Route authentication; see `create_app`. Returning a response
        refuses the request (for `/stream`: the websocket upgrade)."""
        if request.endpoint in UNAUTHENTICATED_ENDPOINTS:
            return None
        pem = request.environ.get('SSL_CLIENT_CERT')
        if not pem:
            logger.debug(
                "API: %s %s from %s without a client certificate: 401",
                request.method, request.path, request.remote_addr,
            )
            return jsonify(CERT_REQUIRED_ERROR), 401
        try:
            fingerprint = fingerprint_der(ssl.PEM_cert_to_DER_cert(pem))
        except (ValueError, TypeError):
            fingerprint = None
        pin = trust.lookup(fingerprint) if fingerprint else None
        if pin is None or clock() > pin.not_after:
            logger.warning(
                "API: refusing %s %s from %s: client certificate %s is %s",
                request.method, request.path, request.remote_addr,
                fingerprint or '(unreadable)',
                'not pinned' if pin is None else 'expired',
            )
            return jsonify(NOT_TRUSTED_ERROR), 403
        g.client_id = pin.client_id
        g.fingerprint = fingerprint
        url_id = (request.view_args or {}).get('client_id')
        if url_id is not None and url_id != pin.client_id:
            logger.warning(
                "API: refusing %s %s from %s: the URL names client %r but "
                "certificate %s is bound to %r",
                request.method, request.path, request.remote_addr,
                url_id, fingerprint, pin.client_id,
            )
            return jsonify({'error': 'client_id mismatch', 'bound': pin.client_id}), 403
        if pin.predecessor is not None:
            _retire_predecessor(fingerprint, pin.predecessor)
        return None

    @app.errorhandler(RequestEntityTooLarge)
    def _too_large(e):
        return jsonify({
            'error': f'request body too large (limit {MAX_REQUEST_BYTES} bytes)'
        }), 413

    # ------------------------------------------------------ trust endpoints

    def _trust_body():
        """The JSON object body of `/enroll` or `/rotate`, read under
        `MAX_TRUST_REQUEST_BYTES`; -> `(body, None)` or `(None, response)`
        (413 too large; 400 not a JSON object or an unsupported `version`).
        The body is never logged or echoed."""
        request.max_content_length = MAX_TRUST_REQUEST_BYTES
        try:
            raw = request.get_data(cache=False)
        except RequestEntityTooLarge:
            return None, (jsonify({
                'error': f'request body too large (limit {MAX_TRUST_REQUEST_BYTES} bytes)'
            }), 413)
        try:
            body = json.loads(raw)
        except (ValueError, UnicodeDecodeError, RecursionError):
            body = None
        if not isinstance(body, dict):
            return None, (jsonify({'error': 'request body must be a JSON object'}), 400)
        version = body.get('version')
        if type(version) is not int or version != TRUST_WIRE_VERSION:
            return None, (jsonify({
                'error': f'unsupported request version {version!r}; this server '
                         f'speaks version {TRUST_WIRE_VERSION}'
            }), 400)
        return body, None

    def _bad(message: str, status: int = 400):
        return jsonify({'error': message}), status

    def _text_field(body: dict, name: str) -> Optional[str]:
        value = body.get(name)
        return value if isinstance(value, str) and value else None

    def _signature(body: dict) -> Optional[bytes]:
        """`signature`, standard base64 (padding required, no whitespace)."""
        value = _text_field(body, 'signature')
        if value is None:
            return None
        try:
            decoded = base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError):
            return None
        return decoded or None

    def _now_utc() -> datetime:
        return datetime.fromtimestamp(clock(), tz=timezone.utc)

    def _refuse_trust_write():
        """503 (no retry) when this server may not change trust: a dry run
        makes no persistent change (plan D7)."""
        if config.dry_run:
            return jsonify({
                'error': 'this server runs in dry-run mode and makes no trust '
                         'changes; enroll or rotate against the real server',
                'reason': 'dry_run',
            }), 503
        return None

    def _store_failure(action: str, e: Exception):
        """The response for an exception of a store trust writer."""
        from zfsbackup.config.store import ConfigBusy
        if isinstance(e, ConfigBusy):
            logger.warning(
                "%s from %s: the config database is busy (503, retry)",
                action, request.remote_addr,
            )
            return jsonify(TRUST_BUSY_ERROR), 503
        logger.error(
            "%s from %s failed: cannot write client trust: %s: %s",
            action, request.remote_addr, type(e).__name__, e,
        )
        return jsonify(TRUST_UNAVAILABLE_ERROR), 503

    def _bounded_refresh(action: str) -> bool:
        """`trust.refresh(timeout=REQUEST_REFRESH_WAIT)`; never raises."""
        try:
            return trust.refresh(timeout=REQUEST_REFRESH_WAIT)
        except Exception:  # refresh() never raises; belt and braces
            logger.debug("trust refresh after %s failed", action, exc_info=True)
            return False

    def _refresh_after_commit(action: str) -> None:
        """Apply `action`'s committed trust change to this worker's view.

        **Before the response when it can be done quickly:** a refresh with
        `timeout=REQUEST_REFRESH_WAIT`, which waits at most that long for a
        running refresh/poll and for the store lock, then reads the pins
        (bounded by the store's own busy timeout, up to 5 s on top) and
        rebuilds the context with only the rest of that budget as its
        identity-lock wait. When it succeeds, the change works on the
        client's very next connection.

        **Otherwise right after the response is sent:** if it could not run
        in time (another refresh holds the locks) or failed, the same
        bounded refresh is retried from `call_on_close`, which werkzeug runs
        as soon as the whole response has been written and flushed -- not
        on the next poll. Either way no request waits without limit for
        another thread, so a client that got a committed write's answer
        never times out waiting for a refresh (the CLI's read timeout is
        30 s). A refresh that fails or does not run also marks the gateway
        dirty (`TrustGateway.refresh`), so the next trust poll re-reads
        even if the key it sees is the installed view's -- a view installed
        by a read that predates the commit cannot stay stale.

        **A brief window:** in the fallback case a client that connects
        immediately after its 200 can be refused at the handshake
        (`TLSV1_ALERT_UNKNOWN_CA` for a new certificate) or still see the
        old pin set until that refresh lands -- milliseconds normally. Under
        contention it is the sum of the waits that can stack: up to
        `REQUEST_REFRESH_WAIT` for the locks before the response, the same
        again after it is sent, the store's busy timeout (5 s) for the
        read, and the poll's next turn (`TRUST_POLL_SECONDS`, whose own
        rebuild may wait `GATEWAY_LOCK_TIMEOUT`) -- roughly 7-12 s.
        Callers should retry such a refusal once after a moment."""
        if _bounded_refresh(action):
            return

        @after_this_request
        def _schedule(response):
            def _late() -> None:
                if not _bounded_refresh(action):
                    logger.warning(
                        "%s committed, but the trust view could not be refreshed; "
                        "the next trust poll applies it", action,
                    )
            response.call_on_close(_late)
            return response

    @app.route('/health')
    def health():
        return jsonify({'status': 'ok'})

    @app.route('/whoami')
    def whoami():
        """`GET /whoami` (authenticated): who the server takes this client
        certificate for.

        200 `{"client_id": <bound id>, "fingerprint": <this certificate's
        SHA-256, 64 lowercase hex>}`. 401/403 as for every authenticated
        endpoint. Not gated: it answers while the worker drains, and with
        remote backup disabled."""
        return jsonify({'client_id': g.client_id, 'fingerprint': g.fingerprint})

    @app.route('/enroll', methods=['POST'])
    @_gated
    def enroll():
        """`POST /enroll` (unauthenticated; reached WITHOUT a client
        certificate): redeem the server's enrollment token to pin the
        client's certificate under a client id (plan M10, D3).

        Request body (JSON, at most 64 KiB), `Content-Type` not required::

            {
              "version": 1,
              "client_id": "<A7 grammar: [A-Za-z0-9][A-Za-z0-9._-]{0,63}>",
              "label": "<optional, at most 128 characters, no control characters>",
              "token_secret": "<the WHOLE token as `trust token create` printed it:
                                zfsb1.<server fingerprint>.<secret>>",
              "cert_pem": "<the client certificate to pin: one PEM, <= 16 KiB>",
              "signature": "<standard base64 of the certificate key's signature over
                             identity.enrollment_payload(server_fp, client_id,
                             cert_fp, secret_sha256)>"
            }

        `server_fp` is the fingerprint in the token (it must be this
        server's), `cert_fp` the SHA-256 of `cert_pem`'s DER and
        `secret_sha256` `identity.secret_hash(<the token's decoded secret>)`.
        The server re-derives all three; the signature proves the caller
        holds the certificate's key and binds the token, the id and the
        certificate to THIS server (no relay).

        Responses (`reason` is a stable code; `error` is for humans):

        - 200 `{"status": "enrolled", "client_id", "fingerprint"}`: pinned
          now; the token is spent. The certificate works on the next
          connection (the trust view is refreshed, bounded, before this
          response; if that cannot run in time, right after it is sent --
          then a client connecting at once may be refused at the handshake
          for a moment and should retry once; see `_refresh_after_commit`).
          A `--replace` token scoped to this id also answers `enrolled` for
          a certificate that was already pinned to it: the token is spent
          and the client's OTHER pins are dropped.
        - 200 `{"status": "already_trusted", "client_id", "fingerprint"}`:
          this certificate is already pinned to this id (and the token is
          not a `--replace` token for it); the token was NOT spent.
        - 400 `{"error"}`: a malformed body -- not a JSON object, `version`
          not 1, a missing/non-string field, a client id outside the grammar,
          a bad label, a malformed token or signature encoding.
        - 403 `{"error": "enrollment refused", "reason": "refused"}`:
          identical for every token problem (none, expired, spent, for
          another server, wrong secret, scoped to another id) and for a
          certificate this server has revoked (revoked, dropped by a
          `--replace`, or a retired rotation predecessor): a revoked key is
          never pinned again, whatever the token.
        - 403 `{"error", "reason": "bad_signature"}`: the signature does not
          verify against `cert_pem`.
        - Check order: shape 400 -> grammar 400 -> label 400 -> token parse
          400 -> token for another server 403 refused -> certificate 422 ->
          signature 403 -> token pre-check 403 refused (read-only, before any
          `zfs` call; also refuses a revoked certificate) -> `zfs.exists`
          503 -> the store (`enroll_client`, the authority).
        - 409 `{"error", "reason": "client_id_taken", "client_id", "hint"}`:
          the id is already known here (a client record, a revoked one, or an
          existing `<target>/<client_id>`) and the token is not scoped to it.
          The token was NOT spent and stays valid.
        - 409 `{"error", "reason": "cert_bound_elsewhere"}`: the certificate
          is pinned to another client id.
        - 413 `{"error"}`: body over 64 KiB.
        - 422 `{"error", "reason": "bad_certificate"}`: `cert_pem` is not
          acceptable (`identity.check_client_cert`; the reason is in
          `error`).
        - 503 `{"error", "retry": true}`: draining for a reload
          (`RELOADING_ERROR`), the database busy (`reason: "busy"`) or the
          server's `zfs` failing (`reason: "zfs"`) -- retry later.
        - 503 without `retry`: remote backup disabled (`{"error":
          "remote_backup disabled"}`), a dry-run server (`reason:
          "dry_run"`) or the trust tables unusable (`reason: "unavailable"`).

        The token and the body are never logged or echoed."""
        refused = _refuse_trust_write()
        if refused is not None:
            return refused
        body, problem = _trust_body()
        if problem is not None:
            return problem
        remote = request.remote_addr

        def failed(category: str, response):
            logger.warning("enrollment from %s refused: %s", remote, category)
            return response

        client_id = body.get('client_id')
        id_problem = client_id_problem(client_id)
        if id_problem is not None:
            return failed('bad client id', _bad(f"client_id: {id_problem}"))
        label = body.get('label')
        if label is not None:
            if not isinstance(label, str):
                return failed('bad label', _bad('label must be a string'))
            if len(label) > LABEL_MAX_CHARS:
                return failed('bad label', _bad(
                    f'label is {len(label)} characters long; the maximum is {LABEL_MAX_CHARS}'
                ))
            if any(unicodedata.category(c).startswith('C') for c in label):
                return failed('bad label', _bad('label must not contain control characters'))
            label = label or None
        token = _text_field(body, 'token_secret')
        cert_pem = _text_field(body, 'cert_pem')
        signature = _signature(body)
        if token is None or cert_pem is None or signature is None:
            missing = [
                name for name, value in (
                    ('token_secret', token), ('cert_pem', cert_pem), ('signature', signature),
                ) if value is None
            ]
            return failed('incomplete request', _bad(
                f"missing or malformed field(s): {', '.join(missing)} "
                "(signature: standard base64)"
            ))
        try:
            token_fp, secret = parse_token(token)
        except ValueError as e:  # the message never contains the token
            return failed('malformed token', _bad(str(e)))
        if token_fp != own_fp:
            # Refused exactly like a wrong secret: the token is for another
            # server (a relay, or a re-keyed server).
            return failed('token (another server)', (jsonify(ENROLL_REFUSED_ERROR), 403))
        secret_sha256 = secret_hash(secret)
        try:
            info = check_client_cert(cert_pem, now=_now_utc())
        except ValueError as e:
            return failed('certificate not acceptable', (jsonify({
                'error': f'certificate not acceptable: {e}',
                'reason': 'bad_certificate',
            }), 422))
        cert_fp = info.fingerprint
        payload = enrollment_payload(own_fp, client_id, cert_fp, secret_sha256)
        if not verify_payload(cert_pem, payload, signature):
            return failed('bad signature', (jsonify({
                'error': 'the signature does not verify against cert_pem (proof '
                         'of possession of its key failed)',
                'reason': 'bad_signature',
            }), 403))
        # Token pre-check: a read-only filter BEFORE the first costly step
        # (`zfs.exists`), so an unauthenticated caller without a valid token
        # -- or presenting a certificate revoked here -- costs one SQLite read
        # and never a `zfs` subprocess. Only a filter: `enroll_client` below
        # stays the authority and re-checks everything.
        try:
            worth_trying = trust.token_precheck(
                secret_sha256=secret_sha256, server_fingerprint=own_fp, now=clock,
                fingerprint=cert_fp,
            )
        except Exception as e:
            return _store_failure('enrollment', e)
        if not worth_trying:
            # The pre-check cannot tell which; the client's answer is the
            # same uniform 403 either way.
            return failed('token or revoked certificate', (jsonify(ENROLL_REFUSED_ERROR), 403))
        try:
            client_root = Dataset(f"{target}/{client_id}")
        except Exception:
            return failed('client id not a dataset name', _bad(
                f"client_id {client_id!r} cannot name a dataset on this server"
            ))
        try:
            dataset_exists = bool(zfs.exists(client_root))
        except Exception as e:
            logger.error(
                "enrollment from %s: cannot check %s: %s", remote, client_root.name, e,
            )
            return jsonify({
                'error': 'the server cannot check its datasets right now; retry later',
                'reason': 'zfs',
                'retry': True,
            }), 503
        from zfsbackup.config.store import EnrollOutcome, NewPin
        try:
            pin = NewPin(cert_fp, cert_pem, info.not_after.timestamp())
            outcome = trust.enroll_client(
                secret_sha256=secret_sha256,
                server_fingerprint=own_fp,
                client_id=client_id,
                pin=pin,
                label=label,
                now=clock,
                dataset_exists=dataset_exists,
            )
        except Exception as e:
            return _store_failure('enrollment', e)

        if outcome == EnrollOutcome.ENROLLED:
            logger.info(
                "enrolled client %s (certificate %s, label %r) from %s",
                client_id, cert_fp, label, remote,
            )
            _refresh_after_commit(f"enrollment of client {client_id}")
            return jsonify({'status': 'enrolled', 'client_id': client_id, 'fingerprint': cert_fp})
        if outcome == EnrollOutcome.ALREADY_TRUSTED:
            logger.info(
                "enrollment from %s: certificate %s is already pinned to client %s "
                "(token not used)", remote, cert_fp, client_id,
            )
            return jsonify({
                'status': 'already_trusted', 'client_id': client_id, 'fingerprint': cert_fp,
            })
        if outcome == EnrollOutcome.CLIENT_ID_TAKEN:
            return failed(f'client id {client_id!r} taken', (jsonify({
                'error': f'client id {client_id!r} is already known to this server',
                'reason': 'client_id_taken',
                'client_id': client_id,
                'hint': (
                    'to enroll a new certificate under this id, ask the server '
                    'operator for a token scoped to it: zfsbackup-config trust '
                    f'token create --client-id {client_id} (with --replace to drop '
                    'its other certificates). The token you sent was not used and '
                    'is still valid'
                ),
            }), 409))
        if outcome == EnrollOutcome.CERT_BOUND_ELSEWHERE:
            return failed('certificate bound to another client id', (jsonify({
                'error': 'this certificate is already enrolled under another client id',
                'reason': 'cert_bound_elsewhere',
            }), 409))
        return failed('token', (jsonify(ENROLL_REFUSED_ERROR), 403))

    @app.route('/rotate', methods=['POST'])
    @_gated
    def rotate():
        """`POST /rotate` (authenticated with the CURRENT certificate): pin a
        successor certificate for the bound client (plan D4, M14b).

        Request body (JSON, at most 64 KiB)::

            {
              "version": 1,
              "successor_cert_pem": "<the new certificate: one PEM, <= 16 KiB>",
              "signature": "<standard base64 of the SUCCESSOR key's signature over
                             identity.rotation_payload(server_fp, client_id,
                             old_fp, new_fp)>"
            }

        `server_fp` is this server's pinned fingerprint, `client_id` the id
        `/whoami` reports, `old_fp` the fingerprint of the certificate this
        request is made with, `new_fp` the SHA-256 of `successor_cert_pem`'s
        DER.

        Responses:

        - 200 `{"status": "rotated" | "already", "client_id", "fingerprint":
          <new_fp>}`: the successor is pinned (`already`: it was, by an
          earlier identical rotation). Both certificates work until the
          successor's first authenticated request, which retires the old pin.
        - 400 `{"error"}`: malformed body (as for `/enroll`).
        - 401 / 403 `client not trusted` / `client_id mismatch`: the usual
          authentication refusals.
        - 403 `{"error", "reason": "bad_signature"}`: the signature does not
          verify against the successor certificate.
        - 403 `{"error": "rotation refused", "reason": "refused"}`: the
          store refused (the successor is pinned elsewhere, equals the
          current certificate or was revoked here before, or the current
          pin already has a different pending successor).
        - 413, 422 (`reason: "bad_certificate"`) and 503 (`retry` while
          draining or busy; none for disabled, dry run, unavailable) as for
          `/enroll`."""
        refused = _refuse_trust_write()
        if refused is not None:
            return refused
        body, problem = _trust_body()
        if problem is not None:
            return problem
        client_id, old_fp = g.client_id, g.fingerprint
        remote = request.remote_addr

        def failed(category: str, response):
            logger.warning(
                "rotation for client %s (certificate %s) from %s refused: %s",
                client_id, old_fp, remote, category,
            )
            return response

        cert_pem = _text_field(body, 'successor_cert_pem')
        signature = _signature(body)
        if cert_pem is None or signature is None:
            missing = [
                name for name, value in (
                    ('successor_cert_pem', cert_pem), ('signature', signature),
                ) if value is None
            ]
            return failed('incomplete request', _bad(
                f"missing or malformed field(s): {', '.join(missing)} "
                "(signature: standard base64)"
            ))
        try:
            info = check_client_cert(cert_pem, now=_now_utc())
        except ValueError as e:
            return failed('certificate not acceptable', (jsonify({
                'error': f'certificate not acceptable: {e}',
                'reason': 'bad_certificate',
            }), 422))
        new_fp = info.fingerprint
        payload = rotation_payload(own_fp, client_id, old_fp, new_fp)
        if not verify_payload(cert_pem, payload, signature):
            return failed('bad signature', (jsonify({
                'error': 'the signature does not verify against successor_cert_pem '
                         '(proof of possession of its key failed)',
                'reason': 'bad_signature',
            }), 403))
        from zfsbackup.config.store import NewPin, RotateOutcome
        try:
            outcome = trust.rotate_client(
                old_fp=old_fp,
                new_pin=NewPin(new_fp, cert_pem, info.not_after.timestamp()),
                now=clock,
            )
        except Exception as e:
            return _store_failure(f"rotation for client {client_id}", e)
        if outcome in (RotateOutcome.ROTATED, RotateOutcome.ALREADY):
            logger.info(
                "client %s: certificate %s %s by %s from %s; the old pin is "
                "retired on the new certificate's first use", client_id, old_fp,
                'succeeded' if outcome == RotateOutcome.ROTATED else 'already succeeded',
                new_fp, remote,
            )
            _refresh_after_commit(f"rotation for client {client_id}")
            return jsonify({
                'status': str(outcome), 'client_id': client_id, 'fingerprint': new_fp,
            })
        return failed('refused by the trust store', (jsonify(ROTATE_REFUSED_ERROR), 403))

    @app.route('/config')
    def get_config():
        return jsonify({
            'snapshot_prefix': config.snapshot_prefix,
            'check_interval_seconds': config.check_interval.total_seconds(),
            'prune_interval_seconds': config.effective_prune_interval.total_seconds(),
            'api_host': config.api_host,
            'api_port': config.api_port,
            'dry_run': config.dry_run,
        })

    @app.route('/datasets')
    def get_datasets():
        return jsonify([
            {
                'name': dsi.name,
                'frequency_seconds': dsi.frequency.total_seconds(),
                'recursive': dsi.recursive,
                'retention_rules': [
                    {
                        'age_seconds': r.age.total_seconds(),
                        'keep_for_seconds': r.keep_for.total_seconds(),
                    }
                    for r in dsi.config.retention_rules
                ],
            }
            for dsi in manager.datasets
        ])

    @app.route('/datasets/<path:name>/snapshots')
    def get_snapshots(name: str):
        dsi = next((d for d in manager.datasets if d.name == name), None)
        if dsi is None:
            return jsonify({'error': f'Dataset {name!r} not configured'}), 404
        snapshots = manager.list_snapshots(dsi)
        return jsonify([
            {
                'name': s.name,
                'full_name': s.full_name,
                'timestamp': s.timestamp.isoformat() if s.timestamp else None,
                'age_seconds': s.age.total_seconds(),
            }
            for s in snapshots
        ])

    @app.route('/backup/<client_id>/<path:dataset>/negotiate', methods=['POST'])
    @_gated
    def backup_negotiate(client_id: str, dataset: str):
        disabled = _require_enabled()
        if disabled is not None:
            return disabled
        # The bound id (`_authenticate`), never the URL's: they are equal
        # here, but only the binding is trusted.
        client_id = g.client_id
        data = request.get_json(force=True)
        client_snapshots: list = data.get('snapshots', [])
        ds_config_encoded: str = data.get('config', '')

        try:
            server_dataset = _server_dataset(client_id, dataset)
        except _BadDatasetName as e:
            return jsonify({'error': str(e)}), 400
        server_fs = Filesystem(server_dataset)
        server_fs_exists = zfs.exists(server_fs)

        # D7 and the binding: a server dataset that records another client
        # id or another client dataset is not this one's. Checked before
        # anything is written, so its recorded owner is not overwritten.
        if server_fs_exists:
            try:
                conflict = _ownership_conflict(server_dataset, dataset, client_id)
            except Exception as e:
                return jsonify({'error': f'Failed to check {server_dataset}: {e}'}), 500
            if conflict is not None:
                return jsonify({'error': conflict}), 409

        # Store client config on server dataset (create dataset if needed)
        if ds_config_encoded:
            if not server_fs_exists:
                if not config.dry_run:
                    zfs.create(server_fs, parents=True)
                    server_fs_exists = True
            if not config.dry_run:
                zfs.set(server_fs, {
                    PROP_CONFIG: ds_config_encoded,
                    PROP_CLIENT_ID: client_id,
                    PROP_SOURCE_DATASET: dataset,
                })

        # Find the most recent snapshot present on both sides
        common_snapshot = None
        if server_fs_exists:
            try:
                server_snaps = zfs.list(roots=server_fs, types='snapshot', properties=['creation'])
                server_names = {s.short for s in server_snaps}
                for snap_name in client_snapshots:
                    if snap_name in server_names:
                        common_snapshot = snap_name
                        break
            except Exception as e:
                return jsonify({'error': f'Failed to list server snapshots: {e}'}), 500

        return jsonify({
            'common_snapshot': common_snapshot,
            'server_dataset': server_dataset,
        })

    @sock.route('/backup/<client_id>/<path:dataset>/stream')
    def backup_stream(ws, client_id: str, dataset: str):
        # Authenticated at the upgrade (`_authenticate`); the bound id only.
        client_id = g.client_id
        if not (enabled and target):
            try:
                ws.send(json.dumps({'error': 'remote_backup disabled'}))
            except Exception:
                pass
            return
        mode = request.args.get('mode')
        if mode not in STREAM_MODES:
            problem = (
                "stream mode required: use ?mode=fresh or ?mode=resume" if mode is None
                else f"unknown stream mode {mode!r}: use ?mode=fresh or ?mode=resume"
            )
            try:
                ws.send(json.dumps({'error': problem}))
            except Exception:
                pass
            return
        if not gate.begin():
            try:
                ws.send(json.dumps(RELOADING_ERROR))
            except Exception:
                pass
            return
        try:
            # `?from=` (the incremental base the client negotiated) is
            # informational only: `zfs receive` takes the base from the stream.
            logger.debug(
                "stream for %s/%s: mode=%s from=%s",
                client_id, dataset, mode, request.args.get('from'),
            )
            _receive_stream(ws, client_id, dataset, mode)
        finally:
            gate.end()

    def _receive_stream(ws, client_id: str, dataset: str, mode: str) -> None:
        # Validate the name before it becomes a key of anything. The key is
        # `<target>/<bound id>/...`, so the per-dataset claim is scoped to
        # the authenticated client.
        try:
            server_dataset = _server_dataset(client_id, dataset)
        except _BadDatasetName as e:
            try:
                ws.send(json.dumps({'error': str(e)}))
            except Exception:
                pass
            return
        if not _claim_dataset(server_dataset):
            try:
                ws.send(json.dumps(BUSY_ERROR))
            except Exception:
                pass
            return
        try:
            _receive_stream_locked(ws, client_id, dataset, server_dataset, mode)
        finally:
            _release_dataset(server_dataset)

    def _receive_stream_locked(
        ws, client_id: str, dataset: str, server_dataset: str, mode: str
    ) -> None:
        server_fs = Filesystem(server_dataset)

        # D7 and the binding: refuse to receive into a server dataset that
        # another client id or client dataset owns (read-only, so also in a
        # dry run).
        try:
            conflict = _ownership_conflict(server_dataset, dataset, client_id)
        except Exception as e:
            ws.send(json.dumps({'error': f'Failed to check {server_dataset}: {e}'}))
            return
        if conflict is not None:
            ws.send(json.dumps({'error': conflict}))
            return

        if config.dry_run:
            ws.send(json.dumps({'status': 'ok', 'dry_run': True}))
            return

        if mode == 'fresh':
            # D3: a fresh stream cannot continue a partial receive. Discard
            # its saved state first, or `zfs receive` refuses with "contains
            # partially-complete state". Here the dataset always exists (the
            # D7 check above and negotiate see to that), so `receive -A`
            # destroys only its `%recv` state; the recreate below is for a
            # dataset a FULL receive created outside this flow, which the
            # abort destroys whole -- nothing before it may assume the
            # dataset still exists. `resume` never reaches this.
            try:
                token = _resume_token(server_dataset)
            except Exception as e:
                ws.send(json.dumps({
                    'error': f'Failed to check {server_dataset} for partial receive state: {e}'
                }))
                return
            if token:
                logger.warning(
                    "discarding the partial receive state of %s: the client "
                    "is sending a fresh stream", server_dataset,
                )
                try:
                    zfs.receive.abort(Dataset(server_dataset))
                except Exception as e:
                    # Idempotent: already clean is fine, anything else refuses.
                    if not any(m in str(e).lower() for m in _ABORT_ALREADY_CLEAN):
                        logger.error(
                            "cannot discard the partial receive state of %s: %s",
                            server_dataset, e,
                        )
                        ws.send(json.dumps({
                            'error': (
                                f'Failed to discard the partial receive state of '
                                f'{server_dataset}: {e}'
                            )
                        }))
                        return

        if not zfs.exists(server_fs):
            # Recreated (or created here): stamp the ownership properties
            # again, or a dataset without `source_dataset` loses the D7 check
            # and one without `client_id` is not found as this client's.
            try:
                zfs.create(server_fs, parents=True)
                zfs.set(server_fs, {
                    PROP_CLIENT_ID: client_id,
                    PROP_SOURCE_DATASET: dataset,
                })
            except Exception as e:
                ws.send(json.dumps({
                    'error': f'Failed to create {server_dataset}: {e}'
                }))
                return

        # zfs.receive.filesystem() returns a StreamHandle wrapping the `zfs
        # receive` process's stdin. A startup failure raises here; it never
        # returns None.
        try:
            writer = zfs.receive.filesystem(server_fs, force=True, save=True, mount=False)
        except Exception as e:
            ws.send(json.dumps({'error': f'Failed to start zfs receive: {e}'}))
            return

        reaped = False  # `writer.close()` already called on the write-error path
        try:
            while True:
                data = ws.receive(timeout=WS_IDLE_TIMEOUT)
                if data is None:
                    raise TimeoutError(
                        f"no data from the client for {WS_IDLE_TIMEOUT}s"
                    )
                if isinstance(data, str):
                    msg = json.loads(data)
                    if msg.get('done'):
                        break
                else:
                    try:
                        writer.write(data)
                    except OSError as pipe_error:
                        # `zfs receive` stopped reading (typically it has
                        # already exited, e.g. a checksum mismatch): the pipe
                        # error is only the symptom. `close()` reaps the
                        # process and raises zfs's own diagnostic; report
                        # that to the client and the log. If it raises
                        # nothing, the pipe error is all there is.
                        reaped = True
                        try:
                            writer.close()
                        except Exception as receive_error:
                            logger.error(
                                "zfs receive for %s failed: %s (stream write: %s)",
                                server_dataset, receive_error, pipe_error,
                            )
                            raise receive_error from pipe_error
                        logger.error(
                            "writing to zfs receive for %s failed: %s",
                            server_dataset, pipe_error,
                        )
                        raise
            # writer.close() (StreamHandle.__exit__/close()) waits on the
            # `zfs receive` subprocess and raises if it exited non-zero. That
            # must happen only after all chunks have been written above (done
            # here); if it raises, we fall through to the `except Exception`
            # branch below, which reports the error to the client instead of
            # sending 'status: ok'.
            reaped = True  # the handler below must not close a second time
            writer.close()
            ws.send(json.dumps({'status': 'ok'}))
        except ConnectionClosed:
            # Client already disconnected; best-effort reap the `zfs
            # receive` process. Any error it raises here can't be reported
            # back (the socket is gone) so it's intentionally suppressed.
            if not reaped:
                with contextlib.suppress(Exception):
                    writer.close()
        except Exception as e:
            if not reaped:
                with contextlib.suppress(Exception):
                    writer.close()
            with contextlib.suppress(Exception):
                ws.send(json.dumps({'error': str(e)}))

    @app.route('/backup/<client_id>/<path:dataset>/resume_token')
    @_gated
    def backup_resume_token(client_id: str, dataset: str):
        disabled = _require_enabled()
        if disabled is not None:
            return disabled
        client_id = g.client_id  # the binding only
        try:
            server_dataset = _server_dataset(client_id, dataset)
        except _BadDatasetName as e:
            return jsonify({'error': str(e)}), 400
        try:
            conflict = _ownership_conflict(server_dataset, dataset, client_id)
            if conflict is not None:
                return jsonify({'error': conflict}), 409
            return jsonify({'resume_token': _resume_token(server_dataset)})
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    return app

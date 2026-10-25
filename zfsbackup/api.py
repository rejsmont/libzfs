"""The server side of remote backup: the HTTPS API clients enroll, negotiate
and stream through. It exposes nothing about the server's own configuration or
datasets, and about its snapshots only what concerns the authenticated
client's own server copy: the names and guids of the copy's snapshots newer
than the common one (`server_newer`) and the refusal messages of `/stream`
(plan S-4).

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

**Protocol version (plan S-5).** Every endpoint except `/health` needs the
`X-Zfsbackup-Protocol` header naming a supported version (426 otherwise);
see `zfsbackup.protocol`.
"""

import base64
import binascii
import collections
import contextlib
import functools
import json
import logging
import os
import re
import shlex
import socket
import ssl
import struct
import threading
import time
import types
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, List, Mapping, NamedTuple, Optional, Tuple

from flask import Flask, after_this_request, g, jsonify, request
from flask_sock import Sock
from simple_websocket import ConnectionClosed
from werkzeug.exceptions import RequestEntityTooLarge

import libzfseasy as zfs
from libzfseasy.types import Dataset, Filesystem

from zfsbackup.backup_manager import (
    PROP_CONFIG, PROP_CLIENT_ID, PROP_SOURCE_DATASET,
    property_int, read_zfsbackup_properties,
)
from zfsbackup.config import (
    BackupConfig, DatasetConfig, client_id_problem, normalize_fingerprint,
    wire_config_problem,
)
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
from zfsbackup.protocol import (
    PROTOCOL_HEADER, PROTOCOL_REFUSAL_REASON, PROTOCOL_VERSION, SUPPORTED_PROTOCOLS,
    parse_protocol_header,
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

#: Most server snapshots `negotiate` names in `server_newer` (the snapshots of
#: the server copy newer than the common one, oldest first); the total is
#: always in `server_newer_count`.
SERVER_NEWER_MAX = 64

#: Most guids `/stream?discard=` accepts.
DISCARD_MAX = 16

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

#: The endpoints that need no `X-Zfsbackup-Protocol` header (plan S-5): only
#: the liveness probe, so a monitor needs no knowledge of the protocol.
#: `/enroll` is NOT exempt: an enrollment by a CLI that speaks another
#: protocol would pin a client its daemon then cannot use.
PROTOCOL_EXEMPT_ENDPOINTS = frozenset({'health'})

#: The ZFS user properties the SERVER owns on a received dataset (plan B-1b):
#: it writes them itself, as LOCAL values, from the authenticated binding
#: (`client_id`), the URL (`source_dataset`) and the validated negotiate body
#: (`config`, stored by negotiate). A `zfs send -p` stream carries the
#: client's own copies, so the receive is given the server's values with
#: `-o` (`_receive_stream_locked`), never `-x`. `-x` is NOT enough: verified
#: on OpenZFS 2.1.5, 2.2.2 and 2.4.1, a full (`-F`) receive of a `-p` stream
#: into an existing dataset replaces the local values with the stream's
#: (source `received`) whatever `-x` says; `-x` only helps for an
#: incremental or resumed receive. `-o` and `-x` of one name are mutually
#: exclusive ("specified multiple times"), so `-o` alone is used. The values
#: are LOCAL after a full, an incremental and a resumed receive, raw or plain
#: (captured on the same versions).
#: The dynamic `anchor.*`/`inflight.*` names cannot be listed; they can only
#: pin snapshots on the server, never cause one to be destroyed.
SERVER_OWNED_PROPERTIES = (PROP_CONFIG, PROP_CLIENT_ID, PROP_SOURCE_DATASET)

#: Plan B-2a: what the server forces onto a received FILESYSTEM, as LOCAL
#: values (`-o`, never `-x`): on OpenZFS 2.1.5, 2.2.2 and 2.4.1 (captures M8,
#: `tests/fixtures/openzfs/*/M8.txt`) `-o <prop>=<safe value>` wins as a local
#: value in every case captured (a missing dataset, an incremental, an existing
#: placeholder, an existing dataset with preset locals, raw), whereas `-x` does
#: NOT protect a full `-F` receive of a `-p` stream into an existing dataset
#: (the stream's value becomes effective, source `received`). Together: never
#: mounted (`canmount=noauto`, so `zfs mount -a` mounts nothing), never shared,
#: no setuid/exec/devices, no mountpoint of the client's choosing, never
#: written by anything but the receive (`readonly`), never a zvol device.
#: Disjoint from `SERVER_OWNED_PROPERTIES` (also `-o`; one name twice is a zfs
#: usage error).
RECEIVE_PROPERTIES_FILESYSTEM = {
    'canmount': 'noauto',
    'readonly': 'on',
    'volmode': 'none',
    'mountpoint': 'none',
    'sharenfs': 'off',
    'sharesmb': 'off',
    'setuid': 'off',
    'exec': 'off',
    'devices': 'off',
}

#: The same for a received VOLUME. A volume REFUSES `-o canmount`
#: ("property 'canmount' does not apply to datasets of this type"; full,
#: incremental and resume, plain and raw) and `-o mountpoint` and the other
#: filesystem-only properties, so only these two are passed; `-x` of the
#: filesystem-only names is accepted on a volume but prints one "does not
#: apply" warning each, so it is not used either (and `-x` protects nothing,
#: see above). A volume has no mount to neutralise; `volmode=none` keeps its
#: device node (udev/LVM probing) off the server.
RECEIVE_PROPERTIES_VOLUME = {
    'readonly': 'on',
    'volmode': 'none',
}

assert not set(RECEIVE_PROPERTIES_FILESYSTEM) & set(SERVER_OWNED_PROPERTIES)
assert not set(RECEIVE_PROPERTIES_VOLUME) & set(SERVER_OWNED_PROPERTIES)

#: The `dmu_replay_record` that opens every `zfs send` substream (OpenZFS
#: `include/sys/zfs_ioctl.h`), 312 bytes: `drr_type` u32 (`DRR_BEGIN` = 0),
#: `drr_payloadlen` u32, then the `drr_begin` union member -- `drr_magic` u64,
#: `drr_versioninfo` u64, `drr_creation_time` u64, `drr_type`
#: (`dmu_objset_type_t`) u32, `drr_flags` u32, `drr_toguid` u64,
#: `drr_fromguid` u64, `drr_toname` char[256] (`MAXNAMELEN`). Fields are in the
#: SENDER's byte order; `DMU_BACKUP_MAGIC` tells which (the receiver byte-swaps
#: when it reads the magic swapped). Offsets: magic 8, versioninfo 16,
#: creation 24, objset type 32, flags 36, toguid 40, fromguid 48, toname 56.
DRR_BEGIN = 0
DRR_RECORD_SIZE = 312
DMU_BACKUP_MAGIC = 0x2F5bacbac
#: `dmu_objset_type_t`: NONE 0, META 1, ZFS 2, ZVOL 3 (sys/fs/zfs.h).
DMU_OST_ZFS = 2
DMU_OST_ZVOL = 3
#: `DMU_GET_STREAM_HDRTYPE(drr_versioninfo)` = bits 0-1: 1 = a plain
#: substream (the only type accepted), 2 = a compound stream (what `zfs send
#: -p`/`-R` writes first: a stub record with objset type 0 and an nvlist
#: payload, then `DRR_END`, then the real substream's begin). A non-zero
#: `drr_payloadlen` is normal for raw and resume substreams; the payload
#: follows the record and is passed to `zfs receive` untouched.
DMU_SUBSTREAM = 1
DMU_COMPOUNDSTREAM = 2


class StreamHeader(NamedTuple):
    """The fields of a stream's `DRR_BEGIN` record the server acts on."""
    kind: str            # 'filesystem' or 'volume' (the stream's object-set type)
    to_guid: int
    from_guid: int       # 0 for a full stream
    versioninfo: int
    to_name: str         # `pool/ds@snap` as the sender wrote it; for logging only
    big_endian: bool


class StreamHeaderError(ValueError):
    """A stream's first record cannot be accepted (`parse_stream_header`, or a
    stream that ended / stalled before it was complete). The text is the
    reply to the client; never contains stream bytes beyond what is named."""


def parse_stream_header(data: bytes) -> StreamHeader:
    """Parse the leading `dmu_replay_record` of a `zfs send` stream (see
    `DRR_RECORD_SIZE`). Raises `StreamHeaderError` for fewer than
    `DRR_RECORD_SIZE` bytes, a magic that is not `DMU_BACKUP_MAGIC` in either
    byte order, a first record that is not `DRR_BEGIN`, a compound stream
    (`zfs send -p`, `-R`, `-I`: the client sends none of these), or an object
    set that is neither a filesystem nor a volume. Does not look at the
    payload that may follow the record."""
    if len(data) < DRR_RECORD_SIZE:
        raise StreamHeaderError(
            f"the stream ended after {len(data)} bytes, before its {DRR_RECORD_SIZE}-byte header"
        )
    if struct.unpack_from('<Q', data, 8)[0] == DMU_BACKUP_MAGIC:
        order = '<'
    elif struct.unpack_from('>Q', data, 8)[0] == DMU_BACKUP_MAGIC:
        order = '>'
    else:
        raise StreamHeaderError("the data is not a zfs send stream (bad magic)")
    layout = struct.Struct(order + 'II' + 'QQQ' + 'II' + 'QQ' + '256s')
    assert layout.size == DRR_RECORD_SIZE
    (drr_type, _payloadlen, _magic, versioninfo, _creation, objset, _flags, to_guid, from_guid,
     to_name) = layout.unpack_from(data, 0)
    if drr_type != DRR_BEGIN:
        raise StreamHeaderError("the stream does not start with a begin record")
    hdrtype = versioninfo & 3
    if hdrtype == DMU_COMPOUNDSTREAM:
        raise StreamHeaderError(
            "compound streams (zfs send -p, -R or -I) are not accepted; "
            "send a plain stream of one snapshot"
        )
    if hdrtype != DMU_SUBSTREAM:
        raise StreamHeaderError(f"unsupported stream header type ({hdrtype})")
    if objset == DMU_OST_ZFS:
        kind = 'filesystem'
    elif objset == DMU_OST_ZVOL:
        kind = 'volume'
    else:
        raise StreamHeaderError(f"the stream holds an unsupported object set type ({objset})")
    name = to_name.split(b'\0', 1)[0].decode('utf-8', errors='replace')
    return StreamHeader(kind, to_guid, from_guid, versioninfo, name, order == '>')


def stream_base_problem(header: StreamHeader, base: 'StreamBase') -> Optional[str]:
    """Why the stream's real incremental source (`drr_fromguid`) disagrees with
    the base the client declared, or `None`. `decide_receive` decides `-F` from
    the DECLARED base, so a stream that is incremental from something else
    could destroy more than was checked. A declared guid must equal
    `drr_fromguid`; a declared name only (a client without `from_guid`) must
    meet a non-zero one; no declared base must meet 0 (a full stream)."""
    if base.guid is not None:
        expected = base.guid
    elif base.name is not None:
        if header.from_guid != 0:
            return None
        return f"the stream is a full stream, but the client named the incremental source {base.name!r}"
    else:
        expected = 0
    if header.from_guid == expected:
        return None
    if expected == 0:
        return (
            "the client declared a full stream, but the stream is incremental "
            f"(source guid {header.from_guid})"
        )
    if header.from_guid == 0:
        return f"the client declared the incremental source guid {expected}, but the stream is a full stream"
    return (
        f"the client declared the incremental source guid {expected}, but the stream's source "
        f"guid is {header.from_guid}"
    )


#: The configurations negotiate has accepted for server datasets that do not
#: exist yet are kept in memory until the first receive creates the dataset
#: (`create_app`'s `pending_configs`); at most this many are remembered, the
#: oldest evicted first. An evicted or lost (restart) entry only means the
#: client must negotiate again.
PENDING_CONFIGS_MAX = 4096

#: Seconds a pending configuration is honoured after the negotiate that made
#: (or last refreshed) it, on a monotonic clock. The entry is only needed for
#: the gap between negotiate and the start of the stream, which copies the
#: values at its start (and a cut receive leaves them as local properties on
#: the partial dataset). Every re-negotiate refreshes the deadline, so the
#: limit frees the children (`_ancestors_state`) only of a parent that STOPPED
#: negotiating (removed from the configuration, its client gone): a parent
#: whose first send keeps failing keeps negotiating and keeps its children
#: waiting (the client and the server both log that at WARNING). An expired
#: entry is ignored and dropped by every reader; a long full send is
#: unaffected.
PENDING_CONFIG_TTL = 600

#: A ZFS user-property value must be SHORTER than this many bytes
#: (OpenZFS `ZAP_MAXVALUELEN`, 8 KiB, the terminating NUL included). The
#: canonical configuration is stored as such a value, in negotiate and again
#: by `zfs receive -o`; a longer one is refused at negotiate (400), or the
#: negotiate would succeed for a dataset that does not exist yet and every
#: stream fail at the receive.
ZFS_USER_PROPERTY_MAXLEN = 8192

#: The text zfs gives when a receive would overwrite an existing dataset with
#: an encrypted stream, or overwrite an encrypted one (`-F` onto an existing
#: dataset under an encrypted target, a raw full onto any existing dataset).
_ENCRYPTED_OVERWRITE_MARKER = 'overwrite an unencrypted one with an encrypted one'

#: ERROR lines a protocol-refused peer (`who`: its address, or its client id
#: and address) can cause before the rest are DEBUG, and how many peers the
#: de-duplication remembers (the oldest is evicted).
PROTOCOL_ERROR_LINES_PER_PEER = 3
PROTOCOL_LOGGED_PEERS = 1024

#: 400 for a negotiate `config` that cannot be decoded or has the wrong shape.
#: Deliberately unspecific: decoder details stay in the server's log.
BAD_CONFIG_ERROR = 'config: not a valid encoded dataset configuration'

#: 400 for a negotiate `config` whose canonical form does not fit a ZFS user
#: property value (`ZFS_USER_PROPERTY_MAXLEN`): the client's log then says why.
CONFIG_TOO_LARGE_ERROR = 'config: too large to store as a ZFS property (the limit is 8 KiB)'

#: 400 when a value zfsbackup would store as a ZFS property is refused by
#: libzfseasy (`ValueError`, e.g. a control character zfs cannot frame).
BAD_PROPERTY_ERROR = 'a value in this request cannot be stored as a ZFS property'

#: Characters refused in a client dataset name before it reaches any `zfs`
#: argument or property value (`zfs list -H` frames rows with them).
_UNFRAMEABLE = frozenset('\n\r\t\0')

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

#: How long the end of a `/stream` session waits for simple-websocket's reader
#: thread to leave after our close frame (normally at once: it leaves on the
#: client's close reply, EOF or reset), and again after a forced
#: `shutdown(SHUT_RDWR)` of a silent peer (`_end_websocket`).
WS_READER_JOIN_TIMEOUT = 2.0


def _end_websocket(ws) -> None:
    """End a `/stream` session so that simple-websocket's reader thread has
    left the socket before the route returns. Werkzeug closes the connection
    right after the route (`shutdown_request`); a reader still inside
    `SSL_read` then would be reading a closed -- possibly already reused --
    file descriptor, and a silent peer would keep it there indefinitely.

    Sends our close frame (`ws.close()`; flask-sock's own later `close()`
    then finds the session closed and does nothing), waits up to
    `WS_READER_JOIN_TIMEOUT` for the reader, and if it is still blocked shuts
    the socket down at the OS level (a plain `socket.shutdown`: the
    `SSLSocket` is left as it is, so its TLS object is not pulled from under
    the reader) and waits once more. Bounded and quiet: never raises, and a
    reader that still does not leave is left to the connection's close.

    Under the API worker's server `ws.sock` is not the `SSLSocket` but its
    `workers._SessionSocket` wrapper (serialised writes; the server itself
    waits for the reader before it closes the connection), whose
    `force_eof()` is the same plain `socket.shutdown`."""
    with contextlib.suppress(Exception):
        ws.close()
    thread = getattr(ws, 'thread', None)
    if not isinstance(thread, threading.Thread) or thread is threading.current_thread():
        return
    with contextlib.suppress(RuntimeError):  # never started
        thread.join(WS_READER_JOIN_TIMEOUT)
    if not thread.is_alive():
        return
    sock = getattr(ws, 'sock', None)
    force_eof = getattr(sock, 'force_eof', None)
    if not callable(force_eof):
        if not isinstance(sock, socket.socket):
            return
        force_eof = functools.partial(socket.socket.shutdown, sock, socket.SHUT_RDWR)
    logger.debug("stream: the websocket reader is still waiting on a silent peer; "
                 "shutting the socket down")
    with contextlib.suppress(OSError):
        force_eof()
    with contextlib.suppress(RuntimeError):
        thread.join(WS_READER_JOIN_TIMEOUT)


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


def dedupe_store_warnings() -> Callable[[], None]:
    """Install `_RepeatFilter` on the config store's loggers and return the
    uninstall handle. Called by `ApiWorker.run` (the only worker that opens
    the database, for the trust poll), which calls the handle on every exit.

    Idempotent: a logger that already carries a `_RepeatFilter` is left alone
    and the handle does not remove it, so only the call that installed a
    filter can remove it. The handle itself is idempotent."""
    installed = []
    for name in _STORE_LOGGERS:
        lg = logging.getLogger(name)
        if not any(isinstance(f, _RepeatFilter) for f in lg.filters):
            flt = _RepeatFilter()
            lg.addFilter(flt)
            installed.append((lg, flt))

    def uninstall() -> None:
        while installed:
            lg, flt = installed.pop()
            lg.removeFilter(flt)

    return uninstall


def canonical_dataset_config(encoded) -> str:
    """The canonical `org.zfsbackup:config` value for a client's negotiate
    `config` (plan B-1a): decoded with `DatasetConfig.from_property`, checked
    for the shape the pruner relies on, and re-encoded with `to_property()`.
    Only this re-encoding is ever stored, so whatever the client sent -- odd
    base64, extra keys, control characters inside JSON strings -- the stored
    value is plain base64 that `zfs list -H` frames correctly and that
    decodes again.

    Raises `ValueError` (its message names the problem, never the input) when
    `encoded` is not a non-empty string, does not decode, or decodes to the
    wrong types: `name` a string, `recursive`/`enabled` booleans, durations
    finite and greater than zero, exactly as the client's own configuration
    requires (the decoder already refuses NaN and infinity),
    `remote[].destination` strings. A decoded remote `frequency` of 0 is
    already `None` (inherit), so only a negative one reaches the check. The
    canonical value must also fit a ZFS user property (`ZFS_USER_PROPERTY_MAXLEN`)."""
    if not isinstance(encoded, str) or not encoded:
        raise ValueError("missing or empty")
    try:
        cfg = DatasetConfig.from_property(encoded)
    except Exception as e:  # any decoder failure is the client's input
        raise ValueError(f"does not decode ({type(e).__name__})") from None
    problem = wire_config_problem(cfg)
    if problem is not None:
        raise ValueError(problem)
    canonical = cfg.to_property()
    if len(canonical) >= ZFS_USER_PROPERTY_MAXLEN:  # base64: ASCII, bytes == characters
        raise ValueError(CONFIG_TOO_LARGE_ERROR)
    DatasetConfig.from_property(canonical)  # must decode again (never fails)
    return canonical


def retention_horizon(config: DatasetConfig) -> Optional[timedelta]:
    """How far back `config` keeps snapshots: its longest `keep_for` (the
    pruner keeps nothing older than the largest expiry of any rule), or
    `None` -- forever -- for a config with no retention rules (the pruner
    prunes nothing then)."""
    rules = config.retention_rules
    return max(r.keep_for for r in rules) if rules else None


def format_horizon(horizon: Optional[timedelta]) -> str:
    """`'forever'` or the horizon as a short literal (`30d`, `36h`, `90s`):
    the largest of d/h/m/s that divides it exactly; a fractional horizon is
    shown in seconds with its full decimal value (never an exponent)."""
    if horizon is None:
        return 'forever'
    # Exact integer microseconds (a float has too few digits for a long horizon).
    micros = (horizon.days * 86400 + horizon.seconds) * 1_000_000 + horizon.microseconds
    whole, fraction = divmod(abs(micros), 1_000_000)
    sign = '-' if micros < 0 else ''
    if fraction:
        return f"{sign}{whole}.{fraction:06d}".rstrip('0') + 's'
    for unit, size in (('d', 86400), ('h', 3600), ('m', 60)):
        if whole >= size and whole % size == 0:
            return f"{sign}{whole // size}{unit}"
    return f"{sign}{whole}s"


def retention_shrink(
    stored: str, canonical_new: str
) -> Optional[Tuple[Optional[timedelta], Optional[timedelta]]]:
    """B-3-lite: `(old, new)` retention horizons (`retention_horizon`) when the
    config in `canonical_new` keeps snapshots for a SHORTER time than the
    stored `stored` (a finite horizon replacing a longer or unbounded one),
    else `None` (equal, longer). Raises `ValueError` when `stored` does not
    decode (the caller skips the comparison)."""
    try:
        old = retention_horizon(DatasetConfig.from_property(stored))
    except Exception as e:
        raise ValueError(f"stored config does not decode ({type(e).__name__})") from None
    new = retention_horizon(DatasetConfig.from_property(canonical_new))
    if new is not None and (old is None or new < old):
        return old, new
    return None


def client_dataset_problem(dataset) -> Optional[str]:
    """Why the client dataset name in a `/backup/<id>/<dataset>/...` URL is
    unusable, or `None`. It becomes `org.zfsbackup:source_dataset` verbatim,
    so a character that `zfs list -H` uses as a row or column separator is
    refused before any `zfs` call, and the whole name must be a valid
    filesystem name (`_server_dataset` drops its pool component, so checking
    only the derived server name would not cover it)."""
    if not isinstance(dataset, str) or not dataset:
        return "empty client dataset name"
    if any(c in _UNFRAMEABLE for c in dataset):
        return "client dataset name contains a control character"
    try:
        Filesystem(dataset)
    except Exception:
        return "invalid client dataset name"
    return None


_GUID_RE = re.compile(r'[0-9]{1,20}')


class StreamBase(NamedTuple):
    """What a `/stream` request says about its incremental base and the
    server snapshots the client allows the receive to destroy: `name` (`from`,
    the fallback of a client that sends no guid), `guid` (`from_guid`, the
    base's decimal guid; preferred when present) and `discard` (`discard`, a
    comma-separated list of decimal guids). All optional: a full stream has no
    base."""
    name: Optional[str] = None
    guid: Optional[int] = None
    discard: frozenset = frozenset()


def parse_stream_base(args: Mapping[str, str]) -> StreamBase:
    """The `StreamBase` of a `/stream` request's query arguments. Raises
    `ValueError` (its text is the reply) for a `from_guid` or `discard` that is
    not decimal guids, or a `discard` with more than `DISCARD_MAX` of them."""
    name = args.get('from') or None
    raw_guid = args.get('from_guid')
    guid = None
    if raw_guid is not None:
        if _GUID_RE.fullmatch(raw_guid) is None:
            raise ValueError("stream parameter from_guid: not a decimal snapshot guid")
        guid = int(raw_guid)
    raw_discard = args.get('discard')
    discard = set()
    if raw_discard:
        parts = raw_discard.split(',')
        if len(parts) > DISCARD_MAX:
            raise ValueError(f"stream parameter discard: more than {DISCARD_MAX} guids")
        for part in parts:
            if _GUID_RE.fullmatch(part) is None:
                raise ValueError("stream parameter discard: not a comma-separated list of decimal guids")
            discard.add(int(part))
    return StreamBase(name, guid, frozenset(discard))


def read_stream_header(ws, timeout: float) -> bytes:
    """Read the websocket until at least `DRR_RECORD_SIZE` stream bytes have
    arrived and return everything read so far (the header and whatever came
    with it; the caller feeds all of it to `zfs receive` unchanged). A text
    message other than `done` is ignored, as in the main receive loop; `done`
    (or a close) before the header is complete raises `StreamHeaderError`.
    `ws.receive` returning `None` is a stall: `TimeoutError`, like the main
    loop. `ConnectionClosed` propagates."""
    buf = b''
    while len(buf) < DRR_RECORD_SIZE:
        data = ws.receive(timeout=timeout)
        if data is None:
            raise TimeoutError(f"no data from the client for {timeout}s")
        if isinstance(data, str):
            try:
                done = bool(json.loads(data).get('done'))
            except (ValueError, AttributeError):
                done = False
            if done:
                raise StreamHeaderError(
                    f"the stream ended after {len(buf)} bytes, before its "
                    f"{DRR_RECORD_SIZE}-byte header"
                )
            continue
        buf += data
    return buf


class ServerSnapshot(NamedTuple):
    """A snapshot of a server dataset as `server_snapshots` lists it."""
    name: str                # short name
    txg: Optional[int]       # createtxg
    guid: Optional[int]


def server_snapshots(server_fs, *, strict: bool = True) -> List[ServerSnapshot]:
    """The direct snapshots of `server_fs`, with `createtxg` and `guid` (None
    where zfs did not report one). Raises if zfs fails. `strict` (the default)
    also raises when libzfseasy would have skipped a row it cannot trust (a
    snapshot name it cannot parse, say): a decision about what a receive may
    destroy must not rest on a listing that silently lacks a snapshot."""
    return [
        ServerSnapshot(s.short, property_int(s, 'createtxg'), property_int(s, 'guid'))
        for s in zfs.list(
            roots=server_fs, types='snapshot', properties=['createtxg', 'guid'],
            strict=strict,
        )
    ]


def _rollback_range(names: List[str]) -> str:
    """`first` or `first…last` for a list of snapshot names, oldest first."""
    return names[0] if len(names) == 1 else f"{names[0]}\u2026{names[-1]}"


class ReceiveDecision(NamedTuple):
    """Whether, and how, a stream may be received into an existing server
    dataset (`decide_receive`). `reason` is `None` to proceed, else the
    refusal (`base_missing`, `server_newer`, `no_common`) with `message` as its
    text; `force` is whether `zfs receive` gets `-F`; `rolled_back` names the
    server snapshots that `-F` will destroy because the client allowed it."""
    force: bool
    reason: Optional[str] = None
    message: str = ''
    rolled_back: Tuple[str, ...] = ()


def decide_receive(
    server_dataset: str, base: StreamBase, rows: List[ServerSnapshot]
) -> ReceiveDecision:
    """Decide `-F` for a receive into the EXISTING server dataset holding
    `rows` (its direct snapshots). `-F` on an incremental receive destroys
    every server snapshot newer than the base, so it is only passed when that
    destroys nothing, or when the client named exactly those snapshots by
    guid in `discard`. "Newer" is by `createtxg`, never by name. Raises
    `ValueError` when a snapshot has no `createtxg` (it cannot be ordered:
    the caller fails closed).

    - no snapshots (a placeholder, or a first receive that was cut): `-F`;
    - a base the server does not hold (by guid when given, else by name):
      refused, `base_missing`;
    - the base is the newest: `-F` (it discards changes to the head only);
    - snapshots newer than the base, all in `discard`: `-F`, those are
      rolled back; otherwise refused, `server_newer`;
    - no base (a full stream) but the copy holds snapshots: refused,
      `no_common` (zfs refuses a full stream over them anyway)."""
    if any(r.txg is None for r in rows):
        raise ValueError("a server snapshot has no createtxg")
    if not rows:
        return ReceiveDecision(True)
    rename = (
        f"`zfs rename {server_dataset} <a name outside the backup target>` "
        "(the next cycle sends a full stream; see README 'Renaming a server copy aside')"
    )
    if base.guid is None and base.name is None:
        return ReceiveDecision(False, 'no_common', (
            f"{server_dataset} already holds snapshots and a full stream cannot replace "
            f"them, so nothing is received. On the server: {rename}"
        ))
    if base.guid is not None:
        found = next((r for r in rows if r.guid == base.guid), None)
    else:
        found = next((r for r in rows if r.name == base.name), None)
    if found is None:
        what = f"guid {base.guid}" if base.guid is not None else repr(base.name)
        return ReceiveDecision(False, 'base_missing', (
            f"{server_dataset} does not hold the incremental source snapshot ({what}), "
            "so nothing is received. This can be transient (the server's pruner may have "
            "removed it since negotiation; the next cycle negotiates again). If it "
            f"persists, on the server: {rename}"
        ))
    newer = sorted((r for r in rows if r.txg > found.txg), key=lambda r: r.txg)
    if not newer:
        return ReceiveDecision(True)
    names = [r.name for r in newer]
    if all(r.guid is not None and r.guid in base.discard for r in newer):
        return ReceiveDecision(True, rolled_back=tuple(names))
    return ReceiveDecision(False, 'server_newer', (
        f"{server_dataset} holds {len(names)} snapshot(s) newer than {found.name} "
        f"({_rollback_range(names)}) that the client did not allow to be destroyed, "
        "so nothing is received (receiving would destroy them). To keep them, on the "
        f"server: {rename}. If they are expendable: "
        f"`zfs destroy {server_dataset}@{names[0]}"
        + (f"%{names[-1]}" if len(names) > 1 else "")
        + "` (the next cycle continues)"
    ))


def newer_than(server_dataset: str, common: str, snaps: List[ServerSnapshot]):
    """The server snapshots newer (by `createtxg`) than the snapshot named
    `common`, oldest first, as `{"name", "guid"}` (the guid a decimal
    string, `None` if unknown); `None` when they cannot be told (a
    snapshot has no `createtxg`: logged, and the stream, which decides
    for itself, fails closed)."""
    if any(s.txg is None for s in snaps):
        logger.warning(
            "negotiate for %s: a server snapshot has no createtxg; "
            "not reporting server_newer", server_dataset,
        )
        return None
    base = next(s for s in snaps if s.name == common)
    return [
        {'name': s.name, 'guid': None if s.guid is None else str(s.guid)}
        for s in sorted((s for s in snaps if s.txg > base.txg), key=lambda s: s.txg)
    ]


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
       `/health` is also exempt from the protocol check; `/enroll` is not
       (426 as in 3a).
    2. No client certificate (`SSL_CLIENT_CERT` absent) -> 401
       `{"error": "client certificate required"}`. Also for an unknown URL:
       routing errors are answered only to authenticated clients.
    3. The certificate's SHA-256 (DER) fingerprint is not pinned, or the pin's
       `not_after` has passed -> 403 `{"error": "client not trusted"}`.
    3a. No `X-Zfsbackup-Protocol` header, or a version not in
       `SUPPORTED_PROTOCOLS` -> 426 `{"error", "reason": "protocol",
       "protocol": <this server's version>, "supported": [...]}`, with the
       header on the response (plan S-5; `zfsbackup.protocol`). No
       `Upgrade` header: the protocol is not an HTTP upgrade, and the
       request may itself be a websocket upgrade.
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
    #: The server datasets a `/stream` is working on right now, claimed
    #: non-blocking around its check -> abort -> create -> receive sequence:
    #: two streams for the same dataset (a client retry after a timeout, say)
    #: must not race one client's abort against the other's receive. A name is
    #: in the set from the claim until the `zfs receive` child has exited (or
    #: until the stream is refused), and is released BEFORE the final reply
    #: (the `ok` or the error), so a client that answers at once with its next
    #: stream is not told `busy`. The set never grows beyond the number of
    #: concurrent streams.
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

    #: `server dataset -> (canonical config, client dataset name)` for datasets
    #: negotiate has accepted but nothing has received yet. Such a dataset does
    #: not exist: the first full stream CREATES it (zfs refuses a raw full, or
    #: a plain full under an encrypted target, onto a pre-created one), so
    #: there is nowhere on disk to keep its configuration until then. The
    #: stream takes the config from the dataset's own local property when it
    #: exists, from here when it does not. Bounded (`PENDING_CONFIGS_MAX`),
    #: dropped once a local config exists, lost on a restart of this worker --
    #: a client always negotiates right before it streams, so a missing entry
    #: means "negotiate again".
    pending_configs: 'collections.OrderedDict[str, Tuple[str, str]]' = collections.OrderedDict()
    pending_guard = threading.Lock()

    #: `server dataset -> monotonic deadline` of the entries of `pending_configs`
    #: (a separate map so the entries keep their `(config, dataset)` shape).
    pending_deadlines: dict = {}

    def _drop_pending_locked(server_dataset: str) -> None:
        pending_configs.pop(server_dataset, None)
        pending_deadlines.pop(server_dataset, None)

    def _store_pending_locked(server_dataset: str, canonical: str, dataset: str) -> None:
        pending_configs[server_dataset] = (canonical, dataset)
        pending_configs.move_to_end(server_dataset)
        pending_deadlines[server_dataset] = time.monotonic() + PENDING_CONFIG_TTL
        while len(pending_configs) > PENDING_CONFIGS_MAX:
            _drop_pending_locked(next(iter(pending_configs)))

    def _live_pending_locked(server_dataset: str) -> Optional[Tuple[str, str]]:
        """The entry for the name unless it has expired (then it is dropped)."""
        entry = pending_configs.get(server_dataset)
        if entry is None:
            return None
        if pending_deadlines.get(server_dataset, 0.0) <= time.monotonic():
            _drop_pending_locked(server_dataset)
            return None
        return entry

    def _claim_pending(server_dataset: str, canonical: str, dataset: str) -> Optional[str]:
        """Check and record in ONE acquisition of the lock: -> `None` once
        `(canonical, dataset)` is recorded for the name (a re-negotiate of the
        same client dataset refreshes its deadline), or the OTHER client
        dataset that already has a live entry."""
        with pending_guard:
            entry = _live_pending_locked(server_dataset)
            if entry is not None and entry[1] != dataset:
                return entry[1]
            _store_pending_locked(server_dataset, canonical, dataset)
            return None

    def _unclaim_pending(server_dataset: str, canonical: str, dataset: str) -> None:
        """Remove the entry `_claim_pending` inserted, if it is still ours."""
        with pending_guard:
            if pending_configs.get(server_dataset) == (canonical, dataset):
                _drop_pending_locked(server_dataset)

    def _pending(server_dataset: str) -> Optional[Tuple[str, str]]:
        with pending_guard:
            return _live_pending_locked(server_dataset)

    def _forget_pending(server_dataset: str) -> None:
        with pending_guard:
            _drop_pending_locked(server_dataset)

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
        `_BadDatasetName` (the routes answer 400) when the client dataset
        name (`client_dataset_problem`) or the result is not a valid
        filesystem name, so no `zfs` command ever sees it."""
        parts = client_dataset.split('/', 1)
        relative = parts[1] if len(parts) > 1 else ''
        name = f"{target}/{client_id}/{relative}" if relative else f"{target}/{client_id}"
        try:
            Filesystem(name)
        except Exception as e:
            raise _BadDatasetName(f"invalid dataset name {name!r}: {e}") from None
        problem = client_dataset_problem(client_dataset)
        if problem is not None:
            # `!r`: the name may hold the very control character refused.
            raise _BadDatasetName(f"invalid dataset name {client_dataset!r}: {problem}")
        return name

    def _server_copy_kind(server_fs, client_id: str) -> str:
        """`'raw'` or `'plain'` for a server dataset that holds snapshots (the
        caller answers `'none'` for one that does not): raw if it is encrypted
        (`encryption != off`) and is its own encryption root, which is what a
        raw receive of one dataset produces (`keystatus` is `unavailable`); a
        plain receive into an encrypted target inherits the root of the TARGET
        (or of its encrypted parent), and an unencrypted dataset has none.
        (A root elsewhere under `<target>/<id>/` only comes from a `-R` tree,
        which the client never sends; counting it would call a plain child of
        a raw parent raw.) Raises if zfs fails."""
        encryption = zfs.get.value(server_fs, 'encryption')
        if encryption in (None, 'off'):
            return 'plain'
        return 'raw' if zfs.get.value(server_fs, 'encryptionroot') == server_fs.name else 'plain'

    def _receive_error(e: Exception, server_dataset: str) -> str:
        """The error text sent to the client for a failed receive: zfs's own
        message, except for the refusal to overwrite onto an existing dataset
        with an encrypted stream, which has two causes the client can act on
        (a placeholder parent made for another dataset, or a server copy
        whose encryption does not match the stream: see `server_copy`)."""
        text = str(e)
        if _ENCRYPTED_OVERWRITE_MARKER in text:
            logger.error("receive into %s refused: %s", server_dataset, text)
            return (
                f"{server_dataset} already exists on the server and cannot receive a full "
                "encrypted stream (or a full stream under an encrypted target) over it. "
                "It is either the parent that was created as a placeholder when a dataset "
                "below it was backed up first (back the parent up before its children, or "
                "rename the placeholder aside on the server; renaming it moves the server "
                "copies of its children too, so they are sent in full again), or a copy of "
                "the other kind (plain vs raw): on the server, `zfs rename` it aside; the "
                "next cycle sends a full stream. " + text
            )
        return text

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

    #: `(server dataset, reason)` pairs whose stream refusal (`refuse_stream`)
    #: was already announced at WARNING; forgotten when a stream of the
    #: dataset succeeds. Bounded.
    _refused_noted: set = set()

    #: Datasets already announced at WARNING as unowned (once each, bounded):
    #: a client negotiates every cycle, and the refusal repeats until the
    #: operator acts.
    _unowned_noted: set = set()

    def _owner_problem(
        server_dataset: str, local_id: Optional[str], client_id: str
    ) -> Optional[str]:
        """The 409 text for an EXISTING server dataset whose LOCAL
        `org.zfsbackup:client_id` is `local_id` (`None`: not set locally), or
        `None` when it is this client's. A dataset with no local id was not
        made by a negotiate or a receive of this server (an operator's own
        `<target>/<id>`, a dataset moved in, a received-only copy of the
        property): it is never adopted silently (plan B-2c, because the full
        `-F` receive would overwrite it and the pruner would take over its
        snapshots); the message names the one command that adopts it on
        purpose."""
        if local_id is None:
            if server_dataset not in _unowned_noted:
                if len(_unowned_noted) > 1024:
                    _unowned_noted.clear()
                _unowned_noted.add(server_dataset)
                logger.warning(
                    "refusing to use %s: it exists with no local %s, so it was not created "
                    "for a backup; to adopt it on purpose run: zfs set %s=%s %s",
                    server_dataset, PROP_CLIENT_ID, PROP_CLIENT_ID, client_id,
                    shlex.quote(server_dataset),
                )
            return (
                f"server dataset {server_dataset} already exists on the server and is not "
                f"recorded as a backup ({PROP_CLIENT_ID} is not set on it); refusing to "
                "adopt it. If it is meant to hold this client's backups, the server "
                f"operator can adopt it on purpose: zfs set {PROP_CLIENT_ID}={client_id} "
                f"{shlex.quote(server_dataset)}"
            )
        if local_id != client_id:
            # The other id is not named: it is not this client's business.
            return (
                f"server dataset {server_dataset} is recorded as another "
                f"client's ({PROP_CLIENT_ID} differs from your bound client id "
                f"{client_id!r}); refusing to use it. The server operator must "
                "resolve this"
            )
        _unowned_noted.discard(server_dataset)
        return None

    def _ownership_conflict(
        server_dataset: str, dataset: str, client_id: str
    ) -> Optional[str]:
        """An error message (the routes answer 409) when `server_dataset`
        exists and is not this client dataset's, else `None`:

        - it records a different client id (`org.zfsbackup:client_id`) than
          the bound `client_id` -- the server namespace is the binding's, and
          a dataset stamped for another id (an operator rename, a changed
          host id) is never written through;
        - it has NO local client id (plan B-2c): an existing dataset nothing
          here made is not adopted (`_owner_problem`). Every dataset a
          negotiate or a receive creates carries a local id from the start
          (`zfs create -o`, `zfs receive -o`, also while a first receive is
          only partial), so this only meets datasets that came from
          elsewhere;
        - it records a different client dataset as its source.
          `_server_dataset` drops the pool, so `tank/data` and `backup/data`
          map to the same server dataset and would otherwise silently share
          (and overwrite) it.

        A dataset without a source-dataset property (a container negotiate
        made, never negotiated itself) is not a conflict for that property.
        Raises if the dataset's properties cannot be read."""
        ds = Dataset(server_dataset)
        if not zfs.exists(ds):
            return None
        props = read_zfsbackup_properties(ds)
        recorded_id = props.get(PROP_CLIENT_ID)
        if recorded_id and recorded_id != client_id:
            return _owner_problem(server_dataset, recorded_id, client_id)
        problem = _owner_problem(
            server_dataset, zfs.get.value(ds, PROP_CLIENT_ID, sources='local'), client_id
        )
        if problem is not None:
            return problem
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

    #: Per peer (`who`), the protocol versions whose refusal was already
    #: logged at ERROR. At most `PROTOCOL_ERROR_LINES_PER_PEER` lines per peer
    #: (a legitimate client upgrading through a couple of versions still gets
    #: told), later ones are DEBUG: the key is the peer, never the header
    #: value, which an unauthenticated peer (`/enroll`) chooses freely. The
    #: table is bounded by evicting the OLDEST peer, so a flood of new
    #: addresses can only make the server forget one peer at a time, never
    #: reset them all at once.
    protocol_logged: 'collections.OrderedDict[str, set]' = collections.OrderedDict()
    protocol_guard = threading.Lock()

    def _protocol_refusal(who: str):
        """None if the request names a supported protocol version
        (`X-Zfsbackup-Protocol`), else the 426 response (plan S-5). Logged
        at ERROR for the first `PROTOCOL_ERROR_LINES_PER_PEER` distinct
        versions a peer sends, then at DEBUG."""
        raw = request.headers.get(PROTOCOL_HEADER)
        version = parse_protocol_header(raw)
        if version in SUPPORTED_PROTOCOLS:
            return None
        sent = 'no protocol version' if raw is None else f'protocol version {raw[:16]!r}'
        version_key = None if raw is None else raw[:16]
        with protocol_guard:
            seen = protocol_logged.get(who)
            if seen is None:
                if len(protocol_logged) >= PROTOCOL_LOGGED_PEERS:
                    protocol_logged.popitem(last=False)
                seen = protocol_logged[who] = set()
            first = (version_key not in seen
                     and len(seen) < PROTOCOL_ERROR_LINES_PER_PEER)
            if first:
                seen.add(version_key)
        (logger.error if first else logger.debug)(
            "API: refusing %s %s from %s: it sent %s, this server speaks protocol %s "
            "(426); upgrade the client and the server together",
            request.method, request.path, who, sent,
            ', '.join(str(v) for v in sorted(SUPPORTED_PROTOCOLS)),
        )
        response = jsonify({
            'error': (
                f'unsupported zfsbackup protocol: the request sent {sent}; this server '
                f'speaks protocol {PROTOCOL_VERSION}. Upgrade the client and the server '
                'to the same zfsbackup release'
            ),
            'reason': PROTOCOL_REFUSAL_REASON,
            'protocol': PROTOCOL_VERSION,
            'supported': sorted(SUPPORTED_PROTOCOLS),
        })
        response.status_code = 426
        response.headers[PROTOCOL_HEADER] = str(PROTOCOL_VERSION)
        return response

    @app.before_request
    def _authenticate():
        """Route authentication and the protocol check; see `create_app`.
        Returning a response refuses the request (for `/stream`: the
        websocket upgrade)."""
        if request.endpoint in PROTOCOL_EXEMPT_ENDPOINTS:
            return None
        if request.endpoint in UNAUTHENTICATED_ENDPOINTS:
            return _protocol_refusal(request.remote_addr or '?')
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
        # After authentication, so an unauthenticated caller learns nothing
        # but 401/403; before anything with a side effect (the predecessor
        # retirement below, the route).
        refused = _protocol_refusal(f"client {pin.client_id} ({request.remote_addr})")
        if refused is not None:
            return refused
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
        after the whole response has been written and flushed and its drain
        of any unread request body has ended (at most `API_DRAIN_TIMEOUT`,
        `workers.py`) -- not on the next poll. Either way no request waits
        without limit for another thread, so a client that got a committed write's answer
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
        SHA-256, 64 lowercase hex>, "protocol": <this server's protocol
        version>}`. A client refuses a server whose answer lacks `protocol`
        or names one it does not support. 401/403/426 as for every
        authenticated endpoint. Not gated: it answers while the worker
        drains, and with remote backup disabled."""
        return jsonify({
            'client_id': g.client_id,
            'fingerprint': g.fingerprint,
            'protocol': PROTOCOL_VERSION,
        })

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

        - 200 `{"status": "enrolled", "client_id", "fingerprint", "protocol"}`
          (`protocol`: this server's wire protocol version, so the enrolling
          CLI can detect an incompatible server before its first backup): pinned
          now; the token is spent. The certificate works on the next
          connection (the trust view is refreshed, bounded, before this
          response; if that cannot run in time, right after it is sent --
          then a client connecting at once may be refused at the handshake
          for a moment and should retry once; see `_refresh_after_commit`).
          A `--replace` token scoped to this id also answers `enrolled` for
          a certificate that was already pinned to it: the token is spent
          and the client's OTHER pins are dropped.
        - 200 `{"status": "already_trusted", "client_id", "fingerprint",
          "protocol"}`:
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
        - 426 `{"error", "reason": "protocol", "protocol", "supported"}`:
          no `X-Zfsbackup-Protocol` header or an unsupported version; checked
          before everything else, the body is not read.
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
            return jsonify({
                'status': 'enrolled', 'client_id': client_id, 'fingerprint': cert_fp,
                'protocol': PROTOCOL_VERSION,
            })
        if outcome == EnrollOutcome.ALREADY_TRUSTED:
            logger.info(
                "enrollment from %s: certificate %s is already pinned to client %s "
                "(token not used)", remote, cert_fp, client_id,
            )
            return jsonify({
                'status': 'already_trusted', 'client_id': client_id, 'fingerprint': cert_fp,
                'protocol': PROTOCOL_VERSION,
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
          <new_fp>, "protocol": <this server's protocol version>}`: the
          successor is pinned (`already`: it was, by an earlier identical
          rotation). Both certificates work until the
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
                'protocol': PROTOCOL_VERSION,
            })
        return failed('refused by the trust store', (jsonify(ROTATE_REFUSED_ERROR), 403))

    def _mix_message(server_dataset: str, recorded: str, dataset: str) -> str:
        return (
            f"server dataset {server_dataset} already holds backups of "
            f"{recorded!r}; refusing to mix in {dataset!r}"
        )

    def _is_claimed(name: str) -> bool:
        with streams_guard:
            return name in streams_in_progress

    def _ancestors_state(
        server_dataset: str, client_id: str
    ) -> Tuple[Optional[Tuple[str, str]], list, Optional[str]]:
        """`(unsettled, missing, conflict)` for the ancestors of
        `server_dataset` up to `<target>/<id>`. `unsettled` is `(ancestor,
        kind)` for the nearest ancestor a child must not be created under
        yet, or `None`: `'claimed'` (a stream or negotiate is working on it),
        `'pending'` (negotiated, first receive not completed), or `'partial'`
        (a NEW partial first receive: a `receive_resume_token` and no
        snapshot yet; a child under it would make the later `zfs receive -A`
        fail with "has children". An ancestor that already has snapshots, a
        cut incremental, does not block). `missing` lists the ancestors that
        do not exist (negotiate creates them, one level at a time), nearest
        first. `conflict` (plan B-2c) is the 409 text for the nearest EXISTING
        ancestor that is not this client's container -- no local client id
        (an operator's own `<target>/<id>` or a dataset below it) or another
        client's -- else `None`. Ownership is read per ancestor, walking up
        from the leaf's parent: an existing ancestor is checked for an owner
        BEFORE its token, but an ancestor that is claimed or pending returns
        first (`unsettled`), so the owner of that ancestor and of those above
        it is not read until it settles. One
        `zfs get` per existing ancestor for the owner, plus one more for the
        token and one listing for one that has a token. Raises if zfs
        fails."""
        base = f"{target}/{client_id}"
        ancestor = server_dataset.rsplit('/', 1)[0]
        missing = []
        while ancestor == base or ancestor.startswith(base + '/'):
            if _is_claimed(ancestor):
                return (ancestor, 'claimed'), missing, None
            if _pending(ancestor) is not None:
                return (ancestor, 'pending'), missing, None
            ds = Dataset(ancestor)
            if zfs.exists(ds):
                conflict = _owner_problem(
                    ancestor, zfs.get.value(ds, PROP_CLIENT_ID, sources='local'), client_id
                )
                if conflict is not None:
                    return None, missing, conflict
                if (
                    zfs.get.value(ds, 'receive_resume_token') is not None
                    and not zfs.list(roots=ds, types='snapshot', properties=['creation'])
                ):
                    return (ancestor, 'partial'), missing, None
            else:
                missing.append(ancestor)
            if ancestor == base:
                break
            ancestor = ancestor.rsplit('/', 1)[0]
        return None, missing, None

    #: Ancestors already announced at WARNING as holding up their children
    #: (once each; forgotten when a child gets past them, so a later episode
    #: is announced again). Bounded by the datasets of the clients.
    _unsettled_noted: set = set()

    def _note_unsettled(client_id: str, dataset: str, ancestor: str, kind: str) -> None:
        """Log why `ancestor` holds up a child's negotiate: WARNING once per
        ancestor and episode, naming the remedy; DEBUG after. A claimed
        ancestor is a passing busy state (DEBUG only)."""
        if kind == 'claimed':
            logger.debug("negotiate from client %s for %r: %s is busy", client_id, dataset, ancestor)
            return
        if ancestor in _unsettled_noted:
            logger.debug("negotiate from client %s for %r: still waiting for %s",
                         client_id, dataset, ancestor)
            return
        if len(_unsettled_noted) > 1024:
            _unsettled_noted.clear()
        _unsettled_noted.add(ancestor)
        if kind == 'partial':
            logger.warning(
                "children of %s wait: it holds the partial state of a first receive (a "
                "receive_resume_token and no snapshot); it completes with the client's next "
                "stream, or discard it with `zfs receive -A %s`", ancestor, ancestor,
            )
        else:
            logger.warning(
                "children of %s wait: its first backup has not completed (it negotiated but "
                "nothing was received yet); they follow once it has", ancestor,
            )

    def _note_retention_shrink(
        client_id: str, dataset: str, server_dataset: str, server_fs, canonical_new: str
    ) -> None:
        """B-3-lite (plan DD2): before negotiate overwrites the stored config,
        compare the longest retention horizon of the stored canonical config
        with the new one and log ONE WARNING when it shrinks (old -> new).
        Never refuses and never raises: the server applies the client's
        retention verbatim (a client admin's typo would make the next
        pruning cycle destroy history), so this is only the operator's
        notice. A stored config that cannot be read or decoded is skipped
        (DEBUG). The comparison is against the config now stored; after the
        overwrite the shrink is the stored state, so a repeat negotiate with
        the same config warns no more."""
        try:
            stored = zfs.get.value(server_fs, PROP_CONFIG, sources='local')
            if stored is None:
                return
            shrink = retention_shrink(stored, canonical_new)
        except Exception as e:
            logger.debug(
                "negotiate from client %s for %r: not comparing the retention of %s: %s",
                client_id, dataset, server_dataset, e,
            )
            return
        if shrink is not None:
            logger.warning(
                "negotiate from client %s for %r: the retention horizon of %s shrinks from %s "
                "to %s; the server applies the client's retention as sent, so snapshots older "
                "than %s are pruned from the next pruning cycle",
                client_id, dataset, server_dataset, format_horizon(shrink[0]),
                format_horizon(shrink[1]), format_horizon(shrink[1]),
            )

    def _negotiate_claimed(
        client_id: str, dataset: str, server_dataset: str, client_snapshots: list,
        ds_config_canonical: str,
    ) -> object:
        """The body of negotiate, run with `server_dataset` claimed."""
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

        # A dataset that is missing -- or exists without a recorded SOURCE
        # dataset (a placeholder parent a child's negotiate created; it carries
        # the client id, B-2c) -- may already be spoken for by
        # another client dataset that negotiated first and has not received
        # yet: same check, same message as for an owned one. (A recorded
        # source dataset is `_ownership_conflict`'s.)
        earlier = _pending(server_dataset)
        if earlier is not None and earlier[1] != dataset:
            try:
                owned_already = server_fs_exists and bool(
                    read_zfsbackup_properties(Dataset(server_dataset)).get(PROP_SOURCE_DATASET)
                )
            except Exception as e:
                return jsonify({'error': f'Failed to check {server_dataset}: {e}'}), 500
            if not owned_already:
                return jsonify({'error': _mix_message(server_dataset, earlier[1], dataset)}), 409

        # A child must not create, with `create -p`, an ancestor that has not
        # settled: one a stream is working on, one negotiated but not received
        # yet (a placeholder would break its own full stream), or one with a
        # cut first receive (`receive_resume_token` on the ancestor itself; a
        # child under it would make the later `zfs receive -A` fail with "has
        # children"). The ancestor settles first, the child follows next cycle.
        missing_ancestors: list = []
        if not server_fs_exists and not config.dry_run:
            try:
                unsettled, missing_ancestors, ancestor_conflict = _ancestors_state(
                    server_dataset, client_id
                )
            except Exception as e:
                logger.error("negotiate from client %s: cannot check the parents of %s: %s",
                             client_id, server_dataset, e)
                return jsonify({'error': f'Failed to check the parents of {server_dataset}'}), 500
            if ancestor_conflict is not None:
                return jsonify({'error': ancestor_conflict}), 409
            if unsettled is not None:
                ancestor, kind = unsettled
                _note_unsettled(client_id, dataset, ancestor, kind)
                return jsonify({
                    'error': f'{ancestor} has not settled yet; retry later', 'retry': True,
                    'reason': 'unsettled_ancestor', 'ancestor': ancestor,
                }), 503
            _unsettled_noted.difference_update(
                a for a in list(_unsettled_noted) if server_dataset.startswith(a + '/')
            )

        # Store the canonical client config. On an existing dataset it is the
        # local `org.zfsbackup:config` property. A dataset that does not exist
        # is NOT created: the first full stream creates it (zfs refuses a raw
        # full, or a plain full under an encrypted target, onto a pre-created
        # dataset), so only its parent is made here and the config waits in
        # `pending_configs`. Always: an empty config is refused above. The
        # stream handler hands the config to `zfs receive -o`, so a copy of
        # the property inside a `-p` stream is never the one in effect.
        if not config.dry_run:
            try:
                if server_fs_exists:
                    _note_retention_shrink(
                        client_id, dataset, server_dataset, server_fs, ds_config_canonical
                    )
                    zfs.set(server_fs, {
                        PROP_CONFIG: ds_config_canonical,
                        PROP_CLIENT_ID: client_id,
                        PROP_SOURCE_DATASET: dataset,
                    })
                    _forget_pending(server_dataset)
                else:
                    # The ancestors the `create -p` will make are claimed for
                    # its duration, like the dataset itself: a concurrent
                    # negotiate or stream of one of them (or of a sibling
                    # that would create it too) gets busy/retry.
                    claimed_ancestors = []
                    try:
                        for ancestor in missing_ancestors:
                            if not _claim_dataset(ancestor):
                                return jsonify(BUSY_ERROR), 503
                            claimed_ancestors.append(ancestor)
                        # Claimed in one step, BEFORE the slow create, so two
                        # negotiates for one missing name cannot both win.
                        rival = _claim_pending(server_dataset, ds_config_canonical, dataset)
                        if rival is not None:
                            return jsonify({'error': _mix_message(server_dataset, rival, dataset)}), 409
                        try:
                            # Only the parent chain, never the dataset itself,
                            # and one level at a time, outermost first (no `-p`:
                            # only a name this pass checked as missing is ever
                            # created, so a dataset that appeared meanwhile is a
                            # failure, not adopted). Each one is stamped with the
                            # local `client_id` (B-2c) and never mounted (B-2a).
                            # Limitation: this makes `<target>/<id>` a plain
                            # placeholder for the first non-root dataset, so a raw
                            # backup of the client's pool-root dataset received
                            # after another one meets the encrypted-overwrite
                            # refusal (`_receive_error` explains it).
                            # Filesystems only (no size, no positional argument:
                            # `zfs.create` never dispatches to `create -V`; see the
                            # note at the receive about volumes).
                            for ancestor in reversed(missing_ancestors):
                                zfs.create(
                                    Filesystem(ancestor),
                                    properties={
                                        'canmount': 'noauto',
                                        PROP_CLIENT_ID: client_id,
                                    },
                                    mount=False,
                                )
                        except BaseException:
                            _unclaim_pending(server_dataset, ds_config_canonical, dataset)
                            raise
                    finally:
                        for ancestor in claimed_ancestors:
                            _release_dataset(ancestor)
            except ValueError as e:
                # libzfseasy refused a value before running zfs (B1: a control
                # character zfs could not frame). The values are the canonical
                # config, the A7 client id and a checked name, so this is a
                # safety net; the details stay in the log.
                logger.warning(
                    "negotiate from client %s for %r: libzfseasy refused a property "
                    "value: %r", client_id, dataset, str(e),
                )
                return _bad(BAD_PROPERTY_ERROR)
            except Exception as e:
                logger.error(
                    "negotiate from client %s: cannot record %s: %s",
                    client_id, server_dataset, e,
                )
                return jsonify({'error': f'Failed to record the configuration of {server_dataset}'}), 500

        # Find the most recent snapshot present on both sides
        common_snapshot = None
        server_copy = 'none'
        server_newer = None
        if server_fs_exists:
            try:
                # Strict: `server_newer` must not rest on a listing that lacks a
                # snapshot libzfseasy could not parse. If the listing cannot be
                # trusted, negotiate still answers from the plain listing (as it
                # always has) but reports `server_newer_unknown` instead of
                # `server_newer`; the stream, which does decide what `-F` may
                # destroy, then refuses on its own.
                try:
                    server_snaps = server_snapshots(server_fs)
                    listing_trusted = True
                except Exception as e:
                    logger.warning(
                        "negotiate for %s: the snapshot listing cannot be trusted (%s); "
                        "not reporting server_newer", server_dataset, e,
                    )
                    server_snaps = server_snapshots(server_fs, strict=False)
                    listing_trusted = False
                server_names = {s.name for s in server_snaps}
                for snap_name in client_snapshots:
                    if snap_name in server_names:
                        common_snapshot = snap_name
                        break
                if server_names:
                    server_copy = _server_copy_kind(server_fs, client_id)
                if common_snapshot is not None and listing_trusted:
                    server_newer = newer_than(server_dataset, common_snapshot, server_snaps)
            except Exception as e:
                return jsonify({'error': f'Failed to list server snapshots: {e}'}), 500

        reply = {
            'common_snapshot': common_snapshot,
            'server_dataset': server_dataset,
            'server_copy': server_copy,
        }
        if server_newer is not None:
            reply['server_newer'] = server_newer[:SERVER_NEWER_MAX]
            reply['server_newer_count'] = len(server_newer)
        elif common_snapshot is not None:
            # A common snapshot, but the snapshots newer than it cannot be
            # told. Said explicitly: silence would read as "a server that does
            # not report them", and the client must not settle its anchor (or
            # let a rollback pass for a backstop) on a list it does not have.
            reply['server_newer_unknown'] = True
        return jsonify(reply)

    @app.route('/backup/<client_id>/<path:dataset>/negotiate', methods=['POST'])
    @_gated
    def backup_negotiate(client_id: str, dataset: str):
        """`POST /backup/<client_id>/<dataset>/negotiate` (authenticated):
        record the client dataset's configuration and answer the newest
        snapshot both sides hold and what kind of copy the server holds.

        The configuration is stored on the server dataset when it exists.
        When it does not, negotiate creates only its PARENT and keeps the
        configuration in memory (`pending_configs`) until the stream creates
        the dataset: a raw full stream, or a plain one under an encrypted
        target, is refused by zfs onto a pre-created dataset.

        Request body (JSON object, `Content-Type` not required)::

            {
              "snapshots": ["<short snapshot name>", ...],   # newest first
              "config": "<DatasetConfig.to_property(destination)>"
            }

        Both fields are required. `config` must decode with
        `DatasetConfig.from_property` and have the expected types
        (`canonical_dataset_config`); only its canonical re-encoding is
        stored as `org.zfsbackup:config` (plan B-1a), never the client's
        string.

        Responses: 200 `{"common_snapshot": <name or null>,
        "server_dataset", "server_copy": "none" | "plain" | "raw"}`
        (`none`: the dataset is missing or holds no snapshot; `raw`: it is
        encrypted with its own encryption root, which is what a raw receive
        leaves; `plain`: otherwise; an additive field of protocol 1). With a
        common snapshot the reply also carries `server_newer`
        (`[{"name", "guid"}, ...]`: the server snapshots newer than it by
        `createtxg`, oldest first, the guid a decimal string, at most
        `SERVER_NEWER_MAX`) and `server_newer_count` (their number), both
        additive: a client that does not know them ignores them, and a
        `/stream` receive that would destroy such snapshots is refused
        unless the client names them in `discard`. When there is a common
        snapshot but the newer ones cannot be told (the listing cannot be
        trusted, or a snapshot has no `createtxg`) the reply carries
        `"server_newer_unknown": true` instead of the two fields above. 400
        `{"error"}` for a body that is not a JSON object, a missing or mistyped field, an undecodable or empty
        `config` (`BAD_CONFIG_ERROR`), an invalid dataset name, or a value
        libzfseasy refuses to store (`BAD_PROPERTY_ERROR`); 409 when the
        server dataset belongs to another client id or client dataset, or
        when it, or an existing ancestor up to `<target>/<id>`, has no local
        `org.zfsbackup:client_id` (an operator's dataset is never adopted
        silently: the message names the `zfs set` that adopts it; B-2c); 500
        when `zfs` fails; 401/403/426/503 as for every gated endpoint. A
        400 is decided before anything is written."""
        disabled = _require_enabled()
        if disabled is not None:
            return disabled
        # The bound id (`_authenticate`), never the URL's: they are equal
        # here, but only the binding is trusted.
        client_id = g.client_id
        try:
            server_dataset = _server_dataset(client_id, dataset)
        except _BadDatasetName as e:
            return jsonify({'error': str(e)}), 400
        try:
            data = json.loads(request.get_data(cache=False))
        except (ValueError, UnicodeDecodeError, RecursionError):
            data = None
        if not isinstance(data, dict):
            return _bad('request body must be a JSON object')
        client_snapshots = data.get('snapshots')
        if not isinstance(client_snapshots, list) or not all(
            isinstance(s, str) for s in client_snapshots
        ):
            return _bad('snapshots: must be a list of snapshot names')
        try:
            ds_config_canonical = canonical_dataset_config(data.get('config'))
        except ValueError as e:
            logger.warning(
                "negotiate from client %s for %r: refusing its config: %s",
                client_id, dataset, e,
            )
            return _bad(
                CONFIG_TOO_LARGE_ERROR if str(e) == CONFIG_TOO_LARGE_ERROR else BAD_CONFIG_ERROR
            )

        # The server dataset is claimed for the whole of negotiate, like a
        # stream: a negotiate must not `zfs set` or `zfs create` a name a
        # stream is working on (a receive creating it, an abort destroying
        # it), nor two negotiates of one name race each other. The loser
        # gets the retryable busy reply.
        if not _claim_dataset(server_dataset):
            return jsonify(BUSY_ERROR), 503
        try:
            return _negotiate_claimed(
                client_id, dataset, server_dataset, client_snapshots, ds_config_canonical
            )
        finally:
            _release_dataset(server_dataset)

    @sock.route('/backup/<client_id>/<path:dataset>/stream')
    def backup_stream(ws, client_id: str, dataset: str):
        try:
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
            try:
                base = parse_stream_base(request.args)
            except ValueError as e:
                try:
                    ws.send(json.dumps({'error': str(e)}))
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
                # `?from=` / `?from_guid=` name the incremental base the
                # client negotiated and `?discard=` the server snapshots it
                # allows the receive to destroy. `zfs receive` takes the base
                # from the stream itself; these decide only whether it gets
                # `-F` (`decide_receive`), which destroys every snapshot newer
                # than the base.
                logger.debug(
                    "stream for %s/%s: mode=%s from=%s from_guid=%s discard=%d",
                    client_id, dataset, mode, base.name, base.guid, len(base.discard),
                )
                _receive_stream(ws, client_id, dataset, mode, base)
            finally:
                gate.end()
        finally:
            # After the session's `gate.end()`: a reload's drain does not
            # wait for this.
            _end_websocket(ws)

    def _receive_stream(
        ws, client_id: str, dataset: str, mode: str, base: StreamBase
    ) -> None:
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
        # The claim is released exactly once, by whichever of the two comes
        # first: `_receive_stream_locked` as soon as `zfs receive` has been
        # reaped (BEFORE it answers the client, whose next stream may arrive
        # at once), or this `finally` on any other path. Once released, the
        # name may belong to another stream, so a second release would drop
        # THAT stream's claim.
        released = False

        def release_claim() -> None:
            nonlocal released
            if not released:
                released = True
                _release_dataset(server_dataset)

        try:
            _receive_stream_locked(
                ws, client_id, dataset, server_dataset, mode, base, release_claim
            )
        finally:
            release_claim()

    def _receive_stream_locked(
        ws, client_id: str, dataset: str, server_dataset: str, mode: str,
        base: StreamBase, release_claim: Callable[[], None],
    ) -> None:
        server_fs = Filesystem(server_dataset)

        # D7 and the binding: refuse to receive into a server dataset that
        # another client id or client dataset owns (read-only, so also in a
        # dry run).
        try:
            conflict = _ownership_conflict(server_dataset, dataset, client_id)
        except Exception as e:
            release_claim()
            ws.send(json.dumps({'error': f'Failed to check {server_dataset}: {e}'}))
            return
        if conflict is not None:
            release_claim()
            ws.send(json.dumps({'error': conflict}))
            return

        if config.dry_run:
            release_claim()
            ws.send(json.dumps({'status': 'ok', 'dry_run': True}))
            return

        # The values the server owns, read BEFORE anything is destroyed
        # (the fresh-mode abort below can take the whole dataset with it): the
        # canonical config negotiate stored -- on the dataset (local source
        # only), or, for a dataset that does not exist yet, in
        # `pending_configs` -- re-encoded, so it is plain base64 whatever the
        # dataset holds, the bound id and the URL's dataset name. They go to
        # the receive itself (`-o`), so they are the values in effect the
        # moment it finishes -- see `SERVER_OWNED_PROPERTIES`.
        try:
            # Asked once and reused for the `-F` decision below (the dataset
            # is claimed, so nothing else creates or destroys it meanwhile).
            existed = zfs.exists(server_fs)
            stored_config = (
                zfs.get.value(Dataset(server_dataset), PROP_CONFIG, sources='local')
                if existed else None
            )
        except Exception as e:
            logger.error("cannot read the configuration of %s: %s", server_dataset, e)
            release_claim()
            ws.send(json.dumps({
                'error': f'Failed to read the configuration of {server_dataset}'
            }))
            return
        if stored_config is not None:
            # The dataset has its own config: any pending entry is obsolete.
            _forget_pending(server_dataset)
        else:
            # Absent on disk: only a pending entry of THIS client dataset
            # counts (another one's is a different dataset's config).
            entry = _pending(server_dataset)
            if entry is not None and entry[1] == dataset:
                stored_config = entry[0]
        try:
            owned = {
                PROP_CONFIG: canonical_dataset_config(stored_config),
                PROP_CLIENT_ID: client_id,
                PROP_SOURCE_DATASET: dataset,
            }
        except ValueError as e:
            # Never negotiated (or the property was removed or damaged, or this
            # worker restarted since): the server has no config of its own to
            # put in effect, and without one a stream's copy could become the
            # one in effect.
            logger.warning(
                "stream from client %s for %r refused: no usable configuration "
                "is recorded on %s (%s); the client must negotiate first",
                client_id, dataset, server_dataset, e,
            )
            release_claim()
            ws.send(json.dumps({
                'error': (
                    f'no configuration is recorded for {server_dataset}; '
                    'negotiate again before streaming (a restart of the server '
                    'forgets the configuration of a dataset that has not been '
                    'received yet)'
                ),
                'retry': True,
            }))
            return

        # Plan B-2a: the stream's first record says what it is (a filesystem
        # or a volume: the property set below) and what it is incremental
        # from. It is read BEFORE the snapshot listing and the `-F` decision
        # (so the decision is made after the wait for the first bytes, with
        # nothing between it and the receive but the abort) and before
        # anything is destroyed or spawned, so a refusal discards nothing.
        # Everything read (the header and whatever arrived with it) is fed to
        # `zfs receive` unchanged. Refusals carry a `reason` and no `retry`.
        def refuse_stream(reason: str, message: str) -> None:
            # WARNING once per (server dataset, reason) until a stream of this
            # dataset succeeds (a client retries a refused stream every cycle).
            key = (server_dataset, reason)
            if key in _refused_noted:
                logger.debug(
                    "stream from client %s for %r refused (%s): %s",
                    client_id, dataset, reason, message,
                )
            else:
                if len(_refused_noted) > 1024:
                    _refused_noted.clear()
                _refused_noted.add(key)
                logger.warning(
                    "stream from client %s for %r refused (%s): %s",
                    client_id, dataset, reason, message,
                )
            release_claim()
            with contextlib.suppress(Exception):
                ws.send(json.dumps({'error': message, 'reason': reason}))

        try:
            head_bytes = read_stream_header(ws, WS_IDLE_TIMEOUT)
            header = parse_stream_header(head_bytes)
        except ConnectionClosed:
            return  # nobody to answer; nothing was spawned, the caller releases the claim
        except StreamHeaderError as e:
            refuse_stream('stream_header', str(e))
            return
        except Exception as e:
            release_claim()
            with contextlib.suppress(Exception):
                ws.send(json.dumps({'error': _receive_error(e, server_dataset)}))
            return
        mismatch = stream_base_problem(header, base)
        if mismatch is not None:
            refuse_stream('base_mismatch', f"{mismatch}, so nothing is received")
            return
        if existed:
            try:
                existing_type = zfs.get.value(Dataset(server_dataset), 'type')
            except Exception as e:
                logger.error("cannot read the type of %s: %s", server_dataset, e)
                release_claim()
                with contextlib.suppress(Exception):
                    ws.send(json.dumps({'error': f'Failed to read the type of {server_dataset}'}))
                return
            if existing_type != header.kind:
                refuse_stream('type_mismatch', (
                    f"{server_dataset} is a {existing_type} on the server and the stream is a "
                    f"{header.kind}, so nothing is received"
                ))
                return

        # Whether the receive may be forced (`-F`), decided after the header
        # (above) and BEFORE the abort below: a refusal must not first discard
        # a partial receive. `-F` on
        # an incremental receive destroys every snapshot newer than its base
        # (a client that rolled back or restored an older snapshot would
        # otherwise make the server destroy history it holds), so it is only
        # passed when that destroys nothing or the client named exactly
        # those snapshots (`decide_receive`). A listing that fails refuses.
        decision = ReceiveDecision(False)
        if existed:
            try:
                rows = server_snapshots(server_fs)
            except Exception as e:
                # zfs failed, or the listing could not be trusted (libzfseasy
                # skipped a row): either way a snapshot the decision never saw
                # could be destroyed by `-F`.
                logger.error(
                    "cannot list the snapshots of %s (the receive is refused): %s",
                    server_dataset, e,
                )
                release_claim()
                ws.send(json.dumps({
                    'error': f'Failed to list the snapshots of {server_dataset}: {e}',
                    'reason': 'listing',
                }))
                return
            if base.guid is None and base.name is not None:
                # A client that named its base without a guid: the snapshot
                # the server holds under that name must be the one the stream
                # is really incremental from (`decide_receive` finds the base
                # by name here, and `-F` is decided from what it finds).
                named = next((r for r in rows if r.name == base.name), None)
                if named is not None and named.guid is not None and named.guid != header.from_guid:
                    refuse_stream('base_mismatch', (
                        f"the client named {base.name!r} as the incremental source, which the "
                        f"server holds with guid {named.guid}, but the stream's source guid is "
                        f"{header.from_guid}, so nothing is received"
                    ))
                    return
            try:
                decision = decide_receive(server_dataset, base, rows)
            except ValueError as e:
                logger.error(
                    "cannot order the snapshots of %s (the receive is refused): %s",
                    server_dataset, e,
                )
                release_claim()
                ws.send(json.dumps({
                    'error': (
                        f'Cannot order the snapshots of {server_dataset}: {e}, '
                        'so nothing is received'
                    ),
                    'reason': 'listing',
                }))
                return
        if decision.reason is not None:
            logger.warning(
                "stream from client %s for %r refused (%s): %s",
                client_id, dataset, decision.reason, decision.message,
            )
            release_claim()
            ws.send(json.dumps({'error': decision.message, 'reason': decision.reason}))
            return
        if decision.rolled_back:
            logger.warning(
                "stream from client %s for %r: the client allowed the receive to destroy "
                "%d snapshot(s) of %s newer than its incremental source, which it no "
                "longer has: %s",
                client_id, dataset, len(decision.rolled_back), server_dataset,
                ', '.join(decision.rolled_back),
            )

        if mode == 'fresh':
            # D3: a fresh stream cannot continue a partial receive. Discard
            # its saved state first, or `zfs receive` refuses with "contains
            # partially-complete state". `receive -A` on a dataset that
            # existed before destroys only its `%recv` state; on a NEW
            # dataset whose first full receive was cut (it exists and is
            # visible while partial, Z3) it destroys the whole dataset, and
            # nothing is recreated: the full stream below creates it, with the
            # `owned` values read above. Nothing after the abort may assume
            # the dataset still exists. `resume` never reaches this.
            try:
                token = _resume_token(server_dataset)
            except Exception as e:
                release_claim()
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
                        release_claim()
                        ws.send(json.dumps({
                            'error': (
                                f'Failed to discard the partial receive state of '
                                f'{server_dataset}: {e}'
                            )
                        }))
                        return

        # zfs.receive.filesystem() returns a StreamHandle wrapping the `zfs
        # receive` process's stdin. A startup failure raises here; it never
        # returns None.
        #
        # `props` (`-o`, plan B-1b): a copy of the zfsbackup properties that
        # arrives IN the stream (a `zfs send -p`/`-R` the client is not
        # supposed to make) never takes effect; the server's own values are
        # set as LOCAL values by the receive itself, so there is no moment
        # at which the stream's copy is in effect. Verified on OpenZFS 2.1.5,
        # 2.2.2 and 2.4.1 for a full, an incremental, a resumed full and a
        # resumed incremental receive (`-F -s -u` throughout). One call for
        # both modes, so a resumed receive (`mode=resume`) gets exactly the
        # same flags as the fresh one it continues.
        #
        # `-F` only for a dataset that exists AND that `decide_receive` allowed:
        # an incremental (or a resume) onto an existing dataset needs it to
        # discard changes made to the head since the last snapshot, but it
        # also destroys every snapshot newer than the base, so it is passed
        # only when none exist or the client allowed them. A full stream into
        # a dataset that does not exist creates it and needs none: zfs refuses
        # `-F` together with a raw stream (or an encrypted target) onto an
        # EXISTING dataset, so the dataset must not be pre-created, and without
        # `-F` a dataset that appears in between is an error rather than
        # overwritten. The abort above may have destroyed a new dataset, so
        # existence is asked again.
        #
        # Plan B-2a: the same call also neutralises what arrives, by the
        # stream's own object-set type (`header`, read above): `-o` of the
        # mount/share/privilege properties and canmount for a filesystem, only
        # `readonly`/`volmode` for a volume (it refuses the rest), `-h` (no
        # holds from the stream: a client hold would pin server snapshots
        # against the pruner) and never `-x` (it protects nothing on a full
        # `-F` receive into an existing dataset). Identical for fresh and
        # resumed receives, full and incremental. The outcome of `zfs receive`
        # is its EXIT STATUS (`writer.close()`), never its stderr: OpenZFS 2.1
        # prints a "sharesmb ... must be upgraded" line on raw receives that
        # succeed.
        #
        # The server never creates a volume (`zfs create -V`): OpenZFS 2.4.1
        # oopses on `zfs create -V ... -o readonly=...`. Volumes only arrive
        # through the receive, which is fine. The only `zfs create` here is the
        # container filesystems negotiate makes.
        receive_props = (
            RECEIVE_PROPERTIES_VOLUME if header.kind == 'volume' else RECEIVE_PROPERTIES_FILESYSTEM
        )
        try:
            writer = zfs.receive.filesystem(
                server_fs, props={**owned, **receive_props},
                force=decision.force and zfs.exists(server_fs), holds=False,
                save=True, mount=False,
            )
        except Exception as e:
            release_claim()
            ws.send(json.dumps({'error': f'Failed to start zfs receive: {e}'}))
            return

        reaped = False  # `writer.close()` already called on the write-error path
        try:
            first = head_bytes  # the header record and what arrived with it, unchanged
            while True:
                if first is not None:
                    data, first = first, None
                else:
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
            # The dataset exists now with the owned values as local ones.
            _forget_pending(server_dataset)
            _refused_noted.difference_update(
                {k for k in _refused_noted if k[0] == server_dataset}
            )
            # The receive has finished and been reaped: nothing more touches
            # this dataset. Release the per-dataset claim BEFORE the final
            # ack, or a client that streams its next snapshot as soon as it
            # reads `ok` (a chain step, a resume follow-up) can get `busy`.
            release_claim()
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
            # Reaped (or given up on): same ordering as the success path, the
            # claim goes before the client reads the error and retries.
            release_claim()
            with contextlib.suppress(Exception):
                ws.send(json.dumps({'error': _receive_error(e, server_dataset)}))

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

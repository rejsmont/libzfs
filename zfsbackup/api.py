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
import ipaddress
import itertools
import json
import logging
import math
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
from zfsbackup._vendor.flask_sock import Sock
from simple_websocket import ConnectionClosed
from werkzeug.exceptions import BadRequest, RequestEntityTooLarge

import libzfseasy as zfs
from libzfseasy import ZFSTimeoutError, timeout as zfs_timeout
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
from zfsbackup.logsetup import PEER_LOGGER
from zfsbackup.protocol import (
    PROTOCOL_HEADER, PROTOCOL_REFUSAL_REASON, PROTOCOL_VERSION, SUPPORTED_PROTOCOLS,
    parse_protocol_header,
)
from zfsbackup.remote import HTTP_TIMEOUT, RESUME_TOKEN_TIMEOUT, WS_IDLE_TIMEOUT

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

#: S-14: the overall time (seconds) the non-streaming `zfs` calls of one
#: request handler may take, `ZFSBudget`. Not applied to the `zfs receive` of a
#: stream, which a running transfer is never cut by.
ZFS_REQUEST_BUDGET = 15.0

#: What a timed-out `zfs` call adds to its limit before the handler can answer:
#: libzfseasy waits up to 2 s after SIGTERM and 2 s after SIGKILL.
_ZFS_KILL_OVERHEAD = 4.0

#: Margin for the extra commands of a helper that runs several (up to three
#: `zfs get`s in `read_zfsbackup_properties`), each limited to what was left
#: when the helper started.
_ZFS_HELPER_MARGIN = 6.0

#: The refusal body (HTTP 503 for the plain endpoints, a text message on a
#: stream) when a handler's `zfs` call did not answer in time, or was refused
#: because an earlier timed-out `zfs` child of this worker is still stuck
#: (libzfseasy `ZFSTimeoutError`). `retry` marks it as transient, like
#: `BUSY_ERROR`: the client skips the cycle and never falls back to a fresh
#: send. The wording has no "reloading" in it (the client tells that apart by
#: text).
ZFS_TIMEOUT_ERROR = {
    'error': 'zfs timed out; retry later', 'retry': True, 'reason': 'zfs_timeout',
}

# The server must answer `zfs_timeout` while the client is still listening:
# budget + kill + helper margin has to stay below every client timeout of a
# request whose handler uses the budget (`remote.HTTP_TIMEOUT` for negotiate,
# `remote.RESUME_TOKEN_TIMEOUT`). A client that gave up first would leave the
# server's hung `zfs` behind and start another with its next request.
assert (
    ZFS_REQUEST_BUDGET + _ZFS_KILL_OVERHEAD + _ZFS_HELPER_MARGIN
    < min(HTTP_TIMEOUT, RESUME_TOKEN_TIMEOUT)
), "ZFS_REQUEST_BUDGET must stay below the client's request timeouts"

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

# ---------------------------------------------------------------------------
# S-3: anonymous flood control and log throttling
# ---------------------------------------------------------------------------
#
# Everything here is a constant (plan DD6/D5): no configuration, no schema.
#
# **Peer.** A "peer" is the address a connection came from (`peer_key`): the
# IPv4 address, or the /64 of an IPv6 address (one host owns a whole /64, so
# per-address limits would be free to dodge), IPv4-mapped IPv6 as IPv4.
# `X-Forwarded-For` is never read. Behind a NAT, a proxy or a load balancer
# that does not preserve the source address, every client of that network is
# ONE peer, and the limits below are shared by all of them (see each class).
#
# **Threads.** The objects below are shared by the API worker's threads:
#
# - the accept thread (werkzeug's `serve_forever`): `PeerRateLimiter.admit`
#   / `release` and `_PerPeerThrottle.log` from `ApiServer.process_request`;
# - every connection thread (it also runs the Flask app for that
#   connection): `PeerRateLimiter.release` / `vouch` from
#   `process_request_thread`, `EnrollGuard.*` and `_PerPeerThrottle.log` from
#   the routes and from `before_request`;
# - the trust-poll thread: `_PerPeerThrottle.take_summary` (the hourly count)
#   via `PeerLimits.flush_log`.
#
# Each object owns ONE `threading.Lock`, held only for dictionary and counter
# updates -- O(1), never across I/O, logging, a store call or another lock --
# so the accept thread can never wait on anything slow. Nothing here takes
# `_STORE_LOCK`. Time comes from `time.monotonic` (looked up at each call, so
# a test may patch it) or from the injected `clock`. Every table is bounded
# and evicts its least recently used entry; a flood of distinct source
# addresses can therefore make the server forget some peers' state (a
# forgotten bucket is a full one, a forgotten lockout is lifted), never grow
# its memory.

#: Per-peer token bucket on accept: a burst of this many connections, refilled
#: at this many per second, over a table of this many peers.
API_RATE_BURST = 60
API_RATE_REFILL = 10.0
API_RATE_PEERS = 4096

#: At most this many connections of one peer open at once, counted from
#: accept to close (the TLS handshake included). The bucket bounds how fast
#: connections arrive; this bounds how many a SILENT peer can hold: each
#: waits `API_HANDSHAKE_TIMEOUT` (30 s), so without it one address at 10 per
#: second would fill all `API_MAX_CONNECTIONS` slots and starve every pinned
#: client. Not in the S-3 spec; the starvation requirement needs it.
API_MAX_CONNECTIONS_PER_IP = 8

#: A peer that completed a TLS handshake with a PINNED client certificate
#: (`ApiServer._is_pinned`) is "vouched" while such a connection is open and
#: for `API_VOUCH_TTL` seconds after the last one ended (8 days: a client
#: that backs up daily or weekly must still be vouched when it next arrives
#: during a flood, or it would be dropped by the reserve before it can
#: present its certificate; the table is a bounded LRU of
#: `API_VOUCHED_PEERS` and a vouch needs a pinned handshake. The price: an
#: anonymous host behind a vouched NAT shares that allowance for up to 8
#: days): no bucket, and `API_MAX_CONNECTIONS_PER_VOUCHED_IP`
#: concurrent connections. This is the NAT answer: a site of backup clients
#: whose cycles start together (aligned schedules) would otherwise lose
#: connections to a 60-burst bucket. A TCP handshake plus a pinned
#: certificate cannot be forged from another address, but an ANONYMOUS host
#: behind the same address as a vouched client shares the allowance; it is
#: still bound by the connection caps. At most `API_VOUCHED_PEERS` peers.
API_VOUCH_TTL = 8 * 86400.0
API_MAX_CONNECTIONS_PER_VOUCHED_IP = 32
API_VOUCHED_PEERS = 1024

#: Connection slots kept for vouched peers (item 6 of the S-3 review): while
#: this many of the server's `max_connections` slots are still free, only a
#: vouched peer (or one with a live pinned connection) is admitted; an
#: unvouched peer is dropped once `max_connections - reserve` are open. The
#: reserve actually used is `min(API_VOUCHED_RESERVE, max_connections // 4)`
#: (16 of 64): enough for the concurrent streams of a typical site, a
#: quarter of the total, and not above the 16 anonymous connections the
#: server already allows. A silent-handshake flood from any number of
#: addresses can therefore never take more than 48 slots.
API_VOUCHED_RESERVE = 16

#: `/enroll`: this many failed attempts from one peer within
#: `ENROLL_FAILURE_WINDOW` seconds lock that peer out of `/enroll` (429 with
#: `Retry-After`) for the window; at most `ENROLL_MAX_IN_FLIGHT` requests run
#: at once (more: 503 + retry); the table holds `ENROLL_FAILURE_PEERS` peers.
ENROLL_FAILURES_MAX = 5
ENROLL_FAILURE_WINDOW = 900.0
ENROLL_MAX_IN_FLIGHT = 2
ENROLL_MAX_IN_FLIGHT_PER_PEER = 1
ENROLL_FAILURE_PEERS = 4096

#: Refusal log lines (`_PerPeerThrottle`): one WARNING per (kind, peer) per
#: `PEER_LOG_EVERY` seconds, at most `PEER_LOG_GLOBAL_MAX` lines in all per
#: that interval (a flood from many addresses), over `PEER_LOG_KEYS` keys.
#: Suppressed lines are DEBUG and counted into one summary per interval.
PEER_LOG_EVERY = 3600.0
PEER_LOG_KEYS = 4096
PEER_LOG_GLOBAL_MAX = 120
PEER_LOG_ACCESS_MAX = 120
PEER_LOG_ANON_MAX = 120

#: Log kinds, in three classes with a budget each (per `PEER_LOG_EVERY`), so
#: that a class anyone can trigger can never use up a class only certificate
#: holders can reach:
#:
#: - "flood" (`PEER_LOG_ACCESS_MAX`): capacity and access lines that routine
#:   traffic produces (a reload drain, 60 busy hosts) -- `access-*`, `rate`,
#:   `conn-cap`, `conn-reserve`, `enroll-busy`, `store-busy`;
#: - "anonymous" (`PEER_LOG_ANON_MAX`): refusals a caller WITHOUT a
#:   certificate can cause -- `enroll`, `enroll-locked`, and `protocol` keyed
#:   by a bare peer address (the key of an unauthenticated 426);
#: - "holder" (`PEER_LOG_GLOBAL_MAX`): what only a certificate holder (or a
#:   former one) reaches -- `auth`, `rotate`, `pending`, `tls-validity`, and
#:   `protocol` keyed `<client id>@<peer>` (an authenticated 426).
FLOOD_LOG_KINDS = frozenset({'rate', 'conn-cap', 'conn-reserve', 'enroll-busy', 'store-busy'})
ANONYMOUS_LOG_KINDS = frozenset({'enroll', 'enroll-locked'})

LOG_CLASS_FLOOD = 'flood'
LOG_CLASS_ANONYMOUS = 'anonymous'
LOG_CLASS_HOLDER = 'holder'


def is_flood_log_kind(kind: str) -> bool:
    return kind in FLOOD_LOG_KINDS or kind.startswith('access-')


def log_class(kind: str, peer: str) -> str:
    """The budget class of a log line (see above). A client id never contains
    `@`, so `<client id>@<peer>` marks an authenticated peer."""
    if is_flood_log_kind(kind):
        return LOG_CLASS_FLOOD
    if kind in ANONYMOUS_LOG_KINDS or (kind == 'protocol' and '@' not in peer):
        return LOG_CLASS_ANONYMOUS
    return LOG_CLASS_HOLDER


#: Negotiate's pending-config map (`create_app`): at most this many entries
#: per client id; a client past it evicts its OWN oldest entry.
PENDING_CONFIGS_PER_CLIENT = 256

#: The response statuses an unauthenticated flood repeats (no certificate,
#: protocol, lockout, busy): werkzeug's access line for these is logged once
#: per peer, status and hour (`workers` `log_request`), the rest at DEBUG.
THROTTLED_ACCESS_STATUSES = frozenset({401, 403, 426, 429, 503})

#: `PeerRateLimiter.admit` refusals.
ADMIT_RATE = 'rate'
ADMIT_RESERVED = 'reserved'

#: `_claim_pending`'s answer when the client is at its cap and nothing of its
#: own can be evicted (the negotiate is then refused 503 + retry).
PENDING_FULL = object()
ADMIT_CONCURRENT = 'concurrent'


def peer_key(client_address) -> str:
    """The peer a connection or request comes from: `client_address` is a
    socket address tuple, a bare host string, or `None`. IPv4 -> the address;
    IPv4-mapped IPv6 -> the IPv4 address; other IPv6 -> its /64
    (`2001:db8::/64`), except loopback; anything unparseable -> the string
    itself (clipped). Never raises."""
    try:
        host = client_address
        if isinstance(client_address, (tuple, list)):
            host = client_address[0] if client_address else None
        if host is None:
            return '?'
        host = str(host)
        try:
            ip = ipaddress.ip_address(host.split('%', 1)[0])
        except ValueError:
            return host[:64] or '?'
        if ip.version == 6:
            if ip.ipv4_mapped is not None:
                return str(ip.ipv4_mapped)
            if ip.is_loopback:
                return str(ip)
            return f"{ipaddress.IPv6Address(int(ip) >> 64 << 64)}/64"
        return str(ip)
    except Exception:
        return '?'


class PeerRateLimiter:
    """Per-peer admission on accept: a token bucket (`API_RATE_BURST`,
    `API_RATE_REFILL`), a cap on concurrent connections and a reserve of
    connection slots for vouched peers, all keyed by `peer_key`. **O(1) and
    never blocks** (one short lock; no I/O): called on the accept thread,
    which must never wait.

    `admit(key)` -> `None` (admitted: one concurrency slot is held and the
    caller MUST `release(key)` exactly once when the connection is over, or
    at once if it does not go on to use it), `ADMIT_CONCURRENT`,
    `ADMIT_RESERVED` or `ADMIT_RATE` (refused, nothing held). A refused
    connection is dropped before any TLS byte is read.

    **Vouched.** `vouch(key)` is called once per connection that completed a
    handshake with a pinned client certificate and `unvouch(key)` when that
    connection ends. A peer is vouched while it has such a connection open
    OR for `API_VOUCH_TTL` seconds after the last one ended (the clock
    starts at the end, so a long stream keeps its address vouched for as long
    as it runs: a NAT with many streams open can always open another). A
    vouched peer skips the bucket, gets `API_MAX_CONNECTIONS_PER_VOUCHED_IP`
    connections and may use the reserve.

    **Reserve.** `configure_capacity(max_connections)` (the server calls it)
    sets the reserve to `min(API_VOUCHED_RESERVE, max_connections // 4)`:
    once `max_connections - reserve` connections are open, only vouched
    peers are admitted. Anonymous peers can therefore never take the last
    slots, however many addresses the flood comes from; the per-peer cap
    (`API_MAX_CONNECTIONS_PER_IP`) bounds what each address holds.

    Bounded: the bucket table by `max_peers` (LRU; forgetting a peer
    refills its bucket), the vouched table by `max_vouched` (LRU; a forgotten
    peer is simply anonymous again), the concurrency and pinned-open tables
    by the number of open connections."""

    def __init__(
        self,
        *,
        burst: int = API_RATE_BURST,
        rate: float = API_RATE_REFILL,
        max_peers: int = API_RATE_PEERS,
        max_per_peer: int = API_MAX_CONNECTIONS_PER_IP,
        max_per_vouched: int = API_MAX_CONNECTIONS_PER_VOUCHED_IP,
        vouch_ttl: float = API_VOUCH_TTL,
        max_vouched: int = API_VOUCHED_PEERS,
        reserve: int = API_VOUCHED_RESERVE,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        self.burst = burst
        self.rate = rate
        self._max_peers = max_peers
        self._max_per_peer = max_per_peer
        self._max_per_vouched = max_per_vouched
        self._vouch_ttl = vouch_ttl
        self._max_vouched = max_vouched
        self._reserve_wanted = reserve
        self._clock = clock
        self._guard = threading.Lock()
        #: key -> [tokens, time of last update]; insertion order = recency.
        self._buckets: 'collections.OrderedDict[str, list]' = collections.OrderedDict()
        #: key -> open connections (entries are removed at zero).
        self._active: dict = {}
        #: Connections open across all keys (the sum of `_active`).
        self._active_total = 0
        #: key -> open connections that presented a pinned certificate.
        self._pinned_open: dict = {}
        #: key -> time the last pinned connection began or ended; insertion
        #: order = recency.
        self._vouched: 'collections.OrderedDict[str, float]' = collections.OrderedDict()
        #: Connections an unvouched peer may reach (`None`: no reserve).
        self._unvouched_limit: Optional[int] = None
        self._reserve = 0

    def _now(self) -> float:
        return self._clock() if self._clock is not None else time.monotonic()

    @property
    def reserve(self) -> int:
        """Slots only vouched peers may take (0 until `configure_capacity`)."""
        return self._reserve

    def configure_capacity(self, max_connections: int) -> None:
        """Tell the limiter the server's connection cap; sets the reserve."""
        reserve = min(self._reserve_wanted, max_connections // 4)
        with self._guard:
            self._reserve = reserve
            self._unvouched_limit = max_connections - reserve if reserve > 0 else None

    def _is_vouched_locked(self, key: str, now: float) -> bool:
        if self._pinned_open.get(key, 0) > 0:
            return True
        at = self._vouched.get(key)
        if at is None:
            return False
        if now - at < self._vouch_ttl:
            return True
        del self._vouched[key]
        return False

    def admit(self, key: str) -> Optional[str]:
        now = self._now()
        with self._guard:
            vouched = self._is_vouched_locked(key, now)
            active = self._active.get(key, 0)
            if active >= (self._max_per_vouched if vouched else self._max_per_peer):
                return ADMIT_CONCURRENT
            if (
                not vouched
                and self._unvouched_limit is not None
                and self._active_total >= self._unvouched_limit
            ):
                return ADMIT_RESERVED
            if not vouched:
                bucket = self._buckets.pop(key, None)  # re-insert last: recency
                if bucket is None:
                    tokens = float(self.burst)
                else:
                    tokens = min(float(self.burst), bucket[0] + (now - bucket[1]) * self.rate)
                refused = tokens < 1.0
                self._buckets[key] = [tokens if refused else tokens - 1.0, now]
                while len(self._buckets) > self._max_peers:
                    del self._buckets[next(iter(self._buckets))]
                if refused:
                    return ADMIT_RATE
            self._active[key] = active + 1
            self._active_total += 1
        return None

    def release(self, key: str) -> None:
        with self._guard:
            active = self._active.get(key, 0)
            if active <= 0:
                return
            if active == 1:
                del self._active[key]
            else:
                self._active[key] = active - 1
            self._active_total -= 1

    def _touch_vouched_locked(self, key: str, now: float) -> None:
        self._vouched.pop(key, None)
        self._vouched[key] = now
        while len(self._vouched) > self._max_vouched:
            del self._vouched[next(iter(self._vouched))]

    def vouch(self, key: str) -> None:
        """One more open connection of `key` presented a pinned certificate."""
        now = self._now()
        with self._guard:
            self._pinned_open[key] = self._pinned_open.get(key, 0) + 1
            self._touch_vouched_locked(key, now)

    def unvouch(self, key: str) -> None:
        """A connection that was `vouch`ed is over (call it exactly once per
        `vouch`, whatever happened)."""
        now = self._now()
        with self._guard:
            n = self._pinned_open.get(key, 0)
            if n <= 0:
                return
            if n == 1:
                del self._pinned_open[key]
            else:
                self._pinned_open[key] = n - 1
            self._touch_vouched_locked(key, now)  # the TTL runs from here

    def is_vouched(self, key: str) -> bool:
        now = self._now()
        with self._guard:
            return self._is_vouched_locked(key, now)


class EnrollGuard:
    """`/enroll` flood control: a per-peer failure lockout and a global cap on
    requests in flight. Used by the Flask threads (`before_request`, the
    route, `teardown_request`); one lock, O(1) sections, no I/O.

    **Failures.** `record_failure(key)` after an attempt the peer got wrong
    (a refusal, a malformed body; NOT a 503: the server's own trouble).
    `ENROLL_FAILURES_MAX` failures within `ENROLL_FAILURE_WINDOW` lock the
    peer out for that long: `retry_after(key)` is then the whole seconds
    left (900 right after the lockout, the `Retry-After` value) and the route
    answers 429 before reading the body or touching the store. The lockout
    is `/enroll` only: a pinned client's other requests are unaffected. A
    success `forget`s the peer's failures. Behind a NAT five mistyped tokens
    from anyone on the network lock the whole network out of `/enroll` for
    15 minutes; the operator's way round is to enroll from another address.

    **In flight.** `try_enter(key)` / `leave(key)`: at most `max_in_flight`
    `/enroll` requests past their body and shape checks at once (taken right
    before the token pre-check, NEVER while a body is being read: a peer that
    sends headers and no body would otherwise hold the slots for the whole
    read deadline), and at most `max_per_peer` of them per peer, because
    each can take the process-wide store lock (`_store_call`) and run `zfs`.
    More are answered 503 + retry. `key=None` counts only the global cap.
    """

    def __init__(
        self,
        *,
        max_failures: int = ENROLL_FAILURES_MAX,
        window: float = ENROLL_FAILURE_WINDOW,
        max_in_flight: int = ENROLL_MAX_IN_FLIGHT,
        max_per_peer: int = ENROLL_MAX_IN_FLIGHT_PER_PEER,
        max_peers: int = ENROLL_FAILURE_PEERS,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        self.max_failures = max_failures
        self.window = window
        self.max_in_flight = max_in_flight
        self.max_per_peer = max_per_peer
        self._max_peers = max_peers
        self._clock = clock
        self._guard = threading.Lock()
        #: key -> [failure times within the window, locked-out-until or 0.0];
        #: insertion order = recency of the last failure.
        self._peers: 'collections.OrderedDict[str, list]' = collections.OrderedDict()
        self._in_flight = 0
        #: key -> its requests in flight (entries removed at zero).
        self._in_flight_by_peer: dict = {}

    def _now(self) -> float:
        return self._clock() if self._clock is not None else time.monotonic()

    def retry_after(self, key: str) -> int:
        """Whole seconds `key` is still locked out for, else 0."""
        now = self._now()
        with self._guard:
            entry = self._peers.get(key)
            if entry is None or entry[1] <= now:
                return 0
            return max(1, math.ceil(entry[1] - now))

    def record_failure(self, key: str) -> bool:
        """Count a failed attempt; -> `True` when this one locked `key` out."""
        now = self._now()
        with self._guard:
            entry = self._peers.pop(key, None)
            if entry is None:
                entry = [collections.deque(), 0.0]
            times = entry[0]
            while times and now - times[0] >= self.window:
                times.popleft()
            times.append(now)
            locked = len(times) >= self.max_failures
            if locked:
                times.clear()
                entry[1] = now + self.window
            self._peers[key] = entry
            while len(self._peers) > self._max_peers:
                del self._peers[next(iter(self._peers))]
            return locked

    def forget(self, key: str) -> None:
        with self._guard:
            self._peers.pop(key, None)

    def try_enter(self, key: Optional[str] = None) -> bool:
        with self._guard:
            if self._in_flight >= self.max_in_flight:
                return False
            if key is not None:
                mine = self._in_flight_by_peer.get(key, 0)
                if mine >= self.max_per_peer:
                    return False
                self._in_flight_by_peer[key] = mine + 1
            self._in_flight += 1
            return True

    def leave(self, key: Optional[str] = None) -> None:
        with self._guard:
            if self._in_flight > 0:
                self._in_flight -= 1
            if key is not None:
                mine = self._in_flight_by_peer.get(key, 0)
                if mine <= 1:
                    self._in_flight_by_peer.pop(key, None)
                else:
                    self._in_flight_by_peer[key] = mine - 1

    @property
    def in_flight(self) -> int:
        with self._guard:
            return self._in_flight


class _PerPeerThrottle:
    """The one throttle for log lines about a peer (enroll, rotate, auth and
    protocol refusals, TLS certificate validity, flood drops, ...): at most
    one line per `(kind, peer)` per `every` seconds, and at most `global_max`
    lines in all per `every` seconds -- counted separately for three classes
    of kind (`log_class`): "flood" (`access_max`), "anonymous" (`anon_max`:
    what a caller without a certificate can cause) and "holder" (`global_max`:
    what only a certificate holder can reach), so a class anyone can trigger
    can never use up the budget of a class only certificate holders reach.
    `log()` emits the admitted line on the
    dedicated `logsetup.PEER_LOGGER` at the caller's level and every other one
    at DEBUG (so `-v` still shows them), and counts the suppressed ones.

    Once per `every` seconds `take_summary()` returns one line counting what
    was suppressed (and by which kinds) in the interval that just ended, or
    `None`; `PeerLimits.flush_log` logs it at WARNING. The trust-poll thread
    calls it every tick, so it is emitted promptly even when no further
    event arrives; without that thread (tests, `create_app` alone) it is
    returned to whoever calls it. The interval is a fixed window started at
    construction; `allow`/`log` roll it too when it has ended, so the cap
    resets even if nothing calls `take_summary`.

    Thread-safe: one lock around dictionary updates; nothing is logged under
    it. `kind` is a small constant string chosen by the caller (it keys the
    summary); `peer` is any string (`peer_key`, or `"<client id>@<peer>"`).
    Bounded: `max_keys` keys, the least recently logged evicted first -- a
    flood of addresses can make the server log an evicted peer's next line
    early, never more than `global_max` per interval."""

    def __init__(
        self,
        every: float = PEER_LOG_EVERY,
        max_keys: int = PEER_LOG_KEYS,
        global_max: int = PEER_LOG_GLOBAL_MAX,
        clock: Optional[Callable[[], float]] = None,
        access_max: int = PEER_LOG_ACCESS_MAX,
        anon_max: int = PEER_LOG_ANON_MAX,
    ) -> None:
        self._every = every
        self._max = max_keys
        self._global_max = global_max
        self._access_max = access_max
        self._anon_max = anon_max
        self._clock = clock
        self._guard = threading.Lock()
        self._last: 'collections.OrderedDict[tuple, float]' = collections.OrderedDict()
        self._window_start = self._now()
        #: class -> lines written in this window.
        self._emitted: dict = {}
        self._suppressed: dict = {}
        #: Suppressed counts of finished intervals not yet returned.
        self._finished: dict = {}

    def _now(self) -> float:
        return self._clock() if self._clock is not None else time.monotonic()

    def _roll_locked(self, now: float) -> None:
        if now - self._window_start < self._every:
            return
        for kind, count in self._suppressed.items():
            self._finished[kind] = self._finished.get(kind, 0) + count
        self._suppressed = {}
        self._emitted = {}
        self._window_start = now

    def allow(self, kind: str, peer: str) -> bool:
        """`True` when a line about `(kind, peer)` may be logged now."""
        now = self._now()
        with self._guard:
            self._roll_locked(now)
            key = (kind, peer)
            last = self._last.get(key)
            klass = log_class(kind, peer)
            cap = {
                LOG_CLASS_FLOOD: self._access_max,
                LOG_CLASS_ANONYMOUS: self._anon_max,
            }.get(klass, self._global_max)
            if (last is not None and now - last < self._every) or (
                self._emitted.get(klass, 0) >= cap
            ):
                self._suppressed[kind] = self._suppressed.get(kind, 0) + 1
                return False
            self._last.pop(key, None)  # re-insert last: dict order = recency
            self._last[key] = now
            while len(self._last) > self._max:
                del self._last[next(iter(self._last))]
            self._emitted[klass] = self._emitted.get(klass, 0) + 1
            return True

    def log(self, kind: str, peer: str, level: int, msg: str, *args) -> bool:
        """Log `msg % args` on `PEER_LOGGER` at `level` if `allow`ed, else at
        DEBUG; -> whether it was the admitted line."""
        admitted = self.allow(kind, peer)
        logging.getLogger(PEER_LOGGER).log(level if admitted else logging.DEBUG, msg, *args)
        return admitted

    def emit(self, kind: str, peer: str, level: int, msg: str, *args) -> bool:
        """Like `log`, but a suppressed line is not formatted or logged at
        all (only counted): for the accept thread, which must not spend time
        on a line nobody asked for. -> whether the line was written."""
        if not self.allow(kind, peer):
            return False
        logging.getLogger(PEER_LOGGER).log(level, msg, *args)
        return True

    def take_summary(self) -> Optional[str]:
        """The count of lines suppressed in the interval(s) that ended since
        the last call, as one line, or `None`."""
        now = self._now()
        with self._guard:
            self._roll_locked(now)
            finished, self._finished = self._finished, {}
        if not finished:
            return None
        total = sum(finished.values())
        kinds = ', '.join(f"{kind} {count}" for kind, count in sorted(finished.items()))
        window = 'hour' if self._every == 3600 else f"{self._every:g} seconds"
        return (
            f"{total} refusal log line(s) suppressed in the last {window} "
            f"({kinds}); at most one line per kind and peer is logged per {window}"
        )


class PeerLimits:
    """The API worker's flood-control state, one per worker: the accept-time
    `rate` limiter, the `/enroll` guard and the shared log throttle. Built by
    `create_app` (`app.extensions['zfsbackup.peer_limits']`); the server and
    the trust-poll thread take it from there. `clock` (monotonic seconds)
    is shared by all three; tests inject it."""

    def __init__(
        self,
        *,
        clock: Optional[Callable[[], float]] = None,
        rate: Optional[PeerRateLimiter] = None,
        enroll: Optional[EnrollGuard] = None,
        log: Optional[_PerPeerThrottle] = None,
    ) -> None:
        self.rate = rate if rate is not None else PeerRateLimiter(clock=clock)
        self.enroll = enroll if enroll is not None else EnrollGuard(clock=clock)
        self.log = log if log is not None else _PerPeerThrottle(clock=clock)

    def flush_log(self) -> None:
        """Log the hourly suppressed-lines summary if one is due (WARNING on
        `PEER_LOGGER`). Never raises."""
        try:
            summary = self.log.take_summary()
            if summary:
                logging.getLogger(PEER_LOGGER).warning("API: %s", summary)
        except Exception:
            logger.debug("peer log summary failed", exc_info=True)


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
    force_eof = _ws_force_eof(ws)
    if force_eof is None:
        return
    logger.debug("stream: the websocket reader is still waiting on a silent peer; "
                 "shutting the socket down")
    with contextlib.suppress(OSError):
        force_eof()
    with contextlib.suppress(RuntimeError):
        thread.join(WS_READER_JOIN_TIMEOUT)


class _StreamCut(Exception):
    """The running stream's session was cut by `SessionRegistry` (its pin was
    revoked, or a write to the client stalled); never leaves
    `create_app._receive_stream_locked`."""


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
      a revocation reaches new connections through it, and a RUNNING
      `/stream` through `SessionRegistry.enforce` (plan S-2: cut on
      `revoked`, `successor_revoked`, `client_revoked` and `replaced`; a
      `retired` pin -- a rotation -- finishes its stream).
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
        list_revoked: Optional[Callable] = None,
    ) -> None:
        """`identity` is the loaded server `identity.Identity`; `resolved`
        the `ResolvedConfigPath` of the config database. The callables are
        seams for tests; the defaults are `store.trust.read_trust_key`,
        `store.trust.read_trust`, `identity.server_context` and
        `store.trust.enroll_client` / `rotate_client` / `retire_predecessor` /
        `token_precheck` / `list_revoked` (all through the
        `zfsbackup.config.store` package).
        With the default factory, rebuilds wait at most
        `GATEWAY_LOCK_TIMEOUT` for the identity lock; the initial context
        waits the identity module's default. A `context_factory` is called
        as `(identity, pems)`.
        Building the initial (empty) context raises `IdentityError` if the
        server identity cannot be loaded into `ssl` -- the caller
        (`ApiWorker`) then exits."""
        store = (
            read_trust_key, read_trust, enroll_client, rotate_client,
            retire_predecessor, token_precheck, list_revoked,
        )
        if any(f is None for f in store):
            from zfsbackup.config import store as _store
            read_trust_key = read_trust_key or _store.read_trust_key
            read_trust = read_trust or _store.read_trust
            enroll_client = enroll_client or _store.enroll_client
            rotate_client = rotate_client or _store.rotate_client
            retire_predecessor = retire_predecessor or _store.retire_predecessor
            token_precheck = token_precheck or _store.token_precheck
            list_revoked = list_revoked or _store.list_revoked
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
        self._list_revoked = list_revoked
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

    def list_revoked(self) -> tuple:
        """`store.trust.list_revoked(resolved)` -> every `RevokedPin`
        (read-only, one snapshot). For `SessionRegistry`, called from the
        trust-poll thread. Waits at most `STORE_CALL_WAIT` for the store lock,
        then raises `ConfigBusy`: skipping is safe, because the registry
        marks a session checked only after a completed read, so the next tick
        asks again -- and the stall pass has already run by then."""
        return tuple(_store_call(self._list_revoked, self._resolved, wait=STORE_CALL_WAIT))

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


#: The `RevokedPin.reason` values that cut a RUNNING stream today (plan S-2,
#: DD5): the key was unpinned because it is not to be trusted any more. The
#: RULE is a denylist, though (`SessionRegistry._revocation`): every recorded
#: reason cuts except `NOT_CUT_REVOCATION_REASONS`, so a reason added to
#: `store.models.REVOCATION_REASONS` later cuts by default. This set documents
#: the reasons known now.
CUT_REVOCATION_REASONS = frozenset({
    'revoked', 'successor_revoked', 'client_revoked', 'replaced',
})

#: The one reason that does not cut by itself: `retired` is a rotation
#: predecessor retired by its successor's first request (the legitimate host
#: moving to its new key), and its stream finishes -- unless a key further
#: down its `superseded_by` chain was revoked. Nor is a certificate's expiry
#: mid-stream a reason.
NOT_CUT_REVOCATION_REASONS = frozenset({'retired'})


class StreamSession:
    """One running `/stream` websocket session, as `SessionRegistry` knows it.

    Created and (un)registered by the route's own thread; `cut_reason` is
    written once, by `SessionRegistry` under its lock (the trust-poll
    thread), and read by the route's thread, which stops at once when it is
    set. `server_dataset` is filled in by the route once it has validated the
    name (for the log line only)."""

    __slots__ = ('ws', 'fingerprint', 'client_id', 'dataset', 'server_dataset',
                 'cut_reason', 'checked_key')

    def __init__(self, ws, fingerprint: str, client_id: str, dataset: str) -> None:
        self.ws = ws
        self.fingerprint = fingerprint
        self.client_id = client_id
        self.dataset = dataset
        self.server_dataset: Optional[str] = None
        #: Why the session was cut (`SessionRegistry._cut`), else `None`.
        self.cut_reason: Optional[str] = None
        #: The trust-view key at which `SessionRegistry` last read the
        #: revocation records for this session successfully and found
        #: nothing that cuts it (written and read by the trust-poll thread
        #: only). Anything that can change the answer -- a revocation, a
        #: rotation, an enrollment -- bumps the trust generation and so the
        #: view's key; until it moves the store is not asked again.
        self.checked_key: Optional[tuple] = None


def _ws_force_eof(ws) -> Optional[Callable[[], None]]:
    """How to shut a websocket's connection down at the OS level from another
    thread: the `_SessionSocket`'s `force_eof` under the API worker's server,
    else a plain `socket.shutdown` of a real socket, else `None`."""
    sock = getattr(ws, 'sock', None)
    force_eof = getattr(sock, 'force_eof', None)
    if callable(force_eof):
        return force_eof
    if isinstance(sock, socket.socket):
        return functools.partial(socket.socket.shutdown, sock, socket.SHUT_RDWR)
    return None


class SessionRegistry:
    """The running `/stream` sessions of this API worker (plan S-2), so a
    revoked client's transfer and a writer stalled on a peer that does not
    read can be cut. `app.extensions['zfsbackup.sessions']`.

    **Threads.**

    - A *route thread* (one per connection; `create_app.backup_stream`)
      `register()`s its session before it does any work and `unregister()`s
      it, in a `finally`, AFTER `_end_websocket` and before the route
      returns. It reads its own session's `cut_reason`.
    - The *trust-poll thread* (`workers.ApiWorker._start_trust_poll`) calls
      `enforce()` after every `TrustGateway.poll()`. It is the only caller of
      `enforce`, hence the only thread that cuts a session and the only one
      that calls `store.trust.list_revoked`.
    - Nothing else touches the registry.

    **Lock.** `_lock` guards `_sessions` and is held for the whole of a cut
    (a `shutdown(2)` call and a counter, both non-blocking) and for
    `register`/`unregister`. It is never held across a `store.trust` call or
    a log call, and the only lock taken under it is
    `_SessionSocket._state`, which nothing holding `_state` ever takes
    `_lock` under. So the order is `_lock` -> `_state`, never the reverse.

    **Descriptor lifetime.** A cut is `force_eof()`, a `socket.shutdown` of
    the connection's descriptor. That is only safe while the connection is
    open: werkzeug closes it after the route returns, and the number can then
    belong to another client. So the rule is: *a cut happens only to a
    session that is registered, under `_lock`, and a session is unregistered
    (under `_lock`) before its route returns*. `unregister()` therefore
    returns only after any cut that was running has finished, and none
    starts afterwards; the connection is closed later still.
    `_end_websocket` runs BEFORE `unregister`, deliberately: a route stuck in
    it (its close frame waits for `_write_lock`, held by the reader thread's
    blocked pong) must still be reachable by the write-stall check.

    **What is cut, and why a trust-view diff.** `enforce` asks the store
    only about registered sessions whose fingerprint is no longer in the
    gateway's view (`TrustGateway.view.pins`) and that have not been checked
    at the view's current key: a pin leaves the view when it is unpinned,
    and every unpinning is recorded in `revoked_fingerprints` in the same
    transaction, with the reason. So the cost of an idle server is one
    dictionary lookup per session per tick, and of a session on a retired
    key one `list_revoked` per trust change, however long it runs. A session
    whose pin is still in the view is never cut by revocation; an expired
    certificate is not cut either (the pin stays in the view). The reasons
    that cut are every recorded reason but `retired` (a denylist,
    `NOT_CUT_REVOCATION_REASONS`; `CUT_REVOCATION_REASONS` lists today's);
    no record at all (e.g. the
    database file was replaced) cuts nothing.

    **A retired key is followed down its chain.** A rotation records the
    old key as `retired` with `superseded_by` = the key whose first request
    retired it, and a LATER revocation of that key does not change its
    record (`revoke_pin` keeps the first reason and unpins the live
    successors as `successor_revoked`; `revoke_client` unpins them as
    `client_revoked`). So a session on a retired key is cut when any key
    along `superseded_by` is itself recorded with a cut reason: the thief
    who kept the old key's connection open through the rotation does not
    outlive `trust revoke OLD`, `trust revoke NEW` or `trust revoke
    --client`. A chain that ends in a live pin, or in nothing (no record,
    or a `retired` record without `superseded_by`, written before that
    column existed), cuts nothing. The chain walk uses only the records
    `list_revoked` returns, is cycle-safe, and costs no extra store call.

    **Stall bound.** Every tick, each registered session's socket is asked to
    `force_eof_if_write_stalled(WS_IDLE_TIMEOUT)`.

    **What this does not cover.** A trusted peer sending one frame every
    less than `WS_IDLE_TIMEOUT` seconds is neither stalled (it reads) nor
    idle; it holds its dataset claim (and a gate slot, so a reload waits
    for it) as long as it likes, until it is revoked. The claim is scoped
    to the client's own `<target>/<client_id>/...`, so only that client and
    a reload suffer."""

    def __init__(self, stall_limit: float = WS_IDLE_TIMEOUT) -> None:
        self._stall_limit = stall_limit
        self._lock = threading.Lock()
        self._sessions: 'list[StreamSession]' = []
        #: The failure in effect per kind of pass (`_note`): "read" (the
        #: revocation list), "stall" (the write-stall check) and "cut"
        #: (the revocation pass's per-session work), each logged once.
        self._problems: dict = {}

    def register(self, session: StreamSession) -> None:
        with self._lock:
            self._sessions.append(session)

    def unregister(self, session: StreamSession) -> None:
        """Remove `session`; after this returns nothing can cut it (see the
        class docstring). Idempotent."""
        with self._lock:
            with contextlib.suppress(ValueError):
                self._sessions.remove(session)

    def __len__(self) -> int:
        with self._lock:
            return len(self._sessions)

    def enforce(self, trust: 'TrustGateway') -> int:
        """Both passes, stall check first -> how many sessions were cut.
        The API worker's trust-poll thread calls the two separately
        (`enforce_stalls` before `TrustGateway.poll()`,
        `enforce_revocations` after it). Never raises."""
        return self.enforce_stalls() + self.enforce_revocations(trust)

    def enforce_stalls(self) -> int:
        """The write-stall pass, from the trust-poll thread: shut down every
        session whose write has been blocked for `stall_limit`. Needs no
        database, so it runs BEFORE `TrustGateway.poll()` and is never held
        up by a slow store. A failure for one session is isolated from the
        others, and logged once while it persists (WARNING; INFO when it
        clears). Never raises."""
        try:
            with self._lock:
                sessions = list(self._sessions)
        except Exception:  # pragma: no cover -- a plain list copy
            return 0
        cuts = 0
        failure = None
        for session in sessions:
            try:
                if self._cut_if_stalled(session):
                    cuts += 1
            except Exception as e:
                logger.debug("stall check of a session failed", exc_info=True)
                failure = failure or f"{type(e).__name__}: {e}"
        self._note(
            'stall', failure and f"the write-stall check of a running stream failed: {failure}",
            "the write-stall check works again",
        )
        return cuts

    def enforce_revocations(self, trust: 'TrustGateway') -> int:
        """The revocation pass, from the trust-poll thread after
        `TrustGateway.poll()`: cut the sessions whose pin was revoked (see the
        class docstring). Never raises; a failure is logged once while it
        persists and the pass is retried next tick (no session is marked
        checked unless its own check completed)."""
        try:
            return self._cut_revoked(trust)
        except Exception as e:
            logger.debug("revocation pass failed", exc_info=True)
            self._note(
                'cut', f"the revocation check of running streams failed: "
                f"{type(e).__name__}: {e}", "the revocation check works again",
            )
            return 0

    def _note(self, kind: str, problem: Optional[str], recovered: str) -> None:
        """Log `problem` at WARNING once per distinct text per `kind` while it
        lasts, and `recovered` at INFO when `problem` is `None` after one."""
        before = self._problems.get(kind)
        if problem is None:
            if before is not None:
                del self._problems[kind]
                logger.info(recovered)
        elif problem != before:
            self._problems[kind] = problem
            logger.warning(problem)

    def _cut_if_stalled(self, session: StreamSession) -> bool:
        message = None
        with self._lock:
            if session not in self._sessions or session.cut_reason is not None:
                return False
            check = getattr(getattr(session.ws, 'sock', None),
                            'force_eof_if_write_stalled', None)
            if not callable(check):
                return False
            # Unlike `_cut`, the reason is published AFTER the shutdown: the
            # socket decides "stalled" and shuts down in one step under its
            # own `_state` (so a finished write is never cut by mistake), and
            # setting `cut_reason` beforehand would mark a healthy session
            # cut. A route thread woken by the shutdown that looks before the
            # next line still ends the stream (`ConnectionClosed`); it only
            # misses the "was cut" INFO line.
            age = check(self._stall_limit)
            if age is None:
                return False
            session.cut_reason = f"a write to the client has been blocked for {age:.0f}s"
            message = self._describe(session)
        logger.warning("cutting the running stream %s", message)
        return True

    def _cut_revoked(self, trust: 'TrustGateway') -> int:
        with self._lock:
            sessions = list(self._sessions)
        view = trust.view
        if view.key is None:
            return 0   # nothing has been trusted yet: no session can exist
        gone = [s for s in sessions
                if s.cut_reason is None and s.fingerprint not in view.pins
                and s.checked_key != view.key]
        if not gone:
            self._note('read', None, "the revocation list can be read again")
            return 0
        try:
            records = {r.fingerprint: r for r in trust.list_revoked()}
        except Exception as e:
            # Includes `ConfigBusy`: the store lock was not free within
            # `STORE_CALL_WAIT`. Nothing is marked checked, so the next tick
            # asks again.
            self._note(
                'read',
                f"cannot check whether {len(gone)} running stream(s) lost their "
                f"client certificate: {type(e).__name__}: {e}; retrying every "
                f"{TRUST_POLL_SECONDS:g} s",
                "the revocation list can be read again",
            )
            return 0
        self._note('read', None, "the revocation list can be read again")
        cuts = 0
        failure = None
        for session in gone:
            try:
                why = self._revocation(session.fingerprint, records)
                if why is None:
                    session.checked_key = view.key
                elif self._cut(session, why):
                    cuts += 1
            except Exception as e:
                logger.debug("revocation check of a session failed", exc_info=True)
                failure = failure or f"{type(e).__name__}: {e}"
        self._note(
            'cut', failure and f"the revocation check of a running stream failed: {failure}",
            "the revocation check works again",
        )
        return cuts

    @staticmethod
    def _revocation(fingerprint: str, records: dict) -> Optional[str]:
        """Why the session on `fingerprint` must be cut, or `None`. The
        key's own record, else -- for a `retired` one -- that of each key
        along `superseded_by`, oldest first. A denylist: any recorded reason
        cuts except `retired`, which is followed instead."""
        record = records.get(fingerprint)
        seen = {fingerprint}
        via = ''
        while record is not None:
            if record.reason not in NOT_CUT_REVOCATION_REASONS:
                if not via:
                    return f"its client certificate was revoked ({record.reason})"
                return f"{via}revoked ({record.reason})"
            successor = record.superseded_by if record.reason == 'retired' else None
            if successor is None or successor in seen:
                return None
            seen.add(successor)
            via = f"its certificate was retired by a rotation and its successor {successor} was "
            record = records.get(successor)
        return None

    def _cut(self, session: StreamSession, reason: str) -> bool:
        with self._lock:
            if session not in self._sessions or session.cut_reason is not None:
                return False
            session.cut_reason = reason
            force_eof = _ws_force_eof(session.ws)
            if force_eof is not None:
                with contextlib.suppress(Exception):
                    force_eof()
            message = self._describe(session)
        logger.warning("cutting the running stream %s", message)
        return True

    @staticmethod
    def _describe(session: StreamSession) -> str:
        return (
            f"of client {session.client_id} for "
            f"{session.server_dataset or session.dataset!r}: {session.cut_reason}"
        )


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


class ZFSBudget:
    """One overall time budget for the non-streaming `zfs` calls of one request
    handler (plan S-14).

    `call(fn, ...)` runs `fn` under `libzfseasy.timeout(<what is left>)`, so a
    handler making N calls shares `seconds` between them instead of getting
    `seconds` per call; with the budget spent `call` raises `ZFSTimeoutError`
    without running anything. The limit applies per `zfs` command: a helper
    that runs several (`read_zfsbackup_properties` runs up to three) can
    overrun by its extra commands, each limited to what was left when the helper
    started, and the kill of a timed-out child adds up to 4 s. A child that
    survives SIGKILL (a hung pool) stays registered in this process, and every
    later timed call here fails fast, healthy pools included, until it exits.

    The libzfseasy limit follows the calling thread, so `call` must run `fn`
    synchronously, and `fn` must not return a lazy iterator. Never use it for
    `zfs.receive`/`zfs.send`: those ignore the timeout and must not be cut."""

    def __init__(self, seconds: Optional[float] = None, clock: Callable[[], float] = time.monotonic) -> None:
        self.seconds = ZFS_REQUEST_BUDGET if seconds is None else seconds
        self._clock = clock
        self._deadline = clock() + self.seconds

    def remaining(self) -> float:
        return self._deadline - self._clock()

    def call(self, fn, *args, **kwargs):
        left = self.remaining()
        if left <= 0:
            raise ZFSTimeoutError(
                f"the {self.seconds:g} s budget for this request is spent"
            )
        with zfs_timeout(left):
            return fn(*args, **kwargs)


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
    peer_limits: Optional['PeerLimits'] = None,
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
    `peer_limits` (default: a new `PeerLimits`) is the flood-control state
    (plan S-3), also published as `app.extensions['zfsbackup.peer_limits']` for
    the server (accept-time limits) and the trust-poll thread (the hourly
    summary); here it guards `/enroll` and throttles the refusal log lines.

    **Authentication** (`before_request`; it also runs on the websocket
    upgrade of `/stream`, so it is checked when a request or a stream
    STARTS; a stream already running is cut only by `SessionRegistry.enforce`,
    which the API worker's trust-poll thread runs, when its pin is REVOKED --
    not when it is retired by a rotation or the certificate expires):

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
    #: The running `/stream` sessions, for the trust-poll thread to cut a
    #: revoked client's transfer and a stalled writer (plan S-2).
    sessions = SessionRegistry()
    app.extensions['zfsbackup.sessions'] = sessions
    limits = peer_limits if peer_limits is not None else PeerLimits()
    app.extensions['zfsbackup.peer_limits'] = limits
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

    #: `client id -> its server datasets in pending_configs, oldest first`
    #: (S-3). The client id is the one the server dataset name is built from
    #: (`<target>/<bound id>/...`), so it is the authenticated binding. One
    #: client's entries are capped at `PENDING_CONFIGS_PER_CLIENT`: past it,
    #: ITS oldest entry goes, never another client's -- the global
    #: `PENDING_CONFIGS_MAX` eviction stays as the backstop only. Same lock.
    pending_by_client: 'dict[str, collections.OrderedDict]' = {}

    #: `pending server dataset -> monotonic deadline` of the entries a CHILD's
    #: refused negotiate (`unsettled_ancestor`, kind `pending`) is waiting
    #: for: they are not evicted by the per-client cap until the mark lapses
    #: (`PENDING_CONFIG_TTL`, refreshed by each retry). A refused child holds
    #: no entry of its own, so without the mark nothing but its own retry
    #: would keep the parent. Same lock.
    pending_needed: dict = {}

    def _pending_owner(server_dataset: str) -> str:
        """The client id a pending server dataset belongs to."""
        if target and server_dataset.startswith(target + '/'):
            return server_dataset[len(target) + 1:].split('/', 1)[0]
        return ''

    def _drop_pending_locked(server_dataset: str) -> None:
        pending_configs.pop(server_dataset, None)
        pending_deadlines.pop(server_dataset, None)
        pending_needed.pop(server_dataset, None)
        owned = pending_by_client.get(_pending_owner(server_dataset))
        if owned is not None:
            owned.pop(server_dataset, None)
            if not owned:
                del pending_by_client[_pending_owner(server_dataset)]

    def _evictable_locked(owned, new_name: str, claimed) -> Optional[str]:
        """The oldest of one client's entries that may be evicted to make room
        for `new_name`: an expired one first; else the oldest that is NOT an
        ancestor of another pending name, of a claimed name or of the new
        name (`_ancestors_state` holds a child back on a pending ancestor;
        losing it would let a placeholder parent be created under it). `None`
        when every entry is such an ancestor."""
        now = time.monotonic()
        for name in owned:
            if pending_deadlines.get(name, 0.0) <= now:
                return name
        needed = {n for n, until in pending_needed.items() if until > now}
        for name in itertools.chain(owned, claimed, (new_name,)):
            while '/' in name:
                name = name.rsplit('/', 1)[0]
                needed.add(name)
        for name in owned:
            if name not in needed:
                return name
        return None

    def _store_pending_locked(
        server_dataset: str, canonical: str, dataset: str, claimed=frozenset()
    ) -> Optional[int]:
        """Record the entry; -> how many of the SAME client's entries the
        per-client cap evicted (the caller logs it, outside the lock), or
        `None` when the client is at its cap and none of its entries may be
        evicted (nothing is recorded; the negotiate is refused, retryable).
        An entry being refreshed never counts against the cap."""
        owner = _pending_owner(server_dataset)
        owned = pending_by_client.get(owner)
        evicted = 0
        if owned is not None and server_dataset not in owned:
            while len(owned) >= PENDING_CONFIGS_PER_CLIENT:
                victim = _evictable_locked(owned, server_dataset, claimed)
                if victim is None:
                    return None
                _drop_pending_locked(victim)
                evicted += 1
                owned = pending_by_client.get(owner)
                if owned is None:
                    break
        pending_configs[server_dataset] = (canonical, dataset)
        pending_configs.move_to_end(server_dataset)
        pending_deadlines[server_dataset] = time.monotonic() + PENDING_CONFIG_TTL
        owned = pending_by_client.setdefault(owner, collections.OrderedDict())
        owned[server_dataset] = None
        owned.move_to_end(server_dataset)
        while len(pending_configs) > PENDING_CONFIGS_MAX:
            _drop_pending_locked(next(iter(pending_configs)))
        return evicted

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
        same client dataset refreshes its deadline), the OTHER client
        dataset that already has a live entry, or `PENDING_FULL` (the client
        is at `PENDING_CONFIGS_PER_CLIENT` and none of its entries may be
        evicted: the caller answers 503 + retry)."""
        with streams_guard:  # a copy, taken before (never inside) pending_guard
            claimed = frozenset(streams_in_progress)
        with pending_guard:
            entry = _live_pending_locked(server_dataset)
            if entry is not None and entry[1] != dataset:
                return entry[1]
            evicted = _store_pending_locked(server_dataset, canonical, dataset, claimed)
        if evicted is None:
            owner = _pending_owner(server_dataset)
            limits.log.log(
                'pending', owner, logging.WARNING,
                "client %s has %d datasets negotiated and not yet received, all needed "
                "as parents of the others; refusing another negotiate (503, retry)",
                owner, PENDING_CONFIGS_PER_CLIENT,
            )
            return PENDING_FULL
        if evicted:
            owner = _pending_owner(server_dataset)
            limits.log.log(
                'pending', owner, logging.WARNING,
                "client %s has more than %d datasets negotiated and not yet received; "
                "its %d oldest pending configuration(s) were dropped (it must negotiate "
                "them again; other clients are not affected)",
                owner, PENDING_CONFIGS_PER_CLIENT, evicted,
            )
        return None

    def _mark_pending_needed(server_dataset: str) -> None:
        """A child was refused because `server_dataset` is pending: keep that
        entry (most recently used, and protected from the per-client cap)."""
        with pending_guard:
            if _live_pending_locked(server_dataset) is None:
                return
            pending_needed[server_dataset] = time.monotonic() + PENDING_CONFIG_TTL
            owned = pending_by_client.get(_pending_owner(server_dataset))
            if owned is not None and server_dataset in owned:
                owned.move_to_end(server_dataset)
            pending_configs.move_to_end(server_dataset)

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

    # ------------------------------------------------------- zfs time budget

    def _zfs(fn, *args, **kwargs):
        """Run the non-streaming `zfs` call `fn` under the request's
        `ZFSBudget` (plan S-14; created at the request's first call, so it does
        not count time spent before it). Raises `ZFSTimeoutError`; a
        handler answers that with `ZFS_TIMEOUT_ERROR`, never as a `zfs`
        failure. Never wrap `zfs.receive.filesystem` in it. The budget is not
        restarted after the receive starts: nothing after it is a pre-check."""
        budget = g.get('zfs_budget')
        if budget is None:
            budget = g.zfs_budget = ZFSBudget()
        return budget.call(fn, *args, **kwargs)

    def _zfs_new_budget() -> None:
        """Start the request's `ZFSBudget` afresh (a stream after the header
        wait, which is the peer's time, not zfs's)."""
        g.zfs_budget = ZFSBudget()

    #: `(handler, server dataset)` pairs whose `zfs` timeout was already
    #: announced at WARNING; forgotten when the handler succeeds for that
    #: dataset, so each episode is announced once. Bounded.
    _zfs_timeout_noted: set = set()

    def _zfs_timed_out(
        where: str, client_id: str, dataset: str, server_dataset: str, exc: Exception,
        *, unknown_outcome: bool = False,
    ) -> None:
        """Log a handler's `zfs` timeout: WARNING once per (handler, server
        dataset) and episode, DEBUG after. A write or an abort that timed out
        has an unknown outcome (`unknown_outcome`), said in the line."""
        key = (where, server_dataset)
        text = (
            "%s from client %s for %r: zfs did not answer in time on %s (%s)%s; "
            "answering 503 retry"
        )
        tail = (
            "; the outcome of that command is unknown and is re-read on the "
            "next attempt" if unknown_outcome else ""
        )
        if key in _zfs_timeout_noted:
            logger.debug(text, where, client_id, dataset, server_dataset, exc, tail)
            return
        if len(_zfs_timeout_noted) > 1024:
            _zfs_timeout_noted.clear()
        _zfs_timeout_noted.add(key)
        logger.warning(text, where, client_id, dataset, server_dataset, exc, tail)

    def _zfs_recovered(where: str, server_dataset: str) -> None:
        _zfs_timeout_noted.discard((where, server_dataset))

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
        encryption = _zfs(zfs.get.value, server_fs, 'encryption')
        if encryption in (None, 'off'):
            return 'plain'
        return 'raw' if _zfs(zfs.get.value, server_fs, 'encryptionroot') == server_fs.name else 'plain'

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
        token). Raises on any other failure (`ZFSTimeoutError` included):
        callers must not guess."""
        ds = Dataset(server_dataset)
        if not _zfs(zfs.exists, ds):
            return None
        results = _zfs(zfs.list, roots=ds, properties=['receive_resume_token'])
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
        Raises if the dataset's properties cannot be read
        (`ZFSTimeoutError` included)."""
        ds = Dataset(server_dataset)
        if not _zfs(zfs.exists, ds):
            return None
        props = _zfs(read_zfsbackup_properties, ds)
        recorded_id = props.get(PROP_CLIENT_ID)
        if recorded_id and recorded_id != client_id:
            return _owner_problem(server_dataset, recorded_id, client_id)
        problem = _owner_problem(
            server_dataset, _zfs(zfs.get.value, ds, PROP_CLIENT_ID, sources='local'), client_id
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

    def _peer() -> str:
        """The throttle key of the requesting peer (`peer_key`)."""
        return peer_key(request.remote_addr)

    def _protocol_refusal(who: str, key: Optional[str] = None):
        """None if the request names a supported protocol version
        (`X-Zfsbackup-Protocol`), else the 426 response (plan S-5). `who` is
        the text for the log line (an address, or a client id and address);
        the log line (ERROR) goes through the shared per-peer throttle
        (S-3: one per `key` per hour, `key` defaulting to the requesting
        peer): the key is the peer, never the header value, which an
        unauthenticated peer (`/enroll`) chooses freely. A legitimate client
        that is refused is told once an hour and by its own log every cycle."""
        raw = request.headers.get(PROTOCOL_HEADER)
        version = parse_protocol_header(raw)
        if version in SUPPORTED_PROTOCOLS:
            return None
        sent = 'no protocol version' if raw is None else f'protocol version {raw[:16]!r}'
        limits.log.log(
            'protocol', key if key is not None else _peer(), logging.ERROR,
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

    def _enroll_admission():
        """S-3: the `/enroll` lockout, in `before_request` after the
        protocol check and before the body is read or the store touched:
        429 + `Retry-After` while the peer is locked out (`EnrollGuard`: 5
        failures in 15 min), else `None`. It takes no in-flight slot: a slot
        held while the body is read could be held by a peer that never sends
        it (`_enroll_enter` takes it, later).
        """
        peer = _peer()
        wait = limits.enroll.retry_after(peer)
        if wait:
            limits.log.log(
                'enroll-locked', peer, logging.WARNING,
                "enrollment from %s refused: locked out for %d more seconds after %d "
                "failed attempts", request.remote_addr, wait, limits.enroll.max_failures,
            )
            response = jsonify({
                'error': f'too many failed enrollment attempts from this address; '
                         f'retry in {wait} seconds',
                'reason': 'rate_limited',
                'retry_after': wait,
            })
            response.status_code = 429
            response.headers['Retry-After'] = str(wait)
            return response
        return None

    def _enroll_enter():
        """S-3: take an `/enroll` in-flight slot -- called after the body
        was read and the cheap checks passed, right before the token
        pre-check (the first store call). -> a 503 + `retry` response when
        `ENROLL_MAX_IN_FLIGHT` requests already run or this peer already has
        `ENROLL_MAX_IN_FLIGHT_PER_PEER`, else `None` with the slot held
        (`g.enroll_slot`, given back by `_enroll_leave` on every outcome).
        503, not 429: the cause is this server's capacity, not the caller's
        behaviour; the existing client (`cli/enroll.py`) already treats 503 +
        `retry` as "busy, retry in a moment" (exit 75). Not a failure."""
        peer = _peer()
        if not limits.enroll.try_enter(peer):
            limits.log.log(
                'enroll-busy', peer, logging.WARNING,
                "enrollment from %s refused: enrollments are already in progress "
                "(at most %d at once, %d per address)", request.remote_addr,
                limits.enroll.max_in_flight, limits.enroll.max_per_peer,
            )
            response = jsonify({
                'error': 'the server is busy with other enrollments; retry in a moment',
                'reason': 'busy',
                'retry': True,
            })
            response.status_code = 503
            response.headers['Retry-After'] = '2'
            return response
        g.enroll_slot = peer
        return None

    @app.teardown_request
    def _enroll_leave(exc):
        """Give back the `/enroll` in-flight slot, on every outcome."""
        peer = g.pop('enroll_slot', None)
        if peer is not None:
            limits.enroll.leave(peer)

    def _enroll_failed() -> bool:
        """Count a failed `/enroll` against the requesting peer's lockout."""
        peer = _peer()
        locked = limits.enroll.record_failure(peer)
        if locked:
            limits.log.log(
                'enroll-locked', peer, logging.WARNING,
                "enrollment from %s: %d failed attempts; locked out of /enroll for %d seconds",
                request.remote_addr, limits.enroll.max_failures,
                int(limits.enroll.window),
            )
        return locked

    @app.before_request
    def _authenticate():
        """Route authentication and the protocol check; see `create_app`.
        Returning a response refuses the request (for `/stream`: the
        websocket upgrade)."""
        if request.endpoint in PROTOCOL_EXEMPT_ENDPOINTS:
            return None
        if request.endpoint in UNAUTHENTICATED_ENDPOINTS:
            refused = _protocol_refusal(request.remote_addr or '?')
            if refused is None and request.endpoint == 'enroll':
                return _enroll_admission()
            return refused
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
            limits.log.log(
                'auth', _peer(), logging.WARNING,
                "API: refusing %s %s from %s: client certificate %s is %s",
                request.method, request.path, request.remote_addr,
                fingerprint or '(unreadable)',
                'not pinned' if pin is None else 'expired',
            )
            return jsonify(NOT_TRUSTED_ERROR), 403
        # After authentication, so an unauthenticated caller learns nothing
        # but 401/403; before anything with a side effect (the predecessor
        # retirement below, the route).
        refused = _protocol_refusal(
            f"client {pin.client_id} ({request.remote_addr})", f"{pin.client_id}@{_peer()}"
        )
        if refused is not None:
            return refused
        g.client_id = pin.client_id
        g.fingerprint = fingerprint
        url_id = (request.view_args or {}).get('client_id')
        if url_id is not None and url_id != pin.client_id:
            limits.log.log(
                'auth', f"{pin.client_id}@{_peer()}", logging.WARNING,
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
        except (BadRequest, OSError, ValueError):
            # The peer announced a body and did not send it (werkzeug's
            # `ClientDisconnected` is a `BadRequest`), hung up, or hit the
            # read deadline (`TimeoutError`). A refusal like any other: the
            # callers count it as a failed attempt (S-3).
            return None, (jsonify({'error': 'the request body could not be read'}), 400)
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
            limits.log.log(
                'store-busy', _peer(), logging.WARNING,
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
        - 429 `{"error", "reason": "rate_limited", "retry_after"}` with
          `Retry-After: <seconds>` (900 right after the lockout): this address
          made `ENROLL_FAILURES_MAX` failed attempts (any 4xx below except
          426, counted per address -- the /64 for IPv6) within
          `ENROLL_FAILURE_WINDOW`; checked in `before_request`, so the body
          is not read and the store not touched. Only `/enroll` is locked.
        - 503 `{"error", "reason": "busy", "retry": true}` with `Retry-After`:
          `ENROLL_MAX_IN_FLIGHT` enrollments are already past their body and
          shape checks (or this address already has one); taken right before
          the token pre-check, never while the body is read; not counted as
          a failure.
        - Check order: shape 400 -> grammar 400 -> label 400 -> token parse
          400 -> token for another server 403 refused -> certificate 422 ->
          signature 403 -> in-flight cap 503 -> token pre-check 403 refused (read-only, before any
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
            limits.log.log(
                'enroll', _peer(), logging.WARNING,
                "enrollment from %s refused: malformed request", request.remote_addr,
            )
            _enroll_failed()
            return problem
        remote = request.remote_addr

        def failed(category: str, response):
            # One WARNING per peer per hour (S-3); every failure counts
            # towards the peer's lockout.
            limits.log.log(
                'enroll', _peer(), logging.WARNING,
                "enrollment from %s refused: %s", remote, category,
            )
            _enroll_failed()
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
        busy = _enroll_enter()  # first store call from here on: bound them
        if busy is not None:
            return busy
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
            dataset_exists = bool(_zfs(zfs.exists, client_root))
        except Exception as e:
            # Includes `ZFSTimeoutError` (S-14): the same retryable 503 as any
            # `zfs` failure here, which the enrolling CLI already handles.
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

        if outcome in (EnrollOutcome.ENROLLED, EnrollOutcome.ALREADY_TRUSTED):
            limits.enroll.forget(_peer())
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
            limits.log.log(
                'rotate', f"{client_id}@{_peer()}", logging.WARNING,
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
        fails (`ZFSTimeoutError` included)."""
        base = f"{target}/{client_id}"
        ancestor = server_dataset.rsplit('/', 1)[0]
        missing = []
        while ancestor == base or ancestor.startswith(base + '/'):
            if _is_claimed(ancestor):
                return (ancestor, 'claimed'), missing, None
            if _pending(ancestor) is not None:
                return (ancestor, 'pending'), missing, None
            ds = Dataset(ancestor)
            if _zfs(zfs.exists, ds):
                conflict = _owner_problem(
                    ancestor, _zfs(zfs.get.value, ds, PROP_CLIENT_ID, sources='local'), client_id
                )
                if conflict is not None:
                    return None, missing, conflict
                if (
                    _zfs(zfs.get.value, ds, 'receive_resume_token') is not None
                    and not _zfs(zfs.list, roots=ds, types='snapshot', properties=['creation'])
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
            stored = _zfs(zfs.get.value, server_fs, PROP_CONFIG, sources='local')
            if stored is None:
                return
            shrink = retention_shrink(stored, canonical_new)
        except ZFSTimeoutError:
            raise  # not "unreadable": the handler answers 503 retry
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
        server_fs_exists = _zfs(zfs.exists, server_fs)

        # D7 and the binding: a server dataset that records another client
        # id or another client dataset is not this one's. Checked before
        # anything is written, so its recorded owner is not overwritten.
        if server_fs_exists:
            try:
                conflict = _ownership_conflict(server_dataset, dataset, client_id)
            except ZFSTimeoutError:
                raise  # `backup_negotiate` answers 503 retry
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
                    _zfs(read_zfsbackup_properties, Dataset(server_dataset)).get(PROP_SOURCE_DATASET)
                )
            except ZFSTimeoutError:
                raise
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
            except ZFSTimeoutError:
                raise
            except Exception as e:
                logger.error("negotiate from client %s: cannot check the parents of %s: %s",
                             client_id, server_dataset, e)
                return jsonify({'error': f'Failed to check the parents of {server_dataset}'}), 500
            if ancestor_conflict is not None:
                return jsonify({'error': ancestor_conflict}), 409
            if unsettled is not None:
                ancestor, kind = unsettled
                if kind == 'pending':
                    _mark_pending_needed(ancestor)
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
                    _zfs(zfs.set, server_fs, {
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
                        if rival is PENDING_FULL:
                            return jsonify(BUSY_ERROR), 503
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
                                _zfs(
                                    zfs.create,
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
            except ZFSTimeoutError:
                # A `set` or `create` that timed out may or may not have
                # happened: nothing is assumed; the next negotiate re-reads
                # the dataset and does it again.
                raise
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
                    server_snaps = _zfs(server_snapshots, server_fs)
                    listing_trusted = True
                except ZFSTimeoutError:
                    raise
                except Exception as e:
                    logger.warning(
                        "negotiate for %s: the snapshot listing cannot be trusted (%s); "
                        "not reporting server_newer", server_dataset, e,
                    )
                    server_snaps = _zfs(server_snapshots, server_fs, strict=False)
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
            except ZFSTimeoutError:
                raise
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
            reply = _negotiate_claimed(
                client_id, dataset, server_dataset, client_snapshots, ds_config_canonical
            )
        except ZFSTimeoutError as e:
            # S-14: a `zfs` call of the handler did not answer in time (or the
            # worker's stuck-child registry refused it). Nothing is assumed
            # about a write that timed out; the claim is released below.
            _zfs_timed_out(
                'negotiate', client_id, dataset, server_dataset, e,
                unknown_outcome=True,
            )
            return jsonify(ZFS_TIMEOUT_ERROR), 503
        finally:
            _release_dataset(server_dataset)
        _zfs_recovered('negotiate', server_dataset)
        return reply

    @sock.route('/backup/<client_id>/<path:dataset>/stream')
    def backup_stream(ws, client_id: str, dataset: str):
        # Authenticated at the upgrade (`_authenticate`); the bound id only.
        client_id = g.client_id
        # Registered before anything that can block on the peer, so the
        # trust-poll thread can cut this session (revoked pin, stalled
        # write); unregistered after `_end_websocket` and before the route
        # returns, i.e. while the connection is certainly still open (see
        # `SessionRegistry`).
        session = StreamSession(ws, getattr(g, 'fingerprint', None) or '', client_id, dataset)
        sessions.register(session)
        try:
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
                _receive_stream(ws, client_id, dataset, mode, base, session)
            finally:
                gate.end()
        finally:
            try:
                # After the session's `gate.end()`: a reload's drain does not
                # wait for this.
                _end_websocket(ws)
            finally:
                sessions.unregister(session)

    def _receive_stream(
        ws, client_id: str, dataset: str, mode: str, base: StreamBase,
        session: StreamSession,
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
        session.server_dataset = server_dataset
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
                ws, client_id, dataset, server_dataset, mode, base, release_claim, session
            )
        finally:
            release_claim()

    def _receive_stream_locked(
        ws, client_id: str, dataset: str, server_dataset: str, mode: str,
        base: StreamBase, release_claim: Callable[[], None], session: StreamSession,
    ) -> None:
        server_fs = Filesystem(server_dataset)

        def zfs_timed_out(exc: Exception, *, unknown_outcome: bool = False) -> None:
            # S-14: a pre-check `zfs` call did not answer in time (or was
            # refused by the stuck-child registry). Nothing was spawned or
            # changed by the stream; the claim goes before the reply, like
            # every other refusal, and the client skips the cycle (`retry`).
            _zfs_timed_out(
                'stream', client_id, dataset, server_dataset, exc,
                unknown_outcome=unknown_outcome,
            )
            release_claim()
            with contextlib.suppress(Exception):
                ws.send(json.dumps(ZFS_TIMEOUT_ERROR))

        # D7 and the binding: refuse to receive into a server dataset that
        # another client id or client dataset owns (read-only, so also in a
        # dry run).
        try:
            conflict = _ownership_conflict(server_dataset, dataset, client_id)
        except ZFSTimeoutError as e:
            zfs_timed_out(e)
            return
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
            existed = _zfs(zfs.exists, server_fs)
            stored_config = (
                _zfs(zfs.get.value, Dataset(server_dataset), PROP_CONFIG, sources='local')
                if existed else None
            )
        except ZFSTimeoutError as e:
            zfs_timed_out(e)
            return
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

        def cut_before_changes(where: str) -> bool:
            # The session was cut (`SessionRegistry`: revoked pin, stalled
            # write) but simple-websocket still hands out the frames it had
            # queued before the EOF, so the route can get this far. From here
            # on nothing is destroyed or spawned: `receive -A` on a new
            # dataset whose first full receive was cut would destroy it.
            # Nobody to answer; the caller releases the claim.
            if session.cut_reason is None:
                return False
            logger.info(
                "stream for %s was cut (%s) %s; nothing was changed",
                server_dataset, session.cut_reason, where,
            )
            return True

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
        if cut_before_changes("after its header"):
            return
        # The wait for the header was the peer's time, not zfs's: the checks
        # from here to the receive get a budget of their own.
        _zfs_new_budget()
        mismatch = stream_base_problem(header, base)
        if mismatch is not None:
            refuse_stream('base_mismatch', f"{mismatch}, so nothing is received")
            return
        if existed:
            try:
                existing_type = _zfs(zfs.get.value, Dataset(server_dataset), 'type')
            except ZFSTimeoutError as e:
                zfs_timed_out(e)
                return
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
                rows = _zfs(server_snapshots, server_fs)
            except ZFSTimeoutError as e:
                zfs_timed_out(e)
                return
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

        if cut_before_changes("before the receive state was discarded"):
            release_claim()
            return
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
            except ZFSTimeoutError as e:
                zfs_timed_out(e)
                return
            except Exception as e:
                release_claim()
                ws.send(json.dumps({
                    'error': f'Failed to check {server_dataset} for partial receive state: {e}'
                }))
                return
            if token:
                # Again, after the lookup (a zfs call that can take a while):
                # a revoke landing during it must not reach `receive -A`.
                if cut_before_changes("before the receive state was discarded"):
                    release_claim()
                    return
                logger.warning(
                    "discarding the partial receive state of %s: the client "
                    "is sending a fresh stream", server_dataset,
                )
                try:
                    _zfs(zfs.receive.abort, Dataset(server_dataset))
                except ZFSTimeoutError as e:
                    # The outcome of the abort is unknown: the saved state may
                    # or may not be gone (and a new dataset's first partial
                    # receive with it). Nothing is assumed: the receive is not
                    # started, and the next negotiate and stream re-read the
                    # dataset and its token.
                    zfs_timed_out(e, unknown_outcome=True)
                    return
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
            # Asked before the receive is built, under the budget: a timeout
            # here spawns nothing. The receive itself gets no timeout.
            force = decision.force and _zfs(zfs.exists, server_fs)
        except ZFSTimeoutError as e:
            zfs_timed_out(e)
            return
        except Exception as e:
            release_claim()
            ws.send(json.dumps({'error': f'Failed to start zfs receive: {e}'}))
            return
        # Last, after the (possibly slow) `exists` above: a revoke landing
        # during it must not reach the spawn.
        if cut_before_changes("before zfs receive was started"):
            release_claim()
            return
        _zfs_recovered('stream', server_dataset)
        try:
            writer = zfs.receive.filesystem(
                server_fs, props={**owned, **receive_props},
                force=force, holds=False,
                save=True, mount=False,
            )
        except Exception as e:
            release_claim()
            ws.send(json.dumps({'error': f'Failed to start zfs receive: {e}'}))
            return

        reaped = False  # `writer.close()` already called on the write-error path
        closed_ok = False  # `writer.close()` returned: zfs receive exited 0

        def note_cut(completed: bool) -> None:
            # Only for a session `SessionRegistry` cut. What is kept is
            # decided by `writer.close()` (zfs receive's own exit status),
            # not by where the cut landed: a cut after the last data frame
            # (a queued `{"done": true}` met the cut check first) or during
            # the final ack leaves a complete, committed receive.
            if session.cut_reason is None:
                return
            if completed:
                logger.info(
                    "stream for %s was cut (%s) after the receive had completed; "
                    "the snapshot was committed",
                    server_dataset, session.cut_reason,
                )
            else:
                logger.info(
                    "stream for %s was cut (%s); zfs receive reaped, the partial "
                    "receive state is kept for a resume",
                    server_dataset, session.cut_reason,
                )

        try:
            first = head_bytes  # the header record and what arrived with it, unchanged
            while True:
                if first is not None:
                    data, first = first, None
                else:
                    data = ws.receive(timeout=WS_IDLE_TIMEOUT)
                if session.cut_reason is not None:
                    # Cut by the trust-poll thread (revoked pin, stalled
                    # write; `SessionRegistry`): the socket is already shut
                    # down, but frames the reader had queued before that would
                    # otherwise still be fed to `zfs receive`. Stop here; the
                    # handler below reaps the receive, which `-s` leaves
                    # resumable.
                    raise _StreamCut(session.cut_reason)
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
            closed_ok = True
            # The dataset exists now with the owned values as local ones.
            _forget_pending(server_dataset)
            _refused_noted.difference_update(
                {k for k in list(_refused_noted) if k[0] == server_dataset}
            )
            # The receive has finished and been reaped: nothing more touches
            # this dataset. Release the per-dataset claim BEFORE the final
            # ack, or a client that streams its next snapshot as soon as it
            # reads `ok` (a chain step, a resume follow-up) can get `busy`.
            release_claim()
            ws.send(json.dumps({'status': 'ok'}))
        except (ConnectionClosed, _StreamCut):
            # Client already disconnected, or the session was cut
            # (`_StreamCut`, `session.cut_reason`); best-effort reap the `zfs
            # receive` process: its stdin is closed, so it exits (`-s` keeps
            # a partial receive resumable) and no child outlives the stream.
            # Any error it raises here can't be reported back (the socket is
            # gone) so it's intentionally suppressed. A cut session also
            # carries a read error that is the symptom, not the cause, when
            # the reader saw the shutdown first -- hence the same branch.
            if not reaped:
                try:
                    writer.close()
                    closed_ok = True
                except Exception:
                    pass
            note_cut(closed_ok)
        except Exception as e:
            if not reaped:
                try:
                    writer.close()
                    closed_ok = True
                except Exception:
                    pass
            note_cut(closed_ok)
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
            reply = jsonify({'resume_token': _resume_token(server_dataset)})
        except ZFSTimeoutError as e:
            _zfs_timed_out('resume_token', client_id, dataset, server_dataset, e)
            return jsonify(ZFS_TIMEOUT_ERROR), 503
        except Exception as e:
            return jsonify({'error': str(e)}), 500
        _zfs_recovered('resume_token', server_dataset)
        return reply

    return app

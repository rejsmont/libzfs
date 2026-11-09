"""Receive-side helpers shared by the HTTP API (`zfsbackup.api`) and, later, the
local destination: plain functions, no HTTP, no claims, no logging.

Each function that runs `zfs` takes a `zfs_call` first: a callable `zfs_call(fn,
*args, **kwargs)` that runs the non-streaming `zfs` call `fn` (the API passes its
per-request budgeted `_zfs`; a local destination passes a timed call). Whatever
`zfs_call` raises (`ZFSTimeoutError` included) propagates: every caller decides
what a failure means. `lib` (keyword-only, default `libzfseasy`) is the module
the `zfs` commands are looked up on; the API passes its own `zfs` name so a test
that replaces `zfsbackup.api.zfs` still reaches these functions.

The check order of a receive is a security property and stays with the caller:
ownership -> the owned values -> the stream header -> the `-F` decision ->
the abort of a partial receive -> the spawn. Nothing here reorders it; these are
only the steps.

This module imports nothing from `zfsbackup.api` (the API imports it, and
re-exports the names tests and callers know from there). The pure decisions
the API made at module level -- the stream header (`parse_stream_header`), the
declared base (`stream_base_problem`), the canonical configuration, the
retention horizon, the snapshot rows and the `-F` decision (`decide_receive`)
-- live here too, so the local destination (`zfsbackup.local`) uses them
without importing Flask.
"""

import shlex
import struct
from datetime import timedelta
from typing import Callable, List, Mapping, NamedTuple, Optional, Tuple

import libzfseasy as zfs
from libzfseasy import ZFSTimeoutError
from libzfseasy.types import Dataset, Filesystem

from zfsbackup.backup_manager import (
    PROP_CONFIG, PROP_CLIENT_ID, PROP_SOURCE_DATASET, property_int, read_zfsbackup_properties,
)
from zfsbackup.config import DatasetConfig, wire_config_problem

__all__ = [
    'ABORT_ALREADY_CLEAN', 'BadDatasetName', 'CONFIG_TOO_LARGE_ERROR', 'DMU_BACKUP_FEATURE_RAW',
    'DMU_BACKUP_MAGIC',
    'DMU_COMPOUNDSTREAM', 'DMU_OST_ZFS', 'DMU_OST_ZVOL', 'DMU_SUBSTREAM', 'DRR_BEGIN',
    'DRR_RECORD_SIZE', 'OwnerVerdict', 'RECEIVE_PROPERTIES_FILESYSTEM',
    'RECEIVE_PROPERTIES_VOLUME', 'ReceiveDecision', 'SERVER_OWNED_PROPERTIES', 'ServerSnapshot',
    'StreamBase', 'StreamHeader', 'StreamHeaderError', 'ZFS_USER_PROPERTY_MAXLEN',
    'abort_partial_receive', 'ancestor_holds_partial_receive', 'canonical_dataset_config',
    'client_dataset_problem', 'create_ancestors', 'decide_receive', 'enforce_receive_properties',
    'format_horizon',
    'local_client_id', 'may_force', 'owner_verdict', 'ownership_conflict', 'parse_stream_header',
    'receive_properties', 'resume_token', 'retention_horizon', 'retention_shrink',
    'server_copy_kind', 'server_dataset', 'server_snapshots', 'snapshots_newer_than',
    'start_receive', 'stream_base_problem',
]

#: Characters refused in a client dataset name before it reaches any `zfs`
#: argument or property value (`zfs list -H` frames rows with them).
_UNFRAMEABLE = frozenset('\n\r\t\0')

#: Substrings of `zfs receive -A`'s stderr that mean "nothing to abort": rc 1
#: "'<ds>' does not have any resumable receive state to abort"; rc 2 "cannot
#: open '<ds>': dataset does not exist". In this flow the server dataset
#: always exists first (negotiate or `_receive_stream` creates it), so `-A`
#: destroys only the saved `<ds>/%recv` state and rc 1 is the normal "already
#: clean". rc 2 only arises for a dataset that a FULL receive created
#: outside this flow (a legacy or manual one), which the abort itself
#: destroys.
ABORT_ALREADY_CLEAN = (
    'does not have any resumable receive state',
    'dataset does not exist',
)

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


class BadDatasetName(ValueError):
    """The server dataset derived from a request is not a valid filesystem
    name (`server_dataset`); the API answers 400."""


def client_dataset_problem(dataset) -> Optional[str]:
    """Why the client dataset name in a `/backup/<id>/<dataset>/...` URL is
    unusable, or `None`. It becomes `org.zfsbackup:source_dataset` verbatim,
    so a character that `zfs list -H` uses as a row or column separator is
    refused before any `zfs` call, and the whole name must be a valid
    filesystem name on its own (the derived server name being valid does
    not prove it: its pool component has stricter rules than one below
    `<target>/<id>/`, e.g. `1tank`)."""
    if not isinstance(dataset, str) or not dataset:
        return "empty client dataset name"
    if any(c in _UNFRAMEABLE for c in dataset):
        return "client dataset name contains a control character"
    try:
        Filesystem(dataset)
    except Exception:
        return "invalid client dataset name"
    return None


def server_dataset(target: str, client_id: str, client_dataset: str) -> str:
    """Derive the server-side dataset path from the client's dataset
    name: `<target>/<client_id>/<client dataset>`, pool included, so
    datasets of the same name in two of the client's pools never meet.
    `client_id` is always the BOUND id. Raises `BadDatasetName` (the routes
    answer 400) when the client dataset name (`client_dataset_problem`) or
    the result is not a valid filesystem name, so no `zfs` command ever
    sees it. The client name is checked first: its refusal names a control
    character without repeating it raw (`Filesystem`'s own message
    would)."""
    problem = client_dataset_problem(client_dataset)
    if problem is not None:
        # `!r`: the name may hold the very control character refused.
        raise BadDatasetName(f"invalid dataset name {client_dataset!r}: {problem}")
    name = f"{target}/{client_id}/{client_dataset}"
    try:
        Filesystem(name)
    except Exception as e:
        raise BadDatasetName(f"invalid dataset name {name!r}: {e}") from None
    return name


def server_copy_kind(zfs_call: Callable, server_fs, *, lib=None) -> str:
    """`'raw'` or `'plain'` for a server dataset that holds snapshots (the
    caller answers `'none'` for one that does not): raw if it is encrypted
    (`encryption != off`) and is its own encryption root, which is what a
    raw receive of one dataset produces (`keystatus` is `unavailable`); a
    plain receive into an encrypted target inherits the root of the TARGET
    (or of its encrypted parent), and an unencrypted dataset has none.
    (A root elsewhere under `<target>/<id>/` only comes from a `-R` tree,
    which the client never sends; counting it would call a plain child of
    a raw parent raw.) Raises if zfs fails."""
    lib = zfs if lib is None else lib
    encryption = zfs_call(lib.get.value, server_fs, 'encryption')
    if encryption in (None, 'off'):
        return 'plain'
    return 'raw' if zfs_call(lib.get.value, server_fs, 'encryptionroot') == server_fs.name else 'plain'


def resume_token(zfs_call: Callable, server_dataset: str, *, lib=None) -> Optional[str]:
    """The `receive_resume_token` of the server dataset -- a filesystem
    or a volume, found by name alone -- or `None` (no dataset, or no
    token). Raises on any other failure (`ZFSTimeoutError` included):
    callers must not guess."""
    lib = zfs if lib is None else lib
    ds = Dataset(server_dataset)
    if not zfs_call(lib.exists, ds):
        return None
    results = zfs_call(lib.list, roots=ds, properties=['receive_resume_token'])
    if not results:
        return None
    prop = results[0]['receive_resume_token']
    if prop is None:
        return None
    val = str(prop)
    return val if val != 'none' else None


class OwnerVerdict(NamedTuple):
    """The outcome of an ownership check of an EXISTING server dataset.
    `message` is the refusal text (the API answers 409) or `None` when the
    dataset is this client's. The two flags are for the caller's log-once
    memory (this module logs nothing): `unowned` is True when the dataset
    has no LOCAL client id (the caller warns once, naming the adopting
    command), `confirmed` when its LOCAL client id is this client's (the
    caller forgets an earlier warning). They are never both True.
    `source_mismatch` is True when the refusal is that the dataset is this
    client's but records another client dataset as its source (the "refusing to
    mix in" case), so a caller words that refusal without parsing `message`."""

    message: Optional[str]
    unowned: bool = False
    confirmed: bool = False
    source_mismatch: bool = False


def owner_verdict(server_dataset: str, local_id: Optional[str], client_id: str) -> OwnerVerdict:
    """The verdict for an EXISTING server dataset whose LOCAL
    `org.zfsbackup:client_id` is `local_id` (`None`: not set locally). A
    dataset with no local id was not made by a negotiate or a receive of
    this server (an operator's own `<target>/<id>`, a dataset moved in, a
    received-only copy of the property): it is never adopted silently (plan
    B-2c, because the full `-F` receive would overwrite it and the pruner
    would take over its snapshots); the message names the one command that
    adopts it on purpose."""
    if local_id is None:
        return OwnerVerdict(
            f"server dataset {server_dataset} already exists on the server and is not "
            f"recorded as a backup ({PROP_CLIENT_ID} is not set on it); refusing to "
            "adopt it. If it is meant to hold this client's backups, the server "
            f"operator can adopt it on purpose: zfs set {PROP_CLIENT_ID}={client_id} "
            f"{shlex.quote(server_dataset)}",
            unowned=True,
        )
    if local_id != client_id:
        # The other id is not named: it is not this client's business.
        return OwnerVerdict(
            f"server dataset {server_dataset} is recorded as another "
            f"client's ({PROP_CLIENT_ID} differs from your bound client id "
            f"{client_id!r}); refusing to use it. The server operator must "
            "resolve this"
        )
    return OwnerVerdict(None, confirmed=True)


def local_client_id(zfs_call: Callable, ds, *, lib=None) -> Optional[str]:
    """The LOCAL `org.zfsbackup:client_id` of the existing dataset `ds`
    (`None`: not set locally). Raises if zfs fails."""
    lib = zfs if lib is None else lib
    return zfs_call(lib.get.value, ds, PROP_CLIENT_ID, sources='local')


def ownership_conflict(
    zfs_call: Callable, server_dataset: str, dataset: str, client_id: str,
    *, read_properties: Callable = read_zfsbackup_properties, lib=None,
) -> OwnerVerdict:
    """`OwnerVerdict` for `server_dataset` (the API answers 409 when its
    `message` is not `None`); a dataset that does not exist is not a
    conflict. The message is set when the dataset exists and is not this
    client dataset's:

    - it records a different client id (`org.zfsbackup:client_id`) than
      the bound `client_id` -- the server namespace is the binding's, and
      a dataset stamped for another id (an operator rename, a changed
      host id) is never written through;
    - it has NO local client id (plan B-2c): an existing dataset nothing
      here made is not adopted (`owner_verdict`). Every dataset a
      negotiate or a receive creates carries a local id from the start
      (`zfs create -o`, `zfs receive -o`, also while a first receive is
      only partial), so this only meets datasets that came from
      elsewhere;
    - it records a different client dataset as its source. Two client
      names never derive the same server name (`server_dataset` keeps
      the pool), so this meets a copy an operator renamed into place, or
      one made under another layout, which a send would otherwise
      silently share (and overwrite).

    A dataset without a source-dataset property (a container negotiate
    made, never negotiated itself) is not a conflict for that property.
    `read_properties` reads the `org.zfsbackup:*` properties (the API passes
    its own name for it). Raises if the dataset's properties cannot be read
    (`ZFSTimeoutError` included)."""
    lib = zfs if lib is None else lib
    ds = Dataset(server_dataset)
    if not zfs_call(lib.exists, ds):
        return OwnerVerdict(None)
    props = zfs_call(read_properties, ds)
    recorded_id = props.get(PROP_CLIENT_ID)
    if recorded_id and recorded_id != client_id:
        return owner_verdict(server_dataset, recorded_id, client_id)
    verdict = owner_verdict(
        server_dataset, zfs_call(lib.get.value, ds, PROP_CLIENT_ID, sources='local'), client_id
    )
    if verdict.message is not None:
        return verdict
    recorded = props.get(PROP_SOURCE_DATASET)
    if recorded and recorded != dataset:
        return verdict._replace(message=(
            f"server dataset {server_dataset} already holds backups of "
            f"{recorded!r}; refusing to mix in {dataset!r}"
        ), source_mismatch=True)
    return verdict


def ancestor_holds_partial_receive(zfs_call: Callable, ds, *, lib=None) -> bool:
    """True when the existing ancestor `ds` holds the partial state of a NEW
    first receive: a `receive_resume_token` and no snapshot yet. A child
    created under it would make the later `zfs receive -A` fail with "has
    children". An ancestor that already has snapshots (a cut incremental)
    does not block. One `zfs get`, plus one listing when there is a token.
    Raises if zfs fails."""
    lib = zfs if lib is None else lib
    return (
        zfs_call(lib.get.value, ds, 'receive_resume_token') is not None
        and not zfs_call(lib.list, roots=ds, types='snapshot', properties=['creation'])
    )


def create_ancestors(
    zfs_call: Callable, missing_ancestors, client_id: str, *, lib=None,
) -> None:
    """Create the missing ancestors, `missing_ancestors` being nearest-first
    (as `_ancestors_state` lists them): the OUTERMOST first, one level at a
    time, and never the dataset itself (no `-p`: only a name the caller
    checked as missing is ever created, so a dataset that appeared meanwhile
    is a failure, not adopted). Each one is stamped with the local
    `client_id` (B-2c) and never mounted (B-2a): `canmount=noauto` and
    `mount=False`.

    Filesystems only (no size, no positional argument: `zfs.create` never
    dispatches to `create -V`; a volume only arrives through a receive).
    Raises on the first failure; the ones already made stay (the caller's
    claims and pending entries are its own)."""
    lib = zfs if lib is None else lib
    for ancestor in reversed(missing_ancestors):
        zfs_call(
            lib.create,
            Filesystem(ancestor),
            properties={
                'canmount': 'noauto',
                PROP_CLIENT_ID: client_id,
            },
            mount=False,
        )


def abort_partial_receive(zfs_call: Callable, server_dataset: str, *, lib=None) -> None:
    """`zfs receive -A` on `server_dataset`, idempotent: a failure whose
    message says there was nothing to abort (`ABORT_ALREADY_CLEAN`) is fine,
    anything else raises, `ZFSTimeoutError` included (the outcome is then
    unknown). The caller looks up the token (`resume_token`) and logs before
    calling this, and decides whether the abort is still allowed."""
    lib = zfs if lib is None else lib
    try:
        zfs_call(lib.receive.abort, Dataset(server_dataset))
    except ZFSTimeoutError:
        raise
    except Exception as e:
        if not any(m in str(e).lower() for m in ABORT_ALREADY_CLEAN):
            raise


def receive_properties(kind: str) -> dict:
    """The forced `-o` set for the stream's object-set type (`StreamHeader.
    kind`): `RECEIVE_PROPERTIES_VOLUME` for `'volume'`, else
    `RECEIVE_PROPERTIES_FILESYSTEM`."""
    return RECEIVE_PROPERTIES_VOLUME if kind == 'volume' else RECEIVE_PROPERTIES_FILESYSTEM


def enforce_receive_properties(zfs_call: Callable, server_fs, header: 'StreamHeader', *,
                               lib=None) -> List[str]:
    """After a RAW receive that exited 0: `zfs set` every property of the
    forced set (`receive_properties(header.kind)`) that is not a LOCAL value of
    `server_fs`, to its forced value; -> the names set, sorted (normally empty:
    one value-free `zfs get -s local`, nothing written). A plain stream
    (`header.raw` False) returns `[]` without running anything.

    Needed because a `-o` of `zfs receive` can be skipped without failing the
    receive. OpenZFS 2.1 does it for `-o sharesmb=off` on a RAW stream: it
    prints "cannot receive sharesmb property on <ds>: pool and or dataset must
    be upgraded to set this property or value", exits 0, and the copy inherits
    its parent's `sharesmb` (2.1.5, a raw full receive under a parent with
    `sharesmb=on`: the copy reads `on  inherited from <parent>`, every other
    forced value is local). A plain receive applies it on 2.1 (also into an
    encrypted target), and 2.2+ apply it for raw streams too, so only a raw
    stream is checked. A `zfs set sharesmb=off` after the receive works (the
    key need not be loaded), and later incremental raw receives keep it. A
    LOCAL value is taken as the receive's own: every receive passes the whole
    set again. Raises if a `zfs` call fails (the snapshot is committed by then;
    the caller decides what that means)."""
    if not header.raw:
        return []
    lib = zfs if lib is None else lib
    forced = receive_properties(header.kind)
    name = str(server_fs)
    rows = zfs_call(lib.get.property_sources, server_fs, sources='local')
    local = {row.property for row in rows if row.name == name}
    missing = {prop: value for prop, value in forced.items() if prop not in local}
    if missing:
        zfs_call(lib.set, server_fs, missing)
    return sorted(missing)


def may_force(zfs_call: Callable, server_fs, allowed: bool, *, lib=None) -> bool:
    """Whether the receive may pass `-F`: `allowed` (the `decide_receive`
    decision) AND the dataset exists now (asked again: the abort may have
    destroyed a new dataset; `zfs` refuses `-F` together with a raw stream or
    an encrypted target onto an EXISTING dataset, and a full stream into a
    missing one needs none). `exists` is not asked when `allowed` is False.
    Raises if zfs fails; the receive itself gets no timeout."""
    lib = zfs if lib is None else lib
    return allowed and zfs_call(lib.exists, server_fs)


def start_receive(
    server_fs, owned: Mapping[str, str], kind: str, force: bool, *, lib=None,
):
    """Spawn `zfs receive -s -u -h` into `server_fs` and return its
    `StreamHandle` (stdin). A startup failure raises; it never returns
    `None`. `owned` are the server's `SERVER_OWNED_PROPERTIES` values and
    `kind` is the stream's object-set type; both go as `-o` (`{**owned,
    **receive_properties(kind)}`), so a copy of any of them inside the stream
    is never in effect, and there is no moment at which it is (plan B-1b,
    B-2a). `-h` (no holds from the stream: a client hold would pin server
    snapshots against the pruner) and never `-x`. `force` (`-F`) only comes
    from `may_force`. The outcome of `zfs receive` is its EXIT STATUS
    (`StreamHandle.close()`), never its stderr: OpenZFS 2.1 prints a
    "sharesmb ... must be upgraded" line on raw receives that succeed, and
    leaves that property unset, so the caller runs
    `enforce_receive_properties` after a receive that exited 0. Not wrapped
    in a `zfs_call`: the receive is never timed."""
    lib = zfs if lib is None else lib
    return lib.receive.filesystem(
        server_fs, props={**owned, **receive_properties(kind)},
        force=force, holds=False,
        save=True, mount=False,
    )


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
#: `DMU_GET_FEATUREFLAGS(drr_versioninfo)` = bits 2-31; `DMU_BACKUP_FEATURE_RAW`
#: is feature bit 24 (`zfs send -w`, and the resume of a raw send). Checked
#: against the M8 captures of 2.1.5/2.2.2/2.3.9/2.4.1: `0x5080011` (raw
#: filesystem), `0x5080001` (raw volume), `0x1080011`/`0x11`/`0x1` (plain); the resume
#: headers (`zfs send -t`) add the resume feature bits: `0x5480011` (raw
#: resume), `0x1480011`/`0x1480001` (plain resume).
DMU_BACKUP_FEATURE_RAW = 1 << 24


class StreamHeader(NamedTuple):
    """The fields of a stream's `DRR_BEGIN` record the server acts on."""
    kind: str            # 'filesystem' or 'volume' (the stream's object-set type)
    to_guid: int
    from_guid: int       # 0 for a full stream
    versioninfo: int
    to_name: str         # `pool/ds@snap` as the sender wrote it; for logging only
    big_endian: bool

    @property
    def raw(self) -> bool:
        """A raw stream (`DMU_BACKUP_FEATURE_RAW` in the feature flags)."""
        return bool((self.versioninfo >> 2) & DMU_BACKUP_FEATURE_RAW)


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


#: A ZFS user-property value must be SHORTER than this many bytes
#: (OpenZFS `ZAP_MAXVALUELEN`, 8 KiB, the terminating NUL included). The
#: canonical configuration is stored as such a value, in negotiate and again
#: by `zfs receive -o`; a longer one is refused at negotiate (400), or the
#: negotiate would succeed for a dataset that does not exist yet and every
#: stream fail at the receive.
ZFS_USER_PROPERTY_MAXLEN = 8192


#: 400 for a negotiate `config` whose canonical form does not fit a ZFS user
#: property value (`ZFS_USER_PROPERTY_MAXLEN`): the client's log then says why.
CONFIG_TOO_LARGE_ERROR = 'config: too large to store as a ZFS property (the limit is 8 KiB)'


def canonical_dataset_config(encoded, *, max_length: Optional[int] = None) -> str:
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
    canonical value must also fit a ZFS user property (`ZFS_USER_PROPERTY_MAXLEN`,
    or `max_length` when given)."""
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
    limit = ZFS_USER_PROPERTY_MAXLEN if max_length is None else max_length
    if len(canonical) >= limit:  # base64: ASCII, bytes == characters
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


class ServerSnapshot(NamedTuple):
    """A snapshot of a server dataset as `server_snapshots` lists it."""
    name: str                # short name
    txg: Optional[int]       # createtxg
    guid: Optional[int]


def server_snapshots(server_fs, *, strict: bool = True, lib=None) -> List[ServerSnapshot]:
    """The direct snapshots of `server_fs`, with `createtxg` and `guid` (None
    where zfs did not report one). Raises if zfs fails. `strict` (the default)
    also raises when libzfseasy would have skipped a row it cannot trust (a
    snapshot name it cannot parse, say): a decision about what a receive may
    destroy must not rest on a listing that silently lacks a snapshot.

    Runs one `zfs list` itself, so it takes no `zfs_call`: callers pass the
    function TO theirs (`zfs_call(server_snapshots, fs)`)."""
    lib = zfs if lib is None else lib
    return [
        ServerSnapshot(s.short, property_int(s, 'createtxg'), property_int(s, 'guid'))
        for s in lib.list(
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
    server_dataset: str, base: StreamBase, rows: List[ServerSnapshot], *,
    where: str = 'the server', sender: str = 'the client',
    rename_doc: str = "README 'Renaming a server copy aside'",
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
      `no_common` (zfs refuses a full stream over them anyway).

    `where` (the receiving host, `On <where>:`), `sender` (who allows the
    destroy) and `rename_doc` (the runbook section) only word the refusal
    messages; the defaults are the server's."""
    if any(r.txg is None for r in rows):
        raise ValueError("a server snapshot has no createtxg")
    if not rows:
        return ReceiveDecision(True)
    rename = (
        f"`zfs rename {server_dataset} <a name outside the backup target>` "
        f"(the next cycle sends a full stream; see {rename_doc})"
    )
    if base.guid is None and base.name is None:
        return ReceiveDecision(False, 'no_common', (
            f"{server_dataset} already holds snapshots and a full stream cannot replace "
            f"them, so nothing is received. On {where}: {rename}"
        ))
    if base.guid is not None:
        found = next((r for r in rows if r.guid == base.guid), None)
    else:
        found = next((r for r in rows if r.name == base.name), None)
    if found is None:
        what = f"guid {base.guid}" if base.guid is not None else repr(base.name)
        return ReceiveDecision(False, 'base_missing', (
            f"{server_dataset} does not hold the incremental source snapshot ({what}), "
            f"so nothing is received. This can be transient ({where}'s pruner may have "
            "removed it since negotiation; the next cycle negotiates again). If it "
            f"persists, on {where}: {rename}"
        ))
    newer = sorted((r for r in rows if r.txg > found.txg), key=lambda r: r.txg)
    if not newer:
        return ReceiveDecision(True)
    names = [r.name for r in newer]
    if all(r.guid is not None and r.guid in base.discard for r in newer):
        return ReceiveDecision(True, rolled_back=tuple(names))
    return ReceiveDecision(False, 'server_newer', (
        f"{server_dataset} holds {len(names)} snapshot(s) newer than {found.name} "
        f"({_rollback_range(names)}) that {sender} did not allow to be destroyed, "
        f"so nothing is received (receiving would destroy them). To keep them, on "
        f"{where}: {rename}. If they are expendable: "
        f"`zfs destroy {server_dataset}@{names[0]}"
        + (f"%{names[-1]}" if len(names) > 1 else "")
        + "` (the next cycle continues)"
    ))


def snapshots_newer_than(common: str, snaps: List[ServerSnapshot]) -> Optional[list]:
    """The snapshots in `snaps` newer (by `createtxg`) than the one named
    `common` (which must be among them), oldest first, as `{"name", "guid"}`
    (the guid a decimal string, `None` if unknown); `None` when they cannot be
    told because a snapshot has no `createtxg` (the caller says so and does
    not report them; the receive, which decides for itself, fails closed)."""
    if any(s.txg is None for s in snaps):
        return None
    base = next(s for s in snaps if s.name == common)
    return [
        {'name': s.name, 'guid': None if s.guid is None else str(s.guid)}
        for s in sorted((s for s in snaps if s.txg > base.txg), key=lambda s: s.txg)
    ]

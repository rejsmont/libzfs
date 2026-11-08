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
re-exports the names tests and callers know from there).
"""

import shlex
from typing import Callable, Mapping, NamedTuple, Optional

import libzfseasy as zfs
from libzfseasy import ZFSTimeoutError
from libzfseasy.types import Dataset, Filesystem

from zfsbackup.backup_manager import (
    PROP_CONFIG, PROP_CLIENT_ID, PROP_SOURCE_DATASET, read_zfsbackup_properties,
)

__all__ = [
    'ABORT_ALREADY_CLEAN', 'BadDatasetName', 'OwnerVerdict', 'RECEIVE_PROPERTIES_FILESYSTEM',
    'RECEIVE_PROPERTIES_VOLUME', 'SERVER_OWNED_PROPERTIES', 'abort_partial_receive',
    'ancestor_holds_partial_receive', 'client_dataset_problem', 'create_ancestors',
    'local_client_id', 'may_force', 'owner_verdict', 'ownership_conflict', 'receive_properties',
    'resume_token', 'server_copy_kind', 'server_dataset', 'start_receive',
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
    caller forgets an earlier warning). They are never both True."""

    message: Optional[str]
    unowned: bool = False
    confirmed: bool = False


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
        ))
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
    "sharesmb ... must be upgraded" line on raw receives that succeed.
    Not wrapped in a `zfs_call`: the receive is never timed."""
    lib = zfs if lib is None else lib
    return lib.receive.filesystem(
        server_fs, props={**owned, **receive_properties(kind)},
        force=force, holds=False,
        save=True, mount=False,
    )

"""Snapshot manager for creating and managing ZFS snapshots."""

from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, FrozenSet, Iterable, List, Mapping, NamedTuple, Optional, Set, Tuple, Union
import contextlib
import dataclasses
import logging
import re
import shlex
import sys
import os
import time
import types

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import libzfseasy as zfs
from libzfseasy.types import Dataset, Filesystem, Snapshot as ZFSSnapshot
from libzfseasy.zfs import ZFSOutputError, ZFSTimeoutError
from zfsbackup.config import (
    BackupConfig, DatasetConfig, _collapse_retention_rules, client_id_problem,
    wire_config_problem,
)


logger = logging.getLogger(__name__)

PROP_CONFIG = 'org.zfsbackup:config'
PROP_CLIENT_ID = 'org.zfsbackup:client_id'
PROP_SOURCE_DATASET = 'org.zfsbackup:source_dataset'
PROP_ANCHOR_PREFIX = 'org.zfsbackup:anchor.'
#: `inflight.<destination>` = `<from>,<to>` (snapshot SHORT names): the pair a
#: fresh `zfs send` to that destination is moving. Set before the send starts
#: and cleared once the new anchor is recorded, so the pruner never destroys
#: either end of a transfer that is still running or can still be resumed. The
#: comma separates the two names (it cannot appear in a ZFS snapshot name);
#: `<from>` is empty for a full send.
PROP_INFLIGHT_PREFIX = 'org.zfsbackup:inflight.'
PROP_PREFIX = 'org.zfsbackup:'

#: Columns of the one snapshot listing (`DatasetManager._list_snapshots_strict`).
#: `createtxg` orders snapshots (the name timestamp is only a label, and a
#: wrong clock or a hand-made name makes it disagree with creation order);
#: `guid` identifies a snapshot across hosts; `userrefs` (holds) and `clones`
#: say whether a destroy can succeed; `encryption` and `keystatus` pick the
#: send mode.
SNAPSHOT_LIST_PROPERTIES = ['createtxg', 'guid', 'userrefs', 'clones', 'encryption', 'keystatus']

#: A clock this far (or `check_interval`, if longer) behind the newest
#: snapshot's name is treated as wrong rather than as jitter.
CLOCK_TOLERANCE = timedelta(minutes=5)

#: The local discovery (`DatasetManager.local_copies`) runs each `zfs` command
#: under `libzfseasy.timeout(LOCAL_DISCOVER_COMMAND_BUDGET)` and one local
#: target's discovery under an overall `LOCAL_DISCOVER_BUDGET`, so a target on
#: an exported or suspended pool ends in an error instead of wedging the
#: pruning worker. The command budget equals `zfsbackup.local.LOCAL_ZFS_BUDGET`
#: (not imported: the pruning worker must not load `zfsbackup.local` lazily
#: mid-upgrade, S-8). The server's discovery (`received_datasets`) is untimed,
#: as it always was.
#:
#: The overall budget is checked when a scope is entered, not during a
#: command, and every command enters its own scope (the per-dataset fallback
#: reads its three properties in three), each up to
#: `min(LOCAL_DISCOVER_COMMAND_BUDGET, time left)`: one target's discovery
#: ends within the seconds a timed-out child needs to be terminated of
#: `LOCAL_DISCOVER_BUDGET`.
LOCAL_DISCOVER_COMMAND_BUDGET = 60.0
LOCAL_DISCOVER_BUDGET = 600.0

#: Pruning a LOCAL COPY (`DatasetInfo.local_target` set) runs each of its `zfs`
#: commands under `libzfseasy.timeout`: the reads (snapshot listing, protection
#: re-read) at `LOCAL_PRUNE_COMMAND_BUDGET`, a destroy at the longer
#: `LOCAL_PRUNE_DESTROY_BUDGET` (destroying many snapshots on a slow USB disk
#: legitimately takes a while). A target on a suspended pool (`failmode=wait`)
#: may still answer a listing while `zfs destroy` sits in D state; without a
#: budget that would wedge the whole pruning worker, and with it the pruning of
#: every SOURCE dataset and every reload. Source datasets are never timed.
LOCAL_PRUNE_COMMAND_BUDGET = 60.0
LOCAL_PRUNE_DESTROY_BUDGET = 300.0


class LocalDiscovery(NamedTuple):
    """How the discovery of one local destination's copies went
    (`DatasetManager.last_local_discovery`): `error` is `None` when it
    worked, `count` the copies found (0 on an error)."""
    destination: str
    target: str
    count: int
    error: Optional[str]


class _DiscoveryFailed(Exception):
    """A discovery failure that is not an exception from `zfs` (its message
    is the whole account): the target dataset does not exist."""


def _failure_kind(failure: BaseException) -> str:
    """The class of a failure of a local target, which the once-per-episode
    WARNING is keyed on (the message varies: pids, seconds): `missing` (the
    target does not exist), `stuck` (a `zfs` child that timed out earlier
    survived SIGKILL and is still running: every timed call is refused at
    once, whichever target it is for), `timeout`, or `other:<exception
    type>`."""
    if isinstance(failure, _DiscoveryFailed):
        return 'missing'
    if isinstance(failure, ZFSTimeoutError):
        return 'stuck' if zfs.stuck_children() else 'timeout'
    return f"other:{type(failure).__name__}"


def _describe_failure(failure: BaseException) -> str:
    if isinstance(failure, _DiscoveryFailed):
        return str(failure)
    text = f"{type(failure).__name__}: {failure}"
    if isinstance(failure, ZFSTimeoutError):
        stuck = zfs.stuck_children()
        if stuck:
            # Say what is hung: the stuck child may belong to another target
            # (or an earlier command on this one), and a healthy target must
            # not be reported as the hung one.
            child = stuck[0]
            text += (
                f" [zfs is not answering: `{child.label}` (pid {child.pid}) timed out "
                f"{max(0.0, time.monotonic() - child.since):.0f} s ago and is still running; "
                "while it is, this worker's timed zfs calls, for any local target, are refused at once]"
            )
    return text


def _no_scope():
    """The scope of an untimed discovery."""
    return contextlib.nullcontext()


def _discovery_scope(deadline: float) -> Callable[[], 'contextlib.AbstractContextManager']:
    """A `scope()` for `DatasetManager._discover_under`: each call returns a
    `libzfseasy.timeout` of `LOCAL_DISCOVER_COMMAND_BUDGET` (less, near the
    monotonic `deadline`) and raises `ZFSTimeoutError` once the deadline has
    passed."""
    def scope():
        left = deadline - time.monotonic()
        if left <= 0:
            raise ZFSTimeoutError(
                f"the {LOCAL_DISCOVER_BUDGET:g} s budget for discovering local copies is spent"
            )
        return zfs.timeout(min(LOCAL_DISCOVER_COMMAND_BUDGET, left))
    return scope


# ── Module-level property helpers (item 14.1) ─────────────────────────────────
#
# Used by the `zfsbackup-config` CLI's rename/edit commands, which have no
# `BackupConfig` and no `DatasetManager`. Unlike the `DatasetManager` methods
# they do NOT swallow ZFS errors: the CLI must be able to tell the operator
# that a property copy failed. All take `dry_run` and then touch nothing.


#: Sources whose `org.zfsbackup:*` values count as "set on this dataset" for
#: the property readers. An inherited value belongs to an ancestor, and acting
#: on it would be acting on the wrong dataset.
_OWN_SOURCES = ('local', 'received')


class _PropertiesChanged(ZFSOutputError):
    """The two reads of `_read_own_properties` disagree about which `org.zfsbackup:*` properties an object has:
    something was set or cleared between them (or a value forged a row). Retried once, then raised."""


def _own_zfsbackup_rows(rows) -> Dict[str, Dict[str, str]]:
    """`{dataset name: {org.zfsbackup:* property: value}}` from `GetRow`s,
    keeping the `local`/`received` rows only and using nothing but `value`
    and `source` (the `received` column is never read: it holds the sending
    host's raw bytes)."""
    result: Dict[str, Dict[str, str]] = {}
    for row in rows:
        if not row.property.startswith(PROP_PREFIX) or row.source not in _OWN_SOURCES:
            continue
        result.setdefault(row.name, {})[row.property] = row.value
    return result


def _read_own_properties_once(
    ds: Dataset, recursive: bool, also: Tuple[str, ...] = (),
    sources: Optional[Dict[str, Dict[str, str]]] = None,
    prefix: Union[str, Tuple[str, ...]] = PROP_PREFIX,
    scope: Optional[Callable[[], 'contextlib.AbstractContextManager']] = None,
) -> Dict[str, Dict[str, str]]:
    """One attempt of `_read_own_properties` (two execs, each in its own
    `scope()`, a context manager such as a `libzfseasy.timeout`; none by
    default, and `scope` may raise to refuse an exec). `also` (own read only)
    names properties that are asked for in phase 2 whether or not phase 1 saw
    them. `sources`, when given, is filled with `{object: {property: source}}`
    (`local` or `received`) of the rows that were read. `prefix` narrows phase
    1 to properties whose name starts with it (a string, or a tuple of them),
    which must lie within `org.zfsbackup:`."""
    below = ds.name + '/'
    if recursive:
        def in_scope(name: str) -> bool:
            return name.startswith(below) and '@' not in name and '#' not in name
    else:
        def in_scope(name: str) -> bool:
            return name == ds.name

    # Phase 1: which org.zfsbackup:* properties exist, and where they come
    # from. No value column, so nothing an owner writes can disturb it.
    scope = scope or _no_scope
    with scope():
        found = [
            r for r in zfs.get.property_sources(
                ds, types='filesystem,volume', sources=list(_OWN_SOURCES),
                recursive=recursive, prefix=prefix if isinstance(prefix, str) else list(prefix),
            )
            if in_scope(r.name)
        ]
    expected = {(r.name, r.property) for r in found}
    if recursive and not expected:
        # Nothing below `ds` carries a property of ours: that is the answer,
        # and phase 1 is strict (it raised on any bad row, and zfs failed on
        # a missing `ds`, so `ds` exists; the caller's own read of `ds` is in
        # the same try).
        return {}

    # Phase 2: read exactly those property names, strictly. An object that
    # lacks one of them prints `-` (explicit lists are exact), so a forged or
    # missing row cannot go unnoticed. With no names the read asks for `type`
    # alone, which is what proves the dataset itself was read.
    names = list(dict.fromkeys([r.property for r in found] + list(also))) or ['type']
    with scope():
        rows = zfs.get.rows(
            ds, names, types='filesystem,volume', recursive=recursive,
            received=False, strict=True,
            # Phase 1 (unforgeable, names only) is the completeness oracle: every
            # object that has one of our properties is in it and must come back
            # below with the same ones, so a name-only `zfs list` adds nothing.
            verify_names=False,
        )
    if not recursive and not any(r.name == ds.name and r.property == 'type' for r in rows):
        raise ZFSOutputError(f"zfs get printed no usable rows for {ds.name!r}")
    got = _own_zfsbackup_rows(r for r in rows if in_scope(r.name))
    if sources is not None:
        for r in rows:
            if in_scope(r.name) and r.property.startswith(PROP_PREFIX) and r.source in _OWN_SOURCES:
                sources.setdefault(r.name, {})[r.property] = r.source
    seen = {(n, p) for n, props in got.items() for p in props}
    # A property of `also` that is here now but was not in phase 1 was set
    # between the two execs: it can only grow the protect-set, so it is kept.
    # (Nothing else can be added: a forged row cannot join a non-recursive
    # group, and any other new name was never asked for.)
    allowed = {(n, p) for (n, p) in seen - expected if p in also}
    if seen - allowed != expected:
        gone = sorted(expected - seen)
        new = sorted(seen - expected - allowed)
        detail = (f"{gone[0][1]} on {gone[0][0]} is no longer set" if gone
                  else f"{new[0][1]} on {new[0][0]} was not there before")
        raise _PropertiesChanged(
            f"the org.zfsbackup properties of {ds.name!r} changed while they were being read ({detail})"
        )
    return got


def _read_own_properties(
    ds: Dataset, recursive: bool, also: Tuple[str, ...] = (),
    sources: Optional[Dict[str, Dict[str, str]]] = None,
    prefix: Union[str, Tuple[str, ...]] = PROP_PREFIX,
    scope: Optional[Callable[[], 'contextlib.AbstractContextManager']] = None,
) -> Dict[str, Dict[str, str]]:
    """`{object name: {org.zfsbackup:* property: value}}` for `ds` (or, with
    `recursive`, its descendants), local/received only. The two-phase read
    behind `read_zfsbackup_properties` and `read_descendant_zfsbackup_properties`.

    A property set or cleared between the two execs makes them disagree; the
    pair is then read once more, and a second disagreement raises
    `ZFSOutputError`. Never a smaller set: a vanished property could only be
    trusted together with a new one the explicit list would not have asked for
    (an `inflight.*` cleared and an `anchor.*` set between the execs would
    otherwise read as "nothing pinned"). `also` (non-recursive only) closes the
    remaining hole: names the caller knows it cares about are always asked for,
    so one created after phase 1 is still seen, and such an addition is accepted.

    `scope()` (the health check passes one) is entered once per exec, the
    retry's included, so a time limit applies to each command separately."""
    if also and recursive:
        raise ValueError("`also` is for the own read only")
    also = tuple(dict.fromkeys(also))
    try:
        return _read_own_properties_once(ds, recursive, also, sources, prefix, scope)
    except _PropertiesChanged as e:
        logger.debug(f"{e}; reading again")
    if sources is not None:
        sources.clear()
    return _read_own_properties_once(ds, recursive, also, sources, prefix, scope)


def read_zfsbackup_properties(ds: Dataset, also: Iterable[str] = ()) -> Dict[str, str]:
    """Return every `org.zfsbackup:*` user property set on `ds`, name -> value.

    Properties are kept with their `local` and `received` sources only (an
    inherited value belongs to an ancestor, and acting on it would be acting
    on the wrong dataset).

    Two execs, so only `org.zfsbackup:*` values are ever read as values:
    `zfs.get.property_sources` lists which of our properties `ds` has
    (`-o name,property,source`, no value column, nothing forgeable), then a
    strict explicit-list `zfs.get.rows` (`received=False`) reads exactly
    those. A newline in an unrelated property, or in any value `ds` merely
    inherits, can no longer matter. A newline in one of OUR values (a tab is
    recovered unambiguously: no `received` column), a duplicated or forged
    row, a group that is not exactly the requested rows, or a property that
    disappeared (or appeared, unless named in `also`) between the two execs
    (after one re-read) makes the call RAISE (`ZFSOutputError`) instead of
    returning a smaller set.

    `also` names properties to ask for in the second exec even if the first
    did not list them (the pruner passes `anchor.<d>` and `inflight.<d>` of
    every configured destination): one created between the two execs is then
    returned, not missed. The cost is that a newline in a value of such a name
    that `ds` merely INHERITS now breaks the read as well. With no `org.zfsbackup:*` property the
    second exec asks for `type` alone, which proves `ds` itself was read: an
    empty answer is never a silent "no anchors". Callers treat a raise as
    "unknown, do not act" (the pruner skips the dataset, the API answers
    500, the CLI reports the failure). Also raises whatever `zfs get` raises.
    """
    return _read_own_properties(ds, False, tuple(also)).get(ds.name, {})


def read_descendant_zfsbackup_properties(
    ds: Dataset,
    sources: Optional[Dict[str, Dict[str, str]]] = None,
    prefix: Union[str, Tuple[str, ...]] = PROP_PREFIX,
    *,
    scope: Optional[Callable[[], 'contextlib.AbstractContextManager']] = None,
) -> Dict[str, Dict[str, str]]:
    """`read_zfsbackup_properties` for every DESCENDANT of `ds` (not `ds`
    itself), recursively: `{descendant name: {property: value}}`, only
    descendants that have at least one local/received `org.zfsbackup:*`
    property appear. Snapshots and bookmarks are excluded (`-t
    filesystem,volume`). Same two-phase read (two execs; ONE when no
    descendant has a property of ours). Phase 1 is the completeness check
    that replaces the name-only `zfs list` of `verify_names`: every object it
    names must come back from phase 2 with the same properties. Raises
    whatever `zfs get` raises, and `ZFSOutputError` if any group in the
    subtree was dropped or any row skipped, or the two phases disagree: the
    pruner must not prune on an unknown protect-set, so the ancestor is
    skipped this cycle. (A bad value INHERITED by `ds` itself from above can
    still break `ds`'s own group in phase 2, but only when a descendant sets
    that same property name locally.)

    `sources`, when given, is filled with `{descendant: {property: source}}`
    (`local` or `received`) of the values returned, for a caller that counts
    only LOCAL ones (the health check). `prefix` (a string or a tuple of
    them) narrows the read to properties whose name starts with it, so an
    unrelated `org.zfsbackup:*` value (a newline in a `config`) cannot break
    it; the default is every `org.zfsbackup:*` property.

    `scope()` (the health check passes one) returns a context manager that
    every exec runs in (a `libzfseasy.timeout`); it is entered once per exec
    (two, or four after a retry), and it may raise to refuse one."""
    return _read_own_properties(ds, True, sources=sources, prefix=prefix, scope=scope)


def copy_anchor_property(
    ds: Dataset,
    old_dest: str,
    new_dest: str,
    *,
    dry_run: bool = False,
    overwrite: bool = False,
) -> str:
    """Set `anchor.<new_dest>` from `anchor.<old_dest>` on `ds`. Never deletes
    anything (the old property is left for the caller to clear once the rename
    has actually landed).

    Both properties are read with `zfs.get.value(..., sources='local')`: only
    a LOCAL value is a source or a blocker (a received or inherited one is
    neither, and is never copied into a local one). A caller that wants to
    show the two values (e.g. for a dry run) reads them beforehand; this
    function returns only the outcome. A failed read raises.

    Returns exactly one of:

    * `'no-source'`   -- no `anchor.<old_dest>` on `ds`; nothing is touched
                         (checked first, whatever `overwrite` says).
    * `'copied'`      -- `anchor.<new_dest>` was absent and has been set.
    * `'dry-run'`     -- `dry_run` was true and a `zfs set` would have been
                         issued (`anchor.<new_dest>` absent, or present with a
                         different value and `overwrite=True`); nothing was set.
    * `'overwritten'` -- `overwrite=True`, `anchor.<new_dest>` existed with a
                         value different from `anchor.<old_dest>`'s, and has
                         been set to the latter.
    * `'same'`        -- `overwrite=True` and `anchor.<new_dest>` already
                         equals `anchor.<old_dest>`; nothing is set.
    * `'exists'`      -- `overwrite=False` and `anchor.<new_dest>` is already
                         set (whatever its value); left alone.

    `dry_run` never issues a `zfs set` and never changes the choice between
    outcomes except `'copied'`/`'overwritten'` -> `'dry-run'`. Overwriting may
    move `anchor.<new_dest>` BACKWARDS if a newer daemon already advanced it;
    that is the caller's explicit choice. Note that an overwrite UNPINS the
    snapshot the old `anchor.<new_dest>` value named: the pruner protects the
    snapshots named by current `anchor.*` properties
    (`DatasetManager.get_anchors`), not their previous values. Raises whatever
    `zfs get`/`zfs set` raises.
    """
    old_prop = f"{PROP_ANCHOR_PREFIX}{old_dest}"
    new_prop = f"{PROP_ANCHOR_PREFIX}{new_dest}"
    # Exact LOCAL reads (`zfs get -H -o value -s local`): a received or
    # inherited anchor is not this host's evidence (A1), so a rename must never
    # turn one into a local value. An unset or non-local property reads as
    # None; a failed read raises (never read as "no source").
    value = zfs.get.value(ds, old_prop, sources='local')
    if value is None:
        return 'no-source'
    existing = zfs.get.value(ds, new_prop, sources='local')
    exists = existing is not None
    if exists:
        if not overwrite:
            return 'exists'
        if existing == value:
            return 'same'
    if dry_run:
        if exists:
            logger.info(
                f"[DRY RUN] Would overwrite {new_prop}={existing} "
                f"with {value} on {ds.name}"
            )
        else:
            logger.info(f"[DRY RUN] Would set {new_prop}={value} on {ds.name}")
        return 'dry-run'
    zfs.set(ds, {new_prop: value})
    if exists:
        logger.debug(f"Overwrote {new_prop} from {old_prop} on {ds.name}")
        return 'overwritten'
    logger.debug(f"Copied {old_prop} -> {new_prop} on {ds.name}")
    return 'copied'


def _inflight_names(value: Optional[str]) -> List[str]:
    """The snapshot short names an `inflight.*` value (`<from>,<to>`) pins:
    both, empty parts (a full send has no `<from>`) and `-` dropped."""
    if not value or value == '-':
        return []
    return [n for n in (part.strip() for part in value.split(',')) if n and n != '-']


def _parse_inflight(value: Optional[str]) -> Optional[Tuple[str, str]]:
    """The `(from, to)` pair of an `inflight.*` value (`<from>,<to>`; `from`
    is `''` for a full send), or `None` when unset, `-` or malformed. Unlike
    `_inflight_names` it keeps which end is which."""
    if not value or value == '-' or ',' not in value:
        return None
    src, _, dst = value.partition(',')
    return (src, dst) if dst else None


def clear_zfsbackup_properties(
    ds: Dataset,
    *,
    config: bool,
    anchors: Union[Iterable[str], bool],
    dry_run: bool = False,
) -> List[str]:
    """`zfs inherit` the named `org.zfsbackup:*` properties from `ds`.

    `config=True` clears `org.zfsbackup:config`. `anchors` is an iterable of
    destination names (clears `anchor.<name>` for each), or `True` for every
    `org.zfsbackup:anchor.*` AND `org.zfsbackup:inflight.*` currently set on
    `ds` (the in-flight pins go with the anchors they stand in for), or
    `False`/empty for none. Only properties that are actually set are touched,
    so a clear is idempotent. Raises whatever `zfs` raises.

    `anchor.*`/`inflight.*` go through `zfs inherit -S`: it reverts a LOCAL
    value to the RECEIVED one when there is one (otherwise it acts as a plain
    inherit), so it never HIDES a received anchor, which the pruner still
    protects. A name that has only a RECEIVED value is therefore left alone:
    no command is run for it and it is NOT in the result (it is still set, so
    it is not "cleared"). The sources come from the same read as the names
    (`local` wins over `received` when a property has both, and `-S` then
    leaves the received value in effect: the LOCAL value is what was cleared).
    `org.zfsbackup:config` keeps a plain inherit, which acts on a local or a
    received value alike (a received one is masked), so it is returned
    whenever it was set.

    Returns the property names that were cleared: `config` if it was set, and
    the `anchor.*`/`inflight.*` names that had a LOCAL value -- or, under
    `dry_run`, that would have been.

    A non-empty bare `str` for `anchors` is rejected with `TypeError`: iterating it
    would treat each character as a destination name and silently clear
    nothing. Pass `[name]` for one destination (an empty string is falsy,
    like `False`, and clears nothing).
    """
    if anchors and isinstance(anchors, (str, bytes)):
        raise TypeError(
            "anchors must be True, False or an iterable of destination names, "
            f"not a bare string {anchors!r}; pass [name]"
        )
    sources: Dict[str, Dict[str, str]] = {}
    present = _read_own_properties(ds, False, (), sources).get(ds.name, {})
    source_of = sources.get(ds.name, {})
    wanted: List[str] = []
    if config and PROP_CONFIG in present:
        wanted.append(PROP_CONFIG)
    if anchors is True:
        wanted.extend(sorted(
            n for n in present
            if n.startswith(PROP_ANCHOR_PREFIX) or n.startswith(PROP_INFLIGHT_PREFIX)
        ))
    elif anchors:
        for dest in anchors:
            name = f"{PROP_ANCHOR_PREFIX}{dest}"
            if name in present and name not in wanted:
                wanted.append(name)
    # `-S` leaves a value that is only received where it is: not cleared.
    wanted = [n for n in wanted if n == PROP_CONFIG or source_of.get(n) == 'local']
    for name in wanted:
        if dry_run:
            logger.info(f"[DRY RUN] Would inherit {name} on {ds.name}")
        else:
            zfs.inherit(ds, name, received=(name != PROP_CONFIG))
            logger.debug(f"Cleared {name} on {ds.name}")
    return wanted


class LocalAnchorUnreadable(Exception):
    """`DatasetManager.get_local_anchor` or `get_local_inflight` (the anchor
    or the send pin) could not read the property because zfs failed (not
    because it is unset). The text is zfs's own message."""


@dataclasses.dataclass(frozen=True)
class PruneResult:
    """What one `DatasetManager.prune_snapshots` pass did, for the worker's
    status file (S-7): `pruned` snapshots destroyed, `failed` single destroys
    that did not go through, `stuck` candidates currently held back (a user
    hold, a clone, or a destroy that keeps failing; not a failure by itself),
    and `skipped` -- why the dataset was not pruned at all this pass (the
    snapshots or the protect-set could not be read), `None` otherwise."""

    pruned: int = 0
    failed: int = 0
    stuck: int = 0
    skipped: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.skipped is None and self.failed == 0

    def summary(self) -> str:
        text = f"pruned {self.pruned}"
        if self.stuck:
            text += f", {self.stuck} held back"
        return text


class DatasetInfo:
    """Information about a dataset."""

    def __init__(self, dataset: Dataset, config: DatasetConfig):
        self.dataset = dataset
        self.name = dataset.name
        self.config = config
        self.snapshots: List[SnapshotInfo] = []
        # Short names of every managed snapshot in the last listing,
        # including ones dropped from `snapshots` for a missing `createtxg`:
        # a generated name that is already taken must not be created again.
        self.listed_names: Set[str] = set()
        #: The local destination's target dataset (`zfs:<target>`) when this is
        #: a copy found by `DatasetManager.local_copies`, else `None`. Its
        #: pruning runs under timeouts (`LOCAL_PRUNE_*_BUDGET`).
        self.local_target: Optional[str] = None
        #: The enabled recursive configured dataset this one was expanded from
        #: (`DatasetManager.expand_recursive`), else `None`. An expanded child
        #: is a SENDING pair only: it is never snapshotted or pruned by its own
        #: entry (the parent's `_snapshot_tree`, which keeps backup copies out,
        #: and its recursive prune, which spares names a copy holds, cover it).
        self.expanded_from: Optional[str] = None

    @property
    def reference_time(self) -> datetime:
        return self.get_reference_time()

    @property
    def frequency(self) -> timedelta:
        return self.config.frequency

    @property
    def recursive(self) -> bool:
        return self.config.recursive

    def get_reference_time(self, now: Optional[datetime] = None) -> datetime:
        """Return the aligned reference time for the given snapshot frequency."""
        if self.config.frequency <= timedelta(0):
            raise ValueError("Snapshot frequency must be positive")
        interval = self.config.frequency.total_seconds()
        if now is None:
            now = datetime.now(timezone.utc)
        timestamp = int(now.timestamp())
        aligned_timestamp = (timestamp // int(interval)) * int(interval)
        return datetime.fromtimestamp(aligned_timestamp, tz=timezone.utc)

    def __repr__(self):
        return f"DatasetInfo({self.name})"


#: `Expansion.skipped`'s default: an empty mapping nobody can mutate (a
#: NamedTuple default is shared by every instance).
_NO_SKIPPED: Mapping[str, List[Tuple[str, str]]] = types.MappingProxyType({})


class Expansion(NamedTuple):
    """What `DatasetManager.expand_recursive` found: the `children` (one
    `DatasetInfo` per descendant to send, parents' order, each with
    `expanded_from` set), `failed`, which maps a recursive parent whose
    discovery failed this time to the error (its children are unknown, not
    gone), and `skipped`: for each recursive parent, the `(name, why)` of every
    backup copy or receive target found at or below it that was left out
    (`name` is the topmost one of each subtree, a copy root or a receive
    target, or the parent itself when it sits inside a copy or a target; `why`
    is a short phrase), in listing order, one entry per name. Additive and
    read-only (the daemon logs them once per episode; `check` reports them); a
    parent with none has no key, and a parent whose discovery failed has none
    either. The default is an empty read-only mapping."""
    children: List['DatasetInfo']
    failed: Dict[str, str]
    skipped: Mapping[str, List[Tuple[str, str]]] = _NO_SKIPPED


#: The start of the error `zfs.list(strict=True)` raises for output it cannot trust.
_UNTRUSTED_LISTING = 'zfs list output could not be trusted'
#: What that refusal says when the first row it could not trust was a name
#: libzfseasy cannot represent (`_SkippedRows.unrepresentable`).
_FIRST_UNTRUSTED_IS_A_NAME = ', the first: libzfseasy cannot represent the name'


class _CopyTops(Dict[str, str]):
    """The topmost backup copies `DatasetManager._copy_tops` found below (or at)
    a recursive dataset, `{dataset: why}`, plus two attributes (absent from a
    plain dict, so read them with `getattr`): `inside` is `(origin, source)`
    when the dataset ITSELF carries or inherits a copy marker (`source` is
    what zfs printed: `local`, `received`, `inherited from <dataset>`;
    `origin` the dataset the marker comes from, the dataset itself for a local
    or received one), and `covered` the tops that exist only because of that:
    every child inherits the marker, so each one is a top, and one message
    about the dataset replaces one per child."""

    inside: Optional[Tuple[str, str]] = None
    covered: FrozenSet[str] = frozenset()


class SnapshotRide(NamedTuple):
    """`DatasetManager.ridden_snapshot`: the snapshot `snapshot` of a
    configured dataset was created, in this cycle, by the snapshot of its
    enabled recursive configured ancestor `ancestor`, and counts as its own."""
    snapshot: str
    ancestor: str


class _InsideReceiveTarget(RuntimeError):
    """A recursive dataset to snapshot is itself at or below a receive target
    (`DatasetManager._snapshot_tree`). `first` is False after the first one of
    an episode: the caller logs the repeats quietly."""

    def __init__(self, message: str, first: bool):
        super().__init__(message)
        self.first = first


class DatasetManager:
    """Manages datasets based on configuration."""

    def __init__(self, config: BackupConfig):
        self.config = config
        self._datasets = None
        # (dataset name, ruleset) pairs whose retention-rule collisions have
        # already been warned about, so `needs_prunning` (called every pruning
        # cycle) warns once per distinct ruleset for this manager's lifetime
        # rather than every cycle. Keyed on content, not name alone: received
        # datasets are re-parsed fresh from PROP_CONFIG every pruning cycle
        # (`PruningWorker._get_datasets` -> `received_datasets()`), so a
        # client that re-negotiates and pushes a *different* colliding
        # ruleset under the same dataset name must still warn.
        self._retention_warned: Set[Tuple[str, FrozenSet[Tuple[float, float]]]] = set()
        # (dataset name, property) pairs of anchors belonging to no configured
        # destination that were already announced at INFO (once each; the
        # per-cycle repeat is DEBUG).
        self._orphan_anchor_noted: Set[Tuple[str, str]] = set()
        # (dataset name, snapshot name) pairs kept out of a recursive destroy
        # because a descendant protects them (announced once at INFO).
        self._descendant_protect_noted: Set[Tuple[str, str]] = set()
        # Datasets whose descendant evaluation failed and was announced at
        # ERROR (once while it persists; the per-cycle repeat is DEBUG).
        self._descendant_error_noted: Set[str] = set()
        # (recursive dataset, topmost backup copy below it) pairs whose ERROR
        # was logged (`_copy_protection`); a pair that is not found again logs
        # an INFO and leaves the set.
        self._copy_protect_noted: Set[Tuple[str, str]] = set()
        # The SNAPSHOT worker's own episode memory (`create_snapshot`): the
        # snapshot and the pruning worker are different processes, each with
        # its own manager, and even in one manager neither set feeds the
        # other's decision (they only gate log lines). (recursive dataset,
        # topmost copy below it) pairs whose "not snapshotted" ERROR was
        # logged, and the datasets whose copy read or atomic snapshot fell
        # back to a plain `snapshot -r` (ERROR once while it persists).
        self._snapshot_copy_noted: Set[Tuple[str, str]] = set()
        self._snapshot_fallback_noted: Set[str] = set()
        # (recursive dataset, origin) pairs, per worker like the sets above,
        # whose "this dataset itself sits inside a backup copy" ERROR was logged
        # (a copy marker it has or inherits: `_CopyTops.inside`), and the
        # datasets whose "itself at or below a receive target" refusal was
        # (`_snapshot_tree`). Each is announced once per episode and ends with
        # an INFO.
        self._snapshot_inside_copy_noted: Set[Tuple[str, str]] = set()
        self._snapshot_inside_target_noted: Set[str] = set()
        self._copy_protect_inside_noted: Set[Tuple[str, str]] = set()
        #: Datasets whose protect-set read is failing, so the ERROR is logged
        #: once while it persists (re-armed by the next successful read).
        self._anchor_error_noted: Set[str] = set()
        # Received datasets skipped by `received_datasets()`: name -> the
        # problem last announced at WARNING. Warned once while the same
        # problem persists (the per-cycle repeat is DEBUG); forgotten once the
        # dataset is accepted again or disappears, so a later problem warns.
        self._received_skip_noted: Dict[str, str] = {}
        # (dataset name, snapshot name) pairs dropped from the managed list
        # for having no `createtxg`, announced at WARNING once each.
        self._no_txg_noted: Set[Tuple[str, str]] = set()
        # (dataset name, what, snapshot name) of future-dated snapshots/anchors
        # announced at ERROR once each (`clock_behind`).
        self._clock_noted: Set[Tuple[str, str, str]] = set()
        # (dataset name, snapshot name) of generated names that already
        # exist (a clock that went backwards), announced at WARNING once.
        self._name_taken_noted: Set[Tuple[str, str]] = set()
        # A2e: what the enabled RECURSIVE datasets' own `create_snapshot` created
        # in the snapshot worker's current cycle (`begin_snapshot_cycle` clears
        # it): dataset name -> (snapshot name, the names of the datasets the
        # command covered, or `None` for every descendant: `snapshot -r`). A
        # configured descendant whose own name for this period is that snapshot
        # counts it as its own (`ridden_snapshot`).
        self._tree_snapshots: Dict[str, Tuple[str, Optional[FrozenSet[str]]]] = {}
        # (recursive dataset, snapshot name) whose retry without the holders
        # of the name was announced (`_retry_without_holders`).
        self._snapshot_holders_noted: Set[Tuple[str, str]] = set()
        # What `_snapshot_tree` covered, handed to `create_snapshot`.
        self._tree_cover: Optional[FrozenSet[str]] = None
        # (dataset name, snapshot name) -> why a destroy of it cannot go
        # through right now (held, cloned, or a failed single destroy). Such a
        # name is never put back into a batch; it is tried alone each cycle
        # until it goes or stops being a candidate.
        self._prune_stuck: Dict[Tuple[str, str], str] = {}
        #: Why the last `list_snapshots` / `create_snapshot` / protect-set
        #: re-read failed (`None`: it did not). The failure is also logged;
        #: these let a worker record it without a changed return type (S-7).
        self.last_list_error: Optional[str] = None
        # The exception behind `last_list_error` (`prune_snapshots` re-raises a
        # timeout of a local copy's listing).
        self._last_list_failure: Optional[BaseException] = None
        self.last_snapshot_error: Optional[str] = None
        #: A short standing warning from the last `create_snapshot` that DID
        #: create the snapshot: backup copies below a recursive dataset were
        #: left out of it, or could not be told apart (`None`: nothing to say).
        #: The snapshot worker puts it in the dataset's status record.
        self.last_snapshot_note: Optional[str] = None
        #: Why the last `received_datasets()` could not enumerate the target
        #: (`None`: it could, or remote backup is off).
        self.last_received_error: Optional[str] = None
        #: How the last `local_copies()` went, one entry per local
        #: destination target it examined (see `LocalDiscovery`).
        self.last_local_discovery: List[LocalDiscovery] = []
        # Per local target: the skip memory of `_discover_under`, and the
        # discovery error already announced at WARNING (once per episode).
        self._local_skip_noted: Dict[str, Dict[str, str]] = {}
        self._local_error_noted: Dict[str, str] = {}
        #: Why the last `local_copies()` discovered nothing for a reason that
        #: concerns every local destination (no client id, an invalid one), else
        #: `None`. The worker then keeps the status records under all of them.
        self.last_local_skip: Optional[str] = None
        # The invalid client ids already warned about (once per incarnation).
        self._bad_client_id_noted: Set[str] = set()
        # Local targets whose pruning timed out THIS cycle (target -> why; reset
        # by every `local_copies()`), so the rest of their copies are skipped
        # without a `zfs` call; and the failure class already announced at
        # WARNING per target (once per episode).
        self._local_prune_blocked: Dict[str, str] = {}
        self._local_prune_noted: Dict[str, str] = {}
        # True while `prune_snapshots` runs a local copy under timeouts.
        self._prune_timed = False
        self._reread_error: Optional[str] = None
        # (dataset, snapshot name or None for the read failure) already logged
        # at the normal level in the current pruning pass (`_pass_logged_once`).
        self._reread_logged: Set[Tuple[str, Optional[str]]] = set()
        # `expand_recursive`: the parents whose discovery is failing (ERROR
        # once per episode, INFO when it works again) and the (parent, target)
        # pairs whose descendants under a receive target were announced.
        self._expand_error_noted: Set[str] = set()
        self._expand_target_noted: Set[Tuple[str, str]] = set()
        # ... and the (parent, copy root) pairs skipped for a local
        # `client_id`/`source_dataset`.
        self._expand_copy_noted: Set[Tuple[str, str]] = set()

    @property
    def datasets(self) -> List[DatasetInfo]:
        if self._datasets is None:
            self._datasets = self._get_datasets()
        return self._datasets

    @property
    def prefix(self) -> str:
        return self.config.snapshot_prefix

    def _get_datasets(self) -> List[DatasetInfo]:
        datasets = []
        for ds_config in self.config.enabled_datasets:
            dataset = Dataset(ds_config.name)
            datasets.append(DatasetInfo(dataset, ds_config))
        return datasets

    def _receive_targets(self) -> List[str]:
        """The datasets that receive copies: the server target and every
        declared local (`zfs:`) destination's target, a trailing `/` stripped.
        Nothing at or below one of them is a source. The server target counts
        whether or not `remote_backup` is enabled: a former server still holds
        other clients' received copies there, and sending them under this
        host's id is the one thing this must never allow."""
        targets: List[str] = []
        rb = self.config.remote_backup
        if rb and rb.target_dataset:
            targets.append(rb.target_dataset.rstrip('/'))
        for dest in (self.config.destinations or {}).values():
            if getattr(dest, 'is_local', False) is True:
                target = (dest.target_dataset or '').rstrip('/')
                if target and target not in targets:
                    targets.append(target)
        return targets

    def expand_recursive(
        self, should_stop: Optional[Callable[[], bool]] = None,
        *, scope: Optional[Callable[[], 'contextlib.AbstractContextManager']] = None,
        quiet: bool = False,
    ) -> Expansion:
        """The descendants of the enabled `recursive: true` datasets that have
        `remote:` entries, each as its own sending pair (never `send -R`, `-p`
        or `-I`: every child is an ordinary `-i` chain into its own copy).
        Needs no daemon: the workers and the CLI (`check`, `rename`) call it
        on a `DatasetManager` built from a `BackupConfig`. Nothing is stored
        on the manager's `datasets`: the snapshot and pruning workers never see
        a child (the parent's `_snapshot_tree`, which keeps backup copies out
        of the snapshot, and its recursive prune cover it).

        * **Discovery** is a name-only `zfs list -r -t filesystem,volume` of
          each such parent, UNTIMED (source datasets never are). It names no
          property, so no value can forge a row; the default (non-strict) list
          reader is right: a name libzfseasy cannot represent is dropped with
          its own WARNING, and nothing else can be skipped. Only names strictly
          below the parent count (hidden `%recv` datasets are never listed by
          zfs, and `@`, `#`, `%` names are refused here regardless).
        * **Explicit configuration wins.** A descendant configured explicitly,
          enabled or not, is not expanded (it is sent by its own entry or not
          at all). Walking down from a parent, a configured DISABLED dataset
          excludes its whole subtree (R-1), and so does any configured ENABLED
          RECURSIVE one, whether or not it has `remote:` entries (`remote: []`
          means "snapshot locally, send nothing", and those names are not the
          outer parent's to send): it is the root of its own subtree, expanded
          from its own listing when it has `remote:` entries, so each name has
          exactly one owner and a pair is unique by (dataset, destination).
          A configured enabled NON-recursive ancestor is passed through: the
          names below it still expand from the nearest enabled recursive one.
        * **Stamped config.** A child's config is the parent's with the
          child's name and `recursive=False`: `to_property(destination)`
          stamps the child's name, the parent's retention for that destination
          and the frequency; `raw` per destination is the parent's (`raw: auto`
          is decided per child by the sender). Each child owns copies of the
          parent's `remote` and `retention_rules` (the dataclasses are mutable).
          Clones are ordinary children.
        * **Backup copies are never sources** (trust boundary: a copy of
          another client's data must not be sent under this host's id, raw
          needs no key). A name at or below a receive target
          (`_receive_targets`: the server target ENABLED OR NOT, and every
          local destination's target) is skipped with one ERROR per parent
          and target; and so is any descendant, with its whole subtree, that
          carries a LOCAL, RECEIVED or INHERITED `org.zfsbackup:client_id` or
          `source_dataset` (one `zfs get -r -s local,received,inherited`,
          name/property/source only, so no value is read; one call per
          parent, after the listing, so every listed name existed when it was
          read). A tree replicated onto this host (`send -R`, syncoid with
          properties) carries them as RECEIVED, and a dataset under a marked
          ancestor ABOVE the recursive parent inherits them: then the parent
          itself is inside a copy and EVERY child is skipped (the ERROR says
          so). Only those two keys: a source has a LOCAL
          `org.zfsbackup:config` and anchors, never these (the daemon's own
          sends carry no properties and the documented restore uses no
          `-p`/`-R`). One ERROR per (parent, copy root) per episode. If that
          read fails the parent's expansion FAILS (`failed`): it is never
          expanded without it.

        `failed` maps a parent whose listing failed to the error: its children
        are then unknown (not gone), the parent itself is unaffected. ERROR once
        per parent while it persists, INFO when it works again. `should_stop`
        is checked before each parent; once it is true the result is partial
        and only fit for leaving. `Expansion.skipped` lists every backup copy and
        receive target left out, per parent (the ERRORs above say the same, once
        per episode); `quiet=True` (the health check, which reports `skipped`
        itself) logs those two ERRORs at DEBUG instead and does not mark the
        episode as announced. `scope()` (the health check passes one)
        returns a context manager every `zfs` call of the discovery runs in
        (its timeouts); a `ZFSTimeoutError` from it or from a call is that
        parent's failure, like any other. Never raises."""
        children: List[DatasetInfo] = []
        failed: Dict[str, str] = {}
        configured = {c.name: c for c in self.config.datasets}
        roots = [c for c in self.config.enabled_datasets if c.recursive and c.remote]
        if not roots:
            self._expand_error_noted.clear()
            self._expand_target_noted.clear()
            self._expand_copy_noted.clear()
            return Expansion(children, failed, {})
        scope = scope or _no_scope
        targets = self._receive_targets()
        seen: Set[str] = set()
        skipped: Dict[str, Dict[str, str]] = {}
        blocked_now: Set[Tuple[str, str]] = set()
        copies_now: Set[Tuple[str, str]] = set()
        listed_ok: Set[str] = set()
        stopped = False
        for root in roots:
            if should_stop is not None and should_stop():
                stopped = True
                break
            try:
                with scope():
                    listing = [
                        d.name for d in zfs.list(
                            roots=Dataset(root.name), recursive=True, types='filesystem,volume'
                        )
                    ]
                copy_roots = self._copy_roots(root.name, scope=scope)
            except Exception as e:
                failed[root.name] = f"{type(e).__name__}: {e}"
                if root.name not in self._expand_error_noted:
                    self._expand_error_noted.add(root.name)
                    logger.error(
                        "Cannot list the descendants of %s, so they are not sent until this "
                        "passes (%s: %s); %s itself is unaffected",
                        root.name, type(e).__name__, e, root.name,
                    )
                else:
                    logger.debug("Still cannot list the descendants of %s: %r", root.name, e)
                continue
            listed_ok.add(root.name)
            if root.name in self._expand_error_noted:
                self._expand_error_noted.discard(root.name)
                logger.info("The descendants of %s can be listed again", root.name)
            prefix = root.name + '/'
            for name in listing:
                if (
                    not name.startswith(prefix) or name in seen
                    or any(c in name for c in '@#%')
                ):
                    continue
                if self._expansion_owner(name, root.name, configured) is None:
                    continue
                hit = next((t for t in targets if name == t or name.startswith(t + '/')), None)
                if hit is not None:
                    key = (root.name, hit)
                    blocked_now.add(key)
                    skipped.setdefault(root.name, {}).setdefault(hit, 'a receive target, which holds copies')
                    if quiet:
                        logger.debug("%s is at or below the receive target %s: not sent", name, hit)
                    elif key not in self._expand_target_noted:
                        self._expand_target_noted.add(key)
                        logger.error(
                            "%s is at or below %s, which receives copies: it and its descendants "
                            "are not sent (a recursive dataset must not contain a receive target; "
                            "move the target out of %s)", name, hit, root.name,
                        )
                    continue
                copy_root = self._topmost_copy(name, root.name, copy_roots)
                if copy_root is not None:
                    key = (root.name, copy_root)
                    copies_now.add(key)
                    skipped.setdefault(root.name, {}).setdefault(
                        copy_root, self._copy_why(copy_roots[copy_root]))
                    if quiet:
                        logger.debug("%s is a backup copy: not sent", copy_root)
                    elif key not in self._expand_copy_noted:
                        self._expand_copy_noted.add(key)
                        logger.error(self._copy_root_message(
                            root.name, copy_root, copy_roots[copy_root],
                        ))
                    continue
                seen.add(name)
                info = DatasetInfo(
                    Dataset(name),
                    dataclasses.replace(
                        root, name=name, recursive=False,
                        retention_rules=[dataclasses.replace(r) for r in root.retention_rules],
                        remote=[
                            dataclasses.replace(
                                rc, retention_rules=[dataclasses.replace(r) for r in rc.retention_rules]
                            )
                            for rc in root.remote
                        ],
                    ),
                )
                info.expanded_from = root.name
                children.append(info)
        if not stopped:
            # Forget an episode that is over (the recovery INFO above covers
            # the parents that were listed; a parent no longer configured just goes).
            self._expand_error_noted &= {r.name for r in roots}
            self._expand_target_noted = {
                k for k in self._expand_target_noted
                if k in blocked_now or k[0] not in listed_ok
            }
            self._expand_copy_noted = {
                k for k in self._expand_copy_noted
                if k in copies_now or k[0] not in listed_ok
            }
        return Expansion(
            children, failed,
            {parent: list(names.items()) for parent, names in skipped.items()},
        )

    @staticmethod
    def _copy_why(source: str) -> str:
        """`Expansion.skipped`'s short phrase for a copy root whose marker comes
        from `source` (what zfs printed: `local`, `received`, `inherited from X`)."""
        props = f"{PROP_CLIENT_ID} or {PROP_SOURCE_DATASET}"
        if source.startswith('inherited'):
            origin = source[len('inherited from '):] or 'an ancestor'
            return f"inherits {props} from {origin}"
        return f"carries a {'received' if source == 'received' else 'local'} {props}"

    @staticmethod
    def _copy_roots(
        parent: str, scope: Optional[Callable[[], 'contextlib.AbstractContextManager']] = None,
    ) -> Dict[str, str]:
        """The datasets at or below `parent` that carry a LOCAL, RECEIVED or
        INHERITED `org.zfsbackup:client_id` or `org.zfsbackup:source_dataset`:
        backup copies (a server's, a local destination's, or a replica of
        either), wherever they sit, mapped to the source zfs printed
        (`local`, `received` or `inherited from <dataset>`; a local or
        received one wins over an inherited one). One recursive, value-free
        `zfs get -H -o name,property,source -s local,received,inherited -r -t
        filesystem,volume org.zfsbackup:client_id,org.zfsbackup:source_dataset
        <parent>` (just the two properties: `all` would print a row for every
        inherited native property of every dataset) filtered by property
        prefix, so a hostile value cannot disturb it, and names libzfseasy
        cannot represent still count. `inherited` also puts `parent` itself in the
        map when an ancestor ABOVE it carries them: the parent is then inside
        a copy. Raises on any failure (the caller then does not expand): a
        `ZFSTimeoutError` unchanged, anything else as a `RuntimeError` that
        names `parent` and chains the cause."""
        try:
            with (scope or _no_scope)():
                rows = zfs.get.property_sources(
                    Dataset(parent), types='filesystem,volume',
                    sources=['local', 'received', 'inherited'], recursive=True,
                    prefix=[PROP_CLIENT_ID, PROP_SOURCE_DATASET],
                    properties=[PROP_CLIENT_ID, PROP_SOURCE_DATASET],
                )
        except ZFSTimeoutError:
            raise
        except Exception as e:
            raise RuntimeError(
                f"cannot read the backup-copy markers ({PROP_CLIENT_ID}, {PROP_SOURCE_DATASET}) "
                f"at or below {parent}: {e}"
            ) from e
        found: Dict[str, str] = {}
        for r in rows:
            if r.property not in (PROP_CLIENT_ID, PROP_SOURCE_DATASET):
                continue
            have = found.get(r.name)
            if have is None or (have.startswith('inherited') and not r.source.startswith('inherited')):
                found[r.name] = r.source
        return found

    @staticmethod
    def _copy_root_message(parent: str, copy_root: str, source: str) -> str:
        """The ERROR for a copy root met while expanding `parent`: what it is,
        that it and everything below it is not sent, and the remedy that fits
        where the marker comes from. `zfs inherit` clears only a LOCAL value.
        On a RECEIVED one it merely MASKS it (`zfs inherit -S` only reverts a
        local override to it): the property then reads as inherited or unset,
        which `_copy_roots` does not see, so the copy would stop being
        recognised as one. For a received marker the remedy is therefore to
        move the copy out, never to inherit it."""
        props = f"{PROP_CLIENT_ID} or {PROP_SOURCE_DATASET}"
        mask = (
            "a plain `zfs inherit` of a received value only masks it, and a masked marker "
            "makes this check stop seeing the copy"
        )
        if source.startswith('inherited'):
            origin = source[len('inherited from '):] or 'an ancestor'
            return (
                f"{parent} itself is inside a backup copy: it inherits {props} from {origin}, "
                f"so none of its descendants is sent. If {origin} is a backup copy, move "
                f"{parent} out of it (zfs rename) or stop it being recursive; if it is really a "
                f"source, clear the properties on {origin} (zfs inherit {PROP_CLIENT_ID} "
                f"{origin}; zfs inherit {PROP_SOURCE_DATASET} {origin}), which works only for "
                f"local values ({mask})"
            )
        if source == 'received':
            return (
                f"{copy_root} carries a received {props}: it is a replicated backup copy, so it "
                f"and its descendants (under {parent}) are not sent. A received value is not "
                f"removed by `zfs inherit` (`zfs inherit -S` only reverts a local override to it; "
                f"{mask}): move the copy out of {parent} (zfs rename {copy_root} <a name outside "
                f"{parent}>)"
            )
        return (
            f"{copy_root} carries a local {props}: it is a backup copy, so it and its "
            f"descendants (under {parent}) are not sent. If it is really a source, clear them: "
            f"zfs inherit {PROP_CLIENT_ID} {copy_root}; zfs inherit {PROP_SOURCE_DATASET} "
            f"{copy_root}. Otherwise move it out of {parent} (zfs rename)"
        )

    @staticmethod
    def _topmost_copy(name: str, parent: str, copy_roots: Dict[str, str]) -> Optional[str]:
        """The topmost dataset among `parent` (itself) and the names between
        it and `name`, `name` included, that is in `copy_roots`, else `None`."""
        if not copy_roots:
            return None
        node = parent
        if node in copy_roots:
            return node
        for part in name[len(parent) + 1:].split('/'):
            node = f"{node}/{part}"
            if node in copy_roots:
                return node
        return None

    @staticmethod
    def _expansion_owner(
        name: str, root: str, configured: Dict[str, DatasetConfig],
    ) -> Optional[str]:
        """`root` if `name` (strictly below it) is to be expanded from it,
        else `None`: the name itself is configured (explicit config wins), a
        configured disabled dataset lies between (R-1), or a configured
        ENABLED RECURSIVE dataset lies between (it owns its subtree, with or
        without `remote:` entries: `remote: []` sends nothing and nothing
        outer may send there). An enabled non-recursive configured dataset
        between is passed through."""
        if name in configured:
            return None
        ancestor = name.rpartition('/')[0]
        while len(ancestor) > len(root):
            cfg = configured.get(ancestor)
            if cfg is not None and (not cfg.enabled or cfg.recursive):
                return None
            ancestor = ancestor.rpartition('/')[0]
        return root

    def verify_datasets(self, should_stop: Optional[Callable[[], bool]] = None) -> None:
        """Log an ERROR for each configured dataset that does not exist.
        `should_stop`, as for `sync_all_config_properties`: checked before
        each dataset, and the check ends there once it returns true."""
        for ds_info in self.datasets:
            if should_stop is not None and should_stop():
                return
            if not zfs.exists(ds_info.dataset):
                logger.error(f"Dataset {ds_info.name} does not exist")

    def dataset_report(self) -> None:
        logger.info(f"Config: {len(self.config.datasets)} datasets configured")
        logger.info(f"Snapshot prefix: {self.config.snapshot_prefix}")
        logger.info(f"Check interval: {self.config.check_interval}")
        logger.info(f"Dry run mode: {self.config.dry_run}")
        logger.info("=" * 60)
        for ds in self.config.enabled_datasets:
            logger.info(f"  Dataset: {ds.name}")
            logger.info(f"    Frequency: {ds.frequency}")
            logger.info(f"    Recursive: {ds.recursive}")
            logger.info(f"    Retention rules: {len(ds.retention_rules)}")
            for rule in ds.retention_rules:
                logger.info(f"      - Age {rule.age} -> Keep for {rule.keep_for}")
            # Surface retention-rule collisions at startup, in the
            # supervisor's log -- the first signal for a colliding config
            # should be at boot, not a worker's first prune/backup cycle.
            _, collisions = _collapse_retention_rules(ds.retention_rules)
            for msg in collisions:
                logger.warning(f"Dataset {ds.name}: {msg}")
            if ds.remote:
                logger.info(f"    Remote destinations: {[r.destination for r in ds.remote]}")
                for r in ds.remote:
                    _, dest_collisions = _collapse_retention_rules(
                        ds.effective_retention_rules(r.destination)
                    )
                    for msg in dest_collisions:
                        logger.warning(f"Dataset {ds.name} -> {r.destination}: {msg}")

    # ── ZFS property helpers ──────────────────────────────────────────────────

    def _get_prop(self, ds: Dataset, prop: str) -> Optional[str]:
        """Read a single ZFS user property from a dataset. Returns None if unset.

        The effective value, whatever its source (local, received or
        inherited), read with `zfs get -H -o value` (`zfs.get.value`): exact
        even when the value holds a tab or newline, which a `zfs list -H` row
        cannot frame. Inherited values are right for PROTECTION only: the
        pruner (`get_anchors`, through `get_anchor`) wants a parent's anchor
        to widen the protect-set (it never reads an inherited pin: its pin
        readers take local/received values), and `sync_config_property`
        compares the value as it stands. Evidence about THIS dataset's own
        send history (the due check, the anchor-never-backwards rule,
        settling; health reads local values too, through `get_local_anchor` /
        `get_local_inflight` or its batched equivalent) must read only a LOCAL
        value -- `get_own_anchor` / `get_own_inflight` / `get_local_anchor` --
        or a child without one is
        credited with its parent's progress.

        While a local copy is being pruned (`_prune_timed`) a `ZFSTimeoutError`
        propagates instead of reading as "unset": "could not read" must never
        shrink the pruner's protect-set (`get_anchors`). Elsewhere (source
        datasets, the senders) the read is untimed or handles it itself."""
        try:
            return zfs.get.value(ds, prop)
        except Exception as e:
            if self._prune_timed and isinstance(e, ZFSTimeoutError):
                raise
            logger.debug(f"Failed to read property {prop} from {ds.name}: {e!r}")
        return None

    def sync_config_property(self, dsi: DatasetInfo) -> None:
        """Write the dataset's config to its ZFS user property (text config is authoritative)."""
        encoded = dsi.config.to_property()
        current = self._get_prop(dsi.dataset, PROP_CONFIG)
        if current != encoded:
            if self.config.dry_run:
                # `--dry-run` must not write to ZFS: log what would be set.
                logger.info(f"[DRY RUN] Would set {PROP_CONFIG} on {dsi.name}")
                return
            try:
                zfs.set(dsi.dataset, {PROP_CONFIG: encoded})
                logger.debug(f"Synced config property for {dsi.name}")
            except Exception as e:
                logger.warning(f"Failed to sync config property for {dsi.name}: {e}")

    def sync_all_config_properties(
        self, should_stop: Optional[Callable[[], bool]] = None
    ) -> None:
        """Sync config properties for all locally configured datasets.

        `should_stop`, when given, is checked before each dataset; once it
        returns true the sync stops there (the supervisor passes its stop
        flag, so a shutdown during a reload's sync is not held up by one
        `zfs set` per remaining dataset)."""
        for dsi in self.datasets:
            if should_stop is not None and should_stop():
                return
            self.sync_config_property(dsi)

    def get_anchor(self, dsi: DatasetInfo, destination: str) -> Optional[str]:
        """Return the anchor snapshot name for a given destination, or None."""
        prop = f"{PROP_ANCHOR_PREFIX}{destination}"
        return self._get_prop(dsi.dataset, prop)

    def _get_own_prop(self, ds: Dataset, prop: str) -> Optional[str]:
        """`prop` as a LOCAL value of `ds` only (`zfs get -s local`); `None`
        when unset, inherited or received. Any failure is ALSO `None` (with a
        DEBUG line, as for `_get_prop`), so this read must only feed
        decisions where "unknown" is safe as "no history": the due check
        (due, and negotiate decides) and a pin read that merely
        over-protects. NEVER use it for health (`health_collect` reads with
        the raising `get_local_anchor` / `get_local_inflight`: an unreadable
        value is a CRIT there) or where the result leads to writing
        or clearing an anchor or pin: those use `get_local_anchor`, which
        raises. While a
        local copy is being pruned a `ZFSTimeoutError` propagates instead (as
        in `_get_prop`; no current caller runs there, but "could not read"
        must never shrink a protect-set)."""
        try:
            return zfs.get.value(ds, prop, sources='local')
        except Exception as e:
            if self._prune_timed and isinstance(e, ZFSTimeoutError):
                raise
            logger.debug(f"Failed to read local property {prop} from {ds.name}: {e!r}")
        return None

    def get_own_anchor(self, dsi: DatasetInfo, destination: str) -> Optional[str]:
        """The anchor snapshot name for a destination as a LOCAL value of
        this dataset, or `None` (unset, inherited, received OR UNREADABLE --
        the failure is swallowed). Use it only for the "due" check, where an
        unreadable anchor safely reads as "no history" (health does not: it
        reads with `get_local_anchor`, and an unreadable anchor is a CRIT).
        Anything that writes or clears an anchor, or decides from the anchor
        whether something may be sent or destroyed (`_record_step`,
        `_settle_up_to_date`, `_rollback_permit`), must use
        `get_local_anchor`, which raises `LocalAnchorUnreadable`. A parent's
        inherited anchor never counts here; `get_anchor` stays the pruner's
        (inherited values only widen protection)."""
        return self._get_own_prop(dsi.dataset, f"{PROP_ANCHOR_PREFIX}{destination}")

    def get_local_anchor(self, dsi: DatasetInfo, destination: str) -> Optional[str]:
        """The RAISING local read, for every caller that writes or clears an
        anchor or decides from it: the anchor snapshot name for a destination
        as a LOCAL value of this very dataset, or `None` (unset, inherited or
        received). `get_own_anchor` is the swallowing variant, for the "due"
        check only.
        `get_anchor` also reports an inherited value, which must not
        count where the anchor is evidence about this dataset's own history
        (`RemoteBackupManager._rollback_permit`): a dataset restored under a
        new name has no local anchor even if its parent has one.

        Raises `LocalAnchorUnreadable` when zfs fails: "unset" and "could not
        be read" are different evidence and the caller must tell them apart."""
        return self._get_local_prop(dsi.dataset, f"{PROP_ANCHOR_PREFIX}{destination}")

    @staticmethod
    def _get_local_prop(ds: Dataset, prop: str) -> Optional[str]:
        """`prop` as a LOCAL value of `ds` (`zfs get -s local`), `None` when
        unset, inherited or received; RAISES `LocalAnchorUnreadable` when zfs
        fails (a `ZFSTimeoutError` included), so "could not read" is never
        "unset"."""
        try:
            return zfs.get.value(ds, prop, sources='local')
        except Exception as e:
            logger.debug(f"Failed to read local property {prop} from {ds.name}: {e!r}")
            raise LocalAnchorUnreadable(str(e) or repr(e)) from e

    def set_anchor(self, dsi: DatasetInfo, destination: str, snapshot_name: str) -> bool:
        """Record the anchor snapshot for a destination. `True` if the
        property was written, `False` (after a WARNING) if not."""
        prop = f"{PROP_ANCHOR_PREFIX}{destination}"
        try:
            zfs.set(dsi.dataset, {prop: snapshot_name})
            logger.debug(f"Set anchor for {dsi.name} -> {destination}: {snapshot_name}")
            return True
        except Exception as e:
            logger.warning(f"Failed to set anchor property for {dsi.name}: {e}")
            return False

    def clear_anchor(self, dsi: DatasetInfo, destination: str) -> None:
        """Clear the LOCAL anchor snapshot for a destination (`zfs inherit -S`:
        removes the local value, or reverts it to the received one, which a
        plain inherit would hide from the pruner's protect set)."""
        prop = f"{PROP_ANCHOR_PREFIX}{destination}"
        try:
            zfs.inherit(dsi.dataset, prop, received=True)
            logger.debug(f"Cleared anchor for {dsi.name} -> {destination}")
        except Exception as e:
            logger.warning(f"Failed to clear anchor property for {dsi.name}: {e}")

    def get_own_inflight(self, dsi: DatasetInfo, destination: str) -> Optional[Tuple[str, str]]:
        """The `(from, to)` pair recorded in a LOCAL `inflight.<destination>`
        (`from` is `''` for a full send): the pin of this dataset's own send,
        never a parent's inherited one. `None` when unset, inherited,
        received, malformed or unreadable. A failed read means "not pinned",
        which is safe only where that merely over-protects (settling then does
        not clear a pin it could not read); health uses `get_local_inflight`,
        and the pruner reads pins through `read_zfsbackup_properties`, not
        here."""
        return _parse_inflight(
            self._get_own_prop(dsi.dataset, f"{PROP_INFLIGHT_PREFIX}{destination}")
        )

    def get_local_inflight(self, dsi: DatasetInfo, destination: str) -> Optional[Tuple[str, str]]:
        """`get_own_inflight` that RAISES: the `(from, to)` pair of this
        dataset's LOCAL `inflight.<destination>`, `None` when unset, inherited,
        received or malformed, and `LocalAnchorUnreadable` when zfs fails (so
        "could not read" is never reported as "not pinned"). For health
        reporting (`health_collect`), where an unreadable value must be
        reported, not a silent "no pin"."""
        return _parse_inflight(
            self._get_local_prop(dsi.dataset, f"{PROP_INFLIGHT_PREFIX}{destination}")
        )

    def set_inflight(
        self, dsi: DatasetInfo, destination: str, from_name: Optional[str], to_name: str
    ) -> bool:
        """Pin `from_name` (None/'' for a full send) and `to_name` for
        `destination` while a fresh send runs: `inflight.<dest>=<from>,<to>`.
        Must be written BEFORE the `zfs send` starts. `True` if written,
        `False` (after a WARNING) if not -- the send then runs unpinned, as
        it did before this property existed."""
        prop = f"{PROP_INFLIGHT_PREFIX}{destination}"
        try:
            zfs.set(dsi.dataset, {prop: f"{from_name or ''},{to_name}"})
            logger.debug(
                f"Set in-flight pin for {dsi.name} -> {destination}: "
                f"{from_name or '(full)'} -> {to_name}"
            )
            return True
        except Exception as e:
            logger.warning(f"Failed to set in-flight property for {dsi.name}: {e}")
            return False

    def clear_inflight(self, dsi: DatasetInfo, destination: str) -> bool:
        """Remove the LOCAL `inflight.<destination>` (`zfs inherit -S`, which
        reverts to a received value instead of hiding it). `True` if cleared, `False` (after a WARNING) if not: a stale pin only
        over-protects two snapshots until the next send overwrites it."""
        prop = f"{PROP_INFLIGHT_PREFIX}{destination}"
        try:
            zfs.inherit(dsi.dataset, prop, received=True)
            logger.debug(f"Cleared in-flight pin for {dsi.name} -> {destination}")
            return True
        except Exception as e:
            logger.warning(f"Failed to clear in-flight property for {dsi.name}: {e}")
            return False

    def get_anchors(self, dsi: DatasetInfo, own_properties_last: bool = False) -> Set[str]:
        """Return the snapshot names pinned by ANY `org.zfsbackup:anchor.*`
        property on the dataset, whether or not its destination is configured.

        This is the pruner's protect-set, and it may only ever err on the side
        of protecting more. Sources:

        * every `anchor.*` user property set `local` or `received` on the
          dataset (via `read_zfsbackup_properties`: a two-exec read that
          only ever reads our own values and raises rather than return a
          partial set).
          An anchor of a destination
          that was renamed or removed from the config still points at the
          real incremental base until an operator clears it, and a daemon on
          an older config may keep advancing it. `received` counts too: this
          daemon's own sends carry no properties (no `-p`), so a received
          anchor only appears on a dataset replicated with `-p` from another
          host, where it names that origin's incremental base -- protecting it
          errs toward keeping a snapshot.
        * both snapshot names of every `inflight.*` property
          (`<from>,<to>`, see `PROP_INFLIGHT_PREFIX`), local or received: a
          send that is running, or was interrupted and can be resumed, needs
          its base and its target to survive pruning.
        * plus, as before, the per-configured-destination `get_anchor` value
          (which also sees inherited values), so the result is a superset of
          the old behaviour.

        `own_properties_last` puts the read of the dataset's own `anchor.*`
        and `inflight.*` properties (the one that sees an in-flight pin)
        AFTER the per-destination reads, so it is the last `zfs` call made
        here: used right before a destroy (`_still_prunable`), to leave as
        little time as possible between reading the protection and acting on it.

        Raises if the property read fails: the caller must not prune on an
        unknown protect-set (`prune_snapshots` skips the dataset instead).
        """
        anchors: Set[str] = set()
        configured = {r.destination for r in (dsi.config.remote or [])}
        if own_properties_last:
            for name in sorted(configured):
                anchor = self.get_anchor(dsi, name)
                if anchor:
                    anchors.add(anchor)
        # Ask for the configured destinations' names up front: an `inflight.*`
        # the remote worker creates between the two execs is then still seen.
        also = [f"{pre}{name}" for name in sorted(configured)
                for pre in (PROP_ANCHOR_PREFIX, PROP_INFLIGHT_PREFIX)]
        for prop, value in read_zfsbackup_properties(dsi.dataset, also).items():
            if prop.startswith(PROP_ANCHOR_PREFIX):
                prefix = PROP_ANCHOR_PREFIX
                pinned = [value] if value and value != '-' else []
            elif prop.startswith(PROP_INFLIGHT_PREFIX):
                prefix = PROP_INFLIGHT_PREFIX
                pinned = _inflight_names(value)
            else:
                continue
            if not pinned:
                continue
            anchors.update(pinned)
            dest = prop[len(prefix):]
            if dest not in configured:
                logger.debug(
                    f"{prop}={value} on {dsi.name} belongs to no configured "
                    "destination; still protecting it from pruning"
                )
                key = (dsi.name, prop)
                if key not in self._orphan_anchor_noted:
                    self._orphan_anchor_noted.add(key)
                    logger.info(
                        f"Dataset {dsi.name}: {prop} is not a configured destination but "
                        f"still pins snapshot(s) {', '.join(pinned)} against pruning. "
                        f"If it is stale, release it with: zfs inherit {prop} "
                        f"{shlex.quote(dsi.name)}"
                    )
        if not own_properties_last:
            for name in sorted(configured):
                anchor = self.get_anchor(dsi, name)
                if anchor:
                    anchors.add(anchor)
        return anchors

    def received_datasets(
        self, should_stop: Optional[Callable[[], bool]] = None,
        *, scope: Optional[Callable[[], 'contextlib.AbstractContextManager']] = None,
        bulk_scope: Optional[Callable[[], 'contextlib.AbstractContextManager']] = None,
    ) -> List[DatasetInfo]:
        """Discover datasets received from remote clients under the configured target.

        `should_stop` (the worker passes "an exit reason is pending", as for
        `verify_datasets`) is checked before each dataset is examined, because
        a dataset that falls back costs two or three `zfs get` executions and
        a server with hundreds of them would otherwise not notice a stop or a
        reload for minutes. Once it returns true the enumeration ends
        and the datasets examined so far are returned: a PARTIAL list, which
        the caller must not use for anything but leaving (the pruner treats
        each dataset independently, so a short list prunes less, never
        more). The skip memory (`_received_skip_noted`) is left untouched
        then, since it is trimmed against the full listing only.

        Plan B-1c (cross-tenant property injection). A user-property value
        can hold a newline or tab, and `zfs list -H` prints it raw, so one
        `zfs list -o …client_id,config` could be made to print a forged row
        naming another client's dataset with the attacker's retention. So:

        * `zfs list` only ENUMERATES (name and type columns; a dataset name
          cannot hold a tab or newline, and libzfseasy drops any row it
          cannot trust);
        * plan B-1d: the properties are then read for ALL of them in one
          framing-safe `zfs.get.rows` (`client_id`, `config`; recursive over
          the target, no `received` column, `verify_names` off because the
          enumeration above is the cross-check). The batch is used only if
          NOTHING in it was skipped (`rows.complete`): a skipped row can be
          the unattributable tail of a forged value, which leaves a
          valid-looking row behind. Within a complete batch a dataset is
          taken only if its group was read cleanly (not dropped, exactly the
          two properties); any other name (a forged or malformed group, a
          duplicate, a name the listing has and the batch lacks) is read the
          old way, one `zfs.get.value` per property, which frames any value
          exactly. If the batch is incomplete or fails, EVERY dataset is read
          that way. A forged group can thus never make a victim dataset
          vanish from pruning;
        * only LOCAL values count: these are written by the server's own
          negotiate, so a received copy (a `zfs send -p` stream) or an
          inherited one (a nested server dataset under another client's)
          never counts;
        * the client id must pass the A7 grammar (`client_id_problem`) and
          the dataset must be `<target>/<client_id>` or lie under
          `<target>/<client_id>/` -- the only place negotiate puts that
          client's datasets -- so a config can only ever drive pruning of
          its own client's tree;
        * any mismatch skips THAT dataset with a WARNING (once while the
          problem persists, see `_received_skip_noted`); the others are still
          returned. A dataset with neither property set locally is not a
          received client dataset (the target itself, the `<target>/<id>`
          container, intermediate parents): skipped at DEBUG -- or at
          WARNING if it carries a *received* config. A dataset with a valid
          local `client_id` (inside its client's tree) and no config is a
          container negotiate stamped (B-2c): skipped at DEBUG as well.

        Cost: one `zfs list` and one `zfs get -r` per pruning cycle; if the
        batch is incomplete or fails, plus two or three `zfs get` executions
        for EVERY dataset, else for each dataset the batch dropped.

        `scope()` (the health check passes one) returns a context manager
        every `zfs` call of the discovery runs in (as for `local_copies`); it
        may raise to refuse a call, and every call enters its own. A failure,
        a timeout included, is then what it always is: `[]` and
        `last_received_error`. `bulk_scope()` (default: `scope`) is the one for
        the two calls whose run time grows with the number of received
        datasets (the recursive listing and the batched property read); the
        per-dataset fallback reads use `scope`.
        """
        self.last_received_error = None
        rb = self.config.remote_backup
        if not rb or not rb.enabled:
            return []
        target = rb.target_dataset.rstrip('/')
        result, failure = self._discover_under(
            target, should_stop=should_stop, noted=self._received_skip_noted, scope=scope,
            bulk_scope=bulk_scope,
        )
        if failure is not None:
            self.last_received_error = f"{type(failure).__name__}: {failure}"
            logger.error(f"Failed to discover received datasets: {failure!r}")
            return []
        return result

    def local_copies(
        self, client_id: Optional[str], should_stop: Optional[Callable[[], bool]] = None
    ) -> List[DatasetInfo]:
        """Discover THIS host's copies under each local destination's target
        (`zfs:<dataset>`): the datasets of `<target>/<client_id>/...` that
        `LocalBackupManager` stamped with a local `client_id`, `source_dataset`
        and `config`, for the pruner. They are found and checked exactly as the
        server's received datasets are (`_discover_under`: names enumerated by
        `zfs list`, the properties read in one framing-safe batch with the
        exact per-dataset fallback, local values only, the home-path check,
        `wire_config_problem`, never recursive), but ONLY under our own id:
        another host's tree on a shared target -- including one whose id has
        ours as a prefix (`A` and `Ax`: component-wise, never a string prefix)
        -- is never listed, read or pruned. A child dataset without a local
        `client_id` is not discovered (so not pruned).

        Every `zfs` call here runs under `libzfseasy.timeout`
        (`LOCAL_DISCOVER_COMMAND_BUDGET` per command, `LOCAL_DISCOVER_BUDGET`
        in all for one target), so a target on an exported or suspended pool
        ends that target's discovery with an error (one WARNING per episode),
        never wedges the worker. Per-target outcomes are left in
        `last_local_discovery` (one `LocalDiscovery` per local destination
        with a usable target, in configuration order); a failed target
        contributes no datasets. `should_stop` is as in `received_datasets`: a
        stop ends the enumeration and the (partial) list is only for leaving.
        With no client id nothing is discovered (the caller says why).

        A local destination that no enabled dataset sends to any more is
        still discovered when its target exists (the copies already there are
        still ours to prune), but a missing target is then no error and leaves
        no outcome at all: a destination that was declared and never used must
        not report a failing discovery every cycle.

        `last_local_skip` says why nothing was discovered for the whole
        configuration (no client id, an invalid one), so that the caller keeps
        the old outcomes instead of dropping them; the invalid id is warned
        about once per manager (a worker incarnation)."""
        self.last_local_discovery = []
        self.last_local_skip = None
        self._local_prune_blocked = {}
        if client_id is None:
            self.last_local_skip = "this host has no client id"
            return []
        id_problem = client_id_problem(client_id)
        if id_problem is not None:
            self.last_local_skip = f"the client id {client_id!r} is invalid ({id_problem})"
            if client_id not in self._bad_client_id_noted:
                self._bad_client_id_noted.add(client_id)
                logger.warning(
                    "Local copies are not pruned: the client id %r is invalid (%s)",
                    client_id, id_problem,
                )
            else:
                logger.debug("Local copies are not pruned: the client id %r is invalid", client_id)
            return []
        used = {r.destination for ds in self.config.enabled_datasets for r in (ds.remote or [])}
        result: List[DatasetInfo] = []
        seen_targets: Set[str] = set()
        for name, dest in self.config.destinations.items():
            if getattr(dest, 'is_local', False) is not True:
                continue
            target = (dest.target_dataset or '').rstrip('/')
            if not target or target in seen_targets:
                continue
            seen_targets.add(target)
            if should_stop is not None and should_stop():
                return result
            deadline = time.monotonic() + LOCAL_DISCOVER_BUDGET
            found, failure = self._discover_under(
                target, only_client_id=client_id, should_stop=should_stop,
                noted=self._local_skip_noted.setdefault(target, {}),
                scope=_discovery_scope(deadline),
            )
            if should_stop is not None and should_stop():
                return result
            if isinstance(failure, _DiscoveryFailed) and name not in used:
                logger.debug(
                    "Local destination %s (%s) is not used by any dataset and its target does "
                    "not exist; nothing to discover", name, target,
                )
                self._local_error_noted.pop(target, None)
                self._local_prune_noted.pop(target, None)
                continue
            if failure is not None or not found:
                # No prune pass can run on this target this cycle, so a prune
                # timeout noted earlier belongs to a finished episode: forget
                # it, or the next one would be logged at DEBUG only.
                self._local_prune_noted.pop(target, None)
            error = None if failure is None else _describe_failure(failure)
            if failure is not None:
                kind = _failure_kind(failure)
                if self._local_error_noted.get(target) != kind:
                    self._local_error_noted[target] = kind
                    logger.warning(
                        "Cannot discover the local copies under %s (destination %s): %s; "
                        "they are not pruned until this passes", target, name, error,
                    )
                else:
                    logger.debug("Still cannot discover the local copies under %s: %s", target, error)
                found = []
            elif self._local_error_noted.pop(target, None) is not None:
                logger.info("The local copies under %s can be discovered again", target)
            for dsi in found:
                dsi.local_target = target
            self.last_local_discovery.append(LocalDiscovery(name, target, len(found), error))
            result.extend(found)
        for target in [t for t in self._local_error_noted if t not in seen_targets]:
            del self._local_error_noted[target]
        for target in [t for t in self._local_skip_noted if t not in seen_targets]:
            del self._local_skip_noted[target]
        for target in [t for t in self._local_prune_noted if t not in seen_targets]:
            del self._local_prune_noted[target]
        return result

    def _discover_under(
        self,
        target: str,
        only_client_id: Optional[str] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        *,
        noted: Optional[Dict[str, str]] = None,
        scope: Optional[Callable[[], 'contextlib.AbstractContextManager']] = None,
        bulk_scope: Optional[Callable[[], 'contextlib.AbstractContextManager']] = None,
    ) -> Tuple[List[DatasetInfo], Optional[Exception]]:
        """`(datasets, failure)`: the client datasets under `target`
        (`received_datasets` documents the checks), or `([], exception)` if the
        enumeration failed (the caller logs it). Nothing here raises.

        `only_client_id` restricts it to `<target>/<only_client_id>` and what
        lies under it (the enumeration and the batch read are rooted there,
        and a dataset stamped with another id is refused); a missing
        `<target>/<only_client_id>` under an existing `target` is no error,
        there is simply nothing yet. `noted` is the skip memory
        (`_received_skip_noted` for the server). `scope()` returns a context
        manager every `zfs` call of the discovery runs in (the local
        discovery's timeouts); it raises `ZFSTimeoutError` when the overall
        budget is spent. `bulk_scope` (default: `scope`) wraps the recursive
        listing and the batched property read, the two calls that grow with the
        tree; each call enters its own scope."""
        scope = scope or _no_scope
        bulk_scope = bulk_scope or scope
        root = target if only_client_id is None else f"{target}/{only_client_id}"
        try:
            with bulk_scope():
                all_ds = zfs.list(
                    roots=Dataset(root),
                    recursive=True,
                    # Volumes too: a client volume is received as a volume
                    # (`volmode=none`, never a device) and is pruned and
                    # health-checked like a filesystem. Nothing downstream
                    # assumes a mount: it lists snapshots under the name and
                    # destroys them.
                    types='filesystem,volume',
                )
        except Exception as e:
            if only_client_id is None or isinstance(e, ZFSTimeoutError):
                return [], e
            # The failure may be a missing `<target>/<id>` (nothing sent yet:
            # the container appears with the first copy) or anything else.
            # Ask zfs rather than read the message: a dataset NAME may contain
            # "dataset does not exist".
            try:
                with scope():
                    root_exists = zfs.exists(Dataset(root))
                if root_exists:
                    return [], e
                with scope():
                    target_exists = zfs.exists(Dataset(target))
            except Exception as e2:
                return [], e2
            if target_exists:
                return [], None
            # The whole target is gone (an exported pool reads the same).
            return [], _DiscoveryFailed(f"the target dataset {target} does not exist")

        try:
            batch = self._received_property_batch(
                root, {ds.name for ds in all_ds}, bulk_scope,
                raise_timeout=only_client_id is not None,
            )
        except ZFSTimeoutError as e:
            return [], e

        result = []
        if noted is None:
            noted = {}
        seen: Set[str] = set()
        for ds in all_ds:
            if should_stop is not None and should_stop():
                return result, None
            seen.add(ds.name)
            try:
                dsi, problem = self._received_dataset_info(
                    ds, target, batch.get(ds.name), only_client_id=only_client_id, scope=scope
                )
            except ZFSTimeoutError as e:
                return [], e
            if dsi is not None:
                noted.pop(ds.name, None)
                result.append(dsi)
                continue
            if problem is None:
                continue
            if noted.get(ds.name) != problem:
                noted[ds.name] = problem
                logger.warning(
                    "Skipping %s %r: %s; it is not pruned until this is fixed",
                    'local copy' if only_client_id is not None else 'received dataset',
                    ds.name, problem,
                )
            else:
                logger.debug(
                    "Skipping %s %r: %s",
                    'local copy' if only_client_id is not None else 'received dataset',
                    ds.name, problem,
                )
        for name in [n for n in noted if n not in seen]:
            del noted[name]
        return result, None

    def _received_property_batch(
        self, target: str, listed: Set[str],
        scope: Optional[Callable[[], 'contextlib.AbstractContextManager']] = None,
        raise_timeout: bool = False,
    ) -> Dict[str, Tuple[Optional[str], Optional[str], bool]]:
        """Plan B-1d: `{name: (client_id, config, config_only_received)}` for
        every dataset of `listed` (the trusted name-only enumeration under
        `target`) whose group one framing-safe `zfs.get.rows` read cleanly.
        Local values only; `config_only_received` is true when `config` has
        no local value but a received one (a `zfs send -p` stream carried
        it), which is never trusted but worth a WARNING.

        A name is OMITTED -- the caller reads it with `zfs.get.value`, one
        property at a time -- if the batch dropped its group, its group is
        not there, or its rows are not exactly the two properties asked for.
        Nothing in the batch can therefore remove a dataset from the result
        of `received_datasets`, only send it to the exact per-dataset path.
        Never raises: if the batch fails, it is empty (everything falls
        back) -- except a `ZFSTimeoutError` when `raise_timeout` (the local
        discovery: a target that does not answer is not read dataset by
        dataset, 60 s each)."""
        try:
            with (scope or _no_scope)():
                rows = zfs.get.rows(
                    Dataset(target), [PROP_CLIENT_ID, PROP_CONFIG], types='filesystem,volume',
                    recursive=True, received=False,
                    # The caller's enumeration is the cross-check: a name it lists
                    # that was not read cleanly is sent to the fallback below.
                    verify_names=False,
                )
        except Exception as e:
            if raise_timeout and isinstance(e, ZFSTimeoutError):
                raise
            logger.warning(
                "Cannot read the properties of %s in one call (%r); reading them "
                "one dataset at a time", target, e,
            )
            return {}
        if not rows.complete:
            # A skipped row may be the unattributable tail of a forged value
            # (`FORGED<TAB>local<NL>` leaves a valid-looking 4-column row with
            # source `local` that the group check cannot tell from a real
            # one). Trust nothing from this batch: every dataset is read
            # exactly, one `zfs.get.value` per property.
            logger.warning(
                "The batched property read of %s skipped %d row(s); reading the "
                "datasets one at a time", target, rows.skipped,
            )
            return {}
        by_name: Dict[str, Dict[str, Tuple[str, str]]] = {}
        for row in rows:
            if row.name in listed and row.name not in rows.dropped and \
                    row.property in (PROP_CLIENT_ID, PROP_CONFIG):
                by_name.setdefault(row.name, {})[row.property] = (row.value, row.source)
        result: Dict[str, Tuple[Optional[str], Optional[str], bool]] = {}
        for name, found in by_name.items():
            if set(found) != {PROP_CLIENT_ID, PROP_CONFIG}:
                continue

            def local(prop: str) -> Optional[str]:
                value, source = found[prop]
                return value if source == 'local' and value != '-' else None

            config_value = local(PROP_CONFIG)
            client_id = local(PROP_CLIENT_ID)
            value, source = found[PROP_CONFIG]
            only_received = (
                client_id is None and config_value is None
                and source == 'received' and value != '-'
            )
            result[name] = (client_id, config_value, only_received)
        return result

    def _received_dataset_info(
        self, ds: Dataset, target: str,
        props: Optional[Tuple[Optional[str], Optional[str], bool]] = None,
        *,
        only_client_id: Optional[str] = None,
        scope: Optional[Callable[[], 'contextlib.AbstractContextManager']] = None,
    ) -> Tuple[Optional[DatasetInfo], Optional[str]]:
        """`(DatasetInfo, None)` for a valid received client dataset (see
        `received_datasets`), else `(None, problem)` -- `problem` is `None`
        for a dataset that is simply not a client dataset (no local
        zfsbackup properties at all). Never raises. Values are logged with
        `!r`: they may hold control characters or lone surrogates.

        `props` is `(client_id, config, config_only_received)` from
        `_received_property_batch`; without it the properties are read here,
        one `zfs.get.value` each (the exact path).

        `only_client_id` (the local discovery) additionally refuses a dataset
        whose local `client_id` is any other id, whatever its name. `scope`
        wraps the `zfs` reads (timeouts); with `only_client_id` or a caller's `scope`
        (the health check's budget; the internal `_no_scope` default is not one), a `ZFSTimeoutError` -- a per-call timeout
        or the scope's refusal after the deadline -- propagates instead of
        becoming a problem, so the dataset is never silently dropped. The
        server's pruner passes neither and keeps the skip-with-WARNING."""
        if props is None:
            try:
                # One scope per exec: a time limit applies to each command.
                enter = scope or _no_scope
                with enter():
                    client_id = zfs.get.value(ds, PROP_CLIENT_ID, sources='local')
                with enter():
                    config_value = zfs.get.value(ds, PROP_CONFIG, sources='local')
                only_received = False
                if client_id is None and config_value is None:
                    with enter():
                        only_received = zfs.get.value(ds, PROP_CONFIG, sources='received') is not None
            except Exception as e:
                if (only_client_id is not None or (scope is not None and scope is not _no_scope)) and isinstance(e, ZFSTimeoutError):
                    raise
                return None, f"cannot read its properties ({e!r})"
        else:
            client_id, config_value, only_received = props
        if client_id is None and config_value is None:
            # Not a client dataset (the target, a `<target>/<id>` container,
            # an intermediate parent) -- unless a config arrived in a
            # `zfs send -p` stream, which is never trusted but worth a
            # WARNING.
            if only_received:
                return None, (
                    f"{PROP_CONFIG} is only received (zfs send -p), not set locally; "
                    "a received config is never trusted"
                )
            logger.debug(f"{ds.name}: no local {PROP_CLIENT_ID}/{PROP_CONFIG}; not a client dataset")
            return None, None
        if client_id is None:
            return None, f"{PROP_CLIENT_ID} is not set locally"
        id_problem = client_id_problem(client_id)
        if id_problem is not None:
            return None, f"{PROP_CLIENT_ID}={client_id!r} is invalid: {id_problem}"
        if only_client_id is not None and client_id != only_client_id:
            return None, (
                f"{PROP_CLIENT_ID}={client_id!r} is not this host's id {only_client_id!r}; "
                "another host's copy is never pruned from here"
            )
        home = f"{target}/{client_id}"
        if ds.name != home and not ds.name.startswith(home + '/'):
            return None, (
                f"it lies outside its client's tree {home!r} "
                f"({PROP_CLIENT_ID}={client_id!r})"
            )
        if config_value is None:
            # B-2c: the server's negotiate stamps `client_id` (and nothing
            # else) on every container it creates (`<target>/<id>` and the
            # intermediate parents), so "client_id, no config" is such a
            # container, not a damaged client dataset: not pruned, not worth
            # a WARNING. (A leaf whose config was removed by hand lands here
            # too; it is then not pruned either, only quietly.)
            logger.debug(
                f"{ds.name}: {PROP_CLIENT_ID} set, no local {PROP_CONFIG}; "
                "a container, not a client dataset"
            )
            return None, None
        try:
            ds_config = DatasetConfig.from_property(config_value)
        except Exception as e:
            return None, f"{PROP_CONFIG} does not decode ({type(e).__name__}: {e})"
        # Received datasets are never pruned recursively. The client does not
        # send with `-R`: every received child is its own configured client
        # dataset with its own received config and retention, so a parent's
        # `zfs destroy -r` would apply the parent's retention to the children's
        # history (and could remove a child's incremental base). The client's
        # `recursive` describes how the CLIENT snapshots; it has no meaning for
        # the copy on this server. Forced here, on a copy of the config,
        # because the only consumer of these objects is the pruner
        # (`PruningWorker`); sends, negotiation and the API use the manager's
        # own configured `datasets`.
        # The decoder checks types loosely and the property may predate the
        # API's positive-duration check (or have been set by hand): a zero
        # `age` is a division by zero in `needs_prunning`.
        shape_problem = wire_config_problem(ds_config)
        if shape_problem is not None:
            return None, f"{PROP_CONFIG} is unusable ({shape_problem})"
        try:
            ds_config = dataclasses.replace(ds_config, recursive=False)
            return DatasetInfo(ds, ds_config), None
        except Exception as e:
            return None, f"{PROP_CONFIG} is unusable ({type(e).__name__}: {e})"

    # ── Snapshot operations ───────────────────────────────────────────────────

    def clock_tolerance(self) -> timedelta:
        """How far ahead of now a snapshot name may be before it is taken to
        mean the clock is wrong: `max(5 minutes, check_interval)`."""
        return max(CLOCK_TOLERANCE, self.config.check_interval)

    def clock_behind(self, dsi: DatasetInfo, snap: 'SnapshotInfo', what: str = 'snapshot') -> bool:
        """True if `snap` is dated beyond `clock_tolerance()` in the future,
        i.e. the system clock is behind it. Logs an ERROR once per
        (dataset, `what`, snapshot) while that persists; `what` tells the
        callers' checks apart (the newest snapshot, or one anchor)."""
        if snap.timestamp is None:
            return False
        ahead = snap.timestamp - datetime.now(timezone.utc)
        if ahead <= self.clock_tolerance():
            self._clock_noted = {
                k for k in self._clock_noted if k[0] != dsi.name or k[1] != what
            }
            return False
        key = (dsi.name, what, snap.name)
        if key not in self._clock_noted:
            self._clock_noted.add(key)
            logger.error(
                f"Dataset {dsi.name}: {what} {snap.full_name} is dated {ahead} in the future: "
                "system clock is behind the newest snapshot; check NTP"
            )
        else:
            logger.debug(f"Dataset {dsi.name}: {what} {snap.full_name} is still dated in the future")
        return True

    def begin_snapshot_cycle(self) -> None:
        """Forget what this cycle's recursive snapshots created (A2e). The
        snapshot worker calls it at the top of every cycle, so only a snapshot
        made in THIS cycle is ever taken to be an ancestor's."""
        self._tree_snapshots.clear()

    def _covering_snapshot(self, dsi: DatasetInfo, name: str) -> Optional[str]:
        """The enabled recursive configured ancestor of `dsi` whose
        `create_snapshot` in this cycle created `name` over `dsi` (its command
        covered `dsi`), else `None`. Only the record of a SUCCESSFUL
        `create_snapshot` since `begin_snapshot_cycle` counts: a name that
        merely exists (an old one, a clock that went backwards) never does."""
        if not self._tree_snapshots:
            return None
        for ancestor in self.datasets:
            if ancestor is dsi or not ancestor.recursive or not dsi.name.startswith(ancestor.name + '/'):
                continue
            made = self._tree_snapshots.get(ancestor.name)
            if made is None:
                continue
            made_name, cover = made
            if made_name == name and (cover is None or dsi.name in cover):
                return ancestor.name
        return None

    def ridden_snapshot(self, dsi: DatasetInfo) -> Optional[SnapshotRide]:
        """A2e. `SnapshotRide` when the snapshot `dsi` would generate for this
        period was just created over it by an enabled recursive configured
        ancestor's snapshot in this cycle (`create_snapshot`'s own record,
        not a re-read of who made it), else `None`. The dataset then has its
        snapshot for the period: nothing to create, no "already exists"
        WARNING, not a failure. The record only says the ancestor's command
        covered `dsi`; one listing of `dsi` confirms it really holds the name
        (a configured dataset that does not exist, or one created after the
        ancestor's listing, does not), and a failed listing answers `None`, so
        the dataset takes its normal path."""
        if not self._tree_snapshots:
            return None
        name = self._generate_snapshot_name(dsi)
        ancestor = self._covering_snapshot(dsi, name)
        if ancestor is None:
            return None
        self.last_list_error = None
        self._last_list_failure = None
        try:
            # Quiet on failure (DEBUG): `needs_snapshot` lists again right
            # after and reports a failure once, at ERROR; one failed listing
            # is one ERROR, not two.
            self._list_snapshots_strict(dsi)
        except Exception as e:
            self.last_list_error = f"{type(e).__name__}: {e}"
            self._last_list_failure = e
            logger.debug(f"Dataset {dsi.name}: could not confirm the ridden snapshot {name}: {e}")
            return None
        if name not in dsi.listed_names:
            return None
        logger.debug(
            f"Dataset {dsi.name}: snapshot {name} was created by the recursive snapshot of "
            f"{ancestor} in this cycle; it counts as its own"
        )
        return SnapshotRide(name, ancestor)

    def needs_snapshot(self, dsi: DatasetInfo) -> bool:
        snapshots = self.list_snapshots(dsi, recursive=False)
        if not snapshots:
            logger.debug(f"Dataset {dsi.name} needs snapshot: True (no existing snapshots)")
            return True
        latest = snapshots[0]  # newest by createtxg
        age = latest.age
        if self.clock_behind(dsi, latest):
            # The newest snapshot's name is in the future: its age says
            # nothing, and waiting would leave the dataset unprotected until
            # the clock catches up. A snapshot taken now is the newest by
            # txg, which ends the condition.
            needs = True
        else:
            needs = age >= dsi.frequency
        logger.debug(f"Latest snapshot for {dsi.name} is {latest.full_name} (age={age})")
        logger.debug(f"Dataset {dsi.name} needs snapshot: {needs} (age={age}, frequency={dsi.frequency})")
        if needs:
            name = self._generate_snapshot_name(dsi)
            if name in dsi.listed_names or any(s.name == name for s in snapshots):
                ancestor = self._covering_snapshot(dsi, name)
                if ancestor is not None:
                    # Not a backwards clock: the recursive snapshot of
                    # `ancestor` created it a moment ago, in this cycle (A2e).
                    logger.debug(
                        f"Dataset {dsi.name}: snapshot {name} was created by the recursive "
                        f"snapshot of {ancestor} in this cycle; it counts as its own"
                    )
                    return False
                # A clock that went backwards regenerates a name that exists:
                # `zfs snapshot` would fail on every cycle. Do nothing.
                key = (dsi.name, name)
                if key not in self._name_taken_noted:
                    self._name_taken_noted.add(key)
                    logger.warning(
                        f"Dataset {dsi.name}: the snapshot {name} is due but already exists "
                        "(the system clock went backwards?); not creating it"
                    )
                else:
                    logger.debug(f"Dataset {dsi.name}: snapshot {name} already exists; not creating it")
                return False
            self._name_taken_noted = {k for k in self._name_taken_noted if k[0] != dsi.name}
        return needs

    def _retention_plan(self, dsi: DatasetInfo) -> Dict[float, float]:
        """Return the resolved `{keep_for_seconds: age_seconds}` plan for a
        dataset's retention rules (see `_collapse_retention_rules`). Never
        hard-fails; any collision is resolved toward retaining more data and
        logged once per distinct ruleset for the lifetime of this manager
        (re-fires if the ruleset actually changes -- see `_retention_warned`).
        """
        plan, collisions = _collapse_retention_rules(dsi.config.retention_rules)
        if collisions:
            key = (
                dsi.name,
                frozenset(
                    (r.age.total_seconds(), r.keep_for.total_seconds())
                    for r in dsi.config.retention_rules
                ),
            )
            if key not in self._retention_warned:
                self._retention_warned.add(key)
                for msg in collisions:
                    logger.warning(f"Dataset {dsi.name}: {msg}")
        return plan

    def needs_prunning(self, dsi: DatasetInfo, anchors: Optional[Set[str]] = None) -> List['SnapshotInfo']:
        snapshots = dsi.snapshots
        if not snapshots or not dsi.config.retention_rules:
            return []

        plan = self._retention_plan(dsi)
        sorted_expiries = sorted(plan.keys())

        now = datetime.now(timezone.utc)
        claimed: dict = {}
        # The newest snapshot (by createtxg) is always kept.
        keep: set = {id(max(snapshots, key=snapshot_order_key))}

        # Slots and ages are by name timestamp, walked oldest first (then by
        # createtxg), so the OLDEST snapshot of a slot claims it.
        walk = sorted(
            (s for s in snapshots if s.timestamp is not None),
            key=lambda s: (s.timestamp, s.createtxg if s.createtxg is not None else -1, s.name),
        )
        for sni in walk:
            age_secs = (now - sni.timestamp).total_seconds()
            applicable_expiry = None
            for key in sorted_expiries:
                if key >= age_secs:
                    applicable_expiry = key
                    break
            if applicable_expiry is None:
                continue
            interval_secs = plan[applicable_expiry]
            slot_key = (applicable_expiry, int(sni.timestamp.timestamp() / interval_secs))
            if slot_key not in claimed:
                claimed[slot_key] = True
                keep.add(id(sni))

        to_prune = [sni for sni in snapshots if id(sni) not in keep]
        if anchors:
            to_prune = [sni for sni in to_prune if sni.name not in anchors]
        for sni in to_prune:
            logger.debug(f"Snapshot {sni.full_name} marked for pruning")
        return to_prune

    def _list_snapshots_strict(self, dsi: DatasetInfo, recursive: bool = False) -> List['SnapshotInfo']:
        """`list_snapshots` that lets a `zfs list` failure propagate.

        One `zfs list` call carries everything a decision needs
        (`SNAPSHOT_LIST_PROPERTIES`). The result is newest first by
        `createtxg`, then by name timestamp, then by name (two snapshots
        taken in one `zfs snapshot a b` share a txg). A managed snapshot with
        no `createtxg` cannot be ordered, so it is left out of the list --
        never pruned, never "newest" -- with a WARNING once."""
        snapshots = zfs.list(
            roots=dsi.dataset,
            types='snapshot',
            recursive=recursive,
            properties=list(SNAPSHOT_LIST_PROPERTIES),
        )
        result: List[SnapshotInfo] = []
        names: Set[str] = set()
        no_txg: List[SnapshotInfo] = []
        for snap in snapshots:
            if isinstance(snap, ZFSSnapshot):
                info = SnapshotInfo(snap, self.prefix)
                if not info.is_managed:
                    continue
                names.add(info.name)
                if info.createtxg is None:
                    no_txg.append(info)
                else:
                    result.append(info)
        fresh = [i for i in no_txg if (dsi.name, i.full_name) not in self._no_txg_noted]
        if fresh:
            logger.warning(
                f"Dataset {dsi.name}: {len(fresh)} managed snapshot(s) have no createtxg and are "
                f"ignored (never sent, pruned or treated as newest): "
                f"{', '.join(i.full_name for i in fresh[:5])}{' ...' if len(fresh) > 5 else ''}"
            )
        self._no_txg_noted = {k for k in self._no_txg_noted if k[0] != dsi.name}
        self._no_txg_noted.update((dsi.name, i.full_name) for i in no_txg)
        result.sort(key=snapshot_order_key, reverse=True)
        dsi.snapshots = result
        dsi.listed_names = names
        return dsi.snapshots

    def _list_snapshot_tree_strict(
        self, ds: Dataset,
        *, scope: Optional[Callable[[], 'contextlib.AbstractContextManager']] = None,
    ) -> Dict[str, List['SnapshotInfo']]:
        """The managed snapshots of `ds` AND every descendant, in ONE recursive
        `zfs list` (`SNAPSHOT_LIST_PROPERTIES`), as `{dataset name: [newest
        first by createtxg, ...]}` (the order of `_list_snapshots_strict`). A
        dataset with no managed snapshot has no key. For the health check,
        which asks once per recursive parent instead of once per child.

        Strict, with one exception. Whatever `zfs list` raises (a non-zero
        exit, a timeout) propagates instead of reading as an empty tree, and
        so does `zfs.list(strict=True)`'s refusal of a row it cannot trust (a
        wrong column count, a row outside the tree, a duplicate), because a
        dropped row may be a child's newest snapshot and the answer would be
        wrong, not smaller. The exception is a listing refused because its
        FIRST untrusted row is a name libzfseasy cannot represent (its names
        are ASCII: a user's snapshot or dataset with an umlaut in its name)
        anywhere under the parent. Strict alone made one such name fail EVERY
        child of the parent ("could not be trusted") while the daemon, which
        skips the name with a WARNING, and the parent's own observation carry
        on. That listing is read again without `strict` (libzfseasy logs each
        skipped name itself). This is safe because no column of this listing
        is writable by a user (`createtxg`, `guid` and `userrefs` are numbers,
        `clones` holds dataset names, `encryption` and `keystatus` are enums),
        so a skipped name cannot hide a forged row, and a managed snapshot
        (`{prefix}_{14 digits}`) of a child that `expand_recursive` listed is
        always representable. Residual: libzfseasy's refusal names only the
        first untrusted row, so a second, corrupt row in a listing that also
        has an unrepresentable name is skipped with a WARNING instead of
        refused (zfs cannot print one from these columns). `scope()` (the health
        check passes one) is entered once per `zfs list`, the second one
        included. Read-only: unlike
        `_list_snapshots_strict` it sets no `DatasetInfo.snapshots` and logs
        no WARNING for a managed snapshot without `createtxg` (it is left out,
        as there, so it is never "newest")."""
        def listing(strict: bool):
            with (scope or _no_scope)():
                return zfs.list(
                    roots=ds, types='snapshot', recursive=True,
                    properties=list(SNAPSHOT_LIST_PROPERTIES), strict=strict,
                )
        try:
            listed = listing(True)
        except Exception as e:
            message = str(e)
            if not (message.startswith(_UNTRUSTED_LISTING) and _FIRST_UNTRUSTED_IS_A_NAME in message):
                raise
            logger.debug(f"The listing of {ds.name} was refused for a name ({e}); reading it again")
            listed = listing(False)
        tree: Dict[str, List[SnapshotInfo]] = {}
        for snap in listed:
            if not isinstance(snap, ZFSSnapshot):
                continue
            info = SnapshotInfo(snap, self.prefix)
            if not info.is_managed:
                continue
            if info.createtxg is None:
                logger.debug(f"{info.full_name} has no createtxg and is ignored")
                continue
            tree.setdefault(snap.dataset.name, []).append(info)
        for group in tree.values():
            group.sort(key=snapshot_order_key, reverse=True)
        return tree

    def list_snapshots(self, dsi: DatasetInfo, recursive: bool = False) -> List['SnapshotInfo']:
        """List all managed snapshots for a dataset."""
        self.last_list_error = None
        self._last_list_failure = None
        try:
            return self._list_snapshots_strict(dsi, recursive)
        except Exception as e:
            self.last_list_error = f"{type(e).__name__}: {e}"
            self._last_list_failure = e
            if self._prune_timed and isinstance(e, ZFSTimeoutError):
                # A local copy's pass: `prune_snapshots` announces the episode.
                logger.debug(f"Failed to list snapshots for {dsi.name}: {e}")
                return []
            logger.error(f"Failed to list snapshots for {dsi.name}: {e}")
            return []

    def _generate_snapshot_name(self, dsi: DatasetInfo, timestamp: Optional[datetime] = None) -> str:
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        timestamp = dsi.get_reference_time(timestamp)
        date_str = timestamp.strftime("%Y%m%d")
        time_str = timestamp.strftime("%H%M%S")
        return f"{self.prefix}_{date_str}{time_str}"

    def create_snapshot(self, dsi: DatasetInfo) -> Optional['SnapshotInfo']:
        """Create a new snapshot for the dataset. `None` when it could not be
        created (`last_snapshot_error` says why) and in a dry run. A recursive
        dataset is snapshotted by `_snapshot_tree`, which keeps the backup
        copies below it out of the snapshot (`last_snapshot_note` then says so,
        for the status record). Nothing here depends on what `zfs.snapshot`
        returns: the snapshot object is built from `dsi` and the name."""
        self.last_snapshot_error = None
        self.last_snapshot_note = None
        self._tree_snapshots.pop(dsi.name, None)
        snap_name = self._generate_snapshot_name(dsi)
        full_name = f"{dsi.name}@{snap_name}"
        try:
            logger.info(f"Creating snapshot: {full_name} (recursive={dsi.recursive})")
            if self.config.dry_run:
                logger.info(f"[DRY RUN] Would create snapshot: {full_name}")
                return None
            if dsi.recursive:
                self._tree_cover = None
                self._snapshot_tree(dsi, snap_name)
                # The command succeeded: a configured descendant of `dsi` that
                # it covered may count `snap_name` as its own this cycle (A2e).
                self._tree_snapshots[dsi.name] = (snap_name, self._tree_cover)
            else:
                zfs.snapshot(dsi.dataset, snap_name, recursive=False)
            snapshot = ZFSSnapshot(dsi.dataset, snap_name)
            logger.info(f"Successfully created snapshot: {full_name}")
        except _InsideReceiveTarget as e:
            self._tree_snapshots.pop(dsi.name, None)
            self.last_snapshot_error = f"Failed to create snapshot {full_name}: {e}"
            # The refusal repeats every cycle until the configuration or the
            # layout changes: ERROR for the first, DEBUG for the rest.
            logger.log(
                logging.ERROR if e.first else logging.DEBUG,
                f"Failed to create snapshot {full_name}: {e}",
            )
            return None
        except Exception as e:
            self._tree_snapshots.pop(dsi.name, None)
            self.last_snapshot_error = f"Failed to create snapshot {full_name}: {e}"
            logger.error(f"Failed to create snapshot {full_name}: {e}")
            return None
        # The object `zfs snapshot` returns carries no `createtxg`, so it
        # cannot be ordered. Re-list (one `zfs list`) and return the listed
        # one. A failed re-list leaves `dsi.snapshots` as it was (the next
        # cycle lists again) and returns the bare object: the snapshot exists.
        try:
            for info in self._list_snapshots_strict(dsi):
                if info.name == snap_name:
                    return info
        except Exception as e:
            logger.warning(f"Created {full_name} but could not re-list {dsi.name}: {e}")
        return SnapshotInfo(snapshot, self.prefix)

    def _snapshot_tree(self, dsi: DatasetInfo, snap_name: str) -> None:
        """Run the snapshot command for the RECURSIVE `dsi`, never snapshotting
        INTO a backup copy below it (A2c). Returns nothing: only that the
        command succeeded is guaranteed (it raises otherwise), and the caller
        builds the snapshot object itself, so no shape of `zfs.snapshot`'s
        return value (a list, in listing order, that may have dropped a row)
        is relied on.

        * No copy below it (`_copy_tops`): plain `zfs snapshot -r`, as always.
        * Copies found: ONE atomic `zfs snapshot dsi@s child@s ...` (`zfs.snapshot.many`)
          over `dsi` and every source descendant (`_snapshot_sources`: a
          strict, name-only listing minus every copy top's subtree). One
          ERROR per (dsi, copy top) per episode names the copy, why it is one
          and the remedy; an INFO when it is gone. When `dsi` ITSELF has or
          inherits a copy marker (every child inherits it, so every child is a
          top) there is ONE ERROR per (dsi, origin of the marker) instead, and
          only `dsi` is snapshotted.
        * If the copy read or the listing FAILS (any `Exception`: a
          `RuntimeError` from the wrapped reads, a timeout, an untrusted
          listing outside the copies; a stop or interrupt is a
          `BaseException` and is not caught), or `many` refuses the list
          BEFORE running zfs (a `ValueError`: more than
          `SnapshotCommand.MANY_MAX_ARGV_BYTES` of argv, ~1000 datasets, which
          is never split because it would not be atomic), fall back to plain
          `snapshot -r` with one ERROR per episode. Deliberate (user
          decision): a stray snapshot in a copy is recoverable, a missed
          source snapshot is not. A zfs FAILURE of `many` itself is not
          caught (a plain `-r` would fail the same way): it ends the cycle as
          before.
        * `dsi` itself at or below a receive target (`validate_config`
          refuses such a configuration; `_copy_tops` then makes `dsi` the top):
          nothing is snapshotted and this raises `_InsideReceiveTarget`
          (ERROR once per episode, INFO when it is over). Everything there is
          a copy, so any snapshot, `dsi` alone included, would be one INTO a
          copy, and there is no source whose snapshot could be missed.

        `last_snapshot_note` is set to a short standing warning whenever copies
        were left out or could not be told apart.

        The check "`dsi` at or below a receive target" is config only and runs
        BEFORE any zfs read, so a failing read can never fall back to
        `snapshot -r` inside a target. After a successful `many`,
        `self._tree_cover` holds the names it covered (`None` stays for
        `snapshot -r`: every descendant), which `create_snapshot` records for
        `ridden_snapshot` (A2e).

        The command is atomic and fails as a whole: a source child destroyed
        or renamed between the listing and the snapshot fails the whole
        command, so NOTHING is snapshotted that cycle (the next one lists
        again and retries). A child created in that window misses this one
        snapshot (the next period catches up), and a copy created in it gets
        one stray snapshot. A collision (a configured child that took the
        same name first: a period boundary fell between this dataset's turn
        and the child's, or this command failed once and the child snapshotted
        itself) is retried once without the datasets that hold the name
        (`_run_tree_command`), so it never locks the tree out of a period.
        Source datasets are never timed."""
        # Config only, before any zfs read: a read that fails must never get
        # to fall back to `snapshot -r` inside a receive target.
        at_target = self._receive_target_of(dsi.name)
        if at_target is not None:
            raise self._inside_target_refusal(dsi, at_target[1])
        tops: Dict[str, str] = {}
        sources: Optional[List[Dataset]] = None
        failure: Optional[str] = None
        try:
            tops = self._copy_tops(dsi)
            if tops and dsi.name not in tops:
                sources = self._snapshot_sources(dsi, tops)
        except Exception as e:
            failure = f"{type(e).__name__}: {e}"
        if failure is None:
            if dsi.name in tops:
                raise self._inside_target_refusal(dsi, tops[dsi.name])
            if dsi.name in self._snapshot_inside_target_noted:
                self._snapshot_inside_target_noted.discard(dsi.name)
                logger.info(f"{dsi.name} is no longer at or below a receive target")
        if failure is None and sources is not None:
            try:
                self._run_tree_command(dsi, snap_name, sources)
            except ValueError as e:
                # Raised before zfs runs (argv bound, names): nothing was created.
                failure = f"ValueError: {e}"
        if failure is not None:
            if dsi.name not in self._snapshot_fallback_noted:
                self._snapshot_fallback_noted.add(dsi.name)
                logger.error(
                    f"Cannot keep the backup copies out of the snapshot of {dsi.name} ({failure}); "
                    f"falling back to `zfs snapshot -r`, which also snapshots into any copy below it "
                    "(a stray snapshot in a copy is recoverable, a missed source snapshot is not). "
                    "This repeats until the read passes"
                )
            else:
                logger.debug(f"Still cannot tell the copies below {dsi.name} apart: {failure}")
            self.last_snapshot_note = (
                "backup copies below could not be told apart, so `zfs snapshot -r` was used and may "
                f"have snapshotted into one ({failure})"
            )
            self._run_tree_command(dsi, snap_name, None)
            return
        if dsi.name in self._snapshot_fallback_noted:
            self._snapshot_fallback_noted.discard(dsi.name)
            logger.info(
                f"The backup copies below {dsi.name} can be told apart again"
                + ("; they are kept out of its snapshot again" if tops else " (none was found)")
            )
        if sources is None:
            # No copy below it.
            self._run_tree_command(dsi, snap_name, None)
        self._snapshot_copy_episodes(dsi, snap_name, tops)
        inside = getattr(tops, 'inside', None)
        if inside is not None and getattr(tops, 'covered', None):
            self.last_snapshot_note = (
                f"{dsi.name} sits inside a backup copy ({inside[0]}): the datasets below it "
                "were not snapshotted"
            )
        elif tops:
            names = sorted(tops)
            shown = ', '.join(names[:3]) + (f" (+{len(names) - 3} more)" if len(names) > 3 else '')
            self.last_snapshot_note = (
                f"{len(names)} backup cop{'y' if len(names) == 1 else 'ies'} below "
                f"{'was' if len(names) == 1 else 'were'} not snapshotted: {shown}"
            )

    def _run_tree_command(self, dsi: DatasetInfo, snap_name: str, sources: Optional[List[Dataset]]) -> None:
        """Run the tree's snapshot command: `zfs snapshot -r` (`sources is
        None`) or the atomic `many` over `sources`, and set `self._tree_cover`
        to what it covered (`None` for `-r`: every descendant). A `ValueError`
        (raised before zfs runs) propagates untouched. A zfs failure is given
        ONE retry by `_retry_without_holders`, which raises when it cannot
        help, so the caller sees a failure exactly as before."""
        try:
            if sources is None:
                zfs.snapshot(dsi.dataset, snap_name, recursive=True)
                cover: Optional[FrozenSet[str]] = None
            else:
                zfs.snapshot.many(sources, snap_name)
                cover = frozenset(d.name for d in sources)
        except ValueError:
            raise
        except Exception as e:
            cover = self._retry_without_holders(dsi, snap_name, sources, e)
        self._tree_cover = cover

    def _retry_without_holders(
        self, dsi: DatasetInfo, snap_name: str, sources: Optional[List[Dataset]], error: Exception,
    ) -> FrozenSet[str]:
        """The tree command failed with `error`. When that is because some
        datasets of the tree's SOURCE set already hold `snap_name` (a
        configured child took it first: the period's boundary fell between
        this dataset's turn and the child's, or an earlier failure of this
        command let the child snapshot itself), a retry would collide for the
        rest of the period and the whole tree would miss it. So: ONE
        name-only `zfs list -H -r -t snapshot` of the tree finds the holders,
        and the command is run ONCE more, atomically, over the other source
        datasets (`zfs snapshot many`); the holders already have the period's
        snapshot and their own record path handles them. Returns what the
        retry covered (the holders are not in it).

        The decision is made from the listing, never from the error text: the
        OpenZFS captures in `tests/fixtures/openzfs/` hold no snapshot-name
        collision (only `cannot create snapshot '<ds@name>': dataset is busy`
        and M4's "multiple snapshots of same fs"), and libzfseasy forces
        `LC_ALL=C` but a message can change between versions. A failure with
        no holder (out of space, a source destroyed in the window) re-raises
        `error` unchanged. So does an unreadable listing (DEBUG), a listing
        in which `dsi` itself holds the name (a backwards clock, which
        `needs_snapshot` already refuses) and a source list that cannot be
        read for a `-r`. A failed retry raises a `RuntimeError` naming both
        errors. Called at most once per `create_snapshot`, and never raises a
        `ValueError` (the caller would turn that into a second attempt). The
        listing is not strict: a row it drops is a holder it misses, and then
        the retry fails like the first command did; it can never exclude a
        dataset that does not hold the name. Source datasets are never timed."""
        try:
            holders = {
                s.dataset.name for s in zfs.list(roots=dsi.dataset, types='snapshot', recursive=True)
                if isinstance(s, ZFSSnapshot) and s.short == snap_name
            }
            if not holders:
                raise error     # nothing collides: the failure is something else
            # Only now the list of source datasets (for a `-r`, one more read).
            everything = list(sources) if sources is not None else self._snapshot_sources(dsi, {})
            held = holders & {d.name for d in everything}
        except Exception as e:
            if e is error:
                raise
            logger.debug(f"Dataset {dsi.name}: could not look for holders of {snap_name} after {error}: {e}")
            raise error
        if not held or dsi.name in held:
            raise error
        rest = [d for d in everything if d.name not in held]
        try:
            zfs.snapshot.many(rest, snap_name)
        except Exception as e:
            logger.debug(f"Dataset {dsi.name}: retry of {snap_name} without {len(held)} holder(s) failed: {e}")
            raise RuntimeError(
                f"{e} (retried once without the {len(held)} dataset(s) that already hold {snap_name}; "
                f"the first attempt failed with: {error})"
            ) from e
        key = (dsi.name, snap_name)
        shown = sorted(held)
        listing = ', '.join(shown[:5]) + (f" +{len(shown) - 5}" if len(shown) > 5 else '')
        if key not in self._snapshot_holders_noted:
            self._snapshot_holders_noted = {k for k in self._snapshot_holders_noted if k[0] != dsi.name}
            self._snapshot_holders_noted.add(key)
            logger.info(
                f"Snapshot {dsi.name}@{snap_name}: {len(held)} dataset(s) below already held {snap_name} "
                f"and were left out of the atomic snapshot ({listing}); normal after a period boundary "
                "that fell between two datasets' turns"
            )
        else:
            logger.debug(f"Snapshot {dsi.name}@{snap_name}: left out the holders {listing}")
        return frozenset(d.name for d in rest)

    def _inside_target_refusal(self, dsi: DatasetInfo, why: str) -> '_InsideReceiveTarget':
        """The refusal to snapshot a recursive `dsi` that is itself at or below
        a receive target (`why`: `_receive_target_of`'s clause), to be raised.
        `first` is True for the first of an episode."""
        first = dsi.name not in self._snapshot_inside_target_noted
        self._snapshot_inside_target_noted.add(dsi.name)
        return _InsideReceiveTarget(
            f"{dsi.name} is itself inside a backup copy ({why}): not snapshotting "
            "it (a configured dataset must be outside every receive target; remove it from "
            "the configuration or move it out with zfs rename)",
            first,
        )

    def _snapshot_copy_episodes(self, dsi: DatasetInfo, snap_name: str, tops: Dict[str, str]) -> None:
        """The once-per-episode ERROR / INFO lines of a successful
        `_snapshot_tree`: one per copy top (those that exist only because
        `dsi` itself has or inherits a marker are in `tops.covered` and get
        the one `dsi`-level message instead), and the INFOs for what is gone."""
        covered = getattr(tops, 'covered', frozenset())
        for top, why in tops.items():
            if top in covered:
                continue
            key = (dsi.name, top)
            if key in self._snapshot_copy_noted:
                continue
            self._snapshot_copy_noted.add(key)
            logger.error(
                f"{top} is a backup copy ({why}) below the recursive dataset {dsi.name}: it and "
                f"everything below it is NOT snapshotted (the rest of {dsi.name} is, as "
                f"{dsi.name}@{snap_name}, in one atomic command). Move it out of {dsi.name} "
                f"(zfs rename {top} <a name outside {dsi.name}>) or make {dsi.name} non-recursive"
            )
        for key in [k for k in self._snapshot_copy_noted if k[0] == dsi.name and k[1] not in tops]:
            self._snapshot_copy_noted.discard(key)
            logger.info(f"{key[1]}, a backup copy below {dsi.name}, is gone; {dsi.name} is "
                        "snapshotted recursively again")
        # A top that is now COVERED (`dsi` itself carries the marker) is not
        # gone: the `dsi`-level episode below speaks for it, so its own one
        # ends without a word.
        self._snapshot_copy_noted.difference_update(
            k for k in list(self._snapshot_copy_noted) if k[0] == dsi.name and k[1] in covered
        )
        self._inside_episode(
            dsi, tops, self._snapshot_inside_copy_noted,
            f"only {dsi.name} itself is snapshotted (as {dsi.name}@{snap_name}); nothing below it is",
        )

    def _inside_episode(
        self, dsi: DatasetInfo, tops: Dict[str, str], noted: Set[Tuple[str, str]], effect: str,
    ) -> None:
        """ERROR once per (`dsi`, origin) while `dsi` itself has or inherits a
        copy marker (`tops.inside`) and children below it are copies by that
        inheritance (`tops.covered`), INFO when that is over. Replaces one
        message per child, whose `zfs rename <child>` remedy would be wrong:
        the children are copies only by inheritance."""
        inside = getattr(tops, 'inside', None)
        # Only when something below it is a copy by inheritance: a dataset that
        # has no children has nothing to say.
        now = {(dsi.name, inside[0])} if inside is not None and getattr(tops, 'covered', None) else set()
        for key in sorted(now - noted):
            noted.add(key)
            logger.error(self._inside_copy_message(dsi.name, inside[0], inside[1], effect))
        for key in [k for k in noted if k[0] == dsi.name and k not in now]:
            noted.discard(key)
            logger.info(f"Nothing below {dsi.name} is a backup copy by inheritance from {key[1]} "
                        "any more")

    @staticmethod
    def _inside_copy_message(dsi: str, origin: str, source: str, effect: str) -> str:
        """The ERROR for a recursive `dsi` that itself sits inside a backup copy:
        it has (`local`/`received`) or inherits `source` the copy markers, and
        so does everything below it."""
        props = f"{PROP_CLIENT_ID} or {PROP_SOURCE_DATASET}"
        where = (
            f"it inherits {props} from {origin}" if source.startswith('inherited')
            else f"it carries a {source} {props}"
        )
        return (
            f"{dsi} itself sits inside a backup copy: {where}, so everything below it is a copy by "
            f"inheritance; {effect}. If {origin} is a backup copy, move {dsi} out of it (zfs "
            f"rename) or make {dsi} non-recursive; if it is really a source, clear the markers on "
            f"{origin} (zfs inherit {PROP_CLIENT_ID} {origin}; zfs inherit {PROP_SOURCE_DATASET} "
            f"{origin}), which works only for local values (a plain `zfs inherit` of a received "
            "one only masks it, and a masked marker hides the copy from this check)"
        )

    def _snapshot_sources(self, dsi: DatasetInfo, tops: Dict[str, str]) -> List[Dataset]:
        """`dsi` (first) and every descendant that is not at or below one of
        `tops`, from strict, name-only `zfs list -H -t filesystem,volume -o
        name,type` reads, UNTIMED (source datasets never are). Strict so that
        a name libzfseasy cannot represent never silently misses its snapshot
        (the caller then falls back to `snapshot -r`), but only OUTSIDE the
        copies: a bad name inside a copy is none of this snapshot's business.

        One recursive listing of `dsi` first (the usual case). Only if it is
        refused as untrusted (a name libzfseasy cannot represent: its error
        does not say which, and the non-strict list does not report skipped
        names) the tree is walked instead (`_walk_sources`): every dataset
        that has a copy top below it is listed one level (`-d 1`), every other
        child recursively, and no top is ever listed, so a bad name below a top
        is never read and one anywhere else is still refused. A bad name that
        is itself a copy top cannot be told from a source's, so that falls back
        to `snapshot -r` too. Raises a `RuntimeError` naming `dsi` (cause
        chained) on any failure or a listing that does not contain `dsi` or has
        a name outside it.

        When `dsi` itself has or inherits a copy marker (`tops.inside`) every
        descendant is a copy by inheritance, so the answer is `dsi` alone and
        nothing is listed: a name below it that libzfseasy cannot represent
        can then never force the fallback to `snapshot -r` into the copies."""
        if getattr(tops, 'inside', None) is not None and dsi.name not in tops:
            return [dsi.dataset]
        try:
            try:
                listed = list(zfs.list(
                    roots=dsi.dataset, types='filesystem,volume', recursive=True, strict=True,
                ))
            except Exception as e:
                if not str(e).startswith(_UNTRUSTED_LISTING):
                    raise
                logger.debug(f"The listing of {dsi.name} was refused ({e}); walking it around the copies")
                listed = self._walk_sources(dsi, tops)
        except Exception as e:
            raise RuntimeError(f"cannot list the datasets of {dsi.name}: {e}") from e
        prefix = dsi.name + '/'
        own: Optional[Dataset] = None
        below: List[Dataset] = []
        for item in listed:
            name = item.name
            if name == dsi.name:
                own = item
            elif not name.startswith(prefix):
                raise RuntimeError(f"the listing of {dsi.name} contains {name}, which is outside it")
            elif not any(name == t or name.startswith(t + '/') for t in tops):
                below.append(item)
        if own is None:
            raise RuntimeError(f"the listing of {dsi.name} does not contain {dsi.name} itself")
        return [own] + below

    @staticmethod
    def _walk_sources(dsi: DatasetInfo, tops: Dict[str, str]) -> List[Dataset]:
        """The datasets of `dsi` outside every top of `tops`, listed without
        ever listing a top (`_snapshot_sources`). Each listing is strict, so a
        name libzfseasy cannot represent anywhere outside the copies raises.
        Raises whatever a listing raises."""
        def inside(name: str) -> bool:
            return any(name == t or name.startswith(t + '/') for t in tops)

        def above(name: str) -> bool:
            return any(t.startswith(name + '/') for t in tops)

        found: List[Dataset] = []

        def visit(node: str) -> None:
            for item in zfs.list(
                roots=Dataset(node), types='filesystem,volume', depth=1, strict=True,
            ):
                if item.name == node:
                    found.append(item)
                elif not item.name.startswith(node + '/'):
                    raise RuntimeError(f"the listing of {node} contains {item.name}, which is outside it")
                elif inside(item.name):
                    continue
                elif above(item.name):
                    visit(item.name)
                else:
                    found.extend(zfs.list(
                        roots=item, types='filesystem,volume', recursive=True, strict=True,
                    ))

        visit(dsi.name)
        return found

    def _descendant_anchor_protection(self, dsi: DatasetInfo) -> Dict[str, str]:
        """For a RECURSIVE dataset: `{snapshot name: reason}` pinned by any
        `org.zfsbackup:anchor.*` or `inflight.*` (local or received) on a descendant, read in
        one `zfs get -r`. The parent's recursive destroy would otherwise take
        `@snap` off every descendant, including one a descendant's destination
        still needs as its incremental base. Raises if the read fails."""
        protected: Dict[str, str] = {}
        by_child = read_descendant_zfsbackup_properties(dsi.dataset)
        for child in sorted(by_child):
            for prop, value in sorted(by_child[child].items()):
                if prop.startswith(PROP_ANCHOR_PREFIX):
                    names = [value] if value and value != '-' else []
                elif prop.startswith(PROP_INFLIGHT_PREFIX):
                    names = _inflight_names(value)
                else:
                    continue
                for name in names:
                    protected.setdefault(name, f"{prop} on {child}")
        return protected

    def _descendant_retention_protection(self, dsi: DatasetInfo) -> Dict[str, str]:
        """For a RECURSIVE dataset: `{snapshot name: reason}` that the
        retention of an enabled, configured descendant (another dataset of
        this manager below `dsi`) keeps. A disabled descendant is not in
        `self.datasets`, so only its anchors count. Raises if a descendant's
        snapshots cannot be listed."""
        protected: Dict[str, str] = {}
        prefix = dsi.name + '/'
        for child in self.datasets:
            if child is dsi or not child.name.startswith(prefix):
                continue
            # A scratch DatasetInfo: listing must not replace the snapshot
            # list of the manager's own object for `child`.
            scratch = DatasetInfo(child.dataset, child.config)
            if not zfs.exists(child.dataset):
                # A configured but nonexistent descendant has no snapshots to
                # lose; a real list error on an existing one still raises.
                logger.debug(f"Descendant {child.name} of {dsi.name} does not exist; ignoring")
                continue
            snaps = self._list_snapshots_strict(scratch)
            pruned = {id(sni) for sni in self.needs_prunning(scratch)}
            for sni in snaps:
                if id(sni) not in pruned:
                    protected.setdefault(sni.name, f"retention of {child.name} keeps it")
        return protected

    def copy_tops(self, config: DatasetConfig) -> Mapping[str, str]:
        """The topmost backup copies at or below the dataset `config.name`,
        `{top: why}`, shallowest first (the public form of `_copy_tops`, for
        the CLI: it builds the `DatasetInfo` itself). Read-only (one
        recursive value-free `zfs get`, plus one `zfs.exists` per candidate
        receive target) and untimed here: wrap the call in
        `libzfseasy.timeout`. The result carries two attributes (read them
        with `getattr`): `.inside` is `(origin, source)` when the dataset
        ITSELF has or inherits a copy marker (else `None`), and `.covered`
        the frozenset of tops that exist only because of that (every child
        inherits the marker); list the tops outside `.covered`. Raises on any
        read failure (a `RuntimeError` naming the dataset, a
        `ZFSTimeoutError` unchanged): never a partial answer. The caller
        checks that the dataset exists first."""
        return self._copy_tops(DatasetInfo(Dataset(config.name), config))

    def _copy_tops(self, dsi: DatasetInfo) -> Dict[str, str]:
        """The TOPMOST backup copies below the recursive `dsi`, as `{dataset:
        why}` (`why` is a clause for the ERROR), shallowest first. The result
        is a `_CopyTops`: it also says whether `dsi` itself sits inside a copy
        (`inside`, `covered`). Two sources:

        * a dataset STRICTLY below `dsi` with a local, received or inherited
          `org.zfsbackup:client_id`/`source_dataset` (`_copy_roots`: one
          value-free recursive `zfs get`; an inherited marker covers the whole
          subtree, so the topmost one is enough). `dsi` itself is not "a copy
          below it": if it is inside one, every child inherits the marker and
          counts;
        * every name at or below one of `_receive_targets()` that is strictly
          below `dsi` (the target itself is then the top). It needs no `zfs`
          call, matches `expand_recursive`, and covers a copy without markers
          under a known target (a `send -R` replica, a copy whose properties
          were cleared). A target that does not exist yet holds nothing and is
          dropped (one `zfs.exists` per such candidate). If `dsi` itself is at
          or below a target (a configuration `validate_config` refuses),
          everything below it is a copy and `dsi` is the top.

        The limit that remains: a copy with no markers that is not under a
        known target is indistinguishable from a source child, and is
        treated as one. Raises (a `RuntimeError` naming `dsi`; a
        `ZFSTimeoutError` unchanged)."""
        prefix = dsi.name + '/'
        why: Dict[str, str] = {}
        from_target: Set[str] = set()
        inside: Optional[Tuple[str, str]] = None
        at_target = self._receive_target_of(dsi.name)
        if at_target is not None:
            # Config only: `dsi` is the top and everything below it is a copy,
            # so the marker read (a zfs call that could fail) is not needed.
            found: Dict[str, str] = {}
            why[dsi.name] = at_target[1]
            from_target.add(dsi.name)
        else:
            found = self._copy_roots(dsi.name)
            for name, source in found.items():
                if name.startswith(prefix):
                    why[name] = f"it has {PROP_CLIENT_ID} or {PROP_SOURCE_DATASET}, source: {source}"
            # `dsi` itself has or inherits a marker: every child inherits it.
            own = found.get(dsi.name)
            if own is not None:
                if own.startswith('inherited'):
                    inside = (own[len('inherited from '):] or 'an ancestor', own)
                else:
                    inside = (dsi.name, own)
            for target in self._receive_targets():
                if target.startswith(prefix) and target not in why:
                    why[target] = f"it is the receive target {target}"
                    from_target.add(target)
        tops: List[str] = []
        # Sorted, so an ancestor always precedes its descendants.
        for name in sorted(why):
            if not any(name.startswith(t + '/') for t in tops):
                tops.append(name)
        result: Dict[str, str] = {}
        for top in tops:
            if top in from_target:
                try:
                    present = zfs.exists(Dataset(top))
                except ZFSTimeoutError:
                    raise
                except Exception as e:
                    raise RuntimeError(
                        f"cannot check whether {top}, which receives copies, exists below "
                        f"{dsi.name}: {e}"
                    ) from e
                if not present:
                    continue
            result[top] = why[top]
        tops = _CopyTops(result)
        tops.inside = inside
        # A top is covered when its marker is INHERITED from an origin that is
        # `dsi` itself or above it (so `dsi` has or inherits the marker and
        # the child is a copy only by that inheritance). Not "the same source
        # string as `dsi`'s": `_copy_roots` keeps one source per dataset, and
        # a `dsi` with a LOCAL `source_dataset` and an INHERITED `client_id`
        # has children whose rows both read `inherited from ...`.
        tops.covered = frozenset(
            top for top in result
            if inside is not None and top not in from_target
            and self._inherited_from_at_or_above(found.get(top), dsi.name)
        )
        return tops

    @staticmethod
    def _inherited_from_at_or_above(source: Optional[str], name: str) -> bool:
        """True if `source` (`inherited from <origin>`, as `_copy_roots` keeps
        it) names an origin that is `name` itself or one of its ancestors:
        the marker comes through `name`, not from a dataset below it."""
        if source is None or not source.startswith('inherited from '):
            return False
        origin = source[len('inherited from '):]
        return bool(origin) and (name == origin or name.startswith(origin + '/'))

    def _receive_target_of(self, name: str) -> Optional[Tuple[str, str]]:
        """`(target, why)` if the dataset `name` is at or below one of
        `_receive_targets()` (everything there is a copy; `validate_config`
        refuses such a configuration), else `None`. Config only: no zfs call."""
        for target in self._receive_targets():
            if name == target or name.startswith(target + '/'):
                return target, (
                    f"{name} is itself at or below the receive target {target}, so "
                    "everything below it is a copy"
                )
        return None

    def _copy_protection(self, dsi: DatasetInfo) -> Dict[str, str]:
        """For a RECURSIVE dataset: `{snapshot name: reason}` for every name
        held by a BACKUP COPY below it. `zfs destroy -r ds@snap` also destroys
        `@snap` in every descendant, a copy included, and a copy carries the
        very names its source has (the copies of a `zfs:` destination that
        was removed from the config, a `send -R` replica of a server's tree,
        another host's copy with the same prefix). Those names are therefore
        never destroyed recursively. Source children are still pruned, but
        only for a name the copy does NOT have. The snapshot worker no longer
        snapshots into a copy (`_snapshot_tree`, A2c), but names a copy got
        from earlier versions or from the fallback to `snapshot -r` stay held
        back until it is moved out. (Chosen over "no recursive destroy while a copy exists": that
        would let the whole tree grow for ever, and needs the same copy read;
        the extra listing below is strict, so the protect-set is complete or
        the pass does not act.)

        The copies are `_copy_tops`: markers (local, received or inherited
        `client_id`/`source_dataset`) and everything strictly below `dsi`
        that is at or below a receive target. Remaining limit: a copy with no
        markers that is not under a known target is indistinguishable from a
        source child. One recursive `zfs list -t snapshot` per TOPMOST copy
        root. Logs one ERROR per (`dsi`, copy root) episode naming the copy
        and the remedy, one INFO when it is gone; when `dsi` ITSELF has or
        inherits a copy marker (every child is then a copy by inheritance) one
        ERROR per (`dsi`, origin of the marker) instead of one per child. Raises (`zfs` failure, an
        untrusted listing: a `RuntimeError` naming the copy, a
        `ZFSTimeoutError` unchanged) so that the caller does not prune."""
        tops = self._copy_tops(dsi)
        covered = getattr(tops, 'covered', frozenset())
        prefix = dsi.name + '/'
        protected: Dict[str, str] = {}
        for top in tops:
            try:
                listed = list(zfs.list(
                    roots=Dataset(top), types='snapshot', recursive=True, strict=True,
                ))
            except ZFSTimeoutError:
                raise
            except Exception as e:
                raise RuntimeError(
                    f"cannot list the snapshots of the backup copy {top} below {dsi.name}: {e}"
                ) from e
            for snap in listed:
                # (`dsi` itself can be a top only when it is inside a receive
                # target: its own snapshots are not the copy's.)
                if isinstance(snap, ZFSSnapshot) and snap.name.partition('@')[0].startswith(prefix):
                    protected.setdefault(snap.short, f"it is also held by the backup copy {snap.name}")
        for top, why in tops.items():
            if top in covered:
                continue
            key = (dsi.name, top)
            if key in self._copy_protect_noted:
                continue
            self._copy_protect_noted.add(key)
            logger.error(
                f"{top} is a backup copy ({why}) below the recursive dataset {dsi.name}: a "
                f"recursive destroy of {dsi.name}@<snapshot> would destroy the copy's snapshot of "
                f"that name, so every snapshot name the copy holds is kept in {dsi.name} until "
                f"the copy is gone. Move it out of {dsi.name} (zfs rename {top} <a name outside "
                f"{dsi.name}>) or make {dsi.name} non-recursive. A zfs snapshot -r of {dsi.name} "
                "(older versions; the snapshot worker's fallback when it cannot read the copies) "
                "also snapshots into the copy, and on a live replica that stops its incremental "
                f"receives. Such a stray has the SAME createtxg as {dsi.name}@<name> (check: zfs "
                f"get -H -o name,value createtxg {dsi.name}@<name>; zfs get -r -t snapshot -H -o "
                f"name,value createtxg {top}, which covers every dataset below it); every other "
                "snapshot in the copy is received backup history, which must never be destroyed"
            )
        for key in [k for k in self._copy_protect_noted if k[0] == dsi.name and k[1] not in tops]:
            self._copy_protect_noted.discard(key)
            logger.info(f"{key[1]}, a backup copy below {dsi.name}, is gone; recursive pruning of "
                        f"{dsi.name} is no longer held back by it")
        # Covered, not gone: the `dsi`-level episode below speaks for it.
        self._copy_protect_noted.difference_update(
            k for k in list(self._copy_protect_noted) if k[0] == dsi.name and k[1] in covered
        )
        self._inside_episode(
            dsi, tops, self._copy_protect_inside_noted,
            f"a recursive destroy of {dsi.name}@<snapshot> would destroy that name in every copy "
            f"below it, so every snapshot name they hold is kept in {dsi.name}",
        )
        return protected

    def prune_snapshots(self, dsi: DatasetInfo) -> PruneResult:
        """Prune snapshots according to retention policy, skipping anchored
        snapshots. -> what the pass did (`PruneResult`); callers that only
        want the side effect may ignore it.

        A SOURCE dataset is pruned untimed, as always. A LOCAL COPY
        (`dsi.local_target`) runs every `zfs` command of its pass -- the
        listing, the protection re-reads, the destroys -- under
        `libzfseasy.timeout` (`LOCAL_PRUNE_COMMAND_BUDGET`, destroys
        `LOCAL_PRUNE_DESTROY_BUDGET`), because the target can be on a pool that
        suspends (`failmode=wait`): discovery may still answer while `zfs
        destroy` sits in D state. A `ZFSTimeoutError` (a command that did not
        finish, or the stuck-children fail-fast) ends that copy's pass with a
        `skipped` result, skips the rest of the same target's copies for the
        cycle (no `zfs` call; `local_copies` re-arms it) and is announced at
        WARNING once per episode and failure class. It never enters the B-4
        one-at-a-time retry or `_prune_stuck`: the destroy's outcome is
        unknown and retrying a hung pool snapshot by snapshot would only hang
        again. Untimed calls (the source datasets') are never refused by the
        stuck-children registry (`libzfseasy._admit_timed` runs only for a
        call under a timeout), so a stuck local destroy does not stop them.

        An abandoned `zfs destroy` (it survived SIGKILL) is not waited for
        when the worker leaves for a reload or an upgrade, unlike the API
        worker's receive (`ApiWorker._wait_for_stuck_children`): the
        successor's duplicate destroy of the same snapshot just gets "does not
        exist" (or fails busy); the late destroy only carries out a decision
        that was already allowed by the retention pass and by the protection
        re-read made right before it (the newest snapshot of a copy -- the
        only one that can be the next incremental base -- is never a
        candidate), exactly like a destroy in flight when a worker is killed;
        and a successor that finds the pool still suspended times out on its
        first command and does nothing more -- whereas two overlapping
        receives would corrupt the stream."""
        target = dsi.local_target
        if target is None:
            return self._prune_snapshots(dsi)
        blocked = self._local_prune_blocked.get(target)
        if blocked is not None:
            logger.debug("Not pruning %s: %s", dsi.name, blocked)
            return PruneResult(skipped=blocked)
        self._prune_timed = True
        try:
            with zfs.timeout(LOCAL_PRUNE_COMMAND_BUDGET):
                result = self._prune_snapshots(dsi)
        except ZFSTimeoutError as e:
            return self._local_prune_timed_out(dsi, target, e)
        finally:
            self._prune_timed = False
        if self._local_prune_noted.pop(target, None) is not None:
            logger.info("The local copies under %s can be pruned again", target)
        return result

    def _local_prune_timed_out(self, dsi: DatasetInfo, target: str, failure: ZFSTimeoutError) -> PruneResult:
        """A zfs command of a local copy's pass timed out (or was refused as
        the registry holds a stuck child): block the rest of `target`'s copies
        for this cycle and announce the episode (WARNING once per failure
        class, DEBUG after)."""
        kind = _failure_kind(failure)
        text = _describe_failure(failure)
        reason = (
            f"the local target {target} did not answer while pruning {dsi.name} ({text}); "
            "its other copies are skipped this cycle"
        )
        self._local_prune_blocked[target] = f"the local target {target} did not answer earlier in this cycle"
        if self._local_prune_noted.get(target) != kind:
            self._local_prune_noted[target] = kind
            logger.warning("Not pruning the local copies under %s: %s", target, reason)
        else:
            logger.debug("Still not pruning the local copies under %s: %s", target, reason)
        return PruneResult(skipped=reason)

    def _prune_snapshots(self, dsi: DatasetInfo) -> PruneResult:
        self._reread_error = None
        self._reread_logged = set()
        snapshots = self.list_snapshots(dsi, recursive=False)
        if not snapshots:
            if self.last_list_error is not None:
                if self._prune_timed and isinstance(self._last_list_failure, ZFSTimeoutError):
                    raise self._last_list_failure
                return PruneResult(skipped=f"cannot list snapshots: {self.last_list_error}")
            logger.debug(f"No snapshots to prune for {dsi.name}")
            self._forget_stuck(dsi, set())
            return PruneResult()

        try:
            anchors = self.get_anchors(dsi)
            desc_protected = (
                self._descendant_anchor_protection(dsi) if dsi.recursive else {}
            )
        except Exception as e:
            if self._prune_timed and isinstance(e, ZFSTimeoutError):
                raise
            # Unknown protect-set: pruning blind could destroy an incremental base.
            msg = f"Cannot read anchors for {dsi.name}; skipping pruning this cycle: {e}"
            if isinstance(e, ZFSOutputError):
                msg += (
                    f". An org.zfsbackup:* property set on {dsi.name} (or, for a recursive "
                    "dataset, on a descendant) holds a value zfs printed in a way that cannot "
                    "be framed (a newline), or changed while it was being "
                    "read (the next cycle reads it again). Unrelated properties do not "
                    "matter, and an inherited value matters only if a descendant sets the "
                    "same property itself (or, for a configured destination, its anchor or "
                    "in-flight name). The dataset or property is named above. Find where it "
                    "is set with `zfs get <property> <dataset>`, then "
                    "`zfs inherit <property> <dataset>` or `zfs set <property>=<one line> "
                    "<dataset>` on the dataset whose SOURCE column says so (`zfs inherit` "
                    "hides a received value). Pruning of this dataset resumes by itself."
                )
            if dsi.name in self._anchor_error_noted:
                logger.debug(msg)
            else:
                self._anchor_error_noted.add(dsi.name)
                logger.error(msg)
            return PruneResult(skipped=f"cannot read the anchors: {e}")
        if dsi.name in self._anchor_error_noted:
            self._anchor_error_noted.discard(dsi.name)
            logger.info(f"The anchors of {dsi.name} can be read again; pruning resumes")
        to_prune = self.needs_prunning(dsi, anchors=anchors)
        if dsi.recursive and to_prune:
            # `zfs destroy -r ds@snap` also destroys `@snap` on every
            # descendant, so a name a descendant protects must not be
            # destroyed at all.
            try:
                for name, reason in self._descendant_retention_protection(dsi).items():
                    desc_protected.setdefault(name, reason)
                for name, reason in self._copy_protection(dsi).items():
                    desc_protected.setdefault(name, reason)
            except Exception as e:
                msg = f"Cannot evaluate descendants of {dsi.name}; skipping pruning this cycle: {e}"
                if dsi.name in self._descendant_error_noted:
                    logger.debug(msg)
                else:
                    self._descendant_error_noted.add(dsi.name)
                    logger.error(msg)
                return PruneResult(skipped=f"cannot evaluate the descendants: {e}")
            if dsi.name in self._descendant_error_noted:
                self._descendant_error_noted.discard(dsi.name)
                logger.info(
                    f"The descendants of {dsi.name} can be evaluated again; pruning resumes"
                )
            all_candidates = to_prune
            kept = []
            for sni in to_prune:
                reason = desc_protected.get(sni.name)
                if reason is None:
                    kept.append(sni)
                    continue
                if (dsi.name, sni.name) not in self._descendant_protect_noted:
                    self._descendant_protect_noted.add((dsi.name, sni.name))
                    logger.info(
                        f"Dataset {dsi.name}: keeping {sni.full_name} (recursive prune): "
                        f"{reason}"
                    )
            to_prune = kept
            # Forget names no longer held back (released, pruned or gone) so
            # the set stays bounded; a name held again is announced again.
            held_now = {(dsi.name, sni.name) for sni in all_candidates
                        if sni.name in desc_protected}
            self._descendant_protect_noted = {
                key for key in self._descendant_protect_noted
                if key[0] != dsi.name or key in held_now
            }
        else:
            # Nothing is (or can be) held back: drop this dataset's entries.
            self._descendant_protect_noted = {
                key for key in self._descendant_protect_noted if key[0] != dsi.name
            }
            if not dsi.recursive:
                # (A recursive dataset with nothing to prune did not read the
                # copies this pass: its episode memory stays.)
                self._copy_protect_noted = {
                    key for key in self._copy_protect_noted if key[0] != dsi.name
                }
            if dsi.name in self._descendant_error_noted:
                self._descendant_error_noted.discard(dsi.name)
                logger.info(
                    f"Nothing is due for pruning in {dsi.name}, so its descendants are not "
                    "evaluated this cycle; pruning resumes when something is due"
                )
        # A name that is no longer a candidate (destroyed, gone, now kept)
        # leaves the stuck memory.
        self._forget_stuck(dsi, {sni.name for sni in to_prune})
        to_prune, retry_alone = self._split_stuck(dsi, to_prune)
        logger.info(
            f"Dataset {dsi.name} has {len(to_prune) + len(retry_alone)} snapshots to prune"
        )

        batch_size = 20
        pruned = 0
        for i in range(0, len(to_prune), batch_size):
            batch = to_prune[i:i + batch_size]
            names = ', '.join(s.full_name for s in batch)
            logger.info(f"Pruning snapshots (batch {i // batch_size + 1}): {names}")
            if self.config.dry_run:
                logger.info(f"[DRY RUN] Would prune snapshots: {names}")
                continue
            batch = self._still_prunable(dsi, batch)
            if not batch:
                continue
            try:
                with self._destroy_scope(dsi):
                    zfs.destroy([s.snapshot for s in batch], destroy=True, recursive=dsi.recursive)
                pruned += len(batch)
            except Exception as e:
                if self._prune_timed and isinstance(e, ZFSTimeoutError):
                    raise
                # One bad member fails the whole call and destroys none of
                # them. Retry each alone so the others still go.
                logger.error(f"Failed to prune snapshot batch: {e}; retrying its snapshots one at a time")
                retry_alone = retry_alone + batch
        # Stuck names (and the members of a failed batch) are tried alone.
        failed: List['SnapshotInfo'] = []
        for sni in retry_alone:
            if self.config.dry_run:
                logger.info(f"[DRY RUN] Would retry pruning {sni.full_name} on its own")
                continue
            if not self._still_prunable(dsi, [sni]):
                continue
            try:
                with self._destroy_scope(dsi):
                    zfs.destroy([sni.snapshot], destroy=True, recursive=dsi.recursive)
            except Exception as e:
                if self._prune_timed and isinstance(e, ZFSTimeoutError):
                    raise
                logger.debug(f"Could not prune {sni.full_name}: {e}")
                failed.append(sni)
                continue
            pruned += 1
            if self._prune_stuck.pop((dsi.name, sni.name), None) is not None:
                logger.info(f"Pruned {sni.full_name}, which could not be destroyed earlier")
        if failed:
            self._mark_stuck_after_failure(dsi, failed)
        if pruned:
            logger.info(f"Successfully pruned {pruned} snapshots")
        return PruneResult(
            pruned=pruned,
            failed=len(failed),
            stuck=sum(1 for key in self._prune_stuck if key[0] == dsi.name),
            skipped=(
                f"cannot re-read the anchors before destroying: {self._reread_error}"
                if self._reread_error is not None else None
            ),
        )

    # ── Prune fallback and stuck-snapshot memory (B4) ─────────────────────────

    def _destroy_scope(self, dsi: DatasetInfo) -> 'contextlib.AbstractContextManager':
        """The scope of one `zfs destroy`: the longer destroy budget for a
        local copy (innermost `timeout` wins), nothing for a source dataset."""
        if dsi.local_target is None:
            return _no_scope()
        return zfs.timeout(LOCAL_PRUNE_DESTROY_BUDGET)

    def _still_prunable(self, dsi: DatasetInfo, group: List['SnapshotInfo']) -> List['SnapshotInfo']:
        """`group` minus every snapshot that has become protected since the
        pass began, read IMMEDIATELY before a destroy call (a batch, or one
        snapshot of the fallback): the anchors and in-flight pins
        (`get_anchors`, local and received) and, for a recursive dataset, the
        descendants' anchors and pins and the snapshot names held by backup copies
        below it. The pass's first read goes stale: the
        remote worker pins a pair, sends, writes the anchor and re-pins the
        next pair while this pass retries a failed batch (a send in progress
        makes a destroy fail with EBUSY, which is exactly the fallback's
        case), and a destroy using the old protect-set would take the snapshot
        just sent -- the next incremental base. The remote side is gap-free
        (pin before the send, anchor before the pin is cleared), so a fresh
        read always sees a snapshot in use; one destroyed before its step
        starts is a vanished chain member, which the sender rebuilds around.
        Costs, per destroy call, two execs for the dataset's own properties
        (discovery, then the values; the second is the last `zfs` call made
        before the destroy; it also asks for `anchor.<d>`/`inflight.<d>` of
        every configured destination, so a pin the remote worker creates
        between the two execs is seen -- a pin of an UNCONFIGURED destination
        created in that window is not, the same race as any set after the
        read), one per configured destination (`get_anchor`)
        and, for a recursive dataset, two recursive `zfs get`s (one when no
        descendant has a property of ours), one more recursive value-free `zfs
        get` for the backup copies below it (`_copy_protection`), one
        `zfs list -t snapshot -r` per topmost copy root and one existence
        check per marker-less receive target below it (`_copy_tops`); no listing of the
        dataset's own snapshots. A re-read after a property changed between the two execs
        adds two more. If the read fails nothing is
        destroyed (-> `[]`). (A `zfs hold` on the in-flight pair would be the
        airtight alternative; it is deferred because holds left by a crash
        would need cleaning up.)"""
        if not group:
            return []
        try:
            # Order matters: the descendants' read first, then the
            # per-destination anchors, and the dataset's own properties (where
            # an in-flight pin shows) LAST, so that a whole chain step cannot
            # fit between the read and the destroy but for the destroy's own
            # start-up. The sender additionally keeps its pin between steps.
            protected: Set[str] = set()
            if dsi.recursive:
                # A backup copy that appeared since the decision (or a
                # snapshot it received since) is seen here, before the
                # destroy. First: it is independent of the pins below, so
                # the time-critical reads keep their order.
                protected |= set(self._copy_protection(dsi))
                protected |= set(self._descendant_anchor_protection(dsi))
            protected |= set(self.get_anchors(dsi, own_properties_last=True))
        except Exception as e:
            if self._prune_timed and isinstance(e, ZFSTimeoutError):
                raise
            self._reread_error = str(e)
            msg = (
                f"Cannot re-read what protects the snapshots of {dsi.name} (its anchors and "
                f"in-flight pins and, for a recursive dataset, the backup copies below it); "
                f"not destroying {', '.join(s.full_name for s in group)} now: {e}"
            )
            # ERROR the first time in a pass; the batch's one-at-a-time retry
            # and the next batches would only repeat it.
            if self._pass_logged_once((dsi.name, None)):
                logger.error(msg)
            else:
                logger.debug(msg)
            return []
        kept = [s for s in group if s.name not in protected]
        for sni in group:
            if sni.name in protected:
                msg = (
                    f"Dataset {dsi.name}: not pruning {sni.full_name}: it became protected "
                    "(an anchor, an in-flight pin, or a backup copy below it) during this pass"
                )
                if self._pass_logged_once((dsi.name, sni.name)):
                    logger.info(msg)
                else:
                    logger.debug(msg)
        return kept

    def _pass_logged_once(self, key: Tuple[str, Optional[str]]) -> bool:
        """True the first time `key` is seen in this pruning pass (the set is
        reset by `_prune_snapshots`), so a re-read message is logged once per
        dataset (and snapshot) per pass and repeats go to DEBUG."""
        if key in self._reread_logged:
            return False
        self._reread_logged.add(key)
        return True

    @staticmethod
    def _busy_reason(sni: 'SnapshotInfo') -> Optional[str]:
        """Why a destroy of `sni` cannot succeed, from its listed `userrefs`
        and `clones` (never from stderr: "busy" is ambiguous), or `None`."""
        if sni.userrefs:
            return (
                f"it has {sni.userrefs} user hold(s) (see: zfs holds {shlex.quote(sni.full_name)})"
            )
        if sni.clones:
            return (
                f"it has clone(s) {', '.join(sni.clones)} "
                "(destroy or promote them first)"
            )
        return None

    def _note_stuck(self, dsi: DatasetInfo, sni: 'SnapshotInfo', reason: str) -> None:
        """Record `sni` as unprunable: WARNING once per stuck episode, DEBUG
        on repeats."""
        key = (dsi.name, sni.name)
        new = key not in self._prune_stuck
        self._prune_stuck[key] = reason
        if new:
            logger.warning(
                f"Dataset {dsi.name}: not pruning {sni.full_name}: {reason}; "
                "it is tried again alone every cycle"
            )
        else:
            logger.debug(f"Dataset {dsi.name}: still not pruning {sni.full_name}: {reason}")

    def _forget_stuck(self, dsi: DatasetInfo, candidates: Set[str]) -> None:
        """Drop this dataset's stuck entries whose snapshot is not among
        `candidates` (names): destroyed, gone, or no longer due for pruning."""
        for key in [k for k in self._prune_stuck if k[0] == dsi.name and k[1] not in candidates]:
            del self._prune_stuck[key]
            logger.debug(f"Dataset {dsi.name}: {key[1]} is no longer a pruning candidate")

    def _split_stuck(
        self, dsi: DatasetInfo, to_prune: List['SnapshotInfo']
    ) -> Tuple[List['SnapshotInfo'], List['SnapshotInfo']]:
        """-> `(batchable, retry_alone)`. Held and cloned candidates are
        removed up front (also in a dry run: a cloned snapshot fails even
        `destroy -n`); candidates already stuck are tried alone and never go
        back into a batch."""
        batchable: List[SnapshotInfo] = []
        alone: List[SnapshotInfo] = []
        for sni in to_prune:
            reason = self._busy_reason(sni)
            if reason is not None:
                self._note_stuck(dsi, sni, reason)
            elif (dsi.name, sni.name) in self._prune_stuck:
                alone.append(sni)
            else:
                batchable.append(sni)
        return batchable, alone

    def _mark_stuck_after_failure(self, dsi: DatasetInfo, failed: List['SnapshotInfo']) -> None:
        """Single-snapshot destroys failed. Re-list once for the reason (a hold
        or clone that appeared after the first listing) and remember each
        name as stuck. A snapshot no longer listed is just gone."""
        fresh: Dict[str, 'SnapshotInfo'] = {}
        listed = False
        try:
            scratch = DatasetInfo(dsi.dataset, dsi.config)
            fresh = {s.name: s for s in self._list_snapshots_strict(scratch)}
            listed = True
        except Exception as e:
            if self._prune_timed and isinstance(e, ZFSTimeoutError):
                raise
            logger.debug(f"Could not re-list {dsi.name} to explain a failed destroy: {e}")
        for sni in failed:
            current = fresh.get(sni.name)
            if listed and current is None:
                self._prune_stuck.pop((dsi.name, sni.name), None)
                logger.debug(f"{sni.full_name} disappeared while pruning")
                continue
            reason = self._busy_reason(current) if current is not None else None
            if reason is None:
                reason = (
                    "zfs would not destroy it (a recursive destroy also needs every descendant's "
                    "copy free of holds and clones; a send in progress also blocks it)"
                    if dsi.recursive else
                    "zfs would not destroy it (a send in progress also blocks it)"
                )
            self._note_stuck(dsi, sni, reason)


_EPOCH = datetime.min.replace(tzinfo=timezone.utc)


def snapshot_order_key(sni: 'SnapshotInfo'):
    """Total order of a dataset's managed snapshots, oldest smallest: by
    `createtxg`, then by name timestamp, then by name. A snapshot without a
    txg (never in a listed result; only a hand-built object) sorts as the
    oldest."""
    return (
        sni.createtxg if sni.createtxg is not None else -1,
        sni.timestamp if sni.timestamp is not None else _EPOCH,
        sni.name,
    )


def snapshot_newer(a: Optional['SnapshotInfo'], b: Optional['SnapshotInfo']) -> bool:
    """`a` is strictly newer than `b` by `snapshot_order_key` (`createtxg`
    first). False when either is missing or has no `createtxg`: an unknown
    order is never taken for "newer"."""
    return (
        a is not None and b is not None
        and a.createtxg is not None and b.createtxg is not None
        and snapshot_order_key(a) > snapshot_order_key(b)
    )


def _property_text(snapshot, name: str) -> Optional[str]:
    """The text of one property of a listed libzfseasy object, or `None` if
    it is absent, `-` or empty. Never raises."""
    try:
        prop = snapshot[name]
        if prop is None:
            return None
        value = getattr(prop, 'value', prop)
        if value is None:
            return None
        text = str(value)
    except Exception:
        return None
    return None if text in ('', '-') else text


def _property_int(snapshot, name: str) -> Optional[int]:
    text = _property_text(snapshot, name)
    if text is None:
        return None
    try:
        return int(text, 0)
    except ValueError:
        return None


#: The public spelling of `_property_int` (`api.py` reads server snapshots with it).
property_int = _property_int


def parse_snapshot_timestamp(prefix: str, name: str) -> Optional[datetime]:
    """`{prefix}_{YYYYMMDDHHMMSS}` -> UTC datetime, else None. The whole
    name must match: the prefix (which may itself contain `_`), `_`, and
    exactly 14 ASCII digits that form a valid date. Anything after the
    digits (`..._suffix`) is not ours. A name is managed exactly when this
    is not None."""
    m = re.fullmatch(re.escape(prefix) + '_([0-9]{14})', name)
    if m is None:
        return None
    try:
        dt = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc)


class SnapshotInfo:
    """Information about a snapshot with parsed metadata.

    `createtxg`, `guid`, `userrefs`, `clones`, `encrypted` and `key_loaded`
    come from the one listing (`SNAPSHOT_LIST_PROPERTIES`) and are `None` (or
    empty) when the object was not listed with them. `createtxg` is what
    orders snapshots; `timestamp` (from the name) only places a snapshot in a
    retention slot and gives its age."""

    def __init__(self, snapshot: ZFSSnapshot, prefix: str):
        self.snapshot = snapshot
        self.name = snapshot.short
        self.full_name = str(snapshot)
        self.prefix = prefix
        self.timestamp = self._parse_timestamp()
        self.createtxg: Optional[int] = _property_int(snapshot, 'createtxg')
        self.guid: Optional[int] = _property_int(snapshot, 'guid')
        self.userrefs: Optional[int] = _property_int(snapshot, 'userrefs')
        clones = _property_text(snapshot, 'clones')
        self.clones: Tuple[str, ...] = tuple(c for c in clones.split(',') if c) if clones else ()
        encryption = _property_text(snapshot, 'encryption')
        self.encrypted: Optional[bool] = None if encryption is None else encryption != 'off'
        keystatus = _property_text(snapshot, 'keystatus')
        self.key_loaded: Optional[bool] = None if keystatus is None else keystatus == 'available'

    def _parse_timestamp(self) -> Optional[datetime]:
        """`{prefix}_{YYYYMMDDHHMMSS}` -> UTC datetime, else None
        (`parse_snapshot_timestamp`)."""
        return parse_snapshot_timestamp(self.prefix, self.name)

    @property
    def age(self) -> timedelta:
        if self.timestamp is None:
            return timedelta(0)
        return datetime.now(timezone.utc) - self.timestamp

    @property
    def is_managed(self) -> bool:
        return self.timestamp is not None

    def __repr__(self):
        return f"SnapshotInfo({self.full_name}, age={self.age})"

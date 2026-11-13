"""The ZFS-reading half of `zfsbackup-config check`: builds the plain
observations that `zfsbackup.health.evaluate` grades.

**Read-only.** Only `zfs list` and `zfs get` run (through `DatasetManager`); no
property is written, no snapshot created or destroyed, no lock taken and no
signal sent. It never touches the config database either: the caller hands it a
`BackupConfig`. It may import `libzfseasy` and `zfsbackup.config`;
`zfsbackup.health` stays stdlib only, which is why the two are separate modules.

Entry point: `collect(config, manager=None, *, deadline=None, per_call=10.0) ->
list`. The result mixes `health.Observation` (a lag to grade) and
`health.CheckEntry` (a verdict already reached: `--dry-run` / `dry_run: true`
is a WARN, so is a part of the check that ran out of time) and goes straight to
`health.evaluate`.

**Bounded.** Every `zfs` read here runs under its own `libzfseasy.timeout`,
entered right before that command starts (a function that runs several
commands enters one scope per command), so a hung pool cannot hang `check`.
`per_call` (seconds, default `DEFAULT_PER_CALL_SECONDS`; `None` = no per-call
limit) bounds each command that reads ONE dataset, at `min(per_call, time
left)`; `deadline` (seconds for the whole collection, measured from the call;
default `None` = unbounded, the caller sets it) bounds the run. The BATCHED
reads, whose run time grows with the size of a tree rather than with the
health of the pool (the recursive listings under a recursive dataset, the
two-step property read of its descendants, the descendant listing and
backup-copy marker read of the expansion itself, and the listing and batched
property read of the received datasets), get the time LEFT of `deadline`
instead of `per_call`: ~60 000 snapshots can take longer than 10 s on a
healthy host. Without a `deadline` they get `per_call`, as everything else.
A read that times out is an ordinary failure: "cannot read" (CRIT) for exactly
what it was reading.
Once the deadline passes, no further read starts and everything not yet
checked is named in ONE WARN `check` entry ("not checked within the time
limit"), never silence. The unit of work is a configured dataset, one recursive
parent's descendants, or one received dataset: a unit that runs out of time
mid-way contributes none of its observations and is named in that entry, so
every reported observation is complete. A command that timed out and could not
be killed (a hung pool) makes every later timed call fail fast (libzfseasy):
the rest of the run then reads as "cannot read", which is the right answer
for a probe. The overshoot of the deadline is at most one command's kill time
(libzfseasy terminates a timed-out child: SIGTERM, then SIGKILL after 2 s,
each waited for at most 2 s, plus at most `_EXPIRY_JOIN` (0.05 s) to join its
stderr reader, so about 4 s): a command starts only if its scope is entered
with time left, and is cut at the deadline, and the next command's scope is
refused. What runs between commands (parsing a listing) is not timed.

What is observed, per enabled dataset (a disabled one is skipped):

* its newest managed snapshot -- by `createtxg`, the daemon's own ordering --
  against the dataset's `frequency`, but never less than `check_interval`: the
  snapshot worker cannot snapshot more often than it checks;
* for each configured destination, the anchor snapshot (the one last sent) against
  `max(dataset frequency, destination frequency, check_interval)`, with a `inflight` pin noted;
  with no anchor, the oldest managed snapshot decides how long that is
  tolerated (`health` module docstring). An anchor that cannot be READ is an
  error observation (CRIT), not "never sent"; a send pin that cannot be read
  does not discard a readable anchor: the anchor is graded normally and the
  detail says "send pin could not be read";
* for each enabled `recursive: true` dataset with destinations, every expanded
  descendant (`DatasetManager.expand_recursive`; backup copies are never among
  them) gets the same anchor observation per destination, against the PARENT's
  expectation (a child's config is the parent's). A descendant has no snapshot
  observation of its own: the parent's covers the `-r` snapshot. A parent whose
  descendants cannot be listed is one WARN `descendants` entry, never silence.
  The descendants are read in BATCHES, O(parents) not O(children), four execs
  per parent: the two-step
  `org.zfsbackup:anchor.*`/`inflight.*` read of
  `read_descendant_zfsbackup_properties` (value-free `property_sources`, then a
  strict explicit-list `get.rows`; read FIRST, so an anchor that moves to a
  snapshot created in between is still found in the listing), one recursive
  snapshot listing, and a name-only listing that drops a child destroyed or
  renamed since the expansion. The snapshot listing skips (with libzfseasy's
  own WARNING) a snapshot or dataset name libzfseasy cannot represent, as the
  daemon does, instead of failing every child of the parent
  (`DatasetManager._list_snapshot_tree_strict`). Only LOCAL values count (a child
  never inherits its parent's progress). A batch that cannot be read makes
  EVERY child of that parent an error observation (CRIT) for each destination,
  never a smaller or wrong answer. A child with no managed snapshot yet
  (created after the parent's last snapshot) is "not yet sent" (WARN, until the
  parent's next slot snapshots it): accepted, not a fault;
* when `remote_backup` is enabled, each received client dataset's newest snapshot
  against the largest frequency in that client's own config. The target is
  listed first, strictly: a target that cannot be listed or does not exist is a
  CRIT `received` entry on the target itself (a discovery that fails is the
  same: it would otherwise hide as an empty list). A received dataset with no
  managed snapshot stays WARN: with none, there is no time reference to turn
  "nothing yet" into CRIT.

Ages come from the snapshot names (as everywhere in the daemon), not from
`creation`. A send whose raw/plain mode disagrees with the server's copy leaves no
marker yet, so it appears only as lag.
"""

import logging
import math
import time
from datetime import timezone
from typing import Callable, Dict, List, Optional, Tuple, Union

import libzfseasy as zfs
from libzfseasy.types import Dataset
from libzfseasy.zfs import ZFSTimeoutError
from zfsbackup.backup_manager import (
    PROP_ANCHOR_PREFIX, PROP_INFLIGHT_PREFIX,
    DatasetInfo, DatasetManager, LocalAnchorUnreadable, SnapshotInfo, _parse_inflight,
    read_descendant_zfsbackup_properties,
)
from zfsbackup.config import BackupConfig
from zfsbackup.health import (
    SOURCE_ANCHOR,
    SOURCE_RECEIVED,
    SOURCE_SNAPSHOT,
    STATE_CRIT,
    STATE_WARN,
    CheckEntry,
    Observation,
)

logger = logging.getLogger(__name__)

__all__ = [
    'collect', 'DEFAULT_PER_CALL_SECONDS', 'SOURCE_CHECK', 'SOURCE_COPIES', 'SOURCE_DESCENDANTS',
    'COPIES_SHOWN_PER_PARENT',
]

#: Subject and source of the dry-run verdict.
_CONFIG_SOURCE = 'config'
_DRY_RUN_SUBJECT = 'dry_run'
#: Source of the entry for a recursive dataset whose descendants cannot be listed.
SOURCE_DESCENDANTS = 'descendants'
#: Source of the entries for backup copies sitting below a recursive dataset.
SOURCE_COPIES = 'copies'
#: How many copies below one recursive parent get an entry each; the rest are
#: counted in one "and N more" entry.
COPIES_SHOWN_PER_PARENT = 20
#: Source of the entry that names what was not checked within the time limit.
SOURCE_CHECK = 'check'
#: Default `per_call` of `collect`: the longest any one `zfs` command may run.
DEFAULT_PER_CALL_SECONDS = 10.0
#: How many names of the unchecked datasets the time-limit entry spells out.
_UNCHECKED_NAMES_SHOWN = 5

_Result = List[Union[Observation, CheckEntry]]


class _OutOfTime(BaseException):
    """The collection's deadline passed. A `BaseException` on purpose: it is
    control flow between this module's own frames (`_Budget` raises it,
    `_run_unit` catches it) and must slip through every `except Exception`
    that turns a failed read into a "cannot read" observation."""


class _Budget:
    """The collection's time limits: `per_call` seconds per `zfs` command,
    `deadline` seconds in all (measured from construction); either may be
    `None` for no limit. Bulk reads (`bulk_scope`, `call_bulk`,
    `call_with_bulk_scope`) get the time left of `deadline` instead of
    `per_call` (`per_call` when there is no `deadline`)."""

    def __init__(self, deadline: Optional[float], per_call: Optional[float]):
        for name, value in (('deadline', deadline), ('per_call', per_call)):
            if value is not None and not (
                isinstance(value, (int, float)) and not isinstance(value, bool)
                and math.isfinite(value) and value > 0
            ):
                raise ValueError(f'{name} must be a positive finite number of seconds or None, got {value!r}')
        self.deadline = deadline
        self.per_call = per_call
        self._end = None if deadline is None else time.monotonic() + deadline

    def left(self) -> Optional[float]:
        return None if self._end is None else self._end - time.monotonic()

    def expired(self) -> bool:
        left = self.left()
        return left is not None and left <= 0

    def _timeout(self, bulk: bool):
        # `left` is read once, here, so the check and the limit agree.
        left = self.left()
        if left is not None and left <= 0:
            raise ZFSTimeoutError(f'the check\'s time limit ({self.deadline:g} s) is spent')
        if bulk and left is not None:
            return zfs.timeout(left)
        limits = [x for x in (self.per_call, left) if x is not None]
        return zfs.timeout(min(limits) if limits else None)

    def scope(self):
        """A `libzfseasy.timeout` of `min(per_call, time left)`, for a manager
        method that takes `scope=` and enters it once per command; once the
        deadline has passed it raises `ZFSTimeoutError`, which such a method
        reports as its own failure."""
        return self._timeout(False)

    def bulk_scope(self):
        """`scope` for a read that grows with the size of a tree: a
        `libzfseasy.timeout` of the time left (`per_call` without a deadline)."""
        return self._timeout(True)

    def _run(self, fn: Callable, args, kwargs, enter: Optional[Callable]):
        """`fn(*args, **kwargs)`, inside `enter()` if given, converting a
        failure at or after the deadline into `_OutOfTime`. `enter()` is inside
        the `try`: a deadline that passes between the check and the scope is
        the limit's, not a finding."""
        if self.expired():
            raise _OutOfTime()
        try:
            if enter is None:
                return fn(*args, **kwargs)
            with enter():
                return fn(*args, **kwargs)
        except Exception as exc:
            if self.expired():
                raise _OutOfTime() from exc
            raise

    def call(self, fn: Callable, *args, **kwargs):
        """`fn(*args, **kwargs)`, ONE `zfs` command, under `scope`. Raises
        `_OutOfTime` when the deadline has passed before the call, or passes
        while it fails (a failure that ends at the deadline is the limit's, not
        a finding)."""
        return self._run(fn, args, kwargs, self.scope)

    def call_bulk(self, fn: Callable, *args, **kwargs):
        """`call` for ONE bulk command, under `bulk_scope`."""
        return self._run(fn, args, kwargs, self.bulk_scope)

    def call_with_bulk_scope(self, fn: Callable, *args, **kwargs):
        """`call` for a function that runs several bulk commands and takes
        `scope=`: it gets `bulk_scope` and enters it once per command, so each
        is timed separately."""
        kwargs['scope'] = self.bulk_scope
        return self._run(fn, args, kwargs, None)


def collect(
    config: BackupConfig,
    manager: Optional[DatasetManager] = None,
    *,
    deadline: Optional[float] = None,
    per_call: Optional[float] = DEFAULT_PER_CALL_SECONDS,
) -> _Result:
    """Observe `config`'s datasets (and, on a server, its received datasets).

    Ages are not computed here: the observations carry snapshot timestamps and
    `health.evaluate` grades them at the caller's `now`. `manager` lets a caller
    (a test) supply the `DatasetManager`; by default one is built over
    `config`. Never raises for a ZFS failure: an unreadable dataset becomes an
    observation with `error` set.

    `deadline`: seconds, from this call, for the whole collection (positive,
    finite; `None` = unbounded). `per_call`: seconds any one `zfs` command may
    run (positive, finite; `None` = unlimited; default
    `DEFAULT_PER_CALL_SECONDS`), except the batched reads (a recursive
    dataset's descendant listing and backup-copy marker read, its
    descendants' snapshot listing, property read and name re-listing, the
    received datasets' listing and property read), which may run for the time
    left of `deadline` (for `per_call` when there is no `deadline`). Each
    command is timed separately, and the deadline is overshot by at most the
    time a timed-out child needs to be killed (about 4 s: 2 s after SIGTERM, 2 s
    after SIGKILL, and at most 0.05 s to join the stderr reader). Both keyword-only; anything else
    raises `ValueError` before a command runs. When the deadline passes, what is not
    yet checked is named in one WARN `CheckEntry` (`source` `check`, subject
    `(time limit)`) at the end of the result; the order of work is the
    configured datasets, then the descendants of the recursive ones, then the
    received datasets."""
    budget = _Budget(deadline, per_call)
    manager = manager if manager is not None else DatasetManager(config)
    result: _Result = []
    unchecked: List[str] = []

    if config.dry_run:
        result.append(CheckEntry(
            source=_CONFIG_SOURCE, subject=_DRY_RUN_SUBJECT, state=STATE_WARN,
            reason='dry_run is set: no snapshot is created or destroyed and nothing is sent',
        ))

    tolerance = manager.clock_tolerance().total_seconds()

    try:
        datasets = manager.datasets
    except Exception as exc:  # building the list reads nothing from ZFS, but never raise
        logger.debug('cannot build the dataset list: %r', exc)
        datasets = []
        result.append(CheckEntry(
            source=SOURCE_SNAPSHOT, subject='(datasets)', state=STATE_CRIT,
            reason=f'cannot read: {exc}',
        ))
    for dsi in datasets:
        _run_unit(result, unchecked, budget, dsi.name,
                  lambda dsi=dsi: _observe_local(manager, dsi, tolerance, budget))
    _observe_descendants(manager, tolerance, budget, result, unchecked)

    rb = config.remote_backup
    if rb is not None and rb.enabled:
        _observe_received(manager, rb.target_dataset.rstrip('/'), tolerance, budget, result, unchecked)

    if unchecked:
        shown = ', '.join(unchecked[:_UNCHECKED_NAMES_SHOWN])
        more = len(unchecked) - _UNCHECKED_NAMES_SHOWN
        result.append(CheckEntry(
            source=SOURCE_CHECK, subject='(time limit)', state=STATE_WARN,
            reason=f'not checked within the time limit ({budget.deadline:g} s): {shown}'
                   + (f' and {more} more' if more > 0 else ''),
        ))
    return result


def _run_unit(
    result: _Result, unchecked: List[str], budget: _Budget, label: str,
    work: Callable[[], _Result],
) -> None:
    """Run one unit of work: its observations join `result` only if it
    finished; a unit not started, or cut off by the deadline, is `label`led in
    `unchecked`."""
    if budget.expired():
        unchecked.append(label)
        return
    try:
        result.extend(work())
    except _OutOfTime:
        unchecked.append(label)


def _list(manager: DatasetManager, dsi: DatasetInfo) -> List[SnapshotInfo]:
    """The managed snapshots of `dsi`, newest first by `createtxg`; raises on a
    `zfs list` failure (unlike `list_snapshots`, which hides it as `[]`)."""
    return manager._list_snapshots_strict(dsi, recursive=False)


def _ts(snap: Optional[SnapshotInfo]) -> Optional[float]:
    if snap is None or snap.timestamp is None:
        return None
    return snap.timestamp.astimezone(timezone.utc).timestamp()


def _error_text(exc: BaseException) -> str:
    return str(exc) or type(exc).__name__


#: What reads a destination's anchor and pin: `(anchor name or None, pin or None,
#: why the pin could not be read or None)`; raises `LocalAnchorUnreadable` for the anchor.
_ReadAnchor = Callable[[str], Tuple[Optional[str], Optional[Tuple[str, str]], Optional[str]]]


def _anchor_observations(
    manager: DatasetManager, dsi: DatasetInfo, tolerance: float,
    snapshots: List[SnapshotInfo], read: _ReadAnchor,
) -> _Result:
    """One anchor observation per destination of `dsi`, over its managed
    `snapshots` (newest first); `read` supplies the local anchor and pin."""
    oldest = snapshots[-1] if snapshots else None
    out: _Result = []
    for remote in dsi.config.remote:
        destination = remote.destination
        expected = _remote_expected(manager, dsi, remote)
        try:
            anchor_name, pin, pin_problem = read(destination)
        except LocalAnchorUnreadable as exc:
            # "Could not read" is never "not sent yet".
            out.append(Observation(
                source=SOURCE_ANCHOR, subject=dsi.name, peer=destination,
                expected_seconds=expected, tolerance_seconds=tolerance,
                error=f'the anchor could not be read: {_error_text(exc)}',
            ))
            continue
        anchor = next((s for s in snapshots if s.name == anchor_name), None) if anchor_name else None
        notes = []
        if anchor_name and anchor is None:
            notes.append(f'anchor {anchor_name} is not among the local managed snapshots')
        if pin is not None:
            src, dst = pin
            notes.append(f'send in flight: {src or "(full)"} -> {dst}')
        if pin_problem is not None:
            notes.append(f'send pin could not be read: {pin_problem}')
        if anchor is not None:
            notes.insert(0, anchor.name)
        out.append(Observation(
            source=SOURCE_ANCHOR, subject=dsi.name, peer=destination,
            expected_seconds=expected,
            newest_ts=_ts(anchor), oldest_ts=_ts(oldest), tolerance_seconds=tolerance,
            detail='; '.join(notes) if notes else None,
        ))
    return out


def _anchor_errors(
    manager: DatasetManager, dsi: DatasetInfo, tolerance: float, error: str,
) -> _Result:
    """An error observation per destination of `dsi` (its snapshots could not be read)."""
    return [
        Observation(
            source=SOURCE_ANCHOR, subject=dsi.name, peer=remote.destination,
            expected_seconds=_remote_expected(manager, dsi, remote),
            tolerance_seconds=tolerance, error=error,
        )
        for remote in dsi.config.remote
    ]


def _observe_local(
    manager: DatasetManager, dsi: DatasetInfo, tolerance: float, budget: _Budget,
) -> _Result:
    """The observations of one configured dataset: its snapshot, and per
    destination its anchor, each read of its own (the descendants of a
    recursive dataset are read in batches, `_observe_parent_children`).
    Raises `_OutOfTime` if the deadline passes first."""
    frequency = _dataset_interval(manager, dsi)
    try:
        snapshots = budget.call(_list, manager, dsi)
    except Exception as exc:
        error = _error_text(exc)
        return [Observation(
            source=SOURCE_SNAPSHOT, subject=dsi.name, expected_seconds=frequency,
            tolerance_seconds=tolerance, error=error,
        )] + _anchor_errors(manager, dsi, tolerance, error)

    newest = snapshots[0] if snapshots else None
    oldest = snapshots[-1] if snapshots else None
    out: _Result = [Observation(
        source=SOURCE_SNAPSHOT, subject=dsi.name, expected_seconds=frequency,
        newest_ts=_ts(newest), oldest_ts=_ts(oldest), tolerance_seconds=tolerance,
        detail=newest.name if newest is not None else None,
    )]

    def read(destination: str):
        # The RAISING reads: an unreadable anchor is an error, never "not sent
        # yet" (the swallowing `get_own_*` read as "no history"). An unreadable
        # pin must not discard a readable anchor.
        anchor = budget.call(manager.get_local_anchor, dsi, destination)
        try:
            return anchor, budget.call(manager.get_local_inflight, dsi, destination), None
        except LocalAnchorUnreadable as exc:
            return anchor, None, _error_text(exc)

    return out + _anchor_observations(manager, dsi, tolerance, snapshots, read)


def _observe_descendants(
    manager: DatasetManager, tolerance: float, budget: _Budget,
    result: _Result, unchecked: List[str],
) -> None:
    """The expanded descendants of the recursive datasets
    (`DatasetManager.expand_recursive`: name-only listing, no daemon needed):
    one anchor observation per child and destination, read in batches per
    parent (`_observe_parent_children`). A parent whose discovery failed gets
    one WARN entry: its children are unknown, which is not the same as
    healthy. Backup copies are never among the children (`expand_recursive`
    refuses them)."""
    roots = [c.name for c in manager.config.enabled_datasets if c.recursive and c.remote]
    try:
        expansion = manager.expand_recursive(
            should_stop=budget.expired, scope=budget.bulk_scope, quiet=True)
    except Exception as exc:  # it never raises; belt and braces
        logger.debug('cannot expand the recursive datasets: %r', exc)
        result.append(CheckEntry(
            source=SOURCE_DESCENDANTS, subject='(recursive datasets)', state=STATE_WARN,
            reason=f'the descendants of the recursive datasets could not be listed: {_error_text(exc)}',
        ))
        return
    _report_skipped_copies(expansion.skipped, result)
    by_parent: Dict[str, List[DatasetInfo]] = {}
    for child in expansion.children:
        by_parent.setdefault(child.expanded_from, []).append(child)
    if budget.expired():
        # The expansion stopped before some parents: those with no child and no
        # failure may have been skipped (or simply have none; the entry is then
        # a harmless WARN under a deadline that has passed anyway).
        for name in roots:
            if name not in by_parent and name not in expansion.failed:
                unchecked.append(f'{name} (descendants)')
    for parent, children in by_parent.items():
        _run_unit(
            result, unchecked, budget, f'{parent} (descendants)',
            lambda parent=parent, children=children: _observe_parent_children(
                manager, parent, children, tolerance, budget),
        )
    for parent, why in expansion.failed.items():
        result.append(CheckEntry(
            source=SOURCE_DESCENDANTS, subject=parent, state=STATE_WARN,
            reason=f'the descendants of {parent} could not be listed: {why}; '
                   f'their copies are not checked',
        ))


def _report_skipped_copies(skipped, result: _Result) -> None:
    """One WARN `copies` entry per backup copy (or receive target) that
    `expand_recursive` left out of a recursive parent, at most
    `COPIES_SHOWN_PER_PARENT` per parent, then one entry for the rest. The
    entry says what to do: the copy is neither snapshotted, sent nor pruned
    with the tree, so move it out. A parent that is itself inside a copy or a
    target (`name` at or above it) is one entry about the parent."""
    for parent, items in skipped.items():
        for name, why in items[:COPIES_SHOWN_PER_PARENT]:
            if name == parent or parent.startswith(name + '/'):
                result.append(CheckEntry(
                    source=SOURCE_COPIES, subject=parent, state=STATE_WARN,
                    reason=f'the recursive dataset {parent} is itself inside a backup copy or '
                           f'receive target ({name}: {why}): none of its descendants is '
                           f'snapshotted, sent or pruned with the tree; move {parent} out '
                           f'(`zfs rename {parent} <a name outside {name}>`) or stop it being recursive',
                ))
                continue
            result.append(CheckEntry(
                source=SOURCE_COPIES, subject=name, state=STATE_WARN,
                reason=f'backup copy below the recursive dataset {parent} ({why}): it is not '
                       f'snapshotted, sent or pruned with the tree; move it out '
                       f'(`zfs rename {name} <a name outside {parent}>`)',
            ))
        more = len(items) - COPIES_SHOWN_PER_PARENT
        if more > 0:
            # Not `parent` alone: the parent-inside-a-copy entry above uses it,
            # and Prometheus labels are `(source, subject)`. A space and a
            # parenthesis cannot occur in a dataset name, so no copy collides.
            result.append(CheckEntry(
                source=SOURCE_COPIES, subject=f'{parent} (+{more} more)', state=STATE_WARN,
                reason=f'and {more} more backup copies below the recursive dataset {parent} '
                       f'(the first {COPIES_SHOWN_PER_PARENT} are listed): none is snapshotted, '
                       f'sent or pruned with the tree; move them out',
            ))


def _existing_names(parent: Dataset) -> set:
    """The filesystems and volumes at or below `parent` (a name-only listing)."""
    return {d.name for d in zfs.list(roots=parent, recursive=True, types='filesystem,volume')}


def _observe_parent_children(
    manager: DatasetManager, parent: str, children: List[DatasetInfo], tolerance: float,
    budget: _Budget,
) -> _Result:
    """The anchor observations of `children`, the expanded descendants of the
    recursive dataset `parent`, from ONE recursive snapshot listing and ONE
    two-step property read below it (plus a name-only listing that drops a
    child destroyed or renamed since the expansion), instead of a listing and
    two reads per child. Only the child's LOCAL anchor and pin count. A batch
    that cannot be read is the error of every child and destination it was
    meant to answer. Raises `_OutOfTime` if the deadline passes first."""
    ds = Dataset(parent)
    # The properties first: an anchor that moves to a snapshot created after
    # this read is then found in the listing below, never "not among the local
    # managed snapshots".
    props: Dict[str, Dict[str, str]] = {}
    sources: Dict[str, Dict[str, str]] = {}
    prop_error: Optional[str] = None
    try:
        props = budget.call_with_bulk_scope(
            read_descendant_zfsbackup_properties, ds, sources,
            (PROP_ANCHOR_PREFIX, PROP_INFLIGHT_PREFIX),
        )
    except Exception as exc:
        prop_error = _error_text(exc)
    tree: Dict[str, List[SnapshotInfo]] = {}
    list_error: Optional[str] = None
    try:
        tree = budget.call_with_bulk_scope(manager._list_snapshot_tree_strict, ds)
    except Exception as exc:
        list_error = _error_text(exc)
    try:
        existing = budget.call_bulk(_existing_names, ds)
    except Exception as exc:  # keep the children as expanded: the reads above stand on their own
        logger.debug('cannot re-list the descendants of %s: %r', parent, exc)
    else:
        children = [c for c in children if c.name in existing]

    out: _Result = []
    for child in children:
        if list_error is not None:
            out.extend(_anchor_errors(manager, child, tolerance, list_error))
            continue

        def read(destination: str, name: str = child.name):
            if prop_error is not None:
                raise LocalAnchorUnreadable(prop_error)
            values, origin = props.get(name, {}), sources.get(name, {})

            def local(prop: str) -> Optional[str]:
                return values.get(prop) if origin.get(prop) == 'local' else None

            return (
                local(f'{PROP_ANCHOR_PREFIX}{destination}'),
                _parse_inflight(local(f'{PROP_INFLIGHT_PREFIX}{destination}')),
                None,
            )

        out.extend(_anchor_observations(manager, child, tolerance, tree.get(child.name, []), read))
    return out


def _dataset_interval(manager: DatasetManager, dsi: DatasetInfo) -> float:
    """How often a new snapshot can be expected: the dataset's `frequency`, but
    the snapshot worker looks only every `check_interval`."""
    return max(
        dsi.config.frequency.total_seconds(),
        manager.config.check_interval.total_seconds(),
    )


def _remote_expected(manager: DatasetManager, dsi: DatasetInfo, remote) -> float:
    """`max(dataset interval, destination frequency)`: a destination asked
    to send hourly still cannot be fresher than the dataset's snapshots."""
    dataset = _dataset_interval(manager, dsi)
    own = remote.frequency.total_seconds() if remote.frequency else dataset
    return max(dataset, own)


def _observe_received(
    manager: DatasetManager, target: str, tolerance: float, budget: _Budget,
    result: _Result, unchecked: List[str],
) -> None:
    # `received_datasets()` logs a failed listing and returns [], which would
    # make a server whose pool did not import look healthy: list the target
    # strictly first, and read `last_received_error` after it.
    label = f'{target} (received datasets)'
    if budget.expired():
        unchecked.append(label)
        return
    try:
        problem = _target_problem(target, budget)
        received: List[DatasetInfo] = []
        if problem is None:
            try:
                received = manager.received_datasets(
                    scope=budget.scope, bulk_scope=budget.bulk_scope)
            except Exception as exc:  # it swallows its own failures; belt and braces
                problem = _error_text(exc)
            else:
                failure = getattr(manager, 'last_received_error', None)
                if failure:
                    if budget.expired():
                        raise _OutOfTime()
                    problem = failure
    except _OutOfTime:
        unchecked.append(label)
        return
    if problem is not None:
        result.append(Observation(
            source=SOURCE_RECEIVED, subject=target, expected_seconds=1.0,
            tolerance_seconds=tolerance, error=problem,
        ))
        return
    for dsi in received:
        _run_unit(result, unchecked, budget, dsi.name,
                  lambda dsi=dsi: _observe_received_dataset(manager, dsi, target, tolerance, budget))


def _observe_received_dataset(
    manager: DatasetManager, dsi: DatasetInfo, target: str, tolerance: float, budget: _Budget,
) -> _Result:
    expected = max(
        [dsi.config.frequency.total_seconds()]
        + [r.frequency.total_seconds() for r in dsi.config.remote if r.frequency]
    )
    client = _client_of(dsi.name, target)
    try:
        snapshots = budget.call(_list, manager, dsi)
    except Exception as exc:
        return [Observation(
            source=SOURCE_RECEIVED, subject=dsi.name, peer=client,
            expected_seconds=expected, tolerance_seconds=tolerance,
            error=_error_text(exc),
        )]
    newest = snapshots[0] if snapshots else None
    return [Observation(
        source=SOURCE_RECEIVED, subject=dsi.name, peer=client,
        expected_seconds=expected, newest_ts=_ts(newest),
        tolerance_seconds=tolerance,
        detail=newest.name if newest is not None else None,
    )]


def _target_problem(target: str, budget: _Budget) -> Optional[str]:
    """Why the target dataset cannot be read, or `None` if it can. Raises
    `_OutOfTime` if the deadline passes first."""
    try:
        found = budget.call(
            lambda: list(zfs.list(roots=Dataset(target), types='filesystem'))
        )
    except Exception as exc:
        return _error_text(exc)
    if not found:
        return f'the target dataset {target} does not exist'
    return None


def _client_of(name: str, target: str) -> Optional[str]:
    """The client id of a received dataset `<target>/<client_id>[/...]`
    (`received_datasets` only returns datasets of that shape)."""
    prefix = target + '/'
    if not name.startswith(prefix):
        return None
    return name[len(prefix):].split('/', 1)[0] or None

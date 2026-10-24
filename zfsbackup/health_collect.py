"""The ZFS-reading half of `zfsbackup-config check`: builds the plain
observations that `zfsbackup.health.evaluate` grades.

**Read-only.** Only `zfs list` and `zfs get` run (through `DatasetManager`); no
property is written, no snapshot created or destroyed, no lock taken and no
signal sent. It never touches the config database either: the caller hands it a
`BackupConfig`. It may import `libzfseasy` and `zfsbackup.config`;
`zfsbackup.health` stays stdlib only, which is why the two are separate modules.

Entry point: `collect(config, manager=None) -> list`. The result mixes
`health.Observation` (a lag to grade) and `health.CheckEntry` (a verdict already
reached: `--dry-run` / `dry_run: true` is a WARN) and goes straight to
`health.evaluate`.

What is observed, per enabled dataset (a disabled one is skipped):

* its newest managed snapshot -- by `createtxg`, the daemon's own ordering --
  against the dataset's `frequency`, but never less than `check_interval`: the
  snapshot worker cannot snapshot more often than it checks;
* for each configured destination, the anchor snapshot (the one last sent) against
  `max(dataset frequency, destination frequency, check_interval)`, with a `inflight` pin noted;
  with no anchor, the oldest managed snapshot decides how long that is
  tolerated (`health` module docstring);
* when `remote_backup` is enabled, each received client dataset's newest snapshot
  against the largest frequency in that client's own config. The target is
  listed first, strictly: a target that cannot be listed or does not exist is a
  CRIT `received` entry on the target itself (`received_datasets()` would hide it
  as an empty list). A received dataset with no managed snapshot stays WARN: with
  none, there is no time reference to turn "nothing yet" into CRIT.

Ages come from the snapshot names (as everywhere in the daemon), not from
`creation`. A send whose raw/plain mode disagrees with the server's copy leaves no
marker yet, so it appears only as lag.
"""

import logging
from datetime import timezone
from typing import List, Optional, Union

import libzfseasy as zfs
from libzfseasy.types import Dataset
from zfsbackup.backup_manager import DatasetInfo, DatasetManager, SnapshotInfo
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

__all__ = ['collect']

#: Subject and source of the dry-run verdict.
_CONFIG_SOURCE = 'config'
_DRY_RUN_SUBJECT = 'dry_run'


def collect(
    config: BackupConfig,
    manager: Optional[DatasetManager] = None,
) -> List[Union[Observation, CheckEntry]]:
    """Observe `config`'s datasets (and, on a server, its received datasets).

    Ages are not computed here: the observations carry snapshot timestamps and
    `health.evaluate` grades them at the caller's `now`. `manager` lets a caller
    (a test) supply the `DatasetManager`; by default one is built over
    `config`. Never raises for a ZFS failure: an unreadable dataset becomes an
    observation with `error` set."""
    manager = manager if manager is not None else DatasetManager(config)
    result: List[Union[Observation, CheckEntry]] = []

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
        result.extend(_observe_local(manager, dsi, tolerance))

    rb = config.remote_backup
    if rb is not None and rb.enabled:
        result.extend(_observe_received(manager, rb.target_dataset.rstrip('/'), tolerance))
    return result


def _list(manager: DatasetManager, dsi: DatasetInfo) -> List[SnapshotInfo]:
    """The managed snapshots of `dsi`, newest first by `createtxg`; raises on a
    `zfs list` failure (unlike `list_snapshots`, which hides it as `[]`)."""
    return manager._list_snapshots_strict(dsi, recursive=False)


def _ts(snap: Optional[SnapshotInfo]) -> Optional[float]:
    if snap is None or snap.timestamp is None:
        return None
    return snap.timestamp.astimezone(timezone.utc).timestamp()


def _observe_local(
    manager: DatasetManager, dsi: DatasetInfo, tolerance: float
) -> List[Union[Observation, CheckEntry]]:
    frequency = _dataset_interval(manager, dsi)
    try:
        snapshots = _list(manager, dsi)
    except Exception as exc:
        error = str(exc) or type(exc).__name__
        out: List[Union[Observation, CheckEntry]] = [Observation(
            source=SOURCE_SNAPSHOT, subject=dsi.name, expected_seconds=frequency,
            tolerance_seconds=tolerance, error=error,
        )]
        for remote in dsi.config.remote:
            out.append(Observation(
                source=SOURCE_ANCHOR, subject=dsi.name, peer=remote.destination,
                expected_seconds=_remote_expected(manager, dsi, remote),
                tolerance_seconds=tolerance, error=error,
            ))
        return out

    newest = snapshots[0] if snapshots else None
    oldest = snapshots[-1] if snapshots else None
    out = [Observation(
        source=SOURCE_SNAPSHOT, subject=dsi.name, expected_seconds=frequency,
        newest_ts=_ts(newest), oldest_ts=_ts(oldest), tolerance_seconds=tolerance,
        detail=newest.name if newest is not None else None,
    )]

    for remote in dsi.config.remote:
        destination = remote.destination
        anchor_name = manager.get_anchor(dsi, destination)
        anchor = next((s for s in snapshots if s.name == anchor_name), None) if anchor_name else None
        notes = []
        if anchor_name and anchor is None:
            notes.append(f'anchor {anchor_name} is not among the local managed snapshots')
        pin = manager.get_inflight(dsi, destination)
        if pin is not None:
            src, dst = pin
            notes.append(f'send in flight: {src or "(full)"} -> {dst}')
        if anchor is not None:
            notes.insert(0, anchor.name)
        out.append(Observation(
            source=SOURCE_ANCHOR, subject=dsi.name, peer=destination,
            expected_seconds=_remote_expected(manager, dsi, remote),
            newest_ts=_ts(anchor), oldest_ts=_ts(oldest), tolerance_seconds=tolerance,
            detail='; '.join(notes) if notes else None,
        ))
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
    manager: DatasetManager, target: str, tolerance: float
) -> List[Union[Observation, CheckEntry]]:
    # `received_datasets()` logs a failed listing and returns [], which would
    # make a server whose pool did not import look healthy: list the target
    # strictly first.
    problem = _target_problem(target)
    if problem is None:
        try:
            received = manager.received_datasets()
        except Exception as exc:  # it swallows its own failures; belt and braces
            problem = str(exc) or type(exc).__name__
    if problem is not None:
        return [Observation(
            source=SOURCE_RECEIVED, subject=target, expected_seconds=1.0,
            tolerance_seconds=tolerance, error=problem,
        )]
    out: List[Union[Observation, CheckEntry]] = []
    for dsi in received:
        expected = max(
            [dsi.config.frequency.total_seconds()]
            + [r.frequency.total_seconds() for r in dsi.config.remote if r.frequency]
        )
        client = _client_of(dsi.name, target)
        try:
            snapshots = _list(manager, dsi)
        except Exception as exc:
            out.append(Observation(
                source=SOURCE_RECEIVED, subject=dsi.name, peer=client,
                expected_seconds=expected, tolerance_seconds=tolerance,
                error=str(exc) or type(exc).__name__,
            ))
            continue
        newest = snapshots[0] if snapshots else None
        out.append(Observation(
            source=SOURCE_RECEIVED, subject=dsi.name, peer=client,
            expected_seconds=expected, newest_ts=_ts(newest),
            tolerance_seconds=tolerance,
            detail=newest.name if newest is not None else None,
        ))
    return out


def _target_problem(target: str) -> Optional[str]:
    """Why the target dataset cannot be read, or `None` if it can."""
    try:
        found = list(zfs.list(roots=Dataset(target), types='filesystem'))
    except Exception as exc:
        return str(exc) or type(exc).__name__
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

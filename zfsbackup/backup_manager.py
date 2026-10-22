"""Snapshot manager for creating and managing ZFS snapshots."""

from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, FrozenSet, Iterable, List, Optional, Set, Tuple, Union
import dataclasses
import logging
import shlex
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import libzfseasy as zfs
from libzfseasy.types import Dataset, Filesystem, Snapshot as ZFSSnapshot
from libzfseasy.zfs import _zfs_cmd
from zfsbackup.config import (
    BackupConfig, DatasetConfig, _collapse_retention_rules, client_id_problem,
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


# ── Module-level property helpers (item 14.1) ─────────────────────────────────
#
# Used by the `zfsbackup-config` CLI's rename/edit commands, which have no
# `BackupConfig` and no `DatasetManager`. Unlike the `DatasetManager` methods
# they do NOT swallow ZFS errors: the CLI must be able to tell the operator
# that a property copy failed. All take `dry_run` and then touch nothing.


def _parse_zfsbackup_get(lines: Iterable[str], accept) -> Dict[str, Dict[str, str]]:
    """Parse `zfs get -H -o name,property,value,received,source` lines into
    `{dataset name: {org.zfsbackup:* property: value}}` (local/received only),
    keeping the rows whose dataset name `accept` approves."""
    result: Dict[str, Dict[str, str]] = {}
    for line in lines:
        parts = line.rstrip('\n').split('\t')
        if len(parts) < 5:
            continue
        name, prop, source = parts[0], parts[1], parts[-1]
        if not prop.startswith(PROP_PREFIX) or not accept(name):
            continue
        if source not in ('local', 'received'):
            continue
        # A value may itself contain a tab. For a `local` property the
        # `received` column is a single '-', so the value is everything
        # between `property` and the last two columns. For a `received`
        # property the `received` column repeats the value, so the value
        # spans exactly half of the columns between `property` and `source`.
        if source == 'received':
            span = (len(parts) - 3) // 2
            value = '\t'.join(parts[2:2 + span])
        else:
            value = '\t'.join(parts[2:-2])
        result.setdefault(name, {})[prop] = value
    return result


def read_zfsbackup_properties(ds: Dataset) -> Dict[str, str]:
    """Return every `org.zfsbackup:*` user property set on `ds`, name -> value.

    Properties are read with their `local` and `received` sources only (an
    inherited value belongs to an ancestor, and acting on it would be acting
    on the wrong dataset). Raises whatever `zfs get` raises.

    Reads the raw `zfs get` lines and keeps only `org.zfsbackup:` names, so
    no libzfseasy object is built for the dataset's other properties. Not
    because libzfseasy rejects unknown native properties (it now keeps
    well-formed native names `zfs` returns in an extras store), but because
    the object path is the wrong tool here: this needs only the user
    properties under our prefix, with their exact values (a value may
    contain tabs, which the line parser below recovers from the fixed
    trailing `received`/`source` columns) and only the `local` and `received`
    sources, and building and updating objects for every property of every
    dataset is wasted work and a place for an unrelated property to fail
    this read.
    """
    cmd = [
        _zfs_cmd(), 'get', '-H', '-o', 'name,property,value,received,source',
        '-s', 'local,received', 'all', ds.name,
    ]
    parsed = _parse_zfsbackup_get(zfs.get._exec_out(cmd), lambda n: n == ds.name)
    return parsed.get(ds.name, {})


def read_descendant_zfsbackup_properties(ds: Dataset) -> Dict[str, Dict[str, str]]:
    """`read_zfsbackup_properties` for every DESCENDANT of `ds` (not `ds`
    itself), in one `zfs get -r`: `{descendant name: {property: value}}`,
    only descendants that have at least one local/received `org.zfsbackup:*`
    property appear. Snapshots and bookmarks are excluded (`-t`). Raises
    whatever `zfs get` raises -- the pruner must not prune on an unknown
    protect-set."""
    cmd = [
        _zfs_cmd(), 'get', '-r', '-t', 'filesystem,volume', '-H',
        '-o', 'name,property,value,received,source',
        '-s', 'local,received', 'all', ds.name,
    ]
    prefix = ds.name + '/'
    return _parse_zfsbackup_get(
        zfs.get._exec_out(cmd),
        lambda n: n.startswith(prefix) and '@' not in n and '#' not in n,
    )


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

    Both properties are read with `read_zfsbackup_properties` (local and
    received sources; an inherited value is neither a source nor a blocker).
    A caller that wants to show the two values (e.g. for a dry run) reads
    them with that function beforehand; this function returns only the outcome.

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
    props = read_zfsbackup_properties(ds)
    if old_prop not in props:
        return 'no-source'
    value = props[old_prop]
    exists = new_prop in props
    if exists:
        if not overwrite:
            return 'exists'
        if props[new_prop] == value:
            return 'same'
    if dry_run:
        if exists:
            logger.info(
                f"[DRY RUN] Would overwrite {new_prop}={props[new_prop]} "
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
    `False`/empty for none.
    Only properties that are actually set (local/received) are inherited, so
    a clear is idempotent. Returns the property names cleared (or, under
    `dry_run`, that would have been). Raises whatever `zfs` raises.

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
    present = read_zfsbackup_properties(ds)
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
    for name in wanted:
        if dry_run:
            logger.info(f"[DRY RUN] Would inherit {name} on {ds.name}")
        else:
            zfs.inherit(ds, name)
            logger.debug(f"Cleared {name} on {ds.name}")
    return wanted


class DatasetInfo:
    """Information about a dataset."""

    def __init__(self, dataset: Dataset, config: DatasetConfig):
        self.dataset = dataset
        self.name = dataset.name
        self.config = config
        self.snapshots: List[SnapshotInfo] = []

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
        # Received datasets skipped by `received_datasets()`: name -> the
        # problem last announced at WARNING. Warned once while the same
        # problem persists (the per-cycle repeat is DEBUG); forgotten once the
        # dataset is accepted again or disappears, so a later problem warns.
        self._received_skip_noted: Dict[str, str] = {}

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
        inherited -- `get_anchor` relies on seeing an inherited anchor), read
        with `zfs get -H -o value` (`zfs.get.value`): exact even when the
        value holds a tab or newline, which a `zfs list -H` row cannot frame."""
        try:
            return zfs.get.value(ds, prop)
        except Exception as e:
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
        """Clear the anchor snapshot for a destination (inherit removes user property)."""
        prop = f"{PROP_ANCHOR_PREFIX}{destination}"
        try:
            zfs.inherit(dsi.dataset, prop)
            logger.debug(f"Cleared anchor for {dsi.name} -> {destination}")
        except Exception as e:
            logger.warning(f"Failed to clear anchor property for {dsi.name}: {e}")

    def get_inflight(self, dsi: DatasetInfo, destination: str) -> Optional[Tuple[str, str]]:
        """The `(from, to)` pair recorded in `inflight.<destination>`
        (`from` is `''` for a full send), or `None` when unset or malformed."""
        value = self._get_prop(dsi.dataset, f"{PROP_INFLIGHT_PREFIX}{destination}")
        if not value or value == '-' or ',' not in value:
            return None
        src, _, dst = value.partition(',')
        return (src, dst) if dst else None

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
        """Remove `inflight.<destination>` (inherit removes a user property).
        `True` if cleared, `False` (after a WARNING) if not: a stale pin only
        over-protects two snapshots until the next send overwrites it."""
        prop = f"{PROP_INFLIGHT_PREFIX}{destination}"
        try:
            zfs.inherit(dsi.dataset, prop)
            logger.debug(f"Cleared in-flight pin for {dsi.name} -> {destination}")
            return True
        except Exception as e:
            logger.warning(f"Failed to clear in-flight property for {dsi.name}: {e}")
            return False

    def get_anchors(self, dsi: DatasetInfo) -> Set[str]:
        """Return the snapshot names pinned by ANY `org.zfsbackup:anchor.*`
        property on the dataset, whether or not its destination is configured.

        This is the pruner's protect-set, and it may only ever err on the side
        of protecting more. Sources:

        * every `anchor.*` user property set `local` or `received` on the
          dataset (via `read_zfsbackup_properties`: raw `zfs get`, filtered
          before any libzfseasy object is built). An anchor of a destination
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

        Raises if the property read fails: the caller must not prune on an
        unknown protect-set (`prune_snapshots` skips the dataset instead).
        """
        anchors: Set[str] = set()
        configured = {r.destination for r in (dsi.config.remote or [])}
        for prop, value in read_zfsbackup_properties(dsi.dataset).items():
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
        for name in sorted(configured):
            anchor = self.get_anchor(dsi, name)
            if anchor:
                anchors.add(anchor)
        return anchors

    def received_datasets(
        self, should_stop: Optional[Callable[[], bool]] = None
    ) -> List[DatasetInfo]:
        """Discover datasets received from remote clients under the configured target.

        `should_stop` (the worker passes "an exit reason is pending", as for
        `verify_datasets`) is checked before each dataset's property reads,
        because those cost two or three `zfs get` executions each and a
        server with hundreds of datasets would otherwise not notice a stop
        or a reload for minutes. Once it returns true the enumeration ends
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
        * each dataset's `client_id` and `config` are then read one at a
          time with `zfs get -H -o value -s local` (`zfs.get.value`), which
          frames any value exactly. LOCAL only: these are written by the
          server's own negotiate, so a received copy (a `zfs send -p`
          stream) or an inherited one (a nested server dataset under
          another client's) never counts;
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
          WARNING if it carries a *received* config (one more read).

        Cost: two `zfs get` executions per received filesystem per pruning
        cycle (three for a dataset with neither property set locally), on
        top of the one listing. Acceptable for the dataset counts
        this targets; a single framed multi-property read (libzfseasy S3
        `zfs.get.rows`) can replace them later.
        """
        rb = self.config.remote_backup
        if not rb or not rb.enabled:
            return []
        target = rb.target_dataset.rstrip('/')
        try:
            all_ds = zfs.list(
                roots=Dataset(target),
                recursive=True,
                types='filesystem',
            )
        except Exception as e:
            logger.error(f"Failed to discover received datasets: {e!r}")
            return []

        result = []
        noted = self._received_skip_noted
        seen: Set[str] = set()
        for ds in all_ds:
            if should_stop is not None and should_stop():
                return result
            seen.add(ds.name)
            dsi, problem = self._received_dataset_info(ds, target)
            if dsi is not None:
                noted.pop(ds.name, None)
                result.append(dsi)
                continue
            if problem is None:
                continue
            if noted.get(ds.name) != problem:
                noted[ds.name] = problem
                logger.warning(
                    "Skipping received dataset %r: %s; it is not pruned until this is fixed",
                    ds.name, problem,
                )
            else:
                logger.debug("Skipping received dataset %r: %s", ds.name, problem)
        for name in [n for n in noted if n not in seen]:
            del noted[name]
        return result

    def _received_dataset_info(
        self, ds: Dataset, target: str
    ) -> Tuple[Optional[DatasetInfo], Optional[str]]:
        """`(DatasetInfo, None)` for a valid received client dataset (see
        `received_datasets`), else `(None, problem)` -- `problem` is `None`
        for a dataset that is simply not a client dataset (no local
        zfsbackup properties at all). Never raises. Values are logged with
        `!r`: they may hold control characters or lone surrogates."""
        try:
            client_id = zfs.get.value(ds, PROP_CLIENT_ID, sources='local')
            config_value = zfs.get.value(ds, PROP_CONFIG, sources='local')
        except Exception as e:
            return None, f"cannot read its properties ({e!r})"
        if client_id is None and config_value is None:
            # Not a client dataset (the target, a `<target>/<id>` container,
            # an intermediate parent) -- unless a config arrived in a
            # `zfs send -p` stream, which is never trusted but worth a
            # WARNING. One extra read, for these few datasets only.
            try:
                received = zfs.get.value(ds, PROP_CONFIG, sources='received')
            except Exception as e:
                return None, f"cannot read its properties ({e!r})"
            if received is not None:
                return None, (
                    f"{PROP_CONFIG} is only received (zfs send -p), not set locally; "
                    "a received config is never trusted"
                )
            logger.debug(f"{ds.name}: no local {PROP_CLIENT_ID}/{PROP_CONFIG}; not a client dataset")
            return None, None
        if client_id is None:
            return None, f"{PROP_CLIENT_ID} is not set locally"
        if config_value is None:
            return None, f"{PROP_CONFIG} is not set locally"
        id_problem = client_id_problem(client_id)
        if id_problem is not None:
            return None, f"{PROP_CLIENT_ID}={client_id!r} is invalid: {id_problem}"
        home = f"{target}/{client_id}"
        if ds.name != home and not ds.name.startswith(home + '/'):
            return None, (
                f"it lies outside its client's tree {home!r} "
                f"({PROP_CLIENT_ID}={client_id!r})"
            )
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
        try:
            ds_config = dataclasses.replace(ds_config, recursive=False)
            return DatasetInfo(ds, ds_config), None
        except Exception as e:
            return None, f"{PROP_CONFIG} is unusable ({type(e).__name__}: {e})"

    # ── Snapshot operations ───────────────────────────────────────────────────

    def needs_snapshot(self, dsi: DatasetInfo) -> bool:
        snapshots = self.list_snapshots(dsi, recursive=False)
        if not snapshots:
            logger.debug(f"Dataset {dsi.name} needs snapshot: True (no existing snapshots)")
            return True
        latest = snapshots[0]
        age = latest.age
        needs = age >= dsi.frequency
        logger.debug(f"Latest snapshot for {dsi.name} is {latest.full_name} (age={age})")
        logger.debug(f"Dataset {dsi.name} needs snapshot: {needs} (age={age}, frequency={dsi.frequency})")
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
        keep: set = {id(snapshots[0])}

        for sni in reversed(snapshots):
            if sni.timestamp is None:
                continue
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
        """`list_snapshots` that lets a `zfs list` failure propagate."""
        snapshots = zfs.list(
            roots=dsi.dataset,
            types='snapshot',
            recursive=recursive,
            properties=['creation'],
        )
        result: List[SnapshotInfo] = []
        for snap in snapshots:
            if isinstance(snap, ZFSSnapshot):
                info = SnapshotInfo(snap, self.prefix)
                if info.is_managed:
                    result.append(info)
        dsi.snapshots = sorted(result, key=lambda s: s.timestamp, reverse=True)
        return dsi.snapshots

    def list_snapshots(self, dsi: DatasetInfo, recursive: bool = False) -> List['SnapshotInfo']:
        """List all managed snapshots for a dataset."""
        try:
            return self._list_snapshots_strict(dsi, recursive)
        except Exception as e:
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
        """Create a new snapshot for the dataset."""
        snap_name = self._generate_snapshot_name(dsi)
        full_name = f"{dsi.name}@{snap_name}"
        try:
            logger.info(f"Creating snapshot: {full_name} (recursive={dsi.recursive})")
            if self.config.dry_run:
                logger.info(f"[DRY RUN] Would create snapshot: {full_name}")
                return None
            result = zfs.snapshot(dsi.dataset, snap_name, recursive=dsi.recursive)
            snapshot = result[0] if isinstance(result, list) else result
            dsi.snapshots.insert(0, SnapshotInfo(snapshot, self.prefix))
            logger.info(f"Successfully created snapshot: {full_name}")
            return SnapshotInfo(snapshot, self.prefix)
        except Exception as e:
            logger.error(f"Failed to create snapshot {full_name}: {e}")
            return None

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

    def prune_snapshots(self, dsi: DatasetInfo) -> None:
        """Prune snapshots according to retention policy, skipping anchored snapshots."""
        snapshots = self.list_snapshots(dsi, recursive=False)
        if not snapshots:
            logger.debug(f"No snapshots to prune for {dsi.name}")
            return

        try:
            anchors = self.get_anchors(dsi)
            desc_protected = (
                self._descendant_anchor_protection(dsi) if dsi.recursive else {}
            )
        except Exception as e:
            # Unknown protect-set: pruning blind could destroy an incremental base.
            logger.error(f"Cannot read anchors for {dsi.name}; skipping pruning this cycle: {e}")
            return
        to_prune = self.needs_prunning(dsi, anchors=anchors)
        if dsi.recursive and to_prune:
            # `zfs destroy -r ds@snap` also destroys `@snap` on every
            # descendant, so a name a descendant protects must not be
            # destroyed at all.
            try:
                for name, reason in self._descendant_retention_protection(dsi).items():
                    desc_protected.setdefault(name, reason)
            except Exception as e:
                msg = f"Cannot evaluate descendants of {dsi.name}; skipping pruning this cycle: {e}"
                if dsi.name in self._descendant_error_noted:
                    logger.debug(msg)
                else:
                    self._descendant_error_noted.add(dsi.name)
                    logger.error(msg)
                return
            self._descendant_error_noted.discard(dsi.name)
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
            self._descendant_error_noted.discard(dsi.name)
        logger.info(f"Dataset {dsi.name} has {len(to_prune)} snapshots to prune")

        batch_size = 20
        for i in range(0, len(to_prune), batch_size):
            batch = to_prune[i:i + batch_size]
            names = ', '.join(s.full_name for s in batch)
            logger.info(f"Pruning snapshots (batch {i // batch_size + 1}): {names}")
            if self.config.dry_run:
                logger.info(f"[DRY RUN] Would prune snapshots: {names}")
                continue
            try:
                zfs.destroy([s.snapshot for s in batch], destroy=True, recursive=dsi.recursive)
                logger.info(f"Successfully pruned {len(batch)} snapshots")
            except Exception as e:
                logger.error(f"Failed to prune snapshot batch: {e}")


class SnapshotInfo:
    """Information about a snapshot with parsed metadata."""

    def __init__(self, snapshot: ZFSSnapshot, prefix: str):
        self.snapshot = snapshot
        self.name = snapshot.short
        self.full_name = str(snapshot)
        self.prefix = prefix
        self.timestamp = self._parse_timestamp()

    def _parse_timestamp(self) -> Optional[datetime]:
        """`{prefix}_{YYYYMMDDHHMMSS}[_anything]` -> UTC datetime, else None.
        The prefix may itself contain `_`; the timestamp is the token right
        after `{prefix}_` and must be exactly 14 ASCII digits."""
        head = self.prefix + '_'
        if not self.name.startswith(head):
            return None
        token = self.name[len(head):].split('_', 1)[0]
        if len(token) != 14 or not (token.isascii() and token.isdigit()):
            return None
        try:
            dt = datetime.strptime(token, "%Y%m%d%H%M%S")
        except ValueError:
            return None
        return dt.replace(tzinfo=timezone.utc)

    @property
    def age(self) -> timedelta:
        if self.timestamp is None:
            return timedelta(0)
        return datetime.now(timezone.utc) - self.timestamp

    @property
    def is_managed(self) -> bool:
        return self.name.startswith(self.prefix + '_') and self.timestamp is not None

    def __repr__(self):
        return f"SnapshotInfo({self.full_name}, age={self.age})"

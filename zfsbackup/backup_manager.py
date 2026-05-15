"""Snapshot manager for creating and managing ZFS snapshots."""

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
import logging
import sys
import os

# Add parent directory to path to import libzfseasy
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import libzfseasy as zfs
from libzfseasy.types import Dataset, Snapshot as ZFSSnapshot
from zfsbackup.config import BackupConfig, DatasetConfig


logger = logging.getLogger(__name__)


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
        """Return the aligned reference time for the given snapshot frequency.

        Snapshots are expected to run at the start of each period. The returned
        reference acts as the anchor from which every `frequency` interval is
        measured. Callers that want to enforce a minimum cadence (for example,
        "no faster than daily") should validate that constraint before
        invoking this helper.
        """

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

    @property
    def datasets(self) -> List[DatasetInfo]:
        """Get list of DatasetInfo objects for all configured datasets."""
        if self._datasets is None:
            self._datasets = self._get_datasets()
        return self._datasets

    @property
    def prefix(self) -> str:
        return self.config.snapshot_prefix

    def _get_datasets(self) -> List[DatasetInfo]:
        """Get list of DatasetInfo objects for all configured datasets."""
        datasets = []
        for ds_config in self.config.enabled_datasets:
            dataset = Dataset(ds_config.name)
            datasets.append(DatasetInfo(dataset, ds_config))
        return datasets
    
    def verify_datasets(self) -> List[str]:
        for ds_info in self.datasets:
            if not zfs.exists(ds_info.dataset):
                logger.error(f"Dataset {ds_info.name} does not exist")
                continue

    def dataset_report(self) -> List[str]:
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

    def needs_snapshot(self, dsi: DatasetInfo) -> bool:
        
        snapshots = self.list_snapshots(dsi, recursive=False)
    
        if not snapshots:
            logger.debug(f"No snapshots found for {dsi.name}")
            logger.debug(f"Dataset {dsi.name} needs snapshot: True (no existing snapshots)")
            return True
        
        latest = snapshots[0]
        age = latest.age

        needs = age >= dsi.frequency

        logger.debug(f"Latest snapshot for {dsi.name} is {latest.full_name} (age={age})")
        logger.debug(f"Dataset {dsi.name} needs snapshot: {needs} (age={age}, frequency={dsi.frequency})")

        return needs

    def needs_prunning(self, dsi: DatasetInfo) -> List['SnapshotInfo']:

        snapshots = dsi.snapshots
        if not snapshots or not dsi.config.retention_rules:
            return []

        plan = {
            rule.keep_for.total_seconds(): rule.age.total_seconds()
            for rule in dsi.config.retention_rules
        }
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
        for sni in to_prune:
            logger.debug(f"Snapshot {sni.full_name} marked for pruning")
        return to_prune

    def list_snapshots(self, dsi: DatasetInfo, recursive: bool = False) -> List[SnapshotInfo]:
        """List all snapshots for a dataset."""
        try:            
            snapshots = zfs.list(
                roots=dsi.dataset,
                types='snapshot',
                recursive=recursive,
                properties=['creation']
            )
            
            result: List[SnapshotInfo] = []
            for snap in snapshots:
                if isinstance(snap, ZFSSnapshot):
                    info = SnapshotInfo(snap, self.prefix)
                    if info.is_managed:
                        result.append(info)
            
            dsi.snapshots = sorted(result, key=lambda s: s.timestamp, reverse=True)

            return dsi.snapshots
            
        except Exception as e:
            logger.error(f"Failed to list snapshots for {dsi.name}: {e}")
            return []

    def _generate_snapshot_name(self, dsi: DatasetInfo,timestamp: Optional[datetime] = None) -> str:
        """Generate snapshot name with timestamp."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        timestamp = dsi.get_reference_time(timestamp)
        
        date_str = timestamp.strftime("%Y%m%d")
        time_str = timestamp.strftime("%H%M%S")
        return f"{self.prefix}_{date_str}{time_str}"
    
    def create_snapshot(self, dsi: DatasetInfo) -> Optional[SnapshotInfo]:
        """Create a new snapshot for the dataset."""
        
        snap_name = self._generate_snapshot_name(dsi)
        full_name = f"{dsi.name}@{snap_name}"
        
        try:
            logger.info(f"Creating snapshot: {full_name} (recursive={dsi.recursive})")
            
            if self.config.dry_run:
                logger.info(f"[DRY RUN] Would create snapshot: {full_name}")
                return None
            
            result = zfs.snapshot(dsi.dataset, snap_name, recursive=dsi.recursive)
            
            if isinstance(result, list):
                snapshot = result[0]
            else:
                snapshot = result
            
            dsi.snapshots.insert(0, SnapshotInfo(snapshot, self.prefix))
            
            logger.info(f"Successfully created snapshot: {full_name}")
            return SnapshotInfo(snapshot, self.prefix)
            
        except Exception as e:
            logger.error(f"Failed to create snapshot {full_name}: {e}")
            return None

    def prune_snapshots(self, dsi: DatasetInfo):
        """Prune snapshots based on retention policy and return list of pruned snapshots."""
        snapshots = self.list_snapshots(dsi, recursive=False)
        
        if not snapshots:
            logger.debug(f"No snapshots to prune for {dsi.name}")
            return []
        
        to_prune = self.needs_prunning(dsi)
        
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
        """Parse timestamp from snapshot name (format: prefix_YYYYMMDDHHMMSS)."""
        try:
            parts = self.name.split('_')
            if len(parts) < 2:
                return None
            
            date_str = parts[1][:8]
            time_str = parts[1][8:]
            
            dt = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
            return dt.replace(tzinfo=timezone.utc)
        except (ValueError, IndexError):
            return None
    
    @property
    def age(self) -> timedelta:
        """Calculate age of this snapshot."""
        if self.timestamp is None:
            return timedelta(0)
        now = datetime.now(timezone.utc)
        return now - self.timestamp
    
    @property
    def is_managed(self) -> bool:
        """Check if this snapshot is managed by our daemon (has our prefix and valid timestamp)."""
        return self.name.startswith(self.prefix + '_') and self.timestamp is not None
    
    def __repr__(self):
        return f"SnapshotInfo({self.full_name}, age={self.age()})"


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
from zfsbackup.config import BackupConfig, DatasetConfig, RetentionRule


logger = logging.getLogger(__name__)


class DatasetInfo:
    """Information about a dataset."""
    
    def __init__(self, dataset: Dataset, config: DatasetConfig):
        self.dataset = dataset
        self.name = dataset.name
        self.config = config
    
    @property
    def reference_time(self) -> datetime:
        return self.reference_time()

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

        if now is None:
            now = datetime.now(timezone.utc)
        elif now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        if self.config.frequency < timedelta(minutes=1):
            return now.replace(microsecond=0)

        if self.config.frequency < timedelta(hours=1):
            return now.replace(second=0, microsecond=0)

        if self.config.frequency < timedelta(days=1):
            return now.replace(minute=0, second=0, microsecond=0)            

        if self.config.frequency > timedelta(years=1):
            return now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        
        if self.config.frequency > timedelta(months=1):
            return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
        return now.replace(hour=0, minute=0, second=0, microsecond=0)

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


class SnapshotManager:
    """Manages ZFS snapshots for datasets."""
    
    def __init__(self, snapshot_prefix: str = "autosnap", dry_run: bool = False):
        self.prefix = snapshot_prefix
        self.dry_run = dry_run
        self.zfs_list = zfs.list
        self.zfs_snapshot = zfs.snapshot
        self.zfs_destroy = zfs.destroy
    
    def generate_snapshot_name(self, dataset_config: DatasetConfig, timestamp: Optional[datetime] = None) -> str:
        """Generate snapshot name with timestamp."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        
        date_str = timestamp.strftime("%Y%m%d")
        time_str = timestamp.strftime("%H%M%S")
        return f"{self.prefix}_{date_str}{time_str}"

    
    def create_snapshot(self, dataset_name: str, snap_name: str, recursive: bool = False) -> Optional[SnapshotInfo]:
        """Create a new snapshot for the dataset."""
        full_name = f"{dataset_name}@{snap_name}"
        
        try:
            logger.info(f"Creating snapshot: {full_name} (recursive={recursive})")
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would create snapshot: {full_name}")
                return None
            
            # Create the dataset object
            dataset = Dataset(dataset_name)
            
            # Create snapshot
            result = self.zfs_snapshot(dataset, snap_name, recursive=recursive)
            
            # If recursive, result is a list
            if isinstance(result, list):
                snapshot = result[0]  # Get the main dataset snapshot
            else:
                snapshot = result
            
            logger.info(f"Successfully created snapshot: {full_name}")
            return SnapshotInfo(snapshot, self.prefix)
            
        except Exception as e:
            logger.error(f"Failed to create snapshot {full_name}: {e}")
            return None
    
    def list_snapshots(self, dataset_name: str, recursive: bool = False) -> List[SnapshotInfo]:
        """List all snapshots for a dataset."""
        try:
            dataset = Dataset(dataset_name)
            
            # List snapshots
            snapshots = self.zfs_list(
                roots=dataset,
                types='snapshot',
                recursive=recursive,
                properties=['creation']
            )
            
            # Filter and wrap in SnapshotInfo
            result = []
            for snap in snapshots:
                if isinstance(snap, ZFSSnapshot):
                    info = SnapshotInfo(snap, self.prefix)
                    if info.is_managed:
                        result.append(info)
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to list snapshots for {dataset_name}: {e}")
            return []
    
    def destroy_snapshot(self, snapshot_info: SnapshotInfo) -> bool:
        """Destroy a snapshot."""
        try:
            logger.info(f"Destroying snapshot: {snapshot_info.full_name}")
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would destroy snapshot: {snapshot_info.full_name}")
                return True
            
            self.zfs_destroy(snapshot_info.snapshot, destroy=True)
            logger.info(f"Successfully destroyed snapshot: {snapshot_info.full_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to destroy snapshot {snapshot_info.full_name}: {e}")
            return False
    
    def needs_snapshot(self, dataset_config: DatasetConfig) -> Tuple[bool, Optional[datetime]]:
        """
        Check if a dataset needs a new snapshot.
        
        Returns:
            Tuple of (needs_snapshot, last_snapshot_time)
        """
        snapshots = self.list_snapshots(dataset_config.name, recursive=False)
        
        if not snapshots:
            return True, None
        
        # Find most recent snapshot
        latest = max(snapshots, key=lambda s: s.timestamp)
        age = latest.age
        
        needs = age >= dataset_config.frequency
        return needs, latest.timestamp
    
    def apply_retention_policy(self, dataset_config: DatasetConfig) -> List[SnapshotInfo]:
        """
        Apply retention policy to snapshots and return list of snapshots to delete.
        
        The retention policy works as follows:
        - For each snapshot, find the applicable retention rule based on its age
        - If snapshot age + keep_for time < now, it should be deleted
        """
        snapshots = self.list_snapshots(dataset_config.name, recursive=dataset_config.recursive)
        
        if not snapshots:
            return []
        
        now = datetime.now(timezone.utc)
        to_delete = []
        
        for snapshot in snapshots:
            age = snapshot.age()
            
            # Find applicable retention rule (largest age that's <= snapshot age)
            applicable_rule = None
            for rule in sorted(dataset_config.retention_rules, key=lambda r: r.age, reverse=True):
                if age >= rule.age:
                    applicable_rule = rule
                    break
            
            # If no rule applies, use the first rule (youngest age threshold)
            if applicable_rule is None and dataset_config.retention_rules:
                applicable_rule = dataset_config.retention_rules[0]
            
            if applicable_rule:
                # Check if snapshot has expired
                expires_at = snapshot.timestamp + applicable_rule.age + applicable_rule.keep_for
                if now >= expires_at:
                    logger.info(
                        f"Snapshot {snapshot.full_name} has expired "
                        f"(age={age}, rule={applicable_rule}, expires_at={expires_at})"
                    )
                    to_delete.append(snapshot)
        
        return to_delete

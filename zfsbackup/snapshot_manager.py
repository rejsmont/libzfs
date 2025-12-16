"""Snapshot manager for creating and managing ZFS snapshots."""

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
import logging
import sys
import os

# Add parent directory to path to import libzfseasy
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from libzfseasy import zfs
from libzfseasy.types import Dataset, Snapshot as ZFSSnapshot
from zfsbackup.config import DatasetConfig, RetentionRule


logger = logging.getLogger(__name__)


class SnapshotInfo:
    """Information about a snapshot with parsed metadata."""
    
    def __init__(self, snapshot: ZFSSnapshot, prefix: str):
        self.snapshot = snapshot
        self.name = snapshot.short
        self.full_name = str(snapshot)
        self.prefix = prefix
        self.timestamp = self._parse_timestamp()
    
    def _parse_timestamp(self) -> Optional[datetime]:
        """Parse timestamp from snapshot name (format: prefix_YYYYMMDD_HHMMSS)."""
        if not self.name.startswith(self.prefix + '_'):
            return None
        
        try:
            # Extract timestamp part after prefix
            parts = self.name.split('_')
            if len(parts) < 3:
                return None
            
            date_str = parts[1]  # YYYYMMDD
            time_str = parts[2]  # HHMMSS
            
            dt = datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M%S")
            # Make timezone-aware (UTC)
            return dt.replace(tzinfo=timezone.utc)
        except (ValueError, IndexError):
            return None
    
    def age(self) -> timedelta:
        """Calculate age of this snapshot."""
        if self.timestamp is None:
            return timedelta(0)
        now = datetime.now(timezone.utc)
        return now - self.timestamp
    
    def is_managed(self) -> bool:
        """Check if this snapshot is managed by our daemon (has our prefix and valid timestamp)."""
        return self.timestamp is not None
    
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
    
    def generate_snapshot_name(self, timestamp: Optional[datetime] = None) -> str:
        """Generate snapshot name with timestamp."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        
        date_str = timestamp.strftime("%Y%m%d")
        time_str = timestamp.strftime("%H%M%S")
        return f"{self.prefix}_{date_str}_{time_str}"
    
    def create_snapshot(self, dataset_name: str, recursive: bool = False) -> Optional[SnapshotInfo]:
        """Create a new snapshot for the dataset."""
        snap_name = self.generate_snapshot_name()
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
                    # Only include snapshots that match our prefix
                    if info.is_managed():
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
        age = latest.age()
        
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

"""Simple test script for the ZFS backup daemon."""

import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from zfsbackup.config import BackupConfig, parse_time_duration
from zfsbackup.snapshot_manager import SnapshotManager
from datetime import timedelta


def test_config_parsing():
    """Test configuration parsing."""
    print("Testing configuration parsing...")
    
    # Test time duration parsing
    assert parse_time_duration("1h") == timedelta(hours=1)
    assert parse_time_duration("2d") == timedelta(days=2)
    assert parse_time_duration("1w") == timedelta(weeks=1)
    assert parse_time_duration("1m") == timedelta(days=30)
    assert parse_time_duration("1y") == timedelta(days=365)
    
    print("✓ Time duration parsing works")
    
    # Test config file loading
    config_path = Path(__file__).parent / "config.example.yaml"
    if config_path.exists():
        config = BackupConfig.from_file(config_path)
        print(f"✓ Loaded config with {len(config.datasets)} datasets")
        
        for ds in config.datasets:
            print(f"  - {ds.name}: frequency={ds.frequency}, rules={len(ds.retention_rules)}")
    else:
        print("⚠ Example config not found")


def test_snapshot_naming():
    """Test snapshot name generation."""
    print("\nTesting snapshot naming...")
    
    manager = SnapshotManager(snapshot_prefix="test", dry_run=True)
    name = manager.generate_snapshot_name()
    
    print(f"✓ Generated snapshot name: {name}")
    assert name.startswith("test_")
    assert len(name.split('_')) == 3


if __name__ == '__main__':
    print("=" * 60)
    print("ZFS Backup Daemon - Basic Tests")
    print("=" * 60)
    
    try:
        test_config_parsing()
        test_snapshot_naming()
        
        print("\n" + "=" * 60)
        print("All basic tests passed! ✓")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

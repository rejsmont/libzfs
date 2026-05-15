"""Simple test script for the ZFS backup daemon."""

import sys
import os
import multiprocessing
from pathlib import Path
from datetime import timedelta

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from zfsbackup.config import BackupConfig, parse_time_duration
from zfsbackup.workers import SnapshotWorker, PruningWorker


def test_config_parsing():
    """Test configuration parsing."""
    print("Testing configuration parsing...")

    assert parse_time_duration("1h") == timedelta(hours=1)
    assert parse_time_duration("2d") == timedelta(days=2)
    assert parse_time_duration("1w") == timedelta(weeks=1)
    assert parse_time_duration("1M") == timedelta(days=30)
    assert parse_time_duration("1y") == timedelta(days=365)

    print("✓ Time duration parsing works")

    config_path = Path(__file__).parent / "config.example.yaml"
    if config_path.exists():
        config = BackupConfig.from_file(config_path)
        print(f"✓ Loaded config with {len(config.datasets)} datasets")
        print(f"  check_interval={config.check_interval}, prune_interval={config.prune_interval}")
        assert config.prune_interval > timedelta(0), "prune_interval must be positive"
        for ds in config.datasets:
            print(f"  - {ds.name}: frequency={ds.frequency}, rules={len(ds.retention_rules)}")
    else:
        print("⚠ Example config not found")


def test_worker_instantiation():
    """Test that workers can be instantiated with a shared stop event."""
    print("\nTesting worker instantiation...")

    config_path = Path(__file__).parent / "config.example.yaml"
    stop_event = multiprocessing.Event()

    sw = SnapshotWorker(config_path, stop_event, dry_run=True)
    assert sw.name == 'snapshot-worker'
    assert sw.daemon is True

    pw = PruningWorker(config_path, stop_event, dry_run=True)
    assert pw.name == 'pruning-worker'
    assert pw.daemon is True

    print("✓ SnapshotWorker and PruningWorker instantiate correctly")


if __name__ == '__main__':
    print("=" * 60)
    print("ZFS Backup Daemon - Basic Tests")
    print("=" * 60)

    try:
        test_config_parsing()
        test_worker_instantiation()

        print("\n" + "=" * 60)
        print("All basic tests passed! ✓")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

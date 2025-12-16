#!/usr/bin/env python3
"""ZFS Backup Daemon - Main entry point."""

import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from zfsbackup.config import BackupConfig, DatasetConfig
from zfsbackup.snapshot_manager import SnapshotManager


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class BackupDaemon:
    """Main backup daemon that manages snapshots for multiple datasets."""
    
    def __init__(self, config: BackupConfig):
        self.config = config
        self.manager = SnapshotManager(
            snapshot_prefix=config.snapshot_prefix,
            dry_run=config.dry_run
        )
        self.running = False
        self.last_snapshot_times: Dict[str, Optional[datetime]] = {}
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        sig_name = 'SIGINT' if signum == signal.SIGINT else 'SIGTERM'
        logger.info(f"Received {sig_name}, shutting down gracefully...")
        self.running = False
    
    def _process_dataset(self, dataset_config: DatasetConfig):
        """Process a single dataset: check if snapshot needed and apply retention."""
        logger.debug(f"Processing dataset: {dataset_config.name}")
        
        # Check if we need to create a new snapshot
        needs_snap, last_snap_time = self.manager.needs_snapshot(dataset_config)
        
        if needs_snap:
            logger.info(
                f"Dataset {dataset_config.name} needs snapshot "
                f"(frequency={dataset_config.frequency}, last={last_snap_time})"
            )
            snapshot = self.manager.create_snapshot(
                dataset_config.name,
                recursive=dataset_config.recursive
            )
            if snapshot:
                self.last_snapshot_times[dataset_config.name] = snapshot.timestamp
        else:
            logger.debug(
                f"Dataset {dataset_config.name} does not need snapshot yet "
                f"(last snapshot: {last_snap_time})"
            )
        
        # Apply retention policy
        expired_snapshots = self.manager.apply_retention_policy(dataset_config)
        
        if expired_snapshots:
            logger.info(
                f"Found {len(expired_snapshots)} expired snapshots for {dataset_config.name}"
            )
            for snapshot in expired_snapshots:
                self.manager.destroy_snapshot(snapshot)
        else:
            logger.debug(f"No expired snapshots for {dataset_config.name}")
    
    def _run_cycle(self):
        """Run one complete cycle: process all enabled datasets."""
        enabled_datasets = self.config.get_enabled_datasets()
        
        if not enabled_datasets:
            logger.warning("No enabled datasets configured")
            return
        
        logger.info(f"Running backup cycle for {len(enabled_datasets)} datasets")
        
        for dataset_config in enabled_datasets:
            try:
                self._process_dataset(dataset_config)
            except Exception as e:
                logger.error(
                    f"Error processing dataset {dataset_config.name}: {e}",
                    exc_info=True
                )
    
    def run(self):
        """Main daemon loop."""
        self.running = True
        
        logger.info("=" * 60)
        logger.info("ZFS Backup Daemon Starting")
        logger.info("=" * 60)
        logger.info(f"Config: {len(self.config.datasets)} datasets configured")
        logger.info(f"Snapshot prefix: {self.config.snapshot_prefix}")
        logger.info(f"Check interval: {self.config.check_interval}")
        logger.info(f"Dry run mode: {self.config.dry_run}")
        logger.info("=" * 60)
        
        for ds in self.config.get_enabled_datasets():
            logger.info(f"  Dataset: {ds.name}")
            logger.info(f"    Frequency: {ds.frequency}")
            logger.info(f"    Recursive: {ds.recursive}")
            logger.info(f"    Retention rules: {len(ds.retention_rules)}")
            for rule in ds.retention_rules:
                logger.info(f"      - Age {rule.age} -> Keep for {rule.keep_for}")
        
        logger.info("=" * 60)
        logger.info("Starting main loop...")
        logger.info("=" * 60)
        
        # Run initial cycle
        self._run_cycle()
        
        last_check = time.time()
        sleep_seconds = self.config.check_interval.total_seconds()
        
        # Main loop
        while self.running:
            try:
                if self.running and (time.time() - last_check) >= sleep_seconds:
                    last_check = time.time()
                    self._run_cycle()
                else:
                    break
                time.sleep(0.25)
            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                time.sleep(60)  # Sleep for a minute before retrying
        logger.info("Daemon stopped")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='ZFS Backup Daemon - Automated snapshot management with retention policies'
    )
    parser.add_argument(
        '-c', '--config',
        type=Path,
        default=Path('/etc/zfsbackup/config.yaml'),
        help='Path to configuration file (default: /etc/zfsbackup/config.yaml)'
    )
    parser.add_argument(
        '-d', '--dry-run',
        action='store_true',
        help='Dry run mode - do not create or destroy snapshots'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose (debug) logging'
    )
    parser.add_argument(
        '--test-config',
        action='store_true',
        help='Test configuration and exit'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    # Load configuration
    try:
        logger.info(f"Loading configuration from: {args.config}")
        config = BackupConfig.from_file(args.config)
        
        # Override dry-run from command line
        if args.dry_run:
            config.dry_run = True
        
        if args.test_config:
            logger.info("Configuration is valid!")
            logger.info(f"Datasets: {len(config.datasets)}")
            for ds in config.datasets:
                logger.info(f"  - {ds.name} (enabled={ds.enabled})")
            return 0
        
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return 1
    
    # Start daemon
    try:
        daemon = BackupDaemon(config)
        daemon.run()
        return 0
    except Exception as e:
        logger.error(f"Daemon failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())

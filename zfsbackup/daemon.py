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

from zfsbackup.config import BackupConfig
from zfsbackup.backup_manager import DatasetInfo, DatasetManager


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
        self.manager = DatasetManager(config)
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        sig_name = 'SIGINT' if signum == signal.SIGINT else 'SIGTERM'
        logger.info(f"Received {sig_name}, shutting down gracefully...")
        self.running = False
    
    def _process_dataset(self, dataset: DatasetInfo):
        """Process a single dataset: check if snapshot needed and apply retention."""
        logger.debug(f"Processing dataset: {dataset.name}")

        if self.manager.needs_snapshot(dataset):
            logger.info(
                f"Dataset {dataset.name} needs snapshot "
            )
            self.manager.create_snapshot(dataset)
        self.manager.prune_snapshots(dataset)
    
    def _run_cycle(self):
        """Run one complete cycle: process all enabled datasets."""
        enabled_datasets = self.manager.datasets
        
        if not enabled_datasets:
            logger.warning("No enabled datasets configured")
            return
        
        logger.info(f"Running backup cycle for {len(enabled_datasets)} datasets")
        
        for dataset in enabled_datasets:
            try:
                self._process_dataset(dataset)
            except Exception as e:
                logger.error(
                    f"Error processing dataset {dataset.name}: {e}",
                    exc_info=True
                )
    
    def run(self):
        """Main daemon loop."""
        self.running = True
        
        logger.info("=" * 60)
        logger.info("ZFS Backup Daemon Starting")
        logger.info("=" * 60)

        self.manager.dataset_report()
        self.manager.verify_datasets()
        
        logger.info("=" * 60)
        logger.info("Starting main loop...")
        logger.info("=" * 60)

        self._run_cycle()
        
        last_check = time.time()
        sleep_seconds = self.config.check_interval.total_seconds()
        
        # Main loop
        while self.running:
            try:
                if self.running and (time.time() - last_check) >= sleep_seconds:
                    last_check = time.time()
                    self._run_cycle()
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

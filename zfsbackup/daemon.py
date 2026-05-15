#!/usr/bin/env python3
"""ZFS Backup Daemon - Main entry point."""

import argparse
import logging
import multiprocessing
import signal
import sys
import time
from pathlib import Path
from typing import Dict

from zfsbackup.config import BackupConfig
from zfsbackup.backup_manager import DatasetManager
from zfsbackup.workers import SnapshotWorker, PruningWorker, ApiWorker


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

WORKER_RESTART_DELAY = 5   # seconds to wait before restarting a crashed worker
SUPERVISOR_POLL = 5        # seconds between liveness checks


class BackupDaemon:
    """Supervisor process: starts and monitors the snapshot and pruning workers."""

    def __init__(self, config: BackupConfig, config_path: Path, verbose: bool = False):
        self.config = config
        self.config_path = config_path
        self.verbose = verbose
        self._stop_event = multiprocessing.Event()
        self._workers: Dict[str, multiprocessing.Process] = {}
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        sig_name = 'SIGINT' if signum == signal.SIGINT else 'SIGTERM'
        logger.info(f"Received {sig_name}, shutting down gracefully...")
        self._stop_event.set()

    def _new_worker(self, name: str) -> multiprocessing.Process:
        cls = {'snapshot': SnapshotWorker, 'pruning': PruningWorker, 'api': ApiWorker}[name]
        return cls(self.config_path, self._stop_event, self.config.dry_run, self.verbose)

    def _start_workers(self) -> None:
        for name in ('snapshot', 'pruning', 'api'):
            worker = self._new_worker(name)
            worker.start()
            logger.info(f"Started {name} worker (pid={worker.pid})")
            self._workers[name] = worker

    def _check_workers(self) -> None:
        for name, worker in list(self._workers.items()):
            if not worker.is_alive():
                logger.error(
                    f"{name} worker (pid={worker.pid}) exited with code "
                    f"{worker.exitcode}, restarting in {WORKER_RESTART_DELAY}s"
                )
                time.sleep(WORKER_RESTART_DELAY)
                new_worker = self._new_worker(name)
                new_worker.start()
                logger.info(f"Restarted {name} worker (pid={new_worker.pid})")
                self._workers[name] = new_worker

    def _shutdown_workers(self, timeout: int = 30) -> None:
        for name, worker in self._workers.items():
            worker.join(timeout=timeout)
            if worker.is_alive():
                logger.warning(f"{name} worker did not exit in {timeout}s, terminating")
                worker.terminate()
                worker.join(timeout=5)

    def run(self) -> None:
        logger.info("=" * 60)
        logger.info("ZFS Backup Daemon Starting")
        logger.info("=" * 60)

        manager = DatasetManager(self.config)
        manager.dataset_report()
        manager.verify_datasets()

        logger.info("=" * 60)
        logger.info("Starting workers...")
        logger.info("=" * 60)

        self._start_workers()

        while not self._stop_event.is_set():
            time.sleep(SUPERVISOR_POLL)
            if not self._stop_event.is_set():
                self._check_workers()

        logger.info("Shutting down workers...")
        self._shutdown_workers()
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

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    try:
        logger.info(f"Loading configuration from: {args.config}")
        config = BackupConfig.from_file(args.config)

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

    try:
        daemon = BackupDaemon(config, args.config, verbose=args.verbose)
        daemon.run()
        return 0
    except Exception as e:
        logger.error(f"Daemon failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())

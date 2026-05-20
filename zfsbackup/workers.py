"""Worker processes for snapshot creation, pruning, API serving, and remote backup."""

import logging
import sys
import threading
from abc import abstractmethod
from pathlib import Path
from multiprocessing import Process
from multiprocessing.synchronize import Event
from typing import List

from zfsbackup.backup_manager import DatasetInfo, DatasetManager
from zfsbackup.config import BackupConfig


def _setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)],
    )


class BaseWorker(Process):
    """Base class for daemon worker processes with a shared polling loop."""

    def __init__(
        self,
        worker_name: str,
        config_path: Path,
        stop_event: Event,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        super().__init__(name=worker_name, daemon=True)
        self.config_path = config_path
        self.stop_event = stop_event
        self.dry_run = dry_run
        self.verbose = verbose

    @abstractmethod
    def _get_interval(self, config: BackupConfig) -> float:
        """Return the loop sleep interval in seconds."""

    @abstractmethod
    def _process_dataset(self, manager: DatasetManager, dsi: DatasetInfo) -> None:
        """Process a single dataset."""

    def _get_datasets(self, manager: DatasetManager) -> List[DatasetInfo]:
        """Return the list of datasets to iterate each cycle. Override to extend."""
        return manager.datasets

    def _before_loop(self, config: BackupConfig, manager: DatasetManager) -> None:
        """Called once after setup but before the main loop. Override for per-run init."""

    def run(self) -> None:
        _setup_logging(self.verbose)
        logger = logging.getLogger(__name__)
        logger.info(f"{self.name} started")

        try:
            config = BackupConfig.from_file(self.config_path)
            if self.dry_run:
                config.dry_run = True
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return

        manager = DatasetManager(config)
        interval = self._get_interval(config)
        self._before_loop(config, manager)

        while not self.stop_event.is_set():
            logger.debug(f"{self.name} cycle")
            for dsi in self._get_datasets(manager):
                if self.stop_event.is_set():
                    break
                try:
                    self._process_dataset(manager, dsi)
                except Exception as e:
                    logger.error(f"{self.name} error on {dsi.name}: {e}", exc_info=True)
            self.stop_event.wait(timeout=interval)

        logger.info(f"{self.name} stopped")


class SnapshotWorker(BaseWorker):
    """Worker that periodically checks all datasets and creates snapshots as needed."""

    def __init__(
        self,
        config_path: Path,
        stop_event: Event,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        super().__init__('snapshot-worker', config_path, stop_event, dry_run, verbose)

    def _get_interval(self, config: BackupConfig) -> float:
        return config.check_interval.total_seconds()

    def _process_dataset(self, manager: DatasetManager, dsi: DatasetInfo) -> None:
        logger = logging.getLogger(__name__)
        if manager.needs_snapshot(dsi):
            logger.info(f"Dataset {dsi.name} needs snapshot")
            manager.create_snapshot(dsi)


class PruningWorker(BaseWorker):
    """Worker that periodically applies retention policy and prunes old snapshots."""

    def __init__(
        self,
        config_path: Path,
        stop_event: Event,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        super().__init__('pruning-worker', config_path, stop_event, dry_run, verbose)

    def _get_interval(self, config: BackupConfig) -> float:
        return config.prune_interval.total_seconds()

    def _get_datasets(self, manager: DatasetManager) -> List[DatasetInfo]:
        return manager.datasets + manager.received_datasets()

    def _process_dataset(self, manager: DatasetManager, dsi: DatasetInfo) -> None:
        manager.prune_snapshots(dsi)


class RemoteBackupWorker(BaseWorker):
    """Worker that periodically sends snapshots to configured remote destinations."""

    def __init__(
        self,
        config_path: Path,
        stop_event: Event,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        super().__init__('remote-backup-worker', config_path, stop_event, dry_run, verbose)
        self._remote_manager = None

    def _get_interval(self, config: BackupConfig) -> float:
        freqs = [
            (r.frequency or ds.frequency).total_seconds()
            for ds in config.enabled_datasets
            for r in ds.remote
        ]
        return min(freqs) if freqs else config.check_interval.total_seconds()

    def _before_loop(self, config: BackupConfig, manager: DatasetManager) -> None:
        from zfsbackup.remote import RemoteBackupManager
        self._remote_manager = RemoteBackupManager(config, manager)

    def _process_dataset(self, manager: DatasetManager, dsi: DatasetInfo) -> None:
        if not dsi.config.remote:
            return
        logger = logging.getLogger(__name__)
        for remote_cfg in dsi.config.remote:
            freq = (remote_cfg.frequency or dsi.config.frequency).total_seconds()
            anchor_name = manager.get_anchor(dsi, remote_cfg.destination)
            if anchor_name:
                snapshots = manager.list_snapshots(dsi)
                anchor = next((s for s in snapshots if s.name == anchor_name), None)
                if anchor and anchor.age.total_seconds() < freq:
                    logger.debug(
                        f"Backup of {dsi.name} to {remote_cfg.destination} not due yet"
                    )
                    continue
            self._remote_manager.backup_dataset(dsi, remote_cfg)


class ApiWorker(Process):
    """Worker that serves a read-only HTTP API for configuration and snapshot inspection."""

    def __init__(
        self,
        config_path: Path,
        stop_event: Event,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        super().__init__(name='api-worker', daemon=True)
        self.config_path = config_path
        self.stop_event = stop_event
        self.dry_run = dry_run
        self.verbose = verbose

    def run(self) -> None:
        _setup_logging(self.verbose)
        logger = logging.getLogger(__name__)
        logger.info("API worker started")

        try:
            config = BackupConfig.from_file(self.config_path)
            if self.dry_run:
                config.dry_run = True
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return

        from zfsbackup.api import create_app
        app = create_app(config)

        thread = threading.Thread(
            target=app.run,
            kwargs={'host': config.api_host, 'port': config.api_port, 'use_reloader': False},
            daemon=True,
        )
        thread.start()
        logger.info(f"API listening on {config.api_host}:{config.api_port}")

        self.stop_event.wait()
        logger.info("API worker stopped")

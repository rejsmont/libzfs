"""Basic smoke tests for the zfsbackup package (migrated from zfsbackup/test_basic.py)."""

import logging
import multiprocessing
from datetime import timedelta
from pathlib import Path

import pytest

from zfsbackup.config import BackupConfig, parse_time_duration
from zfsbackup.workers import SnapshotWorker, PruningWorker


class TestTimeDurationParsing:
    def test_hours(self):
        assert parse_time_duration("1h") == timedelta(hours=1)

    def test_days(self):
        assert parse_time_duration("2d") == timedelta(days=2)

    def test_weeks(self):
        assert parse_time_duration("1w") == timedelta(weeks=1)

    def test_months(self):
        assert parse_time_duration("1M") == timedelta(days=30)

    def test_years(self):
        assert parse_time_duration("1y") == timedelta(days=365)


class TestWorkerInstantiation:
    def test_snapshot_worker_name(self, worker_config):
        # Workers take the supervisor's detached `BackupConfig` (item 15.2(b))
        # -- never a database path. This only constructs the worker and reads
        # `.name`/`.daemon`.
        stop_event = multiprocessing.Event()
        sw = SnapshotWorker(worker_config, stop_event)
        assert sw.name == 'snapshot-worker'
        assert sw.daemon is True
        assert sw.config is worker_config

    def test_pruning_worker_name(self, worker_config):
        stop_event = multiprocessing.Event()
        pw = PruningWorker(worker_config, stop_event, log_level=logging.DEBUG)
        assert pw.name == 'pruning-worker'
        assert pw.daemon is True
        assert pw.config is worker_config
        assert pw.log_level == logging.DEBUG

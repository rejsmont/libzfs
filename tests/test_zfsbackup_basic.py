"""Basic smoke tests for the zfsbackup package (migrated from zfsbackup/test_basic.py)."""

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
    def test_snapshot_worker_name(self, config_yaml_path):
        stop_event = multiprocessing.Event()
        sw = SnapshotWorker(config_yaml_path, stop_event, dry_run=True)
        assert sw.name == 'snapshot-worker'
        assert sw.daemon is True

    def test_pruning_worker_name(self, config_yaml_path):
        stop_event = multiprocessing.Event()
        pw = PruningWorker(config_yaml_path, stop_event, dry_run=True)
        assert pw.name == 'pruning-worker'
        assert pw.daemon is True

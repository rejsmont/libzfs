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
    def test_snapshot_worker_name(self, sqlite_config_db):
        # `sqlite_config_db` (item 8): a genuine `ResolvedConfigPath`
        # addressing an imported SQLite config store, not the `config_
        # yaml_path` `Path` this test used before the daemon started
        # reading the config database. This test only constructs the
        # worker and reads `.name`/`.daemon` -- it never loads config --
        # so it passed even with a bare `Path` before this fix; it was
        # "passing by accident" against a type the constructor no longer
        # accepts anywhere a real load happens.
        stop_event = multiprocessing.Event()
        sw = SnapshotWorker(sqlite_config_db, stop_event, dry_run=True)
        assert sw.name == 'snapshot-worker'
        assert sw.daemon is True

    def test_pruning_worker_name(self, sqlite_config_db):
        stop_event = multiprocessing.Event()
        pw = PruningWorker(sqlite_config_db, stop_event, dry_run=True)
        assert pw.name == 'pruning-worker'
        assert pw.daemon is True

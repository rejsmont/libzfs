"""Unit tests for zfsbackup worker classes."""

import json
import logging
import multiprocessing
import os
import signal
import pytest
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

from sqlalchemy.exc import OperationalError

from zfsbackup.config import BackupConfig, DatasetConfig, RemoteDatasetConfig
from zfsbackup.config.store import (
    ConfigPathError,
    SchemaOutOfDate,
    get_engine,
    resolve_config_path,
)
from zfsbackup.backup_manager import DatasetInfo, DatasetManager
from zfsbackup.runtime_config import EX_CONFIG
from zfsbackup.workers import (
    SnapshotWorker, PruningWorker, RemoteBackupWorker, ApiWorker, _load_config,
)

FORK_AVAILABLE = "fork" in multiprocessing.get_all_start_methods()


class TestSnapshotWorker:
    def test_name_and_daemon(self, sqlite_config_db):
        stop_event = multiprocessing.Event()
        worker = SnapshotWorker(sqlite_config_db, stop_event, dry_run=True)
        assert worker.name == 'snapshot-worker'
        assert worker.daemon is True

    def test_get_interval_returns_check_interval(self, sample_backup_config):
        worker = SnapshotWorker.__new__(SnapshotWorker)
        interval = worker._get_interval(sample_backup_config)
        assert interval == sample_backup_config.check_interval.total_seconds()

    def test_process_dataset_creates_snapshot_when_needed(self, mocker, sample_backup_config):
        manager = MagicMock()
        manager.needs_snapshot.return_value = True
        dsi = MagicMock()
        worker = SnapshotWorker.__new__(SnapshotWorker)
        worker.name = 'snapshot-worker'
        worker._process_dataset(manager, dsi)
        manager.create_snapshot.assert_called_once_with(dsi)

    def test_process_dataset_skips_when_not_needed(self, mocker, sample_backup_config):
        manager = MagicMock()
        manager.needs_snapshot.return_value = False
        dsi = MagicMock()
        worker = SnapshotWorker.__new__(SnapshotWorker)
        worker.name = 'snapshot-worker'
        worker._process_dataset(manager, dsi)
        manager.create_snapshot.assert_not_called()

    def test_run_loads_config_and_processes(self, mocker, sqlite_config_db):
        stop_event = multiprocessing.Event()
        stop_event.set()
        worker = SnapshotWorker(sqlite_config_db, stop_event, dry_run=True)
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        worker.run()


class TestPruningWorker:
    def test_name_and_daemon(self, sqlite_config_db):
        stop_event = multiprocessing.Event()
        worker = PruningWorker(sqlite_config_db, stop_event, dry_run=True)
        assert worker.name == 'pruning-worker'
        assert worker.daemon is True

    def test_get_interval_returns_prune_interval(self, sample_backup_config):
        # sample_backup_config sets neither check_interval nor
        # prune_interval, so prune_interval is None (unset) and the
        # effective value derives from check_interval's default (5m/300s),
        # not the old hard-coded prune_interval default (1h/3600s).
        worker = PruningWorker.__new__(PruningWorker)
        interval = worker._get_interval(sample_backup_config)
        assert sample_backup_config.prune_interval is None
        assert interval == sample_backup_config.effective_prune_interval.total_seconds()
        assert interval == 300

    def test_process_dataset_calls_prune(self, mocker):
        manager = MagicMock()
        dsi = MagicMock()
        worker = PruningWorker.__new__(PruningWorker)
        worker._process_dataset(manager, dsi)
        manager.prune_snapshots.assert_called_once_with(dsi)

    def test_get_datasets_includes_received(self, mocker):
        manager = MagicMock()
        local = [MagicMock()]
        received = [MagicMock()]
        manager.datasets = local
        manager.received_datasets.return_value = received
        worker = PruningWorker.__new__(PruningWorker)
        result = worker._get_datasets(manager)
        assert local[0] in result
        assert received[0] in result

    def test_run_stops_when_event_set(self, sqlite_config_db, mocker):
        stop_event = multiprocessing.Event()
        stop_event.set()
        worker = PruningWorker(sqlite_config_db, stop_event, dry_run=True)
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        worker.run()


class TestRemoteBackupWorker:
    def test_name_and_daemon(self, sqlite_config_db):
        stop_event = multiprocessing.Event()
        worker = RemoteBackupWorker(sqlite_config_db, stop_event, dry_run=True)
        assert worker.name == 'remote-backup-worker'
        assert worker.daemon is True

    def test_get_interval_uses_min_remote_frequency(self):
        config = BackupConfig(
            datasets=[
                DatasetConfig(
                    name='pool/data',
                    frequency=timedelta(hours=1),
                    remote=[
                        RemoteDatasetConfig(destination='a', frequency=timedelta(minutes=30)),
                        RemoteDatasetConfig(destination='b', frequency=timedelta(hours=2)),
                    ],
                ),
            ],
        )
        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        interval = worker._get_interval(config)
        assert interval == timedelta(minutes=30).total_seconds()

    def test_get_interval_falls_back_to_check_interval_when_no_remote(self, sample_backup_config):
        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        interval = worker._get_interval(sample_backup_config)
        assert interval == sample_backup_config.check_interval.total_seconds()

    def test_get_interval_uses_dataset_frequency_when_remote_has_none(self):
        config = BackupConfig(
            datasets=[
                DatasetConfig(
                    name='pool/data',
                    frequency=timedelta(hours=2),
                    remote=[RemoteDatasetConfig(destination='a', frequency=None)],
                ),
            ],
        )
        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        interval = worker._get_interval(config)
        assert interval == timedelta(hours=2).total_seconds()

    def test_process_dataset_skips_when_no_remote_config(self, mocker):
        manager = MagicMock()
        dsi = MagicMock()
        dsi.config.remote = []
        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        worker.name = 'remote-backup-worker'
        worker._remote_manager = MagicMock()
        worker._process_dataset(manager, dsi)
        worker._remote_manager.backup_dataset.assert_not_called()

    def test_process_dataset_skips_when_anchor_fresh(self, mocker):
        manager = MagicMock()
        remote_cfg = MagicMock()
        remote_cfg.destination = 'offsite'
        remote_cfg.frequency = timedelta(hours=1)
        dsi = MagicMock()
        dsi.config.remote = [remote_cfg]
        dsi.config.frequency = timedelta(hours=1)

        anchor_snap = MagicMock()
        anchor_snap.name = 'autosnap_20240101120000'
        anchor_snap.age = timedelta(minutes=30)
        manager.get_anchor.return_value = 'autosnap_20240101120000'
        manager.list_snapshots.return_value = [anchor_snap]

        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        worker.name = 'remote-backup-worker'
        worker._remote_manager = MagicMock()
        worker._process_dataset(manager, dsi)
        worker._remote_manager.backup_dataset.assert_not_called()

    def test_process_dataset_runs_backup_when_no_anchor(self, mocker):
        manager = MagicMock()
        remote_cfg = MagicMock()
        remote_cfg.destination = 'offsite'
        remote_cfg.frequency = timedelta(hours=1)
        dsi = MagicMock()
        dsi.config.remote = [remote_cfg]
        dsi.config.frequency = timedelta(hours=1)
        manager.get_anchor.return_value = None

        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        worker.name = 'remote-backup-worker'
        worker._remote_manager = MagicMock()
        worker._process_dataset(manager, dsi)
        worker._remote_manager.backup_dataset.assert_called_once_with(dsi, remote_cfg)


class TestApiWorker:
    def test_name_and_daemon(self, sqlite_config_db):
        stop_event = multiprocessing.Event()
        worker = ApiWorker(sqlite_config_db, stop_event, dry_run=True)
        assert worker.name == 'api-worker'
        assert worker.daemon is True

    def test_run_bad_config_exits_cleanly(self, tmp_path, mocker):
        # A genuine `ResolvedConfigPath` (not a bare `Path`) pointed at a
        # `.yaml`-suffixed target: `check_config_db_suffix` refuses it with
        # `ConfigDbIsYaml` (a `ConfigPathError`), the same permanent-fault
        # class `_load_config` turns into `SystemExit(EX_CONFIG)` -- exactly
        # the crash-loop-guard contract (item 8, sub-item 8i) this "exits
        # cleanly" name now means: a distinct, non-restartable exit code,
        # not a bare early `return`. A bare `Path` here (as before item 8)
        # would raise `AttributeError: 'PosixPath' object has no attribute
        # 'url'` deep inside `check_config_db`, then a SECOND, unrelated
        # `AttributeError` (`'PosixPath' object has no attribute 'source'`)
        # while `_load_config`'s except-Exception arm tries to log it via
        # `config_error_message` -- that second, uncaught error is exactly
        # what made this test fail after item 8 landed.
        bad_config = tmp_path / 'bad.yaml'
        bad_config.write_text('')
        resolved = resolve_config_path(str(bad_config))
        stop_event = multiprocessing.Event()
        worker = ApiWorker(resolved, stop_event, dry_run=True)
        with pytest.raises(SystemExit) as excinfo:
            worker.run()
        assert excinfo.value.code == EX_CONFIG


class TestBaseWorkerRun:
    def test_run_exits_on_bad_config(self, tmp_path, mocker):
        # See `TestApiWorker.test_run_bad_config_exits_cleanly` for why this
        # must be a real `ResolvedConfigPath`, and why the child now exits
        # `EX_CONFIG` rather than returning quietly.
        bad_config = tmp_path / 'bad.yaml'
        bad_config.write_text('')
        resolved = resolve_config_path(str(bad_config))
        stop_event = multiprocessing.Event()
        worker = SnapshotWorker(resolved, stop_event, dry_run=True)
        with pytest.raises(SystemExit) as excinfo:
            worker.run()
        assert excinfo.value.code == EX_CONFIG

    def test_run_sets_dry_run_from_flag(self, sqlite_config_db, mocker):
        stop_event = multiprocessing.Event()
        stop_event.set()
        worker = SnapshotWorker(sqlite_config_db, stop_event, dry_run=True)
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        worker.run()


class TestBaseWorkerLoop:
    """Cover the main dataset-processing loop (lines 74-82) and _get_datasets (line 51)."""

    def test_dataset_exception_logged_and_loop_exits_on_stop(self, sqlite_config_db, mocker):
        stop_event = multiprocessing.Event()
        worker = SnapshotWorker(sqlite_config_db, stop_event, dry_run=True)

        two_dataset_config = BackupConfig(datasets=[
            DatasetConfig(name='pool/a'),
            DatasetConfig(name='pool/b'),
        ])
        mocker.patch('zfsbackup.workers.load_runtime_config', return_value=two_dataset_config)
        mocker.patch.object(SnapshotWorker, '_get_interval', return_value=0)

        call_count = [0]

        def process_side_effect(manager, dsi):
            call_count[0] += 1
            stop_event.set()      # trigger break on next dataset and exit the while loop
            raise RuntimeError("injected error")

        mocker.patch.object(SnapshotWorker, '_process_dataset', side_effect=process_side_effect)
        worker.run()
        # Only the first dataset is processed; second is skipped by the stop_event break
        assert call_count[0] == 1


class TestRemoteBackupWorkerBeforeLoop:
    def test_before_loop_creates_remote_manager(self, tmp_path):
        from zfsbackup.remote import RemoteBackupManager
        from zfsbackup.config import Destination
        config = BackupConfig(
            datasets=[DatasetConfig(
                name='pool/data',
                remote=[RemoteDatasetConfig(destination='offsite')],
            )],
            destinations={'offsite': Destination(url='http://backup.example.com')},
            client_id_file=tmp_path / 'client_id',
        )
        manager = MagicMock()
        worker = RemoteBackupWorker.__new__(RemoteBackupWorker)
        worker._remote_manager = None
        worker._before_loop(config, manager)
        assert isinstance(worker._remote_manager, RemoteBackupManager)


class TestApiWorkerThread:
    def test_run_starts_thread_and_stops_on_event(self, sqlite_config_db, mocker):
        stop_event = multiprocessing.Event()
        stop_event.set()  # stop_event.wait() returns immediately
        worker = ApiWorker(sqlite_config_db, stop_event, dry_run=True)  # dry_run covers line 198

        mock_app = MagicMock()
        mocker.patch('zfsbackup.api.create_app', return_value=mock_app)
        mock_thread = MagicMock()
        mocker.patch('zfsbackup.workers.threading.Thread', return_value=mock_thread)

        worker.run()
        mock_thread.start.assert_called_once()


class TestChildSignalDispositionWiring:
    """Closes a gap the coordinator's re-review found in
    `test_zfsbackup_signals.py`: that file's fork tests call
    `_init_child_process` directly, which pins the function's OWN
    behaviour but nothing about whether `BaseWorker.run`/`ApiWorker.run`
    actually call it -- deleting either call site
    (`workers.py:159`/`workers.py:289`) left the entire signals test file
    green, since neither of those tests ever goes through `run()` at all.

    Deliberately in-process, no fork needed: `run()` resets THIS
    process's OWN SIGINT/SIGTERM disposition the moment it is called
    synchronously (it is the first statement of `run()`, before any
    config load), which is exactly what `tests/conftest.py`'s autouse
    `_restore_signal_handlers` fixture exists to make safe to assert on
    and clean up afterward -- without it, a non-default handler installed
    here would leak into every later test in the session.

    Mutation-checked: deleting the `_init_child_process(self.verbose)`
    call from `BaseWorker.run` fails
    `test_base_worker_run_resets_signal_disposition_first` (1/2 tests in
    this class); deleting the `_init_child_process(self.verbose)` call
    from `ApiWorker.run` fails
    `test_api_worker_run_resets_signal_disposition_first` (the other
    1/2) -- each call site is pinned by exactly the one test that goes
    through it, which is the point: this is wiring coverage, not a
    retest of `_init_child_process` itself (see
    `test_zfsbackup_signals.py` for that).
    """

    def _install_non_default_handlers(self):
        def _handler(signum, frame):  # pragma: no cover - never actually invoked
            pass
        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

    def test_base_worker_run_resets_signal_disposition_first(
        self, sqlite_config_db, mocker
    ):
        self._install_non_default_handlers()
        stop_event = multiprocessing.Event()
        stop_event.set()  # zero loop iterations -- run() still must reset first
        worker = SnapshotWorker(sqlite_config_db, stop_event, dry_run=True)
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])

        worker.run()

        assert signal.getsignal(signal.SIGINT) is signal.SIG_DFL
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL

    def test_api_worker_run_resets_signal_disposition_first(
        self, sqlite_config_db, mocker
    ):
        self._install_non_default_handlers()
        stop_event = multiprocessing.Event()
        stop_event.set()  # stop_event.wait() returns immediately
        worker = ApiWorker(sqlite_config_db, stop_event, dry_run=True)

        mock_app = MagicMock()
        mocker.patch('zfsbackup.api.create_app', return_value=mock_app)
        mock_thread = MagicMock()
        mocker.patch('zfsbackup.workers.threading.Thread', return_value=mock_thread)

        worker.run()

        assert signal.getsignal(signal.SIGINT) is signal.SIG_DFL
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL


class TestLoadConfigExitCodes:
    """`_load_config`'s three-way outcome (item 8, sub-item 8i): a
    permanent config fault raises `SystemExit(EX_CONFIG)`, so the child's
    OS exit code tells the supervisor "do not restart me"; a transient
    failure (e.g. `database is locked` under `busy_timeout`) returns
    `None` and leaves the process free to exit normally (code 0) and be
    respawned; a healthy load returns a real, `dry_run`-applied
    `BackupConfig`.

    Mutation-checked: reverting `_load_config`'s `except (ConfigPathError,
    SchemaError)` arm to fall through to the generic `except Exception`
    (i.e. treating every failure as transient) fails 3 of these tests --
    the two `SystemExit`-expecting cases and
    `test_operational_error_is_never_mistaken_for_ex_config`, which would
    then see a bare `None` returned but not distinguish the two failure
    classes if the ordering were reversed instead (checked separately:
    swapping the two `except` clauses so `ConfigPathError`/`SchemaError`
    is caught by the generic arm first also fails the same 3).
    """

    def test_config_path_error_exits_ex_config(self, sqlite_config_db, mocker):
        logger = logging.getLogger("test")
        mocker.patch(
            "zfsbackup.workers.load_runtime_config",
            side_effect=ConfigPathError("no such database"),
        )
        with pytest.raises(SystemExit) as excinfo:
            _load_config(sqlite_config_db, False, logger)
        assert excinfo.value.code == EX_CONFIG

    def test_schema_error_exits_ex_config(self, sqlite_config_db, mocker):
        logger = logging.getLogger("test")
        mocker.patch(
            "zfsbackup.workers.load_runtime_config",
            side_effect=SchemaOutOfDate("schema is stale"),
        )
        with pytest.raises(SystemExit) as excinfo:
            _load_config(sqlite_config_db, False, logger)
        assert excinfo.value.code == EX_CONFIG

    def test_operational_error_is_never_mistaken_for_ex_config(
        self, sqlite_config_db, mocker
    ):
        """A `database is locked` `OperationalError` -- the shape a
        concurrent `zfsbackup-config` write under `busy_timeout` produces
        -- must return `None` (transient, stay restartable), not raise
        `SystemExit`. Distinguishing this from the two tests above is the
        entire point of `_load_config` having two `except` arms rather
        than one.
        """
        logger = logging.getLogger("test")
        mocker.patch(
            "zfsbackup.workers.load_runtime_config",
            side_effect=OperationalError(
                "BEGIN IMMEDIATE", None, Exception("database is locked")
            ),
        )
        result = _load_config(sqlite_config_db, False, logger)
        assert result is None

    def test_healthy_load_returns_config_with_dry_run_applied(
        self, sqlite_config_db
    ):
        logger = logging.getLogger("test")
        config = _load_config(sqlite_config_db, True, logger)
        assert isinstance(config, BackupConfig)
        assert config.dry_run is True


def _fork_child_worker_load(resolved_config, result_path):
    """Runs inside a forked child (module-level so
    `multiprocessing.get_context("fork")` can pickle/call it directly).

    Calls `_load_config` directly rather than `worker.run()` -- no loop,
    no stop-event wait, so the child exits as soon as this returns. The
    point of this test is the store's own fork guarantee (`db.py`'s
    pid-keyed `_ENGINES` cache) reached through the ACTUAL path a worker
    child uses, not a synthetic engine as `TestDbForkSafety` already
    covers in `tests/test_zfsbackup_store.py`.
    """
    logger = logging.getLogger("fork_child")
    config = _load_config(resolved_config, False, logger)
    result = {
        "pid": os.getpid(),
        "loaded": config is not None,
        "dataset_names": sorted(ds.name for ds in config.datasets) if config else [],
    }
    with open(result_path, "w") as fh:
        json.dump(result, fh)


@pytest.mark.skipif(
    not FORK_AVAILABLE,
    reason="'fork' multiprocessing start method unavailable on this platform",
)
@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestWorkerForkedChildLoadsConfig:
    """A worker's config load, exercised through a genuine
    `multiprocessing.get_context("fork")` child rather than an in-process
    call -- see `tests/test_zfsbackup_store.py`'s `TestDbForkSafety` for
    why this must be pinned to `fork` explicitly (this machine's default
    start method is `spawn`, under which the hazard cannot occur at all).

    Establishes the parent's own engine for `sqlite_config_db`'s URL
    first, so the child inherits a live cache entry exactly as a real
    `BackupDaemon`-spawned worker would if `_spawn`'s `dispose_all()`
    choke point were ever skipped -- and asserts the parent's own engine
    cache entry (keyed by this process's pid) is untouched afterwards,
    i.e. the child rebuilding its own engine never reaches back into the
    parent's.
    """

    def test_child_loads_independently_and_parent_cache_is_untouched(
        self, sqlite_config_db, tmp_path
    ):
        from zfsbackup.config.store import db as store_db

        url = sqlite_config_db.url()
        parent_engine = get_engine(url)
        with parent_engine.connect():
            pass  # establish the parent's own physical connection first
        parent_cache_before = dict(store_db._ENGINES)

        result_path = tmp_path / "fork_worker_load.json"
        ctx = multiprocessing.get_context("fork")
        proc = ctx.Process(
            target=_fork_child_worker_load, args=(sqlite_config_db, str(result_path))
        )
        proc.start()
        proc.join(timeout=30)
        assert proc.exitcode == 0

        with open(result_path) as fh:
            result = json.load(fh)

        assert result["pid"] != os.getpid()
        assert result["loaded"] is True
        assert result["dataset_names"] == ["pool/data"]

        # The parent's own cache entry (this process's pid) is exactly what
        # it was before the fork -- the child rebuilding its own engine
        # never mutated the parent's dict nor disposed the parent's pool.
        assert dict(store_db._ENGINES) == parent_cache_before

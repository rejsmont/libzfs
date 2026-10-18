"""Unit tests for zfsbackup worker classes."""

import copy
import inspect
import multiprocessing
import pickle
import signal
import pytest
from datetime import timedelta
from unittest.mock import MagicMock

import zfsbackup.workers as workers_module
from _config_handoff import config_signature, literals_in
from zfsbackup.config import BackupConfig, DatasetConfig, Duration, RemoteDatasetConfig
from zfsbackup.config.store import dispose_all
from zfsbackup.runtime_config import load_runtime_config
from zfsbackup.backup_manager import DatasetInfo, DatasetManager
from zfsbackup.stopflag import StopFlag
from zfsbackup.workers import (
    BaseWorker, SnapshotWorker, PruningWorker, RemoteBackupWorker, ApiWorker,
)

#: The union `workers.StopEvent` actually accepts (see `workers.py`'s own
#: module docstring): a plain `multiprocessing.Event` -- what ~35 existing
#: call sites in this file still construct -- and `StopFlag`, the item-8x
#: replacement every real `BackupDaemon` actually hands its workers today.
#: Before this, no FAST (mocked) test ever drove a worker with a real
#: `StopFlag` at all; only the skippable, `real_zfs`-adjacent
#: `test_zfsbackup_stop_signals.py` did. Parametrizing over both here means
#: a change to `BaseWorker.run`/`ApiWorker.run` that happens to work for
#: `Event`'s specific implementation but not `StopFlag`'s (or vice versa)
#: fails in the always-run mocked suite, not only in a slow, occasionally
#: skipped integration module.
STOP_EVENT_FACTORIES = [multiprocessing.Event, StopFlag]
STOP_EVENT_IDS = ['mp_event', 'stopflag']


class TestSnapshotWorker:
    def test_name_and_daemon(self, worker_config):
        stop_event = multiprocessing.Event()
        worker = SnapshotWorker(worker_config, stop_event)
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

    def test_run_uses_handed_config_and_processes(self, mocker, worker_config):
        stop_event = multiprocessing.Event()
        stop_event.set()
        worker = SnapshotWorker(worker_config, stop_event)
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        worker.run()


class TestPruningWorker:
    def test_name_and_daemon(self, worker_config):
        stop_event = multiprocessing.Event()
        worker = PruningWorker(worker_config, stop_event)
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

    def test_run_stops_when_event_set(self, worker_config, mocker):
        stop_event = multiprocessing.Event()
        stop_event.set()
        worker = PruningWorker(worker_config, stop_event)
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        worker.run()


class TestRemoteBackupWorker:
    def test_name_and_daemon(self, worker_config):
        stop_event = multiprocessing.Event()
        worker = RemoteBackupWorker(worker_config, stop_event)
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
    def test_name_and_daemon(self, worker_config):
        stop_event = multiprocessing.Event()
        worker = ApiWorker(worker_config, stop_event)
        assert worker.name == 'api-worker'
        assert worker.daemon is True


class TestBaseWorkerRun:
    def test_run_uses_the_handed_config_with_dry_run_applied(self, worker_config, mocker):
        """`run()` builds its `DatasetManager` from `self.config` -- the very
        object it was constructed with, `dry_run` already applied by the
        supervisor -- and does not substitute another."""
        stop_event = multiprocessing.Event()
        stop_event.set()
        worker = SnapshotWorker(worker_config, stop_event)
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        seen = []
        real_init = DatasetManager.__init__

        def spy(self_, config, *a, **k):
            seen.append(config)
            return real_init(self_, config, *a, **k)

        mocker.patch.object(DatasetManager, '__init__', spy)
        worker.run()
        assert len(seen) == 1 and seen[0] is worker_config
        assert seen[0].dry_run is True


class TestBaseWorkerLoop:
    """Cover the main dataset-processing loop (lines 74-82) and _get_datasets (line 51)."""

    def test_dataset_exception_logged_and_loop_exits_on_stop(self, mocker):
        stop_event = multiprocessing.Event()
        two_dataset_config = BackupConfig(datasets=[
            DatasetConfig(name='pool/a'),
            DatasetConfig(name='pool/b'),
        ])
        worker = SnapshotWorker(two_dataset_config, stop_event)
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
    @pytest.mark.parametrize(
        'stop_event_factory', STOP_EVENT_FACTORIES, ids=STOP_EVENT_IDS
    )
    def test_run_starts_thread_and_stops_on_event(
        self, worker_config, mocker, stop_event_factory
    ):
        stop_event = stop_event_factory()
        stop_event.set()  # stop_event.wait() returns immediately
        worker = ApiWorker(worker_config, stop_event)  # dry_run covers line 198

        mock_app = MagicMock()
        mocker.patch('zfsbackup.api.create_app', return_value=mock_app)
        mock_thread = MagicMock()
        mocker.patch('zfsbackup.workers.threading.Thread', return_value=mock_thread)
        mocker.patch('zfsbackup.workers._bind_api_server', return_value=MagicMock())  # no real socket

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
        self, worker_config, mocker
    ):
        self._install_non_default_handlers()
        stop_event = multiprocessing.Event()
        stop_event.set()  # zero loop iterations -- run() still must reset first
        worker = SnapshotWorker(worker_config, stop_event)
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])

        worker.run()

        assert signal.getsignal(signal.SIGINT) is signal.SIG_DFL
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL

    @pytest.mark.parametrize(
        'stop_event_factory', STOP_EVENT_FACTORIES, ids=STOP_EVENT_IDS
    )
    def test_api_worker_run_resets_signal_disposition_first(
        self, worker_config, mocker, stop_event_factory
    ):
        self._install_non_default_handlers()
        stop_event = stop_event_factory()
        stop_event.set()  # stop_event.wait() returns immediately
        worker = ApiWorker(worker_config, stop_event)

        mock_app = MagicMock()
        mocker.patch('zfsbackup.api.create_app', return_value=mock_app)
        mock_thread = MagicMock()
        mocker.patch('zfsbackup.workers.threading.Thread', return_value=mock_thread)
        mocker.patch('zfsbackup.workers._bind_api_server', return_value=MagicMock())  # no real socket

        worker.run()

        assert signal.getsignal(signal.SIGINT) is signal.SIG_DFL
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL


# ---------------------------------------------------------------------------
# Item 15.2(b): workers are handed the supervisor's detached BackupConfig and
# never open the config database.
# ---------------------------------------------------------------------------

WORKER_CLASSES = [
    pytest.param(SnapshotWorker, 'snapshot-worker', id='snapshot'),
    pytest.param(PruningWorker, 'pruning-worker', id='pruning'),
    pytest.param(RemoteBackupWorker, 'remote-backup-worker', id='remote'),
    pytest.param(ApiWorker, 'api-worker', id='api'),
]


class TestWorkerConstructors:
    """`(config, stop_event, verbose=False)` for every concrete worker;
    `BaseWorker(worker_name, config, stop_event, verbose=False)`. There is no
    path and no `dry_run` parameter any more: `--dry-run` is applied to the
    config by the supervisor before the handoff.

    Mutation-checked (scratch copy): re-adding a trailing `dry_run` parameter
    to `SnapshotWorker.__init__` fails 2 tests
    (`test_the_signature_is_exactly_config_stop_event_verbose[snapshot]` and
    `test_the_old_dry_run_and_path_parameters_are_gone[snapshot]`)."""

    @pytest.mark.parametrize('cls, name', WORKER_CLASSES)
    def test_positional_config_stop_event_verbose(self, cls, name, worker_config):
        stop = multiprocessing.Event()
        worker = cls(worker_config, stop, True)
        assert worker.config is worker_config
        assert worker.stop_event is stop
        assert worker.verbose is True
        assert worker.name == name and worker.daemon is True

    @pytest.mark.parametrize('cls, name', WORKER_CLASSES)
    def test_verbose_defaults_to_false(self, cls, name, worker_config):
        assert cls(worker_config, multiprocessing.Event()).verbose is False

    @pytest.mark.parametrize('cls, name', WORKER_CLASSES)
    def test_the_signature_is_exactly_config_stop_event_verbose(self, cls, name):
        params = list(inspect.signature(cls.__init__).parameters)
        assert params == ['self', 'config', 'stop_event', 'verbose']

    def test_base_worker_signature_takes_the_name_first(self):
        params = list(inspect.signature(BaseWorker.__init__).parameters)
        assert params == ['self', 'worker_name', 'config', 'stop_event', 'verbose']

    @pytest.mark.parametrize('cls, name', WORKER_CLASSES)
    def test_the_old_dry_run_and_path_parameters_are_gone(self, cls, name, worker_config):
        stop = multiprocessing.Event()
        with pytest.raises(TypeError):
            cls(worker_config, stop, dry_run=True)
        with pytest.raises(TypeError):
            cls(worker_config, stop, resolved_config=object())
        with pytest.raises(TypeError):
            cls(worker_config, stop, False, True)  # the old 4th positional (dry_run)

    @pytest.mark.parametrize('cls, name', WORKER_CLASSES)
    def test_the_config_is_not_copied(self, cls, name, worker_config):
        """Identity matters: all workers and every restart share one object."""
        a = cls(worker_config, multiprocessing.Event())
        b = cls(worker_config, multiprocessing.Event())
        assert a.config is b.config is worker_config

    def test_load_config_and_the_config_error_exit_are_gone(self):
        assert not hasattr(workers_module, '_load_config')
        assert not hasattr(workers_module, 'load_runtime_config')

    def test_workers_module_imports_nothing_from_the_config_store(self):
        """The strongest cheap guard that no worker can reach the database:
        nothing in the module's namespace comes from the store or the
        daemon's reader."""
        offenders = [
            name for name, obj in vars(workers_module).items()
            if getattr(obj, '__module__', '').startswith(
                ('zfsbackup.config.store', 'zfsbackup.runtime_config')
            ) or (inspect.ismodule(obj) and obj.__name__.startswith(
                ('zfsbackup.config.store', 'zfsbackup.runtime_config')
            ))
        ]
        assert offenders == []


class TestConfigSurvivesPickling:
    """Under `spawn`/`forkserver` the config crosses the process boundary by
    pickling. `Duration.__eq__` ignores literals, so equality alone proves
    nothing: the signature comparison records each literal.

    Mutation-checked (scratch copy): removing `Duration.__reduce__` fails 6 of
    the 8 tests here -- `test_pickled_signature_is_identical` at all four
    protocols, `test_every_duration_literal_survives` and
    `test_deepcopy_agrees_with_pickle`."""

    @pytest.fixture
    def config(self, literal_config_db):
        snapshot = load_runtime_config(literal_config_db)
        dispose_all()
        snapshot.config.dry_run = True
        return snapshot.config

    @pytest.mark.parametrize('protocol', range(2, pickle.HIGHEST_PROTOCOL + 1))
    def test_pickled_signature_is_identical(self, config, protocol):
        clone = pickle.loads(pickle.dumps(config, protocol))
        assert clone is not config
        assert clone == config
        assert config_signature(clone) == config_signature(config)
        assert clone.dry_run is True

    def test_every_duration_literal_survives(self, config):
        clone = pickle.loads(pickle.dumps(config))
        expected = ['600s', '90m', '60m', '4w', '1M', '120m', '52w']
        assert set(expected) <= set(literals_in(config_signature(config)))
        assert set(expected) <= set(literals_in(config_signature(clone)))
        ds = clone.datasets[0]
        assert ds.frequency.literal == '60m'
        assert [r.age.literal for r in ds.retention_rules] == ['1d', '1w']
        assert [r.keep_for.literal for r in ds.retention_rules] == ['4w', '1M']
        (remote,) = ds.remote
        assert remote.frequency.literal == '120m'
        assert [(r.age.literal, r.keep_for.literal) for r in remote.retention_rules] == [('1d', '52w')]
        assert clone.check_interval.literal == '600s'
        assert clone.prune_interval.literal == '90m'

    def test_deepcopy_agrees_with_pickle(self, config):
        assert config_signature(copy.deepcopy(config)) == config_signature(config)

    def test_equality_alone_cannot_see_a_lost_literal(self):
        """Why the signature exists: these compare equal, and are not the same."""
        a, b = Duration('60m'), Duration('1h')
        assert a == b
        assert config_signature(a) != config_signature(b)

    def test_a_pickled_worker_config_drives_a_worker_identically(self, config):
        clone = pickle.loads(pickle.dumps(config))
        for cls in (SnapshotWorker, PruningWorker):
            original = cls(config, multiprocessing.Event())
            copied = cls(clone, multiprocessing.Event())
            assert original._get_interval(original.config) == copied._get_interval(copied.config)


class TestWorkersNeverOpenTheConfigDatabase:
    """Item 15.2(b): the supervisor read the database once; a worker's `run()`
    must work with the database gone and every way of opening it armed to
    explode. Run for real (stop flag already set, so zero loop iterations but
    the whole setup path: signal reset, `DatasetManager`, interval, and
    `_before_loop`).

    Mutation-checked (scratch copy): making `BaseWorker.run` open any SQLite
    connection fails 8 tests: 7 here (the three-class x two-stop-flag run test
    and the loop test) and `TestForkedWorkerRunsOnTheInheritedConfig`."""

    @pytest.fixture
    def armed(self, literal_config_db, tmp_path, request, mocker):
        """-> the config. Read first, then the database is deleted and every
        way of opening it is armed (`forbid_db`, resolved here so it cannot
        be armed before the read)."""
        snapshot = load_runtime_config(literal_config_db)
        dispose_all()
        config = snapshot.config
        config.dry_run = True
        config.client_id_file = tmp_path / 'client_id'
        for path in tmp_path.glob('literal.db*'):
            path.unlink()
        assert not list(tmp_path.glob('literal.db*'))
        request.getfixturevalue('forbid_db')
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        mocker.patch('zfsbackup.backup_manager.zfs.set')
        return config

    @pytest.mark.parametrize('stop_factory', STOP_EVENT_FACTORIES, ids=STOP_EVENT_IDS)
    @pytest.mark.parametrize('cls', [SnapshotWorker, PruningWorker, RemoteBackupWorker])
    def test_run_completes_without_touching_the_database(
        self, cls, stop_factory, armed, forbid_db
    ):
        stop = stop_factory()
        stop.set()
        cls(armed, stop).run()
        assert forbid_db == []

    @pytest.mark.parametrize('stop_factory', STOP_EVENT_FACTORIES, ids=STOP_EVENT_IDS)
    def test_api_worker_run_completes_without_touching_the_database(
        self, stop_factory, armed, forbid_db, mocker
    ):
        stop = stop_factory()
        stop.set()
        app = MagicMock()
        mocker.patch('zfsbackup.api.create_app', return_value=app)
        mocker.patch('zfsbackup.workers.threading.Thread', return_value=MagicMock())
        mocker.patch('zfsbackup.workers._bind_api_server', return_value=MagicMock())  # no real socket
        ApiWorker(armed, stop).run()
        assert forbid_db == []

    def test_the_api_app_is_built_from_the_handed_config(self, armed, mocker):
        stop = multiprocessing.Event()
        stop.set()
        factory = mocker.patch('zfsbackup.api.create_app', return_value=MagicMock())
        mocker.patch('zfsbackup.workers.threading.Thread', return_value=MagicMock())
        mocker.patch('zfsbackup.workers._bind_api_server', return_value=MagicMock())  # no real socket
        ApiWorker(armed, stop).run()
        factory.assert_called_once_with(armed)

    def test_the_worker_loop_runs_on_the_handed_config_not_a_reloaded_one(self, armed, mocker):
        """A cycle processes the datasets of the config it was handed; there
        is nowhere to get another one from."""
        stop = multiprocessing.Event()
        seen = []

        def process(self_, manager, dsi):
            seen.append(dsi.name)
            stop.set()

        mocker.patch.object(SnapshotWorker, '_process_dataset', process)
        SnapshotWorker(armed, stop).run()
        assert seen == ['pool/data']  # first of the config's datasets, then stopped

    def test_interval_comes_from_the_handed_config(self, armed):
        assert SnapshotWorker(armed, multiprocessing.Event())._get_interval(armed) == 600.0
        assert PruningWorker(armed, multiprocessing.Event())._get_interval(armed) == 5400.0
        # min of the remote frequencies: the `offsite` override of 120m
        assert RemoteBackupWorker(armed, multiprocessing.Event())._get_interval(armed) == 7200.0


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(),
    reason="'fork' multiprocessing start method unavailable on this platform",
)
@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
class TestForkedWorkerRunsOnTheInheritedConfig:
    """A real forked child runs the real `SnapshotWorker.run` on the config it
    inherited, with the database deleted and `sqlite3.connect` armed to raise
    in the parent (a fork child inherits the patched module, so a child that
    opened the database would die with `DbTouched` and a non-zero exit)."""

    def test_child_exits_cleanly_without_opening_the_database(
        self, literal_config_db, tmp_path, mocker
    ):
        from _config_handoff import ForkedSnapshotWorker

        snapshot = load_runtime_config(literal_config_db)
        dispose_all()
        for path in tmp_path.glob('literal.db*'):
            path.unlink()
        forbid = mocker.patch('sqlite3.connect', side_effect=RuntimeError('db opened'))
        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        mocker.patch('zfsbackup.backup_manager.zfs.set')
        stop = StopFlag()
        stop.set()  # zero loop iterations; the whole setup path still runs
        worker = ForkedSnapshotWorker(snapshot.config, stop)
        worker.start_method = 'fork'

        worker.start()
        worker.join(60)

        assert worker.exitcode == 0
        forbid.assert_not_called()

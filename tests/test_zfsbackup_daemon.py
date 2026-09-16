"""Unit tests for BackupDaemon."""

import multiprocessing
import signal
import sqlite3
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

from zfsbackup.config import BackupConfig, DatasetConfig, RemoteDatasetConfig, RemoteServerConfig, Destination
from zfsbackup.config.store import (
    CONFIG_PATH_ENV,
    dispose_all,
    import_yaml,
    resolve_config_path,
)
from zfsbackup.daemon import BackupDaemon, main
from zfsbackup.runtime_config import EX_CONFIG
from zfsbackup.workers import SnapshotWorker, PruningWorker, ApiWorker, RemoteBackupWorker


def _make_config(**kwargs) -> BackupConfig:
    defaults = dict(datasets=[DatasetConfig(name='pool/data')])
    defaults.update(kwargs)
    return BackupConfig(**defaults)


class TestBackupDaemonInit:
    def test_instantiation(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        assert daemon.config is sample_backup_config
        # Renamed from `config_path` (item 8, decision D-3): the type
        # changed from a bare `Path` to `ResolvedConfigPath`, and keeping
        # the old attribute name is exactly how a bare `Path` -- which
        # lacks the `.url()`/`.resolved_path` every real open needs --
        # would sneak back in unnoticed.
        assert daemon.resolved_config == sqlite_config_db
        assert daemon.verbose is False

    def test_verbose_flag(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db, verbose=True)
        assert daemon.verbose is True

    def test_stop_event_initially_clear(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        assert not daemon._stop_event.is_set()


class TestSignalHandler:
    def test_signal_sets_stop_event(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        daemon._signal_handler(signal.SIGINT, None)
        assert daemon._stop_event.is_set()

    def test_sigterm_sets_stop_event(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        daemon._signal_handler(signal.SIGTERM, None)
        assert daemon._stop_event.is_set()


class TestActiveWorkerNames:
    def test_base_workers_always_included(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        names = daemon._active_worker_names()
        assert 'snapshot' in names
        assert 'pruning' in names
        assert 'api' in names

    def test_remote_worker_excluded_when_no_remote(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        names = daemon._active_worker_names()
        assert 'remote' not in names

    def test_remote_worker_included_when_datasets_have_remote(self, sqlite_config_db):
        config = _make_config(datasets=[
            DatasetConfig(
                name='pool/data',
                remote=[RemoteDatasetConfig(destination='offsite')],
            ),
        ])
        daemon = BackupDaemon(config, sqlite_config_db)
        names = daemon._active_worker_names()
        assert 'remote' in names


class TestNewWorker:
    def test_new_worker_snapshot(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        worker = daemon._new_worker('snapshot')
        assert isinstance(worker, SnapshotWorker)

    def test_new_worker_pruning(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        worker = daemon._new_worker('pruning')
        assert isinstance(worker, PruningWorker)

    def test_new_worker_api(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        worker = daemon._new_worker('api')
        assert isinstance(worker, ApiWorker)

    def test_new_worker_remote(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        worker = daemon._new_worker('remote')
        assert isinstance(worker, RemoteBackupWorker)


class TestStartWorkers:
    def test_starts_all_active_workers(self, sqlite_config_db, sample_backup_config, mocker):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        mock_worker = MagicMock()
        mock_worker.pid = 1234
        mocker.patch.object(daemon, '_new_worker', return_value=mock_worker)
        mocker.patch('zfsbackup.daemon.dispose_all')
        daemon._start_workers()
        assert mock_worker.start.call_count == len(daemon._active_worker_names())
        assert len(daemon._workers) == len(daemon._active_worker_names())


class TestCheckWorkers:
    def test_restarts_dead_worker(self, sqlite_config_db, sample_backup_config, mocker):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        dead_worker = MagicMock()
        dead_worker.is_alive.return_value = False
        dead_worker.pid = 100
        dead_worker.exitcode = 1
        daemon._workers = {'snapshot': dead_worker}

        new_worker = MagicMock()
        new_worker.pid = 200
        mocker.patch.object(daemon, '_new_worker', return_value=new_worker)
        mocker.patch('zfsbackup.daemon.dispose_all')
        mocker.patch('zfsbackup.daemon.time.sleep')

        daemon._check_workers()
        new_worker.start.assert_called_once()
        assert daemon._workers['snapshot'] is new_worker
        # A generic exit code (never EX_CONFIG) stays restartable and does
        # not join the config-failed set.
        assert daemon._config_failed == set()

    def test_alive_workers_not_restarted(self, sqlite_config_db, sample_backup_config, mocker):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        alive_worker = MagicMock()
        alive_worker.is_alive.return_value = True
        daemon._workers = {'snapshot': alive_worker}
        mocker.patch.object(daemon, '_new_worker')
        daemon._check_workers()
        daemon._new_worker.assert_not_called()


class TestCrashLoopGuard:
    """8i, and its own follow-up fix (coordinator-reported defect 3): a
    worker that exits `EX_CONFIG` (a permanent configuration fault --
    `SchemaError`/`ConfigPathError`, per `workers._load_config`) must
    never be respawned, and stops the WHOLE daemon on the FIRST such
    exit, not only once every active worker has failed that way.

    The fix matters because the earlier "all workers" rule degraded
    silently: a `snapshot` worker dying on a schema mismatch left
    `pruning`/`api`/`remote` running, so the daemon looked alive while
    permanently taking no more snapshots, with one ERROR line the only
    trace. `_check_workers` is now two passes -- classify every dead
    worker first, decide whether ANY of them is a configuration failure,
    and only respawn anything (including workers that died of an
    unrelated, ordinarily-restartable cause in the SAME pass) if none
    was. `test_mixed_pass_no_respawn_at_all_and_run_returns_one` below is
    the test that actually distinguishes this from the "all workers"
    rule -- `test_ex_config_exit_is_removed_and_never_restarted` and
    `test_run_returns_one_once_every_worker_is_config_failed` would both
    still pass under the OLD "all must fail" rule too, since every dead
    worker in each of those is already `EX_CONFIG`.

    Mutation-checked, two separate mutations:
    - Reverting `_check_workers`'s `if worker.exitcode == EX_CONFIG:`
      branch (so every dead worker falls through to the generic restart
      path) fails 3 of the 4 tests in this class --
      `test_ex_config_exit_is_removed_and_never_restarted`,
      `test_mixed_pass_no_respawn_at_all_and_run_returns_one`, and
      `test_run_returns_one_once_every_worker_is_config_failed` (whose own
      20-iteration sleep-count safety valve turns what would otherwise be
      an unbounded supervisor poll loop -- a real crash-loop fork storm,
      the exact hazard 8i exists to prevent -- into a clean, fast
      assertion failure instead of a pytest hang, verified directly:
      without that valve the same mutation hangs the test rather than
      failing it). `test_non_ex_config_exit_still_respawns` is unaffected
      (it never exercises the EX_CONFIG branch) and correctly still
      passes.
    - Deleting the `if not self._config_failed:` guard in front of the
      respawn loop (i.e. always running the respawn loop over `dead`,
      regardless of whether a config failure was also seen this pass)
      ALSO fails the same 3 of 4 tests, measured -- not only the mixed
      test this mutation is aimed at. `dead` is computed once, before
      classification, so an `EX_CONFIG` worker already deleted from
      `self._workers` by the classification pass is still in `dead`;
      without the guard the (now unconditional) respawn loop iterates it
      anyway and re-adds a live entry for a worker the daemon just
      decided was permanently failed -- which is exactly what
      `test_ex_config_exit_is_removed_and_never_restarted`'s `'snapshot'
      not in daemon._workers` and
      `test_run_returns_one_once_every_worker_is_config_failed`'s
      `daemon._workers == {}` both catch too, not just the mixed test's
      "nothing respawned at all". `test_non_ex_config_exit_still_respawns`
      is unaffected either way: `self._config_failed` is empty in that
      test regardless of the guard, so `if True` and
      `if not self._config_failed` behave identically there.
    """

    def test_ex_config_exit_is_removed_and_never_restarted(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        dead_worker = MagicMock()
        dead_worker.is_alive.return_value = False
        dead_worker.pid = 100
        dead_worker.exitcode = EX_CONFIG
        daemon._workers = {'snapshot': dead_worker}
        mocker.patch.object(daemon, '_new_worker')
        mocker.patch('zfsbackup.daemon.dispose_all')
        mocker.patch('zfsbackup.daemon.time.sleep')

        daemon._check_workers()

        daemon._new_worker.assert_not_called()
        assert 'snapshot' not in daemon._workers
        assert daemon._config_failed == {'snapshot'}

    def test_non_ex_config_exit_still_respawns(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        dead_worker = MagicMock()
        dead_worker.is_alive.return_value = False
        dead_worker.pid = 100
        dead_worker.exitcode = 1  # e.g. an uncaught OperationalError -- transient
        daemon._workers = {'snapshot': dead_worker}
        new_worker = MagicMock()
        new_worker.pid = 200
        mocker.patch.object(daemon, '_new_worker', return_value=new_worker)
        mocker.patch('zfsbackup.daemon.dispose_all')
        mocker.patch('zfsbackup.daemon.time.sleep')

        daemon._check_workers()

        daemon._new_worker.assert_called_once_with('snapshot')
        assert daemon._workers['snapshot'] is new_worker
        assert daemon._config_failed == set()

    def test_mixed_pass_no_respawn_at_all_and_run_returns_one(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        """The test that actually pins "first EX_CONFIG is fatal, not
        only when every worker has failed": one worker exits `EX_CONFIG`,
        an entirely UNRELATED worker exits an ordinary transient code, in
        the SAME `_check_workers` pass. Neither gets respawned -- not
        even the unrelated crash -- and the whole daemon stops.
        """
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)

        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        mocker.patch('zfsbackup.backup_manager.zfs.set')
        mocker.patch('zfsbackup.daemon.dispose_all')
        mocker.patch.object(daemon, '_new_worker')  # must NEVER be called

        config_failed_worker = MagicMock()
        config_failed_worker.is_alive.return_value = False
        config_failed_worker.pid = 100
        config_failed_worker.exitcode = EX_CONFIG

        crashed_worker = MagicMock()
        crashed_worker.is_alive.return_value = False
        crashed_worker.pid = 200
        crashed_worker.exitcode = 1  # ordinary, unrelated, normally-restartable

        def fake_start_workers():
            daemon._workers = {
                'snapshot': config_failed_worker,
                'pruning': crashed_worker,
            }

        mocker.patch.object(daemon, '_start_workers', side_effect=fake_start_workers)
        # Safety valve, not part of the assertion -- see
        # `test_run_returns_one_once_every_worker_is_config_failed`'s
        # identical comment. Measured directly: reverting the EX_CONFIG
        # classification entirely (as opposed to just the respawn guard
        # this test targets) makes `self._config_failed` never populate,
        # so `_stop_event` never gets set and `run()`'s poll loop spins
        # forever; this cap turns that into a clean, fast assertion
        # failure instead of a pytest hang.
        sleep_calls = [0]

        def bounded_sleep(_):
            sleep_calls[0] += 1
            if sleep_calls[0] > 20:
                daemon._stop_event.set()

        mocker.patch('zfsbackup.daemon.time.sleep', side_effect=bounded_sleep)

        result = daemon.run()

        assert sleep_calls[0] <= 20, (
            "supervisor loop did not stop on its own -- a configuration "
            "failure in _check_workers is not being classified/acted on"
        )
        daemon._new_worker.assert_not_called()
        assert result == 1
        assert daemon._config_failed == {'snapshot'}
        # `snapshot` (the EX_CONFIG worker) was removed from `_workers`;
        # `pruning` (the unrelated crash) was left exactly as it died --
        # neither respawned nor cleaned up, since the whole point is that
        # NOTHING gets restarted once a configuration failure is seen in
        # the same pass.
        assert 'snapshot' not in daemon._workers
        assert daemon._workers.get('pruning') is crashed_worker
        assert daemon._stop_event.is_set()

    def test_run_returns_one_once_every_worker_is_config_failed(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)

        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        mocker.patch('zfsbackup.backup_manager.zfs.set')
        mocker.patch('zfsbackup.daemon.dispose_all')

        dead_workers = {}
        for name in daemon._active_worker_names():
            w = MagicMock()
            w.is_alive.return_value = False
            w.pid = 100
            w.exitcode = EX_CONFIG
            dead_workers[name] = w

        def fake_start_workers():
            daemon._workers = dict(dead_workers)

        mocker.patch.object(daemon, '_start_workers', side_effect=fake_start_workers)
        # `_new_worker` is deliberately NOT left real: if the EX_CONFIG
        # guard under test were broken, `_check_workers` would respawn
        # every dead worker on every poll -- an unmocked `_new_worker`
        # would then fork real OS processes in a tight loop. Mocking it
        # keeps a broken guard from doing anything worse than looping in
        # pure Python.
        mocker.patch.object(daemon, '_new_worker', return_value=MagicMock(pid=999))
        # Safety valve, not part of the assertion: caps the supervisor
        # poll loop at 20 iterations so a regression that breaks the
        # EX_CONFIG guard fails this test with a clean assertion instead
        # of hanging pytest (or, worse under an unmocked `_new_worker`,
        # fork-bombing the test host) -- verified by reverting the guard
        # in `_check_workers` (see this class's own docstring).
        sleep_calls = [0]

        def bounded_sleep(_):
            sleep_calls[0] += 1
            if sleep_calls[0] > 20:
                daemon._stop_event.set()

        mocker.patch('zfsbackup.daemon.time.sleep', side_effect=bounded_sleep)

        result = daemon.run()

        assert sleep_calls[0] <= 20, (
            "supervisor loop did not stop on its own -- the EX_CONFIG "
            "guard in _check_workers is not removing config-failed "
            "workers from daemon._workers"
        )
        assert result == 1
        assert daemon._workers == {}
        assert daemon._config_failed == set(dead_workers)


class TestSpawnOrdering:
    """8g: `_spawn` is the single fork point, and `dispose_all()` must run
    BEFORE the worker's own `start()` -- both at initial startup
    (`_start_workers`) and at every crash re-fork (`_check_workers`),
    which forks arbitrarily late in the daemon's life, after `main()` has
    left a live pooled connection to the config database. Asserting mere
    call counts (as the pre-item-8 `test_starts_all_active_workers` did)
    cannot catch a reordering; only the sequence itself can.

    Mutation-checked: reordering `_spawn` to call `worker.start()` before
    `dispose_all()` fails both tests below (the recorded order becomes
    `['new_worker:...', 'start', 'dispose_all']`), and deleting the
    `dispose_all()` call entirely fails both as well (missing from the
    recorded order).
    """

    def _record_calls(self, daemon, mocker):
        call_order = []
        mocker.patch(
            'zfsbackup.daemon.dispose_all',
            side_effect=lambda: call_order.append('dispose_all'),
        )

        def fake_new_worker(name):
            call_order.append(f'new_worker:{name}')
            worker = MagicMock()
            worker.pid = 4242
            worker.start.side_effect = lambda: call_order.append('start')
            return worker

        mocker.patch.object(daemon, '_new_worker', side_effect=fake_new_worker)
        return call_order

    def test_start_workers_disposes_before_starting(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        call_order = self._record_calls(daemon, mocker)

        daemon._spawn('pruning')

        assert call_order == ['dispose_all', 'new_worker:pruning', 'start']

    def test_check_workers_re_fork_disposes_before_starting(
        self, sqlite_config_db, sample_backup_config, mocker
    ):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        dead_worker = MagicMock()
        dead_worker.is_alive.return_value = False
        dead_worker.pid = 100
        dead_worker.exitcode = 1
        daemon._workers = {'pruning': dead_worker}
        mocker.patch('zfsbackup.daemon.time.sleep')

        call_order = self._record_calls(daemon, mocker)

        daemon._check_workers()

        assert call_order == ['dispose_all', 'new_worker:pruning', 'start']


class TestShutdownWorkers:
    def test_joins_all_workers(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        worker1 = MagicMock()
        worker1.is_alive.return_value = False
        worker2 = MagicMock()
        worker2.is_alive.return_value = False
        daemon._workers = {'snapshot': worker1, 'pruning': worker2}
        daemon._shutdown_workers()
        worker1.join.assert_called()
        worker2.join.assert_called()

    def test_terminates_workers_that_dont_exit(self, sqlite_config_db, sample_backup_config):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        stubborn = MagicMock()
        stubborn.is_alive.return_value = True
        daemon._workers = {'snapshot': stubborn}
        daemon._shutdown_workers(timeout=1)
        stubborn.terminate.assert_called_once()

    def test_escalates_to_sigkill_when_terminate_does_not_work(
        self, sqlite_config_db, sample_backup_config
    ):
        """Coordinator-reported defect 4: `terminate()` sends SIGTERM,
        which a child with a SIGTERM *handler* installed does not have to
        obey (measured: `alive after terminate+join(5): True exitcode
        None`, before `workers._init_child_process` reset the child's
        disposition). `_shutdown_workers` now escalates to `.kill()`
        (SIGKILL) rather than falling through silently -- `is_alive()` is
        checked 3 times in sequence (after the initial `join`, after
        `terminate()+join(5)`, after `kill()+join(5)`); this worker
        reports alive for the first two and dead for the third, so
        `.kill()` must have been the thing that actually stopped it.
        """
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        worker = MagicMock()
        worker.pid = 4242
        worker.is_alive.side_effect = [True, True, False]
        daemon._workers = {'snapshot': worker}

        daemon._shutdown_workers(timeout=1)

        worker.terminate.assert_called_once()
        worker.kill.assert_called_once()

    def test_logs_error_when_worker_survives_sigkill(
        self, sqlite_config_db, sample_backup_config, caplog
    ):
        """The pathological case `_shutdown_workers`'s own docstring
        names explicitly (an uninterruptible sleep, e.g. blocked in a
        `zfs` ioctl): even SIGKILL does not stop it. Must not raise, must
        not loop -- one more ERROR line naming the worker, then move on
        (an earlier version of this exact case fell through silently,
        leaving the interpreter's own exit handler to `join()` with NO
        timeout, hanging `systemctl stop` until TimeoutStopSec).
        """
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)
        worker = MagicMock()
        worker.pid = 4242
        worker.is_alive.side_effect = [True, True, True]
        daemon._workers = {'snapshot': worker}

        with caplog.at_level('ERROR'):
            daemon._shutdown_workers(timeout=1)

        worker.kill.assert_called_once()
        assert 'survived SIGKILL' in caplog.text
        assert '4242' in caplog.text


class TestDaemonRun:
    def test_run_starts_and_shuts_down(self, sqlite_config_db, sample_backup_config, mocker):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)

        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        mocker.patch('zfsbackup.backup_manager.zfs.set')

        mocker.patch.object(daemon, '_start_workers')
        mocker.patch.object(daemon, '_check_workers')
        mocker.patch.object(daemon, '_shutdown_workers')

        daemon._stop_event.set()
        daemon.run()

        daemon._start_workers.assert_called_once()
        daemon._shutdown_workers.assert_called_once()

    def test_run_supervisor_loop_checks_workers(self, sqlite_config_db, sample_backup_config, mocker):
        daemon = BackupDaemon(sample_backup_config, sqlite_config_db)

        mocker.patch('zfsbackup.backup_manager.zfs.exists', return_value=True)
        mocker.patch('zfsbackup.backup_manager.zfs.list', return_value=[])
        mocker.patch('zfsbackup.backup_manager.zfs.set')

        mocker.patch.object(daemon, '_start_workers')
        mocker.patch.object(daemon, '_check_workers')
        mocker.patch.object(daemon, '_shutdown_workers')

        sleep_calls = [0]

        def sleep_and_maybe_stop(_):
            sleep_calls[0] += 1
            if sleep_calls[0] >= 2:
                daemon._stop_event.set()

        mocker.patch('zfsbackup.daemon.time.sleep', side_effect=sleep_and_maybe_stop)
        daemon.run()

        daemon._check_workers.assert_called_once()
        daemon._shutdown_workers.assert_called_once()


class TestMain:
    def _patch_config(self, mocker, **kwargs):
        defaults = dict(
            datasets=[DatasetConfig(name='pool/data')],
            destinations={},
            remote_backup=None,
            dry_run=False,
        )
        defaults.update(kwargs)
        mock_config = MagicMock(spec=BackupConfig)
        for k, v in defaults.items():
            setattr(mock_config, k, v)
        mocker.patch('zfsbackup.daemon.load_runtime_config', return_value=mock_config)
        mocker.patch('zfsbackup.daemon.check_config_schema', return_value='deadbeef')
        return mock_config

    def test_test_config_returns_zero(self, tmp_path, mocker):
        self._patch_config(mocker)
        with patch('sys.argv', ['daemon', '--test-config', '-c', str(tmp_path / 'cfg.db')]):
            result = main()
        assert result == 0

    def test_test_config_with_destinations(self, tmp_path, mocker):
        self._patch_config(
            mocker,
            destinations={'offsite': Destination(url='http://backup.example.com')},
            remote_backup=MagicMock(target_dataset='pool/backups'),
        )
        with patch('sys.argv', ['daemon', '--test-config', '-c', str(tmp_path / 'cfg.db')]):
            result = main()
        assert result == 0

    def test_config_load_failure_returns_one(self, tmp_path, mocker):
        mocker.patch('zfsbackup.daemon.load_runtime_config', side_effect=Exception('bad config'))
        with patch('sys.argv', ['daemon', '-c', str(tmp_path / 'cfg.db')]):
            result = main()
        assert result == 1

    def test_daemon_failure_returns_one(self, tmp_path, mocker):
        self._patch_config(mocker)
        mock_daemon = MagicMock()
        mock_daemon.run.side_effect = Exception('crash')
        mocker.patch('zfsbackup.daemon.BackupDaemon', return_value=mock_daemon)
        with patch('sys.argv', ['daemon', '-c', str(tmp_path / 'cfg.db')]):
            result = main()
        assert result == 1

    def test_normal_run_returns_zero(self, tmp_path, mocker):
        self._patch_config(mocker)
        mock_daemon = MagicMock()
        # `main()` now returns `daemon.run()`'s own value (item 8i: `run()`
        # returns 0/1 rather than always 0, so a stale-schema crash loop is
        # visible in a service manager's exit code) -- an unconfigured
        # `MagicMock` return isn't `== 0`, so this must be explicit.
        mock_daemon.run.return_value = 0
        mocker.patch('zfsbackup.daemon.BackupDaemon', return_value=mock_daemon)
        with patch('sys.argv', ['daemon', '-c', str(tmp_path / 'cfg.db')]):
            result = main()
        assert result == 0
        mock_daemon.run.assert_called_once()

    def test_dry_run_flag_sets_config(self, tmp_path, mocker):
        mock_config = self._patch_config(mocker)
        mock_daemon = MagicMock()
        mock_daemon.run.return_value = 0
        mocker.patch('zfsbackup.daemon.BackupDaemon', return_value=mock_daemon)
        with patch('sys.argv', ['daemon', '-d', '-c', str(tmp_path / 'cfg.db')]):
            main()
        assert mock_config.dry_run is True

    def test_verbose_flag_sets_log_level(self, tmp_path, mocker):
        self._patch_config(mocker)
        mock_daemon = MagicMock()
        mock_daemon.run.return_value = 0
        mocker.patch('zfsbackup.daemon.BackupDaemon', return_value=mock_daemon)
        with patch('sys.argv', ['daemon', '-v', '-c', str(tmp_path / 'cfg.db')]):
            result = main()
        assert result == 0


class TestMainConfigSourceResolution:
    """New coverage (item 8, sub-item 8k): the contract item 6a names as
    the single most likely thing item 8 breaks -- `-c`'s `default=None`
    and the deliberate absence of `type=Path` (`str(Path("")) == "."`,
    which would turn `-c ""` into "the current directory" and silently
    defeat `resolve_config_path`'s documented empty-string fallthrough).
    These exercise `main()` end-to-end, with NO mocking of
    `load_runtime_config`/`check_config_schema`, against a real imported
    database -- so a regression that reintroduces `type=Path` or a
    non-`None` default fails here even though every mocked `TestMain`
    test above would keep passing (they never touch `ZFSBACKUP_CONFIG`
    or an empty `-c` at all).
    """

    def test_env_var_reachable_when_dash_c_absent(self, sqlite_config_db, monkeypatch):
        monkeypatch.setenv(CONFIG_PATH_ENV, str(sqlite_config_db.path))
        with patch('sys.argv', ['daemon', '--test-config']):
            result = main()
        assert result == 0

    def test_dash_c_empty_string_falls_through_to_env_var(self, sqlite_config_db, monkeypatch):
        monkeypatch.setenv(CONFIG_PATH_ENV, str(sqlite_config_db.path))
        with patch('sys.argv', ['daemon', '--test-config', '-c', '']):
            result = main()
        assert result == 0

    def test_dash_c_yaml_target_exits_one_with_import_hint(self, config_yaml_path, caplog):
        with patch('sys.argv', ['daemon', '-c', str(config_yaml_path)]):
            with caplog.at_level('ERROR'):
                result = main()
        assert result == 1
        assert 'zfsbackup-config import' in caplog.text
        # No traceback: the message is operator-grade and logged verbatim,
        # not wrapped by the generic `except Exception` arm.
        assert 'Traceback' not in caplog.text

    def test_test_config_valid_db_creates_no_new_files(self, tmp_path, config_yaml_path, caplog):
        """`check_config_schema` opens a connection through the store's
        pid-keyed, pooled engine cache, which -- as long as ANY process
        holds it -- keeps `-wal`/`-shm` sidecars present on disk (measured;
        see `db.py`'s "WAL sidecars" section and
        `tests/test_zfsbackup_store.py`'s `TestDbWalSidecars`). They are
        checkpointed away once the pool's connection is genuinely closed.

        **Deliberately in-process, with an explicit `dispose_all()` after
        `main()` returns, not a subprocess relying on interpreter-exit GC
        to do that closing implicitly.** An earlier version of this test
        ran the daemon as a real `python -m zfsbackup.daemon` subprocess
        and asserted the sidecars were gone the instant it exited -- true,
        measured, on this repo's default Python 3.14/darwin. It is NOT
        portable: the identical subprocess, under `/tmp/v311/bin/python`
        (3.11), left `-wal`/`-shm` on disk with no sign of going away
        even after a 2s poll -- an environment-dependent detail of
        *when* CPython's interpreter-shutdown sequence finalizes a
        SQLAlchemy pool's DBAPI connection, not a contract `--test-config`
        (or anything in `daemon.py`) actually makes or controls. The
        property this test can honestly pin is narrower and still
        meaningful: `--test-config` itself performs no migration, no
        schema write, and leaves no file behind that a normal, explicit
        `dispose_all()` -- the same cleanup `_spawn` already performs at
        every real fork point -- does not immediately clean up.
        """
        db_dir = tmp_path / 'dbonly'
        db_dir.mkdir()
        db_path = db_dir / 'config.db'
        resolved = resolve_config_path(str(db_path))
        import_yaml(config_yaml_path, resolved)
        dispose_all()  # simulates the importer process having exited

        before = sorted(p.name for p in db_dir.iterdir())
        assert before == ['config.db']

        with patch('sys.argv', ['daemon', '--test-config', '-c', str(db_path)]):
            with caplog.at_level('INFO'):
                result = main()

        assert result == 0
        assert 'Schema revision:' in caplog.text

        dispose_all()
        after = sorted(p.name for p in db_dir.iterdir())
        assert after == ['config.db']

    def test_test_config_out_of_date_schema_exits_one(self, tmp_path, caplog):
        """A valid, but never-migrated, SQLite file -- the same state
        `tests/test_zfsbackup_store.py`'s
        `test_empty_never_migrated_database_raises_schema_out_of_date`
        pins directly against `check_schema`. Built with raw `sqlite3`
        (not `import_yaml`, which would migrate it to head) specifically
        so it has a real SQLite header without an `alembic_version` table
        or any application tables -- `check_schema`'s `SchemaOutOfDate`
        case for "no recorded Alembic revision at all".
        """
        db_path = tmp_path / 'config.db'
        conn = sqlite3.connect(str(db_path))
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('CREATE TABLE _placeholder(x)')
        conn.execute('DROP TABLE _placeholder')
        conn.commit()
        conn.close()

        with patch('sys.argv', ['daemon', '--test-config', '-c', str(db_path)]):
            with caplog.at_level('ERROR'):
                result = main()

        assert result == 1
        # SchemaOutOfDate's message, logged VERBATIM (config_error_message
        # returns ConfigPathError/SchemaError text unmodified).
        assert (
            "check_schema() only checks -- it never migrates the database"
            in caplog.text
        )

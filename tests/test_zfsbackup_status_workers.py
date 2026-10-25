"""S-7a: how the workers, the managers and real worker processes use the status files.

`test_zfsbackup_status_files.py` covers `zfsbackup.health` on its own; this file covers
what feeds it:

* `BaseWorker.run`'s loop (flush before each dataset, after it, the end-of-cycle heartbeat
  and the exit flush; `retain` only on a complete cycle; dry run and no directory write
  nothing; a stop pending records nothing), driven in-process over a mocked `DatasetManager`
  and a REAL `StatusWriter` in `tmp_path`;
* what each worker records (snapshot: only when due, `last_snapshot_error`; prune:
  `PruneResult`, the `discover` record; send: `backup_dataset()`'s answer, `last_problem`,
  the stop/reload case) and the resume-token skip counts it seeds;
* the managers' side: `DatasetManager.last_list_error`, `last_snapshot_error`,
  `last_received_error` and what `prune_snapshots` returns (`PruneResult`);
* REAL worker processes under fork, spawn and forkserver (`tests/_status_workers.py`): the
  file appears with the worker's pid, a reload or a crash restart carries the counters.

The supervisor's side (`<db>.status` made at spawn, dry run, failure) is
`test_zfsbackup_reload.py::TestStatusDirectory`.
"""

import itertools
import logging
import multiprocessing
import os
import time
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import _status_workers
from _prune_support import prune_manager, prune_pool, records  # noqa: F401  (fixture)
from _snapshot_support import listed_snapshot
from zfsbackup import daemonlock, health, workers as workers_module
from zfsbackup.backup_manager import DatasetManager, PruneResult
from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Destination,
    RemoteDatasetConfig,
    RemoteServerConfig,
)
from zfsbackup.health import StatusWriter, ensure_status_dir, read_worker_status
from zfsbackup.stopflag import ReloadEpoch, StopFlag
from zfsbackup.workers import (
    WORKER_EXIT_RELOAD,
    ExitReason,
    PruningWorker,
    RemoteBackupWorker,
    SnapshotWorker,
)

START_METHODS = [
    m for m in ("fork", "spawn", "forkserver") if m in multiprocessing.get_all_start_methods()
]


# ---------------------------------------------------------------------------
# An in-process driver for `BaseWorker.run`
# ---------------------------------------------------------------------------


class Dsi:
    """A stand-in `DatasetInfo`: a name, and the config the remote worker reads."""

    def __init__(self, name, destinations=()):
        self.name = name
        self.config = SimpleNamespace(
            remote=[SimpleNamespace(destination=d, frequency=None) for d in destinations],
            frequency=timedelta(hours=1),
        )


@pytest.fixture
def sdir(tmp_path):
    (tmp_path / 'config.db').write_bytes(b'')
    directory = tmp_path / 'config.db.status'
    ensure_status_dir(directory)
    return directory


def config_for(*names, dry_run=False, destinations=(), remote_backup=None):
    return BackupConfig(
        datasets=[
            DatasetConfig(name=n, remote=[RemoteDatasetConfig(destination=d) for d in destinations])
            for n in names
        ],
        destinations={d: Destination(url='https://h') for d in destinations},
        dry_run=dry_run,
        remote_backup=remote_backup,
    )


def manager_for(*dsis, **attrs):
    """A `DatasetManager` mock whose `.datasets` are `dsis`; `needs_snapshot` is False."""
    manager = MagicMock(name='manager')
    manager.datasets = list(dsis)
    manager.needs_snapshot.return_value = False
    manager.received_datasets.return_value = []
    manager.config.remote_backup = None
    manager.last_snapshot_error = None
    manager.last_received_error = None
    manager.get_anchor.return_value = None
    for key, value in attrs.items():
        setattr(manager, key, value)
    return manager


@pytest.fixture
def run(mocker, sdir):
    """`run(worker, manager, cycles=1)`: `worker.run()` over `manager`, `cycles` cycles, then a
    stop. Returns the worker."""
    def go(worker, manager, cycles=1):
        mocker.patch('zfsbackup.workers.DatasetManager', return_value=manager)
        left = [cycles - 1]

        def wait(interval):
            if left[0] > 0:
                left[0] -= 1
                return None
            return worker._exit_reason() or ExitReason.STOP       # what the real `_wait` reports

        mocker.patch.object(worker, '_wait', side_effect=wait)
        worker.run()
        return worker
    return go


def snapshot_worker(sdir, config=None, **kw):
    return SnapshotWorker(config or config_for('pool/a'), StopFlag(), status_dir=sdir, **kw)


def pruning_worker(sdir, config=None, **kw):
    return PruningWorker(config or config_for('pool/a'), StopFlag(), status_dir=sdir, **kw)


def remote_worker(sdir, config=None, **kw):
    cfg = config or config_for('pool/a', destinations=('off',))
    return RemoteBackupWorker(cfg, StopFlag(), status_dir=sdir, **kw)


def status(sdir, name='snapshot-worker'):
    return read_worker_status(sdir, name)


def by_key(st):
    return {r.key: r for r in st.records}


@pytest.fixture
def flushes(mocker):
    """Every `StatusWriter.flush` as `(force, heartbeat)`, still doing the real thing."""
    calls = []
    real = StatusWriter.flush

    def spy(self, force=False, heartbeat=False):
        calls.append((force, heartbeat))
        return real(self, force=force, heartbeat=heartbeat)

    mocker.patch.object(StatusWriter, 'flush', spy)
    return calls


@pytest.fixture
def writes(mocker):
    """The number of real file writes."""
    count = []
    real = health.write_status_file

    def spy(*a):
        count.append(1)
        return real(*a)

    mocker.patch.object(health, 'write_status_file', spy)
    return count


# ---------------------------------------------------------------------------
# The run loop
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRunLoop:
    def test_the_worker_has_inert_class_defaults(self):
        worker = SnapshotWorker.__new__(SnapshotWorker)
        assert worker._status is None and worker._status_dir is None
        worker._record('snapshot', 'pool/a', ok=False)            # a no-op, not an AttributeError
        worker._flush_status(force=True, heartbeat=True)

    def test_the_directory_is_stored_and_the_writer_is_built_by_run_not_the_constructor(self, sdir):
        worker = snapshot_worker(sdir)
        assert worker._status_dir == sdir and worker._status is None

    def test_a_worker_with_no_status_dir_writes_nothing(self, run, tmp_path, sdir):
        worker = SnapshotWorker(config_for('pool/a'), StopFlag())
        manager = manager_for(Dsi('pool/a'))
        manager.needs_snapshot.return_value = True
        manager.create_snapshot.return_value = SimpleNamespace(name='autosnap_1')
        run(worker, manager)
        assert worker._status is None
        assert list(sdir.iterdir()) == []

    def test_a_dry_run_writes_nothing_even_with_a_directory(self, run, sdir):
        worker = snapshot_worker(sdir, config_for('pool/a', dry_run=True))
        manager = manager_for(Dsi('pool/a'))
        manager.needs_snapshot.return_value = True
        manager.create_snapshot.return_value = SimpleNamespace(name='autosnap_1')
        run(worker, manager)
        assert worker._status is None and list(sdir.iterdir()) == []

    def test_the_call_sequence_for_one_dataset(self, run, sdir, flushes):
        run(snapshot_worker(sdir), manager_for(Dsi('pool/a')))
        assert flushes == [
            (True, False),      # before the dataset (writes only if something is unwritten)
            (False, False),     # after it (rate limited)
            (True, True),       # end of cycle: forced heartbeat
            (True, False),      # on the way out
        ]

    def test_two_cycles_heartbeat_twice(self, run, sdir, flushes):
        run(snapshot_worker(sdir), manager_for(Dsi('pool/a')), cycles=2)
        assert flushes.count((True, True)) == 2 and flushes[-1] == (True, False)

    def test_a_worker_that_completed_a_cycle_always_has_a_file_even_with_nothing_to_report(self, run, sdir):
        run(snapshot_worker(sdir), manager_for(Dsi('pool/a')))
        st = status(sdir)
        assert st is not None and st.records == () and st.pid == os.getpid()

    def test_the_heartbeat_refreshes_updated_at_each_cycle(self, run, sdir, mocker):
        clock = itertools.count(1_800_000_000, 100)               # every reading is 100 s later
        real = health.StatusWriter
        mocker.patch.object(health, 'StatusWriter',
                            lambda directory, worker: real(directory, worker, clock=lambda: next(clock)))
        worker = snapshot_worker(sdir)
        run(worker, manager_for(Dsi('pool/a')), cycles=3)
        st = status(sdir)
        assert st.updated_at - st.started_at >= 200

    def test_the_exit_flush_is_not_a_heartbeat(self, run, sdir, writes, flushes):
        run(snapshot_worker(sdir), manager_for(Dsi('pool/a')))
        # one write for the heartbeat; the (True, False) at exit finds nothing unwritten
        assert writes == [1]

    def test_the_outcome_is_on_disk_before_the_next_dataset_starts(self, run, sdir):
        """A dataset can run for hours: what the previous one recorded must not be held in
        memory meanwhile (`_FLUSH_BEFORE_DATASET`), whatever the 2 s rate limit says."""
        manager = manager_for(Dsi('pool/a'), Dsi('pool/b'))
        seen = []

        def needs(dsi):
            st = status(sdir)
            seen.append((dsi.name, None if st is None else sorted(r.dataset for r in st.records)))
            return True

        manager.needs_snapshot.side_effect = needs
        manager.create_snapshot.return_value = SimpleNamespace(name='autosnap_1')
        run(snapshot_worker(sdir, config_for('pool/a', 'pool/b')), manager)
        assert seen == [('pool/a', None), ('pool/b', ['pool/a'])]

    def test_the_pruning_worker_does_not_flush_before_every_dataset(self, run, sdir, flushes, writes):
        assert PruningWorker._FLUSH_BEFORE_DATASET is False and SnapshotWorker._FLUSH_BEFORE_DATASET is True
        names = ('pool/a', 'pool/b', 'pool/c')
        manager = manager_for(*[Dsi(n) for n in names])
        manager.prune_snapshots.return_value = PruneResult(pruned=1)
        run(pruning_worker(sdir, config_for(*names)), manager)
        assert (True, False) not in flushes[:-1]                  # only the final one
        assert flushes.count((False, False)) == 3
        assert len(writes) == 2                                   # the first (rate limit) + the heartbeat
        assert len(status(sdir, 'pruning-worker').records) == 3

    def test_a_stop_pending_before_a_dataset_records_nothing_for_it(self, mocker, sdir):
        stop = StopFlag()
        worker = SnapshotWorker(config_for('pool/a', 'pool/b'), stop, status_dir=sdir)
        manager = manager_for(Dsi('pool/a'), Dsi('pool/b'))

        def needs(dsi):
            stop.set()                                            # lands while pool/a is in hand
            return True

        manager.needs_snapshot.side_effect = needs
        manager.create_snapshot.return_value = SimpleNamespace(name='autosnap_1')
        mocker.patch('zfsbackup.workers.DatasetManager', return_value=manager)
        worker.run()
        assert manager.needs_snapshot.call_count == 1             # pool/b never started
        assert [r.dataset for r in status(sdir).records] == ['pool/a']   # the one in hand finished

    def test_a_stop_before_the_loop_leaves_the_files_alone(self, mocker, sdir):
        stop = StopFlag()
        stop.set()
        worker = SnapshotWorker(config_for('pool/a'), stop, status_dir=sdir)
        mocker.patch('zfsbackup.workers.DatasetManager', return_value=manager_for(Dsi('pool/a')))
        worker.run()
        assert list(sdir.iterdir()) == []

    def test_an_exception_is_recorded_as_a_failure_logged_and_the_loop_goes_on(self, run, sdir, caplog):
        manager = manager_for(Dsi('pool/a'), Dsi('pool/b'))
        manager.needs_snapshot.side_effect = [RuntimeError('zfs exploded'), False]
        with caplog.at_level(logging.ERROR, logger='zfsbackup.workers'):
            run(snapshot_worker(sdir, config_for('pool/a', 'pool/b')), manager)
        assert 'error on pool/a' in caplog.text
        recs = by_key(status(sdir))
        assert recs[('snapshot', 'pool/a', None)].last_error == 'RuntimeError: zfs exploded'
        assert recs[('snapshot', 'pool/a', None)].consecutive_failures == 1
        assert ('snapshot', 'pool/b', None) not in recs

    def test_a_pruning_exception_is_charged_to_prune(self, run, sdir):
        manager = manager_for(Dsi('pool/a'))
        manager.prune_snapshots.side_effect = OSError('too many open files')
        run(pruning_worker(sdir), manager)
        (rec,) = status(sdir, 'pruning-worker').records
        assert (rec.kind, rec.result, rec.last_error) == ('prune', 'error', 'OSError: too many open files')

    def test_the_final_flush_runs_even_when_the_loop_dies(self, mocker, sdir):
        """`finally`: an exception out of the loop machinery still publishes what was recorded."""
        worker = snapshot_worker(sdir)
        manager = manager_for(Dsi('pool/a'))
        manager.needs_snapshot.return_value = True
        manager.create_snapshot.return_value = SimpleNamespace(name='autosnap_1')
        mocker.patch('zfsbackup.workers.DatasetManager', return_value=manager)
        mocker.patch.object(worker, '_wait', side_effect=KeyboardInterrupt)
        mocker.patch.object(StatusWriter, 'flush', autospec=True,
                            side_effect=[False, False, False, False, False, False])
        flush = StatusWriter.flush
        with pytest.raises(KeyboardInterrupt):
            worker.run()
        assert flush.call_args_list[-1].kwargs == {'force': True, 'heartbeat': False}

    def test_an_unusable_status_writer_does_not_stop_the_worker(self, run, sdir, mocker, caplog):
        mocker.patch.object(StatusWriter, 'load', side_effect=RuntimeError('odd'))
        worker = snapshot_worker(sdir)
        manager = manager_for(Dsi('pool/a'))
        manager.needs_snapshot.return_value = True
        manager.create_snapshot.return_value = SimpleNamespace(name='autosnap_1')
        with caplog.at_level(logging.WARNING, logger='zfsbackup.workers'):
            run(worker, manager)
        assert worker._status is None
        assert 'will not record its outcomes' in caplog.text and 'RuntimeError: odd' in caplog.text
        manager.create_snapshot.assert_called_once()              # the work itself went ahead

    def test_a_write_failure_never_reaches_the_loop(self, run, sdir, caplog):
        import shutil
        manager = manager_for(Dsi('pool/a'), Dsi('pool/b'))
        manager.needs_snapshot.return_value = True

        def create(dsi):
            shutil.rmtree(sdir, ignore_errors=True)
            return SimpleNamespace(name='autosnap_1')

        manager.create_snapshot.side_effect = create
        with caplog.at_level(logging.WARNING, logger='zfsbackup.health'):
            run(snapshot_worker(sdir, config_for('pool/a', 'pool/b')), manager)
        assert manager.create_snapshot.call_count == 2
        assert len([r for r in caplog.records if 'cannot write the status file' in r.getMessage()]) == 1


@pytest.mark.unit
class TestRetainAndCounters:
    def seed(self, sdir, worker, *keys):
        w = StatusWriter(sdir, worker)
        for kind, dataset, dest in keys:
            w.record(kind, dataset, dest, ok=False, error='old failure')
        w.flush(force=True)

    def test_a_dataset_removed_from_the_configuration_stops_being_reported(self, run, sdir):
        self.seed(sdir, 'snapshot-worker', ('snapshot', 'pool/gone', None), ('snapshot', 'pool/a', None))
        run(snapshot_worker(sdir), manager_for(Dsi('pool/a')))
        assert [r.dataset for r in status(sdir).records] == ['pool/a']

    def test_a_cycle_cut_short_by_a_stop_keeps_everything(self, mocker, sdir):
        """The list is only the truth when the cycle is complete."""
        self.seed(sdir, 'snapshot-worker', ('snapshot', 'pool/gone', None), ('snapshot', 'pool/a', None))
        worker = snapshot_worker(sdir)
        mocker.patch('zfsbackup.workers.DatasetManager', return_value=manager_for(Dsi('pool/a')))
        listed = []
        real_get = worker._get_datasets

        def get_datasets(manager):
            result = real_get(manager)
            listed.append(1)                       # the stop lands while the list is being made
            return result

        worker._get_datasets = get_datasets
        mocker.patch.object(worker, '_exit_reason',
                            side_effect=lambda: ExitReason.STOP if listed else None)
        worker.run()
        assert sorted(r.dataset for r in status(sdir).records) == ['pool/a', 'pool/gone']

    def test_a_restarted_worker_carries_the_counters(self, run, sdir):
        for expected in (1, 2, 3):
            manager = manager_for(Dsi('pool/a'))
            manager.needs_snapshot.return_value = True
            manager.create_snapshot.return_value = None
            manager.last_snapshot_error = 'Failed to create snapshot pool/a@x: dataset is busy'
            run(snapshot_worker(sdir), manager)
            (rec,) = status(sdir).records
            assert rec.consecutive_failures == expected
            assert rec.last_error == 'Failed to create snapshot pool/a@x: dataset is busy'

    def test_the_pid_and_start_are_the_new_incarnations(self, run, sdir, mocker):
        run(snapshot_worker(sdir), manager_for(Dsi('pool/a')))
        first = status(sdir)
        real = health.StatusWriter           # (not `os.getpid`: multiprocessing's heap relies on it)
        mocker.patch.object(health, 'StatusWriter',
                            lambda directory, worker: real(directory, worker, pid=424242))
        run(snapshot_worker(sdir), manager_for(Dsi('pool/a')))
        second = status(sdir)
        assert second.pid == 424242 and second.started_at > first.started_at


# ---------------------------------------------------------------------------
# What each worker records
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSnapshotWorkerRecords:
    def test_a_created_snapshot_is_an_ok_record_naming_it(self, run, sdir):
        manager = manager_for(Dsi('pool/a'))
        manager.needs_snapshot.return_value = True
        manager.create_snapshot.return_value = SimpleNamespace(name='autosnap_20261025100000')
        run(snapshot_worker(sdir), manager)
        (rec,) = status(sdir).records
        assert (rec.kind, rec.dataset, rec.destination, rec.result) == ('snapshot', 'pool/a', None, 'ok')
        assert rec.detail == 'autosnap_20261025100000' and rec.last_success is not None

    def test_a_dataset_that_is_not_due_records_nothing(self, run, sdir):
        manager = manager_for(Dsi('pool/a'))
        run(snapshot_worker(sdir), manager)
        manager.create_snapshot.assert_not_called()
        assert status(sdir).records == ()

    def test_a_failed_creation_uses_the_managers_reason(self, run, sdir):
        manager = manager_for(Dsi('pool/a'))
        manager.needs_snapshot.return_value = True
        manager.create_snapshot.return_value = None
        manager.last_snapshot_error = 'Failed to create snapshot pool/a@autosnap_1: out of space'
        run(snapshot_worker(sdir), manager)
        (rec,) = status(sdir).records
        assert rec.result == 'error' and rec.last_error == 'Failed to create snapshot pool/a@autosnap_1: out of space'

    @pytest.mark.parametrize('reason', [None, 5, MagicMock()])
    def test_a_failed_creation_without_a_usable_reason_still_says_something(self, run, sdir, reason):
        manager = manager_for(Dsi('pool/a'))
        manager.needs_snapshot.return_value = True
        manager.create_snapshot.return_value = None
        manager.last_snapshot_error = reason
        run(snapshot_worker(sdir), manager)
        assert status(sdir).records[0].last_error == 'the snapshot was not created (see the journal)'

    def test_a_success_after_a_failure_resets_the_count(self, run, sdir):
        for result in (None, None, SimpleNamespace(name='autosnap_9')):
            manager = manager_for(Dsi('pool/a'))
            manager.needs_snapshot.return_value = True
            manager.create_snapshot.return_value = result
            manager.last_snapshot_error = 'boom'
            run(snapshot_worker(sdir), manager)
        (rec,) = status(sdir).records
        assert rec.consecutive_failures == 0 and rec.result == 'ok' and rec.last_error == 'boom'


@pytest.mark.unit
class TestPruningWorkerRecords:
    def record(self, run, sdir, result):
        manager = manager_for(Dsi('pool/a'))
        manager.prune_snapshots.return_value = result
        run(pruning_worker(sdir), manager)
        return status(sdir, 'pruning-worker').records

    def test_a_clean_pass_is_ok_with_the_count(self, run, sdir):
        (rec,) = self.record(run, sdir, PruneResult(pruned=3))
        assert (rec.kind, rec.result, rec.detail) == ('prune', 'ok', 'pruned 3')

    def test_held_back_snapshots_are_not_a_failure(self, run, sdir):
        (rec,) = self.record(run, sdir, PruneResult(pruned=0, stuck=2))
        assert rec.result == 'ok' and rec.consecutive_failures == 0
        assert rec.detail == 'pruned 0, 2 held back'

    def test_failed_destroys_are_an_error_with_the_count(self, run, sdir):
        (rec,) = self.record(run, sdir, PruneResult(pruned=1, failed=2, stuck=2))
        assert rec.result == 'error' and rec.last_error == '2 snapshot(s) could not be destroyed'
        assert rec.detail == 'pruned 1, 2 held back'

    def test_a_skipped_pass_is_an_error_with_the_reason(self, run, sdir):
        (rec,) = self.record(run, sdir, PruneResult(skipped='cannot read the anchors: boom'))
        assert rec.result == 'error' and rec.last_error == 'cannot read the anchors: boom'

    def test_skipped_wins_over_the_failed_count(self, run, sdir):
        (rec,) = self.record(run, sdir, PruneResult(failed=3, skipped='why'))
        assert rec.last_error == 'why'

    @pytest.mark.parametrize('result', [None, MagicMock(), 'ok', 3, True])
    def test_anything_but_a_prune_result_records_nothing(self, run, sdir, result):
        assert self.record(run, sdir, result) == ()

    def test_a_received_dataset_is_recorded_by_its_name(self, run, sdir):
        manager = manager_for(Dsi('pool/a'))
        manager.received_datasets.return_value = [Dsi('bk/c1/pool/a')]
        manager.prune_snapshots.return_value = PruneResult()
        run(pruning_worker(sdir), manager)
        assert sorted(r.dataset for r in status(sdir, 'pruning-worker').records) == ['bk/c1/pool/a', 'pool/a']


@pytest.mark.unit
class TestDiscoverRecord:
    """The server's listing of received datasets, recorded under the backup target."""

    TARGET = 'bk/backups'

    def manager(self, error=None, received=()):
        manager = manager_for(Dsi('pool/a'))
        manager.config.remote_backup = SimpleNamespace(enabled=True, target_dataset=self.TARGET + '/')
        manager.received_datasets.return_value = [Dsi(n) for n in received]
        manager.last_received_error = error
        manager.prune_snapshots.return_value = PruneResult()
        return manager

    def test_a_good_listing_is_an_ok_record_with_the_count(self, run, sdir):
        run(pruning_worker(sdir), self.manager(received=('bk/backups/c1/x', 'bk/backups/c1/y')))
        rec = by_key(status(sdir, 'pruning-worker'))[('discover', self.TARGET, None)]
        assert rec.result == 'ok' and rec.detail == '2 received dataset(s)'

    def test_a_failed_listing_is_an_error_and_keeps_the_received_prune_records(self, run, sdir):
        seed = StatusWriter(sdir, 'pruning-worker')
        seed.record('prune', 'bk/backups/c1/x', ok=False, error='old problem')
        seed.record('prune', 'pool/gone', ok=False, error='old problem')
        seed.flush(force=True)
        run(pruning_worker(sdir), self.manager(error='CalledProcessError: pool I/O is suspended'))
        recs = by_key(status(sdir, 'pruning-worker'))
        assert recs[('discover', self.TARGET, None)].last_error == (
            'cannot list the received datasets: CalledProcessError: pool I/O is suspended')
        assert ('prune', 'bk/backups/c1/x', None) in recs          # `retain` was skipped
        assert ('prune', 'pool/gone', None) in recs
        assert ('prune', 'pool/a', None) in recs

    def test_after_a_good_listing_removed_datasets_are_dropped(self, run, sdir):
        seed = StatusWriter(sdir, 'pruning-worker')
        seed.record('prune', 'bk/backups/c1/x', ok=True)
        seed.flush(force=True)
        run(pruning_worker(sdir), self.manager(received=()))
        recs = by_key(status(sdir, 'pruning-worker'))
        assert ('prune', 'bk/backups/c1/x', None) not in recs
        assert ('discover', self.TARGET, None) in recs             # its own key is retained

    def test_no_record_when_remote_backup_is_off(self, run, sdir):
        manager = self.manager()
        manager.config.remote_backup = None
        run(pruning_worker(sdir), manager)
        assert all(r.kind != 'discover' for r in status(sdir, 'pruning-worker').records)

    def test_no_record_when_remote_backup_is_disabled(self, run, sdir):
        manager = self.manager()
        manager.config.remote_backup = SimpleNamespace(enabled=False, target_dataset=self.TARGET)
        run(pruning_worker(sdir), manager)
        assert all(r.kind != 'discover' for r in status(sdir, 'pruning-worker').records)

    def test_the_kind_is_a_known_one(self):
        assert health.KIND_DISCOVER == 'discover' and health.KIND_DISCOVER in health.STATUS_KINDS


class RemoteRig:
    def __init__(self, run, sdir, mocker, destinations=('off',), names=('pool/a',)):
        self.sdir, self.run = sdir, run
        self.rbm = MagicMock(name='RemoteBackupManager')
        self.rbm.backup_dataset.return_value = True
        self.rbm.last_problem.return_value = None
        mocker.patch('zfsbackup.remote.RemoteBackupManager', return_value=self.rbm)
        self.names, self.destinations = names, destinations
        self.manager = manager_for(*[Dsi(n, destinations) for n in names])
        self.config = config_for(*names, destinations=destinations)

    def go(self, worker=None, cycles=1):
        worker = worker or remote_worker(self.sdir, self.config)
        self.run(worker, self.manager, cycles)
        return worker

    def records(self):
        return by_key(status(self.sdir, 'remote-backup-worker'))


@pytest.fixture
def rrig(run, sdir, mocker):
    return RemoteRig(run, sdir, mocker)


@pytest.mark.unit
class TestRemoteWorkerRecords:
    def test_a_completed_send_is_ok(self, rrig):
        rrig.go()
        rec = rrig.records()[('send', 'pool/a', 'off')]
        assert rec.result == 'ok' and rec.last_success is not None and rec.consecutive_failures == 0

    def test_an_incomplete_send_is_an_error_with_the_managers_account(self, rrig):
        rrig.rbm.backup_dataset.return_value = False
        rrig.rbm.last_problem.return_value = 'resume token check failed: HTTP 500'
        rrig.go()
        rrig.rbm.last_problem.assert_called_once_with('pool/a', 'off')
        rec = rrig.records()[('send', 'pool/a', 'off')]
        assert rec.result == 'error' and rec.last_error == 'resume token check failed: HTTP 500'

    @pytest.mark.parametrize('problem', [None, 7, MagicMock()])
    def test_an_incomplete_send_without_an_account_says_so(self, rrig, problem):
        rrig.rbm.backup_dataset.return_value = False
        rrig.rbm.last_problem.return_value = problem
        rrig.go()
        assert rrig.records()[('send', 'pool/a', 'off')].last_error == 'not completed (see the journal)'

    def test_a_pending_stop_makes_a_false_neither_success_nor_failure(self, rrig):
        worker = remote_worker(rrig.sdir, rrig.config)

        def backup(dsi, remote_cfg, should_stop=None):
            worker.stop_event.set()
            assert should_stop() is True
            return False

        rrig.rbm.backup_dataset.side_effect = backup
        rrig.go(worker)
        assert rrig.records() == {}
        rrig.rbm.last_problem.assert_not_called()

    def test_a_pending_reload_makes_a_false_neither_success_nor_failure(self, run, sdir, mocker):
        rig = RemoteRig(run, sdir, mocker)
        epoch = ReloadEpoch()
        worker = remote_worker(sdir, rig.config, reload_epoch=epoch, born_epoch=0)

        def backup(dsi, remote_cfg, should_stop=None):
            epoch.set(1)
            return False

        rig.rbm.backup_dataset.side_effect = backup
        with pytest.raises(SystemExit) as exc:
            rig.go(worker)
        assert exc.value.code == WORKER_EXIT_RELOAD
        assert rig.records() == {}

    def test_a_true_while_a_stop_is_pending_is_still_a_success(self, rrig):
        worker = remote_worker(rrig.sdir, rrig.config)

        def backup(dsi, remote_cfg, should_stop=None):
            worker.stop_event.set()
            return True

        rrig.rbm.backup_dataset.side_effect = backup
        rrig.go(worker)
        assert rrig.records()[('send', 'pool/a', 'off')].result == 'ok'

    def test_an_exception_is_charged_to_the_destination_in_hand_and_still_logged(self, rrig, caplog):
        rrig.rbm.backup_dataset.side_effect = ConnectionError('reset by peer')
        with caplog.at_level(logging.ERROR, logger='zfsbackup.workers'):
            rrig.go()
        assert 'error on pool/a' in caplog.text
        rec = rrig.records()[('send', 'pool/a', 'off')]
        assert rec.result == 'error' and rec.last_error == 'ConnectionError: reset by peer'

    def test_destinations_are_recorded_independently_and_in_order(self, run, sdir, mocker):
        rig = RemoteRig(run, sdir, mocker, destinations=('one', 'two', 'three'))
        rig.rbm.backup_dataset.side_effect = lambda dsi, cfg, should_stop=None: cfg.destination != 'two'
        rig.rbm.last_problem.side_effect = lambda ds, dest: f'{dest} is down'
        rig.go()
        recs = rig.records()
        assert [recs[('send', 'pool/a', d)].result for d in ('one', 'two', 'three')] == ['ok', 'error', 'ok']
        assert recs[('send', 'pool/a', 'two')].last_error == 'two is down'

    def test_an_exception_at_the_second_destination_keeps_the_first_ones_success(self, run, sdir, mocker):
        rig = RemoteRig(run, sdir, mocker, destinations=('one', 'two'))

        def backup(dsi, cfg, should_stop=None):
            if cfg.destination == 'two':
                raise RuntimeError('boom')
            return True

        rig.rbm.backup_dataset.side_effect = backup
        rig.go()
        recs = rig.records()
        assert recs[('send', 'pool/a', 'one')].result == 'ok'
        assert recs[('send', 'pool/a', 'two')].last_error == 'RuntimeError: boom'

    def test_a_send_that_is_not_due_records_nothing(self, run, sdir, mocker):
        rig = RemoteRig(run, sdir, mocker)
        anchor = SimpleNamespace(name='autosnap_1', age=timedelta(minutes=5))
        rig.manager.get_anchor.return_value = 'autosnap_1'
        rig.manager.list_snapshots.return_value = [anchor]
        rig.manager.clock_behind.return_value = False
        rig.go()
        rig.rbm.backup_dataset.assert_not_called()
        assert rig.records() == {}

    def test_each_outcome_is_written_before_the_next_destination_begins(self, run, sdir, mocker):
        """The next destination may stream for hours."""
        rig = RemoteRig(run, sdir, mocker, destinations=('one', 'two'))
        seen = []

        def backup(dsi, cfg, should_stop=None):
            st = status(sdir, 'remote-backup-worker')
            seen.append((cfg.destination, None if st is None else sorted(r.destination for r in st.records)))
            return True

        rig.rbm.backup_dataset.side_effect = backup
        rig.go()
        assert seen == [('one', None), ('two', ['one'])]

    def test_a_recorded_send_is_flushed_at_once_but_the_no_record_case_is_not(self, run, sdir, mocker, flushes):
        rig = RemoteRig(run, sdir, mocker)
        rig.go()
        # before the dataset, FORCED right after the record, after the dataset, heartbeat, exit
        assert flushes == [(True, False), (True, False), (False, False), (True, True), (True, False)]
        del flushes[:]
        worker = remote_worker(sdir, rig.config)

        def backup(dsi, remote_cfg, should_stop=None):
            worker.stop_event.set()
            return False

        rig.rbm.backup_dataset.side_effect = backup
        rig.go(worker)
        assert flushes == [(True, False), (False, False), (True, True), (True, False)]

    def test_status_keys_are_dataset_destination_pairs(self, rrig):
        worker = remote_worker(rrig.sdir, rrig.config)
        keys = worker._status_keys([Dsi('pool/a', ('one', 'two')), Dsi('pool/b', ()), Dsi('pool/c', ('one',))])
        assert keys == {('pool/a', 'one'), ('pool/a', 'two'), ('pool/c', 'one')}

    def test_a_removed_destination_stops_being_reported(self, run, sdir, mocker):
        seed = StatusWriter(sdir, 'remote-backup-worker')
        seed.record('send', 'pool/a', 'old-dest', ok=False, error='x')
        seed.record('send', 'pool/a', 'off', ok=False, error='x')
        seed.flush(force=True)
        rig = RemoteRig(run, sdir, mocker)
        rig.go()
        assert sorted(k[2] for k in rig.records()) == ['off']


@pytest.mark.unit
class TestRemoteWorkerSeedsTheSkipCounts:
    NOTE = 'resume token check failed'

    def seed(self, sdir, *rows):
        w = StatusWriter(sdir, 'remote-backup-worker')
        for dataset, dest, failures, text, ok_last in rows:
            for _ in range(failures):
                w.record('send', dataset, dest, ok=False, error=text)
            if ok_last:
                w.record('send', dataset, dest, ok=True)
        w.flush(force=True)

    def test_the_note_is_the_one_the_manager_writes(self):
        from zfsbackup.remote import TOKEN_SKIP_NOTE
        assert TOKEN_SKIP_NOTE == self.NOTE

    def test_skip_records_seed_their_consecutive_failures(self, run, sdir, mocker):
        self.seed(sdir, ('pool/a', 'off', 2, f'{self.NOTE}: HTTP 500', False))
        rig = RemoteRig(run, sdir, mocker)
        rig.go()
        rig.rbm.seed_token_skips.assert_called_once_with({('pool/a', 'off'): 2})

    def test_other_failures_do_not_seed(self, run, sdir, mocker):
        self.seed(sdir, ('pool/a', 'off', 4, 'connection refused', False))
        rig = RemoteRig(run, sdir, mocker)
        rig.go()
        rig.rbm.seed_token_skips.assert_not_called()

    def test_a_record_that_has_since_succeeded_does_not_seed(self, run, sdir, mocker):
        self.seed(sdir, ('pool/a', 'off', 2, f'{self.NOTE}: x', True))
        rig = RemoteRig(run, sdir, mocker)
        rig.go()
        rig.rbm.seed_token_skips.assert_not_called()

    def test_each_pair_is_seeded_on_its_own(self, run, sdir, mocker):
        self.seed(sdir, ('pool/a', 'one', 2, f'{self.NOTE}: x', False), ('pool/a', 'two', 1, f'{self.NOTE}: y', False),
                  ('pool/a', 'three', 5, 'other', False))
        rig = RemoteRig(run, sdir, mocker, destinations=('one', 'two', 'three'))
        rig.go()
        rig.rbm.seed_token_skips.assert_called_once_with({('pool/a', 'one'): 2, ('pool/a', 'two'): 1})

    def test_no_writer_no_seed(self, run, mocker):
        rig = RemoteRig(run, Path('/nonexistent'), mocker)
        worker = RemoteBackupWorker(rig.config, StopFlag())
        rig.go(worker)
        rig.rbm.seed_token_skips.assert_not_called()


# ---------------------------------------------------------------------------
# The managers' side
# ---------------------------------------------------------------------------


def _dm(dry_run=False):
    manager = DatasetManager(config_for('pool/data', dry_run=dry_run))
    return manager, manager.datasets[0]


@pytest.mark.unit
class TestManagerErrorAttributes:
    def test_fresh_managers_have_none(self):
        manager, _ = _dm()
        assert manager.last_list_error is None
        assert manager.last_snapshot_error is None
        assert manager.last_received_error is None

    def test_a_failed_listing_is_remembered_and_the_next_good_one_clears_it(self, mocker):
        manager, dsi = _dm()
        strict = mocker.patch.object(manager, '_list_snapshots_strict', side_effect=RuntimeError('pool is suspended'))
        assert manager.list_snapshots(dsi) == []
        assert manager.last_list_error == 'RuntimeError: pool is suspended'
        strict.side_effect = None
        strict.return_value = []
        assert manager.list_snapshots(dsi) == []
        assert manager.last_list_error is None

    def test_a_failed_creation_is_remembered_with_the_name_and_the_cause(self, mocker):
        manager, dsi = _dm()
        mocker.patch('zfsbackup.backup_manager.zfs.snapshot', side_effect=RuntimeError('out of space'))
        assert manager.create_snapshot(dsi) is None
        assert manager.last_snapshot_error.startswith('Failed to create snapshot pool/data@')
        assert manager.last_snapshot_error.endswith('out of space')

    def test_a_good_creation_clears_it(self, mocker):
        manager, dsi = _dm()
        snap = mocker.patch(
            'zfsbackup.backup_manager.zfs.snapshot',
            side_effect=[RuntimeError('x'), listed_snapshot('autosnap_20261025100000', 'pool/data')],
        )
        mocker.patch.object(manager, '_list_snapshots_strict', return_value=[])
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_error is not None
        manager.create_snapshot(dsi)
        assert manager.last_snapshot_error is None and snap.call_count == 2

    def test_a_dry_run_creation_is_none_without_an_error(self):
        manager, dsi = _dm(dry_run=True)
        assert manager.create_snapshot(dsi) is None
        assert manager.last_snapshot_error is None

    def test_a_failed_received_listing_is_remembered_and_the_next_one_clears_it(self, mocker):
        config = config_for('pool/a', remote_backup=RemoteServerConfig(target_dataset='bk/backups', enabled=True))
        manager = DatasetManager(config)
        lister = mocker.patch('zfsbackup.backup_manager.zfs.list', side_effect=OSError('no zfs'))
        assert manager.received_datasets() == []
        assert manager.last_received_error == 'OSError: no zfs'
        lister.side_effect = None
        lister.return_value = []
        assert manager.received_datasets() == []
        assert manager.last_received_error is None

    def test_remote_backup_off_is_not_an_error(self):
        manager, _ = _dm()
        manager.last_received_error = 'stale'
        assert manager.received_datasets() == []
        assert manager.last_received_error is None


@pytest.mark.unit
class TestPruneResult:
    DS = 'pool/data'

    def test_the_dataclass(self):
        assert PruneResult() == PruneResult(0, 0, 0, None)
        assert PruneResult().ok and PruneResult(pruned=5, stuck=3).ok
        assert not PruneResult(failed=1).ok and not PruneResult(skipped='x').ok
        assert PruneResult(pruned=4).summary() == 'pruned 4'
        assert PruneResult(pruned=4, stuck=2).summary() == 'pruned 4, 2 held back'
        with pytest.raises(Exception):
            PruneResult().pruned = 1                               # frozen

    def test_a_clean_pass_counts_what_it_destroyed(self, prune_pool):
        manager, dsi = prune_manager(self.DS)
        prune_pool.populate(self.DS, old=3)
        result = manager.prune_snapshots(dsi)
        assert result == PruneResult(pruned=3, failed=0, stuck=0, skipped=None) and result.ok

    def test_nothing_to_prune_is_an_ok_zero(self, prune_pool):
        manager, dsi = prune_manager(self.DS)
        prune_pool.populate(self.DS, old=0)
        assert manager.prune_snapshots(dsi) == PruneResult()

    def test_held_and_cloned_snapshots_are_stuck_not_failed(self, prune_pool):
        manager, dsi = prune_manager(self.DS)
        fresh, olds = prune_pool.populate(self.DS, old=4)
        prune_pool.hold(self.DS, olds[0])
        prune_pool.clone(self.DS, olds[1], 'pool/clone')
        result = manager.prune_snapshots(dsi)
        assert (result.pruned, result.failed, result.stuck, result.skipped) == (2, 0, 2, None)
        assert result.ok and result.summary() == 'pruned 2, 2 held back'

    def test_only_held_snapshots_prune_nothing_and_are_ok(self, prune_pool):
        manager, dsi = prune_manager(self.DS)
        fresh, olds = prune_pool.populate(self.DS, old=2)
        for name in olds:
            prune_pool.hold(self.DS, name)
        result = manager.prune_snapshots(dsi)
        assert (result.pruned, result.failed, result.stuck) == (0, 0, 2) and result.ok

    def test_stuck_counts_only_this_datasets_names(self, prune_pool):
        manager, dsi = prune_manager(self.DS)
        manager._prune_stuck[('other/ds', 'x')] = 'held'
        prune_pool.populate(self.DS, old=1)
        assert manager.prune_snapshots(dsi).stuck == 0

    def test_destroys_that_fail_are_counted_and_remembered_as_stuck(self, prune_pool):
        manager, dsi = prune_manager(self.DS)
        fresh, olds = prune_pool.populate(self.DS, old=3)
        for name in olds:
            prune_pool.send(self.DS, name)
        result = manager.prune_snapshots(dsi)
        assert (result.pruned, result.failed, result.stuck) == (0, 3, 3)
        assert not result.ok

    def test_an_unlistable_dataset_is_skipped_with_the_cause(self, mocker):
        manager, dsi = prune_manager(self.DS)
        mocker.patch.object(manager, '_list_snapshots_strict', side_effect=RuntimeError('pool is suspended'))
        result = manager.prune_snapshots(dsi)
        assert result.skipped == 'cannot list snapshots: RuntimeError: pool is suspended'
        assert not result.ok

    def test_a_dataset_with_genuinely_no_snapshots_is_not_a_failure(self, mocker):
        manager, dsi = prune_manager(self.DS)
        mocker.patch.object(manager, '_list_snapshots_strict', return_value=[])
        assert manager.prune_snapshots(dsi) == PruneResult()

    def test_unreadable_anchors_skip_the_pass(self, prune_pool, mocker):
        manager, dsi = prune_manager(self.DS)
        prune_pool.populate(self.DS, old=2)
        mocker.patch.object(manager, 'get_anchors', side_effect=RuntimeError('property unreadable'))
        result = manager.prune_snapshots(dsi)
        assert result.skipped == 'cannot read the anchors: property unreadable' and result.pruned == 0
        assert prune_pool.calls == []

    def test_unevaluable_descendants_skip_the_pass(self, prune_pool, mocker):
        manager, dsi = prune_manager(self.DS, recursive=True)
        prune_pool.populate(self.DS, old=2)
        mocker.patch.object(manager, '_descendant_retention_protection', side_effect=RuntimeError('child unreadable'))
        result = manager.prune_snapshots(dsi)
        assert result.skipped == 'cannot evaluate the descendants: child unreadable'
        assert prune_pool.calls == []

    def test_a_failed_reread_before_destroying_is_reported(self, prune_pool, mocker):
        manager, dsi = prune_manager(self.DS)
        prune_pool.populate(self.DS, old=2)
        real = manager.get_anchors
        calls = []

        def anchors(d, **kw):
            calls.append(kw)
            if kw.get('own_properties_last'):
                raise RuntimeError('reread failed')
            return real(d, **kw)

        mocker.patch.object(manager, 'get_anchors', side_effect=anchors)
        result = manager.prune_snapshots(dsi)
        assert result.skipped == 'cannot re-read the anchors before destroying: reread failed'
        assert result.pruned == 0 and prune_pool.calls == []

    def test_the_reread_error_does_not_leak_into_the_next_pass(self, prune_pool, mocker):
        manager, dsi = prune_manager(self.DS)
        prune_pool.populate(self.DS, old=2)
        real = manager.get_anchors
        fail = [True]

        def anchors(d, **kw):
            if kw.get('own_properties_last') and fail[0]:
                raise RuntimeError('reread failed')
            return real(d, **kw)

        mocker.patch.object(manager, 'get_anchors', side_effect=anchors)
        assert manager.prune_snapshots(dsi).skipped is not None
        fail[0] = False
        result = manager.prune_snapshots(dsi)
        assert result.skipped is None and result.pruned == 2

    def test_dry_run_counts_nothing_destroyed(self, prune_pool):
        manager, dsi = prune_manager(self.DS, dry_run=True)
        prune_pool.populate(self.DS, old=3)
        result = manager.prune_snapshots(dsi)
        assert prune_pool.calls == [] and result.ok and result.failed == 0


# ---------------------------------------------------------------------------
# Real worker processes
# ---------------------------------------------------------------------------


def wait_until(predicate, timeout=60.0, step=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(step)
    return predicate()


@pytest.fixture
def launch(tmp_path, sdir):
    """`launch(method, plan, cls=..., **kw)` -> the started worker. Everything is killed at teardown."""
    import json
    started = []
    plan_path = tmp_path / 'plan.json'
    (tmp_path / 'config.db').write_bytes(b'')

    def start(method, plan=None, cls=_status_workers.ScriptedSnapshotWorker, status_dir=sdir,
              epoch=None, born=0, config=None, lock=True, stop=None):
        plan_path.write_text(json.dumps(plan or {}))
        worker = cls(
            config or config_for('pool/a', 'pool/b'), stop or StopFlag(),
            reload_epoch=epoch, born_epoch=born,
            workers_lock=str(daemonlock.ensure_workers_lock(tmp_path / 'config.db')) if lock else None,
            status_dir=status_dir,
        )
        worker.start_method = method
        worker.plan_path = str(plan_path)
        started.append(worker)
        worker.start()
        return worker

    yield start
    for worker in started:
        if worker.is_alive():
            worker.kill()
        worker.join(15)


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
@pytest.mark.parametrize('method', START_METHODS)
class TestRealWorkerProcesses:
    def file_with(self, sdir, pid, name='snapshot-worker', records=2):
        def ready():
            try:
                st = read_worker_status(sdir, name)
            except health.StatusError:
                return False
            return st is not None and st.pid == pid and len(st.records) >= records
        return ready

    def test_the_file_appears_with_the_workers_own_pid(self, launch, sdir, method):
        stop = StopFlag()
        worker = launch(method, {'pool/a': 'ok', 'pool/b': 'fail'}, stop=stop)
        assert wait_until(self.file_with(sdir, worker.pid))
        assert worker.pid != os.getpid()
        st = status(sdir)
        assert st.worker == 'snapshot-worker' and st.pid == worker.pid
        recs = by_key(st)
        assert recs[('snapshot', 'pool/a', None)].result == 'ok'
        assert recs[('snapshot', 'pool/b', None)].last_error == 'scripted failure'
        assert (Path(sdir) / 'snapshot-worker.json').stat().st_mode & 0o777 == 0o640
        stop.set()
        worker.join(30)
        assert worker.exitcode == 0

    def test_a_reload_restart_carries_the_counters(self, launch, sdir, method):
        epoch = ReloadEpoch()
        first = launch(method, {'pool/a': 'fail', 'pool/b': 'ok'}, epoch=epoch, born=0)
        assert wait_until(self.file_with(sdir, first.pid))
        epoch.set(1)
        first.join(60)
        assert first.exitcode == WORKER_EXIT_RELOAD
        second = launch(method, {'pool/a': 'fail', 'pool/b': 'ok'}, epoch=epoch, born=1)
        assert wait_until(self.file_with(sdir, second.pid))
        recs = by_key(status(sdir))
        assert recs[('snapshot', 'pool/a', None)].consecutive_failures == 2
        assert recs[('snapshot', 'pool/b', None)].consecutive_failures == 0
        assert status(sdir).pid == second.pid

    def test_a_crash_restart_carries_the_counters(self, launch, sdir, method):
        first = launch(method, {'pool/a': 'fail', 'pool/b': 'fail'})
        assert wait_until(self.file_with(sdir, first.pid))
        first.kill()
        first.join(30)
        assert first.exitcode != 0
        second = launch(method, {'pool/a': 'fail', 'pool/b': 'ok'})
        assert wait_until(self.file_with(sdir, second.pid))
        recs = by_key(status(sdir))
        assert recs[('snapshot', 'pool/a', None)].consecutive_failures == 2
        assert recs[('snapshot', 'pool/b', None)].consecutive_failures == 0
        assert recs[('snapshot', 'pool/b', None)].last_error == 'scripted failure'     # kept after the success

    def test_the_pruning_worker_writes_its_own_file(self, launch, sdir, method):
        stop = StopFlag()
        worker = launch(method, cls=_status_workers.ScriptedPruningWorker, stop=stop)
        assert wait_until(self.file_with(sdir, worker.pid, 'pruning-worker', records=1))
        assert not (sdir / 'snapshot-worker.json').exists()
        stop.set()
        worker.join(30)
        assert worker.exitcode == 0

    def test_a_dry_run_worker_writes_nothing(self, launch, sdir, method):
        stop = StopFlag()
        worker = launch(method, {'pool/a': 'ok'}, config=config_for('pool/a', dry_run=True), stop=stop)
        time.sleep(3 if method != 'fork' else 1.5)
        assert worker.is_alive()
        stop.set()
        worker.join(30)
        assert worker.exitcode == 0 and list(sdir.iterdir()) == []

    def test_a_worker_without_a_directory_writes_nothing(self, launch, sdir, method):
        stop = StopFlag()
        worker = launch(method, {'pool/a': 'ok'}, status_dir=None, stop=stop)
        time.sleep(3 if method != 'fork' else 1.5)
        stop.set()
        worker.join(30)
        assert worker.exitcode == 0 and list(sdir.iterdir()) == []

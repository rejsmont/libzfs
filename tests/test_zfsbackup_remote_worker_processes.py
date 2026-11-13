"""The remote worker as a REAL process (Track B, item B3): the scheduler loop and its sender threads under
fork, spawn and forkserver (`tests/_remote_process_workers.py`: the real `RemoteBackupWorker.run()` over scripted
senders, the real status writer).

* a stop and a reload with a long send in flight: the worker leaves only after the sender thread, exits 0
  (stop) or 3 (reload), and the drained sender's outcome is on disk;
* the status file: this incarnation's file is on disk before any send finishes, records carry over a reload,
  the heartbeat moves `updated_at` while the worker is idle;
* a REAL `kill -9` of the supervisor with a send in flight: the worker notices, leaves its loop, joins the
  sender and exits;
* `_wait_for_stuck_writes` runs only after EVERY sender thread was joined, and only for a reload or an
  upgrade drain (in-process, over the real worker and loop).

The epoch -1 (upgrade) drain with real processes is `test_zfsbackup_version_skew.py`; the real daemon with
the remote worker (`kill -9`, stop, reload) is `test_zfsbackup_daemon_closeout.py` and
`test_zfsbackup_reload.py`. Every process is killed at teardown; every wait has a hard bound.
"""

import json
import multiprocessing
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import timedelta

import pytest

import _remote_process_workers as rpw
from _hermetic import hermetic_env
from _sendloop_support import LoopRig, bound, stretch, wait_for
from zfsbackup import daemonlock, health
from zfsbackup.config import BackupConfig, DatasetConfig, Destination, RemoteDatasetConfig
from zfsbackup.health import ensure_status_dir, read_worker_status
from zfsbackup.stopflag import ReloadEpoch, StopFlag
from zfsbackup.workers import WORKER_EXIT_RELOAD

START_METHODS = [
    m for m in ('fork', 'spawn', 'forkserver') if m in multiprocessing.get_all_start_methods()
]
NAME = 'remote-backup-worker'


def config_for(*names, check_interval=timedelta(seconds=1)):
    return BackupConfig(
        datasets=[DatasetConfig(name=n, remote=[RemoteDatasetConfig(destination='d')]) for n in names],
        destinations={'d': Destination(url='https://h.example:8443')},
        check_interval=check_interval,
    )


def events(rec):
    path = rec / 'events'
    return path.read_text().split() if path.exists() else []


def alive(pid) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    state = subprocess.run(['ps', '-o', 'stat=', '-p', str(pid)], capture_output=True, text=True).stdout.strip()
    return bool(state) and not state.startswith('Z')


@pytest.fixture
def launch(tmp_path):
    """`launch(method, plan, ...)` -> `(worker, rec_dir, stop, epoch)`. Everything is released and killed at teardown."""
    sdir = tmp_path / 'config.db.status'
    (tmp_path / 'config.db').write_bytes(b'')
    ensure_status_dir(sdir)
    made = []

    def start(method, plan, *, epoch=None, born=0, stop=None, tag='a'):
        rec = tmp_path / f'rec-{tag}'
        rec.mkdir(exist_ok=True)
        plan_path = tmp_path / f'plan-{tag}.json'
        plan_path.write_text(json.dumps(plan))
        stop = stop if stop is not None else StopFlag()
        worker = rpw.ScriptedRemoteWorker(
            config_for(*plan), stop, reload_epoch=epoch, born_epoch=born, status_dir=sdir,
            workers_lock=str(daemonlock.ensure_workers_lock(tmp_path / 'config.db')),
        )
        worker.start_method = method
        worker.record_dir, worker.plan_path = str(rec), str(plan_path)
        worker.release_path = str(rec / 'release')
        made.append((worker, rec, stop))
        worker.start()
        return worker, rec, stop

    start.sdir = sdir
    yield start
    # The normal path ends every worker CLEANLY: ask it to stop (a worker a test left idle has no other exit
    # trigger), release every gate, join. SIGKILL is the last resort and is then reported as a failure.
    killed = []
    for worker, rec, stop in made:
        stop.set()
        (rec / 'release').touch()
    for worker, rec, stop in made:
        worker.join(bound(60))
        if worker.is_alive():
            killed.append(worker.pid)
            worker.kill()
            worker.join(bound(10))
    assert not killed, f'worker(s) {killed} did not leave after a stop and had to be killed'
    for worker, rec, stop in made:
        assert worker.exitcode in (0, WORKER_EXIT_RELOAD), f'worker exit code {worker.exitcode}'


def status(sdir, pid=None):
    try:
        st = read_worker_status(sdir, NAME)
    except health.StatusError:
        return None
    return st if st is not None and (pid is None or st.pid == pid) else None


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.filterwarnings(
    "ignore:This process \\(pid=.*\\) is multi-threaded, use of fork\\(\\) "
    "may lead to deadlocks in the child:DeprecationWarning"
)
@pytest.mark.parametrize('method', START_METHODS)
class TestRealRemoteWorker:
    def test_a_stop_waits_for_the_long_send_and_exits_0(self, launch, method):
        worker, rec, stop = launch(method, {'pool/a': 'gate'})
        assert wait_for(lambda: 'start:pool/a' in events(rec), bound(90))
        stop.set()
        time.sleep(1.5 * stretch())
        assert worker.is_alive(), 'the worker leaves only after the sender thread (a stream is never cut mid-way)'
        (rec / 'release').touch()
        worker.join(bound(60))
        assert worker.exitcode == 0
        assert events(rec) == ['start:pool/a', 'done:pool/a']
        rec_status = status(launch.sdir, worker.pid)
        assert rec_status is not None and rec_status.records[0].result == 'ok', 'the drained send was recorded'

    def test_a_reload_exits_3_after_the_long_send_and_records_it(self, launch, method):
        epoch = ReloadEpoch()
        worker, rec, _ = launch(method, {'pool/a': 'gate', 'pool/b': 'ok'}, epoch=epoch)
        assert wait_for(lambda: {'start:pool/a', 'done:pool/b'} <= set(events(rec)), bound(90))
        epoch.set(1)
        time.sleep(1.5 * stretch())
        assert worker.is_alive(), 'a reload never interrupts the send in hand'
        (rec / 'release').touch()
        worker.join(bound(60))
        assert worker.exitcode == WORKER_EXIT_RELOAD
        records = {r.dataset: r for r in status(launch.sdir, worker.pid).records}
        assert records['pool/a'].result == 'ok' and records['pool/b'].result == 'ok'

    def test_the_first_flush_is_before_any_send_finishes(self, launch, method):
        worker, rec, _ = launch(method, {'pool/a': 'gate'})
        assert wait_for(lambda: 'start:pool/a' in events(rec), bound(90))
        st = status(launch.sdir, worker.pid)
        assert st is not None and st.started_at is not None and st.records == (), \
            'this incarnation is on disk while its only send is still running'

    def test_a_reload_restart_keeps_the_records_and_writes_its_own_file_at_once(self, launch, method):
        epoch = ReloadEpoch()
        first, rec1, _ = launch(method, {'pool/a': 'fail', 'pool/b': 'ok'}, epoch=epoch, tag='1')
        assert wait_for(lambda: (st := status(launch.sdir, first.pid)) is not None and len(st.records) == 2, bound(90))
        epoch.set(1)
        first.join(bound(60))
        assert first.exitcode == WORKER_EXIT_RELOAD
        # the second incarnation: both pairs were attempted a moment ago, so neither is sent again, yet its
        # file (its own pid) is there at once and the failure count is carried over, not reset
        second, rec2, _ = launch(method, {'pool/a': 'fail', 'pool/b': 'ok'}, epoch=epoch, born=1, tag='2')
        assert wait_for(lambda: status(launch.sdir, second.pid) is not None, bound(90))
        # the file appears at the loop's start, BEFORE its first tick could dispatch anything: a heartbeat later
        # than that first write proves at least one tick has looked (and found nothing due) since
        written = status(launch.sdir, second.pid).updated_at
        assert wait_for(lambda: (st := status(launch.sdir, second.pid)) is not None and st.updated_at > written,
                        bound(30)), 'no heartbeat from the second incarnation'
        records = {r.dataset: r for r in status(launch.sdir, second.pid).records}
        assert records['pool/a'].result == 'error' and records['pool/a'].consecutive_failures == 1
        assert records['pool/b'].result == 'ok'
        assert events(rec2) == [], 'a pair attempted in the current slot is not re-sent after a reload'

    def test_a_chain_cut_by_a_reload_is_sent_again_by_the_successor_but_a_complete_one_is_not(self, launch, method):
        """`pool/a`'s chain was cut (a retryable DONE: recorded as an `incomplete` success), `pool/b` completed.
        Both were attempted a moment ago, yet the successor catches `pool/a` up at once (the cut attempt did not
        serve its slot) and leaves `pool/b` alone until its next slot."""
        epoch = ReloadEpoch()
        first, rec1, _ = launch(method, {'pool/a': 'cut', 'pool/b': 'ok'}, epoch=epoch, tag='1')
        assert wait_for(lambda: (st := status(launch.sdir, first.pid)) is not None and len(st.records) == 2, bound(90))
        records = {r.dataset: r for r in status(launch.sdir, first.pid).records}
        assert records['pool/a'].incomplete is True and records['pool/a'].result == 'ok'
        assert records['pool/b'].incomplete is False
        raw = json.loads((launch.sdir / f'{NAME}.json').read_text())['records']
        assert {r['dataset']: r.get('incomplete') for r in raw} == {'pool/a': True, 'pool/b': None}
        epoch.set(1)
        first.join(bound(60))
        assert first.exitcode == WORKER_EXIT_RELOAD
        second, rec2, _ = launch(method, {'pool/a': 'ok', 'pool/b': 'ok'}, epoch=epoch, born=1, tag='2')
        assert wait_for(lambda: 'done:pool/a' in events(rec2), bound(90)), \
            'the successor did not catch the cut pair up'
        # a later heartbeat: at least one more tick looked at `pool/b` and found it served
        written = status(launch.sdir, second.pid).updated_at
        assert wait_for(lambda: (st := status(launch.sdir, second.pid)) is not None and st.updated_at > written,
                        bound(30)), 'no heartbeat from the second incarnation'
        assert events(rec2) == ['start:pool/a', 'done:pool/a'], 'a complete DONE is not re-sent after a reload'
        records = {r.dataset: r for r in status(launch.sdir, second.pid).records}
        assert records['pool/a'].incomplete is False and records['pool/a'].result == 'ok'
        assert records['pool/b'].incomplete is False

    def test_the_heartbeat_moves_updated_at_while_the_worker_is_idle(self, launch, method):
        worker, rec, stop = launch(method, {'pool/a': 'ok'})
        assert wait_for(lambda: status(launch.sdir, worker.pid) is not None
                        and status(launch.sdir, worker.pid).records, bound(90))
        first = status(launch.sdir, worker.pid).updated_at
        assert wait_for(lambda: status(launch.sdir, worker.pid).updated_at > first, bound(30)), \
            'no heartbeat: `check` would call this worker dead'
        stop.set()
        worker.join(bound(60))
        assert worker.exitcode == 0


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.parametrize('method', START_METHODS)
class TestSupervisorKilled:
    def test_kill_9_of_the_supervisor_with_a_send_in_flight(self, tmp_path, method):
        """The worker notices (`is gone`), leaves its loop, joins the sender thread and exits; it never
        leaves a send running behind it."""
        rec = tmp_path / 'rec'
        rec.mkdir()
        plan = tmp_path / 'plan.json'
        plan.write_text(json.dumps({'pool/a': 'gate'}))
        pid_file = tmp_path / 'worker.pid'
        release = rec / 'release'
        code = (
            "import sys\n"
            "import _remote_process_workers as rpw\n"
            "from zfsbackup.config import BackupConfig, DatasetConfig, Destination, RemoteDatasetConfig\n"
            "cfg = BackupConfig(datasets=[DatasetConfig(name='pool/a', remote=[RemoteDatasetConfig(destination='d')])],\n"
            "                   destinations={'d': Destination(url='https://h.example:8443')})\n"
            "rpw.supervisor_main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], cfg, sys.argv[5])\n"
        )
        supervisor = subprocess.Popen(
            [sys.executable, '-c', code, method, str(rec), str(plan), str(release), str(pid_file)],
            env=hermetic_env(PYTHONPATH=os.pathsep.join(sys.path)), stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, start_new_session=True,
        )
        worker_pid = None
        try:
            assert wait_for(lambda: pid_file.exists() and pid_file.read_text().strip(), bound(90))
            worker_pid = int(pid_file.read_text())
            assert wait_for(lambda: 'start:pool/a' in events(rec), bound(90))
            supervisor.send_signal(signal.SIGKILL)
            supervisor.wait(timeout=bound(30))
            time.sleep(2.0 * stretch())
            assert alive(worker_pid), 'the sender is joined first: the worker must not die mid-send'
            release.touch()
            assert wait_for(lambda: not alive(worker_pid), bound(60)), 'the worker did not leave after its supervisor died'
            assert events(rec) == ['start:pool/a', 'done:pool/a']
        finally:
            release.touch()
            if supervisor.poll() is None:
                supervisor.kill()
            if worker_pid is not None and alive(worker_pid):
                os.kill(worker_pid, signal.SIGKILL)


@pytest.mark.integration
class TestStuckWritesWaitAfterTheSenders:
    """`_finish` waits for abandoned `zfs` write children only after `SendLoop.run` returned (every sender
    joined: the registry is final), and only for a reload or an upgrade drain."""

    def stuck(self, monkeypatch, alive_until):
        import libzfseasy
        child = libzfseasy.StuckChild(4242, 'zfs receive -s pool/x', time.monotonic())
        monkeypatch.setattr(libzfseasy, 'stuck_children', lambda: (child,) if not alive_until.is_set() else ())

    def rig(self, monkeypatch, epoch):
        return LoopRig(config_for('pool/a'), monkeypatch, epoch=epoch, born_epoch=0)

    def test_a_reload_waits_for_a_stuck_writer_but_only_after_the_sender_is_joined(self, monkeypatch, caplog):
        import logging
        gone = threading.Event()
        self.stuck(monkeypatch, gone)
        epoch = ReloadEpoch()
        rig = self.rig(monkeypatch, epoch)
        order = []
        release = threading.Event()
        rig.on_exit_release(release.set)

        def backup(dsi, cfg, should_stop=None):
            epoch.set(1)
            release.wait(bound())
            order.append('sender-ended')
            return True

        rig.remote.backup_dataset.side_effect = backup
        real_wait = rig.worker._wait_for_stuck_writes

        def wait(logger, action):
            order.append('stuck-wait-began')
            return real_wait(logger, action)

        rig.worker._wait_for_stuck_writes = wait
        problems = []

        def controller():
            """Watches from a thread (the worker's `run()` needs the main thread for its signal set-up)."""
            try:
                assert wait_for(lambda: rig.loop is not None and rig.loop.leave.is_set(), None)
                time.sleep(0.3 * stretch())
                if order != []:
                    problems.append(f'the stuck-child wait started while a sender was running: {order}')
                release.set()
                assert wait_for(lambda: 'stuck-wait-began' in order, None)
                time.sleep(0.2 * stretch())
                gone.set()
            except BaseException as e:  # noqa: BLE001
                problems.append(repr(e))
                gone.set()
                release.set()

        helper = threading.Thread(target=controller, daemon=True)
        helper.start()
        with caplog.at_level(logging.INFO, logger='zfsbackup.workers'):
            with pytest.raises(SystemExit) as exited:
                rig.run()
        helper.join(bound())
        assert problems == []
        assert exited.value.code == WORKER_EXIT_RELOAD
        assert order == ['sender-ended', 'stuck-wait-began']
        assert 'waiting for 1 timed-out zfs process(es) to exit before reloading' in caplog.text
        assert 'have exited' in caplog.text, 'it left only after the writer was gone'

    def test_a_stop_does_not_wait_for_stuck_writers(self, monkeypatch):
        import libzfseasy
        child = libzfseasy.StuckChild(4242, 'zfs receive -s pool/x', time.monotonic())
        monkeypatch.setattr(libzfseasy, 'stuck_children', lambda: (child,))
        rig = LoopRig(config_for('pool/a'), monkeypatch)
        started = time.monotonic()
        rig.run()
        assert time.monotonic() - started < bound(), 'a stop leaves at once: nothing replaces the worker'

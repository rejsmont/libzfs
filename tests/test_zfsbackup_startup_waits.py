"""Phase 3, item 18.5: a (re)starting daemon waits out the previous daemon's workers.

* `_wait_for_previous_workers` -- blocks while anything holds `<db>.workers.lock`
  (spawn/forkserver survivors of a `kill -9`'d supervisor hold no daemon lock, so
  this is the only trace of them), WARNING at the start and every 60 s, a
  detection aid rather than a gate.
* `_acquire_presence_lock` -- a daemon lock held by the fork orphans of a
  verifiably DEAD supervisor is waited out; a live one, a legacy file and an
  unreadable identity are refused immediately.
* `_new_worker` -- hands every worker the shared `ReloadEpoch`, the epoch it is
  born under, and the lock PATH (never an open descriptor).

The waits are driven through the seams `_startup_sleep`, `_startup_monotonic`,
`_STARTUP_WAIT_POLL` and `_STARTUP_WARN_EVERY`, so no test sleeps for a minute.
The end-to-end ones start a REAL daemon process and a REAL lock holder; every
process is killed at teardown.
"""

import logging
import os
import signal
import subprocess
import sys
import time

import pytest

import zfsbackup.daemon as daemon_module
from _reload_workers import descriptors_on
from _hermetic import hermetic_env
from test_zfsbackup_daemon import _args, _stub_daemon, no_hard_exit  # noqa: F401
from test_zfsbackup_daemonlock import (  # noqa: F401  (fixtures and helpers)
    _WORKERS_HELPER,
    REPO_ROOT,
    _hard_kill,
    plain_process,
    real_daemon_env,
    spawn_daemon,
    start_helper,
)
from zfsbackup import daemonlock, procident
from zfsbackup.config import BackupConfig, DatasetConfig
from zfsbackup.daemon import BackupDaemon
from zfsbackup.stopflag import ReloadEpoch

WARN_TEXT = "workers of a previous daemon still hold"


def wait_until(predicate, timeout=30.0, step=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(step)
    return predicate()


class FakeTime:
    """Replaces `_startup_sleep` and `_startup_monotonic`: sleeping advances the
    clock instantly, and `on_sleep(n)` runs after the n-th sleep (1-based)."""

    def __init__(self, monkeypatch, on_sleep=None):
        self.now = 5000.0
        self.sleeps = []
        self.on_sleep = on_sleep
        monkeypatch.setattr(daemon_module, "_startup_sleep", self.sleep)
        monkeypatch.setattr(daemon_module, "_startup_monotonic", lambda: self.now)

    def sleep(self, seconds):
        if len(self.sleeps) >= 400:
            raise RuntimeError("runaway startup wait: the code under test never stopped waiting")
        self.sleeps.append(seconds)
        self.now += seconds
        if self.on_sleep is not None:
            self.on_sleep(len(self.sleeps))


def warnings_of(caplog, text):
    return [r for r in caplog.records if r.levelno == logging.WARNING and text in r.getMessage()]


def make_config():
    return BackupConfig(datasets=[DatasetConfig(name="pool/data")])


# ---------------------------------------------------------------------------
# _wait_for_previous_workers
# ---------------------------------------------------------------------------


class TestWaitForPreviousWorkers:
    @pytest.fixture
    def held(self, tmp_path):
        db = tmp_path / "config.db"
        path = daemonlock.ensure_workers_lock(db)
        state = {"fd": daemonlock.hold_workers_lock(path)}
        assert state["fd"] is not None

        def release():
            if state["fd"] is not None:
                os.close(state["fd"])
                state["fd"] = None

        state["release"] = release
        yield db, path, state
        release()

    def test_the_seams_and_their_production_values(self):
        assert daemon_module._STARTUP_WAIT_POLL == 1.0
        assert daemon_module._STARTUP_WARN_EVERY == 60.0
        assert daemon_module._startup_sleep is time.sleep
        assert daemon_module._startup_monotonic is time.monotonic

    def test_nothing_held_returns_at_once_creating_the_lock_file(self, tmp_path, monkeypatch, caplog):
        db = tmp_path / "config.db"
        clock = FakeTime(monkeypatch)
        with caplog.at_level(logging.WARNING):
            daemon_module._wait_for_previous_workers(db)
        assert clock.sleeps == []
        assert caplog.records == []
        assert daemonlock.workers_lock_path(db).is_file()

    def test_it_waits_while_held_polling_once_a_second_and_proceeds_when_released(self, held, monkeypatch, caplog):
        db, path, state = held
        clock = FakeTime(monkeypatch, on_sleep=lambda n: state["release"]() if n == 3 else None)
        with caplog.at_level(logging.WARNING):
            daemon_module._wait_for_previous_workers(db)
        assert clock.sleeps == [1.0, 1.0, 1.0]
        assert len(warnings_of(caplog, WARN_TEXT)) == 1

    def test_the_warning_names_the_file_and_gives_a_fuser_hint(self, held, monkeypatch, caplog):
        db, path, state = held
        FakeTime(monkeypatch, on_sleep=lambda n: state["release"]())
        with caplog.at_level(logging.WARNING):
            daemon_module._wait_for_previous_workers(db)
        (record,) = warnings_of(caplog, WARN_TEXT)
        assert str(path) in record.getMessage()
        assert f"fuser {path}" in record.getMessage()
        assert "never interrupted" in record.getMessage()

    @pytest.mark.parametrize(
        "release_after, expected_warnings",
        [(1, 1), (59, 1), (60, 1), (61, 2), (119, 2), (121, 3), (130, 3)],
    )
    def test_warning_cadence_is_at_the_start_then_every_sixty_seconds(
        self, held, monkeypatch, caplog, release_after, expected_warnings
    ):
        db, path, state = held
        clock = FakeTime(monkeypatch, on_sleep=lambda n: state["release"]() if n == release_after else None)
        with caplog.at_level(logging.WARNING):
            daemon_module._wait_for_previous_workers(db)
        assert len(clock.sleeps) == release_after
        assert len(warnings_of(caplog, WARN_TEXT)) == expected_warnings

    def test_the_poll_interval_comes_from_the_seam(self, held, monkeypatch):
        db, path, state = held
        monkeypatch.setattr(daemon_module, "_STARTUP_WAIT_POLL", 0.25)
        clock = FakeTime(monkeypatch, on_sleep=lambda n: state["release"]())
        daemon_module._wait_for_previous_workers(db)
        assert clock.sleeps == [0.25]

    def test_the_warn_interval_comes_from_the_seam(self, held, monkeypatch, caplog):
        db, path, state = held
        monkeypatch.setattr(daemon_module, "_STARTUP_WARN_EVERY", 5.0)
        FakeTime(monkeypatch, on_sleep=lambda n: state["release"]() if n == 11 else None)
        with caplog.at_level(logging.WARNING):
            daemon_module._wait_for_previous_workers(db)
        assert len(warnings_of(caplog, WARN_TEXT)) == 3        # t = 0, 5, 10

    def test_a_shared_holder_is_enough_and_an_exclusive_one_too(self, tmp_path, monkeypatch):
        db = tmp_path / "config.db"
        path = daemonlock.ensure_workers_lock(db)
        a = daemonlock.hold_workers_lock(path)
        b = daemonlock.hold_workers_lock(path)
        closed = []

        def on_sleep(n):
            if n == 1:
                os.close(a)
                closed.append("a")
            if n == 2:
                os.close(b)
                closed.append("b")

        clock = FakeTime(monkeypatch, on_sleep=on_sleep)
        daemon_module._wait_for_previous_workers(db)
        assert closed == ["a", "b"] and len(clock.sleeps) == 2

    def test_it_leaves_no_descriptor_on_the_lock_behind(self, held, monkeypatch):
        db, path, state = held
        FakeTime(monkeypatch, on_sleep=lambda n: state["release"]())
        daemon_module._wait_for_previous_workers(db)
        assert descriptors_on(path) == 0

    def test_ensure_workers_lock_failing_warns_and_returns_without_waiting(self, tmp_path, monkeypatch, caplog, mocker):
        mocker.patch("zfsbackup.daemonlock.ensure_workers_lock", side_effect=daemonlock.DaemonLockError(13, "denied"))
        present = mocker.patch("zfsbackup.daemonlock.workers_present")
        clock = FakeTime(monkeypatch)
        with caplog.at_level(logging.WARNING):
            daemon_module._wait_for_previous_workers(tmp_path / "config.db")
        assert clock.sleeps == []
        present.assert_not_called()
        assert len(warnings_of(caplog, "cannot check for workers of a previous daemon")) == 1

    def test_workers_present_failing_warns_and_returns(self, tmp_path, monkeypatch, caplog, mocker):
        mocker.patch("zfsbackup.daemonlock.workers_present", side_effect=daemonlock.DaemonLockError(5, "eio"))
        clock = FakeTime(monkeypatch)
        with caplog.at_level(logging.WARNING):
            daemon_module._wait_for_previous_workers(tmp_path / "config.db")
        assert clock.sleeps == []
        assert len(warnings_of(caplog, "cannot check for workers of a previous daemon")) == 1

    def test_workers_present_failing_mid_wait_warns_and_stops_waiting(self, held, monkeypatch, caplog, mocker):
        db, path, state = held
        real = daemonlock.workers_present
        calls = []

        def flaky(resolved):
            calls.append(1)
            if len(calls) >= 3:
                raise OSError("went away")
            return real(resolved)

        mocker.patch("zfsbackup.daemonlock.workers_present", side_effect=flaky)
        clock = FakeTime(monkeypatch)
        with caplog.at_level(logging.WARNING):
            daemon_module._wait_for_previous_workers(db)
        assert len(clock.sleeps) == 2
        assert len(warnings_of(caplog, "cannot check for workers of a previous daemon")) == 1

    def test_a_plain_oserror_from_ensure_is_also_only_a_warning(self, tmp_path, monkeypatch, caplog, mocker):
        mocker.patch("zfsbackup.daemonlock.ensure_workers_lock", side_effect=OSError("odd"))
        FakeTime(monkeypatch)
        with caplog.at_level(logging.WARNING):
            daemon_module._wait_for_previous_workers(tmp_path / "config.db")
        assert warnings_of(caplog, "cannot check for workers")


# ---------------------------------------------------------------------------
# _acquire_presence_lock
# ---------------------------------------------------------------------------


@pytest.fixture
def lockdb(tmp_path):
    return tmp_path / "config.db"


@pytest.fixture
def took(request):
    """Locks `_acquire_presence_lock` returned, released at teardown."""
    locks = []
    yield locks
    for lock in locks:
        lock.release()


class TestAcquirePresenceLock:
    def dead_pid(self, plain_process):
        victim = plain_process()
        token = procident.start_token(victim.pid) or "linux:1"
        pid = victim.pid
        _hard_kill(victim)
        return pid, token

    def test_a_free_lock_is_taken_at_once(self, lockdb, monkeypatch, took):
        clock = FakeTime(monkeypatch)
        lock = daemon_module._acquire_presence_lock(lockdb)
        took.append(lock)
        assert lock.held and clock.sleeps == []

    def test_a_dead_supervisors_orphans_are_waited_out_then_the_lock_is_taken(
        self, lockdb, start_helper, plain_process, monkeypatch, caplog, took
    ):
        pid, token = self.dead_pid(plain_process)
        holder = start_helper(lockdb, spec=f"{pid}\n3\nstart={token}\n")
        clock = FakeTime(monkeypatch, on_sleep=lambda n: _hard_kill(holder.proc) if n == 2 else None)
        with caplog.at_level(logging.WARNING):
            lock = daemon_module._acquire_presence_lock(lockdb)
        took.append(lock)
        assert lock.held
        assert clock.sleeps == [1.0, 1.0]
        (record,) = warnings_of(caplog, "waiting for them to exit")
        assert f"supervisor pid {pid} is gone" in record.getMessage()
        assert "never interrupted" in record.getMessage()

    def test_a_pid_reused_by_another_process_counts_as_dead(self, lockdb, start_helper, monkeypatch, caplog, took):
        """The recorded pid is alive -- it is the holder itself -- but its start
        token is not the recorded one: pid reuse, not a running supervisor."""
        holder = start_helper(lockdb, spec="{pid}\n3\nstart=bogus:1\n")
        assert procident.identity(holder.pid, "bogus:1") in ("mismatch", "unknown")
        clock = FakeTime(monkeypatch, on_sleep=lambda n: _hard_kill(holder.proc))
        if procident.identity(holder.pid, "bogus:1") != "mismatch":
            pytest.skip("no start tokens on this platform")
        with caplog.at_level(logging.WARNING):
            lock = daemon_module._acquire_presence_lock(lockdb)
        took.append(lock)
        assert lock.held and clock.sleeps == [1.0]
        assert warnings_of(caplog, "waiting for them to exit")

    @pytest.mark.parametrize(
        "release_after, expected_warnings", [(1, 1), (59, 1), (61, 2), (121, 3)]
    )
    def test_the_warning_cadence(
        self, lockdb, start_helper, plain_process, monkeypatch, caplog, took, release_after, expected_warnings
    ):
        pid, token = self.dead_pid(plain_process)
        holder = start_helper(lockdb, spec=f"{pid}\n3\nstart={token}\n")
        clock = FakeTime(monkeypatch, on_sleep=lambda n: _hard_kill(holder.proc) if n == release_after else None)
        with caplog.at_level(logging.WARNING):
            lock = daemon_module._acquire_presence_lock(lockdb)
        took.append(lock)
        assert len(clock.sleeps) == release_after
        assert len(warnings_of(caplog, "waiting for them to exit")) == expected_warnings

    def test_a_live_matching_supervisor_is_refused_at_once(self, lockdb, start_helper, monkeypatch):
        holder = start_helper(lockdb, gen=3)
        clock = FakeTime(monkeypatch)
        with pytest.raises(daemonlock.DaemonAlreadyRunning) as exc:
            daemon_module._acquire_presence_lock(lockdb)
        assert exc.value.pid == holder.pid
        assert clock.sleeps == []

    @pytest.mark.parametrize(
        "spec, why",
        [
            ("{pid}\n3\n", "legacy: no start= line"),
            ("{pid}\n3\nstart=\n", "unknown: platform without tokens"),
            ("{pid}\n3\nstart=has space\n", "unknown: unusable token"),
            ("garbage\n", "unreadable"),
            ("", "empty"),
        ],
    )
    def test_legacy_and_unknown_identities_are_refused_immediately(self, lockdb, start_helper, monkeypatch, spec, why):
        start_helper(lockdb, spec=spec)
        clock = FakeTime(monkeypatch)
        with pytest.raises(daemonlock.DaemonAlreadyRunning):
            daemon_module._acquire_presence_lock(lockdb)
        assert clock.sleeps == [], why

    def test_a_holder_naming_pid_one_is_refused_not_waited_on(self, lockdb, start_helper, monkeypatch):
        start_helper(lockdb, spec="1\n3\nstart=whatever\n")
        clock = FakeTime(monkeypatch)
        with pytest.raises(daemonlock.DaemonAlreadyRunning):
            daemon_module._acquire_presence_lock(lockdb)
        assert clock.sleeps == []

    def test_a_lock_error_is_not_swallowed(self, tmp_path, monkeypatch):
        clock = FakeTime(monkeypatch)
        with pytest.raises(daemonlock.DaemonLockError):
            daemon_module._acquire_presence_lock(tmp_path / "nodir" / "config.db")
        assert clock.sleeps == []

    def test_identity_is_judged_on_the_exception_not_on_a_pid_probe(self, lockdb, start_helper, monkeypatch, mocker):
        """A mismatch must wait even when the pid is alive; a match must not."""
        holder = start_helper(lockdb, gen=3)
        seen = []
        mocker.patch("zfsbackup.daemon.procident.identity", side_effect=lambda p, t: seen.append((p, t)) or "match")
        FakeTime(monkeypatch)
        with pytest.raises(daemonlock.DaemonAlreadyRunning):
            daemon_module._acquire_presence_lock(lockdb)
        assert seen and seen[0][0] == holder.pid and seen[0][1]


# ---------------------------------------------------------------------------
# _run: ordering, --test-config, refusals
# ---------------------------------------------------------------------------


class TestRunStartup:
    def test_the_order_is_lock_then_wait_then_load_then_daemon(self, sqlite_config_db, mocker, lock_spy, no_hard_exit):
        events = []
        real_wait = daemon_module._wait_for_previous_workers
        real_load = daemon_module.load_runtime_config

        def wait(resolved):
            events.append(("wait", daemonlock.probe(resolved) is not None))
            return real_wait(resolved)

        def load(resolved, *a, **kw):
            events.append(("load", None))
            return real_load(resolved, *a, **kw)

        mocker.patch("zfsbackup.daemon._wait_for_previous_workers", side_effect=wait)
        mocker.patch("zfsbackup.daemon.load_runtime_config", side_effect=load)
        stub = _stub_daemon(mocker, run_effect=lambda: events.append(("run", None)) or 0)
        assert daemon_module._run(_args(sqlite_config_db.path)) == 0
        assert events == [("wait", True), ("load", None), ("run", None)]

    def test_startup_really_waits_for_a_held_workers_lock_before_building_the_daemon(
        self, sqlite_config_db, mocker, lock_spy, no_hard_exit, monkeypatch
    ):
        path = daemonlock.ensure_workers_lock(sqlite_config_db)
        fd = daemonlock.hold_workers_lock(path)
        events = []

        def on_sleep(n):
            events.append("sleep")
            if n == 2:
                os.close(fd)

        FakeTime(monkeypatch, on_sleep=on_sleep)
        factory, stub = _stub_daemon(mocker, run_effect=lambda: events.append("run") or 0)
        factory.side_effect = lambda *a, **kw: (events.append("build"), stub)[1]
        try:
            assert daemon_module._run(_args(sqlite_config_db.path)) == 0
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
        assert events == ["sleep", "sleep", "build", "run"]

    def test_the_wait_happens_while_the_daemon_lock_is_held(self, sqlite_config_db, mocker, lock_spy, no_hard_exit, monkeypatch):
        path = daemonlock.ensure_workers_lock(sqlite_config_db)
        fd = daemonlock.hold_workers_lock(path)
        seen = []

        def on_sleep(n):
            seen.append(daemonlock.probe(sqlite_config_db))
            os.close(fd)

        FakeTime(monkeypatch, on_sleep=on_sleep)
        _stub_daemon(mocker)
        daemon_module._run(_args(sqlite_config_db.path))
        assert seen[0] is not None and seen[0].pid == os.getpid()

    def test_test_config_takes_neither_lock_nor_wait(self, sqlite_config_db, mocker, monkeypatch):
        for target in (
            "zfsbackup.daemon._acquire_presence_lock",
            "zfsbackup.daemon._wait_for_previous_workers",
            "zfsbackup.daemonlock.acquire",
            "zfsbackup.daemonlock.ensure_workers_lock",
            "zfsbackup.daemonlock.workers_present",
        ):
            mocker.patch(target, side_effect=AssertionError(f"{target} used by --test-config"))
        monkeypatch.setattr(daemon_module, "_startup_sleep", lambda s: pytest.fail("slept"))
        assert daemon_module._run(_args(sqlite_config_db.path, test_config=True)) == 0
        assert not daemonlock.workers_lock_path(sqlite_config_db).exists()
        assert not daemonlock.lock_path(sqlite_config_db).exists()

    def test_test_config_works_while_workers_of_a_previous_daemon_are_running(self, sqlite_config_db, monkeypatch):
        path = daemonlock.ensure_workers_lock(sqlite_config_db)
        fd = daemonlock.hold_workers_lock(path)
        monkeypatch.setattr(daemon_module, "_startup_sleep", lambda s: pytest.fail("slept"))
        try:
            assert daemon_module._run(_args(sqlite_config_db.path, test_config=True)) == 0
        finally:
            os.close(fd)

    def test_a_lock_check_failure_is_a_warning_and_startup_continues(
        self, sqlite_config_db, mocker, lock_spy, no_hard_exit, caplog
    ):
        mocker.patch("zfsbackup.daemonlock.ensure_workers_lock", side_effect=daemonlock.DaemonLockError(13, "denied"))
        factory, stub = _stub_daemon(mocker)
        with caplog.at_level(logging.WARNING):
            assert daemon_module._run(_args(sqlite_config_db.path)) == 0
        factory.assert_called_once()
        stub.run.assert_called_once()
        assert warnings_of(caplog, "cannot check for workers of a previous daemon")

    def test_workers_present_failing_is_a_warning_and_startup_continues(
        self, sqlite_config_db, mocker, lock_spy, no_hard_exit, caplog
    ):
        mocker.patch("zfsbackup.daemonlock.workers_present", side_effect=daemonlock.DaemonLockError(5, "eio"))
        factory, stub = _stub_daemon(mocker)
        with caplog.at_level(logging.WARNING):
            assert daemon_module._run(_args(sqlite_config_db.path)) == 0
        stub.run.assert_called_once()
        assert warnings_of(caplog, "cannot check for workers")

    def test_a_live_daemon_is_refused_with_exit_1_and_no_wait(
        self, sqlite_config_db, mocker, daemon_lock_holder, monkeypatch, caplog, no_hard_exit
    ):
        daemon_lock_holder(sqlite_config_db.path, generation=3)
        monkeypatch.setattr(daemon_module, "_startup_sleep", lambda s: pytest.fail("slept"))
        factory, _ = _stub_daemon(mocker)
        with caplog.at_level(logging.ERROR):
            assert daemon_module._run(_args(sqlite_config_db.path)) == 1
        assert "Refusing to start a second daemon" in caplog.text
        factory.assert_not_called()

    def test_a_dead_supervisors_orphans_are_waited_out_and_startup_succeeds(
        self, sqlite_config_db, mocker, lock_spy, no_hard_exit, start_helper, plain_process, monkeypatch, caplog
    ):
        victim = plain_process()
        token = procident.start_token(victim.pid) or "linux:1"
        dead = victim.pid
        _hard_kill(victim)
        holder = start_helper(sqlite_config_db.path, spec=f"{dead}\n3\nstart={token}\n")
        clock = FakeTime(monkeypatch, on_sleep=lambda n: _hard_kill(holder.proc))
        factory, stub = _stub_daemon(mocker)
        with caplog.at_level(logging.WARNING):
            assert daemon_module._run(_args(sqlite_config_db.path)) == 0
        assert clock.sleeps == [1.0]
        stub.run.assert_called_once()
        assert warnings_of(caplog, "waiting for them to exit")


# ---------------------------------------------------------------------------
# A real daemon process
# ---------------------------------------------------------------------------


@pytest.fixture
def workers_holder(real_daemon_env):
    """`workers_holder()` -> a real process holding `LOCK_SH` on the workers lock."""
    procs = []

    def start():
        path = daemonlock.ensure_workers_lock(real_daemon_env)
        proc = subprocess.Popen(
            [sys.executable, "-c", _WORKERS_HELPER, str(path)],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
            env=hermetic_env(), cwd=str(REPO_ROOT),
        )
        procs.append(proc)
        assert proc.stdout.readline().strip() == "ready"
        return proc

    yield start
    for proc in procs:
        _hard_kill(proc)
        proc.stdout.close()


def log_of(path):
    try:
        return path.read_text(errors="replace")
    except FileNotFoundError:
        return ""


@pytest.mark.integration
@pytest.mark.slow
class TestRealDaemonStartupWait:
    def test_it_waits_while_a_helper_holds_the_workers_lock_and_starts_about_a_second_after_it_exits(
        self, real_daemon_env, workers_holder, spawn_daemon
    ):
        helper = workers_holder()
        proc, log = spawn_daemon()
        assert wait_until(lambda: WARN_TEXT in log_of(log), 90), log_of(log)
        time.sleep(2.5)                        # several polls: still waiting
        assert proc.poll() is None
        assert "Started snapshot worker" not in log_of(log), "workers started under a live previous worker"
        assert daemonlock.probe(real_daemon_env) is not None, "the daemon lock is held during the wait"

        released = time.monotonic()
        _hard_kill(helper)
        assert wait_until(lambda: "Started snapshot worker" in log_of(log), 60), log_of(log)
        assert time.monotonic() - released < 30

        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=60) == 0, log_of(log)

    def test_sigterm_during_the_wait_exits_and_starts_no_worker(self, real_daemon_env, workers_holder, spawn_daemon):
        workers_holder()
        proc, log = spawn_daemon()
        assert wait_until(lambda: WARN_TEXT in log_of(log), 90), log_of(log)
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=30) == -signal.SIGTERM, log_of(log)
        assert "Started" not in log_of(log)
        assert "worker (pid=" not in log_of(log)
        assert wait_until(lambda: daemonlock.probe(real_daemon_env) is None, 10), "the dead daemon's lock must be gone"

    def test_without_survivors_there_is_no_wait(self, real_daemon_env, spawn_daemon):
        proc, log = spawn_daemon()
        assert wait_until(lambda: "Started snapshot worker" in log_of(log), 90), log_of(log)
        assert WARN_TEXT not in log_of(log)
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=60) == 0


# ---------------------------------------------------------------------------
# _new_worker: the reload trio
# ---------------------------------------------------------------------------


class TestNewWorkerHandsOverTheTrio:
    NAMES = ["snapshot", "pruning", "api", "remote"]
    CLASSES = {
        "snapshot": "SnapshotWorker", "pruning": "PruningWorker",
        "api": "ApiWorker", "remote": "RemoteBackupWorker",
    }

    @pytest.fixture
    def daemon(self, literal_config_db):
        return BackupDaemon(make_config(), literal_config_db)

    @pytest.mark.parametrize("name", NAMES)
    def test_the_same_epoch_object_the_born_epoch_and_the_lock_path(self, daemon, literal_config_db, mocker, name):
        factory = mocker.patch(f"zfsbackup.daemon.{self.CLASSES[name]}")
        daemon._new_worker(name)
        (call,) = factory.call_args_list
        assert call.args == (daemon.config, daemon._stop_event, daemon.verbose)
        kw = call.kwargs
        assert kw["reload_epoch"] is daemon._reload_epoch
        assert isinstance(kw["reload_epoch"], ReloadEpoch)
        assert kw["born_epoch"] == daemon._reload_epoch.get() == 0
        assert kw["workers_lock"] == daemonlock.workers_lock_path(literal_config_db)
        assert isinstance(kw["workers_lock"], (str, os.PathLike)), "a path, never an open descriptor"
        assert not isinstance(kw["workers_lock"], int)
        assert str(kw["workers_lock"]).endswith(".workers.lock")

    @pytest.mark.parametrize("name", NAMES)
    def test_without_a_config_path_there_is_no_lock(self, mocker, name):
        daemon = BackupDaemon(make_config(), None)
        assert daemon._workers_lock_path() is None
        factory = mocker.patch(f"zfsbackup.daemon.{self.CLASSES[name]}")
        daemon._new_worker(name)
        assert factory.call_args.kwargs["workers_lock"] is None
        assert factory.call_args.kwargs["reload_epoch"] is daemon._reload_epoch

    def test_every_worker_of_one_daemon_shares_one_epoch_object(self, daemon):
        workers = [daemon._new_worker(n) for n in self.NAMES]
        assert len({id(w.reload_epoch) for w in workers}) == 1
        assert workers[0].reload_epoch is daemon._reload_epoch

    def test_a_respawn_after_the_epoch_moved_is_born_under_the_current_epoch(self, daemon):
        """`born_epoch` comes from the supervisor's `Handoff` (read once per spawn);
        a reload replaces the handoff and then bumps the shared epoch."""
        import dataclasses

        before = daemon._new_worker("snapshot")
        assert before.born_epoch == 0 == daemon._handoff.epoch
        daemon._handoff = dataclasses.replace(daemon._handoff, epoch=2)
        daemon._reload_epoch.set(2)
        after = daemon._new_worker("snapshot")
        assert after.born_epoch == 2
        assert before.born_epoch == 0
        # The old one is stale (it will leave for the reload); the new one is current.
        assert before._exit_reason().value == "reload"
        assert after._exit_reason() is None

    def test_the_epoch_is_created_once(self, daemon, mocker):
        first = daemon._reload_epoch
        daemon._new_worker("snapshot")
        daemon._new_worker("pruning")
        assert daemon._reload_epoch is first

    def test_the_supervisor_holds_no_workers_lock_descriptor_across_a_fork(self, daemon, literal_config_db, mocker):
        path = daemonlock.ensure_workers_lock(literal_config_db)
        assert descriptors_on(path) == 0
        mocker.patch("zfsbackup.daemon.dispose_all")
        start = mocker.patch("multiprocessing.process.BaseProcess.start")
        for name in self.NAMES:
            daemon._spawn(name)
            assert descriptors_on(path) == 0, f"{name}: the supervisor opened the workers lock"
        assert start.call_count == 4

    def test_the_startup_check_closes_its_own_descriptor_before_any_fork(self, literal_config_db, monkeypatch):
        path = daemonlock.ensure_workers_lock(literal_config_db)
        daemon_module._wait_for_previous_workers(literal_config_db)
        assert descriptors_on(path) == 0

"""Batch C -- a dead forkserver must not look like dead workers.

Under ``forkserver`` (CPython 3.14: the Linux default) ``Popen.poll()`` answers 255 for EVERY
worker once the forkserver process is gone, although the workers are alive (possibly hours
into a ``zfs send``). The supervisor therefore records each worker's ``procident`` start token
at spawn, and a "dead" worker whose identity still matches is *lost*, not crashed: one ERROR,
no failure counted (D9), no replacement beside a live orphan, an immediate replacement (no
backoff) once it has exited, and a shutdown that still waits for it and signals it by pid --
after a fresh identity check, never a recycled pid.

- ``TestOrphanClassification``   mock workers: ``_spawn`` token, lost vs crashed, every identity
- ``TestLostWorkerBookkeeping``  turnover, retired, stable-uptime, ``_maybe_complete`` and
                                 ``_log_deferred_switches`` with a lost worker
- ``TestShutdownOfLostWorkers``  ``_shutdown_workers`` / ``_shutdown_lost`` under a fake clock:
                                 budget, SIGTERM, SIGKILL, abandonment, never a recycled pid
- ``TestNeverSignalsAForeignProcess``  the same against a REAL live process we do not own
- ``TestWorkerWording``          ``_SupervisorWatch._supervisor_gone``: forkserver vs "gone"
- ``TestRealForkserverDeath``    REAL processes: the forkserver is killed under stub workers
- ``TestRealNormalDeath``        REAL processes: an ordinary crash under fork / spawn / forkserver

The real-process tests run a driver script in a subprocess of its own (a fresh session, a
hermetic environment, a bounded ``timeout``) so the start method is chosen without touching
pytest's interpreter; whatever it leaves behind is killed by process group, and the driver
kills every worker pid it saw before it exits. Stub ``zfs``/``zpool`` only: nothing here can
reach the host's ZFS.
"""

import json
import logging
import multiprocessing
import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import zfsbackup.daemon as daemon_module
from _hermetic import hermetic_env
from zfsbackup import procident, workers
from zfsbackup.config import BackupConfig, DatasetConfig
from zfsbackup.daemon import (
    SUPERVISOR_POLL,
    WORKER_MAX_CONSECUTIVE_FAILURES,
    WORKER_RESTART_DELAY,
    WORKER_STABLE_UPTIME,
    BackupDaemon,
    Handoff,
)
from zfsbackup.stopflag import StopFlag
from zfsbackup.workers import SnapshotWorker

REPO = Path(__file__).resolve().parent.parent
FAKE_PID = 4_000_001            # never a real pid on a sane system; every signal to it is mocked
NAMES = ("api", "pruning", "snapshot")

HAVE_FORKSERVER = "forkserver" in multiprocessing.get_all_start_methods()
HAVE_TOKENS = procident.start_token(os.getpid()) is not None

needs_forkserver = pytest.mark.skipif(
    not (HAVE_FORKSERVER and HAVE_TOKENS),
    reason="needs the forkserver start method and procident start tokens on this platform",
)


# ---------------------------------------------------------------------------
# In-process helpers
# ---------------------------------------------------------------------------


def make_config():
    return BackupConfig(datasets=[DatasetConfig(name="pool/data")])


def mock_worker(pid=100, exitcode=255):
    """What a `forkserver` `Process` looks like once the forkserver is gone."""
    w = MagicMock()
    w.pid = pid
    w.exitcode = exitcode
    w.is_alive.return_value = False
    w.join.return_value = None
    return w


@pytest.fixture
def daemon(mocker):
    """A `BackupDaemon` with no database, whose signal handlers are put back afterwards."""
    saved = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    mocker.patch("zfsbackup.daemon.dispose_all")
    d = BackupDaemon(make_config(), None)
    yield d
    for s, h in saved.items():
        signal.signal(s, h)


class Clock:
    """Drives `BackupDaemon._monotonic` (restart deadlines, deferral notices)."""

    def __init__(self, daemon, mocker, start=1000.0):
        self.now = start
        mocker.patch.object(daemon, "_monotonic", side_effect=lambda: self.now)


class FakeTime:
    """Replaces the `time` name inside `zfsbackup.daemon`: `sleep` advances `monotonic`
    instead of sleeping, so the shutdown budget and graces cost nothing."""

    def __init__(self):
        self.now = 5000.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds

    def __getattr__(self, name):
        return getattr(time, name)


class World:
    """A patched `procident.identity` / `os.kill` for the daemon module: the identity of the
    lost pid is whatever `ident` says (a value or a callable of this world; `by_pid` overrides
    it for a given pid, e.g. the forkserver's), and every signal is recorded instead of sent.
    `probe` is raised by `kill(pid, 0)` when set. pidfds are switched off so the platform
    cannot change what `_signal_verified` does (they are tested on their own)."""

    def __init__(self, mocker, ident="match", fake_time=True):
        self.ident = ident
        self.by_pid = {}
        self.probe = None
        self.kills = []
        self.checked = []
        self.time = FakeTime() if fake_time else None
        mocker.patch.object(daemon_module.procident, "identity", side_effect=self._identity)
        mocker.patch.object(daemon_module.os, "kill", side_effect=self._kill)
        mocker.patch.object(daemon_module.os, "pidfd_open", None, create=True)
        if fake_time:
            mocker.patch.object(daemon_module, "time", self.time)

    def _identity(self, pid, token):
        self.checked.append((pid, token))
        if pid in self.by_pid:
            return self.by_pid[pid]
        return self.ident(self) if callable(self.ident) else self.ident

    def _kill(self, pid, sig):
        self.kills.append((pid, sig))
        if sig == 0 and self.probe is not None:
            raise self.probe

    @property
    def signals(self):
        """Real signals sent (signal 0 is only an existence probe)."""
        return [(pid, sig) for pid, sig in self.kills if sig != 0]


def errors_of(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]


def adopt(daemon, name="snapshot", worker=None, *, epoch=0, token="tok", lost=False):
    """Make `worker` the daemon's live-looking `name` worker."""
    worker = worker or mock_worker()
    daemon._workers[name] = worker
    daemon._worker_epoch[name] = epoch
    daemon._worker_generation[name] = "1"
    daemon._started_at[name] = 1000.0
    daemon._worker_token[name] = token
    if lost:
        daemon._lost.add(name)
    return worker


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestOrphanClassification:
    @pytest.mark.parametrize(
        "method, tracked", [("forkserver", True), ("spawn", False), ("fork", False)]
    )
    def test_spawn_records_a_start_token_only_under_forkserver(
        self, daemon, mocker, method, tracked
    ):
        mocker.patch.object(daemon_module.multiprocessing, "get_start_method", return_value=method)
        token = mocker.patch.object(daemon_module.procident, "start_token", return_value="T1")
        mocker.patch.object(daemon, "_new_worker", return_value=mock_worker(pid=77))
        daemon._spawn("snapshot")
        assert daemon._worker_token["snapshot"] == ("T1" if tracked else None)
        assert token.called is tracked
        assert "snapshot" not in daemon._lost

    def test_an_exception_from_get_start_method_means_untracked(self, daemon, mocker):
        mocker.patch.object(
            daemon_module.multiprocessing, "get_start_method", side_effect=RuntimeError("x")
        )
        assert BackupDaemon._tracks_orphans() is False

    def test_poll_dead_with_a_matching_identity_is_lost_not_crashed(self, daemon, mocker, caplog):
        World(mocker, "match")
        spawn = mocker.patch.object(daemon, "_spawn")
        adopt(daemon, "snapshot", mock_worker(pid=100))
        with caplog.at_level(logging.INFO):
            daemon._check_workers()
        assert daemon._lost == {"snapshot"}
        assert daemon._failures == {}                     # D9 counts nothing
        assert daemon._restart_at == {}                   # and schedules no backoff
        assert "snapshot" in daemon._workers              # still tracked: shutdown reaps it
        spawn.assert_not_called()
        assert errors_of(caplog) == [
            "forkserver exited; 1 worker(s) orphaned: snapshot (pid=100); "
            "replacements start once they have exited"
        ]

    def test_all_orphans_of_one_forkserver_are_one_error_line(self, daemon, mocker, caplog):
        World(mocker, "match")
        mocker.patch.object(daemon, "_spawn")
        for i, name in enumerate(("snapshot", "pruning", "api")):
            adopt(daemon, name, mock_worker(pid=100 + i))
        with caplog.at_level(logging.INFO):
            daemon._check_workers()
        assert daemon._lost == {"snapshot", "pruning", "api"}
        (line,) = errors_of(caplog)
        assert line.startswith("forkserver exited; 3 worker(s) orphaned: ")
        for i, name in enumerate(("api", "pruning", "snapshot")):
            assert f"{name} (pid=" in line

    def test_a_live_orphan_is_neither_replaced_nor_reported_again(self, daemon, mocker, caplog):
        World(mocker, "match")
        spawn = mocker.patch.object(daemon, "_spawn")
        adopt(daemon, "snapshot")
        daemon._check_workers()
        caplog.clear()
        with caplog.at_level(logging.DEBUG):
            for _ in range(5):
                daemon._check_workers()
        assert errors_of(caplog) == []
        spawn.assert_not_called()
        assert daemon._lost == {"snapshot"} and daemon._failures == {}

    @pytest.mark.parametrize(
        "ident, probe",
        [("gone", None), ("mismatch", None), ("unknown", ProcessLookupError())],
        ids=["gone", "mismatch", "unknown-and-ESRCH"],
    )
    def test_any_other_identity_is_an_ordinary_crash(self, daemon, mocker, caplog, ident, probe):
        world = World(mocker, ident)
        world.probe = probe
        Clock(daemon, mocker)
        spawn = mocker.patch.object(daemon, "_spawn")
        adopt(daemon, "snapshot", mock_worker(pid=100, exitcode=255))
        with caplog.at_level(logging.INFO):
            daemon._check_workers()
        assert daemon._lost == set()
        assert daemon._failures == {"snapshot": 1}
        assert daemon._restart_at == {"snapshot": 1000.0 + WORKER_RESTART_DELAY}
        spawn.assert_not_called()                         # the backoff applies
        (line,) = errors_of(caplog)
        assert "exited with code 255" in line and "restarting in 5s" in line
        assert "forkserver" not in line

    @pytest.mark.parametrize(
        "probe", [None, PermissionError(1, "EPERM")], ids=["alive", "EPERM"]
    )
    def test_unknown_identity_is_resolved_by_kill_zero_towards_alive(
        self, daemon, mocker, caplog, probe
    ):
        """`hidepid` and friends: identity cannot be read, so `kill(pid, 0)` decides, and
        anything but ESRCH means "still running" -- an orphan, not a crash."""
        world = World(mocker, "unknown")
        world.probe = probe
        spawn = mocker.patch.object(daemon, "_spawn")
        adopt(daemon, "snapshot", mock_worker(pid=100, exitcode=255))
        with caplog.at_level(logging.INFO):
            daemon._check_workers()
        assert (100, 0) in world.kills and world.signals == []
        assert daemon._lost == {"snapshot"}
        assert daemon._failures == {} and daemon._restart_at == {}
        spawn.assert_not_called()
        (line,) = errors_of(caplog)
        assert line.startswith("forkserver exited; 1 worker(s) orphaned: snapshot (pid=100)")

    def test_no_token_is_the_ordinary_path_and_identity_is_never_asked(self, daemon, mocker):
        world = World(mocker, "match")
        Clock(daemon, mocker)
        adopt(daemon, "snapshot", token=None)
        daemon._check_workers()
        assert daemon._lost == set()
        assert daemon._failures == {"snapshot": 1}
        assert world.checked == []

    @pytest.mark.parametrize("method", ["spawn", "fork"])
    def test_under_spawn_and_fork_a_death_keeps_the_backoff(self, daemon, mocker, caplog, method):
        # No token is ever recorded for these methods, so even a pid that "matches"
        # something cannot be classified as an orphan.
        mocker.patch.object(daemon_module.multiprocessing, "get_start_method", return_value=method)
        mocker.patch.object(daemon_module.procident, "start_token", return_value="T")
        mocker.patch.object(daemon, "_new_worker", return_value=mock_worker(pid=100, exitcode=-9))
        world = World(mocker, "match")
        Clock(daemon, mocker)
        daemon._spawn("snapshot")
        daemon._check_workers()
        assert daemon._lost == set()
        assert daemon._failures == {"snapshot": 1}
        assert "snapshot" in daemon._restart_at
        assert world.checked == []


# ---------------------------------------------------------------------------
# Lost-worker bookkeeping
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLostWorkerBookkeeping:
    def exit_orphan(self, daemon, mocker, ident="gone"):
        world = World(mocker, "match")
        spawn = mocker.patch.object(daemon, "_spawn")
        daemon._check_workers()                      # classifies it
        assert daemon._lost
        world.ident = ident
        return world, spawn

    @pytest.mark.parametrize("ident", ["gone", "mismatch"])
    def test_an_exited_orphan_is_replaced_at_once_without_backoff(
        self, daemon, mocker, caplog, ident
    ):
        Clock(daemon, mocker)
        adopt(daemon, "snapshot", mock_worker(pid=100))
        world, spawn = self.exit_orphan(daemon, mocker, ident)
        caplog.clear()
        with caplog.at_level(logging.INFO):
            daemon._check_workers()
        spawn.assert_called_once_with("snapshot", verb="Restarted")
        assert daemon._failures == {} and daemon._restart_at == {}
        assert daemon._lost == set()                  # _forget_worker cleared it
        assert "orphaned snapshot worker (pid=100) has exited" in caplog.text
        assert errors_of(caplog) == []

    def test_unknown_identity_falls_back_to_kill_zero(self, daemon, mocker):
        Clock(daemon, mocker)
        adopt(daemon, "snapshot", mock_worker(pid=100))
        world, spawn = self.exit_orphan(daemon, mocker, "unknown")
        daemon._check_workers()
        spawn.assert_not_called()                     # kill(pid, 0) says "alive": err towards live
        assert (100, 0) in world.kills and world.signals == []
        daemon_module.os.kill.side_effect = ProcessLookupError()
        daemon._check_workers()
        spawn.assert_called_once_with("snapshot", verb="Restarted")

    def test_a_signal_zero_failure_other_than_esrch_keeps_the_orphan(self, daemon, mocker):
        Clock(daemon, mocker)
        adopt(daemon, "snapshot", mock_worker(pid=100))
        world, spawn = self.exit_orphan(daemon, mocker, "unknown")
        daemon_module.os.kill.side_effect = PermissionError()
        daemon._check_workers()
        spawn.assert_not_called()

    def test_a_stop_in_progress_forks_no_replacement(self, daemon, mocker, caplog):
        Clock(daemon, mocker)
        adopt(daemon, "snapshot")
        world, spawn = self.exit_orphan(daemon, mocker, "gone")
        daemon._stop_event.set()
        with caplog.at_level(logging.INFO):
            daemon._check_workers()
        spawn.assert_not_called()
        assert "Shutdown requested; not respawning remaining worker(s): snapshot" in caplog.text

    def test_a_reload_that_no_longer_needs_the_worker_retires_it(self, daemon, mocker, caplog):
        # 'remote' is not an active worker of this config: nothing to start in its place
        Clock(daemon, mocker)
        adopt(daemon, "remote", mock_worker(pid=100))
        daemon._failures["remote"] = 2
        world, spawn = self.exit_orphan(daemon, mocker, "gone")
        with caplog.at_level(logging.INFO):
            daemon._check_workers()
        spawn.assert_not_called()
        assert "remote worker retired: not needed by generation unknown" in caplog.text
        assert "remote" not in daemon._workers and daemon._lost == set()
        assert "remote" not in daemon._failures       # a retired worker's history goes with it

    def test_a_replacement_inherits_failures_unless_the_orphan_was_stable(self, daemon, mocker):
        clock = Clock(daemon, mocker)
        adopt(daemon, "snapshot")
        daemon._failures["snapshot"] = 3
        world, spawn = self.exit_orphan(daemon, mocker, "gone")
        clock.now = 1000.0 + WORKER_STABLE_UPTIME - 1
        daemon._check_workers()
        assert daemon._failures == {"snapshot": 3}    # crashes carry across a turnover

        adopt(daemon, "pruning")
        daemon._failures["pruning"] = 3
        daemon._lost.add("pruning")
        clock.now = 1000.0 + WORKER_STABLE_UPTIME
        daemon._check_workers()
        assert daemon._failures["pruning"] == 0       # stayed up long enough: recovered

    def test_a_lost_worker_on_an_old_epoch_blocks_applied_until_it_is_replaced(
        self, daemon, mocker, caplog
    ):
        Clock(daemon, mocker)
        daemon._handoff = Handoff(make_config(), 1, (7, 2))
        adopt(daemon, "snapshot", mock_worker(pid=100), epoch=0)
        world = World(mocker, "match")

        def respawn(name, verb="Started"):
            adopt(daemon, name, mock_worker(pid=200), epoch=daemon._handoff.epoch)

        spawn = mocker.patch.object(daemon, "_spawn", side_effect=respawn)
        with caplog.at_level(logging.INFO):
            daemon._check_workers()
            assert daemon._lost == {"snapshot"}
            assert daemon._applied_epoch == 0         # an orphan still runs the old config
            assert "applied by all workers" not in caplog.text
            world.ident = "gone"
            daemon._check_workers()
        spawn.assert_called_once_with("snapshot", verb="Reloaded")
        assert daemon._applied_epoch == 1
        assert "generation 2 applied by all workers" in caplog.text

    def test_a_lost_worker_on_an_old_epoch_gets_the_deferral_warning(self, daemon, mocker, caplog):
        clock = Clock(daemon, mocker)
        daemon._handoff = Handoff(make_config(), 1, (7, 2))
        daemon._reload_started_at = clock.now - daemon_module.RELOAD_DEFER_NOTICE - 1
        adopt(daemon, "snapshot", mock_worker(pid=100), epoch=0, lost=True)
        with caplog.at_level(logging.WARNING):
            daemon._log_deferred_switches()
        assert "snapshot worker (pid=100) is still finishing work begun under generation" in caplog.text

    def test_a_dead_unclassified_worker_gets_no_deferral_warning(self, daemon, mocker, caplog):
        # the contrast: not lost, not alive -> "classified by the next _check_workers"
        clock = Clock(daemon, mocker)
        daemon._handoff = Handoff(make_config(), 1, (7, 2))
        daemon._reload_started_at = clock.now - daemon_module.RELOAD_DEFER_NOTICE - 1
        adopt(daemon, "snapshot", mock_worker(pid=100), epoch=0)
        with caplog.at_level(logging.WARNING):
            daemon._log_deferred_switches()
        assert "still finishing work" not in caplog.text

    def test_the_deferral_warning_repeats_only_after_the_repeat_interval(self, daemon, mocker, caplog):
        clock = Clock(daemon, mocker)
        daemon._handoff = Handoff(make_config(), 1, (7, 2))
        daemon._reload_started_at = clock.now - daemon_module.RELOAD_DEFER_NOTICE - 1
        adopt(daemon, "snapshot", epoch=0, lost=True)
        with caplog.at_level(logging.WARNING):
            daemon._log_deferred_switches()
            clock.now += 10
            daemon._log_deferred_switches()
            assert caplog.text.count("still finishing work") == 1
            clock.now += daemon_module.RELOAD_DEFER_REPEAT
            daemon._log_deferred_switches()
        assert caplog.text.count("still finishing work") == 2

    def test_forget_worker_drops_the_token_and_the_lost_mark(self, daemon):
        adopt(daemon, "snapshot", lost=True)
        daemon._forget_worker("snapshot")
        assert "snapshot" not in daemon._worker_token
        assert daemon._lost == set() and "snapshot" not in daemon._workers


# ---------------------------------------------------------------------------
# Shutdown with orphans (fake clock, mocked signals)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestShutdownOfLostWorkers:
    def test_an_orphan_that_exits_within_the_budget_is_never_signalled(self, daemon, mocker, caplog):
        def ident(world):
            return "match" if world.time.now < 5003.0 else "gone"      # leaves after ~3s

        world = World(mocker, ident)
        adopt(daemon, "snapshot", mock_worker(pid=FAKE_PID), lost=True)
        with caplog.at_level(logging.INFO):
            daemon._shutdown_workers(timeout=10)
        assert world.signals == []
        assert daemon.abandoned_workers == []
        assert "did not exit within" not in caplog.text
        assert world.time.now >= 5003.0               # it did wait

    def test_past_the_budget_it_is_terminated_by_pid(self, daemon, mocker, caplog):
        def ident(world):                             # dies on the SIGTERM
            return "gone" if (FAKE_PID, signal.SIGTERM) in world.kills else "match"

        world = World(mocker, ident)
        adopt(daemon, "snapshot", mock_worker(pid=FAKE_PID), lost=True)
        with caplog.at_level(logging.WARNING):
            daemon._shutdown_workers(timeout=2)
        assert world.signals == [(FAKE_PID, signal.SIGTERM)]
        assert (
            f"orphaned snapshot worker (pid={FAKE_PID}) did not exit within the 2s "
            "shutdown budget, terminating"
        ) in caplog.text
        assert daemon.abandoned_workers == []
        assert world.time.now >= 5002.0               # the whole budget was offered first

    def test_a_worker_ignoring_sigterm_is_sigkilled(self, daemon, mocker):
        def ident(world):
            return "gone" if (FAKE_PID, signal.SIGKILL) in world.kills else "match"

        world = World(mocker, ident)
        adopt(daemon, "snapshot", mock_worker(pid=FAKE_PID), lost=True)
        daemon._shutdown_workers(timeout=1)
        assert world.signals == [(FAKE_PID, signal.SIGTERM), (FAKE_PID, signal.SIGKILL)]
        assert daemon.abandoned_workers == []
        assert world.time.now >= 5001.0 + 5.0         # one 5s grace for SIGTERM

    def test_a_worker_surviving_sigkill_is_abandoned(self, daemon, mocker, caplog):
        world = World(mocker, "match")
        adopt(daemon, "snapshot", mock_worker(pid=FAKE_PID), lost=True)
        with caplog.at_level(logging.ERROR):
            daemon._shutdown_workers(timeout=1)
        assert world.signals == [(FAKE_PID, signal.SIGTERM), (FAKE_PID, signal.SIGKILL)]
        assert daemon.abandoned_workers == ["snapshot"]
        assert "survived SIGKILL or could not be verified; abandoning it" in caplog.text

    def test_the_budget_is_shared_by_all_orphans(self, daemon, mocker):
        world = World(mocker, "match")
        for i, name in enumerate(("snapshot", "pruning", "api")):
            adopt(daemon, name, mock_worker(pid=FAKE_PID + i), lost=True)
        daemon._shutdown_workers(timeout=10)
        # 10s once, then 3 x (5 + 5) of escalation -- not 3 x 10 of waiting first
        assert world.time.now <= 5000.0 + 10 + 3 * 10 + 1
        assert sorted(daemon.abandoned_workers) == ["api", "pruning", "snapshot"]

    def test_a_dead_forkserver_since_the_last_poll_still_waits_for_orphans(
        self, daemon, mocker
    ):
        """No `_check_workers` ran after the forkserver died: nothing is in `_lost`, every
        `join` returns at once with the worker "not alive". Identity must rescue it."""
        def ident(world):
            return "match" if world.time.now < 5004.0 else "gone"

        world = World(mocker, ident)
        adopt(daemon, "snapshot", mock_worker(pid=FAKE_PID))        # not in _lost
        assert daemon._lost == set()
        daemon._shutdown_workers(timeout=10)
        assert world.time.now >= 5004.0               # waited for it
        assert world.signals == [] and daemon.abandoned_workers == []

    def test_a_really_exited_worker_is_not_mistaken_for_an_orphan(self, daemon, mocker):
        world = World(mocker, "gone")
        adopt(daemon, "snapshot", mock_worker(pid=FAKE_PID))
        daemon._shutdown_workers(timeout=10)
        assert world.kills == [] and daemon.abandoned_workers == []
        assert world.time.now == 5000.0               # no waiting at all

    def test_a_worker_without_a_token_is_treated_as_exited(self, daemon, mocker):
        world = World(mocker, "match")
        adopt(daemon, "snapshot", mock_worker(pid=FAKE_PID), token=None)
        daemon._shutdown_workers(timeout=10)
        assert world.kills == [] and world.time.now == 5000.0

    def test_a_live_worker_still_gets_the_join_terminate_kill_path(self, daemon, mocker):
        World(mocker, "match")
        w = mock_worker(pid=FAKE_PID)
        w.is_alive.return_value = True                # joins time out: a normal live child
        adopt(daemon, "snapshot", w)
        daemon._shutdown_workers(timeout=1)
        w.terminate.assert_called_once()
        w.kill.assert_called_once()
        assert daemon.abandoned_workers == ["snapshot"]


@pytest.mark.unit
class TestNeverSignalsARecycledPid:
    @pytest.mark.parametrize("ident", ["mismatch", "gone"])
    def test_a_pid_that_is_not_ours_any_more_is_not_signalled(self, daemon, mocker, ident):
        world = World(mocker, ident)
        adopt(daemon, "snapshot", mock_worker(pid=FAKE_PID), lost=True)
        daemon._shutdown_workers(timeout=5)
        assert world.kills == []
        assert daemon.abandoned_workers == []          # it left, as far as we can tell

    def test_unknown_identity_is_never_signalled_and_ends_abandoned(self, daemon, mocker, caplog):
        world = World(mocker, "unknown")               # kill(pid, 0) "succeeds": maybe alive
        adopt(daemon, "snapshot", mock_worker(pid=FAKE_PID), lost=True)
        with caplog.at_level(logging.ERROR):
            daemon._shutdown_workers(timeout=1)
        assert world.signals == []
        assert world.kills and all(sig == 0 for _, sig in world.kills)
        assert daemon.abandoned_workers == ["snapshot"]
        assert "could not be verified" in caplog.text

    def test_a_pid_recycled_after_sigterm_is_not_sent_sigkill(self, daemon, mocker):
        def ident(world):
            return "mismatch" if world.signals else "match"

        world = World(mocker, ident)
        adopt(daemon, "snapshot", mock_worker(pid=FAKE_PID), lost=True)
        daemon._shutdown_workers(timeout=1)
        assert world.signals == [(FAKE_PID, signal.SIGTERM)]
        assert daemon.abandoned_workers == []

    def test_identity_lost_after_sigterm_never_escalates_blindly(self, daemon, mocker):
        def ident(world):
            return "unknown" if world.signals else "match"

        world = World(mocker, ident)
        adopt(daemon, "snapshot", mock_worker(pid=FAKE_PID), lost=True)
        daemon._shutdown_workers(timeout=1)
        assert world.signals == [(FAKE_PID, signal.SIGTERM)]
        assert daemon.abandoned_workers == ["snapshot"]

    def test_every_signal_is_preceded_by_a_fresh_identity_check(self, daemon, mocker):
        order = []

        def ident(world):
            order.append("identity")
            return "match"

        world = World(mocker, ident)
        daemon_module.os.kill.side_effect = lambda pid, sig: order.append(("kill", sig))
        adopt(daemon, "snapshot", mock_worker(pid=FAKE_PID), lost=True)
        daemon._shutdown_workers(timeout=1)
        signals = [i for i, step in enumerate(order) if isinstance(step, tuple)]
        assert len(signals) == 2, order                # SIGTERM then SIGKILL
        for i in signals:
            assert order[i - 1] == "identity", order

    def test_esrch_on_the_signal_is_a_clean_exit(self, daemon, mocker):
        world = World(mocker, "match")
        daemon_module.os.kill.side_effect = ProcessLookupError()
        adopt(daemon, "snapshot", mock_worker(pid=FAKE_PID), lost=True)
        daemon._shutdown_workers(timeout=1)
        assert daemon.abandoned_workers == []

    def test_a_refused_signal_is_logged_and_abandoned(self, daemon, mocker, caplog):
        World(mocker, "match")
        daemon_module.os.kill.side_effect = PermissionError(1, "Operation not permitted")
        adopt(daemon, "snapshot", mock_worker(pid=FAKE_PID), lost=True)
        with caplog.at_level(logging.ERROR):
            daemon._shutdown_workers(timeout=1)
        assert "cannot send SIGTERM to orphaned snapshot worker" in caplog.text
        assert daemon.abandoned_workers == ["snapshot"]


# ---------------------------------------------------------------------------
# An orphan that has already exited by the time the supervisor looks
# ---------------------------------------------------------------------------

FS_PID, FS_TOKEN = 900, "FSTOK"


@pytest.mark.unit
class TestOrphanAlreadyExited:
    """An idle orphan leaves within a slice of the forkserver's death, long before the
    supervisor's next poll: `poll()` says 255 and the worker is already gone. The only trace is
    that the forkserver that forked it is gone too."""

    def setup(self, daemon, mocker, *, fs, worker_ident="gone", exitcode=255):
        world = World(mocker, worker_ident)
        if fs is not None:
            world.by_pid[FS_PID] = fs
        Clock(daemon, mocker)
        spawn = mocker.patch.object(daemon, "_spawn")
        adopt(daemon, "snapshot", mock_worker(pid=100, exitcode=exitcode))
        daemon._worker_fs["snapshot"] = (FS_PID, FS_TOKEN)
        return world, spawn

    @pytest.mark.parametrize("fs", ["gone", "mismatch"])
    def test_exit_255_with_a_dead_forkserver_is_replaced_at_once(self, daemon, mocker, caplog, fs):
        world, spawn = self.setup(daemon, mocker, fs=fs)
        with caplog.at_level(logging.INFO):
            daemon._check_workers()
        spawn.assert_called_once_with("snapshot", verb="Restarted")
        assert daemon._failures == {} and daemon._restart_at == {}   # no D9, no backoff
        assert daemon._lost == set()
        (line,) = errors_of(caplog)
        assert line == (
            "forkserver exited; 1 worker(s) orphaned: snapshot (pid=100, already exited); "
            "they have already exited; replacing them now"
        )

    def test_exit_255_with_a_healthy_forkserver_is_a_genuine_crash(self, daemon, mocker, caplog):
        world, spawn = self.setup(daemon, mocker, fs="match")
        with caplog.at_level(logging.INFO):
            daemon._check_workers()
        spawn.assert_not_called()
        assert daemon._failures == {"snapshot": 1}
        assert daemon._restart_at == {"snapshot": 1000.0 + WORKER_RESTART_DELAY}
        (line,) = errors_of(caplog)
        assert "exited with code 255" in line and "forkserver" not in line

    @pytest.mark.parametrize("fs", ["unknown", None])
    def test_exit_255_with_an_unknowable_forkserver_is_a_crash(self, daemon, mocker, fs):
        world, spawn = self.setup(daemon, mocker, fs=fs)
        if fs is None:
            daemon._worker_fs["snapshot"] = None          # never recorded
        daemon._check_workers()
        spawn.assert_not_called()
        assert daemon._failures == {"snapshot": 1}

    def test_exit_255_with_a_dead_forkserver_but_no_worker_token_is_a_crash(
        self, daemon, mocker, caplog
    ):
        # Without the worker's recorded identity an exit cannot be told from a live orphan:
        # no replacement that might run beside it -- the backoff applies.
        world, spawn = self.setup(daemon, mocker, fs="gone")
        daemon._worker_token["snapshot"] = None
        with caplog.at_level(logging.INFO):
            daemon._check_workers()
        spawn.assert_not_called()
        assert daemon._failures == {"snapshot": 1}
        assert daemon._restart_at == {"snapshot": 1000.0 + WORKER_RESTART_DELAY}
        (line,) = errors_of(caplog)
        assert "exited with code 255" in line and "forkserver exited" not in line

    def test_error_wording_when_every_orphan_has_already_exited(self, daemon, mocker, caplog):
        world, spawn = self.setup(daemon, mocker, fs="gone")
        adopt(daemon, "pruning", mock_worker(pid=101, exitcode=255))
        daemon._worker_fs["pruning"] = (FS_PID, FS_TOKEN)
        with caplog.at_level(logging.INFO):
            daemon._check_workers()
        (line,) = errors_of(caplog)
        assert line == (
            "forkserver exited; 2 worker(s) orphaned: snapshot (pid=100, already exited), "
            "pruning (pid=101, already exited); they have already exited; replacing them now"
        )
        assert spawn.call_count == 2

    def test_error_wording_when_some_orphans_are_still_running(self, daemon, mocker, caplog):
        world, spawn = self.setup(daemon, mocker, fs="gone")        # snapshot: already exited
        adopt(daemon, "pruning", mock_worker(pid=101, exitcode=255))
        daemon._worker_fs["pruning"] = (FS_PID, FS_TOKEN)
        world.by_pid[101] = "match"                                  # pruning: still running
        with caplog.at_level(logging.INFO):
            daemon._check_workers()
        (line,) = errors_of(caplog)
        assert line == (
            "forkserver exited; 2 worker(s) orphaned: snapshot (pid=100, already exited), "
            "pruning (pid=101); replacements start once they have exited"
        )
        assert daemon._lost == {"pruning"}
        spawn.assert_called_once_with("snapshot", verb="Restarted")

    def test_a_forkserver_record_without_a_token_is_unknown(self, daemon, mocker):
        world, spawn = self.setup(daemon, mocker, fs="gone")
        daemon._worker_fs["snapshot"] = (FS_PID, None)
        daemon._check_workers()
        spawn.assert_not_called()
        assert daemon._failures == {"snapshot": 1}

    @pytest.mark.parametrize("exitcode", [-9, 1, 0, 3])
    def test_another_exit_code_is_a_crash_even_with_a_dead_forkserver(
        self, daemon, mocker, exitcode
    ):
        # Only the forkserver's "255" is evidence of an unreported outcome.
        world, spawn = self.setup(daemon, mocker, fs="gone", exitcode=exitcode)
        daemon._check_workers()
        spawn.assert_not_called()
        assert daemon._failures == {"snapshot": 1}

    def test_a_still_running_worker_wins_over_the_forkserver_check(self, daemon, mocker, caplog):
        world, spawn = self.setup(daemon, mocker, fs="gone", worker_ident="match")
        with caplog.at_level(logging.INFO):
            daemon._check_workers()
        assert daemon._lost == {"snapshot"}
        (line,) = errors_of(caplog)
        assert "already exited" not in line
        spawn.assert_not_called()

    def test_stop_in_progress_forks_no_replacement_for_an_already_exited_orphan(
        self, daemon, mocker
    ):
        world, spawn = self.setup(daemon, mocker, fs="gone")
        daemon._stop_event.set()
        daemon._check_workers()
        spawn.assert_not_called()
        assert daemon._failures == {}

    def test_the_replacement_does_not_inherit_a_failure(self, daemon, mocker):
        world, spawn = self.setup(daemon, mocker, fs="gone")
        daemon._failures["snapshot"] = 5      # 5 earlier crashes: one more would be fatal
        daemon._check_workers()
        spawn.assert_called_once()
        assert daemon._failures == {"snapshot": 5}      # carried, not incremented
        assert daemon._crash_looped == set()

    @pytest.mark.parametrize(
        "record, ident, expected",
        [
            ((FS_PID, FS_TOKEN), "match", "healthy"),
            ((FS_PID, FS_TOKEN), "gone", "gone"),
            ((FS_PID, FS_TOKEN), "mismatch", "gone"),
            ((FS_PID, FS_TOKEN), "unknown", "unknown"),
            ((FS_PID, None), "match", "unknown"),
            (None, "match", "unknown"),
        ],
    )
    def test_forkserver_state(self, daemon, mocker, record, ident, expected):
        World(mocker, ident)
        daemon._worker_fs["snapshot"] = record
        assert daemon._forkserver_state("snapshot") == expected

    def test_forkserver_state_of_an_unknown_worker_is_unknown(self, daemon):
        assert daemon._forkserver_state("nobody") == "unknown"


@pytest.mark.unit
class TestForkserverRecord:
    def test_current_forkserver_reads_pid_and_token(self, mocker):
        from multiprocessing import forkserver
        mocker.patch.object(forkserver._forkserver, "_forkserver_pid", 4321)
        mocker.patch.object(daemon_module.procident, "start_token", return_value="T")
        assert BackupDaemon._current_forkserver() == (4321, "T")

    @pytest.mark.parametrize("pid", [None, 0, 1, "x"])
    def test_current_forkserver_without_a_usable_pid_is_none(self, mocker, pid):
        from multiprocessing import forkserver
        mocker.patch.object(forkserver._forkserver, "_forkserver_pid", pid)
        assert BackupDaemon._current_forkserver() is None

    def test_current_forkserver_never_raises(self, mocker):
        from multiprocessing import forkserver
        mocker.patch.object(forkserver, "_forkserver", None)    # attribute access fails
        assert BackupDaemon._current_forkserver() is None

    def test_spawn_records_the_forkserver_under_forkserver(self, daemon, mocker):
        mocker.patch.object(daemon_module.multiprocessing, "get_start_method", return_value="forkserver")
        mocker.patch.object(daemon_module.procident, "start_token", return_value="T1")
        mocker.patch.object(BackupDaemon, "_current_forkserver", return_value=(FS_PID, FS_TOKEN))
        mocker.patch.object(daemon, "_new_worker", return_value=mock_worker(pid=77))
        daemon._spawn("snapshot")
        assert daemon._worker_fs["snapshot"] == (FS_PID, FS_TOKEN)

    @pytest.mark.parametrize("method", ["spawn", "fork"])
    def test_fork_and_spawn_record_no_tokens_and_never_look_at_the_forkserver(
        self, daemon, mocker, method
    ):
        mocker.patch.object(daemon_module.multiprocessing, "get_start_method", return_value=method)
        token = mocker.patch.object(daemon_module.procident, "start_token", return_value="T1")
        probe = mocker.patch.object(BackupDaemon, "_current_forkserver", return_value=(1, "x"))
        mocker.patch.object(daemon, "_new_worker", return_value=mock_worker(pid=77))
        daemon._spawn("snapshot")
        assert daemon._worker_token["snapshot"] is None
        assert daemon._worker_fs["snapshot"] is None
        token.assert_not_called()
        probe.assert_not_called()

    def test_forget_worker_drops_the_forkserver_record(self, daemon):
        adopt(daemon, "snapshot")
        daemon._worker_fs["snapshot"] = (FS_PID, FS_TOKEN)
        daemon._forget_worker("snapshot")
        assert "snapshot" not in daemon._worker_fs


# ---------------------------------------------------------------------------
# _signal_verified
# ---------------------------------------------------------------------------


class Pidfds:
    """Mocked pidfd calls over a real (throwaway) descriptor, recording the order of events."""

    def __init__(self, mocker, ident="match", open_error=None):
        self.events = []
        self.fd = None
        self.ident = ident
        self.open_error = open_error
        mocker.patch.object(daemon_module.os, "pidfd_open", side_effect=self._open, create=True)
        mocker.patch.object(daemon_module.signal, "pidfd_send_signal", side_effect=self._send, create=True)
        mocker.patch.object(daemon_module.procident, "identity", side_effect=self._identity)
        mocker.patch.object(daemon_module.os, "kill", side_effect=self._kill)

    def _open(self, pid):
        self.events.append(("pidfd_open", pid))
        if self.open_error is not None:
            raise self.open_error
        self.fd = os.open(os.devnull, os.O_RDONLY)
        return self.fd

    def _identity(self, pid, token):
        self.events.append(("identity", pid, token))
        return self.ident(self) if callable(self.ident) else self.ident

    def _send(self, fd, sig):
        self.events.append(("pidfd_send_signal", fd == self.fd, sig))

    def _kill(self, pid, sig):
        self.events.append(("kill", pid, sig))

    def fd_closed(self):
        if self.fd is None:
            return True
        try:
            os.fstat(self.fd)
        except OSError:
            return True
        return False


@pytest.mark.unit
class TestSignalVerified:
    def sig(self, pid=FAKE_PID, token="tok", sig=signal.SIGTERM):
        return daemon_module._signal_verified(pid, token, sig)

    # --- with pidfds -----------------------------------------------------

    def test_with_pidfds_the_order_is_open_then_identity_then_send(self, mocker):
        p = Pidfds(mocker, "match")
        assert self.sig() == "sent"
        assert p.events == [
            ("pidfd_open", FAKE_PID),
            ("identity", FAKE_PID, "tok"),
            ("pidfd_send_signal", True, signal.SIGTERM),
        ]
        assert p.fd_closed()

    @pytest.mark.parametrize("ident", ["mismatch", "gone"])
    def test_a_pid_recycled_between_open_and_check_is_not_signalled(self, mocker, ident):
        # The descriptor was opened first; the identity check then says "not ours".
        p = Pidfds(mocker, ident)
        assert self.sig() == "gone"
        assert [e[0] for e in p.events] == ["pidfd_open", "identity"]
        assert p.fd_closed()

    def test_unverifiable_identity_sends_nothing(self, mocker):
        p = Pidfds(mocker, "unknown")
        assert self.sig() == "unverified"
        assert [e[0] for e in p.events] == ["pidfd_open", "identity"]
        assert p.fd_closed()

    def test_no_such_process_at_open_is_gone_without_an_identity_check(self, mocker):
        p = Pidfds(mocker, open_error=ProcessLookupError())
        assert self.sig() == "gone"
        assert p.events == [("pidfd_open", FAKE_PID)]

    def test_esrch_from_the_send_is_gone_and_other_errors_are_errors(self, mocker):
        p = Pidfds(mocker, "match")
        daemon_module.signal.pidfd_send_signal.side_effect = ProcessLookupError()
        assert self.sig() == "gone" and p.fd_closed()
        daemon_module.signal.pidfd_send_signal.side_effect = PermissionError()
        assert self.sig() == "error" and p.fd_closed()

    def test_an_unusable_pidfd_open_falls_back_to_kill(self, mocker):
        p = Pidfds(mocker, "match", open_error=PermissionError())
        assert self.sig() == "sent"
        assert p.events == [
            ("pidfd_open", FAKE_PID), ("identity", FAKE_PID, "tok"), ("kill", FAKE_PID, signal.SIGTERM),
        ]

    # --- without pidfds ----------------------------------------------------

    def test_without_pidfds_it_checks_then_kills(self, mocker):
        w = World(mocker, "match", fake_time=False)
        assert self.sig(sig=signal.SIGKILL) == "sent"
        assert w.kills == [(FAKE_PID, signal.SIGKILL)]

    @pytest.mark.parametrize("ident, outcome", [
        ("gone", "gone"), ("mismatch", "gone"), ("unknown", "unverified"),
    ])
    def test_without_pidfds_nothing_is_sent_unless_identity_matches(self, mocker, ident, outcome):
        w = World(mocker, ident, fake_time=False)
        assert self.sig() == outcome
        assert w.kills == []

    @pytest.mark.parametrize("pid, token", [
        (FAKE_PID, None), (FAKE_PID, ""), (None, "tok"), (1, "tok"), (0, "tok"), (-5, "tok"),
    ])
    def test_a_missing_token_or_silly_pid_is_unverified(self, mocker, pid, token):
        w = World(mocker, "match", fake_time=False)
        assert daemon_module._signal_verified(pid, token, signal.SIGTERM) == "unverified"
        assert w.kills == [] and w.checked == []

    def test_kill_errors_map_to_gone_and_error(self, mocker):
        w = World(mocker, "match", fake_time=False)
        daemon_module.os.kill.side_effect = ProcessLookupError()
        assert self.sig() == "gone"
        daemon_module.os.kill.side_effect = PermissionError()
        assert self.sig() == "error"

    # --- a real process ----------------------------------------------------

    @pytest.mark.skipif(not HAVE_TOKENS, reason="procident start tokens unavailable here")
    def test_a_real_process_is_signalled_only_with_its_own_token(self):
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(120)"], start_new_session=True,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=hermetic_env(),
        )
        try:
            token = procident.start_token(proc.pid)
            assert token
            assert daemon_module._signal_verified(proc.pid, "someone-else", signal.SIGTERM) == "gone"
            time.sleep(0.2)
            assert proc.poll() is None                      # not hit
            assert daemon_module._signal_verified(proc.pid, token, signal.SIGTERM) == "sent"
            assert proc.wait(timeout=10) == -signal.SIGTERM
            assert daemon_module._signal_verified(proc.pid, token, signal.SIGKILL) == "gone"   # reaped
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=10)

    @pytest.mark.skipif(
        not (hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal")),
        reason="no pidfds on this platform (Linux 5.3+)",
    )
    @pytest.mark.skipif(not HAVE_TOKENS, reason="procident start tokens unavailable here")
    def test_a_real_pidfd_signals_the_process_it_was_opened_for(self):
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(120)"], start_new_session=True,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=hermetic_env(),
        )
        try:
            token = procident.start_token(proc.pid)
            before = len(os.listdir("/proc/self/fd")) if os.path.isdir("/proc/self/fd") else None
            assert daemon_module._signal_verified(proc.pid, token, signal.SIGTERM) == "sent"
            if before is not None:
                assert len(os.listdir("/proc/self/fd")) <= before          # no descriptor leaked
            assert proc.wait(timeout=10) == -signal.SIGTERM
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=10)


# ---------------------------------------------------------------------------
# A real process that is not ours to kill
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.skipif(not HAVE_TOKENS, reason="procident start tokens unavailable here")
class TestNeverSignalsAForeignProcess:
    @pytest.fixture
    def bystander(self):
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(120)"],
            start_new_session=True,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=hermetic_env(),
        )
        try:
            yield proc
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=10)

    def test_a_live_process_with_another_start_token_is_left_alone(self, daemon, bystander):
        """The pid exists and is alive, but it is not the process `_spawn` started."""
        bystander_token = procident.start_token(bystander.pid)
        assert bystander_token
        adopt(daemon, "snapshot", mock_worker(pid=bystander.pid), token="not-its-token", lost=True)
        started = time.monotonic()
        daemon._shutdown_workers(timeout=1)
        assert time.monotonic() - started < 3
        time.sleep(0.2)
        assert bystander.poll() is None               # still running: never signalled
        assert daemon.abandoned_workers == []

    def test_control_the_same_process_with_its_real_token_is_terminated(self, daemon, bystander):
        token = procident.start_token(bystander.pid)
        adopt(daemon, "snapshot", mock_worker(pid=bystander.pid), token=token, lost=True)
        daemon._shutdown_workers(timeout=1)
        assert bystander.wait(timeout=10) == -signal.SIGTERM
        assert daemon.abandoned_workers == []


# ---------------------------------------------------------------------------
# Worker-side wording
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestWorkerWording:
    SUPERVISOR = 4242

    def armed(self, monkeypatch, *, method, ident, parent=777, pid_exists=True):
        state = {"ppid": parent}
        monkeypatch.setattr(workers.os, "getppid", lambda: state["ppid"])
        monkeypatch.setattr(workers, "_pid_exists", lambda pid: pid_exists)
        monkeypatch.setattr(
            workers.multiprocessing, "get_start_method", lambda allow_none=False: method
        )
        monkeypatch.setattr(workers.procident, "identity", lambda pid, token: ident)
        monkeypatch.setattr(workers.procident, "is_zombie", lambda pid: None)
        worker = SnapshotWorker(make_config(), StopFlag())
        worker.supervisor_pid = self.SUPERVISOR
        worker.supervisor_start = "tok"
        worker._arm_supervisor_watch()
        return worker, state

    def test_forkserver_exit_with_a_verified_supervisor_says_so(self, monkeypatch, caplog):
        worker, state = self.armed(monkeypatch, method="forkserver", ident="match")
        assert worker._direct_child is False
        assert worker._supervisor_gone() is False
        state["ppid"] = 1                              # the forkserver exited
        with caplog.at_level(logging.ERROR):
            assert worker._supervisor_gone() is True
        (line,) = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert line == (
            "forkserver exited (parent changed from 777 to 1); supervisor 4242 is alive; "
            "leaving at this safe point"
        )
        assert "is gone" not in caplog.text

    @pytest.mark.parametrize("ident", ["gone", "mismatch", "unknown"])
    def test_forkserver_with_an_unverified_supervisor_keeps_the_gone_wording(
        self, monkeypatch, caplog, ident
    ):
        worker, state = self.armed(
            monkeypatch, method="forkserver", ident=ident, pid_exists=False
        )
        state["ppid"] = 1
        with caplog.at_level(logging.ERROR):
            assert worker._supervisor_gone() is True
        assert "supervisor 4242 is gone (parent changed from 777 to 1); exiting" in caplog.text
        assert "forkserver exited" not in caplog.text

    @pytest.mark.parametrize("method", ["spawn", "fork"])
    def test_a_direct_child_never_blames_the_forkserver(self, monkeypatch, caplog, method):
        worker, state = self.armed(monkeypatch, method=method, ident="match", parent=self.SUPERVISOR)
        assert worker._direct_child is True
        state["ppid"] = 1                              # reparented: even with a "matching" supervisor
        with caplog.at_level(logging.ERROR):
            assert worker._supervisor_gone() is True
        assert "supervisor 4242 is gone (parent changed from 4242 to 1); exiting" in caplog.text
        assert "forkserver exited" not in caplog.text

    def test_forkserver_with_an_unchanged_parent_and_a_dead_supervisor_is_gone(
        self, monkeypatch, caplog
    ):
        # The forkserver is alive; the SUPERVISOR died. Not the forkserver's doing.
        worker, state = self.armed(monkeypatch, method="forkserver", ident="gone")
        with caplog.at_level(logging.ERROR):
            assert worker._supervisor_gone() is True
        assert "supervisor 4242 is gone (no such process); exiting" in caplog.text
        assert "forkserver exited" not in caplog.text

    def test_the_forkserver_wording_is_logged_once(self, monkeypatch, caplog):
        worker, state = self.armed(monkeypatch, method="forkserver", ident="match")
        state["ppid"] = 1
        with caplog.at_level(logging.ERROR):
            for _ in range(4):
                assert worker._supervisor_gone() is True
        assert caplog.text.count("forkserver exited") == 1


# ---------------------------------------------------------------------------
# Real processes
# ---------------------------------------------------------------------------

STUBS = r'''
"""Stub workers: record START / ORPHAN-NOTICED / TERM-IGNORED / END with pid and time."""
import multiprocessing, os, signal, time

LOG = None
MODES = {}
LINGER = 1.5


def configure(log, modes, linger):
    global LOG, MODES, LINGER
    LOG, MODES, LINGER = log, modes, linger


def rec(path, msg):
    with open(path, "a") as f:
        f.write(f"{time.time():.4f} {msg}\n")


class Stub(multiprocessing.Process):
    NAME = "x"

    def __init__(self, config, stop_event, verbose=False, **kw):
        super().__init__(daemon=True)
        self.stop_event = stop_event
        self.log = LOG
        self.mode = MODES.get(self.NAME, "obey")
        self.linger = LINGER

    def run(self):
        me = os.getpid()
        ppid0 = os.getppid()
        rec(self.log, f"START {self.NAME} {me}")
        if self.mode == "ignore_term":
            signal.signal(signal.SIGTERM, lambda s, f: rec(self.log, f"TERM-IGNORED {self.NAME} {me}"))
        if self.mode == "exit255":                    # a genuine 255, forkserver healthy
            time.sleep(0.5)
            os._exit(255)
        noticed = None
        cap = time.time() + 45                       # no stub outlives the test, whatever happens
        while time.time() < cap:
            if self.mode == "obey" and noticed is None and self.stop_event.is_set():
                break
            if noticed is None and os.getppid() != ppid0:
                noticed = time.time()
                rec(self.log, f"ORPHAN-NOTICED {self.NAME} {me}")
            if noticed is not None and self.mode in ("obey", "linger"):
                time.sleep(self.linger)               # the unfinished "transfer"
                break
            time.sleep(0.05)
        rec(self.log, f"END {self.NAME} {me}")


class Snapshot(Stub):
    NAME = "snapshot"


class Pruning(Stub):
    NAME = "pruning"


class Api(Stub):
    NAME = "api"
'''

DRIVER = r'''
import json, logging, multiprocessing, os, signal, sys, time

spec = json.loads(sys.argv[1])
sys.path.insert(0, spec["stubdir"])
sys.path.insert(0, spec["repo"])


def read_log():
    rows = []
    try:
        with open(spec["log"]) as f:
            for line in f:
                parts = line.split()
                if len(parts) == 4:
                    rows.append([float(parts[0]), parts[1], parts[2], int(parts[3])])
    except FileNotFoundError:
        pass
    return rows


def wait_for(pred, seconds):
    end = time.time() + seconds
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.05)
    return False


def main():
    from zfsbackup import daemon, procident
    from zfsbackup.config import BackupConfig, DatasetConfig
    import fwstubs

    multiprocessing.set_start_method(spec["method"], force=True)
    fwstubs.configure(spec["log"], spec["modes"], spec["linger"])
    daemon.SnapshotWorker, daemon.PruningWorker, daemon.ApiWorker = (
        fwstubs.Snapshot, fwstubs.Pruning, fwstubs.Api)

    records = []

    class H(logging.Handler):
        def emit(self, record):
            records.append([record.levelname, record.getMessage()])

    lg = logging.getLogger("zfsbackup.daemon")
    lg.setLevel(logging.DEBUG)
    lg.addHandler(H())

    d = daemon.BackupDaemon(BackupConfig(datasets=[DatasetConfig(name="pool/data")]), None)
    out = {"error": None, "ever_failures": False, "ever_restart_at": False, "lost_first": None,
           "abandoned": [], "shutdown_seconds": None, "restart_in": None, "kill_time": None}
    try:
        d._start_workers()
        assert wait_for(lambda: len([r for r in read_log() if r[1] == "START"]) >= 3, 20), "stubs did not start"
        time.sleep(0.3)

        names = ("snapshot", "pruning", "api")

        def started(n):
            return len([r for r in read_log() if r[1] == "START"])

        def all_started(times):
            starts = [r for r in read_log() if r[1] == "START"]
            return all(sum(1 for r in starts if r[2] == n) >= times for n in names)

        def kill_forkserver():
            from multiprocessing import forkserver
            os.kill(forkserver._forkserver._forkserver_pid, signal.SIGKILL)
            out["kill_time"] = time.time()

        def observe():
            if d._failures:
                out["ever_failures"] = True
            if d._restart_at:
                out["ever_restart_at"] = True
            if d._lost and out["lost_first"] is None:
                out["lost_first"] = sorted(d._lost)
            if d._restart_at and out["restart_in"] is None:
                out["restart_in"] = {n: at - d._monotonic() for n, at in d._restart_at.items()}

        if spec["kill_cycles"]:
            # the forkserver dies again and again; the supervisor polls at its own cadence
            for cycle in range(spec["kill_cycles"]):
                assert wait_for(lambda: all_started(cycle + 1), 30), f"cycle {cycle}: stubs not up"
                time.sleep(0.3)
                kill_forkserver()
                time.sleep(spec["poll"])
                d._check_workers()
                observe()
            assert wait_for(lambda: all_started(spec["kill_cycles"] + 1), 30), "last replacements"
            out["stopped"] = d._stop_event.is_set()
            out["crash_looped"] = sorted(d._crash_looped)
        elif spec["kill"] == "forkserver":
            kill_forkserver()
            time.sleep(0.4)                          # the sentinel pipes see EOF
        elif spec["kill"] == "worker":
            os.kill(d._workers["pruning"].pid, signal.SIGKILL)
            time.sleep(0.2)

        if spec["checks"] and not spec["kill_cycles"]:
            end = time.time() + spec["check_seconds"]
            while time.time() < end:
                if spec["poll"]:
                    time.sleep(spec["poll"])         # the supervisor's real cadence
                d._check_workers()
                observe()
                if spec["until_replaced"]:
                    if all_started(2) and not d._lost:
                        break
                if not spec["poll"]:
                    time.sleep(0.1)
        out["lost_at_end"] = sorted(d._lost)
        out["failures_at_end"] = dict(d._failures)
        out["restart_at_at_end"] = sorted(d._restart_at)
        out["tokens"] = {n: bool(t) for n, t in d._worker_token.items()}

        if spec["shutdown"]:
            d._stop_event.set()
            out["lost_before_shutdown"] = sorted(d._lost)
            t0 = time.time()
            d._shutdown_workers(timeout=spec["shutdown_timeout"])
            out["shutdown_seconds"] = time.time() - t0
            out["abandoned"] = list(d.abandoned_workers)
            out["lost_after_shutdown"] = sorted(d._lost)
        else:
            d._stop_event.set()
            d._shutdown_workers(timeout=10)
    except BaseException as exc:                      # reported, never swallowed
        out["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        rows = read_log()
        pids = sorted({r[3] for r in rows})
        alive = []
        for pid in pids:
            try:
                os.kill(pid, 0)
            except OSError:
                continue
            if procident.is_zombie(pid) is True:
                continue
            alive.append(pid)
        out["alive_at_end"] = alive
        for pid in alive:                             # nothing outlives the driver
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        out["records"] = records
        out["log"] = rows
        with open(spec["out"], "w") as f:
            json.dump(out, f)


if __name__ == "__main__":
    main()
'''


class Run:
    """The outcome of one driver run, with the stub log decoded."""

    def __init__(self, out, stderr):
        self.out = out
        self.stderr = stderr
        self.rows = out["log"]
        self.records = [tuple(r) for r in out["records"]]

    def __getitem__(self, key):
        return self.out[key]

    def events(self, what, name=None):
        return [r for r in self.rows if r[1] == what and (name is None or r[2] == name)]

    def intervals(self, name):
        """[(start, end_or_None, pid)] in start order."""
        result = []
        for t, what, n, pid in self.rows:
            if n != name:
                continue
            if what == "START":
                result.append([t, None, pid])
            elif what == "END":
                for item in result:
                    if item[2] == pid:
                        item[1] = t
        return [tuple(i) for i in sorted(result)]

    def messages(self, level=None):
        return [m for lv, m in self.records if level is None or lv == level]


@pytest.fixture
def run_driver(tmp_path):
    started = []

    def run(method, *, kill=None, checks=True, check_seconds=12, until_replaced=True,
            shutdown=False, shutdown_timeout=10, modes=None, linger=1.5, timeout=90,
            poll=0, kill_cycles=0):
        stubdir = tmp_path / "stubs"
        stubdir.mkdir(exist_ok=True)
        (stubdir / "fwstubs.py").write_text(STUBS)
        driver = tmp_path / "driver.py"
        driver.write_text(DRIVER)
        for leftover in ("stubs.log", "out.json"):
            (tmp_path / leftover).unlink(missing_ok=True)
        spec = {
            "method": method, "kill": kill, "checks": checks, "check_seconds": check_seconds,
            "until_replaced": until_replaced, "shutdown": shutdown,
            "shutdown_timeout": shutdown_timeout, "modes": modes or {}, "linger": linger,
            "poll": poll, "kill_cycles": kill_cycles,
            "stubdir": str(stubdir), "repo": str(REPO), "log": str(tmp_path / "stubs.log"),
            "out": str(tmp_path / "out.json"),
        }
        proc = subprocess.Popen(
            [sys.executable, str(driver), json.dumps(spec)],
            cwd=str(tmp_path), env=hermetic_env(), start_new_session=True,
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        started.append(proc)
        try:
            _, stderr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            _killpg(proc)
            _, stderr = proc.communicate()
            pytest.fail(f"driver timed out after {timeout}s:\n{stderr[-3000:]}")
        finally:
            _killpg(proc)
        out_path = tmp_path / "out.json"
        assert out_path.exists(), f"driver wrote no result (rc={proc.returncode}):\n{stderr[-3000:]}"
        result = Run(json.loads(out_path.read_text()), stderr)
        assert result["error"] is None, f"{result['error']}\n{stderr[-3000:]}"
        return result

    yield run
    for proc in started:
        _killpg(proc)


def _killpg(proc):
    """Everything the driver started shares its session (pgid == pid)."""
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


@pytest.mark.integration
@pytest.mark.slow
@needs_forkserver
class TestRealForkserverDeath:
    def test_killing_the_forkserver_orphans_workers_that_are_replaced_without_overlap(
        self, run_driver
    ):
        r = run_driver("forkserver", kill="forkserver", linger=1.5)

        # classified as lost, never as crashed
        assert r["lost_first"] == ["api", "pruning", "snapshot"]
        assert r["ever_failures"] is False and r["ever_restart_at"] is False
        assert r["failures_at_end"] == {} and r["restart_at_at_end"] == []

        errors = r.messages("ERROR")
        orphan_lines = [m for m in errors if m.startswith("forkserver exited; ")]
        assert len(orphan_lines) == 1, errors
        assert "3 worker(s) orphaned" in orphan_lines[0]
        assert not [m for m in errors if "exited with code" in m], errors

        for name in NAMES:
            runs = r.intervals(name)
            assert len(runs) == 2, (name, r.rows)
            (s1, e1, pid1), (s2, e2, pid2) = runs
            assert pid1 != pid2
            assert e1 is not None, "the orphan never ended"
            assert s2 >= e1, f"{name}: the replacement started before the orphan ended"
            # an immediate replacement: nowhere near the 5 s backoff
            assert s2 - e1 < WORKER_RESTART_DELAY * 0.6, (name, s2 - e1)
            assert e1 - s1 >= 1.0                      # the orphan really lingered (an unfinished "transfer")
            assert r.events("ORPHAN-NOTICED", name)

        info = r.messages("INFO")
        for name in NAMES:
            assert any(m.startswith(f"orphaned {name} worker (pid=") for m in info), info
            assert any(m.startswith(f"Restarted {name} worker (pid=") for m in info), info
        assert r["lost_at_end"] == []                  # the replacements are normal workers
        assert r["alive_at_end"] == []                 # replacements stopped cleanly too
        for name in NAMES:
            assert r.intervals(name)[1][1] is not None

    def test_no_replacement_starts_while_a_slow_orphan_is_alive(
        self, run_driver
    ):
        """A long-lingering orphan is never joined by a replacement during that time."""
        r = run_driver("forkserver", kill="forkserver", linger=4.0, until_replaced=True,
                       check_seconds=14)
        for name in NAMES:
            (s1, e1, _), (s2, _, _) = r.intervals(name)
            assert s2 >= e1
            assert s2 - s1 >= 4.0                      # the second start waited out the linger
        assert r["ever_failures"] is False

    def test_forkserver_dead_since_the_last_poll_still_waits_for_the_orphans(self, run_driver):
        r = run_driver("forkserver", kill="forkserver", checks=False, shutdown=True,
                       shutdown_timeout=15, modes={n: "linger" for n in NAMES}, linger=3.0)
        assert r["lost_before_shutdown"] == []         # no poll ran: nothing was classified
        assert sorted(r["lost_after_shutdown"]) == list(NAMES)   # shutdown identified them
        assert r["shutdown_seconds"] >= 1.5            # it did wait for the lingering orphans
        assert r["shutdown_seconds"] < 10
        for name in NAMES:
            ((s, e, _),) = r.intervals(name)
            assert e is not None, f"{name} was abandoned to run on"
        assert r["abandoned"] == []
        assert r["alive_at_end"] == []
        assert not [m for m in r.messages("WARNING") if "did not exit" in m]

    def test_shutdown_terminates_orphans_that_overstay_the_budget(self, run_driver):
        r = run_driver("forkserver", kill="forkserver", shutdown=True, shutdown_timeout=1,
                       until_replaced=False, check_seconds=1.0,
                       modes={n: "forever" for n in NAMES})
        assert sorted(r["lost_before_shutdown"]) == list(NAMES)
        warnings = [m for m in r.messages("WARNING") if "did not exit within the 1s shutdown budget" in m]
        assert len(warnings) == 3, r.messages()
        assert all(m.startswith("orphaned ") for m in warnings)
        assert r["abandoned"] == []
        assert r["alive_at_end"] == []                 # SIGTERM ended them
        assert not r.events("END")                     # killed, not graceful
        assert 1.0 <= r["shutdown_seconds"] < 5.0

    def test_shutdown_sigkills_an_orphan_that_ignores_sigterm(self, run_driver):
        r = run_driver("forkserver", kill="forkserver", shutdown=True, shutdown_timeout=1,
                       until_replaced=False, check_seconds=1.0,
                       modes={"api": "ignore_term", "snapshot": "forever", "pruning": "forever"},
                       timeout=120)
        assert r.events("TERM-IGNORED", "api")         # SIGTERM was sent first and refused
        assert r["abandoned"] == []
        assert r["alive_at_end"] == []                 # then SIGKILL (by pid) ended it
        assert not r.events("END")
        assert r["shutdown_seconds"] >= 5.0            # the SIGTERM grace elapsed first
        assert r["shutdown_seconds"] < 20

    def test_control_without_a_forkserver_death_shutdown_is_quick_and_clean(self, run_driver):
        """Control: the same stubs, forkserver alive, stop flag: a plain quick shutdown."""
        r = run_driver("forkserver", kill=None, checks=False, shutdown=True, shutdown_timeout=10)
        assert r["abandoned"] == [] and r["lost_after_shutdown"] == []
        for name in NAMES:
            ((s, e, _),) = r.intervals(name)
            assert e is not None
        assert r["shutdown_seconds"] < 5
        assert r["alive_at_end"] == []


    def test_at_the_real_poll_cadence_idle_orphans_that_left_early_are_replaced_at_once(
        self, run_driver
    ):
        """The review's case: the orphans leave within a slice of the forkserver's death, so at
        the supervisor's real 5 s poll every worker is long gone and `poll()` says 255 -- it
        must not read as three crashes (D9 count, 5 s backoff, a doubling schedule)."""
        r = run_driver("forkserver", kill="forkserver", linger=0.0, poll=SUPERVISOR_POLL,
                       check_seconds=30, timeout=120)
        assert r["ever_failures"] is False and r["ever_restart_at"] is False
        assert r["failures_at_end"] == {} and r["restart_at_at_end"] == []
        errors = r.messages("ERROR")
        orphan_lines = [m for m in errors if m.startswith("forkserver exited; ")]
        assert len(orphan_lines) == 1, errors
        assert "3 worker(s) orphaned" in orphan_lines[0] and orphan_lines[0].count("already exited") == 4
        assert orphan_lines[0].endswith("they have already exited; replacing them now")
        assert not [m for m in errors if "exited with code" in m], errors
        for name in NAMES:
            (s1, e1, _), (s2, _, _) = r.intervals(name)
            assert e1 is not None and e1 < r["kill_time"] + 2.0       # it really left early
            # replaced by the FIRST poll after the death, not one backoff (5 s) later
            assert s2 - r["kill_time"] < SUPERVISOR_POLL + 3.0, (name, s2 - r["kill_time"])
            assert s2 >= e1
        assert not [m for m in r.messages("INFO") if "has exited" in m]  # never classified "lost"

    def test_at_the_real_poll_cadence_a_long_lingering_orphan_is_lost_then_replaced(
        self, run_driver
    ):
        r = run_driver("forkserver", kill="forkserver", linger=7.0, poll=SUPERVISOR_POLL,
                       check_seconds=40, timeout=120)
        assert r["lost_first"] == ["api", "pruning", "snapshot"]    # still running at the 1st poll
        assert r["ever_failures"] is False and r["ever_restart_at"] is False
        orphan_lines = [m for m in r.messages("ERROR") if m.startswith("forkserver exited; ")]
        assert len(orphan_lines) == 1 and "already exited" not in orphan_lines[0]
        for name in NAMES:
            (s1, e1, _), (s2, _, _) = r.intervals(name)
            assert e1 is not None and s2 >= e1                      # never beside the orphan
            assert e1 - r["kill_time"] > SUPERVISOR_POLL            # it outlived the first poll
            assert s2 - e1 < SUPERVISOR_POLL + 2.0                  # replaced by the next poll
        assert any(m.startswith("orphaned api worker (pid=") for m in r.messages("INFO"))

    def test_repeated_forkserver_deaths_never_become_a_crash_loop(self, run_driver):
        """More forkserver deaths in a row than WORKER_MAX_CONSECUTIVE_FAILURES: not one of them
        is a worker failure, so the daemon neither gives up nor raises its stop flag."""
        cycles = WORKER_MAX_CONSECUTIVE_FAILURES + 1
        r = run_driver("forkserver", kill_cycles=cycles, poll=1.0, linger=0.0, checks=False,
                       timeout=170)
        assert r["stopped"] is False and r["crash_looped"] == []
        assert r["ever_failures"] is False and r["ever_restart_at"] is False
        assert r["failures_at_end"] == {}
        for name in NAMES:
            assert len(r.events("START", name)) == cycles + 1
        errors = r.messages("ERROR")
        assert len([m for m in errors if m.startswith("forkserver exited; ")]) == cycles
        assert not [m for m in errors if "consecutive" in m or "giving up" in m], errors

    def test_a_genuine_exit_255_with_a_healthy_forkserver_is_a_crash(self, run_driver):
        r = run_driver("forkserver", checks=True, check_seconds=1.5, until_replaced=False,
                       modes={"pruning": "exit255"}, shutdown=True)
        assert r["lost_first"] is None and r["lost_at_end"] == []
        assert r["failures_at_end"] == {"pruning": 1}
        assert r["restart_at_at_end"] == ["pruning"]
        errors = r.messages("ERROR")
        assert len(errors) == 1 and "exited with code 255" in errors[0], errors
        assert "forkserver exited" not in errors[0]


@pytest.mark.integration
@pytest.mark.slow
class TestRealNormalDeath:
    @pytest.mark.parametrize(
        "method",
        [
            m for m in ("fork", "spawn", "forkserver")
            if m in multiprocessing.get_all_start_methods()
            and (m != "forkserver" or HAVE_TOKENS)
        ],
    )
    def test_an_ordinary_crash_is_counted_and_backed_off(self, run_driver, method):
        r = run_driver(method, kill="worker", checks=True, check_seconds=1.5,
                       until_replaced=False, shutdown=True, shutdown_timeout=10)
        assert r["lost_first"] is None and r["lost_at_end"] == []
        assert r["failures_at_end"] == {"pruning": 1}
        assert r["restart_at_at_end"] == ["pruning"]
        delay = r["restart_in"]["pruning"]
        assert WORKER_RESTART_DELAY - 2.5 < delay <= WORKER_RESTART_DELAY
        errors = r.messages("ERROR")
        assert len(errors) == 1, errors
        assert "pruning worker" in errors[0] and "restarting in 5s" in errors[0]
        assert "forkserver" not in errors[0]
        assert len(r.intervals("pruning")) == 1       # not replaced during the backoff
        # the siblings are untouched
        for name in ("snapshot", "api"):
            assert len(r.intervals(name)) == 1
        assert r["abandoned"] == [] and r["alive_at_end"] == []

    def test_tokens_are_recorded_only_under_forkserver(self, run_driver):
        for method in ("spawn", "fork"):
            if method not in multiprocessing.get_all_start_methods():
                continue
            r = run_driver(method, kill=None, checks=False, shutdown=True)
            assert r["tokens"] and not any(r["tokens"].values()), (method, r["tokens"])
        if HAVE_FORKSERVER and HAVE_TOKENS:
            r = run_driver("forkserver", kill=None, checks=False, shutdown=True)
            assert r["tokens"] and all(r["tokens"].values())

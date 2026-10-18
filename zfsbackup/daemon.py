#!/usr/bin/env python3
"""ZFS Backup Daemon - Main entry point."""

import argparse
import logging
import multiprocessing
import os
import signal
import sys
import time
from typing import Dict, List, Optional, Set

from sqlalchemy.exc import DatabaseError, OperationalError

from zfsbackup.config import BackupConfig
from zfsbackup.config.store import (
    CONFIG_PATH_ENV,
    DEFAULT_CONFIG_DB,
    ConfigPathError,
    ResolvedConfigPath,
    SchemaError,
    check_config_db,
    dispose_all,
    resolve_config_path,
)
from zfsbackup.runtime_config import (
    config_error_message,
    describe_config_source,
    load_runtime_config,
)
from zfsbackup import daemonlock
from zfsbackup.backup_manager import DatasetManager
from zfsbackup.stopflag import StopFlag
from zfsbackup.workers import SnapshotWorker, PruningWorker, ApiWorker, RemoteBackupWorker


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

#: Base of the per-worker restart backoff: the delay before the n-th
#: consecutive restart is `WORKER_RESTART_DELAY * 2**(n-1)`, capped at
#: `WORKER_RESTART_MAX_DELAY`. See `BackupDaemon._record_worker_death`.
#:
#: The 300s cap is currently unreachable: with
#: `WORKER_MAX_CONSECUTIVE_FAILURES = 6` the daemon gives up after the sixth
#: death, so the largest delay ever applied is 5 * 2**4 = 80s. The cap is kept
#: deliberately, as a guard should the failure limit ever be raised.
WORKER_RESTART_DELAY = 5
WORKER_RESTART_MAX_DELAY = 300
#: A worker that stayed up at least this long (seconds) before dying is
#: considered to have recovered: its consecutive-failure count restarts at 1.
WORKER_STABLE_UPTIME = 600
#: This many consecutive deaths of one worker (none of them after a
#: `WORKER_STABLE_UPTIME` run) is fatal to the daemon: it stops and exits 1
#: instead of respawning forever.
WORKER_MAX_CONSECUTIVE_FAILURES = 6
SUPERVISOR_POLL = 5
#: Longest slice `_supervisor_sleep` sleeps for before re-reading the stop
#: flag. This bounds one thing only: how long the supervisor stays in its
#: idle sleep after the flag is raised. It is **not** the daemon's
#: signal-to-shutdown latency, and `TimeoutStopSec` must not be sized from
#: it. The real worst case is the sum of three terms:
#:
#:   1. this slice, 0.25s, if the signal lands in the idle sleep;
#:   2. nothing from the restart path: `_check_workers` no longer sleeps.
#:      A crashed worker's restart is a per-worker `restart_at` deadline
#:      checked once per poll, so backoff can never delay a shutdown;
#:   3. `_shutdown_workers`' escalation, up to `timeout` + 5s terminate-join
#:      + 5s kill-join per worker that will not exit. The 30s budget is now
#:      a single deadline shared by all workers rather than per-worker, so
#:      four workers stuck in a long `zfs send` cost up to 30 + 4x10 = 70s
#:      there, not 4x40 = 160s.
#:
#: So roughly 0.25 + 70 ~= 70s worst case, under systemd's default
#: `TimeoutStopSec=90s`. On the healthy path -- workers parked in
#: `stop_event.wait()` -- the tail is one worker's `stopflag.STOP_POLL`
#: slice, not this one; measured end-to-end shutdowns land at 0.265-0.554s.
SUPERVISOR_SLEEP_SLICE = 0.25


class BackupDaemon:
    """Supervisor process: starts and monitors the snapshot, pruning, API, and remote workers."""

    def __init__(
        self,
        config: BackupConfig,
        resolved_config: ResolvedConfigPath,
        verbose: bool = False,
    ):
        #: The supervisor's detached `BackupConfig`, read once from the
        #: config database before the first fork, with `--dry-run` already
        #: applied. This one object is handed to every worker at startup and
        #: at every crash restart (see `_new_worker`), so all workers always
        #: run the same generation; a restart does not re-read the database
        #: (live reload is Phase 3).
        self.config = config
        #: The config *database* path, with its provenance. Kept for
        #: diagnostics only: nothing here reads the database after startup
        #: and no worker is given it. Renamed from `config_path` rather than
        #: merely retyped, so a bare `Path` cannot sneak back in unnoticed.
        self.resolved_config = resolved_config
        self.verbose = verbose
        #: Not a `multiprocessing.Event`. `Event.set()` blocks forever --
        #: holding the Condition's lock, which then also freezes every
        #: `is_set()` in every process sharing it -- once any worker has
        #: been killed while parked in `wait()`, which is routine here
        #: (`_shutdown_workers` escalates to SIGKILL, and `SIG_DFL`
        #: children die abruptly on SIGTERM/SIGINT). See
        #: `zfsbackup.stopflag` for the mechanism and the measurements.
        self._stop_event = StopFlag()
        self._workers: Dict[str, multiprocessing.Process] = {}
        #: Workers that crashed `WORKER_MAX_CONSECUTIVE_FAILURES` times in a
        #: row and will never be respawned. Any entry stops the daemon.
        self._crash_looped: Set[str] = set()
        #: Per-worker restart bookkeeping, all on the `time.monotonic()`
        #: clock and all touched only by the supervisor's main thread.
        #: `_restart_at[name]` exists exactly while `name` is dead and
        #: waiting out its backoff; `_started_at[name]` is when the current
        #: incarnation was forked; `_failures[name]` counts consecutive
        #: deaths (reset by a run of >= `WORKER_STABLE_UPTIME`).
        self._restart_at: Dict[str, float] = {}
        self._started_at: Dict[str, float] = {}
        self._failures: Dict[str, int] = {}
        #: Workers that survived SIGKILL and were left behind by
        #: `_shutdown_workers`. `main()` hard-exits when this is non-empty:
        #: multiprocessing's atexit handler would otherwise `join()` them
        #: with no timeout.
        self.abandoned_workers: List[str] = []
        #: Set by `_sighup_handler` (one plain store), consumed by `run()`.
        self._hup_requested = False
        #: Set by `_signal_handler` (one plain store), read and logged by
        #: `run()`. `None` means the flag was raised by `_check_workers`,
        #: not by a signal.
        self._stop_signum = None
        self._stop_reported = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGHUP, self._sighup_handler)

    def _sighup_handler(self, signum, frame):
        """Record that SIGHUP arrived and return -- nothing else.

        SIGHUP's default action terminates the process, which would orphan
        every worker. Live reload does not exist yet (Phase 3), so this is a
        stopgap: one plain store, read and logged by `run()`'s poll loop
        ("live reload not implemented; restart to apply"). Same rules as
        `_signal_handler`: no logging, no lock, and above all **no database
        access** -- `db._ENGINES_LOCK` is an RLock, so a handler that
        interrupts the main thread while it holds the lock would not
        deadlock; it would re-enter and see `_ENGINES` half-mutated.
        """
        self._hup_requested = True

    def _signal_handler(self, signum, frame):
        """Record the request and return; `run()`'s poll loop acts on it.

        Two stores, and **deliberately no logging**. Both are single stores
        into memory -- no lock, no condition variable, nothing that can be
        half-done when the handler returns to whatever it interrupted. The
        operator-facing "Received SIGTERM" line is emitted by
        `_report_stop_signal` from the poll loop instead, where taking a
        lock is legal.

        **A signal handler must not call anything that takes a lock.** A
        handler runs on the main thread at an arbitrary instruction,
        including one inside a critical section that thread already owns.
        The rule the `zfsbackup.stopflag` module docstring states for
        `Event.set()` applies verbatim to `logger.info()`, and this handler
        used to do exactly that, *before* raising the flag:

        - **Blocking I/O.** `logger.info` takes `logging._lock` and the
          handler's `Handler.lock`, then performs a blocking `write()` to
          fd 1 -- under systemd, a journald socket. A stalled journald or a
          full pipe buffer blocks that write, so `set()` is never reached.
          Measured with nothing changed but the statement order, against a
          supervisor whose stdout pipe was full:

              ORDER=log-first  ->  WORKER NEVER SAW STOP
              ORDER=set-first  ->  WORKER SAW STOP

          The daemon then ignores SIGTERM *and* every worker keeps running,
          so systemd SIGKILLs the whole cgroup at `TimeoutStopSec` --
          tearing `RemoteBackupWorker` out of a `zfs send`. Setting the
          flag first does not unblock the supervisor (it stays in the
          handler until the write drains) but it does let every worker
          reach a safe point and exit, which is the difference between a
          clean stop and a torn stream.

        - **Re-entrancy.** If a signal lands while the main thread is
          already inside `logging` -- `_spawn`'s `logger.info`,
          `_check_workers`' `logger.error` -- the handler re-enters
          `logging` on that same thread. With today's `StreamHandler` this
          only interleaves output, because `logging._lock` and
          `Handler.lock` are `RLock`s. Add a `SysLogHandler`,
          `SocketHandler` or `QueueHandler` and it becomes a hard
          self-deadlock, with zero dead workers and on every start method.

        `self._stop_event.set()` itself is a `StopFlag` store for the same
        reason at one more remove: `Event.set()` is `with self._cond:`
        around `notify_all()`, a non-recursive lock plus a primitive that
        is not async-signal-safe, so it was never legal in a handler even
        with zero dead sleepers.
        """
        self._stop_signum = signum
        self._stop_event.set()

    def _report_stop_signal(self) -> None:
        """Log the shutdown line `_signal_handler` must not log itself.

        Called from `run()` as soon as the poll loop observes the flag, so
        the message still appears exactly once and in the same place in the
        log as before -- just from a context where blocking on a journald
        socket is harmless. Silent when the flag was raised by
        `_check_workers` (a configuration fault), which logs its own
        explanation.
        """
        signum = self._stop_signum
        if signum is None or self._stop_reported:
            return
        self._stop_reported = True
        sig_name = 'SIGINT' if signum == signal.SIGINT else 'SIGTERM'
        logger.info(f"Received {sig_name}, shutting down gracefully...")

    def _new_worker(self, name: str) -> multiprocessing.Process:
        cls = {
            'snapshot': SnapshotWorker,
            'pruning': PruningWorker,
            'api': ApiWorker,
            'remote': RemoteBackupWorker,
        }[name]
        # The same object for every worker and every respawn -- never a path
        # to re-read. Under `fork` it is inherited; under `spawn` and
        # `forkserver` it is pickled, and `Duration.__reduce__` carries the
        # operator's literals across, so a respawned worker sees exactly the
        # config its siblings run. `dry_run` is already applied to it.
        return cls(self.config, self._stop_event, self.verbose)

    def _active_worker_names(self) -> list:
        names = ['snapshot', 'pruning', 'api']
        if any(ds.remote for ds in self.config.enabled_datasets):
            names.append('remote')
        return names

    def _spawn(self, name: str, verb: str = 'Started') -> multiprocessing.Process:
        """The **single fork point** for every worker, at startup and at
        every crash restart alike.

        `dispose_all()` first, always, and it is **mandatory**, not
        belt-and-braces. The supervisor read the config database before the
        first fork, and SQLAlchemy 2.0's default `QueuePool` *retains* the
        connection it used -- so without this the child would inherit a live
        SQLite file descriptor on the config database. Workers never use
        it, but an inherited descriptor is not harmless merely because it is
        unused: the child's garbage collector may finalise the unreferenced
        pooled connection, and SQLite's close of a connection it believes is
        the last one checkpoints and deletes the `-wal`/`-shm` sidecars out
        from under the parent, while its per-process inode lock bookkeeping
        no longer matches the kernel's. `store/db.py`'s pid-keyed engine
        cache alone is **not** a guarantee: it hands a child a fresh engine,
        but the inherited connection is merely unreferenced, not closed.
        Only `dispose_all()` at the fork point, in the process that owns the
        connections, closes them before the fork.

        This is a choke point on purpose. `_check_workers` re-forks on
        every worker crash, arbitrarily late in the daemon's life; an
        earlier version of this design cleaned up only in
        `_start_workers`, which left exactly that path uncovered.

        The supervisor must be quiescent here: `dispose_all()` is
        pid-guarded and cannot help a connection that is *checked out*, so
        no session may be open across this call.
        """
        dispose_all()
        worker = self._new_worker(name)
        worker.start()
        # Recorded straight after `start()`, before anything that can raise
        # (the log line): an untracked live child would never be joined or
        # terminated at shutdown.
        self._workers[name] = worker
        self._started_at[name] = self._monotonic()
        logger.info(f"{verb} {name} worker (pid={worker.pid})")
        self._restart_at.pop(name, None)
        return worker

    def _monotonic(self) -> float:
        """The supervisor's clock for restart deadlines: a named seam so
        tests can advance time without patching the process-global
        `time.monotonic` (which `_supervisor_sleep` also reads)."""
        return time.monotonic()

    def _start_workers(self) -> None:
        names = self._active_worker_names()
        for i, name in enumerate(names):
            # Re-checked before every fork, not once: a SIGTERM arriving
            # between spawns would otherwise still fork the remaining
            # workers only for them to be stopped a moment later.
            if self._stop_event.is_set():
                logger.info(
                    "Shutdown requested while starting workers; not starting: "
                    f"{', '.join(names[i:])}"
                )
                return
            self._spawn(name)

    def _record_worker_death(self, name: str, worker: multiprocessing.Process) -> bool:
        """Account for one dead worker and schedule its restart.

        Returns `True` if the worker has now died
        `WORKER_MAX_CONSECUTIVE_FAILURES` times in a row, in which case no
        restart is scheduled and the caller must stop the daemon.

        Backoff is `WORKER_RESTART_DELAY * 2**(n-1)` seconds before the n-th
        consecutive restart, capped at `WORKER_RESTART_MAX_DELAY`. A
        worker that stayed up at least `WORKER_STABLE_UPTIME` before dying
        has recovered, so its count restarts at 1 -- a worker that crashes
        once a day should never accumulate towards the limit. The delay is a
        `restart_at` deadline, not a sleep: `_check_workers` returns
        immediately and re-checks the deadline on the next poll, so a
        backoff can never hold up a shutdown.
        """
        now = self._monotonic()
        uptime = now - self._started_at.get(name, now)
        if uptime >= WORKER_STABLE_UPTIME:
            self._failures[name] = 0
        failures = self._failures.get(name, 0) + 1
        self._failures[name] = failures

        if failures >= WORKER_MAX_CONSECUTIVE_FAILURES:
            logger.error(
                f"{name} worker (pid={worker.pid}) exited with code "
                f"{worker.exitcode} for the {failures}th consecutive time "
                f"(it never stayed up {WORKER_STABLE_UPTIME}s); giving up "
                "instead of restarting it forever."
            )
            return True

        delay = min(WORKER_RESTART_DELAY * 2 ** (failures - 1), WORKER_RESTART_MAX_DELAY)
        self._restart_at[name] = now + delay
        logger.error(
            f"{name} worker (pid={worker.pid}) exited with code "
            f"{worker.exitcode} after {uptime:.0f}s (consecutive failure "
            f"{failures}/{WORKER_MAX_CONSECUTIVE_FAILURES}), restarting in {delay}s"
        )
        return False

    def _check_workers(self) -> None:
        """Restart workers that died -- except a worker that keeps crashing,
        which stops the whole daemon.

        Never sleeps. A dead worker gets a `restart_at` deadline (see
        `_record_worker_death`) and is forked again by whichever later poll
        finds the deadline passed, so one call takes microseconds however
        many workers are down, and the stop flag is honoured at the next
        `_supervisor_sleep` slice.

        A restart hands the replacement the supervisor's own `self.config`
        (see `_new_worker`); it does not re-read the config database, so a
        crash can never move one worker onto a different generation than its
        siblings. There is no configuration failure left to classify here:
        a worker never opens the database, so a bad one is refused by
        `_run` before any worker exists, and the `EX_CONFIG` exit a worker
        used to report for it is gone. A worker that dies is restarted with
        exponential backoff and escalated to the fatal treatment below after
        `WORKER_MAX_CONSECUTIVE_FAILURES` consecutive deaths.

        **One crash-looping worker is fatal to the daemon, not just to that
        worker.** A daemon that keeps three of four workers alive after the
        fourth gave up -- say, taking no snapshots while the API keeps
        answering and pruning keeps deleting, with one ERROR line long since
        scrolled away -- is partial degradation with no liveness surface,
        which is worse for a backup daemon than an exit the service manager
        can see, restart, and eventually report as failed.
        """
        # Two passes on purpose: classify every dead worker first, so that
        # a worker that has given up is known before anything is respawned.
        # A single pass would fork a replacement for an unrelated crash and
        # then immediately shut it down.
        #
        # Workers already waiting out a backoff are skipped: they are still
        # in `_workers` (so `_shutdown_workers` reaps them) but have been
        # accounted for.
        dead = [
            (name, w) for name, w in self._workers.items()
            if name not in self._restart_at and not w.is_alive()
        ]

        for name, worker in dead:
            if self._record_worker_death(name, worker):
                self._crash_looped.add(name)
                del self._workers[name]

        if not self._crash_looped:
            now = self._monotonic()
            due = [n for n, at in sorted(self._restart_at.items(), key=lambda kv: kv[1]) if at <= now]
            for i, name in enumerate(due):
                # Forking into a shutdown is never right.
                if self._stop_event.is_set():
                    logger.info(
                        "Shutdown requested; not restarting remaining "
                        f"worker(s): {', '.join(due[i:])}"
                    )
                    return
                self._spawn(name, verb='Restarted')

        if self._crash_looped:
            logger.error(
                "Worker(s) crashed repeatedly and were not restarted: "
                f"{', '.join(sorted(self._crash_looped))}. The daemon "
                "cannot run degraded -- shutting down the remaining "
                f"workers ({', '.join(sorted(self._workers)) or 'none'}) "
                "and exiting non-zero. See the worker errors above."
            )
            # A visible, non-zero exit instead of a spin: this is precisely
            # the "workers have died" case `StopFlag` (see
            # `zfsbackup.stopflag`) was built for, and `set()` cannot wedge
            # on a ghost sleeper the way `multiprocessing.Event` could. If
            # this is ever reverted to `Event`, that guarantee is lost.
            self._stop_event.set()

    def _shutdown_workers(self, timeout: int = 30) -> None:
        """Join every worker, escalating to SIGTERM then SIGKILL.

        `timeout` is a **single budget shared by all workers**, not a
        per-worker one: the deadline is computed once and each join gets
        whatever is left of it. The joins are sequential, so a per-worker
        30s meant four workers stuck in a long `zfs send` -- the case the
        30s exists for -- could cost 4 x (30 + 5 + 5) = 160s *after* the
        poll loop exited, against systemd's default `TimeoutStopSec=90s`.
        An operator sizing `TimeoutStopSec` from the documented bound got
        the cgroup SIGKILLed mid-stream, which is exactly what the
        escalation below is here to prevent. Shared, the graceful phase is
        capped at `timeout` in total and the escalation adds at most 10s
        per stubborn worker; see `SUPERVISOR_SLEEP_SLICE` for the
        end-to-end sum.
        """
        deadline = time.monotonic() + timeout
        for name, worker in self._workers.items():
            worker.join(timeout=max(0.0, deadline - time.monotonic()))
            if not worker.is_alive():
                continue
            logger.warning(
                f"{name} worker did not exit within the {timeout}s shutdown "
                f"budget, terminating"
            )
            worker.terminate()
            worker.join(timeout=5)
            if not worker.is_alive():
                continue
            # SIGKILL backstop. `terminate()` sends SIGTERM, which a child
            # that has a SIGTERM *handler* installed does not have to obey
            # -- and under `fork` a child inherited this supervisor's own
            # handler until `workers._init_child_process` started resetting
            # it to SIG_DFL (measured before that fix: `alive after
            # terminate+join(5): True exitcode None`). This loop used to
            # fall through silently at that point, leaving a live child and
            # no log line; the interpreter's own exit handler then called
            # `join()` with NO timeout, so `systemctl stop` hung until
            # TimeoutStopSec and systemd SIGKILLed the whole cgroup --
            # possibly mid-`zfs send`. Escalating here keeps that decision
            # ours, and keeps it logged.
            logger.error(
                f"{name} worker (pid={worker.pid}) ignored SIGTERM; sending SIGKILL"
            )
            worker.kill()
            worker.join(timeout=5)
            if worker.is_alive():
                logger.error(
                    f"{name} worker (pid={worker.pid}) survived SIGKILL "
                    "(uninterruptible sleep?); abandoning it"
                )
                self.abandoned_workers.append(name)

    def _supervisor_sleep(self, seconds: float) -> None:
        """Sleep up to `seconds`, returning as soon as the stop flag is set.

        Called exactly once per poll iteration, in place of a bare
        `time.sleep(SUPERVISOR_POLL)`.

        A single `time.sleep(5)` is **not** interruptible by a signal:
        per PEP 475 it is retried with the time remaining once the handler
        returns (measured: handler at 0.50s, `time.sleep(5.0)` still
        returned at 5.01s). So `_signal_handler` setting the flag promptly
        bought nothing -- the supervisor still took up to `SUPERVISOR_POLL`
        seconds to reach `_shutdown_workers`, every time, on the healthy
        path. Slicing the sleep bounds that by `SUPERVISOR_SLEEP_SLICE`
        instead.

        This is a named method rather than an inlined loop so that tests
        have one stable seam to patch: patching `zfsbackup.daemon.time.sleep`
        would now see an *unbounded* number of calls per iteration instead
        of one -- a patched `time.sleep` does not sleep, so the loop below
        is bounded only by real `time.monotonic()` and spins a CPU for the
        whole five seconds. (`_check_workers` no longer sleeps at all; its
        restart backoff is a deadline on `_monotonic()`.)

        Why this and not `self._stop_event.wait(seconds)`, which it
        otherwise duplicates: only the return value and the slice size
        differ, and the slice difference is real but narrow.
        `stopflag.STOP_POLL` (0.5s) governs how fast a *worker* notices the
        flag, and that is what bounds the daemon's healthy-path shutdown --
        the supervisor cannot finish until the workers exit, and the
        measured 0.265-0.554s tails are a worker's slice, not this one.
        `SUPERVISOR_SLEEP_SLICE` (0.25s) governs only how fast the
        supervisor leaves its idle sleep, where the finer slice buys
        promptness on the *restart* path (the flag can also be raised by
        `_check_workers`, with no worker involved) at negligible cost: 20
        wakeups per idle poll in one process. If the two ever need to move
        together, collapse them -- they are not independent knobs, just
        deliberately different ones.
        """
        deadline = time.monotonic() + seconds
        while not self._stop_event.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(SUPERVISOR_SLEEP_SLICE, remaining))

    def run(self) -> int:
        """Run the supervisor until the stop event is set. Returns the
        daemon's process exit code: 0 for a clean shutdown, 1 if **any**
        worker crash-looped -- in which case `_check_workers` has already
        stopped the whole daemon rather than let it run degraded. Returning
        a code at all, rather than always 0, is what lets a service manager
        see that a daemon that gave up is not a successful run.
        """
        logger.info("=" * 60)
        logger.info("ZFS Backup Daemon Starting")
        logger.info("=" * 60)

        manager = DatasetManager(self.config)
        manager.dataset_report()
        manager.verify_datasets()

        logger.info("Syncing dataset configs to ZFS properties...")
        manager.sync_all_config_properties()

        # Everything above this line shells out to `zfs` once per dataset
        # and takes tens of seconds on a large config, so a SIGTERM landing
        # in it is ordinary, not exotic. Without this guard `run()` forked
        # every worker anyway, only for the loop guard below to be false on
        # its first evaluation. Forking into a shutdown is never right
        # (`_check_workers` says the same thing about the restart path, and
        # guards it the same way). Structured as a
        # branch rather than an early return so that the shutdown tail
        # below stays the single exit: `_shutdown_workers` over an empty
        # `_workers` is a no-op, and the log ends with "Daemon stopped"
        # either way.
        #
        # The whole of it sits in a try/finally: an exception escaping the
        # poll loop (`worker.start()` failing with EAGAIN under a tight
        # TasksMax, say) used to skip `_shutdown_workers` entirely, and
        # multiprocessing's exit handler then SIGTERMed workers with the
        # default disposition mid-`zfs send`. The `finally` raises the stop
        # flag first, so workers can reach a safe point on their own, then
        # runs the usual escalating join; the exception still propagates to
        # `_run`, which logs it and exits 1.
        try:
            if self._stop_event.is_set():
                # Idempotent, and called here too only so the "Received SIGTERM"
                # line precedes its consequence in the log.
                self._report_stop_signal()
                logger.info("Shutdown requested during startup; no workers started")
            else:
                logger.info("=" * 60)
                logger.info("Starting workers...")
                logger.info("=" * 60)

                self._start_workers()

                while not self._stop_event.is_set():
                    self._supervisor_sleep(SUPERVISOR_POLL)
                    self._report_hup()
                    if not self._stop_event.is_set():
                        self._check_workers()
        finally:
            self._stop_event.set()
            # The line `_signal_handler` deliberately does not log itself. Here
            # it is an ordinary call on the main thread, not a handler, so
            # blocking on a journald socket is merely slow instead of fatal.
            self._report_stop_signal()
            logger.info("Shutting down workers...")
            self._shutdown_workers()
        logger.info("Daemon stopped")
        return 1 if self._crash_looped else 0

    def _report_hup(self) -> None:
        """Log the SIGHUP stopgap message `_sighup_handler` must not log itself."""
        if self._hup_requested:
            self._hup_requested = False
            logger.warning("Received SIGHUP: live reload not implemented; restart to apply")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='ZFS Backup Daemon - Automated snapshot management with retention policies'
    )
    # Deliberately `default=None` and deliberately NOT `type=Path`.
    #
    # `default=None` is what makes step 2 of `resolve_config_path`'s order
    # (`ZFSBACKUP_CONFIG`) reachable at all: with any non-None default,
    # `explicit` is never None and the environment variable becomes dead
    # code. The old `default=Path('/etc/zfsbackup/config.yaml')` was
    # exactly that shape.
    #
    # No `type=Path` because `str(Path("")) == "."`: `-c ""` would arrive
    # as `PosixPath('.')` and `resolve_config_path`'s documented
    # "empty means not given" fallthrough would silently become "use the
    # current directory", failing later with a confusing message about a
    # directory the operator never named. Pass the raw string through.
    parser.add_argument(
        '-c', '--config',
        default=None,
        help=(
            'Path to the config DATABASE (SQLite). Defaults to '
            f'${CONFIG_PATH_ENV} if set, otherwise {DEFAULT_CONFIG_DB}. '
            'YAML is no longer a config source: create a database from an '
            'existing YAML config with `zfsbackup-config import <path.yaml>`.'
        )
    )
    parser.add_argument(
        '-d', '--dry-run',
        action='store_true',
        help='Dry run mode - do not create or destroy snapshots'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose (debug) logging'
    )
    parser.add_argument(
        '--test-config',
        action='store_true',
        help='Test configuration and exit'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    abandoned: List[str] = []
    try:
        code = _run(args, abandoned)
    finally:
        # Every return path -- `--test-config`, a config refusal, a
        # supervisor crash, a clean shutdown -- drops this process's
        # engines before returning, so the last connection to the config
        # database closes here rather than whenever the interpreter
        # happens to finalise a pooled connection (a CPython detail this
        # module does not control, and one that differs between 3.11 and
        # 3.14).
        #
        # That determinism is the point, and the hazard is concrete:
        # SQLite deletes `-wal`/`-shm` when the LAST connection closes
        # cleanly, and `paths.py`'s step 6 documents that a sidecar is
        # owned by whichever process created it and is never re-chmod'd
        # afterwards. A `--test-config` run as root that leaves sidecars
        # behind is exactly the lockout `_stuck_sidecars_message` has to
        # refuse later -- root-owned sidecars next to a database the
        # zfsbackup user is supposed to open.
        #
        # This is also the same discipline `_spawn` applies at every fork
        # point, now applied at every process exit.
        dispose_all()

    if abandoned:
        # A worker survived SIGKILL (uninterruptible kernel sleep, typically
        # a hung `zfs` call). Returning normally would hand control to
        # multiprocessing's atexit handler, which `join()`s every child with
        # NO timeout and so hangs until systemd's `TimeoutStopSec` SIGKILLs
        # the cgroup. The orderly shutdown has already happened, so leave
        # right now -- after `dispose_all()` above, and after flushing the
        # logs that `os._exit` would otherwise discard.
        _hard_exit(code, abandoned)
    return code


def _hard_exit(code: int, abandoned: List[str]) -> None:  # pragma: no cover - exits
    """Flush logging and leave without running atexit handlers."""
    logger.error(
        f"Exiting without waiting for abandoned worker(s): {', '.join(abandoned)}"
    )
    logging.shutdown()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:
            pass
    # Workers were abandoned, so this was not a clean stop: never report 0.
    os._exit(code or 1)


def _release_refused(lock) -> None:
    """A startup refusal after the presence lock was taken (the config failed
    to load): remove the pid file and drop the lock, so a refused start leaves
    nothing behind and does not look like a running daemon to `probe()`."""
    if lock is not None:
        lock.release()


class _StartupHup:
    """A flag-only SIGHUP handler for the window before `BackupDaemon` exists.

    The presence lock is taken at the top of `_run`, i.e. the daemon is
    already visible to `probe()` -- and so to `zfsbackup-config`'s
    auto-reload SIGHUP -- long before `BackupDaemon.__init__` installs the
    real handler. In that window SIGHUP has its default action, terminate,
    so a config edit racing a daemon start killed the starting daemon
    (measured 8/8). This handler does one plain store and returns; `_run`
    carries the result into the daemon's own `_hup_requested`, so the first
    poll logs it exactly as it would a SIGHUP received later.

    Deliberately SIGHUP only. SIGINT/SIGTERM keep their default
    dispositions until `BackupDaemon.__init__`: before the first fork there
    are no workers to orphan, the kernel drops the lock, a stale pid file is
    harmless, and a startup stuck in slow I/O must stay killable. After the
    daemon exists the three signals are all handled by `BackupDaemon`.
    """

    def __init__(self) -> None:
        self.pending = False
        #: Set once `BackupDaemon` owns the signals; `restore` is then a no-op.
        self.handed_over = False
        self._previous = None
        self._installed = False

    def _handler(self, signum, frame) -> None:
        self.pending = True

    def install(self) -> None:
        try:
            self._previous = signal.signal(signal.SIGHUP, self._handler)
            self._installed = True
        except ValueError:  # not the main thread (embedded/test use)
            pass

    def restore(self) -> None:
        """Put back the previous disposition on a path that never created a
        daemon (refusal, `--test-config`); never after hand-over."""
        if self._installed and not self.handed_over:
            self._installed = False
            signal.signal(signal.SIGHUP, self._previous)


def _run(args: argparse.Namespace, abandoned: Optional[List[str]] = None) -> int:
    """Install the startup SIGHUP handler (see `_StartupHup`), then
    `_run_startup`. Installed before anything else -- in particular before the
    presence lock -- and left in place until `BackupDaemon` replaces it.
    """
    early_hup = _StartupHup()
    if not args.test_config:  # `--test-config` takes no lock and is never probed
        early_hup.install()
    try:
        return _run_startup(args, abandoned, early_hup)
    finally:
        # Drop the engines BEFORE SIGHUP gets its default action (terminate)
        # back: a refusal path still holds a pooled connection here, and an
        # auto-reload SIGHUP landing between a restore and `main()`'s own
        # `dispose_all()` would kill the process with it open. Idempotent,
        # so `main()` repeating it is harmless.
        dispose_all()
        early_hup.restore()


def _run_startup(
    args: argparse.Namespace,
    abandoned: Optional[List[str]],
    early_hup: _StartupHup,
) -> int:
    """`main()`'s body, minus argument parsing and the `dispose_all()` that
    must run on every return path. Split out so that cleanup is a single
    `finally` rather than something each `return` has to remember.

    Startup order: resolve the path -> preflight (`check_config_db`, creates
    nothing) -> take the presence lock -> read the config -> record its
    generation in the lock -> `BackupDaemon.run`. `--test-config` skips the
    lock. Any refusal after the lock was taken releases it.
    """
    resolved = None
    lock = None
    try:
        # Inside the `try`: resolution itself performs filesystem I/O
        # (`Path.resolve()` walks each component) and can raise
        # `ConfigDbPermissionError` on a symlink loop or an unlinked cwd.
        resolved = resolve_config_path(args.config)
        logger.info(f"Loading configuration from: {describe_config_source(resolved)}")

        # Presence lock (item 15.2(c)), taken BEFORE the config is read.
        # Order: resolve path -> preflight -> acquire -> read -> record the
        # generation. Were the read first, a `zfsbackup-config` write plus
        # its `probe()` landing between the read and the lock would print
        # "no running daemon detected" while this daemon went on to run the
        # OLD generation. With the lock first, any probe after this point
        # finds a daemon, and a write that lands before the read is simply
        # the generation this daemon loads.
        #
        # The preflight (`check_config_db`, stat-based, builds no engine and
        # creates nothing) comes first so a missing database or directory is
        # refused with the store's own message, never by creating a lock file
        # beside a database that does not exist. Not taken for
        # `--test-config`, which must work beside a running daemon. A second
        # daemon on the same database stops here. The descriptor is held for
        # the life of the process via `lock`; workers inherit it under
        # `fork` (see `zfsbackup.daemonlock`).
        if not args.test_config:
            check_config_db(resolved)
            try:
                lock = daemonlock.acquire(resolved)
            except daemonlock.DaemonAlreadyRunning as e:
                logger.error(f"{e}. Refusing to start a second daemon.")
                return 1
            except daemonlock.DaemonLockError as e:
                logger.error(f"Cannot take the daemon lock: {e}")
                return 1

        # The daemon's ONE read of the config database: a single read
        # snapshot in which the schema check and the rows are the same
        # database state. The supervisor hands the resulting detached
        # `BackupConfig` to its workers, which never open the database
        # (item 15.2(b)).
        snapshot = load_runtime_config(resolved)
        config = snapshot.config
        generation = snapshot.generation
        logger.info(
            f"Configuration generation {generation}, "
            f"schema revision {snapshot.schema_rev}"
        )
        # The generation recorded in the lock is the one the workers are
        # about to run, which is what `zfsbackup-config` reports to an
        # operator after an edit. Until this line the file's generation is
        # empty ("not known yet").
        if lock is not None:
            lock.set_generation(generation)
        # Nothing reads the database again for the life of this process
        # (live reload is Phase 3), so do not sit on its pooled reader
        # connection through the long dataset verification below, nor carry
        # it towards the first fork. `_spawn` repeats this at every fork.
        dispose_all()

        # In memory only, and BEFORE the config is handed to any worker.
        # `--dry-run` is a property of this run, not of the stored
        # configuration, and the daemon never writes to the database.
        if args.dry_run:
            config.dry_run = True

        if args.test_config:
            logger.info("Configuration is valid!")
            # Proof that the schema was actually verified, not merely that
            # some rows could be read: `schema_rev` is what the check inside
            # the snapshot returned. Nothing was migrated or created.
            logger.info(f"Schema revision: {snapshot.schema_rev}")
            logger.info(f"Datasets: {len(config.datasets)}")
            for ds in config.datasets:
                logger.info(f"  - {ds.name} (enabled={ds.enabled})")
            if config.destinations:
                logger.info(f"Destinations: {list(config.destinations)}")
            if config.remote_backup:
                logger.info(f"Remote backup target: {config.remote_backup.target_dataset}")
            return 0

    except (ConfigPathError, SchemaError) as e:
        # Logged VERBATIM and with no traceback. These messages are already
        # operator-grade -- they name the file, the candidate sources, and
        # in `ConfigDbIsYaml`'s case a copy-pasteable
        # `zfsbackup-config import <path>`. A "Failed to load
        # configuration: " prefix would wrap a multi-line message whose
        # last line is meant to be pasted into a shell.
        logger.error(config_error_message(resolved, e))
        _release_refused(lock)
        return 1
    except Exception as e:
        # Also `config_error_message`, not a bare f-string, and this arm is
        # the only way an `OperationalError` is diagnosed at all: the
        # preflight can pass and the real `engine.connect()` still fail,
        # routinely, because the daemon runs as root and `os.access` is
        # uid-based, permissive for root, and blind to POSIX ACLs. A
        # directory ACL, SELinux, or a read-only bind mount lands here.
        # Without this, the operator got "(sqlite3.OperationalError) unable
        # to open database file" and a SQLAlchemy documentation link;
        # `config_error_message` re-runs the preflight via
        # `diagnose_open_failure` and names the actual problem.
        # `exc_info` for everything that is not a database failure (already
        # diagnosed, with its cause, by `config_error_message`; a plain
        # `DatabaseError` is a damaged file and gets its own one-liner): a
        # bare `KeyError` or `ValueError` from the mapper is otherwise
        # undiagnosable from the log.
        logger.error(
            config_error_message(resolved, e),
            exc_info=not (isinstance(e, OperationalError) or type(e) is DatabaseError),
        )
        _release_refused(lock)
        return 1

    daemon = None
    clean = False
    try:
        daemon = BackupDaemon(config, resolved, verbose=args.verbose)
        # `BackupDaemon.__init__` has replaced the startup handler; a SIGHUP
        # that landed before that is carried over. A SIGHUP landing between
        # the replacement and this line went to the daemon's own handler.
        early_hup.handed_over = True
        # Only ever SET the flag: a read-modify-write here could overwrite a
        # SIGHUP that the daemon's handler recorded between the read and the
        # store.
        if early_hup.pending:
            daemon._hup_requested = True
        code = daemon.run()
        clean = True
        return code
    except Exception as e:
        logger.error(f"Daemon failed: {e}", exc_info=True)
        return 1
    finally:
        # Reported on every path out, including a crashed `run()`: its
        # `finally` has already run `_shutdown_workers`, which is what
        # records the abandoned ones.
        if abandoned is not None and daemon is not None:
            abandoned.extend(daemon.abandoned_workers)
        # Remove the pid file only on a clean stop with no abandoned
        # workers. After a crash, or when workers survived SIGKILL (the
        # `_hard_exit` path -- under fork they still hold the inherited
        # lock, correctly: they are still running), leave the file: the
        # kernel drops our lock on exit, and a stale file is harmless
        # because "running" means "lock held", never "file exists".
        if clean and not (daemon is not None and daemon.abandoned_workers):
            lock.release()


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main())

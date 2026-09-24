#!/usr/bin/env python3
"""ZFS Backup Daemon - Main entry point."""

import argparse
import logging
import multiprocessing
import signal
import sys
import time
from typing import Dict, Set

from zfsbackup.config import BackupConfig
from zfsbackup.config.store import (
    CONFIG_PATH_ENV,
    DEFAULT_CONFIG_DB,
    ConfigPathError,
    ResolvedConfigPath,
    SchemaError,
    dispose_all,
    resolve_config_path,
)
from zfsbackup.runtime_config import (
    EX_CONFIG,
    check_config_schema,
    config_error_message,
    describe_config_source,
    load_runtime_config,
)
from zfsbackup.backup_manager import DatasetManager
from zfsbackup.stopflag import StopFlag
from zfsbackup.workers import SnapshotWorker, PruningWorker, ApiWorker, RemoteBackupWorker


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

WORKER_RESTART_DELAY = 5
SUPERVISOR_POLL = 5
#: Longest slice `_supervisor_sleep` sleeps for before re-reading the stop
#: flag. This bounds one thing only: how long the supervisor stays in its
#: idle sleep after the flag is raised. It is **not** the daemon's
#: signal-to-shutdown latency, and `TimeoutStopSec` must not be sized from
#: it. The real worst case is the sum of three terms:
#:
#:   1. this slice, 0.25s, if the signal lands in the idle sleep;
#:   2. `WORKER_RESTART_DELAY`, 5s, if it lands in `_check_workers`'
#:      deliberately un-sliced `time.sleep` (PEP 475 retries that sleep
#:      with the time remaining -- a SIGTERM one second in still costs the
#:      full 5s; see the comment there for why it stays un-sliced);
#:   3. `_shutdown_workers`' escalation, up to `timeout` + 5s terminate-join
#:      + 5s kill-join per worker that will not exit. The 30s budget is now
#:      a single deadline shared by all workers rather than per-worker, so
#:      four workers stuck in a long `zfs send` cost up to 30 + 4x10 = 70s
#:      there, not 4x40 = 160s.
#:
#: So roughly 0.25 + 5 + 70 ~= 75s worst case, under systemd's default
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
        self.config = config
        #: The config *database* path, with its provenance -- not a YAML
        #: path, and deliberately not the loaded `BackupConfig`. Each
        #: worker re-loads from this in its own child process; see
        #: `_spawn` for why nothing that owns a SQLite handle may cross a
        #: fork. Renamed from `config_path` rather than merely retyped:
        #: a silent `Path` -> `ResolvedConfigPath` swap under the old name
        #: is exactly how a bare `Path` (whose `.url()`/`resolved_path`
        #: this code needs) sneaks back in unnoticed.
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
        #: Workers that exited `EX_CONFIG` and will never be respawned.
        self._config_failed: Set[str] = set()
        #: Set by `_signal_handler` (one plain store), read and logged by
        #: `run()`. `None` means the flag was raised by `_check_workers`,
        #: not by a signal.
        self._stop_signum = None
        self._stop_reported = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

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
        return cls(self.resolved_config, self._stop_event, self.config.dry_run, self.verbose)

    def _active_worker_names(self) -> list:
        names = ['snapshot', 'pruning', 'api']
        if any(ds.remote for ds in self.config.enabled_datasets):
            names.append('remote')
        return names

    def _spawn(self, name: str, verb: str = 'Started') -> multiprocessing.Process:
        """The **single fork point** for every worker, at startup and at
        every crash restart alike.

        `dispose_all()` first, always. By the time this runs, `main()` has
        already read the config database, and SQLAlchemy 2.0's default
        `QueuePool` *retains* the connection it used -- so without this the
        child would inherit a live SQLite file descriptor on the config
        database. An inherited descriptor corrupts SQLite silently rather
        than raising; `store/db.py`'s pid-keyed engine cache is the real
        guarantee (a child asking for an engine gets a fresh one), and
        `dispose_all()` is what stops a child ever *holding* the inherited
        fd in the first place.

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
        logger.info(f"{verb} {name} worker (pid={worker.pid})")
        self._workers[name] = worker
        return worker

    def _start_workers(self) -> None:
        for name in self._active_worker_names():
            self._spawn(name)

    def _check_workers(self) -> None:
        """Restart workers that died -- except on a configuration fault,
        which no restart can fix and which stops the whole daemon.

        A worker that exits `EX_CONFIG` has already logged the operator
        message (`workers._load_config`, via the same
        `config_error_message` this supervisor would use). Respawning it
        would re-open SQLite every `WORKER_RESTART_DELAY` seconds forever
        and turn a stale schema into what looks like a hang. Every other
        non-zero exit -- including a transient `database is locked` after
        `busy_timeout` expired, which surfaces as an `OperationalError` and
        never as a `SchemaError` -- stays restartable exactly as before.

        **One `EX_CONFIG` exit is fatal to the daemon, not just to that
        worker.** An earlier version required *every* worker to have failed
        before stopping, which made the common case silent and inverted the
        point of the guard: a daemon running at schema head A, an operator
        importing after a package upgrade so the database moves to head B,
        and then the `snapshot` worker dying for some unrelated reason
        hours later. Its respawn verifies the schema (correctly) and exits
        `EX_CONFIG`; `pruning`, `api` and `remote` are still alive, so the
        supervisor stayed up forever -- **taking no snapshots**, while the
        API kept answering and the pruning worker kept deleting, with one
        ERROR line long since scrolled away. The fork storm this guard
        replaced was at least loud. Partial degradation with no liveness
        surface is worse for a backup daemon than an exit the service
        manager can see, restart, and eventually report as failed.
        """
        # Two passes on purpose: classify every dead worker first, so that
        # a configuration failure is known before anything is respawned. A
        # single pass would fork a replacement for an unrelated crash and
        # then immediately kill it -- one more pointless open of the config
        # database, at the exact moment the database is the problem.
        dead = [(name, w) for name, w in self._workers.items() if not w.is_alive()]

        for name, worker in dead:
            if worker.exitcode == EX_CONFIG:
                logger.error(
                    f"{name} worker (pid={worker.pid}) exited with "
                    f"{EX_CONFIG} (EX_CONFIG): the configuration database is "
                    f"unusable, so restarting it cannot help."
                )
                self._config_failed.add(name)
                del self._workers[name]

        if not self._config_failed:
            for i, (name, worker) in enumerate(dead):
                # Re-check the stop flag on both sides of the delay.
                # `time.sleep` does NOT abort when a signal handler runs:
                # per PEP 475 it is retried with the remaining time and
                # resumes once the handler returns. With three workers
                # crashed in one pass -- a large concurrent import, say --
                # this loop is 15s of sleep-then-fork, and a SIGTERM
                # arriving one second in would otherwise still fork all
                # three, each doing `dispose_all()`, a schema check and a
                # config load before dying at the loop guard a moment
                # later. Forking into a shutdown is never right, and
                # forking to re-open the database is the worst version of
                # it.
                if self._stop_event.is_set():
                    logger.info(
                        "Shutdown requested; not restarting remaining "
                        f"worker(s): {', '.join(n for n, _ in dead[i:])}"
                    )
                    return
                logger.error(
                    f"{name} worker (pid={worker.pid}) exited with code "
                    f"{worker.exitcode}, restarting in {WORKER_RESTART_DELAY}s"
                )
                time.sleep(WORKER_RESTART_DELAY)
                if self._stop_event.is_set():
                    logger.info(
                        f"Shutdown requested during the restart delay; "
                        f"not restarting {name} worker"
                    )
                    return
                self._spawn(name, verb='Restarted')

        if self._config_failed:
            logger.error(
                "Configuration error in worker(s): "
                f"{', '.join(sorted(self._config_failed))}. The daemon "
                "cannot run degraded -- shutting down the remaining "
                f"workers ({', '.join(sorted(self._workers)) or 'none'}) "
                "and exiting non-zero. Fix the configuration and restart."
            )
            # 8i's promise -- that a configuration fault produces a visible,
            # non-zero exit instead of a spin -- now holds unconditionally
            # here: this is precisely the "workers have died" case `StopFlag`
            # (see `zfsbackup.stopflag`) was built for, and `set()` cannot
            # wedge on a ghost sleeper the way `multiprocessing.Event` could.
            # If this is ever reverted to `Event`, that guarantee is lost.
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
        whole five seconds -- and `time.sleep(WORKER_RESTART_DELAY)` in
        `_check_workers` is deliberately left raw so that the bare patches
        of it keep counting what they mean to count.

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
        daemon's process exit code: 0 for a clean shutdown in which no
        worker ever hit a configuration fault, 1 if **any** worker exited
        `EX_CONFIG` -- in which case `_check_workers` has already stopped
        the whole daemon rather than let it run degraded. Returning a code
        at all, rather than always 0, is what lets a service manager see
        that a stale schema is not a successful run.
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
        # every worker anyway -- each doing `dispose_all()`, a schema check
        # and a full config load -- only for the loop guard below to be
        # false on its first evaluation. Forking into a shutdown is never
        # right (`_check_workers` says the same thing about the restart
        # path, and guards it the same way), and forking to re-open the
        # config database is the worst version of it. Structured as a
        # branch rather than an early return so that the shutdown tail
        # below stays the single exit: `_shutdown_workers` over an empty
        # `_workers` is a no-op, and the log ends with "Daemon stopped"
        # either way.
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
                if not self._stop_event.is_set():
                    self._check_workers()

        # The line `_signal_handler` deliberately does not log itself. Here
        # it is an ordinary call on the main thread, not a handler, so
        # blocking on a journald socket is merely slow instead of fatal.
        self._report_stop_signal()
        logger.info("Shutting down workers...")
        self._shutdown_workers()
        logger.info("Daemon stopped")
        return 1 if self._config_failed else 0


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

    try:
        return _run(args)
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


def _run(args: argparse.Namespace) -> int:
    """`main()`'s body, minus argument parsing and the `dispose_all()` that
    must run on every return path. Split out so that cleanup is a single
    `finally` rather than something each `return` has to remember.
    """
    resolved = None
    try:
        # Inside the `try`: resolution itself performs filesystem I/O
        # (`Path.resolve()` walks each component) and can raise
        # `ConfigDbPermissionError` on a symlink loop or an unlinked cwd.
        resolved = resolve_config_path(args.config)
        logger.info(f"Loading configuration from: {describe_config_source(resolved)}")
        config = load_runtime_config(resolved)

        # In memory only. `--dry-run` is a property of this run, not of the
        # stored configuration, and the daemon never writes to the database.
        if args.dry_run:
            config.dry_run = True

        if args.test_config:
            logger.info("Configuration is valid!")
            # Proof that the schema was actually verified, not merely that
            # some rows could be read. `check_config_schema` opens its own
            # short-lived read-only connection -- it never migrates, and
            # never creates anything.
            logger.info(f"Schema revision: {check_config_schema(resolved)}")
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
        # `diagnose_open_failure` and names the actual problem. It also
        # makes the supervisor's diagnosis word-for-word identical to the
        # child's, which `workers._load_config` already produced this way.
        logger.error(config_error_message(resolved, e))
        return 1

    try:
        daemon = BackupDaemon(config, resolved, verbose=args.verbose)
        return daemon.run()
    except Exception as e:
        logger.error(f"Daemon failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main())

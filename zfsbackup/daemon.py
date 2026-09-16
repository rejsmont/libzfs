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
from zfsbackup.workers import SnapshotWorker, PruningWorker, ApiWorker, RemoteBackupWorker


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

WORKER_RESTART_DELAY = 5
SUPERVISOR_POLL = 5


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
        self._stop_event = multiprocessing.Event()
        self._workers: Dict[str, multiprocessing.Process] = {}
        #: Workers that exited `EX_CONFIG` and will never be respawned.
        self._config_failed: Set[str] = set()
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        sig_name = 'SIGINT' if signum == signal.SIGINT else 'SIGTERM'
        logger.info(f"Received {sig_name}, shutting down gracefully...")
        self._stop_event.set()

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
            # DEPENDENCY, stated rather than left implicit: 8i's promise --
            # that a configuration fault produces a visible, non-zero exit
            # instead of a spin -- holds only until an abruptly-killed
            # worker has left a ghost sleeper on this Event, and is fully
            # restored only by item 8x replacing the Event with a flag the
            # poll loop transfers.
            #
            # `multiprocessing.Event.set()` calls `Condition.notify_all()`,
            # which blocks on `_woken_count` waiting for each registered
            # sleeper to answer. A worker SIGKILLed (OOM killer) or
            # segfaulted while parked in `stop_event.wait()` is registered
            # and will never answer. This `set()` then blocks **holding the
            # Condition's lock**, measured >6s and unbounded on all three
            # start methods:
            #
            #     [fork]       worker exitcode=-9  set() BLOCKED, wedged in notify_all
            #     [spawn]      worker exitcode=-9  set() BLOCKED, wedged in notify_all
            #     [forkserver] worker exitcode=-9  set() BLOCKED, wedged in notify_all
            #
            # and a subsequent `systemctl stop` makes it worse, not better:
            # `_signal_handler` runs on this same blocked thread and
            # re-enters `with self._cond` on a non-recursive lock it
            # already holds. Only SIGKILL recovers. The ERROR above is
            # emitted first, so the operator sees the cause and then a hang
            # -- the exact inverse of the fork storm 8i replaced.
            #
            # No signal is needed to reach it: one abruptly-killed sleeper
            # at any earlier time, plus one later `EX_CONFIG` exit. The
            # pure-`EX_CONFIG` case is safe on its own, because a worker
            # that fails in `_load_config` exits before it ever reaches
            # `stop_event.wait()` and so never registers as a sleeper.
            self._stop_event.set()

    def _shutdown_workers(self, timeout: int = 30) -> None:
        for name, worker in self._workers.items():
            worker.join(timeout=timeout)
            if not worker.is_alive():
                continue
            logger.warning(f"{name} worker did not exit in {timeout}s, terminating")
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

        logger.info("=" * 60)
        logger.info("Starting workers...")
        logger.info("=" * 60)

        self._start_workers()

        while not self._stop_event.is_set():
            time.sleep(SUPERVISOR_POLL)
            if not self._stop_event.is_set():
                self._check_workers()

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

#!/usr/bin/env python3
"""ZFS Backup Daemon - Main entry point."""

import signal

# FIRST, before any other import: in the daemon process itself (`python -m
# zfsbackup.daemon`), ignore SIGHUP. Its default action is terminate, and the
# imports below (SQLAlchemy, Alembic, libzfseasy, ...) plus argparse take a
# few hundred milliseconds before `_StartupHup.install()` can catch it -- so
# `systemctl reload` straight after `start`, or a CLI write racing the start,
# killed the daemon (exit 129, which `Restart=on-failure` does not restart).
# Ignoring is lossless: the startup read happens later and sees every write
# committed before it. `_StartupHup.install()` replaces this with its flag
# handler, and `_StartupHup.restore()` puts it back. Only for `__main__`, so an
# importer (the test suite, the CLI) keeps its own disposition -- and SIG_IGN,
# unlike a handler, would survive into every process it execs.
#
# Accepted residual: the interpreter's own start-up before this line (exec,
# `site`, runpy, the trivial `zfsbackup/__init__.py`; ~25 ms) cannot be closed
# from Python -- a SIGHUP there still kills the process. The systemd unit
# covers it with `RestartForceExitStatus=SIGHUP` (a SIGHUP death is
# restarted, and the restart's startup read sees every committed write).
if __name__ == '__main__':  # pragma: no cover - the real process only
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

import argparse  # noqa: E402
import logging  # noqa: E402
import multiprocessing  # noqa: E402
import os  # noqa: E402
import sys  # noqa: E402
import time  # noqa: E402
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

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
    read_store_state,
    resolve_config_path,
    validate_config,
)
from zfsbackup.runtime_config import (
    config_error_message,
    describe_config_source,
    load_runtime_config,
)
from zfsbackup import daemonlock
from zfsbackup.backup_manager import DatasetManager
from zfsbackup import procident
from zfsbackup.stopflag import ReloadEpoch, StopFlag
from zfsbackup.workers import (
    WORKER_EXIT_RELOAD,
    ApiWorker,
    PruningWorker,
    RemoteBackupWorker,
    SnapshotWorker,
)


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
#:
#: A fourth term since live reload (item 17.2): a SIGTERM that lands while
#: the supervisor is inside `_reload_tick` waits for that tick to finish. A
#: poll or reload read can block for up to SQLite's `busy_timeout` (5s), and
#: an accepted reload then runs `dataset_report`/`verify_datasets`/
#: `sync_all_config_properties` for the new config -- one `zfs` call per
#: dataset, though the property sync breaks off at the next dataset once the
#: flag is up. Nothing is forked after the flag is raised, so the term only
#: delays the start of `_shutdown_workers`; size `TimeoutStopSec` with it.
SUPERVISOR_SLEEP_SLICE = 0.25

#: How often the supervisor checks the config database for a committed write
#: it was not signalled about (decision D2). Shared with the CLI's wording.
RELOAD_POLL_SECONDS = daemonlock.RELOAD_POLL_SECONDS
#: A worker still finishing work begun under an older generation is reported
#: at WARNING once this long after the reload was accepted (decision D5: it is
#: never killed for a reload) ...
RELOAD_DEFER_NOTICE = 60
#: ... and again every this many seconds for as long as it keeps going.
RELOAD_DEFER_REPEAT = 900


@dataclass(frozen=True)
class Handoff:
    """What every worker is born with, as one object: the config it runs, the
    reload epoch that config belongs to, and the `(store_id, generation)` key
    it was read at (`None` when not known -- a daemon built without one, see
    `BackupDaemon.__init__`).

    The supervisor replaces the whole object on an accepted reload and only
    then bumps the shared `ReloadEpoch`; `_new_worker` reads it exactly once,
    so a worker can never be given one generation's config with another
    generation's `born_epoch` -- the pairing that would make it either never
    switch or switch forever.
    """

    config: BackupConfig
    epoch: int
    key: Optional[Tuple[int, int]]


def _gen(key: Optional[Tuple[int, int]]) -> str:
    """The generation half of a reload key, for log lines."""
    return "unknown" if key is None else str(key[1])


#: Config-load failures that are already diagnosed by `config_error_message`
#: and are logged without a traceback. Anything else -- including a bare
#: `RuntimeError` such as `MigrationPackagingError` -- is a bug and is logged
#: with one. `OperationalError` is a `DatabaseError` subclass.
_EXPECTED_CONFIG_ERRORS = (ConfigPathError, SchemaError, ValueError, DatabaseError)


def _build_manager(config: BackupConfig) -> DatasetManager:
    """Build the `DatasetManager` and its dataset list for `config` exactly as
    every worker does first thing in `run()`, so a config no worker could
    start with is refused by the supervisor -- before any worker gets it --
    instead of crash-looping every worker. Pure: no `zfs` call. Raises what
    the build raises (`ValueError` for a name `libzfseasy` refuses, e.g.
    `pool/has space`)."""
    manager = DatasetManager(config)
    manager.datasets  # noqa: B018 -- the property builds and validates the list
    return manager


#: Top-level `BackupConfig` settings compared by a reload's summary line.
_SETTINGS = (
    'snapshot_prefix', 'check_interval', 'prune_interval', 'api_host',
    'api_port', 'dry_run', 'destinations', 'remote_backup', 'client_id_file',
)


def _changed_settings(old: BackupConfig, new: BackupConfig) -> List[str]:
    """Names of the top-level settings that differ between two configs."""
    return [n for n in _SETTINGS if getattr(old, n, None) != getattr(new, n, None)]


def _describe_dataset(ds) -> str:
    return (
        f"frequency {ds.frequency}, recursive {ds.recursive}, "
        f"{len(ds.retention_rules)} retention rule(s), "
        f"{len(getattr(ds, 'remote', None) or [])} remote(s)"
    )


class BackupDaemon:
    """Supervisor process: starts, monitors and reconfigures the snapshot,
    pruning, API and remote workers.

    One main thread, single-threaded by design: the signal handlers only
    store flags, and every decision -- restart, reload, shutdown -- is taken
    by `run()`'s poll loop (`_supervisor_sleep` -> `_reload_tick` ->
    `_check_workers`, with the stop flag re-checked between steps).

    **Live reload (item 17.2).** Two triggers feed one code path: SIGHUP
    (`zfsbackup-config` sends it after every committed write, once it has
    verified this process) and a `RELOAD_POLL_SECONDS` poll of the store's
    `(store_id, generation)` key. A changed key is read in full, validated
    and, if accepted, becomes the new `Handoff`; the shared `ReloadEpoch` is
    then bumped, and each worker leaves at its own next safe point with
    `WORKER_EXIT_RELOAD` and is respawned on the new config by
    `_check_workers` (a staggered turnover; in-flight work is never killed).
    A rejected candidate leaves every worker running the old generation.
    Progress is published in the presence lock's pid file
    (`applied=`/`target=`/`rejected=`).
    """

    def __init__(
        self,
        config: BackupConfig,
        resolved_config: ResolvedConfigPath,
        verbose: bool = False,
        *,
        key: Optional[Tuple[int, int]] = None,
        presence_lock=None,
        dry_run_override: bool = False,
        workers_lock=None,
    ):
        """`key` is the `(store_id, generation)` that `config` was read at.
        `_run_startup` always passes it. A daemon built without one (unit
        tests, embedded use) cannot tell a changed store from its own, so
        live reload is off for it: `_reload_tick` consumes a SIGHUP, logs
        that it was ignored and never reads the database.

        `presence_lock` is the held `daemonlock.DaemonLock` whose pid file
        records reload progress (`None` = record nothing).
        `dry_run_override` is `--dry-run`, re-applied to every config a
        reload reads. `workers_lock` is the `<db>.workers.lock` path every
        worker holds `LOCK_SH` on (default: derived from `resolved_config`).
        """
        #: Config, epoch and key, swapped as one object by `_try_reload` and
        #: read once per spawn by `_new_worker`. See `Handoff`.
        self._handoff = Handoff(config, 0, key)
        #: The config *database* path, with its provenance. Read by the
        #: supervisor only: at startup, by every poll and by every reload
        #: (`_reload_tick`); no worker is given it. Renamed from
        #: `config_path` rather than merely retyped, so a bare `Path` cannot
        #: sneak back in unnoticed.
        self.resolved_config = resolved_config
        self.verbose = verbose
        self._presence_lock = presence_lock
        self._dry_run_override = dry_run_override
        self._workers_lock = workers_lock
        #: Not a `multiprocessing.Event`. `Event.set()` blocks forever --
        #: holding the Condition's lock, which then also freezes every
        #: `is_set()` in every process sharing it -- once any worker has
        #: been killed while parked in `wait()`, which is routine here
        #: (`_shutdown_workers` escalates to SIGKILL, and `SIG_DFL`
        #: children die abruptly on SIGTERM/SIGINT). See
        #: `zfsbackup.stopflag` for the mechanism and the measurements.
        self._stop_event = StopFlag()
        #: The reload epoch shared with every worker (`zfsbackup.stopflag`):
        #: created once, never recreated, never touched from a signal
        #: handler. Bumped by `_try_reload` only, always AFTER the matching
        #: `Handoff` is in place; always equal to `self._handoff.epoch`
        #: once that call returns. A supervisor-local counter, never the DB
        #: generation.
        self._reload_epoch = ReloadEpoch()
        self._workers: Dict[str, multiprocessing.Process] = {}
        #: The epoch each live (or not yet classified) worker was born under,
        #: and the generation of that epoch's config (for log lines only).
        #: Set by `_spawn`, dropped with the rest of a worker's bookkeeping.
        self._worker_epoch: Dict[str, int] = {}
        self._worker_generation: Dict[str, str] = {}
        #: The last key a reload refused; the poll does not retry it (a
        #: SIGHUP does). `_rejection_logged` dedupes the ERROR line by
        #: `(key, message)`.
        self._rejected: Optional[Tuple[int, int]] = None
        self._rejection_logged: Optional[tuple] = None
        #: Poll-only marker: the stored key a poll saw when the full read it
        #: triggered failed before its own key was known (a stale schema, a
        #: damaged file, an unloadable row). The poll skips that key until it
        #: changes, so a broken store is not fully re-read every
        #: `RELOAD_POLL_SECONDS`. Never published as `rejected=`, never set
        #: for a transient `OperationalError` (locked/busy) or a
        #: `SchemaError` (stale schema: the check is cheap, and `migrate`
        #: changes no key) -- the next poll retries those -- and a SIGHUP
        #: still forces a read.
        self._poll_skip: Optional[Tuple[int, int]] = None
        #: The epoch every live worker has switched to; equal to the current
        #: epoch when no turnover is in progress.
        self._applied_epoch = 0
        #: Wall-clock "HH:MM:SS" of the last accepted reload, for the D9
        #: crash-loop message.
        self._accepted_at: Optional[str] = None
        self._next_poll_at = self._monotonic() + RELOAD_POLL_SECONDS
        #: `_monotonic()` when the current reload was accepted; `None` before
        #: the first one. Drives the deferral WARNINGs.
        self._reload_started_at: Optional[float] = None
        self._defer_logged_at: Dict[str, float] = {}
        #: The operator message of the poll failure currently in effect, or
        #: `None` when the last poll succeeded. See `_note_poll_problem`.
        self._poll_problem: Optional[str] = None
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
        #: Set by `_sighup_handler` (one plain store); makes
        #: `_supervisor_sleep` return early and is consumed -- cleared before
        #: the read it forces -- by `_reload_tick`.
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

        SIGHUP means "the configuration database was written; apply it now"
        (`zfsbackup-config` sends it after every committed write). The
        handler is one plain store. `_supervisor_sleep` notices the flag
        within one `SUPERVISOR_SLEEP_SLICE` and returns, and `_reload_tick`
        clears it and forces a full read on the main thread. Same rules as
        `_signal_handler`: no logging, no lock, and above all **no database
        access** -- `db._ENGINES_LOCK` is an RLock, so a handler that
        interrupts the main thread while it holds the lock would not
        deadlock; it would re-enter and see `_ENGINES` half-mutated. Its
        default action (terminate) would orphan every worker, which is the
        other reason it is handled at all.
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

    @property
    def config(self) -> BackupConfig:
        """The config new workers are given: the current `Handoff`'s.
        Read-only -- a reload replaces the whole `Handoff`, never this alone
        (see `_try_reload`)."""
        return self._handoff.config

    def _new_worker(self, name: str) -> multiprocessing.Process:
        cls = {
            'snapshot': SnapshotWorker,
            'pruning': PruningWorker,
            'api': ApiWorker,
            'remote': RemoteBackupWorker,
        }[name]
        # ONE read of the hand-over: config and `born_epoch` come from the
        # same `Handoff`, never `self.config` plus a separate
        # `self._reload_epoch.get()` (a reload between the two would pair a
        # config with the wrong epoch). Never a path to re-read: under `fork`
        # the config is inherited; under `spawn` and `forkserver` it is
        # pickled, and `Duration.__reduce__` carries the operator's literals
        # across, so a respawned worker sees exactly the config its siblings
        # on the same epoch run. `dry_run` is already applied to it.
        h = self._handoff
        return cls(
            h.config,
            self._stop_event,
            self.verbose,
            reload_epoch=self._reload_epoch,
            born_epoch=h.epoch,
            workers_lock=self._workers_lock_path(),
        )

    def _workers_lock_path(self):
        """Path of `<db>.workers.lock` for every worker to hold `LOCK_SH` on
        (item 18.5): the one `_run_startup` passed, else derived from the
        database path, else `None`. A path, never an open descriptor: the
        supervisor keeps no `workers.lock` fd across a fork -- each worker
        opens its own, `O_CLOEXEC`."""
        if self._workers_lock is not None:
            return self._workers_lock
        if self.resolved_config is None:
            return None
        return daemonlock.workers_lock_path(self.resolved_config)

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
        no session may be open across this call. Every database read the
        supervisor makes (`_run_startup`, `_reload_tick`, `_try_reload`)
        disposes in a `finally` straight after it, so none is.

        The worker's epoch is recorded from the same `Handoff` `_new_worker`
        reads: only this (main) thread ever replaces `self._handoff`, and
        not between these two lines.
        """
        dispose_all()
        h = self._handoff
        worker = self._new_worker(name)
        worker.start()
        # Recorded straight after `start()`, before anything that can raise
        # (the log line): an untracked live child would never be joined or
        # terminated at shutdown.
        self._workers[name] = worker
        self._worker_epoch[name] = h.epoch
        self._worker_generation[name] = _gen(h.key)
        self._started_at[name] = self._monotonic()
        on = f" on generation {_gen(h.key)}" if verb == 'Reloaded' else ""
        logger.info(f"{verb} {name} worker (pid={worker.pid}){on}")
        self._restart_at.pop(name, None)
        return worker

    def _forget_worker(self, name: str, *, keep_failures: bool = True) -> None:
        """Drop the bookkeeping of a worker that is gone for good (a turnover
        about to be respawned, or a worker the new config no longer needs).
        `_failures` survives a turnover, so a worker that crash-loops across
        a reload still reaches the A-5 limit (D9)."""
        self._workers.pop(name, None)
        self._restart_at.pop(name, None)
        self._started_at.pop(name, None)
        self._worker_epoch.pop(name, None)
        self._worker_generation.pop(name, None)
        self._defer_logged_at.pop(name, None)
        if not keep_failures:
            self._failures.pop(name, None)

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

        A crash restart hands the replacement the supervisor's current
        `Handoff` (see `_new_worker`); it does not re-read the config
        database, so a crash can never move one worker onto a different
        generation than its siblings born on the same epoch. There is no
        configuration failure left to classify here: a worker never opens
        the database, so a bad one is refused by `_run_startup` before any worker
        exists (or, on a reload, by `_try_reload` while the old workers keep
        running). A worker that dies is restarted with exponential backoff
        and escalated to the fatal treatment below after
        `WORKER_MAX_CONSECUTIVE_FAILURES` consecutive deaths.

        **Turnover (item 17.2).** A worker that exits `WORKER_EXIT_RELOAD`
        while its `born_epoch` is below the current epoch did exactly what a
        reload asked: it is respawned at once on the current `Handoff`
        ("Reloaded"), with no failure count and no backoff -- or retired, if
        the current config no longer needs it. A code 3 from a worker born
        on the current epoch cannot be a reload, so it takes the crash path;
        during a shutdown any code 3 is just a normal exit. Respawning only
        a worker that is already dead is also what keeps the API port free:
        a replacement `ApiWorker` never exists beside the old one.

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
        h = self._handoff
        dead = [
            (name, w) for name, w in self._workers.items()
            if name not in self._restart_at and not w.is_alive()
        ]

        active: Optional[Set[str]] = None
        turnovers: List[Tuple[str, multiprocessing.Process]] = []
        for name, worker in dead:
            if worker.exitcode == WORKER_EXIT_RELOAD:
                if self._stop_event.is_set():
                    # It saw RELOAD just before STOP: a normal exit, left in
                    # `_workers` for `_shutdown_workers` to reap.
                    continue
                if self._worker_epoch.get(name, h.epoch) < h.epoch:
                    turnovers.append((name, worker))
                    continue
            if active is None:
                active = set(self._active_worker_names())
            if name not in active:
                # Draining after a reload that no longer needs it, and it
                # crashed instead of switching: nothing to restart.
                logger.error(
                    f"{name} worker (pid={worker.pid}) exited with code "
                    f"{worker.exitcode}; not restarting it: not needed by "
                    f"generation {_gen(h.key)}"
                )
                self._forget_worker(name, keep_failures=False)
                continue
            if self._record_worker_death(name, worker):
                self._crash_looped.add(name)
                del self._workers[name]

        if not self._crash_looped:
            if turnovers and active is None:
                active = set(self._active_worker_names())
            for i, (name, worker) in enumerate(turnovers):
                # Forking into a shutdown is never right.
                if self._stop_event.is_set():
                    logger.info(
                        "Shutdown requested; not respawning remaining "
                        f"worker(s): {', '.join(n for n, _ in turnovers[i:])}"
                    )
                    return
                retired = name not in active
                # The same recovery rule a death applies
                # (`_record_worker_death`): an incarnation that stayed up
                # `WORKER_STABLE_UPTIME` has recovered, so a turnover must
                # not carry its earlier crashes forward.
                now = self._monotonic()
                if now - self._started_at.get(name, now) >= WORKER_STABLE_UPTIME:
                    self._failures[name] = 0
                self._forget_worker(name, keep_failures=not retired)
                if retired:
                    logger.info(
                        f"{name} worker retired: not needed by generation {_gen(h.key)}"
                    )
                else:
                    self._spawn(name, verb='Reloaded')

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
            # D9: no rollback. After a reload the operator needs to know
            # which generation the crash loop happened on.
            running = ""
            if h.epoch > 0:
                running = (
                    f" The daemon is running configuration generation "
                    f"{_gen(h.key)} (accepted at {self._accepted_at})."
                )
            logger.error(
                "Worker(s) crashed repeatedly and were not restarted: "
                f"{', '.join(sorted(self._crash_looped))}. The daemon "
                "cannot run degraded -- shutting down the remaining "
                f"workers ({', '.join(sorted(self._workers)) or 'none'}) "
                f"and exiting non-zero.{running} See the worker errors above."
            )
            # A visible, non-zero exit instead of a spin: this is precisely
            # the "workers have died" case `StopFlag` (see
            # `zfsbackup.stopflag`) was built for, and `set()` cannot wedge
            # on a ghost sleeper the way `multiprocessing.Event` could. If
            # this is ever reverted to `Event`, that guarantee is lost.
            self._stop_event.set()

        if self._stop_event.is_set():
            return
        self._log_deferred_switches()
        self._maybe_complete()

    def _log_deferred_switches(self) -> None:
        """D5: a reload never kills in-flight work, so a worker busy with a
        long transfer can stay on the old generation indefinitely. Say so:
        WARNING per such worker once `RELOAD_DEFER_NOTICE` has passed since
        the reload was accepted, then every `RELOAD_DEFER_REPEAT`."""
        h = self._handoff
        if self._reload_started_at is None or self._applied_epoch >= h.epoch:
            return
        now = self._monotonic()
        if now - self._reload_started_at < RELOAD_DEFER_NOTICE:
            return
        for name, worker in self._workers.items():
            if name in self._restart_at or self._worker_epoch.get(name, h.epoch) >= h.epoch:
                continue
            if not worker.is_alive():
                continue  # classified by the next `_check_workers`
            last = self._defer_logged_at.get(name)
            if last is not None and now - last < RELOAD_DEFER_REPEAT:
                continue
            self._defer_logged_at[name] = now
            logger.warning(
                f"{name} worker (pid={worker.pid}) is still finishing work begun "
                f"under generation {self._worker_generation.get(name, 'unknown')}; "
                f"it switches to {_gen(h.key)} at its next safe point "
                "(in-flight transfers are never interrupted)"
            )

    def _maybe_complete(self) -> None:
        """Record a reload as applied once every live worker runs the
        current epoch. A worker waiting out a crash backoff counts as
        switched: it will be respawned on the current `Handoff`."""
        h = self._handoff
        if self._applied_epoch >= h.epoch:
            return
        for name in self._workers:
            if name in self._restart_at:
                continue
            if self._worker_epoch.get(name, h.epoch) < h.epoch:
                return
        self._applied_epoch = h.epoch
        if self._presence_lock is not None:
            self._presence_lock.set_state(applied=h.key, target=None)
        logger.info(f"generation {_gen(h.key)} applied by all workers")

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
        """Sleep up to `seconds`, returning as soon as the stop flag is set
        or a SIGHUP is pending (`_hup_requested`, one attribute read per
        slice), so a reload request waits at most one
        `SUPERVISOR_SLEEP_SLICE` before `_reload_tick` acts on it.

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
            if self._hup_requested:
                return
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
        # Stop-guarded per dataset, like a reload's: a SIGTERM here is not
        # held up by one `zfs` call per remaining dataset.
        manager.verify_datasets(should_stop=self._stop_event.is_set)

        logger.info("Syncing dataset configs to ZFS properties...")
        manager.sync_all_config_properties(should_stop=self._stop_event.is_set)

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
            if not self._stop_event.is_set():
                # Before the first spawn: a SIGHUP carried over from
                # `_StartupHup`, or a write that landed while the sync above
                # ran, is applied now -- onto workers that do not exist yet,
                # so there is no turnover and no double cycle.
                self._reload_tick(startup=True)

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
                    if self._stop_event.is_set():
                        break
                    self._reload_tick()
                    if self._stop_event.is_set():
                        break
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

    # -- live reload (item 17.2) -------------------------------------------

    def _reload_tick(self, startup: bool = False) -> None:
        """Decide whether to read the configuration again, and do it.

        Called from `run()`'s loop between `_supervisor_sleep` and
        `_check_workers`, and once with `startup=True` before the first
        spawn. Two triggers, one path (D1):

        - **SIGHUP** (`_hup_requested`) forces a full read. The flag is
          cleared BEFORE the read: a SIGHUP that lands later forces another
          tick, and the write behind this one was committed before its
          signal was sent, so the read below sees it.
        - **The poll**, every `RELOAD_POLL_SECONDS` (and always at startup):
          `read_store_state`, and a full read only when the stored key
          differs from both the running key and the last rejected one.

        Every database read is followed by `dispose_all()` in a `finally`,
        including a read that raises, so no pooled connection exists at the
        next fork. Stop beats everything: nothing is read once the flag is
        up.
        """
        h = self._handoff
        forced = self._hup_requested
        if forced:
            self._hup_requested = False
        if h.key is None:
            if forced:
                logger.warning(
                    "Received SIGHUP, but this daemon was started without a "
                    "configuration key, so live reload is off; restart it to "
                    "apply configuration changes"
                )
            return
        now = self._monotonic()
        if not (forced or startup or now >= self._next_poll_at):
            return
        self._next_poll_at = now + RELOAD_POLL_SECONDS
        if self._stop_event.is_set():
            return
        if not forced:
            try:
                try:
                    state = read_store_state(self.resolved_config)
                finally:
                    dispose_all()
            except Exception as e:
                self._note_poll_problem(e)
                return
            self._note_poll_problem(None)
            stored = (state.store_id, state.generation)
            if stored in (h.key, self._rejected, self._poll_skip):
                return
            # The poll can block for up to `busy_timeout`: a stop that landed
            # meanwhile must not start a full read and a reload.
            if self._stop_event.is_set():
                return
            self._try_reload('poll', startup=startup, polled=stored)
        else:
            self._try_reload('SIGHUP', startup=startup)

    def _describe_failure(self, exc: Exception) -> str:
        """`config_error_message` with the reload wording, never raising."""
        try:
            return config_error_message(self.resolved_config, exc, reloading=True)
        except Exception:  # pragma: no cover - the message must never kill the loop
            return f"{type(exc).__name__}: {exc}"

    def _note_poll_problem(self, exc: Optional[Exception]) -> None:
        """Log a poll failure once per state change: WARNING when a problem
        starts (or its message changes), INFO when it clears, DEBUG while it
        persists. The daemon keeps running its current generation either
        way."""
        running = _gen(self._handoff.key)
        if exc is None:
            if self._poll_problem is not None:
                self._poll_problem = None
                logger.info(
                    "the configuration database can be checked for changes "
                    f"again; running generation {running}"
                )
            return
        message = self._describe_failure(exc)
        # The store's message goes LAST and verbatim: its last line may be a
        # command meant to be pasted into a shell.
        line = (
            "cannot check the configuration database for changes; still "
            f"running generation {running}:\n{message}"
        )
        if message != self._poll_problem:
            self._poll_problem = message
            # Diagnosed failures carry no traceback; anything else is a bug
            # and is undiagnosable without one.
            logger.warning(line, exc_info=not isinstance(exc, _EXPECTED_CONFIG_ERRORS))
        else:
            logger.debug(line)

    def _reject(
        self,
        trigger: str,
        key: Optional[Tuple[int, int]],
        exc: Exception,
        *,
        polled: Optional[Tuple[int, int]] = None,
        message: Optional[str] = None,
    ) -> None:
        """A candidate that could not be read, failed `validate_config`, or
        cannot be built (`_build_manager`): log it (deduplicated by
        `(key, message)`), and when its key is known remember it so the poll
        does not retry it, and publish it as `rejected=` in the pid file.
        When the key is unknown but a poll triggered the read, `polled` is
        remembered as the poll-only `_poll_skip` marker instead (not for a
        transient `OperationalError`). The workers keep running the current
        generation. `message` replaces `config_error_message`'s wording."""
        if message is None:
            message = self._describe_failure(exc)
        # The clause first, the store's message LAST and verbatim: its last
        # line may be a command meant to be pasted into a shell.
        line = (
            f"configuration generation {_gen(key)} rejected (trigger: {trigger}); "
            f"still running generation {_gen(self._handoff.key)}:\n{message}"
        )
        if (key, message) != self._rejection_logged:
            self._rejection_logged = (key, message)
            # Diagnosed failures carry no traceback; anything else is a bug
            # and is undiagnosable without one.
            logger.error(line, exc_info=not isinstance(exc, _EXPECTED_CONFIG_ERRORS))
        else:
            logger.debug(line)
        if key is not None:
            if key != self._rejected:
                self._rejected = key
                if self._presence_lock is not None:
                    self._presence_lock.set_state(rejected=key)
        elif polled is not None and not isinstance(exc, (OperationalError, SchemaError)):
            # Not for a transient storage failure, and not for a schema
            # failure: `check_schema` runs first in `read_config`, before any
            # row, so a retry costs one tiny read transaction, and `migrate`
            # keeps the store id and generation -- only a retry picks the
            # migrated store up without a SIGHUP. The ERROR above is logged
            # once per `(key, message)`; each retry logs at DEBUG.
            self._poll_skip = polled

    def _try_reload(
        self,
        trigger: str,
        *,
        startup: bool = False,
        polled: Optional[Tuple[int, int]] = None,
    ) -> None:
        """Read, validate, build and -- if it is a different, usable
        configuration -- switch to it. The key recorded is the one
        `read_config`'s snapshot returned, never the poll's (`polled` is only
        a `_poll_skip` marker for a read that failed before its key was
        known): a write between the poll and this read is simply part of
        what is applied.

        Refused, with the old workers left running, when the read fails,
        `validate_config` raises, or the dataset list cannot be built
        (`_build_manager`, the first thing every worker does): a config that
        would crash-loop every worker is never accepted. Only the steps that
        call `zfs` -- `verify_datasets` and the property sync -- are
        advisory, logged and continued past. Stop beats all of it: nothing
        is accepted, recorded or switched once the flag is up.

        Order matters and is the contract workers rely on: the new `Handoff`
        is stored BEFORE `ReloadEpoch.set()`, so any worker spawned after a
        worker could observe the new epoch is born with the new config and
        the new epoch together.

        The catch is deliberately broad (`Exception`, bare `RuntimeError`
        included, unlike startup's): a running daemon keeps its current
        generation rather than dying over a candidate. Undiagnosed types are
        logged with a traceback (`_EXPECTED_CONFIG_ERRORS`).
        """
        h = self._handoff
        key: Optional[Tuple[int, int]] = None
        try:
            try:
                snap = load_runtime_config(self.resolved_config)
                key = (snap.store_id, snap.generation)
                warnings = validate_config(snap.config)
            finally:
                dispose_all()
        except Exception as e:
            self._reject(trigger, key, e, polled=polled)
            return

        if key == h.key:
            if trigger == 'SIGHUP':
                logger.info(f"SIGHUP: configuration unchanged (generation {_gen(key)})")
            return

        config = snap.config
        try:
            manager = _build_manager(config)
        except Exception as e:
            self._reject(
                trigger, key, e, message=f"its dataset list cannot be used: {e}"
            )
            return
        if self._stop_event.is_set():
            return

        # `--dry-run` is a property of this run, never of the stored
        # configuration: it survives every reload.
        if self._dry_run_override:
            config.dry_run = True
        for warning in warnings:
            logger.warning(f"configuration generation {_gen(key)}: {warning}")
        logger.info(
            f"configuration generation {_gen(key)} accepted "
            f"(was {_gen(h.key)}; trigger: {trigger})"
        )
        if self._presence_lock is not None:
            self._presence_lock.set_state(target=key, rejected=None)

        old = {ds.name: ds for ds in h.config.datasets}
        new = {ds.name: ds for ds in config.datasets}
        added = sorted(new.keys() - old.keys())
        removed = sorted(old.keys() - new.keys())
        changed = sorted(n for n in new.keys() & old.keys() if new[n] != old[n])
        settings = _changed_settings(h.config, config)
        if logger.isEnabledFor(logging.DEBUG):
            manager.dataset_report()

        try:
            manager.verify_datasets(should_stop=self._stop_event.is_set)
            logger.info("Syncing dataset configs to ZFS properties...")
            manager.sync_all_config_properties(should_stop=self._stop_event.is_set)
        except Exception as e:
            # The `zfs`-calling steps only (existence checks, property sync):
            # advisory, and a failure here must not take a running daemon
            # down. Everything that could stop a worker from starting was
            # checked above.
            logger.error(
                f"checking or syncing the datasets of generation {_gen(key)} "
                f"failed: {e}; continuing",
                exc_info=True,
            )
        if self._stop_event.is_set():
            # Shutdown proceeds; `target=` stays recorded, nothing switches.
            return

        epoch = h.epoch + 1
        self._handoff = Handoff(config, epoch, key)
        self._reload_epoch.set(epoch)
        self._rejected = None
        self._rejection_logged = None
        self._poll_skip = None
        self._reload_started_at = self._monotonic()
        self._defer_logged_at.clear()
        self._accepted_at = time.strftime("%H:%M:%S")

        how = (
            "no workers are running yet" if startup
            else "workers switch over at their next safe point"
        )
        what = f"+{len(added)}, -{len(removed)}, ~{len(changed)} datasets"
        if settings:
            what += f"; settings changed: {', '.join(settings)}"
        logger.info(
            f"Reloading: generation {_gen(h.key)} -> {_gen(key)} ({what}); {how}"
        )
        # Logged only now, after the stop check above: a reload interrupted
        # by a shutdown must not describe a switch that never happened. Only
        # what changed at INFO; the full per-dataset block (which startup
        # prints) only under --verbose, not on every `set`.
        for name in added:
            logger.info(f"  added dataset {name}: {_describe_dataset(new[name])}")
        for name in changed:
            logger.info(f"  changed dataset {name}: {_describe_dataset(new[name])}")
        for name in removed:
            logger.info(f"  removed dataset {name}")
        if 'snapshot_prefix' in settings:
            logger.warning(
                f"snapshot_prefix changed from {h.config.snapshot_prefix} to "
                f"{config.snapshot_prefix}: snapshots named "
                f"{h.config.snapshot_prefix}_* are no longer managed or pruned"
            )

        if not startup:
            self._adjust_worker_set()
        self._maybe_complete()

    def _adjust_worker_set(self) -> None:
        """After a reload: retire workers in backoff that the new config no
        longer needs, and start the ones it newly needs. A name still in
        `_workers` -- live, draining, or dead and not yet classified -- is
        left to `_check_workers`, which respawns it only once it has exited:
        never a second instance beside the first (the API port)."""
        if self._crash_looped:
            return
        h = self._handoff
        active = self._active_worker_names()
        for name in sorted(self._restart_at):
            if name not in active:
                self._forget_worker(name, keep_failures=False)
                logger.info(f"{name} worker retired: not needed by generation {_gen(h.key)}")
        missing = [n for n in active if n not in self._workers]
        for i, name in enumerate(missing):
            # Forking into a shutdown is never right.
            if self._stop_event.is_set():
                logger.info(
                    "Shutdown requested; not starting worker(s): "
                    f"{', '.join(missing[i:])}"
                )
                return
            self._spawn(name)


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


#: Seams for the startup waits: tests advance time without sleeping.
_startup_sleep = time.sleep
_startup_monotonic = time.monotonic

#: Poll interval of the startup waits, and how often their WARNING repeats.
_STARTUP_WAIT_POLL = 1.0
_STARTUP_WARN_EVERY = 60.0


def _acquire_presence_lock(resolved):
    """`daemonlock.acquire(resolved)`, except that a lock held by the fork
    orphans of a verifiably dead supervisor is waited out rather than refused.

    `DaemonAlreadyRunning` is re-raised unless the recorded supervisor is
    `'gone'` or `'mismatch'` by `procident.identity(e.pid, e.start)`: then the
    holders are workers of a daemon that was `kill -9`'d, which exit at their
    next safe point (a transfer in progress is never interrupted). Retried
    every second, with a WARNING at the start and every 60s. `'match'` (a live
    daemon) and `'unknown'` (legacy file, no token, unreadable) are refused as
    before. SIGINT/SIGTERM keep their default dispositions here, so the wait
    stays killable.
    """
    last_warned = None
    while True:
        try:
            return daemonlock.acquire(resolved)
        except daemonlock.DaemonAlreadyRunning as e:
            if procident.identity(e.pid, e.start) not in ('gone', 'mismatch'):
                raise
            now = _startup_monotonic()
            if last_warned is None or now - last_warned >= _STARTUP_WARN_EVERY:
                last_warned = now
                logger.warning(
                    f"{daemonlock.stale_supervisor_message(e.path, e.pid)}; "
                    "waiting for them to exit (they stop at their next safe "
                    "point and a transfer in progress is never interrupted)"
                )
            _startup_sleep(_STARTUP_WAIT_POLL)


def _wait_for_previous_workers(resolved) -> None:
    """Block while workers of a previous daemon still hold
    `<db>.workers.lock` (item 18.5). Under `spawn`/`forkserver` such survivors
    hold no presence lock, so without this a restarted daemon could overlap
    with a worker finishing its last dataset. Called right after the presence
    lock is taken and before the config is read.

    A detection aid, not a gate: if the lock file cannot be created or
    inspected, log a WARNING and carry on. SIGINT/SIGTERM keep their default
    dispositions during the wait (no workers exist yet; stay killable).
    """
    try:
        path = daemonlock.ensure_workers_lock(resolved)
        present = daemonlock.workers_present(resolved)
    except (daemonlock.DaemonLockError, OSError) as e:
        logger.warning(f"cannot check for workers of a previous daemon: {e}")
        return
    last_warned = None
    while present:
        now = _startup_monotonic()
        if last_warned is None or now - last_warned >= _STARTUP_WARN_EVERY:
            last_warned = now
            logger.warning(
                f"workers of a previous daemon still hold {path}; waiting for "
                "them to finish -- they exit at their next safe point and a "
                f"transfer in progress is never interrupted. Find them with `fuser {path}`"
            )
        _startup_sleep(_STARTUP_WAIT_POLL)
        try:
            present = daemonlock.workers_present(resolved)
        except (daemonlock.DaemonLockError, OSError) as e:
            logger.warning(f"cannot check for workers of a previous daemon: {e}")
            return


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
    carries the result into the daemon's own `_hup_requested` (set-only), and
    `run()`'s startup `_reload_tick` -- before the first spawn -- forces the
    read, so a write that raced the start is applied with no worker turnover.

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
    # Every full read runs Alembic's schema check, which logs two routine
    # INFO lines ("Context impl SQLiteImpl", "Will assume non-transactional
    # DDL"). With live reload the supervisor reads on every accepted change,
    # so keep them out of the log unless something is actually wrong --
    # except under --verbose, which shows them as before.
    if not getattr(args, 'verbose', False):
        logging.getLogger('alembic').setLevel(logging.WARNING)
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
    nothing) -> take the presence lock -> wait out a predecessor's workers ->
    read the config -> `validate_config` -> record its key as `applied=` in
    the lock -> `BackupDaemon.run`. `--test-config` skips the lock but not
    the validation. Any refusal after the lock was taken releases it.
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
                lock = _acquire_presence_lock(resolved)
            except daemonlock.DaemonAlreadyRunning as e:
                logger.error(f"{e}. Refusing to start a second daemon.")
                return 1
            except daemonlock.DaemonLockError as e:
                logger.error(f"Cannot take the daemon lock: {e}")
                return 1
            # Survivors of a previous daemon (item 18.5), before the config
            # is read and long before the first fork.
            _wait_for_previous_workers(resolved)

        # The daemon's startup read of the config database: a single read
        # snapshot in which the schema check and the rows are the same
        # database state. The supervisor hands the resulting detached
        # `BackupConfig` to its workers, which never open the database
        # (item 15.2(b)); later reads are the supervisor's reload polls.
        # Disposed in a `finally`, so not even a failed read leaves a pooled
        # reader connection to sit on through the long dataset verification
        # below or to carry towards the first fork. `_spawn` repeats the
        # dispose at every fork.
        try:
            snapshot = load_runtime_config(resolved)
        finally:
            dispose_all()
        config = snapshot.config
        generation = snapshot.generation
        key = (snapshot.store_id, generation)
        logger.info(
            f"Configuration generation {generation}, "
            f"schema revision {snapshot.schema_rev}"
        )
        # D10: the same check every reload applies, so a configuration this
        # daemon would refuse on a reload is never run at startup either. A
        # `ValueError` is an operator-grade message: no traceback.
        try:
            warnings = validate_config(config)
        except ValueError as e:
            logger.error(f"configuration generation {generation} is invalid: {e}")
            _release_refused(lock)
            return 1
        for warning in warnings:
            logger.warning(f"configuration generation {generation}: {warning}")
        # And the build every worker does first (`_build_manager`): a
        # dataset list no worker could start with -- an invalid ZFS name --
        # is refused here, cleanly, rather than by six crash-loops per worker
        # and a "Daemon failed" exit minutes later. Same refusal as above.
        try:
            _build_manager(config)
        except Exception as e:
            logger.error(
                f"configuration generation {generation} cannot be used: "
                f"its dataset list cannot be built: {e}",
                exc_info=not isinstance(e, ValueError),
            )
            _release_refused(lock)
            return 1
        # The key recorded in the lock is the one the workers are about to
        # run, which is what `zfsbackup-config` reports to an operator after
        # an edit (`applied=` also rewrites line 2 with the generation).
        # Until this line the file's generation is empty ("not known yet").
        if lock is not None:
            lock.set_state(applied=key)

        # In memory only, and BEFORE the config is handed to any worker.
        # `--dry-run` is a property of this run, not of the stored
        # configuration, and the daemon never writes to the database. Every
        # config a reload reads gets it again (`dry_run_override`).
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
        daemon = BackupDaemon(
            config,
            resolved,
            verbose=args.verbose,
            key=key,
            presence_lock=lock,
            dry_run_override=bool(args.dry_run),
            workers_lock=daemonlock.workers_lock_path(resolved),
        )
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


def _process_main() -> int:  # pragma: no cover - the real process only
    """`main()` for the daemon process itself: SIGHUP is ignored again on the
    way out, whatever path `main()` took, so a SIGHUP during interpreter
    shutdown (multiprocessing's atexit joins, module teardown) cannot turn an
    exit status into a signal death. Not part of `main()`, so embedded and
    test callers keep their own disposition (`_StartupHup.restore()`)."""
    try:
        return main()
    finally:
        signal.signal(signal.SIGHUP, signal.SIG_IGN)


if __name__ == '__main__':  # pragma: no cover
    sys.exit(_process_main())

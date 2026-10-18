"""Worker processes for snapshot creation, pruning, API serving, and remote backup.

Workers never open the config database. The supervisor reads it
(`zfsbackup.runtime_config.load_runtime_config`) at startup and on every
reload, applies `--dry-run` to the result in memory, and hands every worker
its detached `BackupConfig` together with the reload epoch that config belongs
to (`born_epoch`), both from one `daemon.Handoff`. Under `fork` the child
inherits the object; under `spawn`/`forkserver` it is pickled
(`Duration.__reduce__` preserves the operator's literals), so a respawned
worker sees exactly the configuration its siblings on the same epoch run. A
crash restart does not re-read the database; it gets the current hand-over.

**Live reload.** When the supervisor accepts a new configuration it bumps the
shared `ReloadEpoch`. A worker whose `born_epoch` no longer matches leaves at
its next safe point -- loop top, between datasets, between remote
destinations, each `_wait` slice; never inside `_process_dataset` or a
stream -- and exits `WORKER_EXIT_RELOAD`; the supervisor then respawns it on
the new configuration. Workers switch one by one (staggered), and in-flight
work is never killed for a reload. A stop beats a reload.

Nothing that owns a SQLite handle may cross the process boundary -- never a
`Session`, an `Engine`, or an ORM instance -- and the supervisor calls
`dispose_all()` before each fork so that no inherited descriptor exists
either. A `BackupConfig` is plain dataclasses, safe on both counts.

A worker also never outlives its supervisor. The stop flag cannot say "the
supervisor was `kill -9`'d" (nobody is left to raise it), so every worker
watches for that itself and treats it as a stop; see `_SupervisorWatch`.
"""

import enum
import logging
import multiprocessing
import os
import signal
import sys
import threading
import time
from abc import abstractmethod
from multiprocessing import Process
from multiprocessing.synchronize import Event
from typing import List, Optional, Union

from zfsbackup import procident
from zfsbackup.backup_manager import DatasetInfo, DatasetManager
from zfsbackup.config import BackupConfig
from zfsbackup.stopflag import STOP_POLL, ReloadEpoch, StopFlag

#: What a worker accepts as its stop signal.
#:
#: The daemon passes a `StopFlag` (see `zfsbackup.stopflag`); a plain
#: `multiprocessing.Event` is still accepted, and that is deliberate rather
#: than transitional. Workers only ever call `.is_set()` and
#: `.wait(timeout=...)`, both of which the two types implement identically,
#: so callers holding an `Event` -- roughly thirty-five of them in the test
#: suite, including one that hands a real `Event` to a real `ApiWorker` --
#: keep working unchanged. Narrowing this to `StopFlag` would buy nothing at
#: runtime and break all of them at once.
StopEvent = Union[StopFlag, Event]

#: Smallest sleep between worker cycles. The config store rejects intervals
#: <= 0 on write, but a hand-edited database can still hold one, and
#: `StopFlag.wait(<=0)` returns immediately: the worker would shell out to
#: `zfs` in a hot loop. `BaseWorker.run` floors the interval here.
MIN_WORKER_INTERVAL = 1.0

#: Exit status of a worker that stopped because the configuration changed
#: (the supervisor's reload epoch moved past the one the worker was born
#: under). Distinct from 0 (stop / supervisor gone) and 1 (crash). The
#: supervisor counts it as a turnover only when the worker's `born_epoch` is
#: older than the current epoch; otherwise it is a crash.
WORKER_EXIT_RELOAD = 3

#: How long the API worker waits for in-flight receives on a STOP or a gone
#: supervisor, before abandoning them (the sender resumes: `zfs receive -s`).
#: Inside `BackupDaemon._shutdown_workers`' shared 30s budget.
API_STOP_DRAIN = 20


class ExitReason(enum.Enum):
    """Why a worker should leave its loop. Priority is the declaration order:
    a stop beats a gone supervisor beats a reload."""

    STOP = 'stop'
    SUPERVISOR_GONE = 'supervisor-gone'
    RELOAD = 'reload'


def _setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def _init_child_process(verbose: bool = False) -> None:
    """First statement of **every** worker `run()`. Nothing may precede it.

    Resets the `SIGINT`/`SIGTERM` dispositions this child must not own,
    ignores `SIGHUP` (it is addressed to the supervisor, which reloads the
    configuration and tells workers through the shared `ReloadEpoch`; a
    worker dying on a SIGHUP sent to its process group, or on the terminal
    closing, would just be a crash to restart -- and under `fork` it would
    otherwise inherit the supervisor's flag-setting handler, which does
    nothing in a worker), and only then configures logging.

    `BackupDaemon.__init__` installs the supervisor's handlers before any
    worker is created, and under the `fork` start method (CPython's default
    on Linux up to 3.13) a child inherits them; `spawn` and `forkserver`
    start from `SIG_DFL` instead. Measured dispositions in the child:

        fork         SIGTERM=INHERITED HANDLER  SIGINT=INHERITED HANDLER
        spawn        SIGTERM=SIG_DFL            SIGINT=default_int_handler
        forkserver   SIGTERM=SIG_DFL            SIGINT=default_int_handler

    The inherited case **deadlocked** the whole daemon, for as long as the
    stop signal was a `multiprocessing.Event`. A tty Ctrl-C signals every
    process in the foreground group, so each worker -- which sits in
    `stop_event.wait()` essentially all of the time -- ran the supervisor's
    handler on its own main thread: `Event.set()` ->
    `Condition.notify_all()` -> wake a sleeper -> block acquiring
    `_woken_count`, waiting for the sleeper it just woke to respond. That
    sleeper was this same thread, suspended inside the handler, so it never
    would; the handler then wedged *holding the shared `Condition`'s lock*,
    and because `Event.is_set()` takes that same lock, the supervisor
    froze too. Measured: parent `is_set()` blocked >5s. It was invisible to
    CI -- macOS is spawn, Linux on 3.14 is forkserver, and no test raises a
    real SIGINT.

    With `zfsbackup.stopflag.StopFlag` that is no longer true: a fork child
    that inherits `BackupDaemon._signal_handler` runs two plain stores and
    returns, on every start method. **Item 8x alone is sufficient for the
    fork case to be safe** -- item 8 alone was not (see the measurements
    below). What still justifies the reset is the *other* reason:

    `SIG_DFL` makes the fork case behave exactly as spawn and forkserver
    already do: a Ctrl-C or a `terminate()` kills the child promptly rather
    than being handled. That is what keeps `_shutdown_workers`' `terminate()`
    escalation working -- a child that merely records the signal and keeps
    running ignores SIGTERM in exactly the way that escalation exists to
    punish (measured before this reset: `alive after terminate+join(5):
    True exitcode None`). Deadlock avoidance is no longer the reason; the
    escalation is. The cost is that a worker *signalled directly* (a tty
    Ctrl-C to the process group, `terminate()`) dies mid-cycle. The
    cooperative path is the other one: the supervisor raises the stop flag
    or bumps the reload epoch, and the worker leaves at its next safe point
    (`_exit_reason`, item 18.2) -- after the dataset, and so the `zfs send`,
    in hand -- on every start method; `terminate()` is only
    `_shutdown_workers`' escalation once its budget is spent.

    That said, this reset alone made a plain Ctrl-C *less* survivable, not
    more, for as long as the stop signal was a `multiprocessing.Event`.
    With Python's default SIGINT disposition a child raises
    `KeyboardInterrupt`, which unwinds through `Condition.wait`'s `finally`
    and releases the notify token; `SIG_DFL` does not run any Python code,
    so it does not. Measured, three workers parked in `stop_event.wait()`:

        children at default_int_handler:  exitcodes=[1,1,1]     set() returned in 6.9 ms
        children at SIG_DFL (item 8):     exitcodes=[-2,-2,-2]  set() WEDGED

    So item 8's reset removed the fork inherited-handler deadlock above and,
    in doing so, converted a survivable Ctrl-C into a wedging one on every
    start method. Item 8x's `StopFlag` (see `zfsbackup.stopflag`) is what
    removes that wedge -- and it removes the inherited-handler deadlock as
    well, which is why reverting to an `Event` would be wrong even with
    this reset in place. Item 8 is not a regression on its own, but it
    should not be read in isolation.

    Two related rules this function does not implement, both belonging to
    `BackupDaemon._signal_handler` (where they now hold): a signal handler
    must never call `Event.set()`, because `notify_all()` is not
    async-signal-safe, and it must never log, because `logging` takes locks
    and then blocks writing to a socket the handler cannot know is draining.
    """
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    _setup_logging(verbose)


def _pid_exists(pid: int) -> bool:
    """`kill(pid, 0)`. `EPERM` means the process exists; any other error is
    taken as "cannot tell", i.e. alive -- this check may only ever err towards
    *not* declaring the supervisor gone."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        return True
    return True


class _SupervisorWatch:
    """Lets a worker notice that its supervisor died, and treat that as a stop.

    Without it a `kill -9`'d supervisor leaves its workers running forever on
    a frozen config: under `fork` they still hold the inherited presence lock
    (the daemon looks alive but nothing supervises it); under `spawn` and
    `forkserver` they hold no lock, so a second daemon can start beside them
    and the pair double-snapshot and double-prune. The stop flag cannot help:
    only the (dead) supervisor ever raises it.

    **What is compared, per start method.** The supervisor's pid is recorded
    in the worker's constructor, which runs in the supervisor
    (`self.supervisor_pid = os.getpid()`). Under `run()` the worker captures
    `os.getppid()` as the first thing after `_init_child_process` and later
    treats any of these as "supervisor gone":

    * `os.getppid()` differs from the value captured at start. A child that
      loses its parent is reparented (to init, or to a subreaper), on every
      platform and start method, and that is detectable without any
      cooperation from the dead process;
    * the captured parent is not the recorded supervisor pid, *except* under
      `forkserver`. Under `fork` and `spawn` the supervisor is the worker's
      direct parent, so any other parent at start means the supervisor had
      already died (and been reparented away) before the worker's first
      instruction -- a state `getppid()` can never report as "changed". Under
      `forkserver` the direct parent is the forkserver process, not the
      supervisor, so this comparison is meaningless there and is skipped. The
      forkserver itself exits when the supervisor does (it watches a pipe the
      supervisor holds), which reparents its children and trips the first rule;
    * `procident.identity(supervisor_pid, supervisor_start)` -- the
      supervisor's start token, recorded in the constructor -- is `'gone'`
      (no such process; covers the forkserver-before-reparent race) or
      `'mismatch'` (the pid was reused by another process). Where no token is
      available (`'unknown'`) it falls back to `kill(supervisor_pid, 0)`
      saying the pid no longer exists. `identity` reports a zombie
      supervisor (exited, not yet reaped) as `'gone'` too; only the
      `'unknown'` fallback still answers "alive" for one, which is why the
      reparent rule above is the primary one.

    In a process that is not a child of the constructing one -- a unit test
    calling `run()` inline, where `os.getpid() == supervisor_pid` -- the watch
    is never armed and never fires.

    **Checked only at safe points** (loop top, between datasets, inside the
    sliced wait): never mid-transfer. A worker in the middle of a `zfs send`
    finishes its current dataset first, exactly as for a stop request.
    `prctl(PR_SET_PDEATHSIG)` was considered and deliberately not used: its
    signal would be delivered at an arbitrary instruction -- with `SIG_DFL`,
    tearing a stream in half, the one thing the stop protocol exists to avoid
    -- it is Linux-only so the macOS behaviour would differ and could not be
    exercised here, it fires on the death of the parent *thread* that forked
    (not the process), and it still needs the `getppid()` re-check for the
    window before it is armed. Polling gives one portable behaviour with
    safe-point semantics; the cost is detection latency of up to one
    `stopflag.STOP_POLL` slice while idle.
    """

    #: Class-level defaults: a worker built without them (or by a test that
    #: bypasses `__init__`) behaves exactly as before live reload existed.
    reload_epoch: Optional[ReloadEpoch] = None
    born_epoch: int = 0
    workers_lock: Optional[str] = None
    supervisor_start: Optional[str] = None

    def _watch_init(
        self,
        reload_epoch: Optional[ReloadEpoch] = None,
        born_epoch: int = 0,
        workers_lock=None,
    ) -> None:
        #: Constructors run in the supervisor, so this is its pid. Not a
        #: constructor parameter: the worker signatures are pinned to
        #: `(config, stop_event, verbose)` plus the keyword-only reload trio.
        self.supervisor_pid = os.getpid()
        #: Its start token (`procident`): pid reuse after a `kill -9` is
        #: detected by identity, not by "some process has that pid".
        self.supervisor_start = procident.start_token(self.supervisor_pid)
        self.reload_epoch = reload_epoch
        self.born_epoch = born_epoch
        self.workers_lock = workers_lock
        self._workers_lock_fd: Optional[int] = None
        self._parent_at_start: Optional[int] = None
        self._direct_child = True
        self._supervisor_gone_logged = False

    def _arm_supervisor_watch(self) -> None:
        """Call first in `run()`, straight after `_init_child_process`."""
        parent = os.getppid()
        if os.getpid() == self.supervisor_pid:
            return  # inline in the constructing process: nothing to watch
        self._parent_at_start = parent
        self._direct_child = (
            multiprocessing.get_start_method(allow_none=True) != 'forkserver'
        )

    def _supervisor_gone(self) -> bool:
        if self._parent_at_start is None:
            return False
        reason = None
        ppid = os.getppid()
        if ppid != self._parent_at_start:
            reason = f"parent changed from {self._parent_at_start} to {ppid}"
        elif self._direct_child and ppid != self.supervisor_pid:
            reason = f"parent at start was {ppid}, not the supervisor"
        else:
            ident = procident.identity(self.supervisor_pid, self.supervisor_start)
            if ident == 'gone':
                # `identity` reports a zombie (exited, unreaped) as gone too.
                reason = (
                    "it has exited (a zombie, not yet reaped)"
                    if procident.is_zombie(self.supervisor_pid) is True
                    else "no such process"
                )
            elif ident == 'mismatch':
                reason = f"pid {self.supervisor_pid} now belongs to another process"
            elif ident != 'match' and not _pid_exists(self.supervisor_pid):
                # 'unknown' (no token on this platform / unreadable): fall
                # back to plain existence, which only errs towards "alive".
                reason = "no such process"
        if reason is None:
            return False
        if not self._supervisor_gone_logged:
            self._supervisor_gone_logged = True
            logging.getLogger(__name__).error(
                f"supervisor {self.supervisor_pid} is gone ({reason}); exiting"
            )
        return True

    def _hold_workers_lock(self) -> None:
        """Take `LOCK_SH` on `<db>.workers.lock` for the life of this process
        (item 18.5): the presence lock that lets a restarted daemon see
        survivors under every start method. The descriptor is opened HERE, in
        the worker, never inherited from the supervisor, and is `O_CLOEXEC`.
        A detection aid, not a gate: failure is a WARNING and the worker runs."""
        if not self.workers_lock:
            return
        try:
            from zfsbackup import daemonlock
            self._workers_lock_fd = daemonlock.hold_workers_lock(self.workers_lock)
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "cannot hold workers lock %s: %s", self.workers_lock, exc
            )

    def _exit_reason(self) -> Optional[ExitReason]:
        """Why this worker should leave its loop, or `None`. Priority: STOP,
        SUPERVISOR_GONE, RELOAD -- so a stop always beats a reload. Checked
        only at safe points, never inside `_process_dataset`."""
        if self.stop_event.is_set():
            return ExitReason.STOP
        if self._supervisor_gone():
            return ExitReason.SUPERVISOR_GONE
        epoch = self.reload_epoch
        if epoch is not None and epoch.get() != self.born_epoch:
            return ExitReason.RELOAD
        return None

    def _should_stop(self) -> bool:
        """Any reason to leave the loop (stop flag, supervisor gone, reload)."""
        return self._exit_reason() is not None

    def _wait(self, timeout: float) -> Optional[ExitReason]:
        """Sleep up to `timeout` seconds in `STOP_POLL` slices; -> the reason
        that ended the wait, or `None` on timeout. Each slice re-checks the
        supervisor and the reload epoch, so a dead supervisor or a reload is
        noticed within one slice rather than after a whole cycle interval
        (minutes to hours). Like `StopFlag.wait`, a zero, negative or NaN
        timeout returns the current reason without sleeping."""
        if not (timeout > 0):
            return self._exit_reason()
        deadline = time.monotonic() + timeout
        while True:
            reason = self._exit_reason()
            if reason is not None:
                return reason
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            self.stop_event.wait(timeout=min(STOP_POLL, remaining))


class BaseWorker(_SupervisorWatch, Process):
    """Base class for daemon worker processes with a shared polling loop."""

    def __init__(
        self,
        worker_name: str,
        config: BackupConfig,
        stop_event: StopEvent,
        verbose: bool = False,
        *,
        reload_epoch: Optional[ReloadEpoch] = None,
        born_epoch: int = 0,
        workers_lock=None,
    ):
        """`config` is the supervisor's detached `BackupConfig`, with
        `--dry-run` already applied. It is used as is; the worker never
        reads the config database. The constructing process is the
        supervisor, and its pid is recorded here (see `_SupervisorWatch`).

        `reload_epoch`/`born_epoch`: the worker leaves its loop with
        `WORKER_EXIT_RELOAD` at its next safe point once the shared epoch
        differs from `born_epoch` (`None` = never). `workers_lock`: path of
        `<db>.workers.lock` to hold `LOCK_SH` on while running (`None` = none).
        """
        super().__init__(name=worker_name, daemon=True)
        self.config = config
        self.stop_event = stop_event
        self.verbose = verbose
        self._watch_init(reload_epoch, born_epoch, workers_lock)

    @abstractmethod
    def _get_interval(self, config: BackupConfig) -> float:
        """Return the loop sleep interval in seconds."""

    @abstractmethod
    def _process_dataset(self, manager: DatasetManager, dsi: DatasetInfo) -> None:
        """Process a single dataset."""

    def _get_datasets(self, manager: DatasetManager) -> List[DatasetInfo]:
        """Return the list of datasets to iterate each cycle. Override to extend."""
        return manager.datasets

    def _before_loop(self, config: BackupConfig, manager: DatasetManager) -> None:
        """Called once after setup but before the main loop. Override for per-run init."""

    def run(self) -> None:
        _init_child_process(self.verbose)
        self._arm_supervisor_watch()
        self._hold_workers_lock()
        logger = logging.getLogger(__name__)
        logger.info(f"{self.name} started")

        config = self.config
        manager = DatasetManager(config)
        interval = self._get_interval(config)
        if interval < MIN_WORKER_INTERVAL:
            logger.warning(
                f"{self.name} interval {interval}s is below the {MIN_WORKER_INTERVAL:g}s "
                f"minimum (hand-edited config database?); using {MIN_WORKER_INTERVAL:g}s"
            )
            interval = MIN_WORKER_INTERVAL
        self._before_loop(config, manager)

        # `_exit_reason` (stop flag > supervisor gone > reload) is checked at
        # every safe point: loop top, between datasets, and each `_wait`
        # slice -- never inside `_process_dataset`, so a reload (or a stop)
        # lets the dataset in hand finish.
        reason = self._exit_reason()
        while reason is None:
            logger.debug(f"{self.name} cycle")
            for dsi in self._get_datasets(manager):
                reason = self._exit_reason()
                if reason is not None:
                    break
                try:
                    self._process_dataset(manager, dsi)
                except Exception as e:
                    logger.error(f"{self.name} error on {dsi.name}: {e}", exc_info=True)
            if reason is None:
                reason = self._wait(interval)

        self._finish(reason, logger)

    def _finish(self, reason: ExitReason, logger) -> None:
        if reason is ExitReason.RELOAD:
            logger.info(
                f"{self.name} finished its current work; exiting to switch configuration"
            )
            sys.exit(WORKER_EXIT_RELOAD)
        # STOP and SUPERVISOR_GONE both end here with exit status 0. The
        # latter was already logged once, at ERROR, by the watch.
        logger.info(f"{self.name} stopped")


class SnapshotWorker(BaseWorker):
    """Worker that periodically checks all datasets and creates snapshots as needed."""

    def __init__(
        self,
        config: BackupConfig,
        stop_event: StopEvent,
        verbose: bool = False,
        *,
        reload_epoch: Optional[ReloadEpoch] = None,
        born_epoch: int = 0,
        workers_lock=None,
    ):
        super().__init__(
            'snapshot-worker', config, stop_event, verbose,
            reload_epoch=reload_epoch, born_epoch=born_epoch, workers_lock=workers_lock,
        )

    def _get_interval(self, config: BackupConfig) -> float:
        return config.check_interval.total_seconds()

    def _process_dataset(self, manager: DatasetManager, dsi: DatasetInfo) -> None:
        logger = logging.getLogger(__name__)
        if manager.needs_snapshot(dsi):
            logger.info(f"Dataset {dsi.name} needs snapshot")
            manager.create_snapshot(dsi)


class PruningWorker(BaseWorker):
    """Worker that periodically applies retention policy and prunes old snapshots."""

    def __init__(
        self,
        config: BackupConfig,
        stop_event: StopEvent,
        verbose: bool = False,
        *,
        reload_epoch: Optional[ReloadEpoch] = None,
        born_epoch: int = 0,
        workers_lock=None,
    ):
        super().__init__(
            'pruning-worker', config, stop_event, verbose,
            reload_epoch=reload_epoch, born_epoch=born_epoch, workers_lock=workers_lock,
        )

    def _get_interval(self, config: BackupConfig) -> float:
        return config.effective_prune_interval.total_seconds()

    def _get_datasets(self, manager: DatasetManager) -> List[DatasetInfo]:
        return manager.datasets + manager.received_datasets()

    def _process_dataset(self, manager: DatasetManager, dsi: DatasetInfo) -> None:
        manager.prune_snapshots(dsi)


class RemoteBackupWorker(BaseWorker):
    """Worker that periodically sends snapshots to configured remote destinations."""

    def __init__(
        self,
        config: BackupConfig,
        stop_event: StopEvent,
        verbose: bool = False,
        *,
        reload_epoch: Optional[ReloadEpoch] = None,
        born_epoch: int = 0,
        workers_lock=None,
    ):
        super().__init__(
            'remote-backup-worker', config, stop_event, verbose,
            reload_epoch=reload_epoch, born_epoch=born_epoch, workers_lock=workers_lock,
        )
        self._remote_manager = None

    def _get_interval(self, config: BackupConfig) -> float:
        freqs = [
            (r.frequency or ds.frequency).total_seconds()
            for ds in config.enabled_datasets
            for r in ds.remote
        ]
        return min(freqs) if freqs else config.check_interval.total_seconds()

    def _before_loop(self, config: BackupConfig, manager: DatasetManager) -> None:
        from zfsbackup.remote import RemoteBackupManager
        self._remote_manager = RemoteBackupManager(config, manager)

    def _process_dataset(self, manager: DatasetManager, dsi: DatasetInfo) -> None:
        if not dsi.config.remote:
            return
        logger = logging.getLogger(__name__)
        for index, remote_cfg in enumerate(dsi.config.remote):
            if index and self._exit_reason() is not None:
                # Between destinations no stream is open: a safe point.
                return
            freq = (remote_cfg.frequency or dsi.config.frequency).total_seconds()
            anchor_name = manager.get_anchor(dsi, remote_cfg.destination)
            if anchor_name:
                snapshots = manager.list_snapshots(dsi)
                anchor = next((s for s in snapshots if s.name == anchor_name), None)
                if anchor and anchor.age.total_seconds() < freq:
                    logger.debug(
                        f"Backup of {dsi.name} to {remote_cfg.destination} not due yet"
                    )
                    continue
            self._remote_manager.backup_dataset(dsi, remote_cfg)


def _bind_api_server(app, host: str, port: int):
    """Create the HTTP server **and bind its listening socket**, here, in the
    caller's thread. Raises `OSError` (EADDRINUSE, EACCES, bad host...) if it
    cannot. Werkzeug's `make_server` binds and listens in the constructor, so
    a returned server is already accepting connections into its backlog.
    Werkzeug itself turns EADDRINUSE/EADDRNOTAVAIL into a printed message plus
    `sys.exit(1)`; that `SystemExit` is converted back into an `OSError` here
    so the caller logs one clear "cannot listen" line for every bind failure.

    This replaces `app.run()` in a thread, which bound inside the thread: a
    failed bind killed only that thread, leaving a worker process that was
    alive, logged "API listening", and served nothing -- so the supervisor's
    restart/backoff never applied. It also skips `Flask.run`'s extras that a
    daemon does not want (`.env` loading, the Werkzeug debugger under
    `FLASK_DEBUG`). A seam so tests need not open real sockets.
    """
    from werkzeug.serving import make_server
    try:
        return make_server(host, port, app, threaded=True)
    except SystemExit as exc:
        raise OSError(f"cannot bind {host}:{port} (address in use or not available)") from exc


def _serve(server, logger) -> None:
    """Thread body: serve until the process exits. A crash is logged here and
    is noticed by `ApiWorker.run`'s wait loop via `Thread.is_alive`."""
    try:
        server.serve_forever()
    except Exception:
        logger.error("API server thread died", exc_info=True)


class ApiWorker(_SupervisorWatch, Process):
    """Worker that serves a read-only HTTP API for configuration and snapshot inspection."""

    def __init__(
        self,
        config: BackupConfig,
        stop_event: StopEvent,
        verbose: bool = False,
        *,
        reload_epoch: Optional[ReloadEpoch] = None,
        born_epoch: int = 0,
        workers_lock=None,
    ):
        """`config` is the supervisor's detached `BackupConfig`, dry-run
        already applied; see `BaseWorker`."""
        super().__init__(name='api-worker', daemon=True)
        self.config = config
        self.stop_event = stop_event
        self.verbose = verbose
        self._watch_init(reload_epoch, born_epoch, workers_lock)

    def run(self) -> None:
        """Serve until stopped or until the configuration changes. Exits with
        status 1 -- so the supervisor's crash backoff and restart apply -- if
        the listening socket cannot be bound or the server thread dies; a
        worker that is alive but serving nothing is the one outcome that must
        not happen.

        This process hosts the server side of `zfs receive -s`, so leaving
        for a reload must not tear an inbound stream: once the reason to
        leave is known the stream gate (`zfsbackup.api.StreamGate`) refuses
        new streams, read-only endpoints keep serving, and the worker waits
        for the receives already running (on RELOAD: indefinitely, but a stop
        or a gone supervisor aborts the wait; on STOP / SUPERVISOR_GONE: at
        most `API_STOP_DRAIN` seconds). Then the server is shut down, its
        socket closed (so the replacement worker can bind the port), and the
        worker exits `WORKER_EXIT_RELOAD` or returns."""
        _init_child_process(self.verbose)
        self._arm_supervisor_watch()
        self._hold_workers_lock()
        logger = logging.getLogger(__name__)
        logger.info("API worker started")

        # The supervisor's detached `BackupConfig`: the server thread never
        # touches a Session or an Engine, because this process has none.
        config = self.config

        from zfsbackup.api import create_app
        app = create_app(config)
        gate = app.extensions['zfsbackup.stream_gate']

        try:
            server = _bind_api_server(app, config.api_host, config.api_port)
        except OSError as e:
            logger.error(
                f"API worker cannot listen on {config.api_host}:{config.api_port}: {e}"
            )
            sys.exit(1)

        thread = threading.Thread(target=_serve, args=(server, logger), daemon=True)
        thread.start()
        logger.info(f"API listening on {config.api_host}:{config.api_port}")

        died = False
        reason: Optional[ExitReason] = None
        try:
            # Sliced wait: wakes every `STOP_POLL` to check the exit reasons
            # and that the server thread is still alive.
            while True:
                reason = self._exit_reason()
                if reason is not None:
                    break
                if not thread.is_alive():
                    logger.error("API server thread exited unexpectedly; exiting")
                    died = True
                    break
                self.stop_event.wait(timeout=STOP_POLL)

            if not died:
                gate.start_draining()
                active = gate.active
                if reason is ExitReason.RELOAD:
                    if active:
                        logger.info(
                            f"waiting for {active} in-flight receive(s) to finish "
                            "before reloading"
                        )
                        gate.wait_idle(None, abort=lambda: self._exit_reason() in (
                            ExitReason.STOP, ExitReason.SUPERVISOR_GONE))
                        # A stop that aborted the wait changes what we are doing.
                        reason = self._exit_reason() or reason
                if reason is not ExitReason.RELOAD:
                    active = gate.active
                    if active and not gate.wait_idle(API_STOP_DRAIN):
                        logger.warning(
                            f"abandoning {gate.active} in-flight receive(s); the "
                            "sender resumes them (zfs receive -s)"
                        )
        finally:
            # Every exit path past a successful bind: stop accepting, free
            # the port, reap the thread.
            try:
                if thread.is_alive():
                    server.shutdown()
            except Exception:
                logger.debug("server.shutdown() failed", exc_info=True)
            try:
                server.server_close()
            except Exception:
                logger.debug("server.server_close() failed", exc_info=True)
            thread.join(timeout=5)

        if died:
            sys.exit(1)
        if reason is ExitReason.RELOAD:
            logger.info(
                "API worker finished its current work; exiting to switch configuration"
            )
            sys.exit(WORKER_EXIT_RELOAD)
        logger.info("API worker stopped")

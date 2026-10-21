"""Worker processes for snapshot creation, pruning, API serving, and remote backup.

Workers never read the configuration from the database. The supervisor reads
it (`zfsbackup.runtime_config.load_runtime_config`) at startup and on every
reload, applies `--dry-run` to the result in memory, and hands every worker
its detached `BackupConfig` together with the reload epoch that config belongs
to (`born_epoch`) and this host's client id, all from one `daemon.Handoff`.
The one exception to "never opens the database" is the API worker's trust
view: it reads the pinned client certificates (and, from M10, writes
enrollments) only through `zfsbackup.config.store.trust`, on a fresh unpooled
engine per call, never the configuration tables. Under `fork` the child
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
            if (
                not self._direct_child
                and ppid != self._parent_at_start
                and procident.identity(self.supervisor_pid, self.supervisor_start) == 'match'
            ):
                # Under `forkserver` the parent is the forkserver, not the
                # supervisor. It died but the supervisor is alive and
                # verified: this worker is an orphan the supervisor will
                # replace once it has exited. Leaving is still right (the
                # supervisor can no longer stop or reload it, and its
                # replacement must not run beside it) -- but the supervisor
                # is not gone.
                logging.getLogger(__name__).error(
                    f"forkserver exited ({reason}); supervisor "
                    f"{self.supervisor_pid} is alive; leaving at this safe point"
                )
            else:
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

    def _before_loop(self, config: BackupConfig, manager: DatasetManager) -> None:
        """Verify that the configured datasets exist and sync their
        `org.zfsbackup:config` properties, once per worker incarnation.

        This is the supervisor's old startup/reload step, moved here so that
        the supervisor makes no `zfs` call: a hung pool then stalls this
        worker only, never SIGTERM, SIGHUP or the restart of the others. The
        snapshot worker is respawned once per accepted generation, which
        keeps "sync once per generation"; a crash-restarted one syncs again,
        and the sync only writes differences. Both steps are advisory: a
        failure is logged and the worker carries on. `self._should_stop`
        (stop, supervisor gone, reload) ends either step at the next
        dataset, and `run()` then leaves at its first safe-point check.
        `--dry-run` writes nothing (`sync_config_property`)."""
        logger = logging.getLogger(__name__)
        try:
            manager.verify_datasets(should_stop=self._should_stop)
            if not self._should_stop():
                logger.info("Syncing dataset configs to ZFS properties...")
                manager.sync_all_config_properties(should_stop=self._should_stop)
        except Exception as e:
            logger.error(
                f"{self.name} could not check or sync the datasets: {e}; continuing",
                exc_info=True,
            )

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
        tls_dir=None,
        client_id: Optional[str] = None,
    ):
        """`tls_dir` is `<db>.tls` (the default client TLS identity lives
        there unless `client_cert_file`/`client_key_file` override it);
        `client_id` is this host's client id from the supervisor's config
        snapshot (`daemon.Handoff.client_id`), `None` while none has been
        generated (`zfsbackup-config enroll` does that). Both come from the
        same hand-over as `config`; a changed id is a reload, so a worker
        never runs with one generation's config and another's id. Both are
        passed to `RemoteBackupManager(config, manager, client_id=,
        tls_dir=)`."""
        super().__init__(
            'remote-backup-worker', config, stop_event, verbose,
            reload_epoch=reload_epoch, born_epoch=born_epoch, workers_lock=workers_lock,
        )
        self.tls_dir = tls_dir
        self.client_id = client_id
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
        self._remote_manager = RemoteBackupManager(
            config, manager, client_id=self.client_id, tls_dir=self.tls_dir
        )

    def _get_datasets(self, manager: DatasetManager) -> List[DatasetInfo]:
        # Called once at the top of every cycle: each destination's `/whoami`
        # check then runs once per cycle, and a client TLS identity that
        # failed to load is retried.
        if self._remote_manager is not None:
            self._remote_manager.begin_cycle()
        return super()._get_datasets(manager)

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
            # `should_stop` is polled before any send starts and between a
            # resumed send and its follow-up: no new stream during a stop or
            # a reload (C3).
            self._remote_manager.backup_dataset(
                dsi, remote_cfg, should_stop=lambda: self._exit_reason() is not None
            )


#: At most this many connections are served (handshaking or in a request) at
#: once. Over the cap a new connection is closed straight away, on the accept
#: thread, before any TLS byte: unauthenticated peers can tie up at most this
#: many threads (plan "Threads before authentication").
API_MAX_CONNECTIONS = 64

#: Timeout of a connection's TLS handshake (seconds; CPython applies a
#: socket timeout to `do_handshake` as one deadline for the whole handshake,
#: not per read). A peer that connects and goes silent is dropped after this
#: long, on its own thread; it never stalls the accept loop or
#: `server.shutdown()` (plan D6).
API_HANDSHAKE_TIMEOUT = 30.0

#: ABSOLUTE deadline for a request's line and headers (seconds from the
#: moment the server starts waiting for them), and so how long an idle
#: keep-alive connection may hold one of the `API_MAX_CONNECTIONS` slots. A
#: deadline, not a per-read timeout: a peer dribbling one byte every few
#: seconds is cut off all the same. For a connection with a pinned client
#: certificate it is cleared when the app starts (a request body or a
#: websocket stream is not limited by it); a connection without one gets a
#: fresh deadline of the same length for the whole request (`/health`,
#: `/enroll` and refusals are quick).
API_REQUEST_IDLE_TIMEOUT = 30.0

#: ABSOLUTE deadline for werkzeug's drain of an unread request body after
#: the response (seconds from the drain's first read), for every connection,
#: pinned or not. A peer that vanishes mid-drain (a lost RST, a partition)
#: would otherwise hold its handler thread and slot until TCP gives up --
#: for a pinned connection, with no keepalive, possibly never. Expiry is a
#: quiet end of the drain (EOF, DEBUG) and the connection is closed. Never
#: applies to a websocket (its socket is not drained) or to anything before
#: the response's status line.
API_DRAIN_TIMEOUT = 5.0

#: At most this many of the `API_MAX_CONNECTIONS` may be connections whose
#: handshake completed WITHOUT a pinned client certificate (`/health`,
#: `/enroll`, refusals). Decided after the handshake; over the cap the
#: connection is closed. Anonymous peers cannot starve pinned clients of
#: slots past the handshake.
API_MAX_ANONYMOUS_CONNECTIONS = 16

#: After a failed TLS handshake, how long (seconds, at most) the connection is
#: half-closed and what the peer still sends is read and discarded before the
#: close (`_close_after_failed_handshake`). Ends early when the peer closes.
API_REJECT_LINGER = 1.0

#: At the end of a websocket session, how long (seconds) the connection's
#: thread waits for simple-websocket's reader thread to leave the socket on
#: its own (the peer's close reply, EOF or reset) before it shuts the socket
#: down at the OS level, and again after that (`_SessionSocket.end`).
API_WS_END_TIMEOUT = 2.0

#: The capacity WARNING is logged at most once per this many seconds.
_API_FULL_WARN_EVERY = 60.0

_API_SERVER_CLASS = None


def _api_server_class():
    """The API server class: werkzeug's `ThreadedWSGIServer` (what
    `make_server(..., threaded=True)` builds), HTTPS only, with the TLS
    handshake done per connection on that connection's own thread, and
    without the reverse DNS lookup in its bind.

    **TLS (plan D6, verified by spike S2/S3).** Werkzeug's own `ssl_context`
    wraps the *listening* socket, so every handshake runs inside `accept()`
    on the serving thread, with no timeout: one silent peer stalls every
    later client and `server.shutdown()` (and with it the reload drain).
    Here the listening socket is never wrapped. `process_request` (accept
    thread) only takes a slot -- non-blocking, `API_MAX_CONNECTIONS`, over
    the cap the connection is closed at once -- and starts the thread;
    `process_request_thread` (the connection's thread) wraps the socket with
    `TrustGateway.current_context()` *as of that connection*, runs the
    handshake with `API_HANDSHAKE_TIMEOUT`, clears the timeout and hands the
    `SSLSocket` to werkzeug. A failed handshake (unpinned or expired client
    certificate, TLS < 1.3, timeout, a port scan) is logged at DEBUG and
    closed -- half-closed first, for at most `API_REJECT_LINGER`, so a TLS 1.3
    client reads the rejection alert rather than a reset
    (`_close_after_failed_handshake`). After the handshake a connection without a pinned client
    certificate (no certificate: `/health`, `/enroll`) must also take one of
    `API_MAX_ANONYMOUS_CONNECTIONS` slots, or is closed, so anonymous peers
    cannot hold every slot. The request line and headers are read under an
    ABSOLUTE deadline (`API_REQUEST_IDLE_TIMEOUT`, enforced before every read
    by the handler's `rfile`), so a peer dribbling bytes cannot keep a slot;
    it is lifted when the app starts for a pinned connection and renewed for
    the whole request otherwise. After the response, werkzeug's drain of an
    unread body runs under its own short deadline (`API_DRAIN_TIMEOUT`, every
    connection), and its expiry or a peer tearing the connection down is a
    quiet EOF (DEBUG); a websocket's socket is not drained at all
    (`_DeadlineReader`). A websocket session gets a `_SessionSocket` that
    serialises its writes and lets the connection be closed only once
    simple-websocket's reader thread has left it (`API_WS_END_TIMEOUT`).
    The slots are released in a `finally`.
    `self.ssl_context` is
    assigned after construction purely so werkzeug's environ reports `https`,
    fills `SSL_CLIENT_CERT` and treats SSL errors as dropped connections; it
    is never used to wrap anything. Handler threads are daemon threads that
    `server_close()` does not join, so `shutdown()` returns promptly with
    stalled handshakes pending (they end at their timeout, or with the
    process).

    There is no plaintext mode: the constructor refuses a gateway that does
    not yield an `ssl.SSLContext`.

    **Bind.** `http.server.HTTPServer.server_bind` -- inherited by every
    werkzeug server -- sets `server_name = socket.getfqdn(host)`, a reverse
    lookup that stalls for ~30 s on a host with a slow or broken resolver
    (measured on macOS CI runners) and delayed every API worker start by that
    long. Nothing here uses `server_name` (werkzeug builds `SERVER_NAME` and
    `Host` handling from `server_address` and the request), so `server_bind`
    is `socketserver.TCPServer.server_bind` (which does the `SO_REUSEADDR`
    and the bind) plus the host/port bookkeeping, with `server_name` set to
    the configured host. Built lazily so importing this module does not
    import werkzeug."""
    global _API_SERVER_CLASS
    if _API_SERVER_CLASS is None:
        import errno
        import hashlib
        import io
        import math
        import socket
        import socketserver
        import ssl
        from werkzeug.serving import ThreadedWSGIServer, WSGIRequestHandler

        log = logging.getLogger(__name__)

        def _is_teardown(exc: BaseException) -> bool:
            """Whether `exc`, raised by a read after the response went out,
            is the peer tearing the connection down: any TLS error (a reset
            mid-record surfaces as a record-layer failure, a bad record MAC
            or, on some OpenSSL builds, a bare "failure in the SSL library";
            `SSLEOFError` / `SSLZeroReturnError` are subclasses), a reset or
            broken pipe, the read deadline, or a socket already closed or
            disconnected (`EBADF`, `ENOTCONN`, e.g. from `settimeout`)."""
            if isinstance(exc, (ssl.SSLError, ConnectionError, TimeoutError)):
                return True
            return isinstance(exc, OSError) and exc.errno in (errno.EBADF, errno.ENOTCONN)

        class _DeadlineReader(io.RawIOBase):
            """The handler's raw `rfile`: before each read, the socket's
            timeout is set to what is left of the handler's deadline (none
            set: the read is untouched), and a read past it raises
            `TimeoutError` -- which `http.server` treats as "Request timed
            out" and closes the connection.

            **After the response has gone out** the only reader is
            werkzeug's drain of an unread request body (`run_wsgi`'s
            `execute`, which always closes the connection afterwards), and a
            connection teardown there -- or an EOF after some of the unread
            body was discarded -- is a quiet end of the drain (EOF from then
            on, one DEBUG line), not an "Error on request" traceback: the
            request itself was served. The drain is bounded by an ABSOLUTE
            deadline (`API_DRAIN_TIMEOUT` from its first read, or the
            request's own deadline if that is earlier), whose expiry ends it
            the same way -- pinned connections included. For a websocket
            request the socket is not read at all then -- it belongs to the
            websocket session, whose reader thread (simple-websocket) may
            still be inside `recv` on it, and one `SSLSocket` must not be
            read from two threads at once; the session had no request body
            anyway. Before the response, reads and their errors are
            unchanged.

            **Constraint on routes:** "after the response" starts at the
            status line (`send_response`), so EVERY `rfile` read from then on
            is treated as the drain. A route that streams its response while
            still reading the request body would see a teardown as a silent
            EOF (a truncated body, not an error) and its reads capped at
            `API_DRAIN_TIMEOUT`. No route does that today; one that needs it
            must read its body before it starts the response."""

            def __init__(self, raw, handler):
                super().__init__()
                self._raw = raw
                self._handler = handler

            def readable(self):
                return True

            def readinto(self, buffer):
                handler = self._handler
                if handler.response_started:
                    return self._drain_into(buffer)
                deadline = handler.read_deadline
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("request not received in time")
                    handler.connection.settimeout(remaining)
                return self._raw.readinto(buffer)

            def _drain_into(self, buffer):
                handler = self._handler
                if handler.drain_ended or handler.is_websocket_request():
                    return 0
                try:
                    if handler.drain_deadline is None:
                        handler.drain_deadline = handler.start_drain_deadline()
                    deadline = handler.drain_deadline
                    if handler.read_deadline is not None:
                        deadline = (
                            handler.read_deadline if deadline is None
                            else min(deadline, handler.read_deadline)
                        )
                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError("drain deadline passed")
                        handler.connection.settimeout(remaining)
                    n = self._raw.readinto(buffer)
                except Exception as exc:
                    if not _is_teardown(exc):
                        raise
                    return self._end_drain(f"{type(exc).__name__}: {exc}")
                if n == 0 and handler.drained_bytes:
                    # The peer closed while its unread body was being
                    # discarded. A reset can arrive this way too, not only as
                    # an exception: CPython 3.11/3.12 with OpenSSL 3.0 report
                    # a TCP reset during `SSL_read` as a ragged EOF, which
                    # `SSLSocket` (`suppress_ragged_eofs`) returns as 0 bytes.
                    # Same end, same one DEBUG line. An EOF with nothing
                    # drained is a normal close (nothing was left unread).
                    return self._end_drain("EOF")
                handler.drained_bytes += n
                return n

            def _end_drain(self, why):
                # EOF from now on (one DEBUG line, no read of a dead
                # socket), and the connection is not reused.
                handler = self._handler
                handler.drain_ended = True
                handler.close_connection = True
                log.debug(
                    "API: connection from %s ended after the response (%s)",
                    handler.client_address, why,
                )
                return 0

            def close(self):
                try:
                    self._raw.close()
                finally:
                    super().close()

        class _SessionSocket:
            """The socket a websocket session gets (`werkzeug.socket` in its
            environ, which simple-websocket's `Server` takes), in place of
            the connection's `SSLSocket`. Two threads use it: the route's
            (sends, and the close frame) and simple-websocket's reader thread
            (blocked in `recv`; it also answers ping and close frames).

            **Why.** When the route returns, werkzeug closes the connection
            on this thread while the reader may still be inside `SSL_read`
            on it (observed: about every second session). The descriptor
            number is then free, the next accepted connection gets it, and
            OpenSSL's next `read()` on that number -- the body of a record
            whose header it already read, or the next `recv` -- consumes the
            NEW connection's bytes, and the alert it sends when they fail to
            decrypt goes to the new peer: that client's handshake or first
            read fails (`UNEXPECTED_MESSAGE`, `DECRYPTION_FAILED_OR_BAD_
            RECORD_MAC`) and the server logs `WRONG_VERSION_NUMBER`.

            So `end()` (the connection's thread, after the route, before the
            connection is closed) refuses new I/O and waits until no call is
            inside the `SSLSocket`. `close()` is a no-op: the connection
            belongs to the server, which closes it after `end()`. If a call
            is still inside after that (a reader stalled past both waits: a
            frozen cgroup, a paused VM, SIGSTOP), the descriptor must still
            not be closed under it: `end()` hands the connection over to
            this object, the server skips its close
            (`ApiServer.shutdown_request`), and the last call to leave closes
            it (`_leave`). The worst case is a descriptor held for as long as
            the stall lasts, never a reused one.

            `upgraded` is set at the first write (the `101`): from then on
            the connection carries the websocket stream, and no HTTP response
            may be written on it (`ApiRequestHandler.send_response`).

            Writes are serialised by one lock (`SSL_write` must never run on
            one SSL object from two threads; the reader's control-frame
            replies and the route's sends would otherwise be able to
            overlap) and are always complete (`sendall`: simple-websocket
            ignores `send`'s count). Reads are not locked: the reader blocks
            in `recv` for the whole session, and it is the only reader --
            the post-response drain never reads a websocket request
            (`_DeadlineReader._drain_into`).

            Only what simple-websocket and `api._end_websocket` use is
            offered; anything else fails loudly rather than bypassing the
            write lock or the descriptor accounting."""

            def __init__(self, tls):
                self._tls = tls
                self._write_lock = threading.Lock()
                self._state = threading.Condition(threading.Lock())
                #: Calls currently inside `self._tls` (reads and writes).
                self._busy = 0
                self._ended = False
                #: `end()` failed: the last call to leave closes `self._tls`.
                self._close_when_idle = False
                #: Something was written (the `101`): the session is upgraded.
                self.upgraded = False

            def _enter(self) -> bool:
                with self._state:
                    if self._ended:
                        return False
                    self._busy += 1
                    return True

            def _leave(self) -> None:
                with self._state:
                    self._busy -= 1
                    if self._busy:
                        return
                    self._state.notify_all()
                    close, self._close_when_idle = self._close_when_idle, False
                if close:
                    # The connection was handed over by a failed `end()`.
                    try:
                        self._tls.close()
                    except OSError:
                        pass

            def recv(self, bufsize, flags=0):
                if not self._enter():
                    return b''                 # EOF: the session is over
                try:
                    return self._tls.recv(bufsize, flags)
                finally:
                    self._leave()

            def sendall(self, data, flags=0) -> None:
                with self._write_lock:
                    if not self._enter():
                        raise BrokenPipeError(errno.EPIPE, "the websocket session has ended")
                    # Before the write: even a failed one may have put bytes
                    # of the `101` on the wire.
                    self.upgraded = True
                    try:
                        self._tls.sendall(data, flags)
                    finally:
                        self._leave()

            def send(self, data, flags=0) -> int:
                self.sendall(data, flags)
                return memoryview(data).nbytes

            def fileno(self) -> int:
                return self._tls.fileno()      # simple-websocket's ping selector

            def close(self) -> None:
                """simple-websocket's reader closes the socket as it leaves;
                the connection is the server's to close (after `end()`)."""

            def force_eof(self) -> None:
                """Shut the connection down at the OS level, so a `recv` or
                `sendall` blocked on a silent peer returns at once. A plain
                `socket.shutdown` (a kernel call, no TLS record written, and
                the descriptor stays open): `SSLSocket.shutdown` would drop
                the TLS object from under a thread inside it. Never raises."""
                try:
                    socket.socket.shutdown(self._tls, socket.SHUT_RDWR)
                except OSError:
                    pass

            def end(self, timeout: float) -> bool:
                """Refuse new I/O (a `recv` is EOF, a write `EPIPE`), then
                wait up to `timeout` for the calls in progress to leave on
                their own, then `force_eof()` and wait up to `timeout` again.
                -> True: nothing is inside the socket any more, the caller
                closes the connection. False: a call is still inside; the
                connection now belongs to this object, which closes it when
                the last call leaves, and the caller must NOT close it."""
                with self._state:
                    self._ended = True
                    if self._state.wait_for(lambda: not self._busy, timeout):
                        return True
                log.debug("API: the websocket reader is still waiting on a silent "
                          "peer; shutting the connection down")
                self.force_eof()
                with self._state:
                    if self._state.wait_for(lambda: not self._busy, timeout):
                        return True
                    self._close_when_idle = True
                    return False

        class _ResponseWriter(io.BufferedIOBase):
            """The handler's `wfile`: passes writes through, except while the
            handler's `discard_response` is set, when every write and flush
            is dropped (reported as written). Set for the response werkzeug
            writes after a websocket session (`send_response`)."""

            def __init__(self, inner, handler):
                super().__init__()
                self._inner = inner
                self._handler = handler

            def writable(self):
                return True

            def write(self, data):
                if self._handler.discard_response:
                    return memoryview(data).nbytes
                return self._inner.write(data)

            def flush(self):
                if not self._handler.discard_response and not self._inner.closed:
                    self._inner.flush()

            def close(self):
                # `IOBase.close` flushes first, so the inner writer must
                # still be open then; it is closed even if that flush fails
                # (`finish()` must reach `rfile.close()`, which releases the
                # socket's last io-ref so the connection's fd is closed).
                try:
                    super().close()
                finally:
                    self._inner.close()

        class ApiRequestHandler(WSGIRequestHandler):
            """Bounds the wait for a request's line and headers by an
            ABSOLUTE deadline (`API_REQUEST_IDLE_TIMEOUT`), so an idle or
            dribbling (keep-alive or post-handshake) connection gives its
            slot back. For a connection with a pinned client certificate the
            bound is lifted before the app runs; otherwise a fresh one covers
            the whole request."""

            read_deadline: Optional[float] = None
            #: The status line of this request's response is being or has
            #: been sent (`send_response`): from here on, `rfile` reads are
            #: werkzeug's post-response drain (see `_DeadlineReader`).
            response_started: bool = False
            #: The peer tore the connection down during that drain (or its
            #: deadline passed).
            drain_ended: bool = False
            #: The drain's own absolute deadline, set at its first read.
            drain_deadline: Optional[float] = None
            #: Bytes of an unread request body the drain has discarded.
            drained_bytes: int = 0
            #: Drop everything written to `wfile` (see `send_response`).
            discard_response: bool = False
            #: A websocket request's `_SessionSocket` (`make_environ`).
            session_socket: Optional[_SessionSocket] = None

            def setup(self):
                super().setup()
                # `detach` hands over the socket reader without closing it.
                self.rfile = io.BufferedReader(
                    _DeadlineReader(self.rfile.detach(), self), io.DEFAULT_BUFFER_SIZE
                )
                self.wfile = _ResponseWriter(self.wfile, self)

            def _arm(self) -> None:
                timeout = getattr(self.server, 'request_idle_timeout', None)
                self.read_deadline = (time.monotonic() + timeout) if timeout else None

            def handle_one_request(self):
                self.response_started = False
                self.drain_ended = False
                self.drain_deadline = None
                self.drained_bytes = 0
                self.discard_response = False
                self.session_socket = None
                self._arm()
                return super().handle_one_request()

            def make_environ(self):
                environ = super().make_environ()
                if self.is_websocket_request():
                    # See `_SessionSocket`; ended in `run_wsgi`.
                    self.session_socket = _SessionSocket(self.connection)
                    environ['werkzeug.socket'] = self.session_socket
                return environ

            def send_response(self, code, message=None):
                self.response_started = True
                session = self.session_socket
                if (code == 200 and self.is_websocket_request()) or (
                    session is not None and session.upgraded
                ):
                    # flask-sock's response to werkzeug once a websocket
                    # session has ended (its `101` went out on the socket
                    # directly; refusals before the upgrade are 4xx/5xx and
                    # are written) -- or ANY response after the `101`, such
                    # as werkzeug's 500 for a websocket route that raised.
                    # After a `101` it is protocol garbage inside the
                    # websocket stream, and `wfile` is the raw `SSLSocket`,
                    # outside the session's write lock: it would race
                    # simple-websocket's reader thread (once a client reset
                    # had failed the reader's `SSL_read`, the write raised
                    # `ssl.SSLError`, "Error on request"). It is logged as
                    # usual, never sent.
                    self.discard_response = True
                super().send_response(code, message)

            def start_drain_deadline(self) -> Optional[float]:
                timeout = getattr(self.server, 'drain_timeout', None)
                return (time.monotonic() + timeout) if timeout else None

            def is_websocket_request(self) -> bool:
                """A websocket upgrade request by werkzeug's own rule
                (`Map.bind_to_environ`): `Upgrade: websocket` and an
                `upgrade` token in `Connection`. Only such a request can
                match a websocket route; any other gets a plain response."""
                headers = getattr(self, 'headers', None)
                if headers is None:
                    return False

                def field(name):  # as `make_environ` builds HTTP_<NAME>
                    values = headers.get_all(name) or ()
                    return ','.join(v.replace('\r\n', '') for v in values).lower()

                if field('Upgrade') != 'websocket':
                    return False
                return any(token.strip() == 'upgrade' for token in field('Connection').split(','))

            def run_wsgi(self):
                if getattr(self.server.connection_state, 'pinned', True):
                    self.read_deadline = None
                    try:
                        self.connection.settimeout(None)
                    except OSError:
                        pass
                else:
                    self._arm()
                try:
                    return super().run_wsgi()
                finally:
                    session = self.session_socket
                    if session is not None:
                        # Before the connection is closed (`finish`,
                        # `shutdown_request`): no thread may still be inside
                        # its `SSLSocket` then (`_SessionSocket`).
                        self.close_connection = True
                        timeout = getattr(self.server, 'ws_end_timeout', API_WS_END_TIMEOUT)
                        if not session.end(timeout):
                            # The session now owns the connection and closes
                            # it when the reader leaves; the server must not.
                            self.server.connection_state.handed_over = self.connection
                            log.warning(
                                "API: a websocket reader thread from %s did not leave "
                                "its connection; it will be closed when the reader "
                                "returns", self.client_address,
                            )

            def log_error(self, format, *args):
                # An idle keep-alive connection reaching its timeout is
                # routine, not an error (`http.server` reports it this way).
                if format.startswith('Request timed out'):
                    log.debug("API: closing idle connection from %s", self.client_address)
                    return
                super().log_error(format, *args)

        class ApiServer(ThreadedWSGIServer):
            def __init__(
                self, host, port, app, gateway, *,
                max_connections: int = API_MAX_CONNECTIONS,
                handshake_timeout: float = API_HANDSHAKE_TIMEOUT,
                request_idle_timeout: float = API_REQUEST_IDLE_TIMEOUT,
                drain_timeout: float = API_DRAIN_TIMEOUT,
                max_anonymous: int = API_MAX_ANONYMOUS_CONNECTIONS,
                reject_linger: float = API_REJECT_LINGER,
                ws_end_timeout: float = API_WS_END_TIMEOUT,
            ):
                if (
                    isinstance(ws_end_timeout, bool)
                    or not isinstance(ws_end_timeout, (int, float))
                    or not math.isfinite(ws_end_timeout)
                    or ws_end_timeout <= 0
                ):
                    raise ValueError(
                        f"ws_end_timeout must be a finite number of seconds > 0, "
                        f"not {ws_end_timeout!r}"
                    )
                current = getattr(gateway, 'current_context', None)
                context = current() if callable(current) else None
                if not isinstance(context, ssl.SSLContext):
                    raise TypeError(
                        "the API server is HTTPS only: it needs a trust gateway "
                        "whose current_context() returns an ssl.SSLContext"
                    )
                self.gateway = gateway
                self.handshake_timeout = handshake_timeout
                self.request_idle_timeout = request_idle_timeout
                self.drain_timeout = drain_timeout
                self.reject_linger = reject_linger
                self.ws_end_timeout = ws_end_timeout
                self.max_connections = max_connections
                self._slots = threading.BoundedSemaphore(max_connections)
                self.max_anonymous = max_anonymous
                self._anonymous_slots = threading.BoundedSemaphore(max_anonymous)
                #: Per connection thread: `pinned` (the handler reads it).
                self.connection_state = threading.local()
                self._full_warned_at: Optional[float] = None
                self._anonymous_full_warned_at: Optional[float] = None
                # No `ssl_context` here: werkzeug would wrap the listening
                # socket (handshake on the accept thread).
                super().__init__(host, port, app, ApiRequestHandler, False, None)
                # Only for werkzeug's environ (`https`, `SSL_CLIENT_CERT`)
                # and error classification; never used to wrap a socket.
                self.ssl_context = context

            def server_bind(self):
                socketserver.TCPServer.server_bind(self)
                self.server_name = str(self.host)
                if not isinstance(self.server_address, str):  # not AF_UNIX
                    self.server_port = self.server_address[1]

            def process_request(self, request, client_address):
                # Accept thread: never block, never handshake here.
                if not self._slots.acquire(blocking=False):
                    now = time.monotonic()
                    if (
                        self._full_warned_at is None
                        or now - self._full_warned_at >= _API_FULL_WARN_EVERY
                    ):
                        self._full_warned_at = now
                        log.warning(
                            "API server: %d connections already open; refusing "
                            "new ones until some close", self.max_connections,
                        )
                    self.shutdown_request(request)
                    return
                try:
                    super().process_request(request, client_address)
                except BaseException:
                    # The thread never started: nobody else releases the slot.
                    self._slots.release()
                    raise

            def process_request_thread(self, request, client_address):
                try:
                    try:
                        context = self.gateway.current_context()
                        tls = context.wrap_socket(
                            request, server_side=True, do_handshake_on_connect=False
                        )
                    except Exception as exc:
                        log.debug("API: cannot set up TLS for %s: %s", client_address, exc)
                        self.shutdown_request(request)
                        return
                    try:
                        # One deadline for the whole handshake: CPython's
                        # `do_handshake` honours a socket timeout as a deadline.
                        tls.settimeout(self.handshake_timeout)
                        tls.do_handshake()
                        tls.settimeout(None)
                    except Exception as exc:  # SSLError, TimeoutError, OSError
                        log.debug("API: TLS handshake with %s failed: %s", client_address, exc)
                        self._close_after_failed_handshake(tls)
                        return
                    pinned = self._is_pinned(tls)
                    if not pinned and not self._anonymous_slots.acquire(blocking=False):
                        now = time.monotonic()
                        if (
                            self._anonymous_full_warned_at is None
                            or now - self._anonymous_full_warned_at >= _API_FULL_WARN_EVERY
                        ):
                            self._anonymous_full_warned_at = now
                            log.warning(
                                "API server: %d connections without a pinned client "
                                "certificate already open; refusing more until some "
                                "close", self.max_anonymous,
                            )
                        self._close_tls(tls)
                        return
                    self.connection_state.pinned = pinned
                    self.connection_state.handed_over = None
                    try:
                        super().process_request_thread(tls, client_address)
                    finally:
                        self.connection_state.pinned = True
                        self.connection_state.handed_over = None
                        if not pinned:
                            self._anonymous_slots.release()
                finally:
                    self._slots.release()

            def shutdown_request(self, request):
                # A connection a websocket session took over (its reader was
                # still inside it, `_SessionSocket.end`) is closed by the
                # session when the reader leaves -- never here, under it.
                if request is getattr(self.connection_state, 'handed_over', None):
                    return
                super().shutdown_request(request)

            def _is_pinned(self, tls) -> bool:
                """Whether the peer presented a certificate the gateway has
                pinned (route authentication decides everything else)."""
                try:
                    der = tls.getpeercert(binary_form=True)
                except Exception:
                    return False
                if not der:
                    return False
                lookup = getattr(self.gateway, 'lookup', None)
                if not callable(lookup):
                    return False
                try:
                    return lookup(hashlib.sha256(der).hexdigest()) is not None
                except Exception:
                    return False

            @staticmethod
            def _close_tls(tls) -> None:
                try:
                    tls.close()
                except OSError:
                    pass

            def _close_after_failed_handshake(self, tls) -> None:
                """Close a connection whose handshake failed so that the TLS
                alert OpenSSL sent reaches the peer.

                Under TLS 1.3 a client's handshake is complete before the
                server has checked its certificate, so the client sends its
                request right away. A rejection (an unpinned or expired
                certificate) arrives as an alert, and an immediate `close()`
                answers every byte the client sends after that with a TCP
                reset, which can discard the alert unread. The client then
                sees a reset, or, on CPython 3.11/3.12 with OpenSSL 3.0, an
                `SSLEOFError` ("EOF occurred in violation of protocol"),
                instead of the rejection. So: half-close (a FIN after the
                alert), read and discard what the peer still sends until it
                closes or `reject_linger` seconds pass, then close."""
                linger = getattr(self, 'reject_linger', None)
                if not linger:
                    self._close_tls(tls)
                    return
                try:
                    # `SSLSocket.shutdown` drops the TLS layer: from here on
                    # `recv` is the plain socket's.
                    tls.shutdown(socket.SHUT_WR)
                    deadline = time.monotonic() + linger
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        tls.settimeout(remaining)
                        if not tls.recv(65536):
                            break
                except (OSError, ValueError):
                    pass
                finally:
                    self._close_tls(tls)

        _API_SERVER_CLASS = ApiServer
    return _API_SERVER_CLASS


def _bind_api_server(app, host: str, port: int, gateway):
    """Create the HTTPS server **and bind its listening socket**, here, in the
    caller's thread. `gateway` supplies each connection's TLS context
    (`api.TrustGateway`; see `_api_server_class`); there is no plaintext
    server. Raises `OSError` (EADDRINUSE, EACCES, bad host...) if it cannot
    bind, `TypeError` without a usable gateway. Werkzeug's server binds and
    listens in its constructor, so a returned server is already accepting
    connections into its backlog. Werkzeug itself turns
    EADDRINUSE/EADDRNOTAVAIL into a printed message plus `sys.exit(1)`; that
    `SystemExit` is converted back into an `OSError` here so the caller logs
    one clear "cannot listen" line for every bind failure.

    This replaces `app.run()` in a thread, which bound inside the thread: a
    failed bind killed only that thread, leaving a worker process that was
    alive, logged "API listening", and served nothing -- so the supervisor's
    restart/backoff never applied. It also skips `Flask.run`'s extras that a
    daemon does not want (`.env` loading, the Werkzeug debugger under
    `FLASK_DEBUG`). A seam so tests need not open real sockets.
    """
    try:
        return _api_server_class()(host, port, app, gateway)
    except SystemExit as exc:
        raise OSError(f"cannot bind {host}:{port} (address in use or not available)") from exc


def _serve(server, logger) -> None:
    """Thread body: serve until the process exits. A crash is logged here and
    is noticed by `ApiWorker.run`'s wait loop via `Thread.is_alive`."""
    try:
        server.serve_forever()
    except Exception:
        logger.error("API server thread died", exc_info=True)


def _server_identity(config_db, dry_run: bool, logger):
    """The API server's TLS identity for config database `config_db`;
    -> `(identity, scratch_dir)`. Raises `IdentityError`.

    Normally `<db>.tls/server.{crt,key}`, which the supervisor created at
    startup (`daemon._ensure_server_identity`); an expired server certificate
    is used with a warning (clients ignore its expiry, A1).

    **`--dry-run` with no (or a half-written) server identity:** the
    supervisor does not create one in a dry run (a key is a persistent
    change; `daemon._ensure_server_identity`), so this worker
    makes a throwaway identity in a private temporary directory, removed when
    the worker exits (`scratch_dir`). A worker killed by a signal (SIG_DFL:
    `terminate()`, `kill -9`) cannot remove it; the directory name carries
    the worker's pid, and the next throwaway removes those whose process is
    gone (`_sweep_dry_run_dirs`, run at every API worker start). Its key is pinned by nobody either way. No client can have pinned it, and a dry-run server
    refuses enrollment (D7), so it only lets the API run over TLS as it
    would for real. Never outside a dry run: a missing identity is then an
    error (exit 1, so the supervisor's backoff applies)."""
    from zfsbackup import identity as ident

    directory = ident.tls_dir(getattr(config_db, 'resolved_path', config_db))
    cert, key = ident.identity_paths(directory, ident.SERVER_NAME)
    if dry_run and not (os.path.lexists(cert) and os.path.lexists(key)):
        import tempfile
        scratch = tempfile.mkdtemp(
            prefix=f'{_DRY_RUN_PREFIX}{_pid_namespace()}-{os.getpid()}-'
        )
        try:
            identity = ident.ensure_identity(
                os.path.join(scratch, 'server.tls'), ident.ROLE_SERVER, group_of=scratch
            )
        except BaseException:
            _remove_tree(scratch)
            raise
        logger.warning(
            f"dry run: there is no server TLS identity in {directory} and a dry "
            "run does not create one; serving with a throwaway identity "
            f"(fingerprint {identity.fingerprint}) that no client has pinned"
        )
        return identity, scratch
    return ident.load_identity(cert, key, ident.ROLE_SERVER, allow_expired=True), None


_DRY_RUN_PREFIX = 'zfsbackup-dry-run-'


def _pid_namespace() -> str:
    """An id of this process's PID namespace: the inode of
    `/proc/self/ns/pid` on Linux, `0` where there is none (macOS). A pid
    means something only within its namespace."""
    try:
        return str(os.stat('/proc/self/ns/pid').st_ino)
    except OSError:
        return '0'


def _sweep_dry_run_dirs(tmp: str) -> None:
    """Remove the throwaway identity directories of dry-run API workers that
    died without cleaning up: `<prefix><pid namespace>-<pid>-<random>`, the
    namespace this process's own and the pid no longer running. A directory
    of another PID namespace (a container sharing `/tmp`) is never touched:
    its pid cannot be checked from here. Only this user's (`mkdtemp` makes
    them 0700); best effort, never raises."""
    try:
        names = os.listdir(tmp)
    except OSError:
        return
    uid = os.geteuid()
    namespace = _pid_namespace()
    for name in names:
        if not name.startswith(_DRY_RUN_PREFIX):
            continue
        fields = name[len(_DRY_RUN_PREFIX):].split('-', 2)
        if len(fields) != 3 or fields[0] != namespace:
            continue
        pid_text = fields[1]
        if not pid_text.isdigit() or _pid_exists(int(pid_text)):
            continue
        path = os.path.join(tmp, name)
        try:
            if os.lstat(path).st_uid != uid:
                continue
        except OSError:
            continue
        _remove_tree(path)


def _remove_tree(path) -> None:
    import shutil
    shutil.rmtree(path, ignore_errors=True)


class ApiWorker(_SupervisorWatch, Process):
    """Worker that serves the HTTPS API (inspection and remote-backup receive)."""

    #: Class-level default, so a worker built by a test that bypasses
    #: `__init__` refuses to serve rather than serving without trust.
    config_db = None

    def __init__(
        self,
        config: BackupConfig,
        stop_event: StopEvent,
        verbose: bool = False,
        *,
        reload_epoch: Optional[ReloadEpoch] = None,
        born_epoch: int = 0,
        workers_lock=None,
        config_db=None,
    ):
        """`config` is the supervisor's detached `BackupConfig`, dry-run
        already applied; see `BaseWorker`.

        `config_db` is the config database's `ResolvedConfigPath`. It is used
        for TLS only: the server identity in `<db>.tls/` and the pinned
        client certificates, read through `zfsbackup.config.store.trust`
        (`api.TrustGateway`). The configuration itself is never read here.
        Without it the worker cannot serve (there is no plaintext mode) and
        exits 1."""
        super().__init__(name='api-worker', daemon=True)
        self.config = config
        self.stop_event = stop_event
        self.verbose = verbose
        self.config_db = config_db
        self._watch_init(reload_epoch, born_epoch, workers_lock)
        self._trust = None

    def _start_trust_poll(self, logger):
        """Start the trust-poll thread: `TrustGateway.poll()` every
        `api.TRUST_POLL_SECONDS` until the returned event is set. A thread of
        its own, so a slow trust read (a busy database: seconds; a hung
        filesystem: unbounded) never delays the main loop's reaction to a
        stop, a reload or a gone supervisor, and revocations keep reaching
        new connections during the drains. A daemon thread: a poll stuck in
        the kernel does not keep the process alive. The event is a
        `threading.Event` set by ordinary code on the main thread, never
        from a signal handler."""
        from zfsbackup.api import TRUST_POLL_SECONDS
        gateway = self._trust
        stopped = threading.Event()

        def loop():
            while not stopped.wait(TRUST_POLL_SECONDS):
                try:
                    gateway.poll()
                except Exception:  # poll() never raises; belt and braces
                    logger.debug("trust poll failed", exc_info=True)

        thread = threading.Thread(target=loop, name='trust-poll', daemon=True)
        thread.start()
        return stopped

    def _reload_drain_abort(self) -> bool:
        """`StreamGate.wait_idle`'s abort callback while draining for a
        reload or a restart: a stop or a gone supervisor ends the wait."""
        return self._exit_reason() in (ExitReason.STOP, ExitReason.SUPERVISOR_GONE)

    def run(self) -> None:
        """Serve until stopped or until the configuration changes. Exits with
        status 1 -- so the supervisor's crash backoff and restart apply -- if
        the server's TLS identity cannot be loaded, the listening socket
        cannot be bound or the server thread dies; a worker that is alive but
        serving nothing is the one outcome that must not happen.

        HTTPS only. Before binding, the server identity is loaded
        (`_server_identity`), a `TrustGateway` is built and read once, so a
        pinned client works from the first connection. A failed first read
        is logged and the server starts trusting nobody (fail closed) until a
        later poll succeeds. A thread of its own (`_start_trust_poll`) polls
        the gateway every `api.TRUST_POLL_SECONDS`, also during the drains,
        so a revocation or a new pin reaches new connections without a
        restart and a slow trust read never delays the main loop.

        If the server identity on disk changes under the worker (the
        gateway's `restart_reason`, set only for `identity.IdentityChanged`
        and only once the certificate on disk is confirmed to carry another
        fingerprint), it drains like for a reload and exits 1, so the
        supervisor restarts it with the new identity (a stop during that
        drain turns it into a stop). Any other failure to rebuild the TLS
        context -- the identity lock held by the CLI, EMFILE, an unreadable
        file -- leaves the gateway degraded and the trust poll retrying; it
        never restarts the worker, because each exit 1 counts toward the
        supervisor's restart limit.

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

        # The supervisor's detached `BackupConfig`. This process opens the
        # config database only through `store.trust` (trust tables, a fresh
        # unpooled engine per call), never for the configuration.
        config = self.config

        from zfsbackup.api import TrustGateway, create_app, dedupe_store_warnings
        from zfsbackup.identity import IdentityError

        if self.config_db is None:
            logger.error(
                "API worker has no config database path, so it has no TLS identity "
                "or client trust; refusing to serve (there is no plaintext mode)"
            )
            sys.exit(1)
        # The trust poll opens the database every few seconds; the store's
        # preflight would otherwise repeat the same WARNING each time.
        dedupe_store_warnings()
        # Throwaway identities of dry-run API workers that died uncleanly.
        try:
            import tempfile
            _sweep_dry_run_dirs(tempfile.gettempdir())
        except Exception:
            logger.debug("sweeping stale dry-run identities failed", exc_info=True)
        scratch = None
        try:
            identity, scratch = _server_identity(self.config_db, config.dry_run, logger)
            self._trust = TrustGateway(identity, self.config_db)
        except Exception as e:
            if scratch is not None:
                _remove_tree(scratch)
            if isinstance(e, IdentityError):
                logger.error(f"API worker cannot load the server TLS identity: {e}")
            else:
                logger.error(
                    f"API worker cannot set up its TLS identity and client trust: "
                    f"{type(e).__name__}: {e}"
                )
            sys.exit(1)
        try:
            self._serve(config, logger, create_app, identity)
        finally:
            if scratch is not None:
                _remove_tree(scratch)

    def _serve(self, config: BackupConfig, logger, create_app, identity) -> None:
        """`run()` once the server identity and the trust gateway exist."""
        gateway = self._trust
        # First read before the bind: pinned clients work from the first
        # connection. A failure is logged by the gateway; nobody is trusted
        # until a poll succeeds.
        gateway.refresh()

        app = create_app(config, gateway, identity.fingerprint)
        gate = app.extensions['zfsbackup.stream_gate']

        try:
            server = _bind_api_server(app, config.api_host, config.api_port, gateway)
        except OSError as e:
            logger.error(
                f"API worker cannot listen on {config.api_host}:{config.api_port}: {e}"
            )
            sys.exit(1)

        thread = threading.Thread(target=_serve, args=(server, logger), daemon=True)
        thread.start()
        logger.info(
            f"API listening on {config.api_host}:{getattr(server, 'port', config.api_port)} "
            f"(HTTPS; server certificate fingerprint {identity.fingerprint})"
        )

        died = False
        reason: Optional[ExitReason] = None
        #: Set when the gateway asks for a restart (`TrustGateway.
        #: restart_reason`: the server identity on disk changed, confirmed).
        restart: Optional[str] = None
        poll_stopped = self._start_trust_poll(logger)
        try:
            # Sliced wait: wakes every `STOP_POLL` to check the exit reasons,
            # that the server thread is still alive and whether the trust
            # gateway wants a restart. Trust is polled by its own thread.
            while True:
                reason = self._exit_reason()
                if reason is not None:
                    break
                if not thread.is_alive():
                    logger.error("API server thread exited unexpectedly; exiting")
                    died = True
                    break
                restart = gateway.restart_reason
                if restart is not None:
                    logger.error(
                        f"API worker restarting: {restart}. It finishes its "
                        "in-flight receives first, then exits 1 and the supervisor "
                        "starts it again with the identity now on disk"
                    )
                    break
                self.stop_event.wait(timeout=STOP_POLL)

            if not died:
                gate.start_draining()
                active = gate.active
                if reason is ExitReason.RELOAD or (reason is None and restart):
                    if active:
                        logger.info(
                            f"waiting for {active} in-flight receive(s) to finish "
                            f"before {'reloading' if reason else 'restarting'}"
                        )
                        gate.wait_idle(None, abort=self._reload_drain_abort)
                        # A stop that aborted the wait changes what we are doing.
                        reason = self._exit_reason() or reason
                if reason in (ExitReason.STOP, ExitReason.SUPERVISOR_GONE):
                    active = gate.active
                    if active and not gate.wait_idle(API_STOP_DRAIN):
                        logger.warning(
                            f"abandoning {gate.active} in-flight receive(s); the "
                            "sender resumes them (zfs receive -s)"
                        )
        finally:
            poll_stopped.set()
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

        if died or (reason is None and restart):
            sys.exit(1)
        if reason is ExitReason.RELOAD:
            logger.info(
                "API worker finished its current work; exiting to switch configuration"
            )
            sys.exit(WORKER_EXIT_RELOAD)
        logger.info("API worker stopped")

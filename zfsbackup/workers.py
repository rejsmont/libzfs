"""Worker processes for snapshot creation, pruning, API serving, and remote backup.

Workers never open the config database. The supervisor reads it once, before
the first fork (`zfsbackup.runtime_config.load_runtime_config`), applies
`--dry-run` to the result in memory, and hands every worker -- at startup and
at every crash restart -- that same detached `BackupConfig`. Under `fork` the
child inherits the object; under `spawn`/`forkserver` it is pickled
(`Duration.__reduce__` preserves the operator's literals), so a respawned
worker sees exactly the configuration its siblings run: one generation for the
whole daemon. A restart does not re-read the database; live reload is Phase 3.

Nothing that owns a SQLite handle may cross the process boundary -- never a
`Session`, an `Engine`, or an ORM instance -- and the supervisor calls
`dispose_all()` before each fork so that no inherited descriptor exists
either. A `BackupConfig` is plain dataclasses, safe on both counts.
"""

import logging
import signal
import sys
import threading
from abc import abstractmethod
from multiprocessing import Process
from multiprocessing.synchronize import Event
from typing import List, Union

from zfsbackup.backup_manager import DatasetInfo, DatasetManager
from zfsbackup.config import BackupConfig
from zfsbackup.stopflag import StopFlag

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


def _setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def _init_child_process(verbose: bool = False) -> None:
    """First statement of **every** worker `run()`. Nothing may precede it.

    Resets the `SIGINT`/`SIGTERM` dispositions this child must not own,
    ignores `SIGHUP` (the supervisor's own SIGHUP stopgap logs "live reload
    not implemented"; a worker dying on a stray hangup, or on the terminal
    closing, would just be a crash to restart), and only then configures
    logging.

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
    escalation is. The cost is that a worker interrupted mid-cycle does not
    finish it; cooperative cancellation of a long `zfs send` is item 18's,
    and it is not available today under any start method.

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


class BaseWorker(Process):
    """Base class for daemon worker processes with a shared polling loop."""

    def __init__(
        self,
        worker_name: str,
        config: BackupConfig,
        stop_event: StopEvent,
        verbose: bool = False,
    ):
        """`config` is the supervisor's detached `BackupConfig`, with
        `--dry-run` already applied. It is used as is; the worker never
        reads the config database."""
        super().__init__(name=worker_name, daemon=True)
        self.config = config
        self.stop_event = stop_event
        self.verbose = verbose

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

        while not self.stop_event.is_set():
            logger.debug(f"{self.name} cycle")
            for dsi in self._get_datasets(manager):
                if self.stop_event.is_set():
                    break
                try:
                    self._process_dataset(manager, dsi)
                except Exception as e:
                    logger.error(f"{self.name} error on {dsi.name}: {e}", exc_info=True)
            self.stop_event.wait(timeout=interval)

        logger.info(f"{self.name} stopped")


class SnapshotWorker(BaseWorker):
    """Worker that periodically checks all datasets and creates snapshots as needed."""

    def __init__(
        self,
        config: BackupConfig,
        stop_event: StopEvent,
        verbose: bool = False,
    ):
        super().__init__('snapshot-worker', config, stop_event, verbose)

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
    ):
        super().__init__('pruning-worker', config, stop_event, verbose)

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
    ):
        super().__init__('remote-backup-worker', config, stop_event, verbose)
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
        for remote_cfg in dsi.config.remote:
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


class ApiWorker(Process):
    """Worker that serves a read-only HTTP API for configuration and snapshot inspection."""

    def __init__(
        self,
        config: BackupConfig,
        stop_event: StopEvent,
        verbose: bool = False,
    ):
        """`config` is the supervisor's detached `BackupConfig`, dry-run
        already applied; see `BaseWorker`."""
        super().__init__(name='api-worker', daemon=True)
        self.config = config
        self.stop_event = stop_event
        self.verbose = verbose

    def run(self) -> None:
        _init_child_process(self.verbose)
        logger = logging.getLogger(__name__)
        logger.info("API worker started")

        # The supervisor's detached `BackupConfig`: the server thread never
        # touches a Session or an Engine, because this process has none.
        config = self.config

        from zfsbackup.api import create_app
        app = create_app(config)

        thread = threading.Thread(
            target=app.run,
            kwargs={'host': config.api_host, 'port': config.api_port, 'use_reloader': False},
            daemon=True,
        )
        thread.start()
        logger.info(f"API listening on {config.api_host}:{config.api_port}")

        self.stop_event.wait()
        logger.info("API worker stopped")

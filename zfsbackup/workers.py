"""Worker processes for snapshot creation, pruning, API serving, and remote backup.

Every worker loads its own `BackupConfig` **inside the child process**, from
the config database `resolved_config` addresses, through
`zfsbackup.runtime_config.load_runtime_config` -- read-only, schema-verified,
and with both handles closed before the loop starts. The supervisor
deliberately passes the *path*, never the loaded `BackupConfig` and never a
`Session` or `Engine`: a handle checked out before a fork is invisible to
`store/db.py`'s pid-keyed engine cache, and an inherited SQLite file
descriptor corrupts the database silently rather than raising.
"""

import logging
import signal
import sys
import threading
from abc import abstractmethod
from multiprocessing import Process
from multiprocessing.synchronize import Event
from typing import List, Optional, Union

from zfsbackup.backup_manager import DatasetInfo, DatasetManager
from zfsbackup.config import BackupConfig
from zfsbackup.config.store import ConfigPathError, ResolvedConfigPath, SchemaError
from zfsbackup.runtime_config import EX_CONFIG, config_error_message, load_runtime_config
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


def _setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def _init_child_process(verbose: bool = False) -> None:
    """First statement of **every** worker `run()`. Nothing may precede it.

    Resets the `SIGINT`/`SIGTERM` dispositions this child must not own, and
    only then configures logging.

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
    _setup_logging(verbose)


def _load_config(
    resolved_config: ResolvedConfigPath,
    dry_run: bool,
    logger: logging.Logger,
) -> Optional[BackupConfig]:
    """Load this child's config, or classify the failure.

    Three outcomes, and the distinction between the last two is the whole
    point of this function:

    - success: a detached `BackupConfig`, with `dry_run` applied in memory
      only (it is never written back to the database).
    - a **permanent** config fault (`ConfigPathError`/`SchemaError` -- a
      stale schema, a missing or malformed database, a bad path): raise
      `SystemExit(EX_CONFIG)`, so the child's exit code tells the
      supervisor that respawning it cannot help. Without that signal a
      stale schema makes every worker exit immediately and the supervisor
      re-fork each one every `WORKER_RESTART_DELAY` seconds forever, each
      re-fork reopening SQLite -- which looks like a hang rather than a
      config error.
    - anything else -- notably an `OperationalError` such as `database is
      locked` after `busy_timeout` expired under a concurrent
      `zfsbackup-config` write -- returns `None`. The caller returns from
      `run()` and the supervisor restarts the worker as it always has,
      because that failure genuinely is transient.

    Both failure arms log through `config_error_message`, so a child's
    diagnosis of a broken database is word-for-word the supervisor's.
    """
    try:
        config = load_runtime_config(resolved_config)
    except (ConfigPathError, SchemaError) as e:
        logger.error(config_error_message(resolved_config, e))
        raise SystemExit(EX_CONFIG)
    except Exception as e:
        logger.error(config_error_message(resolved_config, e))
        return None

    if dry_run:
        config.dry_run = True
    return config


class BaseWorker(Process):
    """Base class for daemon worker processes with a shared polling loop."""

    def __init__(
        self,
        worker_name: str,
        resolved_config: ResolvedConfigPath,
        stop_event: StopEvent,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        super().__init__(name=worker_name, daemon=True)
        self.resolved_config = resolved_config
        self.stop_event = stop_event
        self.dry_run = dry_run
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

        config = _load_config(self.resolved_config, self.dry_run, logger)
        if config is None:
            return

        manager = DatasetManager(config)
        interval = self._get_interval(config)
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
        resolved_config: ResolvedConfigPath,
        stop_event: StopEvent,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        super().__init__('snapshot-worker', resolved_config, stop_event, dry_run, verbose)

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
        resolved_config: ResolvedConfigPath,
        stop_event: StopEvent,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        super().__init__('pruning-worker', resolved_config, stop_event, dry_run, verbose)

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
        resolved_config: ResolvedConfigPath,
        stop_event: StopEvent,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        super().__init__('remote-backup-worker', resolved_config, stop_event, dry_run, verbose)
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
        resolved_config: ResolvedConfigPath,
        stop_event: StopEvent,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        super().__init__(name='api-worker', daemon=True)
        self.resolved_config = resolved_config
        self.stop_event = stop_event
        self.dry_run = dry_run
        self.verbose = verbose

    def run(self) -> None:
        _init_child_process(self.verbose)
        logger = logging.getLogger(__name__)
        logger.info("API worker started")

        # Loaded before the Flask thread starts, and fully closed by the
        # time it does: `create_app` gets a detached `BackupConfig`, so the
        # server thread never touches a Session or an Engine.
        config = _load_config(self.resolved_config, self.dry_run, logger)
        if config is None:
            return

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

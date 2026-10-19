"""Item 17.2 -- the supervisor's live-reload state machine.

Two layers, both in this file:

1. **In-process** (`TestReloadTriggers` ... `TestEdgeCases`): a real
   `BackupDaemon` whose collaborators are scripted by the `World` fixture
   below. Nothing forks and nothing touches SQLite or ZFS:

   - `zfsbackup.daemon.read_store_state` / `load_runtime_config` read a
     scripted "database" (`world.db_key`, `world.db_config`, and the errors
     `world.poll_error` / `world.load_error`);
   - the four worker classes are replaced by `FakeWorker`, which records the
     arguments `_new_worker` passed (config, `born_epoch`, `reload_epoch`) and
     can be told to die with any exit code;
   - `DatasetManager` is a `MagicMock`;
   - `presence_lock` is a `FakeLock` that records every `set_state` call (the
     pid file's `applied=` / `target=` / `rejected=`);
   - time is `world.clock`, installed on `BackupDaemon._monotonic` *before*
     the daemon is constructed (its first poll deadline is computed in
     `__init__`);
   - `world.events` is one ordered log of `poll`, `load`, `dispose`,
     `fork:<name>` so a test can assert what happened *between* a read and
     the next fork.

   `key=` is passed to every daemon that is meant to reload: with `key=None`
   reload is off (see `TestEdgeCases`).

2. **Real processes** (`TestRealDaemonReload`, item 20 of the plan): a real
   `python -m zfsbackup.daemon` on a database made by `zfsbackup-config
   import`, a stub `zfs`, signalled with real `SIGHUP`s. Patterned on
   `test_zfsbackup_stop_signals.py`: the daemon's output goes to a file (never
   a PIPE), it runs in its own session and the whole group is `SIGKILL`ed in a
   `finally`, so nothing outlives the test (the suite's orphan net checks).

Mutation counts (scratch copy of the tree, one revert at a time; tests failing
in this file) are in the hand-off report for item 17.2.
"""

import copy
import errno
import itertools
import logging
import os
import re
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy.exc import OperationalError

import zfsbackup.daemon as daemon_module
from zfsbackup import daemonlock
from zfsbackup.backup_manager import DatasetManager
from zfsbackup.config import (
    BackupConfig,
    DatasetConfig,
    Destination,
    RemoteDatasetConfig,
)
from zfsbackup.config.store import (
    ConfigDbNotFound,
    ConfigSnapshot,
    SchemaOutOfDate,
    StoreState,
    resolve_config_path,
)
from zfsbackup.daemon import (
    RELOAD_DEFER_NOTICE,
    RELOAD_DEFER_REPEAT,
    RELOAD_POLL_SECONDS,
    SUPERVISOR_SLEEP_SLICE,
    WORKER_MAX_CONSECUTIVE_FAILURES,
    WORKER_STABLE_UPTIME,
    BackupDaemon,
    Handoff,
    main,
)
from zfsbackup.runtime_config import config_error_message
from zfsbackup.workers import WORKER_EXIT_RELOAD

ROOT = Path(__file__).resolve().parent.parent

STORE = 1000  # the store id every scripted key uses unless a test changes it


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


def cfg(*names, remote=False, prefix="autosnap", **kwargs) -> BackupConfig:
    """A valid config. `remote=True` gives its first dataset a remote
    (so the `remote` worker is active); `prefix` varies the content."""
    names = names or ("pool/data",)
    datasets = []
    for i, name in enumerate(names):
        extra = {}
        if remote and i == 0:
            extra["remote"] = [RemoteDatasetConfig(destination="offsite")]
        datasets.append(DatasetConfig(name=name, **extra))
    kwargs.setdefault("destinations", {"offsite": Destination(url="ssh://h/pool")} if remote else {})
    return BackupConfig(datasets=datasets, snapshot_prefix=prefix, **kwargs)


class FakeLock:
    """Stands in for the held `DaemonLock`: records `set_state` keyword calls
    and the resulting `applied` / `target` / `rejected` values."""

    def __init__(self):
        self.calls = []
        self.state = {
            "applied": None, "target": None, "rejected": None,
            "deferred": None, "deferred_reason": None,
        }

    def set_state(self, **kw):
        assert set(kw) <= set(self.state), kw
        self.calls.append(dict(kw))
        self.state.update(kw)
        if kw.get("deferred", True) is None:        # like DaemonLock: no key, no reason
            self.state["deferred_reason"] = None

    def kinds(self, key):
        return [c for c in self.calls if key in c]


class FakeWorker:
    """A worker double. Constructed exactly as `_new_worker` constructs the
    real ones, so `config`, `born_epoch` and `reload_epoch` are what a real
    worker would have been born with."""

    _pids = itertools.count(5000)

    def __init__(self, name, world, config, stop_event, verbose=False, *,
                 reload_epoch=None, born_epoch=0, workers_lock=None):
        self.name = name
        self.world = world
        self.config = config
        self.stop_event = stop_event
        self.verbose = verbose
        self.reload_epoch = reload_epoch
        self.born_epoch = born_epoch
        self.workers_lock = workers_lock
        self.pid = None
        self._alive = False
        self.exitcode = None
        world.made.append(self)

    def start(self):
        self.pid = next(self._pids)
        self._alive = True
        self.world.events.append(f"fork:{self.name}")

    def is_alive(self):
        return self._alive

    def die(self, code):
        self._alive = False
        self.exitcode = code

    def join(self, timeout=None):
        if self._alive:
            self.die(0)

    def terminate(self):
        self.die(-signal.SIGTERM)

    def kill(self):
        self.die(-signal.SIGKILL)


class World:
    """The scripted environment around one `BackupDaemon`."""

    def __init__(self, mocker, tmp_path, *, config=None, key=(STORE, 0),
                 dry_run=False, with_lock=True, daemon_key="same"):
        self.events = []
        self.made = []
        self.clock = [1000.0]
        self.db_key = key
        self.db_config = config if config is not None else cfg()
        self.load_error = None
        self.poll_error = None
        self.on_load = None     # called inside the scripted read, before it returns
        self.on_poll = None     # likewise, inside the scripted key read
        self.lock = FakeLock() if with_lock else None

        world = self

        def read_store_state(resolved):
            world.events.append("poll")
            if world.on_poll is not None:
                world.on_poll()
            if world.poll_error is not None:
                raise world.poll_error
            return StoreState(store_id=world.db_key[0], generation=world.db_key[1])

        def load_runtime_config(resolved):
            world.events.append("load")
            if world.on_load is not None:
                world.on_load()
            if world.load_error is not None:
                raise world.load_error
            return ConfigSnapshot(
                copy.deepcopy(world.db_config), world.db_key[1], "rev", world.db_key[0]
            )

        mocker.patch("zfsbackup.daemon.read_store_state", side_effect=read_store_state)
        mocker.patch("zfsbackup.daemon.load_runtime_config", side_effect=load_runtime_config)
        self.dispose = mocker.patch(
            "zfsbackup.daemon.dispose_all", side_effect=lambda: world.events.append("dispose")
        )
        self.manager_cls = mocker.patch("zfsbackup.daemon.DatasetManager")
        self.manager = self.manager_cls.return_value
        for name, attr in (
            ("snapshot", "SnapshotWorker"), ("pruning", "PruningWorker"),
            ("api", "ApiWorker"), ("remote", "RemoteBackupWorker"),
        ):
            mocker.patch(
                f"zfsbackup.daemon.{attr}",
                side_effect=lambda *a, _n=name, **k: FakeWorker(_n, world, *a, **k),
            )
        mocker.patch.object(BackupDaemon, "_monotonic", lambda self: world.clock[0])

        running = copy.deepcopy(self.db_config)
        if dry_run:
            running.dry_run = True
        self.running_config = running
        self.resolved = resolve_config_path(str(tmp_path / "w" / "config.db"))
        self.daemon = BackupDaemon(
            running,
            self.resolved,
            key=key if daemon_key == "same" else daemon_key,
            presence_lock=self.lock,
            dry_run_override=dry_run,
        )

    # -- driving -----------------------------------------------------------

    def advance(self, seconds):
        self.clock[0] += seconds

    def start_workers(self):
        self.daemon._start_workers()
        return dict(self.daemon._workers)

    def hup(self):
        self.daemon._sighup_handler(signal.SIGHUP, None)
        self.daemon._reload_tick()

    def poll(self):
        self.advance(RELOAD_POLL_SECONDS)
        self.daemon._reload_tick()

    def write(self, key, config=None, **cfg_kwargs):
        """The database now holds `key` (and `config`)."""
        self.db_key = key
        if config is not None:
            self.db_config = config
        elif cfg_kwargs:
            self.db_config = cfg(**cfg_kwargs)

    def forks(self):
        return [e for e in self.events if e.startswith("fork:")]

    def workers_made(self, name):
        return [w for w in self.made if w.name == name]


@pytest.fixture(autouse=True)
def _restore_signal_handlers():
    """`BackupDaemon.__init__` installs SIGINT/SIGTERM/SIGHUP handlers."""
    saved = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    yield
    for sig, handler in saved.items():
        signal.signal(sig, handler)


@pytest.fixture(autouse=True)
def _debug_logging(caplog):
    caplog.set_level(logging.DEBUG, logger="zfsbackup.daemon")

@pytest.fixture(autouse=True)
def _private_home(monkeypatch, tmp_path_factory):
    """The default client id file lives under `$HOME`; the pre-acceptance
    check of a newly needed remote worker (B3) reads it. Point `HOME` at a
    private writable directory so no test depends on the developer's own."""
    monkeypatch.setenv("HOME", str(tmp_path_factory.mktemp("home")))


@pytest.fixture
def world(mocker, tmp_path):
    return World(mocker, tmp_path)


def messages(caplog, level=None, containing=None):
    out = [
        r.getMessage() for r in caplog.records
        if r.name == "zfsbackup.daemon"
        and (level is None or r.levelno == level)
    ]
    return [m for m in out if containing is None or containing in m]


#: What `config_error_message(..., reloading=True)` appends to a SchemaOutOfDate.
#: A migration changes neither the store id nor the generation, so the poll
#: relies on retrying the failed read: a SchemaError never sets `_poll_skip`.
RELOAD_NOTE = (
    "The daemon only checks the schema and never migrates it; run the command above, "
    f"and the running daemon picks the configuration up at its next check "
    f"(within {RELOAD_POLL_SECONDS}s)."
)


def exited_for_reload(workers):
    for w in workers.values():
        w.die(WORKER_EXIT_RELOAD)


# ---------------------------------------------------------------------------
# Signals and triggers
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestReloadTriggers:
    def test_the_sighup_handler_is_one_store_and_reads_nothing(self, world, caplog):
        world.start_workers()
        world.events.clear()
        caplog.clear()
        world.daemon._sighup_handler(signal.SIGHUP, None)
        assert world.daemon._hup_requested is True
        assert not world.daemon._stop_event.is_set()
        assert world.events == []          # no poll, no load, no dispose, no fork
        assert caplog.records == []        # and no logging from a handler

    def test_supervisor_sleep_returns_within_one_slice_of_a_hup(self, world, mocker):
        calls = []

        def fake_sleep(seconds):
            calls.append(seconds)
            assert seconds <= SUPERVISOR_SLEEP_SLICE
            if len(calls) == 2:
                world.daemon._sighup_handler(signal.SIGHUP, None)

        mocker.patch("zfsbackup.daemon.time.sleep", side_effect=fake_sleep)
        world.daemon._supervisor_sleep(5)
        # The slice in which the HUP landed was the last one.
        assert len(calls) == 2
        # The sleep did not consume the flag: the tick does.
        assert world.daemon._hup_requested is True

    def test_supervisor_sleep_does_not_sleep_when_a_hup_is_already_pending(self, world, mocker):
        sleep = mocker.patch("zfsbackup.daemon.time.sleep")
        world.daemon._hup_requested = True
        world.daemon._supervisor_sleep(5)
        sleep.assert_not_called()

    def test_a_forced_read_with_an_unchanged_key_logs_unchanged_and_touches_no_worker(
        self, world, caplog
    ):
        before = world.start_workers()
        handoff = world.daemon._handoff
        world.events.clear()
        world.hup()
        assert messages(caplog, logging.INFO, "SIGHUP: configuration unchanged (generation 0)")
        assert world.events == ["load", "dispose"]
        assert world.daemon._handoff is handoff
        assert world.daemon._reload_epoch.get() == 0
        assert world.daemon._workers == before
        assert len(world.made) == len(before)
        assert world.lock.kinds("target") == [] and world.lock.kinds("rejected") == []
        assert not world.manager_cls.call_count   # no DatasetManager: nothing was prepared

    def test_the_hup_flag_is_cleared_by_the_tick(self, world):
        world.hup()
        assert world.daemon._hup_requested is False

    def test_the_flag_is_cleared_before_the_read(self, world):
        """A SIGHUP landing during the read must force another tick, so the
        flag cannot be cleared after the read."""
        seen = []
        world.on_load = lambda: seen.append(world.daemon._hup_requested)
        world.daemon._sighup_handler(signal.SIGHUP, None)
        world.daemon._reload_tick()
        assert seen == [False]

    def test_a_hup_landing_during_the_read_forces_another_tick(self, world):
        world.on_load = lambda: world.daemon._sighup_handler(signal.SIGHUP, None)
        world.hup()
        assert world.daemon._hup_requested is True

    @pytest.mark.parametrize(
        "new_key",
        [(STORE, 1), (STORE + 1, 0)],
        ids=["generation-change", "store-id-change-at-the-same-generation"],
    )
    def test_the_poll_triggers_on_a_changed_key(self, world, caplog, new_key):
        world.start_workers()
        world.write(new_key, prefix="changed")
        world.poll()
        assert messages(caplog, logging.INFO, f"configuration generation {new_key[1]} accepted")
        assert messages(caplog, logging.INFO, "trigger: poll")
        assert world.daemon._handoff.key == new_key
        assert world.daemon._handoff.config.snapshot_prefix == "changed"
        assert world.daemon._handoff.epoch == 1
        assert world.daemon._reload_epoch.get() == 1

    def test_the_poll_is_not_due_until_the_interval_has_passed(self, world):
        world.write((STORE, 1), prefix="changed")
        world.advance(RELOAD_POLL_SECONDS - 0.5)
        world.daemon._reload_tick()
        assert world.events == []
        world.advance(0.5)
        world.daemon._reload_tick()
        assert world.events[:2] == ["poll", "dispose"]

    def test_the_poll_with_an_unchanged_key_reads_only_the_key(self, world):
        world.poll()
        world.poll()
        assert world.events == ["poll", "dispose", "poll", "dispose"]
        assert "load" not in world.events
        assert world.daemon._handoff.epoch == 0

    def test_an_invalid_candidate_is_not_re_polled_but_a_sighup_retries_it(self, world, caplog):
        world.start_workers()
        world.write((STORE, 1), config=BackupConfig(datasets=[]))   # "No datasets configured"
        world.poll()
        assert world.daemon._rejected == (STORE, 1)
        assert world.events.count("load") == 1

        world.poll()
        world.poll()
        assert world.events.count("load") == 1          # the poll skips a rejected key
        assert world.events.count("poll") == 3

        world.hup()                                     # SIGHUP forces a retry...
        assert world.events.count("load") == 2
        assert world.daemon._rejected == (STORE, 1)     # ...which is refused again
        # The retry is still ONE ERROR line for the same (key, message).
        assert len(messages(caplog, logging.ERROR, "rejected")) == 1

    def test_a_rejected_key_that_is_fixed_in_place_is_picked_up_by_a_hup(self, world):
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.poll()
        world.db_config = cfg(prefix="fixed")           # same key, now valid (raw edit)
        world.poll()
        assert world.daemon._handoff.key == (STORE, 0)  # poll does not retry it
        world.hup()
        assert world.daemon._handoff.key == (STORE, 1)
        assert world.daemon._rejected is None


# ---------------------------------------------------------------------------
# Rejection and poll failures
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRejection:
    def _assert_nothing_changed(self, world, before, epoch=0):
        assert world.daemon._workers == before
        assert [w.pid for w in world.daemon._workers.values()] == [w.pid for w in before.values()]
        assert len(world.made) == len(before)
        assert world.daemon._handoff.epoch == epoch
        assert world.daemon._reload_epoch.get() == epoch
        assert not world.manager_cls.call_count

    def test_a_validate_config_value_error_is_rejected_with_the_key_recorded(
        self, world, caplog
    ):
        before = world.start_workers()
        world.write((STORE, 7), config=BackupConfig(datasets=[]))
        world.hup()
        self._assert_nothing_changed(world, before)
        errors = messages(caplog, logging.ERROR)
        assert len(errors) == 1
        # The clause first; the store's message last and verbatim.
        assert errors[0].startswith(
            "configuration generation 7 rejected (trigger: SIGHUP); "
            "still running generation 0:\n"
        )
        assert "No datasets configured" in errors[0]
        assert world.daemon._rejected == (STORE, 7)
        assert world.lock.state["rejected"] == (STORE, 7)
        assert world.lock.state["target"] is None       # never accepted
        # Diagnosed: no traceback.
        assert all(not r.exc_info for r in caplog.records if r.levelno == logging.ERROR)

    def test_a_schema_out_of_date_read_is_rejected_with_the_reload_wording(self, world, caplog):
        before = world.start_workers()
        world.load_error = SchemaOutOfDate("The config database is at an older schema.\nRun it.")
        world.hup()
        self._assert_nothing_changed(world, before)
        error = messages(caplog, logging.ERROR)[0]
        assert error.startswith("configuration generation unknown rejected (trigger: SIGHUP)")
        assert "older schema" in error
        assert error.endswith(RELOAD_NOTE)
        assert "start the daemon again" not in error
        # The key is unknown, so there is nothing to record or to suppress.
        assert world.daemon._rejected is None
        assert world.lock.kinds("rejected") == []

    def test_a_missing_file_is_rejected_and_the_daemon_keeps_running(self, world, caplog):
        before = world.start_workers()
        world.load_error = ConfigDbNotFound("No config database exists at x")
        world.hup()
        self._assert_nothing_changed(world, before)
        assert "No config database exists at x" in messages(caplog, logging.ERROR)[0]
        assert world.lock.kinds("rejected") == []
        assert not world.daemon._stop_event.is_set()

    def test_a_failed_read_with_an_unknown_key_triggered_by_a_poll_is_not_re_read_until_the_key_changes(
        self, world, caplog
    ):
        """The poll knows the stored key but the failed read could not learn
        its own (an unloadable row, "No datasets configured"): the poll must
        not fully re-read a broken store every interval."""
        world.write((STORE, 1), prefix="changed")
        world.load_error = ValueError("No datasets configured")
        world.poll()
        assert world.daemon._poll_skip == (STORE, 1)
        world.poll()
        world.poll()
        assert world.events.count("load") == 1          # no full read
        assert world.events.count("poll") == 3          # the cheap key read still runs
        assert len(messages(caplog, logging.ERROR, "rejected")) == 1
        # Never published: the key is the poll's, not a candidate's.
        assert world.daemon._rejected is None
        assert world.lock.kinds("rejected") == []

        world.hup()                                     # a SIGHUP reads again
        assert world.events.count("load") == 2
        assert len(messages(caplog, logging.ERROR, "rejected")) == 1   # still deduplicated

        world.write((STORE, 2), prefix="newer")        # the stored key moved on: read again
        world.poll()
        assert world.events.count("load") == 3

    def test_a_stale_schema_is_retried_at_every_poll_because_migrate_changes_no_key(
        self, world, caplog
    ):
        """`zfsbackup-config migrate` changes neither the store id nor the
        generation, so the poll must keep retrying a schema failure; the first
        poll after the migration applies the configuration with no SIGHUP."""
        world.write((STORE, 1), prefix="changed")
        world.load_error = SchemaOutOfDate("older")
        world.poll()
        world.poll()
        world.poll()
        assert world.daemon._poll_skip is None
        assert world.events.count("load") == 3
        assert len(messages(caplog, logging.ERROR, "rejected")) == 1     # deduplicated
        assert len(messages(caplog, logging.DEBUG, "rejected")) == 2     # the repeats
        world.load_error = None                          # the operator ran `migrate`
        world.poll()
        assert world.daemon._handoff.key == (STORE, 1)

    def test_a_poll_skip_is_cleared_by_the_next_accepted_reload(self, world):
        world.write((STORE, 1), prefix="changed")
        world.load_error = ValueError("No datasets configured")
        world.poll()
        assert world.daemon._poll_skip == (STORE, 1)
        world.load_error = None                         # repaired in place, then SIGHUP
        world.hup()
        assert world.daemon._handoff.key == (STORE, 1)
        assert world.daemon._poll_skip is None

    def test_a_transient_operational_error_is_not_skipped_so_the_next_poll_retries(
        self, world
    ):
        world.write((STORE, 1), prefix="changed")
        world.load_error = OperationalError("S", {}, Exception("database is locked"))
        world.poll()
        assert world.daemon._poll_skip is None
        world.poll()
        world.poll()
        assert world.events.count("load") == 3
        world.load_error = None
        world.poll()
        assert world.daemon._handoff.key == (STORE, 1)

    def test_a_sighup_triggered_failure_sets_no_poll_skip(self, world):
        world.write((STORE, 1), prefix="changed")
        world.load_error = ValueError("No datasets configured")
        world.hup()
        assert world.daemon._poll_skip is None
        world.poll()                                  # so the poll does read it
        assert world.events.count("load") == 2

    def test_a_candidate_with_a_known_key_is_rejected_not_poll_skipped(self, world):
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.poll()
        assert world.daemon._rejected == (STORE, 1) and world.daemon._poll_skip is None

    def test_a_dataset_list_that_cannot_be_built_is_rejected_with_its_key(
        self, world, mocker, caplog
    ):
        before = world.start_workers()
        build = mocker.patch(
            "zfsbackup.daemon._build_manager",
            side_effect=ValueError("pool/ lead is not a valid ZFS dataset name"),
        )
        world.write((STORE, 3), prefix="changed")
        world.hup()
        build.assert_called_once()
        self._assert_nothing_changed(world, before)
        error = messages(caplog, logging.ERROR)[0]
        assert error.startswith(
            "configuration generation 3 rejected (trigger: SIGHUP); "
            "still running generation 0:\n"
        )
        assert "its dataset list cannot be used: pool/ lead is not a valid" in error
        assert world.daemon._rejected == (STORE, 3)
        assert world.lock.state["rejected"] == (STORE, 3)
        assert world.lock.state["target"] is None       # refused before it was accepted
        assert not messages(caplog, logging.INFO, "accepted")
        world.manager.verify_datasets.assert_not_called()
        world.manager.sync_all_config_properties.assert_not_called()
        world.poll()
        assert build.call_count == 1                    # and the poll does not retry it

    def test_a_dataset_list_that_cannot_be_built_is_a_real_refusal_for_a_bad_name(
        self, world, caplog
    ):
        """No mock of the build: `_build_manager` is pure and runs for real
        (the `DatasetManager` class is replaced here, so rebuild it)."""
        from zfsbackup.daemon import _build_manager
        from zfsbackup.backup_manager import DatasetManager as Real

        with patch("zfsbackup.daemon.DatasetManager", Real):
            # A space is valid INSIDE a component (libzfseasy accepts it now);
            # leading/trailing space and non-ASCII are still refused.
            for bad in ("pool/ lead", "pool/trail ", "pool/d\u015b", "pool/x\n"):
                with pytest.raises(ValueError, match="not a valid ZFS dataset name"):
                    _build_manager(cfg(bad))
            _build_manager(cfg("pool/fine"))
            _build_manager(cfg("pool/has space"))

    def test_the_error_log_is_deduplicated_by_key_and_message(self, world, caplog):
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.hup()
        world.hup()
        world.hup()
        assert len(messages(caplog, logging.ERROR, "rejected")) == 1
        # A different key with the same message is a new rejection...
        world.write((STORE, 2), config=BackupConfig(datasets=[]))
        world.hup()
        assert len(messages(caplog, logging.ERROR, "rejected")) == 2
        # ...and so is the same key with a different message.
        world.load_error = SchemaOutOfDate("older")
        world.hup()
        assert len(messages(caplog, logging.ERROR, "rejected")) == 3

    def test_the_rejected_key_is_recorded_once(self, world):
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.hup()
        world.hup()
        assert world.lock.kinds("rejected") == [{"rejected": (STORE, 1)}]

    def test_a_rejected_candidate_never_forks_or_bumps_the_epoch(self, world):
        world.start_workers()
        world.events.clear()
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.hup()
        assert world.forks() == []
        assert world.daemon._reload_epoch.get() == 0

    def test_rejected_is_cleared_on_the_next_accepted_reload(self, world):
        world.start_workers()                            # so the reload stays pending
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.hup()
        assert world.lock.state["rejected"] == (STORE, 1)
        world.write((STORE, 2), config=cfg(prefix="good"))
        world.hup()
        assert world.daemon._rejected is None
        assert world.lock.state["rejected"] is None
        assert world.lock.state["target"] == (STORE, 2)
        # One call clears the old rejection and records the target together.
        accepted = [c for c in world.lock.calls if c.get("target") == (STORE, 2)]
        assert accepted == [{"target": (STORE, 2), "rejected": None}]

    def test_after_an_accepted_reload_a_new_rejection_is_logged_again(self, world, caplog):
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.hup()
        world.write((STORE, 2), config=cfg(prefix="good"))
        world.hup()
        world.write((STORE, 3), config=BackupConfig(datasets=[]))
        world.hup()
        assert len(messages(caplog, logging.ERROR, "rejected")) == 2

    def test_an_unexpected_exception_is_rejected_with_a_traceback(self, world, caplog):
        """A bug (not a diagnosed fault) must be undiagnosable-proof."""
        world.load_error = KeyError("boom")
        world.hup()
        records = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(records) == 1 and records[0].exc_info is not None
        assert not world.daemon._stop_event.is_set()


@pytest.mark.unit
class TestPollFailures:
    def test_a_poll_failure_logs_warning_then_debug_then_info_when_it_clears(
        self, world, caplog
    ):
        before = world.start_workers()
        handoff = world.daemon._handoff
        world.poll_error = OperationalError("S", {}, Exception("database is locked"))

        world.poll()
        warn = messages(caplog, logging.WARNING, "cannot check the configuration database")
        assert len(warn) == 1 and warn[0].startswith(
            "cannot check the configuration database for changes; "
            "still running generation 0:\n"
        )

        world.poll()
        world.poll()
        assert len(messages(caplog, logging.WARNING, "cannot check")) == 1
        assert len(messages(caplog, logging.DEBUG, "cannot check")) == 2

        world.poll_error = None
        world.poll()
        info = messages(caplog, logging.INFO, "can be checked for changes again")
        assert info == [
            "the configuration database can be checked for changes again; "
            "running generation 0"
        ]
        # The daemon kept running its generation throughout.
        assert world.daemon._handoff is handoff
        assert world.daemon._workers == before
        assert not world.daemon._stop_event.is_set()
        # A further successful poll is silent about it.
        world.poll()
        assert len(messages(caplog, logging.INFO, "can be checked for changes again")) == 1

    def test_a_changed_message_warns_again_and_a_new_failure_after_a_clear_does_too(
        self, world, caplog
    ):
        world.poll_error = ValueError("first")
        world.poll()
        world.poll_error = ValueError("second")
        world.poll()
        assert len(messages(caplog, logging.WARNING, "cannot check")) == 2
        world.poll_error = None
        world.poll()
        world.poll_error = ValueError("second")
        world.poll()
        assert len(messages(caplog, logging.WARNING, "cannot check")) == 3

    def test_a_poll_failure_never_reads_the_full_config_or_forks(self, world):
        world.start_workers()
        world.events.clear()
        world.poll_error = RuntimeError("disk gone")
        world.poll()
        assert world.events == ["poll", "dispose"]

    def test_the_poll_failure_names_the_generation_after_a_reload(self, world, caplog):
        world.write((STORE, 4), prefix="changed")
        world.poll()
        world.poll_error = RuntimeError("x")
        world.poll()
        assert "still running generation 4:\n" in messages(
            caplog, logging.WARNING, "cannot check"
        )[0]

    def test_a_diagnosed_poll_failure_has_no_traceback_and_an_unexpected_one_has(
        self, world, caplog
    ):
        world.poll_error = OperationalError("S", {}, Exception("database is locked"))
        world.poll()
        world.poll_error = RuntimeError("a bug")
        world.poll()
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 2
        assert not warnings[0].exc_info and warnings[1].exc_info

    def test_an_unexpected_poll_failure_warns_once_with_a_traceback_then_goes_quiet(
        self, world, caplog
    ):
        world.poll_error = RuntimeError("a bug")
        world.poll()
        world.poll()
        world.poll()
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1 and warnings[0].exc_info
        assert len(messages(caplog, logging.DEBUG, "cannot check")) == 2
        assert not world.daemon._stop_event.is_set()

    def test_the_stores_message_comes_last_and_verbatim_in_a_poll_failure(self, world, caplog):
        world.poll_error = SchemaOutOfDate("older schema\nRun: zfsbackup-config migrate")
        world.poll()
        warn = messages(caplog, logging.WARNING)[0]
        assert warn.endswith("Run: zfsbackup-config migrate\n" + RELOAD_NOTE)

    def test_a_sighup_still_reads_when_the_poll_is_failing(self, world, caplog):
        world.poll_error = RuntimeError("x")
        world.poll()
        world.write((STORE, 1), prefix="changed")
        world.hup()          # forced: the poll's key read is skipped
        assert world.daemon._handoff.key == (STORE, 1)


# ---------------------------------------------------------------------------
# dispose_all after every read
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDisposeAfterEveryRead:
    def _each_read_is_followed_by_dispose(self, events):
        for i, event in enumerate(events):
            if event in ("poll", "load"):
                assert events[i + 1:i + 2] == ["dispose"], events

    def test_after_a_poll_that_returns(self, world):
        world.poll()
        assert world.events == ["poll", "dispose"]

    def test_after_a_poll_that_raises(self, world):
        world.poll_error = RuntimeError("x")
        world.poll()
        assert world.events == ["poll", "dispose"]

    def test_after_a_reload_read(self, world):
        world.write((STORE, 1), prefix="changed")
        world.hup()
        assert world.events[:2] == ["load", "dispose"]

    def test_after_a_reload_read_that_raises(self, world):
        world.load_error = SchemaOutOfDate("older")
        world.hup()
        assert world.events == ["load", "dispose"]

    def test_after_a_reload_whose_validation_raises(self, world):
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.hup()
        assert world.events == ["load", "dispose"]

    def test_a_poll_then_a_full_read_dispose_each_time(self, world):
        world.write((STORE, 1), prefix="changed")
        world.poll()
        assert world.events[:4] == ["poll", "dispose", "load", "dispose"]

    def test_every_read_in_a_long_session_is_followed_by_a_dispose(self, world):
        world.start_workers()
        world.write((STORE, 1), prefix="a")
        world.poll()
        world.poll_error = RuntimeError("x")
        world.poll()
        world.poll_error = None
        world.write((STORE, 2), config=BackupConfig(datasets=[]))
        world.poll()
        world.load_error = SchemaOutOfDate("older")
        world.hup()
        self._each_read_is_followed_by_dispose(world.events)

    def test_a_spawn_after_a_reload_is_preceded_by_a_dispose_and_no_session_is_open(self, world):
        world.write((STORE, 1), config=cfg(remote=True))
        world.hup()               # nothing running yet: the new remote worker is spawned
        forks = [i for i, e in enumerate(world.events) if e.startswith("fork:")]
        assert forks
        for i in forks:
            assert world.events[i - 1] == "dispose"


# ---------------------------------------------------------------------------
# --dry-run
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDryRunSurvivesAReload:
    def test_the_reloaded_config_is_dry_run_when_the_daemon_was_started_with_the_flag(
        self, mocker, tmp_path, caplog
    ):
        w = World(mocker, tmp_path, dry_run=True)
        assert w.db_config.dry_run is False        # the stored config says nothing
        w.start_workers()
        w.write((STORE, 1), prefix="changed")
        w.hup()
        assert w.daemon.config.dry_run is True
        assert w.daemon._handoff.config.snapshot_prefix == "changed"
        # And so is the config the preparation step and the respawned workers get.
        assert w.manager_cls.call_args.args[0].dry_run is True
        exited_for_reload(w.daemon._workers)
        w.daemon._check_workers()
        assert all(x.config.dry_run is True for x in w.made[-3:])

    def test_a_daemon_started_without_the_flag_does_not_gain_it(self, world):
        world.write((STORE, 1), prefix="changed")
        world.hup()
        assert world.daemon.config.dry_run is False

    def test_dry_run_survives_two_reloads(self, mocker, tmp_path):
        w = World(mocker, tmp_path, dry_run=True)
        for gen in (1, 2):
            w.write((STORE, gen), prefix=f"p{gen}")
            w.hup()
            assert w.daemon.config.dry_run is True


# ---------------------------------------------------------------------------
# Hand-over
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestHandoff:
    def test_handoff_is_a_frozen_triple(self):
        h = Handoff(cfg(), 0, (1, 2))
        assert (h.epoch, h.key) == (0, (1, 2))
        with pytest.raises(Exception):
            h.epoch = 5  # type: ignore[misc]

    def test_config_is_a_read_only_view_of_the_handoff(self, world):
        assert world.daemon.config is world.daemon._handoff.config
        with pytest.raises(AttributeError):
            world.daemon.config = cfg()  # type: ignore[misc]

    def test_every_spawn_gets_config_and_born_epoch_from_the_same_handoff(self, world):
        first = world.start_workers()
        assert {w.born_epoch for w in first.values()} == {0}
        assert all(w.config is world.running_config for w in first.values())

        world.write((STORE, 1), prefix="changed")
        world.hup()
        new_config = world.daemon._handoff.config
        assert new_config is not world.running_config
        exited_for_reload(first)
        world.daemon._check_workers()
        second = world.made[len(first):]
        assert len(second) == len(first)
        assert {w.born_epoch for w in second} == {1}
        assert all(w.config is new_config for w in second)
        assert all(w.reload_epoch is world.daemon._reload_epoch for w in second)
        assert {w.name: world.daemon._worker_epoch[w.name] for w in second} == {
            w.name: 1 for w in second
        }

    def test_a_crash_restart_gets_the_current_handoff_not_the_dead_workers(self, world):
        first = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        first["snapshot"].die(1)          # crashed (not a reload exit) mid-drain
        world.daemon._check_workers()
        world.advance(60)
        world.daemon._check_workers()
        restarted = world.workers_made("snapshot")[-1]
        assert restarted is not first["snapshot"]
        assert restarted.born_epoch == 1
        assert restarted.config is world.daemon._handoff.config

    def test_the_handoff_is_replaced_before_the_shared_epoch_is_bumped(self, world):
        """A worker that observes the new epoch (and a respawn that follows)
        must find the matching config: the swap comes first."""
        real = world.daemon._reload_epoch
        seen = []

        class SpyEpoch:
            def get(self):
                return real.get()

            def set(self, value):
                # What a worker spawned the instant after this store is born with:
                born = world.daemon._new_worker("snapshot")
                seen.append((value, born.born_epoch, born.config.snapshot_prefix))
                real.set(value)

        world.daemon._reload_epoch = SpyEpoch()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        assert seen == [(1, 1, "changed")]

    def test_the_epoch_equals_the_handoff_epoch_after_every_reload(self, world):
        for gen in (1, 2, 3):
            world.write((STORE, gen), prefix=f"p{gen}")
            world.hup()
            assert world.daemon._handoff.epoch == world.daemon._reload_epoch.get() == gen

    def test_a_rejected_candidate_leaves_the_handoff_object_untouched(self, world):
        handoff = world.daemon._handoff
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.hup()
        assert world.daemon._handoff is handoff


# ---------------------------------------------------------------------------
# The worker set
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestWorkerSet:
    def test_a_reload_that_adds_a_remote_starts_the_remote_worker_and_touches_no_other(
        self, world
    ):
        before = world.start_workers()
        assert "remote" not in before
        world.events.clear()
        world.write((STORE, 1), config=cfg(remote=True))
        world.hup()
        assert world.forks() == ["fork:remote"]
        assert world.daemon._workers["remote"].born_epoch == 1
        assert world.daemon._workers["remote"].config is world.daemon._handoff.config
        for name, w in before.items():
            assert world.daemon._workers[name] is w and w.is_alive()

    def test_a_reload_that_removes_the_remote_lets_the_worker_drain_then_retires_it(
        self, mocker, tmp_path, caplog
    ):
        w = World(mocker, tmp_path, config=cfg(remote=True))
        workers = w.start_workers()
        assert set(workers) == {"snapshot", "pruning", "api", "remote"}
        w.write((STORE, 1), config=cfg())
        w.hup()
        # Draining, not killed, not retired yet.
        assert w.daemon._workers["remote"] is workers["remote"]
        assert workers["remote"].is_alive()
        assert w.forks() == [f"fork:{n}" for n in workers]    # nothing new

        exited_for_reload(workers)
        w.daemon._check_workers()
        assert "remote" not in w.daemon._workers
        assert messages(caplog, logging.INFO, "remote worker retired: not needed by generation 1")
        assert len(w.workers_made("remote")) == 1             # never respawned
        assert [x.name for x in w.made[len(workers):]] == ["snapshot", "pruning", "api"]
        assert "remote" not in w.daemon._failures

    def test_a_backoff_pending_worker_that_is_no_longer_active_is_retired_at_once(
        self, mocker, tmp_path, caplog
    ):
        w = World(mocker, tmp_path, config=cfg(remote=True))
        workers = w.start_workers()
        workers["remote"].die(1)
        w.daemon._check_workers()
        assert "remote" in w.daemon._restart_at and w.daemon._failures["remote"] == 1

        w.write((STORE, 1), config=cfg())
        w.hup()
        assert "remote" not in w.daemon._restart_at
        assert "remote" not in w.daemon._workers
        assert "remote" not in w.daemon._failures
        assert messages(caplog, logging.INFO, "remote worker retired: not needed by generation 1")
        w.advance(10_000)
        w.daemon._check_workers()
        assert len(w.workers_made("remote")) == 1             # the backoff was dropped

    def test_a_backoff_pending_worker_that_is_still_active_is_left_to_its_backoff(self, world):
        workers = world.start_workers()
        workers["snapshot"].die(1)
        world.daemon._check_workers()
        restart_at = world.daemon._restart_at["snapshot"]
        world.events.clear()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        assert world.forks() == []                            # not respawned early
        assert world.daemon._restart_at == {"snapshot": restart_at}
        world.advance(60)
        world.daemon._check_workers()
        assert world.workers_made("snapshot")[-1].born_epoch == 1
        assert world.daemon._failures["snapshot"] == 1        # the crash still counts

    def test_adjust_worker_set_never_spawns_a_name_already_in_workers(self, world):
        """No second ApiWorker beside the draining first one (the API port)."""
        workers = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        world.daemon._adjust_worker_set()
        world.daemon._adjust_worker_set()
        assert len(world.workers_made("api")) == 1
        # Even once it has exited but has not been classified yet.
        workers["api"].die(WORKER_EXIT_RELOAD)
        world.daemon._adjust_worker_set()
        assert len(world.workers_made("api")) == 1
        assert world.daemon._workers["api"] is workers["api"]

    def test_adjust_worker_set_starts_every_missing_active_worker(self, world):
        world.daemon._adjust_worker_set()
        assert world.forks() == ["fork:snapshot", "fork:pruning", "fork:api"]

    def test_the_stop_guard_in_adjust_worker_set_forks_nothing_into_a_shutdown(
        self, world, caplog
    ):
        world.daemon._stop_event.set()
        world.daemon._adjust_worker_set()
        assert world.forks() == []
        assert messages(caplog, logging.INFO, "Shutdown requested; not starting worker(s)")

    def test_the_stop_guard_is_re_checked_between_spawns(self, world, mocker, caplog):
        spawned = []

        def spawn(name, verb="Started"):
            spawned.append(name)
            world.daemon._stop_event.set()

        mocker.patch.object(world.daemon, "_spawn", side_effect=spawn)
        world.daemon._adjust_worker_set()
        assert spawned == ["snapshot"]
        assert "pruning, api" in messages(caplog, logging.INFO, "not starting worker(s)")[0]

    def test_nothing_respawns_while_a_worker_is_crash_looped_in_check_workers(self, world):
        workers = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        world.daemon._crash_looped.add("pruning")
        workers["snapshot"].die(WORKER_EXIT_RELOAD)       # a turnover that would be respawned
        workers["api"].die(1)                             # and a crash whose backoff is due
        world.events.clear()
        world.daemon._check_workers()
        world.advance(10_000)
        world.daemon._check_workers()
        assert world.forks() == []

    def test_nothing_respawns_while_a_worker_is_crash_looped_in_adjust_worker_set(
        self, world
    ):
        world.daemon._crash_looped.add("pruning")
        world.daemon._adjust_worker_set()
        assert world.forks() == []

    def test_an_added_remote_is_not_started_by_a_reload_into_a_crash_loop(self, world):
        world.start_workers()
        world.daemon._crash_looped.add("pruning")
        world.events.clear()
        world.write((STORE, 1), config=cfg(remote=True))
        world.hup()
        assert world.forks() == []


# ---------------------------------------------------------------------------
# Turnover
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestTurnover:
    def test_a_turnover_is_respawned_at_once_with_no_backoff_and_no_failure_count(
        self, world, caplog
    ):
        old = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        exited_for_reload(old)
        world.daemon._check_workers()        # one call: no clock advance

        assert world.daemon._restart_at == {}
        assert world.daemon._failures == {}
        for name, w in old.items():
            new = world.daemon._workers[name]
            assert new is not w and new.is_alive() and new.born_epoch == 1
        assert len(messages(caplog, logging.INFO, "Reloaded")) == 3
        assert any(
            re.fullmatch(r"Reloaded snapshot worker \(pid=\d+\) on generation 1", m)
            for m in messages(caplog, logging.INFO)
        )
        assert not messages(caplog, logging.ERROR)

    def test_failures_survive_an_active_workers_turnover(self, world):
        old = world.start_workers()
        world.daemon._failures["snapshot"] = 3
        world.write((STORE, 1), prefix="changed")
        world.hup()
        exited_for_reload(old)
        world.daemon._check_workers()
        assert world.daemon._failures == {"snapshot": 3}

    def test_a_retired_workers_failure_count_is_dropped(self, mocker, tmp_path):
        w = World(mocker, tmp_path, config=cfg(remote=True))
        workers = w.start_workers()
        w.daemon._failures["remote"] = 2
        w.daemon._failures["snapshot"] = 2
        w.write((STORE, 1), config=cfg())
        w.hup()
        exited_for_reload(workers)
        w.daemon._check_workers()
        assert "remote" not in w.daemon._failures
        assert w.daemon._failures == {"snapshot": 2}      # an active one survives

    def test_a_turnover_after_a_stable_uptime_resets_the_failure_count(self, world):
        """The recovery rule a death applies (`WORKER_STABLE_UPTIME`) holds
        for a turnover too: an incarnation that stayed up that long has
        recovered, so its earlier crashes are not carried across a reload."""
        old = world.start_workers()
        world.daemon._failures["snapshot"] = 3
        world.daemon._failures["api"] = 3
        world.write((STORE, 1), prefix="changed")
        world.hup()
        world.advance(WORKER_STABLE_UPTIME - 1)
        old["api"].die(WORKER_EXIT_RELOAD)
        world.daemon._check_workers()
        assert world.daemon._failures["api"] == 3         # not stable enough yet
        world.advance(1)
        old["snapshot"].die(WORKER_EXIT_RELOAD)
        world.daemon._check_workers()
        assert world.daemon._failures["snapshot"] == 0    # stable: recovered

    def test_code_3_with_the_current_epoch_is_a_crash(self, world, caplog):
        workers = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        exited_for_reload(workers)
        world.daemon._check_workers()                      # turnover onto epoch 1
        fresh = world.daemon._workers["snapshot"]
        fresh.die(WORKER_EXIT_RELOAD)                      # born on the CURRENT epoch
        world.daemon._check_workers()
        assert "snapshot" in world.daemon._restart_at
        assert world.daemon._failures["snapshot"] == 1
        assert messages(caplog, logging.ERROR, "exited with code 3")
        assert world.daemon._workers["snapshot"] is fresh  # waiting out its backoff

    def test_code_3_with_no_reload_ever_is_a_crash(self, world):
        workers = world.start_workers()
        workers["api"].die(WORKER_EXIT_RELOAD)
        world.daemon._check_workers()
        assert "api" in world.daemon._restart_at
        assert world.daemon._failures["api"] == 1

    def test_code_3_while_stop_is_set_is_neither_a_crash_nor_a_turnover(self, world):
        workers = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        world.daemon._stop_event.set()
        world.events.clear()
        workers["snapshot"].die(WORKER_EXIT_RELOAD)
        world.daemon._check_workers()
        assert world.forks() == []
        assert world.daemon._failures == {}
        assert world.daemon._restart_at == {}
        assert world.daemon._workers["snapshot"] is workers["snapshot"]   # left for shutdown

    def test_code_3_at_the_current_epoch_while_stop_is_set_is_not_a_crash(self, world, caplog):
        """The worker saw RELOAD just before STOP: a normal exit even though
        its epoch is current (so it cannot be a turnover either)."""
        workers = world.start_workers()
        world.daemon._stop_event.set()
        workers["api"].die(WORKER_EXIT_RELOAD)
        world.daemon._check_workers()
        assert world.daemon._failures == {} and world.daemon._restart_at == {}
        assert not messages(caplog, logging.ERROR)
        assert world.daemon._workers["api"] is workers["api"]

    def test_a_crash_mid_drain_takes_the_crash_path_onto_the_new_config(self, world, caplog):
        workers = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        workers["pruning"].die(1)
        world.daemon._check_workers()
        assert world.daemon._failures["pruning"] == 1
        assert messages(caplog, logging.ERROR, "restarting in")
        world.advance(60)
        world.daemon._check_workers()
        assert world.workers_made("pruning")[-1].config is world.daemon._handoff.config

    def test_only_the_dead_workers_are_respawned_the_others_keep_draining(self, world):
        old = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        old["snapshot"].die(WORKER_EXIT_RELOAD)
        world.events.clear()
        world.daemon._check_workers()
        assert world.forks() == ["fork:snapshot"]
        assert world.daemon._workers["pruning"] is old["pruning"]

    def test_a_crash_of_a_no_longer_active_worker_is_not_restarted_and_drops_its_failures(
        self, mocker, tmp_path, caplog
    ):
        w = World(mocker, tmp_path, config=cfg(remote=True))
        workers = w.start_workers()
        w.daemon._failures["remote"] = 2
        w.write((STORE, 1), config=cfg())
        w.hup()
        workers["remote"].die(1)                          # crashed instead of switching
        w.events.clear()
        w.daemon._check_workers()
        w.advance(10_000)
        w.daemon._check_workers()
        assert w.forks() == []
        assert "remote" not in w.daemon._workers
        assert "remote" not in w.daemon._failures
        assert "remote" not in w.daemon._restart_at
        assert messages(caplog, logging.ERROR, "not restarting it: not needed by generation 1")
        assert not w.daemon._crash_looped

    def test_the_turnover_stops_at_the_first_shutdown_request(self, world, caplog):
        old = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        exited_for_reload(old)
        world.events.clear()
        real_spawn = world.daemon._spawn

        def spawn(name, verb="Started"):
            real_spawn(name, verb=verb)
            world.daemon._stop_event.set()

        world.daemon._spawn = spawn
        world.daemon._check_workers()
        assert len(world.forks()) == 1
        assert messages(caplog, logging.INFO, "Shutdown requested; not respawning")


# ---------------------------------------------------------------------------
# Deferral and completion
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDeferralLogging:
    def _deferrals(self, caplog):
        return messages(caplog, logging.WARNING, "is still finishing work begun under")

    def test_cadence_with_a_fake_clock(self, world, caplog):
        old = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        t0 = world.clock[0]

        world.clock[0] = t0 + RELOAD_DEFER_NOTICE - 1
        world.daemon._check_workers()
        assert self._deferrals(caplog) == []

        world.clock[0] = t0 + RELOAD_DEFER_NOTICE
        world.daemon._check_workers()
        first = self._deferrals(caplog)
        assert len(first) == len(old)
        sample = [m for m in first if m.startswith("api worker")][0]
        assert sample == (
            f"api worker (pid={old['api'].pid}) is still finishing work begun under "
            "generation 0; it switches to 1 at its next safe point "
            "(in-flight transfers are never interrupted)"
        )

        world.clock[0] = t0 + RELOAD_DEFER_NOTICE + RELOAD_DEFER_REPEAT - 1
        world.daemon._check_workers()
        assert len(self._deferrals(caplog)) == len(old)

        world.clock[0] = t0 + RELOAD_DEFER_NOTICE + RELOAD_DEFER_REPEAT
        world.daemon._check_workers()
        assert len(self._deferrals(caplog)) == 2 * len(old)

    def test_a_worker_that_already_switched_is_not_reported(self, world, caplog):
        old = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        old["snapshot"].die(WORKER_EXIT_RELOAD)
        world.daemon._check_workers()                      # snapshot is on epoch 1 now
        world.advance(RELOAD_DEFER_NOTICE + 1)
        world.daemon._check_workers()
        names = [m.split()[0] for m in self._deferrals(caplog)]
        assert sorted(names) == ["api", "pruning"]

    def test_a_dead_worker_is_classified_not_reported(self, world, caplog):
        old = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        world.advance(RELOAD_DEFER_NOTICE + 1)
        for w in old.values():
            w.die(WORKER_EXIT_RELOAD)
        world.daemon._check_workers()
        assert self._deferrals(caplog) == []

    def test_nothing_is_reported_once_the_reload_is_applied(self, world, caplog):
        old = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        exited_for_reload(old)
        world.daemon._check_workers()
        world.advance(10 * RELOAD_DEFER_REPEAT)
        world.daemon._check_workers()
        assert self._deferrals(caplog) == []

    def test_a_new_reload_restarts_the_notice_clock(self, world, caplog):
        world.start_workers()
        world.write((STORE, 1), prefix="a")
        world.hup()
        world.advance(RELOAD_DEFER_NOTICE)
        world.daemon._check_workers()
        n = len(self._deferrals(caplog))
        assert n == 3
        world.write((STORE, 2), prefix="b")
        world.hup()
        world.advance(RELOAD_DEFER_NOTICE - 1)
        world.daemon._check_workers()
        assert len(self._deferrals(caplog)) == n          # the clock restarted
        world.advance(1)
        world.daemon._check_workers()
        assert len(self._deferrals(caplog)) == 2 * n
        assert "switches to 2" in self._deferrals(caplog)[-1]

    def test_before_any_reload_nothing_is_ever_reported(self, world, caplog):
        world.start_workers()
        world.advance(10 * RELOAD_DEFER_REPEAT)
        world.daemon._check_workers()
        assert self._deferrals(caplog) == []


@pytest.mark.unit
class TestCompletion:
    def test_completion_writes_applied_and_clears_target(self, world, caplog):
        old = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        assert world.lock.state["target"] == (STORE, 1)
        assert world.lock.state["applied"] is None          # FakeLock starts empty
        assert world.daemon._applied_epoch == 0

        old["snapshot"].die(WORKER_EXIT_RELOAD)
        old["pruning"].die(WORKER_EXIT_RELOAD)
        world.daemon._check_workers()
        assert world.daemon._applied_epoch == 0             # api still on the old epoch
        assert world.lock.kinds("applied") == []

        old["api"].die(WORKER_EXIT_RELOAD)
        world.daemon._check_workers()
        assert world.daemon._applied_epoch == 1
        assert world.lock.kinds("applied") == [{"applied": (STORE, 1), "target": None}]
        assert {k: world.lock.state[k] for k in ("applied", "target", "rejected")} == {
            "applied": (STORE, 1), "target": None, "rejected": None,
        }
        assert messages(caplog, logging.INFO, "generation 1 applied by all workers")

    def test_applied_is_written_once(self, world):
        old = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        exited_for_reload(old)
        world.daemon._check_workers()
        world.daemon._check_workers()
        world.daemon._check_workers()
        assert len(world.lock.kinds("applied")) == 1

    def test_a_worker_waiting_out_a_crash_backoff_counts_as_switched(self, world):
        old = world.start_workers()
        old["api"].die(1)
        world.daemon._check_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        old["snapshot"].die(WORKER_EXIT_RELOAD)
        old["pruning"].die(WORKER_EXIT_RELOAD)
        world.daemon._check_workers()
        assert world.daemon._applied_epoch == 1
        assert world.lock.state["applied"] == (STORE, 1)

    def test_a_reload_with_no_workers_running_completes_immediately(self, world, caplog):
        world.write((STORE, 1), prefix="changed")
        world.hup()
        assert world.daemon._applied_epoch == 1
        assert world.lock.state["applied"] == (STORE, 1)
        assert messages(caplog, logging.INFO, "generation 1 applied by all workers")

    def test_target_is_recorded_before_the_workers_are_touched(self, world):
        world.start_workers()
        world.events.clear()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        assert world.lock.calls[0] == {"target": (STORE, 1), "rejected": None}

    def test_the_accepted_and_reloading_lines_name_both_generations(self, world, caplog):
        world.start_workers()
        world.write((STORE, 5), config=cfg("pool/data", "pool/new"))
        world.hup()
        assert messages(caplog, logging.INFO) .count(
            "configuration generation 5 accepted (was 0; trigger: SIGHUP)"
        ) == 1
        reloading = messages(caplog, logging.INFO, "Reloading:")
        assert reloading == [
            "Reloading: generation 0 -> 5 (+1, -0, ~0 datasets); "
            "workers switch over at their next safe point"
        ]

    def test_the_summary_counts_added_removed_and_changed_datasets_and_logs_each(
        self, mocker, tmp_path, caplog
    ):
        old = BackupConfig(datasets=[
            DatasetConfig(name="pool/a"), DatasetConfig(name="pool/b"),
            DatasetConfig(name="pool/c"),
        ])
        new = BackupConfig(datasets=[
            DatasetConfig(name="pool/a"), DatasetConfig(name="pool/c", recursive=True),
            DatasetConfig(name="pool/d"),
        ])
        w = World(mocker, tmp_path, config=old)
        w.write((STORE, 1), config=new)
        w.hup()
        assert messages(caplog, logging.INFO, "Reloading:") == [
            "Reloading: generation 0 -> 1 (+1, -1, ~1 datasets); "
            "workers switch over at their next safe point"
        ]
        assert messages(caplog, logging.INFO, "  added dataset pool/d: ")
        assert messages(caplog, logging.INFO, "  changed dataset pool/c: ")
        assert messages(caplog, logging.INFO, "  removed dataset pool/b")
        assert not messages(caplog, logging.INFO, "pool/a")

    def test_on_a_completed_reload_the_dataset_lines_and_prefix_warning_follow_the_summary(
        self, world, caplog
    ):
        world.write((STORE, 1), config=cfg("pool/data", "pool/new", prefix="renamed"))
        world.hup()
        order = [
            r.getMessage() for r in caplog.records
            if r.name == "zfsbackup.daemon" and r.levelno >= logging.INFO
        ]
        summary = next(i for i, m in enumerate(order) if m.startswith("Reloading:"))
        added = next(i for i, m in enumerate(order) if m.startswith("  added dataset"))
        warn = next(i for i, m in enumerate(order) if m.startswith("snapshot_prefix changed"))
        assert summary < added and summary < warn

    def test_a_changed_global_setting_is_named_in_the_summary(self, world, caplog):
        world.write((STORE, 1), prefix="renamed")
        world.hup()
        assert messages(caplog, logging.INFO, "Reloading:")[0].startswith(
            "Reloading: generation 0 -> 1 (+0, -0, ~0 datasets; "
            "settings changed: snapshot_prefix);"
        )

    def test_a_changed_check_interval_is_named_in_the_summary(self, world, caplog):
        from datetime import timedelta

        world.write((STORE, 1), config=cfg(check_interval=timedelta(minutes=7)))
        world.hup()
        assert messages(caplog, logging.INFO, "Reloading:")[0].startswith(
            "Reloading: generation 0 -> 1 (+0, -0, ~0 datasets; "
            "settings changed: check_interval);"
        )

    def test_dry_run_is_never_reported_as_changed_under_the_flag(self, mocker, tmp_path, caplog):
        w = World(mocker, tmp_path, dry_run=True)
        w.write((STORE, 1), prefix="renamed")
        w.hup()
        line = messages(caplog, logging.INFO, "Reloading:")[0]
        assert "settings changed: snapshot_prefix);" in line
        assert "dry_run" not in line

    def test_a_snapshot_prefix_change_warns_that_old_snapshots_are_no_longer_managed(
        self, world, caplog
    ):
        world.write((STORE, 1), prefix="renamed")
        world.hup()
        warn = messages(caplog, logging.WARNING, "snapshot_prefix changed")
        assert warn == [
            "snapshot_prefix changed from autosnap to renamed: snapshots named "
            "autosnap_* are no longer managed or pruned"
        ]

    def test_no_prefix_warning_when_the_prefix_is_unchanged(self, world, caplog):
        world.write((STORE, 1), config=cfg("pool/data", "pool/more"))
        world.hup()
        assert not messages(caplog, logging.WARNING, "snapshot_prefix")
        assert "settings changed" not in messages(caplog, logging.INFO, "Reloading:")[0]

    def test_the_full_dataset_report_is_only_printed_at_debug(self, world, caplog):
        caplog.set_level(logging.INFO, logger="zfsbackup.daemon")
        world.write((STORE, 1), prefix="changed")
        world.hup()
        world.manager.dataset_report.assert_not_called()
        caplog.set_level(logging.DEBUG, logger="zfsbackup.daemon")
        world.write((STORE, 2), prefix="again")
        world.hup()
        world.manager.dataset_report.assert_called_once()

    def test_warnings_from_validate_config_are_logged_once_per_accepted_config(
        self, world, caplog
    ):
        world.write((STORE, 1), prefix="changed")       # a dataset with no retention
        world.hup()
        warnings = messages(caplog, logging.WARNING, "never be pruned")
        assert len(warnings) == 1 and warnings[0].startswith("configuration generation 1: ")
        world.hup()                                      # unchanged: not logged again
        assert len(messages(caplog, logging.WARNING, "never be pruned")) == 1


@pytest.mark.unit
class TestCoalescing:
    def test_two_reloads_during_a_drain_respawn_straight_onto_the_newest_config(
        self, world
    ):
        old = world.start_workers()
        world.write((STORE, 1), prefix="one")
        world.hup()
        world.write((STORE, 2), prefix="two")
        world.hup()
        assert world.daemon._handoff.epoch == 2 and world.daemon._reload_epoch.get() == 2

        exited_for_reload(old)
        world.daemon._check_workers()
        new = world.made[len(old):]
        assert len(new) == len(old)
        assert {w.born_epoch for w in new} == {2}
        assert {w.config.snapshot_prefix for w in new} == {"two"}
        # No worker was ever born for generation 1.
        assert all(w.born_epoch != 1 for w in world.made)

    def test_only_the_newest_generation_is_applied(self, world):
        old = world.start_workers()
        world.write((STORE, 1), prefix="one")
        world.hup()
        world.write((STORE, 2), prefix="two")
        world.hup()
        assert world.lock.state["target"] == (STORE, 2)
        exited_for_reload(old)
        world.daemon._check_workers()
        assert world.lock.kinds("applied") == [{"applied": (STORE, 2), "target": None}]

    def test_a_second_reload_after_completion_is_a_fresh_turnover(self, world):
        old = world.start_workers()
        world.write((STORE, 1), prefix="one")
        world.hup()
        exited_for_reload(old)
        world.daemon._check_workers()
        assert world.daemon._applied_epoch == 1
        mid = dict(world.daemon._workers)
        world.write((STORE, 2), prefix="two")
        world.hup()
        assert world.daemon._applied_epoch == 1            # the new one is pending
        exited_for_reload(mid)
        world.daemon._check_workers()
        assert world.daemon._applied_epoch == 2
        assert [c["applied"] for c in world.lock.kinds("applied")] == [(STORE, 1), (STORE, 2)]

    def test_a_crash_during_the_drain_after_a_second_reload_restarts_on_the_newest(
        self, world
    ):
        old = world.start_workers()
        world.write((STORE, 1), prefix="one")
        world.hup()
        world.write((STORE, 2), prefix="two")
        world.hup()
        old["snapshot"].die(1)
        world.daemon._check_workers()
        world.advance(60)
        world.daemon._check_workers()
        assert world.workers_made("snapshot")[-1].born_epoch == 2


# ---------------------------------------------------------------------------
# Stop
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestStopDuringAReload:
    def test_a_stop_during_the_reload_preparation_keeps_target_and_forks_nothing(
        self, world, caplog
    ):
        """The last stop check of `_try_reload` sits after the candidate was
        accepted and recorded (`target=`) and before the `Handoff` swap and
        the epoch bump. B1 removed the `zfs` calls that used to sit in front of
        it; the per-dataset report (DEBUG) is what runs there now."""
        world.start_workers()
        handoff = world.daemon._handoff
        forks_before = len(world.forks())

        def report():
            world.daemon._stop_event.set()              # SIGTERM lands mid-preparation

        world.manager.dataset_report.side_effect = report
        world.write((STORE, 1), config=cfg(remote=True))
        world.hup()

        world.manager.dataset_report.assert_called_once()
        assert world.daemon._handoff is handoff                  # nothing switched
        assert world.daemon._reload_epoch.get() == 0             # no epoch bump
        assert len(world.forks()) == forks_before                # nothing forked
        assert world.lock.state["target"] == (STORE, 1)          # recorded, stays
        assert world.lock.kinds("applied") == []
        assert not messages(caplog, logging.INFO, "Reloading:")

    def test_a_stop_during_the_polls_key_read_starts_no_full_read(self, world, mocker):
        """The flag is re-checked after the poll's `read_store_state` and the
        key comparison, before `_try_reload`: a changed key must not start a
        full read (and a possible accept) into a shutdown."""
        world.start_workers()
        handoff = world.daemon._handoff
        try_reload = mocker.spy(world.daemon, "_try_reload")
        world.on_poll = world.daemon._stop_event.set        # SIGTERM lands during the poll
        world.write((STORE, 1), prefix="changed")           # and the stored key differs
        world.events.clear()
        world.poll()
        try_reload.assert_not_called()
        assert "load" not in world.events
        assert world.events == ["poll", "dispose"]          # dispose still ran
        assert world.lock.calls == []                       # nothing published
        assert world.daemon._handoff is handoff and world.daemon._reload_epoch.get() == 0

    def test_a_stop_during_the_read_accepts_nothing_and_describes_nothing(self, world, caplog):
        world.start_workers()
        handoff = world.daemon._handoff
        world.on_load = world.daemon._stop_event.set        # SIGTERM lands during the read
        world.write((STORE, 1), config=cfg("pool/data", "pool/new", prefix="changed"))
        world.hup()
        assert not messages(caplog, logging.INFO, "accepted")
        assert not messages(caplog, logging.INFO, "added dataset")
        assert not messages(caplog, logging.WARNING, "snapshot_prefix changed")
        assert world.lock.calls == []                       # no target=, nothing
        assert world.daemon._handoff is handoff and world.daemon._reload_epoch.get() == 0
        world.manager.verify_datasets.assert_not_called()
        world.manager.sync_all_config_properties.assert_not_called()
        world.manager.dataset_report.assert_not_called()

    def test_a_stop_during_validate_config_accepts_nothing(self, world, mocker, caplog):
        def validate(config):
            world.daemon._stop_event.set()
            return []

        mocker.patch("zfsbackup.daemon.validate_config", side_effect=validate)
        world.write((STORE, 1), prefix="changed")
        world.hup()
        assert not messages(caplog, logging.INFO, "accepted")
        assert world.lock.calls == []
        assert world.daemon._reload_epoch.get() == 0

    def test_a_stop_during_the_preparation_describes_no_switch_that_never_happened(
        self, world, caplog
    ):
        world.manager.dataset_report.side_effect = world.daemon._stop_event.set
        world.write((STORE, 1), config=cfg("pool/data", "pool/new", prefix="changed"))
        world.hup()
        assert not messages(caplog, logging.INFO, "added dataset")
        assert not messages(caplog, logging.WARNING, "snapshot_prefix changed")
        assert not messages(caplog, logging.INFO, "Reloading:")
        # The candidate WAS accepted and recorded; it just never switched.
        assert messages(caplog, logging.INFO, "configuration generation 1 accepted")
        assert world.lock.state["target"] == (STORE, 1)
        assert world.daemon._reload_epoch.get() == 0 and world.daemon._handoff.epoch == 0

    def test_nothing_is_read_once_the_stop_flag_is_up(self, world):
        world.daemon._stop_event.set()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        world.poll()
        assert world.events == []

    def test_a_stop_during_a_drain_respawns_nothing_and_counts_nothing(self, world):
        old = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        world.daemon._stop_event.set()
        exited_for_reload(old)
        world.events.clear()
        world.daemon._check_workers()
        assert world.forks() == []
        assert world.daemon._failures == {} and world.daemon._restart_at == {}
        assert world.daemon._applied_epoch == 0          # a stopping daemon completes nothing

    def test_a_stop_during_the_reload_preparation_is_not_a_reload(self, world):
        world.manager.dataset_report.side_effect = world.daemon._stop_event.set
        world.write((STORE, 1), prefix="changed")
        world.hup()
        assert world.daemon._handoff.epoch == 0
        assert world.lock.state["target"] == (STORE, 1)

    def test_a_stop_during_the_candidate_checks_is_not_a_reload_either(self, world, mocker):
        """The B3 checks run after the stop check that follows the build; a
        stop raised inside them is caught by the one before the swap."""
        mocker.patch(
            "zfsbackup.daemon._candidate_refusal",
            side_effect=lambda *a, **k: world.daemon._stop_event.set(),
        )
        world.write((STORE, 1), prefix="changed")
        world.hup()
        assert world.daemon._handoff.epoch == 0 and world.daemon._reload_epoch.get() == 0
        assert world.lock.state["target"] == (STORE, 1)

    def test_run_checks_the_stop_flag_between_the_sleep_and_the_tick(self, world, mocker):
        mocker.patch.object(world.daemon, "_shutdown_workers")
        tick = mocker.patch.object(world.daemon, "_reload_tick")
        check = mocker.patch.object(world.daemon, "_check_workers")
        mocker.patch.object(
            world.daemon, "_supervisor_sleep",
            side_effect=lambda s: world.daemon._stop_event.set(),
        )
        world.daemon.run()
        # Only the startup tick ran; the loop's tick and check never did.
        assert [c.kwargs for c in tick.call_args_list] == [{"startup": True}]
        check.assert_not_called()

    def test_run_checks_the_stop_flag_between_the_tick_and_the_check(self, world, mocker):
        mocker.patch.object(world.daemon, "_shutdown_workers")
        check = mocker.patch.object(world.daemon, "_check_workers")
        mocker.patch.object(world.daemon, "_supervisor_sleep")
        mocker.patch.object(
            world.daemon, "_reload_tick",
            side_effect=lambda startup=False: (
                None if startup else world.daemon._stop_event.set()
            ),
        )
        world.daemon.run()
        check.assert_not_called()


class TestSyncAllConfigPropertiesShouldStop:
    @pytest.fixture
    def manager(self, mocker):
        mocker.patch("zfsbackup.backup_manager.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.backup_manager.zfs.list", return_value=[])
        mocker.patch("zfsbackup.backup_manager.zfs.set")
        m = DatasetManager(cfg("pool/a", "pool/b", "pool/c"))
        mocker.patch.object(m, "sync_config_property")
        return m

    @pytest.mark.unit
    def test_it_stops_before_the_next_dataset(self, manager):
        calls = []

        def should_stop():
            calls.append(len(manager.sync_config_property.call_args_list))
            return len(manager.sync_config_property.call_args_list) >= 1

        manager.sync_all_config_properties(should_stop=should_stop)
        assert manager.sync_config_property.call_count == 1
        assert calls == [0, 1]                           # asked before each dataset

    @pytest.mark.unit
    def test_verify_datasets_stops_before_the_next_dataset(self, mocker):
        exists = mocker.patch("zfsbackup.backup_manager.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.backup_manager.zfs.list", return_value=[])
        m = DatasetManager(cfg("pool/a", "pool/b", "pool/c"))
        m.verify_datasets(should_stop=lambda: exists.call_count >= 1)
        assert exists.call_count == 1
        exists.reset_mock()
        m.verify_datasets()
        assert exists.call_count == 3
        exists.reset_mock()
        m.verify_datasets(should_stop=lambda: True)
        exists.assert_not_called()

    @pytest.mark.unit
    def test_it_syncs_everything_without_a_callback_or_with_a_false_one(self, manager):
        manager.sync_all_config_properties()
        assert manager.sync_config_property.call_count == len(manager.datasets) == 3
        manager.sync_config_property.reset_mock()
        manager.sync_all_config_properties(should_stop=lambda: False)
        assert manager.sync_config_property.call_count == 3

    @pytest.mark.unit
    def test_a_flag_already_set_syncs_nothing(self, manager):
        manager.sync_all_config_properties(should_stop=lambda: True)
        manager.sync_config_property.assert_not_called()


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestStartupTick:
    def _arm_run(self, world, mocker):
        """`run()` with the loop stopped at its first sleep and the real
        `_start_workers` / `_shutdown_workers` over `FakeWorker`s."""
        mocker.patch.object(
            world.daemon, "_supervisor_sleep",
            side_effect=lambda s: world.daemon._stop_event.set(),
        )

    def test_a_pending_sighup_is_applied_before_the_first_spawn_with_no_double_cycle(
        self, world, mocker, caplog
    ):
        self._arm_run(world, mocker)
        world.write((STORE, 1), prefix="changed")
        world.daemon._hup_requested = True               # carried over from `_StartupHup`
        assert world.daemon.run() == 0

        # One reload, applied onto workers that did not exist yet...
        assert len(messages(caplog, logging.INFO, "accepted")) == 1
        assert messages(caplog, logging.INFO, "no workers are running yet")
        # ...so each worker was started exactly once, directly on generation 1.
        assert world.forks() == ["fork:snapshot", "fork:pruning", "fork:api"]
        assert {w.born_epoch for w in world.made} == {1}
        assert {w.config.snapshot_prefix for w in world.made} == {"changed"}
        assert not messages(caplog, logging.INFO, "Reloaded")
        assert world.daemon._applied_epoch == 1
        assert world.lock.state["applied"] == (STORE, 1)
        # The read came before the first fork.
        assert world.events.index("load") < world.events.index("fork:snapshot")

    def test_a_write_that_landed_since_the_startup_read_is_applied_by_the_poll(
        self, world, mocker, caplog
    ):
        self._arm_run(world, mocker)
        world.write((STORE, 3), prefix="landed")
        assert world.daemon.run() == 0
        assert messages(caplog, logging.INFO, "configuration generation 3 accepted (was 0; trigger: poll)")
        assert {w.born_epoch for w in world.made} == {1}
        assert len(world.made) == 3

    def test_the_startup_runs_no_existence_check_or_property_sync(self, world, mocker):
        """B1: both moved to `SnapshotWorker._before_loop`."""
        self._arm_run(world, mocker)
        world.daemon.run()
        world.manager.dataset_report.assert_called_once()
        world.manager.verify_datasets.assert_not_called()
        world.manager.sync_all_config_properties.assert_not_called()

    def test_an_unchanged_database_costs_the_startup_one_key_read_and_no_reload(
        self, world, mocker
    ):
        self._arm_run(world, mocker)
        world.daemon.run()
        assert world.events.count("poll") == 1 and "load" not in world.events
        assert {w.born_epoch for w in world.made} == {0}

    def test_the_startup_tick_runs_before_start_workers(self, world, mocker):
        order = []
        real_tick = world.daemon._reload_tick
        mocker.patch.object(
            world.daemon, "_reload_tick",
            side_effect=lambda startup=False: (order.append(("tick", startup)), real_tick(startup=startup))[1],
        )
        mocker.patch.object(
            world.daemon, "_start_workers", side_effect=lambda: order.append("start")
        )
        mocker.patch.object(
            world.daemon, "_supervisor_sleep",
            side_effect=lambda s: world.daemon._stop_event.set(),
        )
        world.daemon.run()
        assert order[:2] == [("tick", True), "start"]

    def test_a_stop_before_the_startup_tick_reads_nothing_and_starts_nothing(
        self, world, mocker
    ):
        world.daemon._stop_event.set()
        world.daemon._hup_requested = True
        world.daemon.run()
        assert "load" not in world.events and "poll" not in world.events
        assert world.forks() == []

    def test_the_startup_tick_cannot_re_spawn_anything(self, world):
        """`startup=True` skips `_adjust_worker_set`: `_start_workers` follows."""
        world.write((STORE, 1), config=cfg(remote=True))
        world.daemon._hup_requested = True
        world.daemon._reload_tick(startup=True)
        assert world.forks() == []


class _Args(SimpleNamespace):
    pass


@pytest.mark.unit
class TestStartupValidation:
    def _argv(self, db, *extra):
        return patch("sys.argv", ["daemon", "-c", str(db), *extra])

    def _poison(self, db):
        con = sqlite3.connect(str(db))
        con.execute("UPDATE global_settings SET api_port = 70000")
        con.commit()
        con.close()

    def test_a_refused_start_exits_one_without_a_traceback_and_releases_the_lock(
        self, sqlite_config_db, lock_spy, mocker, caplog
    ):
        self._poison(sqlite_config_db.path)
        daemon_cls = mocker.patch("zfsbackup.daemon.BackupDaemon")
        with self._argv(sqlite_config_db.path):
            with caplog.at_level(logging.INFO):
                assert main() == 1
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(errors) == 1
        assert errors[0].getMessage().startswith("configuration generation 0 is invalid: ")
        assert "api_port" in errors[0].getMessage()
        assert errors[0].exc_info is None
        daemon_cls.assert_not_called()
        # The refusal released the presence lock and removed the pid file.
        assert len(lock_spy) == 1 and not lock_spy[0].path.exists()
        assert daemonlock.probe(sqlite_config_db) is None

    def test_the_same_refusal_under_test_config(
        self, sqlite_config_db, lock_spy, caplog
    ):
        self._poison(sqlite_config_db.path)
        with self._argv(sqlite_config_db.path, "--test-config"):
            with caplog.at_level(logging.INFO):
                assert main() == 1
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(errors) == 1 and "is invalid" in errors[0].getMessage()
        assert errors[0].exc_info is None
        assert "Configuration is valid" not in caplog.text
        assert lock_spy == []                            # --test-config takes no lock

    def test_a_valid_test_config_still_passes(self, sqlite_config_db, caplog):
        with self._argv(sqlite_config_db.path, "--test-config"):
            with caplog.at_level(logging.INFO):
                assert main() == 0
        assert "Configuration is valid!" in caplog.text

    def test_an_invalid_stored_dataset_name_is_refused_cleanly(
        self, sqlite_config_db, lock_spy, mocker, caplog
    ):
        """`validate_config` catches a bad name first; `_build_manager` (below)
        is the second line of defence for whatever it lets through."""
        con = sqlite3.connect(str(sqlite_config_db.path))
        con.execute("UPDATE datasets SET name = 'pool/ lead' WHERE id = (SELECT MIN(id) FROM datasets)")
        con.commit()
        con.close()
        daemon_cls = mocker.patch("zfsbackup.daemon.BackupDaemon")
        with self._argv(sqlite_config_db.path):
            with caplog.at_level(logging.INFO):
                assert main() == 1
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(errors) == 1
        assert errors[0].getMessage().startswith(
            "configuration generation 0 is invalid: datasets[pool/ lead]: dataset name"
        )
        assert not errors[0].exc_info
        daemon_cls.assert_not_called()
        assert not lock_spy[0].path.exists()              # lock released, pid file removed

    def test_a_dataset_list_no_worker_could_start_with_is_refused_cleanly(
        self, sqlite_config_db, lock_spy, mocker, caplog
    ):
        mocker.patch(
            "zfsbackup.daemon._build_manager",
            side_effect=ValueError("pool/x is not a valid ZFS dataset name"),
        )
        daemon_cls = mocker.patch("zfsbackup.daemon.BackupDaemon")
        with self._argv(sqlite_config_db.path):
            with caplog.at_level(logging.INFO):
                assert main() == 1
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(errors) == 1
        assert errors[0].getMessage() == (
            "configuration generation 0 cannot be used: its dataset list cannot be "
            "built: pool/x is not a valid ZFS dataset name"
        )
        assert not errors[0].exc_info                     # a ValueError is diagnosed
        daemon_cls.assert_not_called()
        assert not lock_spy[0].path.exists()
        assert daemonlock.probe(sqlite_config_db) is None

    def test_the_same_refusal_under_test_config(self, sqlite_config_db, lock_spy, mocker, caplog):
        mocker.patch(
            "zfsbackup.daemon._build_manager", side_effect=ValueError("not a valid name")
        )
        with self._argv(sqlite_config_db.path, "--test-config"):
            with caplog.at_level(logging.INFO):
                assert main() == 1
        assert "cannot be used" in caplog.text and "Configuration is valid" not in caplog.text
        assert lock_spy == []

    def test_an_unexpected_build_failure_is_logged_with_a_traceback(
        self, sqlite_config_db, lock_spy, mocker, caplog
    ):
        mocker.patch("zfsbackup.daemon._build_manager", side_effect=RuntimeError("a bug"))
        mocker.patch("zfsbackup.daemon.BackupDaemon")      # a regression must not fork workers
        with self._argv(sqlite_config_db.path):
            with caplog.at_level(logging.INFO):
                assert main() == 1
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(errors) == 1 and errors[0].exc_info

    @pytest.mark.parametrize("verbose", [False, True], ids=["quiet", "verbose"])
    def test_main_quiets_alembics_routine_info_lines_unless_verbose(
        self, sqlite_config_db, verbose
    ):
        alembic_logger = logging.getLogger("alembic")
        root = logging.getLogger()
        saved = (alembic_logger.level, root.level, logging.getLogger("zfsbackup.daemon").level)
        try:
            alembic_logger.setLevel(logging.NOTSET)
            extra = ("-v",) if verbose else ()
            with self._argv(sqlite_config_db.path, "--test-config", *extra):
                assert main() == 0
            assert alembic_logger.level == (logging.NOTSET if verbose else logging.WARNING)
        finally:
            alembic_logger.setLevel(saved[0])
            root.setLevel(saved[1])
            logging.getLogger("zfsbackup.daemon").setLevel(saved[2])

    def test_validate_config_warnings_are_logged_at_startup(
        self, sqlite_config_db, mocker, caplog
    ):
        mocker.patch(
            "zfsbackup.daemon.validate_config", return_value=["something odd about x"]
        )
        with self._argv(sqlite_config_db.path, "--test-config"):
            with caplog.at_level(logging.WARNING):
                assert main() == 0
        assert "configuration generation 0: something odd about x" in caplog.text

    def test_a_failing_startup_read_still_disposes_the_engines(
        self, sqlite_config_db, lock_spy, mocker
    ):
        events = []
        def load(resolved):
            events.append("load")
            raise SchemaOutOfDate("older")

        mocker.patch("zfsbackup.daemon.load_runtime_config", side_effect=load)
        mocker.patch(
            "zfsbackup.daemon.dispose_all", side_effect=lambda: events.append("dispose")
        )
        with self._argv(sqlite_config_db.path):
            assert main() == 1
        assert events[:2] == ["load", "dispose"]

    def test_startup_records_the_key_as_applied_and_carries_a_startup_hup(
        self, sqlite_config_db, mocker
    ):
        """`set_state(applied=key)` replaces `set_generation`, the daemon is
        built with the key, the presence lock and `--dry-run`, and a SIGHUP
        caught by `_StartupHup` is carried into the daemon's own flag."""
        seen = {}

        def fake_run(self):
            seen["hup"] = self._hup_requested
            seen["key"] = self._handoff.key
            seen["dry_run_override"] = self._dry_run_override
            seen["dry_run"] = self.config.dry_run
            seen["has_lock"] = self._presence_lock is not None
            info = daemonlock.probe(sqlite_config_db)
            seen["applied"] = info.applied
            seen["generation"] = info.generation
            return 0

        mocker.patch.object(BackupDaemon, "run", fake_run)
        early = daemon_module._StartupHup()
        early.pending = True
        args = _Args(config=str(sqlite_config_db.path), dry_run=True,
                     test_config=False, verbose=False)
        assert daemon_module._run_startup(args, None, early) == 0
        assert seen["hup"] is True
        assert seen["key"][1] == 0 and seen["key"][0] != 0
        assert seen["applied"] == seen["key"] and seen["generation"] == 0
        assert seen["dry_run_override"] is True and seen["dry_run"] is True
        assert seen["has_lock"] is True
        assert early.handed_over is True


# ---------------------------------------------------------------------------
# D9
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCrashLoopMessage:
    def _crash_loop(self, world):
        workers = world.start_workers()
        return workers

    def _fatal(self, world, name="snapshot"):
        world.daemon._failures[name] = WORKER_MAX_CONSECUTIVE_FAILURES - 1
        world.daemon._workers[name].die(1)
        world.daemon._check_workers()

    def test_after_a_reload_it_names_the_generation_and_when_it_was_accepted(
        self, world, caplog
    ):
        world.start_workers()
        world.write((STORE, 5), prefix="changed")
        world.hup()
        accepted_at = world.daemon._accepted_at
        assert re.fullmatch(r"\d\d:\d\d:\d\d", accepted_at)
        self._fatal(world)
        fatal = messages(caplog, logging.ERROR, "crashed repeatedly")[0]
        assert (
            f"The daemon is running configuration generation 5 (accepted at {accepted_at})."
            in fatal
        )
        assert world.daemon._stop_event.is_set()
        assert world.daemon._crash_looped == {"snapshot"}

    def test_before_any_reload_the_message_is_unchanged(self, world, caplog):
        world.start_workers()
        self._fatal(world)
        fatal = messages(caplog, logging.ERROR, "crashed repeatedly")[0]
        assert "running configuration generation" not in fatal and "accepted at" not in fatal

    def test_a_crash_loop_that_spans_a_reload_still_reaches_the_limit(self, world):
        """`_failures` survives a turnover, so a worker cannot dodge the limit
        by having a reload land between its crashes."""
        old = world.start_workers()
        world.daemon._failures["snapshot"] = WORKER_MAX_CONSECUTIVE_FAILURES - 2
        world.write((STORE, 1), prefix="changed")
        world.hup()
        old["snapshot"].die(WORKER_EXIT_RELOAD)
        world.daemon._check_workers()                     # turnover: count unchanged
        world.daemon._workers["snapshot"].die(1)
        world.daemon._check_workers()                     # one more crash
        assert world.daemon._failures["snapshot"] == WORKER_MAX_CONSECUTIVE_FAILURES - 1
        world.advance(10_000)
        world.daemon._check_workers()
        world.daemon._workers["snapshot"].die(1)
        world.daemon._check_workers()
        assert world.daemon._crash_looped == {"snapshot"}


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEdgeCases:
    @pytest.fixture
    def keyless(self, mocker, tmp_path):
        return World(mocker, tmp_path, daemon_key=None)

    def test_key_none_is_inert_it_logs_once_and_never_reads(self, keyless, caplog):
        keyless.write((STORE, 1), prefix="changed")
        keyless.hup()
        assert keyless.events == []
        warn = messages(caplog, logging.WARNING)
        assert len(warn) == 1 and "live reload is off" in warn[0]
        assert keyless.daemon._handoff.epoch == 0

    def test_key_none_consumes_the_flag_so_the_sleep_does_not_spin(self, keyless, mocker):
        keyless.daemon._sighup_handler(signal.SIGHUP, None)
        keyless.daemon._reload_tick()
        assert keyless.daemon._hup_requested is False
        sleeps = []
        mocker.patch(
            "zfsbackup.daemon.time.sleep",
            side_effect=lambda s: (
                sleeps.append(s),
                keyless.daemon._stop_event.set() if len(sleeps) == 2 else None,
            ),
        )
        keyless.daemon._supervisor_sleep(5)
        assert len(sleeps) == 2         # it slept; the flag no longer short-circuits it

    def test_key_none_never_polls_even_when_the_poll_is_due(self, keyless):
        keyless.advance(10 * RELOAD_POLL_SECONDS)
        keyless.daemon._reload_tick()
        keyless.daemon._reload_tick(startup=True)
        assert keyless.events == []

    def test_key_none_warns_on_every_hup(self, keyless, caplog):
        keyless.hup()
        keyless.hup()
        assert len(messages(caplog, logging.WARNING, "live reload is off")) == 2

    def test_a_reload_never_calls_verify_or_sync_so_a_failing_one_cannot_matter(
        self, world, caplog
    ):
        """B1: the old "a sync/verify exception is logged and the reload still
        applies" behaviour has no supervisor-side code left. These now live in
        `SnapshotWorker._before_loop` (test_zfsbackup_workers.py)."""
        world.start_workers()
        world.manager.sync_all_config_properties.side_effect = OSError("pool busy")
        world.manager.verify_datasets.side_effect = RuntimeError("zfs exploded")
        world.write((STORE, 1), prefix="changed")
        world.hup()
        world.manager.verify_datasets.assert_not_called()
        world.manager.sync_all_config_properties.assert_not_called()
        assert not messages(caplog, logging.ERROR)
        assert world.daemon._handoff.key == (STORE, 1)
        assert world.daemon._reload_epoch.get() == 1
        assert world.lock.state["target"] == (STORE, 1)

    def test_the_poll_failure_wording_for_a_stale_schema_is_the_reload_one(self, world, caplog):
        world.poll_error = SchemaOutOfDate("older schema\nRun the command.")
        world.poll()
        warn = messages(caplog, logging.WARNING)[0]
        assert warn.endswith(RELOAD_NOTE)

    def test_the_next_poll_deadline_moves_forward_after_every_tick(self, world):
        world.poll()
        first = world.daemon._next_poll_at
        world.poll()
        assert world.daemon._next_poll_at == first + RELOAD_POLL_SECONDS

    def test_a_forced_read_also_resets_the_poll_deadline(self, world):
        world.advance(3)
        world.hup()
        assert world.daemon._next_poll_at == world.clock[0] + RELOAD_POLL_SECONDS

    def test_no_presence_lock_means_nothing_is_recorded_and_nothing_breaks(
        self, mocker, tmp_path
    ):
        w = World(mocker, tmp_path, with_lock=False)
        old = w.start_workers()
        w.write((STORE, 1), prefix="changed")
        w.hup()
        exited_for_reload(old)
        w.daemon._check_workers()
        assert w.daemon._applied_epoch == 1
        w.write((STORE, 2), config=BackupConfig(datasets=[]))
        w.hup()
        assert w.daemon._rejected == (STORE, 2)

    def test_every_worker_class_receives_the_workers_lock_path(self, mocker, tmp_path):
        w = World(mocker, tmp_path, config=cfg(remote=True))
        workers = w.start_workers()
        paths = {x.workers_lock for x in workers.values()}
        assert paths == {daemonlock.workers_lock_path(w.resolved)}


@pytest.mark.unit
class TestConfigErrorMessageForReloads:
    """`config_error_message(..., reloading=True)` -- the SchemaOutOfDate note
    for a running daemon (it needs no restart) versus a start."""

    EXC = SchemaOutOfDate("The config database is at an older schema.\nRun: zfsbackup-config migrate")

    def test_reloading_tells_the_operator_to_sighup_the_running_daemon(self):
        message = config_error_message(None, self.EXC, reloading=True)
        assert message.startswith(str(self.EXC))
        assert message.endswith(RELOAD_NOTE)
        assert "start the daemon again" not in message

    def test_the_default_still_says_to_start_the_daemon_again(self):
        message = config_error_message(None, self.EXC)
        assert message.endswith("run the command above, then start the daemon again.")
        assert "picks the configuration up" not in message

    def test_other_errors_are_unaffected_by_reloading(self):
        exc = ValueError("No datasets configured")
        assert config_error_message(None, exc, reloading=True) == config_error_message(None, exc)

    def test_the_reload_flag_is_keyword_only(self):
        with pytest.raises(TypeError):
            config_error_message(None, self.EXC, True)  # type: ignore[misc]


# ===========================================================================
# B1: the supervisor makes no zfs call
# ===========================================================================


class _NoZfs:
    """Stands in for `zfsbackup.backup_manager.zfs`: any use is a failure."""

    def __getattr__(self, name):
        if name.startswith("_"):          # introspection (mock, copy, pickle)
            raise AttributeError(name)
        raise AssertionError(f"the supervisor reached libzfseasy ({name})")


@pytest.fixture
def no_zfs(mocker, monkeypatch):
    """No `libzfseasy` entry point and no subprocess at all, and the REAL
    `DatasetManager` behind the supervisor (the `World` replaces it with a
    mock, which could never notice a `zfs` call)."""
    monkeypatch.setattr("zfsbackup.backup_manager.zfs", _NoZfs())
    mocker.patch("subprocess.Popen", side_effect=AssertionError("a subprocess was started"))
    mocker.patch("zfsbackup.daemon.DatasetManager", DatasetManager)


@pytest.mark.unit
class TestSupervisorMakesNoZfsCall:
    """B1. `Popen.wait` is retried after a signal (PEP 475), so one hung `zfs`
    call in the supervisor would stall SIGTERM, SIGHUP and every restart."""

    def test_the_guard_itself_trips_on_a_zfs_call_and_on_a_subprocess(self, no_zfs):
        """Without this a broken fixture would make every test below vacuous."""
        with pytest.raises(AssertionError, match="libzfseasy"):
            DatasetManager(cfg()).verify_datasets()
        with pytest.raises(AssertionError, match="subprocess"):
            subprocess.Popen(["true"])

    def test_an_accepted_reload_makes_none(self, world, no_zfs):
        old = world.start_workers()
        world.write((STORE, 1), config=cfg("pool/data", "pool/new", prefix="changed"))
        world.hup()
        assert world.daemon._handoff.key == (STORE, 1)
        exited_for_reload(old)
        world.daemon._check_workers()
        assert world.daemon._applied_epoch == 1

    def test_a_poll_triggered_reload_makes_none(self, world, no_zfs):
        world.write((STORE, 1), prefix="changed")
        world.poll()
        assert world.daemon._handoff.key == (STORE, 1)

    def test_a_rejection_makes_none(self, world, no_zfs):
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.hup()
        assert world.daemon._rejected == (STORE, 1)

    def test_an_identical_candidate_makes_none(self, world, no_zfs):
        world.write((STORE, 1))
        world.hup()
        assert world.daemon._handoff.key == (STORE, 1)

    def test_startup_makes_none(self, world, no_zfs, mocker):
        mocker.patch.object(
            world.daemon, "_supervisor_sleep",
            side_effect=lambda s: world.daemon._stop_event.set(),
        )
        world.write((STORE, 1), prefix="landed")
        world.daemon._hup_requested = True
        assert world.daemon.run() == 0
        assert world.daemon._handoff.key == (STORE, 1)


# ===========================================================================
# B2: an identical candidate turns nothing over
# ===========================================================================


@pytest.mark.unit
class TestIdenticalCandidate:
    def _identical(self, world, key=(STORE, 1)):
        before_made = len(world.made)
        world.write(key)
        world.hup()
        return before_made

    def test_a_rewrite_that_changed_nothing_adopts_the_key_and_restarts_nothing(
        self, world, caplog
    ):
        before = world.start_workers()
        running = world.daemon._handoff.config
        made = self._identical(world)
        assert world.daemon._workers == before
        assert len(world.made) == made                            # nothing spawned
        assert world.daemon._reload_epoch.get() == 0              # no epoch bump
        h = world.daemon._handoff
        assert h.epoch == 0 and h.key == (STORE, 1)
        assert h.config is running                                # same object, just a new key
        assert all(w.is_alive() and w.born_epoch == 0 for w in before.values())
        assert messages(
            caplog, logging.INFO,
            "configuration generation 1 is identical to running generation 0; "
            "no worker restarts",
        )
        assert not messages(caplog, logging.INFO, "accepted")
        assert not messages(caplog, logging.INFO, "Reloading:")

    def test_the_pid_file_says_applied_at_once_with_target_and_rejected_cleared(self, world):
        world.start_workers()
        self._identical(world)
        assert world.lock.calls[-1] == {
            "applied": (STORE, 1), "target": None, "rejected": None,
        }
        assert {k: world.lock.state[k] for k in ("applied", "target", "rejected")} == {
            "applied": (STORE, 1), "target": None, "rejected": None,
        }
        # Exactly one write: the CLI's ack loop must never see a half state.
        assert len(world.lock.calls) == 1

    def test_the_new_key_is_remembered_so_the_next_hup_is_unchanged(self, world, caplog):
        self._identical(world)
        world.events.clear()
        world.hup()
        assert messages(caplog, logging.INFO, "SIGHUP: configuration unchanged (generation 1)")
        world.events.clear()
        world.poll()
        assert "load" not in world.events              # and the poll does not re-read it

    def test_later_workers_are_born_on_the_adopted_key_and_the_old_epoch(self, world):
        first = world.start_workers()
        self._identical(world)
        first["snapshot"].die(1)                       # a crash, restarted on the same config
        world.daemon._check_workers()                  # schedules the restart
        world.advance(60)
        world.daemon._check_workers()
        fresh = world.daemon._workers["snapshot"]
        assert fresh is not first["snapshot"] and fresh.born_epoch == 0
        assert fresh.config is world.daemon._handoff.config

    def test_a_literal_only_change_is_not_identical_and_turns_every_worker_over(
        self, mocker, tmp_path, caplog
    ):
        from zfsbackup.config import Duration

        w = World(mocker, tmp_path, config=cfg(check_interval=Duration("1h")))
        old = w.start_workers()
        w.write((STORE, 1), config=cfg(check_interval=Duration("60m")))
        w.hup()
        assert w.daemon._handoff.epoch == 1 and w.daemon._reload_epoch.get() == 1
        assert not messages(caplog, logging.INFO, "identical")
        assert messages(caplog, logging.INFO, "accepted")
        exited_for_reload(old)
        w.daemon._check_workers()
        assert {x.born_epoch for x in w.workers_made("snapshot")} == {0, 1}
        assert w.lock.state["applied"] == (STORE, 1)

    def test_a_value_change_is_not_identical(self, world, caplog):
        world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        assert world.daemon._reload_epoch.get() == 1
        assert not messages(caplog, logging.INFO, "identical")

    def test_a_dataset_order_change_is_not_identical(self, mocker, tmp_path):
        w = World(mocker, tmp_path, config=cfg("pool/a", "pool/b"))
        w.start_workers()
        w.write((STORE, 1), config=cfg("pool/b", "pool/a"))
        w.hup()
        assert w.daemon._reload_epoch.get() == 1

    def test_a_dry_run_daemon_treats_the_stored_false_copy_as_identical(
        self, mocker, tmp_path, caplog
    ):
        """The stored config says `dry_run: false`; the `--dry-run` daemon runs
        with `dry_run=True`. The override is applied BEFORE the comparison."""
        w = World(mocker, tmp_path, dry_run=True)
        w.start_workers()
        assert w.daemon.config.dry_run is True and w.db_config.dry_run is False
        running = w.daemon._handoff.config
        w.write((STORE, 1))
        w.hup()
        assert w.daemon._handoff.config is running and w.daemon._handoff.epoch == 0
        assert w.daemon._handoff.key == (STORE, 1)
        assert w.daemon._reload_epoch.get() == 0
        assert messages(caplog, logging.INFO, "is identical to running generation 0")

    def test_a_dry_run_daemon_still_reloads_a_real_change(self, mocker, tmp_path):
        w = World(mocker, tmp_path, dry_run=True)
        w.start_workers()
        w.write((STORE, 1), prefix="changed")
        w.hup()
        assert w.daemon._reload_epoch.get() == 1 and w.daemon.config.dry_run is True

    def test_mid_turnover_the_new_key_becomes_the_target_and_completion_applies_it(
        self, world, caplog
    ):
        old = world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()                                    # accepted, not yet switched
        assert world.daemon._applied_epoch == 0 and world.lock.state["target"] == (STORE, 1)
        epoch_before = world.daemon._reload_epoch.get()

        world.write((STORE, 2), prefix="changed")      # a rewrite of the SAME config
        world.hup()
        assert messages(
            caplog, logging.INFO,
            "configuration generation 2 is identical to running generation 1",
        )
        assert world.daemon._reload_epoch.get() == epoch_before == 1    # no further bump
        assert world.daemon._handoff.key == (STORE, 2) and world.daemon._handoff.epoch == 1
        assert world.lock.state["target"] == (STORE, 2)
        assert world.lock.state["rejected"] is None
        assert world.lock.state["applied"] is None or world.lock.state["applied"] != (STORE, 2)
        assert world.daemon._applied_epoch == 0        # the turnover is still in flight

        exited_for_reload(old)
        world.daemon._check_workers()                  # the turnover completes
        assert world.daemon._applied_epoch == 1
        assert world.lock.state["applied"] == (STORE, 2)
        assert world.lock.state["target"] is None
        assert messages(caplog, logging.INFO, "generation 2 applied by all workers")

    def test_an_identical_candidate_after_a_rejection_clears_all_rejection_state(
        self, world, caplog
    ):
        world.start_workers()
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.hup()
        assert world.daemon._rejected == (STORE, 1)
        assert world.daemon._rejection_logged is not None
        assert world.lock.state["rejected"] == (STORE, 1)

        world.write((STORE, 2), config=cfg())          # repaired: back to the running config
        world.hup()
        assert world.daemon._rejected is None
        assert world.daemon._rejection_logged is None
        assert world.daemon._poll_skip is None
        assert world.lock.state["rejected"] is None
        assert world.lock.state["applied"] == (STORE, 2) and world.lock.state["target"] is None
        assert world.daemon._handoff.epoch == 0

        # A fresh rejection is logged again (the dedup state really was cleared).
        world.write((STORE, 3), config=BackupConfig(datasets=[]))
        world.hup()
        assert len(messages(caplog, logging.ERROR, "rejected")) == 2

    def test_an_identical_candidate_clears_a_poll_skip(self, world):
        world.write((STORE, 1), prefix="changed")
        world.load_error = ValueError("No datasets configured")
        world.poll()
        assert world.daemon._poll_skip == (STORE, 1)
        world.load_error = None
        world.write((STORE, 1), config=cfg())           # repaired in place, same key
        world.hup()                                     # ...to exactly the running config
        assert world.daemon._poll_skip is None
        assert world.daemon._handoff.key == (STORE, 1)
        assert world.daemon._handoff.epoch == 0

    def test_it_is_not_subject_to_the_candidate_checks(self, world, mocker):
        refusal = mocker.patch("zfsbackup.daemon._candidate_refusal", return_value="never")
        world.write((STORE, 1))
        world.hup()
        refusal.assert_not_called()
        assert world.daemon._handoff.key == (STORE, 1)

    def test_a_stop_during_the_read_adopts_nothing(self, world):
        world.start_workers()
        world.on_load = world.daemon._stop_event.set
        world.write((STORE, 1))
        world.hup()
        assert world.daemon._handoff.key == (STORE, 0)
        assert world.lock.calls == []

    def test_without_a_presence_lock_it_still_adopts_the_key(self, mocker, tmp_path):
        w = World(mocker, tmp_path, with_lock=False)
        w.write((STORE, 1))
        w.hup()
        assert w.daemon._handoff.key == (STORE, 1) and w.daemon._handoff.epoch == 0

    def test_an_invalid_candidate_is_still_rejected_not_adopted(self, world):
        """Identical-or-not is decided AFTER validation and the build."""
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.hup()
        assert world.daemon._handoff.key == (STORE, 0)
        assert world.daemon._rejected == (STORE, 1)


# ===========================================================================
# B3: refuse what no worker could start with
# ===========================================================================


def _is_root() -> bool:
    return hasattr(os, "geteuid") and os.geteuid() == 0


needs_non_root = pytest.mark.skipif(
    _is_root(), reason="permission checks do not bind root"
)


@pytest.fixture
def probe(mocker):
    """`_probe_api_bind` replaced; returns the mock (answers None = bindable)."""
    return mocker.patch("zfsbackup.daemon._probe_api_bind", return_value=None)


def _within(seconds, fn, *args):
    """Run `fn(*args)` in a thread and fail the test if it does not return in
    `seconds` (a blocking `open` of a FIFO would hang for ever otherwise)."""
    import threading

    box = {}

    def run():
        try:
            box["result"] = fn(*args)
        except BaseException as e:  # noqa: BLE001
            box["error"] = e

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(seconds)
    if t.is_alive():
        pytest.fail(f"{getattr(fn, '__name__', fn)} blocked for more than {seconds}s")
    if "error" in box:
        raise box["error"]
    return box["result"]


def _busy(message="busy"):
    """A probe answer that can clear by itself (EADDRINUSE)."""
    return daemon_module._BindProblem(message, busy=True)


def _client_cfg(path, **kwargs) -> BackupConfig:
    return cfg(remote=True, client_id_file=path, **kwargs)


@pytest.mark.unit
@pytest.mark.usefixtures("real_api_probe")
class TestProbeApiBind:
    def _fake_socket(self, mocker, bind_error=None):
        sock = mocker.MagicMock()
        if bind_error is not None:
            sock.bind.side_effect = bind_error
        factory = mocker.patch("zfsbackup.daemon.socket.socket", return_value=sock)
        return factory, sock

    def test_a_free_port_is_bindable_and_left_free(self):
        port = _free_port()
        assert daemon_module._probe_api_bind("127.0.0.1", port) is None
        # Closed again: the very port can be bound by a real listener now.
        import socket

        with socket.socket() as s:
            s.bind(("127.0.0.1", port))
            s.listen()

    def test_a_port_held_by_a_listening_socket_is_refused_with_the_reason(self):
        import socket

        with socket.socket() as holder:
            holder.bind(("127.0.0.1", 0))
            holder.listen()
            port = holder.getsockname()[1]
            reason = daemon_module._probe_api_bind("127.0.0.1", port)
        assert reason is not None
        assert "in use" in reason.lower() or "address" in reason.lower()

    @pytest.mark.parametrize(
        "err", [PermissionError(13, "Permission denied"),
                OSError(49, "Can't assign requested address")],
        ids=["EACCES", "EADDRNOTAVAIL"],
    )
    def test_a_bind_error_is_returned_as_text_not_raised(self, mocker, err):
        self._fake_socket(mocker, err)
        reason = daemon_module._probe_api_bind("127.0.0.1", 9)
        assert reason == str(err)

    def test_the_socket_is_closed_after_a_success(self, mocker):
        factory, sock = self._fake_socket(mocker)
        assert daemon_module._probe_api_bind("127.0.0.1", 9) is None
        sock.close.assert_called_once()

    def test_the_socket_is_closed_after_a_failure(self, mocker):
        factory, sock = self._fake_socket(mocker, OSError(98, "Address already in use"))
        assert daemon_module._probe_api_bind("127.0.0.1", 9) is not None
        sock.close.assert_called_once()

    def test_it_binds_only_never_listens_and_sets_reuseaddr_and_cloexec(self, mocker):
        import socket

        factory, sock = self._fake_socket(mocker)
        daemon_module._probe_api_bind("127.0.0.1", 9)
        sock.bind.assert_called_once_with(("127.0.0.1", 9))
        sock.listen.assert_not_called()
        sock.setsockopt.assert_any_call(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        family, kind = factory.call_args.args[:2]
        assert family == socket.AF_INET and kind & socket.SOCK_STREAM
        cloexec = getattr(socket, "SOCK_CLOEXEC", 0)
        assert kind & cloexec == cloexec

    def test_a_port_held_by_a_listening_socket_is_busy(self):
        import socket

        with socket.socket() as holder:
            holder.bind(("127.0.0.1", 0))
            holder.listen()
            reason = daemon_module._probe_api_bind("127.0.0.1", holder.getsockname()[1])
        assert isinstance(reason, daemon_module._BindProblem) and reason.busy is True

    def test_a_mocked_eaddrinuse_is_busy(self, mocker):
        self._fake_socket(mocker, OSError(errno.EADDRINUSE, "Address already in use"))
        assert daemon_module._probe_api_bind("127.0.0.1", 9).busy is True

    @pytest.mark.parametrize(
        "error",
        [
            OSError(errno.EADDRNOTAVAIL, "Can't assign requested address"),
            PermissionError(errno.EACCES, "Permission denied"),
            OSError(errno.EAFNOSUPPORT, "Address family not supported"),
            OSError(errno.EINVAL, "Invalid argument"),
            ValueError("bad port"),
            UnicodeError("label empty or too long"),
        ],
        ids=["EADDRNOTAVAIL", "EACCES", "EAFNOSUPPORT", "EINVAL", "ValueError", "UnicodeError"],
    )
    def test_every_other_failure_is_not_busy(self, mocker, error):
        self._fake_socket(mocker, error)
        reason = daemon_module._probe_api_bind("127.0.0.1", 9)
        assert isinstance(reason, str) and reason.busy is False
        assert reason == str(error)

    def test_a_resolver_failure_is_not_busy(self, mocker):
        import socket

        mocker.patch(
            "werkzeug.serving.get_sockaddr",
            side_effect=socket.gaierror(8, "nodename nor servname provided"),
        )
        mocker.patch("zfsbackup.daemon.socket.socket")
        assert daemon_module._probe_api_bind("backup.example.", 9).busy is False

    def test_an_idna_label_that_is_too_long_is_not_busy_and_not_raised(self):
        reason = daemon_module._probe_api_bind("a" * 70 + ".example", 9)
        assert reason is not None and reason.busy is False

    def test_a_family_lookup_failure_is_not_busy(self, mocker):
        mocker.patch("werkzeug.serving.select_address_family", side_effect=ValueError("odd"))
        reason = daemon_module._probe_api_bind("x", 9)
        assert reason == "odd" and reason.busy is False

    def test_a_bind_problem_is_a_str_and_defaults_to_not_busy(self):
        problem = daemon_module._BindProblem("m")
        assert problem == "m" and problem.busy is False and isinstance(problem, str)
        assert daemon_module._BindProblem("m", busy=True).busy is True

    def test_a_name_that_does_not_resolve_is_a_reason_not_an_exception(self, mocker):
        """`get_sockaddr` runs before the socket exists: a resolver failure is
        an `OSError` (`gaierror`) and is answered, not raised."""
        import socket

        mocker.patch(
            "werkzeug.serving.get_sockaddr",
            side_effect=socket.gaierror(8, "nodename nor servname provided"),
        )
        factory = mocker.patch("zfsbackup.daemon.socket.socket")
        reason = daemon_module._probe_api_bind("backup.example.", 9)
        assert reason is not None and "nodename" in reason
        factory.assert_not_called()

    @pytest.mark.skipif(not hasattr(__import__("socket"), "AF_UNIX"), reason="no AF_UNIX")
    def test_a_unix_host_is_not_probed(self, mocker, tmp_path):
        factory = mocker.patch("zfsbackup.daemon.socket.socket")
        path = tmp_path / "api.sock"
        path.write_text("stale")
        assert daemon_module._probe_api_bind(f"unix://{path}", 0) is None
        factory.assert_not_called()
        assert path.read_text() == "stale"             # and nothing was unlinked


@pytest.mark.unit
class TestClientIdProblem:
    def _problem(self, path):
        return daemon_module._client_id_problem(_client_cfg(path))

    def _snapshot(self, root):
        return sorted(str(p) for p in Path(root).rglob("*"))

    def test_a_readable_nonempty_file_passes(self, tmp_path):
        f = tmp_path / "client_id"
        f.write_text("abc123\n")
        before = self._snapshot(tmp_path)
        assert self._problem(f) is None
        assert self._snapshot(tmp_path) == before and f.read_text() == "abc123\n"

    def test_a_missing_file_under_a_writable_directory_passes_and_creates_nothing(
        self, tmp_path
    ):
        assert self._problem(tmp_path / "client_id") is None
        assert list(tmp_path.iterdir()) == []

    def test_a_deep_missing_path_under_a_writable_ancestor_passes_and_creates_nothing(
        self, tmp_path
    ):
        assert self._problem(tmp_path / "a" / "b" / "c" / "client_id") is None
        assert list(tmp_path.iterdir()) == []

    def test_a_blank_writable_file_passes(self, tmp_path):
        f = tmp_path / "client_id"
        f.write_text("  \n")
        assert self._problem(f) is None
        assert f.read_text() == "  \n"

    def test_the_default_path_that_cannot_be_derived_is_a_problem(self, mocker):
        mocker.patch("pathlib.Path.home", side_effect=RuntimeError("no home"))
        problem = daemon_module._client_id_problem(cfg(remote=True))
        assert problem is not None and "cannot be derived" in problem
        assert "no home" in problem and "client_id_file" in problem

    def test_the_default_path_is_derived_from_home(self, tmp_path, monkeypatch):
        home = tmp_path / "h"
        home.mkdir()
        monkeypatch.setenv("HOME", str(home))
        assert daemon_module._client_id_problem(cfg(remote=True)) is None
        assert list(home.iterdir()) == []

    @needs_non_root
    def test_an_unreadable_file_is_a_problem(self, tmp_path):
        f = tmp_path / "client_id"
        f.write_text("abc")
        f.chmod(0)
        try:
            problem = self._problem(f)
        finally:
            f.chmod(0o600)
        assert problem is not None and "cannot be read" in problem and str(f) in problem

    @needs_non_root
    def test_a_missing_file_with_an_unwritable_nearest_parent_is_a_problem(self, tmp_path):
        d = tmp_path / "ro"
        d.mkdir()
        d.chmod(0o500)
        try:
            problem = self._problem(d / "client_id")
            deep = self._problem(d / "x" / "y" / "client_id")
        finally:
            d.chmod(0o700)
        assert problem is not None and "not writable" in problem and str(d) in problem
        assert deep is not None and "not writable" in deep
        assert list(d.iterdir()) == []

    @needs_non_root
    def test_a_blank_unwritable_file_is_a_problem(self, tmp_path):
        f = tmp_path / "client_id"
        f.write_text("")
        f.chmod(0o400)
        try:
            problem = self._problem(f)
        finally:
            f.chmod(0o600)
        assert problem is not None and "empty and cannot be written" in problem

    def test_an_ancestor_that_is_a_file_is_a_problem(self, tmp_path):
        blocker = tmp_path / "file"
        blocker.write_text("x")
        problem = self._problem(blocker / "sub" / "client_id")
        assert problem is not None and "not a directory" in problem

    def test_a_directory_in_place_of_the_file_is_a_problem(self, tmp_path):
        d = tmp_path / "client_id"
        d.mkdir()
        problem = self._problem(d)
        assert problem is not None and "cannot be read" in problem

    def test_an_undecodable_file_is_reported_as_unreadable(self, tmp_path, mocker):
        """Read the way `ClientIdentity` reads it (`Path.read_text()`): a file the
        locale cannot decode is a problem here exactly when it is one there."""
        f = tmp_path / "client_id"
        f.write_bytes(b"\xff\xfe\x80 not text")
        mocker.patch(
            "pathlib.Path.read_text",
            side_effect=UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"),
        )
        problem = self._problem(f)
        assert problem is not None and "cannot be read" in problem and "utf-8" in problem

    def test_real_undecodable_bytes_are_reported_under_a_utf8_locale(self, tmp_path):
        import locale

        if locale.getpreferredencoding(False).lower().replace("-", "") != "utf8":
            pytest.skip("the locale's encoding is not UTF-8")
        f = tmp_path / "client_id"
        f.write_bytes(b"\xff\xfe\x80")
        problem = self._problem(f)
        assert problem is not None and "cannot be read" in problem

    def test_it_reads_the_file_through_the_bounded_reader(self, tmp_path, mocker):
        f = tmp_path / "client_id"
        f.write_text("abc")
        reader = mocker.spy(daemon_module, "_read_id_file")
        assert self._problem(f) is None
        reader.assert_called_once_with(f)

    def test_a_fifo_is_a_problem_and_never_blocks(self, tmp_path):
        fifo = tmp_path / "client_id"
        os.mkfifo(fifo)
        problem = _within(5, self._problem, fifo)
        assert problem is not None and "not a regular file" in problem

    def test_an_oversized_file_is_a_problem(self, tmp_path):
        f = tmp_path / "client_id"
        f.write_bytes(b"x" * (daemon_module._ID_FILE_MAX + 1))
        problem = self._problem(f)
        assert problem is not None and "cannot be read" in problem and "4096" in problem

    @pytest.mark.skipif(not os.path.exists("/dev/zero"), reason="no /dev/zero")
    def test_a_device_is_a_problem_not_an_endless_read(self, tmp_path):
        link = tmp_path / "client_id"
        link.symlink_to("/dev/zero")
        problem = _within(5, self._problem, link)
        assert problem is not None and "not a regular file" in problem

    def test_a_symlink_to_a_regular_file_is_followed(self, tmp_path):
        real = tmp_path / "real"
        real.write_text("abc")
        link = tmp_path / "client_id"
        link.symlink_to(real)
        assert self._problem(link) is None

    def test_a_symlink_to_a_fifo_is_a_problem_and_never_blocks(self, tmp_path):
        fifo = tmp_path / "fifo"
        os.mkfifo(fifo)
        link = tmp_path / "client_id"
        link.symlink_to(fifo)
        problem = _within(5, self._problem, link)
        assert problem is not None and "not a regular file" in problem

    def test_a_dangling_symlink_counts_as_missing_in_a_writable_directory(self, tmp_path):
        link = tmp_path / "client_id"
        link.symlink_to(tmp_path / "nowhere")
        assert self._problem(link) is None

    def test_a_symlink_loop_counts_as_missing(self, tmp_path):
        link = tmp_path / "client_id"
        link.symlink_to(link)
        assert self._problem(link) is None

    def test_exactly_the_cap_is_accepted(self, tmp_path):
        f = tmp_path / "client_id"
        f.write_bytes(b"x" * daemon_module._ID_FILE_MAX)
        assert self._problem(f) is None

    def test_the_check_does_not_take_a_lock_or_change_a_mode(self, tmp_path):
        f = tmp_path / "client_id"
        f.write_text("abc")
        f.chmod(0o640)
        self._problem(f)
        assert f.stat().st_mode & 0o777 == 0o640


@pytest.mark.unit
class TestCandidateRefusal:
    def test_remote_worker_needed_follows_enabled_datasets_with_a_remote(self):
        assert daemon_module._remote_worker_needed(cfg(remote=True)) is True
        assert daemon_module._remote_worker_needed(cfg()) is False
        off = cfg(remote=True)
        off.datasets[0].enabled = False
        assert daemon_module._remote_worker_needed(off) is False

    def test_it_agrees_with_the_supervisors_worker_set(self, world):
        assert "remote" not in world.daemon._active_worker_names()
        w = cfg(remote=True)
        assert daemon_module._remote_worker_needed(w)

    def test_nothing_is_refused_when_nothing_is_wrong(self, probe, tmp_path):
        assert daemon_module._candidate_refusal(cfg(api_port=0)) is None
        assert daemon_module._candidate_refusal(
            _client_cfg(tmp_path / "client_id", api_port=9100)
        ) is None

    def test_startup_probes_the_port_with_no_old_address_tail(self, probe):
        probe.return_value = "Address already in use"
        text = daemon_module._candidate_refusal(cfg(api_port=9100))
        probe.assert_called_once_with("127.0.0.1", 9100)
        assert text == "api_host:api_port 127.0.0.1:9100 cannot be bound: Address already in use"

    def test_a_busy_port_on_a_reload_says_in_use_with_the_old_address_tail(self, probe):
        probe.return_value = _busy("Address already in use")
        text = daemon_module._candidate_refusal(cfg(api_port=9100), cfg(api_port=9000))
        assert text == (
            "api_host:api_port 127.0.0.1:9100 is in use: Address already in use; "
            "the API keeps listening on its old address"
        )
        assert text.deferrable is True
        assert "not retried" not in text and "checked again" not in text

    def test_any_other_bind_failure_is_final_and_says_so(self, probe):
        probe.return_value = daemon_module._BindProblem("Can't assign requested address")
        text = daemon_module._candidate_refusal(cfg(api_port=9100), cfg(api_port=9000))
        assert text == (
            "api_host:api_port 127.0.0.1:9100 cannot be bound: Can't assign requested "
            "address; the API keeps listening on its old address; this is not retried "
            "until the configuration changes (fix api_host/api_port)"
        )
        assert text.deferrable is False

    def test_a_plain_string_answer_counts_as_not_busy(self, probe):
        probe.return_value = "Address already in use"
        text = daemon_module._candidate_refusal(cfg(api_port=9100), cfg(api_port=9000))
        assert text.deferrable is False and "cannot be bound" in text

    def test_at_startup_there_is_no_tail_either_way(self, probe):
        probe.return_value = _busy("Address already in use")
        text = daemon_module._candidate_refusal(cfg(api_port=9100))
        assert text == "api_host:api_port 127.0.0.1:9100 is in use: Address already in use"
        assert text.deferrable is True

    def test_no_refusal_is_none_not_an_empty_refusal(self, probe):
        assert daemon_module._candidate_refusal(cfg(api_port=0), cfg(api_port=0)) is None

    def test_the_probe_is_called_with_the_new_host_and_port(self, probe):
        daemon_module._candidate_refusal(
            cfg(api_host="127.0.0.2", api_port=9100), cfg(api_host="127.0.0.1", api_port=9000)
        )
        probe.assert_called_once_with("127.0.0.2", 9100)

    def test_no_probe_when_the_address_is_unchanged(self, probe):
        assert daemon_module._candidate_refusal(
            cfg(api_port=9000), cfg(api_port=9000, prefix="other")
        ) is None
        probe.assert_not_called()

    def test_a_new_port_zero_is_probed_as_any_free_port(self, probe):
        daemon_module._candidate_refusal(cfg(api_port=0), cfg(api_port=9000))
        probe.assert_called_once_with("127.0.0.1", 0)

    def test_a_host_only_change_while_the_old_worker_holds_the_port_probes_port_zero(
        self, probe
    ):
        """The old worker's socket still holds the port until the turnover: the
        port itself cannot be bound, so only the host is checked."""
        new, old = cfg(api_port=9000, api_host="127.0.0.2"), cfg(api_port=9000)
        assert daemon_module._candidate_refusal(
            new, old, live_api=[("127.0.0.1", 9000)]
        ) is None
        probe.assert_called_once_with("127.0.0.2", 0)

    def test_a_host_only_change_with_no_live_api_worker_binds_the_real_port(self, probe):
        new, old = cfg(api_port=9000, api_host="127.0.0.2"), cfg(api_port=9000)
        daemon_module._candidate_refusal(new, old)
        probe.assert_called_once_with("127.0.0.2", 9000)

    def test_an_address_a_live_worker_already_holds_is_not_probed(self, probe):
        """Reverting a change the old (draining) worker never gave up must not
        collide with that worker's own socket."""
        new, old = cfg(api_port=9000), cfg(api_port=9100)
        assert daemon_module._candidate_refusal(
            new, old, live_api=[("127.0.0.1", 9000)]
        ) is None
        probe.assert_not_called()

    def test_a_different_port_than_any_live_worker_holds_is_bound_for_real(self, probe):
        daemon_module._candidate_refusal(
            cfg(api_port=9200), cfg(api_port=9100), live_api=[("127.0.0.1", 9100)]
        )
        probe.assert_called_once_with("127.0.0.1", 9200)

    def test_probe_api_false_skips_it(self, probe):
        daemon_module._candidate_refusal(cfg(api_port=9100), probe_api=False)
        probe.assert_not_called()

    def test_the_client_id_is_no_longer_a_refusal_reason(self, mocker, probe, tmp_path):
        import inspect

        assert "check_client_id" not in inspect.signature(
            daemon_module._candidate_refusal
        ).parameters
        check = mocker.patch("zfsbackup.daemon._client_id_problem", return_value="no way")
        assert daemon_module._candidate_refusal(_client_cfg(tmp_path / "id")) is None
        assert daemon_module._candidate_refusal(
            _client_cfg(tmp_path / "id"), cfg()
        ) is None
        check.assert_not_called()

    def test_only_the_api_bind_can_be_a_problem_so_there_is_one_line(self, probe):
        probe.return_value = "busy"
        text = daemon_module._candidate_refusal(cfg(api_port=9100), cfg(api_port=9000))
        assert "\n" not in text


@pytest.mark.unit
class TestCandidateWarnings:
    """The client id is advisory: it breaks remote backup only, so it warns."""

    def test_no_warning_when_the_remote_worker_is_not_needed(self, mocker):
        check = mocker.patch("zfsbackup.daemon._client_id_problem", return_value="bad")
        assert daemon_module._candidate_warnings(cfg()) == []
        assert daemon_module._candidate_warnings(cfg(), cfg(remote=True)) == []
        check.assert_not_called()

    def test_no_check_when_the_path_is_unchanged(self, mocker, tmp_path):
        check = mocker.patch("zfsbackup.daemon._client_id_problem", return_value="bad")
        path = tmp_path / "id"
        new = _client_cfg(path, prefix="changed")
        assert daemon_module._candidate_warnings(new, _client_cfg(path)) == []
        check.assert_not_called()

    def test_the_default_path_counts_as_unchanged_when_both_derive_it(self, mocker):
        check = mocker.patch("zfsbackup.daemon._client_id_problem", return_value="bad")
        assert daemon_module._candidate_warnings(
            cfg(remote=True, prefix="b"), cfg(remote=True)
        ) == []
        check.assert_not_called()

    def test_a_newly_needed_remote_worker_is_checked_and_warned_about(self, mocker, tmp_path):
        check = mocker.patch("zfsbackup.daemon._client_id_problem", return_value="no way")
        new = _client_cfg(tmp_path / "id")
        warnings = daemon_module._candidate_warnings(new, cfg())
        check.assert_called_once_with(new)
        assert warnings == [
            "the remote backup worker may not be able to start remote backups: no way"
            "; local snapshots and pruning are unaffected"
        ]

    def test_a_changed_client_id_path_is_checked(self, mocker, tmp_path):
        check = mocker.patch("zfsbackup.daemon._client_id_problem", return_value="no way")
        warnings = daemon_module._candidate_warnings(
            _client_cfg(tmp_path / "new"), _client_cfg(tmp_path / "old")
        )
        check.assert_called_once()
        assert len(warnings) == 1 and "no way" in warnings[0]

    def test_startup_checks_unconditionally(self, mocker, tmp_path):
        check = mocker.patch("zfsbackup.daemon._client_id_problem", return_value="no way")
        assert daemon_module._candidate_warnings(_client_cfg(tmp_path / "id"))
        check.assert_called_once()

    def test_a_clean_client_id_gives_no_warning(self, mocker, tmp_path):
        mocker.patch("zfsbackup.daemon._client_id_problem", return_value=None)
        assert daemon_module._candidate_warnings(_client_cfg(tmp_path / "id")) == []

    def test_an_underivable_previous_path_counts_as_changed(self, mocker):
        check = mocker.patch("zfsbackup.daemon._client_id_problem", return_value=None)
        mocker.patch("pathlib.Path.home", side_effect=RuntimeError("no home"))
        daemon_module._candidate_warnings(cfg(remote=True), cfg(remote=True))
        check.assert_called_once()

    def test_it_creates_nothing(self, tmp_path):
        target = tmp_path / "not" / "yet" / "client_id"
        daemon_module._candidate_warnings(_client_cfg(target))
        assert list(tmp_path.iterdir()) == []


@pytest.mark.unit
class TestReloadHandlesWhatNoWorkerCouldStartWith:
    """An API address that cannot be bound is DEFERRED (re-checked every poll,
    `deferred=` in the pid file); a bug in the check is a final rejection; a
    client id problem only warns."""

    def _deferred_line(self, err="[Errno 48] Address already in use", trigger="SIGHUP", gen=1):
        return (
            f"configuration generation {gen} deferred (trigger: {trigger}); "
            f"still running generation 0: api_host:api_port 127.0.0.1:9100 is in use: {err}; the API keeps listening on its old address; re-checked every poll"
        )

    def test_a_changed_port_that_is_busy_is_deferred_and_nothing_changes(
        self, world, probe, caplog
    ):
        before = world.start_workers()
        probe.return_value = _busy("[Errno 48] Address already in use")
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()
        probe.assert_called_once_with("127.0.0.1", 9100)
        assert world.daemon._workers == before
        assert len(world.made) == len(before)
        assert world.daemon._handoff.epoch == 0 and world.daemon._reload_epoch.get() == 0
        assert world.daemon._deferred == (STORE, 1) and world.daemon._rejected is None
        state = world.lock.state
        assert state["deferred"] == (STORE, 1)
        assert state["deferred_reason"] == (
            "api_host:api_port 127.0.0.1:9100 is in use: "
            "[Errno 48] Address already in use; the API keeps listening on its old address"
        )
        assert state["rejected"] is None and state["target"] is None
        assert [c for c in world.lock.calls if "rejected" in c and c["rejected"]] == []
        warnings = messages(caplog, logging.WARNING)
        assert warnings == [self._deferred_line()]
        assert not messages(caplog, logging.ERROR)       # not an error: it may pass later
        assert not messages(caplog, logging.INFO, "accepted")

    def test_a_deferred_key_is_never_published_as_rejected(self, world, probe):
        probe.return_value = _busy("busy")
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()
        world.poll()
        assert [c for c in world.lock.calls if c.get("rejected") is not None] == []

    def test_a_deferred_port_is_re_checked_at_every_poll_without_a_new_write(
        self, world, probe, caplog
    ):
        world.start_workers()
        probe.return_value = _busy("busy")
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()
        world.poll()
        world.poll()
        assert probe.call_count == 3
        assert len(messages(caplog, logging.WARNING, "deferred")) == 1    # once
        assert len(messages(caplog, logging.DEBUG, "deferred (trigger: poll)")) == 2
        assert world.lock.kinds("deferred") == [
            {"deferred": (STORE, 1), "deferred_reason": world.lock.state["deferred_reason"],
             "rejected": None}
        ]                                                # published once, not per poll
        assert world.daemon._reload_epoch.get() == 0

    def test_a_port_that_frees_up_is_accepted_by_the_next_poll_and_deferred_is_cleared(
        self, world, probe
    ):
        world.start_workers()
        probe.return_value = _busy("busy")
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()
        assert world.lock.state["deferred"] == (STORE, 1)
        probe.return_value = None                       # the holder went away
        world.poll()
        assert world.daemon._handoff.key == (STORE, 1) and world.daemon._reload_epoch.get() == 1
        assert world.daemon._deferred is None and world.daemon._deferred_reason is None
        state = world.lock.state
        assert state["deferred"] is None and state["deferred_reason"] is None
        assert state["target"] == (STORE, 1) and state["rejected"] is None

    def test_a_host_only_change_is_checked_against_port_zero_while_the_old_api_runs(
        self, world, probe
    ):
        world.start_workers()                           # the api worker is bound to 8080
        world.write((STORE, 1), config=cfg(api_host="127.0.0.2"))
        world.hup()
        probe.assert_called_once_with("127.0.0.2", 0)
        assert world.daemon._handoff.key == (STORE, 1)

    def test_a_revert_to_the_address_the_draining_worker_still_holds_is_not_probed(
        self, world, probe
    ):
        world.start_workers()                           # api bound to 127.0.0.1:8080
        probe.return_value = None
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()                                     # accepted; the old api worker drains
        probe.reset_mock()
        probe.return_value = "must not be asked"
        world.write((STORE, 2), config=cfg())           # back to 8080, still held by it
        world.hup()
        probe.assert_not_called()
        assert world.daemon._handoff.key == (STORE, 2)

    def test_a_free_port_is_accepted_and_probed_once(self, world, probe):
        world.start_workers()
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()
        probe.assert_called_once_with("127.0.0.1", 9100)
        assert world.daemon._handoff.key == (STORE, 1) and world.daemon._reload_epoch.get() == 1

    def test_a_really_held_port_defers_end_to_end(self, world, caplog):
        import socket

        world.start_workers()
        with socket.socket() as holder:
            holder.bind(("127.0.0.1", 0))
            holder.listen()
            port = holder.getsockname()[1]
            world.write((STORE, 1), config=cfg(api_port=port))
            world.hup()
            assert world.daemon._deferred == (STORE, 1) and world.daemon._rejected is None
            assert f"127.0.0.1:{port} is in use" in world.lock.state["deferred_reason"]
            world.poll()                                  # still held: still deferred
            assert world.daemon._deferred == (STORE, 1)
        world.poll()                                      # released: applied
        assert world.daemon._handoff.key == (STORE, 1) and world.daemon._deferred is None

    def test_the_real_probe_mocked_to_eaddrinuse_defers_it(self, world, mocker, caplog):
        sock = mocker.MagicMock()
        sock.bind.side_effect = OSError(errno.EADDRINUSE, "Address already in use")
        mocker.patch("zfsbackup.daemon.socket.socket", return_value=sock)
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()
        assert world.daemon._deferred == (STORE, 1) and world.daemon._rejected is None
        assert "is in use" in messages(caplog, logging.WARNING)[0]
        assert "is in use" in world.lock.state["deferred_reason"]
        sock.close.assert_called_once()

    @pytest.mark.parametrize(
        "error",
        [
            PermissionError(errno.EACCES, "Permission denied"),
            OSError(errno.EADDRNOTAVAIL, "Can't assign requested address"),
        ],
        ids=["EACCES", "EADDRNOTAVAIL"],
    )
    def test_any_other_bind_failure_is_a_final_rejection_the_poll_skips(
        self, world, mocker, caplog, error
    ):
        sock = mocker.MagicMock()
        sock.bind.side_effect = error
        factory = mocker.patch("zfsbackup.daemon.socket.socket", return_value=sock)
        world.start_workers()
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()
        assert world.daemon._rejected == (STORE, 1) and world.daemon._deferred is None
        state = world.lock.state
        assert state["rejected"] == (STORE, 1) and state["deferred"] is None
        assert state["deferred_reason"] is None
        assert [c for c in world.lock.calls if c.get("deferred") is not None] == []
        (error_line,) = messages(caplog, logging.ERROR)
        assert error_line.startswith(
            "configuration generation 1 rejected (trigger: SIGHUP); "
            "still running generation 0:\n"
        )
        assert str(error.strerror) in error_line
        assert error_line.endswith(
            "the API keeps listening on its old address; this is not retried until "
            "the configuration changes (fix api_host/api_port)"
        )
        assert not messages(caplog, logging.WARNING, "deferred")
        assert world.daemon._reload_epoch.get() == 0
        sock.close.assert_called_once()
        loads = world.events.count("load")
        world.poll()
        world.poll()
        assert world.events.count("load") == loads        # the poll skips a final rejection
        assert factory.call_count == 1                    # and nothing is re-probed

    def test_a_dead_name_is_resolved_once_not_at_every_poll(self, world, mocker, caplog):
        import socket

        resolver = mocker.patch(
            "werkzeug.serving.get_sockaddr",
            side_effect=socket.gaierror(8, "nodename nor servname provided"),
        )
        world.start_workers()
        world.write((STORE, 1), config=cfg(api_host="backup.invalid", api_port=9100))
        world.hup()
        for _ in range(3):
            world.poll()
        assert resolver.call_count == 1
        assert world.daemon._rejected == (STORE, 1) and world.daemon._deferred is None
        assert "not retried" in messages(caplog, logging.ERROR)[0]

    def test_a_new_write_after_a_final_rejection_is_probed_afresh(self, world, mocker):
        probe_ = mocker.patch(
            "zfsbackup.daemon._probe_api_bind",
            return_value=daemon_module._BindProblem("Can't assign requested address"),
        )
        world.write((STORE, 1), config=cfg(api_host="192.0.2.77", api_port=9100))
        world.hup()
        assert world.daemon._rejected == (STORE, 1)
        probe_.return_value = None
        world.write((STORE, 2), config=cfg(api_host="127.0.0.1", api_port=9100))
        world.poll()
        assert world.daemon._handoff.key == (STORE, 2) and world.daemon._rejected is None

    def test_an_exception_inside_the_check_is_a_final_rejection_with_a_traceback(
        self, world, mocker, caplog
    ):
        mocker.patch("zfsbackup.daemon._candidate_refusal", side_effect=RuntimeError("bug"))
        world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        assert world.daemon._rejected == (STORE, 1) and world.daemon._deferred is None
        assert world.daemon._reload_epoch.get() == 0
        assert world.lock.state["rejected"] == (STORE, 1)
        assert world.lock.state["deferred"] is None
        records = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(records) == 1 and records[0].exc_info is not None
        assert (
            "the pre-start check (API bind) failed to run: RuntimeError: bug"
            in records[0].getMessage()
        )
        assert not world.daemon._stop_event.is_set()

    def test_a_final_rejection_is_not_retried_by_the_poll(self, world, mocker):
        mocker.patch("zfsbackup.daemon._candidate_refusal", side_effect=RuntimeError("bug"))
        world.write((STORE, 1), prefix="changed")
        world.hup()
        loads = world.events.count("load")
        world.poll()
        world.poll()
        assert world.events.count("load") == loads

    # -- the client id only warns --------------------------------------------

    def test_a_newly_needed_remote_worker_with_a_bad_client_id_is_accepted_with_a_warning(
        self, world, mocker, caplog
    ):
        mocker.patch(
            "zfsbackup.daemon._client_id_problem", return_value="no writable directory"
        )
        world.start_workers()
        world.write((STORE, 1), config=cfg(remote=True))
        world.hup()
        assert world.daemon._handoff.key == (STORE, 1) and world.daemon._reload_epoch.get() == 1
        assert world.daemon._rejected is None and world.daemon._deferred is None
        assert world.lock.state["target"] == (STORE, 1)
        warns = messages(caplog, logging.WARNING, "remote backup worker")
        assert warns == [
            "configuration generation 1: the remote backup worker may not be able to "
            "start remote backups: no writable directory; "
            "local snapshots and pruning are unaffected"
        ]
        assert not messages(caplog, logging.ERROR)

    def test_the_local_workers_still_run_and_the_remote_one_is_started(
        self, world, mocker
    ):
        mocker.patch("zfsbackup.daemon._client_id_problem", return_value="bad")
        old = world.start_workers()
        world.write((STORE, 1), config=cfg(remote=True))
        world.hup()
        exited_for_reload(old)
        world.daemon._check_workers()
        assert set(world.daemon._workers) == {"snapshot", "pruning", "api", "remote"}
        assert all(w.is_alive() for w in world.daemon._workers.values())

    def test_the_same_warning_is_logged_once_across_repeated_attempts_at_a_key(
        self, world, mocker, caplog
    ):
        """A stop that lands after the checks leaves the key un-swapped, so the
        next attempt re-runs them; `_warned` keeps the WARNING to one line per
        `(key, message)`."""
        mocker.patch("zfsbackup.daemon._client_id_problem", return_value="bad")
        world.start_workers()
        world.write((STORE, 1), config=cfg(remote=True))
        world.manager.dataset_report.side_effect = world.daemon._stop_event.set
        world.hup()                                     # first attempt: stopped before the swap
        assert world.daemon._handoff.key == (STORE, 0)
        world.daemon._stop_event.clear()
        world.manager.dataset_report.side_effect = None
        world.hup()                                     # second attempt: accepted
        assert world.daemon._handoff.key == (STORE, 1)
        assert len(messages(caplog, logging.WARNING, "remote backup worker")) == 1

    def test_a_different_key_with_the_same_problem_warns_again(self, world, mocker, caplog):
        mocker.patch("zfsbackup.daemon._client_id_problem", return_value="bad")
        world.start_workers()
        world.write((STORE, 1), config=cfg(remote=True, client_id_file=Path("/x/one")))
        world.hup()
        world.write((STORE, 2), config=cfg(remote=True, client_id_file=Path("/x/two")))
        world.hup()
        assert len(messages(caplog, logging.WARNING, "remote backup worker")) == 2

    def test_warned_is_forgotten_by_a_reload_with_nothing_to_warn_about(self, world, mocker):
        problem = mocker.patch("zfsbackup.daemon._client_id_problem", return_value="bad")
        world.start_workers()
        world.write((STORE, 1), config=cfg(remote=True))
        world.hup()
        assert world.daemon._warned is not None
        problem.return_value = None
        world.write((STORE, 2), config=cfg(remote=True, client_id_file=Path("/x/two")))
        world.hup()
        assert world.daemon._warned is None

    def test_an_exception_in_the_client_id_check_is_a_warning_not_a_rejection(
        self, world, mocker, caplog
    ):
        mocker.patch("zfsbackup.daemon._candidate_warnings", side_effect=RuntimeError("bug"))
        world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        assert world.daemon._handoff.key == (STORE, 1)
        assert messages(
            caplog, logging.WARNING, "the client id check failed to run: RuntimeError: bug"
        )

    def test_nothing_is_created_for_a_client_id_that_cannot_be_written(self, world, tmp_path):
        target = tmp_path / "not" / "yet" / "client_id"
        world.write((STORE, 1), config=cfg(remote=True, client_id_file=target))
        world.hup()
        assert world.daemon._handoff.key == (STORE, 1)
        assert not (tmp_path / "not").exists()

    @needs_non_root
    def test_an_unwritable_client_id_location_warns_for_real_and_is_accepted(
        self, world, tmp_path, caplog
    ):
        ro = tmp_path / "ro"
        ro.mkdir()
        ro.chmod(0o500)
        try:
            world.write((STORE, 1), config=cfg(remote=True, client_id_file=ro / "client_id"))
            world.hup()
        finally:
            ro.chmod(0o700)
        assert world.daemon._handoff.key == (STORE, 1) and world.daemon._rejected is None
        assert "not writable" in messages(caplog, logging.WARNING, "remote backup")[0]
        assert list(ro.iterdir()) == []

    def test_an_unchanged_client_id_path_runs_no_check_on_a_later_reload(
        self, world, mocker, tmp_path
    ):
        path = tmp_path / "id"
        w = world
        w.daemon._handoff = Handoff(_client_cfg(path), 0, (STORE, 0))
        w.db_config = _client_cfg(path)
        check = mocker.patch("zfsbackup.daemon._client_id_problem", return_value="bad")
        w.write((STORE, 1), config=_client_cfg(path, prefix="changed"))
        w.hup()
        check.assert_not_called()
        assert w.daemon._handoff.key == (STORE, 1)

    def test_no_remote_worker_needed_means_no_client_id_check(self, world, mocker):
        check = mocker.patch("zfsbackup.daemon._client_id_problem", return_value="bad")
        world.write((STORE, 1), prefix="changed")
        world.hup()
        check.assert_not_called()

    def test_a_deferral_leaves_the_old_config_running_across_a_poll(self, world, probe):
        old = world.start_workers()
        probe.return_value = _busy("busy")
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()
        world.poll()
        assert world.daemon._workers == old
        assert world.daemon._applied_epoch == 0 and world.daemon._handoff.key == (STORE, 0)

    def test_a_later_valid_write_after_the_deferral_is_accepted_and_clears_it(
        self, world, probe
    ):
        probe.return_value = _busy("busy")
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()
        probe.return_value = None                      # the port was freed
        world.write((STORE, 2), config=cfg(api_port=9100))
        world.hup()
        assert world.daemon._handoff.key == (STORE, 2)
        assert world.daemon._deferred is None and world.lock.state["deferred"] is None


@pytest.mark.unit
class TestLiveApiAddresses:
    """`_spawn` records where each worker's API would be bound, and
    `_live_api_addrs()` says which addresses a live API worker still holds --
    the running config only says what the NEXT worker will bind."""

    def test_spawn_records_the_address_from_the_handoff_the_worker_was_born_on(self, world):
        world.start_workers()
        assert world.daemon._worker_addr["api"] == ("127.0.0.1", 8080)
        assert set(world.daemon._worker_addr) == {"snapshot", "pruning", "api"}

    def test_a_respawn_on_a_new_config_records_the_new_address(self, world, probe):
        old = world.start_workers()
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()
        exited_for_reload(old)
        world.daemon._check_workers()
        assert world.daemon._worker_addr["api"] == ("127.0.0.1", 9100)
        assert world.daemon._live_api_addrs() == [("127.0.0.1", 9100)]

    def test_a_draining_worker_of_an_older_epoch_still_holds_its_address(self, world, probe):
        old = world.start_workers()
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()                                   # accepted; the old api has not exited
        assert world.daemon.config.api_port == 9100   # the config says 9100 ...
        assert world.daemon._live_api_addrs() == [("127.0.0.1", 8080)]   # ... the socket is 8080
        assert old["api"].is_alive()

    def test_a_dead_worker_waiting_for_its_restart_holds_nothing(self, world):
        old = world.start_workers()
        old["api"].die(1)
        world.daemon._check_workers()                 # classified: scheduled for a restart
        assert "api" in world.daemon._restart_at
        assert world.daemon._live_api_addrs() == []

    def test_a_dead_worker_not_yet_classified_and_without_a_token_holds_nothing(self, world):
        old = world.start_workers()
        old["api"].die(1)
        assert world.daemon._live_api_addrs() == []

    def test_a_lost_forkserver_orphan_holds_its_address(self, world):
        old = world.start_workers()
        old["api"].die(-9)
        world.daemon._lost.add("api")
        assert world.daemon._live_api_addrs() == [("127.0.0.1", 8080)]

    @pytest.mark.parametrize(
        "identity, holds", [("match", True), ("gone", False), ("mismatch", False)]
    )
    def test_an_orphan_found_by_process_identity_holds_its_address_only_if_it_matches(
        self, world, mocker, identity, holds
    ):
        old = world.start_workers()
        old["api"].die(-9)
        world.daemon._worker_token["api"] = "token"
        mocker.patch("zfsbackup.daemon.procident.identity", return_value=identity)
        assert world.daemon._live_api_addrs() == ([("127.0.0.1", 8080)] if holds else [])

    def test_no_api_worker_means_no_address(self, world):
        assert world.daemon._live_api_addrs() == []

    def test_forgetting_the_worker_forgets_the_address(self, world):
        world.start_workers()
        world.daemon._forget_worker("api")
        assert "api" not in world.daemon._worker_addr


@pytest.mark.unit
class TestReloadProbesWhatActuallyChanged:
    def test_an_unrelated_change_with_an_unchanged_address_probes_nothing(self, world, probe):
        world.start_workers()
        world.write((STORE, 1), prefix="changed")
        world.hup()
        probe.assert_not_called()

    def test_a_new_port_is_bound_for_real(self, world, probe):
        world.start_workers()
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()
        probe.assert_called_once_with("127.0.0.1", 9100)

    def test_a_host_only_change_on_the_port_the_live_worker_holds_probes_port_zero(
        self, world, probe
    ):
        world.start_workers()
        world.write((STORE, 1), config=cfg(api_host="127.0.0.2"))
        world.hup()
        probe.assert_called_once_with("127.0.0.2", 0)

    def test_a_host_change_to_a_different_port_than_any_live_worker_is_bound_for_real(
        self, world, probe
    ):
        world.start_workers()
        world.write((STORE, 1), config=cfg(api_host="127.0.0.2", api_port=9100))
        world.hup()
        probe.assert_called_once_with("127.0.0.2", 9100)

    def test_a_changed_address_with_port_zero_probes_port_zero(self, world, probe):
        world.start_workers()
        world.write((STORE, 1), config=cfg(api_port=0))
        world.hup()
        probe.assert_called_once_with("127.0.0.1", 0)

    def test_the_address_a_draining_worker_holds_is_never_probed_again(self, world, probe):
        world.start_workers()
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()
        probe.reset_mock()
        world.write((STORE, 2), config=cfg(api_port=8080))      # back to what is still bound
        world.hup()
        probe.assert_not_called()
        assert world.daemon._handoff.key == (STORE, 2)

    def test_a_lost_orphan_also_protects_its_address_from_a_probe(self, world, probe):
        old = world.start_workers()
        old["api"].die(-9)
        world.daemon._lost.add("api")
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()
        probe.reset_mock()
        world.write((STORE, 2), config=cfg(api_port=8080))
        world.hup()
        probe.assert_not_called()

    def test_a_host_only_change_after_the_live_worker_is_gone_binds_the_real_port(
        self, world, probe
    ):
        old = world.start_workers()
        old["api"].die(1)
        world.daemon._check_workers()                 # in backoff: nothing holds 8080
        world.write((STORE, 1), config=cfg(api_host="127.0.0.2"))
        world.hup()
        probe.assert_called_once_with("127.0.0.2", 8080)


@pytest.mark.unit
class TestDeferral:
    """The `deferred` state machine: a refusal that may pass later with the
    stored config unchanged (the API address is busy). Distinct from the final
    `rejected`."""

    @pytest.fixture
    def deferred(self, world, probe):
        world.start_workers()
        probe.return_value = _busy("busy")
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()
        assert world.daemon._deferred == (STORE, 1)
        return world

    @staticmethod
    def _logged(caplog, level):
        """The `deferred` fixture's own line is logged in the setup phase."""
        records = list(caplog.get_records("setup")) + list(caplog.records)
        return [
            r.getMessage() for r in records
            if r.name == "zfsbackup.daemon" and r.levelno == level
            and "deferred (trigger" in r.getMessage()
        ]

    def test_it_is_deferred_not_rejected(self, deferred):
        assert deferred.daemon._rejected is None
        assert deferred.lock.state["rejected"] is None
        assert deferred.lock.state["deferred"] == (STORE, 1)

    def test_only_one_of_rejected_and_deferred_is_ever_set(self, deferred, probe):
        deferred.write((STORE, 2), config=BackupConfig(datasets=[]))
        deferred.hup()
        assert deferred.daemon._rejected == (STORE, 2) and deferred.daemon._deferred is None
        assert deferred.lock.state["deferred"] is None
        assert deferred.lock.state["deferred_reason"] is None
        assert deferred.lock.state["rejected"] == (STORE, 2)
        # ...and back again: a newer key deferred supersedes the rejection.
        deferred.write((STORE, 3), config=cfg(api_port=9200))
        deferred.hup()
        assert deferred.daemon._deferred == (STORE, 3) and deferred.daemon._rejected is None
        assert deferred.lock.state["rejected"] is None
        assert deferred.lock.state["deferred"] == (STORE, 3)

    def test_deferred_stays_published_through_every_failed_recheck(self, deferred):
        for _ in range(3):
            deferred.poll()
        assert deferred.lock.state["deferred"] == (STORE, 1)
        assert len(deferred.lock.kinds("deferred")) == 1                 # written once

    def test_the_poll_re_reads_the_same_key_every_poll(self, deferred, probe):
        loads = deferred.events.count("load")
        deferred.poll()
        deferred.poll()
        assert deferred.events.count("load") == loads + 2
        assert probe.call_count == 3

    def test_the_warning_is_logged_once_then_at_debug(self, deferred, caplog):
        deferred.poll()
        deferred.poll()
        assert len(self._logged(caplog, logging.WARNING)) == 1
        assert len(self._logged(caplog, logging.DEBUG)) == 2
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_the_warning_wording(self, deferred, caplog):
        (line,) = self._logged(caplog, logging.WARNING)
        assert line.startswith(
            "configuration generation 1 deferred (trigger: SIGHUP); "
            "still running generation 0: api_host:api_port 127.0.0.1:9100 is in use: busy"
        )
        assert line.endswith("; re-checked every poll")

    def test_a_different_reason_for_the_same_key_is_logged_and_published_again(
        self, deferred, probe, caplog
    ):
        probe.return_value = _busy("still busy")
        deferred.poll()
        assert len(self._logged(caplog, logging.WARNING)) == 2
        assert "still busy" in deferred.lock.state["deferred_reason"]
        assert len(deferred.lock.kinds("deferred")) == 2

    def test_a_later_pass_is_accepted_with_deferred_cleared_and_target_set(
        self, deferred, probe
    ):
        probe.return_value = None
        deferred.poll()
        assert deferred.daemon._handoff.key == (STORE, 1)
        assert deferred.daemon._deferred is None and deferred.daemon._deferred_reason is None
        state = deferred.lock.state
        assert state["deferred"] is None and state["deferred_reason"] is None
        assert state["target"] == (STORE, 1) and state["rejected"] is None
        accepted = [c for c in deferred.lock.calls if c.get("target") == (STORE, 1)]
        assert accepted == [{"target": (STORE, 1), "rejected": None, "deferred": None}]
        assert deferred.daemon._reload_epoch.get() == 1

    def test_an_accept_with_nothing_deferred_does_not_touch_the_deferred_key(self, world):
        world.write((STORE, 1), prefix="changed")
        world.hup()
        accepted = [c for c in world.lock.calls if c.get("target") == (STORE, 1)]
        assert accepted == [{"target": (STORE, 1), "rejected": None}]

    def test_after_the_accept_the_poll_does_not_re_read(self, deferred, probe):
        probe.return_value = None
        deferred.poll()
        deferred.events.clear()
        deferred.poll()
        assert "load" not in deferred.events

    def test_a_final_rejection_is_still_skipped_by_the_poll(self, world):
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.hup()
        loads = world.events.count("load")
        world.poll()
        world.poll()
        assert world.events.count("load") == loads

    def test_a_sighup_still_forces_a_read_of_a_rejected_key(self, world):
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.hup()
        loads = world.events.count("load")
        world.hup()
        assert world.events.count("load") == loads + 1

    def test_a_newer_accepted_key_supersedes_the_deferral(self, deferred, probe):
        probe.return_value = None
        deferred.write((STORE, 2), config=cfg(prefix="changed"))
        deferred.hup()
        assert deferred.daemon._handoff.key == (STORE, 2) and deferred.daemon._deferred is None
        assert deferred.lock.state["deferred"] is None
        assert deferred.lock.state["deferred_reason"] is None

    def test_a_newer_final_rejection_supersedes_the_deferral(self, deferred):
        deferred.write((STORE, 2), config=BackupConfig(datasets=[]))
        deferred.poll()
        assert deferred.daemon._deferred is None and deferred.daemon._rejected == (STORE, 2)
        assert deferred.lock.calls[-1] == {"rejected": (STORE, 2), "deferred": None}
        before = deferred.events.count("load")
        deferred.poll()
        assert deferred.events.count("load") == before          # now it is skipped

    def test_a_newer_deferral_replaces_the_older_one(self, deferred, probe):
        deferred.write((STORE, 2), config=cfg(api_port=9300))
        deferred.poll()
        assert deferred.daemon._deferred == (STORE, 2)
        assert deferred.lock.state["deferred"] == (STORE, 2)
        assert "9300" in deferred.lock.state["deferred_reason"]

    def test_an_identical_candidate_supersedes_the_deferral(self, deferred):
        deferred.write((STORE, 2), config=cfg())
        deferred.hup()
        assert deferred.daemon._deferred is None and deferred.daemon._deferred_reason is None
        state = deferred.lock.state
        assert state["deferred"] is None and state["deferred_reason"] is None
        assert state["applied"] == (STORE, 2)
        applied = [c for c in deferred.lock.calls if c.get("applied") == (STORE, 2)]
        assert applied == [
            {"applied": (STORE, 2), "target": None, "rejected": None, "deferred": None}
        ]

    def test_a_stored_key_equal_to_the_running_one_ends_the_recheck(self, deferred):
        deferred.write((STORE, 0), config=cfg())       # reverted to the generation that runs
        deferred.events.clear()
        deferred.poll()
        assert "load" not in deferred.events

    def test_status_names_the_deferred_generation(self, deferred):
        assert deferred.daemon._status_text() == "generation 0; generation 1 deferred"

    def test_status_names_a_rejected_generation_and_prefers_it(self, world):
        world.write((STORE, 1), config=BackupConfig(datasets=[]))
        world.hup()
        assert world.daemon._status_text() == "generation 0; generation 1 rejected"

    def test_status_is_plain_once_the_deferral_is_gone(self, deferred, probe):
        probe.return_value = None
        deferred.poll()
        assert deferred.daemon._status_text() == "generation 1"

    def test_the_deferral_reason_is_one_line(self, world, probe):
        probe.return_value = _busy("first line\nsecond line")
        world.write((STORE, 1), config=cfg(api_port=9100))
        world.hup()
        assert "\n" not in world.lock.state["deferred_reason"]


@pytest.mark.unit
class TestStartupChecksWhatNoWorkerCouldStartWith:
    def _argv(self, db, *extra):
        return patch("sys.argv", ["daemon", "-c", str(db), *extra])

    def _set_port(self, db, port):
        con = sqlite3.connect(str(db))
        con.execute("UPDATE global_settings SET api_port = ?", (port,))
        con.commit()
        con.close()

    def test_a_busy_port_is_refused_with_exit_one_no_pid_file_and_the_lock_released(
        self, sqlite_config_db, lock_spy, mocker, caplog
    ):
        import socket

        with socket.socket() as holder:
            holder.bind(("127.0.0.1", 0))
            holder.listen()
            self._set_port(sqlite_config_db.path, holder.getsockname()[1])
            daemon_cls = mocker.patch("zfsbackup.daemon.BackupDaemon")
            with self._argv(sqlite_config_db.path):
                with caplog.at_level(logging.INFO):
                    assert main() == 1
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(errors) == 1
        message = errors[0].getMessage()
        assert message.startswith("configuration generation 0 cannot be used: api_host:api_port ")
        assert "is in use" in message and "keeps listening" not in message
        assert errors[0].exc_info is None
        daemon_cls.assert_not_called()
        assert len(lock_spy) == 1 and not lock_spy[0].path.exists()   # no pid file
        assert daemonlock.probe(sqlite_config_db) is None             # lock released

    def test_a_second_start_after_the_refusal_is_not_blocked_by_a_leftover_lock(
        self, sqlite_config_db, lock_spy, mocker
    ):
        mocker.patch("zfsbackup.daemon._probe_api_bind", return_value="Address already in use")
        mocker.patch("zfsbackup.daemon.BackupDaemon")
        with self._argv(sqlite_config_db.path):
            assert main() == 1
            assert main() == 1                          # refused for the port, not the lock
        assert len(lock_spy) == 2

    def test_a_client_id_that_cannot_exist_only_warns_at_startup_and_the_daemon_runs(
        self, sqlite_config_db, lock_spy, mocker, caplog
    ):
        mocker.patch("zfsbackup.daemon._probe_api_bind", return_value=None)
        mocker.patch("zfsbackup.daemon._remote_worker_needed", return_value=True)
        mocker.patch("zfsbackup.daemon._client_id_problem", return_value="no way")
        run = mocker.patch.object(BackupDaemon, "run", return_value=0)
        with self._argv(sqlite_config_db.path):
            with caplog.at_level(logging.INFO):
                assert main() == 0
        run.assert_called_once()
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert any(
            "the remote backup worker may not be able to start remote backups: no way"
            in w for w in warnings
        )
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_an_exception_in_the_client_id_check_at_startup_is_a_warning(
        self, sqlite_config_db, mocker, caplog
    ):
        mocker.patch("zfsbackup.daemon._probe_api_bind", return_value=None)
        mocker.patch("zfsbackup.daemon._candidate_warnings", side_effect=RuntimeError("bug"))
        mocker.patch.object(BackupDaemon, "run", return_value=0)
        with self._argv(sqlite_config_db.path):
            with caplog.at_level(logging.INFO):
                assert main() == 0
        assert "the client id check failed to run: RuntimeError: bug" in caplog.text

    def test_a_bug_in_the_api_bind_check_refuses_the_start_with_exit_one(
        self, sqlite_config_db, lock_spy, mocker, caplog
    ):
        mocker.patch("zfsbackup.daemon._candidate_refusal", side_effect=RuntimeError("bug"))
        daemon_cls = mocker.patch("zfsbackup.daemon.BackupDaemon")
        with self._argv(sqlite_config_db.path):
            with caplog.at_level(logging.INFO):
                assert main() == 1
        assert "the pre-start check (API bind) failed to run: RuntimeError: bug" in caplog.text
        daemon_cls.assert_not_called()
        assert not lock_spy[0].path.exists()

    def test_test_config_is_not_refused_for_a_busy_port(
        self, sqlite_config_db, lock_spy, mocker, caplog
    ):
        import socket

        with socket.socket() as holder:
            holder.bind(("127.0.0.1", 0))
            holder.listen()
            self._set_port(sqlite_config_db.path, holder.getsockname()[1])
            probe_ = mocker.patch("zfsbackup.daemon._probe_api_bind")
            with self._argv(sqlite_config_db.path, "--test-config"):
                with caplog.at_level(logging.INFO):
                    assert main() == 0
        probe_.assert_not_called()
        assert "Configuration is valid!" in caplog.text
        assert lock_spy == []

    def test_test_config_does_not_check_the_client_id(self, sqlite_config_db, mocker, caplog):
        mocker.patch("zfsbackup.daemon._remote_worker_needed", return_value=True)
        check = mocker.patch("zfsbackup.daemon._client_id_problem", return_value="no way")
        with self._argv(sqlite_config_db.path, "--test-config"):
            assert main() == 0
        check.assert_not_called()

    def test_a_valid_start_still_goes_ahead_when_the_port_is_free(
        self, sqlite_config_db, lock_spy, mocker
    ):
        mocker.patch.object(BackupDaemon, "run", return_value=0)
        self._set_port(sqlite_config_db.path, _free_port())
        with self._argv(sqlite_config_db.path):
            assert main() == 0


# ===========================================================================
# Real processes (plan item 20)
# ===========================================================================
#
# A real `python -m zfsbackup.daemon` on a database made by `zfsbackup-config
# import`, with a stub `zfs`, signalled by real SIGHUPs sent from the test (the
# CLI does not signal the daemon itself until item 19.1, and these tests must
# not depend on whether it does). Writes that must NOT involve a signal are raw
# sqlite3 UPDATEs, so they keep meaning the same thing after 19.1 lands.
#
# Every daemon runs in its own session; the whole group is SIGKILLed in a
# `finally`, and the supervisor is first asked to stop with SIGTERM so the
# happy path is a clean "Daemon stopped". Worker identity is read from the
# supervisor's own "Started/Reloaded/Restarted <name> worker (pid=N)" lines.

_WORKER_LINE = re.compile(r"(?:Started|Reloaded|Restarted) (\w+) worker \(pid=(\d+)\)")

READY_TIMEOUT = 40.0
APPLY_TIMEOUT = 40.0
POLL_TIMEOUT = 25.0     # the poll interval is 10s; the loop ticks every 5s
HUP_TIMEOUT = 5.0       # a SIGHUP must act in seconds, not in a poll interval


def _free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _killpg_quiet(pid: int, sig: int = signal.SIGKILL) -> None:
    try:
        os.killpg(pid, sig)       # the daemon leads its own session: pgid == pid
    except (ProcessLookupError, PermissionError):
        pass


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class RealDaemon:
    """One real daemon and the database it reads."""

    def __init__(self, tmp_path: Path, monkeypatch, *, datasets=1, zfs_sleep=0.0):
        from _hermetic import hermetic_env

        self.tmp = tmp_path
        self.db = tmp_path / "config.db"
        self.log_path = tmp_path / "daemon.log"
        self.resolved = resolve_config_path(str(self.db))
        self.proc = None
        self._log_fh = None
        names = [f"zfsbackup_reload_{os.getpid()}_{i}/data" for i in range(datasets)]
        yaml_path = tmp_path / "config.yaml"
        body = (
            "snapshot_prefix: autosnap\n"
            "check_interval: 1m\n"
            "api_host: 127.0.0.1\n"
            f"api_port: {_free_port()}\n"
            "datasets:\n"
        )
        for name in names:
            body += f"  - name: {name}\n    frequency: 1h\n    retention:\n      \"1d\": \"30d\"\n"
        yaml_path.write_text(body)
        self.yaml_path = yaml_path

        env = hermetic_env()
        env["PYTHONUNBUFFERED"] = "1"
        if zfs_sleep:
            stub = tmp_path / "slow_zfs"
            stub.write_text(f"#!/bin/sh\nsleep {zfs_sleep}\nexit 0\n")
            stub.chmod(0o755)
            env["ZFS_CMD"] = str(stub)
        self.env = env

    # -- the CLI and the database -----------------------------------------

    def cli(self, *args) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "zfsbackup.cli", "-c", str(self.db), *args],
            cwd=ROOT, env=self.env, capture_output=True, text=True, timeout=60,
        )

    def import_config(self) -> None:
        result = self.cli("import", str(self.yaml_path))
        assert result.returncode == 0, result.stdout + result.stderr

    def raw_update(self, assignments: str) -> None:
        """A committed write that bumps the generation, with no signal and no
        CLI: `UPDATE global_settings SET <assignments>, generation = generation + 1`."""
        con = sqlite3.connect(str(self.db), timeout=30)
        try:
            con.execute(
                f"UPDATE global_settings SET {assignments}, generation = generation + 1"
            )
            con.commit()
        finally:
            con.close()

    # -- the daemon --------------------------------------------------------

    def start(self, *flags) -> None:
        self._log_fh = open(self.log_path, "wb")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "zfsbackup.daemon", "-c", str(self.db), *flags],
            stdout=self._log_fh, stderr=subprocess.STDOUT, cwd=ROOT, env=self.env,
            start_new_session=True,
        )

    @property
    def supervisor(self) -> int:
        return self.proc.pid

    def log(self) -> str:
        try:
            return self.log_path.read_text(errors="replace")
        except FileNotFoundError:
            return ""

    def wait_for(self, predicate, timeout, what):
        deadline = time.monotonic() + timeout
        while True:
            text = self.log()
            result = predicate(text)
            if result:
                return result
            if self.proc.poll() is not None:
                pytest.fail(f"daemon exited ({self.proc.returncode}) while waiting for {what}:\n{text}")
            if time.monotonic() >= deadline:
                pytest.fail(f"timed out after {timeout}s waiting for {what}:\n{text}")
            time.sleep(0.05)

    def wait_for_regex(self, pattern, timeout, count=1):
        rx = re.compile(pattern)
        return self.wait_for(
            lambda t: len(rx.findall(t)) >= count, timeout, f"/{pattern}/ x{count}"
        )

    def worker_pids(self, text=None) -> dict:
        """The most recent pid logged for each worker name."""
        pids = {}
        for name, pid in _WORKER_LINE.findall(text if text is not None else self.log()):
            pids[name] = int(pid)
        return pids

    def wait_ready(self) -> dict:
        self.wait_for(
            lambda t: len(self.worker_pids(t)) == 3 and "API listening on" in t,
            READY_TIMEOUT, "three workers and the API listening",
        )
        time.sleep(0.5)            # let the workers get past their first pass
        return self.worker_pids()

    def wait_applied(self, generation, timeout=APPLY_TIMEOUT):
        self.wait_for_regex(rf"generation {generation} applied by all workers", timeout)

    def info(self):
        return daemonlock.probe(self.resolved)

    def hup(self) -> float:
        os.kill(self.supervisor, signal.SIGHUP)
        return time.monotonic()

    def stop(self) -> None:
        if self.proc is None:
            return
        try:
            if self.proc.poll() is None:
                os.kill(self.proc.pid, signal.SIGTERM)
                try:
                    self.proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    pass
        finally:
            _killpg_quiet(self.proc.pid)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
            if self._log_fh is not None:
                self._log_fh.close()


@pytest.fixture
def real_daemon(tmp_path, monkeypatch):
    made = []

    def make(**kwargs):
        d = RealDaemon(tmp_path, monkeypatch, **kwargs)
        made.append(d)
        d.import_config()
        return d

    yield make
    for d in made:
        d.stop()


@pytest.mark.integration
class TestRealDaemonReload:
    def test_a_cli_set_plus_sighup_reloads_in_seconds_and_turns_every_worker_over(
        self, real_daemon
    ):
        d = real_daemon()
        d.start()
        before = d.wait_ready()
        store_id = d.info().applied[0]
        assert d.info().applied == (store_id, 0) and store_id != 0

        result = d.cli("set", "check_interval", "2m")
        assert result.returncode == 0, result.stdout + result.stderr
        t0 = d.hup()

        d.wait_for_regex(
            r"configuration generation 1 accepted \(was 0; trigger: SIGHUP\)", HUP_TIMEOUT + 5
        )
        # "Within seconds": the poll alone would take up to 10s.
        assert time.monotonic() - t0 < HUP_TIMEOUT + 5
        d.wait_applied(1)

        after = d.worker_pids()
        assert set(after) == set(before)
        assert all(after[n] != before[n] for n in before), (before, after)
        assert all(_pid_alive(p) for p in after.values())
        assert d.proc.poll() is None                        # the supervisor never restarted
        info = d.info()
        assert info.pid == d.supervisor
        assert info.applied == (store_id, 1) and info.target is None
        assert info.rejected is None
        assert "Reloaded snapshot worker" in d.log()
        assert d.log().count("Started snapshot worker") == 1

    def test_a_sighup_with_an_unchanged_database_is_unchanged_and_touches_no_worker(
        self, real_daemon
    ):
        d = real_daemon()
        d.start()
        before = d.wait_ready()
        d.hup()
        d.wait_for_regex(r"SIGHUP: configuration unchanged \(generation 0\)", HUP_TIMEOUT)
        time.sleep(1.5)
        assert d.worker_pids() == before
        assert all(_pid_alive(p) for p in before.values())
        assert "accepted" not in d.log() and "Reloaded" not in d.log()
        assert d.info().applied[1] == 0 and d.info().target is None

    @pytest.mark.slow
    def test_a_database_write_with_no_signal_is_picked_up_by_the_poll(self, real_daemon):
        d = real_daemon()
        d.start()
        before = d.wait_ready()
        d.raw_update("snapshot_prefix = 'polled'")
        t0 = time.monotonic()

        d.wait_for_regex(r"configuration generation 1 accepted \(was 0; trigger: poll\)", POLL_TIMEOUT)
        d.wait_applied(1)

        assert "trigger: SIGHUP" not in d.log()
        after = d.worker_pids()
        assert all(after[n] != before[n] for n in before)
        assert d.info().applied[1] == 1
        assert time.monotonic() - t0 < POLL_TIMEOUT + APPLY_TIMEOUT

    def test_an_invalid_stored_config_is_rejected_and_every_worker_keeps_running(
        self, real_daemon
    ):
        d = real_daemon()
        d.start()
        before = d.wait_ready()
        store_id = d.info().applied[0]

        d.raw_update("api_port = 70000")
        d.hup()
        d.wait_for_regex(r"configuration generation 1 rejected \(trigger: SIGHUP\)", HUP_TIMEOUT + 5)
        d.wait_for(
            lambda t: d.info() is not None and d.info().rejected == (store_id, 1),
            HUP_TIMEOUT + 5, "rejected= in the pid file",
        )

        time.sleep(1.5)
        d.hup()                                  # a SIGHUP retries it...
        time.sleep(1.5)
        assert d.log().count("rejected (trigger") == 1      # ...but the ERROR is deduplicated
        assert d.worker_pids() == before
        assert all(_pid_alive(p) for p in before.values())
        assert d.proc.poll() is None
        info = d.info()
        assert info.applied == (store_id, 0) and info.rejected == (store_id, 1)
        assert "still running generation 0" in d.log()
        assert "Reloaded" not in d.log()

    def test_an_invalid_stored_dataset_name_is_rejected_and_every_worker_keeps_running(
        self, real_daemon
    ):
        d = real_daemon()
        d.start()
        before = d.wait_ready()
        store_id = d.info().applied[0]
        con = sqlite3.connect(str(d.db), timeout=30)
        con.execute("UPDATE datasets SET name = 'pool/ lead'")
        con.execute("UPDATE global_settings SET generation = generation + 1")
        con.commit()
        con.close()
        d.hup()
        d.wait_for_regex(r"configuration generation 1 rejected \(trigger: SIGHUP\)", HUP_TIMEOUT + 5)
        log = d.log()
        assert "dataset name 'pool/ lead'" in log
        assert "cannot be managed by zfsbackup" in log or "is not a valid ZFS dataset name" in log
        d.wait_for(
            lambda t: d.info() is not None and d.info().rejected == (store_id, 1),
            HUP_TIMEOUT + 5, "rejected= in the pid file",
        )
        time.sleep(1.0)
        assert d.worker_pids() == before
        assert all(_pid_alive(p) for p in before.values())
        assert "accepted" not in d.log() and "Reloaded" not in d.log()

    def test_a_dry_run_daemon_stays_dry_run_after_a_reload(self, real_daemon):
        d = real_daemon()
        d.start("--dry-run")
        d.wait_ready()
        # Startup's property sync, in dry-run, only says what it would do.
        d.wait_for_regex(r"\[DRY RUN\] Would set org.zfsbackup:config", 10)

        d.raw_update("snapshot_prefix = 'reloaded'")      # the stored dry_run stays false
        d.hup()
        d.wait_for_regex(r"configuration generation 1 accepted", HUP_TIMEOUT + 5)
        # The reload's own property sync runs on the RELOADED config: if it had
        # lost `dry_run` it would not log this line a second time.
        d.wait_for_regex(r"\[DRY RUN\] Would set org.zfsbackup:config", 10, count=2)
        d.wait_applied(1)
        assert "Dry run mode: False" not in d.log()
        assert d.info().applied[1] == 1

    @pytest.mark.slow
    @pytest.mark.parametrize("also_signal", [False, True], ids=["poll", "sighup"])
    def test_a_write_during_the_snapshot_workers_startup_checks_is_not_held_up_by_them(
        self, real_daemon, also_signal
    ):
        """B1. Every `zfs` call takes a second and the snapshot worker makes
        a few per dataset in `_before_loop` (existence check, property read,
        property write): with the supervisor out of that path a write is
        accepted at once, and the worker breaks off its pre-loop work at the
        next dataset instead of finishing it."""
        d = real_daemon(datasets=4, zfs_sleep=1)
        d.start()
        d.wait_ready()
        before = d.worker_pids()
        d.raw_update("snapshot_prefix = 'during_startup'")
        t0 = time.monotonic()
        if also_signal:
            d.hup()

        trigger = "SIGHUP" if also_signal else "poll"
        d.wait_for_regex(
            rf"configuration generation 1 accepted \(was 0; trigger: {trigger}\)",
            POLL_TIMEOUT,
        )
        if also_signal:
            assert time.monotonic() - t0 < HUP_TIMEOUT + 5
        d.wait_applied(1)
        after = d.worker_pids()
        assert all(after[n] != before[n] for n in before)
        text = d.log()
        assert text.count("accepted") == 1
        assert d.info().applied[1] == 1 and d.info().target is None


def _listening_socket():
    """A bound, listening loopback socket holding a port; the caller closes it."""
    import socket

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    return sock, sock.getsockname()[1]


def _supervisor_children_running(supervisor: int, needle: str) -> list:
    """`ps` lines of direct children of `supervisor` whose command has `needle`."""
    out = subprocess.run(
        ["ps", "-ax", "-o", "pid=,ppid=,command="], capture_output=True, text=True
    ).stdout
    found = []
    for line in out.splitlines():
        parts = line.split(None, 2)
        if len(parts) == 3 and int(parts[1]) == supervisor and needle in parts[2]:
            found.append(line)
    return found


@pytest.mark.integration
class TestRealDaemonSupervisorBatchB:
    """B1-B3 against a real `python -m zfsbackup.daemon` (stub `zfs`, restricted
    PATH, own session killed in a `finally`)."""

    # -- B1 ----------------------------------------------------------------

    @pytest.mark.slow
    def test_a_zfs_that_never_returns_stalls_neither_sighup_nor_sigterm(self, real_daemon):
        """Every `zfs` call blocks for ten minutes. The snapshot worker sits in
        its first existence check and the pruning worker in its first prune; the
        supervisor is on neither path. A reload is accepted within seconds
        (`target=` in the pid file), and SIGTERM ends the daemon within the
        shutdown budget (30s) plus the escalation (10s per stuck worker)."""
        d = real_daemon(datasets=2, zfs_sleep=600)
        d.start()
        before = d.wait_ready()
        store_id = d.info().applied[0]
        # The supervisor itself runs no zfs: only its workers have children.
        assert _supervisor_children_running(d.supervisor, "slow_zfs") == []

        d.raw_update("snapshot_prefix = 'while_zfs_hangs'")
        t0 = d.hup()
        d.wait_for_regex(
            r"configuration generation 1 accepted \(was 0; trigger: SIGHUP\)", HUP_TIMEOUT + 5
        )
        assert time.monotonic() - t0 < HUP_TIMEOUT + 5
        d.wait_for(
            lambda t: d.info() is not None and d.info().target == (store_id, 1),
            HUP_TIMEOUT + 5, "target= in the pid file",
        )
        assert d.info().applied == (store_id, 0)       # stuck workers cannot switch yet
        assert d.proc.poll() is None
        assert _supervisor_children_running(d.supervisor, "slow_zfs") == []

        started = time.monotonic()
        os.kill(d.supervisor, signal.SIGTERM)
        try:
            d.proc.wait(timeout=30 + 2 * 10 + 15)
        except subprocess.TimeoutExpired:
            pytest.fail(f"SIGTERM did not end the daemon in time:\n{d.log()}")
        took = time.monotonic() - started
        assert d.proc.returncode == 0, d.log()
        assert took < 30 + 2 * 10 + 10, took
        assert "Daemon stopped" in d.log()
        assert not any(_pid_alive(p) for p in d.worker_pids().values()), d.log()
        assert before.keys() == d.worker_pids().keys()

    # -- B2 ----------------------------------------------------------------

    def _identical_key(self, d, pattern=r"configuration generation (\d+) is identical to running generation (\d+)"):
        text = d.wait_for(lambda t: re.search(pattern, t), HUP_TIMEOUT + 5, "identical")
        return int(re.search(pattern, d.log()).group(1))

    def test_an_export_then_reimport_restarts_nothing_and_is_reported_applied(
        self, real_daemon
    ):
        from zfsbackup.cli.write import await_applied

        d = real_daemon()
        d.start()
        before = d.wait_ready()
        store_id = d.info().applied[0]
        exported = d.cli("export")
        assert exported.returncode == 0, exported.stdout + exported.stderr
        yaml_path = d.tmp / "roundtrip.yaml"
        yaml_path.write_text(exported.stdout)
        result = d.cli("import", str(yaml_path))
        assert result.returncode == 0, result.stdout + result.stderr
        d.hup()

        gen = self._identical_key(d)
        assert gen >= 1
        time.sleep(1.5)
        text = d.log()
        assert d.worker_pids() == before                   # not one worker restarted
        assert all(_pid_alive(p) for p in before.values())
        assert "accepted" not in text and "Reloaded" not in text and "Restarted" not in text
        info = d.info()
        assert info.applied == (store_id, gen) and info.target is None
        assert info.rejected is None
        # What `zfsbackup-config` waits for after a write: the daemon says APPLIED.
        answer, seen = await_applied(d.resolved, (store_id, gen), wait=5)
        assert answer == "applied", (answer, seen)

    def test_a_literal_only_edit_turns_every_worker_over(self, real_daemon):
        d = real_daemon()
        d.start()
        before = d.wait_ready()
        result = d.cli("set", "check_interval", "60s")      # was `1m`: same value, other text
        assert result.returncode == 0, result.stdout + result.stderr
        d.hup()
        d.wait_for_regex(r"configuration generation \d+ accepted \(was 0; trigger: SIGHUP\)",
                         HUP_TIMEOUT + 5)
        d.wait_for_regex(r"generation \d+ applied by all workers", APPLY_TIMEOUT)
        after = d.worker_pids()
        assert all(after[n] != before[n] for n in before)
        assert "is identical" not in d.log()

    def test_a_dry_run_daemon_sees_the_stored_dry_run_false_copy_as_identical(
        self, real_daemon
    ):
        d = real_daemon()
        d.start("--dry-run")
        before = d.wait_ready()
        store_id = d.info().applied[0]
        d.raw_update("snapshot_prefix = 'autosnap'")        # rewritten with the same value
        d.hup()
        gen = self._identical_key(d)
        time.sleep(1.0)
        assert gen == 1
        assert d.worker_pids() == before
        assert "accepted" not in d.log() and "Reloaded" not in d.log()
        assert d.info().applied == (store_id, 1)

    def test_an_identical_rewrite_after_a_rejection_clears_rejected_in_the_pid_file(
        self, real_daemon
    ):
        d = real_daemon()
        d.start()
        before = d.wait_ready()
        store_id = d.info().applied[0]
        con = sqlite3.connect(str(d.db))
        port = con.execute("SELECT api_port FROM global_settings").fetchone()[0]
        con.close()

        d.raw_update("api_port = 70000")
        d.hup()
        d.wait_for(
            lambda t: d.info() is not None and d.info().rejected == (store_id, 1),
            HUP_TIMEOUT + 5, "rejected= in the pid file",
        )
        d.raw_update(f"api_port = {port}")                  # repaired in place
        d.hup()
        d.wait_for(
            lambda t: d.info() is not None and d.info().applied == (store_id, 2),
            HUP_TIMEOUT + 5, "applied= generation 2",
        )
        info = d.info()
        assert info.rejected is None and info.target is None
        assert d.worker_pids() == before
        assert "is identical to running generation 0" in d.log()
        assert "Reloaded" not in d.log()

    # -- B3 ----------------------------------------------------------------

    @pytest.mark.slow
    def test_a_changed_port_that_is_held_is_deferred_then_applied_when_it_is_released(
        self, real_daemon
    ):
        d = real_daemon()
        d.start()
        before = d.wait_ready()
        store_id = d.info().applied[0]
        holder, port = _listening_socket()
        try:
            d.raw_update(f"api_port = {port}")
            d.hup()
            d.wait_for_regex(
                r"configuration generation 1 deferred \(trigger: SIGHUP\)", HUP_TIMEOUT + 5
            )
            d.wait_for(
                lambda t: d.info() is not None and d.info().deferred == (store_id, 1),
                HUP_TIMEOUT + 5, "deferred= in the pid file",
            )
            time.sleep(1.0)
            log = d.log()
            assert f"127.0.0.1:{port} is in use" in log
            assert "the API keeps listening on its old address; re-checked every poll" in log
            assert "rejected" not in log                       # final rejections only
            assert d.worker_pids() == before
            assert all(_pid_alive(p) for p in before.values())
            assert "accepted" not in log and "Reloaded" not in log
            info = d.info()
            assert info.applied == (store_id, 0) and info.target is None
            assert info.rejected is None
            assert f"127.0.0.1:{port} is in use" in info.deferred_reason
            assert "\n" not in info.deferred_reason
        finally:
            holder.close()

        # The port is free again: with no new write and no signal, the next poll
        # re-checks the deferred key and accepts it; the API worker moves.
        d.wait_for_regex(
            r"configuration generation 1 accepted \(was 0; trigger: poll\)", POLL_TIMEOUT
        )
        d.wait_applied(1)
        d.wait_for_regex(rf"API listening on 127.0.0.1:{port}", APPLY_TIMEOUT)
        d.wait_for(
            lambda t: d.info() is not None and d.info().applied == (store_id, 1),
            HUP_TIMEOUT + 5, "applied= generation 1",
        )
        info = d.info()
        assert info.deferred is None and info.deferred_reason is None
        assert info.rejected is None and info.target is None

    def test_a_start_with_a_held_port_is_refused_and_leaves_nothing_behind(self, real_daemon):
        d = real_daemon()
        holder, port = _listening_socket()
        try:
            con = sqlite3.connect(str(d.db))
            con.execute("UPDATE global_settings SET api_port = ?", (port,))
            con.commit()
            con.close()
            d.start()
            try:
                d.proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                pytest.fail(f"the daemon did not refuse to start:\n{d.log()}")
        finally:
            holder.close()
        assert d.proc.returncode == 1, d.log()
        log = d.log()
        assert "cannot be used" in log and f"127.0.0.1:{port} is in use" in log
        assert "Started" not in log
        assert "keeps listening" not in log
        assert d.info() is None                              # no pid file, lock released

    def test_test_config_is_not_refused_for_a_held_port(self, real_daemon):
        d = real_daemon()
        holder, port = _listening_socket()
        try:
            con = sqlite3.connect(str(d.db))
            con.execute("UPDATE global_settings SET api_port = ?", (port,))
            con.commit()
            con.close()
            d.start("--test-config")
            try:
                d.proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                pytest.fail(f"--test-config did not finish:\n{d.log()}")
        finally:
            holder.close()
        assert d.proc.returncode == 0, d.log()
        assert "Configuration is valid!" in d.log()

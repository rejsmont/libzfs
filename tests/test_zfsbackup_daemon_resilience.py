"""A-5 and A-6.2 -- daemon restart backoff, failure classification, interval
floor, graceful-shutdown hardening, and the SIGHUP stopgap.

Time is driven through `BackupDaemon._monotonic`, the named seam: a restart
is a per-worker `_restart_at[name]` deadline checked on each poll, never a
sleep, so nothing here patches `time.sleep` for the restart path (one test
asserts that `_check_workers` calls it zero times).

Mutation counts (one revert at a time, against a scratch copy of the tree;
number of tests in this file that then fail):

- constant restart delay (no doubling): 5; no cap at 300s: 1; give-up
  threshold `>` instead of `>=`: 2; uptime reset `>` instead of `>=`: 2; no
  uptime reset at all: 2;
- the stop-flag check before a due restart removed: 2;
- `run()`'s `finally` not calling `_shutdown_workers`: 5; not raising the stop
  flag first: 3; `_start_workers` ignoring the flag between spawns: 2;
- `_sighup_handler` also setting the stop flag: 3; workers not `SIG_IGN`ing
  SIGHUP: 2;
- `_hard_exit` using `sys.exit` instead of `os._exit`: 1; abandoned workers not
  recorded: 1;
- interval floor removed: 4;
- `config_error_message`: "Could not open" for every OperationalError: 4;
  "Failed to read" for every one: 1; no `diagnose_open_failure`: 1; wrong
  `require_writable`: 1; resolved path shown for the default source: 1;
- `exc_info` always off in `_run`'s generic arm: 1;
- `_hard_exit` passing `code` instead of `code or 1` (A-17): 2; `_spawn`
  recording the worker after the log line: 1.
"""

import logging
import multiprocessing
import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

import zfsbackup.daemon as daemon_module
from zfsbackup.config.store import (
    ConfigDbPermissionError,
    ConfigSnapshot,
    ConfigPathError,
    NotAConfigDatabase,
    SchemaIntegrityError,
    SchemaOutOfDate,
    SchemaSplitBrain,
    SchemaVersionMismatch,
    resolve_config_path,
)
from zfsbackup.daemon import (
    BackupDaemon,
    WORKER_MAX_CONSECUTIVE_FAILURES,
    WORKER_RESTART_DELAY,
    WORKER_RESTART_MAX_DELAY,
    WORKER_STABLE_UPTIME,
    main,
)
from zfsbackup.runtime_config import (
    config_error_message,
    describe_config_source,
)
from zfsbackup.workers import (
    MIN_WORKER_INTERVAL,
    SnapshotWorker,
    _init_child_process,
)

pytestmark = pytest.mark.unit

FORK_AVAILABLE = "fork" in multiprocessing.get_all_start_methods()
ROOT = Path(__file__).resolve().parent.parent


def dead_worker(exitcode=1, pid=100):
    worker = MagicMock()
    worker.is_alive.return_value = False
    worker.pid = pid
    worker.exitcode = exitcode
    return worker


def fake_clock(daemon, mocker, start=1000.0):
    clock = [start]
    mocker.patch.object(daemon, "_monotonic", side_effect=lambda: clock[0])
    return clock


@pytest.fixture
def daemon(sqlite_config_db, sample_backup_config, mocker):
    d = BackupDaemon(sample_backup_config, sqlite_config_db)
    mocker.patch("zfsbackup.daemon.dispose_all")
    return d


def patch_manager(mocker):
    """The `DatasetManager` start-up that `run()` performs shells out to zfs."""
    mocker.patch("zfsbackup.backup_manager.zfs.exists", return_value=True)
    mocker.patch("zfsbackup.backup_manager.zfs.list", return_value=[])
    mocker.patch("zfsbackup.backup_manager.zfs.set")


def errors_of(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]


# ---------------------------------------------------------------------------
# A-5: backoff
# ---------------------------------------------------------------------------


class TestRestartBackoff:
    def _respawn_dying_workers(self, daemon, mocker):
        """`_new_worker` hands out workers that are dead on arrival."""
        spawned = []

        def make(name):
            worker = dead_worker(pid=200 + len(spawned))
            spawned.append(worker)
            return worker

        mocker.patch.object(daemon, "_new_worker", side_effect=make)
        return spawned

    def test_schedule_is_5_10_20_40_80_then_give_up(self, daemon, mocker, caplog):
        clock = fake_clock(daemon, mocker)
        spawned = self._respawn_dying_workers(daemon, mocker)
        daemon._workers = {"snapshot": dead_worker()}
        expected_delays = [5, 10, 20, 40, 80]

        for failure, delay in enumerate(expected_delays, start=1):
            t0 = clock[0]
            daemon._check_workers()
            assert daemon._failures["snapshot"] == failure
            assert daemon._restart_at["snapshot"] == t0 + delay
            assert len(spawned) == failure - 1  # still waiting out the backoff

            clock[0] = t0 + delay - 0.01
            daemon._check_workers()
            assert len(spawned) == failure - 1  # not one tick early

            clock[0] = t0 + delay
            daemon._check_workers()
            assert len(spawned) == failure  # exactly at the deadline
            assert "snapshot" not in daemon._restart_at

        with caplog.at_level(logging.ERROR):
            daemon._check_workers()  # the 6th consecutive death
        assert daemon._failures["snapshot"] == WORKER_MAX_CONSECUTIVE_FAILURES == 6
        assert daemon._crash_looped == {"snapshot"}
        assert "snapshot" not in daemon._workers
        assert len(spawned) == 5  # nothing further was forked
        assert daemon._stop_event.is_set()
        assert any("crashed repeatedly" in m for m in errors_of(caplog))
        assert any("giving up" in m for m in errors_of(caplog))

    def test_delay_is_capped(self, daemon, mocker, monkeypatch):
        clock = fake_clock(daemon, mocker)
        monkeypatch.setattr(daemon_module, "WORKER_MAX_CONSECUTIVE_FAILURES", 50)
        daemon._failures["snapshot"] = 8  # next failure would be 5 * 2**8
        daemon._workers = {"snapshot": dead_worker()}

        daemon._check_workers()

        assert daemon._restart_at["snapshot"] == clock[0] + WORKER_RESTART_MAX_DELAY == clock[0] + 300

    def test_run_returns_one_after_the_sixth_consecutive_failure(
        self, daemon, mocker
    ):
        """End to end through `run()`: a worker that dies on arrival every
        time is respawned five times on the 5/10/20/40/80s schedule, and
        the daemon then stops with exit code 1 (not a forever-loop)."""
        patch_manager(mocker)
        clock = fake_clock(daemon, mocker)
        spawned = self._respawn_dying_workers(daemon, mocker)
        spawn_times = []
        real_spawn = daemon._spawn

        def recording_spawn(name, verb="Started"):
            spawn_times.append(clock[0])
            return real_spawn(name, verb)

        mocker.patch.object(daemon, "_spawn", side_effect=recording_spawn)
        mocker.patch.object(
            daemon, "_start_workers",
            side_effect=lambda: daemon._workers.update({"snapshot": dead_worker()}),
        )
        mocker.patch.object(daemon, "_shutdown_workers")
        polls = [0]

        def advance_five_seconds(_seconds):
            polls[0] += 1
            clock[0] += 5.0
            if polls[0] > 200:  # safety valve: fail instead of hanging
                daemon._stop_event.set()

        mocker.patch.object(daemon, "_supervisor_sleep", side_effect=advance_five_seconds)

        t0 = clock[0]
        result = daemon.run()

        assert polls[0] <= 200, "supervisor never gave up on the crash loop"
        assert result == 1
        assert len(spawned) == 5
        # The detection poll is the one after the worker is found dead, and
        # the poll granularity is 5s, so restarts land at >= the deadlines.
        offsets = [t - t0 for t in spawn_times]
        gaps = [b - a for a, b in zip(offsets, offsets[1:])]
        assert all(g >= d for g, d in zip(gaps, [10, 20, 40, 80])), gaps

    def test_uptime_of_at_least_600s_resets_the_count(self, daemon, mocker):
        clock = fake_clock(daemon, mocker)
        daemon._failures["snapshot"] = 5
        daemon._started_at["snapshot"] = clock[0] - WORKER_STABLE_UPTIME
        daemon._workers = {"snapshot": dead_worker()}

        daemon._check_workers()

        assert daemon._failures["snapshot"] == 1  # reset, then counted
        assert daemon._restart_at["snapshot"] == clock[0] + WORKER_RESTART_DELAY

    def test_uptime_just_under_600s_does_not_reset(self, daemon, mocker):
        clock = fake_clock(daemon, mocker)
        daemon._failures["snapshot"] = 4
        daemon._started_at["snapshot"] = clock[0] - (WORKER_STABLE_UPTIME - 1)
        daemon._workers = {"snapshot": dead_worker()}

        daemon._check_workers()

        assert daemon._failures["snapshot"] == 5
        assert daemon._restart_at["snapshot"] == clock[0] + 80

    def test_a_worker_that_keeps_recovering_never_reaches_the_limit(self, daemon, mocker):
        clock = fake_clock(daemon, mocker)
        spawned = []
        mocker.patch.object(
            daemon, "_new_worker",
            side_effect=lambda name: spawned.append(dead_worker()) or spawned[-1],
        )
        daemon._workers = {"snapshot": dead_worker()}
        for _ in range(12):  # twice the limit
            daemon._check_workers()
            clock[0] += WORKER_RESTART_MAX_DELAY
            daemon._check_workers()  # respawn
            clock[0] += WORKER_STABLE_UPTIME  # it then lives a long time
        assert len(spawned) == 12

    def test_a_worker_waiting_out_backoff_is_not_counted_again(self, daemon, mocker):
        clock = fake_clock(daemon, mocker)
        daemon._workers = {"snapshot": dead_worker()}
        daemon._check_workers()
        for _ in range(5):
            clock[0] += 0.5
            daemon._check_workers()
        assert daemon._failures["snapshot"] == 1

    def test_backoff_is_per_worker(self, daemon, mocker):
        clock = fake_clock(daemon, mocker)
        daemon._failures["pruning"] = 3
        daemon._workers = {"snapshot": dead_worker(), "pruning": dead_worker()}
        daemon._check_workers()
        assert daemon._restart_at["snapshot"] == clock[0] + 5
        assert daemon._restart_at["pruning"] == clock[0] + 40

    def test_check_workers_never_sleeps(self, daemon, mocker):
        """The restart path used to `time.sleep(WORKER_RESTART_DELAY)`
        inline, once per dead worker, holding up shutdown. Now: a deadline."""
        clock = fake_clock(daemon, mocker)
        sleeper = mocker.patch("zfsbackup.daemon.time.sleep")
        mocker.patch.object(daemon, "_new_worker", return_value=MagicMock(pid=1))
        daemon._workers = {n: dead_worker() for n in ("snapshot", "pruning", "api")}

        started = time.monotonic()
        daemon._check_workers()
        clock[0] += 1000
        daemon._check_workers()
        elapsed = time.monotonic() - started

        sleeper.assert_not_called()
        assert elapsed < 1.0

    def test_spawn_records_start_time_and_clears_the_deadline(self, daemon, mocker):
        clock = fake_clock(daemon, mocker)
        mocker.patch.object(daemon, "_new_worker", return_value=MagicMock(pid=1))
        daemon._restart_at["snapshot"] = 5.0

        daemon._spawn("snapshot", verb="Restarted")

        assert daemon._started_at["snapshot"] == clock[0]
        assert "snapshot" not in daemon._restart_at

    def test_spawn_records_the_worker_even_if_the_log_call_raises(self, daemon, mocker):
        """A-17: `start()` has forked a live child by the time the log line
        runs; if that raises, the child must already be tracked or it would
        never be joined or terminated at shutdown."""
        clock = fake_clock(daemon, mocker)
        worker = MagicMock(pid=4242)
        mocker.patch.object(daemon, "_new_worker", return_value=worker)
        mocker.patch.object(daemon_module.logger, "info", side_effect=RuntimeError("log sink broke"))

        with pytest.raises(RuntimeError, match="log sink broke"):
            daemon._spawn("snapshot")

        worker.start.assert_called_once()
        assert daemon._workers["snapshot"] is worker
        assert daemon._started_at["snapshot"] == clock[0]

        # ...so the shutdown path reaches it.
        worker.is_alive.return_value = False
        daemon._shutdown_workers(timeout=0)
        worker.join.assert_called()

    def test_spawn_does_not_record_a_worker_whose_start_failed(self, daemon, mocker):
        worker = MagicMock(pid=None)
        worker.start.side_effect = OSError("fork failed")
        mocker.patch.object(daemon, "_new_worker", return_value=worker)

        with pytest.raises(OSError):
            daemon._spawn("snapshot")

        assert "snapshot" not in daemon._workers
        assert "snapshot" not in daemon._started_at

    def test_exit_code_78_is_an_ordinary_crash_not_a_fatal_config_fault(self, daemon, mocker):
        """Item 15.2(b): a worker never opens the config database, so the
        `EX_CONFIG` (78) exit it used to report for a bad one no longer has
        a producer and no meaning. An exit code of 78 is backed off and
        restarted like any other; the supervisor has no "config failed" set
        left. Mutation-checked (scratch copy): treating exit code 78 as
        fatal again in `_check_workers` fails this test (1)."""
        clock = fake_clock(daemon, mocker)
        mocker.patch.object(daemon, "_new_worker", return_value=MagicMock(pid=2))
        daemon._workers = {"snapshot": dead_worker(exitcode=78)}
        daemon._check_workers()
        assert not hasattr(daemon, "_config_failed")
        assert daemon._crash_looped == set()
        assert daemon._restart_at == {"snapshot": clock[0] + WORKER_RESTART_DELAY}
        assert not daemon._stop_event.is_set()
        daemon._new_worker.assert_not_called()  # backing off, as for any crash
        clock[0] += WORKER_RESTART_DELAY
        daemon._check_workers()
        daemon._new_worker.assert_called_once_with("snapshot")


class TestBackoffNeverDelaysShutdown:
    def test_stop_flag_during_backoff_means_no_respawn(self, daemon, mocker, caplog):
        clock = fake_clock(daemon, mocker)
        mocker.patch.object(daemon, "_new_worker")
        daemon._workers = {"snapshot": dead_worker()}
        daemon._check_workers()  # schedules the restart
        clock[0] += 3600  # long past the deadline...
        daemon._stop_event.set()  # ...but a shutdown was requested

        with caplog.at_level(logging.INFO):
            daemon._check_workers()

        daemon._new_worker.assert_not_called()
        assert "Shutdown requested; not restarting" in caplog.text

    def test_stop_between_two_due_restarts_stops_the_second(self, daemon, mocker):
        clock = fake_clock(daemon, mocker)
        daemon._workers = {"snapshot": dead_worker(), "pruning": dead_worker()}
        daemon._check_workers()
        clock[0] += 5

        def spawn_then_stop(name):
            daemon._stop_event.set()
            return MagicMock(pid=1)

        mocker.patch.object(daemon, "_new_worker", side_effect=spawn_then_stop)
        daemon._check_workers()

        assert daemon._new_worker.call_count == 1

    def test_run_exits_promptly_while_a_restart_is_pending(self, daemon, mocker):
        patch_manager(mocker)
        fake_clock(daemon, mocker)
        mocker.patch.object(daemon, "_new_worker")
        mocker.patch.object(
            daemon, "_start_workers",
            side_effect=lambda: daemon._workers.update({"snapshot": dead_worker()}),
        )
        shutdown = mocker.patch.object(daemon, "_shutdown_workers")
        polls = [0]

        def sleep(_s):
            polls[0] += 1
            if polls[0] == 2:  # the poll after the backoff was scheduled
                daemon._signal_handler(signal.SIGTERM, None)

        mocker.patch.object(daemon, "_supervisor_sleep", side_effect=sleep)

        started = time.monotonic()
        result = daemon.run()

        assert time.monotonic() - started < 2.0
        assert result == 0
        assert polls[0] == 2
        daemon._new_worker.assert_not_called()
        shutdown.assert_called_once()

    def test_shutdown_still_reaps_a_worker_waiting_out_its_backoff(self, daemon, mocker):
        fake_clock(daemon, mocker)
        corpse = dead_worker()
        daemon._workers = {"snapshot": corpse}
        daemon._check_workers()
        assert "snapshot" in daemon._restart_at
        daemon._shutdown_workers(timeout=1)
        corpse.join.assert_called_once()


# ---------------------------------------------------------------------------
# A-5: interval floor
# ---------------------------------------------------------------------------


class TestIntervalFloor:
    def _run(self, worker_config, mocker, interval, cycles=3):
        mocker.patch("zfsbackup.backup_manager.zfs.exists", return_value=True)
        mocker.patch("zfsbackup.backup_manager.zfs.list", return_value=[])
        stop = MagicMock()
        stop.is_set.side_effect = [False] * cycles + [True] * 100
        worker = SnapshotWorker(worker_config, stop)
        mocker.patch.object(worker, "_get_interval", return_value=interval)
        mocker.patch.object(worker, "_process_dataset")
        return worker, stop

    @pytest.mark.parametrize("interval", [0.0, -5.0, 0.25, 0.999])
    def test_sub_second_interval_is_floored_with_one_warning(
        self, worker_config, mocker, caplog, interval
    ):
        worker, stop = self._run(worker_config, mocker, interval)
        with caplog.at_level(logging.WARNING):
            worker.run()

        waits = [c.kwargs["timeout"] for c in stop.wait.call_args_list]
        assert waits and all(w == MIN_WORKER_INTERVAL == 1.0 for w in waits)
        warnings = [r for r in caplog.records if "below the 1s minimum" in r.getMessage()]
        assert len(warnings) == 1  # once, not once per cycle
        assert str(interval) in warnings[0].getMessage()

    @pytest.mark.parametrize("interval", [1.0, 5.0, 300.0])
    def test_interval_at_or_above_the_floor_is_untouched_and_silent(
        self, worker_config, mocker, caplog, interval
    ):
        worker, stop = self._run(worker_config, mocker, interval)
        with caplog.at_level(logging.WARNING):
            worker.run()
        waits = [c.kwargs["timeout"] for c in stop.wait.call_args_list]
        assert waits and all(w == interval for w in waits)
        assert not [r for r in caplog.records if "minimum" in r.getMessage()]


# ---------------------------------------------------------------------------
# A-5: config_error_message / describe_config_source
# ---------------------------------------------------------------------------


def operational(message):
    import sqlite3

    return OperationalError("SELECT 1", {}, sqlite3.OperationalError(message))


class TestConfigErrorMessage:
    @pytest.fixture
    def resolved(self, tmp_path):
        return resolve_config_path(str(tmp_path / "config.db"))

    @pytest.fixture
    def no_diagnosis(self, mocker):
        return mocker.patch(
            "zfsbackup.config.store.messages.diagnose_open_failure", return_value=None
        )

    def test_unable_to_open_says_could_not_open(self, resolved, no_diagnosis):
        message = config_error_message(resolved, operational("unable to open database file"))
        assert message.startswith("Could not open the config database")
        assert "unable to open database file" in message
        assert describe_config_source(resolved) in message

    @pytest.mark.parametrize(
        "reason",
        ["no such table: datasets", "database is locked", "attempt to write a readonly database"],
    )
    def test_any_other_operational_error_says_failed_to_read(
        self, resolved, no_diagnosis, reason
    ):
        message = config_error_message(resolved, operational(reason))
        assert message.startswith("Failed to read the config database")
        assert "Could not open" not in message
        assert reason in message

    def test_a_diagnosed_open_failure_replaces_the_sqlite_text(self, resolved, mocker):
        diagnosed = ConfigDbPermissionError("directory X is not writable; chmod it")
        spy = mocker.patch(
            "zfsbackup.config.store.messages.diagnose_open_failure", return_value=diagnosed
        )
        exc = operational("unable to open database file")

        message = config_error_message(resolved, exc)

        assert message == str(diagnosed)
        spy.assert_called_once_with(resolved, exc, require_writable=True)

    def test_config_path_and_schema_errors_are_returned_verbatim(self, resolved):
        text = "multi\nline\n  zfsbackup-config import /x.yaml"
        assert config_error_message(resolved, ConfigDbPermissionError(text)) == text
        assert config_error_message(None, ConfigPathError(text)) == text
        # Every other SchemaError stays verbatim too (item 10.4 adds
        # NotAConfigDatabase to the family).
        for cls in (
            SchemaSplitBrain,
            SchemaVersionMismatch,
            NotAConfigDatabase,
            SchemaIntegrityError,
        ):
            assert config_error_message(resolved, cls(text)) == text, cls

    def test_schema_out_of_date_gets_the_daemons_own_sentence_appended(self, resolved):
        """The store's SchemaOutOfDate text is neutral (the CLI shows it as
        is); the daemon appends what is true of the daemon only."""
        text = "/x/config.db is at schema revision a; this package expects b. Run `zfsbackup-config migrate`."
        message = config_error_message(resolved, SchemaOutOfDate(text))
        assert message == (
            text + "\nThe daemon only checks the schema and never migrates it; "
            "run the command above, then start the daemon again."
        )
        # Appended, never prefixed: the pasteable command keeps its place.
        assert message.startswith(text)

    def test_other_exceptions_get_the_generic_prefix(self, resolved):
        message = config_error_message(resolved, KeyError("boom"))
        assert message.startswith("Failed to load configuration")
        assert describe_config_source(resolved) in message

    def test_without_a_resolved_path_there_is_no_source_suffix(self):
        assert config_error_message(None, ValueError("x")) == "Failed to load configuration: x"
        # No resolved path means no diagnosis to attempt either.
        assert config_error_message(None, operational("unable to open database file")).startswith(
            "Failed to load configuration"
        )


class TestDescribeConfigSource:
    def test_dash_c(self, tmp_path):
        resolved = resolve_config_path(str(tmp_path / "config.db"))
        text = describe_config_source(resolved)
        assert text.endswith("(via -c/--config)")
        assert str(resolved.path) in text

    def test_environment_variable(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ZFSBACKUP_CONFIG", str(tmp_path / "env.db"))
        resolved = resolve_config_path(None)
        assert describe_config_source(resolved).endswith("(via $ZFSBACKUP_CONFIG)")

    def test_default_path_suppresses_the_resolved_form(self, monkeypatch):
        monkeypatch.delenv("ZFSBACKUP_CONFIG", raising=False)
        resolved = resolve_config_path(None)
        text = describe_config_source(resolved)
        assert text.endswith("(default path)")
        assert "->" not in text  # `/var` -> `/private/var` would be noise

    def test_a_symlinked_explicit_path_shows_where_it_points(self, tmp_path):
        real = tmp_path / "real.db"
        link = tmp_path / "link.db"
        os.symlink(real, link)
        resolved = resolve_config_path(str(link))
        text = describe_config_source(resolved)
        assert f"{link} -> " in text and "real.db" in text
        assert text.endswith("(via -c/--config)")


# ---------------------------------------------------------------------------
# A-5 D3 and the daemon-level "missing DB" behaviour
# ---------------------------------------------------------------------------


class TestMainFailureReporting:
    """Each test is backed by a real (schema-current) database file: since
    item 15.5 `_run` stats the database and takes the presence lock BEFORE
    `load_runtime_config`, so a path with no database behind it is refused
    by the preflight and never reaches the mocked load. `lock_spy` closes a
    lock that a refusal path did not release.
    """

    def _failure_records(self, caplog):
        return [
            r for r in caplog.records
            if r.name == "zfsbackup.daemon" and r.levelno == logging.ERROR
        ]

    def test_unexpected_exceptions_are_logged_with_a_traceback(self, sqlite_config_db, lock_spy, mocker, caplog):
        mocker.patch(
            "zfsbackup.daemon.load_runtime_config", side_effect=KeyError("surprise")
        )
        with patch("sys.argv", ["daemon", "-c", str(sqlite_config_db.path)]):
            with caplog.at_level(logging.ERROR):
                assert main() == 1
        (record,) = self._failure_records(caplog)
        assert record.exc_info is not None
        assert "surprise" in "".join(str(x) for x in record.exc_info[1].args)

    def test_operational_errors_carry_no_traceback(self, sqlite_config_db, lock_spy, mocker, caplog):
        mocker.patch(
            "zfsbackup.daemon.load_runtime_config",
            side_effect=operational("database is locked"),
        )
        mocker.patch("zfsbackup.config.store.messages.diagnose_open_failure", return_value=None)
        with patch("sys.argv", ["daemon", "-c", str(sqlite_config_db.path)]):
            with caplog.at_level(logging.ERROR):
                assert main() == 1
        (record,) = self._failure_records(caplog)
        assert not record.exc_info
        assert "Failed to read the config database" in record.getMessage()

    def test_config_path_and_schema_errors_carry_no_traceback(self, sqlite_config_db, lock_spy, mocker, caplog):
        mocker.patch(
            "zfsbackup.daemon.load_runtime_config",
            side_effect=SchemaOutOfDate("stale schema"),
        )
        with patch("sys.argv", ["daemon", "-c", str(sqlite_config_db.path)]):
            with caplog.at_level(logging.ERROR):
                assert main() == 1
        (record,) = self._failure_records(caplog)
        assert not record.exc_info
        assert record.getMessage() == (
            "stale schema\nThe daemon only checks the schema and never "
            "migrates it; run the command above, then start the daemon again."
        )

    def test_other_schema_errors_are_logged_verbatim_without_traceback(
        self, sqlite_config_db, lock_spy, mocker, caplog
    ):
        mocker.patch(
            "zfsbackup.daemon.load_runtime_config",
            side_effect=NotAConfigDatabase("not a zfsbackup config database"),
        )
        with patch("sys.argv", ["daemon", "-c", str(sqlite_config_db.path)]):
            with caplog.at_level(logging.ERROR):
                assert main() == 1
        (record,) = self._failure_records(caplog)
        assert not record.exc_info
        assert record.getMessage() == "not a zfsbackup config database"


class TestMissingDatabase:
    @pytest.mark.parametrize("extra", [[], ["--test-config"]])
    def test_missing_db_exits_one_and_creates_nothing(self, tmp_path, caplog, extra):
        target = tmp_path / "nodir" / "config.db"
        before = sorted(p.name for p in tmp_path.iterdir())

        with patch("sys.argv", ["daemon", "-c", str(target), *extra]):
            with caplog.at_level(logging.ERROR):
                code = main()

        assert code == 1
        assert not target.exists()
        assert not target.parent.exists()
        assert sorted(p.name for p in tmp_path.iterdir()) == before
        assert "zfsbackup-config import" in caplog.text or "No config database" in caplog.text
        assert "Traceback" not in caplog.text

    def test_missing_db_in_an_existing_directory_creates_no_file(self, tmp_path, caplog):
        target = tmp_path / "config.db"
        with patch("sys.argv", ["daemon", "-c", str(target)]):
            with caplog.at_level(logging.ERROR):
                assert main() == 1
        assert list(tmp_path.iterdir()) == []

    def test_missing_db_via_environment_variable(self, tmp_path, monkeypatch, caplog):
        target = tmp_path / "env" / "config.db"
        monkeypatch.setenv("ZFSBACKUP_CONFIG", str(target))
        with patch("sys.argv", ["daemon"]):
            with caplog.at_level(logging.ERROR):
                assert main() == 1
        assert not target.parent.exists()
        assert "ZFSBACKUP_CONFIG" in caplog.text


# ---------------------------------------------------------------------------
# A-6.2: shutdown hardening
# ---------------------------------------------------------------------------


class TestRunFinally:
    def _arm(self, daemon, mocker):
        patch_manager(mocker)
        return mocker.patch.object(daemon, "_shutdown_workers")

    def test_a_poll_loop_exception_still_shuts_workers_down(self, daemon, mocker):
        shutdown = self._arm(daemon, mocker)
        mocker.patch.object(daemon, "_start_workers")
        mocker.patch.object(daemon, "_supervisor_sleep", side_effect=RuntimeError("boom"))

        with pytest.raises(RuntimeError, match="boom"):
            daemon.run()

        shutdown.assert_called_once()
        # The flag is raised first, so workers can reach a safe point.
        assert daemon._stop_event.is_set()

    def test_a_check_workers_exception_still_shuts_workers_down(self, daemon, mocker):
        shutdown = self._arm(daemon, mocker)
        mocker.patch.object(daemon, "_start_workers")
        mocker.patch.object(daemon, "_supervisor_sleep")
        mocker.patch.object(daemon, "_check_workers", side_effect=OSError("EAGAIN"))

        with pytest.raises(OSError):
            daemon.run()

        shutdown.assert_called_once()

    def test_a_start_workers_exception_still_shuts_workers_down(self, daemon, mocker):
        shutdown = self._arm(daemon, mocker)
        mocker.patch.object(daemon, "_start_workers", side_effect=OSError("EAGAIN"))

        with pytest.raises(OSError):
            daemon.run()

        shutdown.assert_called_once()
        assert daemon._stop_event.is_set()

    def test_the_stop_flag_is_raised_before_the_join(self, daemon, mocker):
        patch_manager(mocker)
        mocker.patch.object(daemon, "_start_workers")
        mocker.patch.object(daemon, "_supervisor_sleep", side_effect=RuntimeError("boom"))
        seen = []
        mocker.patch.object(
            daemon, "_shutdown_workers",
            side_effect=lambda: seen.append(daemon._stop_event.is_set()),
        )
        with pytest.raises(RuntimeError):
            daemon.run()
        assert seen == [True]

    def test_main_reports_a_crashed_run_as_exit_one(
        self, sqlite_config_db, lock_spy, mocker, caplog
    ):
        mocker.patch(
            "zfsbackup.daemon.load_runtime_config",
            return_value=ConfigSnapshot(MagicMock(dry_run=False), 3, "rev"),
        )
        crashing = MagicMock()
        crashing.run.side_effect = RuntimeError("poll loop died")
        crashing.abandoned_workers = []
        mocker.patch("zfsbackup.daemon.BackupDaemon", return_value=crashing)
        with patch("sys.argv", ["daemon", "-c", str(sqlite_config_db.path)]):
            with caplog.at_level(logging.ERROR):
                assert main() == 1
        assert "Daemon failed: poll loop died" in caplog.text
        # A crash is not a clean stop: the pid file is left (the kernel drops
        # the lock when the process exits, and a stale file is harmless).
        assert lock_spy and lock_spy[0].path.exists()


class TestStartWorkersHonoursTheStopFlag:
    def test_flag_set_between_spawns_stops_the_remaining_forks(self, daemon, mocker, caplog):
        spawned = []

        def spawn(name, verb="Started"):
            spawned.append(name)
            daemon._stop_event.set()  # SIGTERM lands after the first fork

        mocker.patch.object(daemon, "_spawn", side_effect=spawn)
        names = daemon._active_worker_names()
        assert len(names) >= 3

        with caplog.at_level(logging.INFO):
            daemon._start_workers()

        assert spawned == [names[0]]
        assert "Shutdown requested while starting workers" in caplog.text
        for skipped in names[1:]:
            assert skipped in caplog.text

    def test_flag_already_set_spawns_nothing(self, daemon, mocker):
        mocker.patch.object(daemon, "_spawn")
        daemon._stop_event.set()
        daemon._start_workers()
        daemon._spawn.assert_not_called()

    def test_flag_clear_spawns_everything_in_order(self, daemon, mocker):
        mocker.patch.object(daemon, "_spawn")
        daemon._start_workers()
        assert [c.args[0] for c in daemon._spawn.call_args_list] == daemon._active_worker_names()


class TestAbandonedWorkers:
    def test_a_worker_surviving_sigkill_is_recorded(self, daemon):
        worker = MagicMock()
        worker.pid = 4242
        worker.is_alive.return_value = True
        daemon._workers = {"snapshot": worker}
        daemon._shutdown_workers(timeout=0)
        assert daemon.abandoned_workers == ["snapshot"]

    def test_a_worker_that_exits_is_not_recorded(self, daemon):
        worker = MagicMock()
        worker.is_alive.return_value = False
        daemon._workers = {"snapshot": worker}
        daemon._shutdown_workers(timeout=0)
        assert daemon.abandoned_workers == []

    def test_main_hard_exits_with_the_runs_code_when_workers_were_abandoned(self, tmp_path, mocker):
        def fake_run(args, abandoned):
            abandoned.extend(["snapshot", "pruning"])
            return 1

        mocker.patch("zfsbackup.daemon._run", side_effect=fake_run)
        order = []
        mocker.patch(
            "zfsbackup.daemon.dispose_all", side_effect=lambda: order.append("dispose_all")
        )
        mocker.patch(
            "zfsbackup.daemon._hard_exit",
            side_effect=lambda code, names: order.append(("hard_exit", code, list(names))),
        )
        with patch("sys.argv", ["daemon", "-c", str(tmp_path / "c.db")]):
            main()
        assert order == ["dispose_all", ("hard_exit", 1, ["snapshot", "pruning"])]

    def test_main_returns_normally_when_nothing_was_abandoned(self, tmp_path, mocker):
        mocker.patch("zfsbackup.daemon._run", return_value=0)
        mocker.patch("zfsbackup.daemon.dispose_all")
        hard = mocker.patch("zfsbackup.daemon._hard_exit")
        with patch("sys.argv", ["daemon", "-c", str(tmp_path / "c.db")]):
            assert main() == 0
        hard.assert_not_called()

    def test_run_collects_abandoned_workers_even_when_run_crashes(
        self, sqlite_config_db, lock_spy, mocker
    ):
        crashing = MagicMock()
        crashing.run.side_effect = RuntimeError("x")
        crashing.abandoned_workers = ["api"]
        mocker.patch(
            "zfsbackup.daemon.load_runtime_config",
            return_value=ConfigSnapshot(MagicMock(dry_run=False), 3, "rev"),
        )
        mocker.patch("zfsbackup.daemon.BackupDaemon", return_value=crashing)
        collected = []
        args = MagicMock(
            config=str(sqlite_config_db.path), dry_run=False, test_config=False, verbose=False
        )
        assert daemon_module._run(args, collected) == 1
        assert collected == ["api"]

    def test_hard_exit_skips_atexit_handlers(self):
        """`os._exit`, not `sys.exit`: multiprocessing's atexit handler
        would `join()` an abandoned child with no timeout. Proven in a
        subprocess, because the point is what the *interpreter* does."""
        script = textwrap.dedent(
            """
            import atexit, sys
            atexit.register(lambda: print("ATEXIT-RAN", flush=True))
            from zfsbackup import daemon
            print("before", flush=True)
            daemon._hard_exit(7, ["snapshot-worker"])
            print("UNREACHABLE", flush=True)
            """
        )
        env = dict(os.environ)
        proc = subprocess.run(
            [sys.executable, "-c", script], cwd=ROOT, env=env,
            capture_output=True, text=True, timeout=60,
        )
        assert proc.returncode == 7, proc.stderr
        assert "ATEXIT-RAN" not in proc.stdout
        assert "UNREACHABLE" not in proc.stdout
        # Logging was flushed before the hard exit: the operator can see why.
        assert "Exiting without waiting for abandoned worker(s): snapshot-worker" in (
            proc.stdout + proc.stderr
        )

    @pytest.mark.parametrize("code, expected", [(0, 1), (1, 1), (2, 2), (7, 7)])
    def test_hard_exit_never_reports_success_for_abandoned_workers(
        self, monkeypatch, code, expected
    ):
        """A-17: abandoned workers mean this was not a clean stop, so an
        otherwise-clean run (code 0) must still exit non-zero."""
        exits = []
        monkeypatch.setattr(daemon_module.os, "_exit", lambda c: exits.append(c))
        monkeypatch.setattr(daemon_module.logging, "shutdown", lambda: None)  # keep pytest's handlers

        daemon_module._hard_exit(code, ["snapshot"])

        assert exits == [expected]

    def test_hard_exit_with_code_zero_exits_1_in_a_real_interpreter(self):
        script = textwrap.dedent(
            """
            from zfsbackup import daemon
            daemon._hard_exit(0, ["snapshot-worker"])
            print("UNREACHABLE", flush=True)
            """
        )
        proc = subprocess.run(
            [sys.executable, "-c", script], cwd=ROOT, env=dict(os.environ),
            capture_output=True, text=True, timeout=60,
        )
        assert proc.returncode == 1, proc.stderr
        assert "UNREACHABLE" not in proc.stdout

    def test_a_normal_sys_exit_does_run_atexit_handlers_control(self):
        # Control for the test above: the same script with a plain exit DOES
        # run the handler, so its absence there is caused by `_hard_exit`.
        script = textwrap.dedent(
            """
            import atexit, sys
            atexit.register(lambda: print("ATEXIT-RAN", flush=True))
            from zfsbackup import daemon
            sys.exit(7)
            """
        )
        proc = subprocess.run(
            [sys.executable, "-c", script], cwd=ROOT, env=dict(os.environ),
            capture_output=True, text=True, timeout=60,
        )
        assert proc.returncode == 7
        assert "ATEXIT-RAN" in proc.stdout


# ---------------------------------------------------------------------------
# A-6.2: SIGHUP
# ---------------------------------------------------------------------------


class TestSighupStopgap:
    def test_handler_sets_only_the_flag_and_logs_nothing(self, daemon, caplog):
        with caplog.at_level(logging.DEBUG):
            daemon._sighup_handler(signal.SIGHUP, None)
        assert daemon._hup_requested is True
        assert not daemon._stop_event.is_set()
        assert daemon._stop_signum is None
        assert caplog.records == []

    def test_poll_loop_logs_the_stopgap_message_once(self, daemon, caplog):
        daemon._sighup_handler(signal.SIGHUP, None)
        with caplog.at_level(logging.WARNING):
            daemon._report_hup()
            daemon._report_hup()
        messages = [r.getMessage() for r in caplog.records]
        assert messages == ["Received SIGHUP: live reload not implemented; restart to apply"]
        assert daemon._hup_requested is False

    def test_silent_without_a_signal(self, daemon, caplog):
        with caplog.at_level(logging.WARNING):
            daemon._report_hup()
        assert caplog.records == []

    def test_run_reports_a_hup_and_keeps_running(self, daemon, mocker, caplog):
        patch_manager(mocker)
        mocker.patch.object(daemon, "_start_workers")
        mocker.patch.object(daemon, "_check_workers")
        mocker.patch.object(daemon, "_shutdown_workers")
        polls = [0]

        def sleep(_s):
            polls[0] += 1
            if polls[0] == 1:
                daemon._sighup_handler(signal.SIGHUP, None)
            elif polls[0] == 3:
                daemon._stop_event.set()

        mocker.patch.object(daemon, "_supervisor_sleep", side_effect=sleep)
        with caplog.at_level(logging.WARNING):
            assert daemon.run() == 0

        assert "live reload not implemented" in caplog.text
        assert daemon._check_workers.call_count >= 1  # the daemon kept running past the HUP

    def test_a_real_sighup_does_not_terminate_the_supervisor(self, daemon):
        """SIGHUP's default action kills the process, orphaning every worker."""
        assert signal.getsignal(signal.SIGHUP) == daemon._sighup_handler
        os.kill(os.getpid(), signal.SIGHUP)
        deadline = time.monotonic() + 5
        while not daemon._hup_requested and time.monotonic() < deadline:
            time.sleep(0.01)
        assert daemon._hup_requested is True
        assert not daemon._stop_event.is_set()


class TestWorkersIgnoreSighup:
    def test_init_child_process_sets_sig_ign(self):
        _init_child_process()
        assert signal.getsignal(signal.SIGHUP) == signal.SIG_IGN
        # ...and still resets the stop signals to their defaults.
        assert signal.getsignal(signal.SIGINT) == signal.SIG_DFL
        assert signal.getsignal(signal.SIGTERM) == signal.SIG_DFL

    @pytest.mark.skipif(not FORK_AVAILABLE, reason="needs the fork start method")
    def test_a_forked_worker_survives_a_hangup(self, sqlite_config_db):
        """A worker that died on SIGHUP (terminal closed) would just be a
        crash to restart. Forked as a real child, signalled for real."""
        ctx = multiprocessing.get_context("fork")
        proc = ctx.Process(target=_child_survives_hup, daemon=True)
        proc.start()
        try:
            proc.join(timeout=20)
            alive = proc.is_alive()
        finally:
            if proc.is_alive():
                proc.kill()
                proc.join(timeout=5)
        assert not alive, "child hung"
        # 0 = survived; a negative code is the signal that killed it.
        assert proc.exitcode == 0


def _child_survives_hup():
    _init_child_process()
    os.kill(os.getpid(), signal.SIGHUP)
    time.sleep(0.2)
    os._exit(0)

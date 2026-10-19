"""A4 -- the first writer's `PRAGMA journal_mode=WAL` retries while the file is busy.

Switching a rollback-journal database to WAL needs the write lock as an UPGRADE
from the SHARED lock the connection already holds, and SQLite never runs the
busy handler for that step, so `busy_timeout` does not cover it: against a
concurrent writer (`BEGIN IMMEDIATE`) it fails at once with `database is
locked`. The other steps (a reader draining, SHARED while another connection is
PENDING) DO wait in the busy handler, up to `busy_timeout`.

`db._execute_wal_pragma` therefore retries, to a WALL-CLOCK deadline
(`_WAL_SWITCH_BUDGET_S` == `_BUSY_TIMEOUT_MS / 1000`, read through the patchable
`db._wal_switch_clock`): before each attempt `busy_timeout` is lowered to
`clamp(ceil(remaining_ms / 2), 1, _BUSY_TIMEOUT_MS)` -- HALF the time left, since
one statement can wait for SHARED and then again, with a fresh count, for
EXCLUSIVE -- so one attempt cannot overrun,
it is put back after every attempt (on an error path a failed restore is logged
and never replaces the error in flight; on success it PROPAGATES so the connect
hook closes the connection instead of pooling one on a tiny timeout), and sleeps (patchable `db._wal_switch_sleep`) are
`min(delay, remaining)` with the delay 10 ms doubling to 250 ms. Only
SQLITE_BUSY / SQLITE_LOCKED (by `sqlite_errorcode & 0xFF`; message text only if
the attribute is missing) are retried; anything else raises at once, and
exhaustion re-raises the last error unchanged.

Hermetic and fast: real SQLite files under `tmp_path`, always built through
`store.db`. The `sleeps` fixture replaces the sleep AND the clock with a fake
clock that the fake sleep advances, so exhaustion costs no real time; the
real-wait tests (threaded holder, wall-time bound) use reduced constants. The
lock holder is a second plain `sqlite3` connection in autocommit
(`isolation_level=None`); by default it holds `BEGIN IMMEDIATE; INSERT` and is
released by a COMMIT with `timeout=0`, which can only succeed if the writer
holds no SHARED lock while it sleeps.

Mutation counts (scratch copy, one revert at a time, failing tests in this file
plus `test_zfsbackup_store.py`): no retry (raise on the first error) 30; retry
every error 22; no restore of `busy_timeout` 19; text-only classification
(ignore `sqlite_errorcode`) 11; 50 s budget 13; no doubling 10; sum-of-sleeps
budget instead of the clock 5; a failing restore allowed to raise 4; the
per-attempt timeout not lowered 4; sleep not clipped 4; delay not capped 4;
extended codes not masked to the primary code 3; no clamp to >= 1 ms 2; no
clamp to <= 5000 ms 2; `floor` instead of `ceil` 1. Equivalent mutants:
`_WAL_SWITCH_BUDGET_S = 5.0` written as a literal (same value), and
`except Exception` in the loop (`_is_busy_error` still gates it).
"""

import logging
import math
import sqlite3
import threading
import time
from pathlib import Path

import pytest
from sqlalchemy.exc import OperationalError as SAOperationalError

from zfsbackup.config.store import dispose_all
from zfsbackup.config.store import db as store_db
from zfsbackup.config.store.db import make_engine
from zfsbackup.config.store.migrate import EXPECTED_HEAD

pytestmark = pytest.mark.integration

LOCKED = "database is locked"
HEAD = EXPECTED_HEAD  # the real schema head: a revision this code recognises as ours


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _header(path):
    """File-format bytes 18-19: (1, 1) = rollback journal, (2, 2) = WAL."""
    raw = Path(path).read_bytes()
    return raw[18], raw[19]


def _journal_mode(path) -> str:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute("PRAGMA journal_mode").fetchone()[0]
    finally:
        conn.close()


def _ours(path) -> Path:
    """A rollback-journal file that holds a table and is recognisably ours
    (a known Alembic revision), so a writer engine is allowed to switch it."""
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL PRIMARY KEY)")
    conn.execute("INSERT INTO alembic_version VALUES (?)", (HEAD,))
    conn.execute("CREATE TABLE datasets (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()
    assert _header(path) == (1, 1)
    return Path(path)


class Blocker:
    """A second connection holding RESERVED on `path`: `BEGIN IMMEDIATE` plus
    an uncommitted INSERT (a real write, not an empty transaction).
    `release()` COMMITs it with `timeout=0`, so it raises `database is locked`
    if the connection being tested still holds a SHARED lock at that moment."""

    def __init__(self, path, write="INSERT INTO datasets DEFAULT VALUES"):
        self.conn = sqlite3.connect(str(path), isolation_level=None, timeout=0)
        self.conn.execute("BEGIN IMMEDIATE")
        self.conn.execute(write)
        self.held = True

    def release(self):
        if self.held:
            self.held = False
            self.conn.execute("COMMIT")

    def close(self):
        try:
            if self.held:
                self.held = False
                self.conn.execute("ROLLBACK")
        finally:
            self.conn.close()


@pytest.fixture
def blocker_for():
    """`blocker_for(path)` -> a held `Blocker`, always closed at teardown."""
    made = []

    def make(path, **kwargs):
        blocker = Blocker(path, **kwargs)
        made.append(blocker)
        return blocker

    yield make
    for blocker in made:
        blocker.close()


class FakeClock:
    """A monotonic clock that only moves when told to."""

    def __init__(self, start=1000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


@pytest.fixture
def sleeps(monkeypatch):
    """Record every requested delay; nothing really sleeps and the retry loop
    runs on a fake clock that each fake sleep advances by exactly what was
    requested. `sleeps.clock` is that clock; a test that needs a side effect
    on a sleep sets `sleeps.hook(n)` (called after the clock moved)."""

    class Recorder(list):
        hook = None

    rec = Recorder()
    rec.clock = FakeClock()

    def fake_sleep(seconds):
        rec.append(seconds)
        rec.clock.advance(seconds)
        if rec.hook is not None:
            rec.hook(len(rec))

    monkeypatch.setattr(store_db, "_wal_switch_sleep", fake_sleep)
    monkeypatch.setattr(store_db, "_wal_switch_clock", rec.clock)
    return rec


@pytest.fixture
def db_logger():
    """`zfsbackup.config.store.db`'s logger may have been disabled by another
    test's `fileConfig()`; make it live for the test."""
    logger = logging.getLogger("zfsbackup.config.store.db")
    original = (logger.level, logger.disabled)
    logger.disabled = False
    yield logger
    logger.level, logger.disabled = original


class FakeCursor:
    """Raises `errors` in order from the `journal_mode=WAL` execute, then
    succeeds returning `row`. `busy_timeout` statements are recorded apart:

    - `pre`   the value set before each attempt,
    - `post`  the value set after each attempt (the restore);
    - `wal`   one entry per `journal_mode=WAL` execute;
    - `starts` the `clock` reading at the start of each of those;
    - `attempt_cost` advances `clock` during each (a slow, busy-waiting attempt);
    - `restore_errors` maps the index of a `post` statement to the error it raises.
    """

    def __init__(self, errors=(), row=("wal",), clock=None, attempt_cost=0.0, restore_errors=None):
        self.errors = list(errors)
        self.row = row
        self.clock = clock
        self.attempt_cost = attempt_cost
        self.restore_errors = restore_errors or {}
        self.statements = []
        self.wal = []
        self.starts = []
        self.pre = []
        self.post = []
        self.fetches = 0
        self._after_wal = False

    def execute(self, sql, *args):
        self.statements.append(sql)
        if sql.startswith("PRAGMA busy_timeout="):
            value = int(sql.split("=")[1])
            if self._after_wal:
                self._after_wal = False
                index = len(self.post)
                self.post.append(value)
                if index in self.restore_errors:
                    raise self.restore_errors[index]
            else:
                self.pre.append(value)
            return
        assert sql == "PRAGMA journal_mode=WAL", sql
        self.wal.append(sql)
        self._after_wal = True
        if self.clock is not None:
            self.starts.append(self.clock())
            self.clock.advance(self.attempt_cost)
        if self.errors:
            raise self.errors.pop(0)

    def fetchone(self):
        self.fetches += 1
        return self.row


def _locked():
    return sqlite3.OperationalError(LOCKED)


def _coded(code, message=LOCKED):
    """An OperationalError carrying `sqlite_errorcode`, as the driver sets it."""
    error = sqlite3.OperationalError(message)
    error.sqlite_errorcode = code
    return error


# ---------------------------------------------------------------------------
# the constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_the_documented_schedule(self):
        assert store_db._BUSY_TIMEOUT_MS == 5000
        assert store_db._WAL_SWITCH_BUDGET_S == 5.0
        assert store_db._WAL_SWITCH_FIRST_DELAY_S == 0.01
        assert store_db._WAL_SWITCH_MAX_DELAY_S == 0.25

    def test_the_budget_is_the_busy_timeout(self):
        """One constant behind both, so they cannot drift apart."""
        assert store_db._WAL_SWITCH_BUDGET_S == store_db._BUSY_TIMEOUT_MS / 1000

    def test_the_sleep_is_a_module_level_function_so_it_can_be_patched(self, monkeypatch):
        seen = []
        monkeypatch.setattr(store_db.time, "sleep", seen.append)
        store_db._wal_switch_sleep(0.123)
        assert seen == [0.123]

    def test_the_clock_is_monotonic_and_patchable(self):
        assert store_db._wal_switch_clock is time.monotonic

    def test_the_connect_hook_sets_the_busy_timeout_constant_and_the_switch_restores_it(
        self, tmp_path, monkeypatch, blocker_for, sleeps
    ):
        """The hook's `busy_timeout` is the constant (not a literal 5000), and
        it is what the connection is left with after a retried switch."""
        monkeypatch.setattr(store_db, "_BUSY_TIMEOUT_MS", 1234)
        monkeypatch.setattr(store_db, "_WAL_SWITCH_BUDGET_S", 1.234)
        path = _ours(tmp_path / "const.db")
        blocker = blocker_for(path)
        sleeps.hook = lambda n: blocker.release()
        eng = make_engine(f"sqlite:///{path}")
        try:
            with eng.connect() as conn:
                assert conn.exec_driver_sql("PRAGMA busy_timeout").scalar() == 1234
                assert conn.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"
        finally:
            eng.dispose()
        assert sleeps == [0.01]

    def test_the_hooks_own_first_pragma_is_the_constant(self, tmp_path, monkeypatch):
        """Seen on a READER engine, which never runs the WAL switch (and so
        never restores anything): the value left on the connection is the
        hook's own `busy_timeout`, taken from the constant."""
        monkeypatch.setattr(store_db, "_BUSY_TIMEOUT_MS", 1234)
        path = _ours(tmp_path / "reader_const.db")
        eng = make_engine(f"sqlite:///{path}", readonly=True)
        try:
            with eng.connect() as conn:
                assert conn.exec_driver_sql("PRAGMA busy_timeout").scalar() == 1234
        finally:
            eng.dispose()


# ---------------------------------------------------------------------------
# a real lock holder, a real file
# ---------------------------------------------------------------------------


class TestAgainstARealLockHolder:
    def test_the_premise_a_plain_switch_fails_instantly_while_a_writer_holds_the_file(
        self, tmp_path, blocker_for
    ):
        """Why the loop exists: the un-retried statement fails at once, even
        with a generous `timeout` -- the busy handler is not run for an
        upgrade. If SQLite ever changes that, this fails and the retry loop is
        no longer needed."""
        path = _ours(tmp_path / "premise.db")
        blocker_for(path)
        conn = sqlite3.connect(str(path), timeout=5.0)
        try:
            started = time.monotonic()
            with pytest.raises(sqlite3.OperationalError, match=LOCKED) as excinfo:
                conn.execute("PRAGMA journal_mode=WAL").fetchone()
            assert time.monotonic() - started < 2
        finally:
            conn.close()
        assert excinfo.value.sqlite_errorcode == sqlite3.SQLITE_BUSY
        assert store_db._is_busy_error(excinfo.value)
        assert _header(path) == (1, 1)

    def test_a_cursor_retries_until_the_holder_lets_go(self, tmp_path, blocker_for, sleeps):
        path = _ours(tmp_path / "retry.db")
        blocker = blocker_for(path)
        sleeps.hook = lambda n: blocker.release()
        conn = sqlite3.connect(str(path), timeout=0)
        try:
            row = store_db._execute_wal_pragma(conn.cursor())
        finally:
            conn.close()
        assert row == ("wal",)
        assert sleeps == [0.01]  # exactly one sleep, the first delay
        assert _journal_mode(path) == "wal"

    def test_a_writer_engine_ends_in_wal_after_exactly_one_sleep(
        self, tmp_path, blocker_for, sleeps
    ):
        path = _ours(tmp_path / "engine.db")
        blocker = blocker_for(path)
        sleeps.hook = lambda n: blocker.release()
        eng = make_engine(f"sqlite:///{path}")
        try:
            with eng.connect() as conn:
                assert conn.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"
                assert conn.exec_driver_sql("PRAGMA busy_timeout").scalar() == 5000
        finally:
            eng.dispose()
        assert sleeps == [0.01]
        assert _header(path) == (2, 2)
        # the engine's dispose checkpointed and removed the sidecars
        assert not Path(f"{path}-wal").exists() and not Path(f"{path}-shm").exists()

    def test_the_holders_commit_succeeds_from_inside_the_sleep_so_no_shared_lock_is_held(
        self, tmp_path, blocker_for, sleeps
    ):
        """The holder's COMMIT needs an exclusive lock and runs with
        `timeout=0` from inside the sleep: it can only succeed if the writer
        engine's connection holds NO lock while it sleeps. (A failed attempt
        releases its SHARED lock; holding it across the sleep would deadlock
        the two writers.) The committed row must be there afterwards."""
        path = _ours(tmp_path / "nolock.db")
        blocker = blocker_for(path)
        committed = []

        def commit(n):
            blocker.release()  # raises `database is locked` if a SHARED lock is held
            committed.append(n)

        sleeps.hook = commit
        eng = make_engine(f"sqlite:///{path}")
        try:
            with eng.connect() as conn:
                assert conn.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"
                assert conn.exec_driver_sql("SELECT count(*) FROM datasets").scalar() == 1
        finally:
            eng.dispose()
        assert committed == [1]
        assert _header(path) == (2, 2)

    def test_a_holder_released_after_several_sleeps_costs_that_many(
        self, tmp_path, blocker_for, sleeps
    ):
        path = _ours(tmp_path / "later.db")
        blocker = blocker_for(path)
        sleeps.hook = lambda n: blocker.release() if n == 4 else None
        conn = sqlite3.connect(str(path), timeout=0)
        try:
            assert store_db._execute_wal_pragma(conn.cursor()) == ("wal",)
        finally:
            conn.close()
        assert sleeps == [0.01, 0.02, 0.04, 0.08]
        assert _journal_mode(path) == "wal"

    def test_a_holder_that_never_lets_go_exhausts_the_budget_with_the_original_error(
        self, tmp_path, blocker_for, sleeps
    ):
        path = _ours(tmp_path / "stuck.db")
        blocker_for(path)
        conn = sqlite3.connect(str(path), timeout=0)
        try:
            with pytest.raises(sqlite3.OperationalError) as excinfo:
                store_db._execute_wal_pragma(conn.cursor())
            # ...and the connection is left with the connect-hook's timeout.
            assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000
        finally:
            conn.close()
        # Same type and message as the un-retried statement raised.
        assert type(excinfo.value) is sqlite3.OperationalError
        assert str(excinfo.value) == LOCKED
        assert sum(sleeps) == pytest.approx(store_db._WAL_SWITCH_BUDGET_S)
        assert max(sleeps) <= store_db._WAL_SWITCH_MAX_DELAY_S
        assert _header(path) == (1, 1)  # nothing was switched

    def test_a_writer_engine_against_a_holder_that_never_lets_go_raises_the_lock_error(
        self, tmp_path, blocker_for, sleeps
    ):
        path = _ours(tmp_path / "stuck_engine.db")
        blocker_for(path)
        eng = make_engine(f"sqlite:///{path}")
        try:
            with pytest.raises((SAOperationalError, sqlite3.OperationalError)) as excinfo:
                eng.connect()
        finally:
            eng.dispose()
        original = getattr(excinfo.value, "orig", excinfo.value)
        assert type(original) is sqlite3.OperationalError
        assert str(original) == LOCKED
        assert sum(sleeps) == pytest.approx(5.0)
        assert max(sleeps) <= 0.25
        assert _header(path) == (1, 1)

    def test_the_connection_is_not_leaked_when_the_switch_gives_up(
        self, tmp_path, blocker_for, sleeps
    ):
        """The connect hook closes the DBAPI connection when it raises; after
        the holder is gone the very same engine connects and switches."""
        path = _ours(tmp_path / "recover.db")
        blocker = blocker_for(path)
        eng = make_engine(f"sqlite:///{path}")
        try:
            with pytest.raises((SAOperationalError, sqlite3.OperationalError)):
                eng.connect()
            blocker.release()
            sleeps.clear()
            with eng.connect() as conn:
                assert conn.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"
            assert sleeps == []  # no holder, no sleep
        finally:
            eng.dispose()
        assert _header(path) == (2, 2)


def _timer_stretch(samples: int = 20, nominal: float = 0.01) -> float:
    """How many times longer than asked this host's short sleeps really take.

    SQLite's busy handler adds up the *nominal* delay of each step (1, 2, 5,
    10, 15, ... ms) and gives up when that sum reaches `busy_timeout`, however
    long each sleep really took. On a host whose timer is coarse (a small VM
    where `sleep(0.01)` takes ~65 ms) a wait therefore stretches by about this
    factor, and a real-clock bound on one statement stops meaning anything.
    """
    started = time.monotonic()
    for _ in range(samples):
        time.sleep(nominal)
    return (time.monotonic() - started) / (samples * nominal)


# Above this the 2x-budget bound below cannot be told apart from a stretched
# timer: (budget + 0.2) * stretch would reach twice the budget.
_MAX_TIMER_STRETCH = 1.6


# The shared/exclusive-holder test's bound is (budget + busy_timeout) * stretch
# + slack. The regression it guards against ("5 s" stretching to ~25x) is a
# wait of ~25 * budget; past this stretch the scaled bound approaches that
# and no longer separates the two.
_MAX_TIMER_STRETCH_HOLDERS = 4.0


class TestRealWaits:
    """Real clock, real sleeps, reduced constants (each test well under 2 s)."""

    def test_a_holder_committing_from_another_thread_lets_the_switch_through(self, tmp_path):
        path = _ours(tmp_path / "threaded.db")
        holding = threading.Event()
        problems = []

        def hold():
            try:
                conn = sqlite3.connect(str(path), isolation_level=None, timeout=5)
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute("INSERT INTO datasets DEFAULT VALUES")
                    holding.set()
                    time.sleep(0.5)
                    conn.execute("COMMIT")
                finally:
                    conn.close()
            except BaseException as exc:  # noqa: BLE001
                problems.append(exc)
                holding.set()

        thread = threading.Thread(target=hold, daemon=True)
        thread.start()
        assert holding.wait(5)
        started = time.monotonic()
        eng = make_engine(f"sqlite:///{path}")
        try:
            with eng.connect() as conn:
                assert conn.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"
                assert conn.exec_driver_sql("SELECT count(*) FROM datasets").scalar() == 1
        finally:
            eng.dispose()
            thread.join(10)
        elapsed = time.monotonic() - started
        assert not thread.is_alive() and problems == []
        assert 0.3 <= elapsed < store_db._WAL_SWITCH_BUDGET_S
        assert _header(path) == (2, 2)

    def test_foreign_shared_and_exclusive_holders_are_bounded_by_the_budget(
        self, tmp_path, monkeypatch
    ):
        """Attempts wait inside SQLite's busy handler as well as between
        sleeps; a budget counted only in sleeps let a SHARED holder stretch "5 s"
        to ~25x that. The deadline is wall clock and each attempt's
        `busy_timeout` is clipped to what is left.

        The upper bound is scaled by the host's measured sleep granularity
        (see `_timer_stretch`: SQLite counts nominal, not real, busy-handler
        sleep time), and the test is skipped when that is too coarse to tell
        a stretched timer from the ~25x regression."""
        stretch = _timer_stretch()
        if stretch > _MAX_TIMER_STRETCH_HOLDERS:
            pytest.skip(
                f"timer too coarse for a real-clock bound: sleep(0.01) takes "
                f"{stretch:.1f}x as long as asked, and SQLite's busy handler "
                f"counts nominal rather than real sleep time"
            )
        budget, busy_ms = 0.4, 400
        upper = (budget + busy_ms / 1000) * max(1.0, stretch) + 0.5
        assert upper < 25 * budget
        monkeypatch.setattr(store_db, "_WAL_SWITCH_BUDGET_S", budget)
        monkeypatch.setattr(store_db, "_BUSY_TIMEOUT_MS", busy_ms)

        # (a) a foreign reader mid-statement (its SHARED lock is held while
        # the cursor is referenced and not exhausted)
        path = _ours(tmp_path / "shared.db")
        seed = sqlite3.connect(str(path))
        seed.executemany("INSERT INTO datasets DEFAULT VALUES", [()] * 50)
        seed.commit()
        seed.close()
        reader = sqlite3.connect(str(path))
        reader_cursor = reader.execute("SELECT * FROM datasets")
        assert reader_cursor.fetchone() is not None
        writer = sqlite3.connect(str(path), timeout=5)
        try:
            started = time.monotonic()
            with pytest.raises(sqlite3.OperationalError) as excinfo:
                store_db._execute_wal_pragma(writer.cursor())
            shared_elapsed = time.monotonic() - started
            assert writer.execute("PRAGMA busy_timeout").fetchone()[0] == busy_ms
        finally:
            writer.close()
            reader_cursor.close()
            reader.close()
        assert store_db._is_busy_error(excinfo.value)
        assert budget - 0.05 <= shared_elapsed < upper, (shared_elapsed, stretch)
        assert _header(path) == (1, 1)

        # (b) a foreign BEGIN EXCLUSIVE: even acquiring SHARED waits
        path = _ours(tmp_path / "exclusive.db")
        holder = sqlite3.connect(str(path), isolation_level=None, timeout=0)
        holder.execute("BEGIN EXCLUSIVE")
        writer = sqlite3.connect(str(path), timeout=5)
        try:
            started = time.monotonic()
            with pytest.raises(sqlite3.OperationalError) as excinfo:
                store_db._execute_wal_pragma(writer.cursor())
            exclusive_elapsed = time.monotonic() - started
            assert writer.execute("PRAGMA busy_timeout").fetchone()[0] == busy_ms
        finally:
            writer.close()
            holder.execute("ROLLBACK")
            holder.close()
        assert store_db._is_busy_error(excinfo.value)
        assert budget - 0.05 <= exclusive_elapsed < upper, (exclusive_elapsed, stretch)
        assert _header(path) == (1, 1)


    def test_a_two_stage_wait_stays_within_the_budget_not_twice_it(self, tmp_path, monkeypatch):
        """One `journal_mode=WAL` statement can wait for SHARED (while a
        foreign writer's COMMIT holds PENDING) and then, with a FRESH busy
        count, for EXCLUSIVE (a foreign reader that never lets go): up to 2x
        its `busy_timeout`. Each attempt is therefore given only HALF the time
        left. Here the writer gives up its COMMIT after 0.45 s; with the full
        remaining second as `busy_timeout` the statement would take
        ~0.35 + 1.0 s, with half of it ~0.85 s, then the loop retries once
        more inside the budget.

        The upper bound is scaled by the host's measured sleep granularity,
        and the test is skipped when that is too coarse to distinguish "twice
        the budget" from a stretched timer (see `_timer_stretch`)."""
        stretch = _timer_stretch()
        if stretch > _MAX_TIMER_STRETCH:
            pytest.skip(
                f"timer too coarse for a real-clock bound: sleep(0.01) takes "
                f"{stretch:.1f}x as long as asked, and SQLite's busy handler "
                f"counts nominal rather than real sleep time"
            )
        budget = 1.0
        monkeypatch.setattr(store_db, "_WAL_SWITCH_BUDGET_S", budget)
        monkeypatch.setattr(store_db, "_BUSY_TIMEOUT_MS", 1000)
        path = _ours(tmp_path / "twostage.db")
        seed = sqlite3.connect(str(path))
        seed.executemany("INSERT INTO datasets DEFAULT VALUES", [()] * 50)
        seed.commit()
        seed.close()

        reader = sqlite3.connect(str(path), check_same_thread=False)
        reader_cursor = reader.execute("SELECT * FROM datasets")
        assert reader_cursor.fetchone() is not None  # SHARED, kept
        foreign = sqlite3.connect(
            str(path), isolation_level=None, timeout=0.45, check_same_thread=False
        )
        foreign.execute("BEGIN IMMEDIATE")
        foreign.execute("INSERT INTO datasets DEFAULT VALUES")
        outcome = []

        def commit():  # takes PENDING, waits for the reader, gives up after 0.45 s
            try:
                foreign.execute("COMMIT")
                outcome.append("committed")
            except sqlite3.OperationalError as exc:
                outcome.append(str(exc))
                foreign.execute("ROLLBACK")

        thread = threading.Thread(target=commit, daemon=True)
        thread.start()
        time.sleep(0.1)  # let it reach PENDING
        writer = sqlite3.connect(str(path), timeout=5)
        try:
            started = time.monotonic()
            with pytest.raises(sqlite3.OperationalError) as excinfo:
                store_db._execute_wal_pragma(writer.cursor())
            elapsed = time.monotonic() - started
            assert writer.execute("PRAGMA busy_timeout").fetchone()[0] == 1000
        finally:
            writer.close()
            thread.join(10)
            reader_cursor.close()
            reader.close()
            foreign.close()
        assert store_db._is_busy_error(excinfo.value)
        assert outcome and outcome[0] == LOCKED  # the foreign COMMIT did give up first
        upper = (budget + 0.2) * max(1.0, stretch)
        assert upper < 2 * budget  # guaranteed by the skip above; this is the point
        assert budget - 0.05 <= elapsed < upper, (elapsed, stretch)
        assert _header(path) == (1, 1)


# ---------------------------------------------------------------------------
# the schedule, without a database
# ---------------------------------------------------------------------------


class TestSchedule:
    def test_success_on_the_first_try_never_sleeps(self, sleeps):
        cursor = FakeCursor()
        assert store_db._execute_wal_pragma(cursor) == ("wal",)
        assert sleeps == []
        # the first attempt gets HALF the budget (one statement can wait for
        # SHARED and then, with a fresh busy count, for EXCLUSIVE), then the
        # connect-hook value is put back
        assert cursor.statements == [
            "PRAGMA busy_timeout=2500", "PRAGMA journal_mode=WAL", "PRAGMA busy_timeout=5000",
        ]
        assert cursor.fetches == 1

    def test_the_row_is_fetched_once_and_only_after_the_successful_execute(self, sleeps):
        cursor = FakeCursor(errors=[_locked(), _locked()], row=("delete",))
        assert store_db._execute_wal_pragma(cursor) == ("delete",)  # unchecked here
        assert len(cursor.wal) == 3
        assert cursor.fetches == 1

    def test_the_delay_starts_at_10ms_and_doubles(self, sleeps):
        store_db._execute_wal_pragma(FakeCursor(errors=[_locked()] * 5))
        assert sleeps == [0.01, 0.02, 0.04, 0.08, 0.16]

    def test_the_delay_is_capped_at_250ms(self, sleeps):
        store_db._execute_wal_pragma(FakeCursor(errors=[_locked()] * 9))
        assert sleeps == [0.01, 0.02, 0.04, 0.08, 0.16, 0.25, 0.25, 0.25, 0.25]

    def test_exhaustion_sums_to_the_budget_and_the_last_pause_is_clipped(self, sleeps):
        cursor = FakeCursor(errors=[_locked() for _ in range(1000)])
        with pytest.raises(sqlite3.OperationalError) as excinfo:
            store_db._execute_wal_pragma(cursor)
        assert str(excinfo.value) == LOCKED
        assert sum(sleeps) == pytest.approx(5.0)
        assert all(0 < s <= 0.25 for s in sleeps)
        # 0.01 + 0.02 + 0.04 + 0.08 + 0.16 = 0.31, then 0.25 each: 18 full
        # pauses reach 4.81 and the 19th is clipped to the 0.19 that remain.
        assert sleeps[:5] == [0.01, 0.02, 0.04, 0.08, 0.16]
        assert sleeps[5:-1] == [0.25] * (len(sleeps) - 6)
        assert sleeps[-1] == pytest.approx(5.0 - sum(sleeps[:-1]))
        assert sleeps[-1] == pytest.approx(0.19) and len(sleeps) == 5 + 18 + 1
        # one attempt before each sleep, plus the final one that re-raises
        assert len(cursor.wal) == len(sleeps) + 1

    def test_exhaustion_re_raises_the_last_error_object_itself(self, sleeps):
        errors = [sqlite3.OperationalError(f"database is locked #{i}") for i in range(500)]
        cursor = FakeCursor(errors=list(errors))
        with pytest.raises(sqlite3.OperationalError) as excinfo:
            store_db._execute_wal_pragma(cursor)
        # the error raised by the final attempt, not a wrapper or a copy
        assert excinfo.value is errors[len(sleeps)]
        assert excinfo.value.__cause__ is None

    @pytest.mark.parametrize(
        "message",
        [
            "database is locked",
            "database table is locked",
            "Database Is Locked",
            "database is busy",
            "SQLITE_BUSY",
        ],
    )
    def test_locked_and_busy_wordings_are_retried_when_the_error_has_no_code(self, sleeps, message):
        cursor = FakeCursor(errors=[sqlite3.OperationalError(message)])
        assert store_db._execute_wal_pragma(cursor) == ("wal",)
        assert sleeps == [0.01]


class TestWallClockBudget:
    """The deadline is the wall clock (`_wal_switch_clock`), advanced here by
    the fake sleep AND by slow fake attempts -- attempts that wait in SQLite's
    busy handler count against the budget."""

    def test_one_slow_attempt_that_spends_the_budget_ends_the_loop_without_a_sleep(self, sleeps):
        error = _locked()
        cursor = FakeCursor(errors=[error, _locked()], clock=sleeps.clock, attempt_cost=5.0)
        with pytest.raises(sqlite3.OperationalError) as excinfo:
            store_db._execute_wal_pragma(cursor)
        assert excinfo.value is error  # unchanged, and the only attempt
        assert len(cursor.wal) == 1
        assert sleeps == []

    def test_slow_attempts_stop_the_loop_at_the_deadline(self, sleeps):
        t0 = sleeps.clock.now
        errors = [_locked() for _ in range(100)]
        cursor = FakeCursor(errors=list(errors), clock=sleeps.clock, attempt_cost=1.2)
        with pytest.raises(sqlite3.OperationalError) as excinfo:
            store_db._execute_wal_pragma(cursor)
        # Attempts start at 0, 1.21, 2.43, 3.67, 4.95 (each costs 1.2 s, then
        # a sleep of 0.01/0.02/0.04/0.08 -- the last one clipped to the 0.13
        # left would be 0.13, but 0.08 < 0.13 so it is not).
        assert [round(t - t0, 2) for t in cursor.starts] == [0.0, 1.21, 2.43, 3.67, 4.95]
        assert sleeps == [0.01, 0.02, 0.04, 0.08]
        assert excinfo.value is errors[4]  # the last attempt's own error
        # nothing started after the deadline; the last attempt may overrun by
        # at most its own length (it is not interrupted)
        assert all(t - t0 < 5.0 for t in cursor.starts)

    def test_a_sleep_is_clipped_to_the_time_left_on_the_clock(self, sleeps):
        t0 = sleeps.clock.now
        # first attempt burns 4.9 s, leaving 0.1 s: the 10 ms sleep fits;
        # then the next attempt burns 0.085 s, leaving 5 ms: the 20 ms sleep
        # is clipped to 5 ms.
        class Slow(FakeCursor):
            costs = iter([4.9, 0.085, 10.0])

            def execute(self, sql, *args):
                if sql == "PRAGMA journal_mode=WAL":
                    self.attempt_cost = next(self.costs)
                return super().execute(sql, *args)

        cursor = Slow(errors=[_locked() for _ in range(10)], clock=sleeps.clock)
        with pytest.raises(sqlite3.OperationalError):
            store_db._execute_wal_pragma(cursor)
        assert sleeps[0] == pytest.approx(0.01)
        assert sleeps[1] == pytest.approx(0.005)
        assert all(s <= 0.25 for s in sleeps)
        assert sleeps.clock.now - t0 >= 5.0

    def test_the_clock_not_the_sum_of_sleeps_decides(self, sleeps):
        """The sleeps alone sum to far less than the budget here, yet the loop
        ends: the (fake) clock jumped during the attempts. A sum-of-delays
        budget would keep going for ~24 sleeps."""
        cursor = FakeCursor(
            errors=[_locked() for _ in range(1000)], clock=sleeps.clock, attempt_cost=2.0
        )
        with pytest.raises(sqlite3.OperationalError):
            store_db._execute_wal_pragma(cursor)
        assert sum(sleeps) < 0.5
        assert len(cursor.wal) == 3  # 0 -> 2.01 -> 4.03 -> over budget at 6.03

    def test_a_clock_that_does_not_move_never_ends_the_loop_early(self, sleeps, monkeypatch):
        """Defensive mirror: with a frozen clock the loop is bounded by the
        script of errors, not by anything else."""
        monkeypatch.setattr(store_db, "_wal_switch_clock", lambda: 1000.0)
        cursor = FakeCursor(errors=[_locked() for _ in range(7)])
        assert store_db._execute_wal_pragma(cursor) == ("wal",)
        assert len(sleeps) == 7


class TestBusyTimeoutPerAttempt:
    @staticmethod
    def _close(actual, expected):
        return len(actual) == len(expected) and all(abs(a - e) <= 1 for a, e in zip(actual, expected))

    def test_each_attempt_gets_half_the_remaining_milliseconds(self, sleeps):
        cursor = FakeCursor(errors=[_locked(), _locked(), _locked()], clock=sleeps.clock)
        store_db._execute_wal_pragma(cursor)
        # remaining 5000, 4990, 4970, 4930 ms (after 0, 10, 30, 70 ms of
        # sleeping), halved: one statement may wait for SHARED and then again
        # for EXCLUSIVE, so a full-remaining timeout could take twice as long
        assert self._close(cursor.pre, [2500, 2495, 2485, 2465]), cursor.pre
        assert cursor.post == [5000] * 4

    def test_a_slow_attempt_shortens_the_next_ones_timeout(self, sleeps):
        cursor = FakeCursor(
            errors=[_locked(), _locked()], clock=sleeps.clock, attempt_cost=1.5
        )
        store_db._execute_wal_pragma(cursor)
        # t = 0; 1.5 + 0.01 = 1.51; 1.51 + 1.5 + 0.02 = 3.03
        assert self._close(cursor.pre, [2500, 1745, 985]), cursor.pre

    def test_the_value_is_clamped_to_at_least_one_millisecond(self, sleeps):
        cursor = FakeCursor(errors=[_locked() for _ in range(10)], clock=sleeps.clock)
        # The first attempt leaves 0.4 ms: clamp(ceil(0.4 / 2), 1, ...) == 1.
        original = cursor.execute

        def execute(sql, *args):
            if sql == "PRAGMA journal_mode=WAL" and not cursor.wal:
                cursor.attempt_cost = 5.0 - 0.0004
            else:
                cursor.attempt_cost = 0.0
            return original(sql, *args)

        cursor.execute = execute
        with pytest.raises(sqlite3.OperationalError):
            store_db._execute_wal_pragma(cursor)
        assert cursor.pre[0] == 2500
        assert cursor.pre[1] == 1
        assert min(cursor.pre) == 1 and max(cursor.pre) <= 5000

    def test_the_value_is_clamped_to_the_busy_timeout_when_more_time_remains(
        self, sleeps, monkeypatch
    ):
        monkeypatch.setattr(store_db, "_WAL_SWITCH_BUDGET_S", 20.0)
        cursor = FakeCursor(errors=[_locked()] * 3, clock=sleeps.clock)
        store_db._execute_wal_pragma(cursor)
        assert cursor.pre == [5000, 5000, 5000, 5000]  # half of 20 s is still capped

    def test_the_value_is_half_the_remaining_time_below_the_cap(self, sleeps, monkeypatch):
        monkeypatch.setattr(store_db, "_WAL_SWITCH_BUDGET_S", 8.0)
        cursor = FakeCursor()
        store_db._execute_wal_pragma(cursor)
        assert cursor.pre == [4000]

    def test_the_value_rounds_up(self, sleeps):
        cursor = FakeCursor(errors=[_locked()], clock=sleeps.clock)
        # 4.9805 s remain at the second attempt after a 9.5 ms attempt + 10 ms
        # sleep: half is 2490.25 ms, which rounds UP
        cursor.attempt_cost = 0.0095
        store_db._execute_wal_pragma(cursor)
        assert cursor.pre[1] == 2491  # not 2490

    def test_5000_is_restored_after_success(self, sleeps):
        cursor = FakeCursor()
        store_db._execute_wal_pragma(cursor)
        assert cursor.statements[-1] == "PRAGMA busy_timeout=5000"
        assert cursor.post == [5000]

    def test_5000_is_restored_after_exhaustion(self, sleeps):
        cursor = FakeCursor(errors=[_locked() for _ in range(1000)], clock=sleeps.clock)
        with pytest.raises(sqlite3.OperationalError):
            store_db._execute_wal_pragma(cursor)
        assert cursor.statements[-1] == "PRAGMA busy_timeout=5000"
        assert cursor.post == [5000] * len(cursor.pre)

    def test_5000_is_restored_after_a_non_busy_error(self, sleeps):
        fatal = sqlite3.OperationalError("attempt to write a readonly database")
        cursor = FakeCursor(errors=[fatal])
        with pytest.raises(sqlite3.OperationalError) as excinfo:
            store_db._execute_wal_pragma(cursor)
        assert excinfo.value is fatal
        assert cursor.statements[-1] == "PRAGMA busy_timeout=5000"

    def test_5000_is_restored_after_a_non_sqlite_error_too(self, sleeps):
        cursor = FakeCursor(errors=[RuntimeError("boom")])
        with pytest.raises(RuntimeError):
            store_db._execute_wal_pragma(cursor)
        assert cursor.statements[-1] == "PRAGMA busy_timeout=5000"

    def test_the_restore_uses_the_constant(self, sleeps, monkeypatch):
        monkeypatch.setattr(store_db, "_BUSY_TIMEOUT_MS", 777)
        monkeypatch.setattr(store_db, "_WAL_SWITCH_BUDGET_S", 0.777)
        cursor = FakeCursor()
        store_db._execute_wal_pragma(cursor)
        # the attempt's own timeout is half the 0.777 s budget (rounded up);
        # the restore is the full constant
        assert cursor.pre == [389] and cursor.post == [777]

    def test_a_failing_restore_after_success_propagates(self, sleeps, db_logger, caplog):
        """The switch is already on disk, but returning would pool a
        connection stuck on a tiny `busy_timeout`; raising lets the connect
        hook close it. Nothing is logged: the error itself is the signal."""
        failure = sqlite3.OperationalError("disk I/O error")
        cursor = FakeCursor(restore_errors={0: failure})
        with caplog.at_level(logging.WARNING, logger=db_logger.name):
            with pytest.raises(sqlite3.OperationalError) as excinfo:
                store_db._execute_wal_pragma(cursor)
        assert excinfo.value is failure
        assert [r for r in caplog.records if r.levelno == logging.WARNING] == []

    def test_a_failing_restore_after_a_retried_success_propagates_too(self, sleeps):
        failure = RuntimeError("closed")
        cursor = FakeCursor(errors=[_locked()], restore_errors={1: failure})
        with pytest.raises(RuntimeError) as excinfo:
            store_db._execute_wal_pragma(cursor)
        assert excinfo.value is failure
        assert sleeps == [0.01]

    def test_the_connect_hook_closes_the_connection_when_the_success_restore_fails(
        self, tmp_path, monkeypatch
    ):
        """Real engine: the DBAPI connection whose restore failed is closed,
        not pooled (it would keep a shortened busy_timeout for good); the
        switch itself is on disk; the next connect is healthy."""
        path = _ours(tmp_path / "restorefail.db")
        real = store_db._restore_busy_timeout
        seen = []

        def failing(cursor, *, strict):
            if strict:
                seen.append(cursor.connection)
                raise sqlite3.OperationalError("disk I/O error")
            return real(cursor, strict=strict)

        monkeypatch.setattr(store_db, "_restore_busy_timeout", failing)
        eng = make_engine(f"sqlite:///{path}")
        try:
            with pytest.raises((SAOperationalError, sqlite3.OperationalError)) as excinfo:
                eng.connect()
            assert "disk I/O error" in str(excinfo.value)
            assert len(seen) == 1
            with pytest.raises(sqlite3.ProgrammingError):
                seen[0].execute("SELECT 1")  # closed by the hook
            assert eng.pool.checkedin() == 0 and eng.pool.checkedout() == 0
            assert _header(path) == (2, 2)  # the switch was already done

            monkeypatch.setattr(store_db, "_restore_busy_timeout", real)
            with eng.connect() as conn:
                assert conn.exec_driver_sql("PRAGMA busy_timeout").scalar() == 5000
                assert conn.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"
        finally:
            eng.dispose()

    def test_a_failing_restore_does_not_replace_the_error_in_flight(
        self, sleeps, db_logger, caplog
    ):
        fatal = sqlite3.OperationalError("attempt to write a readonly database")
        cursor = FakeCursor(
            errors=[fatal], restore_errors={0: sqlite3.OperationalError("disk I/O error")}
        )
        with caplog.at_level(logging.WARNING, logger=db_logger.name):
            with pytest.raises(sqlite3.OperationalError) as excinfo:
                store_db._execute_wal_pragma(cursor)
        assert excinfo.value is fatal
        assert any("could not restore busy_timeout" in r.getMessage() for r in caplog.records)

    def test_a_failing_restore_does_not_stop_the_busy_retries(self, sleeps, db_logger, caplog):
        cursor = FakeCursor(
            errors=[_locked(), _locked()],
            restore_errors={0: RuntimeError("closed"), 1: RuntimeError("closed")},
        )
        with caplog.at_level(logging.WARNING, logger=db_logger.name):
            assert store_db._execute_wal_pragma(cursor) == ("wal",)
        assert sleeps == [0.01, 0.02]
        assert len([r for r in caplog.records if "could not restore" in r.getMessage()]) == 2

    def test_a_failing_restore_after_exhaustion_still_raises_the_busy_error(
        self, sleeps, db_logger
    ):
        last = _locked()
        errors = [_locked() for _ in range(1000)]
        errors[-1] = last
        # restore fails on every attempt
        cursor = FakeCursor(errors=errors, restore_errors={i: RuntimeError("x") for i in range(100)})
        with pytest.raises(sqlite3.OperationalError) as excinfo:
            store_db._execute_wal_pragma(cursor)
        assert excinfo.value is errors[len(sleeps)]
        assert str(excinfo.value) == LOCKED


# ---------------------------------------------------------------------------
# classification
# ---------------------------------------------------------------------------


class TestIsBusyError:
    @pytest.mark.parametrize(
        "code",
        [
            sqlite3.SQLITE_BUSY,
            sqlite3.SQLITE_LOCKED,
            261,  # SQLITE_BUSY_RECOVERY
            517,  # SQLITE_BUSY_SNAPSHOT
            262,  # SQLITE_LOCKED_SHAREDCACHE
        ],
    )
    def test_busy_and_locked_codes_including_extended_ones(self, code):
        # The text deliberately says nothing about locks: the code decides.
        assert store_db._is_busy_error(_coded(code, "something unrelated"))

    @pytest.mark.parametrize(
        "code",
        [
            sqlite3.SQLITE_READONLY,
            sqlite3.SQLITE_IOERR,
            sqlite3.SQLITE_CORRUPT,
            sqlite3.SQLITE_CANTOPEN,
            attempt := 1,  # SQLITE_ERROR
        ],
    )
    def test_other_codes_are_not_busy_even_with_locked_in_the_text(self, code):
        assert not store_db._is_busy_error(_coded(code, "database is locked"))
        assert not store_db._is_busy_error(_coded(code, "database is busy"))

    def test_a_real_busy_error_carries_the_code(self, tmp_path, blocker_for):
        path = _ours(tmp_path / "code.db")
        blocker_for(path)
        conn = sqlite3.connect(str(path), timeout=0)
        try:
            with pytest.raises(sqlite3.OperationalError) as excinfo:
                conn.execute("PRAGMA journal_mode=WAL").fetchone()
        finally:
            conn.close()
        assert excinfo.value.sqlite_errorcode & 0xFF in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
        assert store_db._is_busy_error(excinfo.value)

    @pytest.mark.parametrize(
        "message, expected",
        [
            ("database is locked", True),
            ("database table is locked", True),
            ("database is busy", True),
            ("attempt to write a readonly database", False),
            ("disk I/O error", False),
        ],
    )
    def test_without_the_attribute_the_text_decides(self, message, expected):
        error = sqlite3.OperationalError(message)
        assert getattr(error, "sqlite_errorcode", None) is None
        assert store_db._is_busy_error(error) is expected

    @pytest.mark.parametrize("value", [None, "5", 5.0, b"5"])
    def test_a_missing_or_non_int_code_falls_back_to_the_text(self, value):
        locked = sqlite3.OperationalError(LOCKED)
        locked.sqlite_errorcode = value
        assert store_db._is_busy_error(locked)
        other = sqlite3.OperationalError("disk I/O error")
        other.sqlite_errorcode = value
        assert not store_db._is_busy_error(other)

    def test_only_operational_errors_qualify(self):
        for error in (
            sqlite3.DatabaseError(LOCKED), RuntimeError(LOCKED), OSError(LOCKED), KeyboardInterrupt(),
        ):
            assert not store_db._is_busy_error(error)
        coded = sqlite3.DatabaseError("x")
        coded.sqlite_errorcode = sqlite3.SQLITE_BUSY
        assert not store_db._is_busy_error(coded)

    def test_the_loop_follows_the_code_not_the_text(self, sleeps):
        """A READONLY error whose text says "locked" is not retried; a BUSY
        error whose text does not is."""
        readonly = _coded(sqlite3.SQLITE_READONLY, "database is locked")
        cursor = FakeCursor(errors=[readonly])
        with pytest.raises(sqlite3.OperationalError) as excinfo:
            store_db._execute_wal_pragma(cursor)
        assert excinfo.value is readonly and sleeps == []
        cursor = FakeCursor(errors=[_coded(sqlite3.SQLITE_BUSY, "no wording at all")])
        assert store_db._execute_wal_pragma(cursor) == ("wal",)
        assert sleeps == [0.01]


# ---------------------------------------------------------------------------
# what is NOT retried
# ---------------------------------------------------------------------------


class TestNotRetried:
    @pytest.mark.parametrize(
        "message",
        [
            "attempt to write a readonly database",
            "disk I/O error",
            "database disk image is malformed",
            "unable to open database file",
            "not authorized",
        ],
    )
    def test_a_non_lock_operational_error_raises_at_once(self, sleeps, message):
        error = sqlite3.OperationalError(message)
        cursor = FakeCursor(errors=[error])
        with pytest.raises(sqlite3.OperationalError) as excinfo:
            store_db._execute_wal_pragma(cursor)
        assert excinfo.value is error
        assert sleeps == []
        assert len(cursor.wal) == 1  # a single attempt
        assert cursor.fetches == 0

    @pytest.mark.parametrize(
        "error",
        [
            # A DatabaseError that is not an OperationalError, even with the
            # lock wording: only OperationalError is the retry signal.
            sqlite3.DatabaseError(LOCKED),
            sqlite3.ProgrammingError("Cannot operate on a closed database."),
            sqlite3.InterfaceError("bad"),
            RuntimeError(LOCKED),
            OSError("boom"),
            KeyboardInterrupt(),
        ],
        ids=lambda e: type(e).__name__,
    )
    def test_anything_that_is_not_an_operational_error_raises_at_once(self, sleeps, error):
        cursor = FakeCursor(errors=[error])
        with pytest.raises(type(error)) as excinfo:
            store_db._execute_wal_pragma(cursor)
        assert excinfo.value is error
        assert sleeps == []
        assert len(cursor.wal) == 1

    def test_a_non_lock_error_after_some_lock_errors_stops_the_retries(self, sleeps):
        fatal = sqlite3.OperationalError("attempt to write a readonly database")
        cursor = FakeCursor(errors=[_locked(), _locked(), fatal])
        with pytest.raises(sqlite3.OperationalError) as excinfo:
            store_db._execute_wal_pragma(cursor)
        assert excinfo.value is fatal
        assert sleeps == [0.01, 0.02]

    def test_a_readonly_file_is_refused_at_once_through_a_real_engine(
        self, tmp_path, sleeps
    ):
        """The real "attempt to write a readonly database" (a permission-denied,
        never-WAL file) is not a busy error: zero sleeps. Skipped where chmod
        cannot take write access away (root)."""
        import os

        path = _ours(tmp_path / "ro.db")
        path.chmod(0o444)
        if os.access(path, os.W_OK):
            pytest.skip("this user can write a 0444 file (root?)")
        try:
            eng = make_engine(f"sqlite:///{path}")
            try:
                with pytest.raises((SAOperationalError, sqlite3.OperationalError)) as excinfo:
                    eng.connect()
            finally:
                eng.dispose()
        finally:
            path.chmod(0o644)
        assert "readonly" in str(excinfo.value).lower()
        assert sleeps == []


# ---------------------------------------------------------------------------
# who reaches the retry path
# ---------------------------------------------------------------------------


class TestWhoReachesTheRetryPath:
    @pytest.fixture
    def tripwire(self, monkeypatch):
        calls = []

        def fail(cursor):
            calls.append(cursor)
            raise AssertionError("the WAL switch was reached")

        monkeypatch.setattr(store_db, "_execute_wal_pragma", fail)
        return calls

    def test_a_readonly_engine_never_reaches_it(self, tmp_path, blocker_for, sleeps, tripwire):
        path = _ours(tmp_path / "reader.db")
        before = path.read_bytes()
        blocker_for(path)  # even with a writer present
        eng = make_engine(f"sqlite:///{path}", readonly=True)
        try:
            with eng.connect() as conn:
                assert conn.exec_driver_sql("SELECT count(*) FROM datasets").scalar() == 0
        finally:
            eng.dispose()
        assert tripwire == [] and sleeps == []
        assert path.read_bytes() == before  # the reader did not touch the header
        assert _header(path) == (1, 1)

    def test_an_in_memory_engine_never_reaches_it(self, tripwire, sleeps):
        eng = make_engine("sqlite:///:memory:")
        try:
            with eng.connect() as conn:
                conn.exec_driver_sql("SELECT 1").fetchall()
        finally:
            eng.dispose()
        assert tripwire == [] and sleeps == []

    def test_a_foreign_file_never_reaches_it_even_against_a_holder(
        self, tmp_path, blocker_for, sleeps, tripwire
    ):
        """`may_switch_to_wal` decides first: a foreign file is not ours to
        switch, so there is nothing to retry (and nothing is rewritten)."""
        path = tmp_path / "foreign.db"
        conn = sqlite3.connect(str(path))
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()
        before = path.read_bytes()
        blocker_for(path, write="INSERT INTO orders DEFAULT VALUES")
        eng = make_engine(f"sqlite:///{path}")
        try:
            # A raw connection runs the connect hook but executes nothing, so
            # the writer's `BEGIN IMMEDIATE` (issued by the first statement on
            # a SQLAlchemy 2.0 connection, not by `connect()`) never runs
            # against the holder and cannot wait out `busy_timeout`.
            raw = eng.raw_connection()
            raw.close()
        finally:
            eng.dispose()
        assert tripwire == [] and sleeps == []
        assert path.read_bytes() == before

    def test_a_writer_engine_goes_through_the_retrying_function(self, tmp_path, monkeypatch):
        """`_switch_to_wal` -> `_execute_wal_pragma` (not a bare execute)."""
        path = _ours(tmp_path / "via.db")
        seen = []
        real = store_db._execute_wal_pragma

        def spy(cursor):
            seen.append(True)
            return real(cursor)

        monkeypatch.setattr(store_db, "_execute_wal_pragma", spy)
        eng = make_engine(f"sqlite:///{path}")
        try:
            with eng.connect():
                pass
        finally:
            eng.dispose()
        assert seen == [True]
        assert _header(path) == (2, 2)

    def test_the_wal_state_check_still_runs_after_a_retry(self, sleeps, caplog):
        """After the retried switch returns an UNCHANGED mode, `_switch_to_wal`
        still warns -- the retry did not replace the check."""
        import logging

        logger = logging.getLogger("zfsbackup.config.store.db")
        original = (logger.level, logger.disabled)
        logger.disabled = False
        try:
            cursor = FakeCursor(errors=[_locked()], row=("delete",))
            eng = make_engine("sqlite:///:memory:")  # only for its url in the message
            with caplog.at_level(logging.WARNING, logger="zfsbackup.config.store.db"):
                store_db._switch_to_wal(cursor, eng)
            eng.dispose()
        finally:
            logger.level, logger.disabled = original
        assert sleeps == [0.01]
        assert any("SQLite refused WAL journal mode" in r.getMessage() for r in caplog.records)


@pytest.fixture(autouse=True)
def _dispose_engines():
    yield
    dispose_all()

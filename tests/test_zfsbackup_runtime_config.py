"""Concurrency regression coverage for
`zfsbackup.runtime_config.load_runtime_config`'s torn-read protection.

Follow-up to item 8 Phase C, coordinator-reported defect 1: `load_config`
issues five-plus independent statements, and reader engines deliberately
begin no transaction of their own (`db.py`'s reader rule), so a
`zfsbackup-config import` (`import_yaml`) committing mid-read used to be
seen not as an error but as a *different, spliced* config -- half from
before the commit, half from after. Measured before the fix: two
datasets' whole retention policies swapped, silently, at a 13.99% rate
over a reader/writer soak with a ~1.6ms race window -- narrow, but a
worker hits it on every start and every crash restart, forever.

`load_runtime_config` now wraps `load_config` in one explicit
`BEGIN`/`COMMIT` pair, scoped to the single call (never to the session's
or connection's lifetime -- see that function's own docstring for why a
longer-lived snapshot would violate `db.py`'s separate "never adopt
BEGIN-on-connect for reader engines" rule instead). Since item 15.1 that
pair lives in the store's `read_config`, which `load_runtime_config` merely
delegates to (and which also runs the schema check inside the same
snapshot); the soak below therefore exercises the store's read path
through the daemon's wrapper, as the supervisor does.

**"No exception was raised" proves nothing here** -- every one of the
1052 torn reads in the pre-fix soak was exception-free. The only sound
test is a real concurrency soak with a per-dataset invariant check, which
is what this file is. It is deliberately marked `integration`/`slow` and
kept short (a bounded real-time window, not an unbounded run) -- the
measured rate is high enough that a sub-two-second window is already
"a few hundred" reads, comfortably enough to make the control positive
and the fix negative without turning this into a multi-second addition
to the default `pytest` run.
"""

import multiprocessing as mp
import time
from pathlib import Path

import pytest

from zfsbackup.config.store import (
    dispose_all,
    import_yaml,
    load_config,
    open_config_session,
    resolve_config_path,
)
from zfsbackup.runtime_config import load_runtime_config

# The soak classes are `integration`/`slow`; the wrapper-contract classes at
# the end are fast unit tests, so the markers are per class, not module-wide.

# Two datasets, swapped order between the two YAMLs -- the writer
# alternates between them so every commit either preserves or reverses
# which dataset comes "first" in the underlying tables. A torn read
# across such a commit is what hands a reader `tank/archive`'s name with
# `tank/scratch`'s retention rule, or vice versa.
_YAML_A = """
snapshot_prefix: autosnap
datasets:
  - name: tank/archive
    frequency: 1d
    retention:
      1d: 10y
  - name: tank/scratch
    frequency: 1h
    retention:
      1h: 2h
"""
_YAML_B = """
snapshot_prefix: autosnap
datasets:
  - name: tank/scratch
    frequency: 1h
    retention:
      1h: 2h
  - name: tank/archive
    frequency: 1d
    retention:
      1d: 10y
"""
_EXPECTED = {
    "tank/archive": [("1 day, 0:00:00", "3650 days, 0:00:00")],
    "tank/scratch": [("1:00:00", "2:00:00")],
}

# The soaks are COUNT-based, not time-based: they run until the writer has
# committed `_SOAK_MIN_COMMITS` imports AND the reader has completed
# `_SOAK_MIN_READS` reads (the control additionally until it has seen a torn
# read), bounded only by a generous `_SOAK_DEADLINE`. A fixed wall-clock window
# measures how fast the host is, not whether the code is right: a loaded CI box
# used to finish few enough commits to fail `commits > 20` with correct code.
# The deterministic interleaving test below proves the fix and the harness's
# sensitivity without any race at all; the soaks only add real concurrency.
_SOAK_MIN_COMMITS = 60
_SOAK_MIN_READS = 200
_SOAK_DEADLINE = 90
_JOIN_TIMEOUT = 20


def _seen(config) -> dict:
    return {
        ds.name: [(str(r.age), str(r.keep_for)) for r in ds.retention_rules]
        for ds in config.datasets
    }


def _writer_loop(db_path: str, stop, commit_count) -> None:
    """Runs in its own process. Alternates `import_yaml` between the two
    YAMLs until `stop` is set -- every iteration is a full, independent
    writer transaction (schema phase + data phase), exactly the shape a
    real `zfsbackup-config import` produces.

    `commit_count` (a `multiprocessing.Value('l', 0)`) is incremented
    after every SUCCESSFUL `import_yaml` call -- read back by `_run_soak`
    so a caller can tell "this writer imported N times" from "this writer
    died on its first iteration and the reader's ok/torn tally reflects a
    database nothing ever wrote to during the soak". Deliberately NOT a
    bare local counter reported only via `print`/the process's own return
    value: `Process.run()`'s return value is discarded by
    `multiprocessing`, and a counter this test cannot read back is not
    meaningfully different from not counting at all.
    """
    import logging
    logging.disable(logging.CRITICAL)
    a, b = Path(db_path).with_name("a.yaml"), Path(db_path).with_name("b.yaml")
    resolved = resolve_config_path(db_path)
    i = 0
    while not stop.is_set():
        import_yaml(a if i % 2 == 0 else b, resolved)
        i += 1
        with commit_count.get_lock():
            commit_count.value = i
    dispose_all()


def _load_runtime_config_no_snapshot(resolved):
    """Reimplements the PRE-FIX shape of `load_runtime_config`,
    independently of the real (fixed) function -- a bare read-only
    session with no surrounding `BEGIN`/`COMMIT`, so each of
    `load_config`'s five-plus statements is its own read transaction.

    This exists ONLY as the harness's own negative control (see
    `TestLoadRuntimeConfigTornReadControl` below) -- it must never be
    confused with, imported by, or substituted for the real
    `load_runtime_config`, and nothing outside this test file may use it.
    """
    with open_config_session(resolved, readonly=True) as session:
        return load_config(session)


def _reader_loop(db_path: str, stop, out, use_snapshot: bool, read_count, torn_count) -> None:
    """Runs in its own process. Loops the given reader shape until `stop`
    is set (set either by the caller's timer or by this function itself
    once its own window has run out), tallying `ok`/`torn`/`exc` against
    the `_EXPECTED` per-dataset invariant.
    """
    import logging
    logging.disable(logging.CRITICAL)
    resolved = resolve_config_path(db_path)
    ok = torn = exc = 0
    samples = []
    while not stop.is_set():
        try:
            if use_snapshot:
                config = load_runtime_config(resolved).config
            else:
                config = _load_runtime_config_no_snapshot(resolved)
        except Exception as e:  # pragma: no cover - diagnostic only
            exc += 1
            if len(samples) < 3:
                samples.append(f"EXC {type(e).__name__}: {e}")
            with read_count.get_lock():
                read_count.value = ok + torn + exc
            continue
        seen = _seen(config)
        if seen == _EXPECTED:
            ok += 1
        else:
            torn += 1
            if len(samples) < 3:
                samples.append(f"TORN {seen}")
        # shared tallies, so the parent can stop the soak on COUNTS
        with read_count.get_lock():
            read_count.value = ok + torn + exc
        with torn_count.get_lock():
            torn_count.value = torn
    dispose_all()
    out.put((ok, torn, exc, samples))


def _run_soak(
    tmp_path: Path,
    *,
    use_snapshot: bool,
    min_commits: int = _SOAK_MIN_COMMITS,
    min_reads: int = _SOAK_MIN_READS,
    until_torn: bool = False,
    deadline: float = _SOAK_DEADLINE,
):
    """Seeds a fresh config DB, runs one writer + one reader process
    concurrently until the writer has committed `min_commits` imports and the
    reader has completed `min_reads` reads (and, with `until_torn`, has seen at
    least one torn read), or `deadline` seconds have passed -- whichever is
    first; then returns
    `(ok, torn, exc, samples, writer_exitcode, commits)`.

    The stop condition is on counts, never on elapsed time, so a slow host
    just takes longer instead of failing; the deadline is only a hang guard,
    and the callers' count assertions turn a deadline stop into a clear
    failure. A writer or reader that dies early ends the wait at once.

    `writer_exitcode`/`commits` exist so a caller can tell "the writer
    ran healthily and actually wrote N times" from "the writer died on
    its first iteration and the reader's ok/torn tally reflects a
    database nothing was concurrently written to" -- without them,
    `ok > 20, torn == 0` is equally consistent with a working
    torn-read fix AND with a dead writer plus a reader reading one static
    (never mutated) config over and over, which is not a soak at all.
    """
    (tmp_path / "a.yaml").write_text(_YAML_A)
    (tmp_path / "b.yaml").write_text(_YAML_B)
    db_path = str(tmp_path / "config.db")
    resolved = resolve_config_path(db_path)
    import_yaml(tmp_path / "a.yaml", resolved)
    dispose_all()

    ctx = mp.get_context("spawn")
    stop = ctx.Event()
    out = ctx.Queue()
    commit_count = ctx.Value("l", 0)
    read_count = ctx.Value("l", 0)
    torn_count = ctx.Value("l", 0)
    writer = ctx.Process(target=_writer_loop, args=(db_path, stop, commit_count))
    reader = ctx.Process(
        target=_reader_loop, args=(db_path, stop, out, use_snapshot, read_count, torn_count)
    )
    writer.start()
    reader.start()
    try:
        give_up = time.monotonic() + deadline
        while time.monotonic() < give_up:
            if not (writer.is_alive() and reader.is_alive()):
                break  # one died: stop waiting, the caller's assertions report it
            if (
                commit_count.value >= min_commits
                and read_count.value >= min_reads
                and (torn_count.value > 0 or not until_torn)
            ):
                break
            time.sleep(0.05)
        stop.set()
        ok, torn, exc, samples = out.get(timeout=_JOIN_TIMEOUT)
    finally:
        stop.set()
        writer.join(_JOIN_TIMEOUT)
        reader.join(_JOIN_TIMEOUT)
        for proc in (writer, reader):
            if proc.is_alive():  # pragma: no cover - safety net, not the assertion
                proc.terminate()
                proc.join(5)
    return ok, torn, exc, samples, writer.exitcode, commit_count.value


class _CommitAfterDatasetsSelect:
    """Deterministic interleaving, no processes and no timing: a SQLAlchemy
    `after_cursor_execute` hook that, right after the reader's SELECT on the
    `datasets` table (the first statement that returns row identity; the
    retention rules are fetched per dataset AFTER it), runs `import_yaml` to
    completion on the calling thread. That is the exact interleaving the
    soaks only hit by chance: a writer commit landing between a read's
    statements.

    The hook fires once (the writer's own statements pass through it too, so
    it disarms itself before importing).
    """

    def __init__(self, yaml_path, resolved):
        self.yaml_path, self.resolved = yaml_path, resolved
        self.fired = 0
        self._armed = True

    def __enter__(self):
        from sqlalchemy import event
        from sqlalchemy.engine import Engine

        def after(conn, cursor, statement, parameters, context, executemany):
            if self._armed and "FROM datasets" in statement:
                self._armed = False
                self.fired += 1
                import_yaml(self.yaml_path, self.resolved)

        self._listener = after
        self._event, self._engine = event, Engine
        event.listen(Engine, "after_cursor_execute", after)
        return self

    def __exit__(self, *exc_info):
        self._event.remove(self._engine, "after_cursor_execute", self._listener)
        return False


@pytest.mark.integration
class TestTornReadDeterministicInterleaving:
    """The torn-read fix and the harness's sensitivity, proven WITHOUT a race.

    `_CommitAfterDatasetsSelect` commits an import (A -> B, which swaps which
    dataset owns which row id) between the reader's `datasets` SELECT and its
    per-dataset retention loads. The unscoped (pre-fix) read must splice the
    two states; `load_runtime_config` must return one consistent snapshot.
    Every run is the same, so the assertions are exact -- unlike the soaks
    below, which can only ever say "no tear seen in N tries".

    Mutation-checked (scratch copy): replacing `read_config`'s `BEGIN`/`COMMIT`
    with a bare `load_config` fails
    `test_load_runtime_config_is_one_snapshot_across_a_commit` (1 test).
    """

    @pytest.fixture
    def seeded(self, tmp_path):
        (tmp_path / "a.yaml").write_text(_YAML_A)
        (tmp_path / "b.yaml").write_text(_YAML_B)
        resolved = resolve_config_path(str(tmp_path / "config.db"))
        import_yaml(tmp_path / "a.yaml", resolved)
        dispose_all()
        yield tmp_path, resolved
        dispose_all()

    def test_the_unscoped_read_splices_two_states(self, seeded):
        tmp_path, resolved = seeded
        with _CommitAfterDatasetsSelect(tmp_path / "b.yaml", resolved) as hook:
            config = _load_runtime_config_no_snapshot(resolved)

        assert hook.fired == 1, "the interleaving never happened: the hook did not fire"
        assert _seen(config) != _EXPECTED, (
            "the control did not tear under a commit placed between its statements: "
            "the harness is insensitive"
        )

    def test_load_runtime_config_is_one_snapshot_across_a_commit(self, seeded):
        tmp_path, resolved = seeded
        before = load_runtime_config(resolved).generation
        with _CommitAfterDatasetsSelect(tmp_path / "b.yaml", resolved) as hook:
            snapshot = load_runtime_config(resolved)

        assert hook.fired == 1, "the interleaving never happened: the hook did not fire"
        assert _seen(snapshot.config) == _EXPECTED          # never a splice
        assert snapshot.generation == before                # ... and wholly the OLD state
        assert [d.name for d in snapshot.config.datasets] == ["tank/archive", "tank/scratch"]
        # the commit really landed: the next read is the NEW state, also consistent
        after = load_runtime_config(resolved)
        assert after.generation > before
        assert [d.name for d in after.config.datasets] == ["tank/scratch", "tank/archive"]
        assert _seen(after.config) == _EXPECTED


@pytest.mark.integration
@pytest.mark.slow
class TestLoadRuntimeConfigTornReadFix:
    """Mutation-checked: replacing `load_runtime_config`'s
    `BEGIN`/`load_config`/`COMMIT` sequence (now `access.read_config`'s) with a bare
    `return load_config(session)` (i.e. reverting the fix) fails 1 of the
    2 tests in this module -- this class's own
    `test_fixed_load_runtime_config_never_tears`, which caught 131 torn
    reads out of a few hundred in one 1.5s run under that mutation. The
    control test in `TestLoadRuntimeConfigTornReadControl` is unaffected
    by this mutation (it already calls the unscoped shape directly) and
    correctly keeps passing either way.
    """

    def test_fixed_load_runtime_config_never_tears(self, tmp_path):
        ok, torn, exc, samples, writer_exitcode, commits = _run_soak(
            tmp_path, use_snapshot=True
        )
        # Without these two, `ok > 20, torn == 0` is equally consistent
        # with a working fix AND with a writer that died on its first
        # `import_yaml` and a reader reading one never-mutated config
        # over and over -- which is not a concurrency soak at all, and
        # would pass this test for a completely unrelated reason. The
        # control test's writer is independent (its own soak, its own
        # process) and would not catch this: a dead writer here says
        # nothing about whether the control's writer ran either.
        assert writer_exitcode == 0, "writer process crashed during the soak"
        assert commits >= _SOAK_MIN_COMMITS, "writer performed too few imports to be a meaningful soak"
        assert exc == 0, samples
        assert torn == 0, samples
        # A weak sanity check that the soak actually ran enough reads to
        # be meaningful -- not the load-bearing assertion, but a torn==0
        # result over zero reads would prove nothing at all.
        assert ok >= _SOAK_MIN_READS, "soak produced too few reads to be a meaningful check"


@pytest.mark.integration
@pytest.mark.slow
class TestLoadRuntimeConfigTornReadControl:
    """The harness's own sensitivity control (mandated alongside the fix
    itself): the SAME writer, the SAME invariant check, the SAME real
    concurrency -- the only difference is the reader using the pre-fix,
    unscoped shape. If this does not detect torn reads, the fix test
    above proves nothing except that the harness never looks.
    """

    def test_unscoped_read_does_tear(self, tmp_path):
        ok, torn, exc, samples, writer_exitcode, commits = _run_soak(
            tmp_path, use_snapshot=False, until_torn=True
        )
        assert writer_exitcode == 0, "writer process crashed during the soak"
        assert commits >= _SOAK_MIN_COMMITS, "writer performed too few imports to be a meaningful soak"
        assert exc == 0, samples
        assert torn > 0, (
            "control did not detect any torn read -- the harness is "
            "insensitive, so the fixed-path test above proves nothing"
        )


# ---------------------------------------------------------------------------
# Item 15.2(a): the wrapper's contract -- what it returns, that it is the
# store's read path, and what it no longer has.
# ---------------------------------------------------------------------------

import sqlite3

from zfsbackup import runtime_config
from zfsbackup.config import BackupConfig
from zfsbackup.config.store import (
    EXPECTED_HEAD,
    ConfigSnapshot,
    SchemaOutOfDate,
    access as store_access,
    read_config,
)


@pytest.mark.unit
class TestLoadRuntimeConfigSnapshot:
    def test_returns_a_config_snapshot_with_generation_and_schema_rev(
        self, sqlite_config_db
    ):
        snapshot = load_runtime_config(sqlite_config_db)

        assert isinstance(snapshot, ConfigSnapshot)
        assert isinstance(snapshot.config, BackupConfig)
        assert [d.name for d in snapshot.config.datasets] == ["pool/data"]
        assert isinstance(snapshot.generation, int)
        assert snapshot.schema_rev == EXPECTED_HEAD

    def test_is_the_stores_read_config_and_nothing_more(self, sqlite_config_db):
        direct = read_config(sqlite_config_db)
        wrapped = load_runtime_config(sqlite_config_db)
        assert wrapped.generation == direct.generation
        assert wrapped.schema_rev == direct.schema_rev
        assert wrapped.config == direct.config

    def test_delegates_to_read_config_exactly_once_with_the_resolved_path(
        self, sqlite_config_db, mocker
    ):
        sentinel = ConfigSnapshot(BackupConfig(datasets=[]), 41, "rev")
        spy = mocker.patch.object(runtime_config, "read_config", return_value=sentinel)

        assert load_runtime_config(sqlite_config_db) is sentinel
        # `warn_never_prune=False`: the supervisor logs `validate_config`'s
        # never-prune warning once per accepted configuration instead.
        spy.assert_called_once_with(sqlite_config_db, warn_never_prune=False)

    def test_generation_tracks_the_database(self, sqlite_config_db, config_yaml_path):
        first = load_runtime_config(sqlite_config_db).generation
        import_yaml(config_yaml_path, sqlite_config_db)  # an import bumps it
        second = load_runtime_config(sqlite_config_db).generation
        assert second > first

    def test_verify_schema_keyword_is_gone(self, sqlite_config_db):
        """The schema check is no longer optional: it is inside the read
        snapshot, so there is nothing left to switch off."""
        with pytest.raises(TypeError):
            load_runtime_config(sqlite_config_db, verify_schema=False)

    def test_ex_config_has_no_producer_and_is_gone(self):
        assert not hasattr(runtime_config, "EX_CONFIG")

    def test_check_config_schema_and_describe_config_source_are_the_stores(self):
        from zfsbackup.config import store

        assert runtime_config.check_config_schema is store.check_config_schema
        assert runtime_config.describe_config_source is store.describe_config_source

    def test_a_stale_schema_is_refused_and_the_file_is_not_touched(self, tmp_path):
        db = tmp_path / "config.db"
        # A valid SQLite file with no tables and no recorded revision: the
        # shape of a database that was never migrated.
        conn = sqlite3.connect(str(db))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE _placeholder(x)")
        conn.execute("DROP TABLE _placeholder")
        conn.commit()
        conn.close()
        before = db.read_bytes()

        with pytest.raises(SchemaOutOfDate):
            load_runtime_config(resolve_config_path(str(db)))

        assert db.read_bytes() == before  # never migrated, never created

    def test_schema_in_snapshot_the_check_runs_inside_the_read_transaction(
        self, sqlite_config_db, mocker
    ):
        """The schema check and the row load are one read snapshot: the
        check runs on a connection that is already inside the transaction
        `read_config` opened, and strictly before the rows are loaded -- so
        a migration cannot land between them.

        Mutation-checked (scratch copy of the store): running
        `check_schema` before the `BEGIN` (the pre-15.1 shape) fails this
        test."""
        events = []
        real_check = store_access.check_schema
        real_load = store_access.load_config_snapshot

        def in_sqlite_txn(connection):
            # The DBAPI connection's own flag: SQLite has an open
            # transaction, i.e. the explicit `BEGIN` has been issued.
            # (SQLAlchemy's `Connection.in_transaction()` is also true
            # after an autobegin, which proves nothing about a snapshot.)
            return connection.connection.driver_connection.in_transaction

        def check(connection):
            events.append(("check", in_sqlite_txn(connection)))
            return real_check(connection)

        def load(session, **kw):
            events.append(("load", in_sqlite_txn(session.connection())))
            return real_load(session, **kw)

        mocker.patch.object(store_access, "check_schema", side_effect=check)
        mocker.patch.object(store_access, "load_config_snapshot", side_effect=load)

        snapshot = load_runtime_config(sqlite_config_db)

        assert events == [("check", True), ("load", True)]
        assert snapshot.schema_rev == EXPECTED_HEAD

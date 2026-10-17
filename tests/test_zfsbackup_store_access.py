"""Item 15.1 / the store half of 15.4: `read_config`, `write_config`, the
generation compare-and-swap, `ConfigConflict` / `ConfigBusy`, the writer lock
(`paths.writer_lock`), and the store's shared error wording.

Session-level CAS and `load_config_snapshot` are in `test_zfsbackup_store.py`
(`TestSaveConfigGenerationCas`, `TestLoadConfigSnapshot`); the effect of the
writer lock on `import_yaml`'s cleanup is in `test_zfsbackup_importer_cleanup.py`.

Hermetic: every database is a real file under `tmp_path`, always opened through
`store.db` (so `foreign_keys=ON`, WAL and `BEGIN IMMEDIATE` are the project's).
Tests that need a second *process* (a flock holder, a racing import) start a
short-lived `python -c` child and always kill it in a `finally`; none uses
`mock_subprocess`.

Mutation counts for this file (scratch copy of the tree, one revert at a time)
are in the hand-off report for item 15.1.
"""

import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy.exc import DatabaseError, OperationalError

from zfsbackup.config import BackupConfig, DatasetConfig, Duration, RetentionRule
from zfsbackup.config.store import (
    CONFIG_PATH_ENV,
    EXPECTED_HEAD,
    ConfigBusy,
    ConfigConflict,
    ConfigDbNotADatabase,
    ConfigDbNotFound,
    ConfigDbPermissionError,
    ConfigPathError,
    ConfigSnapshot,
    SchemaError,
    SchemaOutOfDate,
    SchemaVersionMismatch,
    WriteResult,
    check_config_schema,
    config_error_message,
    describe_config_source,
    dispose_all,
    import_yaml,
    read_config,
    resolve_config_path,
    validate_config,
    write_config,
    writer_lock,
    writer_lock_path,
)
from zfsbackup.config.store import access as store_access
from zfsbackup.config.store import paths as store_paths
from zfsbackup.config.store.paths import WRITER_LOCK_SUFFIX

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE_YAML = ROOT / "zfsbackup" / "config.example.yaml"
TEST_YAML = ROOT / "zfsbackup" / "config.test.yaml"

IS_ROOT = hasattr(os, "geteuid") and os.geteuid() == 0


def cfg(name="tank/x", *, prefix="auto") -> BackupConfig:
    """The smallest storable config: one dataset, one retention rule."""
    return BackupConfig(
        snapshot_prefix=prefix,
        datasets=[
            DatasetConfig(
                name=name,
                frequency=Duration("1h"),
                retention_rules=[RetentionRule(age=Duration("1d"), keep_for=Duration("30d"))],
            )
        ],
    )


def write_yaml(path: Path, dataset: str) -> Path:
    path.write_text(f"datasets:\n  - name: {dataset}\n    frequency: 1h\n")
    return path


@pytest.fixture
def store_dir(tmp_path):
    directory = tmp_path / "store"
    directory.mkdir()
    return directory


@pytest.fixture
def db_path(store_dir):
    return store_dir / "config.db"


@pytest.fixture
def resolved(db_path):
    return resolve_config_path(str(db_path))


@pytest.fixture
def seeded(db_path, resolved):
    """A config database holding `config.example.yaml`, at generation 0."""
    import_yaml(EXAMPLE_YAML, resolved)
    dispose_all()
    return resolved


def listing(directory: Path):
    return sorted(p.name for p in directory.iterdir())


def stored_names(resolved):
    snap = read_config(resolved)
    dispose_all()
    return [d.name for d in snap.config.datasets]


# ---------------------------------------------------------------------------
# Subprocess helpers
# ---------------------------------------------------------------------------

HOLDER = textwrap.dedent(
    """
    import sys
    from pathlib import Path
    from zfsbackup.config.store.paths import resolve_config_path, writer_lock
    with writer_lock(resolve_config_path(sys.argv[1])):
        Path(sys.argv[2]).write_text("held")
        sys.stdin.read()          # held until the parent closes the pipe / kills us
    """
)

IMPORTER = textwrap.dedent(
    """
    import logging, sys, time
    from pathlib import Path
    logging.disable(logging.WARNING)
    from zfsbackup.config.store.importer import import_yaml
    from zfsbackup.config.store.paths import resolve_config_path
    yaml_path, db, gate = sys.argv[1:4]
    deadline = time.time() + 30
    while not Path(gate).exists() and time.time() < deadline:
        time.sleep(0.005)
    result = import_yaml(yaml_path, resolve_config_path(db))
    print("created=%s" % result.created)
    """
)


def _child_env():
    return dict(os.environ, PYTHONPATH=str(ROOT))


class LockHolder:
    """A child process that holds `<db>.writer.lock` until `release()`/`kill()`."""

    def __init__(self, db: Path, ready: Path):
        self.proc = subprocess.Popen(
            [sys.executable, "-c", HOLDER, str(db), str(ready)],
            cwd=ROOT, env=_child_env(), stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
        )
        deadline = time.time() + 30
        while not ready.exists():
            if self.proc.poll() is not None or time.time() > deadline:
                err = self.proc.stderr.read() if self.proc.stderr else ""
                self.close()
                pytest.fail(f"lock holder never became ready:\n{err}")
            time.sleep(0.02)

    def release(self):
        self.proc.stdin.close()
        self.proc.wait(timeout=30)

    def kill(self):
        self.proc.kill()
        self.proc.wait(timeout=30)

    def close(self):
        if self.proc.poll() is None:
            self.proc.kill()
        self.proc.wait(timeout=30)
        for stream in (self.proc.stdin, self.proc.stderr):
            if stream and not stream.closed:
                try:
                    stream.close()
                except OSError:
                    pass


@pytest.fixture
def lock_holder(db_path, tmp_path):
    holders = []

    def start():
        holder = LockHolder(db_path, tmp_path / f"ready{len(holders)}")
        holders.append(holder)
        return holder

    yield start
    for holder in holders:
        holder.close()


# ---------------------------------------------------------------------------
# read_config
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestReadConfig:
    @pytest.mark.parametrize("yaml_path", [EXAMPLE_YAML, TEST_YAML], ids=lambda p: p.name)
    def test_equals_from_file_for_every_repo_yaml(self, db_path, resolved, yaml_path):
        """The mapper-equivalence contract through the public read path: what
        the daemon/CLI read back is exactly what `from_file` would have given."""
        import_yaml(yaml_path, resolved)
        dispose_all()
        snap = read_config(resolved)
        assert isinstance(snap, ConfigSnapshot)
        assert snap.config == BackupConfig.from_file(yaml_path)
        assert snap.generation == 0
        assert snap.schema_rev == EXPECTED_HEAD

    def test_generation_follows_writes(self, seeded):
        assert read_config(seeded).generation == 0
        write_config(seeded, cfg(), expected_generation=0)
        dispose_all()
        snap = read_config(seeded)
        assert snap.generation == 1
        assert snap.config == cfg()

    def test_it_creates_nothing_for_a_missing_database(self, tmp_path):
        missing_dir = tmp_path / "nodir"
        resolved = resolve_config_path(str(missing_dir / "config.db"))
        before = listing(tmp_path)
        with pytest.raises(ConfigDbNotFound):
            read_config(resolved)
        assert listing(tmp_path) == before
        assert not missing_dir.exists()

    def test_it_creates_nothing_beside_an_existing_directory(self, store_dir, resolved):
        with pytest.raises(ConfigDbNotFound):
            read_config(resolved)
        assert listing(store_dir) == []  # no database, no sidecar, no lock file

    def test_a_zero_byte_file_is_refused_and_left_alone(self, db_path, resolved):
        db_path.write_bytes(b"")
        with pytest.raises(ConfigDbNotADatabase):
            read_config(resolved)
        assert db_path.read_bytes() == b""
        assert listing(db_path.parent) == ["config.db"]

    def test_it_takes_no_writer_lock_and_leaves_the_database_bytes_alone(self, seeded, db_path):
        (db_path.parent / (db_path.name + WRITER_LOCK_SUFFIX)).unlink()  # the import's
        dispose_all()
        before = db_path.read_bytes()
        read_config(seeded)
        dispose_all()
        assert db_path.read_bytes() == before
        assert listing(db_path.parent) == ["config.db"]  # no lock file, no sidecar

    def test_it_works_while_a_writer_holds_the_writer_lock(self, seeded, lock_holder):
        """Readers (the daemon, `export`) never wait for the CLI writer lock."""
        lock_holder()
        started = time.monotonic()
        assert read_config(seeded).generation == 0
        assert time.monotonic() - started < 3

    def test_an_unknown_schema_revision_is_refused_in_the_same_call(self, seeded, db_path):
        dispose_all()
        conn = sqlite3.connect(str(db_path))
        conn.execute("UPDATE alembic_version SET version_num = 'ffffffffffff'")
        conn.commit()
        conn.close()
        with pytest.raises(SchemaVersionMismatch):
            read_config(seeded)

    def test_check_config_schema_returns_the_head(self, seeded):
        assert check_config_schema(seeded) == EXPECTED_HEAD

    def test_the_schema_is_checked_inside_the_explicit_transaction(
        self, seeded, monkeypatch
    ):
        """The point of the helper: `check_schema` and the row reads are one
        snapshot. Drop the explicit `BEGIN` and the check runs outside any
        transaction (pysqlite emits no BEGIN for a bare SELECT)."""
        seen = {}
        real = store_access.check_schema

        def spy(connection):
            seen["in_transaction"] = connection.connection.dbapi_connection.in_transaction
            return real(connection)

        monkeypatch.setattr(store_access, "check_schema", spy)
        read_config(seeded)
        assert seen == {"in_transaction": True}

    def test_an_import_landing_mid_read_does_not_tear_the_snapshot(
        self, seeded, tmp_path, monkeypatch
    ):
        """Measured in the item-15.1 review: without the `BEGIN`, a concurrent
        import is read as a config assembled from two commits. Here an import
        by ANOTHER PROCESS commits between the schema check and the row reads;
        the snapshot must be the OLD config, with the OLD generation."""
        other = write_yaml(tmp_path / "other.yaml", "tank/other")
        real = store_access.check_schema
        ran = {}

        def check_then_race(connection):
            rev = real(connection)  # the snapshot starts at this first read
            proc = subprocess.run(
                [sys.executable, "-c", IMPORTER, str(other), str(seeded.path), str(other)],
                cwd=ROOT, env=_child_env(), capture_output=True, text=True, timeout=120,
            )
            ran["proc"] = proc
            return rev

        monkeypatch.setattr(store_access, "check_schema", check_then_race)
        snap = read_config(seeded)
        monkeypatch.undo()

        assert ran["proc"].returncode == 0, ran["proc"].stderr
        assert "created=False" in ran["proc"].stdout
        assert snap.config == BackupConfig.from_file(EXAMPLE_YAML)
        assert snap.generation == 0
        # ...and the import really did land: the next read sees it.
        dispose_all()
        later = read_config(seeded)
        assert [d.name for d in later.config.datasets] == ["tank/other"]
        assert later.generation == 1

    def test_a_failed_read_rolls_back_and_the_next_read_works(self, seeded, monkeypatch):
        def boom(connection):
            raise SchemaOutOfDate("pretend")

        monkeypatch.setattr(store_access, "check_schema", boom)
        with pytest.raises(SchemaOutOfDate):
            read_config(seeded)
        monkeypatch.undo()
        assert read_config(seeded).generation == 0


# ---------------------------------------------------------------------------
# write_config: compare-and-swap
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestWriteConfigGenerationCas:
    def test_the_current_generation_succeeds_and_returns_plus_one(self, seeded):
        snap = read_config(seeded)
        result = write_config(seeded, cfg("tank/new"), expected_generation=snap.generation)
        assert isinstance(result, WriteResult)
        assert result.generation == snap.generation + 1
        dispose_all()
        after = read_config(seeded)
        assert after.generation == result.generation
        assert after.config == cfg("tank/new")

    def test_the_returned_generation_chains_without_a_re_read(self, seeded):
        first = write_config(seeded, cfg("tank/a"), expected_generation=0)
        second = write_config(seeded, cfg("tank/b"), expected_generation=first.generation)
        assert (first.generation, second.generation) == (1, 2)
        assert stored_names(seeded) == ["tank/b"]

    def test_an_import_between_read_and_write_is_a_conflict(self, seeded, tmp_path):
        snap = read_config(seeded)
        dispose_all()
        import_yaml(write_yaml(tmp_path / "x.yaml", "tank/imported"), seeded)
        dispose_all()

        with pytest.raises(ConfigConflict) as excinfo:
            write_config(seeded, cfg("tank/mine"), expected_generation=snap.generation)
        assert excinfo.value.expected == snap.generation == 0
        assert excinfo.value.actual == 1
        # The store is exactly what the import left: nothing of ours, and the
        # generation did not move.
        after = read_config(seeded)
        assert [d.name for d in after.config.datasets] == ["tank/imported"]
        assert after.generation == 1

    def test_a_write_between_read_and_write_is_a_conflict(self, seeded):
        snap = read_config(seeded)
        write_config(seeded, cfg("tank/theirs"), expected_generation=snap.generation)
        with pytest.raises(ConfigConflict) as excinfo:
            write_config(seeded, cfg("tank/mine"), expected_generation=snap.generation)
        assert (excinfo.value.expected, excinfo.value.actual) == (0, 1)
        assert stored_names(seeded) == ["tank/theirs"]

    def test_a_conflict_changes_no_byte_of_the_stored_configuration(self, seeded):
        before = read_config(seeded)
        dispose_all()
        with pytest.raises(ConfigConflict):
            write_config(seeded, cfg("tank/mine"), expected_generation=41)
        after = read_config(seeded)
        assert after.config == before.config
        assert after.generation == before.generation

    def test_none_overwrites_whatever_is_there(self, seeded):
        write_config(seeded, cfg("tank/a"), expected_generation=0)
        result = write_config(seeded, cfg("tank/z"), expected_generation=None)
        assert result.generation == 2
        assert stored_names(seeded) == ["tank/z"]

    def test_expected_generation_is_keyword_only_and_required(self, seeded):
        with pytest.raises(TypeError):
            write_config(seeded, cfg())  # type: ignore[call-arg]
        with pytest.raises(TypeError):
            write_config(seeded, cfg(), 0)  # type: ignore[misc]

    def test_no_configuration_at_all_reports_actual_none(self, db_path, resolved):
        """A schema-only database (no `global_settings` row): there is no
        generation to match, so any expectation conflicts with `actual=None`;
        `None` initialises it at generation 0."""
        from zfsbackup.config.store import ensure_schema, open_config_connection

        # `open_config_connection` refuses a zero-byte file: start from one
        # SQLite has written its header to.
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.commit()
        conn.close()
        os.chmod(db_path, 0o660)
        with open_config_connection(resolved, readonly=False, foreign_keys=False) as connection:
            trans = connection.begin()
            ensure_schema(connection)
            trans.commit()
        dispose_all()

        with pytest.raises(ConfigConflict) as excinfo:
            write_config(resolved, cfg(), expected_generation=0)
        assert excinfo.value.actual is None
        assert excinfo.value.expected == 0

        result = write_config(resolved, cfg(), expected_generation=None)
        assert result.generation == 0
        assert read_config(resolved).config == cfg()

    def test_two_racing_writers_cannot_both_win(self, seeded):
        """Both start from generation 0. Exactly one succeeds; the other gets
        `ConfigConflict`; the generation advanced exactly once."""
        outcomes = []
        barrier = threading.Barrier(2)

        def writer(name):
            barrier.wait()
            try:
                write_config(seeded, cfg(name), expected_generation=0)
                outcomes.append(("ok", name))
            except ConfigConflict as exc:
                outcomes.append(("conflict", exc.actual))
            except BaseException as exc:  # pragma: no cover - reported below
                outcomes.append(("error", repr(exc)))

        threads = [threading.Thread(target=writer, args=(n,)) for n in ("tank/p", "tank/q")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)

        kinds = sorted(kind for kind, _ in outcomes)
        assert kinds == ["conflict", "ok"], outcomes
        winner = next(value for kind, value in outcomes if kind == "ok")
        assert next(v for k, v in outcomes if k == "conflict") == 1
        dispose_all()
        snap = read_config(seeded)
        assert snap.generation == 1
        assert [d.name for d in snap.config.datasets] == [winner]


# ---------------------------------------------------------------------------
# write_config: preflight, validation, schema re-check, warnings
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestWriteConfigPreconditions:
    def test_a_missing_database_is_refused_and_creates_nothing(self, tmp_path):
        resolved = resolve_config_path(str(tmp_path / "nodir" / "config.db"))
        with pytest.raises(ConfigDbNotFound):
            write_config(resolved, cfg(), expected_generation=None)
        assert listing(tmp_path) == []  # no directory, no database, no lock file

    def test_a_missing_database_in_an_existing_directory_leaves_no_lock_file(
        self, store_dir, resolved
    ):
        with pytest.raises(ConfigDbNotFound):
            write_config(resolved, cfg(), expected_generation=None)
        assert listing(store_dir) == []

    def test_an_invalid_config_is_a_value_error_before_anything_is_opened(self, seeded, db_path):
        import_yaml_lock = db_path.name + WRITER_LOCK_SUFFIX
        (db_path.parent / import_yaml_lock).unlink()  # the seeding import's
        dispose_all()
        before = db_path.read_bytes()
        with pytest.raises(ValueError):
            write_config(seeded, BackupConfig(datasets=[]), expected_generation=0)
        dispose_all()
        assert db_path.read_bytes() == before
        # Validation precedes the writer lock: not even a new lock file.
        assert import_yaml_lock not in listing(db_path.parent)

    def test_an_invalid_config_wins_over_a_stale_generation(self, seeded):
        with pytest.raises(ValueError):
            write_config(seeded, BackupConfig(datasets=[]), expected_generation=99)

    @staticmethod
    def _tamper(db_path):
        conn = sqlite3.connect(str(db_path))
        conn.execute("UPDATE alembic_version SET version_num = 'ffffffffffff'")
        conn.commit()
        conn.close()

    @staticmethod
    def _rows(db_path):
        conn = sqlite3.connect(str(db_path))
        try:
            names = [r[0] for r in conn.execute("SELECT name FROM datasets ORDER BY name")]
            generation = conn.execute("SELECT generation FROM global_settings").fetchone()[0]
        finally:
            conn.close()
        return names, generation

    def test_the_schema_is_rechecked_inside_the_write_transaction(
        self, seeded, db_path, monkeypatch
    ):
        """The read that produced `config` may be minutes old; a newer zfsbackup
        can have migrated the file since. `write_config` checks the schema up
        front (so a foreign database gets no lock file) AND again under the
        lock, inside the `BEGIN IMMEDIATE` transaction -- the authority, since
        the file can change in between. Tamper `alembic_version` exactly in
        that window: the pre-check has passed, only the in-transaction check
        can refuse."""
        snap = read_config(seeded)
        dispose_all()
        before = self._rows(db_path)
        real_lock = store_access.writer_lock

        @contextmanager
        def tamper_once_locked(resolved, **kwargs):
            with real_lock(resolved, **kwargs) as lock:
                self._tamper(db_path)  # after the pre-check, before the write
                yield lock

        monkeypatch.setattr(store_access, "writer_lock", tamper_once_locked)
        with pytest.raises(SchemaVersionMismatch):
            write_config(seeded, cfg("tank/mine"), expected_generation=snap.generation)
        monkeypatch.undo()

        names, generation = self._rows(db_path)
        assert (names, generation) == before  # nothing written
        assert "tank/mine" not in names

    def test_a_wrong_schema_is_refused_before_the_lock_file_exists(self, seeded, db_path):
        """The pre-check: a database that is not ours gets no writer lock
        beside it (item 10.4's "never touch a foreign database")."""
        (db_path.parent / (db_path.name + WRITER_LOCK_SUFFIX)).unlink()  # the seeding import's
        dispose_all()
        self._tamper(db_path)
        before = db_path.read_bytes()
        with pytest.raises(SchemaVersionMismatch):
            write_config(seeded, cfg(), expected_generation=0)
        dispose_all()
        assert db_path.read_bytes() == before
        assert (db_path.name + WRITER_LOCK_SUFFIX) not in listing(db_path.parent)

    def test_the_schema_check_runs_before_the_compare(self, seeded, db_path):
        """A mismatched schema is the more fundamental refusal: it is reported
        even when the generation is also stale."""
        dispose_all()
        self._tamper(db_path)
        with pytest.raises(SchemaVersionMismatch):
            write_config(seeded, cfg(), expected_generation=12345)

    def test_warnings_are_the_validate_config_findings(self, seeded):
        risky = cfg(prefix="a_b")  # '_' in the prefix: snapshots would never be managed
        expected = tuple(validate_config(risky))
        assert expected
        result = write_config(seeded, risky, expected_generation=0)
        assert result.warnings == expected
        assert any("contains '_'" in w for w in result.warnings)

    def test_a_clean_config_has_no_warnings(self, seeded):
        result = write_config(seeded, cfg(), expected_generation=0)
        assert result.warnings == ()

    def test_never_prune_is_warned_about(self, seeded):
        never = BackupConfig(
            datasets=[DatasetConfig(name="tank/n", frequency=Duration("1h"), retention_rules=[])]
        )
        result = write_config(seeded, never, expected_generation=0)
        assert result.warnings and any("never" in w and "pruned" in w for w in result.warnings)
        assert result.warnings == tuple(validate_config(never))

    def test_write_result_is_frozen(self, seeded):
        result = write_config(seeded, cfg(), expected_generation=0)
        with pytest.raises(Exception):
            result.generation = 5  # type: ignore[misc]


# ---------------------------------------------------------------------------
# write_config: ConfigBusy
# ---------------------------------------------------------------------------


@pytest.mark.integration
def _cached_engines():
    from zfsbackup.config.store import db as store_db

    return dict(store_db._ENGINES)


class TestEngineCacheAfterAccess:
    """Mutation counts (15.5, scratch copy, tests failing in this file):
    `write_config`'s `dispose_all()` removed 8; `read_config` disposing 3.

    `write_config` disposes every engine it opened, on every exit (a retry
    loop must never carry a connection across `$EDITOR` or a fork);
    `read_config` deliberately does NOT (its caller owns the human wait and
    may read several times back to back)."""

    @pytest.fixture(autouse=True)
    def _start_clean(self):
        dispose_all()
        yield
        dispose_all()

    def test_read_config_leaves_its_reader_engine_cached(self, seeded):
        assert _cached_engines() == {}
        read_config(seeded)
        assert len(_cached_engines()) >= 1, "read_config is documented to leave the engine cached"

    def test_two_back_to_back_reads_share_the_cached_engine(self, seeded):
        read_config(seeded)
        first = _cached_engines()
        read_config(seeded)
        assert _cached_engines().keys() == first.keys()
        assert list(_cached_engines().values()) == list(first.values())

    def test_write_config_success_leaves_nothing_cached(self, seeded):
        read_config(seeded)  # something is already pooled
        assert _cached_engines()
        write_config(seeded, cfg("tank/new"), expected_generation=0)
        assert _cached_engines() == {}

    def test_write_config_disposes_even_the_engines_it_did_not_open(self, seeded):
        read_config(seeded)
        before = _cached_engines()
        assert before
        write_config(seeded, cfg("tank/new"), expected_generation=0)
        assert _cached_engines() == {}

    def test_write_config_conflict_leaves_nothing_cached(self, seeded):
        with pytest.raises(ConfigConflict):
            write_config(seeded, cfg("tank/mine"), expected_generation=99)
        assert _cached_engines() == {}

    def test_write_config_busy_from_the_writer_lock_leaves_nothing_cached(
        self, seeded, lock_holder
    ):
        lock_holder()
        with pytest.raises(ConfigBusy):
            write_config(seeded, cfg("tank/mine"), expected_generation=0, lock_timeout=0.2)
        assert _cached_engines() == {}

    def test_write_config_busy_from_sqlite_leaves_nothing_cached(self, seeded, monkeypatch):
        def locked(*_a, **_k):
            raise OperationalError("BEGIN IMMEDIATE", {}, Exception("database is locked"))

        monkeypatch.setattr(store_access, "save_config", locked)
        with pytest.raises(ConfigBusy):
            write_config(seeded, cfg(), expected_generation=0)
        assert _cached_engines() == {}

    def test_write_config_schema_error_leaves_nothing_cached(self, seeded, db_path):
        con = sqlite3.connect(str(db_path))
        con.execute("UPDATE alembic_version SET version_num = 'ffffffffffff'")
        con.commit()
        con.close()
        with pytest.raises(SchemaError):
            write_config(seeded, cfg(), expected_generation=0)
        assert _cached_engines() == {}

    def test_write_config_value_error_leaves_nothing_cached(self, seeded):
        read_config(seeded)
        with pytest.raises(ValueError):
            write_config(seeded, BackupConfig(datasets=[]), expected_generation=0)
        assert _cached_engines() == {}

    def test_write_config_preflight_refusal_leaves_nothing_cached(self, resolved):
        with pytest.raises(ConfigDbNotFound):
            write_config(resolved, cfg(), expected_generation=0)
        assert _cached_engines() == {}

    def test_dispose_runs_even_when_the_write_raises_something_unexpected(
        self, seeded, monkeypatch
    ):
        read_config(seeded)
        monkeypatch.setattr(
            store_access, "_write_config",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        with pytest.raises(RuntimeError):
            write_config(seeded, cfg(), expected_generation=0)
        assert _cached_engines() == {}


class TestImportResultGeneration:
    """`ImportResult.generation` is the generation the import itself committed,
    so a caller reports it without a second read after the writer lock is
    released (by which time another writer may have moved it on)."""

    @staticmethod
    def _stored_generation(db_path):
        con = sqlite3.connect(str(db_path))
        try:
            return con.execute("SELECT generation FROM global_settings").fetchone()[0]
        finally:
            con.close()

    def test_a_first_import_reports_generation_zero(self, db_path, resolved):
        result = import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        assert result.created is True
        assert result.generation == 0 == self._stored_generation(db_path)

    def test_it_equals_the_committed_generation_and_increases_across_reimports(
        self, db_path, resolved, tmp_path
    ):
        seen = []
        for i in range(4):
            result = import_yaml(write_yaml(tmp_path / f"{i}.yaml", f"tank/d{i}"), resolved)
            dispose_all()
            assert result.generation == self._stored_generation(db_path)
            seen.append(result.generation)
        assert seen == [0, 1, 2, 3]

    def test_it_equals_what_read_config_reports(self, seeded, tmp_path):
        result = import_yaml(write_yaml(tmp_path / "x.yaml", "tank/x"), seeded)
        dispose_all()
        assert result.generation == read_config(seeded).generation

    def test_it_follows_a_write_config_in_between(self, seeded, db_path, tmp_path):
        write_config(seeded, cfg("tank/w"), expected_generation=0)  # -> 1
        result = import_yaml(write_yaml(tmp_path / "x.yaml", "tank/x"), seeded)
        assert result.generation == 2 == self._stored_generation(db_path)

    def test_a_later_write_does_not_change_what_the_import_reported(self, seeded, tmp_path):
        """The result is a value, not a view: a read-back after the lock is
        released would see the later writer's generation instead."""
        result = import_yaml(write_yaml(tmp_path / "x.yaml", "tank/x"), seeded)
        dispose_all()
        write_config(seeded, cfg("tank/after"), expected_generation=result.generation)
        dispose_all()
        assert result.generation == 1
        assert read_config(seeded).generation == 2

    def test_a_hand_built_result_defaults_to_no_generation(self):
        from zfsbackup.config.store import ImportResult

        assert ImportResult(True, "head", 0, 0).generation is None


class TestWriteConfigBusy:
    def test_the_writer_lock_held_by_another_process_is_config_busy(
        self, seeded, db_path, lock_holder
    ):
        lock_holder()
        started = time.monotonic()
        with pytest.raises(ConfigBusy) as excinfo:
            write_config(seeded, cfg("tank/mine"), expected_generation=0, lock_timeout=0.4)
        assert 0.3 <= time.monotonic() - started < 5
        assert str(seeded.path) in str(excinfo.value)
        assert not isinstance(excinfo.value, (ConfigPathError, SchemaError))
        dispose_all()
        snap = read_config(seeded)
        assert snap.generation == 0 and snap.config == BackupConfig.from_file(EXAMPLE_YAML)

    def test_the_write_goes_through_once_the_holder_is_gone(self, seeded, lock_holder):
        holder = lock_holder()
        with pytest.raises(ConfigBusy):
            write_config(seeded, cfg("tank/mine"), expected_generation=0, lock_timeout=0.2)
        holder.kill()  # the kernel releases a dead process's flock
        assert write_config(seeded, cfg("tank/mine"), expected_generation=0).generation == 1

    def test_a_waiting_writer_gets_the_lock_when_the_holder_lets_go(self, seeded, lock_holder):
        holder = lock_holder()
        timer = threading.Timer(0.5, holder.release)
        timer.start()
        try:
            started = time.monotonic()
            result = write_config(seeded, cfg("tank/mine"), expected_generation=0, lock_timeout=20)
        finally:
            timer.cancel()
            timer.join()
        assert result.generation == 1
        assert 0.3 <= time.monotonic() - started < 15

    def test_a_sqlite_locked_error_is_mapped_to_config_busy(self, seeded, monkeypatch):
        def locked(*_a, **_k):
            raise OperationalError("BEGIN IMMEDIATE", {}, Exception("database is locked"))

        monkeypatch.setattr(store_access, "save_config", locked)
        with pytest.raises(ConfigBusy) as excinfo:
            write_config(seeded, cfg(), expected_generation=0)
        assert isinstance(excinfo.value.__cause__, OperationalError)
        assert "nothing was written" in str(excinfo.value)

    def test_other_operational_errors_are_not_mapped(self, seeded, monkeypatch):
        def broken(*_a, **_k):
            raise OperationalError("INSERT", {}, Exception("no such table: datasets"))

        monkeypatch.setattr(store_access, "save_config", broken)
        with pytest.raises(OperationalError, match="no such table"):
            write_config(seeded, cfg(), expected_generation=0)

    @pytest.mark.slow
    def test_an_external_write_lock_held_past_busy_timeout_is_config_busy(
        self, seeded, db_path
    ):
        """SQLite's own lock: a writer outside the CLI (the `sqlite3` shell, a
        script) holds `BEGIN IMMEDIATE`; the real 5 s `busy_timeout` expires."""
        dispose_all()
        external = sqlite3.connect(str(db_path), timeout=0, isolation_level=None)
        external.execute("PRAGMA journal_mode=WAL")
        external.execute("BEGIN IMMEDIATE")
        try:
            started = time.monotonic()
            with pytest.raises(ConfigBusy) as excinfo:
                write_config(seeded, cfg("tank/mine"), expected_generation=0)
            assert time.monotonic() - started >= 4  # the busy_timeout really ran
            assert isinstance(excinfo.value.__cause__, OperationalError)
        finally:
            external.execute("ROLLBACK")
            external.close()
        dispose_all()
        assert read_config(seeded).generation == 0  # nothing was written
        assert write_config(seeded, cfg("tank/mine"), expected_generation=0).generation == 1

    def test_a_busy_writer_does_not_block_readers(self, seeded, db_path):
        dispose_all()
        external = sqlite3.connect(str(db_path), timeout=0, isolation_level=None)
        external.execute("BEGIN IMMEDIATE")
        try:
            assert read_config(seeded).generation == 0
        finally:
            external.execute("ROLLBACK")
            external.close()


# ---------------------------------------------------------------------------
# writer_lock
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestWriterLockFile:
    def test_the_path_is_the_database_path_plus_a_suffix(self, resolved):
        assert WRITER_LOCK_SUFFIX == ".writer.lock"
        assert writer_lock_path(resolved) == Path(str(resolved.resolved_path) + ".writer.lock")
        # NOT `<db>.lock`: that name belongs to SQLite's unix-dotfile VFS.
        assert writer_lock_path(resolved).name != "config.db.lock"

    @pytest.mark.parametrize("umask", [0o022, 0o077, 0o000])
    def test_a_new_lock_file_is_0660_whatever_the_umask(self, resolved, umask):
        old = os.umask(umask)
        try:
            with writer_lock(resolved) as lock:
                mode = lock.stat().st_mode & 0o7777
        finally:
            os.umask(old)
        assert mode == 0o660
        assert (lock.stat().st_mode & 0o7777) == 0o660

    def test_it_is_empty_and_never_deleted(self, resolved):
        lock = writer_lock_path(resolved)
        assert not lock.exists()
        with writer_lock(resolved):
            assert lock.exists()
        assert lock.exists() and lock.stat().st_size == 0
        inode = lock.stat().st_ino
        with writer_lock(resolved):
            pass
        assert lock.stat().st_ino == inode  # re-used, not re-created

    def test_it_survives_an_exception_in_the_block(self, resolved):
        lock = writer_lock_path(resolved)
        with pytest.raises(KeyError):
            with writer_lock(resolved):
                raise KeyError("boom")
        assert lock.exists()
        with writer_lock(resolved, timeout=0):  # and was released
            pass

    def test_it_creates_no_directory(self, tmp_path):
        resolved = resolve_config_path(str(tmp_path / "nodir" / "config.db"))
        with pytest.raises(ConfigDbPermissionError):
            with writer_lock(resolved):
                pass  # pragma: no cover
        assert not (tmp_path / "nodir").exists()

    @pytest.mark.skipif(IS_ROOT, reason="root can write a 0444 file")
    def test_a_read_only_existing_lock_file_still_locks(self, resolved):
        """flock(2) needs no write access: an operator who can only read a
        root-made lock file must still be able to take the lock."""
        lock = writer_lock_path(resolved)
        lock.write_bytes(b"")
        os.chmod(lock, 0o444)
        with writer_lock(resolved, timeout=0):
            pass
        assert (lock.stat().st_mode & 0o7777) == 0o444  # not touched off the default path

    @pytest.mark.skipif(IS_ROOT, reason="root can write a 0444 file")
    def test_a_read_only_lock_file_still_excludes_a_second_holder(self, resolved):
        lock = writer_lock_path(resolved)
        lock.write_bytes(b"")
        os.chmod(lock, 0o444)
        outcome = {}
        entered = threading.Event()
        leave = threading.Event()

        def holder():
            with writer_lock(resolved):
                entered.set()
                leave.wait(30)

        thread = threading.Thread(target=holder, daemon=True)
        thread.start()
        try:
            assert entered.wait(10)
            with pytest.raises(ConfigBusy):
                with writer_lock(resolved, timeout=0.1):
                    outcome["entered"] = True  # pragma: no cover
        finally:
            leave.set()
            thread.join(10)
        assert "entered" not in outcome

    def test_a_symlink_in_place_of_the_lock_file_is_refused(self, tmp_path, resolved):
        target = tmp_path / "victim"
        target.write_bytes(b"precious")
        lock = writer_lock_path(resolved)
        lock.symlink_to(target)
        with pytest.raises(ConfigDbPermissionError):
            with writer_lock(resolved):
                pass  # pragma: no cover
        assert target.read_bytes() == b"precious"
        assert lock.is_symlink()

    def test_a_dangling_symlink_is_not_created_through(self, tmp_path, resolved):
        target = tmp_path / "not-yet"
        lock = writer_lock_path(resolved)
        lock.symlink_to(target)
        with pytest.raises(ConfigDbPermissionError):
            with writer_lock(resolved):
                pass  # pragma: no cover
        assert not target.exists()

    def test_the_same_thread_cannot_take_it_twice(self, resolved):
        with writer_lock(resolved):
            with pytest.raises(RuntimeError, match="not re-entrant"):
                with writer_lock(resolved, timeout=0):
                    pass  # pragma: no cover
            # The refusal did not release or corrupt the outer hold.
            holder_error = []

            def probe():
                try:
                    with writer_lock(resolved, timeout=0):
                        pass  # pragma: no cover
                except ConfigBusy as exc:
                    holder_error.append(exc)

            thread = threading.Thread(target=probe)
            thread.start()
            thread.join(10)
            assert len(holder_error) == 1
        with writer_lock(resolved, timeout=0):  # fully released afterwards
            pass

    def test_re_entry_is_caught_through_a_different_spelling_of_the_path(
        self, tmp_path, store_dir, resolved
    ):
        link = tmp_path / "link"
        link.symlink_to(store_dir)
        via_link = resolve_config_path(str(link / "config.db"))
        assert writer_lock_path(via_link) == writer_lock_path(resolved)
        with writer_lock(resolved):
            with pytest.raises(RuntimeError, match="not re-entrant"):
                with writer_lock(via_link, timeout=0):
                    pass  # pragma: no cover

    def test_another_thread_waits_then_gets_config_busy_on_timeout(self, resolved):
        errors = []
        with writer_lock(resolved):
            def contender():
                started = time.monotonic()
                try:
                    with writer_lock(resolved, timeout=0.3):
                        pass  # pragma: no cover
                except ConfigBusy as exc:
                    errors.append((exc, time.monotonic() - started))

            thread = threading.Thread(target=contender)
            thread.start()
            thread.join(10)
        (exc, waited), = errors
        assert 0.25 <= waited < 5
        assert str(resolved.path) in str(exc)

    def test_timeout_zero_tries_exactly_once(self, resolved):
        errors = []
        with writer_lock(resolved):
            def contender():
                started = time.monotonic()
                try:
                    with writer_lock(resolved, timeout=0):
                        pass  # pragma: no cover
                except ConfigBusy:
                    errors.append(time.monotonic() - started)

            thread = threading.Thread(target=contender)
            thread.start()
            thread.join(10)
        assert errors and errors[0] < 1.0

    def test_another_thread_proceeds_as_soon_as_the_holder_leaves(self, resolved):
        got = []
        release = threading.Event()
        entered = threading.Event()

        def holder():
            with writer_lock(resolved):
                entered.set()
                release.wait(10)

        def waiter():
            with writer_lock(resolved, timeout=20):
                got.append(time.monotonic())

        h = threading.Thread(target=holder)
        h.start()
        assert entered.wait(10)
        w = threading.Thread(target=waiter)
        w.start()
        time.sleep(0.3)
        assert got == []  # genuinely waiting
        released_at = time.monotonic()
        release.set()
        h.join(10)
        w.join(10)
        assert got and got[0] - released_at < 5


@pytest.mark.integration
class TestWriterLockAcrossProcesses:
    def test_a_lock_held_by_another_process_is_config_busy(self, resolved, lock_holder):
        lock_holder()
        with pytest.raises(ConfigBusy):
            with writer_lock(resolved, timeout=0.3):
                pass  # pragma: no cover

    def test_a_killed_holder_releases_the_lock(self, resolved, lock_holder):
        holder = lock_holder()
        with pytest.raises(ConfigBusy):
            with writer_lock(resolved, timeout=0.1):
                pass  # pragma: no cover
        holder.kill()
        assert holder.proc.returncode == -signal.SIGKILL
        with writer_lock(resolved, timeout=0):
            pass
        assert writer_lock_path(resolved).exists()  # still never deleted

    def test_a_released_holder_frees_the_lock(self, resolved, lock_holder):
        holder = lock_holder()
        holder.release()
        assert holder.proc.returncode == 0
        with writer_lock(resolved, timeout=0):
            pass

    @pytest.mark.parametrize("round_no", [0, 1, 2])
    def test_two_concurrent_first_imports_serialise(self, tmp_path, round_no):
        """Two processes import different YAMLs into a database that does not
        exist yet, released together. Under the writer lock the loser finds the
        winner's file and RE-imports over it: both succeed, EXACTLY ONE
        reports `created=True`, and the database holds one complete config."""
        directory = tmp_path / f"race{round_no}"
        directory.mkdir()
        db = directory / "config.db"
        gate = tmp_path / f"gate{round_no}"
        yamls = [
            write_yaml(tmp_path / f"a{round_no}.yaml", "tank/a"),
            write_yaml(tmp_path / f"b{round_no}.yaml", "tank/b"),
        ]
        procs = [
            subprocess.Popen(
                [sys.executable, "-c", IMPORTER, str(y), str(db), str(gate)],
                cwd=ROOT, env=_child_env(), stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True,
            )
            for y in yamls
        ]
        try:
            time.sleep(1.0)  # both are parked on the gate, imports already loaded
            gate.write_text("go")
            results = [p.communicate(timeout=120) for p in procs]
        finally:
            for p in procs:
                if p.poll() is None:
                    p.kill()
                    p.communicate()

        for proc, (out, err) in zip(procs, results):
            assert proc.returncode == 0, err
        created = sorted(out.strip() for out, _ in results)
        assert created == ["created=False", "created=True"], results

        resolved = resolve_config_path(str(db))
        snap = read_config(resolved)
        dispose_all()
        assert [d.name for d in snap.config.datasets] in (["tank/a"], ["tank/b"])
        assert snap.generation == 1  # created at 0, re-imported once

    def test_a_write_waits_for_a_running_import_and_then_conflicts_or_wins(
        self, seeded, tmp_path
    ):
        """An import holds the same lock `write_config` takes, so a write
        started during it does not interleave: afterwards the generation it
        expected is stale."""
        other = write_yaml(tmp_path / "o.yaml", "tank/other")
        gate = tmp_path / "go"
        proc = subprocess.Popen(
            [sys.executable, "-c", IMPORTER, str(other), str(seeded.path), str(gate)],
            cwd=ROOT, env=_child_env(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            time.sleep(1.0)
            gate.write_text("go")
            time.sleep(0.05)
            try:
                write_config(seeded, cfg("tank/mine"), expected_generation=0, lock_timeout=60)
                outcome = "won"
            except ConfigConflict:
                outcome = "conflict"
            out, err = proc.communicate(timeout=120)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.communicate()
        assert proc.returncode == 0, err
        dispose_all()
        names = stored_names(seeded)
        # Either order is serialised; what can never happen is both applying
        # on top of generation 0.
        if outcome == "won":
            assert names == ["tank/other"]  # the import ran second and replaced it
            assert read_config(seeded).generation == 2
        else:
            assert names == ["tank/other"]
            assert read_config(seeded).generation == 1


# ---------------------------------------------------------------------------
# import_yaml and the writer lock
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestImportTakesTheWriterLock:
    def test_a_successful_import_leaves_the_empty_lock_file(self, store_dir, resolved):
        import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        lock = store_dir / ("config.db" + WRITER_LOCK_SUFFIX)
        assert lock.exists() and lock.stat().st_size == 0
        assert (lock.stat().st_mode & 0o7777) == 0o660

    def test_import_waits_for_a_held_lock_and_then_config_busy(self, store_dir, resolved, monkeypatch):
        """Another thread holds the lock; an import with a short timeout gives
        up with ConfigBusy and touches nothing."""
        from zfsbackup.config.store import importer as importer_module

        real = importer_module.writer_lock
        monkeypatch.setattr(
            importer_module, "writer_lock", lambda r, **k: real(r, timeout=0.2)
        )
        entered = threading.Event()
        leave = threading.Event()

        def holder():
            with writer_lock(resolved):
                entered.set()
                leave.wait(30)

        thread = threading.Thread(target=holder, daemon=True)
        thread.start()
        try:
            assert entered.wait(10)
            with pytest.raises(ConfigBusy):
                import_yaml(EXAMPLE_YAML, resolved)
        finally:
            leave.set()
            thread.join(10)
        assert listing(store_dir) == ["config.db" + WRITER_LOCK_SUFFIX]  # no database made

    def test_whether_the_database_exists_is_decided_under_the_lock(
        self, store_dir, db_path, resolved, tmp_path, monkeypatch
    ):
        """B parks on the lock having seen NO database; while it waits, another
        party creates one. B must then take the re-import branch
        (`created=False`): the existence decision belongs inside the lock, not
        before it (item 15.4: "takes it before phase 1 decides whether the file
        exists")."""
        from contextlib import contextmanager

        from zfsbackup.config.store import importer as importer_module

        yaml_a = write_yaml(tmp_path / "a.yaml", "tank/a")
        yaml_b = write_yaml(tmp_path / "b.yaml", "tank/b")
        box = {}

        def b_import():
            try:
                box["result"] = import_yaml(yaml_b, resolved)
                dispose_all()
            except BaseException as exc:  # reported by the test thread
                box["error"] = exc

        with writer_lock(resolved):
            thread = threading.Thread(target=b_import, daemon=True)
            thread.start()
            thread.join(0.5)
            assert thread.is_alive() and not db_path.exists()  # parked, DB absent

            @contextmanager
            def no_lock(*_a, **_k):
                yield None

            real = importer_module.writer_lock
            monkeypatch.setattr(importer_module, "writer_lock", no_lock)
            try:
                assert import_yaml(yaml_a, resolved).created is True  # a lock-ignoring creator
            finally:
                monkeypatch.setattr(importer_module, "writer_lock", real)
            dispose_all()
        thread.join(60)

        assert not thread.is_alive() and "error" not in box, box
        assert box["result"].created is False
        assert stored_names(resolved) == ["tank/b"]

    def test_create_false_on_a_missing_database_creates_no_lock_file(self, store_dir, resolved):
        with pytest.raises(ConfigDbNotFound):
            import_yaml(EXAMPLE_YAML, resolved, create=False)
        assert listing(store_dir) == []

    def test_a_bad_yaml_creates_no_lock_file(self, store_dir, resolved, tmp_path):
        bad = tmp_path / "bad.yaml"
        bad.write_text("datasets:\n  - name: tank/x\n    enabled: 5\n")
        with pytest.raises(ValueError):
            import_yaml(bad, resolved)
        assert listing(store_dir) == []

    def test_a_yaml_named_target_creates_no_lock_file(self, store_dir):
        resolved = resolve_config_path(str(store_dir / "config.yaml"))
        with pytest.raises(store_paths.ConfigDbIsYaml):
            import_yaml(EXAMPLE_YAML, resolved)
        assert listing(store_dir) == []

    def test_orphan_sidecars_create_no_lock_file(self, store_dir, resolved):
        orphan = store_dir / "config.db-wal"
        orphan.write_bytes(b"x")
        with pytest.raises(store_paths.ConfigDbOrphanedSidecars):
            import_yaml(EXAMPLE_YAML, resolved)
        assert listing(store_dir) == ["config.db-wal"]

    def test_a_missing_directory_is_created_for_the_lock_and_the_database(self, tmp_path):
        resolved = resolve_config_path(str(tmp_path / "fresh" / "config.db"))
        assert import_yaml(EXAMPLE_YAML, resolved).created is True
        dispose_all()
        assert (tmp_path / "fresh" / ("config.db" + WRITER_LOCK_SUFFIX)).exists()

    def test_create_false_does_not_create_the_missing_directory(self, tmp_path):
        resolved = resolve_config_path(str(tmp_path / "fresh" / "config.db"))
        with pytest.raises(ConfigDbNotFound):
            import_yaml(EXAMPLE_YAML, resolved, create=False)
        assert not (tmp_path / "fresh").exists()

    def test_an_import_bumps_the_generation_a_stale_writer_holds(self, seeded, tmp_path):
        snap = read_config(seeded)
        dispose_all()
        import_yaml(EXAMPLE_YAML, seeded)  # the SAME yaml: still a new generation
        dispose_all()
        with pytest.raises(ConfigConflict):
            write_config(seeded, cfg(), expected_generation=snap.generation)


# ---------------------------------------------------------------------------
# The store's shared error wording
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestConfigErrorMessage:
    @pytest.fixture
    def named(self, tmp_path):
        return resolve_config_path(str(tmp_path / "named.db"))

    def test_a_database_error_is_one_line_in_sqlites_words(self, named):
        exc = DatabaseError(
            "SELECT datasets.id FROM datasets", {},
            Exception("database disk image is malformed\nsecond line"),
        )
        message = config_error_message(named, exc)
        assert message == (
            f"the config database {describe_config_source(named)} is damaged or not an "
            "SQLite database: database disk image is malformed; restore it from a backup."
        )
        assert "\n" not in message
        assert "SELECT" not in message and "sqlalche.me" not in message

    def test_without_a_resolved_path_the_subject_is_generic(self):
        exc = DatabaseError("S", {}, Exception("file is not a database"))
        assert config_error_message(None, exc) == (
            "the config database is damaged or not an SQLite database: "
            "file is not a database; restore it from a backup."
        )

    @pytest.mark.parametrize("text", ["", "   ", "\n"], ids=["empty", "blank", "newline"])
    def test_an_empty_sqlite_message_falls_back_to_the_exception_type(self, text):
        exc = DatabaseError("SELECT 1", {}, Exception(text))
        assert config_error_message(None, exc).endswith(
            "is damaged or not an SQLite database: DatabaseError; restore it from a backup."
        )

    def test_only_an_exact_database_error_is_called_damaged(self, seeded):
        """SQLAlchemy's subclasses (`OperationalError`, `IntegrityError`, ...)
        are statements somebody built wrongly or a locked file, not a bad file."""
        locked = OperationalError("S", {}, Exception("database is locked"))
        message = config_error_message(seeded, locked)
        assert "damaged" not in message
        assert message.startswith("Failed to read the config database")
        assert "restore it from a backup" not in message

    def test_schema_out_of_date_is_the_stores_text_verbatim(self, named):
        exc = SchemaOutOfDate(
            "The config database is at an older schema.\nRun `zfsbackup-config migrate`."
        )
        message = config_error_message(named, exc)
        assert message == str(exc)
        # The daemon's own closing sentence is NOT the store's to add.
        assert "daemon" not in message.lower()
        assert "restart" not in message.lower()

    def test_other_schema_errors_are_verbatim_too(self, named):
        exc = SchemaVersionMismatch("newer package wrote this\nsecond line kept")
        assert config_error_message(named, exc) == str(exc)

    def test_conflict_and_busy_are_verbatim(self, named):
        conflict = ConfigConflict(3, 4)
        busy = ConfigBusy("another zfsbackup-config write is still in progress")
        assert config_error_message(named, conflict) == str(conflict)
        assert config_error_message(None, busy) == str(busy)

    def test_a_path_error_is_verbatim_and_not_prefixed(self, named):
        exc = store_paths.ConfigDbNotFound("No config database exists at x\nRun: cmd")
        assert config_error_message(named, exc) == str(exc)

    def test_an_unknown_exception_gets_the_generic_prefix(self, named):
        message = config_error_message(named, ValueError("No datasets configured"))
        assert message.startswith("Failed to load configuration")
        assert "No datasets configured" in message

    def test_the_path_error_family_is_not_swallowed_by_the_damaged_branch(self, named):
        assert "damaged" not in config_error_message(
            named, store_paths.ConfigDbNotADatabase("not a database")
        )


@pytest.mark.unit
class TestDescribeConfigSource:
    def test_default_path_has_no_resolved_suffix(self):
        resolved = resolve_config_path(None, environ={})
        text = describe_config_source(resolved)
        assert text == f"{resolved.path} (default path)"

    def test_c_flag(self, tmp_path):
        resolved = resolve_config_path(str(tmp_path / "x.db"))
        assert describe_config_source(resolved).endswith("(via -c/--config)")
        assert describe_config_source(resolved).startswith(str(resolved.path))

    def test_environment(self, tmp_path):
        resolved = resolve_config_path(None, environ={CONFIG_PATH_ENV: str(tmp_path / "e.db")})
        assert describe_config_source(resolved).endswith(f"(via ${CONFIG_PATH_ENV})")

    def test_a_symlinked_path_shows_both_spellings(self, tmp_path):
        real = tmp_path / "real"
        real.mkdir()
        link = tmp_path / "link"
        link.symlink_to(real)
        resolved = resolve_config_path(str(link / "c.db"))
        text = describe_config_source(resolved)
        assert f"{resolved.path} -> {resolved.resolved_path}" in text


@pytest.mark.unit
class TestPublicSurface:
    def test_the_new_names_are_exported(self):
        import zfsbackup.config.store as store

        for name in (
            "ConfigSnapshot", "load_config_snapshot", "read_config", "write_config",
            "WriteResult", "check_config_schema", "config_error_message",
            "describe_config_source", "ConfigConflict", "ConfigBusy", "writer_lock",
            "writer_lock_path", "WRITER_LOCK_SUFFIX", "DEFAULT_WRITER_LOCK_TIMEOUT",
            "save_config",
        ):
            assert name in store.__all__, name
            assert hasattr(store, name), name

    def test_conflict_and_busy_are_plain_exceptions(self):
        assert ConfigConflict.__mro__[1] is Exception
        assert ConfigBusy.__mro__[1] is Exception
        assert not issubclass(ConfigConflict, ConfigBusy)

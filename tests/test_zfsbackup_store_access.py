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


# ===========================================================================
# Item 17.0 -- store id (PRAGMA user_version) and read_store_state
# ===========================================================================
#
# `(store_id, generation)` is the reload key. The store id is a random per-FILE
# number, stamped by `save_config` as its LAST statement, so a delete + re-import
# (generation 0 again) is still told apart from the file a stale reader saw.
#
# Mutation counts for this section (scratch copy of the tree, one revert at a
# time, tests failing across the four 17.0 files) are in the hand-off report.

import hashlib  # noqa: E402
import logging  # noqa: E402
from types import SimpleNamespace  # noqa: E402

from sqlalchemy import event  # noqa: E402
from sqlalchemy.engine import Engine  # noqa: E402

from zfsbackup.config.store import StoreState, read_store_state  # noqa: E402
from zfsbackup.config.store import importer as store_importer  # noqa: E402
from zfsbackup.config.store import mapper as store_mapper  # noqa: E402
from zfsbackup.config.store import require_writer_lock_held  # noqa: E402
from zfsbackup.config.store.importer import ImportResult  # noqa: E402
from zfsbackup.config.store import StoreIdConflict  # noqa: E402

STORE_ID_LIMIT = 2**31 - 1  # user_version is a signed 32-bit int; the stamp stays below


def raw_user_version(db: Path) -> int:
    con = sqlite3.connect(str(db))
    try:
        return con.execute("PRAGMA user_version").fetchone()[0]
    finally:
        con.close()


def set_raw_user_version(db: Path, value: int) -> None:
    con = sqlite3.connect(str(db))
    try:
        con.execute(f"PRAGMA user_version = {int(value)}")
        con.commit()
    finally:
        con.close()


def raw_generation(db: Path) -> int:
    con = sqlite3.connect(str(db))
    try:
        return con.execute("SELECT generation FROM global_settings").fetchone()[0]
    finally:
        con.close()


def file_state(db: Path):
    """The bytes of the database and its WAL, without opening either."""
    wal = db.with_name(db.name + "-wal")
    return (
        hashlib.sha256(db.read_bytes()).hexdigest(),
        hashlib.sha256(wal.read_bytes()).hexdigest() if wal.exists() else None,
    )


def remove_database(db: Path) -> None:
    dispose_all()
    for suffix in ("", "-wal", "-shm"):
        p = db.with_name(db.name + suffix)
        if p.exists():
            p.unlink()


@pytest.fixture
def fixed_ids(monkeypatch):
    """Make the stamp deterministic: successive stamps are 1001, 1002, ...
    (`secrets.randbelow(n) + 1`, so the fake returns 1000, 1001, ...)."""
    calls = []

    def randbelow(n):
        calls.append(n)
        return 999 + len(calls)

    monkeypatch.setattr(store_mapper, "secrets", SimpleNamespace(randbelow=randbelow))
    return calls


@pytest.mark.integration
class TestStoreIdStamping:
    def test_the_first_import_stamps_a_non_zero_store_id_below_the_limit(self, db_path, resolved):
        result = import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        raw = raw_user_version(db_path)
        assert 0 < raw < STORE_ID_LIMIT
        assert result.store_id == raw

    def test_import_write_and_read_agree_on_the_store_id_and_later_saves_keep_it(
        self, db_path, resolved
    ):
        imported = import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        snap0 = read_config(resolved)
        first = write_config(resolved, cfg("tank/a"), expected_generation=0)
        second = write_config(resolved, cfg("tank/b"), expected_generation=first.generation)
        snap2 = read_config(resolved)
        dispose_all()

        ids = {imported.store_id, snap0.store_id, first.store_id, second.store_id, snap2.store_id}
        assert len(ids) == 1
        assert ids == {raw_user_version(db_path)}
        assert 0 < ids.pop() < STORE_ID_LIMIT

    def test_a_write_config_store_id_is_the_id_in_the_file_not_a_fresh_one(
        self, db_path, resolved, fixed_ids
    ):
        import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        assert raw_user_version(db_path) == 1000 + 1
        for generation in range(3):
            result = write_config(resolved, cfg(), expected_generation=generation)
            assert result.store_id == 1001
        assert fixed_ids == [2**31 - 2]  # drawn once, by the import

    def test_a_legacy_database_is_stamped_by_its_next_write_config(self, db_path, resolved):
        import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        set_raw_user_version(db_path, 0)
        assert read_config(resolved).store_id == 0
        dispose_all()

        result = write_config(resolved, cfg("tank/new"), expected_generation=0)
        dispose_all()

        assert 0 < result.store_id < STORE_ID_LIMIT
        assert raw_user_version(db_path) == result.store_id
        assert read_config(resolved).store_id == result.store_id

    def test_a_reimport_over_an_existing_database_keeps_its_store_id(self, db_path, resolved):
        first = import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        second = import_yaml(write_yaml(db_path.parent / "n.yaml", "tank/other"), resolved)
        dispose_all()
        assert first.created and not second.created
        assert second.store_id == first.store_id == raw_user_version(db_path)
        assert second.generation == 1

    def test_a_reimport_over_a_legacy_database_stamps_it(self, db_path, resolved):
        import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        set_raw_user_version(db_path, 0)
        result = import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        assert result.store_id != 0
        assert raw_user_version(db_path) == result.store_id

    def test_a_non_zero_store_id_is_never_re_stamped(self, db_path, resolved):
        import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        set_raw_user_version(db_path, 7)
        result = write_config(resolved, cfg(), expected_generation=0)
        dispose_all()
        assert result.store_id == 7 == raw_user_version(db_path)


@pytest.mark.integration
class TestStoreIdRollback:
    """The stamp is the same transaction as the data: a failure after it leaves
    the legacy 0 and the previous generation."""

    def test_a_write_config_that_fails_after_the_stamp_leaves_zero_and_the_old_data(
        self, db_path, resolved, monkeypatch
    ):
        import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        before_names = stored_names(resolved)
        set_raw_user_version(db_path, 0)

        def boom(session):
            raise RuntimeError("injected after the stamp")

        monkeypatch.setattr(store_access, "read_store_id", boom)
        with pytest.raises(RuntimeError, match="injected"):
            write_config(resolved, cfg("tank/new"), expected_generation=0)
        dispose_all()

        assert raw_user_version(db_path) == 0
        assert raw_generation(db_path) == 0
        assert stored_names(resolved) == before_names

        # ... and the next, healthy write stamps.
        monkeypatch.undo()
        result = write_config(resolved, cfg("tank/new"), expected_generation=0)
        assert result.store_id != 0

    def test_an_import_that_fails_after_the_stamp_leaves_zero_and_the_old_data(
        self, db_path, resolved, monkeypatch
    ):
        import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        before_names = stored_names(resolved)
        set_raw_user_version(db_path, 0)

        def boom(session):
            raise RuntimeError("injected after the stamp")

        monkeypatch.setattr(store_importer, "read_store_id", boom)
        with pytest.raises(RuntimeError, match="injected"):
            import_yaml(write_yaml(db_path.parent / "n.yaml", "tank/other"), resolved)
        dispose_all()

        assert raw_user_version(db_path) == 0
        assert raw_generation(db_path) == 0
        assert stored_names(resolved) == before_names

    def test_a_failed_first_import_does_not_leave_a_stamped_file(
        self, db_path, resolved, monkeypatch
    ):
        def boom(session):
            raise RuntimeError("injected after the stamp")

        monkeypatch.setattr(store_importer, "read_store_id", boom)
        with pytest.raises(RuntimeError, match="injected"):
            import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        assert not db_path.exists() or raw_user_version(db_path) == 0


@pytest.mark.integration
class TestStoreIdCompare:
    @pytest.fixture
    def replaced(self, db_path, resolved, fixed_ids):
        """An old snapshot (id 1001, generation 0) of a file that was then
        deleted and re-imported (id 1002, generation 0 again)."""
        import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        old = read_config(resolved)
        dispose_all()
        remove_database(db_path)
        new = import_yaml(write_yaml(db_path.parent / "n.yaml", "tank/replacement"), resolved)
        dispose_all()
        return SimpleNamespace(old=old, new=new)

    def test_delete_and_reimport_gives_a_new_store_id_and_generation_zero(self, replaced):
        assert replaced.old.generation == 0 and replaced.new.generation == 0
        assert replaced.old.store_id == 1001
        assert replaced.new.store_id == 1002

    def test_a_stale_store_id_with_a_matching_generation_is_a_store_id_conflict(
        self, db_path, resolved, replaced
    ):
        before = file_state(db_path)
        with pytest.raises(ConfigConflict) as excinfo:
            write_config(
                resolved, cfg("tank/mine"),
                expected_generation=replaced.old.generation,
                expected_store_id=replaced.old.store_id,
            )
        assert isinstance(excinfo.value, StoreIdConflict)
        assert (excinfo.value.expected_store_id, excinfo.value.actual_store_id) == (1001, 1002)
        # The inherited attributes keep their ConfigConflict meaning: generations.
        assert (excinfo.value.expected, excinfo.value.actual) == (0, 0)
        assert "replaced" in str(excinfo.value) and "nothing was written" in str(excinfo.value)
        assert file_state(db_path) == before
        assert stored_names(resolved) == ["tank/replacement"]

    def test_expected_store_id_none_skips_the_compare(self, resolved, replaced):
        result = write_config(
            resolved, cfg("tank/mine"), expected_generation=0, expected_store_id=None
        )
        assert result.generation == 1 and result.store_id == 1002
        assert stored_names(resolved) == ["tank/mine"]

    def test_expected_store_id_defaults_to_not_compared(self, resolved, replaced):
        assert write_config(resolved, cfg(), expected_generation=0).store_id == 1002

    def test_the_current_store_id_passes(self, resolved, replaced):
        result = write_config(
            resolved, cfg("tank/mine"), expected_generation=0,
            expected_store_id=replaced.new.store_id,
        )
        assert result.generation == 1

    def test_the_store_id_check_wins_when_both_it_and_the_generation_are_wrong(
        self, db_path, resolved, replaced
    ):
        before = file_state(db_path)
        with pytest.raises(ConfigConflict) as excinfo:
            write_config(
                resolved, cfg("tank/mine"), expected_generation=99, expected_store_id=1001
            )
        assert type(excinfo.value) is StoreIdConflict
        assert (excinfo.value.expected_store_id, excinfo.value.actual_store_id) == (1001, 1002)
        assert (excinfo.value.expected, excinfo.value.actual) == (99, 0)
        assert file_state(db_path) == before

    def test_store_id_conflict_expected_and_actual_are_generations(
        self, db_path, resolved, fixed_ids
    ):
        """The old file got to generation 2; the replacement is at 1. A
        generic `ConfigConflict` consumer that prints `expected`/`actual` as
        generations must be told the truth, not two store ids."""
        import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        write_config(resolved, cfg("tank/a"), expected_generation=0)
        write_config(resolved, cfg("tank/b"), expected_generation=1)
        old = read_config(resolved)
        dispose_all()
        assert (old.store_id, old.generation) == (1001, 2)
        remove_database(db_path)
        import_yaml(EXAMPLE_YAML, resolved)
        write_config(resolved, cfg("tank/c"), expected_generation=0)

        with pytest.raises(StoreIdConflict) as excinfo:
            write_config(
                resolved, cfg("tank/mine"), expected_generation=old.generation,
                expected_store_id=old.store_id,
            )
        exc = excinfo.value
        assert (exc.expected, exc.actual) == (2, 1)
        assert (exc.expected_store_id, exc.actual_store_id) == (1001, 1002)

    def test_store_id_conflict_expected_is_none_when_no_generation_was_expected(
        self, resolved, replaced
    ):
        with pytest.raises(StoreIdConflict) as excinfo:
            write_config(
                resolved, cfg("tank/mine"), expected_generation=None,
                expected_store_id=replaced.old.store_id,
            )
        assert excinfo.value.expected is None
        assert excinfo.value.actual == 0

    def test_a_wrong_generation_alone_is_still_a_plain_conflict(self, resolved, replaced):
        with pytest.raises(ConfigConflict) as excinfo:
            write_config(
                resolved, cfg("tank/mine"), expected_generation=99,
                expected_store_id=replaced.new.store_id,
            )
        assert type(excinfo.value) is ConfigConflict
        assert (excinfo.value.expected, excinfo.value.actual) == (99, 0)

    def test_f2_a_legacy_snapshot_stamped_since_is_a_plain_generation_conflict(
        self, db_path, resolved
    ):
        """The editor read a legacy, unstamped file (0, g); another writer
        stamped it (X, g+1). That is a moved generation, not a replaced file:
        a plain `ConfigConflict` carrying the generations, never a
        `StoreIdConflict`."""
        import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        set_raw_user_version(db_path, 0)
        legacy = read_config(resolved)
        dispose_all()
        assert (legacy.store_id, legacy.generation) == (0, 0)
        theirs = write_config(resolved, cfg("tank/theirs"), expected_generation=0)
        assert theirs.store_id != 0
        before = file_state(db_path)
        dispose_all()

        with pytest.raises(ConfigConflict) as excinfo:
            write_config(
                resolved, cfg("tank/mine"), expected_generation=legacy.generation,
                expected_store_id=legacy.store_id,
            )
        assert type(excinfo.value) is ConfigConflict
        assert not isinstance(excinfo.value, StoreIdConflict)
        assert (excinfo.value.expected, excinfo.value.actual) == (0, 1)
        assert file_state(db_path) == before
        assert stored_names(resolved) == ["tank/theirs"]

    def test_f2_a_zero_expected_id_with_no_expected_generation_writes_unconditionally(
        self, db_path, resolved
    ):
        import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        stamped = raw_user_version(db_path)
        result = write_config(
            resolved, cfg("tank/mine"), expected_generation=None, expected_store_id=0
        )
        assert result.store_id == stamped != 0

    def test_a_stale_non_zero_id_with_no_global_settings_row_reports_actual_none(
        self, seeded, db_path
    ):
        con = sqlite3.connect(str(db_path))
        con.execute("DELETE FROM global_settings")
        con.commit()
        con.close()
        stored = raw_user_version(db_path)
        with pytest.raises(StoreIdConflict) as excinfo:
            write_config(
                seeded, cfg(), expected_generation=0, expected_store_id=stored + 1
            )
        assert excinfo.value.actual is None
        assert excinfo.value.expected == 0
        assert (excinfo.value.expected_store_id, excinfo.value.actual_store_id) == (
            stored + 1, stored,
        )

    def test_a_non_zero_expected_id_against_a_legacy_file_is_a_store_id_conflict(
        self, db_path, resolved
    ):
        """Only a NON-ZERO expected id that differs means "another file"."""
        import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        set_raw_user_version(db_path, 0)
        with pytest.raises(StoreIdConflict) as excinfo:
            write_config(
                resolved, cfg(), expected_generation=0, expected_store_id=1234
            )
        assert (excinfo.value.expected_store_id, excinfo.value.actual_store_id) == (1234, 0)

    def test_a_legacy_snapshot_against_a_still_legacy_file_writes_and_stamps(
        self, db_path, resolved
    ):
        import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        set_raw_user_version(db_path, 0)
        result = write_config(resolved, cfg(), expected_generation=0, expected_store_id=0)
        assert result.store_id != 0


@pytest.mark.integration
class TestStoreIdWithAHeldWriterLock:
    def test_a_write_under_a_held_lock_compares_and_reports_the_store_id(self, seeded):
        snap = read_config(seeded)
        with writer_lock(seeded) as held:
            result = write_config(
                seeded, cfg("tank/held"), expected_generation=snap.generation,
                expected_store_id=snap.store_id, lock=held,
            )
        assert result.store_id == snap.store_id != 0
        assert stored_names(seeded) == ["tank/held"]

    def test_a_store_id_mismatch_leaves_the_callers_lock_held(self, seeded):
        snap = read_config(seeded)
        with writer_lock(seeded) as held:
            with pytest.raises(StoreIdConflict):
                write_config(
                    seeded, cfg("tank/held"), expected_generation=snap.generation,
                    expected_store_id=snap.store_id + 1, lock=held,
                )
            require_writer_lock_held(seeded, held)  # still the caller's
            box = {}

            def other():
                try:
                    with writer_lock(seeded, timeout=0.2):
                        box["got"] = True
                except BaseException as exc:  # noqa: BLE001
                    box["exc"] = exc

            thread = threading.Thread(target=other)
            thread.start()
            thread.join(10)
            assert isinstance(box.get("exc"), ConfigBusy), box
            # ... and the caller may still use it for the corrected write.
            write_config(
                seeded, cfg("tank/ok"), expected_generation=snap.generation,
                expected_store_id=snap.store_id, lock=held,
            )
        assert stored_names(seeded) == ["tank/ok"]


# ---------------------------------------------------------------------------
# read_store_state
# ---------------------------------------------------------------------------


def _wal_checkpoint_passive(db: Path):
    """(busy, wal frames, frames checkpointed) as a third connection sees them."""
    con = sqlite3.connect(str(db), timeout=5, isolation_level=None)
    try:
        return con.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
    finally:
        con.close()


def _commit_a_generation(writer: sqlite3.Connection) -> None:
    writer.execute("BEGIN IMMEDIATE")
    writer.execute("UPDATE global_settings SET generation = generation + 1")
    writer.execute("COMMIT")


@pytest.fixture
def pending_writer(seeded, db_path):
    """An open writer connection with auto-checkpointing off, so its commits
    stay in the WAL ("a pending writer commit") until somebody checkpoints."""
    writer = sqlite3.connect(str(db_path), timeout=5, isolation_level=None)
    writer.execute("PRAGMA wal_autocheckpoint = 0")
    yield writer
    writer.close()


@pytest.mark.integration
class TestReadStoreState:
    def test_it_returns_the_store_id_and_generation_pair(self, seeded, db_path):
        state = read_store_state(seeded)
        assert isinstance(state, StoreState)
        assert state == StoreState(store_id=raw_user_version(db_path), generation=0)
        assert type(state.store_id) is int and type(state.generation) is int
        assert 0 < state.store_id < STORE_ID_LIMIT

    def test_it_tracks_writes_and_replacements(self, db_path, resolved):
        import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        first = read_store_state(resolved)
        write_config(resolved, cfg(), expected_generation=0)
        second = read_store_state(resolved)
        assert second == StoreState(first.store_id, 1)
        remove_database(db_path)
        import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        third = read_store_state(resolved)
        assert third.generation == 0
        assert third != first

    def test_it_agrees_with_read_config(self, seeded):
        snap = read_config(seeded)
        assert read_store_state(seeded) == StoreState(snap.store_id, snap.generation)

    def test_a_legacy_database_reports_store_id_zero(self, seeded, db_path):
        set_raw_user_version(db_path, 0)
        assert read_store_state(seeded) == StoreState(0, 0)

    def test_it_is_a_frozen_value(self):
        state = StoreState(1, 2)
        with pytest.raises(Exception):
            state.generation = 3  # type: ignore[misc]
        assert state == StoreState(store_id=1, generation=2)

    def test_a_missing_file_is_config_db_not_found(self, resolved, db_path):
        with pytest.raises(ConfigDbNotFound):
            read_store_state(resolved)
        assert not db_path.exists()

    def test_a_zero_byte_file_is_config_db_not_a_database(self, resolved, db_path):
        db_path.write_bytes(b"")
        with pytest.raises(ConfigDbNotADatabase):
            read_store_state(resolved)
        assert db_path.stat().st_size == 0

    def test_a_database_without_a_global_settings_table_is_an_operational_error(
        self, resolved, db_path
    ):
        con = sqlite3.connect(str(db_path))
        con.execute("CREATE TABLE unrelated (x)")
        con.commit()
        con.close()
        with pytest.raises(OperationalError, match="global_settings"):
            read_store_state(resolved)

    def test_a_table_with_no_row_is_a_value_error(self, seeded, db_path):
        con = sqlite3.connect(str(db_path))
        con.execute("DELETE FROM global_settings")
        con.commit()
        con.close()
        with pytest.raises(ValueError, match="uninitialized"):
            read_store_state(seeded)

    def test_it_does_no_schema_check_on_a_newer_revision(self, seeded, db_path):
        con = sqlite3.connect(str(db_path))
        con.execute("UPDATE alembic_version SET version_num = 'ffffffffffff'")
        con.commit()
        con.close()
        with pytest.raises(SchemaError):  # control: the full read does check
            read_config(seeded)
        dispose_all()
        assert read_store_state(seeded).generation == 0

    def test_it_does_no_schema_check_on_an_out_of_date_database(
        self, seeded, monkeypatch
    ):
        """A known revision that is behind a (pretended) newer head."""
        from zfsbackup.config.store import migrate as migrate_module

        monkeypatch.setattr(migrate_module, "EXPECTED_HEAD", "bbbbbbbbbbbb")
        monkeypatch.setattr(
            migrate_module, "KNOWN_REVISIONS", frozenset((EXPECTED_HEAD, "bbbbbbbbbbbb"))
        )
        with pytest.raises(SchemaOutOfDate):  # control: the full read does check
            read_config(seeded)
        dispose_all()
        assert read_store_state(seeded).generation == 0

    def test_it_calls_neither_the_schema_check_nor_the_config_load(self, seeded, monkeypatch):
        def forbidden(*a, **k):
            raise AssertionError("read_store_state must read two facts and nothing else")

        monkeypatch.setattr(store_access, "check_schema", forbidden)
        monkeypatch.setattr(store_access, "load_config_snapshot", forbidden)
        assert read_store_state(seeded).generation == 0

    def test_it_leaves_its_engine_cached_until_dispose_all(self, seeded):
        dispose_all()
        assert _cached_engines() == {}
        read_store_state(seeded)
        assert len(_cached_engines()) >= 1
        dispose_all()
        assert _cached_engines() == {}

    def test_it_opens_a_read_only_connection_and_changes_no_byte(self, seeded, db_path, monkeypatch):
        seen = []
        real = store_access.open_config_connection

        def spy(resolved, **kwargs):
            seen.append(kwargs)
            return real(resolved, **kwargs)

        monkeypatch.setattr(store_access, "open_config_connection", spy)
        before = file_state(db_path)
        read_store_state(seeded)
        assert [kw.get("readonly") for kw in seen] == [True]
        dispose_all()
        assert file_state(db_path) == before

    def test_the_connection_it_opens_refuses_writes(self, seeded):
        """The reader engine is `query_only`: even the pragma cannot be set."""
        from zfsbackup.config.store import open_config_connection
        from sqlalchemy import text

        with open_config_connection(seeded, readonly=True) as connection:
            with pytest.raises(OperationalError):
                connection.execute(text("UPDATE global_settings SET generation = 9"))

    def test_the_pair_comes_from_one_snapshot_even_if_a_writer_commits_between_the_reads(
        self, seeded, db_path
    ):
        """Deterministic: right after the pragma runs, a second connection
        commits a new (id, generation). The SELECT must still see the old
        pair -- which it does only if `BEGIN` came first and the pragma opened
        the read snapshot."""
        set_raw_user_version(db_path, 1000)
        fired = []

        def writer_commits(conn, cursor, statement, parameters, context, executemany):
            if statement.strip().upper() == "PRAGMA USER_VERSION" and not fired:
                fired.append(True)
                con = sqlite3.connect(str(db_path), timeout=5, isolation_level=None)
                try:
                    con.execute("BEGIN IMMEDIATE")
                    con.execute("PRAGMA user_version = 1001")
                    con.execute("UPDATE global_settings SET generation = 1")
                    con.execute("COMMIT")
                finally:
                    con.close()

        event.listen(Engine, "after_cursor_execute", writer_commits)
        try:
            state = read_store_state(seeded)
        finally:
            event.remove(Engine, "after_cursor_execute", writer_commits)
        assert fired
        assert state == StoreState(1000, 0)
        dispose_all()
        assert read_store_state(seeded) == StoreState(1001, 1)

    def test_a_writer_loop_against_a_reader_loop_never_yields_a_torn_pair(
        self, seeded, db_path
    ):
        set_raw_user_version(db_path, 1000)
        rounds = 400
        stop = threading.Event()
        errors = []

        def writer():
            con = sqlite3.connect(str(db_path), timeout=10, isolation_level=None)
            try:
                for i in range(1, rounds + 1):
                    con.execute("BEGIN IMMEDIATE")
                    con.execute(f"PRAGMA user_version = {1000 + i}")
                    con.execute(f"UPDATE global_settings SET generation = {i}")
                    con.execute("COMMIT")
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)
            finally:
                con.close()
                stop.set()

        thread = threading.Thread(target=writer)
        thread.start()
        torn, reads, last = [], 0, 0
        try:
            while not stop.is_set() or reads < 20:
                state = read_store_state(seeded)
                reads += 1
                if state.store_id - 1000 != state.generation:
                    torn.append(state)
                assert state.generation >= last
                last = state.generation
        finally:
            thread.join(30)
        assert not errors, errors
        assert not thread.is_alive()
        assert torn == []
        assert reads >= 20

    def test_it_leaves_no_wal_read_mark(self, seeded, db_path, pending_writer):
        """After `read_store_state` returns, a pending writer commit can be
        checkpointed in full by a third connection: nothing of ours pins an
        older snapshot (that would let the WAL grow without bound under a
        polling supervisor)."""
        read_store_state(seeded)                 # leaves its pooled connection open
        assert _cached_engines()
        _commit_a_generation(pending_writer)
        _commit_a_generation(pending_writer)

        busy, log, checkpointed = _wal_checkpoint_passive(db_path)
        assert log > 0
        assert checkpointed == log, (busy, log, checkpointed)

    def test_it_leaves_no_wal_read_mark_after_repeated_polls_between_commits(
        self, seeded, db_path, pending_writer
    ):
        for _ in range(3):
            read_store_state(seeded)
            _commit_a_generation(pending_writer)
        busy, log, checkpointed = _wal_checkpoint_passive(db_path)
        assert log > 0 and checkpointed == log, (busy, log, checkpointed)

    def test_control_a_held_read_transaction_does_block_the_checkpoint(
        self, seeded, db_path, pending_writer
    ):
        """Proves the check above can fail: with a reader that opened its
        snapshot BEFORE the commit and has not finished, PASSIVE cannot
        checkpoint everything."""
        reader = sqlite3.connect(str(db_path), timeout=5, isolation_level=None)
        try:
            reader.execute("BEGIN")
            reader.execute("SELECT generation FROM global_settings").fetchall()
            _commit_a_generation(pending_writer)
            busy, log, checkpointed = _wal_checkpoint_passive(db_path)
            assert log > 0
            assert checkpointed < log, (busy, log, checkpointed)
            reader.execute("COMMIT")
            _, log2, checkpointed2 = _wal_checkpoint_passive(db_path)
            assert checkpointed2 == log2
        finally:
            reader.close()

    def test_a_failing_read_store_state_leaves_no_read_mark_either(
        self, resolved, db_path, pending_writer
    ):
        """The documented errors (here: no row) are raised after the
        transaction is closed."""
        pending_writer.execute("BEGIN IMMEDIATE")
        pending_writer.execute("DELETE FROM global_settings")
        pending_writer.execute("COMMIT")
        with pytest.raises(ValueError):
            read_store_state(resolved)
        pending_writer.execute("BEGIN IMMEDIATE")
        pending_writer.execute("CREATE TABLE junk (x)")
        pending_writer.execute("COMMIT")
        busy, log, checkpointed = _wal_checkpoint_passive(db_path)
        assert checkpointed == log, (busy, log, checkpointed)


# ---------------------------------------------------------------------------
# warn_never_prune, old constructors, public surface, error wording
# ---------------------------------------------------------------------------


def never_prune_cfg() -> BackupConfig:
    return BackupConfig(
        datasets=[DatasetConfig(name="tank/forever", frequency=Duration("1h"), retention_rules=[])]
    )


def never_prune_lines(caplog):
    return [
        r for r in caplog.records
        if r.levelno == logging.WARNING and "never be pruned" in r.getMessage()
    ]


@pytest.mark.integration
class TestReadConfigWarnNeverPrune:
    @pytest.fixture
    def forever(self, seeded):
        write_config(seeded, never_prune_cfg(), expected_generation=0)
        return seeded

    def test_the_default_still_logs_the_never_prune_line(self, forever, caplog):
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="zfsbackup.config.store.mapper"):
            snap = read_config(forever)
        assert snap.config.datasets[0].retention_rules == []
        lines = never_prune_lines(caplog)
        assert len(lines) == 1 and "tank/forever" in lines[0].getMessage()

    def test_warn_never_prune_false_logs_no_never_prune_line(self, forever, caplog):
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="zfsbackup.config.store.mapper"):
            snap = read_config(forever, warn_never_prune=False)
        assert snap.config.datasets[0].retention_rules == []   # the data is the same
        assert never_prune_lines(caplog) == []

    def test_warn_never_prune_is_keyword_only(self, forever):
        with pytest.raises(TypeError):
            read_config(forever, False)  # type: ignore[misc]

    def test_the_flag_changes_nothing_but_the_log(self, forever):
        assert read_config(forever, warn_never_prune=False) == read_config(forever)


@pytest.mark.unit
class TestStoreIdOldConstructors:
    def test_a_three_argument_config_snapshot_still_constructs(self):
        snap = ConfigSnapshot(cfg(), 3, EXPECTED_HEAD)
        assert (snap.generation, snap.schema_rev, snap.store_id) == (3, EXPECTED_HEAD, 0)

    def test_a_one_argument_write_result_still_constructs(self):
        result = WriteResult(1)
        assert (result.generation, result.warnings, result.store_id) == (1, (), 0)

    def test_an_import_result_without_a_store_id_still_constructs(self):
        result = ImportResult(True, EXPECTED_HEAD, 0, 0)
        assert result.store_id == 0 and result.generation is None

    def test_store_id_is_carried_when_given(self):
        assert ConfigSnapshot(cfg(), 0, "r", 5).store_id == 5
        assert WriteResult(1, (), 5).store_id == 5
        assert ImportResult(True, "r", 0, 0, store_id=5).store_id == 5


@pytest.mark.unit
class TestStoreIdPublicSurface:
    @pytest.mark.parametrize("name", ["StoreState", "read_store_state"])
    def test_the_new_names_are_exported(self, name):
        import zfsbackup.config.store as store

        assert name in store.__all__ and hasattr(store, name)
        assert name in store_access.__all__

    def test_the_package_docstring_lists_the_new_names(self):
        import zfsbackup.config.store as store

        assert "read_store_state" in store.__doc__ and "StoreState" in store.__doc__

    def test_the_package_names_are_the_access_objects(self):
        import zfsbackup.config.store as store

        assert store.StoreState is store_access.StoreState
        assert store.read_store_state is store_access.read_store_state

    def test_store_id_conflict_is_exported_from_the_package(self):
        import zfsbackup.config.store as store

        assert "StoreIdConflict" in store.__all__
        assert store.StoreIdConflict is StoreIdConflict is store_mapper.StoreIdConflict

    def test_store_id_conflict_is_a_config_conflict_and_nothing_else_the_cli_special_cases(self):
        exc = StoreIdConflict(3, 4)
        assert isinstance(exc, ConfigConflict)
        assert not isinstance(exc, (ConfigPathError, SchemaError, ConfigBusy))
        assert (exc.expected_store_id, exc.actual_store_id) == (3, 4)
        assert "replaced" in str(exc) and "nothing was written" in str(exc)
        assert "generation" not in str(exc)

    def test_store_id_conflict_expected_and_actual_are_the_generations_given(self):
        exc = StoreIdConflict(3, 4, 7, 9)
        assert (exc.expected, exc.actual) == (7, 9)
        assert (exc.expected_store_id, exc.actual_store_id) == (3, 4)
        none = StoreIdConflict(3, 4)
        assert (none.expected, none.actual) == (None, None)


@pytest.mark.unit
class TestStoreIdConflictWording:
    def test_config_error_message_passes_it_through_verbatim(self, tmp_path):
        named = resolve_config_path(str(tmp_path / "named.db"))
        exc = StoreIdConflict(3, 4)
        assert config_error_message(named, exc) == str(exc)
        assert config_error_message(None, exc) == str(exc)

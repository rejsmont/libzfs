"""Phase 2 item 10.1 -- the store half of the config CLI's `migrate` / `import`.

Covers, all in `zfsbackup/config/store/{migrate,importer,__init__}.py`:

- **B1** `EXPECTED_HEAD` / `KNOWN_REVISIONS`: `check_schema` decides from the
  two constants and reads no migration scripts; the write paths assert the
  scripts agree with them first (`_assert_scripts_match_constants`).
- **`upgrade_schema(resolved) -> SchemaUpgrade`**: atomic, FK-off connection,
  `BEGIN IMMEDIATE`, never creates a database, serialises concurrent runs.
- **B2** `PRAGMA foreign_key_check` before commit -> `SchemaIntegrityError`,
  on `upgrade_schema` and on `ensure_schema`'s migrating branch.
- **D-5 / 8c pre-import backup**: `import_yaml(..., backup=,
  allow_unreadable_outgoing=)`, `ImportResult.backup_path`,
  `OutgoingConfigUnreadable`.
- **A-11 note**: a skipped first-import cleanup attaches its WARNING text to the
  in-flight exception with `add_note`.
- the new names are exported from `zfsbackup.config.store`.

Synthetic revisions: the real history has one revision, so "behind", "success"
and "foreign-key failure" need a second one. `synthetic_revision` copies the
real `migrations/` tree into `tmp_path` and patches `_MIGRATIONS_DIR`,
`EXPECTED_HEAD` and `KNOWN_REVISIONS` TOGETHER (patching only `EXPECTED_HEAD`
trips the `KNOWN_REVISIONS` check, which is itself pinned below).

Everything is file-backed under `tmp_path` and goes through `store.db` (the
project's engine factory), so `foreign_keys=ON`, WAL and `BEGIN IMMEDIATE` are
the real ones. No test here uses `mock_subprocess`.

Mutation counts (scratch copy of the tree, one revert at a time; tests in this
file that then fail) are recorded in the item 10.1 hand-off report.
"""

import hashlib
import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.pool import StaticPool

from zfsbackup.config import BackupConfig
from zfsbackup.config.store import importer as importer_module
from zfsbackup.config.store import migrate as migrate_module
from zfsbackup.config.store.db import dispose_all
from zfsbackup.config.store.importer import (
    ImportResult,
    OutgoingConfigUnreadable,
    import_yaml,
)
from zfsbackup.config.store.mapper import load_config
from zfsbackup.config.store.migrate import (
    EXPECTED_HEAD,
    KNOWN_REVISIONS,
    SchemaError,
    SchemaIntegrityError,
    SchemaOutOfDate,
    SchemaSplitBrain,
    SchemaUpgrade,
    SchemaVersionMismatch,
    _assert_scripts_match_constants,
    check_schema,
    ensure_schema,
    upgrade_schema,
)
from zfsbackup.config.store.models import Base
from zfsbackup.config.store.paths import (
    ConfigDbNotADatabase,
    ConfigDbNotFound,
    ConfigPathError,
    open_config_connection,
    open_config_session,
    resolve_config_path,
)
from alembic.script import ScriptDirectory

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE_YAML = ROOT / "zfsbackup" / "config.example.yaml"

#: The id of the synthetic revision every test below stacks on the real head.
SYNTH = "bbbbbbbbbbbb"

NEW_YAML = "snapshot_prefix: other\ndatasets:\n  - name: tank/x\n    frequency: 1h\n"

REVISION_TEMPLATE = '''"""synthetic revision for tests"""
revision = "{rid}"
down_revision = "{down}"
branch_labels = None
depends_on = None

import time
from pathlib import Path

import sqlalchemy as sa
from alembic import op


def upgrade():
{body}


def downgrade():
    pass
'''

BODY_GOOD = '''    with op.batch_alter_table("datasets") as batch:
        batch.add_column(sa.Column("note", sa.Text(), nullable=True))'''

# A revision that "succeeds" but leaves an orphan row: with enforcement off
# nothing complains, and only `PRAGMA foreign_key_check` can see it.
BODY_ORPHAN = '''    op.execute("CREATE TABLE child_x (id INTEGER PRIMARY KEY, "
               "pid INTEGER REFERENCES datasets(id))")
    op.execute("INSERT INTO child_x (id, pid) VALUES (1, 99999)")'''


# ---------------------------------------------------------------------------
# helpers / fixtures
# ---------------------------------------------------------------------------


def dump(db: Path) -> list:
    """Every SQL statement of `db`, via a short-lived read-only connection."""
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return list(conn.iterdump())
    finally:
        conn.close()


def file_hashes(directory: Path) -> dict:
    return {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(directory.iterdir())
        if p.is_file()
    }


def sql(db: Path, statement: str):
    conn = sqlite3.connect(str(db))
    try:
        return conn.execute(statement).fetchall()
    finally:
        conn.close()


def write_revision(scripts: Path, filename: str, body: str, *, rid=SYNTH,
                   down=EXPECTED_HEAD) -> None:
    (scripts / "versions" / filename).write_text(
        REVISION_TEMPLATE.format(rid=rid, down=down, body=body)
    )


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
def imported(db_path, resolved):
    """A real database at the real head holding `config.example.yaml`, with no
    engine left open (so the file is quiescent and byte-comparable)."""
    import_yaml(EXAMPLE_YAML, resolved)
    dispose_all()
    return db_path


@pytest.fixture
def synthetic_scripts(tmp_path, monkeypatch):
    """Copy the real migrations tree to `tmp_path/scripts` and point
    `migrate` at it. Returns the directory; the caller adds revision files and
    then calls `activate(...)` -- or just uses `synthetic_revision`."""
    import shutil

    scripts = tmp_path / "scripts"
    shutil.copytree(
        migrate_module._MIGRATIONS_DIR, scripts,
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    monkeypatch.setattr(migrate_module, "_MIGRATIONS_DIR", scripts)
    return scripts


def activate(monkeypatch, head=SYNTH, known=(EXPECTED_HEAD, SYNTH)):
    """Patch `EXPECTED_HEAD` and `KNOWN_REVISIONS` together."""
    monkeypatch.setattr(migrate_module, "EXPECTED_HEAD", head)
    monkeypatch.setattr(migrate_module, "KNOWN_REVISIONS", frozenset(known))


@pytest.fixture
def synthetic_revision(synthetic_scripts, monkeypatch):
    """Factory: `synthetic_revision(BODY_GOOD)` stacks one revision on the real
    head and patches the three things that must move together."""

    def make(body, filename="synth.py"):
        write_revision(synthetic_scripts, filename, body)
        activate(monkeypatch)
        return synthetic_scripts

    return make


# ---------------------------------------------------------------------------
# B1: the constants
# ---------------------------------------------------------------------------


class TestExpectedHeadConstants:
    """Adding a migration without updating the constants must fail CI."""

    @staticmethod
    def _script():
        from alembic.config import Config

        cfg = Config()
        cfg.set_main_option("script_location", str(migrate_module._MIGRATIONS_DIR))
        return ScriptDirectory.from_config(cfg)

    def test_expected_head_equals_the_real_script_head(self):
        assert self._script().get_heads() == [EXPECTED_HEAD]

    def test_known_revisions_equals_the_real_revision_set(self):
        revisions = {rev.revision for rev in self._script().walk_revisions()}
        assert revisions == set(KNOWN_REVISIONS)

    def test_the_head_is_one_of_the_known_revisions(self):
        assert EXPECTED_HEAD in KNOWN_REVISIONS

    def test_known_revisions_is_immutable(self):
        assert isinstance(KNOWN_REVISIONS, frozenset)

    def test_the_real_tree_passes_the_script_assertion(self):
        _assert_scripts_match_constants()  # does not raise


# ---------------------------------------------------------------------------
# B1: check_schema reads no scripts
# ---------------------------------------------------------------------------


class TestCheckSchemaReadsNoScripts:
    """`check_schema` is the daemon's startup path: no `ScriptDirectory`, no
    filesystem walk of the migrations package."""

    @pytest.fixture
    def no_script_reads(self, monkeypatch):
        def explode(*_a, **_k):
            raise AssertionError("check_schema read the migration scripts")

        monkeypatch.setattr(ScriptDirectory, "from_config", explode)
        monkeypatch.setattr(ScriptDirectory, "__init__", explode)
        monkeypatch.setattr(migrate_module, "_MIGRATIONS_DIR", Path("/nonexistent/migrations"))

    def _check(self, db, resolved):
        dispose_all()
        with open_config_connection(resolved, readonly=True) as conn:
            return check_schema(conn)

    def test_a_current_database_passes_without_reading_scripts(
        self, imported, resolved, no_script_reads
    ):
        assert self._check(imported, resolved) == EXPECTED_HEAD

    def test_a_brand_new_database_is_out_of_date_without_reading_scripts(
        self, tmp_path, no_script_reads
    ):
        eng = create_engine("sqlite:///:memory:", poolclass=StaticPool,
                            connect_args={"check_same_thread": False})
        try:
            with eng.connect() as conn:
                with pytest.raises(SchemaOutOfDate):
                    check_schema(conn)
        finally:
            eng.dispose()

    def test_an_unknown_revision_is_a_mismatch_without_reading_scripts(
        self, imported, resolved, no_script_reads
    ):
        sql_conn = sqlite3.connect(str(imported))
        sql_conn.execute("UPDATE alembic_version SET version_num='deadbeefcafe'")
        sql_conn.commit()
        sql_conn.close()
        with pytest.raises(SchemaVersionMismatch) as excinfo:
            self._check(imported, resolved)
        assert "deadbeefcafe" in str(excinfo.value)
        assert EXPECTED_HEAD in str(excinfo.value)
        assert excinfo.value.__cause__ is None

    def test_split_brain_is_detected_without_reading_scripts(
        self, db_path, resolved, no_script_reads
    ):
        conn = sqlite3.connect(str(db_path))
        conn.close()
        eng = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(eng)
        eng.dispose()
        with pytest.raises(SchemaSplitBrain):
            self._check(db_path, resolved)

    def test_a_script_that_disagrees_with_the_constants_is_invisible_to_it(
        self, imported, resolved, synthetic_scripts
    ):
        """The constants are NOT checked against the scripts on this path: an
        extra script revision changes nothing for `check_schema`."""
        write_revision(synthetic_scripts, "stray.py", BODY_GOOD)  # head now SYNTH on disk
        assert self._check(imported, resolved) == EXPECTED_HEAD


class TestCheckSchemaKnownButBehind:
    """The case `TestMigrateCheckSchema`'s old docstring said needed a second
    revision: a database at a revision this package recognises but which is
    not the head."""

    def test_known_but_behind_raises_schema_out_of_date(
        self, imported, resolved, synthetic_revision
    ):
        synthetic_revision(BODY_GOOD)
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(SchemaOutOfDate) as excinfo:
                check_schema(conn)
        message = str(excinfo.value)
        # Names the recorded revision, not the "no recorded revision" branch.
        assert repr([EXPECTED_HEAD]) in message
        assert "brand-new" not in message
        assert SYNTH in message
        assert "zfsbackup-config migrate" in message

    def test_known_but_behind_is_not_a_version_mismatch(
        self, imported, resolved, synthetic_revision
    ):
        synthetic_revision(BODY_GOOD)
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(SchemaError) as excinfo:
                check_schema(conn)
        assert type(excinfo.value) is SchemaOutOfDate

    def test_patching_only_the_head_trips_the_known_revisions_check(
        self, imported, resolved, synthetic_revision, monkeypatch
    ):
        """Documents why the fixture patches all three together: with
        `EXPECTED_HEAD` alone moved, the upgrade path refuses with the
        packaging `RuntimeError` (the `KNOWN_REVISIONS` check), not a
        `SchemaError`."""
        synthetic_revision(BODY_GOOD)
        monkeypatch.setattr(
            migrate_module, "KNOWN_REVISIONS", frozenset({EXPECTED_HEAD})
        )
        with pytest.raises(RuntimeError, match="KNOWN_REVISIONS") as excinfo:
            upgrade_schema(resolved)
        assert not isinstance(excinfo.value, SchemaError)


# ---------------------------------------------------------------------------
# B1: _assert_scripts_match_constants
# ---------------------------------------------------------------------------


class TestAssertScriptsMatchConstants:
    def test_head_mismatch_raises_a_bare_runtime_error(self, monkeypatch):
        monkeypatch.setattr(migrate_module, "EXPECTED_HEAD", "cccccccccccc")
        with pytest.raises(RuntimeError) as excinfo:
            _assert_scripts_match_constants()
        assert type(excinfo.value) is RuntimeError
        assert "'cccccccccccc'" in str(excinfo.value)
        assert repr(EXPECTED_HEAD) in str(excinfo.value)
        assert "EXPECTED_HEAD" in str(excinfo.value)

    def test_revision_set_mismatch_raises_a_bare_runtime_error(self, monkeypatch):
        monkeypatch.setattr(
            migrate_module, "KNOWN_REVISIONS", frozenset({EXPECTED_HEAD, "zzzzzzzzzzzz"})
        )
        with pytest.raises(RuntimeError) as excinfo:
            _assert_scripts_match_constants()
        assert type(excinfo.value) is RuntimeError
        assert "KNOWN_REVISIONS" in str(excinfo.value)
        assert "zzzzzzzzzzzz" in str(excinfo.value)

    def test_a_revision_missing_from_the_constants_is_a_mismatch(self, monkeypatch):
        monkeypatch.setattr(migrate_module, "KNOWN_REVISIONS", frozenset())
        with pytest.raises(RuntimeError, match="KNOWN_REVISIONS"):
            _assert_scripts_match_constants()

    def test_multiple_heads_raise_a_bare_runtime_error(
        self, synthetic_scripts, monkeypatch
    ):
        write_revision(synthetic_scripts, "one.py", BODY_GOOD, rid="b1b1b1b1b1b1")
        write_revision(synthetic_scripts, "two.py", BODY_GOOD, rid="b2b2b2b2b2b2")
        with pytest.raises(RuntimeError) as excinfo:
            _assert_scripts_match_constants()
        assert type(excinfo.value) is RuntimeError
        assert "2 heads" in str(excinfo.value)

    @pytest.mark.parametrize("which", ["head", "revisions"])
    def test_upgrade_schema_raises_before_the_database_is_touched(
        self, imported, db_path, resolved, monkeypatch, which
    ):
        before_files = sorted(p.name for p in db_path.parent.iterdir())
        before_hashes = file_hashes(db_path.parent)
        if which == "head":
            monkeypatch.setattr(migrate_module, "EXPECTED_HEAD", "cccccccccccc")
        else:
            monkeypatch.setattr(
                migrate_module, "KNOWN_REVISIONS",
                frozenset({EXPECTED_HEAD, "zzzzzzzzzzzz"}),
            )
        with pytest.raises(RuntimeError) as excinfo:
            upgrade_schema(resolved)
        assert not isinstance(excinfo.value, SchemaError)

        dispose_all()
        # No connection was ever opened: no sidecar appeared, no byte changed.
        assert sorted(p.name for p in db_path.parent.iterdir()) == before_files
        assert file_hashes(db_path.parent) == before_hashes

    def test_upgrade_schema_checks_the_scripts_before_the_path(self, tmp_path, monkeypatch):
        """Ordered first: a packaging defect is reported even when the target
        is missing, rather than as a `ConfigDbNotFound`."""
        monkeypatch.setattr(migrate_module, "EXPECTED_HEAD", "cccccccccccc")
        with pytest.raises(RuntimeError) as excinfo:
            upgrade_schema(resolve_config_path(str(tmp_path / "missing.db")))
        assert not isinstance(excinfo.value, ConfigPathError)

    def test_ensure_schema_checks_before_writing_anything(self, monkeypatch):
        monkeypatch.setattr(migrate_module, "EXPECTED_HEAD", "cccccccccccc")
        eng = create_engine("sqlite:///:memory:", poolclass=StaticPool,
                            connect_args={"check_same_thread": False})
        try:
            with eng.connect() as conn:
                with pytest.raises(RuntimeError, match="EXPECTED_HEAD"):
                    ensure_schema(conn)
                assert inspect(conn).get_table_names() == []
        finally:
            eng.dispose()

    def test_ensure_schema_on_a_current_database_never_reads_the_scripts(
        self, imported, resolved, monkeypatch
    ):
        """The already-at-head path is `check_schema` alone: a drifted constant
        pair is a write-path concern, not a reason to refuse a current DB."""
        monkeypatch.setattr(migrate_module, "KNOWN_REVISIONS", frozenset({EXPECTED_HEAD, "zzzzzzzzzzzz"}))
        with open_config_connection(resolved, readonly=True) as conn:
            assert ensure_schema(conn) == EXPECTED_HEAD


# ---------------------------------------------------------------------------
# upgrade_schema
# ---------------------------------------------------------------------------


class TestUpgradeSchema:
    def test_a_database_at_head_reports_no_change_and_writes_nothing(
        self, imported, db_path, resolved
    ):
        before = file_hashes(db_path.parent)
        result = upgrade_schema(resolved)
        dispose_all()

        assert result == SchemaUpgrade(
            from_revs=(EXPECTED_HEAD,), to_rev=EXPECTED_HEAD, changed=False
        )
        assert file_hashes(db_path.parent) == before

    def test_the_result_is_a_frozen_value(self, imported, resolved):
        result = upgrade_schema(resolved)
        with pytest.raises(Exception):
            result.changed = True  # type: ignore[misc]

    def test_a_good_synthetic_revision_upgrades_in_place(
        self, imported, db_path, resolved, synthetic_revision
    ):
        synthetic_revision(BODY_GOOD)
        generation = sql(db_path, "SELECT generation FROM global_settings")
        datasets = sql(db_path, "SELECT name FROM datasets ORDER BY id")

        result = upgrade_schema(resolved)
        dispose_all()

        assert result == SchemaUpgrade(
            from_revs=(EXPECTED_HEAD,), to_rev=SYNTH, changed=True
        )
        assert sql(db_path, "SELECT version_num FROM alembic_version") == [(SYNTH,)]
        columns = [r[1] for r in sql(db_path, "PRAGMA table_info(datasets)")]
        assert "note" in columns
        # The stored configuration is kept.
        assert sql(db_path, "SELECT generation FROM global_settings") == generation
        assert sql(db_path, "SELECT name FROM datasets ORDER BY id") == datasets
        assert sql(db_path, "PRAGMA foreign_key_check") == []

    def test_the_second_run_is_idempotent(
        self, imported, db_path, resolved, synthetic_revision
    ):
        synthetic_revision(BODY_GOOD)
        first = upgrade_schema(resolved)
        dispose_all()
        before = file_hashes(db_path.parent)

        second = upgrade_schema(resolved)
        dispose_all()

        assert first.changed is True
        assert second == SchemaUpgrade(
            from_revs=(SYNTH,), to_rev=SYNTH, changed=False
        )
        assert file_hashes(db_path.parent) == before

    def test_a_schema_less_but_valid_sqlite_file_is_migrated(self, db_path, resolved):
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE _t(x)")
        conn.execute("DROP TABLE _t")
        conn.commit()
        conn.close()

        result = upgrade_schema(resolved)
        dispose_all()

        assert result == SchemaUpgrade(from_revs=(), to_rev=EXPECTED_HEAD, changed=True)
        assert sql(db_path, "SELECT version_num FROM alembic_version") == [(EXPECTED_HEAD,)]

    def test_the_connection_is_closed_before_it_returns(
        self, imported, resolved, synthetic_revision, monkeypatch
    ):
        synthetic_revision(BODY_GOOD)
        seen = []
        real = migrate_module.open_config_connection

        @contextmanager
        def spy(*args, **kwargs):
            seen.append(kwargs)
            with real(*args, **kwargs) as conn:
                seen.append(conn)
                yield conn

        monkeypatch.setattr(migrate_module, "open_config_connection", spy)
        upgrade_schema(resolved)

        kwargs, conn = seen
        assert kwargs == {"readonly": False, "foreign_keys": False}
        assert conn.closed
        assert not conn.in_transaction()

    def test_the_first_statement_is_begin_immediate(
        self, imported, resolved, monkeypatch
    ):
        """The write lock is taken before anything reads `alembic_version`, so
        two `migrate` runs serialise instead of racing."""
        from sqlalchemy import event
        from sqlalchemy.engine import Engine

        statements = []

        @event.listens_for(Engine, "before_cursor_execute")
        def record(conn, cursor, statement, *a):
            statements.append(statement.strip())

        try:
            upgrade_schema(resolved)
        finally:
            event.remove(Engine, "before_cursor_execute", record)

        # Skip the pragma statements of the connect-time listener.
        body = [s for s in statements if not s.upper().startswith("PRAGMA")]
        assert body[0].upper() == "BEGIN IMMEDIATE"
        reads = [i for i, s in enumerate(body) if "alembic_version" in s]
        assert reads and reads[0] > 0

    @pytest.mark.parametrize("path_kind", ["missing", "zero_byte"])
    def test_a_missing_or_zero_byte_database_is_refused_and_nothing_is_created(
        self, store_dir, path_kind
    ):
        target = store_dir / "config.db"
        if path_kind == "zero_byte":
            target.touch()
            expected = ConfigDbNotADatabase
        else:
            expected = ConfigDbNotFound
        before = sorted(p.name for p in store_dir.iterdir())

        with pytest.raises(expected):
            upgrade_schema(resolve_config_path(str(target)))
        dispose_all()

        assert sorted(p.name for p in store_dir.iterdir()) == before
        if path_kind == "zero_byte":
            assert target.stat().st_size == 0

    def test_a_missing_database_is_not_created_even_with_the_directory_present(
        self, store_dir):
        with pytest.raises(ConfigDbNotFound):
            upgrade_schema(resolve_config_path(str(store_dir / "config.db")))
        assert list(store_dir.iterdir()) == []

    def test_an_unrecognised_revision_propagates_and_changes_nothing(
        self, imported, db_path, resolved
    ):
        conn = sqlite3.connect(str(db_path))
        conn.execute("UPDATE alembic_version SET version_num='deadbeefcafe'")
        conn.commit()
        conn.close()
        before = dump(db_path)

        with pytest.raises(SchemaVersionMismatch):
            upgrade_schema(resolved)
        dispose_all()

        assert dump(db_path) == before

    def test_split_brain_propagates_and_changes_nothing(
        self, db_path, resolved
    ):
        eng = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(eng)
        eng.dispose()
        before = dump(db_path)

        with pytest.raises(SchemaSplitBrain):
            upgrade_schema(resolved)
        dispose_all()

        assert dump(db_path) == before

    def test_a_migration_that_did_not_land_is_not_reported_as_success(
        self, imported, db_path, resolved, synthetic_revision, monkeypatch
    ):
        """`ensure_schema` re-derives the verdict with `check_schema` after
        `command.upgrade` instead of trusting that the upgrade worked: an
        upgrade that silently did nothing must surface as `SchemaOutOfDate`
        (and `upgrade_schema` must then roll back, not report success)."""
        synthetic_revision(BODY_GOOD)
        monkeypatch.setattr(migrate_module.command, "upgrade", lambda cfg, rev: None)
        before = dump(db_path)

        with pytest.raises(SchemaOutOfDate):
            upgrade_schema(resolved)
        dispose_all()

        assert dump(db_path) == before

    def test_a_failing_migration_script_rolls_back_the_whole_attempt(
        self, imported, db_path, resolved, synthetic_revision
    ):
        synthetic_revision(
            '    op.execute("CREATE TABLE half_done (id INTEGER)")\n'
            '    raise RuntimeError("migration blew up")'
        )
        before = dump(db_path)

        with pytest.raises(RuntimeError, match="migration blew up"):
            upgrade_schema(resolved)
        dispose_all()

        assert dump(db_path) == before
        assert sql(db_path, "SELECT name FROM sqlite_master WHERE name='half_done'") == []


class TestUpgradeSchemaForeignKeyCheck:
    """B2: a migration that leaves an orphan row must not commit."""

    def test_foreign_key_failure_raises_and_leaves_the_database_unchanged(
        self, imported, db_path, resolved, synthetic_revision
    ):
        synthetic_revision(BODY_ORPHAN)
        before = dump(db_path)
        generation = sql(db_path, "SELECT generation FROM global_settings")
        datasets = sql(db_path, "SELECT count(*) FROM datasets")

        with pytest.raises(SchemaIntegrityError) as excinfo:
            upgrade_schema(resolved)
        dispose_all()

        message = str(excinfo.value)
        assert "child_x" in message
        assert "unchanged" in message
        assert isinstance(excinfo.value, SchemaError)

        assert sql(db_path, "SELECT version_num FROM alembic_version") == [(EXPECTED_HEAD,)]
        assert sql(db_path, "SELECT name FROM sqlite_master WHERE name='child_x'") == []
        assert sql(db_path, "SELECT generation FROM global_settings") == generation
        assert sql(db_path, "SELECT count(*) FROM datasets") == datasets
        assert dump(db_path) == before

    def test_the_check_is_what_catches_it_not_the_connection(
        self, imported, db_path, resolved, synthetic_revision
    ):
        """Control: the same orphan really is committed by a bare Alembic
        upgrade on an FK-off connection -- nothing else would object. (Without
        this, the test above could pass for the wrong reason.)"""
        from alembic import command

        synthetic_revision(BODY_ORPHAN)
        with open_config_connection(resolved, foreign_keys=False) as conn:
            trans = conn.begin()
            command.upgrade(migrate_module._build_config(conn), "head")
            trans.commit()
        dispose_all()

        assert sql(db_path, "SELECT version_num FROM alembic_version") == [(SYNTH,)]
        assert sql(db_path, "PRAGMA foreign_key_check") != []

    def test_more_than_five_violations_are_summarised(
        self, imported, db_path, resolved, synthetic_revision
    ):
        body = (
            '    op.execute("CREATE TABLE child_x (id INTEGER PRIMARY KEY, '
            'pid INTEGER REFERENCES datasets(id))")\n'
            '    for i in range(8):\n'
            '        op.execute(f"INSERT INTO child_x (id, pid) VALUES ({i + 1}, 90000)")'
        )
        synthetic_revision(body)
        with pytest.raises(SchemaIntegrityError) as excinfo:
            upgrade_schema(resolved)
        assert "8 foreign-key violation" in str(excinfo.value)
        assert "and 3 more" in str(excinfo.value)

    def test_ensure_schema_runs_the_check_on_its_migrating_branch(
        self, imported, resolved, synthetic_revision
    ):
        synthetic_revision(BODY_ORPHAN)
        with open_config_connection(resolved, foreign_keys=False) as conn:
            trans = conn.begin()
            try:
                with pytest.raises(SchemaIntegrityError):
                    ensure_schema(conn)
            finally:
                trans.rollback()

    def test_ensure_schema_on_a_clean_migration_does_not_raise(
        self, imported, resolved, synthetic_revision
    ):
        synthetic_revision(BODY_GOOD)
        with open_config_connection(resolved, foreign_keys=False) as conn:
            trans = conn.begin()
            try:
                assert ensure_schema(conn) == SYNTH
            finally:
                trans.rollback()

    def test_import_over_an_existing_database_fails_and_leaves_it_alone(
        self, imported, db_path, resolved, synthetic_revision, tmp_path
    ):
        """`import_yaml`'s phase 2 goes through `ensure_schema`, so a bad
        migration is rejected there too, before phase 3 replaces anything."""
        synthetic_revision(BODY_ORPHAN)
        yaml_path = tmp_path / "new.yaml"
        yaml_path.write_text(NEW_YAML)
        before = dump(db_path)

        with pytest.raises(SchemaIntegrityError):
            import_yaml(yaml_path, resolved)
        dispose_all()

        assert dump(db_path) == before
        assert sql(db_path, "SELECT version_num FROM alembic_version") == [(EXPECTED_HEAD,)]
        assert sql(db_path, "SELECT name FROM sqlite_master WHERE name='child_x'") == []

    def test_a_first_import_that_fails_the_check_leaves_no_file(
        self, store_dir, db_path, resolved, tmp_path, synthetic_scripts, monkeypatch
    ):
        """A brand-new database goes through the same migrating branch; with a
        revision whose orphan appears on the way to head, the created file is
        cleaned up (nothing was committed)."""
        write_revision(
            synthetic_scripts, "orphan_on_fresh.py",
            '    op.execute("CREATE TABLE child_x (id INTEGER PRIMARY KEY, '
            'pid INTEGER REFERENCES datasets(id))")\n'
            '    op.execute("INSERT INTO child_x (id, pid) VALUES (1, 99999)")',
        )
        activate(monkeypatch)
        with pytest.raises(SchemaIntegrityError):
            import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        assert not db_path.exists()


# ---------------------------------------------------------------------------
# upgrade_schema: concurrent serialisation (separate OS processes)
# ---------------------------------------------------------------------------

CHILD = textwrap.dedent(
    '''
    import json, sys, time
    from pathlib import Path

    sys.path.insert(0, {root!r})
    import zfsbackup.config.store.migrate as M
    M._MIGRATIONS_DIR = Path({scripts!r})
    M.EXPECTED_HEAD = {synth!r}
    M.KNOWN_REVISIONS = frozenset({{{head!r}, {synth!r}}})
    from zfsbackup.config.store import resolve_config_path, upgrade_schema

    name, db, workdir = sys.argv[1], sys.argv[2], Path(sys.argv[3])
    resolved = resolve_config_path(db)
    (workdir / (name + ".ready")).write_text("x")
    try:
        result = upgrade_schema(resolved)
        out = {{"changed": result.changed, "from": list(result.from_revs), "to": result.to_rev}}
    except BaseException as exc:
        out = {{"error": type(exc).__name__ + ": " + str(exc)}}
    (workdir / (name + ".json")).write_text(json.dumps(out))
    '''
)

SLOW_BODY = '''    op.execute("CREATE TABLE slow_t (id INTEGER PRIMARY KEY)")
    workdir = Path(WORKDIR)
    (workdir / "migrating").write_text("x")
    deadline = time.time() + 30
    while not (workdir / "go").exists() and time.time() < deadline:
        time.sleep(0.05)'''


def _wait_for(path, timeout, proc=None):
    """Poll for `path`; give up early only if the process that is supposed to
    produce it (`proc`) has already exited without doing so."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists():
            return True
        if proc is not None and proc.poll() is not None and not path.exists():
            return False
        time.sleep(0.02)
    return path.exists()


@pytest.mark.integration
@pytest.mark.skipif(sys.platform == "win32", reason="POSIX process groups")
class TestUpgradeSchemaConcurrency:
    """Two `migrate` processes: the first holds the write lock inside a slow
    migration; the second must WAIT (not race it into `table already exists`),
    then see the new head and report `changed=False`. A regression here tends
    to hang or deadlock, so everything is deadline-bounded and killed."""

    def test_two_processes_serialise_one_changed_one_not(
        self, imported, db_path, tmp_path, synthetic_scripts
    ):
        workdir = tmp_path / "work"
        workdir.mkdir()
        body = SLOW_BODY.replace("WORKDIR", repr(str(workdir)))
        write_revision(synthetic_scripts, "slow.py", body)
        script = tmp_path / "child.py"
        script.write_text(
            CHILD.format(
                root=str(ROOT), scripts=str(synthetic_scripts),
                synth=SYNTH, head=EXPECTED_HEAD,
            )
        )
        env = dict(os.environ, PYTHONPATH=str(ROOT))

        def spawn(name):
            return subprocess.Popen(
                [sys.executable, str(script), name, str(db_path), str(workdir)],
                cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, start_new_session=True,
            )

        procs = []
        try:
            a = spawn("a")
            procs.append(a)
            # A is inside the migration, holding BEGIN IMMEDIATE.
            assert _wait_for(workdir / "migrating", 60, a), "A never reached the migration"

            b = spawn("b")
            procs.append(b)
            assert _wait_for(workdir / "b.ready", 60, b), "B never started"
            time.sleep(1.0)  # B is now blocked on the write lock (busy_timeout is 5 s)
            assert b.poll() is None, "B finished while A still held the lock"
            assert not (workdir / "b.json").exists()

            (workdir / "go").write_text("x")
            assert _wait_for(workdir / "a.json", 60, a), "A never finished"
            assert _wait_for(workdir / "b.json", 60, b), "B never finished"
            for p in procs:
                p.wait(timeout=30)
        finally:
            for p in procs:
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
                p.communicate()

        result_a = json.loads((workdir / "a.json").read_text())
        result_b = json.loads((workdir / "b.json").read_text())
        assert result_a == {"changed": True, "from": [EXPECTED_HEAD], "to": SYNTH}, result_a
        assert result_b == {"changed": False, "from": [SYNTH], "to": SYNTH}, result_b
        assert sql(db_path, "SELECT version_num FROM alembic_version") == [(SYNTH,)]
        assert sql(db_path, "SELECT count(*) FROM sqlite_master WHERE name='slow_t'") == [(1,)]
        assert sql(db_path, "PRAGMA integrity_check") == [("ok",)]


# ---------------------------------------------------------------------------
# import_yaml: the pre-import backup
# ---------------------------------------------------------------------------


@pytest.fixture
def new_yaml(tmp_path):
    path = tmp_path / "new.yaml"
    path.write_text(NEW_YAML)
    return path


class Recorder:
    """A `backup=` callback that records what it was handed."""

    def __init__(self, path):
        self.path = path
        self.configs = []

    def __call__(self, config):
        self.configs.append(config)
        return self.path


def state(db: Path):
    """(generation, prefix, dataset names) of the stored config."""
    return (
        sql(db, "SELECT generation, snapshot_prefix FROM global_settings"),
        sql(db, "SELECT name FROM datasets ORDER BY name"),
    )


class TestImportBackup:
    def test_a_fresh_database_gets_no_callback_and_no_backup_path(
        self, resolved, tmp_path
    ):
        rec = Recorder(tmp_path / "bk.yaml")
        result = import_yaml(EXAMPLE_YAML, resolved, backup=rec)

        assert result.created is True
        assert result.backup_path is None
        assert rec.configs == []
        assert isinstance(result, ImportResult)

    def test_a_schema_only_database_gets_no_callback_and_no_backup_path(
        self, db_path, resolved, new_yaml, tmp_path
    ):
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE _t(x)")
        conn.execute("DROP TABLE _t")
        conn.commit()
        conn.close()
        upgrade_schema(resolved)
        dispose_all()
        assert sql(db_path, "SELECT count(*) FROM global_settings") == [(0,)]

        rec = Recorder(tmp_path / "bk.yaml")
        result = import_yaml(new_yaml, resolved, backup=rec)

        assert result.created is False
        assert result.backup_path is None
        assert rec.configs == []
        assert state(db_path)[1] == [("tank/x",)]

    def test_a_reimport_hands_the_outgoing_config_to_the_callback(
        self, imported, db_path, resolved, new_yaml, tmp_path
    ):
        rec = Recorder(tmp_path / "bk.yaml")
        outgoing = BackupConfig.from_file(EXAMPLE_YAML)

        result = import_yaml(new_yaml, resolved, backup=rec)

        assert len(rec.configs) == 1
        assert isinstance(rec.configs[0], BackupConfig)
        assert rec.configs[0] == outgoing
        assert result.backup_path == tmp_path / "bk.yaml"
        assert result.created is False
        assert result.replaced_datasets == len(outgoing.datasets)
        # ... and the new config really replaced it.
        assert state(db_path)[1] == [("tank/x",)]
        with open_config_session(resolved, readonly=True) as session:
            assert load_config(session).snapshot_prefix == "other"

    def test_reimporting_the_same_file_over_itself_with_a_backup_works(
        self, imported, db_path, resolved, tmp_path
    ):
        """The outgoing config is loaded on the very session that then
        `save_config`s: re-inserting rows with the same primary keys must not
        collide with the objects `load_config` left in the identity map."""
        rec = Recorder(tmp_path / "bk.yaml")
        outgoing = BackupConfig.from_file(EXAMPLE_YAML)

        result = import_yaml(EXAMPLE_YAML, resolved, backup=rec)

        assert result.backup_path == tmp_path / "bk.yaml"
        assert rec.configs == [outgoing]
        with open_config_session(resolved, readonly=True) as session:
            assert load_config(session) == outgoing

    def test_the_callback_sees_the_outgoing_prefix_not_the_incoming_one(
        self, imported, resolved, new_yaml, tmp_path
    ):
        rec = Recorder(tmp_path / "bk.yaml")
        import_yaml(new_yaml, resolved, backup=rec)
        assert rec.configs[0].snapshot_prefix == BackupConfig.from_file(EXAMPLE_YAML).snapshot_prefix
        assert rec.configs[0].snapshot_prefix != "other"

    def test_the_path_the_callback_returns_is_passed_through_unchanged(
        self, imported, resolved, new_yaml, tmp_path
    ):
        sentinel = tmp_path / "somewhere" / "else.yaml"
        result = import_yaml(new_yaml, resolved, backup=lambda cfg: sentinel)
        assert result.backup_path is sentinel

    def test_backup_none_skips_the_step_and_never_loads_the_outgoing_config(
        self, imported, db_path, resolved, new_yaml, monkeypatch
    ):
        def explode(*_a, **_k):
            raise AssertionError("the outgoing config was loaded with no backup requested")

        monkeypatch.setattr(importer_module, "load_config", explode)
        result = import_yaml(new_yaml, resolved)  # backup=None

        assert result.backup_path is None
        assert state(db_path)[1] == [("tank/x",)]

    def test_a_raising_callback_leaves_the_database_unchanged(
        self, imported, db_path, resolved, new_yaml
    ):
        before_state = state(db_path)
        before_dump = dump(db_path)

        def boom(config):
            raise OSError("disk full")

        with pytest.raises(OSError, match="disk full"):
            import_yaml(new_yaml, resolved, backup=boom)
        dispose_all()

        assert state(db_path) == before_state  # generation and prefix unchanged
        assert dump(db_path) == before_dump

    def test_the_callback_runs_inside_the_write_transaction(
        self, imported, db_path, resolved, new_yaml, tmp_path
    ):
        """The config backed up is exactly the config replaced: while the
        callback runs, the write lock is held, so no other writer can commit in
        between."""
        observed = {}

        def callback(config):
            other = sqlite3.connect(str(db_path), timeout=0)
            try:
                other.execute("BEGIN IMMEDIATE")
                observed["lock"] = "acquired"
                other.rollback()
            except sqlite3.OperationalError as exc:
                observed["lock"] = str(exc)
            finally:
                other.close()
            return tmp_path / "bk.yaml"

        import_yaml(new_yaml, resolved, backup=callback)

        assert "locked" in observed["lock"]

    def test_a_failure_after_a_successful_backup_names_the_file_and_changes_nothing(
        self, imported, db_path, resolved, new_yaml, tmp_path, monkeypatch
    ):
        before_state = state(db_path)
        before_dump = dump(db_path)
        backup_file = tmp_path / "bk.yaml"
        real_save = importer_module.save_config

        def late_failure(session, config):
            real_save(session, config)
            raise RuntimeError("late failure")

        monkeypatch.setattr(importer_module, "save_config", late_failure)
        with pytest.raises(RuntimeError, match="late failure") as excinfo:
            import_yaml(new_yaml, resolved, backup=lambda cfg: backup_file)
        dispose_all()

        notes = getattr(excinfo.value, "__notes__", [])
        assert len(notes) == 1
        assert str(backup_file) in notes[0]
        assert "was not replaced" in notes[0]
        assert state(db_path) == before_state
        assert dump(db_path) == before_dump

    def test_a_failure_with_no_backup_written_adds_no_backup_note(
        self, imported, resolved, new_yaml, monkeypatch
    ):
        def failing_save(session, config):
            raise RuntimeError("late failure")

        monkeypatch.setattr(importer_module, "save_config", failing_save)
        with pytest.raises(RuntimeError) as excinfo:
            import_yaml(new_yaml, resolved)  # backup=None

        assert getattr(excinfo.value, "__notes__", []) == []

    def test_a_failing_callback_adds_no_backup_note(
        self, imported, resolved, new_yaml
    ):
        def boom(config):
            raise OSError("disk full")

        with pytest.raises(OSError) as excinfo:
            import_yaml(new_yaml, resolved, backup=boom)
        assert getattr(excinfo.value, "__notes__", []) == []

    # -- unreadable outgoing --------------------------------------------

    @pytest.fixture
    def zero_dataset_db(self, imported, db_path):
        """A stored configuration `load_config` refuses: no datasets."""
        conn = sqlite3.connect(str(db_path))
        conn.execute("DELETE FROM datasets")
        conn.commit()
        conn.close()
        return db_path

    def test_an_unreadable_outgoing_config_is_refused_with_the_cause_attached(
        self, zero_dataset_db, resolved, new_yaml, tmp_path
    ):
        before_state = state(zero_dataset_db)
        before_dump = dump(zero_dataset_db)
        rec = Recorder(tmp_path / "bk.yaml")

        with pytest.raises(OutgoingConfigUnreadable) as excinfo:
            import_yaml(new_yaml, resolved, backup=rec)
        dispose_all()

        assert isinstance(excinfo.value, ConfigPathError)
        assert excinfo.value.__cause__ is not None
        assert type(excinfo.value.__cause__).__name__ in str(excinfo.value)
        assert "Nothing was changed" in str(excinfo.value)
        assert rec.configs == []  # callback not called
        assert state(zero_dataset_db) == before_state
        assert dump(zero_dataset_db) == before_dump

    def test_allow_unreadable_outgoing_warns_imports_and_records_no_backup(
        self, zero_dataset_db, resolved, new_yaml, tmp_path, caplog
    ):
        rec = Recorder(tmp_path / "bk.yaml")

        with caplog.at_level(logging.WARNING):
            result = import_yaml(
                new_yaml, resolved, backup=rec, allow_unreadable_outgoing=True
            )

        assert result.backup_path is None
        assert rec.configs == []
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert any("WITHOUT a backup" in m for m in warnings), warnings
        assert state(zero_dataset_db)[1] == [("tank/x",)]

    def test_allow_unreadable_outgoing_does_not_skip_a_readable_backup(
        self, imported, resolved, new_yaml, tmp_path
    ):
        rec = Recorder(tmp_path / "bk.yaml")
        result = import_yaml(
            new_yaml, resolved, backup=rec, allow_unreadable_outgoing=True
        )
        assert len(rec.configs) == 1
        assert result.backup_path == tmp_path / "bk.yaml"

    def test_the_unreadable_flag_means_nothing_without_a_backup(
        self, zero_dataset_db, resolved, new_yaml
    ):
        # backup=None never reads the outgoing config, readable or not.
        result = import_yaml(new_yaml, resolved)
        assert result.backup_path is None

    def test_an_unreadable_outgoing_config_on_a_failed_reimport_leaves_the_file(
        self, zero_dataset_db, resolved, new_yaml, tmp_path
    ):
        # created=False: cleanup must not run, the database stays.
        with pytest.raises(OutgoingConfigUnreadable):
            import_yaml(new_yaml, resolved, backup=Recorder(tmp_path / "bk.yaml"))
        assert zero_dataset_db.exists()


# ---------------------------------------------------------------------------
# A-11: the cleanup note
# ---------------------------------------------------------------------------


def cleanup_warnings(caplog):
    return [
        r.getMessage() for r in caplog.records
        if r.levelno == logging.WARNING and "Failed first import" in r.getMessage()
    ]


@pytest.fixture
def failing_first_import(store_dir, db_path, resolved, monkeypatch):
    """Run a first import whose phase 3 fails; `during` (optional) runs inside
    the failing `save_config`, i.e. after the file was created and migrated."""

    def run(during=None, exc=RuntimeError("late failure")):
        def failing_save(session, config):
            if during is not None:
                during()
            raise exc

        monkeypatch.setattr(importer_module, "save_config", failing_save)
        with pytest.raises(type(exc)) as excinfo:
            import_yaml(EXAMPLE_YAML, resolved)
        return excinfo.value

    return run


class TestCleanupNote:
    def test_a_skipped_cleanup_attaches_the_logged_warning_text(
        self, db_path, failing_first_import, caplog
    ):
        held = []

        def attach_reader():
            conn = sqlite3.connect(str(db_path))
            conn.execute("SELECT count(*) FROM alembic_version").fetchall()
            held.append(conn)

        try:
            with caplog.at_level(logging.WARNING):
                error = failing_first_import(during=attach_reader)
            logged = cleanup_warnings(caplog)
            notes = getattr(error, "__notes__", [])

            assert db_path.exists()  # cleanup was indeed skipped
            assert len(logged) == 1
            assert notes == logged  # the very same text
            assert "NOT removed" in notes[0]
            assert str(db_path) in notes[0]
            assert str(error) == "late failure"  # original not masked
        finally:
            for conn in held:
                conn.close()

    def test_a_clean_cleanup_adds_no_note(self, db_path, failing_first_import, caplog):
        with caplog.at_level(logging.WARNING):
            error = failing_first_import()

        assert not db_path.exists()  # removed
        assert getattr(error, "__notes__", []) == []
        assert cleanup_warnings(caplog) == []

    def test_a_dispose_failure_attaches_the_could_not_close_note(
        self, db_path, failing_first_import, monkeypatch, caplog
    ):
        def wedged():
            raise RuntimeError("pool is wedged")

        monkeypatch.setattr(importer_module, "dispose_all", wedged)
        with caplog.at_level(logging.WARNING):
            error = failing_first_import()
        monkeypatch.undo()

        notes = getattr(error, "__notes__", [])
        assert db_path.exists()
        assert len(notes) == 1
        assert "could not close this process's database connections" in notes[0]
        assert notes == cleanup_warnings(caplog)

    def test_an_unexpected_cleanup_error_attaches_its_own_note(
        self, db_path, failing_first_import, monkeypatch, caplog
    ):
        def broken(target, created_id):
            raise ValueError("kaboom")

        monkeypatch.setattr(importer_module, "_remove_if_unused", broken)
        with caplog.at_level(logging.WARNING):
            error = failing_first_import()

        notes = getattr(error, "__notes__", [])
        assert len(notes) == 1
        assert f"cleanup of {db_path} failed unexpectedly" in notes[0]
        assert "ValueError('kaboom')" in notes[0]
        assert "may still exist" in notes[0]
        assert "-wal/-shm" in notes[0]
        # It is logged (with traceback) too, and never masks the real error.
        assert "unexpected error while cleaning up" in caplog.text
        assert str(error) == "late failure"

    def test_the_helper_works_without_an_error_to_annotate(
        self, db_path, caplog
    ):
        """`error` is optional: a skip still logs and nothing is attached."""
        db_path.touch()
        with caplog.at_level(logging.WARNING):
            # A fingerprint that is not the file's: "not the file this import
            # created" -> skipped, one WARNING, nothing to annotate.
            importer_module._remove_created_target(db_path, (-1, -1))
        assert db_path.exists()
        assert len(cleanup_warnings(caplog)) == 1

    def test_the_helper_returns_none_and_never_raises_on_the_clean_path(
        self, store_dir, db_path
    ):
        db_path.touch()
        created_id = importer_module._file_id(db_path)
        error = RuntimeError("original")
        assert importer_module._remove_created_target(db_path, created_id, error) is None
        assert not db_path.exists()
        assert getattr(error, "__notes__", []) == []

    def test_a_reimport_failure_gets_no_cleanup_note(
        self, imported, resolved, new_yaml, monkeypatch, caplog
    ):
        def failing_save(session, config):
            raise RuntimeError("late failure")

        monkeypatch.setattr(importer_module, "save_config", failing_save)
        with caplog.at_level(logging.WARNING):
            with pytest.raises(RuntimeError) as excinfo:
                import_yaml(new_yaml, resolved)

        assert getattr(excinfo.value, "__notes__", []) == []
        assert cleanup_warnings(caplog) == []
        assert imported.exists()


# ---------------------------------------------------------------------------
# exports
# ---------------------------------------------------------------------------


NEW_NAMES = [
    "EXPECTED_HEAD",
    "KNOWN_REVISIONS",
    "SchemaIntegrityError",
    "SchemaUpgrade",
    "upgrade_schema",
    "OutgoingConfigUnreadable",
]


class TestItem10Exports:
    @pytest.mark.parametrize("name", NEW_NAMES)
    def test_the_name_is_in_all_and_importable(self, name):
        import zfsbackup.config.store as store_pkg

        assert name in store_pkg.__all__
        assert getattr(store_pkg, name) is not None
        namespace: dict = {}
        exec(f"from zfsbackup.config.store import {name}", namespace)  # noqa: S102
        assert namespace[name] is getattr(store_pkg, name)

    def test_the_names_are_the_module_level_objects(self):
        import zfsbackup.config.store as store_pkg

        assert store_pkg.EXPECTED_HEAD is migrate_module.EXPECTED_HEAD
        assert store_pkg.KNOWN_REVISIONS is migrate_module.KNOWN_REVISIONS
        assert store_pkg.upgrade_schema is upgrade_schema
        assert store_pkg.SchemaUpgrade is SchemaUpgrade
        assert store_pkg.SchemaIntegrityError is SchemaIntegrityError
        assert store_pkg.OutgoingConfigUnreadable is OutgoingConfigUnreadable

    def test_the_importer_module_all_lists_the_new_error(self):
        assert "OutgoingConfigUnreadable" in importer_module.__all__

    def test_the_new_errors_sit_in_the_right_hierarchies(self):
        assert issubclass(SchemaIntegrityError, SchemaError)
        assert issubclass(OutgoingConfigUnreadable, ConfigPathError)
        assert not issubclass(OutgoingConfigUnreadable, SchemaError)

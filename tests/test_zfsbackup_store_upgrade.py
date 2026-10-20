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
file that then fail) are recorded in the item 10.1 hand-off report. The item
10.4 counts (foreign-database refusal, `MigrationPackagingError`, the
`in_transaction` guard, the interrupt wording, reader/writer journal modes) are
listed in `tests/test_zfsbackup_cli.py`'s docstring.

Item 10.4 (the `Test...Foreign...`, `TestEnsureSchemaTransactionGuard`,
`TestImporterInterruptNotes`, `TestReimportWithoutExpungeAll` classes at the end
of this file; the reader/writer journal-mode tests live in
`test_zfsbackup_store.py::TestJournalModeByEngineRole`). A schema-only database
is built with `make_schema_only` (`ensure_schema` on a writer connection):
`upgrade_schema` no longer initialises a tableless file.
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
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import pytest
from _hermetic import hermetic_env
from alembic.runtime.migration import MigrationContext
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
    MigrationPackagingError,
    NotAConfigDatabase,
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


def make_schema_only(resolved) -> None:
    """A valid database at head holding NO configuration (`global_settings` is
    empty): `ensure_schema` on a writer connection, committed.

    `upgrade_schema` can no longer build this from a tableless file (item
    10.4: it refuses a database with no recorded revision), and `import_yaml`
    cleans up a failed first import. Going through `ensure_schema` is also what
    a first import's phase 2 does. The file must already be a valid SQLite
    database (`open_config_connection` refuses a zero-byte one).
    """
    with open_config_connection(resolved, readonly=False, foreign_keys=False) as conn:
        trans = conn.begin()
        ensure_schema(conn)
        trans.commit()
    dispose_all()


def valid_empty_sqlite_file(db: Path) -> None:
    """A non-zero-byte SQLite file with no tables (WAL header written)."""
    conn = sqlite3.connect(str(db))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE _t(x)")
    conn.execute("DROP TABLE _t")
    conn.commit()
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


def activate(monkeypatch, head=SYNTH, known=(*sorted(KNOWN_REVISIONS), SYNTH)):
    """Patch `EXPECTED_HEAD` and `KNOWN_REVISIONS` together. `known` is EVERY real
    revision (the baseline and the TLS one, not only the head) plus the synthetic
    one: `_assert_scripts_match_constants` compares it with the scripts on disk."""
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
        # Item 10.4's neutral wording: "<path> is at schema revision X; this
        # package expects Y. Run `zfsbackup-config migrate`." -- no daemon
        # sentence (the daemon appends its own).
        assert message == (
            f"{resolved.path} is at schema revision {EXPECTED_HEAD}; this "
            f"package expects {SYNTH}. Run `zfsbackup-config migrate`."
        )
        assert "daemon" not in message
        assert "no recorded schema revision" not in message

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
        `MigrationPackagingError` (the `KNOWN_REVISIONS` check), not a
        `SchemaError`."""
        synthetic_revision(BODY_GOOD)
        monkeypatch.setattr(
            migrate_module, "KNOWN_REVISIONS", frozenset({EXPECTED_HEAD})
        )
        with pytest.raises(MigrationPackagingError, match="KNOWN_REVISIONS") as excinfo:
            upgrade_schema(resolved)
        assert not isinstance(excinfo.value, SchemaError)


# ---------------------------------------------------------------------------
# B1: _assert_scripts_match_constants
# ---------------------------------------------------------------------------


class TestAssertScriptsMatchConstants:
    def test_head_mismatch_raises_a_migration_packaging_error(self, monkeypatch):
        monkeypatch.setattr(migrate_module, "EXPECTED_HEAD", "cccccccccccc")
        with pytest.raises(RuntimeError) as excinfo:
            _assert_scripts_match_constants()
        assert type(excinfo.value) is MigrationPackagingError
        assert isinstance(excinfo.value, RuntimeError)
        assert not isinstance(excinfo.value, SchemaError)
        assert "'cccccccccccc'" in str(excinfo.value)
        assert repr(EXPECTED_HEAD) in str(excinfo.value)
        assert "EXPECTED_HEAD" in str(excinfo.value)

    def test_revision_set_mismatch_raises_a_migration_packaging_error(self, monkeypatch):
        monkeypatch.setattr(
            migrate_module, "KNOWN_REVISIONS", frozenset({EXPECTED_HEAD, "zzzzzzzzzzzz"})
        )
        with pytest.raises(RuntimeError) as excinfo:
            _assert_scripts_match_constants()
        assert type(excinfo.value) is MigrationPackagingError
        assert isinstance(excinfo.value, RuntimeError)
        assert not isinstance(excinfo.value, SchemaError)
        assert "KNOWN_REVISIONS" in str(excinfo.value)
        assert "zzzzzzzzzzzz" in str(excinfo.value)

    def test_a_revision_missing_from_the_constants_is_a_mismatch(self, monkeypatch):
        monkeypatch.setattr(migrate_module, "KNOWN_REVISIONS", frozenset())
        with pytest.raises(MigrationPackagingError, match="KNOWN_REVISIONS"):
            _assert_scripts_match_constants()

    def test_multiple_heads_raise_a_migration_packaging_error(
        self, synthetic_scripts, monkeypatch
    ):
        write_revision(synthetic_scripts, "one.py", BODY_GOOD, rid="b1b1b1b1b1b1")
        write_revision(synthetic_scripts, "two.py", BODY_GOOD, rid="b2b2b2b2b2b2")
        with pytest.raises(RuntimeError) as excinfo:
            _assert_scripts_match_constants()
        assert type(excinfo.value) is MigrationPackagingError
        assert isinstance(excinfo.value, RuntimeError)
        assert not isinstance(excinfo.value, SchemaError)
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
        with pytest.raises(MigrationPackagingError) as excinfo:
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

    def test_a_schema_less_but_valid_sqlite_file_is_refused_and_pointed_at_import(
        self, db_path, resolved
    ):
        """Item 10.4: `migrate` only upgrades a RECORDED, known revision. A
        valid SQLite file with no tables is not migrated (and so not stamped):
        `SchemaOutOfDate`, whose text points at `import`; the file gains no
        table."""
        valid_empty_sqlite_file(db_path)
        before_tables = sql(db_path, "SELECT name FROM sqlite_master")

        with pytest.raises(SchemaOutOfDate) as excinfo:
            upgrade_schema(resolved)
        dispose_all()

        message = str(excinfo.value)
        assert "no recorded schema revision" in message
        assert "`zfsbackup-config import`" in message
        assert "nothing to migrate" in message
        assert "zfsbackup-config migrate" not in message
        assert sql(db_path, "SELECT name FROM sqlite_master") == before_tables == []

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
    M.KNOWN_REVISIONS = frozenset({{*{known!r}, {synth!r}}})
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
                synth=SYNTH, known=sorted(KNOWN_REVISIONS),
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
        valid_empty_sqlite_file(db_path)
        make_schema_only(resolved)
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
    "MigrationPackagingError",
    "NotAConfigDatabase",
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
        assert store_pkg.MigrationPackagingError is MigrationPackagingError
        assert store_pkg.NotAConfigDatabase is NotAConfigDatabase

    def test_the_importer_module_all_lists_the_new_error(self):
        assert "OutgoingConfigUnreadable" in importer_module.__all__

    def test_the_new_errors_sit_in_the_right_hierarchies(self):
        assert issubclass(SchemaIntegrityError, SchemaError)
        assert issubclass(OutgoingConfigUnreadable, ConfigPathError)
        assert not issubclass(OutgoingConfigUnreadable, SchemaError)
        # Item 10.4: a foreign database is an operator-fixable refusal; a
        # packaging defect is not, and must not share its catchable base.
        assert issubclass(NotAConfigDatabase, SchemaError)
        assert issubclass(MigrationPackagingError, RuntimeError)
        assert not issubclass(MigrationPackagingError, SchemaError)


# ===========================================================================
# Item 10.4 -- review fixes (store half)
# ===========================================================================


def make_foreign(db: Path, *tables: str, wal: bool = False) -> None:
    """A rollback-journal (or `wal`) SQLite file holding only `tables`, none of
    them zfsbackup's."""
    conn = sqlite3.connect(str(db))
    if wal:
        conn.execute("PRAGMA journal_mode=WAL")
    for name in tables or ("orders",):
        conn.execute(f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, x TEXT)")
        conn.execute(f"INSERT INTO {name} (x) VALUES ('a')")
    conn.commit()
    conn.close()
    os.chmod(db, 0o660)


def fingerprint(db: Path):
    """Full bytes, journal-mode header bytes 18-19, `sqlite_master`, and the
    directory listing: everything a refusal must leave as it was. The listing
    is exact on purpose: a database that is not ours (item 10.4) must not even
    gain a `<db>.writer.lock` beside it -- `import_yaml` pre-checks the target
    read-only BEFORE it takes the writer lock (item 15.4)."""
    raw = db.read_bytes()
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        master = conn.execute("SELECT type, name, sql FROM sqlite_master ORDER BY name").fetchall()
    finally:
        conn.close()
    return raw, (raw[18], raw[19]), master, sorted(p.name for p in db.parent.iterdir())


def create_our_tables(db: Path) -> None:
    """Every table of zfsbackup's schema, created without `alembic_version`
    (the lost-stamp state)."""
    eng = create_engine(f"sqlite:///{db}")
    Base.metadata.create_all(eng)
    eng.dispose()


class TestForeignDatabaseRefusal:
    """A database with tables but no recorded revision is not ours to modify
    (item 10.4). Reproduced in review with a foreign `orders` table: `migrate`
    grafted the schema into it and stamped it."""

    @pytest.fixture
    def foreign(self, db_path):
        make_foreign(db_path)
        return db_path

    def test_check_schema_raises_not_a_config_database(self, foreign, resolved):
        before = fingerprint(foreign)
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(NotAConfigDatabase) as excinfo:
                check_schema(conn)
        dispose_all()
        message = str(excinfo.value)
        assert isinstance(excinfo.value, SchemaError)
        assert "not a zfsbackup config database" in message
        assert "(tables: orders)" in message
        assert "nothing was changed" in message
        assert str(foreign) in message
        assert "`zfsbackup-config import`" in message
        assert fingerprint(foreign) == before
        assert before[1] == (1, 1)

    def test_upgrade_schema_raises_and_changes_nothing(self, foreign, resolved):
        before = fingerprint(foreign)
        with pytest.raises(NotAConfigDatabase):
            upgrade_schema(resolved)
        dispose_all()
        assert fingerprint(foreign) == before
        assert "alembic_version" not in [r[1] for r in before[2]]

    def test_import_yaml_over_it_raises_and_changes_nothing(self, foreign, resolved):
        before = fingerprint(foreign)
        with pytest.raises(NotAConfigDatabase):
            import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        assert foreign.exists()  # not created by this import, so never cleaned up
        assert fingerprint(foreign) == before

    def test_import_with_a_backup_callback_takes_no_backup(self, foreign, resolved, tmp_path):
        rec = Recorder(tmp_path / "bk.yaml")
        with pytest.raises(NotAConfigDatabase):
            import_yaml(EXAMPLE_YAML, resolved, backup=rec)
        dispose_all()
        assert rec.configs == []
        assert not (tmp_path / "bk.yaml").exists()

    def test_a_foreign_database_that_is_already_wal_is_refused_unchanged(self, db_path, resolved):
        make_foreign(db_path, wal=True)
        dispose_all()
        before = fingerprint(db_path)
        assert before[1] == (2, 2)
        for call in (upgrade_schema, lambda r: import_yaml(EXAMPLE_YAML, r)):
            with pytest.raises(NotAConfigDatabase):
                call(resolved)
        dispose_all()
        assert fingerprint(db_path) == before

    def test_ours_mixed_with_a_foreign_table_is_not_a_config_database(self, db_path, resolved):
        # The refusal lists the first six table names in SORTED order and
        # summarises the rest ("and N more"). The schema now has eleven tables
        # of ours (M5), so a foreign table named `orders` would sort past the
        # sixth and be hidden; `aa_orders` sorts first and keeps this test
        # about "the foreign table is named". The truncation itself is pinned
        # by `test_the_refusal_lists_six_sorted_names_and_counts_the_rest`.
        make_foreign(db_path, "aa_orders")
        create_our_tables(db_path)
        before = fingerprint(db_path)
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(NotAConfigDatabase) as excinfo:
                check_schema(conn)
        assert "aa_orders" in str(excinfo.value)
        assert f"and {len(Base.metadata.tables) + 1 - 6} more" in str(excinfo.value)
        for call in (upgrade_schema, lambda r: import_yaml(EXAMPLE_YAML, r)):
            with pytest.raises(NotAConfigDatabase):
                call(resolved)
        dispose_all()
        assert fingerprint(db_path) == before

    def test_the_refusal_lists_foreign_tables_first_then_ours_and_counts_the_rest(
        self, db_path, resolved
    ):
        # Foreign tables first, then ours, each group sorted, six shown. A
        # foreign name that sorts LAST overall (`zz_orders`) is therefore
        # still named: it used to be summarised away into "and N more".
        make_foreign(db_path, "zz_orders")
        create_our_tables(db_path)
        ours = sorted(Base.metadata.tables)
        listed = ["zz_orders", *ours]
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(NotAConfigDatabase) as excinfo:
                check_schema(conn)
        message = str(excinfo.value)
        assert f"(tables: {', '.join(listed[:6])} and {len(listed) - 6} more)" in message
        assert "(tables: zz_orders, " in message
        assert ours[5] not in message.split("(tables:")[1].split(")")[0]  # ours truncated first

    def test_every_foreign_table_is_listed_before_any_of_ours_each_group_sorted(
        self, db_path, resolved
    ):
        foreign = ["zz_b", "aa_a", "mm_c"]
        make_foreign(db_path, *foreign)
        create_our_tables(db_path)
        ours = sorted(Base.metadata.tables)
        listed = sorted(foreign) + ours
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(NotAConfigDatabase) as excinfo:
                check_schema(conn)
        assert (
            f"(tables: {', '.join(listed[:6])} and {len(listed) - 6} more)"
            in str(excinfo.value)
        )

    def test_more_than_six_foreign_tables_are_truncated_at_six_without_ours(
        self, db_path, resolved
    ):
        foreign = [f"t{i}" for i in range(8)]
        make_foreign(db_path, *foreign)
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(NotAConfigDatabase) as excinfo:
                check_schema(conn)
        assert "(tables: t0, t1, t2, t3, t4, t5 and 2 more)" in str(excinfo.value)

    def test_a_short_listing_has_no_more_suffix(self, db_path, resolved):
        make_foreign(db_path, "b_t", "a_t")
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(NotAConfigDatabase) as excinfo:
                check_schema(conn)
        assert "(tables: a_t, b_t)" in str(excinfo.value)
        assert "more" not in str(excinfo.value)

    def test_only_our_tables_and_no_revision_is_split_brain_not_foreign(self, db_path, resolved):
        """Kept deliberately: nothing of someone else's is at stake, and the
        message offers the recovery (`alembic stamp head`)."""
        db_path.write_bytes(b"")
        create_our_tables(db_path)
        before = fingerprint(db_path)
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(SchemaSplitBrain):
                check_schema(conn)
        with pytest.raises(SchemaSplitBrain):
            upgrade_schema(resolved)
        with pytest.raises(SchemaSplitBrain):
            import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        assert fingerprint(db_path) == before

    def test_the_head_set_split_brain_message_says_to_stamp_head(self, db_path, resolved):
        db_path.write_bytes(b"")
        create_our_tables(db_path)
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(SchemaSplitBrain) as excinfo:
                check_schema(conn)
        message = str(excinfo.value)
        assert "all of this package's tables already exist" in message
        assert f"`alembic stamp head`" in message
        assert "alembic stamp a1c28e1b3f34" not in message

    def test_an_empty_alembic_version_table_does_not_count_as_a_table_of_ours_or_theirs(
        self, db_path, resolved
    ):
        """An unfinished initial migration (stamp table, no rows, nothing
        else) is "no tables": the empty-file branch, pointing at `import`."""
        valid_empty_sqlite_file(db_path)
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
        conn.commit()
        conn.close()
        with open_config_connection(resolved, readonly=True) as c:
            with pytest.raises(SchemaOutOfDate) as excinfo:
                check_schema(c)
        assert "holds no configuration tables" in str(excinfo.value)
        assert not isinstance(excinfo.value, NotAConfigDatabase)

    def test_a_foreign_table_next_to_an_empty_alembic_version_is_still_foreign(
        self, db_path, resolved
    ):
        make_foreign(db_path, "orders")
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
        conn.commit()
        conn.close()
        with pytest.raises(NotAConfigDatabase):
            upgrade_schema(resolved)

    def test_a_recorded_revision_is_never_refused_for_extra_tables(self, imported, resolved):
        """The guard is for UNRECORDED databases only: ours, at head, with an
        extra table somebody added still checks out."""
        conn = sqlite3.connect(str(imported))
        conn.execute("CREATE TABLE operator_notes (x)")
        conn.commit()
        conn.close()
        dispose_all()
        with open_config_connection(resolved, readonly=True) as c:
            assert check_schema(c) == EXPECTED_HEAD

    def test_a_long_table_list_is_truncated_in_the_message(self, db_path, resolved):
        names = [f"t{i}" for i in range(9)]
        make_foreign(db_path, *names)
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(NotAConfigDatabase) as excinfo:
                check_schema(conn)
        message = str(excinfo.value)
        assert "(tables: t0, t1, t2, t3, t4, t5 and 3 more)" in message
        assert "t6" not in message

    def test_a_tableless_valid_file_is_the_empty_branch_not_foreign(self, db_path, resolved):
        valid_empty_sqlite_file(db_path)
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(SchemaOutOfDate):
                check_schema(conn)
        with pytest.raises(SchemaOutOfDate):
            upgrade_schema(resolved)
        # ...while import is the command that may initialise it.
        result = import_yaml(EXAMPLE_YAML, resolved)
        assert result.created is False and result.head_revision == EXPECTED_HEAD

    def test_a_missing_or_zero_byte_target_is_still_refused_by_the_preflight(
        self, db_path, resolved
    ):
        with pytest.raises(ConfigDbNotFound):
            upgrade_schema(resolved)
        db_path.write_bytes(b"")
        with pytest.raises(ConfigDbNotADatabase):
            upgrade_schema(resolved)


class TestEnsureSchemaTransactionGuard:
    """`ensure_schema` asserts it runs inside the caller's transaction before
    handing Alembic the connection (item 10.4, 5)."""

    @pytest.fixture
    def memory_engine(self):
        eng = create_engine(
            "sqlite:///:memory:", connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        yield eng
        eng.dispose()

    def test_it_raises_when_the_connection_is_not_in_a_transaction(
        self, memory_engine, monkeypatch
    ):
        calls = []

        def out_of_date_with_no_transaction(connection):
            connection.rollback()  # leaves the connection transaction-less
            raise SchemaOutOfDate("behind")

        monkeypatch.setattr(migrate_module, "check_schema", out_of_date_with_no_transaction)
        monkeypatch.setattr(migrate_module.command, "upgrade", lambda *a, **k: calls.append(a))

        with memory_engine.connect() as conn:
            with pytest.raises(RuntimeError, match="must run inside the caller's transaction") as excinfo:
                ensure_schema(conn)

        assert calls == []  # Alembic was never handed the connection
        assert not isinstance(excinfo.value, (SchemaError, MigrationPackagingError))
        assert inspect(memory_engine).get_table_names() == []

    def test_the_normal_path_runs_inside_a_transaction(self, memory_engine):
        with memory_engine.connect() as conn:
            assert ensure_schema(conn) == EXPECTED_HEAD
            assert conn.in_transaction()
            conn.commit()
        assert "alembic_version" in inspect(memory_engine).get_table_names()

    def test_the_script_check_runs_before_the_guard_and_before_any_write(
        self, memory_engine, monkeypatch
    ):
        monkeypatch.setattr(migrate_module, "EXPECTED_HEAD", "cccccccccccc")
        with memory_engine.connect() as conn:
            with pytest.raises(MigrationPackagingError):
                ensure_schema(conn)
        assert inspect(memory_engine).get_table_names() == []


class TestImporterInterruptNotes:
    """A `KeyboardInterrupt` can land after phase 3's COMMIT returned, so the
    importer's notes must not assert "was not replaced" / "NOT applied" for a
    `BaseException` (item 10.4, CLI-4)."""

    def _fail_in_save(self, monkeypatch, exc):
        def failing(session, config):
            raise exc

        monkeypatch.setattr(importer_module, "save_config", failing)

    def _interrupt_after_commit(self, monkeypatch):
        from contextlib import contextmanager as cm

        real = importer_module.open_config_session

        @cm
        def wrapper(*a, **k):
            with real(*a, **k) as session:
                yield session
            raise KeyboardInterrupt

        monkeypatch.setattr(importer_module, "open_config_session", wrapper)

    def test_an_exception_after_the_backup_says_the_config_was_not_replaced(
        self, imported, resolved, new_yaml, tmp_path, monkeypatch
    ):
        rec = Recorder(tmp_path / "bk.yaml")
        self._fail_in_save(monkeypatch, ValueError("late"))
        with pytest.raises(ValueError) as excinfo:
            import_yaml(new_yaml, resolved, backup=rec)
        (note,) = excinfo.value.__notes__
        assert note == (
            f"A pre-import backup of the outgoing configuration was already "
            f"written to {tmp_path / 'bk.yaml'}; the stored configuration was not replaced."
        )

    def test_a_keyboard_interrupt_after_the_backup_does_not_assert_either_outcome(
        self, imported, resolved, new_yaml, tmp_path, monkeypatch
    ):
        rec = Recorder(tmp_path / "bk.yaml")
        self._fail_in_save(monkeypatch, KeyboardInterrupt())
        with pytest.raises(KeyboardInterrupt) as excinfo:
            import_yaml(new_yaml, resolved, backup=rec)
        (note,) = excinfo.value.__notes__
        assert f"already written to {tmp_path / 'bk.yaml'}" in note
        assert "may or may not have been applied" in note
        assert "`zfsbackup-config export`" in note
        assert "was not replaced" not in note

    def test_an_interrupt_after_the_commit_of_a_reimport_really_was_applied(
        self, imported, db_path, resolved, new_yaml, tmp_path, monkeypatch
    ):
        rec = Recorder(tmp_path / "bk.yaml")
        self._interrupt_after_commit(monkeypatch)
        with pytest.raises(KeyboardInterrupt) as excinfo:
            import_yaml(new_yaml, resolved, backup=rec)
        monkeypatch.undo()
        dispose_all()
        assert "may or may not have been applied" in excinfo.value.__notes__[0]
        assert state(db_path)[1] == [("tank/x",)]  # ...and here it was

    def test_a_first_import_interrupted_after_the_commit_keeps_the_file_without_claiming_not_applied(
        self, db_path, resolved, monkeypatch, caplog
    ):
        self._interrupt_after_commit(monkeypatch)
        with caplog.at_level(logging.WARNING):
            with pytest.raises(KeyboardInterrupt) as excinfo:
                import_yaml(EXAMPLE_YAML, resolved)
        monkeypatch.undo()
        dispose_all()

        assert db_path.exists()
        (note,) = excinfo.value.__notes__
        assert "NOT applied" not in note
        assert f"{db_path} holds a configuration and was left intact" in note
        assert "may or may not have been applied" in note
        assert "`zfsbackup-config export`" in note
        assert cleanup_warnings(caplog) == [note]
        assert state(db_path)[1]  # the configuration is there

    def test_an_ordinary_exception_in_the_same_window_still_says_not_applied(
        self, db_path, resolved, monkeypatch
    ):
        """The unchanged wording for an `Exception`: a concurrent import by an
        opener that ignores the writer lock won the race and this one did not
        apply."""
        from zfsbackup.config.store import importer as imp

        real = imp.open_config_session

        @contextmanager
        def failing_after_another_import_committed(*a, **k):
            # Another writer imports first (the A-11 race), then this fails.
            monkeypatch.setattr(imp, "open_config_session", real)
            # The "other writer" ignores the writer lock (item 15.4): a lock-
            # honouring one cannot commit inside this import's critical
            # section (same thread: RuntimeError; elsewhere: it waits). What
            # is pinned here is the WORDING for the case where a lock-ignoring
            # opener did, i.e. the unchanged "NOT applied" note.
            @contextmanager
            def no_lock(*_a, **_k):
                yield None

            monkeypatch.setattr(imp, "writer_lock", no_lock)
            import_yaml(EXAMPLE_YAML, resolved)
            dispose_all()
            raise ValueError("this import lost")
            yield  # pragma: no cover

        monkeypatch.setattr(imp, "open_config_session", failing_after_another_import_committed)
        with pytest.raises(ValueError) as excinfo:
            import_yaml(new_yaml_text_path(db_path.parent), resolved)
        (note,) = excinfo.value.__notes__
        assert "this import was NOT applied" in note
        assert "may or may not" not in note

    def test_the_skipped_cleanup_text_for_imported_is_pinned_for_both_kinds(self, tmp_path):
        plain = importer_module._cleanup_skipped(tmp_path / "x.db", "it now holds a configuration", "imported")
        assert "this import was NOT applied" in plain
        interrupted = importer_module._cleanup_skipped(
            tmp_path / "x.db", "it now holds a configuration", "imported", interrupted=True
        )
        assert "NOT applied" not in interrupted
        assert "may or may not have been applied" in interrupted
        # Other kinds are unaffected by the flag.
        other = importer_module._cleanup_skipped(tmp_path / "x.db", "r", "schema", interrupted=True)
        assert other == importer_module._cleanup_skipped(tmp_path / "x.db", "r", "schema")


def new_yaml_text_path(directory: Path) -> Path:
    path = directory / "late.yaml"
    path.write_text(NEW_YAML)
    return path


class TestReimportWithoutExpungeAll:
    """`session.expunge_all()` was dropped from importer phase 3: the bulk
    `delete()`s in `save_config` evict what `load_config` loaded. A re-import
    that loaded the outgoing config for a backup must still be exactly right,
    including rows that reuse primary keys and a destination graph."""

    YAML_WITH_DESTINATIONS = textwrap.dedent(
        """\
        snapshot_prefix: dest
        destinations:
          offsite:
            url: https://backup.example:8080
            server_fingerprint: "abababababababababababababababababababababababababababababababab"
        datasets:
          - name: tank/a
            frequency: 1h
            retention:
              "1d": "30d"
              "1w": "6M"
            remote:
              - destination: offsite
                frequency: 2h
                retention:
                  "1d": "7d"
          - name: tank/b
            frequency: 30m
        """
    )

    @pytest.fixture
    def dest_yaml(self, tmp_path):
        path = tmp_path / "dest.yaml"
        path.write_text(self.YAML_WITH_DESTINATIONS)
        return path

    def test_repeated_reimports_with_a_backup_callback_equal_from_file(
        self, imported, db_path, resolved, dest_yaml, new_yaml, tmp_path
    ):
        for source in (dest_yaml, dest_yaml, new_yaml, dest_yaml, EXAMPLE_YAML, EXAMPLE_YAML):
            rec = Recorder(tmp_path / "bk.yaml")
            outgoing = None
            with open_config_session(resolved, readonly=True) as session:
                outgoing = load_config(session)
            result = import_yaml(source, resolved, backup=rec)
            dispose_all()

            assert rec.configs == [outgoing]  # the callback saw the previous config
            assert result.backup_path == tmp_path / "bk.yaml"
            with open_config_session(resolved, readonly=True) as session:
                assert load_config(session) == BackupConfig.from_file(source)
            assert sql(db_path, "PRAGMA foreign_key_check") == []

    def test_generation_advances_once_per_reimport(self, imported, db_path, resolved, dest_yaml, tmp_path):
        start = sql(db_path, "SELECT generation FROM global_settings")[0][0]
        for n in (1, 2, 3):
            import_yaml(dest_yaml, resolved, backup=Recorder(tmp_path / "bk.yaml"))
            dispose_all()
            assert sql(db_path, "SELECT generation FROM global_settings")[0][0] == start + n

    def test_a_failed_save_after_the_backup_rolls_back_to_the_loaded_config(
        self, imported, db_path, resolved, dest_yaml, tmp_path, monkeypatch
    ):
        before = state(db_path)
        real = importer_module.save_config

        def failing(session, config):
            real(session, config)  # wipe + insert happen ...
            raise ValueError("late")  # ... and are rolled back

        monkeypatch.setattr(importer_module, "save_config", failing)
        with pytest.raises(ValueError):
            import_yaml(dest_yaml, resolved, backup=Recorder(tmp_path / "bk.yaml"))
        monkeypatch.undo()
        dispose_all()
        assert state(db_path) == before
        with open_config_session(resolved, readonly=True) as session:
            assert load_config(session) == BackupConfig.from_file(EXAMPLE_YAML)


# ===========================================================================
# Item 10.5 -- review fixes (store half)
# ===========================================================================

_STAMP_TABLE_DDL = "CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL PRIMARY KEY)"


def make_alembic_foreign(db: Path, variant: str) -> None:
    """A rollback-journal SQLite file in one of the review's three foreign
    states that have an `alembic_version` table (see `foreign_alembic.py` in
    the 10.5 review): `foreign_rev` (an `orders` table, revision
    'deadbeef0001'), `empty_alembic` (an `orders` table, an EMPTY
    `alembic_version`), `mixed_rows` (a known and an unknown revision)."""
    conn = sqlite3.connect(str(db))
    if variant in ("foreign_rev", "empty_alembic"):
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, x TEXT)")
        conn.execute("INSERT INTO orders VALUES (1, 'a')")
    conn.execute(_STAMP_TABLE_DDL)
    if variant == "foreign_rev":
        conn.execute("INSERT INTO alembic_version VALUES ('deadbeef0001')")
    elif variant == "mixed_rows":
        conn.execute(f"INSERT INTO alembic_version VALUES ('{EXPECTED_HEAD}')")
        conn.execute("INSERT INTO alembic_version VALUES ('deadbeef0001')")
    conn.commit()
    conn.close()
    os.chmod(db, 0o660)


ALEMBIC_FOREIGN_VARIANTS = ["foreign_rev", "empty_alembic", "mixed_rows"]


class TestForeignAlembicDatabaseIsNeverSwitchedToWal:
    """Item 10.5, M-1R. The 10.4 gate switched any file that merely HAD an
    `alembic_version` table; a foreign Alembic-managed database (or a foreign
    file with an empty one) was converted to WAL and then refused with "nothing
    was changed". Every store entry point must now leave it byte-for-byte alone.
    """

    @pytest.mark.parametrize("variant", ALEMBIC_FOREIGN_VARIANTS)
    def test_every_store_entry_point_refuses_and_changes_nothing(
        self, db_path, resolved, variant
    ):
        make_alembic_foreign(db_path, variant)
        before = fingerprint(db_path)
        sha = hashlib.sha256(db_path.read_bytes()).hexdigest()
        assert before[1] == (1, 1)

        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(SchemaError):
                check_schema(conn)
        dispose_all()
        with pytest.raises(SchemaError):
            upgrade_schema(resolved)
        dispose_all()
        with pytest.raises(SchemaError):
            import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()

        assert fingerprint(db_path) == before
        assert hashlib.sha256(db_path.read_bytes()).hexdigest() == sha
        assert db_path.read_bytes()[18:20] == b"\x01\x01"

    @pytest.mark.parametrize(
        "variant, expected",
        [
            ("foreign_rev", SchemaVersionMismatch),
            ("mixed_rows", SchemaVersionMismatch),
            ("empty_alembic", NotAConfigDatabase),
        ],
    )
    def test_the_refusal_is_the_right_one(self, db_path, resolved, variant, expected):
        make_alembic_foreign(db_path, variant)
        with pytest.raises(expected):
            upgrade_schema(resolved)
        dispose_all()

    def test_the_only_empty_alembic_version_file_is_not_switched_when_migrate_refuses_it(
        self, db_path, resolved
    ):
        """10.6: `migrate` refuses a stamp-only file ("nothing to migrate") and
        promises to leave it unchanged, so the writer engine must not rewrite
        its header first. (`import` still migrates it, then the next engine
        switches it -- see `TestFirstImportEndsInWal`.)"""
        conn = sqlite3.connect(str(db_path))
        conn.execute(_STAMP_TABLE_DDL)
        conn.commit()
        conn.close()
        before = db_path.read_bytes()
        assert before[18:20] == b"\x01\x01"
        with pytest.raises(SchemaOutOfDate, match="nothing to migrate"):
            upgrade_schema(resolved)
        dispose_all()
        assert db_path.read_bytes()[18:20] == b"\x01\x01"
        assert db_path.read_bytes() == before
        assert hashlib.sha256(db_path.read_bytes()).hexdigest() == hashlib.sha256(before).hexdigest()
        assert sorted(p.name for p in db_path.parent.iterdir() if p.name.startswith(db_path.name)) == [db_path.name]


class TestFirstImportEndsInWal:
    """A first import onto a file the writer may switch ends in WAL, whatever
    state the (new, tableless or stamp-only) file started in."""

    def test_a_new_file(self, db_path, resolved):
        assert not db_path.exists()
        result = import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        assert result.created is True
        assert db_path.read_bytes()[18:20] == b"\x02\x02"

    def test_a_valid_tableless_rollback_journal_file(self, db_path, resolved):
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA user_version=1")
        conn.commit()
        conn.close()
        assert db_path.read_bytes()[18:20] == b"\x01\x01"
        import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        assert db_path.read_bytes()[18:20] == b"\x02\x02"
        assert state(db_path)[1]

    def test_a_file_whose_only_table_is_an_empty_alembic_version(self, db_path, resolved):
        conn = sqlite3.connect(str(db_path))
        conn.execute(_STAMP_TABLE_DDL)
        conn.commit()
        conn.close()
        result = import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        assert result.head_revision == EXPECTED_HEAD
        assert db_path.read_bytes()[18:20] == b"\x02\x02"
        assert sql(db_path, "SELECT version_num FROM alembic_version") == [(EXPECTED_HEAD,)]

    def test_a_zero_byte_file_is_refused_not_switched(self, db_path, resolved):
        """The preflight, not the gate, decides about a zero-byte file."""
        db_path.write_bytes(b"")
        with pytest.raises(ConfigDbNotADatabase):
            import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        assert db_path.read_bytes() == b""


class TestUnrecordedTableSubsets:
    """Item 10.5: `SchemaSplitBrain` only when the unrecorded tables are EXACTLY
    this package's; any strict subset, alone or beside foreign tables, is
    someone else's database (a lone `datasets` table must not be told to
    `alembic stamp head`)."""

    OURS = sorted(t for t in Base.metadata.tables if t != "alembic_version")

    def _build(self, db: Path, names, *foreign: str) -> None:
        conn = sqlite3.connect(str(db))
        for name in (*names, *foreign):
            conn.execute(f"CREATE TABLE {name} (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()
        os.chmod(db, 0o660)

    def _all_refuse_unchanged(self, db_path, resolved, expected):
        before = fingerprint(db_path)
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(expected) as excinfo:
                check_schema(conn)
        for call in (upgrade_schema, lambda r: import_yaml(EXAMPLE_YAML, r)):
            with pytest.raises(expected):
                call(resolved)
        dispose_all()
        assert fingerprint(db_path) == before
        return excinfo.value

    def test_a_lone_datasets_table_is_not_a_config_database(self, db_path, resolved):
        self._build(db_path, ["datasets"])
        exc = self._all_refuse_unchanged(db_path, resolved, NotAConfigDatabase)
        assert not isinstance(exc, SchemaSplitBrain)
        assert "(tables: datasets)" in str(exc)
        assert "alembic stamp" not in str(exc)

    @pytest.mark.parametrize("missing", OURS)
    def test_all_of_ours_but_one_is_not_a_config_database(self, db_path, resolved, missing):
        self._build(db_path, [t for t in self.OURS if t != missing])
        exc = self._all_refuse_unchanged(db_path, resolved, NotAConfigDatabase)
        assert missing not in str(exc)

    def test_a_subset_of_ours_beside_a_foreign_table_is_not_a_config_database(
        self, db_path, resolved
    ):
        self._build(db_path, ["datasets", "destinations"], "orders")
        exc = self._all_refuse_unchanged(db_path, resolved, NotAConfigDatabase)
        assert "orders" in str(exc)

    def test_all_of_ours_beside_a_foreign_table_is_not_a_config_database(self, db_path, resolved):
        self._build(db_path, self.OURS, "orders")
        self._all_refuse_unchanged(db_path, resolved, NotAConfigDatabase)

    def test_exactly_all_of_ours_and_no_stamp_is_split_brain(self, db_path, resolved):
        self._build(db_path, self.OURS)
        exc = self._all_refuse_unchanged(db_path, resolved, SchemaSplitBrain)
        assert "alembic stamp" in str(exc)
        assert not isinstance(exc, NotAConfigDatabase)

    def test_all_of_ours_and_an_empty_stamp_table_is_still_split_brain(self, db_path, resolved):
        """`alembic_version` itself is not application data."""
        self._build(db_path, self.OURS)
        conn = sqlite3.connect(str(db_path))
        conn.execute(_STAMP_TABLE_DDL)
        conn.commit()
        conn.close()
        self._all_refuse_unchanged(db_path, resolved, SchemaSplitBrain)


class TestSchemaVersionMismatchWording:
    """Item 10.5: the text no longer claims the file is certainly from a newer
    package -- a foreign Alembic database lands here too."""

    def test_the_message(self, db_path, resolved):
        make_alembic_foreign(db_path, "foreign_rev")
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(SchemaVersionMismatch) as excinfo:
                check_schema(conn)
        dispose_all()
        message = str(excinfo.value)
        assert message == (
            f"Database schema is at revision 'deadbeef0001', which this installed "
            f"zfsbackup package (migrations head {EXPECTED_HEAD!r}) does not "
            f"recognise. {db_path} was written by a newer zfsbackup package, or is "
            f"not a zfsbackup config database; nothing was changed. If it is ours, "
            f"upgrade the zfsbackup package installed on this host before opening "
            f"it again; otherwise point -c at the config database."
        )

    def test_the_first_unknown_revision_in_sorted_order_is_named(self, db_path, resolved):
        conn = sqlite3.connect(str(db_path))
        conn.execute(_STAMP_TABLE_DDL)
        conn.execute("INSERT INTO alembic_version VALUES ('zzzz')")
        conn.execute("INSERT INTO alembic_version VALUES ('aaaa')")
        conn.commit()
        conn.close()
        with open_config_connection(resolved, readonly=True) as c:
            with pytest.raises(SchemaVersionMismatch, match="revision 'aaaa'"):
                check_schema(c)
        dispose_all()


# ===========================================================================
# Item 17.0 -- the store id (PRAGMA user_version) across upgrade, and foreign DBs
# ===========================================================================


def raw_user_version(db: Path) -> int:
    return sql(db, "PRAGMA user_version")[0][0]


def set_raw_user_version(db: Path, value: int) -> None:
    conn = sqlite3.connect(str(db))
    conn.execute(f"PRAGMA user_version = {int(value)}")
    conn.commit()
    conn.close()


class TestUpgradeSchemaKeepsTheStoreId:
    def test_the_import_stamped_id_survives_a_table_rebuilding_migration(
        self, imported, db_path, resolved, synthetic_revision
    ):
        """`BODY_GOOD` is an Alembic `batch_alter_table`: it rebuilds the
        `datasets` table. `user_version` is a header field, not a table, so it
        must come through untouched."""
        before = raw_user_version(db_path)
        assert before != 0
        synthetic_revision(BODY_GOOD)

        result = upgrade_schema(resolved)
        dispose_all()

        assert result.changed is True
        assert raw_user_version(db_path) == before

    def test_a_distinctive_id_survives_so_a_re_stamp_would_be_seen(
        self, imported, db_path, resolved, synthetic_revision
    ):
        set_raw_user_version(db_path, 123456789)
        synthetic_revision(BODY_GOOD)
        upgrade_schema(resolved)
        dispose_all()
        assert raw_user_version(db_path) == 123456789

    def test_a_legacy_zero_is_not_stamped_by_an_upgrade(
        self, imported, db_path, resolved, synthetic_revision
    ):
        """Only `save_config` stamps; an upgrade neither stamps nor clears."""
        set_raw_user_version(db_path, 0)
        synthetic_revision(BODY_GOOD)
        upgrade_schema(resolved)
        dispose_all()
        assert raw_user_version(db_path) == 0

    def test_an_upgrade_that_changes_nothing_keeps_the_id(self, imported, db_path, resolved):
        before = raw_user_version(db_path)
        assert upgrade_schema(resolved).changed is False
        dispose_all()
        assert raw_user_version(db_path) == before

    def test_the_upgraded_database_reports_the_same_store_state(
        self, imported, db_path, resolved, synthetic_revision
    ):
        from zfsbackup.config.store import read_store_state

        before = read_store_state(resolved)
        dispose_all()
        synthetic_revision(BODY_GOOD)
        upgrade_schema(resolved)
        dispose_all()
        assert read_store_state(resolved) == before


FOREIGN_BUILDERS = {
    "foreign_tables": (lambda db: make_foreign(db), NotAConfigDatabase),
    "foreign_alembic": (lambda db: make_alembic_foreign(db, "foreign_rev"), SchemaVersionMismatch),
    "ours_without_stamp": (create_our_tables, SchemaSplitBrain),
    "empty_valid_file": (valid_empty_sqlite_file, None),
}


class TestForeignDatabaseIgnoresUserVersion:
    """Foreign-database detection never consults `user_version`: an unrelated
    SQLite file that happens to carry a non-zero value is refused exactly as it
    is with 0 -- not trusted because the field looks stamped."""

    @pytest.mark.parametrize("kind", sorted(FOREIGN_BUILDERS))
    def test_check_schema_raises_the_same_error_with_a_non_zero_user_version(
        self, kind, db_path, resolved
    ):
        build, expected = FOREIGN_BUILDERS[kind]
        build(db_path)

        def outcome():
            with open_config_connection(resolved, readonly=True) as conn:
                try:
                    return ("ok", check_schema(conn))
                except SchemaError as exc:
                    return (type(exc), str(exc))

        zero = outcome()
        set_raw_user_version(db_path, 424242)
        stamped = outcome()
        dispose_all()

        assert zero == stamped
        assert zero[0] != "ok"
        if expected is not None:
            assert zero[0] is expected

    def test_a_foreign_file_with_a_non_zero_user_version_is_refused_by_import_and_migrate(
        self, db_path, resolved
    ):
        make_foreign(db_path)
        set_raw_user_version(db_path, 424242)
        before = fingerprint(db_path)
        for call in (upgrade_schema, lambda r: import_yaml(EXAMPLE_YAML, r)):
            with pytest.raises(NotAConfigDatabase):
                call(resolved)
        dispose_all()
        assert fingerprint(db_path) == before
        assert raw_user_version(db_path) == 424242


BASELINE_REVISION = "a1c28e1b3f34"
BASELINE_TABLES = frozenset({
    "global_settings", "datasets", "destinations", "dataset_remotes",
    "retention_rules", "remote_server",
})


def make_interrupted_first_ensure_schema(db: Path) -> None:
    """The real way to get the baseline's six tables and no stamp: a FIRST
    `ensure_schema` on a plain pysqlite connection (implicit DDL autocommit,
    so not `db.py`'s writer engine) that is rolled back. The baseline's
    `CREATE TABLE`s autocommit; its stamp INSERT and the later revision's DDL
    share the transaction the rollback discards."""
    db.write_bytes(b"")
    engine = create_engine(f"sqlite:///{db}")
    try:
        with engine.connect() as conn:
            ensure_schema(conn)
            conn.rollback()
    finally:
        engine.dispose()


class TestBaselineOnlyTableSetIsSplitBrain:
    """Item 4 of the review fixes: the table set of ANY known revision, with no
    revision recorded, is ours (`SchemaSplitBrain`, with the revision to
    stamp), matched EXACTLY."""

    @pytest.fixture
    def interrupted(self, db_path):
        make_interrupted_first_ensure_schema(db_path)
        names = {r[0] for r in sqlite3.connect(str(db_path)).execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        # Guard the fixture itself: the state is exactly what the issue describes.
        assert names == BASELINE_TABLES | {"alembic_version"}
        return db_path

    def test_the_known_table_sets_are_pinned(self):
        sets = dict(migrate_module._TABLES_AT_REVISION)
        assert sets[EXPECTED_HEAD] == frozenset(Base.metadata.tables)
        assert sets[BASELINE_REVISION] == BASELINE_TABLES
        assert set(sets) == KNOWN_REVISIONS

    def test_check_schema_raises_split_brain_with_the_stamp_wording(self, interrupted, resolved):
        before = fingerprint(interrupted)
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(SchemaSplitBrain) as excinfo:
                check_schema(conn)
        dispose_all()
        message = str(excinfo.value)
        assert not isinstance(excinfo.value, NotAConfigDatabase)
        assert "`alembic stamp a1c28e1b3f34`" in message
        assert "`zfsbackup-config migrate`" in message
        assert message.index("alembic stamp a1c28e1b3f34") < message.index("zfsbackup-config migrate")
        assert f"head {EXPECTED_HEAD!r}" in message
        assert "older schema revision 'a1c28e1b3f34'" in message
        assert "Nothing was changed" in message
        assert "`alembic stamp head`" not in message
        assert fingerprint(interrupted) == before

    def test_every_write_path_refuses_it_and_changes_nothing(self, interrupted, resolved):
        before = fingerprint(interrupted)
        with pytest.raises(SchemaSplitBrain):
            upgrade_schema(resolved)
        with pytest.raises(SchemaSplitBrain):
            import_yaml(EXAMPLE_YAML, resolved)
        dispose_all()
        assert fingerprint(interrupted) == before

    def test_stamping_the_baseline_then_migrating_recovers(self, interrupted, resolved):
        conn = sqlite3.connect(str(interrupted))
        conn.execute("INSERT INTO alembic_version (version_num) VALUES (?)", (BASELINE_REVISION,))
        conn.commit()
        conn.close()
        result = upgrade_schema(resolved)
        dispose_all()
        assert result.from_revs == (BASELINE_REVISION,)
        assert result.to_rev == EXPECTED_HEAD and result.changed
        with open_config_connection(resolved, readonly=True) as c:
            assert check_schema(c) == EXPECTED_HEAD

    def test_the_baseline_set_plus_a_foreign_table_is_not_ours(self, interrupted, resolved):
        conn = sqlite3.connect(str(interrupted))
        conn.execute("CREATE TABLE orders (id INTEGER)")
        conn.commit()
        conn.close()
        with open_config_connection(resolved, readonly=True) as c:
            with pytest.raises(NotAConfigDatabase) as excinfo:
                check_schema(c)
        # The foreign table is the first name listed, never summarised away.
        assert "(tables: orders, " in str(excinfo.value)

    def test_a_strict_subset_of_the_baseline_is_not_ours(self, interrupted, resolved):
        conn = sqlite3.connect(str(interrupted))
        conn.execute("DROP TABLE remote_server")
        conn.commit()
        conn.close()
        with open_config_connection(resolved, readonly=True) as c:
            with pytest.raises(NotAConfigDatabase):
                check_schema(c)

    def test_a_set_between_the_baseline_and_the_head_is_not_ours(self, db_path, resolved):
        # Baseline plus ONE of the new tables: neither revision's set.
        create_our_tables(db_path)
        conn = sqlite3.connect(str(db_path))
        for table in ("host_identity", "trusted_clients", "enrollment_token", "trust_state"):
            conn.execute(f"DROP TABLE {table}")
        conn.execute("DROP TABLE clients")
        conn.execute("CREATE TABLE clients (client_id TEXT)")
        conn.commit()
        conn.close()
        with open_config_connection(resolved, readonly=True) as c:
            with pytest.raises(NotAConfigDatabase):
                check_schema(c)

    def test_a_lone_foreign_table_named_datasets_is_still_foreign(self, db_path, resolved):
        make_foreign(db_path, "datasets")
        with open_config_connection(resolved, readonly=True) as c:
            with pytest.raises(NotAConfigDatabase) as excinfo:
                check_schema(c)
        assert "alembic stamp" not in str(excinfo.value)


# ---------------------------------------------------------------------------
# `check_schema` is quiet; `migrate` is not
# ---------------------------------------------------------------------------

ALEMBIC_LOGGER = "alembic.runtime.migration"


def alembic_records(caplog):
    return [r for r in caplog.records if r.name == ALEMBIC_LOGGER]


class TestAlembicIsQuietWhileChecking:
    """`MigrationContext.configure` logs INFO lines on every call, and `check_schema` runs on
    every trust read (the API worker polls from several threads), which flooded the daemon log.
    They are dropped for the calling thread inside `check_schema` only: `ensure_schema` and
    `upgrade_schema` keep theirs, and another thread's records are never swallowed."""

    @pytest.fixture
    def at_head(self, imported, resolved):
        return resolved

    @pytest.fixture
    def info(self, caplog):
        with caplog.at_level(logging.INFO, logger=ALEMBIC_LOGGER):
            caplog.clear()
            yield caplog

    def test_the_control_alembic_does_log_at_info_here(self, info, at_head):
        """If this fails the tests below pass vacuously."""
        with open_config_connection(at_head, readonly=True) as conn:
            MigrationContext.configure(conn)
        assert alembic_records(info), "alembic logged nothing: the quiet tests prove nothing"

    def test_check_schema_emits_no_alembic_record(self, info, at_head):
        for _ in range(3):
            with open_config_connection(at_head, readonly=True) as conn:
                assert check_schema(conn) == EXPECTED_HEAD
        assert alembic_records(info) == []

    def test_check_schema_is_quiet_when_it_raises_too(self, info, db_path, resolved):
        make_foreign(db_path)
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(NotAConfigDatabase):
                check_schema(conn)
        assert alembic_records(info) == []

    def test_read_trust_key_emits_no_alembic_record(self, info, at_head):
        from zfsbackup.config.store.trust import read_trust_key

        for _ in range(3):
            read_trust_key(at_head)
        assert alembic_records(info) == []

    def test_token_precheck_emits_no_alembic_record(self, info, at_head):
        from zfsbackup.config.store.trust import token_precheck

        for _ in range(3):
            token_precheck(at_head, secret_sha256="a" * 64, server_fingerprint="b" * 64, now=1.0)
        assert alembic_records(info) == []

    def test_ensure_schema_still_logs(self, info, db_path, resolved):
        valid_empty_sqlite_file(db_path)
        with open_config_connection(resolved, readonly=False, foreign_keys=False) as conn:
            trans = conn.begin()
            ensure_schema(conn)
            trans.commit()
        dispose_all()
        assert alembic_records(info)

    def test_upgrade_schema_still_logs(self, info, at_head):
        upgrade_schema(at_head)
        dispose_all()
        assert alembic_records(info)

    def test_the_depth_counter_is_balanced_after_an_exception(self, info, db_path, resolved):
        make_foreign(db_path)
        with open_config_connection(resolved, readonly=True) as conn:
            with pytest.raises(NotAConfigDatabase):
                check_schema(conn)
        assert getattr(migrate_module._quiet, "depth", 0) == 0
        # ... so this thread's own later records are NOT suppressed.
        logging.getLogger(ALEMBIC_LOGGER).info("after the check")
        assert [r.getMessage() for r in alembic_records(info)] == ["after the check"]

    def test_the_quiet_block_is_reentrant(self, info):
        logger = logging.getLogger(ALEMBIC_LOGGER)
        with migrate_module._quiet_alembic():
            with migrate_module._quiet_alembic():
                logger.info("inner")
            logger.info("outer")
        logger.info("after")
        assert [r.getMessage() for r in alembic_records(info)] == ["after"]

    def test_another_threads_records_still_come_through_while_one_is_inside_check_schema(
        self, info, at_head, monkeypatch
    ):
        inside, release = threading.Event(), threading.Event()
        real_configure = MigrationContext.configure

        def slow_configure(*args, **kwargs):
            if threading.current_thread().name == "thread-a":
                inside.set()
                assert release.wait(30)
            return real_configure(*args, **kwargs)

        monkeypatch.setattr(
            migrate_module, "MigrationContext", mock.Mock(configure=slow_configure))
        errors = []

        def thread_a():
            try:
                with open_config_connection(at_head, readonly=True) as conn:
                    check_schema(conn)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        a = threading.Thread(target=thread_a, name="thread-a")
        a.start()
        try:
            assert inside.wait(30), "thread A never reached check_schema"
            # A is now inside the quiet block. B (this thread) logs and must be heard.
            logging.getLogger(ALEMBIC_LOGGER).info("from thread B")
            with open_config_connection(at_head, readonly=True) as conn:
                real_configure(conn)                      # alembic's own record, from B
            heard = [r for r in alembic_records(info) if r.threadName != "thread-a"]
            assert "from thread B" in [r.getMessage() for r in heard]
            assert len(heard) >= 2
        finally:
            release.set()
            a.join(30)
        assert errors == [] and not a.is_alive()
        # ... and A's own records were dropped.
        assert [r for r in alembic_records(info) if r.threadName == "thread-a"] == []

    def test_the_logger_level_is_untouched_by_concurrent_checks(self, at_head):
        logger = logging.getLogger(ALEMBIC_LOGGER)
        before = (logger.level, logger.disabled, logger.propagate, list(logger.filters))
        errors = []

        def worker():
            try:
                for _ in range(20):
                    with open_config_connection(at_head, readonly=True) as conn:
                        check_schema(conn)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        assert errors == [] and not any(t.is_alive() for t in threads)
        assert (logger.level, logger.disabled, logger.propagate, list(logger.filters)) == before
        assert getattr(migrate_module._quiet, "depth", 0) == 0

    RELOAD_SCRIPT = textwrap.dedent('''
        import importlib, logging, sys
        from zfsbackup.config.store import migrate
        logger = logging.getLogger("alembic.runtime.migration")

        def ours():
            return [f for f in logger.filters if type(f).__name__ == "_QuietWhileChecking"]

        reloads = int(sys.argv[1])
        assert len(ours()) == 1, ("before any reload", ours())
        for n in range(reloads):
            importlib.reload(migrate)
            assert len(ours()) == 1, ("after reload", n + 1, ours())
        assert isinstance(ours()[0], migrate._QuietWhileChecking)
        print("ok")
    ''')

    def run_reload_script(self, tmp_path, reloads):
        # Reloaded in a CHILD interpreter: reloading `migrate` in this process would rebind
        # `SchemaError` & co. and break every `except`/`isinstance` elsewhere in the suite.
        script = tmp_path / "reload_check.py"
        script.write_text(self.RELOAD_SCRIPT)
        return subprocess.run(
            [sys.executable, str(script), str(reloads)], cwd=ROOT, text=True,
            capture_output=True, timeout=120, env=dict(os.environ, PYTHONPATH=str(ROOT)),
        )

    def test_the_filter_is_installed_exactly_once_on_a_plain_import(self, tmp_path):
        result = self.run_reload_script(tmp_path, 0)
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "ok"

    def test_the_filter_is_installed_exactly_once_after_a_reload(self, tmp_path):
        result = self.run_reload_script(tmp_path, 1)
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "ok"

    def test_the_filter_stays_single_over_repeated_reloads(self, tmp_path):
        """The marker attribute (not an `isinstance` against the freshly rebound class) is what
        finds the previous module's filter, so N reloads still leave exactly one."""
        result = self.run_reload_script(tmp_path, 4)
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "ok"


class TestTheQuietFilterPinsNoEngine:
    """The Alembic quiet filter is installed on a logger that lives as long as the interpreter,
    and a filter instance keeps its class's methods, hence the DEFINING module's globals,
    alive. Defined in `migrate.py` those globals reached `paths` -> `db._ENGINES` -> the pooled
    SQLite connections, which then outlived interpreter finalization: a process that exited
    without `dispose_all()` never ran `sqlite3_close` and left `-wal`/`-shm` behind (which the
    failed-first-import cleanup reads as "another process has this database open"). The filter
    lives in `_alembic_quiet.py`, which imports nothing from the store."""

    EXIT_SCRIPT = textwrap.dedent('''
        import sys
        from zfsbackup.config.store import import_yaml, resolve_config_path
        resolved = resolve_config_path(sys.argv[2])
        import_yaml(sys.argv[1], resolved)
        if sys.argv[3] == "checked":
            # Installs nothing new, but USES the quiet filter on every record the check logs.
            from zfsbackup.config.store.migrate import check_schema
            from zfsbackup.config.store.paths import open_config_connection
            with open_config_connection(resolved, readonly=True) as conn:
                check_schema(conn)
        # Falls off the end: no dispose_all(), no engine.dispose(), no os._exit().
        print("done")
    ''')

    @pytest.mark.parametrize("mode", ["imported_only", "checked"])
    def test_a_process_that_exits_without_dispose_all_leaves_no_sidecars(self, tmp_path, mode):
        yaml_path = tmp_path / "in.yaml"
        yaml_path.write_text(NEW_YAML)
        db = tmp_path / "store" / "config.db"
        script = tmp_path / "exit_check.py"
        script.write_text(self.EXIT_SCRIPT)
        result = subprocess.run(
            [sys.executable, str(script), str(yaml_path), str(db), mode], cwd=ROOT, text=True,
            capture_output=True, timeout=120, env=hermetic_env(PYTHONPATH=str(ROOT)),
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "done"
        assert db.exists()
        left = sorted(f.name for f in db.parent.iterdir())
        assert not [n for n in left if n.endswith(("-wal", "-shm", "-journal"))], left
        # The row really was committed to the main file (nothing is waiting in a WAL).
        conn = sqlite3.connect(str(db))
        try:
            assert conn.execute("SELECT count(*) FROM datasets").fetchone() == (1,)
        finally:
            conn.close()

    @staticmethod
    def installed_filters():
        from zfsbackup.config.store import _alembic_quiet

        return _alembic_quiet, [
            f for f in logging.getLogger("alembic.runtime.migration").filters
            if getattr(f, _alembic_quiet.QUIET_FILTER_MARKER, False) is True]

    def test_the_installed_filter_is_defined_in_the_leaf_module(self):
        quiet_module, filters = self.installed_filters()
        assert len(filters) == 1
        (installed,) = filters
        assert type(installed).__name__ == "_QuietWhileChecking"
        assert type(installed).filter.__globals__ is quiet_module.__dict__
        assert type(installed).__module__ == quiet_module.__name__
        # migrate re-exports the names callers and tests use; they are the same objects
        assert migrate_module._QuietWhileChecking is quiet_module.QuietWhileChecking
        assert migrate_module._quiet is quiet_module.quiet
        assert migrate_module._quiet_alembic is quiet_module.quiet_alembic

    def test_the_filters_globals_reach_no_store_module_and_no_engine(self):
        import types

        from zfsbackup.config.store import db as store_db
        from sqlalchemy.engine import Connection, Engine

        quiet_module, (installed,) = self.installed_filters()
        namespace = type(installed).filter.__globals__
        assert namespace is quiet_module.__dict__
        # The pin the bug made: store modules (and so `db._ENGINES`) in the filter's globals.
        modules = {k: v.__name__ for k, v in namespace.items() if isinstance(v, types.ModuleType)}
        assert not [n for n in modules.values() if n.startswith("zfsbackup")], modules
        assert "_ENGINES" not in namespace and "get_engine" not in namespace
        for name, value in namespace.items():
            assert not isinstance(value, (Engine, Connection)), name
            assert value is not store_db._ENGINES, name
            owner = getattr(value, "__module__", None) or ""
            assert not owner.startswith("zfsbackup") or owner == quiet_module.__name__, (name, value)
        # nor does the instance itself hold anything but its own (empty) state
        assert not [k for k, v in vars(installed).items() if isinstance(v, (Engine, Connection))]


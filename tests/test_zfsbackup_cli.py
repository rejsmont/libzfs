"""`zfsbackup-config` (plan Phase 2, item 10.2) -- the config CLI skeleton and
its `import` / `export` / `migrate` commands, through Click's `CliRunner`.

What is covered here (the YAML renderer has its own file,
`test_zfsbackup_cli_render.py`):

- ``TestSkeleton``        help, usage errors, `python -m`, the console-script
                          declaration, "imports neither libzfseasy nor the daemon".
- ``TestPathResolution``  `-c` > `$ZFSBACKUP_CONFIG` > default; `-c`/`-v` before
                          and after the subcommand; the store's `.yaml` hint run
                          verbatim; only `import` creates a database.
- ``TestErrorMapping``    `errors.py`: every exception -> message + exit code,
                          notes, tracebacks only with `-v`, "installation error".
- ``TestWarnings``        library log records -> `warning: ...` lines, once each.
- ``TestImport``          full replace, the replace warning, the pre-import backup
                          (name, 0660, O_EXCL, suffixes, identical to `export`),
                          backup failure rollback, `--no-backup`, an unreadable
                          outgoing configuration.
- ``TestExport``          stdout / `-o` / `--force`, read-only, unloadable config.
- ``TestMigrate``         up to date, `--check`, no recorded revision, messages.
- ``TestPackaging``       wheel contents / installed entry point (`slow`).

Every database here is built by the CLI itself (`import`) or, for the states
the CLI refuses to produce, tampered with a raw `sqlite3` connection that has
`foreign_keys=ON` -- so a cascade tamper behaves like the real store's.

Mutation counts (scratch copy of the tree, one revert at a time; failing tests
across this file and `test_zfsbackup_cli_render.py`): `-c` ignored by the
group-level resolve 56 / by the subcommand 61; subcommand `-v` ignored 4;
ConfigPathError->1 32; CreatedConcurrently not 75 2; "database is locked" not 75
7; bare RuntimeError not 78 3; OutgoingConfigUnreadable->78 4; notes not
printed 6; traceback without `-v` 6; "installation error" label 2; usage->1 1;
no warning dedupe 1; the store's own replace warning shown 1; backup `fchmod`
removed 2; backup without O_EXCL 4; no `-N` suffix 3; backup mode 0666 3;
backup always skipped 19 / `--no-backup` ignored 3; unreadable-outgoing note
dropped 2; `--no-backup` hint dropped from the render-failure message 1 / the
write-failure message 2; partial backup not unlinked 1; backup name stem 14 /
written to the cwd 14; replace warning without counts 2 / without the backup
path 1; retry note always 3 / never 1; `export -o` overwriting without `--force`
2 / `--force` ignored 1 / not truncating 1; `-o -` not stdout 1; unloadable
export unwrapped 1; `migrate --check` exiting 0 when behind 2; no restart hint
4; no-op reported as a migration 3; `--check` migrating 4; `dispose_all` not
registered 4; log handler left attached 6 / level not restored 1;
KeyboardInterrupt unmapped 1.

Item 10.4 (same method; failing tests across the store, store-upgrade, CLI and
daemon files): bare `RuntimeError` mapped to 78 3 / labelled an installation
error 2; corrupt-body `DatabaseError` not 78 8 / not routed through
`config_error_message` 1; retry note added for an unusable database 1;
`SchemaOutOfDate` given the daemon's sentence by the CLI 2; `migrate` success
text reverted 3; `--check` verdict prefix dropped 2; cleanup note printed twice
1; ^C notes not printed 4 / import ^C worded as plain "interrupted" 5 / hint
without `-c` 3 / hint not shell-quoted 1; backup fd left open when `fchmod`
fails 2 / only `OSError` cleaned up 2 / directory not fsynced 3; `export -o`
link replaced by rename (clobbers a mid-run target) 11 / `--force` symlink not
followed 2 / dangling symlink not refused (`exists` for `lexists`) 1 / mode not
kept 2 / temp left behind 6 / no-hard-link fallback dropped 2 / `OSError` not
wrapped 2. Store side: no-revision tables not refused 22 / `upgrade_schema`
migrating an empty file 3 / foreign -> split-brain 8 / ours-only -> foreign 7;
reader switching to WAL 15 / writer ignoring the foreign-table guard 9 / reader
warning per connection 1 / never 6; `in_transaction` guard removed 1;
`MigrationPackagingError` a `SchemaError` 8 / a bare `RuntimeError` 2; the
daemon sentence dropped 3 / added to every `SchemaError` 4; interrupted-import
wording 3 + 2 + 3. Re-adding `session.expunge_all()` to importer phase 3 fails 0
tests (the claim that it guarded nothing observable holds).

Item 10.5 (same method; failing tests across `test_zfsbackup_store.py`,
`test_zfsbackup_store_upgrade.py` and this file): writer always switches to WAL
43 / empty `alembic_version` + foreign tables switched 34 / any recorded
revision switched 13 / `alembic_version` without `version_num` switched 4 /
the 10.4 "has an `alembic_version` table" rule 20 / subset of our tables
reported as split-brain 9 / hard-coded known-revision set 8 / mismatch wording
reverted 2 / reader warning wording reverted 6; `export --force` renames over a
FIFO 5 / in-place open with `O_CREAT|O_TRUNC` 1 / temp name derived from the
target 5 / `fchown` EPERM not ignored 1 / every `OSError` ignored 1 / `fchmod`
before `fchown` 1 / no `fchown` 2 / mode not kept 3 / tolerated directory-fsync
errnos emptied 6 / every directory-fsync error tolerated 3 / export directory
not fsynced 3 / directory fsync before the rename 3 / temp not removed on
failure 11; `is_database_unusable` as `isinstance` 12 / damaged message not cut
to one line 1 / descriptor not cleared before `fdopen` 1 (export) + 1 (backup);
`_without_applied_claim` the identity 8 / notes not trimmed 4 / empty notes
printed 1; SIGTERM: second signal not ignored 2 / handler not restored 4 /
parse-time Abort not remapped 4 / "seen" flag never set 4 / exit 130 for
SIGTERM 2 / worded "interrupted" 6 / flag not reset per `main()` 3 / handler
not installed 7; `migrate` restart text reverted 3. Any SQLAlchemyError other than
OperationalError, IntegrityError and a bare DatabaseError (e.g. ProgrammingError) is
an internal error: exit 1, no SQL, no sqlalche.me link.
"""

import configparser
import errno
import gc
import hashlib
import io
import logging
import os
import re
import select
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
import textwrap
import threading
import time
import tomllib
import warnings
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import click
import pytest
import yaml
from click.testing import CliRunner
from sqlalchemy import exc as sa_exc
from sqlalchemy.exc import DatabaseError, IntegrityError, OperationalError, SQLAlchemyError

from zfsbackup.cli import errors as cli_errors
from zfsbackup.cli import main as cli_main
from zfsbackup.cli.main import cli
from zfsbackup.config import BackupConfig
from zfsbackup.config.store import (
    CONFIG_PATH_ENV,
    ConfigDbCreatedConcurrently,
    ConfigDbIsYaml,
    ConfigDbNotFound,
    ConfigPathError,
    EXPECTED_HEAD,
    ImportResult,
    KNOWN_REVISIONS,
    MigrationPackagingError,
    NotAConfigDatabase,
    OutgoingConfigUnreadable,
    SchemaIntegrityError,
    SchemaOutOfDate,
    SchemaSplitBrain,
    SchemaUpgrade,
    SchemaVersionMismatch,
    dispose_all,
)
from zfsbackup.config.store import migrate as store_migrate
from zfsbackup.config.store import paths as store_paths

REPO = Path(__file__).resolve().parent.parent
EXAMPLE_YAML = REPO / "zfsbackup" / "config.example.yaml"
TEST_YAML = REPO / "zfsbackup" / "config.test.yaml"

#: A second, different dataset configuration, so "which database did this read"
#: and "what did the replace discard" are visible in the output.
MINI_YAML = """\
snapshot_prefix: "{prefix}"
check_interval: "5m"
datasets:
{datasets}
"""


#: What `migrate` prints after a real upgrade (item 10.5 wording).
RESTART_HINT = (
    "Restart any running zfsbackup daemon now -- it is still running the old "
    "schema check."
)


def mini_yaml(path: Path, prefix: str = "mini", names=("tank/a",)) -> Path:
    datasets = "\n".join(
        f'  - name: "{name}"\n    frequency: "1h"\n    retention:\n      "1d": "30d"'
        for name in names
    )
    path.write_text(MINI_YAML.format(prefix=prefix, datasets=datasets))
    return path


# --------------------------------------------------------------------------
# Fixtures and helpers
# --------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    """No test may see the developer's `$ZFSBACKUP_CONFIG` or touch
    `/var/lib/zfsbackup`: the default path is redirected under `tmp_path`, and
    the working directory is `tmp_path` so relative `-c` values are hermetic."""
    monkeypatch.delenv(CONFIG_PATH_ENV, raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(store_paths, "DEFAULT_CONFIG_DB", tmp_path / "default" / "config.db")
    yield
    dispose_all()


@pytest.fixture
def db(tmp_path) -> Path:
    return tmp_path / "cfg" / "config.db"


class Cli:
    """`CliRunner` with the two checks every call here wants: nothing but
    `SystemExit` may escape a command (the error mapper owns all failures), and
    stdout / stderr are kept apart."""

    def __init__(self) -> None:
        self.runner = CliRunner()

    def __call__(self, *args, env=None, input=None):
        result = self.runner.invoke(
            cli, [str(a) for a in args], env=env, input=input, prog_name="zfsbackup-config"
        )
        assert result.exception is None or isinstance(result.exception, SystemExit), (
            f"an exception escaped the CLI: {result.exception!r}\n{result.stderr}"
        )
        return result


@pytest.fixture
def run() -> Cli:
    return Cli()


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sidecars(database: Path):
    return sorted(
        p.name for suffix in ("-wal", "-shm", "-journal")
        if (p := database.with_name(database.name + suffix)).exists()
    )


def backups(database: Path):
    return sorted(database.parent.glob(f"{database.name}.pre-import-*.yaml"))


@contextmanager
def raw_db(database: Path):
    """A raw sqlite3 connection with the project's `foreign_keys=ON`, for
    tampering with states the CLI itself never produces."""
    conn = sqlite3.connect(database)
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def import_ok(run, database: Path, source: Path = EXAMPLE_YAML, *extra):
    result = run("import", source, "-c", database, *extra)
    assert result.exit_code == 0, result.stderr
    return result


def exported(run, database: Path) -> str:
    result = run("export", "-c", database)
    assert result.exit_code == 0, result.stderr
    return result.stdout


def stderr_lines(result):
    return [line for line in result.stderr.splitlines() if line]


def empty_stored_config(database: Path) -> None:
    """Leave a database whose stored configuration cannot be loaded: a
    `global_settings` row and zero datasets (`load_config` refuses it)."""
    with raw_db(database) as conn:
        conn.execute("DELETE FROM datasets")
        conn.execute("DELETE FROM destinations")


def blank_sqlite(path: Path) -> Path:
    """A valid SQLite file with no tables and no `alembic_version`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    # WAL like every database the store creates: the store's engines switch a
    # rollback-journal file to WAL on first open, which is a header write.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE t (x)")
    conn.execute("DROP TABLE t")
    conn.commit()
    conn.close()
    return path


#: The id of the synthetic second revision `stacked_revision` stacks on the head.
SYNTH = "bbbbbbbbbbbb"

_SYNTH_REVISION = '''"""synthetic revision for tests"""
revision = "{rid}"
down_revision = "{down}"
branch_labels = None
depends_on = None

import sqlalchemy as sa
from alembic import op


def upgrade():
    with op.batch_alter_table("datasets") as batch:
        batch.add_column(sa.Column("note", sa.Text(), nullable=True))


def downgrade():
    pass
'''


@pytest.fixture
def stacked_revision(tmp_path, monkeypatch):
    """Factory: after `stacked_revision()` the installed package "expects" a
    second revision, so a database built at the real head is genuinely *behind*.
    Copies the real migrations tree and moves `_MIGRATIONS_DIR`, `EXPECTED_HEAD`
    and `KNOWN_REVISIONS` together (see test_zfsbackup_store_upgrade.py)."""

    def make() -> Path:
        scripts = tmp_path / "scripts"
        shutil.copytree(
            store_migrate._MIGRATIONS_DIR, scripts,
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        (scripts / "versions" / "synth.py").write_text(
            _SYNTH_REVISION.format(rid=SYNTH, down=EXPECTED_HEAD)
        )
        monkeypatch.setattr(store_migrate, "_MIGRATIONS_DIR", scripts)
        monkeypatch.setattr(store_migrate, "EXPECTED_HEAD", SYNTH)
        monkeypatch.setattr(store_migrate, "KNOWN_REVISIONS", frozenset(KNOWN_REVISIONS | {SYNTH}))
        return scripts

    return make


def foreign_sqlite(path: Path, *, wal: bool = False) -> Path:
    """A rollback-journal SQLite file holding an unrelated `orders` table (the
    item-10 review's reproduction): not a zfsbackup config database."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    if wal:
        conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, x TEXT)")
    conn.execute("INSERT INTO orders VALUES (1, 'a')")
    conn.commit()
    conn.close()
    os.chmod(path, 0o660)
    return path


class _Frozen(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 10, 17, 12, 0, 0, tzinfo=tz or timezone.utc)


STAMP = "20261017T120000Z"


def _locked_error() -> OperationalError:
    return OperationalError("BEGIN IMMEDIATE", {}, Exception("database is locked"))


# --------------------------------------------------------------------------
# skeleton
# --------------------------------------------------------------------------


class TestSkeleton:
    def test_help_lists_exactly_the_three_commands(self, run):
        result = run("--help")
        assert result.exit_code == 0
        assert sorted(cli.commands) == ["export", "import", "migrate"]
        for name in ("import", "export", "migrate"):
            assert name in result.stdout

    def test_short_help_flag(self, run):
        result = run("-h")
        assert result.exit_code == 0
        assert "Usage: zfsbackup-config" in result.stdout

    def test_help_documents_the_exit_codes_and_env_var(self, run):
        out = run("--help").stdout
        for fragment in ("75", "78", CONFIG_PATH_ENV):
            assert fragment in out

    @pytest.mark.parametrize("sub", ["import", "export", "migrate"])
    def test_every_subcommand_has_help_and_the_common_options(self, run, sub):
        result = run(sub, "--help")
        assert result.exit_code == 0
        assert "-c, --config" in result.stdout
        assert "-v, --verbose" in result.stdout

    def test_no_arguments_prints_usage(self, run):
        result = run()
        # Click's group with no subcommand shows help (exit 0 or 2 depending on
        # version); what matters is that it lists the commands, not a traceback.
        assert "Commands:" in (result.stdout + result.stderr)
        assert "Traceback" not in result.stderr

    def test_unknown_subcommand_is_a_usage_error(self, run):
        result = run("frobnicate")
        assert result.exit_code == 2
        assert "No such command" in result.stderr

    def test_unknown_option_is_a_usage_error(self, run):
        assert run("export", "--frobnicate").exit_code == 2

    def test_import_without_yaml_argument_is_a_usage_error(self, run, db):
        result = run("import", "-c", db)
        assert result.exit_code == 2
        assert not db.parent.exists()

    def test_nonexistent_yaml_argument_is_a_usage_error_and_creates_nothing(
        self, run, db, tmp_path
    ):
        result = run("import", tmp_path / "missing.yaml", "-c", db)
        assert result.exit_code == 2
        assert "does not exist" in result.stderr
        assert not db.parent.exists()

    def test_directory_as_yaml_argument_is_a_usage_error(self, run, db, tmp_path):
        result = run("import", tmp_path, "-c", db)
        assert result.exit_code == 2
        assert not db.exists()

    def test_main_is_the_console_entry_point(self):
        with pytest.raises(SystemExit) as exit_info:
            cli_main.main(["--help"])
        assert exit_info.value.code == 0
        with pytest.raises(SystemExit) as exit_info:
            cli_main.main(["frobnicate"])
        assert exit_info.value.code == 2

    def test_cli_leaves_no_log_handler_or_level_behind(self, run, db):
        import_ok(run, db)
        logger = logging.getLogger("zfsbackup")
        handlers, level = list(logger.handlers), logger.level
        for args in (("export", "-v"), ("migrate", "-v"), ("export",)):
            assert run(*args, "-c", db).exit_code == 0
            assert logger.handlers == handlers
            assert logger.level == level
        # ...and a failing invocation detaches it too.
        assert run("export", "-c", db.with_name("missing.db"), "-v").exit_code == 78
        assert logger.handlers == handlers
        assert logger.level == level


class TestSkeletonSubprocess:
    """Real interpreters with a scrubbed environment: no `ZFS_CMD`,
    `ZPOOL_CMD` or `ZFSBACKUP_CONFIG`."""

    @staticmethod
    def env():
        env = {
            k: v for k, v in os.environ.items()
            if k not in ("ZFS_CMD", "ZPOOL_CMD", CONFIG_PATH_ENV)
        }
        env["PYTHONPATH"] = str(REPO)
        return env

    def python(self, *args, cwd):
        return subprocess.run(
            [sys.executable, *args], cwd=cwd, env=self.env(),
            capture_output=True, text=True, timeout=120,
        )

    @pytest.mark.integration
    def test_python_dash_m_help(self, tmp_path):
        proc = self.python("-m", "zfsbackup.cli", "--help", cwd=tmp_path)
        assert proc.returncode == 0, proc.stderr
        assert "Usage: zfsbackup-config" in proc.stdout

    @pytest.mark.integration
    def test_python_dash_m_exits_with_the_commands_code(self, tmp_path):
        proc = self.python("-m", "zfsbackup.cli", "-c", "missing.db", "export", cwd=tmp_path)
        assert proc.returncode == 78
        assert not (tmp_path / "missing.db").exists()
        proc = self.python("-m", "zfsbackup.cli", "frobnicate", cwd=tmp_path)
        assert proc.returncode == 2

    @pytest.mark.integration
    def test_cli_imports_neither_libzfseasy_nor_the_daemon(self, tmp_path):
        code = (
            "import sys\n"
            "import zfsbackup.cli.main, zfsbackup.cli.render, zfsbackup.cli.errors\n"
            "bad = sorted(m for m in sys.modules if m.split('.')[0] == 'libzfseasy' "
            "or m in ('zfsbackup.daemon', 'zfsbackup.workers', "
            "'zfsbackup.backup_manager', 'zfsbackup.remote', 'zfsbackup.api'))\n"
            "print(bad)\n"
        )
        proc = self.python("-c", code, cwd=tmp_path)
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.strip() == "[]"

    @pytest.mark.integration
    def test_full_workflow_runs_with_no_zfs_binaries_configured(self, tmp_path):
        """The whole CLI path -- import, export, migrate -- in a process where
        `ZFS_CMD`/`ZPOOL_CMD` are unset; if anything imported `libzfseasy` it
        would raise at import time when no `zfs` is on PATH, and in any case
        the module check above would have failed."""
        target = tmp_path / "cfg" / "c.db"
        env = self.env()
        env["PATH"] = os.path.dirname(sys.executable)  # no zfs/zpool anywhere
        steps = [
            ["import", str(EXAMPLE_YAML), "-c", str(target)],
            ["export", "-c", str(target), "-o", str(tmp_path / "out.yaml")],
            ["migrate", "--check", "-c", str(target)],
        ]
        for step in steps:
            proc = subprocess.run(
                [sys.executable, "-m", "zfsbackup.cli", *step], cwd=tmp_path, env=env,
                capture_output=True, text=True, timeout=120,
            )
            assert proc.returncode == 0, (step, proc.stderr)
        assert (tmp_path / "out.yaml").exists()


# --------------------------------------------------------------------------
# path resolution
# --------------------------------------------------------------------------


class TestPathResolution:
    @pytest.fixture
    def three(self, run, tmp_path):
        """Three databases with different snapshot prefixes."""
        paths = {}
        for tag in ("flag", "env", "default"):
            target = (
                tmp_path / "default" / "config.db" if tag == "default"
                else tmp_path / tag / "config.db"
            )
            source = mini_yaml(tmp_path / f"{tag}.yaml", prefix=tag)
            import_ok(run, target, source)
            paths[tag] = target
        return paths

    @staticmethod
    def prefix_of(text: str) -> str:
        return yaml.safe_load(text)["snapshot_prefix"]

    def test_dash_c_beats_env_beats_default(self, run, three):
        env = {CONFIG_PATH_ENV: str(three["env"])}
        assert self.prefix_of(run("export", "-c", three["flag"], env=env).stdout) == "flag"
        assert self.prefix_of(run("export", env=env).stdout) == "env"
        assert self.prefix_of(run("export").stdout) == "default"

    def test_empty_dash_c_falls_through_to_the_env_var(self, run, three):
        env = {CONFIG_PATH_ENV: str(three["env"])}
        assert self.prefix_of(run("-c", "", "export", env=env).stdout) == "env"
        assert self.prefix_of(run("export", "-c", "", env=env).stdout) == "env"

    def test_empty_env_var_is_unset(self, run, three):
        assert self.prefix_of(run("export", env={CONFIG_PATH_ENV: ""}).stdout) == "default"

    def test_env_var_that_looks_like_a_url_is_a_config_error(self, run):
        result = run("export", env={CONFIG_PATH_ENV: "sqlite:///x.db"})
        assert result.exit_code == 78
        assert CONFIG_PATH_ENV in result.stderr and "ZFSBACKUP_DB_URL" in result.stderr

    def test_import_with_no_flag_creates_the_default_path(self, run, tmp_path):
        default = tmp_path / "default" / "config.db"
        assert run("import", EXAMPLE_YAML).exit_code == 0
        assert default.exists()

    def test_import_honours_the_env_var(self, run, tmp_path):
        target = tmp_path / "viaenv" / "config.db"
        result = run("import", EXAMPLE_YAML, env={CONFIG_PATH_ENV: str(target)})
        assert result.exit_code == 0, result.stderr
        assert target.exists()
        assert not (tmp_path / "default").exists()

    def test_relative_dash_c_is_resolved_against_the_cwd(self, run, tmp_path):
        assert run("import", EXAMPLE_YAML, "-c", "rel/c.db").exit_code == 0
        assert (tmp_path / "rel" / "c.db").exists()

    @pytest.mark.parametrize("position", ["before", "after", "both"])
    @pytest.mark.parametrize("sub", [["export"], ["migrate", "--check"]])
    def test_dash_c_before_and_after_the_subcommand(self, run, db, sub, position):
        import_ok(run, db)
        args = {
            "before": ["-c", db, *sub],
            "after": [*sub, "-c", db],
            "both": ["-c", db, *sub, "-c", db],
        }[position]
        result = run(*args)
        assert result.exit_code == 0, result.stderr

    def test_dash_c_after_the_subcommand_wins_over_one_before(self, run, tmp_path):
        a = tmp_path / "a" / "c.db"
        b = tmp_path / "b" / "c.db"
        import_ok(run, a, mini_yaml(tmp_path / "a.yaml", prefix="aaa"))
        import_ok(run, b, mini_yaml(tmp_path / "b.yaml", prefix="bbb"))
        out = run("-c", a, "export", "-c", b).stdout
        assert yaml.safe_load(out)["snapshot_prefix"] == "bbb"

    def test_import_dash_c_after_the_yaml_argument(self, run, db):
        """The exact shape the store's `.yaml` hint prints: `import <yaml> -c <db>`."""
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert result.exit_code == 0
        assert db.exists()
        assert run("import", "-c", db, str(EXAMPLE_YAML)).exit_code == 0

    @pytest.mark.parametrize("where", ["before", "after"])
    def test_verbose_before_and_after_the_subcommand(self, run, db, monkeypatch, where):
        import_ok(run, db)

        def boom(resolved):
            raise ValueError("kaboom")

        monkeypatch.setattr(cli_main, "load_runtime_config", boom)
        args = ["-v", "export", "-c", db] if where == "before" else ["export", "-c", db, "-v"]
        result = run(*args)
        assert result.exit_code == 1
        assert "Traceback" in result.stderr and "kaboom" in result.stderr

    def test_the_stores_yaml_hint_works_verbatim(self, run, tmp_path):
        """Take the command out of `ConfigDbIsYaml`'s own message and run it."""
        yaml_path = tmp_path / "legacy.yaml"
        yaml_path.write_text(EXAMPLE_YAML.read_text())
        refused = run("export", "-c", "legacy.yaml")
        assert refused.exit_code == 78
        message = refused.stderr
        marker = "with: "
        line = message[message.index(marker) + len(marker):]
        command = line[: line.index("  and (2)")]
        assert command.startswith("zfsbackup-config import legacy.yaml -c ")
        args = command.split()[1:]
        assert not (tmp_path / "legacy.db").exists()
        result = run(*args)
        assert result.exit_code == 0, result.stderr
        assert (tmp_path / "legacy.db").exists()
        assert run("export", "-c", "legacy.db").exit_code == 0

    @pytest.mark.parametrize("suffix", [".yaml", ".yml"])
    @pytest.mark.parametrize("sub", [["export"], ["migrate"], ["migrate", "--check"]])
    def test_yaml_suffixed_dash_c_is_refused_with_the_hint(self, run, tmp_path, sub, suffix):
        target = tmp_path / f"c{suffix}"
        target.write_text("datasets: []\n")
        before = target.read_text()
        result = run(*sub, "-c", target)
        assert result.exit_code == 78
        assert "zfsbackup-config import" in result.stderr
        assert target.read_text() == before

    @pytest.mark.parametrize("suffix", [".yaml", ".yml"])
    def test_import_into_a_yaml_suffixed_target_creates_nothing(self, run, tmp_path, suffix):
        target = tmp_path / "newdir" / f"c{suffix}"
        result = run("import", EXAMPLE_YAML, "-c", target)
        assert result.exit_code == 78
        assert "zfsbackup-config import" in result.stderr
        assert not target.parent.exists()

    @pytest.mark.parametrize("sub", [["export"], ["migrate"], ["migrate", "--check"]])
    def test_only_import_creates_a_database(self, run, tmp_path, sub):
        target = tmp_path / "nodir" / "missing.db"
        before = sorted(os.listdir(tmp_path))
        result = run(*sub, "-c", target)
        assert result.exit_code == 78
        assert "No config database found" in result.stderr
        assert str(target.name) in result.stderr
        assert sorted(os.listdir(tmp_path)) == before

    def test_missing_default_database_is_not_created_either(self, run, tmp_path):
        result = run("export")
        assert result.exit_code == 78
        assert "default" in result.stderr
        assert not (tmp_path / "default").exists()

    def test_zero_byte_file_is_not_a_database_and_is_left_alone(self, run, tmp_path):
        target = tmp_path / "zero.db"
        target.touch()
        for sub in (["export"], ["migrate"]):
            result = run(*sub, "-c", target)
            assert result.exit_code == 78
            # Importing a zero-byte file "into itself" would be the wrong advice.
            assert f"zfsbackup-config import {target}" not in result.stderr
            assert target.stat().st_size == 0

    def test_directory_as_dash_c(self, run, tmp_path):
        result = run("export", "-c", tmp_path)
        assert result.exit_code == 78
        assert "not a regular file" in result.stderr

    def test_not_a_sqlite_file(self, run, tmp_path):
        target = tmp_path / "garbage.db"
        target.write_bytes(b"this is not a database" * 100)
        before = target.read_bytes()
        result = run("migrate", "-c", target)
        assert result.exit_code == 78
        assert target.read_bytes() == before


# --------------------------------------------------------------------------
# errors.py -- the mapping, unit level
# --------------------------------------------------------------------------


class TestErrorMapping:
    @pytest.mark.parametrize(
        "exc, code",
        [
            (cli_errors.CliError("x"), 1),
            (cli_errors.BackupFailed("x"), 1),
            (ValueError("x"), 1),
            (yaml.YAMLError("x"), 1),
            (FileNotFoundError("x"), 1),
            (KeyError("x"), 1),
            (Exception("x"), 1),
            (OutgoingConfigUnreadable("x"), 1),
            (click.UsageError("x"), 2),
            (ConfigDbCreatedConcurrently("x"), 75),
            (_locked_error(), 75),
            (OperationalError("s", {}, Exception("database table is locked")), 75),
            (OperationalError("s", {}, Exception("database is BUSY")), 75),
            (OperationalError("s", {}, Exception("no such table: x")), 78),
            (OperationalError("s", {}, Exception("unable to open database file")), 78),
            (ConfigPathError("x"), 78),
            (ConfigDbNotFound("x"), 78),
            (ConfigDbIsYaml("x"), 78),
            (SchemaOutOfDate("x"), 78),
            (SchemaVersionMismatch("x"), 78),
            (SchemaIntegrityError("x"), 78),
            (NotAConfigDatabase("x"), 78),
            (SchemaSplitBrain("x"), 78),
            (MigrationPackagingError("scripts disagree"), 78),
            # Item 10.4: only MigrationPackagingError is an installation
            # problem; a bare RuntimeError is a bug.
            (RuntimeError("scripts disagree"), 1),
            (SQLAlchemyError("x"), 1),
            # A valid header over a corrupt body / a file that is not SQLite:
            # the FILE is unusable (78); a constraint is the input (1).
            (DatabaseError("s", {}, Exception("database disk image is malformed")), 78),
            (DatabaseError("s", {}, Exception("file is not a database")), 78),
            (IntegrityError("s", {}, Exception("UNIQUE constraint failed: datasets.name")), 1),
        ],
        ids=lambda v: v if isinstance(v, int) else type(v).__name__,
    )
    def test_exit_code_for(self, exc, code):
        assert cli_errors.exit_code_for(exc) == code

    def test_exit_code_constants(self):
        assert (
            cli_errors.EXIT_OK, cli_errors.EXIT_FAILURE, cli_errors.EXIT_USAGE,
            cli_errors.EXIT_BUSY, cli_errors.EXIT_CONFIG,
        ) == (0, 1, 2, 75, 78)

    def test_outgoing_unreadable_is_a_refusal_not_an_unusable_database(self):
        # It IS a ConfigPathError (the daemon's `except` covers it), and must
        # still map to 1, not to ConfigPathError's 78.
        assert isinstance(OutgoingConfigUnreadable("x"), ConfigPathError)
        assert cli_errors.exit_code_for(OutgoingConfigUnreadable("x")) == 1

    def test_report_prints_message_then_notes_without_a_traceback(self):
        exc = ValueError("the message")
        exc.add_note("first note")
        exc.add_note("second note")
        buf = io.StringIO()
        code = cli_errors.report(exc, None, stream=buf)
        assert code == 1
        assert buf.getvalue().splitlines() == ["error: the message", "first note", "second note"]

    def test_report_verbose_appends_the_traceback(self):
        try:
            raise ValueError("deep")
        except ValueError as exc:
            buf = io.StringIO()
            cli_errors.report(exc, None, verbose=True, stream=buf)
        assert "Traceback (most recent call last)" in buf.getvalue()
        assert buf.getvalue().startswith("error: deep")

    def test_migration_packaging_error_is_labelled_an_installation_error(self):
        buf = io.StringIO()
        code = cli_errors.report(MigrationPackagingError("two heads"), None, stream=buf)
        assert code == 78
        out = buf.getvalue()
        assert out.startswith("installation error: two heads")
        assert "Reinstall zfsbackup." in out
        # Item 10.4: the report does not claim anything about the database
        # (migrate refuses before opening it, but import may have run phase 2).
        assert "not touched" not in out

    def test_bare_runtime_error_is_an_internal_error_not_an_installation_error(self):
        buf = io.StringIO()
        code = cli_errors.report(RuntimeError("some bug"), None, stream=buf)
        assert code == 1
        out = buf.getvalue()
        assert out.startswith("internal error: some bug")
        assert "installation" not in out
        assert "-v" in out

    def test_schema_out_of_date_is_printed_as_the_stores_own_text(self):
        """The daemon's `config_error_message` would append "start the daemon
        again", which is wrong advice for the CLI."""
        text = "/x/config.db is at schema revision a; this package expects b. Run `zfsbackup-config migrate`."
        buf = io.StringIO()
        assert cli_errors.report(SchemaOutOfDate(text), None, stream=buf) == 78
        assert buf.getvalue() == f"error: {text}\n"
        assert "daemon" not in buf.getvalue()

    def test_not_a_config_database_is_printed_verbatim_with_exit_78(self):
        buf = io.StringIO()
        assert cli_errors.report(NotAConfigDatabase("not a zfsbackup config database"), None, stream=buf) == 78
        assert buf.getvalue() == "error: not a zfsbackup config database\n"

    @pytest.mark.parametrize("reason", ["database disk image is malformed", "file is not a database"])
    def test_unusable_database_goes_through_the_daemons_wording(self, reason):
        """A `DatabaseError` that is not Integrity/Operational shares the
        daemon's "Failed to read the config database" message."""
        exc = DatabaseError("SELECT 1", {}, Exception(reason))
        assert cli_errors.is_database_unusable(exc)
        assert not cli_errors.is_database_unusable(IntegrityError("s", {}, Exception("x")))
        assert not cli_errors.is_database_unusable(OperationalError("s", {}, Exception("x")))
        assert not cli_errors.is_database_unusable(ValueError("x"))
        buf = io.StringIO()
        assert cli_errors.report(exc, None, stream=buf) == 78
        assert buf.getvalue().startswith("error: ")
        assert reason in buf.getvalue()

    def test_schema_errors_are_not_installation_errors(self):
        buf = io.StringIO()
        assert cli_errors.report(SchemaOutOfDate("behind"), None, stream=buf) == 78
        assert buf.getvalue().startswith("error: behind")
        assert "installation" not in buf.getvalue()

    def test_unknown_exception_is_labelled_internal_with_a_pointer_to_v(self):
        class Weird(Exception):
            pass

        buf = io.StringIO()
        assert cli_errors.report(Weird("odd"), None, stream=buf) == 1
        assert buf.getvalue().startswith("internal error: odd")
        assert "-v" in buf.getvalue()

    def test_yaml_error_message(self):
        buf = io.StringIO()
        cli_errors.report(yaml.YAMLError("bad token"), None, stream=buf)
        assert buf.getvalue().startswith("error: invalid YAML: bad token")

    def test_busy_message_tells_the_operator_to_retry(self):
        lines = cli_errors.format_error(_locked_error(), None)
        assert "retry" in lines[0].lower()

    def test_empty_message_falls_back_to_the_class_name(self):
        assert cli_errors.format_error(ValueError(), None) == ["ValueError"]


class TestErrorMappingThroughTheCli:
    """The same table, driven through real invocations with the importer stubbed
    to raise: what the operator sees and what the shell sees."""

    @pytest.fixture
    def failing_import(self, monkeypatch):
        def install(exc):
            def fake(*args, **kwargs):
                raise exc
            monkeypatch.setattr(cli_main, "import_yaml", fake)
        return install

    def test_created_concurrently_is_75(self, run, db, failing_import):
        failing_import(ConfigDbCreatedConcurrently("another import created it; retry"))
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert result.exit_code == 75
        assert "another import created it" in result.stderr

    def test_database_is_locked_is_75_with_a_retry_hint(self, run, db, failing_import):
        failing_import(_locked_error())
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert result.exit_code == 75
        assert "busy" in result.stderr.lower()

    def test_other_operational_error_is_78(self, run, db, failing_import):
        blank_sqlite(db)  # exists, so the post-hoc diagnosis finds nothing to blame
        failing_import(OperationalError("SELECT", {}, Exception("no such table: datasets")))
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert result.exit_code == 78
        assert "no such table" in result.stderr

    def test_schema_error_is_78(self, run, db, failing_import):
        failing_import(SchemaVersionMismatch("newer database"))
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert result.exit_code == 78
        assert result.stderr.startswith("error: newer database")

    def test_migration_packaging_error_is_an_installation_error(self, run, db, monkeypatch):
        import_ok(run, db)

        def packaging_defect(resolved):
            raise MigrationPackagingError("migration scripts disagree with EXPECTED_HEAD")

        monkeypatch.setattr(cli_main, "upgrade_schema", packaging_defect)
        result = run("migrate", "-c", db)
        assert result.exit_code == 78
        assert "installation error: migration scripts disagree" in result.stderr

    def test_bare_runtime_error_is_an_internal_error_exit_1(self, run, db, monkeypatch):
        import_ok(run, db)

        def bug(resolved):
            raise RuntimeError("something else entirely")

        monkeypatch.setattr(cli_main, "upgrade_schema", bug)
        result = run("migrate", "-c", db)
        assert result.exit_code == 1
        assert "internal error: something else entirely" in result.stderr
        assert "installation error" not in result.stderr

    def test_unknown_exception_does_not_traceback_without_v(self, run, db, failing_import):
        failing_import(ZeroDivisionError("bug"))
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert result.exit_code == 1
        assert "internal error" in result.stderr
        assert "Traceback" not in result.stderr
        verbose = run("import", EXAMPLE_YAML, "-c", db, "-v")
        assert "Traceback" in verbose.stderr

    def test_keyboard_interrupt_is_130(self, run, db, failing_import):
        failing_import(KeyboardInterrupt())
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert result.exit_code == 130
        assert "Traceback" not in result.stderr

    def test_notes_are_printed_after_the_message(self, run, db, failing_import):
        exc = ValueError("bad value")
        exc.add_note("cleanup skipped: the file at X was left in place")
        failing_import(exc)
        lines = stderr_lines(run("import", EXAMPLE_YAML, "-c", db))
        assert lines[0] == "error: bad value"
        assert lines[1] == "cleanup skipped: the file at X was left in place"

    def test_no_traceback_without_v_for_expected_failures(self, run, db):
        bad = db.parent.parent / "bad.yaml"
        bad.write_text("a: [\n")
        result = run("import", bad, "-c", db)
        assert result.exit_code == 1
        assert "Traceback" not in result.stderr
        assert result.stderr.startswith("error: invalid YAML")

    def test_retry_note_only_when_an_existing_database_may_have_been_touched(
        self, run, db, failing_import
    ):
        # Fresh target: nothing to replace, the reminder would be noise.
        failing_import(_locked_error())
        fresh = run("import", EXAMPLE_YAML, "-c", db)
        assert "replaces whatever is in the database" not in fresh.stderr
        # Existing database, failure after the import began: remind.
        failing_import(RuntimeError("mid-flight"))
        import_ok_db = db
        # (create it with the real importer first -- monkeypatch is per-test, so
        # build the file directly)
        import_ok_db.parent.mkdir(parents=True, exist_ok=True)
        blank_sqlite(import_ok_db)
        again = run("import", EXAMPLE_YAML, "-c", db)
        assert "replaces whatever is in the database" in again.stderr

    def test_no_retry_note_for_failures_before_the_write(self, run, db, failing_import):
        blank_sqlite(db)
        failing_import(ValueError("bad yaml value"))
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert "replaces whatever is in the database" not in result.stderr
        failing_import(ConfigDbIsYaml("looks like yaml"))
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert "replaces whatever is in the database" not in result.stderr


# --------------------------------------------------------------------------
# warnings
# --------------------------------------------------------------------------


class TestWarnings:
    @pytest.fixture
    def noisy_import(self, monkeypatch):
        def install(*calls, level=logging.WARNING):
            def fake(yaml_path, resolved, **kwargs):
                log = logging.getLogger("zfsbackup.config.store.importer")
                for message in calls:
                    log.log(level, "%s", message)
                return ImportResult(
                    created=True, head_revision="rev", replaced_datasets=0,
                    replaced_destinations=0,
                )
            monkeypatch.setattr(cli_main, "import_yaml", fake)
        return install

    def test_library_warning_is_printed_as_warning_on_stderr(self, run, db, noisy_import):
        noisy_import("something odd")
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert result.exit_code == 0
        assert "warning: something odd" in stderr_lines(result)
        assert "something odd" not in result.stdout

    def test_byte_identical_messages_are_printed_once(self, run, db, noisy_import):
        noisy_import("same text", "same text", "other text", "same text")
        lines = stderr_lines(run("import", EXAMPLE_YAML, "-c", db))
        assert lines.count("warning: same text") == 1
        assert lines.count("warning: other text") == 1

    def test_messages_differing_in_a_byte_are_both_printed(self, run, db, noisy_import):
        noisy_import("same text", "same text.")
        lines = stderr_lines(run("import", EXAMPLE_YAML, "-c", db))
        assert "warning: same text" in lines and "warning: same text." in lines

    def test_dedupe_does_not_leak_between_invocations(self, run, db, noisy_import):
        noisy_import("repeat me")
        for _ in range(2):
            assert "warning: repeat me" in stderr_lines(run("import", EXAMPLE_YAML, "-c", db))

    def test_errors_logged_at_error_level_are_labelled_error(self, run, db, noisy_import):
        noisy_import("it broke", level=logging.ERROR)
        assert "error: it broke" in stderr_lines(run("import", EXAMPLE_YAML, "-c", db))

    def test_info_is_hidden_without_v_and_shown_with_it(self, run, db, noisy_import):
        noisy_import("chatty detail", level=logging.INFO)
        assert "chatty detail" not in run("import", EXAMPLE_YAML, "-c", db).stderr
        assert "info: chatty detail" in run("import", EXAMPLE_YAML, "-c", db, "-v").stderr
        assert "info: chatty detail" in run("-v", "import", EXAMPLE_YAML, "-c", db).stderr

    def test_debug_is_shown_with_v(self, run, db, noisy_import):
        noisy_import("fine grained", level=logging.DEBUG)
        assert "debug: fine grained" in run("import", EXAMPLE_YAML, "-c", db, "-v").stderr
        assert "fine grained" not in run("import", EXAMPLE_YAML, "-c", db).stderr

    def test_from_file_warning_surfaces_once_through_a_real_import(self, run, db, tmp_path):
        source = tmp_path / "null_enabled.yaml"
        source.write_text(
            'check_interval: "5m"\n'
            "datasets:\n"
            '  - name: "tank/a"\n'
            "    enabled:\n"
            '    frequency: "1h"\n'
        )
        result = run("import", source, "-c", db)
        assert result.exit_code == 0, result.stderr
        hits = [l for l in stderr_lines(result) if "datasets[tank/a].enabled is empty" in l]
        assert len(hits) == 1
        assert hits[0].startswith("warning: ")

    def test_from_file_and_validate_warnings_both_surface_for_forever(self, run, db, tmp_path):
        source = tmp_path / "forever.yaml"
        source.write_text(
            'check_interval: "5m"\n'
            "datasets:\n"
            '  - name: "tank/a"\n'
            '    frequency: "1h"\n'
            "    retention: forever\n"
        )
        result = run("import", source, "-c", db)
        assert result.exit_code == 0
        lines = stderr_lines(result)
        assert sum("retention is 'forever'" in l for l in lines) == 1
        assert sum("retention is empty" in l for l in lines) == 1

    def test_warnings_precede_the_success_summary(self, run, db, tmp_path):
        source = tmp_path / "forever.yaml"
        source.write_text(
            'datasets:\n  - name: "tank/a"\n    retention: forever\n'
        )
        result = run("import", source, "-c", db)
        assert "warning:" in result.stderr
        assert result.stdout.startswith("Imported ")


# --------------------------------------------------------------------------
# import
# --------------------------------------------------------------------------


class TestImport:
    def test_first_import_creates_the_database(self, run, db):
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert result.exit_code == 0, result.stderr
        assert db.exists()
        assert f"new database {db.resolve()}" in result.stdout
        assert f"Schema revision: {EXPECTED_HEAD}" in result.stdout
        assert "replaced" not in result.stderr
        assert backups(db) == []
        assert sidecars(db) == []

    def test_first_import_content_equals_the_yaml(self, run, db):
        import_ok(run, db)
        stored = yaml.safe_load(exported(run, db))
        original = BackupConfig.from_file(EXAMPLE_YAML)
        assert [d["name"] for d in stored["datasets"]] == [d.name for d in original.datasets]

    def test_reimport_is_a_full_replace(self, run, db, tmp_path):
        import_ok(run, db, mini_yaml(tmp_path / "one.yaml", "one", ("tank/a", "tank/b")))
        import_ok(run, db, mini_yaml(tmp_path / "two.yaml", "two", ("tank/c",)))
        stored = yaml.safe_load(exported(run, db))
        assert stored["snapshot_prefix"] == "two"
        assert [d["name"] for d in stored["datasets"]] == ["tank/c"]

    def test_reimport_prints_one_replace_warning_with_counts_and_backup(self, run, db):
        import_ok(run, db, EXAMPLE_YAML)
        result = run("import", TEST_YAML, "-c", db)
        assert result.exit_code == 0, result.stderr
        warns = [l for l in stderr_lines(result) if l.startswith("warning: replaced")]
        assert len(warns) == 1
        (line,) = warns
        assert "(5 dataset(s), 0 destination(s))" in line
        assert "nothing of the previous configuration was kept" in line
        (backup,) = backups(db)
        assert str(backup) in line
        assert str(TEST_YAML) in line

    def test_stores_own_replacing_warning_is_suppressed(self, run, db):
        import_ok(run, db)
        result = run("import", TEST_YAML, "-c", db)
        assert "Replacing existing config store" not in result.stderr
        assert "will be discarded" not in result.stderr

    def test_replace_warning_counts_destinations(self, run, db, tmp_path):
        with_dest = tmp_path / "dest.yaml"
        with_dest.write_text(
            'destinations:\n  nas:\n    url: "http://nas:8080"\n  cloud:\n    url: "http://c"\n'
            'datasets:\n  - name: "tank/a"\n    frequency: "1h"\n    retention:\n      "1d": "30d"\n'
            '    remote:\n      - destination: "nas"\n'
        )
        import_ok(run, db, with_dest)
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert "(1 dataset(s), 2 destination(s))" in result.stderr

    def test_reimport_summary_lines(self, run, db):
        import_ok(run, db)
        result = run("import", TEST_YAML, "-c", db)
        assert f"Imported {TEST_YAML} into {db.resolve()}." in result.stdout
        assert f"Schema revision: {EXPECTED_HEAD}" in result.stdout

    def test_import_of_a_database_holding_no_configuration(self, run, db):
        import_ok(run, db)
        with raw_db(db) as conn:
            for table in ("datasets", "destinations", "remote_server", "global_settings"):
                conn.execute(f"DELETE FROM {table}")
        result = run("import", TEST_YAML, "-c", db)
        assert result.exit_code == 0, result.stderr
        assert "nothing to back up" in result.stderr
        assert backups(db) == []
        assert "libzfs_test/ROOT/macos" in exported(run, db)

    # ---- invalid input ----------------------------------------------------

    @pytest.mark.parametrize(
        "text, needle",
        [
            ("a: [\n", "invalid YAML"),
            ("", "Config file is empty"),
            ("[]\n", "Config file is empty"),
            ("datasets: []\n", "No datasets configured"),
            ('datasets:\n  - name: "tank/a"\n    frequency: "0m"\n', "frequency"),
            ('datasets:\n  - name: "tank/a"\n  - name: "tank/a"\n', "duplicate dataset name"),
            ("- just\n- a list\n", "mapping"),
        ],
        ids=["syntax", "empty-file", "empty-list", "no-datasets", "bad-frequency", "duplicate", "toplevel-list"],
    )
    def test_bad_yaml_is_exit_1_and_creates_nothing(self, run, db, tmp_path, text, needle):
        source = tmp_path / "bad.yaml"
        source.write_text(text)
        result = run("import", source, "-c", db)
        assert result.exit_code == 1
        assert needle in result.stderr
        assert not db.parent.exists()

    def test_bad_yaml_leaves_an_existing_database_untouched(self, run, db, tmp_path):
        import_ok(run, db)
        before_bytes, before_export = digest(db), exported(run, db)
        source = tmp_path / "bad.yaml"
        source.write_text("a: [\n")
        result = run("import", source, "-c", db)
        assert result.exit_code == 1
        assert digest(db) == before_bytes
        assert exported(run, db) == before_export
        assert backups(db) == []
        assert "replaces whatever is in the database" not in result.stderr

    def test_empty_yaml_over_existing_database_does_not_wipe_it(self, run, db, tmp_path):
        import_ok(run, db)
        source = tmp_path / "empty.yaml"
        source.write_text("")
        before = exported(run, db)
        assert run("import", source, "-c", db).exit_code == 1
        assert exported(run, db) == before
        assert backups(db) == []

    # ---- the pre-import backup -------------------------------------------

    def test_backup_name_and_location(self, run, db):
        import_ok(run, db)
        import_ok(run, db, TEST_YAML)
        (backup,) = backups(db)
        assert backup.parent == db.resolve().parent
        assert re.fullmatch(r"config\.db\.pre-import-\d{8}T\d{6}Z\.yaml", backup.name), backup.name

    @pytest.mark.parametrize("umask", [0o077, 0o000, 0o022])
    def test_backup_mode_is_0660_regardless_of_umask(self, run, db, umask):
        import_ok(run, db)
        old = os.umask(umask)
        try:
            result = run("import", TEST_YAML, "-c", db)
        finally:
            os.umask(old)
        assert result.exit_code == 0, result.stderr
        (backup,) = backups(db)
        assert stat.S_IMODE(backup.stat().st_mode) == 0o660

    def test_backup_is_byte_identical_to_export_of_the_outgoing_config(self, run, db, tmp_path):
        import_ok(run, db, EXAMPLE_YAML)
        before = tmp_path / "before.yaml"
        assert run("export", "-c", db, "-o", before).exit_code == 0
        import_ok(run, db, TEST_YAML)
        (backup,) = backups(db)
        assert backup.read_bytes() == before.read_bytes()

    def test_backup_reimports_to_the_outgoing_configuration(self, run, db, tmp_path):
        import_ok(run, db, EXAMPLE_YAML)
        before = exported(run, db)
        import_ok(run, db, TEST_YAML)
        (backup,) = backups(db)
        other = tmp_path / "restored" / "c.db"
        import_ok(run, other, backup)
        assert exported(run, other) == before

    def test_same_second_collisions_get_numeric_suffixes(self, run, db, tmp_path, monkeypatch):
        monkeypatch.setattr(cli_main, "datetime", _Frozen)
        sources = [mini_yaml(tmp_path / f"{p}.yaml", p) for p in ("p1", "p2", "p3", "p4")]
        for source in sources:
            import_ok(run, db, source)
        stem = f"config.db.pre-import-{STAMP}"
        names = sorted(p.name for p in backups(db))
        assert names == [f"{stem}-2.yaml", f"{stem}-3.yaml", f"{stem}.yaml"]
        # Each file holds the configuration that import replaced.
        held = {
            name: yaml.safe_load((db.parent / name).read_text())["snapshot_prefix"]
            for name in names
        }
        assert held == {f"{stem}.yaml": "p1", f"{stem}-2.yaml": "p2", f"{stem}-3.yaml": "p3"}

    def test_an_existing_backup_name_is_never_overwritten(self, run, db, tmp_path, monkeypatch):
        monkeypatch.setattr(cli_main, "datetime", _Frozen)
        import_ok(run, db)
        taken = db.parent / f"config.db.pre-import-{STAMP}.yaml"
        taken.write_text("precious, do not touch\n")
        result = run("import", TEST_YAML, "-c", db)
        assert result.exit_code == 0, result.stderr
        assert taken.read_text() == "precious, do not touch\n"
        assert (db.parent / f"config.db.pre-import-{STAMP}-2.yaml").exists()

    def test_a_symlink_at_the_backup_name_is_not_followed(self, run, db, tmp_path, monkeypatch):
        monkeypatch.setattr(cli_main, "datetime", _Frozen)
        import_ok(run, db)
        victim = tmp_path / "victim.txt"
        victim.write_text("victim\n")
        (db.parent / f"config.db.pre-import-{STAMP}.yaml").symlink_to(victim)
        dangling = tmp_path / "does-not-exist"
        (db.parent / f"config.db.pre-import-{STAMP}-2.yaml").symlink_to(dangling)
        result = run("import", TEST_YAML, "-c", db)
        assert result.exit_code == 0, result.stderr
        assert victim.read_text() == "victim\n"
        assert not dangling.exists()
        assert (db.parent / f"config.db.pre-import-{STAMP}-3.yaml").exists()

    def test_backup_goes_next_to_a_relative_database(self, run, tmp_path):
        assert run("import", EXAMPLE_YAML, "-c", "rel/c.db").exit_code == 0
        assert run("import", TEST_YAML, "-c", "rel/c.db").exit_code == 0
        assert [p.name for p in (tmp_path / "rel").glob("c.db.pre-import-*.yaml")] != []

    def test_no_backup_flag_writes_no_file(self, run, db):
        import_ok(run, db)
        result = run("import", TEST_YAML, "-c", db, "--no-backup")
        assert result.exit_code == 0, result.stderr
        assert backups(db) == []
        warn = [l for l in stderr_lines(result) if l.startswith("warning: replaced")]
        assert len(warn) == 1
        assert "NOT backed up (--no-backup)" in warn[0]
        assert "nothing of the previous configuration was kept" in warn[0]

    def test_no_backup_flag_on_a_first_import_is_harmless(self, run, db):
        result = run("import", EXAMPLE_YAML, "-c", db, "--no-backup")
        assert result.exit_code == 0
        assert backups(db) == []

    def test_no_backup_flag_before_the_path_option(self, run, db):
        import_ok(run, db)
        assert run("import", "--no-backup", TEST_YAML, "-c", db).exit_code == 0
        assert backups(db) == []

    # ---- backup failures roll the import back -----------------------------

    def test_backup_render_failure_rolls_the_import_back(self, run, db, monkeypatch):
        import_ok(run, db)
        before_export, before_bytes = exported(run, db), digest(db)

        def cannot_render(config):
            raise ValueError("datasets[x].frequency: sub-second precision")

        monkeypatch.setattr(cli_main, "render_yaml", cannot_render)
        result = run("import", TEST_YAML, "-c", db)
        assert result.exit_code == 1
        assert "--no-backup" in result.stderr
        assert "Nothing was replaced" in result.stderr
        assert "sub-second precision" in result.stderr
        monkeypatch.undo()
        assert backups(db) == []
        assert exported(run, db) == before_export
        assert digest(db) == before_bytes

    def test_backup_write_failure_rolls_back_and_leaves_no_partial_file(
        self, run, db, monkeypatch
    ):
        import_ok(run, db)
        before_export = exported(run, db)

        def disk_full(fd):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(cli_main.os, "fsync", disk_full)
        result = run("import", TEST_YAML, "-c", db)
        monkeypatch.undo()
        assert result.exit_code == 1
        assert "No space left on device" in result.stderr
        assert "--no-backup" in result.stderr
        assert backups(db) == []
        assert exported(run, db) == before_export

    def test_exhausted_backup_names_roll_back_and_hint(self, run, db, monkeypatch):
        import_ok(run, db)
        monkeypatch.setattr(cli_main, "datetime", _Frozen)
        for suffix in [""] + [f"-{n}" for n in range(2, 100)]:
            (db.parent / f"config.db.pre-import-{STAMP}{suffix}.yaml").write_text("x")
        before = exported(run, db)
        result = run("import", TEST_YAML, "-c", db)
        assert result.exit_code == 1
        assert "--no-backup" in result.stderr
        assert exported(run, db) == before

    def test_failure_after_the_backup_names_the_file_it_wrote(self, run, db, tmp_path, monkeypatch):
        """If a later step fails once the backup is on disk, the importer
        attaches a note naming it; the CLI must print that note."""
        import_ok(run, db)
        from zfsbackup.config.store import importer

        def fail_save(session, config):
            raise ValueError("save refused")

        monkeypatch.setattr(importer, "save_config", fail_save)
        result = run("import", TEST_YAML, "-c", db)
        assert result.exit_code == 1
        (backup,) = backups(db)
        assert str(backup) in result.stderr
        assert "was not replaced" in result.stderr

    # ---- unreadable outgoing configuration --------------------------------

    def test_unreadable_outgoing_is_refused_with_a_note_naming_no_backup(self, run, db):
        import_ok(run, db)
        empty_stored_config(db)
        before = digest(db)
        result = run("import", TEST_YAML, "-c", db)
        assert result.exit_code == 1
        lines = stderr_lines(result)
        assert "cannot be read" in lines[0]
        assert "No datasets configured" in lines[0]
        assert any("--no-backup" in line for line in lines[1:])
        assert backups(db) == []
        assert digest(db) == before
        assert sidecars(db) == []

    def test_unreadable_outgoing_is_not_a_database_unusable_error(self, run, db):
        import_ok(run, db)
        empty_stored_config(db)
        # exit 1 (refusal the operator can lift), not 78
        assert run("import", TEST_YAML, "-c", db).exit_code == 1

    def test_unreadable_outgoing_succeeds_with_no_backup(self, run, db):
        import_ok(run, db)
        empty_stored_config(db)
        result = run("import", TEST_YAML, "-c", db, "--no-backup")
        assert result.exit_code == 0, result.stderr
        assert backups(db) == []
        # With --no-backup the outgoing configuration is never even read, so
        # the only statement about it is the CLI's own replace warning.
        (warn,) = [l for l in stderr_lines(result) if l.startswith("warning: replaced")]
        assert "NOT backed up (--no-backup)" in warn
        assert "libzfs_test/ROOT/macos" in exported(run, db)

    def test_unreadable_outgoing_retry_note_says_a_retry_replaces_the_current_contents(
        self, run, db
    ):
        import_ok(run, db)
        empty_stored_config(db)
        result = run("import", TEST_YAML, "-c", db)
        assert "a retry replaces whatever is in the database at that time" in result.stderr

    # ---- locked database (real) -------------------------------------------

    @pytest.mark.slow
    def test_import_into_a_locked_database_is_75_and_changes_nothing(self, run, db):
        import_ok(run, db)
        before = exported(run, db)
        holder = sqlite3.connect(db, timeout=0, isolation_level=None)
        holder.execute("BEGIN IMMEDIATE")
        try:
            result = run("import", TEST_YAML, "-c", db)
        finally:
            holder.execute("ROLLBACK")
            holder.close()
        assert result.exit_code == 75, result.stderr
        assert "busy" in result.stderr.lower()
        assert backups(db) == []
        assert exported(run, db) == before


# --------------------------------------------------------------------------
# export
# --------------------------------------------------------------------------


class TestExport:
    def test_stdout_by_default(self, run, db):
        import_ok(run, db)
        result = run("export", "-c", db)
        assert result.exit_code == 0
        parsed = yaml.safe_load(result.stdout)
        assert parsed["snapshot_prefix"] == "autosnap"
        assert result.stderr == ""

    def test_dash_o_dash_is_stdout(self, run, db):
        import_ok(run, db)
        assert run("export", "-c", db, "-o", "-").stdout == run("export", "-c", db).stdout

    def test_output_is_accepted_by_import(self, run, db, tmp_path):
        import_ok(run, db)
        other = tmp_path / "other" / "c.db"
        out = tmp_path / "out.yaml"
        assert run("export", "-c", db, "-o", out).exit_code == 0
        import_ok(run, other, out)
        assert exported(run, other) == exported(run, db)

    def test_dash_o_writes_the_file_and_nothing_on_stdout(self, run, db, tmp_path):
        import_ok(run, db)
        out = tmp_path / "out.yaml"
        result = run("export", "-c", db, "-o", out)
        assert result.exit_code == 0
        assert result.stdout == ""
        assert str(out) in result.stderr
        assert out.read_text() == exported(run, db)

    def test_dash_o_refuses_an_existing_file(self, run, db, tmp_path):
        import_ok(run, db)
        out = tmp_path / "out.yaml"
        out.write_text("keep me\n")
        result = run("export", "-c", db, "-o", out)
        assert result.exit_code == 1
        assert "already exists" in result.stderr and "--force" in result.stderr
        assert out.read_text() == "keep me\n"

    def test_force_overwrites_and_truncates(self, run, db, tmp_path):
        import_ok(run, db)
        out = tmp_path / "out.yaml"
        out.write_text("x" * 100_000)
        result = run("export", "-c", db, "-o", out, "--force")
        assert result.exit_code == 0, result.stderr
        assert out.read_text() == exported(run, db)

    def test_force_without_dash_o_is_harmless(self, run, db):
        import_ok(run, db)
        assert run("export", "-c", db, "--force").exit_code == 0

    def test_refusal_does_not_follow_a_symlink_at_the_output(self, run, db, tmp_path):
        import_ok(run, db)
        victim = tmp_path / "victim.txt"
        victim.write_text("victim\n")
        out = tmp_path / "link.yaml"
        out.symlink_to(victim)
        assert run("export", "-c", db, "-o", out).exit_code == 1
        assert victim.read_text() == "victim\n"

    def test_output_into_a_missing_directory_is_a_clean_failure(self, run, db, tmp_path):
        import_ok(run, db)
        result = run("export", "-c", db, "-o", tmp_path / "nope" / "out.yaml")
        assert result.exit_code == 1
        assert "Traceback" not in result.stderr
        assert "No such file or directory" in result.stderr

    def test_export_is_read_only(self, run, db):
        import_ok(run, db)
        before = digest(db)
        for _ in range(2):
            assert run("export", "-c", db).exit_code == 0
        assert digest(db) == before
        assert sidecars(db) == []

    def test_export_does_not_change_the_database_mode_or_directory(self, run, db):
        import_ok(run, db)
        mode = stat.S_IMODE(db.stat().st_mode)
        listing = sorted(os.listdir(db.parent))
        run("export", "-c", db)
        assert stat.S_IMODE(db.stat().st_mode) == mode
        assert sorted(os.listdir(db.parent)) == listing

    def test_export_while_another_connection_holds_the_write_lock(self, run, db):
        import_ok(run, db)
        expected = exported(run, db)
        holder = sqlite3.connect(db, timeout=0, isolation_level=None)
        holder.execute("BEGIN IMMEDIATE")
        try:
            result = run("export", "-c", db)
        finally:
            holder.execute("ROLLBACK")
            holder.close()
        assert result.exit_code == 0, result.stderr
        assert result.stdout == expected

    def test_unloadable_stored_config_is_exit_1_with_the_stage_named(self, run, db):
        import_ok(run, db)
        empty_stored_config(db)
        result = run("export", "-c", db)
        assert result.exit_code == 1
        assert "the stored configuration cannot be loaded" in result.stderr
        assert "No datasets configured" in result.stderr
        assert "Traceback" not in result.stderr

    def test_unloadable_stored_config_creates_no_output_file(self, run, db, tmp_path):
        import_ok(run, db)
        empty_stored_config(db)
        out = tmp_path / "out.yaml"
        assert run("export", "-c", db, "-o", out).exit_code == 1
        assert not out.exists()

    def test_unloadable_export_is_read_only_too(self, run, db):
        import_ok(run, db)
        empty_stored_config(db)
        before = digest(db)
        run("export", "-c", db)
        assert digest(db) == before

    def test_export_from_an_empty_database_is_78_and_points_at_import(self, run, db):
        """No recorded revision and no tables: `migrate` would refuse (it never
        creates a database), so the store's text points at `import`."""
        blank_sqlite(db)
        before = digest(db)
        result = run("export", "-c", db)
        assert result.exit_code == 78
        assert "has no recorded schema revision (it holds no configuration tables)" in result.stderr
        assert "`zfsbackup-config import`" in result.stderr
        assert "zfsbackup-config migrate" not in result.stderr
        assert digest(db) == before

    def test_export_from_a_database_behind_the_schema_is_78_and_points_at_migrate(
        self, run, db, stacked_revision
    ):
        import_ok(run, db)
        stacked_revision()
        before = digest(db)
        result = run("export", "-c", db)
        assert result.exit_code == 78
        assert f"is at schema revision {EXPECTED_HEAD}; this package expects {SYNTH}." in result.stderr
        assert "Run `zfsbackup-config migrate`." in result.stderr
        # The store's text only: the daemon's "start the daemon again" is not CLI advice.
        assert "daemon" not in result.stderr
        assert digest(db) == before

    def test_export_ends_with_a_single_newline_and_is_stable(self, run, db):
        import_ok(run, db)
        first = exported(run, db)
        assert first.endswith("\n") and not first.endswith("\n\n")
        assert exported(run, db) == first


# --------------------------------------------------------------------------
# migrate
# --------------------------------------------------------------------------


class TestMigrate:
    def test_up_to_date(self, run, db):
        import_ok(run, db)
        result = run("migrate", "-c", db)
        assert result.exit_code == 0, result.stderr
        assert f"Schema is up to date (revision {EXPECTED_HEAD})." in result.stdout
        assert "Restart" not in result.stdout

    def test_check_up_to_date(self, run, db):
        import_ok(run, db)
        result = run("migrate", "--check", "-c", db)
        assert result.exit_code == 0
        assert result.stdout.strip() == f"Schema is up to date (revision {EXPECTED_HEAD})."

    def test_up_to_date_changes_no_bytes(self, run, db):
        import_ok(run, db)
        before = digest(db)
        for args in (["migrate"], ["migrate", "--check"]):
            assert run(*args, "-c", db).exit_code == 0
        assert digest(db) == before
        assert sidecars(db) == []

    def test_empty_database_check_fails_with_exit_1_and_points_at_import(self, run, db):
        blank_sqlite(db)
        before = digest(db)
        result = run("migrate", "--check", "-c", db)
        assert result.exit_code == 1
        assert result.stderr.startswith("error: the schema is not up to date: ")
        assert "has no recorded schema revision (it holds no configuration tables)" in result.stderr
        assert "`zfsbackup-config import`" in result.stderr
        assert "zfsbackup-config migrate" not in result.stderr
        assert "daemon" not in result.stderr
        assert digest(db) == before

    def test_check_on_a_database_behind_prints_the_stores_text_after_the_verdict(
        self, run, db, stacked_revision
    ):
        import_ok(run, db)
        stacked_revision()
        before = digest(db)
        result = run("migrate", "--check", "-c", db)
        assert result.exit_code == 1
        assert result.stderr == (
            f"error: the schema is not up to date: {db} is at schema revision "
            f"{EXPECTED_HEAD}; this package expects {SYNTH}. Run `zfsbackup-config migrate`.\n"
        )
        assert digest(db) == before

    def test_migrate_refuses_an_empty_file_with_78_and_does_not_initialise_it(self, run, db):
        """Item 10.4: `migrate` only upgrades a recorded, known revision; an
        empty (tableless) file is `import`'s to create. It used to be
        initialised here."""
        blank_sqlite(db)
        before = digest(db)
        result = run("migrate", "-c", db)
        assert result.exit_code == 78
        assert "no recorded schema revision" in result.stderr
        assert "nothing to migrate" in result.stderr
        assert "`zfsbackup-config import`" in result.stderr
        assert "Migrated" not in result.stdout
        assert digest(db) == before
        with raw_db(db) as conn:
            assert conn.execute("SELECT name FROM sqlite_master").fetchall() == []

    def test_migrate_upgrades_a_database_behind_and_says_to_restart_the_daemon(
        self, run, db, stacked_revision
    ):
        import_ok(run, db)
        before_export = exported(run, db)
        stacked_revision()
        result = run("migrate", "-c", db)
        assert result.exit_code == 0, result.stderr
        assert f"from {EXPECTED_HEAD} to {SYNTH}" in result.stdout
        assert RESTART_HINT in result.stdout
        assert "will stop at its next worker restart" not in result.stdout
        # ...and the next check is clean, the next migrate is a no-op, and
        # the stored configuration was kept.
        again = run("migrate", "--check", "-c", db)
        assert again.exit_code == 0
        assert "up to date" in again.stdout
        noop = run("migrate", "-c", db)
        assert "up to date" in noop.stdout and "daemon" not in noop.stdout
        assert exported(run, db) == before_export

    def test_migrated_database_can_be_imported_over_and_exported(self, run, db, stacked_revision):
        import_ok(run, db, TEST_YAML)
        stacked_revision()
        assert run("migrate", "-c", db).exit_code == 0
        assert run("import", EXAMPLE_YAML, "-c", db).exit_code == 0
        assert "libzfs_test/ROOT/macos" in exported(run, db)

    def test_check_never_changes_a_database_that_is_behind(self, run, db, stacked_revision):
        import_ok(run, db)
        stacked_revision()
        before = digest(db)
        run("migrate", "--check", "-c", db)
        run("migrate", "--check", "-c", db)
        assert digest(db) == before

    def test_check_order_is_irrelevant_for_the_flag(self, run, db):
        blank_sqlite(db)
        assert run("migrate", "-c", db, "--check").exit_code == 1
        assert run("-c", db, "migrate", "--check").exit_code == 1

    @pytest.mark.parametrize("args", [["migrate"], ["migrate", "--check"]])
    def test_database_from_a_newer_zfsbackup_is_78(self, run, db, args):
        import_ok(run, db)
        with raw_db(db) as conn:
            conn.execute("UPDATE alembic_version SET version_num = 'ffffffffffff'")
        before = digest(db)
        result = run(*args, "-c", db)
        assert result.exit_code == 78
        assert "ffffffffffff" in result.stderr
        assert digest(db) == before

    @pytest.mark.parametrize(
        "from_revs, expected",
        [
            (("aaaa",), "from aaaa to bbbb"),
            (("aaaa", "cccc"), "from aaaa, cccc to bbbb"),
        ],
    )
    def test_from_to_message(self, run, db, monkeypatch, from_revs, expected):
        import_ok(run, db)
        monkeypatch.setattr(
            cli_main, "upgrade_schema",
            lambda resolved: SchemaUpgrade(from_revs=from_revs, to_rev="bbbb", changed=True),
        )
        result = run("migrate", "-c", db)
        assert result.exit_code == 0
        assert expected in result.stdout
        assert RESTART_HINT in result.stdout

    def test_unchanged_upgrade_prints_up_to_date_without_restart_hint(
        self, run, db, monkeypatch
    ):
        import_ok(run, db)
        monkeypatch.setattr(
            cli_main, "upgrade_schema",
            lambda resolved: SchemaUpgrade(from_revs=("bbbb",), to_rev="bbbb", changed=False),
        )
        result = run("migrate", "-c", db)
        assert "Schema is up to date (revision bbbb)." in result.stdout
        assert "Restart" not in result.stdout

    def test_foreign_key_failure_is_78_and_rolls_back(self, run, db, monkeypatch):
        import_ok(run, db)
        before = digest(db)

        def broken(resolved):
            raise SchemaIntegrityError("foreign_key_check found 3 rows")

        monkeypatch.setattr(cli_main, "upgrade_schema", broken)
        result = run("migrate", "-c", db)
        assert result.exit_code == 78
        assert "foreign_key_check" in result.stderr
        assert digest(db) == before

    @pytest.mark.slow
    def test_migrate_against_a_locked_database_is_75(self, run, db):
        import_ok(run, db)
        holder = sqlite3.connect(db, timeout=0, isolation_level=None)
        holder.execute("BEGIN IMMEDIATE")
        try:
            result = run("migrate", "-c", db)
        finally:
            holder.execute("ROLLBACK")
            holder.close()
        assert result.exit_code == 75, result.stderr
        assert "busy" in result.stderr.lower()

    def test_check_does_not_block_behind_a_writer(self, run, db):
        """`--check` is a reader: a writer holding the lock must not make it wait."""
        import_ok(run, db)
        holder = sqlite3.connect(db, timeout=0, isolation_level=None)
        holder.execute("BEGIN IMMEDIATE")
        try:
            result = run("migrate", "--check", "-c", db)
        finally:
            holder.execute("ROLLBACK")
            holder.close()
        assert result.exit_code == 0, result.stderr


# --------------------------------------------------------------------------
# item 10.4 -- a database that is not ours is never touched
# --------------------------------------------------------------------------


def _foreign_snapshot(database: Path):
    """Everything an operator could notice changing: the full bytes, the
    journal-mode header bytes 18-19, `sqlite_master`, and the directory."""
    raw = database.read_bytes()
    with raw_db(database) as conn:
        master = conn.execute("SELECT type, name, tbl_name, sql FROM sqlite_master").fetchall()
    return raw, (raw[18], raw[19]), master, sorted(os.listdir(database.parent))


class TestForeignDatabase:
    """The item-10 review's reproduction: `-c other.db <command>` on a
    rollback-journal SQLite file with an unrelated table used to convert it to
    WAL (readers) or graft zfsbackup's schema into it and stamp it
    (`migrate`/`import`). Now every command refuses with exit 78 and leaves the
    file byte-for-byte alone."""

    @pytest.mark.parametrize(
        "args",
        [["export"], ["migrate", "--check"], ["migrate"], ["import", EXAMPLE_YAML]],
        ids=["export", "migrate-check", "migrate", "import"],
    )
    def test_every_command_refuses_a_foreign_database_and_changes_nothing(self, run, db, args):
        foreign_sqlite(db)
        before = _foreign_snapshot(db)
        assert before[1] == (1, 1)  # rollback journal

        result = run(*args, "-c", db)

        assert result.exit_code == 78, result.stderr
        assert "not a zfsbackup config database" in result.stderr
        assert "orders" in result.stderr
        assert "Traceback" not in result.stderr
        assert "retry" not in result.stderr.lower()
        assert result.stdout == ""
        assert _foreign_snapshot(db) == before
        assert backups(db) == [] and sidecars(db) == []

    def test_a_reader_reports_the_journal_mode_once_but_does_not_change_it(self, run, db):
        foreign_sqlite(db)
        result = run("export", "-c", db)
        assert result.exit_code == 78
        warns = [l for l in stderr_lines(result) if "is not in WAL mode" in l]
        assert len(warns) == 1
        assert warns[0].startswith("warning: ")
        assert "(journal_mode=delete)" in warns[0]
        assert warns[0].endswith(
            "readers and writers will block each other. "
            "(A zfsbackup-config write switches a zfsbackup config database to WAL.)"
        )
        # 10.5: a foreign file never gets a switch, so the warning must not
        # promise one.
        assert "will switch" not in warns[0]
        assert db.read_bytes()[18:20] == b"\x01\x01"

    def test_a_foreign_database_that_is_already_wal_is_refused_too(self, run, db):
        foreign_sqlite(db, wal=True)
        before = db.read_bytes()
        for args in (["export"], ["migrate", "--check"], ["migrate"], ["import", EXAMPLE_YAML]):
            result = run(*args, "-c", db)
            assert result.exit_code == 78, (args, result.stderr)
            assert "not a zfsbackup config database" in result.stderr
        dispose_all()
        with raw_db(db) as conn:
            assert [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")] == ["orders"]
        assert db.read_bytes()[18:20] == b"\x02\x02"
        assert before[18:20] == b"\x02\x02"

    def test_ours_mixed_with_a_foreign_table_and_no_revision_is_not_a_config_database(
        self, run, db
    ):
        from zfsbackup.config.store.models import Base
        from sqlalchemy import create_engine

        foreign_sqlite(db)
        engine = create_engine(f"sqlite:///{db}")
        Base.metadata.create_all(engine)
        engine.dispose()
        before = _foreign_snapshot(db)
        for args in (["export"], ["migrate", "--check"], ["migrate"], ["import", EXAMPLE_YAML]):
            result = run(*args, "-c", db)
            assert result.exit_code == 78, (args, result.stderr)
            assert "not a zfsbackup config database" in result.stderr
        assert _foreign_snapshot(db) == before

    def test_only_our_tables_and_no_revision_is_still_split_brain(self, run, db):
        """Kept deliberately: our tables with the stamp lost is a different,
        recoverable state with its own message."""
        from zfsbackup.config.store.models import Base
        from sqlalchemy import create_engine

        db.parent.mkdir(parents=True)
        engine = create_engine(f"sqlite:///{db}")
        Base.metadata.create_all(engine)
        engine.dispose()
        for args in (["export"], ["migrate", "--check"], ["migrate"]):
            result = run(*args, "-c", db)
            assert result.exit_code == 78, (args, result.stderr)
            assert "no recorded Alembic revision" in result.stderr
            assert "not a zfsbackup config database" not in result.stderr

    def test_an_export_from_a_real_config_database_in_rollback_journal_mode_is_not_modified(
        self, run, db
    ):
        """The reader change on OUR database: a config database that somehow is
        in `delete` mode is read, reported, and left in that mode."""
        import_ok(run, db)
        dispose_all()
        conn = sqlite3.connect(db)
        assert conn.execute("PRAGMA journal_mode=DELETE").fetchone() == ("delete",)
        conn.close()
        before = db.read_bytes()
        assert before[18:20] == b"\x01\x01"

        result = run("export", "-c", db)

        assert result.exit_code == 0, result.stderr
        assert "datasets:" in result.stdout
        assert db.read_bytes() == before
        assert len([l for l in stderr_lines(result) if "is not in WAL mode" in l]) == 1


class TestCorruptDatabaseBody:
    """A valid 100-byte SQLite header over garbage pages raises a
    `DatabaseError` ("database disk image is malformed") that is neither an
    `OperationalError` nor an `IntegrityError`: it used to fall through to
    exit 1 with an internal error."""

    @pytest.fixture
    def corrupt(self, run, db):
        import_ok(run, db)
        dispose_all()
        data = db.read_bytes()
        assert db.with_name(db.name + "-wal").exists() is False
        db.write_bytes(data[:100] + os.urandom(len(data) - 100))
        return db

    @pytest.mark.parametrize(
        "args",
        [["export"], ["migrate", "--check"], ["migrate"], ["import", EXAMPLE_YAML]],
        ids=["export", "migrate-check", "migrate", "import"],
    )
    def test_is_78_with_the_daemons_wording_and_no_retry_note(self, run, corrupt, args):
        result = run(*args, "-c", corrupt)
        assert result.exit_code == 78, result.stderr
        assert "database disk image is malformed" in result.stderr
        assert "Traceback" not in result.stderr
        assert "internal error" not in result.stderr
        # An import over an existing database normally appends "a retry
        # replaces whatever is in the database"; that is for failures AFTER
        # the write began, not for an unreadable file.
        assert "replaces whatever is in the database" not in result.stderr
        assert backups(corrupt) == []

    @pytest.mark.parametrize(
        "args",
        [["export"], ["migrate", "--check"], ["migrate"], ["import", EXAMPLE_YAML]],
        ids=["export", "migrate-check", "migrate", "import"],
    )
    def test_a_failed_connect_leaks_no_connection(self, run, corrupt, args):
        """Item 10.6: the failed connect used to leave its sqlite3 connection
        open inside a reference cycle (an fd per command until the cyclic GC
        ran), reported as `ResourceWarning: unclosed database` when it was
        finally collected. Forced collection inside a recording window makes
        the leak visible; `-W error::ResourceWarning` runs see it too."""
        gc.collect()  # earlier tests' garbage is not this command's leak
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = run(*args, "-c", corrupt)
            exit_code, stderr = result.exit_code, result.stderr
            del result  # its exc_info would keep the leaked frames alive
            dispose_all()
            gc.collect()
            leaked = [str(w.message) for w in caught if issubclass(w.category, ResourceWarning)]
        assert exit_code == 78, stderr
        assert leaked == []

    def test_the_message_is_the_daemons_wording_naming_the_file_and_its_source(self, run, corrupt):
        """Not the raw SQLAlchemy text: `config_error_message` names the
        database and how it was chosen, as the daemon's own log line does."""
        result = run("export", "-c", corrupt)
        (line,) = [l for l in stderr_lines(result) if not l.startswith("warning: ")]
        assert line.startswith(f"error: the config database {corrupt.resolve()}")
        assert "(via -c/--config)" in line
        assert line.endswith(
            "is damaged or not an SQLite database: database disk image is "
            "malformed; restore it from a backup."
        )
        # One line, SQLite's own words: no SQL statement, no SQLAlchemy link.
        assert "SELECT" not in line and "sqlalche.me" not in line
        assert "Failed to load configuration" not in line


class TestFirstImportCleanupNote:
    def test_the_skipped_cleanup_text_is_printed_exactly_once(self, run, db, monkeypatch):
        """The store logs the skip AND attaches the same text to the exception
        (`add_note`); the CLI shows the note, so the log line is dropped."""
        from zfsbackup.config.store import importer as importer_module

        held = []

        def attach_reader_then_fail(session, config):
            conn = sqlite3.connect(str(db))
            conn.execute("SELECT count(*) FROM alembic_version").fetchall()
            held.append(conn)
            raise ValueError("late failure")

        monkeypatch.setattr(importer_module, "save_config", attach_reader_then_fail)
        try:
            result = run("import", EXAMPLE_YAML, "-c", db)
        finally:
            for conn in held:
                conn.close()

        assert result.exit_code == 1
        assert db.exists()  # the cleanup really was skipped
        assert result.stderr.count("NOT removed") == 1
        assert result.stderr.count("Failed first import") <= 1
        lines = stderr_lines(result)
        assert "error: late failure" in lines
        note = [l for l in lines if "NOT removed" in l]
        assert len(note) == 1 and str(db) in note[0]
        assert not any(l.startswith("warning: ") and "NOT removed" in l for l in lines)


class TestInterruptedImport:
    """^C can land before, inside or just after the commit; the CLI says so
    instead of asserting either outcome."""

    @pytest.fixture
    def interrupted(self, monkeypatch):
        def install(*notes):
            exc = KeyboardInterrupt()
            for note in notes:
                exc.add_note(note)

            def fake(*args, **kwargs):
                raise exc

            monkeypatch.setattr(cli_main, "import_yaml", fake)

        return install

    def test_exit_130_prints_the_notes_and_does_not_assert_an_outcome(self, run, db, interrupted):
        interrupted("a note from the store")
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert result.exit_code == 130
        lines = stderr_lines(result)
        assert lines[0].startswith("interrupted -- the import may or may not have been applied")
        assert "a note from the store" in lines
        assert lines.index("a note from the store") > 0
        assert "Traceback" not in result.stderr
        assert "not replaced" not in result.stderr and "NOT applied" not in result.stderr

    def test_a_non_default_database_is_named_with_dash_c_in_the_export_hint(
        self, run, db, interrupted
    ):
        interrupted()
        result = run("import", EXAMPLE_YAML, "-c", db)
        resolved_path = store_paths.resolve_config_path(str(db)).path
        assert f"run `zfsbackup-config -c {resolved_path} export` to check" in result.stderr

    def test_a_path_with_spaces_is_shell_quoted(self, run, tmp_path, interrupted):
        interrupted()
        spaced = tmp_path / "a dir" / "config.db"
        result = run("import", EXAMPLE_YAML, "-c", spaced)
        resolved_path = store_paths.resolve_config_path(str(spaced)).path
        assert f"`zfsbackup-config -c '{resolved_path}' export`" in result.stderr

    def test_the_default_database_needs_no_dash_c(self, run, interrupted):
        interrupted()
        result = run("import", EXAMPLE_YAML)
        assert result.exit_code == 130
        assert "run `zfsbackup-config export` to check" in result.stderr
        assert " -c " not in result.stderr

    def test_the_environment_variable_counts_as_non_default(self, run, db, interrupted):
        interrupted()
        result = run("import", EXAMPLE_YAML, env={CONFIG_PATH_ENV: str(db)})
        assert result.exit_code == 130
        assert "`zfsbackup-config -c " in result.stderr

    def test_other_commands_just_say_interrupted(self, run, db, monkeypatch):
        import_ok(run, db)
        exc = KeyboardInterrupt()
        exc.add_note("export note")

        def boom(resolved):
            raise exc

        monkeypatch.setattr(cli_main, "load_runtime_config", boom)
        result = run("export", "-c", db)
        assert result.exit_code == 130
        lines = stderr_lines(result)
        assert lines[0] == "interrupted"
        assert "export note" in lines
        assert "may or may not" not in result.stderr

    @staticmethod
    def _interrupt_after_commit(monkeypatch):
        """^C lands just after phase 3's COMMIT returned (kbint.py in the
        item-10 review): the wrapper re-raises after the real session closed."""
        from zfsbackup.config.store import importer as importer_module

        real = importer_module.open_config_session

        @contextmanager
        def wrapper(*args, **kwargs):
            with real(*args, **kwargs) as session:
                yield session
            raise KeyboardInterrupt

        monkeypatch.setattr(importer_module, "open_config_session", wrapper)

    def test_a_first_import_interrupted_after_the_commit_keeps_the_database_and_does_not_say_not_applied(
        self, run, db, monkeypatch
    ):
        self._interrupt_after_commit(monkeypatch)
        result = run("import", EXAMPLE_YAML, "-c", db)

        assert result.exit_code == 130
        assert db.exists()  # the import DID commit; cleanup must not remove it
        text = result.stderr
        assert "may or may not have been applied" in text
        assert "NOT applied" not in text
        assert "was left intact" in text
        # ...and the hint is truthful here: export shows the configuration.
        monkeypatch.undo()
        assert "libzfs_test/ROOT/macos" in exported(run, db)

    def test_a_reimport_interrupted_after_the_commit_names_the_backup_and_both_outcomes(
        self, run, db, monkeypatch
    ):
        import_ok(run, db, TEST_YAML)
        self._interrupt_after_commit(monkeypatch)
        result = run("import", EXAMPLE_YAML, "-c", db)

        assert result.exit_code == 130
        (backup,) = backups(db)
        assert f"already written to {backup}" in result.stderr
        assert "may or may not have been applied" in result.stderr
        assert "was not replaced" not in result.stderr
        monkeypatch.undo()
        assert "libzfs_test/ROOT/macos" in exported(run, db)  # it was applied


# --------------------------------------------------------------------------
# item 10.4 -- the pre-import backup writer
# --------------------------------------------------------------------------


class TestBackupWriterHygiene:
    @pytest.fixture
    def writer(self, db):
        db.parent.mkdir(parents=True)
        db.touch()
        return cli_main._backup_writer(store_paths.resolve_config_path(str(db)))

    @pytest.fixture
    def config(self):
        return BackupConfig.from_file(EXAMPLE_YAML)

    @pytest.fixture
    def fds(self, monkeypatch):
        """The descriptors `_create_exclusive` handed out."""
        seen = []
        real = cli_main._create_exclusive

        def spy(*args, **kwargs):
            fd, path = real(*args, **kwargs)
            seen.append(fd)
            return fd, path

        monkeypatch.setattr(cli_main, "_create_exclusive", spy)
        return seen

    @staticmethod
    def _is_open(fd: int) -> bool:
        try:
            os.fstat(fd)
            return True
        except OSError:
            return False

    def test_the_file_descriptor_is_closed_when_fchmod_fails(
        self, writer, config, db, fds, monkeypatch
    ):
        def failing(fd, mode):
            raise PermissionError(1, "Operation not permitted")

        monkeypatch.setattr(os, "fchmod", failing)
        with pytest.raises(cli_errors.BackupFailed) as excinfo:
            writer(config)

        assert "Nothing was replaced" in str(excinfo.value)
        assert "--no-backup" in str(excinfo.value)
        (fd,) = fds
        assert not self._is_open(fd)
        assert backups(db) == []

    def test_a_partial_file_is_unlinked_and_the_fd_closed_on_keyboard_interrupt(
        self, writer, config, db, fds, monkeypatch
    ):
        def interrupt(fd):
            raise KeyboardInterrupt

        monkeypatch.setattr(os, "fsync", interrupt)
        with pytest.raises(KeyboardInterrupt):
            writer(config)

        assert backups(db) == []
        assert sorted(os.listdir(db.parent)) == [db.name]
        (fd,) = fds
        assert not self._is_open(fd)

    def test_an_interrupt_between_creation_and_the_write_leaves_nothing_and_no_open_fd(
        self, writer, config, db, fds, monkeypatch
    ):
        """The window the old `except OSError` could not cover: the fd exists
        but the file object does not yet own it."""

        def interrupt(fd, mode):
            raise KeyboardInterrupt

        monkeypatch.setattr(os, "fchmod", interrupt)
        with pytest.raises(KeyboardInterrupt):
            writer(config)
        assert backups(db) == []
        (fd,) = fds
        assert not self._is_open(fd)

    def test_the_directory_is_fsynced_after_the_file(self, writer, config, db, monkeypatch):
        synced = []
        real = os.fsync

        def recording(fd):
            kind = "dir" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file"
            synced.append(kind)
            return real(fd)

        monkeypatch.setattr(os, "fsync", recording)
        path = writer(config)

        assert synced == ["file", "dir"]
        assert path.exists() and path.parent == db.parent
        assert stat.S_IMODE(path.stat().st_mode) == cli_main.BACKUP_MODE

    def test_the_synced_directory_is_the_databases_directory(self, writer, config, db, monkeypatch):
        opened = []
        real = os.open

        def recording(path, flags, *a, **k):
            if flags == os.O_RDONLY:
                opened.append(Path(path))
            return real(path, flags, *a, **k)

        monkeypatch.setattr(os, "open", recording)
        writer(config)
        assert db.parent in opened

    def test_a_failing_directory_fsync_is_a_backup_failure_that_removes_the_file(
        self, writer, config, db, monkeypatch
    ):
        real = os.fsync

        def failing_for_dirs(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                raise OSError(5, "Input/output error")
            return real(fd)

        monkeypatch.setattr(os, "fsync", failing_for_dirs)
        with pytest.raises(cli_errors.BackupFailed, match="cannot write the pre-import backup"):
            writer(config)
        assert backups(db) == []

    def test_a_clean_write_leaves_exactly_one_backup(self, writer, config, db):
        path = writer(config)
        assert backups(db) == [path]
        assert yaml.safe_load(path.read_text())["datasets"]


# --------------------------------------------------------------------------
# item 10.4 -- export -o is atomic
# --------------------------------------------------------------------------


EXPORT_TEMP_PREFIX = ".zfsbackup-export."


def _temp_files(directory: Path):
    """The export's sibling temp files (`tempfile.mkstemp(prefix=...)`), item
    10.5. The prefix is not derived from the target name any more, so a leaked
    one is found by prefix alone."""
    return sorted(p.name for p in directory.iterdir() if p.name.startswith(EXPORT_TEMP_PREFIX))


class TestExportOutputFile:
    @pytest.fixture
    def populated(self, run, db):
        import_ok(run, db)
        return db

    def test_the_file_is_written_whole_and_no_temp_file_is_left(self, run, populated, tmp_path):
        out = tmp_path / "out.yaml"
        result = run("export", "-c", populated, "-o", out)
        assert result.exit_code == 0, result.stderr
        assert out.read_text() == exported(run, populated)
        assert _temp_files(tmp_path) == []
        assert f"to {out}." in result.stderr

    def test_the_file_is_never_written_in_place(self, run, populated, tmp_path, monkeypatch):
        """The content goes to a sibling temp file first; the target only ever
        appears by link/rename."""
        out = tmp_path / "out.yaml"
        opened = []
        real = os.open

        def recording(path, flags, *a, **k):
            if flags & os.O_CREAT:
                opened.append(Path(path))
            return real(path, flags, *a, **k)

        monkeypatch.setattr(os, "open", recording)
        assert run("export", "-c", populated, "-o", out).exit_code == 0
        assert out not in opened
        (temp,) = opened
        assert temp.parent == tmp_path and temp.name.startswith(EXPORT_TEMP_PREFIX)

    def test_a_new_file_gets_the_usual_mode_for_the_umask(self, run, populated, tmp_path):
        out = tmp_path / "out.yaml"
        old = os.umask(0o027)
        try:
            assert run("export", "-c", populated, "-o", out).exit_code == 0
        finally:
            os.umask(old)
        assert stat.S_IMODE(out.stat().st_mode) == 0o640

    @pytest.mark.parametrize("force", [False, True])
    def test_an_oserror_leaves_the_target_untouched_and_removes_the_temp(
        self, run, populated, tmp_path, monkeypatch, force
    ):
        out = tmp_path / "out.yaml"
        if force:
            out.write_text("precious\n")

        def disk_full(fd):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(os, "fsync", disk_full)
        result = run("export", "-c", populated, "-o", out, *(["--force"] if force else []))

        assert result.exit_code == 1
        assert "cannot write" in result.stderr and "No space left on device" in result.stderr
        assert "Any existing file was left untouched" in result.stderr
        assert "Traceback" not in result.stderr
        assert _temp_files(tmp_path) == []
        if force:
            assert out.read_text() == "precious\n"
        else:
            assert not out.exists()

    @pytest.mark.parametrize("force", [False, True])
    def test_keyboard_interrupt_leaves_the_target_untouched_and_removes_the_temp(
        self, run, populated, tmp_path, monkeypatch, force
    ):
        out = tmp_path / "out.yaml"
        if force:
            out.write_text("precious\n")

        def interrupt(fd):
            raise KeyboardInterrupt

        monkeypatch.setattr(os, "fsync", interrupt)
        result = run("export", "-c", populated, "-o", out, *(["--force"] if force else []))

        assert result.exit_code == 130
        assert _temp_files(tmp_path) == []
        if force:
            assert out.read_text() == "precious\n"
        else:
            assert not out.exists()

    @staticmethod
    def _creations(monkeypatch):
        created = []
        real = os.open

        def recording(path, flags, *a, **k):
            if flags & os.O_CREAT:
                created.append(Path(path).name)
            return real(path, flags, *a, **k)

        monkeypatch.setattr(os, "open", recording)
        return created

    def test_an_existing_target_is_refused_without_force(self, run, populated, tmp_path, monkeypatch):
        out = tmp_path / "out.yaml"
        out.write_text("mine\n")
        created = self._creations(monkeypatch)
        result = run("export", "-c", populated, "-o", out)
        assert result.exit_code == 1
        assert f"{out} already exists; use --force to overwrite it" in result.stderr
        assert out.read_text() == "mine\n"
        assert _temp_files(tmp_path) == []
        assert created == []  # refused up front: no temp file was even created

    def test_a_target_that_appears_mid_run_is_not_clobbered_without_force(
        self, run, populated, tmp_path, monkeypatch
    ):
        """The check-then-rename race: the final step is a hard link, which
        fails if the name exists. A plain `os.replace` would overwrite it."""
        out = tmp_path / "out.yaml"
        real = os.fsync

        def intruder(fd):
            real(fd)
            if not out.exists():
                out.write_text("someone else's file\n")

        monkeypatch.setattr(os, "fsync", intruder)
        result = run("export", "-c", populated, "-o", out)

        assert result.exit_code == 1
        assert "already exists; use --force to overwrite it" in result.stderr
        assert out.read_text() == "someone else's file\n"
        assert _temp_files(tmp_path) == []

    def test_without_hard_link_support_it_falls_back_to_a_rename(
        self, run, populated, tmp_path, monkeypatch
    ):
        out = tmp_path / "out.yaml"

        def no_links(src, dst, *a, **k):
            raise PermissionError(1, "Operation not permitted")

        monkeypatch.setattr(os, "link", no_links)
        result = run("export", "-c", populated, "-o", out)
        assert result.exit_code == 0, result.stderr
        assert out.read_text() == exported(run, populated)
        assert _temp_files(tmp_path) == []

    def test_the_rename_fallback_still_refuses_an_existing_target(
        self, run, populated, tmp_path, monkeypatch
    ):
        out = tmp_path / "out.yaml"
        real = os.fsync

        def intruder(fd):
            real(fd)
            if not out.exists():
                out.write_text("someone else's file\n")

        def no_links(src, dst, *a, **k):
            raise PermissionError(1, "Operation not permitted")

        monkeypatch.setattr(os, "fsync", intruder)
        monkeypatch.setattr(os, "link", no_links)
        result = run("export", "-c", populated, "-o", out)
        assert result.exit_code == 1
        assert "already exists; use --force" in result.stderr
        assert out.read_text() == "someone else's file\n"
        assert _temp_files(tmp_path) == []

    def test_force_replaces_the_content_and_keeps_the_existing_mode(
        self, run, populated, tmp_path
    ):
        out = tmp_path / "out.yaml"
        out.write_text("old\n")
        os.chmod(out, 0o600)
        old = os.umask(0o000)
        try:
            result = run("export", "-c", populated, "-o", out, "--force")
        finally:
            os.umask(old)
        assert result.exit_code == 0, result.stderr
        assert out.read_text() == exported(run, populated)
        assert stat.S_IMODE(out.stat().st_mode) == 0o600
        assert _temp_files(tmp_path) == []

    def test_force_follows_a_symlink_and_keeps_it_a_symlink(self, run, populated, tmp_path):
        real_file = tmp_path / "real.yaml"
        real_file.write_text("old\n")
        os.chmod(real_file, 0o640)
        link = tmp_path / "link.yaml"
        link.symlink_to(real_file)

        result = run("export", "-c", populated, "-o", link, "--force")

        assert result.exit_code == 0, result.stderr
        assert link.is_symlink() and os.readlink(link) == str(real_file)
        assert real_file.read_text() == exported(run, populated)
        assert stat.S_IMODE(real_file.stat().st_mode) == 0o640
        assert _temp_files(tmp_path) == []

    def test_a_symlink_is_refused_without_force_and_left_alone(self, run, populated, tmp_path):
        real_file = tmp_path / "real.yaml"
        real_file.write_text("old\n")
        link = tmp_path / "link.yaml"
        link.symlink_to(real_file)
        result = run("export", "-c", populated, "-o", link)
        assert result.exit_code == 1
        assert "already exists" in result.stderr
        assert real_file.read_text() == "old\n"

    def test_a_dangling_symlink_is_refused_without_force(self, run, populated, tmp_path, monkeypatch):
        target = tmp_path / "nowhere.yaml"
        link = tmp_path / "dangling.yaml"
        link.symlink_to(target)
        assert not link.exists() and link.is_symlink()
        created = self._creations(monkeypatch)

        result = run("export", "-c", populated, "-o", link)

        assert created == []  # `lexists`, not `exists`: refused before any write

        assert result.exit_code == 1
        assert f"{link} already exists; use --force to overwrite it" in result.stderr
        assert not target.exists()
        assert link.is_symlink()
        assert _temp_files(tmp_path) == []

    def test_force_through_a_dangling_symlink_creates_its_target(self, run, populated, tmp_path):
        """What the `open(O_CREAT|O_TRUNC)` this replaced did."""
        target = tmp_path / "nowhere.yaml"
        link = tmp_path / "dangling.yaml"
        link.symlink_to(target)
        result = run("export", "-c", populated, "-o", link, "--force")
        assert result.exit_code == 0, result.stderr
        assert link.is_symlink()
        assert target.read_text() == exported(run, populated)

    def test_a_relative_output_path_lands_in_the_working_directory(self, run, populated, tmp_path):
        result = run("export", "-c", populated, "-o", "rel.yaml")
        assert result.exit_code == 0, result.stderr
        assert (tmp_path / "rel.yaml").read_text() == exported(run, populated)
        assert _temp_files(tmp_path) == []

    def test_output_dash_is_still_stdout(self, run, populated, tmp_path):
        result = run("export", "-c", populated, "-o", "-")
        assert result.exit_code == 0
        assert result.stdout == exported(run, populated)
        assert not (tmp_path / "-").exists()


# --------------------------------------------------------------------------
# packaging
# --------------------------------------------------------------------------


class TestPackaging:
    @pytest.fixture(scope="class")
    def pyproject(self):
        with open(REPO / "pyproject.toml", "rb") as handle:
            return tomllib.load(handle)

    def test_console_script_is_declared(self, pyproject):
        scripts = pyproject["tool"]["poetry"]["scripts"]
        assert scripts["zfsbackup-config"] == "zfsbackup.cli.main:main"

    def test_console_script_target_resolves_and_is_callable(self, pyproject):
        import importlib

        module_name, _, attr = pyproject["tool"]["poetry"]["scripts"]["zfsbackup-config"].partition(":")
        target = getattr(importlib.import_module(module_name), attr)
        assert callable(target)
        assert target is cli_main.main

    def test_both_packages_are_included(self, pyproject):
        includes = {entry["include"] for entry in pyproject["tool"]["poetry"]["packages"]}
        assert {"libzfseasy", "zfsbackup"} <= includes

    def test_runtime_dependencies_the_cli_imports_are_declared(self, pyproject):
        deps = pyproject["tool"]["poetry"]["dependencies"]
        for name in ("click", "sqlalchemy", "alembic", "pyyaml"):
            assert name in deps

    def test_migration_resources_exist_in_the_source_tree(self):
        migrations = REPO / "zfsbackup" / "config" / "store" / "migrations"
        assert (migrations / "script.py.mako").is_file()
        assert list((migrations / "versions").glob("*.py"))

    @pytest.mark.slow
    @pytest.mark.integration
    def test_built_wheel_carries_the_cli_migrations_and_entry_point(self, tmp_path):
        poetry = shutil.which("poetry")
        if poetry is None:
            pytest.skip("poetry is not installed")
        out = tmp_path / "dist"
        proc = subprocess.run(
            [poetry, "build", "-f", "wheel", "-o", str(out)], cwd=REPO,
            capture_output=True, text=True, timeout=300,
        )
        if proc.returncode != 0:
            pytest.skip(f"poetry build unavailable here: {proc.stderr.strip()[-300:]}")
        (wheel,) = out.glob("*.whl")
        with zipfile.ZipFile(wheel) as archive:
            names = archive.namelist()
            entry_points = next(
                archive.read(n).decode() for n in names if n.endswith("entry_points.txt")
            )
        parser = configparser.ConfigParser()
        parser.optionxform = str
        parser.read_string(entry_points)
        assert parser["console_scripts"]["zfsbackup-config"] == "zfsbackup.cli.main:main"
        for expected in (
            "zfsbackup/cli/main.py",
            "zfsbackup/cli/render.py",
            "zfsbackup/cli/errors.py",
            "zfsbackup/cli/__main__.py",
            "zfsbackup/config/store/migrations/script.py.mako",
        ):
            assert expected in names, expected
        assert any(
            n.startswith("zfsbackup/config/store/migrations/versions/") and n.endswith(".py")
            for n in names
        )

    @pytest.mark.slow
    @pytest.mark.integration
    def test_installed_console_script_runs(self, tmp_path):
        from importlib.metadata import entry_points

        found = [
            ep for ep in entry_points(group="console_scripts")
            if ep.name == "zfsbackup-config"
        ]
        if not found:
            pytest.skip("zfsbackup-config is not installed (run `poetry install`)")
        script = shutil.which("zfsbackup-config", path=os.path.dirname(sys.executable))
        if script is None:
            pytest.skip("console script not on the interpreter's bin directory")
        env = {k: v for k, v in os.environ.items() if k not in ("ZFS_CMD", "ZPOOL_CMD")}
        proc = subprocess.run(
            [script, "--help"], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120
        )
        assert proc.returncode == 0, proc.stderr
        assert "Usage: zfsbackup-config" in proc.stdout
        missing = subprocess.run(
            [script, "-c", str(tmp_path / "missing.db"), "export"], cwd=tmp_path, env=env,
            capture_output=True, text=True, timeout=120,
        )
        assert missing.returncode == 78


# --------------------------------------------------------------------------
# item 10.5 -- foreign Alembic databases are never switched to WAL
# --------------------------------------------------------------------------

_STAMP_DDL = "CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL PRIMARY KEY)"


def alembic_foreign(path: Path, variant: str) -> Path:
    """The 10.5 review's reproduction (`foreign_alembic.py`): a rollback-journal
    file that is Alembic-managed, but not by us."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    if variant in ("foreign_rev", "empty_alembic"):
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, x TEXT)")
        conn.execute("INSERT INTO orders VALUES (1, 'a')")
    conn.execute(_STAMP_DDL)
    if variant == "foreign_rev":
        conn.execute("INSERT INTO alembic_version VALUES ('deadbeef0001')")
    elif variant == "mixed_rows":
        conn.execute(f"INSERT INTO alembic_version VALUES ('{EXPECTED_HEAD}')")
        conn.execute("INSERT INTO alembic_version VALUES ('deadbeef0001')")
    conn.commit()
    conn.close()
    os.chmod(path, 0o660)
    return path


class TestForeignAlembicDatabase:
    """Item 10.5 (M-1R). The writer engine used to switch to WAL any file with
    an `alembic_version` table, so `migrate`/`import` converted a foreign
    Alembic-managed database and then refused it with "nothing was changed"."""

    ARGS = pytest.mark.parametrize(
        "args",
        [["export"], ["migrate", "--check"], ["migrate"], ["import", EXAMPLE_YAML]],
        ids=["export", "migrate-check", "migrate", "import"],
    )

    @ARGS
    @pytest.mark.parametrize("variant", ["foreign_rev", "empty_alembic", "mixed_rows"])
    def test_every_command_refuses_and_the_file_is_byte_for_byte_unchanged(
        self, run, db, args, variant
    ):
        alembic_foreign(db, variant)
        before = _foreign_snapshot(db)
        sha = digest(db)
        assert before[1] == (1, 1)

        result = run(*args, "-c", db)

        assert result.exit_code == 78, result.stderr
        assert result.stdout == ""
        assert "Traceback" not in result.stderr
        assert digest(db) == sha
        assert _foreign_snapshot(db) == before
        assert db.read_bytes()[18:20] == b"\x01\x01"
        assert backups(db) == [] and sidecars(db) == []

    @pytest.mark.parametrize(
        "variant, fragment",
        [
            ("foreign_rev", "revision 'deadbeef0001'"),
            ("mixed_rows", "revision 'deadbeef0001'"),
            ("empty_alembic", "not a zfsbackup config database"),
        ],
    )
    def test_the_refusal_names_the_right_problem(self, run, db, variant, fragment):
        alembic_foreign(db, variant)
        result = run("migrate", "-c", db)
        assert fragment in result.stderr
        assert "nothing was changed" in result.stderr

    def test_a_file_whose_only_table_is_an_empty_alembic_version_is_still_imported_into(
        self, run, db
    ):
        """A stamp-only file is an unfinished initial migration: `import`
        migrates it (on the unswitched file) and the next engine, seeing a
        known revision, switches it. So the import ENDS in WAL (10.6)."""
        db.parent.mkdir(parents=True)
        conn = sqlite3.connect(db)
        conn.execute(_STAMP_DDL)
        conn.commit()
        conn.close()
        assert db.read_bytes()[18:20] == b"\x01\x01"
        import_ok(run, db)
        assert db.read_bytes()[18:20] == b"\x02\x02"
        assert "libzfs_test/ROOT/macos" in exported(run, db)

    def test_migrate_refuses_the_stamp_only_file_and_leaves_it_untouched(self, run, db):
        """10.6: `migrate` promises "nothing was changed" for a stamp-only
        file, so the writer engine no longer switches it to WAL first."""
        db.parent.mkdir(parents=True)
        conn = sqlite3.connect(db)
        conn.execute(_STAMP_DDL)
        conn.commit()
        conn.close()
        before = db.read_bytes()
        sha = hashlib.sha256(before).hexdigest()
        result = run("migrate", "-c", db)
        assert result.exit_code == 78
        assert "nothing to migrate" in result.stderr
        assert db.read_bytes()[18:20] == b"\x01\x01"
        assert hashlib.sha256(db.read_bytes()).hexdigest() == sha
        assert db.read_bytes() == before
        assert backups(db) == [] and sidecars(db) == []

    @pytest.mark.parametrize("prepare", ["new", "tableless"])
    def test_a_first_import_ends_in_wal(self, run, db, prepare):
        if prepare == "tableless":
            db.parent.mkdir(parents=True)
            conn = sqlite3.connect(db)
            conn.execute("PRAGMA user_version=1")
            conn.commit()
            conn.close()
            assert db.read_bytes()[18:20] == b"\x01\x01"
        else:
            assert not db.exists()
        import_ok(run, db)
        assert db.read_bytes()[18:20] == b"\x02\x02"
        assert "libzfs_test/ROOT/macos" in exported(run, db)


class TestTableSubsetsThroughTheCli:
    """A strict subset of our tables is somebody else's database: 78, "not a
    zfsbackup config database", never split-brain advice."""

    OURS = sorted(
        t for t in __import__("zfsbackup.config.store.models", fromlist=["Base"]).Base.metadata.tables
        if t != "alembic_version"
    )

    @staticmethod
    def build(path: Path, *names: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path)
        for name in names:
            conn.execute(f"CREATE TABLE {name} (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()
        os.chmod(path, 0o660)

    @pytest.mark.parametrize(
        "names",
        [("datasets",), ("datasets", "orders"), tuple(OURS[:-1])],
        ids=["lone-datasets", "subset-plus-foreign", "all-but-one"],
    )
    def test_a_subset_is_not_a_config_database_and_not_split_brain(self, run, db, names):
        self.build(db, *names)
        before = _foreign_snapshot(db)
        for args in (["export"], ["migrate", "--check"], ["migrate"], ["import", EXAMPLE_YAML]):
            result = run(*args, "-c", db)
            assert result.exit_code == 78, (args, result.stderr)
            assert "not a zfsbackup config database" in result.stderr
            assert "alembic stamp" not in result.stderr
        assert _foreign_snapshot(db) == before

    def test_exactly_all_of_ours_without_a_stamp_is_split_brain(self, run, db):
        self.build(db, *self.OURS)
        before = _foreign_snapshot(db)
        for args in (["export"], ["migrate", "--check"], ["migrate"]):
            result = run(*args, "-c", db)
            assert result.exit_code == 78, (args, result.stderr)
            assert "no recorded Alembic revision" in result.stderr
            assert "alembic stamp" in result.stderr
            assert "not a zfsbackup config database" not in result.stderr
        assert _foreign_snapshot(db) == before


class TestSchemaVersionMismatchThroughTheCli:
    def test_a_newer_database_gets_the_new_wording_and_is_untouched(self, run, db):
        import_ok(run, db)
        with raw_db(db) as conn:
            conn.execute("UPDATE alembic_version SET version_num = 'ffffffffffff'")
        before = digest(db)
        result = run("migrate", "-c", db)
        assert result.exit_code == 78
        assert (
            "was written by a newer zfsbackup package, or is not a zfsbackup config "
            "database; nothing was changed. If it is ours, upgrade the zfsbackup "
            "package installed on this host before opening it again; otherwise "
            "point -c at the config database."
        ) in result.stderr
        assert digest(db) == before


# --------------------------------------------------------------------------
# item 10.5 -- export -o onto something that is not a regular file
# --------------------------------------------------------------------------


class _FifoReader(threading.Thread):
    """Drains a FIFO from a thread so the CLI's blocking `open(O_WRONLY)` has a
    reader. Opened non-blocking and bounded by a deadline: if the code under
    test regresses to rename-over the FIFO, this times out instead of hanging
    the suite."""

    def __init__(self, path: Path, deadline: float = 20.0) -> None:
        super().__init__(daemon=True)
        self.path = path
        self.deadline = deadline
        self.data = b""
        self.opened = threading.Event()

    def run(self) -> None:
        fd = os.open(self.path, os.O_RDONLY | os.O_NONBLOCK)
        self.opened.set()
        try:
            end = time.monotonic() + self.deadline
            seen = False
            while time.monotonic() < end:
                ready, _, _ = select.select([fd], [], [], 0.1)
                if not ready:
                    continue
                try:
                    chunk = os.read(fd, 65536)
                except BlockingIOError:
                    continue
                if chunk:
                    self.data += chunk
                    seen = True
                elif seen:
                    return  # EOF after data: the writer closed
                else:
                    time.sleep(0.01)  # EOF before any writer attached
        finally:
            os.close(fd)


@pytest.mark.integration
@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="no FIFOs on this platform")
class TestExportOntoAFifo:
    @pytest.fixture
    def populated(self, run, db):
        import_ok(run, db)
        return db

    @pytest.fixture
    def fifo(self, tmp_path):
        path = tmp_path / "pipe"
        os.mkfifo(path)
        return path

    def test_force_writes_in_place_and_it_stays_a_fifo(self, run, populated, fifo, tmp_path):
        expected = exported(run, populated)
        reader = _FifoReader(fifo)
        reader.start()
        assert reader.opened.wait(10)

        result = run("export", "-c", populated, "-o", fifo, "--force")
        reader.join(30)

        assert result.exit_code == 0, result.stderr
        assert not reader.is_alive()
        assert reader.data.decode() == expected
        assert stat.S_ISFIFO(os.stat(fifo).st_mode)
        assert not fifo.is_symlink()
        assert _temp_files(tmp_path) == []

    def test_force_through_a_symlink_to_a_fifo_writes_in_place_too(
        self, run, populated, fifo, tmp_path
    ):
        link = tmp_path / "link"
        link.symlink_to(fifo)
        expected = exported(run, populated)
        reader = _FifoReader(fifo)
        reader.start()
        assert reader.opened.wait(10)

        result = run("export", "-c", populated, "-o", link, "--force")
        reader.join(30)

        assert result.exit_code == 0, result.stderr
        assert reader.data.decode() == expected
        assert link.is_symlink() and stat.S_ISFIFO(os.stat(fifo).st_mode)
        assert _temp_files(tmp_path) == []

    def test_force_never_calls_the_temp_file_machinery(self, run, populated, fifo, monkeypatch):
        """Not O_CREAT and not O_TRUNC: the in-place path opens `O_WRONLY` only."""
        flags_seen = []
        real = os.open

        def recording(path, flags, *a, **k):
            if os.fspath(path) == os.path.realpath(fifo):
                flags_seen.append(flags)
            return real(path, flags, *a, **k)

        monkeypatch.setattr(os, "open", recording)
        monkeypatch.setattr(
            cli_main.tempfile, "mkstemp",
            lambda *a, **k: pytest.fail("a temp file was created for a FIFO"),
        )
        reader = _FifoReader(fifo)
        reader.start()
        assert reader.opened.wait(10)
        # The reader's own open also goes through `os.open` (O_NONBLOCK); only
        # the writer's matters.
        run("export", "-c", populated, "-o", fifo, "--force")
        reader.join(30)
        writer_flags = [f for f in flags_seen if not f & os.O_NONBLOCK]
        assert writer_flags and all(
            f & os.O_ACCMODE == os.O_WRONLY and not f & (os.O_CREAT | os.O_TRUNC)
            for f in writer_flags
        )

    def test_without_force_a_fifo_is_refused_and_untouched(self, run, populated, fifo, tmp_path):
        result = run("export", "-c", populated, "-o", fifo)
        assert result.exit_code == 1
        assert f"{fifo} already exists; use --force to overwrite it" in result.stderr
        assert stat.S_ISFIFO(os.stat(fifo).st_mode)
        assert _temp_files(tmp_path) == []

    def test_an_unwritable_special_file_is_a_clean_error(self, run, populated, fifo, tmp_path):
        os.chmod(fifo, 0o444)
        result = run("export", "-c", populated, "-o", fifo, "--force")
        assert result.exit_code == 1
        assert f"error: cannot write {fifo}:" in result.stderr
        assert "Traceback" not in result.stderr
        assert stat.S_ISFIFO(os.stat(fifo).st_mode)
        assert _temp_files(tmp_path) == []


@pytest.mark.integration
@pytest.mark.skipif(
    not hasattr(os, "geteuid") or os.geteuid() == 0 or not os.path.exists("/dev/null"),
    reason="never exercised as root, and needs /dev/null",
)
class TestExportOntoACharacterDevice:
    def test_force_to_dev_null_writes_in_place_and_leaves_the_device_alone(
        self, run, db, tmp_path
    ):
        import_ok(run, db)
        before = os.stat("/dev/null")
        assert stat.S_ISCHR(before.st_mode)

        result = run("export", "-c", db, "-o", "/dev/null", "--force")

        assert result.exit_code == 0, result.stderr
        after = os.stat("/dev/null")
        assert stat.S_ISCHR(after.st_mode)
        assert (after.st_dev, after.st_ino, after.st_rdev, after.st_mode) == (
            before.st_dev, before.st_ino, before.st_rdev, before.st_mode
        )
        assert not [n for n in os.listdir("/dev") if n.startswith(EXPORT_TEMP_PREFIX)]
        assert _temp_files(tmp_path) == []

    def test_without_force_dev_null_is_refused(self, run, db):
        import_ok(run, db)
        result = run("export", "-c", db, "-o", "/dev/null")
        assert result.exit_code == 1
        assert "/dev/null already exists; use --force to overwrite it" in result.stderr
        assert stat.S_ISCHR(os.stat("/dev/null").st_mode)


# --------------------------------------------------------------------------
# item 10.5 -- export -o: names, ownership, durability
# --------------------------------------------------------------------------


class TestWriteOutputRestoresTheUmask:
    """`_write_output` reads the umask by setting it to 0, which is a
    process-wide window: a signal (or any error) landing before the restore
    would leave the whole process, daemon children included, at umask 0."""

    @pytest.fixture
    def umask(self):
        old = os.umask(0o027)
        try:
            yield 0o027
        finally:
            os.umask(old)

    @staticmethod
    def _current_umask():
        value = os.umask(0)
        os.umask(value)
        return value

    @pytest.mark.parametrize("exc_type", [KeyboardInterrupt, OSError, cli_main._Terminated])
    def test_an_interruption_between_the_two_umask_calls_restores_it(
        self, tmp_path, monkeypatch, umask, exc_type
    ):
        target = tmp_path / "out.yaml"

        def boom(fd, mode):
            raise exc_type()

        monkeypatch.setattr(cli_main.os, "fchmod", boom)
        with pytest.raises(exc_type):
            cli_main._write_output(str(target), "text", False)
        assert self._current_umask() == umask
        assert not target.exists()
        assert [p.name for p in tmp_path.iterdir()] == []  # the temp file is removed too

    def test_a_normal_write_restores_it_and_honours_it(self, tmp_path, umask):
        target = tmp_path / "out.yaml"
        cli_main._write_output(str(target), "text", False)
        assert self._current_umask() == umask
        assert stat.S_IMODE(target.stat().st_mode) == 0o666 & ~umask
        assert target.read_text() == "text"


class TestExportOutputFileDetails:
    @pytest.fixture
    def populated(self, run, db):
        import_ok(run, db)
        return db

    @pytest.mark.parametrize("force", [False, True])
    def test_a_245_character_name_works_and_leaves_no_temp(
        self, run, populated, tmp_path, force
    ):
        """The old temp name was derived from the target's (`.<name>.tmp-...`),
        so a name close to NAME_MAX made the TEMP name too long."""
        if os.pathconf(tmp_path, "PC_NAME_MAX") < 250:
            pytest.skip("filesystem NAME_MAX is too small for this test")
        name = "n" * 240 + ".yaml"
        assert len(name) == 245
        out = tmp_path / name
        if force:
            out.write_text("old\n")
        result = run("export", "-c", populated, "-o", out, *(["--force"] if force else []))
        assert result.exit_code == 0, result.stderr
        assert out.read_text() == exported(run, populated)
        assert sorted(p.name for p in tmp_path.iterdir() if p.name.startswith("n")) == [name]
        assert _temp_files(tmp_path) == []

    def test_the_temp_name_does_not_derive_from_the_target(self, run, populated, tmp_path, monkeypatch):
        seen = []
        real = cli_main.tempfile.mkstemp

        def recording(*a, **k):
            seen.append(k.get("prefix"))
            return real(*a, **k)

        monkeypatch.setattr(cli_main.tempfile, "mkstemp", recording)
        assert run("export", "-c", populated, "-o", tmp_path / ("x" * 200)).exit_code == 0
        assert seen == [EXPORT_TEMP_PREFIX]

    def test_an_fchown_eperm_is_ignored_and_the_mode_is_kept(
        self, run, populated, tmp_path, monkeypatch
    ):
        out = tmp_path / "out.yaml"
        out.write_text("old\n")
        os.chmod(out, 0o640)
        old_stat = out.stat()
        order = []
        real_fchmod = os.fchmod

        def denied(fd, uid, gid):
            order.append(("fchown", uid, gid))
            raise PermissionError(errno.EPERM, "Operation not permitted")

        def recording_fchmod(fd, mode):
            order.append(("fchmod", mode))
            return real_fchmod(fd, mode)

        monkeypatch.setattr(os, "fchown", denied)
        monkeypatch.setattr(os, "fchmod", recording_fchmod)

        result = run("export", "-c", populated, "-o", out, "--force")

        assert result.exit_code == 0, result.stderr
        assert out.read_text() == exported(run, populated)
        assert stat.S_IMODE(out.stat().st_mode) == 0o640
        # chown first (it may clear set-id bits), then chmod puts them back.
        assert order == [
            ("fchown", old_stat.st_uid, old_stat.st_gid),
            ("fchmod", 0o640),
        ]
        assert _temp_files(tmp_path) == []

    def test_another_fchown_failure_is_not_swallowed(
        self, run, populated, tmp_path, monkeypatch
    ):
        out = tmp_path / "out.yaml"
        out.write_text("precious\n")

        def broken(fd, uid, gid):
            raise OSError(errno.EIO, "Input/output error")

        monkeypatch.setattr(os, "fchown", broken)
        result = run("export", "-c", populated, "-o", out, "--force")
        assert result.exit_code == 1
        assert "cannot write" in result.stderr and "Input/output error" in result.stderr
        assert out.read_text() == "precious\n"
        assert _temp_files(tmp_path) == []

    def test_a_new_file_is_not_chowned(self, run, populated, tmp_path, monkeypatch):
        monkeypatch.setattr(
            os, "fchown", lambda *a: pytest.fail("fchown on a file that did not exist")
        )
        assert run("export", "-c", populated, "-o", tmp_path / "new.yaml").exit_code == 0

    def test_force_keeps_setid_free_modes_for_an_owner_only_file(self, run, populated, tmp_path):
        out = tmp_path / "out.yaml"
        out.write_text("old\n")
        os.chmod(out, 0o400)
        old = os.umask(0)
        try:
            result = run("export", "-c", populated, "-o", out, "--force")
        finally:
            os.umask(old)
        assert result.exit_code == 0, result.stderr
        assert stat.S_IMODE(out.stat().st_mode) == 0o400
        assert out.read_text() == exported(run, populated)

    @pytest.mark.parametrize("force", [False, True])
    def test_the_directory_is_fsynced_after_the_file_lands(
        self, run, populated, tmp_path, monkeypatch, force
    ):
        out = tmp_path / "out.yaml"
        if force:
            out.write_text("old\n")
        events = []
        real_fsync, real_link, real_replace = os.fsync, os.link, os.replace

        def fsync(fd):
            events.append("dir-fsync" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file-fsync")
            return real_fsync(fd)

        def link(*a, **k):
            events.append("link")
            return real_link(*a, **k)

        def replace(*a, **k):
            events.append("replace")
            return real_replace(*a, **k)

        monkeypatch.setattr(os, "fsync", fsync)
        monkeypatch.setattr(os, "link", link)
        monkeypatch.setattr(os, "replace", replace)

        assert run("export", "-c", populated, "-o", out, *(["--force"] if force else [])).exit_code == 0
        assert events == ["file-fsync", "replace" if force else "link", "dir-fsync"]

    @pytest.mark.parametrize("code", sorted({errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP}))
    def test_a_filesystem_that_cannot_fsync_a_directory_is_tolerated(
        self, run, populated, tmp_path, monkeypatch, code
    ):
        real = os.fsync

        def fsync(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                raise OSError(code, os.strerror(code))
            return real(fd)

        monkeypatch.setattr(os, "fsync", fsync)
        out = tmp_path / "out.yaml"
        result = run("export", "-c", populated, "-o", out)
        assert result.exit_code == 0, result.stderr
        assert out.read_text() == exported(run, populated)
        assert "flushing its directory failed" not in result.stderr

    def test_any_other_directory_fsync_failure_says_the_file_was_written(
        self, run, populated, tmp_path, monkeypatch
    ):
        real = os.fsync

        def fsync(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                raise OSError(errno.EIO, "Input/output error")
            return real(fd)

        monkeypatch.setattr(os, "fsync", fsync)
        out = tmp_path / "out.yaml"
        result = run("export", "-c", populated, "-o", out)
        assert result.exit_code == 1
        assert (
            f"error: {out} was written, but flushing its directory failed: "
            "Input/output error. It may not survive a crash."
        ) in result.stderr
        assert "Any existing file was left untouched" not in result.stderr
        assert out.exists()  # it IS there; only its durability is in doubt
        assert _temp_files(tmp_path) == []

    @pytest.mark.parametrize("code", sorted({errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP}))
    def test_the_backup_writer_tolerates_the_same_directories(
        self, run, db, monkeypatch, code
    ):
        import_ok(run, db, TEST_YAML)
        real = os.fsync

        def fsync(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                raise OSError(code, os.strerror(code))
            return real(fd)

        monkeypatch.setattr(os, "fsync", fsync)
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert result.exit_code == 0, result.stderr
        (backup,) = backups(db)
        assert yaml.safe_load(backup.read_text())["datasets"]

    def test_the_backup_writer_still_fails_on_a_real_directory_fsync_error(
        self, run, db, monkeypatch
    ):
        import_ok(run, db, TEST_YAML)
        before = exported(run, db)
        real = os.fsync

        def fsync(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                raise OSError(errno.EIO, "Input/output error")
            return real(fd)

        monkeypatch.setattr(os, "fsync", fsync)
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert result.exit_code == 1
        assert "cannot write the pre-import backup" in result.stderr
        assert backups(db) == []
        monkeypatch.undo()
        assert exported(run, db) == before

    @staticmethod
    def _reuse_the_descriptor_number(monkeypatch):
        """`os.fdopen` that hands back a file object which has ALREADY closed
        the descriptor, immediately re-opens that number for someone else (the
        lowest free descriptor is reused), and then raises ^C. Code that still
        holds the old number and closes it again closes the stranger."""
        victims = []
        raws = []
        real = os.fdopen

        def fdopen(fd, *a, **k):
            handle = real(fd, *a, **k)
            handle.close()
            victim = os.open(os.devnull, os.O_RDONLY)
            raws.append(fd)
            victims.append(victim)
            raise KeyboardInterrupt

        monkeypatch.setattr(os, "fdopen", fdopen)
        return raws, victims

    def test_export_clears_its_descriptor_before_fdopen_takes_it_over(
        self, run, tmp_path, db, monkeypatch
    ):
        import_ok(run, db)
        raws, victims = self._reuse_the_descriptor_number(monkeypatch)
        try:
            result = run("export", "-c", db, "-o", tmp_path / "out.yaml")
            assert result.exit_code == 130
            assert victims and victims[0] == raws[0], "the descriptor number was not reused"
            os.fstat(victims[0])  # still open: nobody closed it a second time
        finally:
            for victim in victims:
                try:
                    os.close(victim)
                except OSError:
                    pass
        assert _temp_files(tmp_path) == []

    def test_the_backup_writer_clears_its_descriptor_before_fdopen_takes_it_over(
        self, tmp_path, monkeypatch
    ):
        target = tmp_path / "cfg" / "config.db"
        target.parent.mkdir()
        target.touch()
        writer = cli_main._backup_writer(store_paths.resolve_config_path(str(target)))
        config = BackupConfig.from_file(EXAMPLE_YAML)
        raws, victims = self._reuse_the_descriptor_number(monkeypatch)
        try:
            with pytest.raises(KeyboardInterrupt):
                writer(config)
            assert victims and victims[0] == raws[0], "the descriptor number was not reused"
            os.fstat(victims[0])
        finally:
            for victim in victims:
                try:
                    os.close(victim)
                except OSError:
                    pass
        assert backups(target) == []


# --------------------------------------------------------------------------
# item 10.5 -- which database errors mean "the file is damaged"
# --------------------------------------------------------------------------


class TestDamagedDatabaseClassification:
    @pytest.mark.parametrize(
        "exc, unusable",
        [
            (DatabaseError("SELECT 1", {}, Exception("file is not a database")), True),
            (sa_exc.ProgrammingError("SELECT 1", {}, Exception("no such column")), False),
            (sa_exc.DataError("SELECT 1", {}, Exception("x")), False),
            (sa_exc.InternalError("SELECT 1", {}, Exception("x")), False),
            (sa_exc.NotSupportedError("SELECT 1", {}, Exception("x")), False),
            (sa_exc.InterfaceError("SELECT 1", {}, Exception("x")), False),
            (OperationalError("SELECT 1", {}, Exception("unable to open database file")), False),
            (IntegrityError("INSERT", {}, Exception("UNIQUE constraint failed")), False),
            (SQLAlchemyError("x"), False),
            (ValueError("x"), False),
        ],
        ids=lambda v: type(v).__name__ if isinstance(v, BaseException) else str(v),
    )
    def test_only_a_plain_database_error_is_a_damaged_file(self, exc, unusable):
        assert cli_errors.is_database_unusable(exc) is unusable
        assert (cli_errors.exit_code_for(exc) == 78) is (
            unusable or isinstance(exc, OperationalError)
        )

    def test_the_message_is_one_line_in_sqlites_own_words(self):
        exc = DatabaseError(
            "SELECT datasets.id FROM datasets", {}, Exception("database disk image is malformed\nsecond line")
        )
        resolved = store_paths.resolve_config_path("/tmp/does/not/matter.db")
        (line,) = cli_errors.format_error(exc, resolved)
        assert line.startswith("the config database ")
        assert str(resolved.path) in line or "matter.db" in line
        assert line.endswith(
            "is damaged or not an SQLite database: database disk image is malformed; "
            "restore it from a backup."
        )
        assert "\n" not in line
        assert "SELECT" not in line and "sqlalche.me" not in line and "second line" not in line

    @pytest.mark.parametrize("orig_message", ["", "   ", "\n"], ids=["empty", "blank", "newline"])
    def test_an_empty_sqlite_message_falls_back_to_the_exception_type(self, orig_message):
        """Item 10.6: `lines[0]` on an empty message was an IndexError, raised
        from inside the error reporter itself."""
        exc = DatabaseError("SELECT 1", {}, Exception(orig_message))
        resolved = store_paths.resolve_config_path("/tmp/does/not/matter.db")
        (line,) = cli_errors.format_error(exc, resolved)
        assert line.endswith(
            "is damaged or not an SQLite database: DatabaseError; restore it from a backup."
        )
        assert cli_errors._damaged_database_message(None, exc) == (
            "the config database is damaged or not an SQLite database: "
            "DatabaseError; restore it from a backup."
        )

    def test_an_exception_without_orig_and_without_text_names_its_type(self):
        class Odd(Exception):
            pass

        assert cli_errors._damaged_database_message(None, Odd("")) == (
            "the config database is damaged or not an SQLite database: "
            "Odd; restore it from a backup."
        )

    def test_the_reporter_survives_an_empty_message_end_to_end(self, run, db):
        import_ok(run, db)

        def boom(resolved):
            raise DatabaseError("SELECT 1", {}, Exception(""))

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(cli_main, "load_runtime_config", boom)
            result = run("export", "-c", db)
        assert result.exit_code == 78, result.stderr
        assert "Traceback" not in result.stderr and "IndexError" not in result.stderr
        assert "is damaged or not an SQLite database: DatabaseError;" in result.stderr

    def test_without_a_resolved_path_the_subject_is_generic(self):
        exc = DatabaseError("S", {}, Exception("file is not a database"))
        (line,) = cli_errors.format_error(exc, None)
        assert line == (
            "the config database is damaged or not an SQLite database: "
            "file is not a database; restore it from a backup."
        )

    def test_a_non_sqlite_file_through_the_cli_is_one_line_exit_78(self, run, db):
        db.parent.mkdir(parents=True)
        db.write_bytes(b"SQLite format 3\x00" + os.urandom(4096))
        before = digest(db)
        result = run("export", "-c", db)
        assert result.exit_code == 78, result.stderr
        errors = [l for l in stderr_lines(result) if l.startswith("error:")]
        assert len(errors) == 1
        assert "sqlalche.me" not in result.stderr and "[SQL" not in result.stderr
        assert "Traceback" not in result.stderr
        assert digest(db) == before

    @pytest.mark.parametrize(
        "exc",
        [
            sa_exc.ProgrammingError("SELECT oops", {}, Exception("no such column: oops")),
            sa_exc.DataError("SELECT oops", {}, Exception("bad data")),
            sa_exc.InternalError("SELECT oops", {}, Exception("internal")),
        ],
        ids=lambda e: type(e).__name__,
    )
    def test_a_statement_this_tool_built_wrongly_is_exit_1_and_not_called_damaged(
        self, run, db, monkeypatch, exc
    ):
        import_ok(run, db)

        def bug(resolved):
            raise exc

        monkeypatch.setattr(cli_main, "upgrade_schema", bug)
        result = run("migrate", "-c", db)
        assert result.exit_code == 1
        assert "damaged" not in result.stderr
        assert "restore it from a backup" not in result.stderr

    def test_a_programming_error_is_labelled_an_internal_error(self, run, db, monkeypatch):
        import_ok(run, db)

        def bug(resolved):
            raise sa_exc.ProgrammingError("SELECT oops", {}, Exception("no such column: oops"))

        monkeypatch.setattr(cli_main, "upgrade_schema", bug)
        result = run("migrate", "-c", db)
        assert result.exit_code == 1
        assert result.stderr.startswith("internal error: ")


# --------------------------------------------------------------------------
# item 10.5 -- ^C / SIGTERM wording
# --------------------------------------------------------------------------


class TestWithoutAppliedClaim:
    BACKUP_NOTE = (
        "A pre-import backup of the outgoing configuration was already written to "
        "/x/c.db.pre-import-1.yaml; the import was interrupted; it may or may not have "
        "been applied (run `zfsbackup-config export` to check)."
    )
    A11_NOTE = (
        "Failed first import: /x/c.db was NOT removed (it now holds a configuration). "
        "/x/c.db holds a configuration and was left intact; the import was "
        "interrupted, so it may or may not have been applied (run "
        "`zfsbackup-config export` to check)."
    )

    def test_the_backup_note_keeps_only_the_backup_fact(self):
        assert cli_main._without_applied_claim(self.BACKUP_NOTE) == (
            "A pre-import backup of the outgoing configuration was already written to "
            "/x/c.db.pre-import-1.yaml."
        )

    def test_the_cleanup_note_keeps_only_what_was_left_intact(self):
        trimmed = cli_main._without_applied_claim(self.A11_NOTE)
        assert trimmed == (
            "Failed first import: /x/c.db was NOT removed (it now holds a configuration). "
            "/x/c.db holds a configuration and was left intact."
        )
        assert "may or may not" not in trimmed

    def test_the_real_cleanup_note_keeps_the_never_delete_warning(self):
        """The exact text `importer._cleanup_skipped` produces for an
        interrupted first import whose file now holds a configuration. The
        `Never delete its -wal/-shm files on their own.` sentence FOLLOWS the
        "may or may not" clause and must survive it."""
        from zfsbackup.config.store.importer import _cleanup_skipped

        note = _cleanup_skipped(
            Path("/x/c.db"), "it now holds a configuration", "imported", interrupted=True
        )
        assert note == (
            "Failed first import: /x/c.db was NOT removed (it now holds a configuration). "
            "/x/c.db holds a configuration and was left intact; the import was "
            "interrupted, so it may or may not have been applied (run "
            "`zfsbackup-config export` to check). "
            "Never delete its -wal/-shm files on their own."
        )
        assert cli_main._without_applied_claim(note) == (
            "Failed first import: /x/c.db was NOT removed (it now holds a configuration). "
            "/x/c.db holds a configuration and was left intact. "
            "Never delete its -wal/-shm files on their own."
        )

    def test_the_real_backup_note_loses_only_the_verdict(self):
        """The backup note (`importer.import_yaml`) ENDS with the clause, so
        nothing follows it."""
        note = (
            "A pre-import backup of the outgoing configuration was "
            "already written to /x/c.db.pre-import-1.yaml; "
            "the import was interrupted; it may or may not have "
            "been applied (run `zfsbackup-config export` to check)."
        )
        assert cli_main._without_applied_claim(note) == (
            "A pre-import backup of the outgoing configuration was "
            "already written to /x/c.db.pre-import-1.yaml."
        )

    def test_a_trailing_sentence_after_the_clause_is_kept_verbatim(self):
        note = "X was left; it may or may not have been applied (run foo). Keep this one. And this."
        assert cli_main._without_applied_claim(note) == "X was left. Keep this one. And this."

    def test_a_clause_without_a_closing_parenthesis_ends_at_the_sentence(self):
        note = "X was left; it may or may not have been applied. Never delete -wal."
        assert cli_main._without_applied_claim(note) == "X was left. Never delete -wal."

    def test_a_clause_only_note_followed_by_a_sentence_keeps_the_sentence(self):
        note = "it may or may not have been applied (run foo). Never delete its -wal/-shm files on their own."
        assert cli_main._without_applied_claim(note) == (
            "Never delete its -wal/-shm files on their own."
        )

    def test_a_note_without_the_claim_is_returned_unchanged(self):
        note = "cleanup skipped: the file at X was left in place"
        assert cli_main._without_applied_claim(note) == note

    def test_a_note_that_is_only_the_claim_disappears(self):
        assert cli_main._without_applied_claim("it may or may not have been applied") == ""

    def test_a_semicolon_that_is_not_the_interrupted_clause_is_kept(self):
        note = "kept fact; and another; it may or may not have been applied"
        assert cli_main._without_applied_claim(note) == "kept fact; and another."


class TestInterruptedImportSaysItOnce:
    """The CLI states the "may or may not have been applied" verdict itself; the
    store's notes keep their other facts but drop their copy of it."""

    @staticmethod
    def _interrupt_after_commit(monkeypatch, exc_type=KeyboardInterrupt):
        from zfsbackup.config.store import importer as importer_module

        real = importer_module.open_config_session

        @contextmanager
        def wrapper(*args, **kwargs):
            with real(*args, **kwargs) as session:
                yield session
            raise exc_type

        monkeypatch.setattr(importer_module, "open_config_session", wrapper)

    def test_a_reimport_with_a_backup_says_it_exactly_once_and_names_the_backup(
        self, run, db, monkeypatch
    ):
        import_ok(run, db, TEST_YAML)
        self._interrupt_after_commit(monkeypatch)
        result = run("import", EXAMPLE_YAML, "-c", db)

        assert result.exit_code == 130
        assert result.stderr.count("may or may not have been applied") == 1
        (backup,) = backups(db)
        assert f"A pre-import backup of the outgoing configuration was already written to {backup}." in result.stderr
        assert "the import was interrupted" not in result.stderr
        lines = stderr_lines(result)
        assert lines[0].startswith("interrupted -- the import may or may not have been applied")

    def test_a_first_import_says_it_once_and_keeps_the_left_intact_fact(self, run, db, monkeypatch):
        self._interrupt_after_commit(monkeypatch)
        result = run("import", EXAMPLE_YAML, "-c", db)

        assert result.exit_code == 130
        assert result.stderr.count("may or may not have been applied") == 1
        assert f"{db.resolve()} holds a configuration and was left intact." in result.stderr
        assert "NOT applied" not in result.stderr

    def test_a_first_import_interrupted_after_the_commit_keeps_the_cleanup_warning_once(
        self, run, db, monkeypatch
    ):
        """End to end: the store's cleanup note holds BOTH the verdict (which
        the CLI states itself) and `Never delete its -wal/-shm files on their
        own.` (which only the note states). The first goes, the second stays,
        each exactly once."""
        self._interrupt_after_commit(monkeypatch)
        result = run("import", EXAMPLE_YAML, "-c", db)

        assert result.exit_code == 130
        assert db.exists()
        assert result.stderr.count("Never delete its -wal/-shm files") == 1
        assert result.stderr.count("Never delete its -wal/-shm files on their own.") == 1
        assert result.stderr.count("may or may not have been applied") == 1
        assert f"{db.resolve()} holds a configuration and was left intact." in result.stderr
        assert "Failed first import" in result.stderr

    def test_a_note_that_is_only_the_claim_is_not_printed_as_an_empty_line(
        self, run, db, monkeypatch
    ):
        exc = KeyboardInterrupt()
        exc.add_note("the import may or may not have been applied")
        exc.add_note("another fact")

        def fake(*args, **kwargs):
            raise exc

        monkeypatch.setattr(cli_main, "import_yaml", fake)
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert result.stderr.count("may or may not have been applied") == 1
        assert stderr_lines(result)[-1] == "another fact"
        assert "\n\n" not in result.stderr

    def test_other_commands_keep_their_notes_untouched(self, run, db, monkeypatch):
        import_ok(run, db)
        exc = KeyboardInterrupt()
        exc.add_note("a note that says it may or may not have been applied, verbatim")

        def boom(resolved):
            raise exc

        monkeypatch.setattr(cli_main, "load_runtime_config", boom)
        result = run("export", "-c", db)
        assert "a note that says it may or may not have been applied, verbatim" in result.stderr

    def test_a_terminated_import_is_worded_terminated_and_exits_143(self, run, db, monkeypatch):
        import_ok(run, db, TEST_YAML)
        self._interrupt_after_commit(monkeypatch, exc_type=cli_main._Terminated)
        result = run("import", EXAMPLE_YAML, "-c", db)
        assert result.exit_code == 143
        assert stderr_lines(result)[0].startswith(
            "terminated -- the import may or may not have been applied; run "
        )
        assert "interrupted" not in result.stderr
        assert result.stderr.count("may or may not have been applied") == 1

    def test_a_terminated_non_import_command_just_says_terminated(self, run, db, monkeypatch):
        import_ok(run, db)

        def boom(resolved):
            raise cli_main._Terminated()

        monkeypatch.setattr(cli_main, "load_runtime_config", boom)
        result = run("export", "-c", db)
        assert result.exit_code == 143
        assert stderr_lines(result) == ["terminated"]

    def test_terminated_is_a_keyboard_interrupt_so_plain_handlers_unwind(self):
        assert issubclass(cli_main._Terminated, KeyboardInterrupt)


@pytest.fixture
def sigterm_restored():
    previous = signal.getsignal(signal.SIGTERM)
    yield previous
    signal.signal(signal.SIGTERM, previous)


class TestSigtermHandlerLifecycle:
    """`main()` installs the SIGTERM handler for the duration of one command,
    in the main thread, and puts the previous one back however it ends."""

    @staticmethod
    def _fake_cli(monkeypatch, behaviour):
        seen = {}

        def fake_main(*args, **kwargs):
            seen["during"] = signal.getsignal(signal.SIGTERM)
            behaviour(seen)

        monkeypatch.setattr(cli_main.cli, "main", fake_main)
        return seen

    def test_the_handler_is_ours_during_and_restored_after_a_clean_exit(
        self, sigterm_restored, monkeypatch
    ):
        def ok(seen):
            raise SystemExit(0)

        seen = self._fake_cli(monkeypatch, ok)
        with pytest.raises(SystemExit) as excinfo:
            cli_main.main(["x"])
        assert excinfo.value.code == 0
        assert seen["during"] is cli_main._on_sigterm
        assert signal.getsignal(signal.SIGTERM) is sigterm_restored

    def test_the_real_main_restores_the_handler_after_help(self, sigterm_restored):
        with pytest.raises(SystemExit) as excinfo:
            cli_main.main(["--help"])
        assert excinfo.value.code == 0
        assert signal.getsignal(signal.SIGTERM) is sigterm_restored

    def test_the_handler_is_restored_when_the_command_raises(self, sigterm_restored, monkeypatch):
        def boom(seen):
            raise RuntimeError("bug")

        self._fake_cli(monkeypatch, boom)
        with pytest.raises(RuntimeError):
            cli_main.main(["x"])
        assert signal.getsignal(signal.SIGTERM) is sigterm_restored

    def test_the_first_signal_ignores_further_ones_and_is_restored_in_finally(
        self, sigterm_restored, monkeypatch
    ):
        def first_sigterm(seen):
            with pytest.raises(cli_main._Terminated):
                cli_main._on_sigterm(signal.SIGTERM, None)
            seen["after_first"] = signal.getsignal(signal.SIGTERM)
            raise SystemExit(130)

        seen = self._fake_cli(monkeypatch, first_sigterm)
        with pytest.raises(SystemExit) as excinfo:
            cli_main.main(["x"])
        assert seen["after_first"] == signal.SIG_IGN
        assert excinfo.value.code == 143  # remapped: a SIGTERM was seen
        assert signal.getsignal(signal.SIGTERM) is sigterm_restored

    @pytest.mark.parametrize("code", [1, 130])
    def test_a_sigterm_during_argument_parsing_is_remapped_to_143(
        self, sigterm_restored, monkeypatch, code
    ):
        """A signal that lands while Click parses arguments comes back as
        Click's generic Abort (exit 1)."""

        def parsing(seen):
            with pytest.raises(cli_main._Terminated):
                cli_main._on_sigterm(signal.SIGTERM, None)
            raise SystemExit(code)

        self._fake_cli(monkeypatch, parsing)
        with pytest.raises(SystemExit) as excinfo:
            cli_main.main(["x"])
        assert excinfo.value.code == 143

    @pytest.mark.parametrize("code", [0, 1, 2, 75, 78, 130])
    def test_without_a_sigterm_exit_codes_are_untouched(self, sigterm_restored, monkeypatch, code):
        def plain(seen):
            raise SystemExit(code)

        self._fake_cli(monkeypatch, plain)
        with pytest.raises(SystemExit) as excinfo:
            cli_main.main(["x"])
        assert excinfo.value.code == code

    @pytest.mark.parametrize("code", [0, 2, 75, 78])
    def test_other_exit_codes_survive_a_sigterm(self, sigterm_restored, monkeypatch, code):
        def finished_anyway(seen):
            with pytest.raises(cli_main._Terminated):
                cli_main._on_sigterm(signal.SIGTERM, None)
            raise SystemExit(code)

        self._fake_cli(monkeypatch, finished_anyway)
        with pytest.raises(SystemExit) as excinfo:
            cli_main.main(["x"])
        assert excinfo.value.code == code

    def test_the_seen_flag_does_not_leak_into_the_next_invocation(
        self, sigterm_restored, monkeypatch
    ):
        def first(seen):
            with pytest.raises(cli_main._Terminated):
                cli_main._on_sigterm(signal.SIGTERM, None)
            raise SystemExit(0)

        self._fake_cli(monkeypatch, first)
        with pytest.raises(SystemExit):
            cli_main.main(["x"])

        def second(seen):
            raise SystemExit(1)

        self._fake_cli(monkeypatch, second)
        with pytest.raises(SystemExit) as excinfo:
            cli_main.main(["x"])
        assert excinfo.value.code == 1

    def test_off_the_main_thread_the_signal_is_left_alone(self, sigterm_restored, monkeypatch):
        outcome = {}

        def fake_main(*args, **kwargs):
            raise SystemExit(0)

        monkeypatch.setattr(cli_main.cli, "main", fake_main)

        def target():
            try:
                cli_main.main(["x"])
            except SystemExit as exc:
                outcome["code"] = exc.code
            except BaseException as exc:  # pragma: no cover - the failure being tested
                outcome["error"] = exc

        thread = threading.Thread(target=target)
        thread.start()
        thread.join(10)
        assert outcome == {"code": 0}
        assert signal.getsignal(signal.SIGTERM) is sigterm_restored


_SIGNAL_DRIVER = textwrap.dedent(
    '''
    import os, sys, time
    from pathlib import Path
    from zfsbackup.cli import main as m

    mode, marker = sys.argv[1], Path(sys.argv[2])
    argv = sys.argv[3:]

    def ready(suffix=""):
        Path(str(marker) + suffix).write_text("x")

    if mode in ("block_fsync", "slow_cleanup"):
        real_fsync = os.fsync

        def fsync(fd):
            ready()
            time.sleep(120)

        os.fsync = fsync
    if mode == "slow_cleanup":
        real_unlink = os.unlink

        def unlink(path, *a, **k):
            if os.path.basename(os.fspath(path)).startswith(".zfsbackup-export."):
                ready(".cleanup")
                time.sleep(2.0)
            return real_unlink(path, *a, **k)

        os.unlink = unlink
    if mode == "block_import":
        def blocked(*a, **k):
            ready()
            time.sleep(120)

        m.import_yaml = blocked
    if mode == "sigterm_in_parse":
        import signal

        real_parse = m._Group.parse_args

        def parse_args(self, ctx, args):
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(5)
            return real_parse(self, ctx, args)

        m._Group.parse_args = parse_args
    m.main(argv)
    '''
)


@pytest.mark.integration
@pytest.mark.skipif(not hasattr(signal, "SIGTERM") or os.name != "posix", reason="POSIX signals")
class TestSigtermInARealProcess:
    """SIGTERM must unwind like ^C: temp files removed, targets untouched, a
    stable wording and exit code. Each case runs `zfsbackup.cli.main.main` in
    its own interpreter under a hard timeout, and kills it if anything hangs."""

    TIMEOUT = 40

    def _launch(self, tmp_path, mode, *argv):
        env = {
            k: v for k, v in os.environ.items()
            if k not in ("ZFS_CMD", "ZPOOL_CMD", CONFIG_PATH_ENV)
        }
        env["PYTHONPATH"] = str(REPO)
        marker = tmp_path / "ready.marker"
        proc = subprocess.Popen(
            [sys.executable, "-c", _SIGNAL_DRIVER, mode, str(marker), *map(str, argv)],
            cwd=tmp_path, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        return proc, marker

    @staticmethod
    def _wait_for(path, proc):
        end = time.monotonic() + TestSigtermInARealProcess.TIMEOUT
        while time.monotonic() < end:
            if path.exists():
                return
            if proc.poll() is not None:
                out, err = proc.communicate()
                pytest.fail(f"the driver exited early ({proc.returncode}): {err}")
            time.sleep(0.02)
        pytest.fail(f"timed out waiting for {path}")

    def _finish(self, proc):
        try:
            out, err = proc.communicate(timeout=self.TIMEOUT)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, err = proc.communicate()
            pytest.fail(f"the process did not exit after SIGTERM: {err}")
        return proc.returncode, out, err

    def _run(self, tmp_path, mode, *argv, second_signal=False):
        proc, marker = self._launch(tmp_path, mode, *argv)
        try:
            self._wait_for(marker, proc)
            proc.send_signal(signal.SIGTERM)
            if second_signal:
                self._wait_for(Path(str(marker) + ".cleanup"), proc)
                proc.send_signal(signal.SIGTERM)
            return self._finish(proc)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.communicate()

    @pytest.fixture
    def populated(self, run, db):
        import_ok(run, db)
        dispose_all()
        return db

    @pytest.mark.parametrize("force", [False, True])
    def test_sigterm_during_an_export_cleans_up_and_exits_143(
        self, populated, tmp_path, force
    ):
        out = tmp_path / "out.yaml"
        if force:
            out.write_text("precious\n")
        before = digest(populated)

        code, _, err = self._run(
            tmp_path, "block_fsync", "export", "-c", populated, "-o", out,
            *(["--force"] if force else []),
        )

        assert code == 143, err
        assert err.strip().splitlines()[-1] == "terminated"
        assert "Traceback" not in err and "interrupted" not in err
        assert _temp_files(tmp_path) == []
        if force:
            assert out.read_text() == "precious\n"
        else:
            assert not out.exists()
        assert digest(populated) == before
        assert sidecars(populated) == []

    def test_a_second_sigterm_during_cleanup_is_ignored(self, populated, tmp_path):
        out = tmp_path / "out.yaml"
        code, _, err = self._run(
            tmp_path, "slow_cleanup", "export", "-c", populated, "-o", out,
            second_signal=True,
        )
        assert code == 143, err
        assert err.strip().splitlines()[-1] == "terminated"
        # The cleanup the second signal tried to interrupt still finished.
        assert _temp_files(tmp_path) == []
        assert not out.exists()

    def test_sigterm_during_an_import_says_terminated_and_that_it_may_have_applied(
        self, db, tmp_path
    ):
        code, _, err = self._run(tmp_path, "block_import", "import", EXAMPLE_YAML, "-c", db)
        assert code == 143, err
        first = [l for l in err.splitlines() if l][0]
        assert first.startswith("terminated -- the import may or may not have been applied; run ")
        assert "interrupted" not in err
        assert err.count("may or may not have been applied") == 1

    def test_sigterm_while_click_parses_arguments_is_143_not_1(self, tmp_path):
        proc, _ = self._launch(tmp_path, "sigterm_in_parse", "--help")
        try:
            code, _, err = self._finish(proc)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.communicate()
        assert code == 143, err
        assert "Aborted" in err or "terminated" in err

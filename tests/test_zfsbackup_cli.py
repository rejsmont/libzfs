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
"""

import configparser
import hashlib
import io
import logging
import os
import re
import shutil
import sqlite3
import stat
import subprocess
import sys
import tomllib
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import click
import pytest
import yaml
from click.testing import CliRunner
from sqlalchemy.exc import OperationalError, SQLAlchemyError

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
    OutgoingConfigUnreadable,
    SchemaIntegrityError,
    SchemaOutOfDate,
    SchemaUpgrade,
    SchemaVersionMismatch,
    dispose_all,
)
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
            (RuntimeError("scripts disagree"), 78),
            (SQLAlchemyError("x"), 1),
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

    def test_bare_runtime_error_is_labelled_an_installation_error(self):
        buf = io.StringIO()
        code = cli_errors.report(RuntimeError("two heads"), None, stream=buf)
        assert code == 78
        out = buf.getvalue()
        assert out.startswith("installation error: two heads")
        assert "reinstall zfsbackup" in out

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

    def test_bare_runtime_error_is_an_installation_error(self, run, db, monkeypatch):
        import_ok(run, db)

        def packaging_defect(resolved):
            raise RuntimeError("migration scripts disagree with EXPECTED_HEAD")

        monkeypatch.setattr(cli_main, "upgrade_schema", packaging_defect)
        result = run("migrate", "-c", db)
        assert result.exit_code == 78
        assert "installation error: migration scripts disagree" in result.stderr

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

    def test_export_from_a_database_behind_the_schema_is_78(self, run, db):
        blank_sqlite(db)
        result = run("export", "-c", db)
        assert result.exit_code == 78
        assert "zfsbackup-config migrate" in result.stderr

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

    def test_no_recorded_revision_check_fails_with_exit_1(self, run, db):
        blank_sqlite(db)
        before = digest(db)
        result = run("migrate", "--check", "-c", db)
        assert result.exit_code == 1
        assert "no recorded Alembic revision" in result.stderr
        assert "zfsbackup-config migrate" in result.stderr
        assert digest(db) == before

    def test_no_recorded_revision_migrate_upgrades_and_says_to_restart(self, run, db):
        blank_sqlite(db)
        result = run("migrate", "-c", db)
        assert result.exit_code == 0, result.stderr
        assert f"from none (no recorded revision) to {EXPECTED_HEAD}" in result.stdout
        assert "Restart a running zfsbackup daemon" in result.stdout
        # ...and the next check is clean, and the next migrate is a no-op.
        again = run("migrate", "--check", "-c", db)
        assert again.exit_code == 0
        assert "up to date" in again.stdout
        noop = run("migrate", "-c", db)
        assert "up to date" in noop.stdout and "Restart" not in noop.stdout

    def test_migrated_database_can_be_imported_over_and_exported(self, run, db):
        blank_sqlite(db)
        assert run("migrate", "-c", db).exit_code == 0
        assert run("import", EXAMPLE_YAML, "-c", db).exit_code == 0
        assert "libzfs_test/ROOT/macos" in exported(run, db)

    def test_check_never_changes_a_database_that_is_behind(self, run, db):
        blank_sqlite(db)
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
            ((), "from none (no recorded revision) to bbbb"),
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
        assert "Restart a running zfsbackup daemon" in result.stdout

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
        blank_sqlite(db)
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

"""`zfsbackup-config --version` (milestone C, item C10): an eager flag on the
`cli` group that prints `version_string("zfsbackup-config")` and exits 0 before
any database path is looked at.

- ``TestVersionFlag``     output, exit status, regex, `--help`, not cached.
- ``TestNoDatabase``      no DB needed, nothing created, `-c` never examined.
- ``TestVersionImports``  a hermetic `python -I` run leaves the daemon and workers out.
"""

import importlib.metadata
import os
import re
import subprocess
import sys
import textwrap
from pathlib import Path
from unittest import mock

import pytest

import zfsbackup.version as version_mod
from zfsbackup.config.store import CONFIG_PATH_ENV
from zfsbackup.config.store.migrate import EXPECTED_HEAD
from zfsbackup.protocol import PROTOCOL_VERSION
from zfsbackup.version import version_string

from _cli_support import db, isolated, run  # noqa: F401  (fixtures)
from _hermetic import hermetic_env

REPO = Path(__file__).resolve().parent.parent
VERSION_RE = re.compile(
    r"zfsbackup-config \S+ \(wire protocol (\d+), config schema ([0-9a-f]{12})\)"
)


def tree(root: Path):
    return sorted(str(p.relative_to(root)) for p in root.rglob("*"))


@pytest.mark.unit
class TestVersionFlag:
    def test_exits_zero_and_prints_the_version_string(self, run):
        result = run("--version")
        assert result.exit_code == 0, result.stderr
        assert result.stdout == version_string("zfsbackup-config") + "\n"
        assert result.stderr == ""

    def test_matches_the_documented_format(self, run):
        match = VERSION_RE.fullmatch(run("--version").stdout.strip())
        assert match, run("--version").stdout
        assert int(match.group(1)) == PROTOCOL_VERSION
        assert match.group(2) == EXPECTED_HEAD

    def test_before_a_subcommand_prints_only_the_version(self, run):
        result = run("--version", "list", "datasets")
        assert result.exit_code == 0, result.stderr
        assert result.stdout == version_string("zfsbackup-config") + "\n"
        assert result.stderr == ""

    def test_help_lists_it(self, run):
        result = run("--help")
        assert result.exit_code == 0
        assert "--version" in result.stdout

    def test_a_patched_version_is_reflected_and_not_cached(self, run):
        with mock.patch.object(version_mod, "version", return_value="1.2.3"):
            first = run("--version").stdout
        with mock.patch.object(version_mod, "version", return_value="4.5.6"):
            second = run("--version").stdout
        assert first.startswith("zfsbackup-config 1.2.3 (")
        assert second.startswith("zfsbackup-config 4.5.6 (")

    def test_unknown_install_still_exits_zero(self, run):
        with mock.patch.object(
            version_mod, "version", side_effect=importlib.metadata.PackageNotFoundError("zfsbackup")
        ):
            result = run("--version")
        assert result.exit_code == 0
        assert result.stdout.startswith("zfsbackup-config 0+unknown (")

    def test_a_subcommand_without_the_flag_is_unaffected(self, run, db):
        """The flag is eager but only acts when given."""
        result = run("-c", db, "export")
        assert result.exit_code == 78
        assert "wire protocol" not in result.stdout


@pytest.mark.unit
class TestNoDatabase:
    def test_no_database_anywhere_and_nothing_created(self, run, tmp_path):
        env = {CONFIG_PATH_ENV: str(tmp_path / "env" / "nested" / "env.db")}
        before = tree(tmp_path)
        result = run("--version", env=env)
        assert result.exit_code == 0, result.stderr
        assert tree(tmp_path) == before
        for name in ("default", "env"):
            assert not (tmp_path / name).exists()
        assert not any(
            p.name.endswith((".writer.lock", "-wal", "-shm", ".tls", ".db"))
            for p in tmp_path.rglob("*")
        )

    def test_nonexistent_config_directory(self, run, tmp_path):
        before = tree(tmp_path)
        result = run("-c", "/nonexistent/dir/x.db", "--version")
        assert result.exit_code == 0, result.stderr
        assert result.stdout == version_string("zfsbackup-config") + "\n"
        assert not os.path.exists("/nonexistent")
        assert tree(tmp_path) == before

    def test_empty_config_value(self, run):
        result = run("-c", "", "--version")
        assert result.exit_code == 0, result.stderr
        assert result.stdout == version_string("zfsbackup-config") + "\n"

    def test_is_eager_an_invalid_option_before_it_does_not_matter(self, run):
        """Eager: processed before options given earlier on the command line."""
        result = run("--reload-wait", "-1", "--version")
        assert result.exit_code == 0, result.stderr
        assert result.stdout == version_string("zfsbackup-config") + "\n"

    def test_a_config_path_that_is_a_directory(self, run, tmp_path):
        result = run("--version", "-c", tmp_path)
        assert result.exit_code == 0, result.stderr


@pytest.mark.unit
class TestVersionImports:
    def test_version_run_leaves_the_daemon_and_workers_unloaded(self):
        code = textwrap.dedent("""
            import io, runpy, sys
            sys.path.insert(0, sys.argv[1])
            sys.argv = ['zfsbackup-config', '--version']
            out = io.StringIO()
            real, sys.stdout = sys.stdout, out
            status = None
            try:
                runpy.run_module('zfsbackup.cli', run_name='__main__')
            except SystemExit as exc:
                status = exc.code
            finally:
                sys.stdout = real
            bad = sorted(m for m in sys.modules if m in ('zfsbackup.daemon', 'zfsbackup.workers'))
            print(status)
            print(','.join(bad) if bad else 'clean')
            print(out.getvalue().strip())
        """)
        proc = subprocess.run(
            [sys.executable, "-I", "-c", code, str(REPO)],
            capture_output=True, text=True, timeout=120, env=hermetic_env(),
        )
        assert proc.returncode == 0, proc.stderr
        status, imports, line = proc.stdout.strip().splitlines()
        assert status in ("0", "None")
        assert imports == "clean"
        assert VERSION_RE.fullmatch(line), line

    def test_python_dash_m_prints_the_line(self):
        """A real `-I -m zfsbackup.cli --version` (needs the package importable
        without the working directory, i.e. an installed or editable checkout)."""
        proc = subprocess.run(
            [sys.executable, "-I", "-m", "zfsbackup.cli", "--version"],
            capture_output=True, text=True, timeout=120, env=hermetic_env(),
        )
        if proc.returncode != 0 and "No module named" in proc.stderr:
            pytest.skip("zfsbackup is not importable under -I (not installed)")
        assert proc.returncode == 0, proc.stderr
        assert VERSION_RE.fullmatch(proc.stdout.strip()), proc.stdout

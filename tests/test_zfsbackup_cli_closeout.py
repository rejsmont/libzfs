"""Phase 2 close-out, CLI side: `-c`/`-v` on the `list` and `rename` groups, the
console script's SIGTERM disposition after `main()`, `where_flag`, and the
whole-config edit's snapshot_prefix-change warning.

(Rename's lock/restore behaviour is in `test_zfsbackup_cli_rename.py`, the
dot-path `set` prefix warning in `test_zfsbackup_cli_dotpath.py`.)
"""

import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from _cli_support import (
    PLAIN_YAML,
    RICH_YAML,
    ANCHOR,
    Cli,
    db,
    db_state,
    fake_zfs,
    isolated,
    make_db,
    run,
    scratch_tmp,
    stored,
    stub,
    warnings_in,
)
from zfsbackup.cli import main as cli_main
from zfsbackup.cli.write import where_flag
from zfsbackup.config.store import resolve_config_path

REPO = str(Path(__file__).resolve().parent.parent)


@pytest.fixture
def rich(run, db):
    return make_db(run, db)


class TestCommonOptionsOnTheGroups:
    """`-c DB` / `-v` are accepted before the subcommand, between the group and
    the subcommand, and after it -- the same as for every other command."""

    def test_list_group_takes_c(self, run, rich):
        after = run("list", "datasets", "-c", rich).stdout
        assert run("list", "-c", rich, "datasets").stdout == after
        assert run("-c", rich, "list", "datasets").stdout == after
        assert after.splitlines()[0].startswith("NAME")

    def test_list_group_takes_c_for_destinations_and_json(self, run, rich):
        assert run("list", "-c", rich, "destinations", "--json").exit_code == 0

    def test_list_group_takes_v(self, run, rich):
        result = run("list", "-v", "-c", rich, "datasets")
        assert result.exit_code == 0, result.stderr
        assert result.stdout.startswith("NAME")

    def test_the_group_level_database_is_the_one_read(self, run, rich, tmp_path):
        other = make_db(run, tmp_path / "x" / "other.db", PLAIN_YAML, name="other.yaml")
        out = run("list", "-c", other, "datasets").stdout
        assert "tank/a" in out and "tank/data" not in out
        assert "tank/data" in run("list", "-c", rich, "datasets").stdout

    def test_a_subcommand_level_c_wins_over_the_group_level_one(self, run, rich, tmp_path):
        other = make_db(run, tmp_path / "x" / "other.db", PLAIN_YAML, name="other.yaml")
        out = run("list", "-c", other, "datasets", "-c", rich).stdout
        assert "tank/data" in out

    def test_rename_group_takes_c(self, run, rich, fake_zfs):
        result = run("rename", "-c", rich, "dataset", "tank/vm.01", "tank/vm.02")
        assert result.exit_code == 0, result.stderr
        assert "tank/vm.02" in [d.name for d in stored(rich).datasets]

    def test_rename_group_takes_c_for_destinations(self, run, rich, fake_zfs):
        result = run("rename", "-c", rich, "destination", "spare", "spare2")
        assert result.exit_code == 0, result.stderr
        assert "spare2" in stored(rich).destinations

    def test_rename_group_takes_v(self, run, rich, fake_zfs):
        result = run("rename", "-v", "-c", rich, "destination", "spare", "spare2")
        assert result.exit_code == 0, result.stderr

    def test_the_help_of_both_groups_documents_the_options(self, run):
        for group in ("list", "rename"):
            out = run(group, "--help").stdout
            assert "-c, --config" in out and "-v, --verbose" in out

    def test_a_missing_database_through_the_group_option_is_78(self, run, tmp_path):
        result = run("list", "-c", tmp_path / "none.db", "datasets")
        assert result.exit_code == 78
        assert not (tmp_path / "none.db").exists()


class TestWhereFlag:
    def resolved(self, tmp_path, source_env=False, monkeypatch=None):
        return resolve_config_path(str(tmp_path / "config.db"))

    def test_the_default_database_needs_no_flag(self, tmp_path, monkeypatch):
        import zfsbackup.config.store.paths as store_paths

        monkeypatch.setattr(store_paths, "DEFAULT_CONFIG_DB", tmp_path / "d" / "config.db")
        monkeypatch.delenv("ZFSBACKUP_CONFIG", raising=False)
        resolved = resolve_config_path(None)
        assert resolved.source == "default"
        assert where_flag(resolved) == ""

    def test_an_explicit_database_is_named(self, tmp_path):
        resolved = self.resolved(tmp_path)
        assert where_flag(resolved) == f" -c {resolved.resolved_path}"

    def test_a_path_with_spaces_is_shell_quoted(self, tmp_path):
        directory = tmp_path / "my dir"
        directory.mkdir()
        resolved = resolve_config_path(str(directory / "config.db"))
        flag = where_flag(resolved)
        assert flag.startswith(" -c '") and flag.endswith("config.db'")
        import shlex

        assert shlex.split(flag) == ["-c", str(resolved.resolved_path)]

    def test_the_environment_variable_counts_as_explicit(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ZFSBACKUP_CONFIG", str(tmp_path / "env.db"))
        resolved = resolve_config_path(None)
        assert resolved.source != "default"
        assert where_flag(resolved).startswith(" -c ")


class TestMainSigtermDisposition:
    """The console script keeps SIGTERM ignored after `main()` returns -- the
    process is about to exit and a command that has already committed must not be
    killed while it flushes -- whereas an embedded `main(argv)` restores it."""

    def test_embedded_main_restores_the_previous_handler(self):
        def sentinel(signum, frame):
            pass

        previous = signal.signal(signal.SIGTERM, sentinel)
        try:
            with pytest.raises(SystemExit) as info:
                cli_main.main(["--help"])
            assert info.value.code == 0
            assert signal.getsignal(signal.SIGTERM) is sentinel
        finally:
            signal.signal(signal.SIGTERM, previous)

    def test_embedded_main_restores_the_default_too(self):
        previous = signal.signal(signal.SIGTERM, signal.SIG_DFL)
        try:
            with pytest.raises(SystemExit):
                cli_main.main(["--help"])
            assert signal.getsignal(signal.SIGTERM) == signal.SIG_DFL
        finally:
            signal.signal(signal.SIGTERM, previous)

    @pytest.mark.integration
    @pytest.mark.parametrize("argv, code", [(["--help"], 0), (["frobnicate"], 2)])
    def test_the_console_script_leaves_sigterm_ignored(self, tmp_path, argv, code):
        script = (
            "import signal, sys\n"
            "from zfsbackup.cli import main\n"
            "try:\n"
            "    main.main()\n"
            "except SystemExit as exc:\n"
            "    print('EXIT', exc.code, signal.getsignal(signal.SIGTERM) == signal.SIG_IGN)\n"
        )
        done = subprocess.run(
            [sys.executable, "-c", script, *argv], capture_output=True, text=True, timeout=120,
            cwd=str(tmp_path), env=dict(os.environ, PYTHONPATH=REPO),
        )
        assert done.returncode == 0, done.stderr
        assert f"EXIT {code} True" in done.stdout

    @pytest.mark.integration
    def test_a_sigterm_after_main_returned_does_not_kill_the_process(self, tmp_path):
        script = (
            "import os, signal\n"
            "from zfsbackup.cli import main\n"
            "try:\n"
            "    main.main()\n"
            "except SystemExit:\n"
            "    pass\n"
            "os.kill(os.getpid(), signal.SIGTERM)\n"
            "print('SURVIVED', flush=True)\n"
        )
        done = subprocess.run(
            [sys.executable, "-c", script, "--help"], capture_output=True, text=True, timeout=120,
            cwd=str(tmp_path), env=dict(os.environ, PYTHONPATH=REPO),
        )
        assert done.returncode == 0 and "SURVIVED" in done.stdout, done.stderr

    @pytest.mark.integration
    def test_python_dash_m_behaves_like_the_console_script(self, tmp_path):
        script = (
            "import signal, runpy, sys\n"
            "sys.argv = ['zfsbackup-config', '--help']\n"
            "try:\n"
            "    runpy.run_module('zfsbackup.cli', run_name='__main__')\n"
            "except SystemExit:\n"
            "    print('IGN', signal.getsignal(signal.SIGTERM) == signal.SIG_IGN)\n"
        )
        done = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=120,
            cwd=str(tmp_path), env=dict(os.environ, PYTHONPATH=REPO),
        )
        assert "IGN True" in done.stdout, done.stdout + done.stderr


class TestEditSnapshotPrefixChange:
    def edit(self, run, database, stub, *args):
        return run("edit", *args, "--no-prompt", "-c", database, env=stub.env(database))

    @pytest.fixture
    def plain(self, run, db):
        return make_db(run, db, PLAIN_YAML)

    def test_warns_when_the_whole_config_edit_changes_the_prefix(self, run, plain, stub, scratch_tmp):
        stub.plan([{"replace": ['snapshot_prefix: "autosnap"', 'snapshot_prefix: "newsnap"']}])
        result = self.edit(run, plain, stub)
        assert result.exit_code == 0, result.stderr
        assert warnings_in(result) == [
            "warning: snapshot_prefix changes from 'autosnap' to 'newsnap': existing "
            "autosnap_* snapshots will no longer be managed or pruned "
            "(they stay until destroyed by hand)"
        ]
        assert stored(plain).snapshot_prefix == "newsnap"

    def test_no_warning_when_the_prefix_is_unchanged(self, run, plain, stub, scratch_tmp):
        stub.plan([{"replace": ['check_interval: "5m"', 'check_interval: "6m"']}])
        result = self.edit(run, plain, stub)
        assert [w for w in warnings_in(result) if "snapshot_prefix" in w] == []

    def test_a_rejected_buffer_prints_no_prefix_warning(self, run, plain, stub, scratch_tmp):
        # the warning belongs to an edit that is going to be written
        stub.plan([
            {"replace": ['snapshot_prefix: "autosnap"', 'snapshot_prefix: "newsnap"']},
            {"replace": ['check_interval: "5m"', 'check_interval: "bogus"']},
        ])
        result = self.edit(run, plain, stub)
        assert result.exit_code == 1
        assert [w for w in warnings_in(result) if "snapshot_prefix changes" in w] == []

    def test_a_noop_buffer_prints_none(self, run, plain, stub, scratch_tmp):
        stub.plan([])
        assert warnings_in(self.edit(run, plain, stub)) == []

    def test_the_entry_edits_cannot_change_the_prefix(self, run, plain, stub, scratch_tmp):
        stub.plan([{"append": 'snapshot_prefix: "x"\n'}])
        result = run("edit", "dataset", "tank/a", "--no-prompt", "-c", plain, env=stub.env(plain))
        assert result.exit_code == 1 and "unknown key 'snapshot_prefix'" in result.stderr

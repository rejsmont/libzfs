"""`zfsbackup-config edit` -- the whole configuration in `$EDITOR` (plan Phase 2,
item 12), driven by scripted editors (`tests/_editor_stub.py`): small Python
programs that mutate the buffer file they are given. No real, interactive
editor is ever started.

Sections:

- ``TestEditorArgv``          `--editor` > `$VISUAL` > `$EDITOR` > `vi`, shlex.
- ``TestEditSuccess``         a saved edit, what the buffer holds, no database
                              handle while the editor runs, temp file hygiene.
- ``TestEditNoChange``        unchanged, comment-only and noise-only buffers
                              write nothing; literal-only edits persist.
- ``TestEditorFailure``       non-zero exit, killed by a signal, missing binary,
                              buffer removed, non-UTF-8: abort, DB untouched,
                              buffer kept and its path printed.
- ``TestEditPrompting``       non-tty refusal, `--no-prompt`, Retry / Abort / EOF.
- ``TestEditValidation``      YAML syntax errors show the line; model errors.
- ``TestEditRenameGuard``     rename-shaped diffs, `--allow-recreate`.
- ``TestEditAnchorWarnings``  dropped remote / deleted destination warnings and
                              the still-referenced destination refusal.
- ``TestEditConflict``        a `set` run from inside the editor -> exit 75.
- ``TestEditBaseline``        warnings about untouched settings are not repeated.

Every database is created by `import`; temp files are confined to the test's
own directory (``scratch_tmp``) so a leftover buffer is visible.

Review-fix round (same method; failing tests across the edit, rename and
signal files): unknown keys unchecked 11; no "did you mean" 8; duplicate
retention collapse missing from the no-change check 3; interrupted write
unannotated 7; recreate warning off 3; destination-name warning off 3.

Mutation counts (scratch copy, one revert at a time; failing tests across this
file, `test_zfsbackup_cli_rename.py`, the dot-path conflict class and
`test_zfsbackup_cli.py::TestSkeleton`): `$VISUAL`/`$EDITOR` order swapped 2;
no shlex split 3; `--editor` ignored 10; non-zero exit accepted 4; signal
death undetected 2; missing-binary mapping removed 1; tty check removed 2;
`--no-prompt` prompts anyway 1; buffer not kept 14 / never removed 8; no-change
check off 4 / by dataclass `==` 4; rename guard off 2 / `--allow-recreate`
ignored 2; anchor warnings off 4; only the first user of an undeclared
destination named 1; buffer dir 0755 2 / file 0644 2; `name:` accepted in an
entry buffer 2; empty entry buffer accepted 2; extra destination keys accepted
2; `--allow-recreate` with a subcommand accepted 2; group flags not inherited
1; YAML line context dropped 1. A database connection left open while the editor
runs fails 1 test, but only with BOTH release points removed (`read_snapshot`'s
and the editor's own: either alone is enough).
"""

import os
import signal
import stat
import sys

import pytest

import _cli_support
from _cli_support import (
    AMBIGUOUS_YAML,
    NEVER_PRUNE_TEXT,
    PLAIN_YAML,
    RICH_YAML,
    WARNY_YAML,
    Cli,
    buffers_in,
    count_containing,
    db,
    db_state,
    generation,
    isolated,
    make_db,
    run,
    scratch_tmp,
    stderr_lines,
    stored,
    stub,
    warnings_in,
)
from zfsbackup.cli import dotpath, editor
from zfsbackup.cli.errors import CliError
from zfsbackup.config.store import ConfigBusy, dispose_all

# No module-level tier marker: conftest marks every test that declares none as
# `unit`, and the few that spawn a process are marked `integration` individually
# (a test carrying both would be double-counted; see test_marker_tiers.py).

HEADER_START = "# zfsbackup configuration."


@pytest.fixture
def rich(run, db):
    return make_db(run, db)


@pytest.fixture
def plain(run, db):
    return make_db(run, db, PLAIN_YAML)


@pytest.fixture
def tty(monkeypatch):
    monkeypatch.setattr(editor, "_stdin_is_tty", lambda: True)


def edit(run, database, stub, *args, input=None, **env):
    """`edit` with the stub as `$EDITOR`."""
    return run("edit", *args, "-c", database, env=stub.env(database, **env), input=input)


def replace(old, new):
    return {"replace": [old, new]}


FREQ_1H = ('frequency: "1h"', 'frequency: "30m"')


class TestEditorArgv:
    @pytest.fixture(autouse=True)
    def clean_env(self, monkeypatch):
        monkeypatch.delenv("VISUAL", raising=False)
        monkeypatch.delenv("EDITOR", raising=False)

    def test_defaults_to_vi(self):
        assert editor.editor_argv() == ["vi"]

    def test_editor_variable(self, monkeypatch):
        monkeypatch.setenv("EDITOR", "nano")
        assert editor.editor_argv() == ["nano"]

    def test_visual_beats_editor(self, monkeypatch):
        monkeypatch.setenv("EDITOR", "nano")
        monkeypatch.setenv("VISUAL", "emacs")
        assert editor.editor_argv() == ["emacs"]

    def test_the_flag_beats_both(self, monkeypatch):
        monkeypatch.setenv("EDITOR", "nano")
        monkeypatch.setenv("VISUAL", "emacs")
        assert editor.editor_argv("micro") == ["micro"]

    def test_the_value_is_split_like_a_shell_would(self, monkeypatch):
        monkeypatch.setenv("EDITOR", "code -w")
        assert editor.editor_argv() == ["code", "-w"]
        assert editor.editor_argv("subl --wait --new-window") == ["subl", "--wait", "--new-window"]

    def test_quoting_is_honoured(self):
        assert editor.editor_argv("'/opt/my editor/ed' -x") == ["/opt/my editor/ed", "-x"]

    @pytest.mark.parametrize("blank", ["", "   "])
    def test_blank_values_fall_through(self, monkeypatch, blank):
        monkeypatch.setenv("EDITOR", "nano")
        monkeypatch.setenv("VISUAL", blank)
        assert editor.editor_argv(blank) == ["nano"]

    def test_unparseable_value_is_a_clean_error(self):
        with pytest.raises(CliError, match="cannot parse --editor"):
            editor.editor_argv("vi 'unterminated")

    def test_the_cli_flag_wins_over_the_environment(self, run, rich, stub):
        # $VISUAL runs the stub plainly; --editor runs it with an extra
        # argument. The argument in the log shows which one was started.
        stub.plan([])
        env = stub.env(rich, VISUAL=_cli_support.STUB_COMMAND)
        result = run(
            "edit", "--no-prompt", "--editor", f"{_cli_support.STUB_COMMAND} --flag", "-c", rich, env=env
        )
        assert result.exit_code == 0, result.stderr
        assert stub.runs == 1
        assert stub.log[0]["argv"][0] == "--flag"
        assert stub.log[0]["argv"][-1] == stub.log[0]["buffer"]

    def test_environment_editor_arguments_precede_the_buffer(self, run, rich, stub):
        stub.plan([])
        env = stub.env(rich)
        env["EDITOR"] = f"{_cli_support.STUB_COMMAND} -w --extra"
        result = run("edit", "--no-prompt", "-c", rich, env=env)
        assert result.exit_code == 0, result.stderr
        argv = stub.log[0]["argv"]
        assert argv[:2] == ["-w", "--extra"] and argv[-1] == stub.log[0]["buffer"]

    def test_visual_beats_editor_through_the_cli(self, run, rich, stub):
        stub.plan([])
        env = stub.env(rich, VISUAL=f"{_cli_support.STUB_COMMAND} --visual", EDITOR="/nonexistent/editor")
        assert run("edit", "--no-prompt", "-c", rich, env=env).exit_code == 0
        assert stub.log[0]["argv"][0] == "--visual"


class TestEditSuccess:
    def test_a_saved_edit_is_written_and_reported(self, run, plain, stub, scratch_tmp):
        stub.plan([replace(*FREQ_1H)])
        gen = generation(plain)
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        lines = result.stdout.splitlines()
        assert lines[0] == "Configuration saved (datasets changed: tank/a)."
        assert lines[1].startswith(f"config updated (generation {gen + 1})")
        assert stored(plain).datasets[0].frequency.literal == "30m"
        assert generation(plain) == gen + 1
        assert buffers_in(scratch_tmp) == []

    def test_the_buffer_is_the_header_plus_what_export_prints(self, run, plain, stub, scratch_tmp):
        stub.plan([])
        edit(run, plain, stub, "--no-prompt")
        before = stub.log[0]["before"]
        assert before.startswith(HEADER_START)
        body = before.split("(Comments are not stored.)\n", 1)[1]
        assert body == run("export", "-c", plain).stdout

    def test_the_buffer_is_private(self, run, plain, stub, scratch_tmp):
        stub.plan([])
        old = os.umask(0)
        try:
            edit(run, plain, stub, "--no-prompt")
        finally:
            os.umask(old)
        record = stub.log[0]
        assert record["dir_mode"] == 0o700 and record["file_mode"] == 0o600
        assert record["buffer"].endswith("/config.yaml")

    def test_no_database_handle_is_open_while_the_editor_runs(self, run, plain, stub, scratch_tmp):
        stub.plan([])
        edit(run, plain, stub, "--no-prompt")
        assert stub.log[0]["sidecars"] == []

    def test_a_new_dataset_can_be_added(self, run, plain, stub, scratch_tmp):
        stub.plan([{"append": '  - name: "tank/b"\n    frequency: "2h"\n'}])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert "datasets created: tank/b" in result.stdout
        assert [d.name for d in stored(plain).datasets] == ["tank/a", "tank/b"]

    def test_a_dataset_can_be_deleted(self, run, db, stub, scratch_tmp):
        make_db(run, db, RICH_YAML)
        stub.plan([{"write": run("export", "-c", db).stdout.split("  - name: \"a\"")[0]}])
        result = edit(run, db, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert "datasets deleted: a, a.retention" in result.stdout

    def test_global_settings_edit(self, run, plain, stub, scratch_tmp):
        stub.plan([replace('check_interval: "5m"', 'check_interval: "10m"')])
        result = edit(run, plain, stub, "--no-prompt")
        assert "settings changed: check_interval" in result.stdout

    def test_the_daemon_notice_follows_a_save(self, run, plain, stub, scratch_tmp):
        stub.plan([replace(*FREQ_1H)])
        assert "no running daemon detected" in edit(run, plain, stub, "--no-prompt").stdout

    def test_c_before_the_subcommand(self, run, plain, stub, scratch_tmp):
        stub.plan([replace(*FREQ_1H)])
        result = run("-c", plain, "edit", "--no-prompt", env=stub.env(plain))
        assert result.exit_code == 0, result.stderr

    def test_a_missing_database_is_never_created(self, run, db, stub, scratch_tmp):
        stub.plan([])
        result = edit(run, db, stub, "--no-prompt")
        assert result.exit_code == 78
        assert stub.runs == 0 and not db.parent.exists()


class TestEditNoChange:
    NO_CHANGES = "No changes; the stored configuration was not touched.\n"

    def check_untouched(self, run, database, result, scratch_tmp, gen, state):
        assert result.exit_code == 0, result.stderr
        assert result.stdout == self.NO_CHANGES  # and no daemon notice
        assert db_state(database) == state and generation(database) == gen
        assert buffers_in(scratch_tmp) == []

    def snapshot(self, database):
        return generation(database), db_state(database)

    def test_an_untouched_buffer(self, run, plain, stub, scratch_tmp):
        gen, state = self.snapshot(plain)
        stub.plan([])
        result = edit(run, plain, stub, "--no-prompt")
        self.check_untouched(run, plain, result, scratch_tmp, gen, state)

    def test_a_comment_only_edit(self, run, plain, stub, scratch_tmp):
        gen, state = self.snapshot(plain)
        stub.plan([{"append": "\n# just a note to myself\n"}, replace("# (Comments", "# edited (Comments")])
        result = edit(run, plain, stub, "--no-prompt")
        self.check_untouched(run, plain, result, scratch_tmp, gen, state)

    def test_quoting_and_blank_line_noise(self, run, plain, stub, scratch_tmp):
        gen, state = self.snapshot(plain)
        stub.plan([
            replace('check_interval: "5m"', "check_interval: 5m"),
            replace('- name: "tank/a"', "- name: tank/a"),
            {"append": "\n\n\n"},
        ])
        result = edit(run, plain, stub, "--no-prompt")
        self.check_untouched(run, plain, result, scratch_tmp, gen, state)

    def test_key_order_noise(self, run, plain, stub, scratch_tmp):
        gen, state = self.snapshot(plain)
        stub.plan([{"replace": ['snapshot_prefix: "autosnap"\ncheck_interval: "5m"\n',
                                'check_interval: "5m"\nsnapshot_prefix: "autosnap"\n']}])
        result = edit(run, plain, stub, "--no-prompt")
        self.check_untouched(run, plain, result, scratch_tmp, gen, state)

    def test_a_value_changed_and_changed_back_within_one_session(self, run, plain, stub, scratch_tmp):
        gen, state = self.snapshot(plain)
        stub.plan([replace('check_interval: "5m"', 'check_interval: "5m"  # same')])
        result = edit(run, plain, stub, "--no-prompt")
        self.check_untouched(run, plain, result, scratch_tmp, gen, state)

    def test_a_literal_only_edit_persists(self, run, plain, stub, scratch_tmp):
        gen = generation(plain)
        stub.plan([replace('check_interval: "5m"', 'check_interval: "300s"'),
                   replace('frequency: "1h"', 'frequency: "60m"')])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert result.stdout.startswith("Configuration saved")
        assert generation(plain) == gen + 1
        config = stored(plain)
        assert config.check_interval.literal == "300s"
        assert config.datasets[0].frequency.literal == "60m"
        assert run("get", "check_interval", "-c", plain).stdout == "300s\n"

    def test_a_retention_age_literal_only_edit_persists(self, run, plain, stub, scratch_tmp):
        stub.plan([replace('"1d": "30d"', '"24h": "30d"')])
        assert edit(run, plain, stub, "--no-prompt").stdout.startswith("Configuration saved")
        assert [r.age.literal for r in stored(plain).datasets[0].retention_rules] == ["24h"]

    def test_the_summary_names_a_literal_only_change(self, run, plain, stub, scratch_tmp):
        stub.plan([replace('check_interval: "5m"', 'check_interval: "5m"'), replace('frequency: "1h"', 'frequency: "60m"')])
        result = edit(run, plain, stub, "--no-prompt")
        assert "datasets changed: tank/a" in result.stdout

    def test_an_exact_duplicate_retention_rule_is_collapsed_before_comparing(
        self, run, plain, stub, scratch_tmp
    ):
        gen, state = self.snapshot(plain)
        stub.plan([replace('      "1d": "30d"\n', '      "1d": "30d"\n      "24h": "720h"\n')])
        result = edit(run, plain, stub, "--no-prompt")
        self.check_untouched(run, plain, result, scratch_tmp, gen, state)

    def test_an_exact_duplicate_alongside_a_real_edit_is_stored_collapsed(self, run, plain, stub, scratch_tmp):
        stub.plan([
            replace('      "1d": "30d"\n', '      "1d": "30d"\n      "24h": "720h"\n'),
            replace('frequency: "1h"', 'frequency: "30m"'),
        ])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert count_containing(warnings_in(result), "same rule") == 1
        rules = stored(plain).datasets[0].retention_rules
        assert [(r.age.literal, r.keep_for.literal) for r in rules] == [("1d", "30d")]


class TestEditorFailure:
    def check(self, run, database, result, stub, scratch_tmp, state):
        assert result.exit_code == 1, result.stdout + result.stderr
        assert db_state(database) == state
        kept = buffers_in(scratch_tmp)
        assert len(kept) == 1
        assert f"your edit is kept in {kept[0] / 'config.yaml'}" in result.stderr
        assert "nothing was written to the database" in result.stderr
        return kept[0] / "config.yaml"

    def test_a_non_zero_exit_aborts_and_keeps_the_buffer(self, run, plain, stub, scratch_tmp):
        state = db_state(plain)
        stub.plan([replace(*FREQ_1H), {"exit": 3}])
        result = edit(run, plain, stub, "--no-prompt")
        assert "exited with status 3" in result.stderr and "nothing was changed" in result.stderr
        buffer = self.check(run, plain, result, stub, scratch_tmp, state)
        assert 'frequency: "30m"' in buffer.read_text()  # the operator's edit is not lost
        assert stat.S_IMODE(buffer.stat().st_mode) == 0o600
        assert stat.S_IMODE(buffer.parent.stat().st_mode) == 0o700

    def test_the_status_is_reported_exactly(self, run, plain, stub, scratch_tmp):
        stub.plan([{"exit": 1}])
        assert "exited with status 1" in edit(run, plain, stub, "--no-prompt").stderr

    @pytest.mark.parametrize("sig, name", [(signal.SIGKILL, "SIGKILL"), (signal.SIGTERM, "SIGTERM")])
    def test_killed_by_a_signal(self, run, plain, stub, scratch_tmp, sig, name):
        state = db_state(plain)
        stub.plan([replace(*FREQ_1H), {"kill": int(sig)}])
        result = edit(run, plain, stub, "--no-prompt")
        assert f"killed by {name}" in result.stderr
        self.check(run, plain, result, stub, scratch_tmp, state)

    def test_a_missing_editor_binary(self, run, plain, stub, scratch_tmp):
        state = db_state(plain)
        result = run("edit", "--no-prompt", "--editor", "/nonexistent/zfsbackup-editor", "-c", plain)
        assert "editor not found" in result.stderr and "--editor" in result.stderr
        self.check(run, plain, result, stub, scratch_tmp, state)

    def test_an_editor_that_cannot_be_executed(self, run, plain, tmp_path, scratch_tmp):
        state = db_state(plain)
        script = tmp_path / "not-executable"
        script.write_text("#!/bin/sh\nexit 0\n")
        script.chmod(0o644)
        result = run("edit", "--no-prompt", "--editor", str(script), "-c", plain)
        assert result.exit_code == 1
        assert "cannot run the editor" in result.stderr
        assert db_state(plain) == state

    def test_the_editor_removing_the_buffer(self, run, plain, stub, scratch_tmp):
        state = db_state(plain)
        stub.plan([{"remove": True}])
        result = edit(run, plain, stub, "--no-prompt")
        assert "removed the buffer" in result.stderr
        assert result.exit_code == 1 and db_state(plain) == state

    def test_a_buffer_that_is_not_utf8_is_rejected_not_crashed(self, run, plain, stub, scratch_tmp):
        state = db_state(plain)
        stub.plan([{"write_bytes_hex": "ff fe 00 41".replace(" ", "")}])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 1
        assert "not valid UTF-8" in result.stderr and "Traceback" not in result.stderr
        assert db_state(plain) == state
        assert len(buffers_in(scratch_tmp)) == 1

    def test_the_kept_buffer_can_be_imported(self, run, plain, stub, scratch_tmp, tmp_path):
        stub.plan([replace(*FREQ_1H), {"exit": 2}])
        result = edit(run, plain, stub, "--no-prompt")
        buffer = buffers_in(scratch_tmp)[0] / "config.yaml"
        again = run("import", buffer, "-c", plain, "--no-backup")
        assert again.exit_code == 0, again.stderr
        assert stored(plain).datasets[0].frequency.literal == "30m"

    def test_failing_before_the_edit_leaves_nothing_to_keep(self, run, plain, stub, scratch_tmp):
        # an unparsable $EDITOR is refused before any buffer exists
        result = run("edit", "--no-prompt", "--editor", "vi 'unterminated", "-c", plain)
        assert result.exit_code == 1 and "cannot parse" in result.stderr
        assert buffers_in(scratch_tmp) == []


class TestEditPrompting:
    BAD = 'check_interval: "5m"'

    def test_a_non_tty_stdin_is_refused_without_no_prompt(self, run, plain, stub, scratch_tmp):
        state = db_state(plain)
        stub.plan([replace(*FREQ_1H)])
        result = edit(run, plain, stub)
        assert result.exit_code == 1
        assert "interactive terminal" in result.stderr
        for pointer in ("export", "import", "--no-prompt"):
            assert pointer in result.stderr
        assert stub.runs == 0  # the editor never started
        assert buffers_in(scratch_tmp) == []
        assert db_state(plain) == state

    def test_no_prompt_needs_no_terminal(self, run, plain, stub, scratch_tmp):
        stub.plan([replace(*FREQ_1H)])
        assert edit(run, plain, stub, "--no-prompt").exit_code == 0

    def test_no_prompt_never_prompts_and_a_rejected_buffer_aborts(self, run, plain, stub, scratch_tmp, tty):
        state = db_state(plain)
        stub.plan([replace(self.BAD, 'check_interval: "bogus"')])
        # `r` is on stdin, and even a terminal is available: --no-prompt must not look.
        result = edit(run, plain, stub, "--no-prompt", input="r\n")
        assert result.exit_code == 1
        assert "[r]etry" not in result.stdout + result.stderr
        assert "--no-prompt never retries" in result.stderr
        assert stub.runs == 1
        assert db_state(plain) == state
        assert len(buffers_in(scratch_tmp)) == 1
        assert "your edit is kept in" in result.stderr

    def test_retry_reopens_the_same_buffer_and_a_fixed_buffer_saves(self, run, plain, stub, scratch_tmp, tty):
        gen = generation(plain)
        stub.plan(
            [replace(self.BAD, 'check_interval: "bogus"')],
            [replace('check_interval: "bogus"', 'check_interval: "7m"')],
        )
        result = edit(run, plain, stub, input="r\n")
        assert result.exit_code == 0, result.stderr
        assert "[r]etry editing or [a]bort" in result.stderr
        assert stub.runs == 2
        first, second = stub.log
        assert first["buffer"] == second["buffer"]
        assert 'check_interval: "bogus"' in second["before"]  # the operator's text, preserved
        assert stored(plain).check_interval.literal == "7m"
        assert generation(plain) == gen + 1
        assert buffers_in(scratch_tmp) == []  # saved: cleaned up

    def test_retry_is_the_default_answer(self, run, plain, stub, scratch_tmp, tty):
        stub.plan(
            [replace(self.BAD, 'check_interval: "bogus"')],
            [replace('check_interval: "bogus"', 'check_interval: "7m"')],
        )
        assert edit(run, plain, stub, input="\n").exit_code == 0
        assert stub.runs == 2

    def test_abort_keeps_the_buffer_and_writes_nothing(self, run, plain, stub, scratch_tmp, tty):
        state = db_state(plain)
        stub.plan([replace(self.BAD, 'check_interval: "bogus"')])
        result = edit(run, plain, stub, input="a\n")
        assert result.exit_code == 1
        assert "edit abandoned; nothing was written" in result.stderr
        assert "never retries" not in result.stderr
        assert stub.runs == 1 and db_state(plain) == state
        assert len(buffers_in(scratch_tmp)) == 1

    def test_end_of_input_at_the_prompt_aborts(self, run, plain, stub, scratch_tmp, tty):
        state = db_state(plain)
        stub.plan([replace(self.BAD, 'check_interval: "bogus"')])
        result = edit(run, plain, stub, input="")
        assert result.exit_code == 1
        assert "edit abandoned" in result.stderr
        assert stub.runs == 1 and db_state(plain) == state
        assert len(buffers_in(scratch_tmp)) == 1

    def test_an_invalid_answer_is_asked_again(self, run, plain, stub, scratch_tmp, tty):
        stub.plan(
            [replace(self.BAD, 'check_interval: "bogus"')],
            [replace('check_interval: "bogus"', 'check_interval: "7m"')],
        )
        result = edit(run, plain, stub, input="maybe\nr\n")
        assert result.exit_code == 0, result.stderr
        assert stub.runs == 2

    def test_several_retries(self, run, plain, stub, scratch_tmp, tty):
        stub.plan(
            [replace(self.BAD, 'check_interval: "x"')],
            [replace('check_interval: "x"', 'check_interval: "y"')],
            [replace('check_interval: "y"', 'check_interval: "9m"')],
        )
        assert edit(run, plain, stub, input="r\nr\n").exit_code == 0
        assert stub.runs == 3 and stored(plain).check_interval.literal == "9m"

    def test_a_retry_that_is_then_left_unchanged_writes_nothing(self, run, plain, stub, scratch_tmp, tty):
        # first the buffer is wrong; on retry the operator reverts it by hand
        state = db_state(plain)
        stub.plan(
            [replace(self.BAD, 'check_interval: "bogus"')],
            [replace('check_interval: "bogus"', 'check_interval: "5m"')],
        )
        result = edit(run, plain, stub, input="r\n")
        assert result.exit_code == 0
        assert "No changes" in result.stdout
        assert db_state(plain) == state and buffers_in(scratch_tmp) == []

    def test_the_editor_failing_on_a_retry_aborts(self, run, plain, stub, scratch_tmp, tty):
        state = db_state(plain)
        stub.plan([replace(self.BAD, 'check_interval: "bogus"')], [{"exit": 5}])
        result = edit(run, plain, stub, input="r\n")
        assert result.exit_code == 1 and "exited with status 5" in result.stderr
        assert db_state(plain) == state and len(buffers_in(scratch_tmp)) == 1


class TestEditValidation:
    def test_a_yaml_syntax_error_shows_the_line_and_a_caret(self, run, plain, stub, scratch_tmp):
        stub.plan([replace('check_interval: "5m"', 'check_interval: "5m\n')])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 1
        lines = result.stderr.splitlines()
        message = next(l for l in lines if l.startswith("error: invalid YAML at line "))
        assert ", column " in message
        # the offending buffer line is quoted back with a caret under it
        index = lines.index(message)
        assert lines[index + 1].startswith("    ") and "^" in lines[index + 2]

    def test_the_quoted_line_is_the_buffers_own_line(self, run, plain, stub, scratch_tmp):
        import re

        stub.plan([replace('api_port: 8080', 'api_port: [')])
        result = edit(run, plain, stub, "--no-prompt")
        kept = buffers_in(scratch_tmp)[0] / "config.yaml"
        text = kept.read_text().splitlines()
        lines = result.stderr.splitlines()
        first = next(l for l in lines if l.startswith("error: invalid YAML at line "))
        number = int(re.search(r"at line (\d+)", first).group(1))
        shown = lines[lines.index(first) + 1]
        assert shown == "    " + text[number - 1]

    @pytest.mark.parametrize(
        "old, new, fragment",
        [
            ('check_interval: "5m"', 'check_interval: "bogus"', "Invalid duration"),
            ("api_port: 8080", "api_port: 70000", "api_port"),
            ('frequency: "1h"', 'frequency: "0m"', "frequency"),
            ('snapshot_prefix: "autosnap"', "snapshot_prefix:", "snapshot_prefix"),
            ("dry_run: false", "dry_run: maybe", "dry_run"),
        ],
    )
    def test_a_model_error_is_shown_and_nothing_is_written(self, run, plain, stub, scratch_tmp, old, new, fragment):
        state = db_state(plain)
        stub.plan([replace(old, new)])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 1
        assert fragment in result.stderr
        assert "Traceback" not in result.stderr
        assert db_state(plain) == state

    def test_a_keep_for_collision_is_refused(self, run, rich, stub, scratch_tmp):
        state = db_state(rich)
        stub.plan([replace('"1w": "3M"', '"1w": "30d"')])
        result = edit(run, rich, stub, "--no-prompt")
        assert result.exit_code == 1 and "same duration" in result.stderr
        assert db_state(rich) == state

    def test_a_top_level_list_is_not_a_configuration(self, run, plain, stub, scratch_tmp):
        state = db_state(plain)
        stub.plan([{"write": "- a\n- b\n"}])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 1 and "Traceback" not in result.stderr
        assert db_state(plain) == state

    def test_an_emptied_buffer_is_not_a_deletion_of_everything(self, run, plain, stub, scratch_tmp):
        state = db_state(plain)
        stub.plan([{"write": ""}])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 1
        assert db_state(plain) == state

    def test_removing_all_datasets_is_refused(self, run, plain, stub, scratch_tmp):
        state = db_state(plain)
        stub.plan([{"write": 'check_interval: "5m"\ndatasets: []\n'}])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 1
        assert db_state(plain) == state

    def test_semantic_warnings_for_the_edit_are_shown_once(self, run, plain, stub, scratch_tmp):
        stub.plan([replace('api_port: 8080', 'api_port: 0')])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert count_containing(warnings_in(result), "api_port is 0") == 1


class TestEditRenameGuard:
    def rename_dataset_edit(self, new="tank/b"):
        return replace('- name: "tank/a"', f'- name: "{new}"')

    def test_a_dataset_rename_is_refused_and_points_at_rename(self, run, plain, stub, scratch_tmp):
        state = db_state(plain)
        stub.plan([self.rename_dataset_edit()])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 1
        assert "looks like a rename (dataset 'tank/a' -> 'tank/b')" in result.stderr
        assert "zfsbackup-config rename dataset tank/a tank/b" in result.stderr
        assert "--allow-recreate" in result.stderr
        assert db_state(plain) == state

    def test_allow_recreate_applies_the_delete_and_create(self, run, plain, stub, scratch_tmp):
        gen = generation(plain)
        stub.plan([self.rename_dataset_edit()])
        result = edit(run, plain, stub, "--no-prompt", "--allow-recreate")
        assert result.exit_code == 0, result.stderr
        assert "datasets created: tank/b" in result.stdout and "datasets deleted: tank/a" in result.stdout
        assert [d.name for d in stored(plain).datasets] == ["tank/b"]
        assert generation(plain) == gen + 1

    def test_a_destination_rename_is_refused(self, run, rich, stub, scratch_tmp):
        state = db_state(rich)
        stub.plan([replace('  "spare":', '  "spare2":')])
        result = edit(run, rich, stub, "--no-prompt")
        assert result.exit_code == 1
        assert "rename (destination 'spare' -> 'spare2')" in result.stderr
        assert "rename destination spare spare2" in result.stderr
        assert db_state(rich) == state

    def test_allow_recreate_for_a_destination(self, run, rich, stub, scratch_tmp):
        stub.plan([replace('  "spare":', '  "spare2":')])
        result = edit(run, rich, stub, "--no-prompt", "--allow-recreate")
        assert result.exit_code == 0, result.stderr
        assert sorted(stored(rich).destinations) == ["dc.two", "offsite", "spare2"]

    def test_a_rename_that_also_changes_a_field_is_not_rename_shaped(self, run, plain, stub, scratch_tmp):
        stub.plan([self.rename_dataset_edit(), replace('frequency: "1h"', 'frequency: "2h"')])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert [d.name for d in stored(plain).datasets] == ["tank/b"]

    def test_a_literal_difference_makes_it_not_rename_shaped(self, run, plain, stub, scratch_tmp):
        # literals count: 1h -> 60m is a change, so this is a delete + create.
        stub.plan([self.rename_dataset_edit(), replace('frequency: "1h"', 'frequency: "60m"')])
        assert edit(run, plain, stub, "--no-prompt").exit_code == 0

    def test_two_datasets_replaced_is_not_rename_shaped(self, run, rich, stub, scratch_tmp):
        stub.plan([replace('- name: "a"', '- name: "a2"'), replace('- name: "a.retention"', '- name: "a3"')])
        assert edit(run, rich, stub, "--no-prompt").exit_code == 0

    def test_adding_one_and_keeping_the_old_is_a_create_not_a_rename(self, run, plain, stub, scratch_tmp):
        stub.plan([{"append": '  - name: "tank/b"\n    frequency: "1h"\n    retention:\n      "1d": "30d"\n'}])
        assert edit(run, plain, stub, "--no-prompt").exit_code == 0
        assert [d.name for d in stored(plain).datasets] == ["tank/a", "tank/b"]

    def test_allow_recreate_is_a_usage_error_with_a_subcommand(self, run, rich, stub):
        stub.plan([])
        result = run("edit", "--allow-recreate", "dataset", "a", "--no-prompt", "-c", rich, env=stub.env(rich))
        assert result.exit_code == 2
        assert stub.runs == 0


class TestEditAnchorWarnings:
    def test_dropping_a_remote_warns_about_its_anchor(self, run, rich, stub, scratch_tmp):
        stub.plan([{"replace": [
            '      - destination: "offsite"\n      - destination: "dc.two"',
            '      - destination: "dc.two"',
        ]}])
        result = edit(run, rich, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert count_containing(
            warnings_in(result), "datasets[tank/data] no longer sends to 'offsite'"
        ) == 1
        assert "org.zfsbackup:anchor.offsite" in result.stderr
        # (the rest of the wording -- "stops protecting them from pruning" -- is
        # stale since the pruner protects every local anchor; deliberately not pinned)

    def test_deleting_a_destination_warns_about_its_anchor(self, run, rich, stub, scratch_tmp):
        stub.plan([{"replace": ['  "spare":\n    url: "http://spare:8080"\n', ""]}])
        result = edit(run, rich, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert count_containing(warnings_in(result), "destination 'spare' is deleted") == 1
        assert sorted(stored(rich).destinations) == ["dc.two", "offsite"]

    def test_a_destination_dropped_from_a_dataset_and_deleted_warns_once(self, run, rich, stub, scratch_tmp):
        stub.plan([
            {"replace": ['      - destination: "offsite"\n', ""]},
            {"replace": ['  "offsite":\n    url: "http://offsite:8080"\n', ""]},
        ])
        result = edit(run, rich, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        warned = [w for w in warnings_in(result) if "offsite" in w]
        assert len(warned) == 1 and "no longer sends to 'offsite'" in warned[0]

    def test_no_warning_when_nothing_is_dropped(self, run, rich, stub, scratch_tmp):
        stub.plan([replace('frequency: "4h"', 'frequency: "3h"')])
        result = edit(run, rich, stub, "--no-prompt")
        assert result.exit_code == 0 and "anchor" not in result.stderr

    def test_deleting_a_destination_that_is_still_used_names_every_user(self, run, db, stub, scratch_tmp):
        text = (
            'destinations:\n  shared:\n    url: "http://s"\n'
            'datasets:\n'
            '  - name: "tank/one"\n    remote:\n      - destination: "shared"\n'
            '  - name: "tank/two"\n    remote:\n      - destination: "shared"\n'
            '  - name: "tank/three"\n    remote:\n      - destination: "shared"\n'
            '  - name: "tank/other"\n'
        )
        make_db(run, db, text)
        state = db_state(db)
        stub.plan([{"replace": ['  "shared":\n    url: "http://s"\n', ""]}])
        result = edit(run, db, stub, "--no-prompt")
        assert result.exit_code == 1
        assert "destination 'shared' is not declared" in result.stderr
        for name in ("tank/one", "tank/two", "tank/three"):
            assert name in result.stderr
        assert "tank/other" not in result.stderr.split("still used by", 1)[1].split(".", 1)[0]
        assert db_state(db) == state

    def test_two_undeclared_destinations_are_both_reported(self, run, db, stub, scratch_tmp):
        text = (
            'destinations:\n  d1:\n    url: "http://1"\n  d2:\n    url: "http://2"\n'
            'datasets:\n  - name: "tank/x"\n    remote:\n      - destination: "d1"\n      - destination: "d2"\n'
        )
        make_db(run, db, text)
        stub.plan([{"replace": ['destinations:\n  "d1":\n    url: "http://1"\n  "d2":\n    url: "http://2"\n', ""]}])
        result = edit(run, db, stub, "--no-prompt")
        assert result.exit_code == 1
        assert "'d1'" in result.stderr and "'d2'" in result.stderr


class TestEditConflict:
    def test_a_set_from_inside_the_editor_is_a_conflict(self, run, plain, stub, scratch_tmp):
        stub.plan([
            replace(*FREQ_1H),
            {"cli": ["-c", str(plain), "set", "api_port", "9999"]},
        ])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 75, result.stderr
        assert "config changed underneath you" in result.stderr
        assert "Nothing was written" in result.stderr
        config = stored(plain)
        assert config.api_port == 9999                       # the other writer's change stands
        assert config.datasets[0].frequency.literal == "1h"  # ours was not applied
        kept = buffers_in(scratch_tmp)
        assert len(kept) == 1
        assert f"your edit is kept in {kept[0] / 'config.yaml'}" in result.stderr
        assert 'frequency: "30m"' in (kept[0] / "config.yaml").read_text()

    def test_the_conflict_does_not_retry_or_write(self, run, plain, stub, scratch_tmp, tty):
        stub.plan([replace(*FREQ_1H), {"cli": ["-c", str(plain), "set", "api_port", "9999"]}])
        result = edit(run, plain, stub, input="r\n")
        assert result.exit_code == 75
        assert stub.runs == 1
        assert "[r]etry" not in result.stderr

    def test_a_no_change_edit_ignores_a_concurrent_write(self, run, plain, stub, scratch_tmp):
        # nothing to write, so nothing to conflict over
        stub.plan([{"cli": ["-c", str(plain), "set", "api_port", "9999"]}])
        result = edit(run, plain, stub, "--no-prompt")
        # The buffer still equals what it was handed: no changes (the other
        # writer's value is not ours to undo).
        assert result.exit_code == 0 and "No changes" in result.stdout
        assert stored(plain).api_port == 9999


class TestEditBaseline:
    @pytest.fixture
    def warny(self, run, db):
        make_db(run, db, WARNY_YAML)
        return db

    def test_warnings_about_untouched_settings_are_not_repeated(self, run, warny, stub, scratch_tmp):
        stub.plan([replace('check_interval: "5m"', 'check_interval: "10m"')])
        result = edit(run, warny, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert warnings_in(result) == []

    def test_only_the_new_warning_is_shown(self, run, warny, stub, scratch_tmp):
        stub.plan([replace('- name: "tank/ok"\n    enabled: true\n    frequency: "1h"\n    recursive: false\n    retention:\n      "1d": "30d"',
                           '- name: "tank/ok"\n    enabled: true\n    frequency: "1h"\n    recursive: false\n    retention: forever')])
        result = edit(run, warny, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        lines = warnings_in(result)
        assert len(lines) == 1 and "tank/ok].retention is empty" in lines[0]

    def test_a_rejected_buffer_is_reported_with_an_error_not_a_warning(self, run, warny, stub, scratch_tmp):
        stub.plan([replace('check_interval: "5m"', 'check_interval: "bogus"')])
        result = edit(run, warny, stub, "--no-prompt")
        assert result.exit_code == 1
        assert warnings_in(result) == []


class TestEditUnknownKeys:
    """The model ignores keys it does not know, so a misspelt `retenton:` would
    silently fall back to a default; in an edit buffer it is refused, naming the
    path and the closest valid key."""

    @pytest.fixture
    def attempt(self, run, rich, stub, scratch_tmp):
        def go(plan_action, *args):
            state = db_state(rich)
            stub.plan([plan_action])
            result = run("edit", *args, "--no-prompt", "-c", rich, env=stub.env(rich))
            assert result.exit_code == 1, result.stdout + result.stderr
            assert db_state(rich) == state
            assert len(buffers_in(scratch_tmp)) == 1
            return result.stderr
        return go

    def test_top_level(self, attempt):
        err = attempt(replace("snapshot_prefix:", "snapshot_prefx:"))
        assert "top level: unknown key 'snapshot_prefx' (did you mean 'snapshot_prefix'?)" in err
        assert "valid keys: snapshot_prefix, check_interval" in err

    def test_dataset(self, attempt):
        err = attempt(replace("    retention:\n", "    retenton:\n"))
        assert "datasets[tank/data]: unknown key 'retenton' (did you mean 'retention'?)" in err

    def test_remote_entry(self, attempt):
        err = attempt(replace('        frequency: "4h"', '        frequncy: "4h"'))
        assert "datasets[tank/data].remote[dc.two]: unknown key 'frequncy' (did you mean 'frequency'?)" in err

    def test_destination(self, attempt):
        err = attempt(replace('    url: "http://dc2:8080"', '    urll: "http://dc2:8080"'))
        assert "destinations[dc.two]: unknown key 'urll' (did you mean 'url'?)" in err

    def test_remote_backup(self, attempt):
        err = attempt({"append": 'remote_backup:\n  enabled: true\n  target_datase: "tank/r"\n'})
        assert "remote_backup: unknown key 'target_datase' (did you mean 'target_dataset'?)" in err

    def test_no_suggestion_when_nothing_is_close(self, attempt):
        err = attempt({"append": "zzzzzz: 1\n"})
        assert "unknown key 'zzzzzz'; valid keys:" in err and "did you mean" not in err

    def test_edit_dataset_checks_keys_too(self, attempt):
        err = attempt(
            {"append": "retenton:\n  1d: 30d\n"}, "dataset", "tank/vm.01"
        )
        assert "datasets[tank/vm.01]: unknown key 'retenton' (did you mean 'retention'?)" in err

    def test_edit_dataset_checks_remote_entry_keys(self, attempt):
        err = attempt(
            {"write": 'remote:\n  - destination: "offsite"\n    frequncy: "2h"\n'}, "dataset", "tank/vm.01"
        )
        assert "datasets[tank/vm.01].remote[offsite]: unknown key 'frequncy'" in err

    def test_edit_destination_stays_url_only(self, attempt):
        err = attempt({"append": 'frequency: "1h"\n'}, "destination", "dc.two")
        assert "unknown key(s) 'frequency'; a destination has only a url:" in err

    def test_all_the_valid_keys_are_accepted(self, run, rich, stub, scratch_tmp):
        stub.plan([{"append": ""}, ])
        full = (
            'snapshot_prefix: "autosnap"\ncheck_interval: "5m"\nprune_interval: "1h"\n'
            'api_host: "127.0.0.1"\napi_port: 8080\ndry_run: false\nclient_id_file: "/x/id"\n'
            'destinations:\n  "d":\n    url: "http://d"\n'
            'remote_backup:\n  enabled: false\n  target_dataset: "tank/r"\n'
            'datasets:\n  - name: "tank/a"\n    enabled: true\n    frequency: "1h"\n    recursive: false\n'
            '    retention:\n      "1d": "30d"\n    remote:\n      - destination: "d"\n'
            '        frequency: "2h"\n        retention:\n          "1d": "7d"\n'
        )
        stub.plan([{"write": full}])
        result = run("edit", "--no-prompt", "-c", rich, env=stub.env(rich))
        assert result.exit_code == 0, result.stderr

    def test_retry_after_an_unknown_key_then_a_fixed_buffer_saves(self, run, rich, stub, scratch_tmp, tty):
        stub.plan(
            [replace("snapshot_prefix:", "snapshot_prefx:")],
            [replace("snapshot_prefx:", "snapshot_prefix:"), replace('check_interval: "5m"', 'check_interval: "6m"')],
        )
        result = run("edit", "-c", rich, env=stub.env(rich), input="r\n")
        assert result.exit_code == 0, result.stderr
        assert stored(rich).check_interval.literal == "6m"


class TestEditInterruptDuringTheWrite:
    def interrupting(self, monkeypatch):
        def commit(*args, **kwargs):
            raise KeyboardInterrupt

        monkeypatch.setattr(editor, "commit_config", commit)

    def test_an_interrupt_after_the_write_began_may_or_may_not_have_been_applied(
        self, run, plain, stub, scratch_tmp, monkeypatch
    ):
        self.interrupting(monkeypatch)
        stub.plan([replace(*FREQ_1H)])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 130
        err = result.stderr
        assert "interrupted" in err
        assert "the edit may or may not have been applied" in err
        assert f"zfsbackup-config -c {plain} export" in err
        kept = buffers_in(scratch_tmp)
        assert len(kept) == 1 and f"Your edit is kept in {kept[0] / 'config.yaml'}" in err
        assert "nothing was written to the database" not in err

    def test_the_hint_has_no_dash_c_for_the_default_database(self, run, tmp_path, stub, scratch_tmp, monkeypatch):
        make_db(run, tmp_path / "default" / "config.db", PLAIN_YAML)
        self.interrupting(monkeypatch)
        stub.plan([replace(*FREQ_1H)])
        result = run("edit", "--no-prompt", env=stub.env())
        assert result.exit_code == 130
        assert "run `zfsbackup-config export` to check" in result.stderr

    @pytest.mark.parametrize(
        "sub, change",
        [
            ((), replace('check_interval: "5m"', 'check_interval: "6m"')),
            (("dataset", "a"), replace("recursive: false", "recursive: true")),
            (("destination", "dc.two"), replace("http://dc2:8080", "http://dc3:1")),
        ],
    )
    def test_the_entry_commands_too(self, run, db, stub, scratch_tmp, monkeypatch, sub, change):
        make_db(run, db, RICH_YAML)
        self.interrupting(monkeypatch)
        stub.plan([change])
        result = run("edit", *sub, "--no-prompt", "-c", db, env=stub.env(db))
        assert result.exit_code == 130
        assert "may or may not have been applied" in result.stderr

    def test_a_conflict_still_says_nothing_was_written(self, run, plain, stub, scratch_tmp):
        stub.plan([replace(*FREQ_1H), {"cli": ["-c", str(plain), "set", "api_port", "9999"]}])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 75
        assert "nothing was written to the database" in result.stderr
        assert "may or may not" not in result.stderr

    @pytest.mark.parametrize("exc", [ConfigBusy("the writer lock is held"), ValueError("rejected")])
    def test_busy_and_validation_errors_keep_the_certain_wording(
        self, run, plain, stub, scratch_tmp, monkeypatch, exc
    ):
        def commit(*args, **kwargs):
            raise exc

        monkeypatch.setattr(editor, "commit_config", commit)
        stub.plan([replace(*FREQ_1H)])
        state = db_state(plain)
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code in (1, 75)
        assert "nothing was written to the database" in result.stderr
        assert "may or may not" not in result.stderr
        assert db_state(plain) == state

    def test_an_interrupt_in_the_editor_itself_is_not_ambiguous(self, run, plain, stub, scratch_tmp, monkeypatch):
        def raising(argv, buffer):
            raise KeyboardInterrupt

        monkeypatch.setattr(editor, "_run_editor", raising)
        stub.plan([])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 130
        assert "nothing was written to the database" in result.stderr
        assert "may or may not" not in result.stderr


class TestEditRecreateWarning:
    """A deleted plus a created dataset that send to a common destination is
    probably a rename the exact-match detector missed."""

    def rename_and_change(self, new="tank/data2"):
        return [
            replace('- name: "tank/data"', f'- name: "{new}"'),
            replace('    frequency: "1h"\n    recursive: true', '    frequency: "2h"\n    recursive: true'),
        ]

    def test_warns_when_a_changed_rename_shares_a_destination(self, run, rich, stub, scratch_tmp):
        stub.plan(self.rename_and_change())
        result = edit(run, rich, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        warned = [w for w in warnings_in(result) if "is deleted and 'tank/data2' created" in w]
        assert len(warned) == 1
        assert "both send to dc.two, offsite" in warned[0]
        assert "zfsbackup-config rename dataset tank/data tank/data2" in warned[0]
        assert "stale" in warned[0] and "full send" in warned[0]

    def test_no_warning_without_a_shared_destination(self, run, rich, stub, scratch_tmp):
        stub.plan([replace('- name: "tank/vm.01"', '- name: "tank/vm.02"'), replace('frequency: "15m"', 'frequency: "20m"')])
        result = edit(run, rich, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert [w for w in warnings_in(result) if "is deleted and" in w] == []

    def test_no_warning_when_nothing_is_deleted(self, run, rich, stub, scratch_tmp):
        stub.plan([{"append": '  - name: "tank/new"\n    remote:\n      - destination: "offsite"\n'}])
        result = edit(run, rich, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert [w for w in warnings_in(result) if "is deleted and" in w] == []

    def test_the_exact_rename_is_refused_not_warned(self, run, rich, stub, scratch_tmp):
        stub.plan([replace('- name: "tank/data"', '- name: "tank/data2"')])
        result = edit(run, rich, stub, "--no-prompt")
        assert result.exit_code == 1 and "looks like a rename" in result.stderr

    def test_allow_recreate_applies_the_exact_rename_without_this_warning(self, run, rich, stub, scratch_tmp):
        stub.plan([replace('- name: "tank/data"', '- name: "tank/data2"')])
        result = edit(run, rich, stub, "--no-prompt", "--allow-recreate")
        assert result.exit_code == 0, result.stderr


class TestEditDestinationNames:
    def test_creating_an_unusable_destination_name_only_warns(self, run, plain, stub, scratch_tmp):
        stub.plan([{"replace": ["datasets:", 'destinations:\n  "bad name":\n    url: "http://b"\n\ndatasets:']}])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        warned = [w for w in warnings_in(result) if "destination 'bad name'" in w]
        assert len(warned) == 1
        assert "it contains ' '" in warned[0] and "its anchor property cannot be set" in warned[0]
        assert "bad name" in stored(plain).destinations

    def test_an_existing_unusable_name_is_not_warned_about_again(self, run, db, stub, scratch_tmp):
        make_db(run, db, 'destinations:\n  "bad name":\n    url: "http://b"\ndatasets:\n  - name: "tank/a"\n')
        stub.plan([replace('frequency: "1h"', 'frequency: "2h"')])
        result = edit(run, db, stub, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert [w for w in warnings_in(result) if "bad name" in w] == []

    def test_usable_names_are_not_warned_about(self, run, plain, stub, scratch_tmp):
        stub.plan([{"replace": ["datasets:", 'destinations:\n  "ok-1_x.y:z":\n    url: "http://b"\n\ndatasets:']}])
        result = edit(run, plain, stub, "--no-prompt")
        assert result.exit_code == 0 and warnings_in(result) == []

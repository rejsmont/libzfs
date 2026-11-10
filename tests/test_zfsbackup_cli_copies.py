"""Item A2d: `zfsbackup-config` keeps backup copies out from below recursive datasets
(`zfsbackup/cli/copies.py`; plan: docs/sender_rework_plan.md, A2d; user decision: refuse, `--force`
overrides, a failed ZFS read only warns).

The daemon snapshots a `recursive: true` dataset with one `zfs snapshot -r`, which would also
snapshot into every backup copy below it. Before a write is committed the CLI therefore reads ZFS
and refuses (exit 1, nothing written) a change that leaves a copy below a recursive dataset:

  (a) a dataset that BECOMES (or is added as) enabled + recursive with a copy below it, or itself
      inside a copy (markers local, received or inherited);
  (b) a receive target that is GIVEN UP (a `zfs:` destination removed, retargeted or switched to
      https; the server's `target_dataset` removed or changed) while it exists below a recursive
      dataset of the new configuration.

Sections:

- ``TestPureHelpers``            `newly_recursive`, `given_up_targets`.
- ``TestNewlyRecursive``         (a) through `set`/`unset`/`import`: markers of every source, one
                                 message per dataset, at most 5 copies shown, nothing looked at for
                                 a dataset that was recursive already or does not exist.
- ``TestGivenUpTargets``         (b) through `import` and `edit`: removed, changed, kind-switched,
                                 removed `remote_backup`, a missing target, dedupe against (a).
- ``TestForce``                  `--force` on every writing command, `edit --force dataset`
                                 inheritance, the help text.
- ``TestCouldNotCheck``          failed reads (no `zfs`, an error, a timeout, the budget) warn and
                                 commit; a refusal leaves the database byte for byte alone.
- ``TestEditor``                 a refusal in the editor is a rejected buffer (Retry / Abort).
- ``TestImportCheck``            FIFO, first import, unreadable outgoing configuration, bad YAML.

Nothing here reaches a real `zfs`: `fake_zfs` (tests/_fake_zfs.py) is a JSON world, and the tests
that need no ZFS at all run against the conftest stub.
"""

import contextlib
import os
import re
import sqlite3
import subprocess
import sys
import threading

import pytest

from _cli_support import (
    NEVER_PRUNE_TEXT,
    PLAIN_YAML,
    buffers_in,
    count_containing,
    db,
    db_state,
    fake_zfs,
    generation,
    isolated,
    make_db,
    run,
    scratch_tmp,
    stderr_lines,
    stored,
    stub,
    warnings_in,
    write_yaml,
)
from zfsbackup.cli import copies, editor
from zfsbackup.config import BackupConfig

CLIENT_ID = "org.zfsbackup:client_id"
SOURCE_DATASET = "org.zfsbackup:source_dataset"

REFUSAL = "error: refusing to write: this change would leave backup copies below a recursive dataset:"
NOT_WRITTEN = "Nothing was written."
COULD_NOT_CHECK = (
    "warning: could not check whether this change leaves a backup copy below a recursive dataset ("
)


# --------------------------------------------------------------------------
# configurations
# --------------------------------------------------------------------------

def cfg(datasets, destinations=None, remote_target=None, remote_enabled=True) -> str:
    """A YAML configuration. `datasets` maps name -> flags (`r` recursive, `d` disabled); `destinations`
    maps name -> URL; `remote_target` is `remote_backup.target_dataset`."""
    text = 'snapshot_prefix: "autosnap"\ncheck_interval: "5m"\n'
    if remote_target:
        text += (
            f"remote_backup:\n  enabled: {str(remote_enabled).lower()}\n"
            f'  target_dataset: "{remote_target}"\n'
        )
    text += "datasets:\n"
    for name, flags in datasets.items():
        text += f'  - name: "{name}"\n    frequency: "1h"\n    retention:\n      "1d": "30d"\n'
        if "r" in flags:
            text += "    recursive: true\n"
        if "d" in flags:
            text += "    enabled: false\n"
    if destinations:
        text += "destinations:\n"
        for name, url in destinations.items():
            text += f'  {name}:\n    url: "{url}"\n'
    return text


#: `tank/data` (index 0) and `tank/other` (1), neither recursive.
FLAT = cfg({"tank/data": "", "tank/other": ""})
#: The same with `tank/data` recursive.
RECURSIVE = cfg({"tank/data": "r", "tank/other": ""})


def seed(fake, *names):
    """Datasets that exist, with no property at all."""
    for name in names:
        fake.add(name, {})
    return fake


def copy_below(fake, name, *, source="local", prop=CLIENT_ID):
    """A backup copy: a dataset carrying a copy marker with the given source."""
    value = "host-9" if prop == CLIENT_ID else "tank/src"
    return fake.set_prop(name, prop, value, source)


def findings(result):
    """The bullet lines of a refusal (or `--force:` warnings), without the lead."""
    return [l.strip()[2:] for l in result.stderr.splitlines() if l.startswith("  - ")]


def forced(result):
    return [l for l in stderr_lines(result) if l.startswith("warning: --force: ")]


def zfs_calls(fake, command):
    return fake.calls_of(command)


@pytest.fixture
def flat(run, db, fake_zfs):
    made = make_db(run, db, FLAT)
    fake_zfs.clear_calls()
    return made


@pytest.fixture
def recursive(run, db, fake_zfs):
    made = make_db(run, db, RECURSIVE)
    fake_zfs.clear_calls()
    return made


def make_recursive(run, database, *extra):
    return run("set", "datasets[tank/data].recursive", "true", *extra, "-c", database)


# --------------------------------------------------------------------------
# pure helpers
# --------------------------------------------------------------------------

class TestPureHelpers:
    def load(self, tmp_path, text, name="c.yaml"):
        return BackupConfig.from_file(write_yaml(tmp_path, text, name))

    def test_newly_recursive_lists_enabled_recursive_datasets_that_were_not(self, tmp_path):
        old = self.load(tmp_path, cfg({"a": "", "b": "r", "c": "rd", "d": ""}), "o.yaml")
        new = self.load(tmp_path, cfg({"a": "r", "b": "r", "c": "r", "d": "rd", "e": "r"}), "n.yaml")
        # a: became recursive; b: recursive before; c: was disabled; d: disabled now; e: added
        assert [d.name for d in copies.newly_recursive(old, new)] == ["a", "c", "e"]

    def test_with_nothing_stored_every_recursive_enabled_dataset_counts(self, tmp_path):
        new = self.load(tmp_path, cfg({"a": "r", "b": "", "c": "rd"}))
        assert [d.name for d in copies.newly_recursive(None, new)] == ["a"]

    def test_nothing_newly_recursive_when_nothing_changed(self, tmp_path):
        config = self.load(tmp_path, cfg({"a": "r", "b": ""}))
        assert copies.newly_recursive(config, config) == []

    def test_given_up_targets_are_the_old_targets_the_new_config_no_longer_has(self, tmp_path):
        old = self.load(
            tmp_path,
            cfg({"a": ""}, {"l1": "zfs:t/one", "l2": "zfs:t/two", "l3": "zfs:t/three", "s": "https://s:1"},
                remote_target="srv/in"),
            "o.yaml",
        )
        new = self.load(
            tmp_path,
            cfg({"a": ""}, {"l1": "zfs:t/one", "l2": "zfs:t/moved", "l3": "https://s:1", "s": "https://s:1"}),
            "n.yaml",
        )
        assert copies.given_up_targets(old, new) == [
            ("remote_backup.target_dataset", "srv/in"),
            ("destinations[l2]", "t/two"),
            ("destinations[l3]", "t/three"),
        ]

    def test_a_target_another_destination_still_receives_at_is_not_given_up(self, tmp_path):
        old = self.load(tmp_path, cfg({"a": ""}, {"l1": "zfs:t/one"}, remote_target="t/one"), "o.yaml")
        new = self.load(tmp_path, cfg({"a": ""}, {"l9": "zfs:t/one"}), "n.yaml")
        assert copies.given_up_targets(old, new) == []

    def test_a_trailing_slash_does_not_make_a_different_target(self, tmp_path):
        old = self.load(tmp_path, cfg({"a": ""}, remote_target="srv/in"), "o.yaml")
        new = self.load(tmp_path, cfg({"a": ""}, remote_target="srv/in/"), "n.yaml")
        assert copies.given_up_targets(old, new) == []

    def test_nothing_stored_gives_up_nothing(self, tmp_path):
        assert copies.given_up_targets(None, self.load(tmp_path, FLAT)) == []

    def test_a_disabled_remote_backup_target_still_counts(self, tmp_path):
        old = self.load(tmp_path, cfg({"a": ""}, remote_target="srv/in", remote_enabled=False), "o.yaml")
        new = self.load(tmp_path, cfg({"a": ""}), "n.yaml")
        assert copies.given_up_targets(old, new) == [("remote_backup.target_dataset", "srv/in")]


class TestFindCopyProblems:
    """`find_copy_problems`/`check_copies` called directly: the bare-module contract."""

    def load(self, tmp_path, text, name="c.yaml"):
        return BackupConfig.from_file(write_yaml(tmp_path, text, name))

    def test_nothing_to_look_at_imports_and_runs_nothing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(copies, "_load_zfs", lambda: pytest.fail("the zfs module was loaded"))
        config = self.load(tmp_path, RECURSIVE)
        assert copies.find_copy_problems(config, config) == ([], [])
        assert copies.find_copy_problems(self.load(tmp_path, RECURSIVE, "r.yaml"), self.load(tmp_path, FLAT, "f.yaml")) == ([], [])

    def test_an_invalid_new_configuration_is_left_to_the_write_and_costs_no_zfs_read(
        self, tmp_path, fake_zfs
    ):
        # a recursive dataset ABOVE a declared target: `validate_config` refuses it, so no look
        old = self.load(tmp_path, FLAT, "old.yaml")
        new = self.load(tmp_path, RECURSIVE, "new.yaml")
        new.destinations = {}
        new.remote_backup = None
        from zfsbackup.config import Destination
        new.destinations["loc"] = Destination(url="zfs:tank/data/bk")
        fake_zfs.add("tank/data", {})
        assert copies.find_copy_problems(old, new) == ([], [])
        assert fake_zfs.calls == []

    def test_a_copy_under_a_declared_target_without_markers_is_found_when_validation_lets_it_by(
        self, tmp_path, fake_zfs, monkeypatch
    ):
        """`validate_config` refuses a recursive dataset above a declared target, so the CLI never gets
        here; the target branch of `DatasetManager.copy_tops` is exercised with validation out of the way."""
        monkeypatch.setattr(copies, "validate_config", lambda config: [])
        from zfsbackup.config import Destination

        old = self.load(tmp_path, FLAT, "old.yaml")
        new = self.load(tmp_path, RECURSIVE, "new.yaml")
        new.destinations = {"loc": Destination(url="zfs:tank/data/bk")}
        seed(fake_zfs, "tank/data", "tank/data/bk")
        problems, could_not = copies.find_copy_problems(old, new)
        assert could_not == []
        (problem,) = problems
        assert problem.startswith("datasets[tank/data] is recursive and the backup copy tank/data/bk is below it (")
        assert "it is the receive target tank/data/bk" in problem

    def test_a_declared_target_that_does_not_exist_holds_no_copy(self, tmp_path, fake_zfs, monkeypatch):
        monkeypatch.setattr(copies, "validate_config", lambda config: [])
        from zfsbackup.config import Destination

        old = self.load(tmp_path, FLAT, "old.yaml")
        new = self.load(tmp_path, RECURSIVE, "new.yaml")
        new.destinations = {"loc": Destination(url="zfs:tank/data/bk")}
        seed(fake_zfs, "tank/data")
        assert copies.find_copy_problems(old, new) == ([], [])

    def test_check_copies_returns_one_refusal_text_or_warns_with_force(self, tmp_path, fake_zfs):
        old = self.load(tmp_path, FLAT, "old.yaml")
        new = self.load(tmp_path, RECURSIVE, "new.yaml")
        seed(fake_zfs, "tank/data")
        copy_below(fake_zfs, "tank/data/c1")
        copy_below(fake_zfs, "tank/data/c2")
        warned = []
        text = copies.check_copies(old, new, force=False, warn=warned.append)
        assert warned == []
        assert text.startswith(REFUSAL[len("error: "):])
        assert text.count("\n  - ") == 2 and NOT_WRITTEN in text
        assert copies.check_copies(old, new, force=True, warn=warned.append) is None
        assert len(warned) == 2 and all(w.startswith("--force: datasets[tank/data] is recursive") for w in warned)

    def test_enforce_copies_raises_clierror_for_a_refusal_only(self, tmp_path, fake_zfs):
        from zfsbackup.cli.errors import CliError

        old = self.load(tmp_path, FLAT, "old.yaml")
        new = self.load(tmp_path, RECURSIVE, "new.yaml")
        seed(fake_zfs, "tank/data")
        copies.enforce_copies(old, new, force=False, warn=lambda m: None)        # nothing below: fine
        copy_below(fake_zfs, "tank/data/c1")
        with pytest.raises(CliError, match="refusing to write"):
            copies.enforce_copies(old, new, force=False, warn=lambda m: None)
        copies.enforce_copies(old, new, force=True, warn=lambda m: None)


# --------------------------------------------------------------------------
# (a) a dataset that becomes recursive
# --------------------------------------------------------------------------

class TestNewlyRecursive:
    @pytest.mark.parametrize("source", ["local", "received"])
    @pytest.mark.parametrize("prop", [CLIENT_ID, SOURCE_DATASET])
    def test_a_copy_marker_below_the_dataset_refuses_and_names_the_rename(
        self, run, flat, db, fake_zfs, source, prop
    ):
        seed(fake_zfs, "tank/data")
        copy_below(fake_zfs, "tank/data/bk", source=source, prop=prop)
        state, gen = db_state(db), generation(db)
        result = make_recursive(run, db)
        assert result.exit_code == 1
        assert REFUSAL in result.stderr and NOT_WRITTEN in result.stderr
        (finding,) = findings(result)
        assert finding.startswith("datasets[tank/data] is recursive and the backup copy tank/data/bk is below it (")
        assert f"source: {source}" in finding
        assert "Move the copy out (`zfs rename tank/data/bk <a name outside tank/data>`)" in finding
        assert "--force" in result.stderr
        assert db_state(db) == state and generation(db) == gen
        assert stored(db).datasets[0].recursive is False

    def test_an_inherited_marker_names_the_top_of_the_copy_only(self, run, flat, db, fake_zfs):
        seed(fake_zfs, "tank/data")
        copy_below(fake_zfs, "tank/data/bk")
        for child in ("tank/data/bk/a", "tank/data/bk/a/deep"):
            copy_below(fake_zfs, child, source="inherited from tank/data/bk")
        result = make_recursive(run, db)
        assert result.exit_code == 1
        (finding,) = findings(result)
        assert "the backup copy tank/data/bk is below it" in finding
        assert "tank/data/bk/a" not in finding

    def test_every_top_is_named_once_and_nested_copies_are_not(self, run, flat, db, fake_zfs):
        seed(fake_zfs, "tank/data")
        copy_below(fake_zfs, "tank/data/one")
        copy_below(fake_zfs, "tank/data/one/nested")
        copy_below(fake_zfs, "tank/data/two", prop=SOURCE_DATASET, source="received")
        result = make_recursive(run, db)
        tops = [re.search(r"the backup copy (\S+) is below", f).group(1) for f in findings(result)]
        assert tops == ["tank/data/one", "tank/data/two"]

    def test_at_most_five_copies_are_shown_per_dataset(self, run, flat, db, fake_zfs):
        seed(fake_zfs, "tank/data")
        for i in range(8):
            copy_below(fake_zfs, f"tank/data/bk{i}")
        result = make_recursive(run, db)
        assert result.exit_code == 1
        lines = findings(result)
        assert len([l for l in lines if "is below it (" in l]) == 5
        assert [l for l in lines if "more backup copies" in l] == [
            "datasets[tank/data] has 3 more backup copies below it"
        ]

    def test_exactly_five_copies_have_no_more_line(self, run, flat, db, fake_zfs):
        seed(fake_zfs, "tank/data")
        for i in range(5):
            copy_below(fake_zfs, f"tank/data/bk{i}")
        lines = findings(make_recursive(run, db))
        assert len(lines) == 5 and not any("more backup copies" in l for l in lines)

    def test_a_dataset_carrying_the_markers_itself_is_a_copy(self, run, flat, db, fake_zfs):
        copy_below(fake_zfs, "tank/data")
        result = make_recursive(run, db)
        assert result.exit_code == 1
        (finding,) = findings(result)
        assert finding.startswith("datasets[tank/data] is recursive and carries the backup-copy markers itself (source: local)")
        assert "zfs inherit org.zfsbackup:client_id tank/data" in finding

    def test_a_dataset_inside_a_copy_gets_one_message_not_one_per_child(self, run, flat, db, fake_zfs):
        copy_below(fake_zfs, "tank", source="local")
        copy_below(fake_zfs, "tank/data", source="inherited from tank")
        for child in ("tank/data/a", "tank/data/b", "tank/data/c"):
            copy_below(fake_zfs, child, source="inherited from tank")
        result = make_recursive(run, db)
        assert result.exit_code == 1
        (finding,) = findings(result)
        assert "datasets[tank/data] is recursive and is itself inside the backup copy tank (source: inherited from tank)" in finding
        assert "zfs rename tank/data <a name outside tank>" in finding
        assert "tank/data/a" not in finding

    def test_a_copy_that_is_not_covered_is_listed_next_to_the_inside_message(self, run, flat, db, fake_zfs):
        copy_below(fake_zfs, "tank", source="local")
        copy_below(fake_zfs, "tank/data", source="inherited from tank")
        copy_below(fake_zfs, "tank/data/own", source="local")           # its own marker: a real top
        result = make_recursive(run, db)
        assert result.exit_code == 1
        lines = findings(result)
        assert any("is itself inside the backup copy tank" in l for l in lines)
        assert any("the backup copy tank/data/own is below it" in l for l in lines)

    def test_a_dataset_with_no_copy_below_it_is_written(self, run, flat, db, fake_zfs):
        seed(fake_zfs, "tank/data", "tank/data/child", "tank/data/child/deep")
        fake_zfs.set_prop("tank/data/child", ANCHOR_PROP, "S1")        # a source's own anchor is no marker
        gen = generation(db)
        result = make_recursive(run, db)
        assert result.exit_code == 0, result.stderr
        assert warnings_in(result) == []
        assert stored(db).datasets[0].recursive is True and generation(db) == gen + 1

    def test_the_check_reads_only_what_it_needs(self, run, flat, db, fake_zfs):
        seed(fake_zfs, "tank/data")
        assert make_recursive(run, db).exit_code == 0
        assert fake_zfs.mutations == []
        # one existence check, then the one recursive value-free read, nothing for the other dataset
        assert [c[0] for c in fake_zfs.calls] == ["list", "get"]
        assert all(c[-1] == "tank/data" for c in fake_zfs.calls)
        assert "-r" in fake_zfs.calls[1]

    def test_a_dataset_that_does_not_exist_yet_is_silent(self, run, flat, db, fake_zfs):
        seed(fake_zfs, "tank")                    # the pool is there, `tank/data` is not (yet)
        result = make_recursive(run, db)
        assert result.exit_code == 0, result.stderr
        assert warnings_in(result) == []
        assert [c[0] for c in fake_zfs.calls] == ["list", "list"]       # the dataset, then its pool
        assert stored(db).datasets[0].recursive is True

    def test_a_dataset_that_was_recursive_already_makes_no_zfs_call_at_all(self, run, recursive, db, fake_zfs):
        copy_below(fake_zfs, "tank/data/bk")
        for args in (
            ("set", "datasets[tank/data].frequency", "2h"),
            ("set", "datasets[tank/data].recursive", "true"),
            ("set", "check_interval", "7m"),
            ("set", "datasets[tank/other].recursive", "false"),
        ):
            result = run(*args, "-c", recursive)
            assert result.exit_code == 0, (args, result.stderr)
        assert fake_zfs.calls == []

    def test_a_dataset_that_stays_non_recursive_or_becomes_disabled_is_not_looked_at(
        self, run, flat, db, fake_zfs
    ):
        copy_below(fake_zfs, "tank/data/bk")
        assert run("set", "datasets[tank/data].frequency", "2h", "-c", flat).exit_code == 0
        run("set", "datasets[tank/data].enabled", "false", "-c", flat)
        assert run("set", "datasets[tank/data].recursive", "true", "-c", flat).exit_code == 0   # disabled: not checked
        assert fake_zfs.calls == []

    def test_enabling_a_disabled_recursive_dataset_is_a_newly_recursive_one(self, run, db, fake_zfs):
        make_db(run, db, cfg({"tank/data": "rd", "tank/other": ""}))
        seed(fake_zfs, "tank/data")
        fake_zfs.clear_calls()
        copy_below(fake_zfs, "tank/data/bk")
        state = db_state(db)
        result = run("set", "datasets[tank/data].enabled", "true", "-c", db)
        assert result.exit_code == 1
        assert REFUSAL in result.stderr and "tank/data/bk" in result.stderr
        assert db_state(db) == state

    def test_enabling_it_with_nothing_below_is_written(self, run, db, fake_zfs):
        make_db(run, db, cfg({"tank/data": "rd"}))
        seed(fake_zfs, "tank/data")
        assert run("set", "datasets[tank/data].enabled", "true", "-c", db).exit_code == 0
        assert stored(db).datasets[0].enabled is True

    def test_unset_runs_the_check_with_the_old_and_the_new_configuration(self, run, db, fake_zfs, monkeypatch):
        """`unset` cannot make a dataset recursive (`recursive` always has a value), so no refusal is
        reachable through it; it still goes through the same check (old -> new, `--force` passed on)."""
        from zfsbackup.cli import main as cli_main

        make_db(run, db, FLAT)
        seen = []
        real = copies.enforce_copies

        def spy(old, new, **kwargs):
            seen.append((len(old.datasets[0].retention_rules), len(new.datasets[0].retention_rules), kwargs["force"]))
            return real(old, new, **kwargs)

        monkeypatch.setattr(cli_main.copies, "enforce_copies", spy)
        path = "datasets[tank/data].retention.1d"
        for args in (("unset", path), ("set", path, "30d"), ("unset", path, "--force")):
            result = run(*args, "-c", db)
            assert result.exit_code == 0, result.stderr
        assert seen == [(1, 0, False), (0, 1, False), (1, 0, True)]

    def test_a_no_op_set_never_looks_at_zfs(self, run, recursive, fake_zfs):
        result = run("set", "datasets[tank/data].recursive", "true", "-c", recursive)
        assert result.exit_code == 0 and "nothing changed" in result.stdout
        assert fake_zfs.calls == []

    def test_a_refusal_comes_after_the_no_change_check_and_before_any_write(
        self, run, flat, db, fake_zfs, monkeypatch
    ):
        from zfsbackup.cli import main as cli_main

        seed(fake_zfs, "tank/data")
        copy_below(fake_zfs, "tank/data/bk")
        monkeypatch.setattr(cli_main, "commit_config", lambda *a, **k: pytest.fail("committed"))
        assert make_recursive(run, db).exit_code == 1

    def test_only_the_dataset_that_became_recursive_is_looked_at(self, run, flat, db, fake_zfs):
        seed(fake_zfs, "tank/data", "tank/other")
        copy_below(fake_zfs, "tank/other/bk")        # below a dataset that stays non-recursive: fine
        assert make_recursive(run, db).exit_code == 0
        assert all(c[-1] == "tank/data" for c in fake_zfs.calls)

    def test_a_newly_added_recursive_dataset_is_checked_through_import(self, run, flat, db, fake_zfs, tmp_path):
        seed(fake_zfs, "tank/new")
        copy_below(fake_zfs, "tank/new/bk", source="received")
        source = write_yaml(tmp_path, cfg({"tank/data": "", "tank/other": "", "tank/new": "r"}), "new.yaml")
        state = db_state(db)
        result = run("import", source, "-c", db)
        assert result.exit_code == 1
        assert "datasets[tank/new] is recursive and the backup copy tank/new/bk is below it" in result.stderr
        assert db_state(db) == state

    def test_a_newly_added_recursive_dataset_without_copies_is_imported(self, run, flat, db, fake_zfs, tmp_path):
        seed(fake_zfs, "tank/new")
        source = write_yaml(tmp_path, cfg({"tank/data": "", "tank/new": "r"}), "new.yaml")
        assert run("import", source, "-c", db).exit_code == 0

    def test_several_datasets_each_get_their_findings(self, run, flat, db, fake_zfs, tmp_path):
        seed(fake_zfs, "tank/data", "tank/other")
        copy_below(fake_zfs, "tank/data/a")
        copy_below(fake_zfs, "tank/other/b")
        source = write_yaml(tmp_path, cfg({"tank/data": "r", "tank/other": "r"}), "both.yaml")
        result = run("import", source, "-c", db)
        assert result.exit_code == 1
        assert [re.match(r"datasets\[(\S+)\]", f).group(1) for f in findings(result)] == ["tank/data", "tank/other"]

    def test_the_stale_markers_remedy_is_in_the_message(self, run, flat, db, fake_zfs):
        copy_below(fake_zfs, "tank/data", source="received")
        (finding,) = findings(make_recursive(run, db))
        assert "(source: received)" in finding

    def test_a_refusal_prints_nothing_on_stdout_and_a_commit_prints_the_usual_line(self, run, flat, db, fake_zfs):
        seed(fake_zfs, "tank/data")
        copy_below(fake_zfs, "tank/data/bk")
        assert make_recursive(run, db).stdout == ""
        fake_zfs.state_path.write_text("{}")
        result = make_recursive(run, db)
        assert result.exit_code == 0 and "recursive" in result.stdout


ANCHOR_PROP = "org.zfsbackup:anchor.offsite"


# --------------------------------------------------------------------------
# (b) a receive target that is given up
# --------------------------------------------------------------------------

BASE_LOCAL = cfg({"tank": "", "tank/data": ""}, {"loc": "zfs:tank/bk"})
GIVEN_UP = "is no longer a receive target, and it lies below the recursive datasets[tank]"


class TestGivenUpTargets:
    """(b) needs a dataset that becomes recursive AND a target that goes in the SAME write (a recursive
    ancestor of a declared target is refused by `validate_config`), so it is reached through `import`
    and `edit`. (`validate_config` runs first: a state that keeps the target is never looked at.)"""

    def importing(self, run, db, tmp_path, new_text, *extra, name="new.yaml"):
        return run("import", write_yaml(tmp_path, new_text, name), *extra, "-c", db)

    @pytest.fixture
    def local(self, run, db, fake_zfs):
        made = make_db(run, db, BASE_LOCAL)
        fake_zfs.clear_calls()
        return made

    def test_a_removed_destination_whose_target_exists_is_refused(self, run, local, db, fake_zfs, tmp_path):
        seed(fake_zfs, "tank", "tank/bk")
        state = db_state(db)
        result = self.importing(run, db, tmp_path, cfg({"tank": "r", "tank/data": ""}))
        assert result.exit_code == 1
        (finding,) = findings(result)
        assert finding.startswith(f"destinations[loc] (tank/bk) {GIVEN_UP}")
        assert "`zfs rename tank/bk <a name outside tank>`" in finding
        assert NOT_WRITTEN in result.stderr
        assert db_state(db) == state

    def test_a_changed_target_gives_up_the_old_one(self, run, local, db, fake_zfs, tmp_path):
        seed(fake_zfs, "tank", "tank/bk")
        result = self.importing(
            run, db, tmp_path, cfg({"tank": "r", "tank/data": ""}, {"loc": "zfs:backup/elsewhere"})
        )
        assert result.exit_code == 1
        (finding,) = findings(result)
        assert finding.startswith(f"destinations[loc] (tank/bk) {GIVEN_UP}")

    def test_a_destination_switched_to_https_gives_up_its_target(self, run, local, db, fake_zfs, tmp_path):
        seed(fake_zfs, "tank", "tank/bk")
        result = self.importing(
            run, db, tmp_path, cfg({"tank": "r", "tank/data": ""}, {"loc": "https://backup.example:8080"})
        )
        assert result.exit_code == 1
        (finding,) = findings(result)
        assert finding.startswith(f"destinations[loc] (tank/bk) {GIVEN_UP}")

    def test_a_removed_remote_backup_gives_up_the_server_target(self, run, db, fake_zfs, tmp_path):
        make_db(run, db, cfg({"tank": "", "tank/data": ""}, remote_target="tank/srv"))
        seed(fake_zfs, "tank", "tank/srv")
        fake_zfs.clear_calls()
        result = self.importing(run, db, tmp_path, cfg({"tank": "r", "tank/data": ""}))
        assert result.exit_code == 1
        (finding,) = findings(result)
        assert finding.startswith(f"remote_backup.target_dataset (tank/srv) {GIVEN_UP}")

    def test_a_changed_server_target_gives_up_the_old_one(self, run, db, fake_zfs, tmp_path):
        make_db(run, db, cfg({"tank": "", "tank/data": ""}, remote_target="tank/srv"))
        seed(fake_zfs, "tank", "tank/srv")
        result = self.importing(
            run, db, tmp_path, cfg({"tank": "r", "tank/data": ""}, remote_target="backup/srv")
        )
        assert result.exit_code == 1
        assert f"remote_backup.target_dataset (tank/srv) {GIVEN_UP}" in result.stderr

    def test_a_disabled_remote_backup_target_is_given_up_too(self, run, db, fake_zfs, tmp_path):
        make_db(run, db, cfg({"tank": "", "tank/data": ""}, remote_target="tank/srv", remote_enabled=False))
        seed(fake_zfs, "tank", "tank/srv")
        result = self.importing(run, db, tmp_path, cfg({"tank": "r", "tank/data": ""}))
        assert result.exit_code == 1

    def test_a_target_that_is_not_in_zfs_gives_no_finding(self, run, local, db, fake_zfs, tmp_path):
        seed(fake_zfs, "tank")
        result = self.importing(run, db, tmp_path, cfg({"tank": "r", "tank/data": ""}))
        assert result.exit_code == 0, result.stderr
        assert count_containing(warnings_in(result), "could not check") == 0 and forced(result) == []
        assert any(c[0] == "list" and c[-1] == "tank/bk" for c in fake_zfs.calls)
        assert stored(db).datasets[0].recursive is True

    def test_a_target_not_below_any_recursive_dataset_is_never_looked_at(self, run, local, db, fake_zfs, tmp_path):
        seed(fake_zfs, "tank", "tank/bk")
        result = self.importing(run, db, tmp_path, cfg({"tank": "", "tank/data": ""}, {"loc": "https://x.example:8080"}))
        assert result.exit_code == 0, result.stderr
        assert fake_zfs.calls == []

    def test_a_target_below_a_dataset_that_is_not_recursive_or_not_enabled_is_not_a_finding(
        self, run, local, db, fake_zfs, tmp_path
    ):
        seed(fake_zfs, "tank", "tank/bk")
        assert self.importing(run, db, tmp_path, cfg({"tank": "rd", "tank/data": ""})).exit_code == 0
        assert fake_zfs.calls == []

    def test_a_target_that_is_kept_is_not_given_up(self, run, local, db, fake_zfs, tmp_path):
        # the destination only changes its name's URL spelling: same target, so (b) has nothing to say;
        # the recursive `tank` above it is then `validate_config`'s refusal (exit 1, no ZFS look)
        seed(fake_zfs, "tank", "tank/bk")
        result = self.importing(
            run, db, tmp_path, cfg({"tank": "r", "tank/data": ""}, {"loc": "zfs:tank/bk"})
        )
        assert result.exit_code == 1
        assert REFUSAL not in result.stderr
        assert fake_zfs.calls == []

    def test_a_target_with_markers_on_it_is_named_once(self, run, local, db, fake_zfs, tmp_path):
        seed(fake_zfs, "tank")
        copy_below(fake_zfs, "tank/bk")                       # (a) finds the target as a top
        result = self.importing(run, db, tmp_path, cfg({"tank": "r", "tank/data": ""}))
        assert result.exit_code == 1
        (finding,) = findings(result)
        assert "the backup copy tank/bk is below it" in finding
        assert "no longer a receive target" not in result.stderr

    def test_markers_below_the_target_and_the_target_itself_are_both_named(self, run, local, db, fake_zfs, tmp_path):
        seed(fake_zfs, "tank", "tank/bk")
        copy_below(fake_zfs, "tank/bk/host-1")                 # a top below the target: not the target itself
        result = self.importing(run, db, tmp_path, cfg({"tank": "r", "tank/data": ""}))
        lines = findings(result)
        assert [bool(re.search(r"the backup copy tank/bk/host-1 is below", l)) for l in lines] == [True, False]
        assert lines[1].startswith(f"destinations[loc] (tank/bk) {GIVEN_UP}")

    def test_two_given_up_targets_are_both_named(self, run, db, fake_zfs, tmp_path):
        make_db(run, db, cfg({"tank": "", "tank/data": ""}, {"l1": "zfs:tank/a", "l2": "zfs:tank/b"}))
        seed(fake_zfs, "tank", "tank/a", "tank/b")
        result = self.importing(run, db, tmp_path, cfg({"tank": "r", "tank/data": ""}))
        assert sorted(re.match(r"destinations\[(\w+)\]", f).group(1) for f in findings(result)) == ["l1", "l2"]

    def test_the_same_target_is_read_once_for_two_recursive_roots(self, run, db, fake_zfs, tmp_path):
        make_db(run, db, cfg({"tank": "", "tank/data": ""}, {"loc": "zfs:tank/data/bk"}))
        seed(fake_zfs, "tank", "tank/data", "tank/data/bk")
        fake_zfs.clear_calls()
        result = self.importing(run, db, tmp_path, cfg({"tank": "r", "tank/data": "r"}))
        assert result.exit_code == 1
        assert len([c for c in fake_zfs.calls if c[0] == "list" and c[-1] == "tank/data/bk"]) == 1
        assert len(findings(result)) == 2

    def test_force_writes_it_with_the_finding_as_a_warning(self, run, local, db, fake_zfs, tmp_path):
        seed(fake_zfs, "tank", "tank/bk")
        result = self.importing(run, db, tmp_path, cfg({"tank": "r", "tank/data": ""}), "--force")
        assert result.exit_code == 0, result.stderr
        (warning,) = forced(result)
        assert GIVEN_UP in warning
        assert stored(db).datasets[0].recursive is True and stored(db).destinations == {}

    def test_whole_config_edit_is_checked_the_same_way(self, run, local, db, fake_zfs, stub, scratch_tmp):
        seed(fake_zfs, "tank", "tank/bk")
        state = db_state(db)
        stub.plan(whole_edit_to_recursive_without_the_destination())
        result = run("edit", "--no-prompt", "-c", db, env=stub.env(db))
        assert result.exit_code == 1
        assert f"destinations[loc] (tank/bk) {GIVEN_UP}" in result.stderr
        assert db_state(db) == state
        assert len(buffers_in(scratch_tmp)) == 1


def whole_edit_to_recursive_without_the_destination():
    """Editor actions: `tank` recursive, the `loc` destination removed."""
    return [
        {"replace": ['  - name: "tank"\n    enabled: true\n    frequency: "1h"\n    recursive: false',
                     '  - name: "tank"\n    enabled: true\n    frequency: "1h"\n    recursive: true']},
        {"replace": ['destinations:\n  "loc":\n    url: "zfs:tank/bk"\n', ""]},
    ]


# --------------------------------------------------------------------------
# --force
# --------------------------------------------------------------------------

def flip_whole(stub_plan_recursive=True):
    """Editor actions for the whole-config buffer: `tank/data` becomes recursive."""
    return [{"replace": [
        '  - name: "tank/data"\n    enabled: true\n    frequency: "1h"\n    recursive: false',
        '  - name: "tank/data"\n    enabled: true\n    frequency: "1h"\n    recursive: true',
    ]}]


def flip_fragment():
    return [{"replace": ["recursive: false", "recursive: true"]}]


class TestForce:
    """Every writing command takes `--force`: the refusal is then printed as `warning: --force:` lines
    and the write goes ahead."""

    @pytest.fixture
    def planted(self, run, flat, fake_zfs):
        seed(fake_zfs, "tank/data")
        copy_below(fake_zfs, "tank/data/bk")
        return flat

    def drive(self, name, run, db, stub, tmp_path, force):
        """Run the writing command `name`, with `--force` or not, that makes `tank/data` recursive."""
        flag = ["--force"] if force else []
        env = None
        if name == "set":
            args = ["set", "datasets[tank/data].recursive", "true", *flag]
        elif name == "import":
            args = ["import", write_yaml(tmp_path, RECURSIVE, "r.yaml"), *flag]
        elif name == "edit":
            stub.plan(flip_whole())
            args, env = ["edit", "--no-prompt", *flag], stub.env(db)
        elif name == "edit dataset":
            stub.plan(flip_fragment())
            args, env = ["edit", "dataset", "tank/data", "--no-prompt", *flag], stub.env(db)
        elif name == "edit --force dataset":          # the group's flag, inherited by the subcommand
            stub.plan(flip_fragment())
            args, env = ["edit", *flag, "--no-prompt", "dataset", "tank/data"], stub.env(db)
        else:
            raise AssertionError(name)
        return run(*args, "-c", db, env=env)

    COMMANDS = ["set", "import", "edit", "edit dataset"]

    @pytest.mark.parametrize("name", COMMANDS)
    def test_without_force_the_command_refuses(self, run, planted, db, stub, scratch_tmp, tmp_path, name):
        state = db_state(db)
        result = self.drive(name, run, db, stub, tmp_path, force=False)
        assert result.exit_code == 1
        assert "refusing to write" in result.stderr and "tank/data/bk" in result.stderr
        assert db_state(db) == state and stored(db).datasets[0].recursive is False

    @pytest.mark.parametrize("name", COMMANDS + ["edit --force dataset"])
    def test_with_force_the_change_is_written_and_the_finding_is_a_warning(
        self, run, planted, db, stub, scratch_tmp, tmp_path, name
    ):
        gen = generation(db)
        result = self.drive(name, run, db, stub, tmp_path, force=True)
        assert result.exit_code == 0, result.stderr
        (warning,) = forced(result)
        assert "datasets[tank/data] is recursive and the backup copy tank/data/bk is below it" in warning
        assert "refusing to write" not in result.stderr
        assert stored(db).datasets[0].recursive is True and generation(db) == gen + 1

    def test_force_with_nothing_wrong_prints_nothing_extra(self, run, flat, db, fake_zfs):
        seed(fake_zfs, "tank/data")
        result = make_recursive(run, db, "--force")
        assert result.exit_code == 0 and forced(result) == []

    def test_force_on_edit_destination_reaches_the_editor_session(self, run, db, monkeypatch):
        make_db(run, db, BASE_LOCAL)
        seen = []
        monkeypatch.setattr(editor, "edit_destination", lambda *a, **kw: seen.append(("dest", kw["force"])))
        monkeypatch.setattr(editor, "edit_dataset", lambda *a, **kw: seen.append(("data", kw["force"])))
        monkeypatch.setattr(editor, "edit_config", lambda *a, **kw: seen.append(("all", kw["force"])))
        for args in (
            ("edit", "destination", "loc"),
            ("edit", "destination", "loc", "--force"),
            ("edit", "--force", "destination", "loc"),
            ("edit", "dataset", "tank", "--force"),
            ("edit", "--force", "dataset", "tank"),
            ("edit", "dataset", "tank"),
            ("edit",),
            ("edit", "--force"),
        ):
            result = run(*args, "-c", db)
            assert result.exit_code == 0, (args, result.stderr)
        assert seen == [
            ("dest", False), ("dest", True), ("dest", True),
            ("data", True), ("data", True), ("data", False),
            ("all", False), ("all", True),
        ]

    @pytest.mark.parametrize("args", [
        ("set",), ("unset",), ("import",), ("edit",), ("edit", "dataset"), ("edit", "destination"),
    ])
    def test_the_help_documents_force_and_says_it_is_unrelated_to_the_other_ones(self, run, args):
        text = " ".join(run(*args, "--help").stdout.split())
        assert "--force" in text
        assert "nrelated to" in text
        assert "backup copy" in text

    def test_the_other_force_flags_keep_their_own_meaning(self, run):
        for args in (("rename", "dataset"), ("export",), ("identity", "set-client-id")):
            text = " ".join(run(*args, "--help").stdout.split())
            assert "--force" in text and "backup copy below a recursive" not in text


# --------------------------------------------------------------------------
# failed reads warn and commit
# --------------------------------------------------------------------------

class TestCouldNotCheck:
    @pytest.fixture
    def ready(self, run, flat, fake_zfs):
        seed(fake_zfs, "tank/data", "tank/other")
        copy_below(fake_zfs, "tank/data/bk")      # would be refused if the read worked
        return flat

    def assert_wrote_with_warning(self, result, db, gen, *, reason=None):
        assert result.exit_code == 0, result.stderr
        (warning,) = [w for w in warnings_in(result) if "could not check" in w]
        assert warning.startswith(COULD_NOT_CHECK)
        assert "writing the configuration anyway" in warning
        if reason:
            assert reason in warning
        assert stored(db).datasets[0].recursive is True and generation(db) == gen + 1

    def test_no_zfs_on_this_host_warns_and_writes(self, run, ready, db, monkeypatch):
        monkeypatch.setattr(copies, "_load_zfs", lambda: (None, "libzfseasy: no zfs binary found"))
        gen = generation(db)
        result = make_recursive(run, db)
        self.assert_wrote_with_warning(result, db, gen, reason="ZFS cannot be used here (libzfseasy: no zfs binary found)")

    def test_a_pool_that_is_not_imported_warns_and_writes(self, run, flat, db, fake_zfs):
        gen = generation(db)                                    # the fake world is empty: no `tank` at all
        result = make_recursive(run, db)
        self.assert_wrote_with_warning(
            result, db, gen, reason="datasets[tank/data]: pool tank is not imported (or does not exist)"
        )

    @pytest.mark.integration
    def test_in_a_process_with_no_zfs_at_all_the_check_warns_and_the_write_goes_ahead(self, tmp_path, run, db):
        """A host that only manages configuration: `ZFS_CMD` unset and no `zfs` on `PATH`, so importing
        libzfseasy raises inside the check. Never reaches a real `zfs` (the test proves it cannot)."""
        import shutil

        make_db(run, db, FLAT)
        home = tmp_path / "home"
        home.mkdir()
        env = {k: v for k, v in os.environ.items() if k not in ("ZFS_CMD", "ZPOOL_CMD")}
        env["PATH"] = os.path.dirname(sys.executable)
        env["PYTHONPATH"] = os.pathsep.join(
            [os.path.dirname(os.path.dirname(os.path.abspath(__file__)))]
        )
        env["HOME"] = str(home)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        assert shutil.which("zfs", path=env["PATH"]) is None and shutil.which("zpool", path=env["PATH"]) is None
        done = subprocess.run(
            [sys.executable, "-m", "zfsbackup.cli", "set", "datasets[tank/data].recursive", "true", "-c", str(db)],
            capture_output=True, text=True, env=env, cwd=str(tmp_path), timeout=120,
        )
        report = f"exit {done.returncode}\n{done.stdout}\n{done.stderr}"
        assert done.returncode == 0, report
        assert COULD_NOT_CHECK in done.stderr and "ZFS cannot be used here" in done.stderr, report
        assert stored(db).datasets[0].recursive is True

    def test_a_failed_recursive_read_warns_and_writes(self, run, ready, db, fake_zfs):
        fake_zfs.fail("get", "tank/data", "-r")
        gen = generation(db)
        result = make_recursive(run, db)
        self.assert_wrote_with_warning(result, db, gen, reason="datasets[tank/data]: ")
        assert "failed on purpose" in result.stderr

    def test_a_failed_existence_check_warns_and_writes(self, run, ready, db, fake_zfs):
        fake_zfs.fail("list", "tank/data")
        gen = generation(db)
        result = make_recursive(run, db)
        self.assert_wrote_with_warning(result, db, gen, reason="datasets[tank/data]: ")

    def test_a_failed_read_of_a_given_up_target_warns_and_writes(self, run, db, fake_zfs, tmp_path):
        make_db(run, db, BASE_LOCAL)
        seed(fake_zfs, "tank")
        fake_zfs.fail("list", "tank/bk")
        gen = generation(db)
        result = run("import", write_yaml(tmp_path, cfg({"tank": "r", "tank/data": ""}), "n.yaml"), "-c", db)
        assert result.exit_code == 0, result.stderr
        (warning,) = [w for w in warnings_in(result) if "could not check" in w]
        assert "tank/bk" in warning
        assert stored(db).datasets[0].recursive is True

    def test_a_timeout_warns_writes_and_leaves_no_child(self, run, ready, db, fake_zfs, monkeypatch):
        monkeypatch.setattr(copies, "ZFS_CALL_TIMEOUT", 0.5)
        fake_zfs.slow_read(60, "tank/data")
        gen = generation(db)
        result = make_recursive(run, db)
        self.assert_wrote_with_warning(result, db, gen, reason="datasets[tank/data]")

    def test_the_first_timeout_stops_the_check_and_names_what_was_not_reached(
        self, run, flat, db, fake_zfs, monkeypatch, tmp_path
    ):
        seed(fake_zfs, "tank/data", "tank/other")
        copy_below(fake_zfs, "tank/other/bk")
        monkeypatch.setattr(copies, "ZFS_CALL_TIMEOUT", 0.5)
        fake_zfs.slow_read(60, "tank/data")
        result = run("import", write_yaml(tmp_path, cfg({"tank/data": "r", "tank/other": "r"}), "n.yaml"), "-c", db)
        assert result.exit_code == 0, result.stderr             # tank/other's copy was never looked for
        text = " ".join(w for w in warnings_in(result) if "could not check" in w)
        assert "datasets[tank/data]" in text and "timed out" in text
        assert "not checked: an earlier zfs call timed out: datasets[tank/other]" in text
        assert "budget" not in text
        assert not any(c[-1] == "tank/other" for c in fake_zfs.calls)

    def test_a_timeout_on_one_dataset_does_not_hide_a_finding_found_before_it(
        self, run, flat, db, fake_zfs, monkeypatch, tmp_path
    ):
        seed(fake_zfs, "tank/data", "tank/other")
        copy_below(fake_zfs, "tank/data/bk")
        monkeypatch.setattr(copies, "ZFS_CALL_TIMEOUT", 0.5)
        fake_zfs.slow_read(60, "tank/other")
        state = db_state(db)
        result = run("import", write_yaml(tmp_path, cfg({"tank/data": "r", "tank/other": "r"}), "n.yaml"), "-c", db)
        assert result.exit_code == 1
        assert "tank/data/bk" in result.stderr and "could not check" in result.stderr
        assert db_state(db) == state

    def test_an_error_on_one_dataset_does_not_stop_the_others(self, run, flat, db, fake_zfs, tmp_path):
        seed(fake_zfs, "tank/data", "tank/other")
        copy_below(fake_zfs, "tank/other/bk")
        fake_zfs.fail("get", "tank/data", "-r")
        state = db_state(db)
        result = run("import", write_yaml(tmp_path, cfg({"tank/data": "r", "tank/other": "r"}), "n.yaml"), "-c", db)
        assert result.exit_code == 1
        assert "tank/other/bk" in result.stderr
        assert count_containing(stderr_lines(result), "could not check") == 1
        assert db_state(db) == state

    def test_an_exhausted_budget_names_the_datasets_it_did_not_reach_and_writes(
        self, run, ready, db, fake_zfs, monkeypatch
    ):
        monkeypatch.setattr(copies, "CHECK_BUDGET", 0.0)
        gen = generation(db)
        result = make_recursive(run, db)
        self.assert_wrote_with_warning(result, db, gen, reason="tank/data")
        assert fake_zfs.calls == []

    def test_a_budget_that_runs_out_between_datasets_skips_the_rest(
        self, run, flat, db, fake_zfs, monkeypatch, tmp_path
    ):
        import types

        import zfsbackup.backup_manager as bm

        seed(fake_zfs, "tank/data", "tank/other")
        copy_below(fake_zfs, "tank/other/bk")                     # never looked for: the budget is gone
        clock = [0.0]
        monkeypatch.setattr(copies, "time", types.SimpleNamespace(monotonic=lambda: clock[0]))
        real = bm.DatasetManager.copy_tops

        def slow_copy_tops(self, config):
            found = real(self, config)
            clock[0] += copies.CHECK_BUDGET + 1                    # the first dataset used it all up
            return found

        monkeypatch.setattr(bm.DatasetManager, "copy_tops", slow_copy_tops)
        result = run("import", write_yaml(tmp_path, cfg({"tank/data": "r", "tank/other": "r"}), "n.yaml"), "-c", db)
        assert result.exit_code == 0, result.stderr
        text = " ".join(w for w in warnings_in(result) if "could not check" in w)
        assert "not reached within the 120 s budget: datasets[tank/other]" in text
        assert "timed out" not in text and "not checked: an earlier" not in text
        assert not any(c[-1] == "tank/other" for c in fake_zfs.calls)

    def test_the_timeout_is_applied_to_every_call(self, run, ready, db, monkeypatch):
        import libzfseasy

        seen = []
        real = libzfseasy.timeout

        @contextlib.contextmanager
        def spy(seconds):
            seen.append(seconds)
            with real(seconds):
                yield

        monkeypatch.setattr(libzfseasy, "timeout", spy)
        monkeypatch.setattr(copies, "ZFS_CALL_TIMEOUT", 12.5)
        assert make_recursive(run, db).exit_code == 1
        assert seen and all(abs(limit - 12.5) < 1e-6 for limit in seen)     # every step, at most this long

    def test_the_defaults_are_bounded(self):
        assert copies.ZFS_CALL_TIMEOUT == 30.0 and copies.CHECK_BUDGET == 120.0

    def test_a_concurrent_change_between_the_check_and_the_write_is_a_conflict_not_a_stale_commit(
        self, run, flat, db, fake_zfs, monkeypatch
    ):
        from zfsbackup.cli import main as cli_main
        from zfsbackup.config.store import dispose_all, read_config, resolve_config_path, write_config

        seed(fake_zfs, "tank/data")
        real = copies.enforce_copies

        def check_then_race(old, new, **kwargs):
            real(old, new, **kwargs)
            path = resolve_config_path(str(db))
            current = read_config(path)
            current.config.api_port = 9999
            write_config(path, current.config, expected_generation=current.generation)
            dispose_all()

        monkeypatch.setattr(cli_main.copies, "enforce_copies", check_then_race)
        result = make_recursive(run, db)
        assert result.exit_code == 75
        assert stored(db).datasets[0].recursive is False

    def test_a_refusal_takes_no_writer_lock_and_leaves_no_trace(self, run, flat, db, fake_zfs):
        from zfsbackup.config.store import resolve_config_path, writer_lock_path

        seed(fake_zfs, "tank/data")
        copy_below(fake_zfs, "tank/data/bk")
        before = db_state(db)
        lock = writer_lock_path(resolve_config_path(str(db)))
        lock_before = lock.exists() and lock.read_bytes()
        assert make_recursive(run, db).exit_code == 1
        assert db_state(db) == before
        assert (lock.exists() and lock.read_bytes()) == lock_before


# --------------------------------------------------------------------------
# a pool that is not imported
# --------------------------------------------------------------------------

POOL_GONE = "pool tank is not imported (or does not exist)"


class TestPoolCheck:
    """zfs answers "dataset does not exist" for a pool that is not imported as well, so a missing dataset
    alone is silent only when its POOL is there; otherwise the check could not look and says so."""

    def could_not(self, result):
        return [w for w in warnings_in(result) if "could not check" in w]

    def test_a_missing_dataset_in_a_present_pool_is_silent(self, run, flat, db, fake_zfs):
        seed(fake_zfs, "tank")
        result = make_recursive(run, db)
        assert result.exit_code == 0 and self.could_not(result) == []
        assert [c[-1] for c in fake_zfs.calls] == ["tank/data", "tank"]       # the dataset, then its pool

    def test_a_missing_dataset_in_an_absent_pool_warns_and_writes(self, run, flat, db, fake_zfs):
        result = make_recursive(run, db)
        assert result.exit_code == 0, result.stderr
        (warning,) = self.could_not(result)
        assert f"(datasets[tank/data]: {POOL_GONE})" in warning
        assert stored(db).datasets[0].recursive is True

    def test_the_pool_is_looked_up_once_for_all_its_datasets(self, run, flat, db, fake_zfs, tmp_path):
        source = write_yaml(tmp_path, cfg({"tank/data": "r", "tank/other": "r"}), "both.yaml")
        result = run("import", source, "-c", db)
        assert result.exit_code == 0, result.stderr
        (warning,) = self.could_not(result)
        assert f"datasets[tank/data]: {POOL_GONE}" in warning and f"datasets[tank/other]: {POOL_GONE}" in warning
        assert [c[-1] for c in fake_zfs.calls] == ["tank/data", "tank", "tank/other"]     # `tank` once

    def test_a_pool_root_dataset_that_does_not_exist_is_an_absent_pool_with_no_second_lookup(
        self, run, db, fake_zfs, tmp_path
    ):
        make_db(run, db, cfg({"tank": "", "tank/data": ""}))
        fake_zfs.clear_calls()
        result = run("import", write_yaml(tmp_path, cfg({"tank": "r", "tank/data": ""}), "n.yaml"), "-c", db)
        assert result.exit_code == 0, result.stderr
        (warning,) = self.could_not(result)
        assert f"datasets[tank]: {POOL_GONE}" in warning
        assert [c[-1] for c in fake_zfs.calls] == ["tank"]

    def test_a_pool_root_dataset_that_exists_is_checked_for_copies(self, run, db, fake_zfs, tmp_path):
        make_db(run, db, cfg({"tank": "", "tank/data": ""}))
        seed(fake_zfs, "tank")
        copy_below(fake_zfs, "tank/data/bk")
        result = run("import", write_yaml(tmp_path, cfg({"tank": "r", "tank/data": ""}), "n.yaml"), "-c", db)
        assert result.exit_code == 1 and "the backup copy tank/data/bk is below it" in result.stderr

    def test_a_given_up_target_missing_in_a_present_pool_is_silent(self, run, db, fake_zfs, tmp_path):
        make_db(run, db, BASE_LOCAL)
        seed(fake_zfs, "tank")
        result = run("import", write_yaml(tmp_path, cfg({"tank": "r", "tank/data": ""}), "n.yaml"), "-c", db)
        assert result.exit_code == 0 and self.could_not(result) == []

    def test_the_pool_cache_serves_check_a_and_check_b(self, run, db, fake_zfs, tmp_path):
        make_db(run, db, BASE_LOCAL)
        fake_zfs.clear_calls()                       # no pool at all: `tank` is both the root and the pool
        result = run("import", write_yaml(tmp_path, cfg({"tank": "r", "tank/data": ""}), "n.yaml"), "-c", db)
        assert result.exit_code == 0, result.stderr
        (warning,) = self.could_not(result)
        assert f"datasets[tank]: {POOL_GONE}" in warning and f"destinations[loc] (tank/bk): {POOL_GONE}" in warning
        assert [c[-1] for c in fake_zfs.calls] == ["tank", "tank/bk"]       # no third lookup of the pool

    def test_a_pool_that_is_there_stops_nothing_and_finds_the_copies(self, run, flat, db, fake_zfs):
        seed(fake_zfs, "tank", "tank/data")
        copy_below(fake_zfs, "tank/data/bk")
        result = make_recursive(run, db)
        assert result.exit_code == 1 and self.could_not(result) == []

    def test_the_pool_lookup_is_under_the_same_budget_checked_before_the_call(
        self, run, flat, db, fake_zfs, monkeypatch
    ):
        import types

        import libzfseasy

        clock = [0.0]
        monkeypatch.setattr(copies, "time", types.SimpleNamespace(monotonic=lambda: clock[0]))
        real = libzfseasy.exists

        def exists_then_out_of_time(ds):
            found = real(ds)
            clock[0] += copies.CHECK_BUDGET + 1        # the dataset's existence check used the budget up
            return found

        monkeypatch.setattr(libzfseasy, "exists", exists_then_out_of_time)
        result = make_recursive(run, db)
        assert result.exit_code == 0, result.stderr
        (warning,) = self.could_not(result)
        assert "not reached within the 120 s budget: datasets[tank/data]" in warning
        assert [c[-1] for c in fake_zfs.calls] == ["tank/data"]             # the pool was NOT looked up

    def test_the_pool_lookup_is_timed_out_like_every_other_call(self, run, flat, db, fake_zfs, monkeypatch):
        monkeypatch.setattr(copies, "ZFS_CALL_TIMEOUT", 0.5)
        fake_zfs.slow_read(60, "tank")
        result = make_recursive(run, db)
        assert result.exit_code == 0, result.stderr
        (warning,) = self.could_not(result)
        assert "datasets[tank/data]" in warning and "timed out" in warning

    def test_each_call_is_limited_by_what_is_left_of_the_budget(self, run, flat, db, fake_zfs, monkeypatch):
        import libzfseasy

        limits = []
        real = libzfseasy.timeout

        @contextlib.contextmanager
        def spy(seconds):
            limits.append(seconds)
            with real(seconds):
                yield

        monkeypatch.setattr(libzfseasy, "timeout", spy)
        monkeypatch.setattr(copies, "CHECK_BUDGET", 5.0)
        monkeypatch.setattr(copies, "ZFS_CALL_TIMEOUT", 30.0)
        seed(fake_zfs, "tank", "tank/data")
        assert make_recursive(run, db).exit_code == 0
        assert limits and all(0 < limit <= 5.0 for limit in limits)


# --------------------------------------------------------------------------
# rename dataset
# --------------------------------------------------------------------------

class TestRenameDataset:
    """`rename dataset` of an enabled recursive dataset is a newly recursive NAME: the copies below NEW are
    looked for before the write. `--force-copies` overrides; `--force` (remote destinations) does not."""

    @pytest.fixture
    def planted(self, run, recursive, fake_zfs):
        seed(fake_zfs, "tank", "tank/renamed")
        copy_below(fake_zfs, "tank/renamed/bk")
        return recursive

    def rename(self, run, db, *extra, old="tank/data", new="tank/renamed"):
        return run("rename", "dataset", old, new, *extra, "-c", db)

    def names(self, db):
        return [d.name for d in stored(db).datasets]

    def test_a_copy_below_the_new_name_refuses_and_names_the_flag(self, run, planted, db):
        state = db_state(db)
        result = self.rename(run, db)
        assert result.exit_code == 1
        assert REFUSAL in result.stderr and NOT_WRITTEN in result.stderr
        (finding,) = findings(result)
        assert finding.startswith("datasets[tank/renamed] is recursive and the backup copy tank/renamed/bk is below it")
        assert "--force-copies" in result.stderr
        assert db_state(db) == state and "tank/data" in self.names(db)

    def test_force_does_not_bypass_it(self, run, planted, db):
        state = db_state(db)
        result = self.rename(run, db, "--force")
        assert result.exit_code == 1 and REFUSAL in result.stderr
        assert db_state(db) == state

    def test_force_copies_renames_with_the_finding_as_a_warning(self, run, planted, db):
        result = self.rename(run, db, "--force-copies")
        assert result.exit_code == 0, result.stderr
        (warning,) = forced(result)
        assert "datasets[tank/renamed] is recursive and the backup copy tank/renamed/bk is below it" in warning
        assert "tank/renamed" in self.names(db) and "tank/data" not in self.names(db)

    def test_force_and_force_copies_are_independent_flags(self, run, planted, db):
        assert self.rename(run, db, "--force", "--force-copies").exit_code == 0

    def test_a_dry_run_is_refused_too_and_writes_nothing(self, run, planted, db):
        state = db_state(db)
        result = self.rename(run, db, "--dry-run")
        assert result.exit_code == 1 and REFUSAL in result.stderr
        assert db_state(db) == state

    def test_a_dry_run_with_force_copies_warns_and_writes_nothing(self, run, planted, db):
        state = db_state(db)
        result = self.rename(run, db, "--dry-run", "--force-copies")
        assert result.exit_code == 0, result.stderr
        assert len(forced(result)) == 1 and "[dry run]" in result.stdout
        assert db_state(db) == state

    def test_a_new_name_that_does_not_exist_in_zfs_yet_finds_nothing(self, run, recursive, db, fake_zfs):
        seed(fake_zfs, "tank")                     # the pool is there; `tank/renamed` is not (yet)
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        assert forced(result) == [] and not [w for w in warnings_in(result) if "could not check" in w]
        assert "tank/renamed" in self.names(db)

    def test_a_new_name_without_copies_is_renamed(self, run, recursive, db, fake_zfs):
        seed(fake_zfs, "tank", "tank/renamed")
        assert self.rename(run, db).exit_code == 0

    def test_a_failed_look_only_warns(self, run, recursive, db, fake_zfs):
        seed(fake_zfs, "tank", "tank/renamed")
        fake_zfs.fail("get", "tank/renamed", "-r")
        result = self.rename(run, db)
        assert result.exit_code == 0, result.stderr
        assert [w for w in warnings_in(result) if "could not check" in w]

    def test_a_non_recursive_dataset_is_never_looked_at(self, run, recursive, db, fake_zfs):
        seed(fake_zfs, "tank", "tank/o2")
        copy_below(fake_zfs, "tank/o2/bk")
        result = self.rename(run, db, old="tank/other", new="tank/o2")
        assert result.exit_code == 0, result.stderr
        assert not [c for c in fake_zfs.calls if "-r" in c]

    def test_a_disabled_recursive_dataset_is_not_looked_at(self, run, db, fake_zfs):
        make_db(run, db, cfg({"tank/data": "rd"}))
        seed(fake_zfs, "tank", "tank/renamed")
        copy_below(fake_zfs, "tank/renamed/bk")
        fake_zfs.clear_calls()
        assert self.rename(run, db).exit_code == 0
        assert not [c for c in fake_zfs.calls if "-r" in c]

    def test_the_help_documents_the_flag_and_that_force_is_separate(self, run):
        text = " ".join(run("rename", "dataset", "--help").stdout.split())
        assert "--force-copies" in text
        assert "Separate from --force" in text
        assert "refused when backup copies are found below it" in text

    def test_the_refusal_names_the_flag_to_re_run_with_not_the_plain_force(self, run, planted, db):
        result = self.rename(run, db)
        assert "re-run with --force-copies to rename anyway" in result.stderr
        assert "re-run with --force to" not in result.stderr


# --------------------------------------------------------------------------
# the wording of the refusals
# --------------------------------------------------------------------------

class TestWording:
    def test_the_closing_advice_says_one_error_per_copy_and_never_every_period(self, run, flat, db, fake_zfs):
        seed(fake_zfs, "tank/data")
        copy_below(fake_zfs, "tank/data/bk")
        result = make_recursive(run, db)
        text = " ".join(result.stderr.split())
        assert "or re-run with --force to write the configuration anyway" in text
        assert "it logs one ERROR per copy and `check` shows a standing warning on the dataset until they are moved" in text
        assert "every period" not in text

    def test_the_could_not_check_warning_does_not_promise_an_error_every_period_either(
        self, run, flat, db, fake_zfs
    ):
        result = make_recursive(run, db)
        assert "every period" not in result.stderr


# --------------------------------------------------------------------------
# a refusal in the editor
# --------------------------------------------------------------------------

@pytest.fixture
def tty(monkeypatch):
    monkeypatch.setattr(editor, "_stdin_is_tty", lambda: True)


class TestEditor:
    @pytest.fixture
    def planted(self, run, flat, fake_zfs):
        seed(fake_zfs, "tank/data")
        copy_below(fake_zfs, "tank/data/bk")
        return flat

    def edit(self, run, db, stub, *args, input=None):
        return run("edit", *args, "-c", db, env=stub.env(db), input=input)

    def test_a_refusal_is_a_rejected_buffer_that_no_prompt_abandons_and_keeps(
        self, run, planted, db, stub, scratch_tmp, tty
    ):
        state = db_state(db)
        stub.plan(flip_whole())
        result = self.edit(run, db, stub, "--no-prompt", input="r\n")
        assert result.exit_code == 1
        assert "refusing to write" in result.stderr and "tank/data/bk" in result.stderr
        assert "[r]etry" not in result.stdout + result.stderr
        assert stub.runs == 1 and db_state(db) == state
        (kept,) = buffers_in(scratch_tmp)
        assert f"your edit is kept in {kept / 'config.yaml'}" in result.stderr
        assert "recursive: true" in (kept / "config.yaml").read_text()

    def test_retry_after_a_refusal_then_flipping_recursive_back_saves(
        self, run, planted, db, stub, scratch_tmp, tty
    ):
        gen = generation(db)
        stub.plan(
            flip_whole(),
            [{"replace": ["recursive: true", "recursive: false"]}, {"replace": ['frequency: "1h"', 'frequency: "2h"']}],
        )
        result = self.edit(run, db, stub, input="r\n")
        assert result.exit_code == 0, result.stderr
        assert "refusing to write" in result.stderr and "[r]etry editing or [a]bort" in result.stderr
        assert stub.runs == 2
        first, second = stub.log
        assert first["buffer"] == second["buffer"] and "recursive: true" in second["before"]   # the buffer was kept
        assert stored(db).datasets[0].recursive is False
        assert stored(db).datasets[0].frequency.total_seconds() == 7200
        assert generation(db) == gen + 1 and buffers_in(scratch_tmp) == []

    def test_abort_after_a_refusal_keeps_the_buffer_and_writes_nothing(
        self, run, planted, db, stub, scratch_tmp, tty
    ):
        state = db_state(db)
        stub.plan(flip_whole())
        result = self.edit(run, db, stub, input="a\n")
        assert result.exit_code == 1 and "edit abandoned; nothing was written" in result.stderr
        assert db_state(db) == state and len(buffers_in(scratch_tmp)) == 1

    def test_the_refusal_in_the_editor_hints_at_the_kept_buffer_not_at_a_plain_rerun(
        self, run, planted, db, stub, scratch_tmp, tty
    ):
        stub.plan(flip_whole())
        result = self.edit(run, db, stub, input="a\n")
        text = " ".join(result.stderr.split())
        assert "Move the copies out and Retry (the buffer reopens)" in text
        assert "or change the buffer to keep those datasets non-recursive" in text
        assert "abandon the edit and run `edit --force`" in text
        assert "starts again from the STORED configuration, not from the kept buffer" in text
        assert "re-run with --force" not in text and "every period" not in text

    def test_an_abandoned_edit_after_a_copies_refusal_says_to_make_the_edits_again(
        self, run, planted, db, stub, scratch_tmp, tty
    ):
        stub.plan(flip_whole())
        result = self.edit(run, db, stub, input="a\n")
        assert result.exit_code == 1
        assert (
            "edit abandoned; nothing was written; `edit --force` starts from the stored configuration, "
            "not from the kept buffer, so make the edits again (copy them from the kept buffer)"
        ) in " ".join(result.stderr.split())

    def test_no_prompt_after_a_copies_refusal_has_both_remarks(self, run, planted, db, stub, scratch_tmp):
        stub.plan(flip_whole())
        result = self.edit(run, db, stub, "--no-prompt")
        assert "edit abandoned; nothing was written (--no-prompt never retries); `edit --force` starts" in " ".join(
            result.stderr.split()
        )

    def test_an_abandoned_edit_for_another_reason_has_no_force_remark(
        self, run, planted, db, stub, scratch_tmp, tty
    ):
        stub.plan([{"replace": ['check_interval: "5m"', 'check_interval: "bogus"']}])
        result = self.edit(run, db, stub, input="a\n")
        assert result.exit_code == 1 and "edit abandoned; nothing was written" in result.stderr
        assert "edit --force" not in result.stderr

    def test_the_force_remark_is_gone_once_a_retry_is_refused_for_another_reason(
        self, run, planted, db, stub, scratch_tmp, tty
    ):
        stub.plan(flip_whole(), [{"replace": ['check_interval: "5m"', 'check_interval: "bogus"']}])
        result = self.edit(run, db, stub, input="r\na\n")
        assert result.exit_code == 1
        tail = result.stderr.split("edit abandoned")[-1]
        assert "edit --force" not in tail

    def test_force_in_the_editor_writes_with_a_warning(self, run, planted, db, stub, scratch_tmp, tty):
        stub.plan(flip_whole())
        result = self.edit(run, db, stub, "--force")
        assert result.exit_code == 0, result.stderr
        assert len(forced(result)) == 1 and stub.runs == 1
        assert stored(db).datasets[0].recursive is True and buffers_in(scratch_tmp) == []

    def test_edit_dataset_refusal_is_a_rejected_buffer_too(self, run, planted, db, stub, scratch_tmp, tty):
        state = db_state(db)
        stub.plan(flip_fragment(), [{"replace": ["recursive: true", "recursive: false"]}])
        result = self.edit(run, db, stub, "dataset", "tank/data", input="r\n")
        assert result.exit_code == 0, result.stderr
        assert "refusing to write" in result.stderr
        assert stub.runs == 2 and "No changes" in result.stdout and db_state(db) == state

    def test_a_dataset_edit_that_is_no_change_never_looks_at_zfs(self, run, flat, db, stub, scratch_tmp, fake_zfs):
        stub.plan([])
        result = self.edit(run, db, stub, "dataset", "tank/data", "--no-prompt")
        assert result.exit_code == 0 and fake_zfs.calls == []

    def test_a_concurrent_writer_still_wins_in_the_editor(self, run, planted, db, stub, scratch_tmp):
        stub.plan([*flip_whole(), {"cli": ["set", "check_interval", "9m", "-c", str(db)]}], )
        result = self.edit(run, db, stub, "--no-prompt", "--force")
        assert result.exit_code == 75
        assert stored(db).datasets[0].recursive is False


# --------------------------------------------------------------------------
# import
# --------------------------------------------------------------------------

class TestImportCheck:
    def test_a_first_import_is_checked_with_every_recursive_dataset_counting(self, run, db, fake_zfs, tmp_path):
        seed(fake_zfs, "tank/data")
        copy_below(fake_zfs, "tank/data/bk")
        source = write_yaml(tmp_path, RECURSIVE, "first.yaml")
        result = run("import", source, "-c", db)
        assert result.exit_code == 1
        assert "tank/data/bk" in result.stderr and not db.exists()

    def test_a_first_import_with_force_creates_the_database(self, run, db, fake_zfs, tmp_path):
        seed(fake_zfs, "tank/data")
        copy_below(fake_zfs, "tank/data/bk")
        result = run("import", write_yaml(tmp_path, RECURSIVE, "first.yaml"), "--force", "-c", db)
        assert result.exit_code == 0, result.stderr
        assert len(forced(result)) == 1 and db.exists()

    def test_an_unreadable_stored_configuration_counts_as_empty(self, run, db, fake_zfs, tmp_path):
        make_db(run, db, RECURSIVE)
        # a stored configuration `load_config` refuses: no datasets at all (what test_zfsbackup_cli.py
        # calls an unreadable outgoing configuration)
        conn = sqlite3.connect(db)
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("DELETE FROM datasets")
        conn.execute("DELETE FROM destinations")
        conn.commit()
        conn.close()
        seed(fake_zfs, "tank/data")
        copy_below(fake_zfs, "tank/data/bk")
        source = write_yaml(tmp_path, RECURSIVE, "again.yaml")
        state = db_state(db)
        result = run("import", source, "--no-backup", "-c", db)
        assert result.exit_code == 1
        assert "refusing to write" in result.stderr and db_state(db) == state
        # what the empty outgoing configuration costs: no given-up target can be known
        (unread,) = [l for l in stderr_lines(result) if l.startswith("warning: the stored configuration could not be read (")]
        assert (
            "so receive targets this import gives up were not checked for backup copies left below a "
            "recursive dataset (only the recursive datasets of the file were looked at)"
        ) in unread
        result = run("import", source, "--no-backup", "--force", "-c", db)
        assert result.exit_code == 0, result.stderr
        assert stored(db).datasets[0].recursive is True

    def test_a_readable_or_missing_stored_configuration_gets_no_unreadable_warning(
        self, run, recursive, db, fake_zfs, tmp_path
    ):
        again = run("import", write_yaml(tmp_path, RECURSIVE, "same.yaml"), "-c", recursive)
        assert again.exit_code == 0 and "could not be read" not in again.stderr
        fresh = run("import", write_yaml(tmp_path, RECURSIVE, "same.yaml"), "-c", tmp_path / "fresh" / "config.db")
        assert fresh.exit_code == 0 and "could not be read" not in fresh.stderr

    def test_an_already_recursive_dataset_is_not_looked_at_again(self, run, recursive, db, fake_zfs, tmp_path):
        copy_below(fake_zfs, "tank/data/bk")
        result = run("import", write_yaml(tmp_path, RECURSIVE.replace('"1h"', '"2h"'), "same.yaml"), "-c", recursive)
        assert result.exit_code == 0, result.stderr
        assert fake_zfs.calls == []

    def test_bad_yaml_is_left_to_the_importer_with_no_zfs_look(self, run, flat, db, fake_zfs, tmp_path):
        state = db_state(db)
        source = write_yaml(tmp_path, "datasets: [unclosed\n", "bad.yaml")
        result = run("import", source, "-c", db)
        assert result.exit_code == 1 and "refusing to write" not in result.stderr
        assert fake_zfs.calls == [] and db_state(db) == state

    def test_a_file_that_does_not_validate_is_left_to_the_importer_too(self, run, flat, db, fake_zfs, tmp_path):
        source = write_yaml(tmp_path, cfg({"tank/data": "r", "tank/data2": ""}, {"l": "zfs:tank/data/bk"}), "inv.yaml")
        result = run("import", source, "-c", db)
        assert result.exit_code == 1 and "refusing to write" not in result.stderr
        assert fake_zfs.calls == []

    @pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="needs named pipes")
    def test_a_fifo_is_not_checked_and_says_so(self, run, flat, db, fake_zfs, tmp_path):
        seed(fake_zfs, "tank/data")
        copy_below(fake_zfs, "tank/data/bk")                   # would be refused if the file were checked
        fifo = tmp_path / "pipe.yaml"
        os.mkfifo(fifo)

        def feed():
            with open(fifo, "w") as handle:
                handle.write(RECURSIVE)

        feeder = threading.Thread(target=feed, daemon=True)
        feeder.start()
        result = run("import", fifo, "-c", db)
        feeder.join(10)
        assert not feeder.is_alive()
        assert result.exit_code == 0, result.stderr
        assert (
            f"{fifo} is not a regular file, so it was not checked for backup copies below recursive datasets"
            in result.stderr
        )
        assert stored(db).datasets[0].recursive is True
        assert fake_zfs.calls == []

    def test_the_files_own_warnings_appear_once(self, run, flat, db, fake_zfs, tmp_path):
        seed(fake_zfs, "tank/data")
        source = write_yaml(
            tmp_path,
            'snapshot_prefix: "autosnap"\ncheck_interval: "5m"\ndatasets:\n'
            '  - name: "tank/data"\n    frequency: "1h"\n    recursive: true\n    retention: forever\n',
            "w.yaml",
        )
        result = run("import", source, "-c", db)
        assert result.exit_code == 0, result.stderr
        assert count_containing(warnings_in(result), NEVER_PRUNE_TEXT) == 1

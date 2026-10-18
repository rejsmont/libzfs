"""`zfsbackup-config rename dataset|destination` and `edit dataset|destination`
(plan Phase 2, item 14.2), plus the lazy-import closure of the whole CLI.

**Every test that reaches the ZFS side uses a FAKE `zfs`** (`tests/_fake_zfs.py`,
a JSON-backed script named by `$ZFS_CMD`); the module's autouse fixture installs
it for every test, and the suite-wide `_never_run_a_real_zfs` guard in
`conftest.py` fails a test that starts a real one. The fake's argv log is what
shows "`zfs set` of anchor.<new>" or "no `zfs inherit`".

Sections:

- ``TestRenameDataset``          refusals (unknown OLD, existing NEW, OLD==NEW,
                                 bad NEW), position kept, stale-property warning,
                                 the zfs-renamed case, `--force`, `--dry-run`.
- ``TestRenameDatasetClearStale`` `--clear-stale-props`: after the write only,
                                 daemon lock, ZFS unavailable, failures.
- ``TestRenameDatasetWithoutZfs`` ZFS cannot be loaded (patched, and a real
                                 process with no zfs on PATH).
- ``TestRenameDestination``      anchors copied before the write, partial
                                 failure, `--dry-run`, conflicts.
- ``TestEditEntry``              `edit dataset` / `edit destination` and the
                                 group-level flags.
- ``TestLazyImports``            importing the CLI loads no libzfseasy / daemon
                                 module; stdout stays free of libzfseasy's debug
                                 noise.

Follow-up round (same method; failing tests across this file and the edit /
signal files): daemon check dropped 8; lock held through the inherits again 6;
probe error read as "no daemon" 5; probe repeated per property 4; after-write
interrupt note dropped 8 / duplicated 4 / inherit commands omitted 4 / dataset
rename not annotated 6 / destination rename not annotated 5 / ordinary
failures annotated 5.

Review-fix round (scratch copy, one revert at a time; failing tests across this
file, `test_zfsbackup_cli_edit.py`, `..._edit_signals.py` and
`test_zfsbackup_manager.py`): shared lock never blocks 5 / released before the
clears 3 / lock error read as "no daemon" 3 / unreadable lock file read as "no
daemon" 3; up-front refusal (OLD exists, NEW does not) removed 5; stale hint
ignoring the pending `zfs rename` 3; cleared-so-far list not kept 4; mid-clear
interrupt note dropped 3; interrupted write unannotated 6; `overwrite=True`
not requested 7; `--skip-missing` ignored 5; destination name unchecked 11;
"likely" dropped from the full-send claim 4; stale hint for copied anchors only
2; mid-copy interrupt note dropped 4.

Earlier round (same method; failing tests across this file and
`test_zfsbackup_cli_edit.py`): existing NEW allowed 2; OLD == NEW
allowed 2; `//` allowed 1; `@`/`#` allowed 2; `--force` not required 5;
dry run writes 3; `--clear-stale-props` ignores the daemon 3 / unreadable
daemon lock treated as "no daemon" 1 / allowed without ZFS 2 / inherit run
before the write 3; zfs-renamed case undetected 2; stale warning dropped 1;
anchors not copied 8 / copy ignores `--dry-run` 1 / copied list not kept 1 /
remote lists not renamed 13; libzfseasy debug not silenced 4; eager
`backup_manager` import 8.
"""

import contextlib
import errno
import fcntl
import json
import os
import shlex
import subprocess
import sys
import time

import pytest

from _cli_support import (
    ANCHOR,
    CONFIG_PROP,
    RICH_YAML,
    Cli,
    FakeZfs,
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
    unsignalable,
    warnings_in,
    write_yaml,
)
from zfsbackup.cli import dotpath, editor, rename
from zfsbackup.config.store import (
    ConfigBusy,
    ConfigConflict,
    dispose_all,
    read_config,
    resolve_config_path,
    write_config,
)

# No module-level tier marker: conftest marks every test that declares none as
# `unit`, and the few that spawn a process are marked `integration` individually
# (a test carrying both would be double-counted; see test_marker_tiers.py).

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RENAME_YAML = RICH_YAML.replace(
    '  - name: "a"\n',
    '  - name: "tank/other"\n    remote:\n      - destination: "offsite"\n  - name: "a"\n',
)


#: The first words of the advice printed after a `rename destination` that left
#: stale anchors (it used to begin "Restart the daemon now"; a running daemon
#: now reloads, so only a legacy one is told to restart).
ADVICE = "A running daemon reloads the configuration"


@pytest.fixture(autouse=True)
def _fake_zfs_everywhere(fake_zfs):
    """No test in this module may see a real `zfs`: ZFS_CMD is always the fake."""
    assert os.environ["ZFS_CMD"] == str(fake_zfs.wrapper)
    yield


@pytest.fixture
def rich(run, db):
    return make_db(run, db, RENAME_YAML)


def names(database):
    return [d.name for d in stored(database).datasets]


def race(monkeypatch, database, *, module=rename):
    """Between the snapshot and the write, another writer commits."""
    real = module.commit_config
    done = []

    def commit(resolved, snapshot, config, **kwargs):
        resolved_path = resolve_config_path(str(database))
        current = read_config(resolved_path)
        current.config.api_port = 9999
        write_config(resolved_path, current.config, expected_generation=current.generation)
        dispose_all()
        done.append(True)
        return real(resolved, snapshot, config, **kwargs)

    monkeypatch.setattr(module, "commit_config", commit)
    return done


def move_generation_before_the_lock(monkeypatch, database):
    """Between "the snapshot was read" and "the writer lock is taken", another
    writer commits (it takes and releases the lock itself)."""
    real = rename.writer_lock
    done = []

    @contextlib.contextmanager
    def racing(resolved, *args, **kwargs):
        current = read_config(resolve_config_path(str(database)))
        current.config.api_port = 9999
        write_config(resolve_config_path(str(database)), current.config,
                     expected_generation=current.generation)
        dispose_all()
        done.append(True)
        with real(resolved, *args, **kwargs) as held:
            yield held

    monkeypatch.setattr(rename, "writer_lock", racing)
    return done


class TestRenameDataset:
    def test_a_dataset_without_remotes_is_renamed_without_force(self, run, rich, fake_zfs):
        gen = generation(rich)
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        assert result.exit_code == 0, result.stderr
        lines = result.stdout.splitlines()
        assert lines[0] == "Renamed dataset 'tank/vm.01' -> 'tank/vm.02' in the configuration."
        assert lines[1].startswith(f"config updated (generation {gen + 1})")
        assert "tank/vm.01" not in names(rich) and "tank/vm.02" in names(rich)
        assert fake_zfs.mutations == []

    def test_the_dataset_keeps_its_position(self, run, rich):
        before = names(rich)
        assert run("rename", "dataset", "tank/vm.01", "tank/zzz", "-c", rich).exit_code == 0
        after = names(rich)
        assert after[before.index("tank/vm.01")] == "tank/zzz"
        assert [n for n in after if n != "tank/zzz"] == [n for n in before if n != "tank/vm.01"]

    def test_everything_but_the_name_is_preserved(self, run, rich):
        before = dotpath.to_dict(stored(rich))
        run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        after = dotpath.to_dict(stored(rich))
        for old_entry, new_entry in zip(before["datasets"], after["datasets"]):
            old_entry = dict(old_entry)
            if old_entry["name"] == "tank/vm.01":
                old_entry["name"] = "tank/vm.02"
            assert old_entry == new_entry
        assert before["destinations"] == after["destinations"]

    def test_literals_survive_the_rename(self, run, db):
        make_db(run, db, 'datasets:\n  - name: "tank/x"\n    frequency: "60m"\n    retention:\n      "7d": "720h"\n')
        run("rename", "dataset", "tank/x", "tank/y", "-c", db)
        ds = stored(db).datasets[0]
        assert ds.frequency.literal == "60m"
        assert [(r.age.literal, r.keep_for.literal) for r in ds.retention_rules] == [("7d", "720h")]

    @pytest.mark.parametrize(
        "old, new, code, fragment",
        [
            ("tank/nosuch", "tank/new", 1, "no dataset named 'tank/nosuch'"),
            ("tank/vm.01", "a", 1, "'a' already exists in the configuration"),
            ("tank/vm.01", "tank/vm.01", 1, "already named 'tank/vm.01'"),
            ("tank/vm.01", "tank/x@snap", 1, "not a valid ZFS dataset name"),
            ("tank/vm.01", "tank/x#mark", 1, "not a valid ZFS dataset name"),
            ("tank/vm.01", "tank//x", 1, "not a valid ZFS dataset name"),
            ("tank/vm.01", "/tank/x", 1, "not a valid ZFS dataset name"),
            ("tank/vm.01", "tank/x/", 1, "not a valid ZFS dataset name"),
            ("tank/vm.01", "", 1, "not a usable dataset name"),
            ("tank/vm.01", " tank/x", 1, "not a usable dataset name"),
            ("tank/vm.01", "tank/x ", 1, "not a usable dataset name"),
        ],
    )
    def test_refusals_write_nothing(self, run, rich, fake_zfs, old, new, code, fragment):
        state, gen = db_state(rich), generation(rich)
        result = run("rename", "dataset", old, new, "-c", rich)
        assert result.exit_code == code, result.stdout + result.stderr
        assert fragment in result.stderr
        assert "Renamed" not in result.stdout
        assert db_state(rich) == state and generation(rich) == gen
        assert fake_zfs.mutations == []

    def test_an_unknown_old_lists_the_datasets(self, run, rich):
        result = run("rename", "dataset", "nosuch", "x", "-c", rich)
        for name in ("tank/data", "tank/vm.01", "a"):
            assert name in result.stderr

    def test_a_dotted_name_is_a_valid_new_name(self, run, rich):
        assert run("rename", "dataset", "tank/vm.01", "tank/vm.02.x", "-c", rich).exit_code == 0

    def test_stale_properties_on_the_old_dataset_are_reported(self, run, rich, fake_zfs):
        # NEW exists in ZFS (without properties): the properties will NOT follow.
        fake_zfs.add("tank/vm.01", {CONFIG_PROP: "cfg", ANCHOR + "offsite": "snap1", "compression": "lz4"})
        fake_zfs.add("tank/vm.02", {})
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        assert result.exit_code == 0, result.stderr
        stale = [w for w in warnings_in(result) if "keeps its org.zfsbackup:*" in w]
        assert len(stale) == 1
        assert CONFIG_PROP in stale[0] and ANCHOR + "offsite" in stale[0]
        assert "compression" not in stale[0]  # only our properties
        assert "become stale" in stale[0] and "zfs rename" not in stale[0]
        # reported, not touched
        assert fake_zfs.mutations == []
        assert fake_zfs.props("tank/vm.01")[ANCHOR + "offsite"] == "snap1"

    def test_the_commands_to_remove_them_are_printed_after_the_rename(self, run, rich, fake_zfs):
        # re-running with --clear-stale-props is impossible (OLD is no longer a configured
        # dataset), so the exact commands are given
        fake_zfs.add("tank/vm.01", {CONFIG_PROP: "cfg", ANCHOR + "offsite": "snap1", "compression": "lz4"})
        fake_zfs.add("tank/vm.02", {})
        out = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich).stdout
        assert "The stale properties on 'tank/vm.01' were left in place. To remove them, once no daemon is running:" in out
        assert f"zfs inherit {ANCHOR}offsite tank/vm.01" in out and f"zfs inherit {CONFIG_PROP} tank/vm.01" in out
        assert "compression" not in out

    def test_no_commands_when_the_properties_will_follow_a_zfs_rename(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", {CONFIG_PROP: "cfg"})          # NEW does not exist yet
        out = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich).stdout
        assert "zfs inherit" not in out and "left in place" not in out

    def test_no_commands_when_the_properties_were_cleared(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", {CONFIG_PROP: "cfg"})
        fake_zfs.add("tank/vm.02", {})
        out = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "--clear-stale-props", "-c", rich).stdout
        assert "left in place" not in out and "Cleared on 'tank/vm.01'" in out

    def test_no_commands_when_there_is_nothing_stale(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", {"compression": "lz4"})
        fake_zfs.add("tank/vm.02", {})
        assert "zfs inherit" not in run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich).stdout

    def test_no_commands_in_a_dry_run(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", {CONFIG_PROP: "cfg"})
        fake_zfs.add("tank/vm.02", {})
        out = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-d", "-c", rich).stdout
        assert "zfs inherit" not in out and fake_zfs.mutations == []

    def test_when_new_does_not_exist_the_warning_says_the_properties_follow_a_zfs_rename(
        self, run, rich, fake_zfs
    ):
        fake_zfs.add("tank/vm.01", {CONFIG_PROP: "cfg"})
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        assert result.exit_code == 0, result.stderr
        stale = [w for w in warnings_in(result) if "keeps its org.zfsbackup:*" in w]
        assert len(stale) == 1
        assert "If you are about to `zfs rename tank/vm.01 tank/vm.02`, these properties will follow it" in stale[0]
        assert "don't clear them" in stale[0]
        assert "removes them" not in stale[0]

    def test_a_dataset_with_no_properties_has_nothing_stale(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", {"compression": "lz4"})
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        assert result.exit_code == 0
        assert [w for w in warnings_in(result) if "org.zfsbackup" in w] == []

    def test_a_dataset_missing_from_zfs_has_nothing_stale(self, run, rich, fake_zfs):
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        assert result.exit_code == 0
        assert [w for w in warnings_in(result) if "org.zfsbackup" in w] == []
        assert "zfs rename" not in result.stdout

    def test_the_zfs_renamed_case_reports_nothing_stale(self, run, rich, fake_zfs):
        # OLD is gone from ZFS and NEW already carries our properties: the
        # operator ran `zfs rename` first.
        fake_zfs.add("tank/vm.02", {CONFIG_PROP: "cfg", ANCHOR + "offsite": "snap1"})
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert [w for w in warnings_in(result) if "keeps its" in w or "stale" in w] == []
        assert "already carries org.zfsbackup:* properties" in result.stdout
        assert "`zfs rename` that happened already" in result.stdout

    def test_with_remotes_force_is_required_and_explains_the_move(self, run, rich, fake_zfs):
        state = db_state(rich)
        result = run("rename", "dataset", "tank/data", "pool2/other", "-c", rich)
        assert result.exit_code == 1
        err = result.stderr
        assert "'tank/data' sends to offsite, dc.two" in err
        # the server-side path before and after
        assert "<server target>/<client-id>/data" in err
        assert "<server target>/<client-id>/other" in err
        # honest about what is known: a full send is LIKELY, not certain
        assert "likely a full send unless the server already has the dataset under the new path" in err
        assert "FULL send" not in err
        assert "orphaned" in err
        assert "--force" in err and "Nothing was changed" in err
        assert db_state(rich) == state and fake_zfs.mutations == []

    def test_force_renames_and_repeats_the_consequence_as_a_warning(self, run, rich):
        gen = generation(rich)
        result = run("rename", "dataset", "tank/data", "pool2/other", "--force", "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert generation(rich) == gen + 1
        assert "pool2/other" in names(rich)
        warned = [w for w in warnings_in(result) if w.startswith("warning: --force:")]
        assert len(warned) == 1 and "likely a full send" in warned[0]
        config = stored(rich)
        moved = next(d for d in config.datasets if d.name == "pool2/other")
        assert [r.destination for r in moved.remote] == ["offsite", "dc.two"]
        assert moved.remote[1].frequency.literal == "4h"

    def test_a_pool_only_rename_keeps_the_server_path_and_promises_no_full_send(self, run, rich):
        result = run("rename", "dataset", "tank/data", "pool2/data", "-c", rich)
        assert result.exit_code == 1
        err = result.stderr
        assert "does not change" in err
        assert "<server target>/<client-id>/data" in err
        # the server path is the same, so incrementals CAN continue; what the
        # new dataset lacks is the old one's anchors
        assert "incremental sends can continue if the new dataset has the snapshots the server already holds" in err
        assert "do not carry" in err or "does not carry" in err
        assert "full send" not in err.lower()

    def test_a_zfs_renamed_pool_only_rename_still_needs_force_but_no_full_send(self, run, rich, fake_zfs):
        fake_zfs.add("pool2/data", {ANCHOR + "offsite": "snap1"})
        result = run("rename", "dataset", "tank/data", "pool2/data", "-c", rich)
        assert result.exit_code == 1
        assert "--force" in result.stderr
        assert "the anchor properties came along with the dataset" in result.stderr
        assert "full send" not in result.stderr.lower()
        assert run("rename", "dataset", "tank/data", "pool2/data", "--force", "-c", rich).exit_code == 0

    def test_a_dataset_without_remotes_needs_no_force_whatever_the_server_path(self, run, rich):
        assert run("rename", "dataset", "tank/vm.01", "other/path", "-c", rich).exit_code == 0

    def test_a_conflict_is_75_and_the_rename_is_not_applied(self, run, rich, monkeypatch, fake_zfs):
        done = race(monkeypatch, rich)
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        assert done == [True]
        assert result.exit_code == 75
        assert "config changed underneath you" in result.stderr
        assert names(rich).count("tank/vm.01") == 1 and "tank/vm.02" not in names(rich)
        assert fake_zfs.mutations == []

    def test_dry_run_changes_nothing_and_runs_no_zfs_mutation(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", {CONFIG_PROP: "cfg", ANCHOR + "offsite": "snap1"})
        state, gen = db_state(rich), generation(rich)
        for flag in ("--dry-run", "-d"):
            result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", flag, "-c", rich)
            assert result.exit_code == 0, result.stderr
            assert "[dry run] would rename dataset 'tank/vm.01' -> 'tank/vm.02'" in result.stdout
            assert "config updated" not in result.stdout
            assert db_state(rich) == state and generation(rich) == gen
        assert fake_zfs.mutations == []
        assert fake_zfs.props("tank/vm.01")[CONFIG_PROP] == "cfg"

    def test_dry_run_with_clear_stale_props_lists_what_it_would_clear(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", {CONFIG_PROP: "cfg", ANCHOR + "offsite": "snap1"})
        fake_zfs.add("tank/vm.02", {})
        state = db_state(rich)
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-d", "--clear-stale-props", "-c", rich)
        assert result.exit_code == 0, result.stderr
        would = next(l for l in result.stdout.splitlines() if "would clear on" in l)
        assert CONFIG_PROP in would and ANCHOR + "offsite" in would
        assert fake_zfs.mutations == [] and db_state(rich) == state

    def test_dry_run_with_remotes_still_needs_force(self, run, rich):
        state = db_state(rich)
        result = run("rename", "dataset", "tank/data", "pool2/other", "-d", "-c", rich)
        assert result.exit_code == 1 and "--force" in result.stderr
        assert db_state(rich) == state
        result = run("rename", "dataset", "tank/data", "pool2/other", "-d", "--force", "-c", rich)
        assert result.exit_code == 0 and "[dry run] would rename" in result.stdout
        assert db_state(rich) == state

    def test_zfs_calls_are_read_only_without_clear_stale_props(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", {CONFIG_PROP: "cfg"})
        run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        assert {c[0] for c in fake_zfs.calls} <= {"list", "get"}

    def test_libzfseasys_debug_chatter_stays_off_stdout(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", {CONFIG_PROP: "cfg"})
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        assert "Executing command" not in result.stdout + result.stderr
        assert fake_zfs.calls  # ZFS really was consulted

    def test_missing_database_is_78_and_nothing_is_created(self, run, db, fake_zfs):
        result = run("rename", "dataset", "a", "b", "-c", db)
        assert result.exit_code == 78 and not db.parent.exists()
        assert fake_zfs.calls == []

    def test_the_rename_and_a_daemon_notice(self, run, rich):
        out = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich).stdout
        assert "no running daemon detected" in out

    def test_a_standing_warning_is_keyed_by_the_name_so_a_rename_says_it_once_under_the_new_one(
        self, run, rich
    ):
        # `tank/vm.01` is a never-prune dataset. The baseline suppression
        # compares warning TEXTS, and this one names the dataset, so the renamed
        # dataset's warning is new text: said once, under the new name only.
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        never = [w for w in warnings_in(result) if "never be pruned" in w]
        assert len(never) == 1 and "datasets[tank/vm.02]" in never[0]
        assert "tank/vm.01" not in result.stderr

    def test_an_unrelated_standing_warning_is_not_repeated(self, run, rich):
        result = run("rename", "dataset", "a", "a2", "-c", rich)
        assert [w for w in warnings_in(result) if "never be pruned" in w] == []


def _try_exclusive_lock(path) -> str:
    """'acquired' if a daemon-style exclusive lock on `path` can be taken right
    now (and releases it), 'blocked' if something holds the file."""
    fd = os.open(str(path), os.O_RDONLY)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return "blocked"
        fcntl.flock(fd, fcntl.LOCK_UN)
        return "acquired"
    finally:
        os.close(fd)


class TestRenameDatasetClearStale:
    PROPS = {CONFIG_PROP: "cfg", ANCHOR + "offsite": "snap1", ANCHOR + "dc.two": "snap2", "compression": "lz4"}
    #: the order the properties are cleared in (sorted)
    ORDER = [ANCHOR + "dc.two", ANCHOR + "offsite", CONFIG_PROP]

    @pytest.fixture
    def stale(self, fake_zfs):
        """OLD carries properties; NEW exists too (the --clear-stale-props
        precondition: the operator did not just forget to `zfs rename`)."""
        fake_zfs.add("tank/vm.01", self.PROPS)
        fake_zfs.add("tank/vm.02", {})
        return fake_zfs

    def clear(self, run, rich, *extra):
        return run("rename", "dataset", "tank/vm.01", "tank/vm.02", "--clear-stale-props", *extra, "-c", rich)

    def inherits(self, fake):
        return [c for c in fake.calls if c[0] == "inherit"]

    # -- the happy path -------------------------------------------------

    def test_clears_after_the_write_and_only_our_properties(self, run, rich, stale):
        result = self.clear(run, rich)
        assert result.exit_code == 0, result.stderr
        assert "tank/vm.02" in names(rich)
        assert stale.props("tank/vm.01") == {"compression": "lz4"}
        assert all(c[-1] == "tank/vm.01" for c in self.inherits(stale))
        assert "Cleared on 'tank/vm.01':" in result.stdout
        assert "removes them" not in result.stderr  # the flag was given: no hint

    def test_one_zfs_inherit_per_property_in_order(self, run, rich, stale):
        self.clear(run, rich)
        assert [c[1] for c in self.inherits(stale)] == self.ORDER

    def test_the_inherit_runs_only_after_the_configuration_is_written(self, run, rich, stale, monkeypatch):
        seen = {}
        real = rename.commit_config

        def commit(*args, **kwargs):
            seen["inherits_before_commit"] = len(self.inherits(stale))
            result = real(*args, **kwargs)
            seen["generation_after_commit"] = generation(rich)
            return result

        monkeypatch.setattr(rename, "commit_config", commit)
        assert self.clear(run, rich).exit_code == 0
        assert seen["inherits_before_commit"] == 0
        assert len(self.inherits(stale)) == 3

    def test_a_failed_write_runs_no_inherit(self, run, rich, stale, monkeypatch):
        race(monkeypatch, rich)
        result = self.clear(run, rich)
        assert result.exit_code == 75
        assert self.inherits(stale) == [] and stale.calls_of("set") == []
        assert stale.props("tank/vm.01") == self.PROPS

    def test_nothing_to_clear_is_fine(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", {"compression": "lz4"})
        fake_zfs.add("tank/vm.02", {})
        result = self.clear(run, rich)
        assert result.exit_code == 0 and fake_zfs.mutations == []

    def test_a_dataset_missing_from_zfs_has_nothing_to_clear(self, run, rich, fake_zfs):
        result = self.clear(run, rich)
        assert result.exit_code == 0 and fake_zfs.mutations == []

    def test_the_zfs_renamed_case_has_nothing_to_clear(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.02", {CONFIG_PROP: "cfg"})  # OLD is gone
        result = self.clear(run, rich)
        assert result.exit_code == 0 and fake_zfs.mutations == []

    # -- M3: the up-front refusal ---------------------------------------

    def test_refused_up_front_when_old_exists_and_new_does_not(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", self.PROPS)
        state = db_state(rich)
        result = self.clear(run, rich)
        assert result.exit_code == 1
        err = result.stderr
        assert "--clear-stale-props refused" in err
        assert "'tank/vm.02' does not exist yet but 'tank/vm.01' does" in err
        assert "zfs rename tank/vm.01 tank/vm.02" in err and "follow the dataset" in err
        assert "Nothing was changed" in err
        assert db_state(rich) == state and fake_zfs.mutations == []

    def test_the_refusal_also_applies_to_a_dry_run(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", self.PROPS)
        assert self.clear(run, rich, "-d").exit_code == 1

    def test_the_refusal_comes_before_the_force_check(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {CONFIG_PROP: "cfg"})
        result = run("rename", "dataset", "tank/data", "pool2/data", "--clear-stale-props", "-c", rich)
        assert result.exit_code == 1 and "does not exist yet" in result.stderr

    def test_without_the_flag_the_same_rename_goes_through(self, run, rich, fake_zfs):
        fake_zfs.add("tank/vm.01", self.PROPS)
        assert run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich).exit_code == 0
        assert fake_zfs.mutations == []

    # -- the daemon check: AFTER the write, held through the clears ------

    def test_a_running_daemon_means_nothing_is_cleared_but_the_rename_happened(
        self, run, rich, stale, daemon_lock_holder, unsignalable
    ):
        holder = daemon_lock_holder(rich, generation=0)
        result = self.clear(run, rich)
        assert result.exit_code == 1
        err = result.stderr
        assert "the configuration was renamed, but the stale properties on 'tank/vm.01' were NOT cleared" in err
        assert f"a daemon is running (pid {holder.pid})" in err
        assert "Once it is stopped, run:" in err
        for prop in self.ORDER:
            assert f"zfs inherit {prop} tank/vm.01" in err
        assert "tank/vm.02" in names(rich)                       # the write stands
        assert stale.mutations == [] and stale.props("tank/vm.01") == self.PROPS
        assert "config updated" in result.stdout                 # the daemon notice still printed

    def test_the_daemon_check_is_after_the_write_not_before(
        self, run, rich, stale, daemon_lock_holder, monkeypatch, unsignalable
    ):
        daemon_lock_holder(rich, generation=0)
        order = []
        real = rename.commit_config
        monkeypatch.setattr(
            rename, "commit_config", lambda *a, **k: (order.append("write"), real(*a, **k))[1]
        )
        real_no_daemon = rename._no_daemon

        def spying(resolved):
            order.append("daemon check")
            return real_no_daemon(resolved)

        monkeypatch.setattr(rename, "_no_daemon", spying)
        self.clear(run, rich)
        assert order == ["write", "daemon check"]

    def test_a_daemon_does_not_block_a_rename_without_the_flag(
        self, run, rich, stale, daemon_lock_holder, unsignalable
    ):
        daemon_lock_holder(rich, generation=0)
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert stale.mutations == []
        assert "running daemon" in result.stdout

    def test_a_missing_lock_file_means_no_daemon(self, run, rich, stale):
        assert not rename.daemonlock.lock_path(rich).exists()
        assert self.clear(run, rich).exit_code == 0

    def test_a_stale_lock_file_nobody_holds_means_no_daemon(self, run, rich, stale):
        rename.daemonlock.lock_path(rich).write_text("12345\n0\n")
        assert self.clear(run, rich).exit_code == 0
        assert stale.props("tank/vm.01") == {"compression": "lz4"}

    @pytest.mark.skipif(os.geteuid() == 0, reason="root can open an unreadable file")
    def test_an_unreadable_lock_file_means_cannot_tell_so_nothing_is_cleared(self, run, rich, stale):
        lock = rename.daemonlock.lock_path(rich)
        lock.write_text("1\n")
        lock.chmod(0)
        try:
            result = self.clear(run, rich)
        finally:
            lock.chmod(0o600)
        assert result.exit_code == 1
        assert "cannot tell whether a daemon is running" in result.stderr
        assert "zfs inherit" in result.stderr
        assert stale.mutations == [] and "tank/vm.02" in names(rich)

    def test_a_probe_that_raises_means_cannot_tell_so_nothing_is_cleared(self, run, rich, stale, monkeypatch):
        def broken(resolved):
            raise OSError(errno.EIO, "Input/output error")

        monkeypatch.setattr(rename.daemonlock, "probe", broken)
        result = self.clear(run, rich)
        assert result.exit_code == 1
        err = result.stderr
        assert "NOT cleared: cannot tell whether a daemon is running (Input/output error)" in err
        assert all(f"zfs inherit {p} tank/vm.01" in err for p in self.ORDER)
        assert stale.mutations == [] and "tank/vm.02" in names(rich)

    def test_one_probe_after_the_write_and_no_lock_is_held_through_the_inherits(self, run, rich, stale, monkeypatch):
        """The presence check is a single `daemonlock.probe` made after the
        commit; nothing is locked while the properties are cleared, so a daemon
        (or anything else) can take the exclusive lock mid-clear."""
        import zfsbackup.backup_manager as bm

        probes = []
        real_running = rename._daemon_running

        def counting(resolved):
            probes.append(generation(rich))   # the generation the probe saw
            return real_running(resolved)

        monkeypatch.setattr(rename, "_daemon_running", counting)
        gen = generation(rich)
        lock_path = rename.daemonlock.lock_path(rich)
        outcomes = []
        real_clear = bm.clear_zfsbackup_properties

        def taking_the_lock(*args, **kwargs):
            # the lock is free in the middle of the clears
            outcomes.append(_try_exclusive_lock(lock_path))
            return real_clear(*args, **kwargs)

        monkeypatch.setattr(bm, "clear_zfsbackup_properties", taking_the_lock)
        lock_path.write_text("")
        assert self.clear(run, rich).exit_code == 0
        assert probes == [gen + 1]            # exactly one, and after the write
        assert outcomes == ["acquired"] * 3

    @pytest.mark.integration
    @pytest.mark.parametrize("pid_file", ["missing", "stale"])
    @pytest.mark.slow
    def test_a_daemon_can_start_while_the_inherits_run(self, run, rich, stale, tmp_path, pid_file):
        """Across processes: a daemon's exclusive lock succeeds in the middle of a
        slow `zfs inherit`, loads the NEW configuration, and the CLI still
        finishes the clear (it already checked, once). A stale pid file left by
        a crashed daemon is not a daemon."""
        from zfsbackup import daemonlock

        resolved = resolve_config_path(str(rich))
        pid_path = daemonlock.lock_path(resolved)
        if pid_file == "stale":
            pid_path.write_text("99999\n0\n")
            pid_path.chmod(0o660)
        marker = tmp_path / "inherit.marker"
        env = dict(
            os.environ, PYTHONPATH=REPO, FAKEZFS_SLOW_INHERIT="1.5", FAKEZFS_MARKER=str(marker),
        )
        cli = subprocess.Popen(
            [sys.executable, "-m", "zfsbackup.cli", "-c", str(rich), "rename", "dataset",
             "tank/vm.01", "tank/vm.02", "--clear-stale-props"],
            env=env, cwd=str(tmp_path), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            deadline = time.monotonic() + 30
            while not marker.exists():
                assert cli.poll() is None, cli.communicate()
                assert time.monotonic() < deadline, "the first inherit never started"
                time.sleep(0.02)
            lock = daemonlock.acquire(resolved)      # a daemon starting mid-clear
            try:
                loaded = read_config(resolved)
                dispose_all()
                assert [d.name for d in loaded.config.datasets].count("tank/vm.02") == 1
                assert "tank/vm.01" not in [d.name for d in loaded.config.datasets]
            finally:
                lock.release()
            out, err = cli.communicate(timeout=60)
        finally:
            if cli.poll() is None:
                cli.kill()
                cli.communicate()
        assert cli.returncode == 0, err
        assert "Cleared on 'tank/vm.01':" in out
        assert "supervisor" not in out + err and "refus" not in (out + err).lower()
        assert stale.props("tank/vm.01") == {"compression": "lz4"}

    def test_dry_run_notes_a_running_daemon_and_still_does_nothing(
        self, run, rich, stale, daemon_lock_holder, unsignalable
    ):
        daemon_lock_holder(rich, generation=0)
        state = db_state(rich)
        result = self.clear(run, rich, "-d")
        assert result.exit_code == 0, result.stderr
        assert "would clear on 'tank/vm.01'" in result.stdout
        assert "a daemon is running" in result.stdout and "zfs inherit commands printed" in result.stdout
        assert db_state(rich) == state and stale.mutations == []

    # -- failures while clearing ----------------------------------------

    def test_a_failing_inherit_reports_what_was_cleared_and_what_is_left(self, run, rich, stale):
        stale.fail("inherit", "tank/vm.01", ANCHOR + "offsite")
        result = self.clear(run, rich)
        assert result.exit_code == 1
        err = result.stderr
        assert "the configuration was renamed, but clearing the stale properties on 'tank/vm.01' failed" in err
        assert f"Cleared so far: {ANCHOR}dc.two" in err
        assert "Still set, to clear by hand:" in err
        assert f"zfs inherit {ANCHOR}offsite tank/vm.01" in err and f"zfs inherit {CONFIG_PROP} tank/vm.01" in err
        assert f"zfs inherit {ANCHOR}dc.two" not in err
        assert "tank/vm.02" in names(rich)
        assert stale.props("tank/vm.01") == {
            CONFIG_PROP: "cfg", ANCHOR + "offsite": "snap1", "compression": "lz4",
        }
        # it stopped at the first failure
        assert [c[1] for c in self.inherits(stale)] == [ANCHOR + "dc.two", ANCHOR + "offsite"]

    def test_a_failure_on_the_first_property_says_none_were_cleared(self, run, rich, stale):
        stale.fail("inherit", "tank/vm.01", ANCHOR + "dc.two")
        err = self.clear(run, rich).stderr
        assert "Cleared so far: none" in err
        assert all(f"zfs inherit {p} tank/vm.01" in err for p in self.ORDER)

    def test_an_interrupt_mid_clear_lists_what_was_cleared(self, run, rich, stale, monkeypatch):
        import zfsbackup.backup_manager as bm

        real = bm.clear_zfsbackup_properties
        calls = []

        def interrupt_on_second(*args, **kwargs):
            calls.append(1)
            if len(calls) == 2:
                raise KeyboardInterrupt
            return real(*args, **kwargs)

        monkeypatch.setattr(bm, "clear_zfsbackup_properties", interrupt_on_second)
        result = self.clear(run, rich)
        assert result.exit_code == 130
        assert "interrupted" in result.stderr
        assert "the configuration was renamed; cleared on 'tank/vm.01' before the interrupt:" in result.stderr
        assert ANCHOR + "dc.two" in result.stderr.split("before the interrupt:")[1].split("Properties that may remain")[0]
        assert f"Properties that may remain: {ANCHOR}offsite, {CONFIG_PROP}" in result.stderr
        assert "tank/vm.02" in names(rich)


class TestRenameDatasetWithoutZfs:
    @pytest.fixture
    def no_zfs(self, monkeypatch):
        monkeypatch.setattr(rename, "_load_backup_manager", lambda: (None, "libzfseasy: no zfs binary here"))

    def test_zfs_unavailable_warns_and_renames_the_configuration_only(self, run, rich, no_zfs, fake_zfs):
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        assert result.exit_code == 0, result.stderr
        warned = [w for w in warnings_in(result) if "ZFS was not consulted" in w]
        assert len(warned) == 1 and "no zfs binary here" in warned[0]
        assert "tank/vm.02" in names(rich)
        assert fake_zfs.calls == []

    def test_clear_stale_props_refuses_without_zfs(self, run, rich, no_zfs):
        state = db_state(rich)
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "--clear-stale-props", "-c", rich)
        assert result.exit_code == 1
        assert "needs ZFS" in result.stderr and "nothing was changed" in result.stderr
        assert db_state(rich) == state

    def test_force_is_still_needed_for_remotes_without_zfs(self, run, rich, no_zfs):
        state = db_state(rich)
        assert run("rename", "dataset", "tank/data", "pool2/other", "-c", rich).exit_code == 1
        assert db_state(rich) == state
        assert run("rename", "dataset", "tank/data", "pool2/other", "--force", "-c", rich).exit_code == 0

    def test_a_zfs_error_while_looking_is_a_warning_not_a_failure(self, run, rich, fake_zfs):
        fake_zfs.fail("get", "tank/vm.01")
        fake_zfs.add("tank/vm.01", {CONFIG_PROP: "x"})
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert count_containing(warnings_in(result), "reading ZFS properties failed") == 1

    @pytest.mark.integration
    def test_in_a_process_with_no_zfs_at_all(self, tmp_path, run, db):
        make_db(run, db, RENAME_YAML)
        env = {k: v for k, v in os.environ.items() if k not in ("ZFS_CMD", "ZPOOL_CMD")}
        env["PATH"] = os.path.dirname(sys.executable)  # no zfs / zpool anywhere
        env["PYTHONPATH"] = REPO
        done = subprocess.run(
            [sys.executable, "-m", "zfsbackup.cli", "rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", str(db)],
            capture_output=True, text=True, env=env, cwd=str(tmp_path), timeout=120,
        )
        assert done.returncode == 0, done.stderr
        assert "ZFS was not consulted" in done.stderr
        assert "tank/vm.02" in names(db)
        refused = subprocess.run(
            [sys.executable, "-m", "zfsbackup.cli", "rename", "dataset", "tank/vm.02", "tank/vm.03",
             "--clear-stale-props", "-c", str(db)],
            capture_output=True, text=True, env=env, cwd=str(tmp_path), timeout=120,
        )
        assert refused.returncode == 1 and "needs ZFS" in refused.stderr
        no_anchors = subprocess.run(
            [sys.executable, "-m", "zfsbackup.cli", "rename", "destination", "offsite", "offsite2", "-c", str(db)],
            capture_output=True, text=True, env=env, cwd=str(tmp_path), timeout=120,
        )
        assert no_anchors.returncode == 1 and "cannot be used here" in no_anchors.stderr


class TestRenameDestination:
    @pytest.fixture
    def anchors(self, fake_zfs):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "snapA", ANCHOR + "dc.two": "snapB"})
        fake_zfs.add("tank/other", {ANCHOR + "offsite": "snapC"})
        return fake_zfs

    def rename(self, run, rich, *extra, new="offsite2", old="offsite"):
        return run("rename", "destination", old, new, *extra, "-c", rich)

    def test_renames_in_the_destinations_and_every_remote_list(self, run, rich, anchors):
        gen = generation(rich)
        result = self.rename(run, rich)
        assert result.exit_code == 0, result.stderr
        assert generation(rich) == gen + 1
        config = stored(rich)
        assert sorted(config.destinations) == ["dc.two", "offsite2", "spare"]
        assert config.destinations["offsite2"].url == "http://offsite:8080"
        by_name = {d.name: d for d in config.datasets}
        assert [r.destination for r in by_name["tank/data"].remote] == ["offsite2", "dc.two"]
        assert [r.destination for r in by_name["tank/other"].remote] == ["offsite2"]
        # the override on the OTHER destination is untouched
        assert by_name["tank/data"].remote[1].frequency.literal == "4h"
        assert by_name["tank/data"].remote[1].retention_rules[0].keep_for.literal == "7d"

    def test_the_anchors_are_copied_before_the_write_and_the_old_ones_kept(self, run, rich, anchors, monkeypatch):
        seen = {}
        real = rename.commit_config

        def commit(*args, **kwargs):
            seen["sets_before_commit"] = [c for c in anchors.calls if c[0] == "set"]
            return real(*args, **kwargs)

        monkeypatch.setattr(rename, "commit_config", commit)
        result = self.rename(run, rich)
        assert result.exit_code == 0, result.stderr
        assert sorted(seen["sets_before_commit"]) == sorted([
            ["set", f"{ANCHOR}offsite2=snapA", "tank/data"],
            ["set", f"{ANCHOR}offsite2=snapC", "tank/other"],
        ])
        # copied, never moved
        assert anchors.props("tank/data")[ANCHOR + "offsite"] == "snapA"
        assert anchors.props("tank/data")[ANCHOR + "offsite2"] == "snapA"
        assert anchors.props("tank/other")[ANCHOR + "offsite2"] == "snapC"
        assert anchors.calls_of("inherit") == []
        assert ANCHOR + "dc.two2" not in anchors.props("tank/data")  # the other destination: untouched

    # -- what is said about each dataset's anchor ------------------------

    def test_the_output_names_each_copy(self, run, rich, anchors):
        out = self.rename(run, rich).stdout
        assert "tank/data: anchor.offsite2 set to snapA (copied from anchor.offsite)" in out
        assert "tank/other: anchor.offsite2 set to snapC (copied from anchor.offsite)" in out
        assert "Renamed destination 'offsite' -> 'offsite2' in the configuration." in out

    def test_a_differing_new_anchor_is_overwritten_and_both_values_are_shown(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "newer", ANCHOR + "offsite2": "older"})
        fake_zfs.add("tank/other", {})
        result = self.rename(run, rich)
        assert result.exit_code == 0, result.stderr
        assert "tank/data: anchor.offsite2 was older, now newer (copied from anchor.offsite)" in result.stdout
        assert fake_zfs.props("tank/data")[ANCHOR + "offsite2"] == "newer"
        assert fake_zfs.props("tank/data")[ANCHOR + "offsite"] == "newer"  # the old one is kept

    def test_an_equal_new_anchor_is_already_current_and_not_set_again(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "snap", ANCHOR + "offsite2": "snap"})
        fake_zfs.add("tank/other", {})
        result = self.rename(run, rich)
        assert "tank/data: anchor.offsite2 is already current (snap)" in result.stdout
        assert fake_zfs.mutations == []

    def test_no_old_anchor_means_nothing_to_copy(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {ANCHOR + "offsite2": "keep-me"})  # a stray new one stays
        fake_zfs.add("tank/other", {})
        result = self.rename(run, rich)
        assert result.exit_code == 0, result.stderr
        assert "tank/data: has no anchor.offsite to copy" in result.stdout
        assert "tank/other: has no anchor.offsite to copy" in result.stdout
        assert fake_zfs.mutations == []
        assert fake_zfs.props("tank/data")[ANCHOR + "offsite2"] == "keep-me"

    def test_the_advice_after_a_rename(self, run, rich, anchors):
        out = self.rename(run, rich).stdout
        assert (
            "A running daemon reloads the configuration (see below) and then uses the "
            "new name 'offsite2'; until then, and after, the daemon protects every "
            "local or received org.zfsbackup:anchor.* property"
        ) in out
        assert "Restart the daemon" not in out
        assert "anchor.offsite keeps protecting its snapshot from pruning" in out
        assert "Once a send to 'offsite2' has succeeded, clear the old property" in out
        assert f"zfs inherit {ANCHOR}offsite tank/data" in out
        assert f"zfs inherit {ANCHOR}offsite tank/other" in out

    def test_only_datasets_that_have_an_old_anchor_get_an_inherit_hint(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {})                                   # nothing to copy
        fake_zfs.add("tank/other", {ANCHOR + "offsite": "x"})
        out = self.rename(run, rich).stdout
        hints = [l for l in out.splitlines() if l.strip().startswith("zfs inherit")]
        assert hints == [f"  zfs inherit {ANCHOR}offsite tank/other"]
        assert "on 'tank/other'" in out and "'tank/data'" not in out.split("A running daemon reloads")[1]

    def test_no_advice_when_no_anchor_was_involved(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {})
        fake_zfs.add("tank/other", {})
        assert ADVICE not in self.rename(run, rich).stdout

    def test_a_destination_nobody_uses_needs_no_zfs(self, run, rich, fake_zfs, monkeypatch):
        monkeypatch.setattr(rename, "_load_backup_manager", lambda: (None, "no zfs"))
        result = run("rename", "destination", "spare", "spare2", "-c", rich)
        assert result.exit_code == 0, result.stderr
        assert "spare2" in stored(rich).destinations and fake_zfs.calls == []
        assert ADVICE not in result.stdout

    def test_a_dotted_destination(self, run, rich, anchors):
        result = self.rename(run, rich, old="dc.two", new="dc.three")
        assert result.exit_code == 0, result.stderr
        assert anchors.props("tank/data")[ANCHOR + "dc.three"] == "snapB"
        assert sorted(stored(rich).destinations) == ["dc.three", "offsite", "spare"]

    @pytest.mark.parametrize(
        "old, new, fragment",
        [
            ("nosuch", "x", "no destination named 'nosuch'"),
            ("offsite", "spare", "'spare' already exists in the configuration"),
            ("offsite", "offsite", "already named 'offsite'"),
        ],
    )
    def test_refusals_write_nothing(self, run, rich, anchors, old, new, fragment):
        state = db_state(rich)
        result = run("rename", "destination", old, new, "-c", rich)
        assert result.exit_code == 1 and fragment in result.stderr
        assert db_state(rich) == state and anchors.mutations == []

    # -- the new name must fit in a ZFS property name --------------------

    @pytest.mark.parametrize(
        "new, fragment",
        [
            ("", "it is empty"),
            (" x", "it contains ' '"),
            ("has space", "it contains ' '"),
            ("a/b", "it contains '/'"),
            ("a@b", "it contains '@'"),
            ("café", "it contains 'é'"),
            ("a=b", "it contains '='"),
            ("x" * 300, "it is too long"),
        ],
    )
    def test_an_unusable_new_name_is_refused_before_anything_else(self, run, rich, anchors, new, fragment):
        state = db_state(rich)
        result = run("rename", "destination", "offsite", new, "-c", rich)
        assert result.exit_code == 1
        assert f"cannot be used as a destination name: {fragment}" in result.stderr
        assert "Nothing was changed" in result.stderr
        assert db_state(rich) == state and anchors.calls == []

    def test_the_new_name_is_checked_first_even_when_old_is_unknown(self, run, rich):
        result = run("rename", "destination", "nosuch", "bad name", "-c", rich)
        assert "cannot be used as a destination name" in result.stderr
        assert "no destination named" not in result.stderr

    @pytest.mark.parametrize("new", ["a-b", "a_b", "a.b", "a:b", "A1", "x" * 200])
    def test_the_accepted_characters(self, run, rich, fake_zfs, new):
        fake_zfs.add("tank/data", {})
        fake_zfs.add("tank/other", {})
        assert run("rename", "destination", "offsite", new, "-c", rich).exit_code == 0

    @pytest.mark.parametrize(
        "name, expected",
        [
            ("ok-1_x.y:z", None),
            ("", "it is empty"),
            ("a b", "it contains ' '"),
            ("x" * 235, None),
            ("x" * 240, "it is too long"),
        ],
    )
    def test_destination_name_problem(self, name, expected):
        problem = dotpath.destination_name_problem(name)
        if expected is None:
            assert problem is None
        else:
            assert problem is not None and expected in problem

    def test_the_length_limit_is_on_the_whole_property_name(self):
        limit = 256 - len("org.zfsbackup:anchor.")
        assert dotpath.destination_name_problem("x" * limit) is None
        assert "too long" in dotpath.destination_name_problem("x" * (limit + 1))

    # -- failures while copying -------------------------------------------

    def test_a_partial_copy_failure_aborts_before_the_write_with_the_restore_commands(self, run, rich, anchors):
        anchors.fail("set", "tank/other")
        state = db_state(rich)
        result = self.rename(run, rich)
        assert result.exit_code == 1
        err = result.stderr
        assert "on 'tank/other' failed" in err
        assert "Nothing was written to the configuration" in err
        # tank/data got a NEW anchor.offsite2 (it had none): putting it back = inherit
        assert "To put the previous values back run:" in err
        assert f"  zfs inherit {ANCHOR}offsite2 tank/data" in err
        assert "tank/other" not in err.split("To put the previous values back run:")[1]
        assert "Already copied" not in err
        assert db_state(rich) == state
        assert anchors.props("tank/data")[ANCHOR + "offsite2"] == "snapA"   # the copy that landed
        assert ANCHOR + "offsite2" not in anchors.props("tank/other")
        assert "offsite" in stored(rich).destinations

    def test_the_restore_command_for_an_overwritten_value_sets_the_old_one_back(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "newer", ANCHOR + "offsite2": "older"})
        fake_zfs.add("tank/other", {ANCHOR + "offsite": "snapC"})
        fake_zfs.fail("set", "tank/other")
        err = self.rename(run, rich).stderr
        assert f"  zfs set {ANCHOR}offsite2=older tank/data" in err
        assert "zfs inherit" not in err.split("To put the previous values back run:")[1]
        assert fake_zfs.props("tank/data")[ANCHOR + "offsite2"] == "newer"   # the overwrite did land

    def test_the_restore_commands_really_restore(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "newer", ANCHOR + "offsite2": "older"})
        fake_zfs.add("tank/other", {ANCHOR + "offsite": "snapC"})
        fake_zfs.add("tank/third", {})
        fake_zfs.fail("set", "tank/other")
        err = self.rename(run, rich).stderr
        commands = [
            line.strip() for line in err.split("To put the previous values back run:")[1].splitlines()
            if line.strip().startswith("zfs ")
        ]
        assert commands == [f"zfs set {ANCHOR}offsite2=older tank/data"]
        # run them, as the operator would, against the fake
        for command in commands:
            subprocess.run([str(fake_zfs.wrapper), *shlex.split(command)[1:]], check=True, env=dict(os.environ))
        assert fake_zfs.props("tank/data")[ANCHOR + "offsite2"] == "older"

    def test_a_first_copy_failure_has_nothing_to_restore(self, run, rich, anchors):
        anchors.fail("set", "tank/data")
        result = self.rename(run, rich)
        assert result.exit_code == 1
        assert "on 'tank/data' failed" in result.stderr
        assert "To put the previous values back" not in result.stderr and "zfs inherit" not in result.stderr

    def test_a_retry_after_a_partial_failure_completes_and_refreshes(self, run, rich, anchors, monkeypatch):
        anchors.fail("set", "tank/other")
        assert self.rename(run, rich).exit_code == 1
        monkeypatch.setenv("FAKEZFS_FAIL", "[]")
        result = self.rename(run, rich)
        assert result.exit_code == 0, result.stderr
        assert "tank/data: anchor.offsite2 is already current (snapA)" in result.stdout
        assert "tank/other: anchor.offsite2 set to snapC (copied from anchor.offsite)" in result.stdout

    def test_an_interrupt_mid_copy_lists_the_restore_commands(self, run, rich, anchors, monkeypatch):
        import zfsbackup.backup_manager as bm

        real = bm.copy_anchor_property
        calls = []

        def interrupting(ds, *args, **kwargs):
            calls.append(ds.name)
            if len(calls) == 2:
                raise KeyboardInterrupt
            return real(ds, *args, **kwargs)

        monkeypatch.setattr(bm, "copy_anchor_property", interrupting)
        state = db_state(rich)
        result = self.rename(run, rich)
        assert result.exit_code == 130
        err = result.stderr
        assert "interrupted" in err
        assert "interrupted while copying org.zfsbackup:anchor.offsite to anchor.offsite2 on 'tank/other'" in err
        assert "Nothing was written to the configuration" in err
        # the finished copy AND the one in flight (which may or may not have landed)
        assert f"  zfs inherit {ANCHOR}offsite2 tank/data" in err
        assert f"  zfs inherit {ANCHOR}offsite2 tank/other" in err
        assert db_state(rich) == state

    def test_an_interrupt_on_the_first_copy_restores_only_the_one_in_flight(self, run, rich, anchors, monkeypatch):
        import zfsbackup.backup_manager as bm

        def interrupting(ds, *args, **kwargs):
            raise KeyboardInterrupt

        monkeypatch.setattr(bm, "copy_anchor_property", interrupting)
        err = self.rename(run, rich).stderr
        assert f"  zfs inherit {ANCHOR}offsite2 tank/data" in err
        assert "tank/other" not in err.split("To put the previous values back run:")[1]

    def test_a_dataset_missing_from_zfs_aborts_before_anything_is_copied(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "snapA"})        # tank/other does not exist
        state = db_state(rich)
        result = self.rename(run, rich)
        assert result.exit_code == 1
        err = result.stderr
        assert "'tank/other' send(s) to 'offsite' but does not exist in ZFS" in err
        assert "anchor.offsite2 cannot be copied for it" in err
        assert "--skip-missing" in err and "Nothing was changed" in err
        assert db_state(rich) == state
        assert fake_zfs.mutations == []          # looked first: not even tank/data was copied

    def test_every_missing_dataset_is_named(self, run, rich, fake_zfs):
        result = self.rename(run, rich)
        assert "'tank/data', 'tank/other' send(s) to 'offsite' but do not exist in ZFS" in result.stderr
        assert "cannot be copied for them" in result.stderr

    def test_skip_missing_renames_and_copies_for_the_rest(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "snapA"})
        result = self.rename(run, rich, "--skip-missing")
        assert result.exit_code == 0, result.stderr
        warned = [w for w in warnings_in(result) if "does not exist in ZFS" in w]
        assert len(warned) == 1 and "dataset 'tank/other'" in warned[0] and "(--skip-missing)" in warned[0]
        assert fake_zfs.props("tank/data")[ANCHOR + "offsite2"] == "snapA"
        assert "offsite2" in stored(rich).destinations
        assert "tank/other:" not in result.stdout.split("Renamed destination")[0]

    def test_skip_missing_with_nothing_present_still_renames(self, run, rich, fake_zfs):
        result = self.rename(run, rich, "--skip-missing")
        assert result.exit_code == 0, result.stderr
        assert len([w for w in warnings_in(result) if "does not exist in ZFS" in w]) == 2
        assert fake_zfs.mutations == []
        assert "offsite2" in stored(rich).destinations

    def test_skip_missing_changes_nothing_when_all_datasets_exist(self, run, rich, anchors):
        result = self.rename(run, rich, "--skip-missing")
        assert result.exit_code == 0, result.stderr
        assert "does not exist" not in result.stderr
        assert anchors.props("tank/other")[ANCHOR + "offsite2"] == "snapC"

    def test_dry_run_also_refuses_a_missing_dataset(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {})
        state = db_state(rich)
        assert self.rename(run, rich, "-d").exit_code == 1
        assert self.rename(run, rich, "-d", "--skip-missing").exit_code == 0
        assert db_state(rich) == state and fake_zfs.mutations == []

    def test_a_zfs_error_while_looking_a_dataset_up_is_not_a_missing_dataset(self, run, rich, anchors, monkeypatch):
        import zfsbackup.backup_manager as bm

        def broken(ds):
            raise RuntimeError("zfs is on fire")

        monkeypatch.setattr(bm.zfs, "exists", broken)
        state = db_state(rich)
        result = self.rename(run, rich, "--skip-missing")
        assert result.exit_code == 1
        assert "cannot look up the ZFS dataset 'tank/data': zfs is on fire" in result.stderr
        assert db_state(rich) == state

    def test_the_help_lists_skip_missing(self, run):
        assert "--skip-missing" in run("rename", "destination", "--help").stdout

    # -- dry run, conflicts, no ZFS ---------------------------------------

    def test_dry_run_copies_nothing_and_writes_nothing(self, run, rich, anchors):
        state, gen = db_state(rich), generation(rich)
        for flag in ("--dry-run", "-d"):
            result = self.rename(run, rich, flag)
            assert result.exit_code == 0, result.stderr
            assert "[dry run] tank/data: anchor.offsite2 would be set to snapA (copied from anchor.offsite)" in result.stdout
            assert "[dry run] would rename destination 'offsite' -> 'offsite2'" in result.stdout
            assert "remote: lists of tank/data, tank/other" in result.stdout
            assert ADVICE not in result.stdout
            assert db_state(rich) == state and generation(rich) == gen
        assert anchors.mutations == []

    def test_dry_run_says_what_would_be_overwritten(self, run, rich, fake_zfs):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "newer", ANCHOR + "offsite2": "older"})
        fake_zfs.add("tank/other", {ANCHOR + "offsite": "same", ANCHOR + "offsite2": "same"})
        result = self.rename(run, rich, "-d")
        assert "[dry run] tank/data: anchor.offsite2 would be overwritten: was older, would become newer (copied from anchor.offsite)" in result.stdout
        assert "[dry run] tank/other: anchor.offsite2 is already current (same)" in result.stdout
        assert fake_zfs.mutations == []

    def test_a_generation_that_moved_before_the_copy_is_75_and_zfs_is_untouched(
        self, run, rich, anchors, monkeypatch
    ):
        done = move_generation_before_the_lock(monkeypatch, rich)
        result = self.rename(run, rich)
        assert done == [True]
        assert result.exit_code == 75, result.stderr
        assert "config changed underneath you" in result.stderr
        assert anchors.mutations == []                   # no zfs set, no zfs inherit
        assert ANCHOR + "offsite2" not in anchors.props("tank/data")
        assert "offsite" in stored(rich).destinations and "offsite2" not in stored(rich).destinations
        assert "To put the previous values back" not in result.stderr  # nothing to put back

    def test_a_commit_that_fails_after_the_copy_is_75_with_the_exact_restore_commands(
        self, run, rich, fake_zfs, monkeypatch
    ):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "newer", ANCHOR + "offsite2": "older"})
        fake_zfs.add("tank/other", {ANCHOR + "offsite": "snapC"})

        def busy(*args, **kwargs):
            raise ConfigBusy("database is locked by another process")

        monkeypatch.setattr(rename, "commit_config", busy)
        result = self.rename(run, rich)
        assert result.exit_code == 75
        err = result.stderr
        assert "database is locked by another process" in err
        assert "To put the previous values back run:" in err
        assert f"  zfs set {ANCHOR}offsite2=older tank/data" in err        # was overwritten
        assert f"  zfs inherit {ANCHOR}offsite2 tank/other" in err         # was absent
        assert "offsite2" not in stored(rich).destinations

    def test_a_conflict_raised_by_the_commit_is_75_with_the_restore_commands(
        self, run, rich, anchors, monkeypatch
    ):
        def conflict(*args, **kwargs):
            raise ConfigConflict(0, 1)

        monkeypatch.setattr(rename, "commit_config", conflict)
        result = self.rename(run, rich)
        assert result.exit_code == 75
        assert f"  zfs inherit {ANCHOR}offsite2 tank/data" in result.stderr
        assert f"  zfs inherit {ANCHOR}offsite2 tank/other" in result.stderr

    def test_the_commit_gets_the_held_writer_lock(self, run, rich, anchors, monkeypatch):
        seen = {}
        real = rename.commit_config

        def spy(resolved, snapshot, config, **kwargs):
            seen["lock"] = kwargs.get("lock")
            return real(resolved, snapshot, config, **kwargs)

        monkeypatch.setattr(rename, "commit_config", spy)
        assert self.rename(run, rich).exit_code == 0
        from zfsbackup.config.store import writer_lock_path

        assert seen["lock"] == writer_lock_path(resolve_config_path(str(rich)))

    def test_a_destination_nobody_uses_commits_without_a_held_lock(self, run, rich, monkeypatch):
        seen = {}
        real = rename.commit_config

        def spy(resolved, snapshot, config, **kwargs):
            seen["lock"] = kwargs.get("lock")
            return real(resolved, snapshot, config, **kwargs)

        monkeypatch.setattr(rename, "commit_config", spy)
        assert run("rename", "destination", "spare", "spare2", "-c", rich).exit_code == 0
        assert seen["lock"] is None

    def test_the_writer_lock_is_released_afterwards(self, run, rich, anchors):
        assert self.rename(run, rich).exit_code == 0
        assert run("set", "check_interval", "10m", "-c", rich).exit_code == 0

    def test_the_writer_lock_is_released_after_a_failure(self, run, rich, anchors, monkeypatch):
        def conflict(*args, **kwargs):
            raise ConfigConflict(0, 1)

        monkeypatch.setattr(rename, "commit_config", conflict)
        assert self.rename(run, rich).exit_code == 75
        monkeypatch.undo()
        assert run("set", "check_interval", "10m", "-c", rich).exit_code == 0

    def test_rerunning_after_a_refused_commit_refreshes_the_copies(self, run, rich, anchors, monkeypatch):
        real_commit = rename.commit_config

        def busy(*args, **kwargs):
            raise ConfigBusy("busy")

        monkeypatch.setattr(rename, "commit_config", busy)
        assert self.rename(run, rich).exit_code == 75
        monkeypatch.setattr(rename, "commit_config", real_commit)
        assert anchors.props("tank/data")[ANCHOR + "offsite2"] == "snapA"
        # the source moved on in the meantime (a daemon still on the old name)
        anchors.add("tank/data", {ANCHOR + "offsite": "snapNEW", ANCHOR + "offsite2": "snapA"})
        result = self.rename(run, rich)
        assert result.exit_code == 0, result.stderr
        assert "tank/data: anchor.offsite2 was snapA, now snapNEW (copied from anchor.offsite)" in result.stdout
        assert anchors.props("tank/data")[ANCHOR + "offsite2"] == "snapNEW"

    def test_zfs_unavailable_with_referencing_datasets_aborts(self, run, rich, monkeypatch):
        monkeypatch.setattr(rename, "_load_backup_manager", lambda: (None, "no zfs"))
        state = db_state(rich)
        result = self.rename(run, rich)
        assert result.exit_code == 1
        assert "ZFS cannot be used here (no zfs)" in result.stderr and "Nothing was changed" in result.stderr
        assert db_state(rich) == state

    def test_libzfseasys_debug_chatter_stays_off_stdout(self, run, rich, anchors):
        out = self.rename(run, rich)
        assert "Executing command" not in out.stdout + out.stderr

    def test_missing_database(self, run, db, fake_zfs):
        assert run("rename", "destination", "a", "b", "-c", db).exit_code == 78
        assert fake_zfs.calls == []


class TestEditEntry:
    """`edit dataset NAME` / `edit destination NAME` and the group's flags."""

    @pytest.fixture
    def ready(self, rich, scratch_tmp):
        return rich

    def edit(self, run, stub, database, *args, **kw):
        return run("edit", *args, "-c", database, env=stub.env(database), **kw)

    def test_the_dataset_buffer_has_no_name(self, run, ready, stub):
        stub.plan([])
        result = self.edit(run, stub, ready, "dataset", "tank/data", "--no-prompt")
        assert result.exit_code == 0, result.stderr
        before = stub.log[0]["before"]
        body = [l for l in before.splitlines() if not l.startswith("#")]
        assert not any(l.startswith("name:") for l in body)
        assert "rename dataset <old> <new>" in before  # the header says how
        for key in ("enabled:", "frequency:", "recursive:", "retention:", "remote:"):
            assert key in before
        assert "No changes" in result.stdout

    def test_a_dataset_edit_saves_and_keeps_position_and_name(self, run, ready, stub):
        before = names(ready)
        stub.plan([{"replace": ['frequency: "15m"', 'frequency: "20m"']}])
        result = self.edit(run, stub, ready, "dataset", "tank/vm.01", "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert result.stdout.startswith("Configuration saved (datasets changed: tank/vm.01).")
        assert names(ready) == before
        assert stored(ready).datasets[1].frequency.literal == "20m"

    def test_a_name_key_is_rejected_and_points_at_rename(self, run, ready, stub, scratch_tmp):
        state = db_state(ready)
        stub.plan([{"append": 'name: "tank/renamed"\n'}])
        result = self.edit(run, stub, ready, "dataset", "tank/vm.01", "--no-prompt")
        assert result.exit_code == 1
        assert "a name: key is not accepted here" in result.stderr
        assert "rename dataset <old> <new>" in result.stderr
        assert db_state(ready) == state and len(buffers_in(scratch_tmp)) == 1

    def test_an_empty_buffer_is_rejected(self, run, ready, stub, scratch_tmp):
        state = db_state(ready)
        stub.plan([{"write": ""}])
        result = self.edit(run, stub, ready, "dataset", "tank/data", "--no-prompt")
        assert result.exit_code == 1 and "the buffer is empty" in result.stderr
        assert db_state(ready) == state

    def test_a_comment_only_buffer_is_rejected_like_an_empty_one(self, run, ready, stub, scratch_tmp):
        stub.plan([{"write": "# nothing here\n"}])
        result = self.edit(run, stub, ready, "dataset", "tank/data", "--no-prompt")
        assert result.exit_code == 1 and "the buffer is empty" in result.stderr

    def test_a_non_mapping_buffer_is_rejected(self, run, ready, stub, scratch_tmp):
        stub.plan([{"write": "- a\n- b\n"}])
        result = self.edit(run, stub, ready, "dataset", "tank/data", "--no-prompt")
        assert result.exit_code == 1 and "must be a mapping" in result.stderr

    def test_omitted_keys_take_their_defaults(self, run, ready, stub):
        stub.plan([{"write": 'recursive: true\n'}])
        result = self.edit(run, stub, ready, "dataset", "tank/vm.01", "--no-prompt")
        assert result.exit_code == 0, result.stderr
        ds = stored(ready).datasets[1]
        assert ds.enabled is True                       # was false
        assert ds.frequency.literal == "1h"             # was 15m
        assert [(r.age.literal, r.keep_for.literal) for r in ds.retention_rules] == [("1d", "30d")]  # was forever
        assert ds.recursive is True and ds.remote == []

    def test_dropping_a_remote_warns_about_its_anchor(self, run, ready, stub):
        stub.plan([{"write": 'enabled: true\nfrequency: "1h"\nrecursive: true\nretention:\n  "1d": "30d"\n  "1w": "3M"\n'
                             'remote:\n  - destination: "dc.two"\n    frequency: "4h"\n    retention:\n      "1d": "7d"\n'}])
        result = self.edit(run, stub, ready, "dataset", "tank/data", "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert count_containing(warnings_in(result), "datasets[tank/data] no longer sends to 'offsite'") == 1
        assert [r.destination for r in stored(ready).datasets[0].remote] == ["dc.two"]

    def test_an_undeclared_destination_is_refused(self, run, ready, stub, scratch_tmp):
        state = db_state(ready)
        stub.plan([{"append": 'remote:\n  - destination: "ghost"\n'}])
        result = self.edit(run, stub, ready, "dataset", "tank/vm.01", "--no-prompt")
        assert result.exit_code == 1 and "'ghost' is not declared" in result.stderr
        assert db_state(ready) == state

    def test_a_conflict_is_75_and_the_buffer_is_kept(self, run, ready, stub, scratch_tmp):
        state_gen = generation(ready)
        stub.plan([
            {"replace": ['frequency: "15m"', 'frequency: "20m"']},
            {"cli": ["-c", str(ready), "set", "api_port", "9999"]},
        ])
        result = self.edit(run, stub, ready, "dataset", "tank/vm.01", "--no-prompt")
        assert result.exit_code == 75
        assert stored(ready).api_port == 9999 and stored(ready).datasets[1].frequency.literal == "15m"
        kept = buffers_in(scratch_tmp)
        assert len(kept) == 1 and f"your edit is kept in {kept[0] / 'config.yaml'}" in result.stderr
        assert generation(ready) == state_gen + 1       # only the racer's write

    def test_an_unknown_dataset(self, run, ready, stub):
        stub.plan([])
        result = self.edit(run, stub, ready, "dataset", "nosuch", "--no-prompt")
        assert result.exit_code == 1
        assert "no dataset named 'nosuch'" in result.stderr and "tank/data" in result.stderr
        assert stub.runs == 0

    def test_a_literal_only_dataset_edit_persists(self, run, ready, stub):
        stub.plan([{"replace": ['frequency: "15m"', 'frequency: "900s"']}])
        assert self.edit(run, stub, ready, "dataset", "tank/vm.01", "--no-prompt").exit_code == 0
        assert stored(ready).datasets[1].frequency.literal == "900s"

    def test_a_dataset_edit_does_not_trip_the_rename_guard(self, run, ready, stub):
        # a one-dataset buffer can never be rename-shaped; and it never needs --allow-recreate
        stub.plan([{"replace": ['recursive: false', 'recursive: true']}])
        assert self.edit(run, stub, ready, "dataset", "tank/vm.01", "--no-prompt").exit_code == 0

    # -- destinations ---------------------------------------------------

    def test_the_destination_buffer_is_just_a_url(self, run, ready, stub):
        stub.plan([])
        self.edit(run, stub, ready, "destination", "dc.two", "--no-prompt")
        body = [l for l in stub.log[0]["before"].splitlines() if l.strip() and not l.startswith("#")]
        assert body == ['url: "http://dc2:8080"']
        assert "rename destination <old> <new>" in stub.log[0]["before"]

    def test_a_destination_url_edit_saves(self, run, ready, stub):
        stub.plan([{"replace": ["http://dc2:8080", "http://dc3:9090"]}])
        result = self.edit(run, stub, ready, "destination", "dc.two", "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert "destinations changed: dc.two" in result.stdout
        assert stored(ready).destinations["dc.two"].url == "http://dc3:9090"

    @pytest.mark.parametrize(
        "extra, fragment",
        [
            ('frequency: "1h"\n', "unknown key(s) 'frequency'"),
            ("enabled: true\nretention: {}\n", "unknown key(s) 'enabled', 'retention'"),
            ('name: "x"\n', "a name: key is not accepted here"),
        ],
    )
    def test_only_url_is_accepted(self, run, ready, stub, scratch_tmp, extra, fragment):
        state = db_state(ready)
        stub.plan([{"append": extra}])
        result = self.edit(run, stub, ready, "destination", "dc.two", "--no-prompt")
        assert result.exit_code == 1 and fragment in result.stderr
        assert db_state(ready) == state

    def test_a_destination_buffer_without_a_url_is_rejected(self, run, ready, stub, scratch_tmp):
        state = db_state(ready)
        stub.plan([{"write": "{}\n"}])
        result = self.edit(run, stub, ready, "destination", "dc.two", "--no-prompt")
        assert result.exit_code == 1 and db_state(ready) == state

    def test_an_unknown_destination(self, run, ready, stub):
        stub.plan([])
        result = self.edit(run, stub, ready, "destination", "nosuch", "--no-prompt")
        assert result.exit_code == 1
        assert "no destination named 'nosuch'" in result.stderr and "dc.two" in result.stderr
        assert stub.runs == 0

    # -- flags ----------------------------------------------------------

    def test_group_level_flags_are_inherited_by_the_subcommands(self, run, ready, stub):
        stub.plan([{"replace": ['frequency: "15m"', 'frequency: "20m"']}])
        # --no-prompt and --editor given BEFORE the subcommand
        result = run(
            "edit", "--no-prompt", "--editor", f"{_stub_command()} --from-group", "dataset", "tank/vm.01",
            "-c", ready, env=stub.env(ready),
        )
        assert result.exit_code == 0, result.stderr
        assert stub.log[0]["argv"][0] == "--from-group"

    def test_subcommand_flags_override_the_groups(self, run, ready, stub):
        stub.plan([])
        result = run(
            "edit", "--editor", "/nonexistent/group-editor", "destination", "dc.two", "--no-prompt",
            "--editor", f"{_stub_command()} --from-sub", "-c", ready, env=stub.env(ready),
        )
        assert result.exit_code == 0, result.stderr
        assert stub.log[0]["argv"][0] == "--from-sub"

    def test_a_subcommand_without_no_prompt_needs_a_terminal(self, run, ready, stub):
        stub.plan([])
        result = self.edit(run, stub, ready, "dataset", "tank/data")
        assert result.exit_code == 1 and "interactive terminal" in result.stderr
        assert stub.runs == 0

    def test_allow_recreate_with_a_subcommand_is_a_usage_error(self, run, ready, stub):
        stub.plan([])
        for sub in (("dataset", "tank/data"), ("destination", "dc.two")):
            result = run("edit", "--allow-recreate", *sub, "--no-prompt", "-c", ready, env=stub.env(ready))
            assert result.exit_code == 2
            assert "--allow-recreate applies to the whole-config edit only" in result.stderr
        assert stub.runs == 0

    def test_bare_edit_still_edits_the_whole_configuration(self, run, ready, stub):
        stub.plan([{"replace": ['check_interval: "5m"', 'check_interval: "6m"']}])
        result = self.edit(run, stub, ready, "--no-prompt")
        assert result.exit_code == 0, result.stderr
        assert "settings changed: check_interval" in result.stdout
        assert stub.log[0]["before"].count("name:") >= 4  # every dataset is in the buffer

    def test_edit_help_lists_the_subcommands(self, run):
        out = run("edit", "--help").stdout
        assert "dataset" in out and "destination" in out
        assert "--editor" in out and "--no-prompt" in out and "--allow-recreate" in out
        out = run("edit", "dataset", "--help").stdout
        assert "--editor" in out and "--no-prompt" in out and "-c, --config" in out
        assert "--allow-recreate" not in out


def _stub_command():
    from _cli_support import STUB_COMMAND

    return STUB_COMMAND


class TestLazyImports:
    """Only the commands that need ZFS may load libzfseasy, and only when they
    run: importing the CLI must work on a host with no zfs at all."""

    CODE = (
        "import sys\n"
        "import zfsbackup.cli.main, zfsbackup.cli.render, zfsbackup.cli.errors, zfsbackup.cli.write\n"
        "import zfsbackup.cli.dotpath, zfsbackup.cli.listing, zfsbackup.cli.editor, zfsbackup.cli.rename\n"
        "bad = sorted(m for m in sys.modules if m.split('.')[0] == 'libzfseasy' "
        "or m in ('zfsbackup.daemon', 'zfsbackup.workers', 'zfsbackup.backup_manager', "
        "'zfsbackup.remote', 'zfsbackup.api'))\n"
        "print(bad)\n"
    )

    def clean_env(self):
        env = {k: v for k, v in os.environ.items() if k not in ("ZFS_CMD", "ZPOOL_CMD")}
        env["PATH"] = os.path.dirname(sys.executable)
        env["PYTHONPATH"] = REPO
        return env

    @pytest.mark.integration
    def test_importing_every_cli_module_loads_no_zfs_or_daemon_module(self, tmp_path):
        done = subprocess.run(
            [sys.executable, "-c", self.CODE], capture_output=True, text=True,
            env=self.clean_env(), cwd=str(tmp_path), timeout=120,
        )
        assert done.returncode == 0, done.stderr
        assert done.stdout.strip() == "[]"

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "args",
        [
            ("list", "datasets"),
            ("list", "destinations", "--json"),
            ("get", "datasets[tank/data].frequency"),
            ("set", "api_port", "9000"),
            ("unset", "prune_interval"),
            ("rename", "destination", "spare", "spare2"),
        ],
    )
    def test_commands_that_need_no_zfs_never_load_libzfseasy(self, tmp_path, run, db, args):
        make_db(run, db, RENAME_YAML)
        code = (
            "import sys\n"
            "from zfsbackup.cli.main import cli\n"
            "try:\n"
            f"    cli.main(args={list(map(str, args)) + ['-c', str(db)]!r}, standalone_mode=False)\n"
            "except SystemExit:\n"
            "    pass\n"
            "bad = sorted(m for m in sys.modules if m.split('.')[0] == 'libzfseasy' "
            "or m in ('zfsbackup.daemon', 'zfsbackup.workers', 'zfsbackup.backup_manager'))\n"
            "print('LOADED', bad)\n"
        )
        done = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True,
            env=self.clean_env(), cwd=str(tmp_path), timeout=120,
        )
        assert done.returncode == 0, done.stderr
        assert "LOADED []" in done.stdout, done.stdout + done.stderr

    def test_a_loaded_libzfseasy_is_silenced_for_stdout(self, run, db, fake_zfs):
        make_db(run, db, RENAME_YAML)
        import libzfseasy.zfs as zfs_module

        zfs_module.debug = True
        fake_zfs.add("tank/vm.01", {CONFIG_PROP: "x"})
        out = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", db)
        assert zfs_module.debug is False
        assert "Executing command" not in out.stdout


class TestInterruptDuringTheWrite:
    """An interrupt that lands once the write was entered may have committed:
    the CLI says so and names the command that finds out. A conflict, a busy
    lock or a validation error cannot have committed and keep "nothing was
    written"."""

    def interrupting(self, monkeypatch):
        def commit(*args, **kwargs):
            raise KeyboardInterrupt

        monkeypatch.setattr(rename, "commit_config", commit)

    CASES = [
        ("dataset", "tank/vm.01", "tank/vm.02"),
        ("destination", "spare", "spare2"),
    ]

    @pytest.mark.parametrize("kind, old, new", CASES)
    def test_an_interrupt_may_or_may_not_have_been_applied(self, run, rich, monkeypatch, kind, old, new):
        self.interrupting(monkeypatch)
        result = run("rename", kind, old, new, "-c", rich)
        assert result.exit_code == 130
        assert "interrupted" in result.stderr
        assert "the rename may or may not have been applied" in result.stderr
        assert f"zfsbackup-config -c {rich} export" in result.stderr
        assert "Nothing was written" not in result.stderr

    @pytest.mark.parametrize("kind, old, new", CASES)
    def test_the_hint_has_no_dash_c_for_the_default_database(self, run, tmp_path, monkeypatch, kind, old, new):
        make_db(run, tmp_path / "default" / "config.db", RENAME_YAML)
        self.interrupting(monkeypatch)
        result = run("rename", kind, old, new)
        assert result.exit_code == 130
        assert "run `zfsbackup-config export` to check" in result.stderr

    @pytest.mark.parametrize("kind, old, new", CASES)
    def test_a_conflict_still_says_nothing_was_written(self, run, rich, monkeypatch, kind, old, new):
        race(monkeypatch, rich)
        result = run("rename", kind, old, new, "-c", rich)
        assert result.exit_code == 75
        assert "Nothing was written" in result.stderr
        assert "may or may not" not in result.stderr

    @pytest.mark.parametrize("exc", [ConfigBusy("the writer lock is held"), ValueError("not valid")])
    @pytest.mark.parametrize("kind, old, new", CASES)
    def test_busy_and_validation_errors_are_not_ambiguous(self, run, rich, monkeypatch, kind, old, new, exc):
        def commit(*args, **kwargs):
            raise exc

        monkeypatch.setattr(rename, "commit_config", commit)
        state = db_state(rich)
        result = run("rename", kind, old, new, "-c", rich)
        assert result.exit_code in (1, 75)
        assert "may or may not" not in result.stderr
        assert db_state(rich) == state


class TestInterruptAfterTheWrite:
    """An interrupt that lands AFTER `commit_config` returned: the rename was
    applied; say so, with the `zfs inherit` commands still due for a dataset
    rename, and never twice."""

    PROPS = {CONFIG_PROP: "cfg", ANCHOR + "offsite": "snap1", ANCHOR + "dc.two": "snap2"}
    ORDER = sorted(PROPS)

    @pytest.fixture
    def stale(self, fake_zfs):
        fake_zfs.add("tank/vm.01", self.PROPS)
        fake_zfs.add("tank/vm.02", {})
        return fake_zfs

    def interrupt(self, monkeypatch, name="notify_daemon"):
        def boom(*args, **kwargs):
            raise KeyboardInterrupt

        monkeypatch.setattr(rename, name, boom)

    def test_dataset_rename_without_properties(self, run, rich, monkeypatch):
        self.interrupt(monkeypatch)
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich)
        assert result.exit_code == 130
        err = result.stderr
        assert f"the rename of dataset 'tank/vm.01' was applied; run `zfsbackup-config -c {rich} export` to see the configuration" in err
        assert "may or may not" not in err and "zfs inherit" not in err
        assert "tank/vm.02" in names(rich)  # it really was applied

    def test_dataset_rename_with_properties_left_lists_the_commands(self, run, rich, stale, monkeypatch):
        self.interrupt(monkeypatch, "_clear_stale")
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "--clear-stale-props", "-c", rich)
        assert result.exit_code == 130
        err = result.stderr
        assert "the rename of dataset 'tank/vm.01' was applied" in err
        assert "to clear them once no daemon is running:" in err
        for prop in self.ORDER:
            assert f"zfs inherit {prop} tank/vm.01" in err
        assert stale.props("tank/vm.01") == self.PROPS and "tank/vm.02" in names(rich)

    def test_no_inherit_commands_without_the_flag(self, run, rich, stale, monkeypatch):
        self.interrupt(monkeypatch)
        err = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "-c", rich).stderr
        assert "was applied" in err and "zfs inherit" not in err

    def test_not_duplicated_when_the_clear_already_explained_itself(self, run, rich, stale, monkeypatch):
        import zfsbackup.backup_manager as bm

        real = bm.clear_zfsbackup_properties
        calls = []

        def second_interrupts(*args, **kwargs):
            calls.append(1)
            if len(calls) == 2:
                raise KeyboardInterrupt
            return real(*args, **kwargs)

        monkeypatch.setattr(bm, "clear_zfsbackup_properties", second_interrupts)
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "--clear-stale-props", "-c", rich)
        assert result.exit_code == 130
        err = result.stderr
        assert err.count("the configuration was renamed; cleared on 'tank/vm.01' before the interrupt") == 1
        assert "was applied" not in err      # the first note already said it
        assert err.count("Properties that may remain") == 1

    def test_destination_rename_without_references(self, run, rich, monkeypatch):
        self.interrupt(monkeypatch)
        result = run("rename", "destination", "spare", "spare2", "-c", rich)
        assert result.exit_code == 130
        assert f"the rename of destination 'spare' was applied; run `zfsbackup-config -c {rich} export`" in result.stderr
        assert "zfs inherit" not in result.stderr
        assert "spare2" in stored(rich).destinations

    def test_destination_rename_with_copied_anchors(self, run, rich, fake_zfs, monkeypatch):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "snapA"})
        fake_zfs.add("tank/other", {ANCHOR + "offsite": "snapC"})
        self.interrupt(monkeypatch)
        result = run("rename", "destination", "offsite", "offsite2", "-c", rich)
        assert result.exit_code == 130
        assert "the rename of destination 'offsite' was applied" in result.stderr
        assert "offsite2" in stored(rich).destinations

    @pytest.mark.parametrize("kind, old, new", [("dataset", "tank/vm.01", "tank/vm.02"), ("destination", "spare", "spare2")])
    def test_an_interrupt_before_the_write_returned_is_not_called_applied(self, run, rich, monkeypatch, kind, old, new):
        def boom(*args, **kwargs):
            raise KeyboardInterrupt

        monkeypatch.setattr(rename, "commit_config", boom)
        err = run("rename", kind, old, new, "-c", rich).stderr
        assert "may or may not have been applied" in err and "was applied" not in err

    def test_an_ordinary_failure_after_the_write_adds_no_note(
        self, run, rich, stale, daemon_lock_holder, unsignalable
    ):
        daemon_lock_holder(rich, generation=0)
        result = run("rename", "dataset", "tank/vm.01", "tank/vm.02", "--clear-stale-props", "-c", rich)
        assert result.exit_code == 1
        assert "was applied" not in result.stderr and "NOT cleared" in result.stderr


class TestRenameDestinationHoldsTheWriterLock:
    """`rename destination` takes the writer lock BEFORE it touches ZFS and holds
    it until the write: nobody else can write in between, so a copy never runs
    against a configuration that has since changed."""

    @pytest.mark.integration
    @pytest.mark.slow
    def test_a_concurrent_set_is_blocked_for_the_whole_copy_and_then_conflicts(self, run, rich, fake_zfs, tmp_path):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "snapA"})
        fake_zfs.add("tank/other", {ANCHOR + "offsite": "snapC"})
        marker = tmp_path / "set.marker"
        env = dict(os.environ, PYTHONPATH=REPO, FAKEZFS_SLOW_SET="1.5", FAKEZFS_MARKER=str(marker))
        renamer = subprocess.Popen(
            [sys.executable, "-m", "zfsbackup.cli", "-c", str(rich), "rename", "destination", "offsite", "offsite2"],
            env=env, cwd=str(tmp_path), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        other = None
        try:
            deadline = time.monotonic() + 30
            while not marker.exists():                      # the first zfs set is running
                assert renamer.poll() is None, renamer.communicate()
                assert time.monotonic() < deadline
                time.sleep(0.02)
            started = time.monotonic()
            other = subprocess.run(
                [sys.executable, "-m", "zfsbackup.cli", "-c", str(rich), "set", "check_interval", "10m"],
                env=dict(os.environ, PYTHONPATH=REPO), cwd=str(tmp_path),
                capture_output=True, text=True, timeout=60,
            )
            waited = time.monotonic() - started
            out, err = renamer.communicate(timeout=60)
        finally:
            if renamer.poll() is None:
                renamer.kill()
                renamer.communicate()
        assert renamer.returncode == 0, err
        # blocked for (most of) the rest of the two slow copies, then lost the compare-and-swap
        assert other.returncode == 75, other.stdout + other.stderr
        assert "config changed underneath you" in other.stderr
        assert waited >= 1.0, waited
        config = stored(rich)
        assert "offsite2" in config.destinations and config.check_interval.literal == "5m"

    def test_the_lock_is_taken_before_the_first_zfs_set(self, run, rich, fake_zfs, monkeypatch):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "snapA"})
        fake_zfs.add("tank/other", {ANCHOR + "offsite": "snapC"})
        from zfsbackup.config.store import writer_lock_path

        lock_path = writer_lock_path(resolve_config_path(str(rich)))
        probes = []
        import zfsbackup.backup_manager as bm

        real = bm.copy_anchor_property

        def probing(*args, **kwargs):
            probes.append(_try_exclusive_lock(lock_path))
            return real(*args, **kwargs)

        monkeypatch.setattr(bm, "copy_anchor_property", probing)
        assert run("rename", "destination", "offsite", "offsite2", "-c", rich).exit_code == 0
        assert probes == ["blocked", "blocked"]          # held during every copy
        assert _try_exclusive_lock(lock_path) == "acquired"   # and released afterwards

    def test_no_database_connection_is_open_during_the_zfs_calls(self, run, rich, fake_zfs, monkeypatch):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "snapA"})
        fake_zfs.add("tank/other", {ANCHOR + "offsite": "snapC"})
        import zfsbackup.backup_manager as bm

        real = bm.copy_anchor_property
        sidecars = []

        def probing(*args, **kwargs):
            sidecars.append(sorted(p.name for p in rich.parent.iterdir() if p.name.endswith(("-wal", "-shm"))))
            return real(*args, **kwargs)

        monkeypatch.setattr(bm, "copy_anchor_property", probing)
        run("rename", "destination", "offsite", "offsite2", "-c", rich)
        assert sidecars == [[], []]

    def test_a_dry_run_takes_no_lock_and_no_generation_check(self, run, rich, fake_zfs, monkeypatch):
        fake_zfs.add("tank/data", {ANCHOR + "offsite": "snapA"})
        fake_zfs.add("tank/other", {})
        monkeypatch.setattr(rename, "writer_lock", lambda *a, **k: pytest.fail("a dry run takes no lock"))
        assert run("rename", "destination", "offsite", "offsite2", "-d", "-c", rich).exit_code == 0

    def test_require_generation_reads_the_stored_generation(self, run, rich):
        snapshot = read_config(resolve_config_path(str(rich)))
        dispose_all()
        resolved = resolve_config_path(str(rich))
        rename._require_generation(resolved, snapshot)             # unchanged: fine
        run("set", "check_interval", "10m", "-c", rich)
        with pytest.raises(ConfigConflict) as excinfo:
            rename._require_generation(resolved, snapshot)
        assert (excinfo.value.expected, excinfo.value.actual) == (snapshot.generation, snapshot.generation + 1)
